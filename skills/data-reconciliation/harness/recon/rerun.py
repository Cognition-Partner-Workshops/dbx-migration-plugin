"""Schema-evolution rerun proof. A job that only works on a fresh table is not idempotent, so
the idempotency proof runs twice: `fresh` (empty target) and `evolved` (the table pre-created in
its previous committed shape, from git history or the prior wave's DDL). The child runs both and
records the shape it observed after each; this module grades those shapes against the shape the
committed DDL declares. Nothing here connects anywhere or executes DDL.

Shape file: {"tables": {"<table>": [{"name", "type", "nullable"}, ...]}}.
Run record: {"run": "fresh"|"evolved", "status": "pass"|"fail", "evidence": "<run id or path>",
             "shape": <shape>, "pre_shape": <shape>}  (pre_shape: evolved only, read before the run).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .config import ConfigError

RERUN_RECORD_KEYS = ("run", "status", "evidence", "shape")
RUNS = ("fresh", "evolved")
STATUSES = ("pass", "fail")

# spellings folded so a catalog's rendering and the DDL's compare equal
_TYPE_ALIASES = {
    "integer": "int", "character varying": "varchar", "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz", "double precision": "double", "boolean": "boolean",
    "numeric": "decimal",
}
# tokens that end a column's type in a column definition
_COLUMN_CLAUSE = re.compile(
    r"\b(NOT\s+NULL|NULL|COMMENT|DEFAULT|GENERATED|PRIMARY\s+KEY|UNIQUE|CHECK|COLLATE|REFERENCES|"
    r"CONSTRAINT|MASK|IDENTITY)\b", re.IGNORECASE)
_CONSTRAINT_START = re.compile(r"^(CONSTRAINT|PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|LIKE)\b",
                               re.IGNORECASE)
_CREATE = re.compile(
    r"^CREATE\s+(?P<replace>OR\s+REPLACE\s+)?(?:EXTERNAL\s+|TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?"
    r"TABLE\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?(?P<name>[^\s(]+)\s*\(", re.IGNORECASE | re.DOTALL)
_ALTER_ADD = re.compile(
    r"^ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<name>[^\s(]+)\s+ADD\s+COLUMNS?\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<body>.*)$", re.IGNORECASE | re.DOTALL)
# ALTER TABLE forms that leave the column shape alone; any other ALTER TABLE is a refusal
_ALTER_NEUTRAL = re.compile(
    r"^ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?[^\s(]+\s+(?:SET|UNSET)\s+(?:TBLPROPERTIES|OWNER)\b",
    re.IGNORECASE | re.DOTALL)
_ALTER = re.compile(r"^ALTER\s+TABLE\b", re.IGNORECASE)
_TABLE_PK = re.compile(r"^(?:CONSTRAINT\s+\S+\s+)?PRIMARY\s+KEY\s*\(", re.IGNORECASE | re.DOTALL)


def normalize_type(raw: str) -> str:
    t = re.sub(r"\s+", " ", str(raw).strip().lower())
    for long, short in _TYPE_ALIASES.items():
        t = re.sub(rf"\b{re.escape(long)}\b", short, t)
    return re.sub(r"\s*([<>(),:])\s*", r"\1", t).replace(" ", "")


def _ident(raw: str) -> str:
    return ".".join(p.strip('`"[]').lower() for p in raw.strip().split("."))


def _split_top(body: str, sep: str = ",") -> list[str]:
    parts, depth, buf, quote = [], 0, [], None
    for ch in body:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "'\"`":
            quote = ch
        elif ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
        elif ch == sep and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _balanced(text: str, start: int) -> int:
    """Index just past the parenthesis group opening at text[start] == '('."""
    depth, quote = 0, None
    for i in range(start, len(text)):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    raise ConfigError("unbalanced parenthesis in CREATE TABLE")


def _top_level(text: str) -> str:
    """`text` with every quoted literal and bracketed group blanked to spaces (same length), so
    keyword searches see only the top level and offsets still index the original."""
    out, depth, quote = [], 0, None
    for ch in text:
        if quote:
            out.append(" ")
            if ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
            out.append(" ")
        elif ch in "(<":
            depth += 1
            out.append(" ")
        elif ch in ")>":
            depth = max(depth - 1, 0)
            out.append(" ")
        else:
            out.append(ch if depth == 0 else " ")
    return "".join(out)


def _column(defn: str) -> dict | None:
    if _CONSTRAINT_START.match(defn):
        return None
    m = re.match(r"^(`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[A-Za-z_][\w$]*)\s+(.*)$", defn, re.DOTALL)
    if not m:
        raise ConfigError(f"cannot read column definition: {defn[:60]!r}")
    name, rest = _ident(m.group(1)), m.group(2)
    top = _top_level(rest)
    clause = _COLUMN_CLAUSE.search(top)
    type_text = rest[:clause.start()] if clause else rest
    tail = top[clause.start():] if clause else ""
    not_null = bool(re.search(r"\bNOT\s+NULL\b", tail, re.IGNORECASE)
                    or re.search(r"\bPRIMARY\s+KEY\b", tail, re.IGNORECASE))
    if not type_text.strip():
        raise ConfigError(f"column {name} has no type")
    return {"name": name, "type": normalize_type(type_text), "nullable": not not_null}


def _table_columns(body: str) -> list[dict]:
    """Columns of a CREATE TABLE body with table-level PRIMARY KEY members made NOT NULL."""
    defs = _split_top(body)
    cols = [c for c in (_column(d) for d in defs) if c]
    for d in defs:
        m = _TABLE_PK.match(d)
        if not m:
            continue
        keys = [_ident(k) for k in _split_top(d[m.end():_balanced(d, m.end() - 1) - 1])]
        for k in keys:
            hit = [c for c in cols if c["name"] == k]
            if not hit:
                raise ConfigError(f"PRIMARY KEY names {k}, missing from the column list")
            hit[0]["nullable"] = False
    return cols


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\r\n]*", " ", sql)


def declared_shape(sql: str) -> dict:
    """The shape the committed DDL leaves behind when every statement takes effect: CREATE TABLE
    column lists with ALTER TABLE ... ADD COLUMN(S) applied on top. Other statements (DML, the
    job body) are counted, not read."""
    tables: dict[str, list[dict]] = {}
    if_not_exists: list[str] = []
    altered: list[str] = []
    counts = {"create_table": 0, "alter_table": 0, "other": 0}
    for stmt in _split_top(_strip_comments(sql), ";"):
        stmt = stmt.strip()
        m = _CREATE.match(stmt)
        if m:
            name = _ident(m.group("name"))
            end = _balanced(stmt, m.end() - 1)
            tables[name] = _table_columns(stmt[m.end():end - 1])
            counts["create_table"] += 1
            if m.group("ine") and name not in if_not_exists:
                if_not_exists.append(name)
            continue
        m = _ALTER_ADD.match(stmt)
        if m:
            name = _ident(m.group("name"))
            body = m.group("body").strip()
            if body.startswith("("):
                body = body[1:_balanced(body, 0) - 1]
            cols = tables.setdefault(name, [])
            for c in (_column(d) for d in _split_top(body)):
                if c and all(x["name"] != c["name"] for x in cols):
                    cols.append(c)
            counts["alter_table"] += 1
            if name not in altered:
                altered.append(name)
            continue
        if _ALTER.match(stmt) and not _ALTER_NEUTRAL.match(stmt):
            raise ConfigError(f"cannot apply ALTER TABLE {stmt[12:60].strip()!r} to the declared shape "
                              "(only ADD COLUMN(S) is applied); pass --expected-shape instead")
        counts["other"] += 1
    if not counts["create_table"]:
        raise ConfigError("no CREATE TABLE statement in the DDL; pass --expected-shape instead")
    return {"tables": tables, "if_not_exists": if_not_exists, "altered": altered, "statements": counts}


def _check_shape(shape, where: str) -> dict:
    if not isinstance(shape, dict) or not isinstance(shape.get("tables"), dict):
        raise ConfigError(f"{where}: shape must be an object with a 'tables' object")
    out = {}
    for table, cols in shape["tables"].items():
        if not isinstance(cols, list):
            raise ConfigError(f"{where}: {table}: columns must be a list")
        rows = []
        for c in cols:
            if not isinstance(c, dict) or not isinstance(c.get("name"), str) or not isinstance(c.get("type"), str):
                raise ConfigError(f"{where}: {table}: every column needs a name and a type string")
            if not isinstance(c.get("nullable"), bool):
                raise ConfigError(f"{where}: {table}.{c['name']}: nullable must be a JSON boolean")
            rows.append({"name": _ident(c["name"]), "type": normalize_type(c["type"]),
                         "nullable": c["nullable"]})
        out[_ident(table)] = rows
    return {"tables": out}


def expected_digest(expected: dict) -> str:
    """sha256 of the declared tables, so a proof names the shape it graded and `run` can refuse
    one that a later DDL change made stale."""
    return hashlib.sha256(json.dumps(expected["tables"], sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from None


def load_shape(path: Path) -> dict:
    return _check_shape(_read_json(path), str(path))


def load_record(path: Path, run: str) -> dict:
    data = _read_json(path)
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: run record must be a JSON object")
    missing = [k for k in RERUN_RECORD_KEYS if k not in data]
    if missing:
        raise ConfigError(f"{path}: run record lacks {missing}")
    if data["run"] != run:
        raise ConfigError(f"{path}: run is {data['run']!r}, expected {run!r}")
    if data["status"] not in STATUSES:
        raise ConfigError(f"{path}: status must be one of {STATUSES}")
    if not isinstance(data["evidence"], str) or not data["evidence"].strip():
        raise ConfigError(f"{path}: evidence must name the run (job run id, log path)")
    rec = {"run": run, "status": data["status"], "evidence": data["evidence"],
           "shape": _check_shape(data["shape"], f"{path} shape")}
    if "pre_shape" in data:
        rec["pre_shape"] = _check_shape(data["pre_shape"], f"{path} pre_shape")
    return rec


def _find_table(observed: dict[str, list[dict]], name: str) -> list[dict] | None:
    if name in observed:
        return observed[name]
    tail = name.rsplit(".", 1)[-1]
    hits = [cols for obs, cols in observed.items()
            if obs.rsplit(".", 1)[-1] == tail and (obs.endswith(name) or name.endswith(obs))]
    return hits[0] if len(hits) == 1 else None


def _compare(run: str, expected: dict[str, list[dict]], observed: dict[str, list[dict]]) -> list[dict]:
    findings = []

    def add(table, check, column, detail):
        findings.append({"run": run, "table": table, "check": check, "column": column, "detail": detail})
    for table, want in expected.items():
        got = _find_table(observed, table)
        if got is None:
            add(table, "table_missing", None, "not in the observed shape")
            continue
        by_name = {c["name"]: c for c in got}
        want_names, got_names = [c["name"] for c in want], [c["name"] for c in got]
        if sorted(want_names) == sorted(got_names) and want_names != got_names:
            add(table, "column_order", None,
                f"declared {', '.join(want_names)}; observed {', '.join(got_names)}")
        for col in want:
            obs = by_name.pop(col["name"], None)
            if obs is None:
                add(table, "column_missing", col["name"], f"declared {col['type']}, absent after the run")
                continue
            if obs["type"] != col["type"]:
                add(table, "type_mismatch", col["name"], f"declared {col['type']}, observed {obs['type']}")
            if obs["nullable"] != col["nullable"]:
                add(table, "nullability_mismatch", col["name"],
                    f"declared {'NULL' if col['nullable'] else 'NOT NULL'}, observed "
                    f"{'NULL' if obs['nullable'] else 'NOT NULL'}")
        for name, obs in by_name.items():
            add(table, "column_extra", name, f"observed {obs['type']}, not declared")
    return findings


def _grade(run: str, expected: dict, record: dict) -> tuple[str, list[dict]]:
    if record["status"] == "fail":
        return "fail", [{"run": run, "table": None, "check": "job_failed", "column": None,
                         "detail": f"the child reported the {run} run failed ({record['evidence']})"}]
    findings = _compare(run, expected["tables"], record["shape"]["tables"])
    return ("fail" if findings else "pass"), findings


def grade_rerun(expected: dict, fresh: dict | None, evolved: dict | None) -> dict:
    """`expected` is `declared_shape(...)` or a `load_shape(...)` result; records come from
    `load_record` (or None when the child did not run that leg)."""
    if fresh is None:
        raise ConfigError("a fresh run record is required; the proof starts on an empty target")
    for rec, run in ((fresh, "fresh"), (evolved, "evolved")):
        if rec is not None and rec.get("run") != run:
            raise ConfigError(f"record run is {rec.get('run')!r}, expected {run!r}")
    fresh = {**fresh, "shape": _check_shape(fresh["shape"], "fresh shape")}
    if evolved is not None:
        evolved = {**evolved, "shape": _check_shape(evolved["shape"], "evolved shape"),
                   **({"pre_shape": _check_shape(evolved["pre_shape"], "evolved pre_shape")}
                      if "pre_shape" in evolved else {})}
    fresh_status, findings = _grade("fresh", expected, fresh)
    evolved_status, reason = "unsupported", None
    if evolved is not None and evolved["status"] == "fail":
        evolved_status, more = _grade("evolved", expected, evolved)
        findings.extend(more)
    elif evolved is None:
        reason = ("no evolved record: pre-create the table in its previous committed shape (git "
                  "history or the prior wave's DDL) and run again")
    elif "pre_shape" not in evolved:
        reason = "evolved record has no pre_shape: the shape before the run was not recorded"
    elif any(_find_table(evolved["pre_shape"]["tables"], t) is None for t in expected["tables"]):
        missing = next(t for t in expected["tables"]
                       if _find_table(evolved["pre_shape"]["tables"], t) is None)
        reason = (f"evolved pre_shape has no {missing}: the table did not exist before the run, "
                  "so that leg was a fresh run")
    elif not _compare("evolved", expected["tables"], evolved["pre_shape"]["tables"]):
        reason = ("evolved pre_shape equals the declared shape: nothing evolved, so the run proves "
                  "only what fresh proved")
    else:
        evolved_status, more = _grade("evolved", expected, evolved)
        findings.extend(more)
    notes = []
    for table in expected.get("if_not_exists", []):
        if table not in expected.get("altered", []):
            notes.append(f"{table}: CREATE TABLE IF NOT EXISTS with no ALTER TABLE ADD COLUMN; the "
                         "evolved run proves whether the shape still evolves")
    out = {
        "fresh": fresh_status,
        "evolved": evolved_status,
        "passed": fresh_status == "pass" and evolved_status != "fail",
        "findings": findings,
        "notes": notes,
        "tables": sorted(expected["tables"]),
        "expected_digest": expected_digest(expected),
        "evidence": {"fresh": fresh["evidence"], **({"evolved": evolved["evidence"]} if evolved else {})},
    }
    if reason:
        out["unsupported_reason"] = reason
    return out


def rerun_gap(proof: dict | None) -> bool:
    """True when result.json must carry `rerun_gap`: a recorded proof whose fresh or evolved leg
    failed, or that lists any finding at all. No proof is recorded as null, never as clean."""
    return proof is not None and (proof.get("fresh") == "fail" or proof.get("evolved") == "fail"
                                  or bool(proof.get("findings")))


def rerun_unsupported(proof: dict | None) -> bool:
    """True when a supplied proof did not exercise the previous shape: honest, but not proof,
    so result.json carries `rerun_unsupported` and merge waits for the evolved leg."""
    return proof is not None and proof.get("evolved") == "unsupported"


def check_proof(data: object, unit: str, where: str, digest: str) -> dict:
    """A rerun_proof.json written by `dbx-recon rerun-proof`, re-read by `run`. Every field is
    typed and the fields agree: `passed` follows the legs, a leg has findings exactly when it
    failed, an unsupported evolved leg names its reason, every run leg has evidence, and the
    proof graded the shape the unit declares now (`digest`, from `expected_digest`)."""
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: rerun proof must be a JSON object")
    for key in ("unit", "fresh", "evolved", "passed", "findings", "notes", "evidence", "expected_digest"):
        if key not in data:
            raise ConfigError(f"{where}: rerun proof lacks {key!r}; write it with dbx-recon rerun-proof")
    if data["unit"] != unit:
        raise ConfigError(f"{where}: rerun proof is for unit {data['unit']!r}, not {unit!r}")
    if not isinstance(data["expected_digest"], str) or data["expected_digest"] != digest:
        raise ConfigError(f"{where}: rerun proof is stale: it graded expected shape "
                          f"{str(data['expected_digest'])[:12]!r}, the unit now declares {digest[:12]!r}; "
                          "run dbx-recon rerun-proof again")
    fresh, evolved = data["fresh"], data["evolved"]
    if fresh not in STATUSES or evolved not in STATUSES + ("unsupported",):
        raise ConfigError(f"{where}: fresh must be pass|fail and evolved pass|fail|unsupported")
    if data["passed"] is not (fresh == "pass" and evolved != "fail"):
        raise ConfigError(f"{where}: passed={data['passed']!r} disagrees with fresh={fresh}, evolved={evolved}")
    findings, notes, evidence = data["findings"], data["notes"], data["evidence"]
    if not isinstance(findings, list) or not all(
            isinstance(f, dict) and {"run", "table", "check", "column", "detail"} <= set(f) for f in findings):
        raise ConfigError(f"{where}: findings must be a list of {{run, table, check, column, detail}}")
    if any(f["run"] not in RUNS for f in findings):
        raise ConfigError(f"{where}: every finding's run must be one of {RUNS}")
    if not isinstance(notes, list) or not all(isinstance(n, str) for n in notes):
        raise ConfigError(f"{where}: notes must be a list of strings")
    if not isinstance(evidence, dict):
        raise ConfigError(f"{where}: evidence must map each run leg to its run id or path")
    for leg, status in (("fresh", fresh), ("evolved", evolved)):
        has_findings = any(f["run"] == leg for f in findings)
        if (status == "fail") != has_findings:
            raise ConfigError(f"{where}: the {leg} leg is {status} but "
                              f"{'lists a finding for it' if has_findings else 'lists no finding for it'}")
        if status != "unsupported" and (not isinstance(evidence.get(leg), str) or not evidence[leg].strip()):
            raise ConfigError(f"{where}: the {leg} leg ran ({status}) but has no evidence")
    if evolved == "unsupported" and not str(data.get("unsupported_reason") or "").strip():
        raise ConfigError(f"{where}: evolved is unsupported without an unsupported_reason")
    return data
