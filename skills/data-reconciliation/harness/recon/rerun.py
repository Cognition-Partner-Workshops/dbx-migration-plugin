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
    "numeric": "decimal", "character": "char",
}
# tokens that end a column's type in a column definition
_COLUMN_CLAUSE = re.compile(
    r"\b(NOT\s+NULL|NULL|COMMENT|DEFAULT|GENERATED|PRIMARY\s+KEY|UNIQUE|CHECK|COLLATE|REFERENCES|"
    r"CONSTRAINT|MASK|IDENTITY)\b", re.IGNORECASE)
_CONSTRAINT_START = re.compile(r"^(CONSTRAINT|PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|LIKE)\b",
                               re.IGNORECASE)
_QNAME = r"(?:`[^`]+`|\"[^\"]+\"|\[(?:[^\]]|\]\])+\]|[A-Za-z_][\w$]*)"
_TNAME = rf"(?:{_QNAME}\.)*{_QNAME}"
_NAME = re.compile(rf"^{_TNAME}$")
_CREATE = re.compile(
    r"^CREATE\s+(?P<replace>OR\s+REPLACE\s+)?(?:EXTERNAL\s+|TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?"
    rf"TABLE\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?(?P<name>{_TNAME})\s*\(", re.IGNORECASE | re.DOTALL)
_ALTER_ADD = re.compile(
    rf"^ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<name>{_TNAME})\s+ADD\s+COLUMNS?\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<body>.*)$", re.IGNORECASE | re.DOTALL)
# ALTER TABLE forms that leave the column shape alone; any other ALTER TABLE is a refusal
_ALTER_NEUTRAL = re.compile(
    rf"^ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?{_TNAME}\s+(?:SET|UNSET)\s+(?:TBLPROPERTIES|OWNER)\b",
    re.IGNORECASE | re.DOTALL)
_ALTER = re.compile(r"^ALTER\s+TABLE\b", re.IGNORECASE)
_CREATE_ANY = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+|TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?"
    rf"TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>{_TNAME})", re.IGNORECASE | re.DOTALL)
_TABLE_PK = re.compile(r"^(?:CONSTRAINT\s+\S+\s+)?PRIMARY\s+KEY\s*\(", re.IGNORECASE | re.DOTALL)
_DROP = re.compile(r"^DROP\s+TABLE\b(?P<rest>.*)$", re.IGNORECASE | re.DOTALL)
_DROP_HEAD = re.compile(r"^\s*(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?", re.IGNORECASE)
_DROP_TAIL = re.compile(r"\s+(?:CASCADE|RESTRICT)\s*$", re.IGNORECASE)


_SERIAL = {"serial": "int", "serial4": "int", "bigserial": "bigint", "serial8": "bigint",
           "smallserial": "smallint", "serial2": "smallint"}
# PostgreSQL format_type puts the precision before the zone words: timestamp(6) without time zone
_TZ = re.compile(r"\b(timestamp|time)\s*(\(\s*\d+\s*\))?\s+(with|without)\s+time\s+zone")


def normalize_type(raw: str) -> str:
    t = re.sub(r"\s+", " ", str(raw).strip().lower())
    t = _TZ.sub(lambda m: m.group(1) + ("tz" if m.group(3) == "with" else "") + (m.group(2) or ""), t)
    for long, short in _TYPE_ALIASES.items():
        t = re.sub(rf"\b{re.escape(long)}\b", short, t)
    return re.sub(r"\s*([<>(),:])\s*", r"\1", t).replace(" ", "")


def _ident(raw: str) -> str:
    return ".".join(p.strip('`"[]').replace("]]", "]").lower() for p in raw.strip().split("."))


def _chars(text: str):
    """(char, quoted) per character. Quotes are '...', \"...\", `...` and T-SQL [...] where a
    doubled `]]` is a literal bracket."""
    quote, i = None, 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                if quote == "]" and text[i + 1:i + 2] == "]":
                    yield ch, True
                    yield ch, True
                    i += 2
                    continue
                quote = None
            yield ch, True
        elif ch in "'\"`[":
            quote = "]" if ch == "[" else ch
            yield ch, True
        else:
            yield ch, False
        i += 1


def _split_top(body: str, sep: str = ",", angle: bool = True) -> list[str]:
    """Split on `sep` outside quotes and parentheses; `<...>` nests too when `angle` (a column
    list, where it is a type's brackets), never when splitting statements (where it compares)."""
    parts, depth, angles, buf = [], 0, 0, []
    for ch, quoted in _chars(body):
        if quoted:
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "<" and angle and depth == 0:
            angles += 1
        elif ch == ">" and angle and depth == 0 and angles:
            angles -= 1
        elif ch == sep and depth == 0 and angles == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _balanced(text: str, start: int) -> int:
    """Index just past the parenthesis group opening at text[start] == '('."""
    depth = 0
    for i, (ch, quoted) in enumerate(_chars(text[start:]), start):
        if quoted:
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    raise ConfigError("unbalanced parenthesis in CREATE TABLE")


def _top_level(text: str) -> str:
    """`text` with every quoted literal and bracketed group blanked to spaces (same length), so
    keyword searches see only the top level and offsets still index the original."""
    out, depth, angles = [], 0, 0
    for ch, quoted in _chars(text):
        if quoted:
            out.append(" ")
        elif ch == "(":
            depth += 1
            out.append(" ")
        elif ch == ")":
            depth = max(depth - 1, 0)
            out.append(" ")
        elif ch == "<" and depth == 0:
            angles += 1
            out.append(" ")
        elif ch == ">" and depth == 0 and angles:
            angles -= 1
            out.append(" ")
        else:
            out.append(ch if depth == 0 and angles == 0 else " ")
    return "".join(out)


def _column(defn: str) -> dict | None:
    if _CONSTRAINT_START.match(defn):
        return None
    m = re.match(r"^(`[^`]+`|\"[^\"]+\"|\[(?:[^\]]|\]\])+\]|[A-Za-z_][\w$]*)\s+(.*)$", defn, re.DOTALL)
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
    kind = normalize_type(type_text)
    if kind in _SERIAL:
        return {"name": name, "type": _SERIAL[kind], "nullable": False}
    return {"name": name, "type": kind, "nullable": not not_null}


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


def _dropped_tables(rest: str) -> list[str]:
    """Every table a DROP TABLE names: `[IF EXISTS] [ONLY] t1, t2 [CASCADE|RESTRICT]`. Anything
    else in the statement is a refusal, never a partial reading."""
    body = _DROP_TAIL.sub("", rest[_DROP_HEAD.match(rest).end():]).strip()
    names = [n.strip() for n in _split_top(body, angle=False)] if body else []
    if not names or body.endswith(",") or any(not _NAME.match(n) for n in names):
        raise ConfigError(f"cannot read DROP TABLE{rest.rstrip(';')[:60]}: only "
                          "[IF EXISTS] [ONLY] <table>[, <table>...] [CASCADE|RESTRICT] is understood")
    return [_ident(n) for n in names]


def _strip_comments(sql: str) -> str:
    """Blank `-- ...` and `/* ... */` outside quoted literals and identifiers (a `--` inside a
    string is text, not a comment)."""
    out, i, quote, n = [], 0, None, len(sql)
    while i < n:
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                if quote == "]" and sql[i + 1:i + 2] == "]":
                    out.append("]")
                    i += 2
                    continue
                quote = None
            i += 1
        elif ch in "'\"`[":
            quote = "]" if ch == "[" else ch
            out.append(ch)
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end
            out.append(" ")
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            i = n if end < 0 else end + 2
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


_PLACEMENT = re.compile(r"\b(?:(?P<first>FIRST)|AFTER\s+(?P<after>\S+))\s*$", re.IGNORECASE)
_ADD_ACTION = re.compile(r"^ADD\s+(?:COLUMNS?\s+)?(?:IF\s+NOT\s+EXISTS\s+)?", re.IGNORECASE)


def _add_column(cols: list[dict], defn: str) -> None:
    """Apply one ADD COLUMN definition, honouring a trailing FIRST / AFTER <column> placement."""
    m = _PLACEMENT.search(_top_level(defn))
    c = _column(defn[:m.start()].rstrip() if m else defn)
    if not c or any(x["name"] == c["name"] for x in cols):
        return
    if m and m.group("after"):
        anchor = _ident(m.group("after"))
        at = [i for i, x in enumerate(cols) if x["name"] == anchor]
        if not at:
            raise ConfigError(f"ADD COLUMN {c['name']} AFTER {anchor}: no such column")
        cols.insert(at[0] + 1, c)
    elif m:
        cols.insert(0, c)
    else:
        cols.append(c)


def _collapse_ws(stmt: str) -> str:
    """Whitespace between tokens folded to one space; quoted literals and identifiers kept as
    written, so two statements hash equal only when they say the same thing."""
    out, i, n = [], 0, len(stmt)
    quoted = [q for _, q in _chars(stmt)]
    while i < n:
        ch = stmt[i]
        if quoted[i]:
            out.append(ch)
            i += 1
        elif ch.isspace():
            while i < n and stmt[i].isspace() and not quoted[i]:
                i += 1
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out).strip()


def declared_shape(sql: str) -> dict:
    """The shape the committed DDL leaves behind when every statement takes effect: CREATE TABLE
    column lists with ALTER TABLE ... ADD COLUMN(S) applied on top. Other statements (DML, the
    job body) are counted, not read."""
    tables: dict[str, list[dict]] = {}
    if_not_exists: list[str] = []
    altered: list[str] = []
    counts = {"create_table": 0, "alter_table": 0, "other": 0}
    statements = [_collapse_ws(st) for st in _split_top(_strip_comments(sql), ";", angle=False)]
    created: set[str] = set()
    for stmt in statements:
        m = _CREATE.match(stmt)
        if m:
            name = _ident(m.group("name"))
            end = _balanced(stmt, m.end() - 1)
            body = stmt[m.end():end - 1]
            if any(re.match(r"LIKE\b", d, re.IGNORECASE) for d in _split_top(body)):
                raise ConfigError(f"CREATE TABLE {name} LIKE: the inherited columns are not in the DDL; "
                                  "pass --expected-shape instead")
            cols = _table_columns(body)
            if name in created and m.group("replace"):
                tables[name] = cols
            elif name in created and not m.group("ine"):
                raise ConfigError(f"{name} is created twice without OR REPLACE; the second CREATE TABLE "
                                  "fails when run, so this DDL lands no shape to grade")
            elif name not in created:
                tables[name] = cols
            created.add(name)
            counts["create_table"] += 1
            if m.group("ine") and name not in if_not_exists:
                if_not_exists.append(name)
            continue
        m = _DROP.match(stmt)
        if m:
            for name in _dropped_tables(m.group("rest")):
                created.discard(name)
                tables.pop(name, None)
                for lst in (if_not_exists, altered):
                    if name in lst:
                        lst.remove(name)
            counts["other"] += 1
            continue
        m = _ALTER_ADD.match(stmt)
        if m:
            name = _ident(m.group("name"))
            body = m.group("body").strip()
            if body.startswith("("):
                body = body[1:_balanced(body, 0) - 1]
            cols = tables.setdefault(name, [])
            for i, d in enumerate(_split_top(body)):
                if i and _ADD_ACTION.match(d):
                    d = d[_ADD_ACTION.match(d).end():]
                elif i and re.match(r"^(?:ADD|DROP|ALTER|RENAME|MODIFY|CHANGE|SET|UNSET)\b", d, re.IGNORECASE):
                    raise ConfigError(f"cannot apply ALTER TABLE {name} action {d[:40].strip()!r} to the "
                                      "declared shape (only ADD COLUMN(S) is applied); pass "
                                      "--expected-shape instead")
                _add_column(cols, d)
            counts["alter_table"] += 1
            if name not in altered:
                altered.append(name)
            continue
        m = _CREATE_ANY.match(stmt)
        if m:
            raise ConfigError(f"CREATE TABLE {_ident(m.group('name'))} without a column list (AS SELECT, "
                              "LIKE): the columns are not in the DDL; pass --expected-shape instead")
        if _ALTER.match(stmt) and not _ALTER_NEUTRAL.match(stmt):
            raise ConfigError(f"cannot apply ALTER TABLE {stmt[12:60].strip()!r} to the declared shape "
                              "(only ADD COLUMN(S) is applied); pass --expected-shape instead")
        counts["other"] += 1
    if not counts["create_table"]:
        raise ConfigError("no CREATE TABLE statement in the DDL; pass --expected-shape instead")
    return {"tables": tables, "if_not_exists": if_not_exists, "altered": altered, "statements": counts,
            "ddl_digest": hashlib.sha256(";".join(statements).encode()).hexdigest()}


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
    """sha256 of the declared tables and, from DDL, of the statements that land them (comments and
    whitespace aside), so a proof names what it graded and `run` refuses one made stale by a
    later change: a shape edit, or a rewrite of the statements that leaves the shape alone."""
    return hashlib.sha256(json.dumps([expected["tables"], expected.get("ddl_digest")], sort_keys=True,
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


def _candidates(observed: dict[str, list[dict]], name: str) -> list[str]:
    if name in observed:
        return [name]
    tail = name.rsplit(".", 1)[-1]
    return [obs for obs in observed
            if obs.rsplit(".", 1)[-1] == tail and (obs.endswith("." + name) or name.endswith("." + obs))]


def _resolve_tables(expected: dict[str, list[dict]], observed: dict[str, list[dict]]) -> dict[str, str | None]:
    """Expected table -> the one observed table it names, one-to-one: exact names first, then a
    trailing-name match when one side is less qualified, kept only when every maximum one-to-one
    matching agrees on it. An observation that could belong to either of two expected tables is a
    match for neither (`None`), never a shared one."""
    exact = {table for table in expected if table in observed}
    out: dict[str, str | None] = {table: table for table in exact}
    pending = {table: [c for c in _candidates(observed, table) if c not in exact]
               for table in expected if table not in exact}
    full = _matching_size(pending)
    for table, cands in pending.items():
        # an edge every maximum matching uses is the one whose removal shrinks the maximum
        forced = [c for c in cands
                  if _matching_size({**pending, table: [x for x in cands if x != c]}) < full]
        out[table] = forced[0] if len(forced) == 1 else None
    return out


def _matching_size(edges: dict[str, list[str]]) -> int:
    """Size of a maximum one-to-one matching of the left keys to their right candidates
    (augmenting paths; polynomial in the number of tables)."""
    owner: dict[str, str] = {}

    def place(left: str, seen: set[str]) -> bool:
        for right in edges[left]:
            if right in seen:
                continue
            seen.add(right)
            if right not in owner or place(owner[right], seen):
                owner[right] = left
                return True
        return False

    return sum(place(left, set()) for left in edges)


def _find_table(observed: dict[str, list[dict]], name: str) -> list[dict] | None:
    hit = _resolve_tables({name: []}, observed)[name]
    return observed[hit] if hit else None


def _compare(run: str, expected: dict[str, list[dict]], observed: dict[str, list[dict]]) -> list[dict]:
    findings = []

    def add(table, check, column, detail):
        findings.append({"run": run, "table": table, "check": check, "column": column, "detail": detail})
    resolved = _resolve_tables(expected, observed)
    for table, want in expected.items():
        hit = resolved[table]
        if hit is None:
            cands = _candidates(observed, table)
            add(table, "table_missing", None,
                (f"observed {', '.join(cands)} is ambiguous: another declared table shares its name"
                 if cands else "not in the observed shape"))
            continue
        got = observed[hit]
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


def grade_rerun(expected: dict, fresh: dict | None, evolved: dict | None, prior: dict | None = None) -> dict:
    """`expected` is `declared_shape(...)` or a `load_shape(...)` result; records come from
    `load_record` (or None when the child did not run that leg); `prior` is the previous committed
    shape, which the evolved leg's pre_shape must equal for that leg to count as evolution."""
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
    elif prior is None:
        reason = ("no prior shape: pass --prior-ddl or --prior-shape (the previous committed shape) "
                  "so the evolved leg can be checked against it")
    elif _compare("evolved", prior["tables"], evolved["pre_shape"]["tables"]):
        drift = _compare("evolved", prior["tables"], evolved["pre_shape"]["tables"])
        reason = ("evolved pre_shape is not the prior committed shape: " + "; ".join(
            f"{f['table']}.{f['column']}: {f['check']}" if f["column"] else f"{f['table']}: {f['check']}"
            for f in drift))
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
    if prior is not None:
        out["prior_digest"] = expected_digest(prior)
    if reason:
        out["unsupported_reason"] = reason
    return out


def rerun_gap(proof: dict | None) -> bool:
    """True when result.json must carry `rerun_gap`: a recorded proof whose fresh or evolved leg
    failed, or that lists any finding at all. No proof is recorded as null, never as clean."""
    return proof is not None and (proof.get("fresh") == "fail" or proof.get("evolved") == "fail"
                                  or bool(proof.get("findings")))


def rerun_missing(proof: dict | None) -> bool:
    """True when no proof was supplied at all: the unit writes its tables, so an unrecorded rerun
    is a missing control and result.json carries `rerun_missing`."""
    return proof is None


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
    if evolved == "pass" and not str(data.get("prior_digest") or "").strip():
        raise ConfigError(f"{where}: the evolved leg passed without a prior_digest naming the previous "
                          "committed shape it started from")
    return data
