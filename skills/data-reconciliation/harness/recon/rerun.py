"""Schema-evolution rerun proof. A job that only works on a fresh table is not idempotent, so
the idempotency proof runs twice: `fresh` (empty target) and `evolved` (the table pre-created in
its previous committed shape). The child runs both and records the shape the catalog shows after
each (`dbx-recon shape`, read-only); this module grades the catalog, never the DDL: the shape the
fresh run landed is the expected shape, the evolved run must land the identical one, and the
evolved leg counts only when it started from the prior committed proof's shape (`--prior-proof`)
or the manifest-declared old shape on a first run (`--prior-shape`). The proof is bound to the
job's source files by digest, so any edit to the job makes an older proof stale. DDL parsing is a
best-effort hint that can add notes and nothing else. Nothing here connects anywhere.

Shape file: {"tables": {"<table>": [{"name", "type", "nullable"}, ...]}}; names as the catalog
reports them (a quoted `"Orders"` stays `Orders`), types folded by `normalize_type`.
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
PROOF_KEYS = ("unit", "fresh", "evolved", "passed", "findings", "notes", "evidence", "source_digest",
              "shape", "shape_digest")

# spellings folded so two catalogs' renderings of one type compare equal
_TYPE_ALIASES = {
    "integer": "int", "character varying": "varchar", "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz", "double precision": "double", "boolean": "boolean",
    "numeric": "decimal", "character": "char",
}
# PostgreSQL format_type puts the precision before the zone words: timestamp(6) without time zone
_TZ = re.compile(r"\b(timestamp|time)\s*(\(\s*\d+\s*\))?\s+(with|without)\s+time\s+zone")
_QNAME = r"(?:`[^`]+`|\"[^\"]+\"|\[(?:[^\]]|\]\])+\]|[A-Za-z_][\w$]*)"
_CREATE_ANY = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+|TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?"
    rf"TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>(?:{_QNAME}\.)*{_QNAME})", re.IGNORECASE)
_COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)


def normalize_type(raw: str) -> str:
    t = re.sub(r"\s+", " ", str(raw).strip().lower())
    t = _TZ.sub(lambda m: m.group(1) + ("tz" if m.group(3) == "with" else "") + (m.group(2) or ""), t)
    for long, short in _TYPE_ALIASES.items():
        t = re.sub(rf"\b{re.escape(long)}\b", short, t)
    return re.sub(r"\s*([<>(),:])\s*", r"\1", t).replace(" ", "")


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
            rows.append({"name": c["name"], "type": normalize_type(c["type"]), "nullable": c["nullable"]})
        out[str(table)] = rows
    return {"tables": out}


def shape_digest(shape: dict) -> str:
    return hashlib.sha256(json.dumps(shape["tables"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def source_digest(paths: list[Path]) -> str:
    """sha256 over the job's source files (name and bytes, order-independent): the proof names the
    job it graded and `run` refuses one made stale by any later edit to any of those files."""
    if not paths:
        raise ConfigError("at least one --source file (the job's DDL, notebook or SQL) is required")
    h = hashlib.sha256()
    for p in sorted(Path(p) for p in paths):
        try:
            data = p.read_bytes()
        except OSError as exc:
            raise ConfigError(f"cannot read source {p}: {exc}") from None
        h.update(p.name.encode() + b"\0" + str(len(data)).encode() + b"\0" + data)
    return h.hexdigest()


def ddl_tables(sql: str) -> list[str]:
    """Best-effort hint: the tables `CREATE TABLE` statements name, comments aside. Never a gate input."""
    seen: dict[str, None] = {}
    for m in _CREATE_ANY.finditer(_COMMENT.sub(" ", sql)):
        name = ".".join(p.strip('`"[]').replace("]]", "]") for p in m.group("name").split("."))
        seen.setdefault(name, None)
    return list(seen)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from None


def load_shape(path: Path) -> dict:
    return _check_shape(_read_json(path), str(path))


def load_prior(path: Path, unit: str) -> dict:
    """The previous committed shape: this unit's earlier rerun_proof.json (its observed `shape`,
    which must still match its `shape_digest`) or a declared shape file."""
    data = _read_json(path)
    if isinstance(data, dict) and "unit" in data and "shape" in data:
        if data["unit"] != unit:
            raise ConfigError(f"{path}: prior proof is for unit {data['unit']!r}, not {unit!r}")
        if not isinstance(data["shape"], dict):
            raise ConfigError(f"{path}: prior proof carries no observed shape; its fresh leg failed")
        shape = _check_shape(data["shape"], str(path))
        if data.get("shape_digest") != shape_digest(shape):
            raise ConfigError(f"{path}: prior proof's shape does not match its shape_digest")
        return shape
    return _check_shape(data, str(path))


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


def _compare(run: str, expected: dict[str, list[dict]], observed: dict[str, list[dict]]) -> list[dict]:
    """Findings where `observed` (a leg's shape) departs from `expected` (the fresh run's shape).
    Both come from the same catalog reader, so table and column names compare as reported."""
    findings = []

    def add(table, check, column, detail):
        findings.append({"run": run, "table": table, "check": check, "column": column, "detail": detail})
    for table, want in expected.items():
        got = observed.get(table)
        if got is None:
            add(table, "table_missing", None, "not in the observed shape")
            continue
        by_name = {c["name"]: c for c in got}
        want_names, got_names = [c["name"] for c in want], [c["name"] for c in got]
        if sorted(want_names) == sorted(got_names) and want_names != got_names:
            add(table, "column_order", None,
                f"fresh run landed {', '.join(want_names)}; observed {', '.join(got_names)}")
        for col in want:
            obs = by_name.pop(col["name"], None)
            if obs is None:
                add(table, "column_missing", col["name"], f"fresh run landed {col['type']}, absent after the run")
                continue
            if obs["type"] != col["type"]:
                add(table, "type_mismatch", col["name"], f"fresh run landed {col['type']}, observed {obs['type']}")
            if obs["nullable"] != col["nullable"]:
                add(table, "nullability_mismatch", col["name"],
                    f"fresh run landed {'NULL' if col['nullable'] else 'NOT NULL'}, observed "
                    f"{'NULL' if obs['nullable'] else 'NOT NULL'}")
        for name, obs in by_name.items():
            add(table, "column_extra", name, f"observed {obs['type']}, not landed by the fresh run")
    for table in observed:
        if table not in expected:
            add(table, "column_extra", None, "table observed after the run, not landed by the fresh run")
    return findings


def _job_failed(run: str, record: dict) -> list[dict]:
    return [{"run": run, "table": None, "check": "job_failed", "column": None,
             "detail": f"the child reported the {run} run failed ({record['evidence']})"}]


def grade_rerun(fresh: dict | None, evolved: dict | None, prior: dict | None = None, *,
                digest: str, ddl: str | None = None) -> dict:
    """Records come from `load_record` (None when the child did not run that leg); `prior` is the
    previous committed shape (`load_prior`), which the evolved leg's pre_shape must equal for that
    leg to count as evolution; `digest` is `source_digest` of the job's files; `ddl` only adds notes."""
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
    expected = fresh["shape"]["tables"]
    findings: list[dict] = []
    if fresh["status"] == "fail":
        fresh_status, findings = "fail", _job_failed("fresh", fresh)
    elif not expected:
        fresh_status = "fail"
        findings = [{"run": "fresh", "table": None, "check": "no_tables", "column": None,
                     "detail": "the fresh run recorded no table; nothing to prove a rerun against"}]
    else:
        fresh_status = "pass"
    evolved_status, reason = "unsupported", None
    if evolved is not None and evolved["status"] == "fail":
        evolved_status = "fail"
        findings += _job_failed("evolved", evolved)
    elif evolved is None:
        reason = ("no evolved record: pre-create the table in its previous committed shape (the "
                  "prior proof's shape) and run again")
    elif fresh_status == "fail":
        reason = "the fresh run failed, so there is no shape the evolved run can be held to"
    elif "pre_shape" not in evolved:
        reason = "evolved record has no pre_shape: the shape before the run was not recorded"
    elif any(t not in evolved["pre_shape"]["tables"] for t in expected):
        missing = next(t for t in expected if t not in evolved["pre_shape"]["tables"])
        reason = (f"evolved pre_shape has no {missing}: the table did not exist before the run, "
                  "so that leg was a fresh run")
    elif not _compare("evolved", expected, evolved["pre_shape"]["tables"]):
        reason = ("evolved pre_shape equals the fresh shape: nothing evolved, so the run proves "
                  "only what fresh proved")
    elif prior is None:
        reason = ("no prior shape: pass --prior-proof (the previous committed rerun_proof.json) or "
                  "--prior-shape (the declared old shape) so the evolved leg can be checked against it")
    elif _compare("evolved", prior["tables"], evolved["pre_shape"]["tables"]):
        drift = _compare("evolved", prior["tables"], evolved["pre_shape"]["tables"])
        reason = ("evolved pre_shape is not the prior committed shape: " + "; ".join(
            f"{f['table']}.{f['column']}: {f['check']}" if f["column"] else f"{f['table']}: {f['check']}"
            for f in drift))
    else:
        more = _compare("evolved", expected, evolved["shape"]["tables"])
        evolved_status = "fail" if more else "pass"
        findings += more
    notes = []
    if ddl is not None:
        hinted = ddl_tables(ddl)
        if not hinted:
            notes.append("DDL hint: no CREATE TABLE found in the DDL")
        notes += [f"DDL hint: {t} is created by the DDL and was not recorded by the fresh run"
                  for t in hinted if t not in expected and t.rsplit(".", 1)[-1] not in expected]
    out = {
        "fresh": fresh_status,
        "evolved": evolved_status,
        "passed": fresh_status == "pass" and evolved_status != "fail",
        "findings": findings,
        "notes": notes,
        "tables": sorted(expected) if fresh_status == "pass" else [],
        "source_digest": digest,
        "evidence": {"fresh": fresh["evidence"], **({"evolved": evolved["evidence"]} if evolved else {})},
    }
    if fresh_status == "pass":
        out["shape"], out["shape_digest"] = fresh["shape"], shape_digest(fresh["shape"])
    else:
        out["shape"] = out["shape_digest"] = None
    if prior is not None:
        out["prior_digest"] = shape_digest(prior)
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
    failed, an unsupported evolved leg names its reason, every run leg has evidence, a passed
    fresh leg carries the shape it observed, and the proof graded the job's files as they are now
    (`digest`, from `source_digest`)."""
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: rerun proof must be a JSON object")
    for key in PROOF_KEYS:
        if key not in data:
            raise ConfigError(f"{where}: rerun proof lacks {key!r}; write it with dbx-recon rerun-proof")
    if data["unit"] != unit:
        raise ConfigError(f"{where}: rerun proof is for unit {data['unit']!r}, not {unit!r}")
    if not isinstance(data["source_digest"], str) or data["source_digest"] != digest:
        raise ConfigError(f"{where}: rerun proof is stale: it graded job sources "
                          f"{str(data['source_digest'])[:12]!r}, the unit's files now digest to {digest[:12]!r}; "
                          "run dbx-recon rerun-proof again")
    fresh, evolved = data["fresh"], data["evolved"]
    if fresh not in STATUSES or evolved not in STATUSES + ("unsupported",):
        raise ConfigError(f"{where}: fresh must be pass|fail and evolved pass|fail|unsupported")
    if data["passed"] is not (fresh == "pass" and evolved != "fail"):
        raise ConfigError(f"{where}: passed={data['passed']!r} disagrees with fresh={fresh}, evolved={evolved}")
    if fresh == "pass":
        shape = _check_shape(data["shape"], f"{where} shape")
        if not shape["tables"] or data["shape_digest"] != shape_digest(shape):
            raise ConfigError(f"{where}: the fresh leg passed but its shape is empty or does not match shape_digest")
    elif data["shape"] is not None or data["shape_digest"] is not None:
        raise ConfigError(f"{where}: the fresh leg failed, so the proof carries no observed shape")
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
