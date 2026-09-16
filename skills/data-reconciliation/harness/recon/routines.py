"""Behavioural parity for writing routines.

A converted routine that writes (its `dependencies.json` row has `writes`) is proven by one committed
run on a dedicated target branch against a fixture snapshot: the rows it left in each written table
are compared to a golden set. Anything less is `unproven`, listed as such in the evidence packet and
as a cutover exception; a run whose rows differ is `failed` and blocks merge (`routine_gap`).

    routine_parity: [{routine, status: proven|unproven|failed, evidence, reason?, findings?}]

Run record (one JSON per routine, `<dir>/*.run.json`):
    {routine, target_family, target_branch, snapshot, evidence,
     golden: {table: [rows]}, observed: {table: [rows]}}
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from .config import ConfigError

STATUSES = ("proven", "unproven", "failed")
RUN_KEYS = ("routine", "target_family", "target_branch", "snapshot", "evidence", "golden", "observed")
# The branch/schema a routine may be exercised in is dedicated to execution proofs, never the
# migration target itself: a Lakebase branch `mig-<pipeline>-exec`, a UC schema `<catalog>.<pipeline>_exec`.
DEDICATED_TARGET = {
    "lakebase": re.compile(r"mig-[A-Za-z0-9_-]+-exec"),
    "databricks": re.compile(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+_exec"),
}
# a run proves a routine only against a committed fixture snapshot, named `fixture:<id>`
FIXTURE_SNAPSHOT = re.compile(r"fixture:[A-Za-z0-9][A-Za-z0-9._/-]*")


def _writers(dependencies: object) -> dict[str, list[str]]:
    rows = dependencies.get("routines") if isinstance(dependencies, dict) else None
    if not isinstance(rows, list):
        raise ConfigError("dependency analysis must be {routines: [...]}")
    return {str(r["routine"]).lower(): [str(t).lower() for t in r.get("writes", [])]
            for r in rows if r.get("writes")}


def _tables(mapping: object, routine: str, which: str) -> dict[str, object]:
    if not isinstance(mapping, dict):
        raise ConfigError(f"{routine}: golden and observed must map table -> rows")
    out: dict[str, object] = {}
    for table, rows in mapping.items():
        key = str(table).lower()
        if key in out:
            raise ConfigError(f"{routine}: {which} names {key} twice (differing only in case)")
        out[key] = rows
    return out


def _canon(rows) -> list[str]:
    if not isinstance(rows, list):
        raise ConfigError("golden and observed tables must be lists of row objects")
    return sorted(json.dumps(r, sort_keys=True, default=str) for r in rows)


def _row(routine: str, status: str, evidence: str | None, **extra) -> dict:
    return {"routine": routine, "status": status, "evidence": evidence, **extra}


def _grade_run(routine: str, writes: list[str], run: dict) -> dict:
    missing = [k for k in RUN_KEYS if k not in run]
    if missing:
        return _row(routine, "unproven", run.get("evidence") or None,
                    reason=f"run record lacks {', '.join(missing)}")
    evidence = str(run["evidence"] or "")
    if not evidence:
        return _row(routine, "unproven", None, reason="run record has no evidence")
    snapshot = run["snapshot"]
    if not isinstance(snapshot, str) or not FIXTURE_SNAPSHOT.fullmatch(snapshot):
        return _row(routine, "unproven", evidence,
                    reason=f"snapshot {snapshot!r} is not a committed fixture snapshot (fixture:<id>)")
    family, branch = str(run["target_family"]), str(run["target_branch"])
    pattern = DEDICATED_TARGET.get(family)
    if pattern is None:
        return _row(routine, "unproven", evidence,
                    reason=f"no dedicated execution target rule for family {family}")
    if not pattern.fullmatch(branch):
        return _row(routine, "unproven", evidence,
                    reason=f"{branch} is not a dedicated execution target ({pattern.pattern})")
    golden = _tables(run["golden"], routine, "golden")
    observed = _tables(run["observed"], routine, "observed")
    findings = []
    for table in writes:
        if table not in golden:
            findings.append({"table": table, "check": "no_golden",
                             "detail": "written by the routine but not in the golden set"})
            continue
        if table not in observed:
            findings.append({"table": table, "check": "table_unobserved",
                             "detail": "written by the routine but not in the observed set"})
            continue
        g, o = _canon(golden[table]), _canon(observed[table])
        if g != o:
            gs, os_ = set(g), set(o)
            findings.append({"table": table, "check": "rows_differ",
                             "detail": f"golden {len(g)} rows, observed {len(o)} rows; "
                                       f"{len(gs - os_)} only in golden, {len(os_ - gs)} only in observed"})
    if findings:
        return _row(routine, "failed", evidence, findings=findings)
    return _row(routine, "proven", evidence)


def grade_routines(dependencies: dict, runs: list[dict]) -> dict:
    writers = _writers(dependencies)
    known = {str(r["routine"]).lower() for r in dependencies["routines"]}
    by_routine: dict[str, dict] = {}
    for run in runs:
        name = str(run.get("routine", "")).lower()
        if name not in known:
            raise ConfigError(f"run for {name or '<unnamed>'}: routine is not in the dependency analysis")
        if name not in writers:
            continue  # a read-only routine has nothing to prove here
        if name in by_routine:
            raise ConfigError(f"routine {name} has a run record twice; one committed run proves it")
        by_routine[name] = run
    parity = []
    for routine, writes in writers.items():
        run = by_routine.get(routine)
        if run is None:
            parity.append(_row(routine, "unproven", None, reason="no committed run"))
        else:
            parity.append(_grade_run(routine, writes, run))
    return {"routine_parity": parity,
            "unproven": [r["routine"] for r in parity if r["status"] == "unproven"],
            "failed": [r["routine"] for r in parity if r["status"] == "failed"]}


def routine_gap(parity: list[dict] | None) -> bool:
    return bool(parity) and any(r.get("status") == "failed" for r in parity)


def check_parity(data: object, where: str, dependencies: object = None) -> list[dict]:
    """Validate a routine_parity list before result.json carries it. With the unit's dependency
    analysis, every writing routine gets a row: one the list lacks is `unproven`, and a row for a
    routine the analysis does not know (another unit's file) is refused."""
    if not isinstance(data, list):
        raise ConfigError(f"{where}: routine_parity must be a list")
    for r in data:
        if not isinstance(r, dict) or not r.get("routine") or r.get("status") not in STATUSES:
            raise ConfigError(f"{where}: each row is {{routine, status: proven|unproven|failed, evidence}}")
        if r["status"] != "unproven" and not r.get("evidence"):
            raise ConfigError(f"{where}: {r['routine']} is {r['status']} without evidence")
    if dependencies is None:
        return data
    writers = _writers(dependencies)
    seen = [str(r["routine"]).lower() for r in data]
    for name in seen:
        if name not in writers:
            raise ConfigError(f"{where}: {name} is not in the dependency analysis as a writing routine")
    return data + [_row(routine, "unproven", None, reason=f"no row in {where}")
                   for routine in writers if routine not in seen]


def load_runs(path: Path) -> list[dict]:
    files = sorted(path.glob("*.run.json")) if path.is_dir() else [path]
    runs = []
    for f in files:
        try:
            run = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigError(f"{f}: cannot read run record: {exc}") from None
        if not isinstance(run, dict):
            raise ConfigError(f"{f}: run record must be a JSON object")
        runs.append(run)
    return runs
