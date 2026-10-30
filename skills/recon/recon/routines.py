
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Callable

from .config import ConfigError

STATUSES = ("proven", "unproven", "failed")
RUN_KEYS = ("routine", "target_family", "target_branch", "snapshot", "evidence", "golden", "observed")
DEDICATED_TARGET = {
    "lakebase": re.compile(r"mig-[A-Za-z0-9_-]+-exec"),
    "databricks": re.compile(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+_exec"),
}
FIXTURE_SNAPSHOT = re.compile(r"fixture:(?P<path>[A-Za-z0-9._][A-Za-z0-9._/-]*)")

Committed = Callable[[str], bool]

def git_committed(repo: Path) -> Committed:
    repo = Path(repo)

    def git(*args: str) -> bool:
        try:
            return subprocess.run(["git", "-C", str(repo), *args], capture_output=True).returncode == 0
        except OSError:
            return False

    def committed(path: str) -> bool:
        if not path or Path(path).is_absolute() or ".." in Path(path).parts or not (repo / path).is_file():
            return False
        node = repo
        for part in Path(path).parts:
            node = node / part
            if node.is_symlink():
                return False
        return (git("cat-file", "-e", f"HEAD:{path}")
                and git("diff", "--cached", "--quiet", "HEAD", "--", path)
                and git("diff", "--quiet", "HEAD", "--", path))
    return committed

def _names(row: dict, key: str) -> list[str]:
    if key not in row:
        raise ConfigError(f"{row['routine']}: {key} must be present")
    values = row[key]
    if not isinstance(values, list):
        raise ConfigError(f"{row['routine']}: {key} must be a list")
    if any(not isinstance(v, str) or not v for v in values):
        raise ConfigError(f"{row['routine']}: {key} must be a list of {'table' if key == 'writes' else 'routine'} names")
    return [v.lower() for v in values]

def writers(dependencies: object) -> dict[str, list[str]]:
    rows = dependencies.get("routines") if isinstance(dependencies, dict) else None
    if not isinstance(rows, list):
        raise ConfigError("dependency analysis must be {routines: [...]}")
    if any(not isinstance(r, dict) or not isinstance(r.get("routine"), str) or not r["routine"] for r in rows):
        raise ConfigError("dependency analysis: every row is {routine, writes, calls, ...}")
    own: dict[str, list[str]] = {}
    calls: dict[str, list[str]] = {}
    for r in rows:
        name = r["routine"].lower()
        if name in own:
            raise ConfigError(f"dependency analysis: routine {name} appears twice")
        own[name], calls[name] = _names(r, "writes"), _names(r, "calls")
    for name, callees in calls.items():
        for c in callees:
            if c not in own:
                raise ConfigError(f"dependency analysis: {name} calls {c}, which has no row")
    out: dict[str, list[str]] = {}
    for name in own:
        tables, seen, stack = list(own[name]), {name}, list(calls[name])
        while stack:
            c = stack.pop(0)
            if c in seen:
                continue
            seen.add(c)
            tables += own[c]
            stack += calls[c]
        if tables:
            out[name] = list(dict.fromkeys(tables))
    return out

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
    if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
        raise ConfigError("golden and observed tables must be lists of row objects")
    return sorted(json.dumps(r, sort_keys=True, default=str) for r in rows)

def _row(routine: str, status: str, evidence: str | None, **extra) -> dict:
    return {"routine": routine, "status": status, "evidence": evidence, **extra}

def _grade_run(routine: str, writes: list[str], run: dict, committed: Committed) -> dict:
    missing = [k for k in RUN_KEYS if k not in run]
    if missing:
        return _row(routine, "unproven", run.get("evidence") or None,
                    reason=f"run record lacks {', '.join(missing)}")
    evidence = str(run["evidence"] or "")
    if not evidence:
        return _row(routine, "unproven", None, reason="run record has no evidence")
    record = run.get("record")
    if not isinstance(record, str) or not record:
        return _row(routine, "unproven", evidence, reason="run record location unknown (load it with load_runs)")
    if record != evidence:
        return _row(routine, "unproven", evidence, reason=f"run record {record} is not its evidence file {evidence}")
    snapshot = run["snapshot"]
    snap = FIXTURE_SNAPSHOT.fullmatch(snapshot) if isinstance(snapshot, str) else None
    if snap is None:
        return _row(routine, "unproven", evidence,
                    reason=f"snapshot {snapshot!r} is not a committed fixture snapshot (fixture:<id>)")
    if not committed(evidence):
        return _row(routine, "unproven", evidence, reason=f"evidence {evidence} is not a committed file")
    if not committed(snap.group("path")):
        return _row(routine, "unproven", evidence,
                    reason=f"fixture snapshot {snap.group('path')} is not a committed file")
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

def grade_routines(dependencies: dict, runs: list[dict], committed: Committed) -> dict:
    writing = writers(dependencies)
    known = {r["routine"].lower() for r in dependencies["routines"]}
    by_routine: dict[str, dict] = {}
    for run in runs:
        name = str(run.get("routine", "")).lower()
        if name not in known:
            raise ConfigError(f"run for {name or '<unnamed>'}: routine is not in the dependency analysis")
        if name not in writing:
            continue  # a read-only routine has nothing to prove here
        if name in by_routine:
            raise ConfigError(f"routine {name} has a run record twice; one committed run proves it")
        by_routine[name] = run
    parity = []
    for routine, writes in writing.items():
        run = by_routine.get(routine)
        if run is None:
            parity.append(_row(routine, "unproven", None, reason="no committed run"))
        else:
            parity.append(_grade_run(routine, writes, run, committed))
    return {"routine_parity": parity,
            "unproven": [r["routine"] for r in parity if r["status"] == "unproven"],
            "failed": [r["routine"] for r in parity if r["status"] == "failed"]}

def routine_gap(parity: list[dict] | None) -> bool:
    return bool(parity) and any(r.get("status") == "failed" for r in parity)

def parity_missing(parity: list[dict] | None, writers: list[str] | None) -> list[str]:
    if not writers:
        return []
    listed = {str(r.get("routine", "")).lower() for r in parity or []}
    return [w for w in writers if w.lower() not in listed]

def check_parity(data: object, where: str, dependencies: object = None,
                 committed: Committed | None = None, repo: Path | None = None) -> list[dict]:
    if not isinstance(data, list):
        raise ConfigError(f"{where}: routine_parity must be a list")
    writing = writers(dependencies) if dependencies is not None else None
    for r in data:
        if not isinstance(r, dict) or not r.get("routine") or r.get("status") not in STATUSES:
            raise ConfigError(f"{where}: each row is {{routine, status: proven|unproven|failed, evidence}}")
        if r["status"] != "unproven" and not r.get("evidence"):
            raise ConfigError(f"{where}: {r['routine']} is {r['status']} without evidence")
    seen = [str(r["routine"]).lower() for r in data]
    for name in seen:
        if seen.count(name) > 1:
            raise ConfigError(f"{where}: {name} appears twice")
    rows = []
    for r in data:
        if r["status"] != "unproven" or r.get("evidence"):
            if committed is None or writing is None:
                raise ConfigError(f"{where}: {r['routine']} is {r['status']}; the dependency analysis and a "
                                  "committed-file check are needed to grade its run again")
            if repo is None:
                raise ConfigError(f"{where}: {r['routine']} is {r['status']}; the repository root is needed "
                                  "to read its run record")
            r = _regrade(r, where, writing, committed, Path(repo))
        rows.append(r)
    if writing is None:
        return rows
    for name in seen:
        if name not in writing:
            raise ConfigError(f"{where}: {name} is not in the dependency analysis as a writing routine")
    return rows + [_row(routine, "unproven", None, reason=f"no row in {where}")
                   for routine in writing if routine not in seen]

def _regrade(claim: dict, where: str, writing: dict[str, list[str]], committed: Committed, repo: Path) -> dict:
    routine, evidence = str(claim["routine"]), str(claim["evidence"])
    writes = writing.get(routine.lower())
    if writes is None:
        raise ConfigError(f"{where}: {routine} is not in the dependency analysis as a writing routine")
    path = repo / evidence
    if not path.is_file():
        return _row(routine, "unproven", evidence,
                    reason=f"{where}: cannot read evidence {evidence}: not a committed file")
    try:
        run = load_runs(path, repo)[0]
    except ConfigError as exc:
        return _row(routine, "unproven", evidence, reason=f"{where}: cannot read evidence {evidence}: {exc}")
    if str(run.get("routine", "")).lower() != routine.lower():
        return _row(routine, "unproven", evidence,
                    reason=f"{where}: evidence {evidence} is a run of {run.get('routine')!r}, not {routine}")
    graded = _grade_run(routine.lower(), writes, run, committed)
    if claim["status"] != "unproven" and graded["status"] not in (claim["status"], "unproven"):
        raise ConfigError(f"{where}: {routine} claims {claim['status']}; its committed run {evidence} "
                          f"grades {graded['status']}")
    return graded

def _walk(path: Path) -> Path:
    parts = Path(os.path.join(os.getcwd(), path)).parts
    node, crossed = Path(parts[0]), None
    for part in parts[1:]:
        if part == "..":
            if crossed is not None:
                raise ConfigError(f"{path}: .. after the symlink {crossed} leaves the repository unseen")
            node = node.parent
        elif part != ".":
            node = node / part
            if crossed is None and node.is_symlink():
                crossed = node
    return node

def load_runs(path: Path, repo: Path = Path(".")) -> list[dict]:
    files = sorted(path.glob("*.run.json")) if path.is_dir() else [path]
    root = _walk(Path(repo))
    runs = []
    for f in files:
        rel = Path(os.path.relpath(_walk(f), root))
        if ".." in rel.parts:
            raise ConfigError(f"{f}: is outside the repository {root}")
        node = root
        for part in rel.parts:
            node = node / part
            if node.is_symlink():
                raise ConfigError(f"{rel.as_posix()}: is a symlink; a committed run record is a regular file")
        real_root, real = os.path.realpath(root), os.path.realpath(f)
        if os.path.commonpath([real_root, real]) != real_root:
            raise ConfigError(f"{f}: is outside the repository {root} (really {real})")
        try:
            run = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigError(f"{f}: cannot read run record: {exc}") from None
        if not isinstance(run, dict):
            raise ConfigError(f"{f}: run record must be a JSON object")
        run["record"] = rel.as_posix()
        runs.append(run)
    return runs
