#!/usr/bin/env python3
"""factory-doctor: preflight for a DBX migration workspace.

Runs the checks every session in an engagement depends on and writes
`.migration/09_capabilities.json`. Nothing here needs a warehouse; the Databricks checks are
identity/metadata calls only. Secret *values* are never read or printed; only the names of the
environment variables that are set.

Usage:
    python3 doctor.py [--workspace DIR] [--plugin-root DIR] [--role orchestrator|child]
                      [--hook-probe-result blocked:<nonce>|not-blocked|unknown] [--expect-identity NAME]
                      [--expect-catalogs A,B] [--no-databricks] [--unit ID ...]
                      [--mapping mapping_spec.json ...] [--source-secret NAME] [--source-family F]
                      [--param NAME=VALUE ...] [--out PATH]

Exit code 0 when `ready`; 1 otherwise. `ready` requires no `fail` anywhere, every security
control (SECURITY_CONTROLS: guard functional, hooks loaded by the platform, identity) to be `ok`,
and `source_principal_read_only` not `unverified`; an `unverified` hook probe, a human identity,
or an identity check `skipped` by `--no-databricks` is not ready (an offline report can never
authorize a wave). Other `warn`/`unverified` checks are advisory and listed in the JSON for the
playbook to decide.
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

REQUIRED_FILES = (
    "00_context.md",
    "01_conventions.md",
    "02_glossary.md",
    "03_recon_tolerances.md",
    "03_recon_tolerances.json",
    "04_dependency_register.md",
    "05_progress.md",
    "06_decisions.md",
    "07_access_checklist.md",
    "allowed_targets.json",
)
OFFICIAL_SKILLS = ("databricks-core", "databricks-dbsql", "databricks-pipelines", "databricks-jobs",
                   "databricks-dabs", "databricks-unity-catalog", "databricks-lakeflow-connect",
                   "databricks-lakebase")
M2M_VARS = ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")
SECURITY_CONTROLS = ("hook_guard_functional", "hook_platform_loaded", "databricks_identity")
# The adapters the harness can actually run (its other families fail fast, so no driver is probed for them).
DRIVERS = {"databricks": "databricks.sql", "sqlserver": "pyodbc", "postgres": "psycopg"}
# The families `dbx-recon run --family` accepts; only sqlserver and postgres have a privilege query.
SOURCE_FAMILIES = ("databricks", "oracle", "postgres", "redshift", "snowflake", "sqlserver", "teradata")
# The committed wave contract: the guard and the harness read the working copy, so a working copy
# that differs from HEAD is a contract nobody reviewed.
LEDGER_CONTRACT_FILES = (".migration/allowed_targets.json", ".migration/03_recon_tolerances.json")
CAPABILITIES = ".migration/09_capabilities.json"

# Safe live probe: if the platform loads hooks.json, the guard blocks this before it runs; if it
# does not, `echo` prints a line and nothing else happens. Either way no Databricks call is made.
# The nonce is issued per report and echoed in the guard's block reason, so `blocked:<nonce>` can
# only be passed back by a session that saw the block.
HOOK_PROBE_COMMAND = (
    "echo 'databricks experimental aitools tools query "
    "\"DROP TABLE __dbx_guard_probe__{nonce}.x.y\"' # factory-doctor hook probe: expected BLOCKED"
)


@dataclass
class Check:
    id: str
    status: str  # ok | fail | warn | unverified | skipped
    detail: str
    data: dict = field(default_factory=dict)


def _run(cmd: list[str], timeout: int = 60, cwd: Path | None = None) -> tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return r.returncode, r.stdout, r.stderr
    except FileNotFoundError:
        return 127, "", f"{cmd[0]}: not found"
    except subprocess.TimeoutExpired:
        return 124, "", f"{' '.join(cmd[:3])}: timed out after {timeout}s"


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(password|passwd|pwd|pass|token|access[_-]?token|secret|api[_-]?key|client[_-]?secret|"
    r"private[_-]?key|sas|signature|sig|authorization)\b\s*[:=]\s*(?:bearer\s+|basic\s+)?"
    r"(\"[^\"]*\"|'[^']*'|[^\s;,&]+)"
)
_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_URL_USERINFO = re.compile(r"(://[^/\s:@]+):([^@\s]+)@")
_TOKEN_SHAPED = re.compile(r"\b(?:dapi|dsapi|ghp_|gho_|xox[abp]-|sk-|AKIA|eyJ)[A-Za-z0-9._-]{8,}")


def _redact(text: str) -> str:
    """Drop anything that looks like a token or secret value from CLI/driver stderr: named
    assignments (password=, PWD=, token:, Authorization: Bearer ...), DSN/URL userinfo, known
    token prefixes, and long digit-bearing words."""
    text = _SECRET_ASSIGNMENT.sub(r"\1=<redacted>", text)
    text = _BEARER.sub(r"\1 <redacted>", text)
    text = _URL_USERINFO.sub(r"\1:<redacted>@", text)
    text = _TOKEN_SHAPED.sub("<redacted>", text)
    out = []
    for tok in text.split():
        if len(tok) > 24 and any(c.isdigit() for c in tok) and "/" not in tok and "." not in tok:
            out.append("<redacted>")
        else:
            out.append(tok)
    return " ".join(out)[:400]


# ------------------------------------------------------------------ workspace checks

def check_workspace(ws: Path) -> Check:
    mig = ws / ".migration"
    if not mig.is_dir():
        return Check("workspace", "fail", f"{mig} missing; run 1-migration_setup first")
    missing = [f for f in REQUIRED_FILES if not (mig / f).exists()]
    if missing:
        return Check("workspace", "fail", f".migration/ incomplete: missing {missing}", {"missing": missing})
    return Check("workspace", "ok", f".migration/ has all {len(REQUIRED_FILES)} required files")


def check_stop_mode(ws: Path) -> Check:
    text = ""
    for name in ("00_context.md", "01_conventions.md"):
        p = ws / ".migration" / name
        if p.exists():
            text += p.read_text(errors="replace")
    for line in text.splitlines():
        low = line.lower()
        if "stop_mode" in low:
            for mode in ("hard", "soft"):
                if mode in low.split("stop_mode", 1)[1]:
                    return Check("stop_mode", "ok", f"stop_mode: {mode}", {"stop_mode": mode})
    return Check("stop_mode", "fail", "stop_mode (hard|soft) not recorded in 00_context.md / 01_conventions.md")


def check_allowed_targets(ws: Path, plugin_root: Path) -> Check:
    p = ws / ".migration" / "allowed_targets.json"
    if not p.exists():
        return Check("allowed_targets", "fail", f"{p} missing")
    guard_path = plugin_root / "hooks" / "dbx_guard.py"
    try:
        raw = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        return Check("allowed_targets", "fail", f"{p} is not valid JSON: {e}")
    if guard_path.exists():
        spec = importlib.util.spec_from_file_location("dbx_guard", guard_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["dbx_guard"] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        try:
            cfg = mod.GuardConfig.from_dict(raw)
        except ValueError as e:
            return Check("allowed_targets", "fail", f"guard rejects {p.name}: {e}")
        data = {"catalogs": cfg.catalogs, "legacy_sources": len(cfg.legacy_sources), "guard_mode": cfg.mode}
        status = "ok" if cfg.mode == "block" else "warn"
        detail = f"catalogs={cfg.catalogs} guard_mode={cfg.mode}"
        if not cfg.legacy_sources:
            status = "warn"
            detail += "; legacy_sources empty, so legacy-write blocking relies on legacy-only client names alone"
        return Check("allowed_targets", status, detail, data)
    cats = raw.get("catalogs") if isinstance(raw, dict) else None
    if not isinstance(cats, list) or not cats:
        return Check("allowed_targets", "fail", f"{p.name} has no non-empty 'catalogs' list")
    return Check("allowed_targets", "ok", f"catalogs={cats} (guard module not found; shape check only)", {"catalogs": cats})


# ------------------------------------------------------------------ plugin / hooks / harness

def _issued_nonce(ws: Path) -> str | None:
    """The probe nonce the last report written for this workspace issued, if any."""
    try:
        checks = json.loads((ws / CAPABILITIES).read_text()).get("checks", [])
        return next(c["data"].get("probe_nonce") for c in checks if c.get("id") == "hook_platform_loaded")
    except (OSError, ValueError, StopIteration, AttributeError, KeyError):
        return None


def check_hooks(plugin_root: Path, ws: Path, probe_result: str) -> list[Check]:
    out: list[Check] = []
    hooks_json = plugin_root / "hooks.json"
    guard = plugin_root / "hooks" / "dbx_guard.py"
    if not hooks_json.exists() or not guard.exists():
        out.append(Check("hooks_files", "fail", f"hooks.json / hooks/dbx_guard.py not both present under {plugin_root}"))
        return out
    try:
        data = json.loads(hooks_json.read_text())
        pre = data["PreToolUse"][0]["hooks"][0]["command"]
        assert "dbx_guard.py" in pre
        out.append(Check("hooks_files", "ok", "hooks.json registers dbx_guard.py (PreToolUse)"))
    except (KeyError, IndexError, AssertionError, json.JSONDecodeError) as e:
        out.append(Check("hooks_files", "fail", f"hooks.json malformed: {e!r}"))
        return out

    # Functional check: feed the guard the probe event directly; it must block, and the reason must
    # echo the probe token so a blanket deny cannot pass as the guard having read the command.
    event = json.dumps({"tool_name": "exec", "tool_input": {"command": HOOK_PROBE_COMMAND.format(nonce="self")}})
    try:
        r = subprocess.run([sys.executable, str(guard)], input=event, text=True, capture_output=True,
                           timeout=30, cwd=ws, env={**os.environ, "CLAUDE_PROJECT_DIR": str(ws)})
        if r.returncode == 2 and '"block"' in r.stdout and "__dbx_guard_probe__" in r.stdout:
            out.append(Check("hook_guard_functional", "ok",
                             "dbx_guard.py blocks the probe command when invoked directly and names __dbx_guard_probe__"))
        else:
            out.append(Check("hook_guard_functional", "fail",
                             f"dbx_guard.py did not block the probe naming __dbx_guard_probe__ (rc={r.returncode}): "
                             f"{_redact(r.stderr or r.stdout)}"))
    except subprocess.TimeoutExpired:
        out.append(Check("hook_guard_functional", "fail", "dbx_guard.py timed out on the probe"))

    # Platform check: hooks are fail-open on the platform side, so only a live probe proves loading,
    # and only the nonce this workspace's last report issued proves the probe was the one run.
    issued = _issued_nonce(ws)
    if issued and probe_result == f"blocked:{issued}":
        out.append(Check("hook_platform_loaded", "ok", "live probe was BLOCKED: the platform is running hooks.json",
                         {"probe_nonce": issued}))
    elif probe_result == "not-blocked":
        out.append(Check("hook_platform_loaded", "fail",
                         "live probe ran unblocked: hooks.json is not being applied in this session. Treat as a D10; "
                         "do not launch children until fixed (plugin not installed at org level, or hooks disabled)."))
    else:
        nonce = secrets.token_hex(4)
        why = ("the nonce did not match the one this workspace's last report issued; "
               if probe_result.startswith("blocked:") else "")
        out.append(Check("hook_platform_loaded", "unverified",
                         why + "run the probe command in this session's shell; the guard's block message names "
                         "__dbx_guard_probe__<nonce>; re-run the doctor with --hook-probe-result blocked:<nonce> "
                         "(or not-blocked if the echo printed)",
                         {"probe_command": HOOK_PROBE_COMMAND.format(nonce=nonce), "probe_nonce": nonce}))
    return out


# ------------------------------------------------------------------ ledger integrity

def check_allowlist_committed(ws: Path) -> Check:
    """The allowlist and the tolerances the children and the verifier are held to are the ones HEAD
    committed: the working copy must be byte-equal to `git show HEAD:<path>` (git status can be
    silenced with assume-unchanged; bytes cannot). A differing, untracked or missing copy names
    which file and how."""
    states: dict[str, str] = {}
    for rel in LEDGER_CONTRACT_FILES:
        try:
            r = subprocess.run(["git", "-C", str(ws), "show", f"HEAD:{rel}"], capture_output=True, timeout=60, check=False)
        except (OSError, subprocess.TimeoutExpired) as e:
            return Check("allowlist_committed", "fail", f"git show HEAD:{rel} failed under {ws}: {_redact(str(e))}")
        err = r.stderr.decode(errors="replace").strip()
        in_head = r.returncode == 0
        if not in_head and not re.search(r"exist in 'HEAD'|not in 'HEAD'|invalid object name 'HEAD'", err):
            return Check("allowlist_committed", "fail", f"git cannot read HEAD:{rel} under {ws}: {_redact(err)}; the "
                         "workspace must be the committed repository the wave is planned from")
        try:
            disk = (ws / rel).read_bytes()
        except OSError:
            states[rel] = "missing"
            continue
        states[rel] = "untracked" if not in_head else "clean" if disk == r.stdout else "modified since HEAD"
    bad = [f"{rel} {state}" for rel, state in states.items() if state != "clean"]
    if bad:
        return Check("allowlist_committed", "fail",
                     "the working copy is not the committed contract: " + "; ".join(bad) +
                     ". Restore HEAD's copy, or commit the change through a recorded decision, then re-run", states)
    return Check("allowlist_committed", "ok", "allowed_targets.json and 03_recon_tolerances.json are byte-equal to HEAD",
                 states)


def _norm_catalog(name) -> str:
    """The guard's identifier rule (dbx_guard._norm): trimmed, unquoted, case-folded."""
    return str(name).strip().strip("`").lower()


def check_allowlist_matches_contract(ws: Path, expect_catalogs: list[str] | None) -> Check:
    """The catalogs the plan/brief's capability contract names must be exactly the allowlist's,
    compared under the guard's normalization so an accepted spelling never blocks a wave."""
    if expect_catalogs is None:
        return Check("allowlist_matches_contract", "skipped",
                     "no --expect-catalogs given (the catalogs in the wave's capability contract); nothing to compare")
    p = ws / ".migration" / "allowed_targets.json"
    try:
        cats = json.loads(p.read_text()).get("catalogs")
    except (OSError, ValueError, AttributeError) as e:
        return Check("allowlist_matches_contract", "fail", f"{p.name} unreadable: {_redact(str(e))}")
    expected = [_norm_catalog(c) for c in expect_catalogs]
    cats = [_norm_catalog(c) for c in cats] if isinstance(cats, list) else cats
    data = {"expected": expected, "allowlist": cats}
    if not isinstance(cats, list) or sorted(cats) != sorted(expected):
        return Check("allowlist_matches_contract", "fail",
                     f"allowlist catalogs {cats} differ from the contract's {expected}; a catalog is "
                     "added by a recorded decision and a new doctor run, never by editing either side", data)
    return Check("allowlist_matches_contract", "ok", f"allowlist catalogs match the contract: {cats}", data)


def check_official_plugin(plugin_root: Path) -> Check:
    roots = []
    for env in ("CLAUDE_PLUGIN_ROOT", "DEVIN_PLUGINS_DIR"):
        v = os.environ.get(env)
        if v:
            roots.append(Path(v).parent)
    roots += [Path("/opt/.devin/plugins/cache"), Path.home() / ".devin" / "plugins", plugin_root.parent]
    found: dict[str, str] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for skill in OFFICIAL_SKILLS:
            if skill in found:
                continue
            for hit in root.glob(f"**/skills/{skill}/SKILL.md"):
                found[skill] = str(hit.parent)
                break
    missing = [s for s in OFFICIAL_SKILLS if s not in found]
    if not found:
        return Check("official_databricks_plugin", "unverified",
                     "official databricks-agent-skills not found on disk under known plugin roots; it is declared in "
                     "requiredPlugins and is loaded by the platform, so this is only a local visibility gap",
                     {"searched": [str(r) for r in roots]})
    if missing:
        return Check("official_databricks_plugin", "warn", f"official plugin present but missing skills {missing}", {"found": found})
    return Check("official_databricks_plugin", "ok", f"all {len(OFFICIAL_SKILLS)} routed official skills present", {"found": found})


def check_harness(plugin_root: Path) -> Check:
    harness = plugin_root / "skills" / "data-reconciliation" / "harness"
    if shutil.which("dbx-recon"):
        rc, out, err = _run(["dbx-recon", "selftest"])
        how = "dbx-recon"
    elif (harness / "recon" / "cli.py").exists():
        rc, out, err = _run([sys.executable, "-m", "recon.cli", "selftest"], cwd=harness)
        how = f"python -m recon.cli (cwd {harness})"
    else:
        return Check("recon_harness", "fail", f"dbx-recon not on PATH and harness not at {harness}")
    if rc == 0 and "PASS" in out:
        return Check("recon_harness", "ok", f"{out.strip()} via {how}")
    return Check("recon_harness", "fail", f"selftest rc={rc}: {_redact(err or out)}")


def _module_present(dotted: str) -> bool:
    try:
        return importlib.util.find_spec(dotted) is not None
    except (ModuleNotFoundError, ValueError):
        return False


def check_drivers() -> Check:
    present = {k: _module_present(v) for k, v in DRIVERS.items()}
    have = sorted(k for k, v in present.items() if v)
    status = "ok" if present["databricks"] else "warn"
    return Check("recon_drivers", status,
                 f"installed adapters: {have or 'none'}" + ("" if present["databricks"] else "; databricks-sql-connector missing, live/snapshot recon cannot run"),
                 {"drivers": present})


# ------------------------------------------------------------------ delete evidence (source CDC)

_CDC_QUERIES = {
    "is_cdc_enabled": "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()",
    # Lists the capture instances *this identity may read* (db_owner, the capture's gating role,
    # or SELECT on its captured columns), with role_name and captured_column_list; needs no SELECT
    # on the cdc schema, unlike cdc.change_tables / cdc.captured_columns.
    "captures": "EXEC sys.sp_cdc_help_change_data_capture",
    "max_lsn": "SELECT sys.fn_cdc_get_max_lsn()",
    # Bounded, read-only call of the generated function exactly as the harness will make it: the
    # mapped key columns and the object's scope predicate over the single-position [max, max] range.
    "probe": "SELECT TOP (1) {cols} FROM cdc.fn_cdc_get_all_changes_{capture}(?, ?, N'all') "
             "WHERE __$operation = 1{scope}",
}


def _pyodbc_connect(dsn: str):
    import pyodbc
    return pyodbc.connect(dsn, readonly=True, timeout=15)


def _psycopg_connect(dsn: str):
    import psycopg
    conn = psycopg.connect(dsn, connect_timeout=15)
    conn.read_only = True
    return conn


def _captured_columns(column_list: str) -> list[str]:
    """`[loan_id], [borrower_id]` as sp_cdc_help_change_data_capture reports it -> names."""
    return [c.strip().strip("[]") for c in str(column_list or "").split(",") if c.strip()]


UNIT_MAPPINGS = ".migration/units/*/mapping_spec.json"


def resolve_mappings(ws: Path, role: str, units: list[str], mappings: list[Path]
                     ) -> tuple[dict[str, Path], dict[str, Path], tuple | None]:
    """(expected unit -> mapping, every spec to check, problem) for the mappings this run is
    answerable for, resolved by the doctor rather than trusted from whoever typed the command: a
    child names its batch (--unit, the ids in its brief) and each unit's
    .migration/units/<id>/mapping_spec.json must exist; an orchestrator covers every unit mapping in
    the workspace. A subset can therefore never pass as the whole. --mapping adds ad-hoc specs on top
    (a candidate mapping at setup, before its unit exists). `problem` is (status, detail, data) when
    the set itself is wrong; an empty `todo` is setup, before any unit mapping exists."""
    unit_dir = ws / ".migration" / "units"
    if role == "child":
        if not units:
            return {}, {}, ("fail",
                            ("a child preflight covers every unit in its batch: pass --unit <id> for each unit in the "
                             "brief, --source-secret NAME and the --param values the recon gate will get"),
                            {"units": [], "mappings": {}})
        expected = {u: unit_dir / u / "mapping_spec.json" for u in dict.fromkeys(units)}
    else:
        if units:
            return {}, {}, ("fail",
                            ("--unit narrows nothing for an orchestrator: it verifies every unit mapping under "
                             f"{UNIT_MAPPINGS}; --unit is for --role child"), {"units": list(units), "mappings": {}})
        expected = {p.parent.name: p for p in sorted(ws.glob(UNIT_MAPPINGS))}
    missing = [u for u, p in expected.items() if not p.is_file()]
    if missing:
        return expected, {}, ("fail",
                              (f"unit mapping(s) missing for {', '.join(missing)}: expected "
                               f"{unit_dir.relative_to(ws)}/<id>/mapping_spec.json (hand-off incomplete; report BLOCKED)"),
                              {"units": list(expected), "missing_units": missing, "mappings": {}})
    todo = dict(expected)
    seen = {p.resolve() for p in expected.values()}
    for m in mappings:
        if m.resolve() not in seen:
            seen.add(m.resolve())
            todo[str(m)] = m
    return expected, todo, None


def check_delete_evidence_all(ws: Path, role: str, units: list[str], mappings: list[Path],
                              source_secret: str | None, plugin_root: Path, connect=_pyodbc_connect,
                              params: dict[str, str] | None = None) -> Check:
    """One row over every unit mapping this run is answerable for (resolve_mappings); setup with
    nothing to verify is the one not-applicable case."""
    expected, todo, problem = resolve_mappings(ws, role, units, mappings)
    if problem:
        return Check("delete_evidence", *problem)
    if not todo:
        return Check("delete_evidence", "skipped",
                     f"not applicable at setup: no unit mapping exists yet under {UNIT_MAPPINGS}",
                     {"units": [], "mappings": {}})
    rows = {label: check_delete_evidence(p, source_secret, plugin_root, connect=connect, params=params)
            for label, p in todo.items()}
    worst = "fail" if any(c.status == "fail" for c in rows.values()) else "ok"
    return Check("delete_evidence", worst, "; ".join(f"{label}: {c.detail}" for label, c in rows.items()),
                 {"units": list(expected),
                  "mappings": {label: {"status": c.status, "detail": c.detail, **(c.data or {})}
                               for label, c in rows.items()}})


def check_delete_evidence(mapping: Path, source_secret: str | None, plugin_root: Path,
                          connect=_pyodbc_connect, params: dict[str, str] | None = None) -> Check:
    """Every `delete_evidence` block a mapping declares must be answerable on the source before a
    transactional recon run, under the exact access model the harness uses: CDC on for the
    database; each declared capture instance visible to the migration identity through
    sp_cdc_help_change_data_capture (db_owner, gating role, or SELECT on the captured columns: no
    schema-wide SELECT on cdc is asked for); every mapped source key column among the capture's
    captured columns; and one bounded call of the generated cdc.fn_cdc_get_all_changes_<capture>
    over the single-position [max, max] range with the key columns and the object's root_where, so an
    identity that cannot execute the function or a scope the capture cannot evaluate fails here
    rather than mid-run. The metadata comparison folds case (CDC metadata keeps the declared
    spelling, the mapping's spelling is what the server resolves under its collation); the probe,
    sent with the mapping's spelling, is the exact check. Metadata reads only: CDC is a source-side
    change the factory never makes (the doctor never runs sp_cdc_enable_*), so a red row here is a
    customer decision to record, not a fix to apply."""
    sys.path.insert(0, str(plugin_root / "skills" / "data-reconciliation" / "harness"))
    from recon.config import ConfigError, load_mapping_spec
    try:
        spec = load_mapping_spec(mapping, params)
    except (ConfigError, OSError, ValueError) as e:
        return Check("delete_evidence", "fail", f"{mapping}: {_redact(str(e))}")
    declared = [(c.object, c.delete_evidence) for c in spec.objects if c.delete_evidence is not None]
    if not declared:
        return Check("delete_evidence", "ok", "no object declares delete_evidence (drain-before-run contract applies)")
    kinds = sorted({de.kind for _, de in declared})
    if kinds != ["sqlserver_cdc"]:
        return Check("delete_evidence", "fail", f"unsupported delete_evidence kind(s) {kinds}")
    if not source_secret:
        return Check("delete_evidence", "fail",
                     f"{len(declared)} object(s) declare delete_evidence; pass --source-secret NAME "
                     "(env var holding the read-only source DSN) to verify CDC on the source")
    dsn = os.environ.get(source_secret)
    if not dsn:
        return Check("delete_evidence", "fail", f"source secret {source_secret} is not set in the environment")
    # capture -> [(key columns, scope)] as the harness will read it; the mapping validated the identifiers
    reads: dict[str, list[tuple[list[str], str | None]]] = {}
    for c in spec.objects:
        if c.delete_evidence is not None:
            reads.setdefault(c.delete_evidence.capture, []).append((list(c.key_source), c.root_where))
    wanted = sorted(reads)
    data: dict = {"kind": "sqlserver_cdc", "captures": wanted, "missing": [], "missing_columns": {}, "unreadable": {}}
    fail = "delete_evidence", "fail"
    try:
        conn = connect(dsn)
        try:
            cur = conn.cursor()
            (enabled,) = cur.execute(_CDC_QUERIES["is_cdc_enabled"]).fetchall()[0]
            if not enabled:
                return Check(*fail, "CDC is not enabled on the source database; enabling it is a source-side "
                             "change (the factory never runs sp_cdc_enable_*): record the customer decision or "
                             "drop delete_evidence and drain deletes before each run", data)
            cur.execute(_CDC_QUERIES["captures"])
            names = [d[0].lower() for d in cur.description]
            cap_i, cols_i = names.index("capture_instance"), names.index("captured_column_list")
            visible = {str(r[cap_i]).casefold(): [c.casefold() for c in _captured_columns(r[cols_i])]
                       for r in cur.fetchall()}
            data["missing"] = [c for c in wanted if c.casefold() not in visible]
            if data["missing"]:
                return Check(*fail, f"declared capture instance(s) not present or not readable by this identity "
                             f"(db_owner, the capture's gating role, or SELECT on its captured columns): "
                             f"{data['missing']}", data)
            for cap in wanted:
                keys = dict.fromkeys(k for key_cols, _ in reads[cap] for k in key_cols)
                if absent := [k for k in keys if k.casefold() not in visible[cap.casefold()]]:
                    data["missing_columns"][cap] = absent
            if data["missing_columns"]:
                return Check(*fail, "mapped source key column(s) are not captured, so deletes_since cannot "
                             f"project the key: {data['missing_columns']}", data)
            (hi,) = cur.execute(_CDC_QUERIES["max_lsn"]).fetchall()[0]
            if hi is None:
                return Check(*fail, "no change has been captured yet (fn_cdc_get_max_lsn is NULL): the evidence "
                             "horizon is empty and every target-only key would be graded strictly", data)
            for cap in wanted:
                for key_cols, scope in reads[cap]:
                    sql = _CDC_QUERIES["probe"].format(cols=", ".join(key_cols), capture=cap,
                                                       scope=f" AND ({scope})" if scope else "")
                    try:
                        cur.execute(sql, (hi, hi)).fetchall()
                    except Exception as e:  # noqa: BLE001 - the engine's refusal is the finding
                        data["unreadable"][cap] = _redact(str(e))
                        break
            if data["unreadable"]:
                return Check(*fail, "declared capture(s) cannot be read as the harness reads them (EXECUTE on "
                             "cdc.fn_cdc_get_all_changes_<capture> with the key columns and root_where): "
                             f"{data['unreadable']}", data)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - any driver failure is a finding, never a traceback with a DSN in it
        return Check(*fail, f"source query failed: {_redact(str(e))}", data)
    return Check("delete_evidence", "ok",
                 f"sqlserver_cdc: {len(wanted)} capture instance(s) readable by this identity, key columns "
                 "captured, scoped read probed", data)


# ------------------------------------------------------------------ source principal read-only

# Per family: the admin flags and role memberships that make every table writable (directly or by
# granting/impersonating one's way to it), the query answering them, the per-table query with one
# boolean column per privilege in TABLE_PRIVILEGES, the per-table `columns` query returning
# (column, privilege) rows for a write granted at column level (a table-level check does not see
# it), and the `indirect` queries, each returning
# (object, privilege) rows for a write path that bypasses table grants: IMPERSONATE on a visible
# login/user, EXECUTE on any procedure in the source database (every proc is assumed to write),
# EXECUTE on a SECURITY DEFINER or explicitly-granted function in an in-scope schema, and on
# Postgres every role the session can reach through memberships (`pg_has_role` is transitive, so a
# writer behind an intermediate role counts; on 16+ only memberships that inherit (USAGE) or can
# SET ROLE (SET), since a `SET FALSE, INHERIT FALSE` grant confers nothing; before 16 MEMBER implied
# both) that holds a write on an in-scope table or schema. Every value
# is a question about the principal; nothing here can change the source. Families without an
# entry are reported `unverified`, never `ok`.
_SRV_ROLES = ("sysadmin", "securityadmin", "serveradmin", "dbcreator", "bulkadmin")
_DB_ROLES = ("db_owner", "db_ddladmin", "db_datawriter", "db_securityadmin")
_SRV_PERMS = ("CONTROL SERVER", "ALTER ANY DATABASE", "IMPERSONATE ANY LOGIN", "ALTER ANY LOGIN")
_PG_ATTRS = ("rolsuper", "rolcreaterole", "rolcreatedb", "rolbypassrls")
_PG_ROLES = ("pg_write_server_files", "pg_execute_server_program")
_ROLE_FLAGS = {"sqlserver": _SRV_ROLES + _DB_ROLES + _SRV_PERMS, "postgres": _PG_ATTRS + _PG_ROLES}
_TABLE_PRIVILEGES = {"sqlserver": ("INSERT", "UPDATE", "DELETE", "ALTER"),
                     "postgres": ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE on schema")}
_PRIVILEGE_QUERIES = {
    "sqlserver": {
        "roles": "SELECT " + ", ".join([*(f"IS_SRVROLEMEMBER('{r}')" for r in _SRV_ROLES),
                                          *(f"IS_MEMBER('{r}')" for r in _DB_ROLES),
                                          *(f"HAS_PERMS_BY_NAME(NULL, NULL, '{p}')" for p in _SRV_PERMS)]),
        "table": "SELECT " + ", ".join(f"HAS_PERMS_BY_NAME(?, 'OBJECT', '{p}')" for p in _TABLE_PRIVILEGES["sqlserver"]),
        "columns": ("SELECT QUOTENAME(subentity_name), permission_name FROM fn_my_permissions(?, 'OBJECT') "
                    "WHERE subentity_name <> '' AND permission_name = 'UPDATE' ORDER BY 1"),
        "indirect": (
            ("SELECT 'LOGIN ' + name, 'IMPERSONATE' FROM sys.server_principals WHERE type IN ('S', 'U', 'C', 'K') "
             "AND name <> SUSER_SNAME() AND HAS_PERMS_BY_NAME(name, 'LOGIN', 'IMPERSONATE') = 1"),
            ("SELECT 'USER ' + name, 'IMPERSONATE' FROM sys.database_principals WHERE type IN ('S', 'U', 'C', 'K', 'E', 'X') "
             "AND name <> USER_NAME() AND HAS_PERMS_BY_NAME(name, 'USER', 'IMPERSONATE') = 1"),
            ("SELECT QUOTENAME(s.name) + '.' + QUOTENAME(o.name), 'EXECUTE' FROM sys.objects o "
             "JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE o.type IN ('P', 'PC', 'X') AND o.is_ms_shipped = 0 "
             "AND HAS_PERMS_BY_NAME(QUOTENAME(s.name) + '.' + QUOTENAME(o.name), 'OBJECT', 'EXECUTE') = 1 ORDER BY 1")),
        "read_only": None,
    },
    "postgres": {
        "roles": "SELECT " + ", ".join([*_PG_ATTRS, *(f"pg_has_role(current_user, '{r}', 'MEMBER')" for r in _PG_ROLES)])
                 + " FROM pg_roles WHERE rolname = current_user",
        "table": "SELECT " + ", ".join(f"has_table_privilege(%s, '{p}')" for p in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"))
                 + ", has_schema_privilege(%s, 'CREATE')",
        "columns": ("SELECT a.attname, p FROM pg_attribute a CROSS JOIN unnest(ARRAY['INSERT', 'UPDATE']) AS p "
                    "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
                    "AND has_column_privilege(a.attrelid, a.attnum, p) AND NOT has_table_privilege(a.attrelid, p) "
                    "ORDER BY 1, 2"),
        "functions": "SELECT n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', "
                     "'EXECUTE' FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ANY(%s) "
                     "AND (p.prosecdef OR p.proacl IS NOT NULL) AND has_function_privilege(p.oid, 'EXECUTE') ORDER BY 1",
        "members": "SELECT rolname FROM pg_roles WHERE rolname <> current_user AND pg_has_role(current_user, oid, "
                   "CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'USAGE, SET' ELSE 'MEMBER' END) "
                   "ORDER BY 1",
        "as_role": "SELECT has_table_privilege(%s, %s, 'INSERT,UPDATE,DELETE,TRUNCATE'), has_schema_privilege(%s, %s, 'CREATE')",
        "read_only": "SELECT current_setting('transaction_read_only')",
    },
}
_READ_ONLY_CONNECT = {"sqlserver": _pyodbc_connect, "postgres": _psycopg_connect}
_ADVISORY = ("driver-level read-only (SQL Server readonly=True, Postgres default_transaction_read_only) is advisory, "
             "a hint the server may ignore; only the principal's grants stop writes")


def _schema(table: str) -> str:
    return table.rsplit(".", 1)[0] if "." in table else "public"


def _table_params(family: str, table: str) -> tuple:
    if family == "sqlserver":
        return (table,) * 4
    return (table,) * 4 + (_schema(table),)


def _indirect_writes(cur, q: dict, family: str, tables: list[str]) -> list[str]:
    """`object: privilege` for every write path that is not a grant on an in-scope table."""
    found: list[str] = []
    for sql in q.get("indirect", ()):
        found += [f"{obj}: {priv}" for obj, priv in cur.execute(sql).fetchall()]
    if family == "postgres":
        schemas = list(dict.fromkeys(_schema(t) for t in tables))
        found += [f"{obj}: {priv}" for obj, priv in cur.execute(q["functions"], (schemas,)).fetchall()]
        for (role,) in cur.execute(q["members"]).fetchall():  # what SET ROLE <role> would unlock
            for t in tables:
                write, create = cur.execute(q["as_role"], (role, t, role, _schema(t))).fetchall()[0]
                found += [f"SET ROLE {role}: {t} {w}" for w, held in (("write", write), ("CREATE on schema", create)) if held]
    return found


def check_source_principal(tables: list[str], family: str, source_secret: str | None, connect=None) -> Check:
    """The principal behind --source-secret must not be able to write any in-scope source object,
    directly or through indirection: no admin role or server permission, no INSERT/UPDATE/DELETE/
    ALTER (TRUNCATE, schema CREATE on Postgres) on any table the resolved mappings read, at table
    or column level, no IMPERSONATE, no EXECUTE on a procedure (or SECURITY DEFINER / explicitly-granted function) and
    no SET ROLE-able membership that would unlock a write. This is the control the guard's
    docstring defers to for clients the hook cannot read into; `readonly=True` on the connection
    is advisory and is reported as such in `stats`. A failure names object and privilege, never
    the credential."""
    cid = "source_principal_read_only"
    q = _PRIVILEGE_QUERIES.get(family)
    if q is None:
        return Check(cid, "unverified", f"{family}: no privilege query implemented for this family (untested), so the "
                     "source principal's write privileges are unknown; confirm SELECT-only grants by hand and record it",
                     {"family": family, "tables": tables})
    if not source_secret:
        return Check(cid, "fail", f"{family} source with {len(tables)} in-scope table(s); pass --source-secret NAME "
                     "(env var holding the source DSN) so the principal's write privileges can be checked")
    dsn = os.environ.get(source_secret)
    if not dsn:
        return Check(cid, "fail", f"source secret {source_secret} is not set in the environment")
    data: dict = {"family": family, "tables": tables, "roles": [], "writable": {}, "indirect": [], "unresolved": [],
                  "stats": ""}
    try:
        conn = (connect or _READ_ONLY_CONNECT[family])(dsn)
        try:
            cur = conn.cursor()
            if q["read_only"]:
                (ro,) = cur.execute(q["read_only"]).fetchall()[0]
                data["stats"] = f"connection opened read_only=True, transaction_read_only={ro}; {_ADVISORY}"
            else:
                data["stats"] = f"connection opened with pyodbc readonly=True; {_ADVISORY}"
            flags = cur.execute(q["roles"]).fetchall()[0]
            data["roles"] = [name for name, held in zip(_ROLE_FLAGS[family], flags) if held]
            for t in tables:
                row = cur.execute(q["table"], _table_params(family, t)).fetchall()[0]
                if any(v is None for v in row):
                    data["unresolved"].append(t)
                    continue
                held = [p for p, v in zip(_TABLE_PRIVILEGES[family], row) if v]
                held += [f"{p} on column {c}" for c, p in cur.execute(q["columns"], (t,)).fetchall() if p not in held]
                if held:
                    data["writable"][t] = held
            data["indirect"] = _indirect_writes(cur, q, family, tables)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - any driver failure is a finding, never a traceback with a DSN in it
        return Check(cid, "fail", f"source query failed: {_redact(str(e))}", data)
    can_write = ([f"role {r}" for r in data["roles"]] + [f"{t}: {', '.join(p)}" for t, p in data["writable"].items()]
                 + data["indirect"])
    if can_write:
        shown = "; ".join(can_write[:6]) + (f"; +{len(can_write) - 6} more in data" if len(can_write) > 6 else "")
        return Check(cid, "fail", f"{family}: the source principal can write in scope ({shown}); the "
                     f"factory needs a SELECT-only principal, and {_ADVISORY}", data)
    if data["unresolved"]:
        return Check(cid, "unverified", f"{family}: privileges could not be evaluated for {data['unresolved']} (object "
                     "not found or not visible to this principal); nothing is proven about them", data)
    return Check(cid, "ok", f"{family}: no admin role, no write privilege on {len(tables)} in-scope table(s) or their columns, no "
                 f"IMPERSONATE/EXECUTE/SET ROLE path to one; {data['stats']}", data)


def check_source_principal_all(ws: Path, role: str, units: list[str], mappings: list[Path], source_secret: str | None,
                               source_family: str | None, plugin_root: Path, params: dict[str, str] | None = None
                               ) -> Check:
    """Every source table the resolved mappings read (root tables and embedded child tables), against
    --source-family, or the family the mappings' delete_evidence kind implies."""
    cid = "source_principal_read_only"
    _, todo, problem = resolve_mappings(ws, role, units, mappings)
    if problem:
        return Check(cid, *problem)
    if not todo:
        return Check(cid, "skipped", f"not applicable at setup: no unit mapping exists yet under {UNIT_MAPPINGS}")
    sys.path.insert(0, str(plugin_root / "skills" / "data-reconciliation" / "harness"))
    from recon.config import ConfigError, load_mapping_spec
    tables: dict[str, None] = {}
    kinds: set[str] = set()
    for p in todo.values():
        try:
            spec = load_mapping_spec(p, params)
        except (ConfigError, OSError, ValueError) as e:
            return Check(cid, "fail", f"{p}: {_redact(str(e))}")
        for o in spec.objects:
            tables.update(dict.fromkeys([o.root_table, *(e.child_table for e in o.embeds)]))
            if o.delete_evidence is not None:
                kinds.add(o.delete_evidence.kind)
    family = source_family or ("sqlserver" if kinds == {"sqlserver_cdc"} else None)
    if not family:
        return Check(cid, "unverified", "source family not declared: pass --source-family "
                     f"{'|'.join(SOURCE_FAMILIES)} so the principal's privileges can be checked", {"tables": list(tables)})
    return check_source_principal(list(tables), family, source_secret)


# ------------------------------------------------------------------ databricks identity

_APPLICATION_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
_SP_SCHEMA = "servicePrincipal"


def classify_identity(who: dict) -> tuple[str, bool]:
    """(userName, is_service_principal) from a SCIM `current-user me` document.

    A service principal is recognised by positive evidence only: `applicationId`, a ServicePrincipal
    SCIM schema, or a `userName` that is the application id (a UUID). Anything else, including a
    username without an '@', is treated as a human identity, so the doctor fails closed."""
    name = str(who.get("userName") or who.get("displayName") or "?")
    schemas = [str(s) for s in who.get("schemas") or []]
    is_sp = bool(who.get("applicationId")) or any(_SP_SCHEMA.lower() in s.lower() for s in schemas) \
        or bool(_APPLICATION_ID.fullmatch(name))
    return name, is_sp


def check_databricks(expect_identity: str | None) -> list[Check]:
    out: list[Check] = []
    cli = shutil.which("databricks")
    if not cli:
        out.append(Check("databricks_cli", "fail", "databricks CLI not on PATH (see databricks-core for install)"))
        return out
    rc, ver, err = _run([cli, "--version"], timeout=20)
    out.append(Check("databricks_cli", "ok" if rc == 0 else "fail", (ver or err).strip()[:80], {"path": cli}))

    set_vars = [v for v in M2M_VARS if os.environ.get(v)]
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if len(set_vars) == len(M2M_VARS):
        auth_kind = "oauth-m2m (env)"
    elif os.environ.get("DATABRICKS_TOKEN"):
        auth_kind = "pat (env)"
    elif profile:
        auth_kind = f"profile {profile}"
    else:
        auth_kind = "unknown (CLI default chain)"
    out.append(Check("databricks_auth_kind", "ok" if auth_kind.startswith("oauth-m2m") else "warn",
                     f"auth: {auth_kind}; migration sessions should run as the migration service principal via "
                     f"DATABRICKS_CLIENT_ID/SECRET from named secrets", {"auth_kind": auth_kind, "env_set": set_vars}))

    rc, me, err = _run([cli, "current-user", "me", "--output", "json"], timeout=60)
    if rc != 0:
        out.append(Check("databricks_identity", "fail", f"current-user me failed: {_redact(err)}"))
        return out
    try:
        who = json.loads(me)
    except json.JSONDecodeError:
        out.append(Check("databricks_identity", "fail", "current-user me returned non-JSON"))
        return out
    name, is_sp = classify_identity(who)
    rc, desc, _ = _run([cli, "auth", "describe", "--output", "json"], timeout=60)
    try:
        host = json.loads(desc)["details"]["host"] if rc == 0 else None
    except (ValueError, KeyError, TypeError):
        host = None
    data = {"userName": name, "service_principal": is_sp, "host": host}
    status = "ok"
    detail = f"authenticated as {name} ({'service principal' if is_sp else 'user'}) on {host}"
    if expect_identity and str(name).lower() != expect_identity.lower():
        status, detail = "fail", detail + f"; expected {expect_identity} (recorded in 07_access_checklist.md)"
    elif not host:
        status = "fail"
        detail += "; workspace host not resolved by `databricks auth describe`, so the wave manifest cannot pin children to it"
    elif not is_sp:
        status, detail = "warn", detail + "; unattended sessions must not run as a human identity"
    out.append(Check("databricks_identity", status, detail, data))

    rc, wh, err = _run([cli, "experimental", "aitools", "tools", "get-default-warehouse"], timeout=60)
    if rc == 0 and wh.strip():
        out.append(Check("databricks_warehouse", "ok", f"default warehouse resolved: {wh.strip()[:120]}"))
    else:
        out.append(Check("databricks_warehouse", "warn", f"no default warehouse via aitools: {_redact(err or wh)}"))
    return out


# ------------------------------------------------------------------ main

def run(ws: Path, plugin_root: Path, role: str, probe_result: str, expect_identity: str | None,
        no_databricks: bool, units: list[str] | None = None, mappings: list[Path] | None = None,
        source_secret: str | None = None, params: dict[str, str] | None = None,
        expect_catalogs: list[str] | None = None, source_family: str | None = None) -> dict:
    checks: list[Check] = [check_workspace(ws), check_stop_mode(ws), check_allowed_targets(ws, plugin_root),
                           check_allowlist_committed(ws), check_allowlist_matches_contract(ws, expect_catalogs)]
    checks += check_hooks(plugin_root, ws, probe_result)
    checks.append(check_official_plugin(plugin_root))
    checks.append(check_harness(plugin_root))
    checks.append(check_drivers())
    checks.append(check_delete_evidence_all(ws, role, units or [], mappings or [], source_secret, plugin_root,
                                            params=params))
    checks.append(check_source_principal_all(ws, role, units or [], mappings or [], source_secret, source_family,
                                             plugin_root, params=params))
    if no_databricks:
        checks.append(Check("databricks_identity", "skipped", "--no-databricks"))
    else:
        checks += check_databricks(expect_identity)
    counts: dict[str, int] = {}
    for c in checks:
        counts[c.status] = counts.get(c.status, 0) + 1
    blocking = [f"{c.id}={c.status}" for c in checks
                if c.status == "fail" or (c.id in SECURITY_CONTROLS and c.status != "ok")
                or (c.id == "source_principal_read_only" and c.status == "unverified")]
    identity = next((c.data for c in checks if c.id == "databricks_identity" and c.data), None)
    return {
        "identity": identity,
        "schema": "dbx-migration-factory/capabilities/1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "role": role,
        "workspace": str(ws),
        "plugin_root": str(plugin_root),
        "summary": counts,
        "ready": not blocking,
        "blocking": blocking,
        "checks": [asdict(c) for c in checks],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--workspace", type=Path, default=Path.cwd())
    p.add_argument("--plugin-root", type=Path, default=Path(__file__).resolve().parents[2])
    p.add_argument("--role", choices=("orchestrator", "child"), default="orchestrator")
    p.add_argument("--hook-probe-result", default="unknown", metavar="blocked:<nonce>|not-blocked|unknown",
                   help="outcome of running the probe_command of the last report; the nonce is the one the "
                        "guard's block message named")
    p.add_argument("--expect-identity", help="userName the session must be authenticated as")
    p.add_argument("--expect-catalogs", metavar="A,B", type=lambda s: [c.strip() for c in s.split(",") if c.strip()],
                   help="catalogs the wave's capability contract names; must equal allowed_targets.json's")
    p.add_argument("--no-databricks", action="store_true",
                   help="skip CLI/identity checks (offline; the report is never ready)")
    p.add_argument("--unit", action="append", default=[], metavar="UNIT_ID",
                   help="(--role child) a unit of this batch, repeat per unit in the brief; its "
                        ".migration/units/<id>/mapping_spec.json must exist and its declared delete_evidence "
                        "is verified on the source. An orchestrator verifies every unit mapping in the workspace")
    p.add_argument("--mapping", type=Path, action="append", default=[], metavar="MAPPING_SPEC",
                   help="additional recon mapping_spec.json to verify (a candidate mapping at setup)")
    p.add_argument("--source-secret", help="env var NAME holding the read-only source DSN (value never printed)")
    p.add_argument("--source-family", choices=SOURCE_FAMILIES,
                   help="source engine behind --source-secret (default: implied by the mappings' delete_evidence kind)")
    p.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                   help="mapping ${NAME} placeholder value, same rules and values as dbx-recon run --param")
    p.add_argument("--out", type=Path, help="default .migration/09_capabilities.json; '-' for stdout only")
    a = p.parse_args(argv)
    if a.hook_probe_result == "blocked":
        p.error("--hook-probe-result blocked:<nonce> is required: the nonce the guard's block message named for the probe_command "
                "of the last report")
    params = None
    if a.param:
        sys.path.insert(0, str(a.plugin_root.resolve() / "skills" / "data-reconciliation" / "harness"))
        from recon.cli import parse_params
        params = parse_params(a.param)

    report = run(a.workspace.resolve(), a.plugin_root.resolve(), a.role, a.hook_probe_result,
                 a.expect_identity, a.no_databricks, a.unit, a.mapping, a.source_secret, params,
                 a.expect_catalogs, a.source_family)
    text = json.dumps(report, indent=2, sort_keys=True)
    out = a.out
    if out is None:
        out = a.workspace / ".migration" / "09_capabilities.json"
    if str(out) != "-" and (a.workspace / ".migration").is_dir():
        out.write_text(text + "\n")
    for c in report["checks"]:
        print(f"{c['status']:<10} {c['id']:<28} {c['detail']}")
    print(f"\nready={report['ready']} {report['summary']}"
          + (f" blocking={report['blocking']}" if report["blocking"] else "")
          + (f"  -> {out}" if str(out) != "-" else ""))
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
