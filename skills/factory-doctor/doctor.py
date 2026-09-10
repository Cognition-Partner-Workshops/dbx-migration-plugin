#!/usr/bin/env python3
"""factory-doctor: preflight for a DBX migration workspace.

Runs the checks every session in an engagement depends on and writes
`.migration/09_capabilities.json`. Nothing here needs a warehouse; the Databricks checks are
identity/metadata calls only. Secret *values* are never read or printed; only the names of the
environment variables that are set.

Usage:
    python3 doctor.py [--workspace DIR] [--plugin-root DIR] [--role orchestrator|child]
                      [--hook-probe-result blocked|not-blocked|unknown] [--expect-identity NAME]
                      [--no-databricks] [--mapping mapping.json --source-secret NAME] [--out PATH]

Exit code 0 when `ready`; 1 otherwise. `ready` requires no `fail` anywhere and every security
control (SECURITY_CONTROLS: guard functional, hooks loaded by the platform, identity) to be `ok`;
an `unverified` hook probe, a human identity, or an identity check `skipped` by `--no-databricks`
is not ready (an offline report can never authorize a wave). Other `warn`/`unverified` checks are
advisory and listed in the JSON for the playbook to decide.
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import re
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
DRIVERS = {"databricks": "databricks.sql", "redshift": "psycopg2", "snowflake": "snowflake.connector",
           "teradata": "teradatasql", "oracle": "oracledb", "sqlserver": "pyodbc", "postgres": "psycopg"}

# Safe live probe: if the platform loads hooks.json, the guard blocks this before it runs; if it
# does not, `echo` prints a line and nothing else happens. Either way no Databricks call is made.
HOOK_PROBE_COMMAND = (
    "echo 'databricks experimental aitools tools query "
    "\"DROP TABLE __dbx_guard_probe__.x.y\"' # factory-doctor hook probe: expected BLOCKED"
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

def check_hooks(plugin_root: Path, ws: Path, probe_result: str) -> list[Check]:
    out: list[Check] = []
    hooks_json = plugin_root / "hooks.json"
    guard = plugin_root / "hooks" / "dbx_guard.py"
    hint = plugin_root / "hooks" / "dbx_post_hint.py"
    if not hooks_json.exists() or not guard.exists() or not hint.exists():
        out.append(Check("hooks_files", "fail", f"hooks.json / hooks/dbx_guard.py / hooks/dbx_post_hint.py not all present under {plugin_root}"))
        return out
    try:
        data = json.loads(hooks_json.read_text())
        pre = data["PreToolUse"][0]["hooks"][0]["command"]
        assert "dbx_guard.py" in pre
        out.append(Check("hooks_files", "ok", "hooks.json registers dbx_guard.py (PreToolUse) and dbx_post_hint.py (PostToolUse)"))
    except (KeyError, IndexError, AssertionError, json.JSONDecodeError) as e:
        out.append(Check("hooks_files", "fail", f"hooks.json malformed: {e!r}"))
        return out

    # Functional check: feed the guard the probe event directly; it must block.
    event = json.dumps({"tool_name": "exec", "tool_input": {"command": HOOK_PROBE_COMMAND}})
    try:
        r = subprocess.run([sys.executable, str(guard)], input=event, text=True, capture_output=True,
                           timeout=30, cwd=ws, env={**os.environ, "CLAUDE_PROJECT_DIR": str(ws)})
        if r.returncode == 2 and '"block"' in r.stdout:
            out.append(Check("hook_guard_functional", "ok", "dbx_guard.py blocks the probe command when invoked directly"))
        else:
            out.append(Check("hook_guard_functional", "fail",
                             f"dbx_guard.py did not block the probe (rc={r.returncode}): {_redact(r.stderr or r.stdout)}"))
    except subprocess.TimeoutExpired:
        out.append(Check("hook_guard_functional", "fail", "dbx_guard.py timed out on the probe"))

    # Platform check: hooks are fail-open on the platform side, so only a live probe proves loading.
    if probe_result == "blocked":
        out.append(Check("hook_platform_loaded", "ok", "live probe was BLOCKED: the platform is running hooks.json"))
    elif probe_result == "not-blocked":
        out.append(Check("hook_platform_loaded", "fail",
                         "live probe ran unblocked: hooks.json is not being applied in this session. Treat as a D10; "
                         "do not launch children until fixed (plugin not installed at org level, or hooks disabled)."))
    else:
        out.append(Check("hook_platform_loaded", "unverified",
                         "run the probe command in this session's shell and re-run the doctor with "
                         "--hook-probe-result blocked|not-blocked", {"probe_command": HOOK_PROBE_COMMAND}))
    return out


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


def _captured_columns(column_list: str) -> list[str]:
    """`[loan_id], [borrower_id]` as sp_cdc_help_change_data_capture reports it -> names."""
    return [c.strip().strip("[]") for c in str(column_list or "").split(",") if c.strip()]


def check_delete_evidence(mapping: Path | None, source_secret: str | None, plugin_root: Path,
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
    if mapping is None:
        return Check("delete_evidence", "skipped", "no --mapping given; declared delete evidence not verified")
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
    data = {"userName": name, "service_principal": is_sp}
    status = "ok"
    detail = f"authenticated as {name} ({'service principal' if is_sp else 'user'})"
    if expect_identity and str(name).lower() != expect_identity.lower():
        status, detail = "fail", detail + f"; expected {expect_identity} (recorded in 07_access_checklist.md)"
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
        no_databricks: bool, mapping: Path | None = None, source_secret: str | None = None,
        params: dict[str, str] | None = None) -> dict:
    checks: list[Check] = [check_workspace(ws), check_stop_mode(ws), check_allowed_targets(ws, plugin_root)]
    checks += check_hooks(plugin_root, ws, probe_result)
    checks.append(check_official_plugin(plugin_root))
    checks.append(check_harness(plugin_root))
    checks.append(check_drivers())
    checks.append(check_delete_evidence(mapping, source_secret, plugin_root, params=params))
    if no_databricks:
        checks.append(Check("databricks_identity", "skipped", "--no-databricks"))
    else:
        checks += check_databricks(expect_identity)
    counts: dict[str, int] = {}
    for c in checks:
        counts[c.status] = counts.get(c.status, 0) + 1
    blocking = [f"{c.id}={c.status}" for c in checks
                if c.status == "fail" or (c.id in SECURITY_CONTROLS and c.status != "ok")]
    return {
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
    p.add_argument("--hook-probe-result", choices=("blocked", "not-blocked", "unknown"), default="unknown")
    p.add_argument("--expect-identity", help="userName the session must be authenticated as")
    p.add_argument("--no-databricks", action="store_true",
                   help="skip CLI/identity checks (offline; the report is never ready)")
    p.add_argument("--mapping", type=Path,
                   help="recon mapping.json; objects declaring delete_evidence are verified on the source")
    p.add_argument("--source-secret", help="env var NAME holding the read-only source DSN (value never printed)")
    p.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                   help="mapping ${NAME} placeholder value, same rules and values as dbx-recon run --param")
    p.add_argument("--out", type=Path, help="default .migration/09_capabilities.json; '-' for stdout only")
    a = p.parse_args(argv)
    params = None
    if a.param:
        sys.path.insert(0, str(a.plugin_root.resolve() / "skills" / "data-reconciliation" / "harness"))
        from recon.cli import parse_params
        params = parse_params(a.param)

    report = run(a.workspace.resolve(), a.plugin_root.resolve(), a.role, a.hook_probe_result,
                 a.expect_identity, a.no_databricks, a.mapping, a.source_secret, params)
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
