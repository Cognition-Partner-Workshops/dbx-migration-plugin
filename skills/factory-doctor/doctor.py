#!/usr/bin/env python3
"""factory-doctor: preflight for a DBX migration workspace.

Runs the checks every session in an engagement depends on and writes
`.migration/09_capabilities.json`. Nothing here needs a warehouse; the Databricks checks are
identity/metadata calls only. Secret *values* are never read or printed; only the names of the
environment variables that are set.

Usage:
    python3 doctor.py [--workspace DIR] [--plugin-root DIR] [--role orchestrator|child]
                      [--hook-probe-result blocked|not-blocked|unknown] [--expect-identity NAME]
                      [--no-databricks] [--out PATH]

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
           "teradata": "teradatasql", "oracle": "oracledb", "sqlserver": "pyodbc", "postgres": "psycopg2"}

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


def _redact(text: str) -> str:
    """Drop anything that looks like a token or secret value from CLI stderr."""
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
        no_databricks: bool) -> dict:
    checks: list[Check] = [check_workspace(ws), check_stop_mode(ws), check_allowed_targets(ws, plugin_root)]
    checks += check_hooks(plugin_root, ws, probe_result)
    checks.append(check_official_plugin(plugin_root))
    checks.append(check_harness(plugin_root))
    checks.append(check_drivers())
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
    p.add_argument("--out", type=Path, help="default .migration/09_capabilities.json; '-' for stdout only")
    a = p.parse_args(argv)

    report = run(a.workspace.resolve(), a.plugin_root.resolve(), a.role, a.hook_probe_result,
                 a.expect_identity, a.no_databricks)
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
