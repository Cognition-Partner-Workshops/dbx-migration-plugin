#!/usr/bin/env python3
"""factory-doctor: preflight for a DBX migration workspace.

Runs the checks every session in an engagement depends on and writes
`.migration/09_capabilities.json`. Nothing here needs a warehouse; the Databricks checks are
identity/metadata calls only. Secret *values* are never read or printed; only the names of the
environment variables that are set.

Usage:
    python3 doctor.py [--workspace DIR] [--plugin-root DIR] [--role orchestrator|child] [--wave MANIFEST]
                      [--hook-probe-result blocked:<nonce>|not-blocked|unknown] [--expect-identity NAME] [--expect-host URL]
                      [--expect-catalogs A,B] [--no-databricks] [--unit ID ...]
                      [--mapping mapping_spec.json ...] [--source-secret NAME] [--source-family F]
                      [--param NAME=VALUE ...] [--lakebase-project NAME] [--lakebase-parent-branch NAME]
                      [--lakebase-dsn ENV_VAR_NAME] [--lakebase-schema NAME]
                      [--analytical-schema CATALOG.SCHEMA] [--live-playbooks PATH] [--out PATH]

Exit code 0 when `ready`; 1 otherwise. `ready` requires no `fail` anywhere, every security
control (SECURITY_CONTROLS: guard functional, hooks loaded by the platform, identity) to be `ok`,
and `source_principal_read_only` not `unverified`; an `unverified` hook probe, a human identity,
or an identity check `skipped` by `--no-databricks` is not ready (an offline report can never
authorize a wave). Other `warn`/`unverified` checks are advisory and listed in the JSON for the
playbook to decide.
"""
from __future__ import annotations

import argparse
import calendar
import datetime
import hashlib
import hmac
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
_RANK = {"ok": 0, "skipped": 1, "warn": 2, "unverified": 3, "fail": 4}
# The adapters the harness can actually run (its other families fail fast, so no driver is probed for them).
DRIVERS = {"databricks": "databricks.sql", "sqlserver": "pyodbc", "postgres": "psycopg"}
# The families `dbx-recon run --family` accepts; only sqlserver and postgres have a privilege query.
SOURCE_FAMILIES = ("databricks", "oracle", "postgres", "redshift", "snowflake", "sqlserver", "teradata")
# The committed wave contract: the guard and the harness read the working copy, so a working copy
# that differs from HEAD is a contract nobody reviewed.
LEDGER_CONTRACT_FILES = (".migration/allowed_targets.json", ".migration/03_recon_tolerances.json")
CAPABILITIES = ".migration/09_capabilities.json"
PLAYBOOKS_LOCK = ".migration/playbooks.lock.json"
LIVE_PLAYBOOKS = ".migration/live_playbooks.json"
LIVE_PLAYBOOKS_MAX_AGE = datetime.timedelta(minutes=15)
HOOK_PROBE_NONCE = ".migration/.hook_probe_nonce"
HOOK_PROBE_NONCE_TTL = 8 * 60 * 60

# Safe live probe: if the platform loads hooks.json, the guard blocks this before it runs; if it
# does not, `echo` prints a line and nothing else happens. Either way no Databricks call is made.
# The nonce is issued per report and echoed in the guard's block reason, so `blocked:<nonce>` can
# only be passed back by a session that saw the block.
HOOK_PROBE_TOKEN = "__dbx_guard_probe__{nonce}"
HOOK_PROBE_COMMAND = (
    "echo 'databricks experimental aitools tools query "
    f"\"DROP TABLE {HOOK_PROBE_TOKEN}.x.y\"' # factory-doctor hook probe: expected BLOCKED"
)


@dataclass
class Check:
    id: str
    status: str  # ok | fail | warn | unverified | skipped | attested
    detail: str
    data: dict = field(default_factory=dict)


def _run(cmd: list[str], timeout: int = 60, cwd: Path | None = None,
         env: dict | None = None) -> tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd, env=env)
        return r.returncode, r.stdout, r.stderr
    except FileNotFoundError:
        return 127, "", f"{cmd[0]}: not found"
    except subprocess.TimeoutExpired:
        return 124, "", f"{' '.join(cmd[:3])}: timed out after {timeout}s"


def manifest_sha(manifest_bytes: bytes) -> str:
    return hashlib.sha256(manifest_bytes).hexdigest()[:12]


def wave_signature(body: dict, manifest_bytes: bytes) -> str:
    """HMAC over the canonical doctor record. Key = manifest bytes + the identity the doctor saw, so a
    record cannot be moved to another manifest or another principal. Tamper-evident, not tamper-proof:
    .migration/ is review-protected, and this closes the 'edited 09_capabilities.json' hole, nothing more.
    The key is derivable on purpose: the workflow sandbox holds no secret to verify one with, and a key
    carried in the manifest is writable by the same session that writes this record, so the gate against
    a lying orchestrator is each child's own --expect-identity doctor run plus PR review, not this HMAC."""
    ident = body.get("identity") or {}
    key = hashlib.sha256(manifest_bytes + str(ident.get("userName") or "").encode()
                         + str(ident.get("host") or "").encode()).digest()
    message = json.dumps({k: v for k, v in body.items() if k != "signature"},
                         sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(key, message, "sha256").hexdigest()


def sign_wave_report(report: dict, manifest_bytes: bytes, signed_at: str | None = None) -> dict:
    body = {k: v for k, v in report.items() if k != "signature"}
    body["manifest_sha"] = manifest_sha(manifest_bytes)
    body["signed_at"] = signed_at or datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    body["signature"] = wave_signature(body, manifest_bytes)
    return body


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
    """The pending probe nonce for this workspace, falling back to the last report."""
    def fresh(nonce, issued_at) -> str | None:
        if not isinstance(nonce, str) or not re.fullmatch(r"[0-9a-f]{8}", nonce):
            return None
        try:
            return nonce if time.time() - float(issued_at) <= HOOK_PROBE_NONCE_TTL else None
        except (TypeError, ValueError):
            return None

    try:
        saved = (ws / HOOK_PROBE_NONCE).read_text().strip().split()
        if len(saved) == 2:
            nonce = fresh(saved[0], saved[1])
            if nonce:
                return nonce
    except OSError:
        pass
    try:
        report = json.loads((ws / CAPABILITIES).read_text())
        checks = report.get("checks", [])
        row = next(c for c in checks if c.get("id") == "hook_guard")
        nonce = row.get("data", {}).get("probe_nonce")
        generated_at = report.get("generated_at") or report.get("timestamp")
        if isinstance(generated_at, str):
            generated_at = calendar.timegm(time.strptime(generated_at, "%Y-%m-%dT%H:%M:%SZ"))
        return fresh(nonce, generated_at)
    except (OSError, ValueError, StopIteration, AttributeError, KeyError, TypeError, OverflowError):
        return None


def _merge(cid: str, subs: list[Check]) -> Check:
    """One row from several sub-checks; sub_results keeps each one."""
    data: dict = {}
    for s in subs:
        data.update(s.data or {})
    data["sub_results"] = [asdict(s) for s in subs]
    if any(s.status == "fail" for s in subs):
        status = "fail"
    else:
        sec = [s for s in subs if s.id in SECURITY_CONTROLS and s.status != "ok"]
        status = max((s.status for s in (sec or subs)), key=_RANK.__getitem__)
    detail = "; ".join(f"{s.id}: {s.detail}" for s in subs)
    return Check(cid, status, detail, data)


def _flat(checks):
    for row in checks:
        sub_results = (row.data or {}).get("sub_results")
        if sub_results:
            yield from (Check(**d) for d in sub_results)
        else:
            yield row


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
    # echo the full probe token (prefix + a nonce fresh for this invocation) so a blanket deny that
    # hardcodes the prefix, or a token seen before, cannot pass as the guard having read the command.
    nonce = secrets.token_hex(4)
    token = HOOK_PROBE_TOKEN.format(nonce=nonce)
    event = json.dumps({"tool_name": "exec", "tool_input": {"command": HOOK_PROBE_COMMAND.format(nonce=nonce)}})
    try:
        r = subprocess.run([sys.executable, str(guard)], input=event, text=True, capture_output=True,
                           timeout=30, cwd=ws, env={**os.environ, "CLAUDE_PROJECT_DIR": str(ws)})
        if r.returncode == 2 and '"block"' in r.stdout and token in r.stdout:
            out.append(Check("hook_guard_functional", "ok",
                             f"dbx_guard.py blocks the probe command when invoked directly and names {token}"))
        else:
            out.append(Check("hook_guard_functional", "fail",
                             f"dbx_guard.py did not block the probe naming {token} (rc={r.returncode}): "
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
        nonce = issued or secrets.token_hex(4)
        if not issued:
            try:
                (ws / HOOK_PROBE_NONCE).write_text(f"{nonce} {int(time.time())}\n")
            except OSError:
                pass
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


# Files in the playbooks dir that are documentation, not importable playbooks (the README itself
# and the pre-kickoff intake form the front doors consume).
_NOT_PLAYBOOKS = frozenset({"0-README.md", "00_intake_template.md"})
_README_MACRO_ROW = re.compile(r"\|\s*`([^`]+\.md)`\s*\|[^|]*\|\s*`(![\w]+)`\s*\|")


def _repo_playbooks(plugin_root: Path) -> dict[str, tuple[str, str]]:
    """macro -> (repo_file, sha256 of the file bytes). Macros come from the Files table in
    playbooks/0-README.md: rows `| `<file>` | <title> | `!<macro>` |`."""
    playbooks = plugin_root / "skills" / "install-dbx-factory" / "playbooks"
    macros: dict[str, tuple[str, str]] = {}
    readme = playbooks / "0-README.md"
    if readme.is_file():
        for line in readme.read_text().splitlines():
            m = _README_MACRO_ROW.match(line)
            if not m:
                continue
            p = playbooks / m.group(1)
            if p.is_file() and p.name not in _NOT_PLAYBOOKS:
                macros[m.group(2)] = (p.name, hashlib.sha256(p.read_bytes()).hexdigest())
    return macros


def _norm(s: str) -> str:
    return s.replace("\r\n", "\n").rstrip("\n")


def check_playbooks_in_sync(ws: Path, plugin_root: Path, role: str,
                            live_playbooks: Path | None = None) -> Check:
    """The playbooks installed in the org library, proven against the repo copies the wave contract
    was reviewed from: install-dbx-factory records each macro's file sha256 in the lock it writes,
    and the orchestrator exports the live library to .migration/live_playbooks.json right before
    the doctor so the live bodies can be compared too; any drift means a live playbook is not the
    reviewed one."""
    cid = "playbooks_in_sync"
    lock = ws / PLAYBOOKS_LOCK
    if not lock.is_file():
        if role == "setup":
            return Check(cid, "skipped", f"no {PLAYBOOKS_LOCK} yet; install-dbx-factory writes it "
                         "(warning: live playbooks unverified)", {"lock": PLAYBOOKS_LOCK})
        return Check(cid, "fail", f"no {PLAYBOOKS_LOCK}: the playbooks installed in the org library "
                     "are unverified; run install-dbx-factory", {"lock": PLAYBOOKS_LOCK})
    try:
        lock_data = json.loads(lock.read_text())
    except (OSError, ValueError) as e:
        return Check(cid, "fail", f"{PLAYBOOKS_LOCK} unreadable: {_redact(str(e))}", {"lock": PLAYBOOKS_LOCK})
    if not isinstance(lock_data, dict):
        return Check(cid, "fail", f"{PLAYBOOKS_LOCK} is not a JSON object", {"lock": PLAYBOOKS_LOCK})
    repo = _repo_playbooks(plugin_root)
    data: dict = {"malformed": [], "stale": [], "missing": [], "unknown": [], "unlisted": [],
                  "checked": len(repo)}
    for macro, (_repo_file, sha) in repo.items():
        entry = lock_data.get(macro)
        if entry is None:
            data["missing"].append(macro)
        elif not isinstance(entry, dict) or not all(
                isinstance(entry.get(k), str) for k in ("sha256", "repo_file", "installed_at")):
            data["malformed"].append(macro)
        elif entry["sha256"] != sha:
            data["stale"].append(macro)
    data["unknown"] = sorted(m for m in lock_data if m not in repo)
    repo_files = {f for f, _sha in repo.values()}
    playbooks_dir = plugin_root / "skills" / "install-dbx-factory" / "playbooks"
    data["unlisted"] = sorted(p.name for p in playbooks_dir.glob("*.md")
                              if p.name not in _NOT_PLAYBOOKS and p.name not in repo_files)
    findings = [f"{k}: {', '.join(v)}" for k, v in
                (("malformed", data["malformed"]), ("stale", data["stale"]),
                 ("missing", data["missing"]), ("unknown", data["unknown"])) if v]
    if data["unlisted"]:
        findings.append(f"not in the 0-README Files table: {', '.join(data['unlisted'])}")
    live = live_playbooks or ws / LIVE_PLAYBOOKS
    data["live"] = None
    data["duplicate"] = {}
    data["live_missing"] = []
    data["live_stale"] = []
    age_min = None
    if not live.is_file():
        if role == "orchestrator" or live_playbooks is not None:
            findings.append(f"no {live if live_playbooks else LIVE_PLAYBOOKS}: export the live library with "
                            "devin_playbook_manage right before the doctor (see 9-orchestrator)")
    else:
        age = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(
            live.stat().st_mtime, datetime.timezone.utc)
        age_min = int(age.total_seconds() // 60)
        if age > LIVE_PLAYBOOKS_MAX_AGE:
            findings.append(f"stale export ({age_min} min old, max 15): re-export")
        else:
            try:
                records = json.loads(live.read_text())
            except (OSError, ValueError):
                records = None
            bad = next((i for i, r in enumerate(records) if not isinstance(r, dict)
                        or not isinstance(r.get("macro"), str)
                        or not isinstance(r.get("content"), str)), -1) \
                if isinstance(records, list) else -2
            if records is None or bad != -1:
                findings.append("live export malformed"
                                + (f" (record {bad})" if bad >= 0 else ""))
            else:
                grouped: dict[str, list[dict]] = {}
                for r in records:
                    grouped.setdefault(r["macro"], []).append(r)
                data["duplicate"] = {m: [r.get("playbook_id") for r in rs]
                                     for m, rs in grouped.items() if len(rs) > 1}
                findings += [f"duplicate: {m} ({', '.join(str(i) for i in ids)})"
                             for m, ids in data["duplicate"].items()]
                for macro, (f, _sha) in repo.items():
                    rs = grouped.get(macro)
                    if not rs:
                        data["live_missing"].append(macro)
                    elif _norm(rs[0]["content"]) != _norm((playbooks_dir / f).read_text()):
                        data["live_stale"].append(macro)
                if data["live_stale"]:
                    findings.append(f"live stale: {', '.join(data['live_stale'])}")
                if data["live_missing"]:
                    findings.append(f"live missing: {', '.join(data['live_missing'])}")
                data["live"] = {"checked": len(repo), "age_minutes": age_min}
    if findings:
        detail = "; ".join(findings)
        if data["duplicate"]:
            detail += "; duplicates: archive the extra"
        return Check(cid, "fail", detail + " — re-run install-dbx-factory", data)
    installed = [e["installed_at"] for e in lock_data.values()
                 if isinstance(e, dict) and isinstance(e.get("installed_at"), str)]
    installed_at = max(installed) if installed else "unknown"
    detail = f"{len(repo)} playbooks match the lock written at the last " \
             f"install-dbx-factory sync ({installed_at})"
    if data["live"]:
        detail += f" and the live export ({age_min} min old)"
    return Check(cid, "ok", detail,
                 {**data, "checked": len(repo), "installed_at": installed_at if installed else None})


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


def _harness_command(plugin_root: Path) -> tuple[list[str], Path | None, str] | None:
    """The harness this doctor grades: the installed dbx-recon first, else the checkout's
    module. Both `recon_harness` and `recon_family_supported` must ask the same executable."""
    harness = plugin_root / "skills" / "data-reconciliation" / "harness"
    if shutil.which("dbx-recon"):
        return ["dbx-recon"], None, "dbx-recon"
    if (harness / "recon" / "cli.py").exists():
        return [sys.executable, "-m", "recon.cli"], harness, f"python -m recon.cli (cwd {harness})"
    return None


def check_harness(plugin_root: Path) -> Check:
    cmd = _harness_command(plugin_root)
    if cmd is None:
        harness = plugin_root / "skills" / "data-reconciliation" / "harness"
        return Check("recon_harness", "fail", f"dbx-recon not on PATH and harness not at {harness}")
    argv, cwd, how = cmd
    rc, out, err = _run(argv + ["selftest"], **({"cwd": cwd} if cwd else {}))
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


def check_recon_family_supported(plugin_root: Path, source_family: str | None) -> Check:
    """Whether the harness can reconcile the declared source family, asked of the same harness
    `recon_harness` ran (dbx-recon on PATH, else the checkout). Deliberately not folded into
    source_principal_read_only: attestation says the principal is read-only; this row says
    whether we can reconcile the family."""
    cid = "recon_family_supported"
    if not source_family:
        return Check(cid, "skipped", "no source family declared (--source-family, or source.family in the wave manifest)")
    cmd = _harness_command(plugin_root)
    if cmd is None:
        return Check(cid, "fail", "cannot ask the harness which families it supports: dbx-recon "
                     "not on PATH and no checkout harness", {"family": source_family})
    argv, cwd, how = cmd
    rc, out, err = _run(argv + ["families"], **({"cwd": cwd} if cwd else {}))
    reg = None
    if rc == 0:
        try:
            reg = json.loads(out)
        except ValueError:
            pass
    if not isinstance(reg, dict) or not isinstance(reg.get("live_tested"), list) \
            or not isinstance(reg.get("untested"), list):
        return Check(cid, "fail", f"cannot ask the harness which families it supports "
                     f"({how} families rc={rc}): {_redact(err or out)}", {"family": source_family})
    live = sorted(reg["live_tested"])
    data = {"family": source_family, "live_tested": live, "harness": how}
    if source_family in reg["untested"]:
        return Check(cid, "fail", f"{source_family}: the harness refuses this family (`dbx-recon run --family "
                     f"{source_family}` exits before connecting). Attestation says the principal is read-only; this "
                     f"row says whether we can reconcile the family, and today we cannot: live-tested families are "
                     f"{live}; adding one is a live-tested adapter, never an attestation ({how})", data)
    if source_family not in live:
        return Check(cid, "fail", f"{source_family}: no source adapter in the harness; live-tested families: {live} ({how})", data)
    return Check(cid, "ok", f"{source_family}: live-tested source adapter ({how})", data)


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
# granting/impersonating one's way to it), the query answering them, the per-table `exists` query
# (NULL: the object does not exist or is not visible; the privilege functions would then answer 0
# on SQL Server, a false "no write", or raise on Postgres, so nothing else is asked about it and the
# row is `unverified`), the per-table query with one
# boolean column per privilege in TABLE_PRIVILEGES, the per-table `columns` query returning
# (column, privilege) rows for a write granted at column level (a table-level check does not see
# it), and the `indirect` queries, each returning
# (object, privilege) rows for a write path that bypasses table grants: IMPERSONATE on a visible
# login/user, EXECUTE on any procedure in the source database (every proc is assumed to write),
# EXECUTE on a SECURITY DEFINER or explicitly-granted function in an in-scope schema, and on
# Postgres every role the session can reach through memberships (`pg_has_role` is transitive, so a
# writer behind an intermediate role counts; on 16+ only memberships that inherit (USAGE) or can
# SET ROLE (SET), since a `SET FALSE, INHERIT FALSE` grant confers nothing; before 16 MEMBER implied
# both) that holds a write on an in-scope table or schema, or EXECUTE on such a function. Every value
# is a question about the principal; nothing here can change the source. Families without an
# entry are reported `unverified`, never `ok`.
_SRV_ROLES = ("sysadmin", "securityadmin", "serveradmin", "dbcreator", "bulkadmin")
_DB_ROLES = ("db_owner", "db_ddladmin", "db_datawriter", "db_securityadmin")
_SRV_PERMS = ("CONTROL SERVER", "ALTER ANY DATABASE", "IMPERSONATE ANY LOGIN", "ALTER ANY LOGIN")
_PG_ATTRS = ("rolsuper", "rolcreaterole", "rolcreatedb")
_PG_ROLES = ("pg_write_server_files", "pg_execute_server_program")
_ROLE_FLAGS = {"sqlserver": _SRV_ROLES + _DB_ROLES + _SRV_PERMS, "postgres": _PG_ATTRS + _PG_ROLES}
_PG_FUNCTIONS = ("SELECT n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', "
                 "'EXECUTE' FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ANY(%s) "
                 "AND (p.prosecdef OR p.proacl IS NOT NULL) AND has_function_privilege({who}p.oid, 'EXECUTE') ORDER BY 1")
_TABLE_PRIVILEGES = {"sqlserver": ("INSERT", "UPDATE", "DELETE", "ALTER"),
                     "postgres": ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE on schema")}
_PRIVILEGE_QUERIES = {
    "sqlserver": {
        "roles": "SELECT " + ", ".join([*(f"IS_SRVROLEMEMBER('{r}')" for r in _SRV_ROLES),
                                          *(f"IS_MEMBER('{r}')" for r in _DB_ROLES),
                                          *(f"HAS_PERMS_BY_NAME(NULL, NULL, '{p}')" for p in _SRV_PERMS)]),
        "exists": "SELECT OBJECT_ID(?)",
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
        "exists": "SELECT to_regclass(%s)",
        "table": "SELECT " + ", ".join(f"has_table_privilege(%s, '{p}')" for p in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"))
                 + ", has_schema_privilege(%s, 'CREATE')",
        "columns": ("SELECT a.attname, p FROM pg_attribute a CROSS JOIN unnest(ARRAY['INSERT', 'UPDATE']) AS p "
                    "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
                    "AND has_column_privilege(a.attrelid, a.attnum, p) AND NOT has_table_privilege(a.attrelid, p) "
                    "ORDER BY 1, 2"),
        "functions": _PG_FUNCTIONS.format(who=""),
        "as_role_functions": _PG_FUNCTIONS.format(who="%s, "),
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


def _indirect_writes(cur, q: dict, family: str, tables: list[str], resolved: list[str]) -> list[str]:
    """`object: privilege` for every write path that is not a grant on an in-scope table. Only `resolved`
    tables are named to the engine (Postgres raises for a relation that does not exist)."""
    found: list[str] = []
    for sql in q.get("indirect", ()):
        found += [f"{obj}: {priv}" for obj, priv in cur.execute(sql).fetchall()]
    if family == "postgres":
        schemas = list(dict.fromkeys(_schema(t) for t in tables))
        found += [f"{obj}: {priv}" for obj, priv in cur.execute(q["functions"], (schemas,)).fetchall()]
        for (role,) in cur.execute(q["members"]).fetchall():  # what SET ROLE <role> would unlock
            for t in resolved:
                write, create = cur.execute(q["as_role"], (role, t, role, _schema(t))).fetchall()[0]
                found += [f"SET ROLE {role}: {t} {w}" for w, held in (("write", write), ("CREATE on schema", create)) if held]
            found += [f"SET ROLE {role}: {obj} {priv}"
                      for obj, priv in cur.execute(q["as_role_functions"], (schemas, role)).fetchall()]
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
    if family == "databricks":
        return _check_databricks_source_principal(tables, source_secret)
    q = _PRIVILEGE_QUERIES.get(family)
    if q is None:
        return Check(cid, "unverified", f"{family}: no privilege query implemented for this family, so the "
                     "source principal's write privileges are unknown; confirm SELECT-only grants by hand and "
                     "record the decision in .migration/06_decisions.md, then pass --source-attested D-<id>",
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
            ro = f", transaction_read_only={cur.execute(q['read_only']).fetchall()[0][0]}" if q["read_only"] else ""
            data["stats"] = f"connection opened readonly=True{ro}; {_ADVISORY}"
            flags = cur.execute(q["roles"]).fetchall()[0]
            data["roles"] = [name for name, held in zip(_ROLE_FLAGS[family], flags) if held]
            resolved = []
            for t in tables:
                if cur.execute(q["exists"], (t,)).fetchall()[0][0] is None:
                    data["unresolved"].append(t)
                    continue
                resolved.append(t)
                row = cur.execute(q["table"], (t,) * 4 + ((_schema(t),) if family == "postgres" else ())).fetchall()[0]
                held = [p for p, v in zip(_TABLE_PRIVILEGES[family], row) if v]
                held += [f"{p} on column {c}" for c, p in cur.execute(q["columns"], (t,)).fetchall() if p not in held]
                if held:
                    data["writable"][t] = held
            data["indirect"] = _indirect_writes(cur, q, family, tables, resolved)
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


# Effective-grant privileges that only read. Anything else the CLI reports on an in-scope
# securable (MODIFY, WRITE_FILES, CREATE_*, OWN, ALL_PRIVILEGES, MANAGE, APPLY_TAG, EXECUTE, or
# something new) is a write path: the check fails closed.
_DBX_READ_PRIVILEGES = frozenset({"SELECT", "USE_CATALOG", "USE_SCHEMA", "BROWSE", "READ_VOLUME"})
_DBX_GET_COMMAND = {"catalog": "catalogs", "schema": "schemas", "table": "tables"}


def _check_databricks_source_principal(tables: list[str], source_secret: str | None) -> Check:
    """The databricks family source principal is the one behind --source-secret — the
    {server_hostname, http_path, access_token} JSON the recon adapter opens — resolved as the
    `current-user me` (applicationId or userName) that token authenticates as on that host.
    `grants get-effective` on every in-scope catalog, schema and table (effective grants already
    include group memberships), plus ownership of each, direct or through a group."""
    cid = "source_principal_read_only"
    data: dict = {"family": "databricks", "tables": tables, "writable": {}, "unresolved": []}
    if not source_secret:
        return Check(cid, "fail", f"databricks source with {len(tables)} in-scope table(s); pass "
                     "--source-secret NAME (env var holding the source DSN) so the principal's "
                     "write privileges can be checked", data)
    secret = os.environ.get(source_secret)
    if not secret:
        return Check(cid, "fail", f"source secret {source_secret} is not set in the environment", data)
    try:
        cfg = json.loads(secret)
        host, token = cfg["server_hostname"], cfg["access_token"]
    except (TypeError, ValueError, KeyError):
        host = token = None
    if not isinstance(host, str) or not isinstance(token, str):
        return Check(cid, "unverified", f"databricks: secret {source_secret} is not the "
                     "{server_hostname,http_path,access_token} JSON the recon adapter uses", data)
    data["host"] = host
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_")}
    env["DATABRICKS_HOST"] = host if "://" in host else f"https://{host}"
    env["DATABRICKS_TOKEN"] = token
    env["DATABRICKS_AUTH_TYPE"] = "pat"
    cli = shutil.which("databricks")
    if not cli:
        return Check(cid, "unverified", "databricks: databricks CLI not on PATH, so the source "
                     "principal's grants could not be read", data)
    rc, out, err = _run([cli, "current-user", "me", "--output", "json"], env=env)
    try:
        who = json.loads(out) if rc == 0 else {}
        principal = who.get("applicationId") or who.get("userName")
        groups = {g["display"] for g in who.get("groups", []) if isinstance(g, dict)
                  and isinstance(g.get("display"), str)}
    except (TypeError, ValueError, AttributeError):
        principal, groups = None, set()
    if not isinstance(principal, str) or not principal:
        return Check(cid, "unverified", f"databricks: current-user me failed: {_redact(err or out)}", data)
    data["principal"] = principal
    securables: dict[tuple[str, str], None] = {}
    for t in tables:
        parts = t.split(".")
        if len(parts) != 3 or not all(parts):
            data["unresolved"].append(t)
            continue
        securables.setdefault(("catalog", parts[0]))
        securables.setdefault(("schema", f"{parts[0]}.{parts[1]}"))
        securables.setdefault(("table", t))
    for kind, name in securables:
        rc, out, err = _run([cli, _DBX_GET_COMMAND[kind], "get", name, "--output", "json"], env=env)
        owner = None
        if rc == 0:
            try:
                payload = json.loads(out)
                owner = payload.get("owner") if isinstance(payload, dict) else None
            except (TypeError, ValueError):
                owner = None
        if not isinstance(owner, str):
            return Check(cid, "unverified", f"databricks: {_DBX_GET_COMMAND[kind]} get {name} "
                         f"failed: {_redact(err or out)}", data)
        if owner.lower() == principal.lower() or owner in groups:
            data["writable"].setdefault(name, []).append("OWNER")
        rc, out, err = _run([cli, "grants", "get-effective", kind, name,
                             "--principal", principal, "--output", "json"], env=env)
        if rc != 0:
            return Check(cid, "unverified", f"databricks: grants get-effective {kind} {name} "
                         f"failed: {_redact(err or out)}", data)
        try:
            privileges = _effective_privileges_strict(json.loads(out))
        except (TypeError, ValueError):
            privileges = None
        if privileges is None:
            return Check(cid, "unverified", f"databricks: grants get-effective {kind} {name} "
                         "returned no privilege_assignments", data)
        offending = sorted(privileges - _DBX_READ_PRIVILEGES)
        if offending:
            data["writable"][name] = data["writable"].get(name, []) + offending
    if data["writable"]:
        can_write = [f"{name}: {', '.join(privs)}" for name, privs in data["writable"].items()]
        shown = "; ".join(can_write[:6]) + (f"; +{len(can_write) - 6} more in data" if len(can_write) > 6 else "")
        return Check(cid, "fail", f"databricks: the source principal can write in scope ({shown}); the "
                     "factory needs a SELECT-only principal", data)
    if data["unresolved"]:
        return Check(cid, "unverified", f"databricks: privileges could not be evaluated for "
                     f"{data['unresolved']}: in-scope tables need 3-part catalog.schema.table names", data)
    return Check(cid, "ok", f"databricks: {principal} holds only read privileges "
                 "(SELECT/USE_CATALOG/USE_SCHEMA/BROWSE/READ_VOLUME) on "
                 f"{len(tables)} in-scope table(s), their schemas and catalogs; no ownership", data)


_USER_PROVENANCE = re.compile(r"(?<![\w-])user:[\w][\w.@/-]*")


def _attested(ws: Path, decision: str, family: str, tables: list[str]) -> Check:
    """--source-attested D-<id>: a ledger decision standing in for a privilege query the family
    does not have (files in object storage, a read-only share, a static dump — no principal to
    query). The decision's line must name the check, the attestation and a `user:<id>` provenance
    (a default-accepted row is not a human attesting); a family with a query runs the query instead."""
    cid = "source_principal_read_only"
    if family == "databricks" or family in _PRIVILEGE_QUERIES:
        return Check(cid, "fail", f"{family}: --source-attested {decision} rejected, this family has a "
                     "privilege query: run the query instead (drop --source-attested)",
                     {"family": family, "decision": decision})
    ledger = ws / ".migration" / "06_decisions.md"
    if not ledger.is_file():
        return Check(cid, "fail", f"{family}: ledger .migration/06_decisions.md not found",
                     {"decision": decision})
    named = re.compile(rf"(?<![\w-]){re.escape(decision)}(?![\w-])")
    for line in ledger.read_text().splitlines():
        if named.search(line) and "source_principal_read_only" in line and "attested" in line:
            who = _USER_PROVENANCE.search(line)
            if not who:
                return Check(cid, "fail", f"{family}: decision {decision} attests source_principal_read_only "
                             "without user:<id> provenance; a default-accepted row cannot attest the source "
                             "is read-only, a human has to reply", {"decision": decision})
            return Check(cid, "attested", f"{family}: source principal read-only attested by decision "
                         f"{decision} ({who.group(0)}) in .migration/06_decisions.md (no principal to query)",
                         {"decision": decision, "family": family, "tables": tables, "provenance": who.group(0)})
    return Check(cid, "fail", f"{family}: decision {decision} is not in .migration/06_decisions.md with "
                 "'source_principal_read_only', 'attested' and user:<id> provenance in its line; record the "
                 "attestation in the ledger first", {"decision": decision})


def check_source_principal_all(ws: Path, role: str, units: list[str], mappings: list[Path], source_secret: str | None,
                               source_family: str | None, plugin_root: Path, params: dict[str, str] | None = None,
                               attested: str | None = None) -> Check:
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
    if attested:
        return _attested(ws, attested, family, list(tables))
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


def _norm_host(host: str) -> str:
    return re.sub(r"^https?://", "", host.strip().lower()).rstrip("/")


def check_databricks(expect_identity: str | None, expect_host: str | None = None) -> list[Check]:
    out: list[Check] = []
    cli = shutil.which("databricks")
    if not cli:
        out.append(Check("databricks_cli", "fail", "databricks CLI not on PATH (see databricks-core for install)"))
        return out
    rc, ver, err = _run([cli, "--version"], timeout=20)
    out.append(Check("databricks_cli", "ok" if rc == 0 else "fail", (ver or err).strip()[:80], {"path": cli}))

    set_vars = [v for v in M2M_VARS if os.environ.get(v)]
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if os.environ.get("DATABRICKS_TOKEN") and len(set_vars) == len(M2M_VARS):
        auth_kind = "conflict (env)"
    elif len(set_vars) == len(M2M_VARS):
        auth_kind = "oauth-m2m (env)"
    elif os.environ.get("DATABRICKS_TOKEN"):
        auth_kind = "pat (env)"
    elif profile:
        auth_kind = f"profile {profile}"
    else:
        auth_kind = "unknown (CLI default chain)"
    if auth_kind == "conflict (env)":
        detail = ("auth: conflicting env — DATABRICKS_TOKEN is set beside DATABRICKS_CLIENT_ID/SECRET; the CLI refuses "
                  "('more than one authorization method') and the guard blocks unsetting or overriding them per command; "
                  "remove DATABRICKS_TOKEN (and a foreign DATABRICKS_HOST) from the org/session environment")
        status = "fail"
    else:
        detail = (f"auth: {auth_kind}; migration sessions should run as the migration service principal via "
                  f"DATABRICKS_CLIENT_ID/SECRET from named secrets")
        status = "ok" if auth_kind.startswith("oauth-m2m") else "warn"
    out.append(Check("databricks_auth_kind", status, detail, {"auth_kind": auth_kind, "env_set": set_vars}))

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
    display_name = name if is_sp else "<human user (redacted)>"
    data = {"userName": display_name, "service_principal": is_sp, "host": host}
    status = "ok"
    detail = f"authenticated as {display_name} ({'service principal' if is_sp else 'user'}) on {host}"
    if expect_identity and str(name).lower() != expect_identity.lower():
        expected_display = "<human user (redacted)>" if "@" in expect_identity else expect_identity
        status, detail = "fail", detail + f"; expected {expected_display} (recorded in 07_access_checklist.md)"
    elif not host:
        status = "fail"
        detail += "; workspace host not resolved by `databricks auth describe`, so the wave manifest cannot pin children to it"
    elif expect_host and _norm_host(str(host)) != _norm_host(expect_host):
        status, detail = "fail", detail + f"; expected host {expect_host} (the capability contract's workspace)"
    elif not is_sp:
        status, detail = "warn", detail + (
            "; unattended sessions must not run as a human identity: provide "
            "DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (plus DATABRICKS_HOST) as named "
            "secrets for the migration service principal, or record a waiver in 06_decisions.md"
        )
    out.append(Check("databricks_identity", status, detail, data))

    rc, wh, err = _run([cli, "experimental", "aitools", "tools", "get-default-warehouse"], timeout=60)
    if rc == 0 and wh.strip():
        out.append(Check("databricks_warehouse", "ok", f"default warehouse resolved: {wh.strip()[:120]}"))
    else:
        out.append(Check("databricks_warehouse", "warn", f"no default warehouse via aitools: {_redact(err or wh)}"))
    return out


def check_lakebase_branch_create(project: str, parent_branch: str) -> Check:
    """Create and delete a short-lived branch to prove Lakebase project access."""
    cli = shutil.which("databricks")
    if not cli:
        return Check("lakebase_branch_create", "unverified",
                     "databricks CLI not on PATH; install databricks-core and databricks-lakebase")
    branch = f"dbx-doctor-probe-{secrets.token_hex(4)}"
    project_path = f"projects/{project}"
    source = f"{project_path}/branches/{parent_branch}"
    rc, out, err = _run(
        [cli, "postgres", "create-branch", project_path, branch, "--json",
         json.dumps({"spec": {"source_branch": source, "ttl": "3600s"}}), "--output", "json"],
        timeout=300)
    if rc != 0:
        low = err.lower()
        if "not authorized" in low:
            detail = (f"migration principal is not authorized; grant the migration principal "
                      f"Can Manage on Lakebase project {project}")
        elif "expiration date cannot have child branches" in low:
            detail = (f"parent branch {parent_branch} has an expiry; clear it "
                      "(Lakebase project → branch → edit → remove expiration), TTLs belong on per-batch children")
        else:
            detail = _redact(err or out)
        return Check("lakebase_branch_create", "fail", detail,
                     {"project": project, "parent_branch": parent_branch})
    delete_rc, delete_out, delete_err = _run(
        [cli, "postgres", "delete-branch", f"{project_path}/branches/{branch}", "--purge"], timeout=300)
    if delete_rc != 0:
        return Check("lakebase_branch_create", "warn",
                     f"branch {branch} was created but could not be deleted: "
                     f"{_redact(delete_err or delete_out)}",
                     {"project": project, "parent_branch": parent_branch, "branch": branch})
    return Check("lakebase_branch_create", "ok", "branch created and deleted",
                 {"project": project, "parent_branch": parent_branch, "branch": branch})


def check_lakebase_target_grants(dsn_name: str, schema: str | None = None, connect=None) -> Check:
    """Check CREATE on the configured Lakebase database or optional schema."""
    if not os.environ.get(dsn_name):
        return Check("lakebase_target_grants", "fail", f"secret {dsn_name} is not set in this shell")
    if connect is None:
        try:
            import psycopg  # lazy: optional extra
        except ImportError:
            return Check("lakebase_target_grants", "fail",
                         "psycopg is unavailable; install the postgres extra for data-reconciliation")
        connect = lambda dsn: psycopg.connect(dsn)
    conn = None
    try:
        conn = connect(os.environ[dsn_name])
        cur = conn.cursor()
        cur.execute("select current_user, current_database(), "
                    "has_database_privilege(current_user, current_database(), 'CREATE')")
        role, database, db_create = cur.fetchone()
        schema_exists = False
        schema_create = False
        if schema:
            cur.execute("select 1 from information_schema.schemata where schema_name=%s", (schema,))
            schema_exists = cur.fetchone() is not None
            if schema_exists:
                cur.execute("select has_schema_privilege(current_user, %s, 'CREATE')", (schema,))
                schema_create = bool(cur.fetchone()[0])
        if schema and schema_exists:
            if not schema_create:
                return Check("lakebase_target_grants", "fail",
                             f"missing required privilege: GRANT CREATE ON SCHEMA {schema} TO {role}",
                             {"role": role, "database": database, "schema": schema})
            return Check("lakebase_target_grants", "ok",
                         f"role {role} can CREATE in schema {schema}",
                         {"role": role, "database": database, "schema": schema})
        if bool(db_create):
            return Check("lakebase_target_grants", "ok",
                         f"role {role} can CREATE in database {database}",
                         {"role": role, "database": database, "schema": schema})
        return Check("lakebase_target_grants", "fail",
                     f"missing required privilege: GRANT CREATE ON DATABASE {database} TO {role}",
                     {"role": role, "database": database, "schema": schema})
    except Exception as e:  # noqa: BLE001 - driver-specific connection errors
        return Check("lakebase_target_grants", "fail",
                     f"connection to {dsn_name} failed ({type(e).__name__}); "
                     "check the DSN secret and network path")
    finally:
        if conn is not None and hasattr(conn, "close"):
            conn.close()


def _effective_privileges(payload) -> set[str]:
    """Extract privilege names from effective assignments or non-effective grant payloads."""
    found: set[str] = set()

    def add(value) -> None:
        if isinstance(value, str):
            found.add(value.upper())
        elif isinstance(value, dict):
            privilege = value.get("privilege")
            if isinstance(privilege, str):
                found.add(privilege.upper())

    if isinstance(payload, dict):
        assignments = payload.get("privilege_assignments")
        if isinstance(assignments, list):
            for assignment in assignments:
                if isinstance(assignment, dict):
                    privileges = assignment.get("privileges", [])
                    if isinstance(privileges, list):
                        for privilege in privileges:
                            add(privilege)
        privileges = payload.get("privileges")
        if isinstance(privileges, list):
            for privilege in privileges:
                add(privilege)
    elif isinstance(payload, list):
        for privilege in payload:
            add(privilege)
    return found


def _effective_privileges_strict(payload) -> set[str] | None:
    """None unless the payload is exactly a get-effective grants response: a dict with a
    `privilege_assignments` list where every assignment is a dict whose `privileges` is a
    list of strings. An empty assignments list is valid (empty set). Anything else means
    the grants could not be read and must not pass as read-only."""
    if not isinstance(payload, dict):
        return None
    assignments = payload.get("privilege_assignments")
    if not isinstance(assignments, list):
        return None
    found: set[str] = set()
    for assignment in assignments:
        if not isinstance(assignment, dict):
            return None
        privileges = assignment.get("privileges")
        if not isinstance(privileges, list) or not all(isinstance(p, str) for p in privileges):
            return None
        found.update(p.upper() for p in privileges)
    return found


def _permission_error(text: str) -> bool:
    low = text.lower()
    return any(marker in low for marker in
               ("does not have", "permission_denied", "permission denied", "insufficient", "unauthorized"))


def _sql_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _grant_statements(catalog: str, full_name: str, principal: str,
                      missing_catalog: list[str], missing_schema: list[str]) -> str:
    schema_required = [("USE_SCHEMA", "USE SCHEMA"), ("CREATE_TABLE", "CREATE TABLE"),
                       ("MODIFY", "MODIFY"), ("SELECT", "SELECT")]
    statements = []
    if missing_catalog:
        statements.append(f"GRANT USE CATALOG ON CATALOG {_sql_ident(catalog)} TO `{principal}`")
    if missing_schema:
        display = ", ".join(label for name, label in schema_required if name in missing_schema)
        schema_catalog, schema_name = full_name.split(".", 1)
        statements.append(
            f"GRANT {display} ON SCHEMA {_sql_ident(schema_catalog)}.{_sql_ident(schema_name)} TO `{principal}`"
        )
    return "; ".join(statements)


def _catalog_privileges(cli: str, catalog: str, principal: str) -> tuple[set[str] | None, str | None, str | None]:
    """Return catalog privileges, owner, and a redacted error when both lookups fail."""
    catalog_owner = None
    rc, out, err = _run([cli, "catalogs", "get", catalog, "--output", "json"])
    if rc == 0:
        try:
            catalog_owner = json.loads(out).get("owner")
        except (TypeError, ValueError, AttributeError):
            catalog_owner = None
        if isinstance(catalog_owner, str) and catalog_owner.lower() == principal.lower():
            return {"ALL_PRIVILEGES"}, catalog_owner, None

    rc, out, err = _run([cli, "grants", "get-effective", "catalog", catalog,
                         "--principal", principal, "--output", "json"])
    if rc != 0:
        return None, catalog_owner, _redact(err or out)
    try:
        return _effective_privileges(json.loads(out)), catalog_owner, None
    except (TypeError, ValueError):
        return None, catalog_owner, _redact(err or out)


def check_analytical_target_grants(full_name: str) -> Check:
    """Check Unity Catalog privileges needed to create or write the promotion schema."""
    cid = "analytical_target_grants"
    if full_name.count(".") != 1 or any(not part for part in full_name.split(".")):
        return Check(cid, "fail", "expected CATALOG.SCHEMA",
                     {"schema": full_name, "principal": None, "owner": None, "catalog_owner": None,
                      "exists": False, "missing": []})
    catalog, schema = full_name.split(".", 1)
    cli = shutil.which("databricks")
    if not cli:
        return Check(cid, "unverified",
                     "databricks CLI not on PATH; install databricks-core and databricks-unity-catalog",
                     {"schema": full_name, "principal": None, "owner": None, "catalog_owner": None,
                      "exists": False, "missing": []})

    rc, out, err = _run([cli, "current-user", "me", "--output", "json"])
    if rc != 0:
        return Check(cid, "fail", _redact(err or out),
                     {"schema": full_name, "principal": None, "owner": None, "catalog_owner": None,
                      "exists": False, "missing": []})
    try:
        current_user = json.loads(out)
        principal = current_user.get("applicationId") or current_user.get("userName")
    except (TypeError, ValueError, AttributeError):
        return Check(cid, "fail", _redact(err or out),
                     {"schema": full_name, "principal": None, "owner": None, "catalog_owner": None,
                      "exists": False, "missing": []})
    if not isinstance(principal, str) or not principal:
        return Check(cid, "fail", "current-user response has no applicationId or userName",
                     {"schema": full_name, "principal": None, "owner": None, "catalog_owner": None,
                      "exists": False, "missing": []})

    base_data = {"schema": full_name, "principal": principal, "owner": None, "catalog_owner": None,
                 "exists": False, "missing": []}
    rc, out, err = _run([cli, "schemas", "get", full_name, "--output", "json"])
    if rc != 0:
        low = err.lower()
        absent = any(marker in low for marker in
                     ("not found", "does not exist", "not_found", "schema_does_not_exist"))
        unreadable = _permission_error(err)
        if not absent and not unreadable:
            return Check(cid, "fail", _redact(err or out), base_data)
        if unreadable:
            base_data["exists"] = True
            rc, out, err = _run([cli, "grants", "get-effective", "schema", full_name,
                                 "--principal", principal, "--output", "json"])
            if rc != 0 and (_permission_error(err) or out.strip() == "{}"):
                schema_privileges = set()
            elif rc != 0:
                return Check(cid, "fail", _redact(err or out), base_data)
            else:
                try:
                    schema_privileges = _effective_privileges(json.loads(out))
                except (TypeError, ValueError):
                    return Check(cid, "fail", _redact(err or out), base_data)
        else:
            schema_privileges = set()
        privileges, catalog_owner, catalog_error = _catalog_privileges(cli, catalog, principal)
        base_data["catalog_owner"] = catalog_owner
        if catalog_error:
            return Check(cid, "fail", catalog_error, base_data)
        assert privileges is not None
        required = [("USE_CATALOG", "USE CATALOG"), ("CREATE_SCHEMA", "CREATE SCHEMA")]
        missing = [name for name, _ in required if "ALL_PRIVILEGES" not in privileges and name not in privileges]
        if unreadable:
            schema_required = [("USE_SCHEMA", "USE SCHEMA"), ("CREATE_TABLE", "CREATE TABLE"),
                               ("MODIFY", "MODIFY"), ("SELECT", "SELECT")]
            missing_schema = [name for name, _ in schema_required
                              if "ALL_PRIVILEGES" not in schema_privileges and name not in schema_privileges]
            missing = ([name for name, _ in [("USE_CATALOG", "USE CATALOG")]
                        if "ALL_PRIVILEGES" not in privileges and name not in privileges] + missing_schema)
            base_data["missing"] = missing
            statement = _grant_statements(catalog, full_name, principal,
                                           ["USE_CATALOG"] if "USE_CATALOG" in missing else [], missing_schema)
            return Check(cid, "fail",
                         f"missing required privileges: {statement}; owner is unknown",
                         base_data)
        base_data["missing"] = missing
        if missing:
            display = ", ".join(label for name, label in required if name in missing)
            return Check(cid, "fail",
                         f"missing required privileges: GRANT {display} ON CATALOG {_sql_ident(catalog)} TO `{principal}`",
                         base_data)
        base_data["exists"] = False
        return Check(cid, "ok",
                     f"schema {full_name} does not exist; setup creates it owned by {principal} (no grants needed)",
                     base_data)

    try:
        schema_payload = json.loads(out)
        owner = schema_payload.get("owner")
    except (TypeError, ValueError, AttributeError):
        return Check(cid, "fail", _redact(err or out), base_data)
    base_data["owner"] = owner
    base_data["exists"] = True
    if isinstance(owner, str) and owner.lower() == principal.lower():
        base_data["owner"] = principal
        catalog_privileges, catalog_owner, catalog_error = _catalog_privileges(cli, catalog, principal)
        base_data["catalog_owner"] = catalog_owner
        if catalog_error:
            return Check(cid, "fail", catalog_error, base_data)
        assert catalog_privileges is not None
        missing_catalog = [] if (
            "ALL_PRIVILEGES" in catalog_privileges or "USE_CATALOG" in catalog_privileges
        ) else ["USE_CATALOG"]
        base_data["missing"] = missing_catalog
        if missing_catalog:
            statement = _grant_statements(catalog, full_name, principal, missing_catalog, [])
            return Check(cid, "fail",
                         f"missing required privileges: {statement}; schema owned by {principal}",
                         base_data)
        return Check(cid, "ok", f"schema {full_name} is owned by {principal}", base_data)

    rc, out, err = _run([cli, "grants", "get-effective", "schema", full_name,
                         "--principal", principal, "--output", "json"])
    if rc != 0:
        if _permission_error(err) or out.strip() == "{}":
            schema_privileges = set()
        else:
            return Check(cid, "fail", _redact(err or out), base_data)
    else:
        try:
            schema_privileges = _effective_privileges(json.loads(out))
        except (TypeError, ValueError):
            return Check(cid, "fail", _redact(err or out), base_data)
    catalog_privileges, catalog_owner, catalog_error = _catalog_privileges(cli, catalog, principal)
    base_data["catalog_owner"] = catalog_owner
    if catalog_error:
        return Check(cid, "fail", catalog_error, base_data)
    assert catalog_privileges is not None

    schema_required = [("USE_SCHEMA", "USE SCHEMA"), ("CREATE_TABLE", "CREATE TABLE"),
                       ("MODIFY", "MODIFY"), ("SELECT", "SELECT")]
    missing_schema = [name for name, _ in schema_required
                      if "ALL_PRIVILEGES" not in schema_privileges and name not in schema_privileges]
    missing_catalog = [] if ("ALL_PRIVILEGES" in catalog_privileges or "USE_CATALOG" in catalog_privileges) else ["USE_CATALOG"]
    missing = missing_catalog + missing_schema
    base_data["missing"] = missing
    if missing:
        statement = _grant_statements(catalog, full_name, principal, missing_catalog, missing_schema)
        owner_text = owner if owner is not None else "unknown"
        return Check(cid, "fail", f"missing required privileges: {statement}; owner is {owner_text}",
                     base_data)
    owner_text = owner if owner is not None else "unknown"
    return Check(cid, "ok",
                 f"principal {principal} can create and write tables in {full_name} (owner {owner_text})",
                 base_data)


# ------------------------------------------------------------------ main

def run(ws: Path, plugin_root: Path, role: str, probe_result: str, expect_identity: str | None,
        no_databricks: bool, units: list[str] | None = None, mappings: list[Path] | None = None,
        source_secret: str | None = None, params: dict[str, str] | None = None,
        expect_catalogs: list[str] | None = None, source_family: str | None = None,
        expect_host: str | None = None, lakebase_project: str | None = None,
        lakebase_parent_branch: str | None = None, lakebase_dsn: str | None = None,
        lakebase_schema: str | None = None, analytical_schema: str | None = None,
        source_attested: str | None = None, live_playbooks: Path | None = None) -> dict:
    checks: list[Check] = [
        _merge("workspace", [check_workspace(ws), check_stop_mode(ws)]),
        _merge("allowed_targets", [check_allowed_targets(ws, plugin_root),
                                   check_allowlist_matches_contract(ws, expect_catalogs)]),
        check_allowlist_committed(ws),
        check_playbooks_in_sync(ws, plugin_root, role, live_playbooks),
        _merge("hook_guard", check_hooks(plugin_root, ws, probe_result)),
        check_official_plugin(plugin_root),
        _merge("recon_harness", [check_harness(plugin_root), check_drivers()]),
        check_recon_family_supported(plugin_root, source_family),
        check_delete_evidence_all(ws, role, units or [], mappings or [], source_secret, plugin_root,
                                  params=params),
        check_source_principal_all(ws, role, units or [], mappings or [], source_secret, source_family,
                                   plugin_root, params=params, attested=source_attested),
    ]
    if no_databricks:
        checks.append(Check("databricks_identity", "skipped", "--no-databricks"))
    else:
        checks.append(_merge("databricks_identity", check_databricks(expect_identity, expect_host)))
    if lakebase_project or lakebase_parent_branch:
        if not lakebase_project or not lakebase_parent_branch:
            checks.append(Check("lakebase_branch_create", "fail",
                                "--lakebase-project and --lakebase-parent-branch must be passed together"))
        else:
            checks.append(check_lakebase_branch_create(lakebase_project, lakebase_parent_branch))
    if lakebase_dsn:
        checks.append(check_lakebase_target_grants(lakebase_dsn, lakebase_schema))
    if analytical_schema:
        checks.append(Check("analytical_target_grants", "skipped", "--no-databricks")
                        if no_databricks else check_analytical_target_grants(analytical_schema))
    counts: dict[str, int] = {}
    for c in checks:
        counts[c.status] = counts.get(c.status, 0) + 1
    blocking = []
    for row in checks:
        subs = list(_flat([row]))
        if any(s.status == "fail" or (s.id in SECURITY_CONTROLS and s.status != "ok")
               or (s.id == "source_principal_read_only" and s.status == "unverified")
               for s in subs):
            blocking.append(f"{row.id}={row.status}")
    identity = next((c.data for c in _flat(checks) if c.id == "databricks_identity" and c.data), None)
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
    p.add_argument("--role", choices=("orchestrator", "child", "setup"), default="orchestrator")
    p.add_argument("--wave", type=Path,
                   help="wave manifest; also writes <manifest>.doctor.json, the signed record the fan-out workflow launches from")
    p.add_argument("--hook-probe-result", default="unknown", metavar="blocked:<nonce>|not-blocked|unknown",
                   help="outcome of running the probe_command of the last report; the nonce is the one the "
                        "guard's block message named")
    p.add_argument("--expect-identity", help="userName the session must be authenticated as")
    p.add_argument("--expect-host", help="workspace host the session must be authenticated against (the contract's)")
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
    p.add_argument("--source-attested", metavar="D-<id>",
                   help="decision id in .migration/06_decisions.md attesting the source has no principal to query "
                        "(files in object storage, a read-only share, a static dump); rejected for families with a "
                        "privilege query")
    p.add_argument("--lakebase-project", help="Lakebase project id for the branch-create preflight")
    p.add_argument("--lakebase-parent-branch", help="Lakebase parent branch for the branch-create preflight")
    p.add_argument("--lakebase-dsn", metavar="ENV_VAR_NAME",
                   help="env var NAME holding the Lakebase DSN (value never printed)")
    p.add_argument("--lakebase-schema", help="optional Lakebase schema to check for CREATE")
    p.add_argument("--analytical-schema", metavar="CATALOG.SCHEMA",
                   help="analytical target schema to check the principal can create and write tables in (or owns)")
    p.add_argument("--live-playbooks", type=Path, metavar="PATH",
                   help="JSON export of the live [DBX v1] playbooks (list of {macro, playbook_id, content}) "
                        "written with devin_playbook_manage right before this run; default "
                        ".migration/live_playbooks.json; required and <15 min old for --role orchestrator")
    p.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                   help="mapping ${NAME} placeholder value, same rules and values as dbx-recon run --param")
    p.add_argument("--out", type=Path, help="default .migration/09_capabilities.json; '-' for stdout only")
    a = p.parse_args(argv)
    if a.hook_probe_result == "blocked":
        p.error("--hook-probe-result blocked:<nonce> is required: the nonce the guard's block message named for the probe_command "
                "of the last report")
    manifest_bytes = None
    if a.wave:
        try:
            manifest_bytes = a.wave.read_bytes()
            manifest = json.loads(manifest_bytes)
        except (OSError, ValueError) as e:
            p.error(f"cannot read wave manifest {a.wave}: {e}")
        caps = manifest.get("capabilities") if isinstance(manifest, dict) else None
        if not isinstance(caps, dict):
            p.error(f"wave manifest {a.wave} has no capabilities object")
        if a.expect_identity is None:
            a.expect_identity = caps.get("identity")
        if a.expect_host is None:
            a.expect_host = caps.get("host")
        if a.expect_catalogs is None:
            catalogs = caps.get("catalogs")
            a.expect_catalogs = catalogs if isinstance(catalogs, list) else None
        source = manifest.get("source")
        if a.source_family is not None or a.source_secret is not None or a.param:
            p.error("--wave takes source settings from the manifest; drop --source-family/--source-secret/--param")
        a.source_family = source.get("family") if isinstance(source, dict) else None
        a.source_secret = source.get("secret") if isinstance(source, dict) else None
        a.param = [f"{k}={v}" for k, v in (source.get("params") or {}).items()] if isinstance(source, dict) else []

    params = None
    if a.param:
        sys.path.insert(0, str(a.plugin_root.resolve() / "skills" / "data-reconciliation" / "harness"))
        from recon.cli import parse_params
        params = parse_params(a.param)

    report = run(a.workspace.resolve(), a.plugin_root.resolve(), a.role, a.hook_probe_result,
                 a.expect_identity, a.no_databricks, a.unit, a.mapping, a.source_secret, params,
                 a.expect_catalogs, a.source_family, a.expect_host, a.lakebase_project,
                 a.lakebase_parent_branch, a.lakebase_dsn, a.lakebase_schema, a.analytical_schema,
                 source_attested=a.source_attested, live_playbooks=a.live_playbooks)
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
    if a.wave:
        a.wave.with_suffix(".doctor.json").write_text(json.dumps(
            sign_wave_report({**report, "hook_probe": a.hook_probe_result, "source": manifest.get("source")},
                             manifest_bytes),
            indent=2, sort_keys=True) + "\n")
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
