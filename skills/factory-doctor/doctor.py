#!/usr/bin/env python3
"""factory-doctor: preflight for a DBX migration workspace; exit 0 when `ready` (see _blocking).

Usage: python3 doctor.py --help
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

REQUIRED_FILES = ("00_context.md", "01_conventions.md", "03_recon_tolerances.md", "03_recon_tolerances.json",
    "04_dependency_register.md", "06_decisions.md", "07_access_checklist.md", "allowed_targets.json")
OFFICIAL_SKILLS = ("databricks-core", "databricks-dbsql", "databricks-pipelines", "databricks-jobs",
    "databricks-dabs", "databricks-unity-catalog", "databricks-lakeflow-connect", "databricks-lakebase")
SECURITY_CONTROLS = ("hook_guard_functional", "hook_platform_loaded", "databricks_identity")
CHILD_SECURITY_CONTROLS = ("hook_guard_functional", "databricks_identity")
_RANK = {"ok": 0, "skipped": 1, "warn": 2, "unverified": 3, "fail": 4}
DRIVERS = {
    "databricks": "databricks.sql",
    "sqlserver": "pyodbc",
    "postgres": "psycopg",
}  # the adapters the harness runs
# The families `dbx-recon run --family` accepts; only sqlserver and postgres have a privilege query.
SOURCE_FAMILIES = ("databricks", "oracle", "postgres", "redshift", "snowflake", "sqlserver", "teradata")
TARGET_KINDS = ("databricks", "lakebase")  # the harness's --target-kind values; the type map is keyed by both
# The committed wave contract: the guard and the harness read the working copy, so a working copy
# that differs from HEAD is a contract nobody reviewed.
LEDGER_CONTRACT_FILES = (".migration/allowed_targets.json", ".migration/03_recon_tolerances.json")
CAPABILITIES = ".migration/09_capabilities.json"
PLAYBOOKS_LOCK = ".migration/playbooks.lock.json"
LIVE_PLAYBOOKS = ".migration/live_playbooks.json"
LIVE_PLAYBOOKS_MAX_AGE = datetime.timedelta(minutes=15)
HOOK_PROBE_NONCE = ".migration/.hook_probe_nonce"
HOOK_PROBE_NONCE_TTL = 8 * 60 * 60
# Safe live probe: if the platform loads hooks.json the guard blocks this; else `echo` prints and
# nothing else happens. The nonce is issued per report and echoed in the block reason, so
# `blocked:<nonce>` can only be passed back by a session that saw the block.
HOOK_PROBE_TOKEN = "__dbx_guard_probe__{nonce}"
HOOK_PROBE_COMMAND = ("echo 'databricks experimental aitools tools query "
    f'"DROP TABLE {HOOK_PROBE_TOKEN}.x.y"\' # factory-doctor hook probe: expected BLOCKED')


@dataclass
class Check:
    id: str
    status: str  # ok | fail | warn | unverified | skipped
    detail: str
    data: dict = field(default_factory=dict)


def _run(cmd: list[str], timeout: int = 60, cwd: Path | None = None, env: dict | None = None) -> tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd, env=env)
        return r.returncode, r.stdout, r.stderr
    except FileNotFoundError:
        return 127, "", f"{cmd[0]}: not found"
    except subprocess.TimeoutExpired:
        return 124, "", f"{' '.join(cmd[:3])}: timed out after {timeout}s"


def _cli_json(cli: str, *args: str, env: dict | None = None, timeout: int = 60):
    """(parsed JSON, redacted output-or-error) of `databricks <args> --output json`; (None, shown) on failure."""
    rc, out, err = _run([cli, *args, "--output", "json"], timeout=timeout, **({"env": env} if env else {}))
    if rc != 0:
        return None, _redact(err or out)
    try:
        return json.loads(out), _redact(out)
    except (TypeError, ValueError):
        return None, _redact(out)


def manifest_sha(manifest_bytes: bytes) -> str:
    return hashlib.sha256(manifest_bytes).hexdigest()[:12]


def wave_signature(body: dict, manifest_bytes: bytes) -> str:
    """HMAC over the canonical record, keyed by manifest bytes + the seen identity (tamper-evident)."""
    ident = body.get("identity") or {}
    key = hashlib.sha256(manifest_bytes + str(ident.get("userName") or "").encode() + str(ident.get("host") or
        "").encode()).digest()
    message = json.dumps({k: v for k, v in body.items() if k != "signature"}, sort_keys=True, separators=(",",
        ":")).encode()
    return hmac.new(key, message, "sha256").hexdigest()


def sign_wave_report(report: dict, manifest_bytes: bytes, signed_at: str | None = None) -> dict:
    body = {k: v for k, v in report.items() if k != "signature"}
    body["manifest_sha"] = manifest_sha(manifest_bytes)
    body["signed_at"] = signed_at or datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    body["signature"] = wave_signature(body, manifest_bytes)
    return body


# Rows a child may take from the orchestrator's signed record: the ones that read the checkout and
# the source, which the child's own run would surface anyway. The record's key is derivable from the
# manifest, so reuse is a policy on cost, not a trust decision.
REUSABLE_ROWS = ("type_map_audit", "delete_evidence", "dictionary_readable")
DOCTOR_MAX_AGE_MINUTES = 15


def inputs_sha(ws: Path) -> str:
    """sha256 over .migration/units/** and top-level .migration/*.json except 09_capabilities.json."""
    mig = ws / ".migration"
    files = sorted({*mig.joinpath("units").rglob("*"), *mig.glob("*.json")} - {mig / "09_capabilities.json"})
    h = hashlib.sha256()
    for f in files:
        if f.is_file():
            h.update(f.relative_to(mig).as_posix().encode() + b"\0" + f.read_bytes() + b"\0")
    return h.hexdigest()


def reusable_record(record, manifest, manifest_bytes: bytes, expect_identity: str | None, expect_host: str | None,
    now: datetime.datetime | None = None, inputs_sha: str | None = None) -> tuple:
    """(record, "") when a child may reuse the orchestrator's signed record, else (None, why)."""
    if not isinstance(record, dict):
        return None, "record is not an object"
    for bad, why in ((record.get("role") != "orchestrator",
        f"record role is {record.get('role')!r}, not the orchestrator's"), (record.get("ready") is not True,
        "record is not ready"), (record.get("manifest_sha") != manifest_sha(manifest_bytes),
        "record was signed for another manifest")):
        if bad:
            return None, why
    try:
        signed = datetime.datetime.fromisoformat(record.get("signed_at") or "")
        recent = signed.tzinfo is not None
    except (TypeError, ValueError):
        return None, f"signed_at {record.get('signed_at')!r} is not an ISO timestamp"
    if not recent:
        return None, "signed_at has no timezone"
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if signed > now:
        return None, "signed_at is in the future"
    max_age = (manifest.get("doctor_max_age", DOCTOR_MAX_AGE_MINUTES) if isinstance(manifest,
        dict) else DOCTOR_MAX_AGE_MINUTES)
    if not isinstance(max_age, int) or isinstance(max_age, bool) or max_age <= 0:
        max_age = DOCTOR_MAX_AGE_MINUTES
    if now - signed > datetime.timedelta(minutes=max_age):
        return None, f"record age exceeds doctor_max_age ({max_age} minutes)"
    if not hmac.compare_digest(str(record.get("signature") or ""), wave_signature(record, manifest_bytes)):
        return None, "signature does not verify"
    ident = record.get("identity") if isinstance(record.get("identity"), dict) else {}
    if not expect_identity or str(ident.get("userName") or "").casefold() != expect_identity.casefold():
        return None, "record identity is not --expect-identity"
    if not expect_host or ident.get("host") != expect_host:
        return None, "record host is not --expect-host"
    if not isinstance(record.get("inputs_sha"), str):
        return None, "record carries no inputs_sha"
    if inputs_sha is not None and record["inputs_sha"] != inputs_sha:
        return (None,
            "workspace inputs differ from the record's (units or .migration/*.json changed since it was signed)")
    return record, ""


_SECRET_ASSIGNMENT = re.compile(r"(?i)\b(password|passwd|pwd|pass|token|access[_-]?token|secret|api[_-]?key|"
    r"client[_-]?secret|private[_-]?key|sas|signature|sig|authorization)\b\s*[:=]\s*(?:bearer\s+|basic\s+)?"
    r"(\"[^\"]*\"|'[^']*'|[^\s;,&]+)")
_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_URL_USERINFO = re.compile(r"(://[^/\s:@]+):([^@\s]+)@")
_TOKEN_SHAPED = re.compile(r"\b(?:dapi|dsapi|ghp_|gho_|xox[abp]-|sk-|AKIA|eyJ)[A-Za-z0-9._-]{8,}")


def _redact(text: str) -> str:
    """Drop anything that looks like a token or secret value from CLI/driver stderr."""
    text = _SECRET_ASSIGNMENT.sub(r"\1=<redacted>", text)
    text = _BEARER.sub(r"\1 <redacted>", text)
    text = _URL_USERINFO.sub(r"\1:<redacted>@", text)
    text = _TOKEN_SHAPED.sub("<redacted>", text)
    return " ".join("<redacted>" if len(tok) > 24 and any(c.isdigit() for c in tok) and "/" not in tok and
        "." not in tok else tok for tok in text.split())[:400]


def check_workspace(ws: Path) -> Check:
    mig = ws / ".migration"
    if not mig.is_dir():
        return Check("workspace", "fail", f"{mig} missing; run 1-migration_setup first")
    missing = [f for f in REQUIRED_FILES if not (mig / f).is_file()]
    if missing:
        return Check("workspace", "fail", f".migration/ incomplete: missing {missing}", {"missing": missing})
    context = (mig / "00_context.md").read_text(errors="replace")
    if not re.search(r"(?m)^##\s+Glossary\b", context) and not (mig / "02_glossary.md").is_file():
        return Check("workspace", "fail",
            "00_context.md has no '## Glossary' section (02_glossary.md was folded into it)", {"missing": [
            "00_context.md#Glossary"]})
    return Check("workspace", "ok", f".migration/ has all {len(REQUIRED_FILES)} required files")


def check_stop_mode(ws: Path) -> Check:
    text = ""
    for name in ("00_context.md", "01_conventions.md"):
        p = ws / ".migration" / name
        if p.is_file():
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
    return Check("allowed_targets", "ok", f"catalogs={cats} (guard module not found; shape check only)", {
        "catalogs": cats})


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
        if len(saved) == 2 and (nonce := fresh(saved[0], saved[1])):
            return nonce
    except OSError:
        pass
    try:
        report = json.loads((ws / CAPABILITIES).read_text())
        row = next(c for c in report.get("checks", []) if c.get("id") == "hook_guard")
        generated_at = report.get("generated_at") or report.get("timestamp")
        if isinstance(generated_at, str):
            generated_at = calendar.timegm(time.strptime(generated_at, "%Y-%m-%dT%H:%M:%SZ"))
        return fresh(row.get("data", {}).get("probe_nonce"), generated_at)
    except (OSError, ValueError, StopIteration, AttributeError, KeyError, TypeError, OverflowError):
        return None


def security_controls(role: str) -> tuple[str, ...]:
    """The sub-check ids that must be `ok` for this role's report to be ready."""
    return CHILD_SECURITY_CONTROLS if role == "child" else SECURITY_CONTROLS


def _merge(cid: str, subs: list[Check], security: tuple[str, ...] = SECURITY_CONTROLS) -> Check:
    """One row from several sub-checks; sub_results keeps each one."""
    data: dict = {}
    for s in subs:
        data.update(s.data or {})
    data["sub_results"] = [asdict(s) for s in subs]
    if any(s.status == "fail" for s in subs):
        status = "fail"
    else:
        sec = [s for s in subs if s.id in security and s.status != "ok"]
        status = max((s.status for s in (sec or subs)), key=_RANK.__getitem__)
    return Check(cid, status, "; ".join(f"{s.id}: {s.detail}" for s in subs), data)


def _flat(checks):
    for row in checks:
        sub_results = (row.data or {}).get("sub_results")
        yield from (Check(**d) for d in sub_results) if sub_results else iter((row,))


def check_hooks(plugin_root: Path, ws: Path, probe_result: str, role: str = "orchestrator",
    reused: dict | None = None) -> list[Check]:
    out: list[Check] = []
    hooks_json, guard = plugin_root / "hooks.json", plugin_root / "hooks" / "dbx_guard.py"
    if not hooks_json.exists() or not guard.exists():
        return [Check("hooks_files", "fail", f"hooks.json / hooks/dbx_guard.py not both present under {plugin_root}")]
    try:
        data = json.loads(hooks_json.read_text())
        pre = data["PreToolUse"][0]["hooks"][0]["command"]
        assert "dbx_guard.py" in pre
        out.append(Check("hooks_files", "ok", "hooks.json registers dbx_guard.py (PreToolUse)"))
    except (KeyError, IndexError, AssertionError, json.JSONDecodeError) as e:
        return [Check("hooks_files", "fail", f"hooks.json malformed: {e!r}")]

    # Functional check: feed the guard the probe event directly; it must block and name the full
    # probe token. One nonce serves this probe and the platform one; a child's is never read/written.
    issued = _issued_nonce(ws) if role != "child" else None
    nonce = issued or secrets.token_hex(4)
    token = HOOK_PROBE_TOKEN.format(nonce=nonce)
    event = json.dumps({"tool_name": "exec", "tool_input": {"command": HOOK_PROBE_COMMAND.format(nonce=nonce)}})
    try:
        r = subprocess.run([sys.executable, str(guard)], input=event, text=True, capture_output=True, timeout=30,
            cwd=ws, env={**os.environ, "CLAUDE_PROJECT_DIR": str(ws)})
        if r.returncode == 2 and '"block"' in r.stdout and token in r.stdout:
            out.append(Check("hook_guard_functional", "ok",
                f"dbx_guard.py blocks the probe command when invoked directly and names {token}"))
        else:
            out.append(Check("hook_guard_functional", "fail",
                f"dbx_guard.py did not block the probe naming {token} (rc={r.returncode}): "
                f"{_redact(r.stderr or r.stdout)}"))
    except subprocess.TimeoutExpired:
        out.append(Check("hook_guard_functional", "fail", "dbx_guard.py timed out on the probe"))

    if role == "child":
        # A child never live-probes the platform hook: it inherits the orchestrator's signed row.
        signed_rows = (reused or {}).get("checks") or []
        row = next((c for c in signed_rows if isinstance(c, dict) and c.get("id") == "hook_guard"), None)
        subs = ((row or {}).get("data") or {}).get("sub_results") or []
        sub = next((s for s in subs if isinstance(s, dict) and s.get("id") == "hook_platform_loaded"), None)
        if sub and isinstance(sub.get("status"), str) and isinstance(sub.get("detail"), str):
            out.append(Check("hook_platform_loaded", sub["status"],
                f"reused from the orchestrator's record signed {reused.get('signed_at')}: {sub['detail']}",
                {**(sub.get("data") or {}), "reused_from": reused.get("signed_at")}))
        else:
            out.append(Check("hook_platform_loaded", "warn",
                "orchestrator-only: the platform hook is proven once per wave by the orchestrator's live probe "
                "and inherited through --reuse-record; a child never runs the live probe"))
        return out

    # Platform check: hooks are fail-open on the platform side, so only a live probe proves loading,
    # and only the nonce this workspace's last report issued proves the probe was the one run.
    if issued and probe_result == f"blocked:{issued}":
        out.append(Check("hook_platform_loaded", "ok", "live probe was BLOCKED: the platform is running hooks.json", {
            "probe_nonce": issued}))
    elif probe_result == "not-blocked":
        out.append(Check("hook_platform_loaded", "fail",
            "live probe ran unblocked: hooks.json is not being applied in this session. Treat as a D10; "
            "do not launch children until fixed (plugin not installed at org level, or hooks disabled)."))
    else:
        if not issued:
            try:
                (ws / HOOK_PROBE_NONCE).write_text(f"{nonce} {int(time.time())}\n")
            except OSError:
                pass
        mismatched = probe_result.startswith("blocked:")
        why = "the nonce did not match the one this workspace's last report issued; " if mismatched else ""
        out.append(Check("hook_platform_loaded", "unverified",
            why + "run the probe command in this session's shell; the guard's block message names "
            "__dbx_guard_probe__<nonce>; re-run the doctor with --hook-probe-result blocked:<nonce> "
            "(or not-blocked if the echo printed)", {"probe_command": HOOK_PROBE_COMMAND.format(nonce=nonce),
            "probe_nonce": nonce}))
    return out


_POLICY_REFS = ("refs/remotes/origin/HEAD", "origin/main", "origin/master", "HEAD")


def _committed(ws: Path, rel: str, refs: tuple[str, ...] = _POLICY_REFS) -> tuple[bytes | None, str | None]:
    """(bytes, ref) of `rel` from the first ref that has it (dbx_guard._committed, byte-precise)."""
    for ref in refs:
        try:
            r = subprocess.run(["git", "-C", str(ws), "show", f"{ref}:{rel}"], capture_output=True, timeout=5, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None, None
        if r.returncode == 0:
            return r.stdout, ref
    return None, None


def check_allowlist_committed(ws: Path) -> Check:
    """The working copy of each contract file must be byte-equal to the copy on the protected
    branch (origin/HEAD, else origin/main|master, else HEAD)."""
    try:
        r = subprocess.run(["git", "-C", str(ws), "rev-parse", "--git-dir"], capture_output=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        return Check("allowlist_committed", "fail", f"git rev-parse failed under {ws}: {_redact(str(e))}")
    if r.returncode != 0:
        return Check("allowlist_committed", "fail",
            f"git cannot read the repository under {ws}: {_redact(r.stderr.decode(errors='replace').strip())}; "
            "the workspace must be the committed repository the wave is planned from")
    _, ref = _committed(ws, LEDGER_CONTRACT_FILES[0])   # the whole contract is pinned to the one ref the allowlist is on
    states: dict[str, str] = {}
    for rel in LEDGER_CONTRACT_FILES:
        committed, _ = _committed(ws, rel, (ref,) if ref else ())
        if not (ws / rel).is_file():
            states[rel] = "missing"
        elif committed is None:
            states[rel] = "untracked"
        else:
            states[rel] = "clean" if (ws / rel).read_bytes() == committed else "modified"
    bad = [f"{rel} {state}" for rel, state in states.items() if state != "clean"]
    if bad:
        shown = "; ".join(bad)
        return Check("allowlist_committed", "fail", "the working copy is not the committed contract: " + shown
            + ". Merge the allowlist PR into the protected branch and `git fetch`, then re-run", states)
    return Check("allowlist_committed", "ok",
        f"allowed_targets.json and 03_recon_tolerances.json are byte-equal to {ref}", states)


def _norm_catalog(name) -> str:
    """The guard's identifier rule (dbx_guard._norm): trimmed, unquoted, case-folded."""
    return str(name).strip().strip("`").lower()


def check_allowlist_matches_contract(ws: Path, expect_catalogs: list[str] | None) -> Check:
    """--expect-catalogs must equal the allowlist's catalogs under the guard's normalization."""
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
            f"allowlist catalogs {cats} differ from the contract's {expected}; "
            "a catalog is added by a PR to the protected branch carrying a `D-<id>` row, then a new doctor run", data)
    return Check("allowlist_matches_contract", "ok", f"allowlist catalogs match the contract: {cats}", data)


# Files in the playbooks dir that are not importable playbooks (the pre-kickoff intake form).
_NOT_PLAYBOOKS = frozenset({"00_intake_template.md"})
PLAYBOOKS_INDEX = "index.json"


def _repo_playbooks(plugin_root: Path) -> dict[str, tuple[str, str]]:
    """macro -> (repo_file, sha256 of the file bytes), from playbooks/index.json."""
    playbooks = plugin_root / "skills" / "install-dbx-factory" / "playbooks"
    macros: dict[str, tuple[str, str]] = {}
    try:
        doc = json.loads((playbooks / PLAYBOOKS_INDEX).read_text())
        rows = doc.get("playbooks", []) if isinstance(doc, dict) else []
    except (OSError, ValueError):
        rows = []
    for row in rows if isinstance(rows, list) else ():
        if not isinstance(row, dict):
            continue
        p = playbooks / str(row.get("file", ""))
        macro = str(row.get("macro", ""))
        if p.is_file() and p.name not in _NOT_PLAYBOOKS and macro.startswith("!"):
            macros[macro] = (p.name, hashlib.sha256(p.read_bytes()).hexdigest())
    return macros


def _norm(s: str) -> str:
    return s.replace("\r\n", "\n").rstrip("\n")


def check_playbooks_in_sync(ws: Path, plugin_root: Path, role: str, live_playbooks: Path | None = None) -> Check:
    """Org-library playbooks proven against the repo copies the wave contract was reviewed from."""
    cid = "playbooks_in_sync"
    lock = ws / PLAYBOOKS_LOCK
    if not lock.is_file():
        if role == "setup":
            return Check(cid, "skipped", f"no {PLAYBOOKS_LOCK} yet; install-dbx-factory writes it "
                "(warning: live playbooks unverified)", {"lock": PLAYBOOKS_LOCK})
        return Check(cid, "warn", f"no {PLAYBOOKS_LOCK}: the playbooks installed in the org library "
            "are unverified; run install-dbx-factory", {"lock": PLAYBOOKS_LOCK})
    try:
        lock_data = json.loads(lock.read_text())
    except (OSError, ValueError) as e:
        return Check(cid, "warn", f"{PLAYBOOKS_LOCK} unreadable: {_redact(str(e))}", {"lock": PLAYBOOKS_LOCK})
    if not isinstance(lock_data, dict):
        return Check(cid, "warn", f"{PLAYBOOKS_LOCK} is not a JSON object", {"lock": PLAYBOOKS_LOCK})
    repo = _repo_playbooks(plugin_root)
    playbooks_dir = plugin_root / "skills" / "install-dbx-factory" / "playbooks"
    data: dict = {"malformed": [], "stale": [], "missing": [], "unknown": [], "unlisted": [], "checked": len(repo),
        "live": None, "duplicate": {}, "live_missing": [], "live_stale": []}
    for macro, (_repo_file, sha) in repo.items():
        entry = lock_data.get(macro)
        if entry is None:
            data["missing"].append(macro)
        elif not isinstance(entry, dict) or not all(isinstance(entry.get(k), str) for k in ("sha256", "repo_file",
            "installed_at")):
            data["malformed"].append(macro)
        elif entry["sha256"] != sha:
            data["stale"].append(macro)
    data["unknown"] = sorted(m for m in lock_data if m not in repo)
    repo_files = {f for f, _sha in repo.values()}
    data["unlisted"] = sorted(p.name for p in playbooks_dir.glob("*.md") if p.name not in _NOT_PLAYBOOKS and
        p.name not in repo_files)
    findings = [f"{k}: {', '.join(data[k])}" for k in ("malformed", "stale", "missing", "unknown") if data[k]]
    if data["unlisted"]:
        findings.append(f"not in playbooks/{PLAYBOOKS_INDEX}: {', '.join(data['unlisted'])}")
    live = live_playbooks or ws / LIVE_PLAYBOOKS
    age_min = None
    if not live.is_file():
        if role == "orchestrator" or live_playbooks is not None:
            findings.append(f"no {live if live_playbooks else LIVE_PLAYBOOKS}: export the live library with "
                "devin_playbook_manage right before the doctor (see 9-orchestrator)")
    else:
        age = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(live.stat().st_mtime,
            datetime.timezone.utc)
        age_min = int(age.total_seconds() // 60)
        if age > LIVE_PLAYBOOKS_MAX_AGE:
            findings.append(f"stale export ({age_min} min old, max 15): re-export")
        else:
            try:
                records = json.loads(live.read_text())
            except (OSError, ValueError):
                records = None
            bad = (next((i for i, r in enumerate(records) if not isinstance(r, dict) or not isinstance(r.get("macro"),
                str) or not isinstance(r.get("content"), str)), -1) if isinstance(records, list) else -2)
            if records is None or bad != -1:
                findings.append("live export malformed" + (f" (record {bad})" if bad >= 0 else ""))
            else:
                grouped: dict[str, list[dict]] = {}
                for r in records:
                    grouped.setdefault(r["macro"], []).append(r)
                data["duplicate"] = {
                    m: [r.get("playbook_id") for r in rs] for m, rs in grouped.items() if len(rs) > 1}
                for m, ids in data["duplicate"].items():
                    findings.append(f"duplicate: {m} ({', '.join(str(i) for i in ids)})")
                for macro, (f, _sha) in repo.items():
                    rs = grouped.get(macro)
                    if not rs:
                        data["live_missing"].append(macro)
                    elif _norm(rs[0]["content"]) != _norm((playbooks_dir / f).read_text()):
                        data["live_stale"].append(macro)
                for key in ("live_stale", "live_missing"):
                    if data[key]:
                        findings.append(f"{key.replace('_', ' ')}: {', '.join(data[key])}")
                data["live"] = {"checked": len(repo), "age_minutes": age_min}
    if findings:
        detail = "; ".join(findings) + ("; duplicates: archive the extra" if data["duplicate"] else "")
        return Check(cid, "warn", detail + " — re-run install-dbx-factory", data)
    installed = [e["installed_at"] for e in lock_data.values() if isinstance(e, dict) and
        isinstance(e.get("installed_at"), str)]
    installed_at = max(installed) if installed else "unknown"
    detail = f"{len(repo)} playbooks match the lock written at the last install-dbx-factory sync ({installed_at})"
    if data["live"]:
        detail += f" and the live export ({age_min} min old)"
    return Check(cid, "ok", detail, {**data, "checked": len(repo),
        "installed_at": installed_at if installed else None})


def check_official_plugin(plugin_root: Path) -> Check:
    roots = [Path(v).parent for env in ("CLAUDE_PLUGIN_ROOT", "DEVIN_PLUGINS_DIR") if (v := os.environ.get(env))]
    roots += [Path("/opt/.devin/plugins/cache"), Path.home() / ".devin" / "plugins", plugin_root.parent]
    found: dict[str, str] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for skill in OFFICIAL_SKILLS:
            if skill not in found and (hit := next(root.glob(f"**/skills/{skill}/SKILL.md"), None)):
                found[skill] = str(hit.parent)
    missing = [s for s in OFFICIAL_SKILLS if s not in found]
    if not found:
        return Check("official_databricks_plugin", "unverified",
            "official databricks-agent-skills not found on disk under known plugin roots; it is declared in "
            "requiredPlugins and is loaded by the platform, so this is only a local visibility gap", {"searched": [
            str (r) for r in roots]})
    if missing:
        return Check("official_databricks_plugin", "warn", f"official plugin present but missing skills {missing}", {
            "found": found})
    return Check("official_databricks_plugin", "ok", f"all {len(OFFICIAL_SKILLS)} routed official skills present", {
        "found": found})


def _harness_command(plugin_root: Path) -> tuple[list[str], Path | None, str] | None:
    """The harness this doctor grades: the installed dbx-recon first, else the checkout's module."""
    harness = plugin_root / "skills" / "data-reconciliation" / "harness"
    if shutil.which("dbx-recon"):
        return ["dbx-recon"], None, "dbx-recon"
    if (harness / "recon" / "cli.py").exists():
        return [sys.executable, "-m", "recon.cli"], harness, f"python -m recon.cli (cwd {harness})"
    return None


def _harness_run(cmd, subcommand: list[str]) -> tuple[int, str, str]:
    argv, cwd, _how = cmd
    return _run(argv + subcommand, **({"cwd": cwd} if cwd else {}))


def _harness_json(cmd, subcommand: list[str]):
    """(parsed JSON or None, rc, redacted err-or-out) for one harness subcommand call."""
    rc, out, err = _harness_run(cmd, subcommand)
    try:
        return (json.loads(out) if rc == 0 else None), rc, _redact(err or out)
    except ValueError:
        return None, rc, _redact(err or out)


def check_harness(plugin_root: Path) -> Check:
    cmd = _harness_command(plugin_root)
    if cmd is None:
        return Check("recon_harness", "fail",
            f"dbx-recon not on PATH and harness not at {plugin_root}/skills/data-reconciliation/harness")
    rc, out, err = _harness_run(cmd, ["selftest"])
    if rc == 0 and "PASS" in out:
        return Check("recon_harness", "ok", f"{out.strip()} via {cmd[2]}")
    return Check("recon_harness", "fail", f"selftest rc={rc}: {_redact(err or out)}")


def _module_present(dotted: str) -> bool:
    try:
        return importlib.util.find_spec(dotted) is not None
    except (ModuleNotFoundError, ValueError):
        return False


def check_drivers() -> Check:
    present = {k: _module_present(v) for k, v in DRIVERS.items()}
    names = sorted(k for k, v in present.items() if v) or "none"
    note = "" if present["databricks"] else "; databricks-sql-connector missing, live/snapshot recon cannot run"
    detail = f"installed adapters: {names}{note}"
    return Check("recon_drivers", "ok" if present["databricks"] else "warn", detail, {"drivers": present})


def check_recon_family_supported(plugin_root: Path, source_family: str | None) -> Check:
    """Whether the harness can reconcile the declared family, asked of the same harness `recon_harness` ran."""
    cid = "recon_family_supported"
    if not source_family:
        return Check(cid, "skipped",
            "no source family declared (--source-family, or source.family in the wave manifest)")
    cmd = _harness_command(plugin_root)
    if cmd is None:
        return Check(cid, "fail", "cannot ask the harness which families it supports: dbx-recon "
            "not on PATH and no checkout harness", {"family": source_family})
    argv, cwd, how = cmd
    reg, rc, shown = _harness_json(cmd, ["families"])
    if (not isinstance(reg, dict) or not isinstance(reg.get("live_tested"), list) or
            not isinstance(reg.get("untested"), list) or
            not all(isinstance(f, str) for f in reg["live_tested"] + reg["untested"])):
        return Check(cid, "fail", f"cannot ask the harness which families it supports "
            f"({how} families rc={rc}): {shown}", {"family": source_family})
    live = sorted(reg["live_tested"])
    data = {"family": source_family, "live_tested": live, "harness": how}
    if source_family in reg["untested"]:
        return Check(cid, "fail", f"{source_family}: the harness refuses this family (`dbx-recon run --family "
            f"{source_family}` exits before connecting). Attestation says the principal is read-only; this "
            f"row says whether we can reconcile the family, and today we cannot: live-tested families are "
            f"{live}; adding one is a live-tested adapter, never an attestation ({how})", data)
    if source_family not in live:
        return Check(cid, "fail",
            f"{source_family}: no source adapter in the harness; live-tested families: {live} ({how})", data)
    return Check(cid, "ok", f"{source_family}: live-tested source adapter ({how})", data)


UNIT_MAPPINGS = ".migration/units/*/mapping_spec.json"


def resolve_mappings(ws: Path, role: str, units: list[str], mappings: list[Path]) -> tuple[dict[str, Path], dict[str,
    Path], tuple | None]:
    """(expected unit -> mapping, every spec to check, problem): an empty `todo` is setup."""
    unit_dir = ws / ".migration" / "units"
    if role == "child":
        if not units:
            return ({}, {}, ("fail", "a child preflight covers every unit in its batch: pass --unit <id> for each "
                "unit in the brief, --source-secret NAME and the --param values the recon gate will get", {"units": [
                ], "mappings": {}}))
        expected = {u: unit_dir / u / "mapping_spec.json" for u in dict.fromkeys(units)}
    else:
        if units:
            return ({}, {}, ("fail",
                "--unit narrows nothing for an orchestrator: it verifies every unit mapping under "
                f"{UNIT_MAPPINGS}; --unit is for --role child", {"units": list(units), "mappings": {}}))
        expected = {p.parent.name: p for p in sorted(ws.glob(UNIT_MAPPINGS))}
    missing = [u for u, p in expected.items() if not p.is_file()]
    if missing:
        return (expected, {}, ("fail", f"unit mapping(s) missing for {', '.join(missing)}: expected "
            f"{unit_dir.relative_to(ws)}/<id>/mapping_spec.json (hand-off incomplete; report BLOCKED)", {
            "units": list (expected), "missing_units": missing, "mappings": {}}))
    todo = dict(expected)
    seen = {p.resolve() for p in expected.values()}
    for m in mappings:
        if m.resolve() not in seen:
            seen.add(m.resolve())
            todo[str(m)] = m
    return expected, todo, None


def per_unit(cid: str, ws: Path, role: str, units: list[str], mappings: list[Path], fn,
    setup: Check | None = None) -> Check:
    """resolve_mappings for row `cid`: the problem/setup row when applicable, else fn(expected, todo)."""
    expected, todo, problem = resolve_mappings(ws, role, units, mappings)
    if problem:
        status, detail, data = problem
        return Check(cid, status, detail, {**data, "units_problem": True})
    if not todo:
        return setup or Check(cid, "skipped",
            f"not applicable at setup: no unit mapping exists yet under {UNIT_MAPPINGS}")
    return fn(expected, todo)


def check_type_map_audit(ws: Path, role: str, units: list[str], mappings: list[Path], source_family: str | None,
    plugin_root: Path, params: dict[str, str] | None = None, target_kind: str = "databricks") -> Check:
    """Committed specs' declared target types against the dialect skill's type_map (see references/checks.md)."""
    cid = "type_map_audit"

    def audit(_expected, todo):
        if not source_family:
            return Check(cid, "unverified", "pass --source-family (or run with --wave): target types "
                "cannot be audited without the source family")
        cmd = _harness_command(plugin_root)
        if cmd is None:
            return Check(cid, "fail", "cannot audit target types: dbx-recon not on PATH and no " "checkout harness", {
                "family": source_family, "target_kind": target_kind})
        argv, cwd, how = cmd
        cargs = [a for c in sorted(plugin_root.glob("skills/*/canonicalization.json")) for a in ("--canonicalization",
            str(c))]
        pargs = [a for k, v in (params or {}).items() for a in ("--param", f"{k}={v}")]
        data = {"family": source_family, "target_kind": target_kind, "harness": how}
        contradictions, unmapped, undeclared, fields = [], [], 0, 0
        family_known = target_known = False
        rel = None
        for _, p_ in sorted(todo.items(), key=lambda kv: str(kv[1])):
            _, spec_err = _load_mapping(p_, params, plugin_root)
            if spec_err:
                return Check(cid, "fail", f"{p_}: {spec_err}", {**data, "units_problem": True})
            reg, rc, shown = _harness_json(cmd, ["type-map-audit", "--spec", str(p_), "--family", source_family,
                "--target-kind", target_kind, *cargs, *pargs])
            if (not isinstance(reg, dict) or not isinstance(reg.get("findings"), list) or
                    not all(isinstance(f.get("verdict"), str) for f in reg["findings"])):
                return Check(cid, "fail", f"cannot audit target types ({how} type-map-audit rc={rc}): {shown}", data)
            if reg.get("error"):
                return Check(cid, "fail", f"{p_}: {_redact(str(reg['error']))} ({how})", data)
            family_known |= bool(reg.get("family_known"))
            target_known |= bool(reg.get("target_known"))
            if reg.get("map"):
                mp = Path(reg["map"])
                rel = str(mp.relative_to(plugin_root)) if mp.is_relative_to(plugin_root) else str(mp)
            for row in reg["findings"]:
                fields += 1
                if row["verdict"] in ("contradiction", "unrepresentable"):
                    contradictions.append(row)
                elif row["verdict"] == "unmapped":
                    unmapped.append(row)
                elif row["verdict"] == "undeclared":
                    undeclared += 1
        if not target_known:
            if family_known:
                return Check(cid, "warn", f"no {source_family}->{target_kind} type map in any "
                    f"skills/*/canonicalization.json ({how}); the spec's target types are unaudited", {**data,
                    "family_known": True})
            return Check(cid, "warn", f"no type map for {source_family} in any skills/*/canonicalization.json; "
                "the spec's target types are unaudited (adding a family is JSON)", {**data, "family_known": False})
        data.update({"map": rel, "fields": fields, "unmapped": [r["field"] for r in unmapped],
            "undeclared": undeclared})
        if contradictions:
            shown = "; ".join(f"{r['field']} {r['source_type']} -> declared {r['target_type']}, "
                f"map says {r['detail']}" for r in contradictions[:5])
            more = f" …and {len(contradictions) - 5} more" if len(contradictions) > 5 else ""
            return Check(cid, "fail", f"{len(contradictions)} field(s) declare a target type the "
                f"{source_family} type map forbids: {shown}{more} ({how})", {**data,
                "contradictions": contradictions})
        return Check(cid, "ok", f"{fields} typed fields agree with {rel}; {len(unmapped)} unmapped "
            f"source types recorded, {undeclared} undeclared targets the harness fills at run time ({how})", data)

    return per_unit(cid, ws, role, units, mappings, audit)


_CDC_QUERIES = {
    "is_cdc_enabled": "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()",
    # Lists the capture instances *this identity may read* (db_owner, the capture's gating role,
    # or SELECT on its captured columns); needs no SELECT on the cdc schema.
    "captures": "EXEC sys.sp_cdc_help_change_data_capture",
    "max_lsn": "SELECT sys.fn_cdc_get_max_lsn()",
    # Bounded, read-only call of the generated function exactly as the harness will make it.
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


def _env_dsn(cid: str, source_secret: str | None) -> str | Check:
    """The DSN env var behind --source-secret, or the fail Check when it is unset."""
    dsn = source_secret and os.environ.get(source_secret)
    return dsn if dsn else Check(cid, "fail", f"source secret {source_secret} is not set in the environment")


def check_delete_evidence_all(ws: Path, role: str, units: list[str], mappings: list[Path], source_secret: str | None,
    plugin_root: Path, connect=_pyodbc_connect, params: dict[str, str] | None = None) -> Check:
    """One row over every unit mapping this run is answerable for (resolve_mappings)."""

    def evidence(expected, todo):
        rows = {label: check_delete_evidence(p, source_secret, plugin_root, connect=connect, params=params) for label,
            p in todo.items()}
        worst = "fail" if any(c.status == "fail" for c in rows.values()) else "ok"
        data = {"units": list(expected), "mappings": {label: {"status": c.status, "detail": c.detail,
            **(c.data or {})} for label, c in rows.items()}}
        if any((c.data or {}).get("units_problem") for c in rows.values()):
            data["units_problem"] = True
        return Check("delete_evidence", worst, "; ".join(f"{label}: {c.detail}" for label, c in rows.items()),
            data)

    return per_unit("delete_evidence", ws, role, units, mappings, evidence, setup=Check("delete_evidence", "skipped",
        f"not applicable at setup: no unit mapping exists yet under {UNIT_MAPPINGS}", {"units": [], "mappings": {}}),)


def check_delete_evidence(mapping: Path, source_secret: str | None, plugin_root: Path, connect=_pyodbc_connect,
    params: dict[str, str] | None = None) -> Check:
    """Every declared `delete_evidence` block must be answerable on the source (references/checks.md)."""
    spec, err = _load_mapping(mapping, params, plugin_root)
    if spec is None:
        return Check("delete_evidence", "fail", f"{mapping}: {err}", {"units_problem": True})
    declared = [(c.object, c.delete_evidence) for c in spec.objects if c.delete_evidence is not None]
    if not declared:
        return Check("delete_evidence", "ok",
            "no object declares delete_evidence (drain-before-run contract applies)")
    kinds = sorted({de.kind for _, de in declared})
    if kinds != ["sqlserver_cdc"]:
        return Check("delete_evidence", "fail", f"unsupported delete_evidence kind(s) {kinds}")
    if not source_secret:
        return Check("delete_evidence", "fail",
            f"{len(declared)} object(s) declare delete_evidence; pass --source-secret NAME "
            "(env var holding the read-only source DSN) to verify CDC on the source")
    dsn = _env_dsn("delete_evidence", source_secret)
    if isinstance(dsn, Check):
        return dsn
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
            rows = cur.fetchall()
            visible = {str(r[cap_i]).casefold(): [c.casefold() for c in _captured_columns(r[cols_i])] for r in rows}
            data["missing"] = [c for c in wanted if c.casefold() not in visible]
            if data["missing"]:
                return Check(*fail, "declared capture instance(s) not present or not readable by this identity "
                    "(db_owner, the capture's gating role, or SELECT on its captured columns): " f"{data['missing']}",
                    data)
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
        f"sqlserver_cdc: {len(wanted)} capture instance(s) readable by this identity, "
        "key columns captured, scoped read probed", data)


# Per family: the queries answering whether the principal can write (roles / exists / per-table
# privileges / column writes / `indirect` rows) plus a read-only probe; absent families are `unverified`.
_SRV_ROLES = ("sysadmin", "securityadmin", "serveradmin", "dbcreator", "bulkadmin")
_DB_ROLES = ("db_owner", "db_ddladmin", "db_datawriter", "db_securityadmin")
_SRV_PERMS = ("CONTROL SERVER", "ALTER ANY DATABASE", "IMPERSONATE ANY LOGIN", "ALTER ANY LOGIN")
_PG_ATTRS = ("rolsuper", "rolcreaterole", "rolcreatedb")
_PG_ROLES = ("pg_write_server_files", "pg_execute_server_program")
_ROLE_FLAGS = {"sqlserver": _SRV_ROLES + _DB_ROLES + _SRV_PERMS, "postgres": _PG_ATTRS + _PG_ROLES}
_PG_FUNCTIONS = ("SELECT n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', "
    "'EXECUTE' FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ANY(%s) "
    "AND (p.prosecdef OR p.proacl IS NOT NULL) AND has_function_privilege({who}p.oid, 'EXECUTE') ORDER BY 1")
_TABLE_PRIVILEGES = {"sqlserver": ("INSERT", "UPDATE", "DELETE", "ALTER"), "postgres": ("INSERT", "UPDATE", "DELETE",
    "TRUNCATE", "CREATE on schema")}
_PG_ROLE_COLS = ", ".join([*_PG_ATTRS, *(f"pg_has_role(current_user, '{r}', 'MEMBER')" for r in _PG_ROLES)])
_PG_TABLE_COLS = ", ".join(f"has_table_privilege(%s, '{p}')" for p in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"))
_SQLSRV_TABLE_COLS = ", ".join(f"HAS_PERMS_BY_NAME(?, 'OBJECT', '{p}')" for p in _TABLE_PRIVILEGES["sqlserver"])
_PRIVILEGE_QUERIES = {
    "sqlserver": {
        "roles": "SELECT " + ", ".join([*(f"IS_SRVROLEMEMBER('{r}')" for r in _SRV_ROLES),
            *(f"IS_MEMBER('{r}')" for r in _DB_ROLES),
            *(f"HAS_PERMS_BY_NAME(NULL, NULL, '{p}')" for p in _SRV_PERMS)]),
        "exists": "SELECT OBJECT_ID(?)",
        "table": f"SELECT {_SQLSRV_TABLE_COLS}",
        "columns": ("SELECT QUOTENAME(subentity_name), permission_name FROM fn_my_permissions(?, 'OBJECT') "
            "WHERE subentity_name <> '' AND permission_name = 'UPDATE' ORDER BY 1"),
        "indirect": (
            "SELECT 'LOGIN ' + name, 'IMPERSONATE' FROM sys.server_principals WHERE type IN ('S', 'U', 'C', 'K') "
            "AND name <> SUSER_SNAME() AND HAS_PERMS_BY_NAME(name, 'LOGIN', 'IMPERSONATE') = 1",
            "SELECT 'USER ' + name, 'IMPERSONATE' FROM sys.database_principals "
            "WHERE type IN ('S', 'U', 'C', 'K', 'E', 'X') "
            "AND name <> USER_NAME() AND HAS_PERMS_BY_NAME(name, 'USER', 'IMPERSONATE') = 1",
            "SELECT QUOTENAME(s.name) + '.' + QUOTENAME(o.name), 'EXECUTE' FROM sys.objects o "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE o.type IN ('P', 'PC', 'X') "
            "AND o.is_ms_shipped = 0 "
            "AND HAS_PERMS_BY_NAME(QUOTENAME(s.name) + '.' + QUOTENAME(o.name), 'OBJECT', 'EXECUTE') = 1 "
            "ORDER BY 1"),
        "read_only": None},
    "postgres": {
        "roles": f"SELECT {_PG_ROLE_COLS} FROM pg_roles WHERE rolname = current_user",
        "exists": "SELECT to_regclass(%s)",
        "table": f"SELECT {_PG_TABLE_COLS}, has_schema_privilege(%s, 'CREATE')",
        "columns": ("SELECT a.attname, p FROM pg_attribute a CROSS JOIN unnest(ARRAY['INSERT', 'UPDATE']) AS p "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
            "AND has_column_privilege(a.attrelid, a.attnum, p) "
            "AND NOT has_table_privilege(a.attrelid, p) ORDER BY 1, 2"),
        "functions": _PG_FUNCTIONS.format(who=""),
        "as_role_functions": _PG_FUNCTIONS.format(who="%s, "),
        "members": "SELECT rolname FROM pg_roles WHERE rolname <> current_user "
            "AND pg_has_role(current_user, oid, "
            "CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'USAGE, SET' ELSE 'MEMBER' END) "
            "ORDER BY 1",
        "as_role": "SELECT has_table_privilege(%s, %s, 'INSERT,UPDATE,DELETE,TRUNCATE'), "
            "has_schema_privilege(%s, %s, 'CREATE')",
        "read_only": "SELECT current_setting('transaction_read_only')"}}


def _databricks_bearer() -> str:
    """Workspace access token for the session's service principal (env-oidc or oauth-m2m), via the SDK."""
    import databricks.sdk.core as _sdk_core  # optional extra (databricks-sdk)
    if os.environ.get("DATABRICKS_AUTH_TYPE") == "env-oidc" and not os.environ.get("DATABRICKS_OIDC_TOKEN"):
        audience = os.environ.get("DATABRICKS_DEVIN_AUDIENCE", "databricks")
        os.environ["DATABRICKS_OIDC_TOKEN"] = subprocess.run(["devin-oidc", "token", "--audience", audience],
            check=True, capture_output=True, text=True).stdout.strip()
    headers = _sdk_core.Config().authenticate()
    return headers["Authorization"].split(" ", 1)[1]


def _databricks_session_connect(http_path: str | None = None):
    """Session identity only: DATABRICKS_HOST + DATABRICKS_CLIENT_ID with DATABRICKS_CLIENT_SECRET
    (oauth-m2m) or DATABRICKS_AUTH_TYPE=env-oidc; the warehouse path from DATABRICKS_HTTP_PATH."""
    try:
        from databricks import sql  # optional extra (databricks-sql-connector)
    except ImportError:
        raise RuntimeError("databricks-sql-connector is not installed") from None
    host = os.environ.get("DATABRICKS_HOST", "").removeprefix("https://").removeprefix("http://").rstrip("/")
    http_path = http_path or os.environ.get("DATABRICKS_HTTP_PATH")
    if not http_path:
        raise RuntimeError("DATABRICKS_HTTP_PATH is not set (SQL warehouse HTTP path)")
    if os.environ.get("DATABRICKS_CLIENT_SECRET"):
        return sql.connect(server_hostname=host, http_path=http_path,
            oauth_client_id=os.environ["DATABRICKS_CLIENT_ID"],
            oauth_client_secret=os.environ["DATABRICKS_CLIENT_SECRET"])
    return sql.connect(server_hostname=host, http_path=http_path, access_token=_databricks_bearer())


def _databricks_sql_connect(_dsn: str):
    """The _READ_ONLY_CONNECT slot for a Databricks-family source: the session's own identity."""
    return _databricks_session_connect()


_READ_ONLY_CONNECT = {"sqlserver": _pyodbc_connect, "postgres": _psycopg_connect,
    "databricks": _databricks_sql_connect}
_ADVISORY = ("driver-level read-only (SQL Server readonly=True, Postgres default_transaction_read_only) is advisory, "
    "a hint the server may ignore; only the principal's grants stop writes")


def _schema(table: str) -> str:
    return table.rsplit(".", 1)[0] if "." in table else "public"


def _indirect_writes(cur, q: dict, family: str, tables: list[str], resolved: list[str]) -> list[str]:
    """`object: privilege` for every write path that is not a grant on an in-scope table."""
    found: list[str] = []
    for sql in q.get("indirect", ()):
        found += [f"{obj}: {priv}" for obj, priv in cur.execute(sql).fetchall()]
    if family == "postgres":
        schemas = list(dict.fromkeys(_schema(t) for t in tables))
        found += [f"{obj}: {priv}" for obj, priv in cur.execute(q["functions"], (schemas,)).fetchall()]
        for (role,) in cur.execute(q["members"]).fetchall():  # what SET ROLE <role> would unlock
            for t in resolved:
                write, create = cur.execute(q["as_role"], (role, t, role, _schema(t))).fetchall()[0]
                found += [f"SET ROLE {role}: {t} {w}" for w, held in (("write", write), ("CREATE on schema",
                    create)) if held]
            found += [f"SET ROLE {role}: {obj} {priv}" for obj, priv in cur.execute(q["as_role_functions"], (schemas,
                role)).fetchall()]
    return found


def check_source_principal(tables: list[str], family: str, source_secret: str | None, connect=None) -> Check:
    """The principal behind --source-secret must not be able to write any in-scope source object."""
    cid = "source_principal_read_only"
    if family == "databricks":
        return _check_databricks_source_principal(tables)
    q = _PRIVILEGE_QUERIES.get(family)
    if q is None:
        return Check(cid, "unverified", f"{family}: no privilege query implemented for this family, so the "
            "source principal's write privileges are unknown; confirm SELECT-only grants by hand and "
            "record the decision in .migration/06_decisions.md, then pass --source-attested D-<id>", {
            "family": family, "tables": tables})
    if not source_secret:
        return Check(cid, "fail", f"{family} source with {len(tables)} in-scope table(s); pass --source-secret NAME "
            "(env var holding the source DSN) so the principal's write privileges can be checked")
    dsn = _env_dsn(cid, source_secret)
    if isinstance(dsn, Check):
        return dsn
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
                row = cur.execute(q["table"], (t,) * 4 + ((_schema(t),) if family == "postgres" else ())).fetchall()[
                    0]
                held = [p for p, v in zip(_TABLE_PRIVILEGES[family], row) if v]
                held += [f"{p} on column {c}" for c, p in cur.execute(q["columns"], (t,)).fetchall() if p not in held]
                if held:
                    data["writable"][t] = held
            data["indirect"] = _indirect_writes(cur, q, family, tables, resolved)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - any driver failure is a finding, never a traceback with a DSN in it
        return Check(cid, "fail", f"source query failed: {_redact(str(e))}", data)
    can_write = [f"role {r}" for r in data["roles"]]
    can_write += [f"{t}: {', '.join(p)}" for t, p in data["writable"].items()] + data["indirect"]
    if can_write:
        shown = "; ".join(can_write[:6]) + (f"; +{len(can_write) - 6} more in data" if len(can_write) > 6 else "")
        return Check(cid, "fail", f"{family}: the source principal can write in scope ({shown}); the "
            f"factory needs a SELECT-only principal, and {_ADVISORY}", data)
    if data["unresolved"]:
        return Check(cid, "unverified",
            f"{family}: privileges could not be evaluated for {data['unresolved']} (object "
            "not found or not visible to this principal); nothing is proven about them", data)
    return Check(cid, "ok",
        f"{family}: no admin role, no write privilege on {len(tables)} in-scope table(s) or their "
        f"columns, no IMPERSONATE/EXECUTE/SET ROLE path to one; {data['stats']}", data)


# Effective-grant privileges that only read; anything else is a write path and the check fails closed.
_DBX_READ_PRIVILEGES = frozenset({"SELECT", "USE_CATALOG", "USE_SCHEMA", "BROWSE", "READ_VOLUME"})
_DBX_GET_COMMAND = {"catalog": "catalogs", "schema": "schemas", "table": "tables"}


def _uc_privileges(cli: str, kind: str, name: str, principal: str, env: dict | None = None, strict: bool = False):
    """(privilege set, shown) of `grants get-effective`; (None, shown) when the grants are unreadable."""
    payload, shown = _cli_json(cli, "grants", "get-effective", kind, name, "--principal", principal, env=env)
    if payload is None:
        return None, shown
    privileges = _effective_privileges_strict(payload) if strict else _effective_privileges(payload)
    return (None, "returned no privilege_assignments") if privileges is None else (privileges, shown)


def _check_databricks_source_principal(tables: list[str]) -> Check:
    """`grants get-effective` plus ownership for the session's service principal on every in-scope
    securable; a Databricks-family source is read through the same identity, so its grants are what
    is checked."""
    cid = "source_principal_read_only"
    data: dict = {"family": "databricks", "tables": tables, "writable": {}, "unresolved": [],
        "identity": "session"}
    cli = shutil.which("databricks")
    if not cli:
        return Check(cid, "unverified", "databricks: databricks CLI not on PATH, so the source "
            "principal's grants could not be read", data)
    who, shown = _cli_json(cli, "current-user", "me")
    who = who if isinstance(who, dict) else {}
    principal = who.get("applicationId") or who.get("userName")
    groups = {g["display"] for g in who.get("groups", []) if isinstance(g, dict) and isinstance(g.get("display"),
        str)}
    if not isinstance(principal, str) or not principal:
        return Check(cid, "unverified", f"databricks: current-user me failed: {shown}", data)
    data["principal"] = principal
    securables: dict[tuple[str, str], None] = {}
    for t in tables:
        parts = t.split(".")
        if len(parts) != 3 or not all(parts):
            data["unresolved"].append(t)
            continue
        for kind, name in (("catalog", parts[0]), ("schema", f"{parts[0]}.{parts[1]}"), ("table", t)):
            securables.setdefault((kind, name))
    for kind, name in securables:
        payload, shown = _cli_json(cli, _DBX_GET_COMMAND[kind], "get", name)
        owner = payload.get("owner") if isinstance(payload, dict) else None
        if not isinstance(owner, str):
            return Check(cid, "unverified", f"databricks: {_DBX_GET_COMMAND[kind]} get {name} failed: {shown}", data)
        if owner.lower() == principal.lower() or owner in groups:
            data["writable"].setdefault(name, []).append("OWNER")
        privileges, shown = _uc_privileges(cli, kind, name, principal, strict=True)
        if privileges is None:
            return Check(cid, "unverified", f"databricks: grants get-effective {kind} {name} failed: {shown}", data)
        if offending := sorted(privileges - _DBX_READ_PRIVILEGES):
            data["writable"][name] = data["writable"].get(name, []) + offending
    if data["writable"]:
        shown = "; ".join(f"{n}: {', '.join(p_)}" for n, p_ in list(data["writable"].items())[:6])
        shown += f"; +{len(data['writable']) - 6} more in data" if len(data["writable"]) > 6 else ""
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
    """--source-attested D-<id>: a ledger decision standing in for a privilege query the family lacks."""
    cid = "source_principal_read_only"
    if family == "databricks" or family in _PRIVILEGE_QUERIES:
        return Check(cid, "fail", f"{family}: --source-attested {decision} rejected, this family has a "
            "privilege query: run the query instead (drop --source-attested)", {"family": family,
            "decision": decision})
    ledger = ws / ".migration" / "06_decisions.md"
    if not ledger.is_file():
        return Check(cid, "fail", f"{family}: ledger .migration/06_decisions.md not found", {"decision": decision})
    named = re.compile(rf"(?<![\w-]){re.escape(decision)}(?![\w-])")
    for line in ledger.read_text().splitlines():
        if named.search(line) and "source_principal_read_only" in line and "attested" in line:
            who = _USER_PROVENANCE.search(line)
            if not who:
                return Check(cid, "fail", f"{family}: decision {decision} attests source_principal_read_only "
                    "without user:<id> provenance; a default-accepted row cannot attest the source "
                    "is read-only, a human has to reply", {"decision": decision})
            return Check(cid, "ok", f"{family}: source principal read-only attested by decision "
                f"{decision} ({who.group(0)}) in .migration/06_decisions.md (no principal to query)", {
                "decision": decision, "attested": decision, "family": family, "tables": tables,
                "provenance": who.group(0)})
    return Check(cid, "fail", f"{family}: decision {decision} is not in .migration/06_decisions.md with "
        "'source_principal_read_only', 'attested' and user:<id> provenance in its line; record the "
        "attestation in the ledger first", {"decision": decision})


def _load_mapping(path: Path, params: dict[str, str] | None, plugin_root: Path):
    """(spec, None) or (None, redacted error): the one place a resolved mapping file is parsed."""
    sys.path.insert(0, str(plugin_root / "skills" / "data-reconciliation" / "harness"))
    from recon.config import ConfigError, load_mapping_spec

    try:
        return load_mapping_spec(path, params), None
    except (ConfigError, OSError, ValueError) as e:
        return None, _redact(str(e))


def _mapped_tables(todo: dict[str, Path], params: dict[str, str] | None, plugin_root: Path,
    source_family: str | None):
    """(tables, family) over every mapping in `todo`, or a Check (id set by the caller) when a spec does not load."""
    tables: dict[str, None] = {}
    kinds: set[str] = set()
    for p in todo.values():
        spec, err = _load_mapping(p, params, plugin_root)
        if spec is None:
            return Check("", "fail", f"{p}: {err}", {"units_problem": True})
        for o in spec.objects:
            tables.update(dict.fromkeys([o.root_table, *(e.child_table for e in o.embeds)]))
            if o.delete_evidence is not None:
                kinds.add(o.delete_evidence.kind)
    return list(tables), source_family or ("sqlserver" if kinds == {"sqlserver_cdc"} else None)


def _from_mappings(cid, todo, params, plugin_root, source_family, unknown_detail, fn):
    """Shared prelude of the *_all per-unit checks: _mapped_tables, the no-family row, else fn(tables, family)."""
    got = _mapped_tables(todo, params, plugin_root, source_family)
    if isinstance(got, Check):
        return Check(cid, got.status, got.detail, got.data)
    tables, family = got
    if not family:
        return Check(cid, "unverified", f"source family not declared: pass --source-family "
            f"{'|'.join(SOURCE_FAMILIES)} " + unknown_detail, {"tables": tables})
    return fn(tables, family)


def check_source_principal_all(ws: Path, role: str, units: list[str], mappings: list[Path], source_secret: str | None,
    source_family: str | None, plugin_root: Path, params: dict[str, str] | None = None, attested: str | None = None,
    ) -> Check:
    """Every source table the resolved mappings read, against --source-family or the mappings' implied family."""
    cid = "source_principal_read_only"

    def principal(_expected, todo):
        return _from_mappings(cid, todo, params, plugin_root, source_family,
            "so the principal's privileges can be checked", lambda tables, family: (_attested(ws, attested, family,
            tables) if attested else check_source_principal(tables, family, source_secret)))

    return per_unit(cid, ws, role, units, mappings, principal)


# The structural tier reads these catalog objects; `views` probes readability, `trigger_census`
# cross-checks declared triggers against listed ones per in-scope table.
_TRIGGER_CENSUS = {"sqlserver": ("SELECT CASE WHEN OBJECTPROPERTY(OBJECT_ID(?), 'TableHasInsertTrigger') = 1 "
    "OR OBJECTPROPERTY(OBJECT_ID(?), 'TableHasUpdateTrigger') = 1 "
    "OR OBJECTPROPERTY(OBJECT_ID(?), 'TableHasDeleteTrigger') = 1 THEN 1 ELSE 0 END",
    "SELECT COUNT(*) FROM sys.triggers WHERE parent_id = OBJECT_ID(?)"), "postgres": (
    "SELECT relhastriggers::int FROM pg_class WHERE oid = to_regclass(%s)",
    "SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = to_regclass(%s) AND NOT tgisinternal")}


def _quote_ident(part: str) -> str:
    """One probe identifier, backtick-delimited with inner backticks doubled (recon.adapters.quote_ident)."""
    if not part or "." in part or "\x00" in part:
        raise ValueError(f"invalid identifier part {part!r}")
    return "`" + part.replace("`", "``") + "`"


def check_dictionary_readable(tables: list[str], family: str, source_secret: str | None, connect=None,
    views: list[tuple] | None = None) -> Check:
    """Catalog visibility is not data SELECT (references/checks.md); `views` is the harness's probe table."""
    cid = "dictionary_readable"
    q = _TRIGGER_CENSUS.get(family)
    if not views:
        return Check(cid, "unverified", f"{family}: no dictionary probe for this family; structural "
            "parity will record its categories as unsupported", {"family": family, "tables": tables})
    if family != "databricks":  # a Databricks-family source is probed as the session identity
        if not source_secret:
            return Check(cid, "fail", f"{family} source with {len(tables)} in-scope table(s); pass "
                "--source-secret NAME so catalog visibility can be checked", {"family": family,
                "tables": tables})
        dsn = _env_dsn(cid, source_secret)
        if isinstance(dsn, Check):
            return dsn
    data: dict = {"family": family, "tables": tables, "views": [], "trigger_census": {}}
    try:
        conn = (connect or _READ_ONLY_CONNECT[family])(os.environ.get(source_secret) or "")
        try:
            cur = conn.cursor()
            for label, sql in views:
                probes = [(None, sql)]
                if "{" in sql:
                    parts = {}
                    for t in tables:
                        p_ = t.replace("`", "").split(".")
                        if len(p_) != 3 or any(not x or "." in x or "\x00" in x for x in p_):
                            return Check(cid, "fail",
                                f"{t} is not catalog.schema.table; cannot scope the dictionary probe", data)
                        parts[t] = p_
                    if "{schema}" in sql or "{table}" in sql:
                        probes = [(t, sql.format(catalog=_quote_ident(p_[0]), schema=_quote_ident(p_[1]),
                            table= _quote_ident(p_[2]))) for t, p_ in parts.items()]
                    else:  # {catalog} only: information_schema is catalog-scoped, probe per catalog
                        cats = sorted({p_[0] for p_ in parts.values()})
                        probes = [(None, sql.format(catalog=_quote_ident(c))) for c in cats]
                for table, probe in probes:
                    try:
                        cur.execute(probe).fetchall()
                    except Exception as e:  # noqa: BLE001
                        return Check(cid, "fail", f"cannot read {label}{f' on {table}' if table else ''}: "
                            f"{_redact(str(e))}: the structural tier would grade on an incomplete dictionary", data)
                data["views"].append(label)
            if q is None:
                return Check(cid, "ok", f"{family}: catalog views readable on {len(tables)} in-scope table(s)", data)
            declared_sql, listed_sql = q
            mismatched = []
            for t in tables:
                n = declared_sql.count("?") or declared_sql.count("%s")
                declared = cur.execute(declared_sql, (t,) * n).fetchall()[0][0]
                listed = cur.execute(listed_sql, (t,)).fetchall()[0][0]
                data["trigger_census"][t] = {"declared": declared, "listed": listed}
                if declared and not listed if family == "sqlserver" else bool(declared) != bool(listed):
                    mismatched.append(t)
            if mismatched and family == "sqlserver":
                return Check(cid, "fail", f"{mismatched[0]} has triggers the principal cannot "
                    "list: sys.triggers is filtered by permission, so the trigger tier "
                    "would pass on an empty view", data)
            if mismatched:
                return Check(cid, "warn", f"declared vs listed trigger census differs on "
                    f"{', '.join(mismatched[:3])}: relhastriggers can stay true after a drop until vacuum", data)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - a driver failure is a finding, never a DSN traceback
        return Check(cid, "fail", f"dictionary probe failed: {_redact(str(e))}", data)
    return Check(cid, "ok", f"{family}: catalog views readable and the trigger census agrees on "
        f"{len(tables)} in-scope table(s)", data)


def check_dictionary_readable_all(ws: Path, role: str, units: list[str], mappings: list[Path],
    source_secret: str | None, source_family: str | None, plugin_root: Path, params: dict[str,
    str] | None = None) -> Check:
    """Every source table the resolved mappings read, against --source-family or the mappings' implied family."""
    cid = "dictionary_readable"

    def dictionary(_expected, todo):
        def probe(tables, family):
            cmd = _harness_command(plugin_root)
            if cmd is None:
                return Check(cid, "fail", "cannot ask the harness which catalog objects to probe: "
                    "dbx-recon not on PATH and the checkout has no harness")
            argv, cwd, how = cmd
            rc, out, err = _harness_run(cmd, ["dictionary-objects", "--family", family])
            try:
                d = json.loads(out) if rc == 0 else {}
                views = [tuple(x) for x in d["objects"]] if d.get("family_known") else None
            except (ValueError, KeyError, TypeError):
                return Check(cid, "fail", f"cannot ask the harness which catalog objects to probe "
                    f"({how} dictionary-objects rc={rc}): {_redact(err or out)}", {"family": family,
                    "tables": tables})
            c = check_dictionary_readable(tables, family, source_secret, views=views)
            c.data["harness"] = how
            return c

        return _from_mappings(cid, todo, params, plugin_root, source_family, "so catalog visibility can be checked",
            probe)

    return per_unit(cid, ws, role, units, mappings, dictionary)


_APPLICATION_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
_SP_SCHEMA = "servicePrincipal"


def classify_identity(who: dict) -> tuple[str, bool]:
    """(userName, is_service_principal) from a SCIM `current-user me` document; human unless proven otherwise."""
    name = str(who.get("userName") or who.get("displayName") or "?")
    schemas = [str(s) for s in who.get("schemas") or []]
    is_sp = (bool(who.get("applicationId")) or
        any(_SP_SCHEMA.lower() in s.lower() for s in schemas) or bool(_APPLICATION_ID.fullmatch(name)))
    return name, is_sp


def _norm_host(host: str) -> str:
    return re.sub(r"^https?://", "", host.strip().lower()).rstrip("/")


SECRET_REF = re.compile(r"(?:\{\{\s*secrets/|\bsecrets/)([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)")
SECRET_GET_CALL = re.compile(r"secrets\.get\(([^()]*)\)")
SECRET_GET_ARG = re.compile(r"""(?:(scope|key)\s*=\s*)?["']([^"']+)["']""")


def _secret_get_names(text: str) -> set:
    out = set()
    for call in SECRET_GET_CALL.finditer(text):
        got = {}
        for arg in call.group(1).split(","):
            m = SECRET_GET_ARG.fullmatch(arg.strip())
            if m:
                got.setdefault(m.group(1) or ("scope" if "scope" not in got else "key"), m.group(2))
        if "scope" in got and "key" in got:
            out.add(f"{got['scope']}/{got['key']}")
    return out


def manifest_secret_names(manifest: dict) -> list[str]:
    lists, briefs = [manifest.get("secrets", [])], []
    for b in manifest.get("batches", []):
        if isinstance(b, dict):
            lists.append(b.get("secrets", []))
            briefs.append(b.get("brief") or "")
    found = set()
    for lst in lists:
        if not isinstance(lst, list) or not all(isinstance(n, str) for n in lst):
            raise SystemExit("wave manifest 'secrets' (top level or per batch) must be a list of scope/key strings")
        found.update(lst)
    for brief in briefs:
        text = str(brief)
        found.update(f"{scope}/{key}" for scope, key in SECRET_REF.findall(text))
        found.update(_secret_get_names(text))
    return sorted(found)


def _list_secrets(scope: str) -> list[str] | None:
    cli = shutil.which("databricks")
    if not cli:
        return None
    payload, _shown = _cli_json(cli, "secrets", "list-secrets", scope)
    rows = payload.get("secrets") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return None
    return [row["key"] for row in rows if isinstance(row, dict) and "key" in row]


def check_named_secrets(names: list[str], list_secrets=None) -> Check:
    cid = "named_secrets_exist"
    data = {"checked": names, "missing": [], "unreadable_scopes": []}
    if not names:
        return Check(cid, "skipped", "no Databricks secret names referenced by the wave manifest or its briefs", data)
    malformed = [n for n in names if not re.fullmatch(r"[^/\s]+/[^/\s]+", n)]
    if malformed:
        return Check(cid, "fail", f"not a scope/key secret name: {', '.join(malformed)}", data)
    list_secrets = list_secrets or _list_secrets
    keys = {scope: list_secrets(scope) for scope in {n.split("/", 1)[0] for n in names}}
    data["unreadable_scopes"] = sorted(s for s, k in keys.items() if k is None)
    parts = [n.split("/", 1) for n in names]
    data["missing"] = [n for n, (scope, key) in zip(names, parts) if keys[scope] is None or key not in keys[scope]]
    if data["missing"]:
        detail = ("missing named secrets (STOP C blocker: create them before launch; values are never "
            "read): " + ", ".join(data["missing"]))
        if data["unreadable_scopes"]:
            scope_msgs = "; ".join(f"scope {s} not readable via `databricks secrets list-secrets`"
                for s in data["unreadable_scopes"])
            detail += "; " + scope_msgs
        return Check(cid, "fail", detail, data)
    return Check(cid, "ok", f"{len(names)} named secret(s) exist in {len(keys)} scope(s), names only", data)


def check_databricks(expect_identity: str | None, expect_host: str | None = None) -> list[Check]:
    out: list[Check] = []
    cli = shutil.which("databricks")
    if not cli:
        out.append(Check("databricks_cli", "fail", "databricks CLI not on PATH (see databricks-core for install)"))
        return out
    rc, ver, err = _run([cli, "--version"], timeout=20)
    out.append(Check("databricks_cli", "ok" if rc == 0 else "fail", (ver or err).strip()[:80], {"path": cli}))

    desc, _shown = _cli_json(cli, "auth", "describe")
    details = desc.get("details") if isinstance(desc, dict) else None
    host = details.get("host") if isinstance(details, dict) else None
    auth_type = details.get("auth_type") if isinstance(details, dict) else None
    auth_data = {"auth_kind": auth_type}
    if os.environ.get("DATABRICKS_DEVIN_AUDIENCE"):
        auth_data["audience"] = os.environ["DATABRICKS_DEVIN_AUDIENCE"]
    if auth_type in ("env-oidc", "oauth-m2m"):
        out.append(Check("databricks_auth_kind", "ok",
            f"auth: {auth_type} (service principal via the org blueprint)", auth_data))
    elif auth_type == "pat":
        out.append(Check("databricks_auth_kind", "warn",
            "auth: pat — a personal access token attributes the session's work to a human and "
            "bypasses the migration service principal; authenticate via the org blueprint "
            "(env-oidc or oauth-m2m), or record a waiver in 06_decisions.md", auth_data))
    else:
        out.append(Check("databricks_auth_kind", "fail",
            f"auth: `databricks auth describe` reports {auth_type!r}; the org blueprint accepts "
            "only env-oidc or oauth-m2m", auth_data))

    who, shown = _cli_json(cli, "current-user", "me")
    if not isinstance(who, dict):
        out.append(Check("databricks_identity", "fail", f"current-user me failed: {shown}"))
        return out
    name, is_sp = classify_identity(who)
    display_name = name if is_sp else "<human user (redacted)>"
    data = {"userName": display_name, "service_principal": is_sp, "host": host}
    if "audience" in auth_data:
        data["audience"] = auth_data["audience"]
    status = "ok"
    detail = f"authenticated as {display_name} ({'service principal' if is_sp else 'user'}) on {host}"
    if expect_identity and str(name).lower() != expect_identity.lower():
        expected_display = "<human user (redacted)>" if "@" in expect_identity else expect_identity
        status, detail = "fail", detail + f"; expected {expected_display} (recorded in 07_access_checklist.md)"
    elif not host:
        status = "fail"
        detail += ("; workspace host not resolved by `databricks auth describe`, so the wave manifest "
            "cannot pin children to it")
    elif expect_host and _norm_host(str(host)) != _norm_host(expect_host):
        status, detail = "fail", detail + f"; expected host {expect_host} (the capability contract's workspace)"
    elif not is_sp:
        status, detail = "warn", detail + ("; unattended sessions must not run as a human identity: "
            "authenticate as the migration service principal via the org blueprint (OIDC token "
            "federation or OAuth M2M; see target-routing), or record a waiver in 06_decisions.md")
    out.append(Check("databricks_identity", status, detail, data))

    rc, wh, err = _run([cli, "experimental", "aitools", "tools", "get-default-warehouse"], timeout=60)
    if rc == 0 and wh.strip():
        out.append(Check("databricks_warehouse", "ok", f"default warehouse resolved: {wh.strip()[:120]}"))
    else:
        out.append(Check("databricks_warehouse", "warn", f"no default warehouse via aitools: {_redact(err or wh)}"))
    return out


def check_lakebase_branch_create(project: str, parent_branch: str) -> Check:
    """Create and delete a short-lived branch to prove Lakebase project access."""
    cid = "lakebase_branch_create"
    cli = shutil.which("databricks")
    if not cli:
        return Check(cid, "unverified", "databricks CLI not on PATH; install databricks-core and databricks-lakebase")
    branch = f"dbx-doctor-probe-{secrets.token_hex(4)}"
    project_path = f"projects/{project}"
    rc, out, err = _run([cli, "postgres", "create-branch", project_path, branch, "--json", json.dumps({"spec": {
        "source_branch": f"{project_path}/branches/{parent_branch}", "ttl": "3600s"}}), "--output", "json"],
        timeout=300)
    if rc != 0:
        low = err.lower()
        detail = next((d for needle, d in (("not authorized",
            f"migration principal is not authorized; grant the migration principal "
            f"Can Manage on Lakebase project {project}"), ("expiration date cannot have child branches",
            f"parent branch {parent_branch} has an expiry; clear it "
            "(Lakebase project → branch → edit → remove expiration), TTLs belong on per-batch children"),
            ) if needle in low), _redact(err or out))
        return Check(cid, "fail", detail, {"project": project, "parent_branch": parent_branch})
    delete_rc, delete_out, delete_err = _run([cli, "postgres", "delete-branch", f"{project_path}/branches/{branch}",
        "--purge"], timeout=300)
    if delete_rc != 0:
        return Check(cid, "warn",
            f"branch {branch} was created but could not be deleted: {_redact(delete_err or delete_out)}", {
            "project": project, "parent_branch": parent_branch, "branch": branch})
    return Check(cid, "ok", "branch created and deleted", {"project": project, "parent_branch": parent_branch,
        "branch": branch})


def check_lakebase_target_grants(dsn_name: str, schema: str | None = None, connect=None) -> Check:
    """Check CREATE on the configured Lakebase database or optional schema."""
    cid = "lakebase_target_grants"
    if not os.environ.get(dsn_name):
        return Check(cid, "fail", f"secret {dsn_name} is not set in this shell")
    if connect is None:
        try:
            import psycopg  # lazy: optional extra
        except ImportError:
            return Check(cid, "fail", "psycopg is unavailable; install the postgres extra for data-reconciliation")
        connect = lambda dsn: psycopg.connect(dsn)
    conn = None
    try:
        conn = connect(os.environ[dsn_name])
        cur = conn.cursor()
        cur.execute("select current_user, current_database(), "
            "has_database_privilege(current_user, current_database(), 'CREATE')")
        role, database, db_create = cur.fetchone()
        data = {"role": role, "database": database, "schema": schema}
        if schema:
            cur.execute("select 1 from information_schema.schemata where schema_name=%s", (schema,))
            if cur.fetchone() is not None:
                cur.execute("select has_schema_privilege(current_user, %s, 'CREATE')", (schema,))
                if not cur.fetchone()[0]:
                    return Check(cid, "fail",
                        f"missing required privilege: GRANT CREATE ON SCHEMA {schema} TO {role}", data)
                return Check(cid, "ok", f"role {role} can CREATE in schema {schema}", data)
        if bool(db_create):
            return Check(cid, "ok", f"role {role} can CREATE in database {database}", data)
        return Check(cid, "fail", f"missing required privilege: GRANT CREATE ON DATABASE {database} TO {role}", data)
    except Exception as e:  # noqa: BLE001 - driver-specific connection errors
        return Check(cid, "fail",
            f"connection to {dsn_name} failed ({type(e).__name__}); check the DSN secret and network path")
    finally:
        if conn is not None and hasattr(conn, "close"):
            conn.close()


def _effective_privileges(payload) -> set[str]:
    """Privilege names from effective-assignment or plain grant payloads ({privilege: name} dicts or bare strings)."""
    if isinstance(payload, dict):
        assigns = [a for a in payload.get("privilege_assignments") or [] if isinstance(a, dict)]
        items = [p for a in assigns for p in a.get("privileges") or []]
        items += list(payload.get("privileges") or [])
    elif isinstance(payload, list):
        items = list(payload)
    else:
        items = []
    return {v.upper() if isinstance(v, str) else v["privilege"].upper() for v in items if isinstance(v, str) or
        isinstance(v, dict) and isinstance(v.get("privilege"), str)}


def _effective_privileges_strict(payload) -> set[str] | None:
    """None unless the payload is exactly a get-effective grants response (unreadable grants must not pass)."""
    if not isinstance(payload, dict) or not isinstance(payload.get("privilege_assignments"), list):
        return None
    found: set[str] = set()
    for assignment in payload["privilege_assignments"]:
        privileges = assignment.get("privileges") if isinstance(assignment, dict) else None
        if not isinstance(privileges, list) or not all(isinstance(p, str) for p in privileges):
            return None
        found.update(p.upper() for p in privileges)
    return found


def _permission_error(text: str) -> bool:
    low = text.lower()
    return any(m in low for m in ("does not have", "permission_denied", "permission denied", "insufficient",
        "unauthorized"))


def _sql_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


_SCHEMA_REQUIRED = [("USE_SCHEMA", "USE SCHEMA"), ("CREATE_TABLE", "CREATE TABLE"), ("MODIFY", "MODIFY"), ("SELECT",
    "SELECT")]


def _grant_statements(catalog: str, full_name: str, principal: str, missing_catalog: list[str],
    missing_schema: list[str]) -> str:
    statements = []
    if missing_catalog:
        statements.append(f"GRANT USE CATALOG ON CATALOG {_sql_ident(catalog)} TO `{principal}`")
    if missing_schema:
        display = ", ".join(label for name, label in _SCHEMA_REQUIRED if name in missing_schema)
        schema_catalog, schema_name = full_name.split(".", 1)
        statements.append(f"GRANT {display} ON SCHEMA {_sql_ident(schema_catalog)}.{_sql_ident(schema_name)} "
            f"TO `{principal}`")
    return "; ".join(statements)


def _catalog_privileges(cli: str, catalog: str, principal: str) -> tuple[set[str] | None, str | None, str | None]:
    """Return catalog privileges, owner, and a redacted error when both lookups fail."""
    catalog_owner = None
    payload, _shown = _cli_json(cli, "catalogs", "get", catalog)
    catalog_owner = payload.get("owner") if isinstance(payload, dict) else None
    if isinstance(catalog_owner, str) and catalog_owner.lower() == principal.lower():
        return {"ALL_PRIVILEGES"}, catalog_owner, None
    privileges, shown = _uc_privileges(cli, "catalog", catalog, principal)
    return (None, catalog_owner, shown) if privileges is None else (privileges, catalog_owner, None)


def check_analytical_target_grants(full_name: str) -> Check:
    """Check Unity Catalog privileges needed to create or write the promotion schema."""
    cid = "analytical_target_grants"

    def row(status: str, detail: str, **data) -> Check:
        base = {"schema": full_name, "principal": None, "owner": None, "catalog_owner": None, "exists": False,
            "missing": []}
        return Check(cid, status, detail, {**base, **data})

    if full_name.count(".") != 1 or any(not part for part in full_name.split(".")):
        return row("fail", "expected CATALOG.SCHEMA")
    catalog, schema = full_name.split(".", 1)
    cli = shutil.which("databricks")
    if not cli:
        return row("unverified", "databricks CLI not on PATH; install databricks-core and databricks-unity-catalog")
    who, shown = _cli_json(cli, "current-user", "me")
    principal = (who.get("applicationId") or who.get("userName")) if isinstance(who, dict) else None
    if not isinstance(principal, str) or not principal:
        return row("fail", shown or "current-user response has no applicationId or userName")

    payload, shown = _cli_json(cli, "schemas", "get", full_name)
    exists = owner = None
    if payload is not None:
        if not isinstance(payload, dict):
            return row("fail", shown, principal=principal)
        owner, exists = payload.get("owner"), True
    else:
        exists = _permission_error(shown)  # exists but unreadable, else truly absent
        if not exists and not any(m in shown.lower() for m in ("not found", "does not exist", "not_found",
            "schema_does_not_exist")):
            return row("fail", shown, principal=principal)
    owned = isinstance(owner, str) and owner.lower() == principal.lower()

    schema_privileges: set[str] = set()
    if exists and not owned:
        privileges, shown = _uc_privileges(cli, "schema", full_name, principal)
        if privileges is None and not (_permission_error(shown) or shown.strip() == "{}"):
            return row("fail", shown, principal=principal, exists=True)
        schema_privileges = privileges or set()

    catalog_privileges, catalog_owner, catalog_error = _catalog_privileges(cli, catalog, principal)
    if catalog_error:
        return row("fail", catalog_error, principal=principal, owner=owner, catalog_owner=catalog_owner,
            exists=bool(exists))
    assert catalog_privileges is not None
    has = lambda p: "ALL_PRIVILEGES" in catalog_privileges or p in catalog_privileges
    has_schema = lambda p: "ALL_PRIVILEGES" in schema_privileges or p in schema_privileges
    data = {"principal": principal, "owner": principal if owned else owner, "catalog_owner": catalog_owner,
        "exists": bool(exists)}

    def granted():
        """(missing list, GRANT statement) for the exists-branches; the caller renders the row."""
        missing_schema = [n for n, _ in _SCHEMA_REQUIRED if not has_schema(n)]
        missing = ([] if has("USE_CATALOG") else ["USE_CATALOG"]) + missing_schema
        statement = _grant_statements(catalog, full_name, principal, [n for n in missing if n == "USE_CATALOG"],
            missing_schema)
        return missing, statement

    if not exists:  # schema absent: catalog CREATE_SCHEMA suffices; setup creates and owns it
        missing = [n for n in ("USE_CATALOG", "CREATE_SCHEMA") if not has(n)]
        if missing:
            display = ", ".join("CREATE SCHEMA" if n == "CREATE_SCHEMA" else "USE CATALOG" for n in missing)
            return row("fail",
                f"missing required privileges: GRANT {display} ON CATALOG {_sql_ident(catalog)} TO `{principal}`",
                missing=missing, **data)
        return row("ok",
            f"schema {full_name} does not exist; setup creates it owned by {principal} (no grants needed)", **data)
    if owner is None:  # exists but unreadable: report the schema grants the row could not read
        missing, statement = granted()
        return row("fail", f"missing required privileges: {statement}; owner is unknown", missing=missing, **data)
    if owned:
        missing = [] if has("USE_CATALOG") else ["USE_CATALOG"]
        if missing:
            statement = _grant_statements(catalog, full_name, principal, missing, [])
            return row("fail", f"missing required privileges: {statement}; schema owned by {principal}",
                missing=missing, **data)
        return row("ok", f"schema {full_name} is owned by {principal}", **data)
    missing, statement = granted()
    if missing:
        return row("fail", f"missing required privileges: {statement}; owner is {owner}", missing=missing, **data)
    return row("ok", f"principal {principal} can create and write tables in {full_name} (owner {owner})", **data)


def _advisory(c: Check) -> Check:
    """In a child, non-security `fail` rows are advisory `warn` (the orchestrator gated them at launch);
    a writable source principal is a legacy-safety finding and stays `fail`."""
    if (c.status == "fail" and c.id not in CHILD_SECURITY_CONTROLS and c.id != "source_principal_read_only"
            and not (c.data or {}).get("units_problem")):
        return Check(c.id, "warn", "advisory in a child (the orchestrator gates it before launch): " + c.detail,
            c.data)
    return c


def _blocking(role: str, checks: list[Check]) -> list[str]:
    """The readiness rule per role: security controls `ok`; a child also blocks on a unit-mapping problem,
    orchestrator/setup on any `fail` or an unverified `source_principal_read_only`."""
    sec = security_controls(role)

    def blocks(s: Check) -> bool:
        if s.id in sec and s.status != "ok":
            return True
        if role == "child":
            return (bool((s.data or {}).get("units_problem")) or
                (s.id == "source_principal_read_only" and s.status == "fail"))
        return s.status == "fail" or (s.id == "source_principal_read_only" and s.status == "unverified")

    blocking = [f"{row.id}={row.status}" for row in checks if any(blocks(s) for s in _flat([row]))]
    seen = {s.id for row in checks for s in _flat([row])}
    # a security control with no row at all (hooks files absent, CLI missing) still blocks
    return blocking + [f"{cid}=missing" for cid in sec if cid not in seen]


def run(ws: Path, plugin_root: Path, role: str, probe_result: str, expect_identity: str | None, no_databricks: bool,
    units: list[str] | None = None, mappings: list[Path] | None = None, source_secret: str | None = None,
    params: dict[str, str] | None = None, expect_catalogs: list[str] | None = None, source_family: str | None = None,
    expect_host: str | None = None, lakebase_project: str | None = None, lakebase_parent_branch: str | None = None,
    lakebase_dsn: str | None = None, lakebase_schema: str | None = None, analytical_schema: str | None = None,
    source_attested: str | None = None, live_playbooks: Path | None = None, target_kind: str = "databricks",
    secret_names: list[str] | None = None, list_secrets=None, reused: dict | None = None) -> dict:
    def _row(row_id, thunk, **binds):
        """`binds` are data keys the recorded row must carry with these exact values to stand in."""
        if isinstance(reused, dict) and row_id in REUSABLE_ROWS:
            row = next((c for c in reused.get("checks") or [] if isinstance(c, dict) and c.get("id") == row_id), None)
            data = row.get("data") if isinstance(row, dict) and isinstance(row.get("data"), dict) else {}
            if (isinstance(row, dict) and isinstance(row.get("status"), str) and
                    isinstance(row.get("detail"), str) and all(data.get(k) == v for k, v in binds.items())):
                return Check(row["id"], row["status"],
                    f"reused from the orchestrator's record signed {reused.get('signed_at')}: {row['detail']}",
                    {**(row.get("data") or {}), "reused_from": reused.get("signed_at")})
        return thunk()

    units, mappings = units or [], mappings or []
    sec = security_controls(role)
    checks: list[Check] = [
        _merge("workspace", [check_workspace(ws), check_stop_mode(ws)]),
        _merge("allowed_targets", [check_allowed_targets(ws, plugin_root),
            check_allowlist_matches_contract(ws, expect_catalogs)]),
        check_allowlist_committed(ws),
        check_playbooks_in_sync(ws, plugin_root, role, live_playbooks),
        _merge("hook_guard", check_hooks(plugin_root, ws, probe_result, role, reused), sec),
        check_official_plugin(plugin_root),
        _merge("recon_harness", [check_harness(plugin_root), check_drivers()]),
        _row("recon_family_supported",
            lambda: check_recon_family_supported(plugin_root, source_family)),
        _row("type_map_audit",
            lambda: check_type_map_audit(ws, role, units, mappings, source_family, plugin_root,
                params=params, target_kind=target_kind), target_kind=target_kind),
        _row("delete_evidence",
            lambda: check_delete_evidence_all(ws, role, units, mappings, source_secret, plugin_root,
                params=params)),
        _row("source_principal_read_only",
            lambda: check_source_principal_all(ws, role, units, mappings, source_secret, source_family,
                plugin_root, params=params, attested=source_attested)),
        _row("dictionary_readable",
            lambda: check_dictionary_readable_all(ws, role, units, mappings, source_secret, source_family,
                plugin_root, params=params)),
        _row("named_secrets_exist",
            lambda: (Check("named_secrets_exist", "skipped", "--no-databricks") if no_databricks
                else check_named_secrets(secret_names or [], list_secrets)))]
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
        checks.append(Check("analytical_target_grants", "skipped",
            "--no-databricks") if no_databricks else check_analytical_target_grants(analytical_schema))
    if role == "child":  # non-security failures are advisory; the orchestrator gated them at launch

        def softened(row: Check) -> Check:
            subs = (row.data or {}).get("sub_results")
            if not subs:
                return _advisory(row)
            return _merge(row.id, [_advisory(Check(**d)) for d in subs], sec)

        checks = [softened(row) for row in checks]
    counts: dict[str, int] = {}
    for c in checks:
        counts[c.status] = counts.get(c.status, 0) + 1
    blocking = _blocking(role, checks)
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
        "reused_doctor": reused["signed_at"] if isinstance(reused, dict) else None,
        "checks": [{**asdict(c), "reusable": c.id in REUSABLE_ROWS} for c in checks]}


def _manifest_source(manifest) -> tuple:
    """(family, secret, --param argv list, referenced secret names, capabilities) of a wave manifest."""
    m = manifest if isinstance(manifest, dict) else {}
    caps = m.get("capabilities") if isinstance(m.get("capabilities"), dict) else {}
    source = m.get("source") if isinstance(m.get("source"), dict) else {}
    params = [f"{k}={v}" for k, v in (source.get("params") or {}).items()]
    return source.get("family"), source.get("secret"), params, manifest_secret_names(m), caps


def _load_manifest(p: argparse.ArgumentParser, path: Path, what: str):
    """(manifest_bytes, manifest) read or p.error."""
    try:
        manifest_bytes = path.read_bytes()
        return manifest_bytes, json.loads(manifest_bytes)
    except (OSError, ValueError) as e:
        p.error(f"cannot read {what} {path}: {e}")


def _apply_manifest(a, manifest):
    """Fill expect_host/expect_catalogs/source settings/secrets from a wave manifest onto parsed args."""
    family, secret, mparams, names, caps = _manifest_source(manifest)
    if a.expect_host is None:
        a.expect_host = caps.get("host")
    if a.expect_catalogs is None:
        catalogs = caps.get("catalogs")
        a.expect_catalogs = catalogs if isinstance(catalogs, list) else None
    a.source_family, a.source_secret, a.param = family, secret, mparams
    a.secret = sorted({*a.secret, *names})
    return caps


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--workspace", type=Path, default=Path.cwd())
    p.add_argument("--plugin-root", type=Path, default=Path(__file__).resolve().parents[2])
    p.add_argument("--role", choices=("orchestrator", "child", "setup"), default="orchestrator")
    p.add_argument("--wave", type=Path,
        help="wave manifest; also writes <manifest>.doctor.json, the signed record the fan-out launches from")
    p.add_argument("--reuse-record", type=Path, metavar="PATH",
        help="(--role child) the orchestrator's <manifest>.doctor.json; reuse its signed source-side rows "
        "when it checks out")
    p.add_argument("--hook-probe-result", default="unknown", metavar="blocked:<nonce>|not-blocked|unknown",
        help="outcome of running the last report's probe_command")
    p.add_argument("--expect-identity", help="userName the session must be authenticated as")
    p.add_argument("--expect-host", help="workspace host the session must be authenticated against (the contract's)")
    p.add_argument("--expect-catalogs", metavar="A,B", type=lambda s: [c.strip() for c in s.split(",") if c.strip()],
        help="catalogs the wave's capability contract names; must equal allowed_targets.json's")
    p.add_argument("--no-databricks", action="store_true",
        help="skip CLI/identity checks (offline; the report is never ready)")
    p.add_argument("--unit", action="append", default=[], metavar="UNIT_ID",
        help="(--role child) a unit of this batch, repeat per unit in the brief")
    p.add_argument("--mapping", type=Path, action="append", default=[], metavar="MAPPING_SPEC",
        help="additional recon mapping_spec.json to verify (a candidate mapping at setup)")
    p.add_argument("--source-secret", help="env var NAME holding the read-only source DSN (value never printed)")
    p.add_argument("--secret", action="append", default=[], metavar="SCOPE/KEY",
        help="Databricks secret name a brief references; checked by name, value never read")
    p.add_argument("--source-family", choices=SOURCE_FAMILIES,
        help="source engine behind --source-secret (default: implied by the mappings' delete_evidence kind)")
    p.add_argument("--target-kind", choices=TARGET_KINDS, default="databricks",
        help="recon target the type_map is audited against")
    p.add_argument("--source-attested", metavar="D-<id>",
        help="ledger decision id attesting the source has no principal to query; rejected for families "
        "with a privilege query")
    p.add_argument("--lakebase-project", help="Lakebase project id for the branch-create preflight")
    p.add_argument("--lakebase-parent-branch", help="Lakebase parent branch for the branch-create preflight")
    p.add_argument("--lakebase-dsn", metavar="ENV_VAR_NAME",
        help="env var NAME holding the Lakebase DSN (value never printed)")
    p.add_argument("--lakebase-schema", help="optional Lakebase schema to check for CREATE")
    p.add_argument("--analytical-schema", metavar="CATALOG.SCHEMA",
        help="promotion schema the principal must be able to write")
    p.add_argument("--live-playbooks", type=Path, metavar="PATH",
        help="JSON export of the live playbooks written right before this run "
        "(default .migration/live_playbooks.json)")
    p.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
        help="mapping ${NAME} placeholder value, same rules as dbx-recon run --param")
    p.add_argument("--out", type=Path, help="default .migration/09_capabilities.json; '-' for stdout only")
    a = p.parse_args(argv)
    if a.hook_probe_result == "blocked":
        p.error("--hook-probe-result blocked:<nonce> is required: the nonce the guard's block message "
            "named for the probe_command of the last report")
    manifest_bytes = None
    if a.wave:
        manifest_bytes, manifest = _load_manifest(p, a.wave, "wave manifest")
        caps = manifest.get("capabilities") if isinstance(manifest, dict) else None
        if not isinstance(caps, dict):
            p.error(f"wave manifest {a.wave} has no capabilities object")
        if a.expect_identity is None:
            a.expect_identity = caps.get("identity")
        if a.source_family is not None or a.source_secret is not None or a.param:
            p.error("--wave takes source settings from the manifest; drop --source-family/--source-secret/--param")
        _apply_manifest(a, manifest)

    reused, reuse_why = None, ""
    if a.reuse_record:
        for bad, why in ((a.wave, "--reuse-record reads the manifest beside the record; drop --wave"),
            (a.role != "child", "--reuse-record is for --role child runs"),
            (a.no_databricks, "--reuse-record still runs the databricks identity check; drop --no-databricks"),
            (a.expect_identity is None, "--reuse-record requires --expect-identity: the principal the record must "
                "be signed for")):
            if bad:
                p.error(why)
        try:
            manifest_path = a.reuse_record.with_name(a.reuse_record.name.replace(".doctor.json", ".json"))
            manifest_bytes, manifest = _load_manifest(p, manifest_path, "--reuse-record's manifest")
            record = json.loads(a.reuse_record.read_bytes())
        except (OSError, ValueError) as e:
            p.error(f"cannot read --reuse-record or the manifest beside it: {e}")
        want_family, want_secret, want_params, _names, _caps = _manifest_source(manifest)
        for flag, given, want in (("--source-family", a.source_family, want_family), ("--source-secret",
            a.source_secret, want_secret), ("--param", sorted(a.param or []), sorted(want_params))):
            if given not in (None, [], want):
                p.error(f"{flag} differs from the manifest's source block; --reuse-record takes source settings "
                    "from the manifest, so pass the same values or none")
        _apply_manifest(a, manifest)
        reused, reuse_why = reusable_record(record, manifest if isinstance(manifest, dict) else {}, manifest_bytes,
            a.expect_identity, a.expect_host, inputs_sha=inputs_sha(a.workspace.resolve()))

    params = None
    if a.param:
        sys.path.insert(0, str(a.plugin_root.resolve() / "skills" / "data-reconciliation" / "harness"))
        from recon.cli import parse_params

        params = parse_params(a.param)

    report = run(a.workspace.resolve(), a.plugin_root.resolve(), a.role, a.hook_probe_result, a.expect_identity,
        a.no_databricks, a.unit, a.mapping, a.source_secret, params, a.expect_catalogs, a.source_family,
        a.expect_host, a.lakebase_project, a.lakebase_parent_branch, a.lakebase_dsn, a.lakebase_schema,
        a.analytical_schema, source_attested=a.source_attested, live_playbooks=a.live_playbooks,
        target_kind=a.target_kind, secret_names=a.secret, reused=reused)
    if a.reuse_record:
        report["checks"].append({**asdict(Check("doctor_record", "ok" if reused else "skipped",
            f"reused the orchestrator's record signed {reused['signed_at']}" if reused else reuse_why)),
            "reusable": False})
        if not reused:
            print(f"doctor record not reused: {reuse_why}", file=sys.stderr)
    text = json.dumps(report, indent=2, sort_keys=True)
    out = a.out
    if out is None:
        out = a.workspace / ".migration" / "09_capabilities.json"
    if str(out) != "-" and (a.workspace / ".migration").is_dir():
        out.write_text(text + "\n")
    for c in report["checks"]:
        print(f"{c['status']:<10} {c['id']:<28} {c['detail']}")
    tail = f"\nready={report['ready']} {report['summary']}"
    tail += f" blocking={report['blocking']}" if report["blocking"] else ""
    print(tail + (f"  -> {out}" if str(out) != "-" else ""))
    if a.wave:
        signed = sign_wave_report({**report, "hook_probe": a.hook_probe_result,
            "source": manifest.get("source"), "inputs_sha": inputs_sha(a.workspace.resolve())}, manifest_bytes)
        a.wave.with_suffix(".doctor.json").write_text(json.dumps(signed, indent=2, sort_keys=True) + "\n")
    if a.role != "child":
        probe = next((s for c in report["checks"]
            for s in (c.get("data") or {}).get("sub_results") or []
            if s.get("id") == "hook_platform_loaded"), None)
        if probe and probe.get("status") == "unverified":
            cmd = probe["data"]["probe_command"]
            print("PROBE_COMMAND (run in the lead session's exec tool, not a sidekick shell): " + cmd)
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
