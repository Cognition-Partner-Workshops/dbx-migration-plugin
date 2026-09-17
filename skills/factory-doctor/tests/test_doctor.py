import hashlib
import json
import os
import re
import subprocess
import time
import sys
import types
from dataclasses import asdict
from pathlib import Path

import pytest

SKILL = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = SKILL.parents[1]
sys.path.insert(0, str(SKILL))
sys.path.insert(0, str(PLUGIN_ROOT / "hooks"))

import doctor  # noqa: E402
import dbx_guard  # noqa: E402


def _git(ws: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(ws), "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
                          check=True, capture_output=True, text=True).stdout


def make_workspace(tmp_path: Path, *, allowed=None, stop_mode="hard", omit=(), commit=True,
                   with_lock=True):
    """A setup-complete workspace, committed as the setup playbook leaves it before STOP A,
    with the playbook lock install-dbx-factory leaves after its sync and the live-playbooks
    export the orchestrator writes right before the doctor."""
    mig = tmp_path / ".migration"
    mig.mkdir(parents=True)
    for f in doctor.REQUIRED_FILES:
        if f in omit:
            continue
        if f == "allowed_targets.json":
            mig.joinpath(f).write_text(json.dumps(allowed if allowed is not None else
                                                  {"catalogs": ["mig_cat"], "legacy_sources": ["LEGACY_DSN"]}))
        elif f == "00_context.md":
            mig.joinpath(f).write_text(
                f"# context\n\nstop_mode: {stop_mode}\n\n## Glossary\n- term: meaning\n"
            )
        else:
            mig.joinpath(f).write_text(f"# {f}\n")
    if with_lock:
        _lock(tmp_path)
        _live(tmp_path)
    _git(tmp_path, "init", "-q")
    if commit:
        _git(tmp_path, "add", "-A")
        _git(tmp_path, "commit", "-qm", "setup")
    return tmp_path


def _lock(ws: Path, overrides=None, drop=()):
    """The .migration/playbooks.lock.json install-dbx-factory writes after a sync: every repo
    playbook's sha256 plus this run's installed_at."""
    entries = {m: {"sha256": sha, "repo_file": f, "installed_at": "2026-01-01T00:00:00Z"}
               for m, (f, sha) in doctor._repo_playbooks(PLUGIN_ROOT).items()}
    for macro, sha in (overrides or {}).items():
        entries.setdefault(macro, {"repo_file": "gone.md", "installed_at": "2026-01-01T00:00:00Z"})
        entries[macro]["sha256"] = sha
    for macro in drop:
        entries.pop(macro, None)
    (ws / doctor.PLAYBOOKS_LOCK).write_text(json.dumps(entries, indent=2, sort_keys=True))
    return entries


def _live(ws: Path, overrides=None, drop=(), duplicate=None, age_minutes=0):
    """The .migration/live_playbooks.json the orchestrator writes with devin_playbook_manage
    right before the doctor: one record per repo macro with the repo file body. `overrides`
    replaces a macro's content, `drop` removes macros, `duplicate` appends a second record
    for that macro, `age_minutes` backdates the file mtime."""
    playbooks_dir = PLUGIN_ROOT / "skills" / "install-dbx-factory" / "playbooks"
    records = []
    for macro, (f, _sha) in doctor._repo_playbooks(PLUGIN_ROOT).items():
        if macro in drop:
            continue
        records.append({"macro": macro, "playbook_id": f"playbook-{macro.lstrip('!')}",
                        "content": (overrides or {}).get(macro, (playbooks_dir / f).read_text())})
    if duplicate:
        records.append({"macro": duplicate, "playbook_id": "playbook-extra-copy",
                        "content": "stale body\n"})
    live = ws / doctor.LIVE_PLAYBOOKS
    live.write_text(json.dumps(records, indent=2))
    if age_minutes:
        t = time.time() - age_minutes * 60
        os.utime(live, (t, t))
    return records


def by_id(report):
    return {c["id"]: c for c in report["checks"]}


def sub_by_id(report, row_id):
    return {c["id"]: c for c in by_id(report)[row_id]["data"]["sub_results"]}


def probed(ws):
    """What a session does before the live probe: one doctor run (report written) issues the nonce the probe echoes."""
    first = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps(first))
    return "blocked:" + sub_by_id(first, "hook_guard")["hook_platform_loaded"]["data"]["probe_nonce"]


def test_probe_command_is_blocked_by_guard_and_harmless_otherwise():
    cfg = dbx_guard.GuardConfig.from_dict({"catalogs": ["mig_cat"]})
    probe = doctor.HOOK_PROBE_COMMAND.format(nonce="c0ffee42")
    v = dbx_guard.evaluate(probe, cfg)
    # the block reason echoes the probe catalog, nonce included: that echo is what the session
    # hands back to the doctor, so a `blocked` claim cannot be typed without having seen it
    assert v.decision == "block" and "__dbx_guard_probe__c0ffee42" in v.reason
    assert probe.startswith("echo ")


def test_offline_run_passes_every_local_check_but_is_never_ready(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", probed(ws), None, no_databricks=True)
    c = by_id(report)
    assert not [x for x in report["checks"] if x["status"] == "fail"]
    # an unverified identity can never certify a wave, however the check was skipped
    assert not report["ready"] and report["blocking"] == ["databricks_identity=skipped"]
    assert c["workspace"]["status"] == "ok"
    assert c["workspace"]["data"]["stop_mode"] == "hard"
    assert sub_by_id(report, "allowed_targets")["allowed_targets"]["status"] == "ok"
    assert c["allowed_targets"]["data"]["catalogs"] == ["mig_cat"]
    assert c["hook_guard"]["status"] == "ok"
    assert sub_by_id(report, "hook_guard")["hook_platform_loaded"]["status"] == "ok"
    assert c["recon_harness"]["status"] == "ok"
    assert c["databricks_identity"]["status"] == "skipped"


def test_unknown_probe_is_unverified_and_carries_command(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    c = by_id(report)
    sub = sub_by_id(report, "hook_guard")
    assert c["hook_guard"]["status"] == "unverified"
    assert sub["hook_platform_loaded"]["status"] == "unverified"
    nonce = c["hook_guard"]["data"]["probe_nonce"]
    assert re.fullmatch(r"[0-9a-f]{8}", nonce)
    assert sub["hook_platform_loaded"]["data"]["probe_command"] == doctor.HOOK_PROBE_COMMAND.format(nonce=nonce)
    assert "blocked:<nonce>" in sub["hook_platform_loaded"]["detail"]
    assert not report["ready"]
    assert report["blocking"] == ["hook_guard=unverified", "databricks_identity=skipped"]


def test_platform_probe_reuses_a_pending_nonce_until_accepted(tmp_path):
    ws = make_workspace(tmp_path)
    first = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps(first))
    nonce = by_id(first)["hook_guard"]["data"]["probe_nonce"]
    saved_nonce, issued_at = (ws / doctor.HOOK_PROBE_NONCE).read_text().strip().split()
    assert saved_nonce == nonce and issued_at.isdigit()
    second = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    assert by_id(second)["hook_guard"]["data"]["probe_nonce"] == nonce
    ok = doctor.run(ws, PLUGIN_ROOT, "orchestrator", f"blocked:{nonce}", None, True)
    assert by_id(ok)["hook_guard"]["status"] == "ok"
    assert by_id(ok)["hook_guard"]["data"]["probe_nonce"] == nonce
    again = doctor.run(ws, PLUGIN_ROOT, "orchestrator", f"blocked:{nonce}", None, True)
    assert by_id(again)["hook_guard"]["status"] == "ok"
    # a workspace with no prior report has no nonce to match, so nothing verifies it yet
    fresh = doctor.run(make_workspace(tmp_path / "fresh"), PLUGIN_ROOT, "orchestrator", f"blocked:{nonce}", None, True)
    assert sub_by_id(fresh, "hook_guard")["hook_platform_loaded"]["status"] == "unverified"


def test_one_nonce_serves_functional_and_platform_probe(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    nonce = by_id(report)["hook_guard"]["data"]["probe_nonce"]
    sub = sub_by_id(report, "hook_guard")
    assert f"__dbx_guard_probe__{nonce}" in sub["hook_guard_functional"]["detail"]
    assert sub["hook_platform_loaded"]["data"]["probe_command"] == \
        doctor.HOOK_PROBE_COMMAND.format(nonce=nonce)
    second = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    sub2 = sub_by_id(second, "hook_guard")
    assert f"__dbx_guard_probe__{nonce}" in sub2["hook_guard_functional"]["detail"]
    assert sub2["hook_platform_loaded"]["data"]["probe_command"] == \
        doctor.HOOK_PROBE_COMMAND.format(nonce=nonce)


def test_failed_hook_report_without_probe_nonce_does_not_crash(tmp_path):
    ws = make_workspace(tmp_path)
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps({
        "checks": [{"id": "hook_guard", "status": "fail", "data": {}}],
    }))
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    row = sub_by_id(report, "hook_guard")["hook_platform_loaded"]
    assert row["status"] == "unverified"
    assert re.fullmatch(r"[0-9a-f]{8}", row["data"]["probe_nonce"])


def test_expired_pending_nonce_is_not_accepted(tmp_path):
    ws = make_workspace(tmp_path)
    old_nonce = "deadbeef"
    (ws / doctor.HOOK_PROBE_NONCE).write_text(
        f"{old_nonce} {int(doctor.time.time()) - doctor.HOOK_PROBE_NONCE_TTL - 1}\n")
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", f"blocked:{old_nonce}", None, True)
    row = sub_by_id(report, "hook_guard")["hook_platform_loaded"]
    assert row["status"] == "unverified"
    assert row["data"]["probe_nonce"] != old_nonce
    saved_nonce, issued_at = (ws / doctor.HOOK_PROBE_NONCE).read_text().strip().split()
    assert saved_nonce == row["data"]["probe_nonce"] and issued_at.isdigit()


def test_expired_report_nonce_is_not_accepted(tmp_path):
    ws = make_workspace(tmp_path)
    old_nonce = "deadbeef"
    old_report = {
        "generated_at": "2000-01-01T00:00:00Z",
        "checks": [{"id": "hook_guard", "status": "unverified",
                    "data": {"probe_nonce": old_nonce}}],
    }
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps(old_report))
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", f"blocked:{old_nonce}", None, True)
    row = sub_by_id(report, "hook_guard")["hook_platform_loaded"]
    assert row["status"] == "unverified"
    assert row["data"]["probe_nonce"] != old_nonce


def test_issued_nonce_reads_hook_guard_row(tmp_path):
    ws = make_workspace(tmp_path)
    nonce = "deadbeef"
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps({
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "checks": [{"id": "hook_guard", "status": "unverified",
                    "data": {"probe_nonce": nonce}}],
    }))
    assert doctor._issued_nonce(ws) == nonce


def test_run_core_rows_are_ten(tmp_path):
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    optional = {"lakebase_branch_create", "lakebase_target_grants", "analytical_target_grants"}
    assert [c["id"] for c in report["checks"] if c["id"] not in optional] == [
        "workspace", "allowed_targets", "allowlist_committed", "playbooks_in_sync", "hook_guard",
        "official_databricks_plugin", "recon_harness", "recon_family_supported", "type_map_audit",
        "delete_evidence", "source_principal_read_only", "dictionary_readable",
        "named_secrets_exist", "databricks_identity",
    ]


def test_security_controls_are_role_specific():
    assert doctor.security_controls("child") == ("hook_guard_functional", "databricks_identity")
    assert doctor.security_controls("orchestrator") == doctor.security_controls("setup") \
        == doctor.SECURITY_CONTROLS
    assert len(doctor.SECURITY_CONTROLS) == 3


def test_merged_row_status_rules():
    ok = doctor.Check("ok", "ok", "ok", {"ok": 1})
    fail = doctor.Check("fail", "fail", "fail", {"fail": 2})
    warn = doctor.Check("warn", "warn", "warn", {"warn": 3})
    security = doctor.Check("hook_platform_loaded", "unverified", "security", {"security": 4})
    assert doctor._merge("row", [ok, fail]).status == "fail"
    assert doctor._merge("row", [ok, warn]).status == "warn"
    assert doctor._merge("row", [doctor.Check("hook_platform_loaded", "ok", "ok"), warn]).status == "warn"
    assert doctor._merge("row", [security, warn]).status == "unverified"
    merged = doctor._merge("row", [ok, warn])
    assert [s["id"] for s in merged.data["sub_results"]] == ["ok", "warn"]
    assert merged.data["ok"] == 1 and merged.data["warn"] == 3


def test_stop_mode_datum_on_workspace_row(tmp_path):
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert by_id(report)["workspace"]["data"]["stop_mode"] in ("soft", "hard")


def test_cli_rejects_a_bare_blocked_claim(tmp_path):
    ws = make_workspace(tmp_path)
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--plugin-root",
                        str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 2 and "blocked:<nonce>" in r.stderr
    assert not (ws / ".migration" / "09_capabilities.json").exists()


def test_human_identity_is_not_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "warn", "pat (env)"),
        doctor.Check("databricks_identity", "warn", "authenticated as someone@example.com (user)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", probed(ws), None, no_databricks=False)
    assert report["summary"].get("fail", 0) == 0
    assert not report["ready"] and report["blocking"] == ["databricks_identity=warn"]


def test_human_identity_redacts_username_and_names_service_principal_secrets(monkeypatch):
    _fake_cli(monkeypatch, {"userName": "someone@example.com"},
              {"status": "success", "details": {"host": "https://adb-1.azuredatabricks.net"}})
    row = by_id({"checks": [asdict(c) for c in doctor.check_databricks(None)]})["databricks_identity"]
    assert row["data"]["userName"] == "<human user (redacted)>"
    assert "someone@example.com" not in row["detail"]
    assert "DATABRICKS_CLIENT_ID" in row["detail"]
    assert "DATABRICKS_CLIENT_SECRET" in row["detail"]
    assert "DATABRICKS_HOST" in row["detail"]


def test_lakebase_rows_are_absent_without_flags(tmp_path):
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert not {c["id"] for c in report["checks"]} & {
        "lakebase_branch_create", "lakebase_target_grants", "analytical_target_grants"
    }


@pytest.mark.parametrize(("stderr", "needle"), [
    ("", "branch created and deleted"),
    ("The user is not authorized to make the request", "Can Manage on Lakebase project loan-servicing"),
    ("BadRequest Branches with an expiration date cannot have child branches", "parent branch primary has an expiry"),
    ("other secret=password123 error", "<redacted>"),
])
def test_lakebase_branch_create_classifies_cli_results(monkeypatch, stderr, needle):
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")
    calls = []

    def fake_run(cmd, timeout=0):
        calls.append(cmd)
        return (0, "{}", "") if not stderr else (1, "", stderr)

    monkeypatch.setattr(doctor, "_run", fake_run)
    row = doctor.check_lakebase_branch_create("loan-servicing", "primary")
    assert needle in row.detail
    assert row.status == ("ok" if not stderr else "fail")
    if not stderr:
        assert len(calls) == 2 and calls[1][2:] == ["delete-branch", calls[1][3], "--purge"]


class LakebaseGrantCursor:
    def __init__(self, db_create=False, schema_exists=False, schema_create=False):
        self.db_create = db_create
        self.schema_exists = schema_exists
        self.schema_create = schema_create
        self.rows = []

    def execute(self, sql, params=()):
        low = sql.lower()
        if "information_schema.schemata" in low:
            self.rows = [(1,)] if self.schema_exists else []
        elif "has_schema_privilege" in low:
            self.rows = [(self.schema_create,)]
        elif "current_user" in low:
            self.rows = [("migration_role", "databricks_postgres", self.db_create)]
        else:
            self.rows = [(self.schema_create,)]

    def fetchone(self):
        return self.rows[0] if self.rows else None


class LakebaseGrantConnection:
    def __init__(self, **kwargs):
        self.cursor_obj = LakebaseGrantCursor(**kwargs)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


@pytest.mark.parametrize(("kwargs", "status"), [
    ({"db_create": True}, "ok"),
    ({"db_create": False}, "fail"),
    ({"db_create": False, "schema_exists": True, "schema_create": True}, "ok"),
])
def test_lakebase_target_grants(monkeypatch, kwargs, status):
    monkeypatch.setenv("LAKEBASE_DSN", "postgresql://redacted")
    conn = LakebaseGrantConnection(**kwargs)
    row = doctor.check_lakebase_target_grants("LAKEBASE_DSN", "app", connect=lambda dsn: conn)
    assert row.status == status
    if status == "fail":
        assert "GRANT CREATE ON DATABASE databricks_postgres TO migration_role" in row.detail


def test_lakebase_target_grants_reports_missing_secret(monkeypatch):
    monkeypatch.delenv("LAKEBASE_DSN", raising=False)
    row = doctor.check_lakebase_target_grants("LAKEBASE_DSN")
    assert row.status == "fail" and row.detail == "secret LAKEBASE_DSN is not set in this shell"


def test_lakebase_target_grants_requires_psycopg(monkeypatch):
    monkeypatch.setenv("LAKEBASE_DSN", "postgresql://redacted")
    monkeypatch.setitem(sys.modules, "psycopg", None)
    row = doctor.check_lakebase_target_grants("LAKEBASE_DSN")
    assert row.status == "fail"
    assert "psycopg is unavailable" in row.detail


def test_lakebase_target_grants_requires_schema_create_when_schema_exists(monkeypatch):
    monkeypatch.setenv("LAKEBASE_DSN", "postgresql://redacted")
    conn = LakebaseGrantConnection(db_create=True, schema_exists=True, schema_create=False)
    row = doctor.check_lakebase_target_grants("LAKEBASE_DSN", "app", connect=lambda dsn: conn)
    assert row.status == "fail"
    assert "GRANT CREATE ON SCHEMA app TO migration_role" in row.detail


def test_lakebase_target_grants_redacts_connection_error(monkeypatch):
    monkeypatch.setenv("LAKEBASE_DSN", "LAKEBASE_DSN_VALUE")

    def connect(_dsn):
        raise RuntimeError("postgresql://role:password123@db.example/target")

    row = doctor.check_lakebase_target_grants("LAKEBASE_DSN", connect=connect)
    assert row.status == "fail"
    assert row.detail == ("connection to LAKEBASE_DSN failed (RuntimeError); "
                          "check the DSN secret and network path")
    assert "password123" not in row.detail


def _analytical_cli(monkeypatch, *, schema_exists=True, owner="owner@example.com",
                    catalog_privileges=("USE_CATALOG",), schema_privileges=("USE_SCHEMA",),
                    schema_error="", catalog_owner=None, schema_grants_rc=0, schema_grants_err=""):
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")

    def fake_run(cmd, timeout=0):
        operation = tuple(cmd[1:3])
        if operation == ("current-user", "me"):
            return 0, json.dumps({"applicationId": "2e90bc1d-e9a1-4703-8c48-ad28ebb1864d"}), ""
        if operation == ("schemas", "get"):
            if schema_exists:
                return 0, json.dumps({"owner": owner}), ""
            return 1, "", schema_error or "SCHEMA_DOES_NOT_EXIST"
        if operation == ("catalogs", "get"):
            return 0, json.dumps({"owner": catalog_owner}), ""
        if operation == ("grants", "get-effective"):
            resource_type = cmd[3]
            if resource_type == "schema" and schema_grants_rc:
                return schema_grants_rc, "{}", schema_grants_err
            privileges = schema_privileges if resource_type == "schema" else catalog_privileges
            return 0, json.dumps({"privilege_assignments": [
                {"principal": "2e90bc1d-e9a1-4703-8c48-ad28ebb1864d",
                 "privileges": [{"privilege": p} for p in privileges]}
            ]}), ""
        raise AssertionError(cmd)

    monkeypatch.setattr(doctor, "_run", fake_run)


def test_analytical_target_schema_absent_with_catalog_grants(monkeypatch):
    _analytical_cli(monkeypatch, schema_exists=False, catalog_privileges=("USE_CATALOG", "CREATE_SCHEMA"))
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "ok"
    assert "does not exist" in row.detail and "no grants needed" in row.detail
    assert row.data == {
        "schema": "tsql_demo.loan_servicing",
        "principal": "2e90bc1d-e9a1-4703-8c48-ad28ebb1864d",
        "owner": None,
        "catalog_owner": None,
        "exists": False,
        "missing": [],
    }


def test_analytical_target_schema_absent_missing_create_schema(monkeypatch):
    _analytical_cli(monkeypatch, schema_exists=False, catalog_privileges=("USE_CATALOG",))
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "fail"
    assert "GRANT CREATE SCHEMA ON CATALOG `tsql_demo` TO `2e90bc1d-e9a1-4703-8c48-ad28ebb1864d`" in row.detail
    assert "USE CATALOG" not in row.detail


def test_analytical_target_schema_unreadable_returns_schema_grants(monkeypatch):
    _analytical_cli(
        monkeypatch,
        schema_exists=False,
        schema_error=("Error: User does not have USE SCHEMA on Schema 'tsql_demo.default'. "
                      "Host: https://workspace.example Auth type: oauth Next steps: grant access"),
        schema_grants_rc=1,
        schema_grants_err="permission denied",
        catalog_privileges=("USE_CATALOG",),
    )
    row = doctor.check_analytical_target_grants("tsql_demo.default")
    assert row.status == "fail"
    assert "GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `tsql_demo`.`default` TO `" in row.detail
    assert "owner is unknown" in row.detail


def test_analytical_target_schema_absent_catalog_owner_has_implicit_privileges(monkeypatch):
    principal = "2e90bc1d-e9a1-4703-8c48-ad28ebb1864d"
    _analytical_cli(monkeypatch, schema_exists=False, catalog_privileges=(), catalog_owner=principal)
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "ok"
    assert row.data["catalog_owner"] == principal


def test_analytical_target_schema_owned_by_principal(monkeypatch):
    _analytical_cli(monkeypatch, owner="2E90BC1D-E9A1-4703-8C48-AD28EBB1864D",
                    catalog_privileges=("USE_CATALOG",))
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "ok"
    assert row.detail == ("schema tsql_demo.loan_servicing is owned by "
                          "2e90bc1d-e9a1-4703-8c48-ad28ebb1864d")
    assert row.data["owner"] == row.data["principal"]


def test_analytical_target_schema_owner_missing_catalog_use(monkeypatch):
    _analytical_cli(monkeypatch, owner="2E90BC1D-E9A1-4703-8C48-AD28EBB1864D",
                    catalog_privileges=(), catalog_owner="owner@example.com")
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "fail"
    assert "GRANT USE CATALOG ON CATALOG `tsql_demo` TO `" in row.detail
    assert row.data["missing"] == ["USE_CATALOG"]


def test_analytical_target_schema_requires_all_non_owner_privileges(monkeypatch):
    _analytical_cli(monkeypatch, schema_privileges=("USE_SCHEMA",), owner="owner@example.com")
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "fail"
    assert "GRANT CREATE TABLE, MODIFY, SELECT ON SCHEMA `tsql_demo`.`loan_servicing` TO `" in row.detail
    assert "owner is owner@example.com" in row.detail


def test_analytical_target_schema_all_privileges_is_ok(monkeypatch):
    _analytical_cli(monkeypatch, schema_privileges=("ALL_PRIVILEGES",))
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "ok"
    assert "can create and write tables" in row.detail


def test_analytical_target_schema_quotes_delimited_identifiers(monkeypatch):
    _analytical_cli(monkeypatch, schema_privileges=("USE_SCHEMA",), owner="owner@example.com")
    row = doctor.check_analytical_target_grants("migration-prod.loan-servicing")
    assert row.status == "fail"
    assert "ON SCHEMA `migration-prod`.`loan-servicing`" in row.detail


def test_analytical_target_schema_requires_catalog_schema_shape(monkeypatch):
    row = doctor.check_analytical_target_grants("tsql_demo")
    assert row.status == "fail" and row.detail == "expected CATALOG.SCHEMA"


def test_analytical_target_schema_redacts_cli_errors(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")

    def fake_run(cmd, timeout=0):
        if tuple(cmd[1:3]) == ("current-user", "me"):
            return 1, "", "request failed password=secret-value-123456789"
        raise AssertionError(cmd)

    monkeypatch.setattr(doctor, "_run", fake_run)
    row = doctor.check_analytical_target_grants("tsql_demo.loan_servicing")
    assert row.status == "fail"
    assert "<redacted>" in row.detail
    assert "secret-value" not in row.detail


def test_analytical_schema_is_skipped_offline(tmp_path):
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        analytical_schema="a.b")
    row = by_id(report)["analytical_target_grants"]
    assert row["status"] == "skipped" and row["detail"] == "--no-databricks"


def test_service_principal_with_advisory_warns_is_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "ok", "oauth-m2m (env)"),
        doctor.Check("databricks_identity", "ok", "authenticated as 1234-sp (service principal)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", probed(ws), None, no_databricks=False)
    assert report["ready"] and report["blocking"] == []


def test_driver_probe_requires_full_module(monkeypatch):
    import importlib.util
    real = importlib.util.find_spec

    def fake(name):
        if name == "databricks.sql":
            raise ModuleNotFoundError("No module named 'databricks.sql'")
        if name == "databricks":
            return real("json")
        return real(name)

    monkeypatch.setattr(importlib.util, "find_spec", fake)
    c = doctor.check_drivers()
    assert c.data["drivers"]["databricks"] is False and c.status == "warn"
    assert "databricks-sql-connector missing" in c.detail


def test_driver_probe_names_match_adapter_imports():
    """The probed modules are exactly the ones the harness adapter lazily imports (Lakebase is
    psycopg 3); a wrong or stale name would report a working environment as missing a driver."""
    adapters = (PLUGIN_ROOT / "skills" / "data-reconciliation" / "harness" / "recon" / "adapters.py").read_text()
    imported = set(re.findall(r"^\s+import ([A-Za-z_][\w.]*)  # lazy", adapters, re.MULTILINE))
    imported |= {f"{a}.{b}" for a, b in re.findall(r"^\s+from ([\w.]+) import (\w+)  # lazy", adapters, re.MULTILINE)}
    assert set(doctor.DRIVERS.values()) == imported
    assert doctor.DRIVERS["postgres"] == "psycopg"


def test_not_blocked_probe_fails(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "not-blocked", None, True)
    assert not report["ready"]
    assert by_id(report)["hook_guard"]["status"] == "fail"


def test_generated_and_folded_files_are_not_required(tmp_path):
    ws = make_workspace(tmp_path, omit=("02_glossary.md", "05_progress.md"))
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "ok"
    (ws / ".migration" / "00_context.md").write_text("# no mode here\n\n## Glossary\n- term: meaning\n")
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert sub_by_id({"checks": [c["workspace"]]}, "workspace")["stop_mode"]["status"] == "fail"


def test_workspace_requires_glossary_section(tmp_path):
    ws = make_workspace(tmp_path, omit=("02_glossary.md",))
    (ws / ".migration" / "00_context.md").write_text("stop_mode: hard\n")
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail"
    assert "Glossary" in c["workspace"]["detail"]

    (ws / ".migration" / "00_context.md").write_text(
        "stop_mode: hard\n\n## Glossary\n- term: meaning\n"
    )
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "ok"


def test_workspace_accepts_legacy_glossary_file(tmp_path):
    ws = make_workspace(tmp_path)
    (ws / ".migration" / "02_glossary.md").write_text("# legacy glossary\n")
    (ws / ".migration" / "00_context.md").write_text("stop_mode: hard\n")
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "ok"


def test_workspace_rejects_glossary_directory(tmp_path):
    ws = make_workspace(tmp_path, omit=("02_glossary.md",))
    (ws / ".migration" / "02_glossary.md").mkdir()
    (ws / ".migration" / "00_context.md").write_text("stop_mode: hard\n")
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail"
    assert "Glossary" in c["workspace"]["detail"]


def test_workspace_rejects_context_directory(tmp_path):
    ws = make_workspace(tmp_path, omit=("00_context.md",))
    (ws / ".migration" / "00_context.md").mkdir()
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail"
    assert "00_context.md" in c["workspace"]["detail"]


def test_setup_outputs_tolerances_json_is_required(tmp_path):
    ws = make_workspace(tmp_path, omit=("03_recon_tolerances.json",))
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail"
    assert c["workspace"]["data"]["missing"] == ["03_recon_tolerances.json"]


@pytest.mark.parametrize("who, expected", [
    ({"userName": "someone@example.com", "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"]}, False),
    ({"userName": "svc_migration", "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"]}, False),  # no '@' is not an SP
    ({"displayName": "Local Admin"}, False),
    ({"userName": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"}, True),  # application id as userName
    ({"userName": "svc", "applicationId": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"}, True),
    ({"displayName": "mig-sp", "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"]}, True),
])
def test_identity_classification_needs_positive_service_principal_evidence(who, expected):
    _, is_sp = doctor.classify_identity(who)
    assert is_sp is expected


def test_warn_mode_and_empty_legacy_sources_are_warned(tmp_path):
    ws = make_workspace(tmp_path, allowed={"catalogs": ["mig_cat"], "guard_mode": "warn"})
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["allowed_targets"]["status"] == "warn"
    assert "legacy_sources empty" in c["allowed_targets"]["detail"]


def test_bad_allowlist_fails(tmp_path):
    ws = make_workspace(tmp_path, allowed={"catalogs": []})
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert by_id(report)["allowed_targets"]["status"] == "fail" and not report["ready"]


def test_cli_writes_capabilities_json_and_exit_codes(tmp_path):
    ws = make_workspace(tmp_path)
    argv = [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--plugin-root", str(PLUGIN_ROOT),
            "--no-databricks"]
    r = subprocess.run(argv, capture_output=True, text=True, check=False)
    assert r.returncode == 1, r.stdout + r.stderr
    cap = json.loads((ws / ".migration" / "09_capabilities.json").read_text())
    nonce = by_id(cap)["hook_guard"]["data"]["probe_nonce"]
    r = subprocess.run([*argv, "--hook-probe-result", f"blocked:{nonce}"], capture_output=True, text=True, check=False)
    assert r.returncode == 1, r.stdout + r.stderr  # offline: identity unverified, so not ready
    cap = json.loads((ws / ".migration" / "09_capabilities.json").read_text())
    assert cap["schema"] == "dbx-migration-factory/capabilities/1" and cap["ready"] is False
    assert cap["blocking"] == ["databricks_identity=skipped"] and cap["identity"] is None
    assert "ready=False" in r.stdout and "databricks_identity=skipped" in r.stdout

    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "not-blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 1


def test_unverified_probe_prints_the_command_as_the_last_line(tmp_path, capsys):
    ws = make_workspace(tmp_path)
    doctor.main(["--workspace", str(ws), "--plugin-root", str(PLUGIN_ROOT),
                 "--no-databricks", "--out", "-"])
    out = capsys.readouterr().out
    nonce = (ws / doctor.HOOK_PROBE_NONCE).read_text().split()[0]
    assert out.splitlines()[-1] == (
        "PROBE_COMMAND (run in the lead session's exec tool, not a sidekick shell): "
        f"{doctor.HOOK_PROBE_COMMAND.format(nonce=nonce)}")
    child = make_workspace(tmp_path / "child")
    _unit_mapping(child, "loans", evidence=False)
    doctor.main(["--workspace", str(child), "--plugin-root", str(PLUGIN_ROOT), "--no-databricks",
                 "--role", "child", "--unit", "loans", "--out", "-"])
    assert "PROBE_COMMAND" not in capsys.readouterr().out


def test_no_workspace_does_not_write(tmp_path):
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(tmp_path),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks"], capture_output=True, text=True)
    assert r.returncode == 1
    assert not (tmp_path / ".migration").exists()


@pytest.mark.parametrize("secretish,expected", [
    ("Error: token dapi1234567890abcdef1234567890abcdef expired", "<redacted>"),
    ("Error: cannot configure default credentials", "credentials"),
])
def test_redact(secretish, expected):
    assert expected in doctor._redact(secretish)


@pytest.mark.parametrize("secretish,leak", [
    ("DRIVER={ODBC Driver 18};SERVER=h;UID=sa;PWD=Sup3r$ecret!;Encrypt=no", "Sup3r$ecret!"),
    ("password=hunter2 rejected", "hunter2"),
    ("token: 'abc' invalid", "abc"),
    ("HTTP 401 for Authorization: Bearer eyJhbGciOi.payload on host", "eyJhbGciOi"),
    ("connection to postgres://mig:hunter2@db.host:5432/lake failed", "hunter2"),
    ("client_secret=\"sh0rt\" expired", "sh0rt"),
])
def test_redact_named_secrets_and_dsn_userinfo(secretish, leak):
    red = doctor._redact(secretish)
    assert leak not in red and "<redacted>" in red


def test_redact_keeps_plain_error_text():
    msg = "login failed for user 'sa' (password mismatch); token_count=3"
    assert doctor._redact(msg) == msg


# ------------------------------------------------------------------ delete evidence preflight

def _mapping(tmp_path: Path, *, evidence=True, key=("Id",), root_where=None) -> Path:
    obj = {"object": "loans", "root_table": "raw.loans",
           "key": {"source": list(key), "target": [k.lower() for k in key]},
           "fields": [{"source": "Id", "target": "id"}],
           "delete_evidence": {"kind": "sqlserver_cdc", "capture": "raw_loans",
                               "applied_position": {"table": "cdc_checkpoint", "column": "lsn"}}}
    if root_where:
        obj["root_where"] = root_where
    if not evidence:
        del obj["delete_evidence"]
    p = tmp_path / "mapping.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": "m1", "objects": [obj]}))
    return p


def _unit_mapping(ws: Path, unit: str, **kw) -> Path:
    """The unit's mapping where the plan playbook writes it: .migration/units/<id>/mapping_spec.json."""
    p = _mapping(ws / ".migration" / "units" / unit, **kw)
    return p.rename(p.with_name("mapping_spec.json"))


_HELP_COLUMNS = ("source_schema", "source_table", "capture_instance", "object_id", "source_object_id",
                 "start_lsn", "end_lsn", "supports_net_changes", "has_drop_pending", "role_name", "index_name",
                 "filegroup_name", "create_date", "index_column_list", "captured_column_list")
LSN = bytes(9) + b"\x01"


class FakeCdcConn:
    """A SQL Server seen by a *capture-level* reader: SELECT on the captured columns of the source
    table (or the gating role), EXECUTE on the generated cdc.fn_cdc_get_all_changes_<capture>,
    nothing on the cdc schema itself (no cdc.change_tables / cdc.captured_columns, HAS_PERMS_BY_NAME
    on the schema is 0). `captures` maps each capture this identity may read to (role_name,
    captured columns), the shape sys.sp_cdc_help_change_data_capture reports. Records every statement
    so a test can prove nothing else (in particular no sp_cdc_enable_*) ran."""

    def __init__(self, *, db_enabled=1, captures=None, max_lsn=LSN, fn_errors=None, case_sensitive=False):
        self.db_enabled = db_enabled
        self.captures = {"raw_loans": (None, ["Id", "Amount"])} if captures is None else captures
        self.max_lsn, self.fn_errors = max_lsn, fn_errors or {}
        self.case_sensitive = case_sensitive  # database collation: identifiers resolve CI unless set
        self.statements: list[str] = []
        self.probes: list[tuple[str, tuple]] = []

    def cursor(self):
        return self

    def execute(self, sql, *params):
        self.statements.append(sql)
        low = sql.lower()
        if "is_cdc_enabled" in low:
            self._rows = [(self.db_enabled,)]
        elif "has_perms_by_name" in low:
            self._rows = [(0,)]
        elif "cdc.change_tables" in low or "cdc.captured_columns" in low:
            raise RuntimeError("The SELECT permission was denied on the object 'change_tables'")
        elif "sp_cdc_help_change_data_capture" in low:
            self._rows = [("raw", cap.split("_", 1)[1], cap, 1, 2, LSN, None, 0, 0, role, "PK", None, None,
                           "[Id]", ", ".join(f"[{c}]" for c in cols))
                          for cap, (role, cols) in self.captures.items()]
        elif "fn_cdc_get_max_lsn" in low:
            self._rows = [(self.max_lsn,)]
        elif "fn_cdc_get_all_changes_" in low:
            capture = re.search(r"fn_cdc_get_all_changes_(\w+)", sql).group(1)
            self.probes.append((sql, params))
            if capture in self.fn_errors:
                raise RuntimeError(self.fn_errors[capture])
            fold = (lambda s: s) if self.case_sensitive else str.casefold
            known = {fold(c): cols for c, (_, cols) in self.captures.items()}
            if fold(capture) not in known:
                raise RuntimeError(f"Invalid object name 'cdc.fn_cdc_get_all_changes_{capture}'")
            projected = re.search(r"SELECT TOP \(\d+\) (.+?) FROM", sql).group(1).split(", ")
            for col in projected:
                if fold(col) not in {fold(c) for c in known[fold(capture)]}:
                    raise RuntimeError(f"Invalid column name '{col}'")
            self._rows = []
        else:
            raise AssertionError(f"unexpected statement: {sql}")
        return self

    @property
    def description(self):
        return [(c,) for c in _HELP_COLUMNS]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def test_delete_evidence_ok_when_none_declared(tmp_path):
    c = doctor.check_delete_evidence(_mapping(tmp_path, evidence=False), None, PLUGIN_ROOT)
    assert c.status == "ok" and "no object declares delete_evidence" in c.detail


def test_orchestrator_verifies_every_unit_mapping_in_the_workspace_itself(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    ws = make_workspace(tmp_path)
    # setup runs before a unit mapping exists: nothing to verify, and the row says so
    c = doctor.check_delete_evidence_all(ws, "orchestrator", [], [], None, PLUGIN_ROOT)
    assert c.status == "skipped" and "not applicable" in c.detail and "no unit mapping" in c.detail
    # once unit mappings exist (plan re-run before a wave) the doctor finds and checks all of them;
    # nothing has to be typed, so nothing can be left out
    _unit_mapping(ws, "orders_load", evidence=False)
    _unit_mapping(ws, "payments")
    conn = FakeCdcConn(db_enabled=0)
    c = doctor.check_delete_evidence_all(ws, "orchestrator", [], [], "LEGACY_ODBC", PLUGIN_ROOT,
                                         connect=lambda dsn: conn)
    assert c.status == "fail" and c.data["units"] == ["orders_load", "payments"]
    assert c.data["mappings"]["orders_load"]["status"] == "ok"
    assert c.data["mappings"]["payments"]["status"] == "fail" and "CDC is not enabled" in c.detail
    # an ad-hoc --mapping (a candidate spec at setup) is verified on top, never instead
    extra = _mapping(tmp_path / "candidate", evidence=False)
    conn = FakeCdcConn()
    c = doctor.check_delete_evidence_all(ws, "orchestrator", [], [extra], "LEGACY_ODBC", PLUGIN_ROOT,
                                         connect=lambda dsn: conn)
    assert c.status == "ok" and set(c.data["mappings"]) == {"orders_load", "payments", str(extra)}
    assert conn.probes
    # --unit is a child's batch declaration; an orchestrator cannot use it to narrow the set
    c = doctor.check_delete_evidence_all(ws, "orchestrator", ["payments"], [], "LEGACY_ODBC", PLUGIN_ROOT,
                                         connect=lambda dsn: conn)
    assert c.status == "fail" and "--role child" in c.detail


def test_child_names_its_batch_and_every_unit_of_it_is_checked(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    _unit_mapping(ws, "payments")
    # a child without its batch has nothing the doctor can hold it to
    c = doctor.check_delete_evidence_all(ws, "child", [], [], None, PLUGIN_ROOT)
    assert c.status == "fail" and "--unit" in c.detail and "--source-secret" in c.detail
    # a unit named in the brief whose mapping was never handed off is a fail, not a shorter list
    c = doctor.check_delete_evidence_all(ws, "child", ["loans", "payments", "fees"], [], "LEGACY_ODBC",
                                         PLUGIN_ROOT, connect=lambda dsn: FakeCdcConn())
    assert c.status == "fail" and c.data["missing_units"] == ["fees"] and "fees" in c.detail
    # the mapping paths come from the unit ids, so a broken capture in any unit of the batch fails
    # the row even though the child never typed that path
    conn = FakeCdcConn(db_enabled=0)
    c = doctor.check_delete_evidence_all(ws, "child", ["loans", "payments"], [], "LEGACY_ODBC", PLUGIN_ROOT,
                                         connect=lambda dsn: conn)
    assert c.status == "fail" and c.data["units"] == ["loans", "payments"]
    assert c.data["mappings"]["loans"]["status"] == "ok"
    assert c.data["mappings"]["payments"]["status"] == "fail" and "CDC is not enabled" in c.detail
    conn = FakeCdcConn()
    c = doctor.check_delete_evidence_all(ws, "child", ["loans", "payments", "payments"],
                                         [ws / ".migration" / "units" / "payments" / "mapping_spec.json"],
                                         "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "ok" and len(conn.probes) == 1  # repeats and the same path as --mapping check once
    assert set(c.data["mappings"]) == {"loans", "payments"}


def test_child_non_security_failures_are_warnings_and_do_not_block(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path, with_lock=False)
    _unit_mapping(ws, "loans", evidence=False)
    monkeypatch.setattr(doctor, "check_harness",
                        lambda *a, **k: doctor.Check("recon_harness", "fail", "selftest rc=1"))
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "ok", "oauth-m2m (env)"),
        doctor.Check("databricks_identity", "ok", "authenticated as 1234-sp (service principal)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, False, units=["loans"])
    row = by_id(report)["recon_harness"]
    assert row["status"] == "warn" and "selftest rc=1" in row["detail"]
    assert by_id(report)["playbooks_in_sync"]["status"] == "warn"
    assert report["ready"] is True and report["blocking"] == []


def test_child_missing_unit_mapping_still_blocks(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, True, units=["nope"])
    rows = by_id(report)
    for rid in ("type_map_audit", "delete_evidence", "source_principal_read_only",
                "dictionary_readable"):
        assert rows[rid]["status"] == "fail" and rows[rid]["data"]["units_problem"] is True, rid
        assert f"{rid}=fail" in report["blocking"], rid
    assert report["ready"] is False


def test_child_blocks_when_a_security_control_is_missing(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    empty_plugin = tmp_path / "no_plugin"  # no hooks.json/dbx_guard.py: check_hooks returns only hooks_files=fail
    empty_plugin.mkdir()
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_cli", "fail", "databricks CLI not on PATH"),
    ])
    report = doctor.run(ws, empty_plugin, "child", "unknown", None, False, units=["loans"])
    assert "hook_guard_functional=missing" in report["blocking"]
    assert "databricks_identity=missing" in report["blocking"]
    assert report["ready"] is False


def test_child_malformed_unit_mapping_still_blocks(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    spec = _unit_mapping(ws, "loans")
    spec.write_text("{not json")
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "ok", "oauth-m2m (env)"),
        doctor.Check("databricks_identity", "ok", "authenticated as 1234-sp (service principal)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, False, units=["loans"],
                        source_family="sqlserver")
    rows = by_id(report)
    for rid in ("type_map_audit", "delete_evidence", "source_principal_read_only",
                "dictionary_readable"):
        assert rows[rid]["status"] == "fail" and rows[rid]["data"]["units_problem"] is True, rid
        assert f"{rid}=fail" in report["blocking"], rid
    assert report["ready"] is False


def test_child_writable_source_principal_blocks(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans")
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "ok", "oauth-m2m (env)"),
        doctor.Check("databricks_identity", "ok", "authenticated as 1234-sp (service principal)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    monkeypatch.setattr(doctor, "check_source_principal", lambda tables, family, secret:
                        doctor.Check("source_principal_read_only", "fail",
                                     "principal can INSERT on raw.loans"))
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, False, units=["loans"],
                        source_secret="LEGACY_ODBC", source_family="sqlserver")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "fail" and "source_principal_read_only=fail" in report["blocking"]
    assert report["ready"] is False
    # unverified stays advisory in a child
    monkeypatch.setattr(doctor, "check_source_principal", lambda tables, family, secret:
                        doctor.Check("source_principal_read_only", "unverified",
                                     "cannot reach the source"))
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, False, units=["loans"],
                        source_secret="LEGACY_ODBC", source_family="sqlserver")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "unverified"
    assert "source_principal_read_only=unverified" not in report["blocking"]


def test_delete_evidence_declared_needs_a_named_source_secret(tmp_path, monkeypatch):
    monkeypatch.delenv("LEGACY_ODBC", raising=False)
    c = doctor.check_delete_evidence(_mapping(tmp_path), None, PLUGIN_ROOT)
    assert c.status == "fail" and "--source-secret" in c.detail
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT)
    assert c.status == "fail" and "LEGACY_ODBC" in c.detail and "not set" in c.detail


def _read_only(statements):
    return all(s.lstrip().upper().startswith("SELECT") or "sp_cdc_help_change_data_capture" in s
               for s in statements) and not any("sp_cdc_enable" in s.lower() or "sp_cdc_disable" in s.lower()
                                                for s in statements)


def test_delete_evidence_ok_for_a_capture_level_reader_without_schema_wide_select(tmp_path, monkeypatch):
    # the identity cannot SELECT from the cdc schema (HAS_PERMS_BY_NAME = 0, cdc.change_tables
    # denied) yet reads its capture through the gating role / captured-column grant: that is the
    # access model the harness uses, so the doctor must pass it and must not ask for more
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x;Server=y;PWD=never-printed")
    conn = FakeCdcConn()
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    assert c.data == {"kind": "sqlserver_cdc", "captures": ["raw_loans"], "missing": [],
                      "missing_columns": {}, "unreadable": {}}
    assert "never-printed" not in c.detail
    assert not any("has_perms_by_name" in s.lower() for s in conn.statements)
    assert _read_only(conn.statements)
    # one bounded probe per capture: the generated function, over the empty (max, max] range,
    # selecting exactly the mapped key columns
    assert len(conn.probes) == 1
    sql, params = conn.probes[0]
    assert "cdc.fn_cdc_get_all_changes_raw_loans(" in sql and "__$operation = 1" in sql
    assert re.search(r"SELECT TOP \(\d+\) Id FROM", sql) and params == ((LSN, LSN),)


def test_delete_evidence_probe_carries_the_scope_predicate(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakeCdcConn()
    c = doctor.check_delete_evidence(_mapping(tmp_path, root_where="Amount > 0"), "LEGACY_ODBC", PLUGIN_ROOT,
                                     connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    assert "AND (Amount > 0)" in conn.probes[0][0]


def test_delete_evidence_identifiers_compare_as_the_server_resolves_them(tmp_path, monkeypatch):
    # CDC metadata keeps the declared spelling while a case-insensitive collation resolves the
    # mapping's spelling: the metadata comparison must not be stricter than the server, and the
    # probe (sent with the mapping's spelling) is what decides on a case-sensitive one
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakeCdcConn(captures={"Raw_Loans": (None, ["ID", "AMOUNT"])})
    c = doctor.check_delete_evidence(_mapping(tmp_path, key=("Id", "Amount")), "LEGACY_ODBC", PLUGIN_ROOT,
                                     connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    assert c.data["missing"] == [] and c.data["missing_columns"] == {}
    assert "fn_cdc_get_all_changes_raw_loans(" in conn.probes[0][0] and "Id, Amount FROM" in conn.probes[0][0]
    conn = FakeCdcConn(captures={"Raw_Loans": (None, ["ID", "AMOUNT"])}, case_sensitive=True)
    c = doctor.check_delete_evidence(_mapping(tmp_path, key=("Id", "Amount")), "LEGACY_ODBC", PLUGIN_ROOT,
                                     connect=lambda dsn: conn)
    assert c.status == "fail" and "raw_loans" in c.data["unreadable"]
    assert "Invalid object name" in c.detail


def test_delete_evidence_resolves_mapping_params_like_the_harness(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    mapping = _mapping(tmp_path, root_where="Amount > ${floor}")
    conn = FakeCdcConn()
    c = doctor.check_delete_evidence(mapping, "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "fail" and "${floor}" in c.detail and "--param floor" in c.detail
    assert conn.probes == []
    c = doctor.check_delete_evidence(mapping, "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn,
                                     params={"floor": "100"})
    assert c.status == "ok", c.detail
    assert "AND (Amount > 100)" in conn.probes[0][0]


def test_cli_param_uses_the_harness_rules(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    loans = _unit_mapping(ws, "loans", root_where="Amount > ${floor}")
    payments = _unit_mapping(ws, "payments")
    seen = {}

    def fake(m, s, root, connect=None, params=None):
        seen["params"] = params
        seen.setdefault("mappings", []).append(m)
        return doctor.Check("delete_evidence", "ok", "")

    monkeypatch.setattr(doctor, "check_delete_evidence", fake)
    argv = ["--workspace", str(ws), "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--role", "child",
            "--unit", "loans", "--unit", "payments", "--source-secret", "X", "--out", "-"]
    doctor.main([*argv, "--param", "floor=100", "--param", "day=2026-01-01"])
    assert seen["params"] == {"floor": "100", "day": "2026-01-01"}
    assert seen["mappings"] == [loans, payments]  # --unit repeats; paths resolved by the doctor
    for bad in ("floor", "floor=1; DROP TABLE x", "floor=1 OR 1=1", "floor='a'"):
        seen.clear()
        with pytest.raises(SystemExit) as e:
            doctor.main([*argv, "--param", bad])
        assert seen == {} and "--param" in str(e.value), bad


def test_delete_evidence_fails_when_a_capture_misses_a_mapped_key_column(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakeCdcConn(captures={"raw_loans": (None, ["Amount"])})
    c = doctor.check_delete_evidence(_mapping(tmp_path, key=("Id", "Amount")), "LEGACY_ODBC", PLUGIN_ROOT,
                                     connect=lambda dsn: conn)
    assert c.status == "fail"
    assert "raw_loans" in c.detail and "['Id']" in c.detail and "captured" in c.detail
    assert c.data["missing_columns"] == {"raw_loans": ["Id"]}
    assert conn.probes == []  # nothing to read when the key cannot be projected
    assert _read_only(conn.statements)


@pytest.mark.parametrize("kw, needle", [
    ({"db_enabled": 0}, "CDC is not enabled"),
    ({"captures": {"raw_payments": (None, ["Id"])}}, "raw_loans"),
    ({"fn_errors": {"raw_loans": "The EXECUTE permission was denied on the object 'fn_cdc_get_all_changes_raw_loans'"}},
     "EXECUTE permission was denied"),
    ({"fn_errors": {"raw_loans": "Invalid column name 'TenantId'"}}, "TenantId"),
    ({"max_lsn": None}, "no change has been captured"),
])
def test_delete_evidence_failures_name_the_source_side_gap(tmp_path, monkeypatch, kw, needle):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x;PWD=never-printed")
    conn = FakeCdcConn(**kw)
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "fail" and needle in c.detail and "never-printed" not in c.detail
    assert "sp_cdc_enable" not in c.detail.replace("never runs sp_cdc_enable", "")
    assert _read_only(conn.statements)


def test_delete_evidence_a_capture_not_visible_to_this_identity_is_named_as_unreadable(tmp_path, monkeypatch):
    # sp_cdc_help_change_data_capture lists only the captures the caller may read, so an absent
    # row is either no capture or no grant; both are the same customer-side finding
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakeCdcConn(captures={})
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "fail" and c.data["missing"] == ["raw_loans"]
    assert "not present" in c.detail and "readable" in c.detail
    assert conn.probes == []


def test_delete_evidence_connection_error_is_redacted(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")

    def boom(dsn):
        raise RuntimeError("login failed for PWD=hunter2token99 at server")

    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT, connect=boom)
    assert c.status == "fail" and "hunter2token99" not in c.detail


def test_doctor_source_never_enables_cdc():
    src = (SKILL / "doctor.py").read_text()
    assert not re.search(r"sp_cdc_enable|sp_cdc_disable", src.replace("never runs sp_cdc_enable_*", ""))


def test_run_includes_delete_evidence_and_a_failed_check_blocks(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_delete_evidence",
                        lambda m, s, root, connect=None, params=None:
                        doctor.Check("delete_evidence", "fail", "CDC is not enabled"))
    _unit_mapping(ws, "loans")
    report = doctor.run(ws, PLUGIN_ROOT, "child", probed(ws), None, True, units=["loans"],
                        source_secret="LEGACY_ODBC")
    row = by_id(report)["delete_evidence"]
    assert row["status"] == "warn" and "CDC is not enabled" in row["detail"]
    assert "delete_evidence=fail" not in report["blocking"]
    # a child preflight without its batch is a blocking row, never a silent skip
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, True)
    assert by_id(report)["delete_evidence"]["status"] == "fail"
    assert "delete_evidence=fail" in report["blocking"]
    # setup (orchestrator, no unit mapping exists yet) is the one explicit not-applicable path
    setup = make_workspace(tmp_path / "setup")
    report = doctor.run(setup, PLUGIN_ROOT, "orchestrator", probed(setup), None, True)
    assert by_id(report)["delete_evidence"]["status"] == "skipped"
    assert report["blocking"] == ["databricks_identity=skipped"]


# ------------------------------------------------------------------ source principal read-only (A1)

class FakePrivConn:
    """A source seen through the doctor's privilege queries. `roles` are the principal's server or
    database-wide flags and role memberships, named as the query names them (sysadmin, db_owner,
    ALTER ANY DATABASE, CONTROL SERVER; rolsuper, pg_write_server_files for Postgres); `writable` maps a
    table to the write privileges it holds; `absent` tables do not exist, and answer like the engines do:
    OBJECT_ID / to_regclass NULL, HAS_PERMS_BY_NAME 0 (not NULL), has_table_privilege / ::regclass an
    UndefinedTable error. Indirection: `impersonate`
    lists principals the principal may IMPERSONATE, `execute` the procedures/functions it may EXECUTE,
    `set_role` maps a Postgres role it is a member of (directly or through `direct`, the roles it was granted
    itself, when given) to the write it would gain via SET ROLE, `set_role_execute` to the functions that role
    (and not the session) may EXECUTE, `unusable` names memberships granted with
    `SET FALSE, INHERIT FALSE` (Postgres 16: a path exists, but neither SET ROLE nor inheritance can use it);
    `columns` maps a table to {column: [privileges]} granted at column level only (invisible to the table-level query).
    Records every statement so a test can prove the check only ever asked questions."""

    def __init__(self, *, roles=(), writable=None, absent=(), read_only="on", impersonate=(), execute=(),
                 set_role=None, columns=None, direct=None, unusable=(), set_role_execute=None):
        self.roles, self.writable, self.absent, self.read_only = set(roles), writable or {}, set(absent), read_only
        self.impersonate, self.execute_on, self.set_role = list(impersonate), list(execute), set_role or {}
        self.set_role_execute = set_role_execute or {}
        self.columns = columns or {}
        self.direct = list({**self.set_role, **self.set_role_execute} if direct is None else direct)
        self.unusable = set(unusable)
        self.statements: list[tuple[str, tuple]] = []
        self.closed = False

    def cursor(self):
        return self

    def execute(self, sql, *params):
        self.statements.append((sql, params))
        low, args = sql.lower(), tuple(params[0]) if params else ()
        if "is_srvrolemember" in low:
            names = re.findall(r"IS_SRVROLEMEMBER\('(\w+)'\)|IS_MEMBER\('(\w+)'\)|HAS_PERMS_BY_NAME\(NULL, NULL, '([A-Z ]+)'\)", sql)
            self._rows = [tuple(int("".join(n) in self.roles) for n in names)]
        elif "sys.server_principals" in low or "sys.database_principals" in low:
            kind = "LOGIN" if "server_principals" in low else "USER"
            self._rows = [(f"{kind} {p.split(' ', 1)[1]}", "IMPERSONATE") for p in self.impersonate
                          if p.startswith(kind.lower())]
        elif "sys.objects" in low:
            self._rows = [(p, "EXECUTE") for p in self.execute_on]
        elif "object_id(?)" in low or "to_regclass(%s)" in low:
            self._rows = [(None if args[0] in self.absent else f"oid of {args[0]}",)]
        elif "fn_my_permissions" in low or "has_column_privilege" in low:
            if args[0] in self.absent and "::regclass" in low:
                raise RuntimeError(f'relation "{args[0]}" does not exist')
            self._rows = [(c, p) for c, privs in self.columns.get(args[0], {}).items() for p in privs]
        elif "has_perms_by_name" in low:  # an absent object answers 0 on every privilege, never NULL
            t = args[0]
            self._rows = [tuple(int(p in self.writable.get(t, ())) for p in ("INSERT", "UPDATE", "DELETE", "ALTER"))]
        elif "rolsuper" in low and "pg_auth_members" not in low:
            names = re.findall(r"\b(rolsuper|rolcreaterole|rolcreatedb|rolbypassrls)\b|pg_has_role\(current_user, '(\w+)'", sql)
            self._rows = [tuple("".join(n) in self.roles for n in names)]
        elif "pg_auth_members" in low:  # direct memberships only
            self._rows = [(r,) for r in self.direct]
        elif "pg_has_role(current_user, oid," in low:  # transitive: every role reachable through memberships
            reachable = {*self.direct, *self.set_role, *self.set_role_execute}
            if "'usage, set'" in low:  # a 16+ server answers only for memberships that inherit or can SET ROLE
                reachable -= self.unusable
            self._rows = [(r,) for r in sorted(reachable)]
        elif "pg_proc" in low and len(args) == 2:  # (schemas, role): what SET ROLE <role> may EXECUTE
            self._rows = [(f, "EXECUTE") for f in self.set_role_execute.get(args[1], ())]
        elif "pg_proc" in low:
            self._rows = [(f, "EXECUTE") for f in self.execute_on]
        elif "has_table_privilege" in low and len(args) == 4:  # (role, table, role, schema): the SET ROLE walk
            role, t = args[0], args[1]
            if t in self.absent:
                raise RuntimeError(f'relation "{t}" does not exist')
            self._rows = [(t in self.set_role.get(role, ()), False)]
        elif "has_table_privilege" in low:
            t = args[0]
            if t in self.absent:
                raise RuntimeError(f'relation "{t}" does not exist')
            held = self.writable.get(t, ())
            self._rows = [tuple(p in held for p in ("INSERT", "UPDATE", "DELETE", "TRUNCATE")) + ("CREATE" in held,)]
        elif "transaction_read_only" in low:
            self._rows = [(self.read_only,)]
        else:
            raise AssertionError(f"unexpected statement: {sql}")
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


def _asked_only_questions(conn):
    return all(s.lstrip().upper().startswith("SELECT") for s, _ in conn.statements)


TABLES = ["raw.loans", "raw.payments"]


def test_sqlserver_select_only_principal_is_ok_and_the_row_says_readonly_is_advisory(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x;PWD=never-printed")
    conn = FakePrivConn()
    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    assert c.data["tables"] == TABLES and c.data["roles"] == [] and c.data["writable"] == {}
    assert "readonly=True" in c.data["stats"] and "advisory" in c.data["stats"]
    assert "never-printed" not in json.dumps(asdict(c))
    assert conn.closed and _asked_only_questions(conn)
    # one role query, then one HAS_PERMS_BY_NAME row per in-scope table, parameterised, never interpolated
    perms = [(s, a) for s, a in conn.statements if "HAS_PERMS_BY_NAME(?" in s]
    assert [a[0][0] for _, a in perms] == TABLES
    assert all(p in perms[0][0] for p in ("'INSERT'", "'UPDATE'", "'DELETE'", "'ALTER'"))
    # and one column-grant query per table: a column-level UPDATE is not a table-level permission
    cols = [(s, a) for s, a in conn.statements if "fn_my_permissions(?, 'OBJECT')" in s]
    assert [a[0] for _, a in cols] == [(t,) for t in TABLES] and "subentity_name <> ''" in cols[0][0]
    assert any("IS_SRVROLEMEMBER('sysadmin')" in s and "IS_MEMBER('db_owner')" in s
               and "HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY DATABASE')" in s for s, _ in conn.statements)
    assert c.data["indirect"] == []


def test_sqlserver_indirection_queries_ask_the_effective_permission_on_every_visible_principal_and_proc(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakePrivConn()
    doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn)
    sql = "\n".join(s for s, _ in conn.statements)
    for flag in ("CONTROL SERVER", "IMPERSONATE ANY LOGIN", "ALTER ANY LOGIN", "ALTER ANY DATABASE"):
        assert f"HAS_PERMS_BY_NAME(NULL, NULL, '{flag}')" in sql
    for role in ("securityadmin", "serveradmin", "dbcreator", "bulkadmin"):
        assert f"IS_SRVROLEMEMBER('{role}')" in sql
    for role in ("db_owner", "db_ddladmin", "db_datawriter", "db_securityadmin"):
        assert f"IS_MEMBER('{role}')" in sql
    assert "sys.server_principals" in sql and "'LOGIN', 'IMPERSONATE'" in sql
    assert "sys.database_principals" in sql and "'USER', 'IMPERSONATE'" in sql
    assert "sys.objects" in sql and "'OBJECT', 'EXECUTE'" in sql and "is_ms_shipped = 0" in sql


def test_many_executable_procs_are_counted_not_dumped(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    procs = [f"[raw].[usp_{i}]" for i in range(12)]
    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: FakePrivConn(execute=procs))
    assert c.status == "fail" and "[raw].[usp_0]: EXECUTE" in c.detail and "+6 more" in c.detail
    assert c.data["indirect"] == [f"{p}: EXECUTE" for p in procs]


@pytest.mark.parametrize("kw, needle", [
    ({"roles": ["sysadmin"]}, "sysadmin"),
    ({"roles": ["db_owner"]}, "db_owner"),
    ({"roles": ["ALTER ANY DATABASE"]}, "ALTER ANY DATABASE"),
    ({"writable": {"raw.payments": ["INSERT", "DELETE"]}}, "raw.payments: INSERT, DELETE"),
    ({"writable": {"raw.loans": ["ALTER"]}}, "raw.loans: ALTER"),
    # indirection: server/database control, impersonation, admin roles, and EXECUTE on any procedure
    ({"roles": ["CONTROL SERVER"]}, "CONTROL SERVER"),
    ({"roles": ["IMPERSONATE ANY LOGIN"]}, "IMPERSONATE ANY LOGIN"),
    ({"roles": ["ALTER ANY LOGIN"]}, "ALTER ANY LOGIN"),
    ({"roles": ["securityadmin"]}, "role securityadmin"),
    ({"roles": ["serveradmin"]}, "role serveradmin"),
    ({"roles": ["dbcreator"]}, "role dbcreator"),
    ({"roles": ["bulkadmin"]}, "role bulkadmin"),
    ({"roles": ["db_ddladmin"]}, "role db_ddladmin"),
    ({"roles": ["db_datawriter"]}, "role db_datawriter"),
    ({"roles": ["db_securityadmin"]}, "role db_securityadmin"),
    ({"impersonate": ["login etl_admin"]}, "LOGIN etl_admin: IMPERSONATE"),
    ({"impersonate": ["user dbo"]}, "USER dbo: IMPERSONATE"),
    ({"execute": ["[raw].[usp_post_payment]"]}, "[raw].[usp_post_payment]: EXECUTE"),
    ({"columns": {"raw.loans": {"[balance]": ["UPDATE"]}}}, "raw.loans: UPDATE on column [balance]"),
])
def test_sqlserver_principal_that_can_write_in_scope_fails_naming_object_and_privilege(monkeypatch, kw, needle):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x;PWD=never-printed")
    conn = FakePrivConn(**kw)
    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn)
    assert c.status == "fail" and needle in c.detail and "never-printed" not in c.detail
    assert "readonly=True" in c.detail and "advisory" in c.detail  # the fail explains why the flag is no defence
    assert _asked_only_questions(conn)


def test_a_table_level_grant_is_not_repeated_per_column(monkeypatch):
    """fn_my_permissions lists every column under a table-level UPDATE; the row names the table once."""
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakePrivConn(writable={"raw.loans": ["UPDATE"]}, columns={"raw.loans": {"[a]": ["UPDATE"], "[b]": ["UPDATE"]}})
    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn)
    assert c.status == "fail" and c.data["writable"] == {"raw.loans": ["UPDATE"]}
    # an unresolved table is not asked about at all: HAS_PERMS_BY_NAME answers 0 for it (a false "no write"),
    # so existence is settled first, by OBJECT_ID, and the privilege queries run only for what resolved
    conn = FakePrivConn(absent=["raw.loans"])
    c = doctor.check_source_principal(["raw.loans", "raw.payments"], "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn)
    assert c.status == "unverified" and c.data["unresolved"] == ["raw.loans"]
    assert [a[0] for s, a in conn.statements if "OBJECT_ID(?)" in s] == [("raw.loans",), ("raw.payments",)]
    asked = [a[0][0] for s, a in conn.statements if "HAS_PERMS_BY_NAME(?" in s or "fn_my_permissions" in s]
    assert asked == ["raw.payments", "raw.payments"]


def test_postgres_branches(monkeypatch):
    monkeypatch.setenv("LAKEBASE_SRC", "postgres://u:never-printed@h/db")
    conn = FakePrivConn()
    c = doctor.check_source_principal(["public.loans"], "postgres", "LAKEBASE_SRC", connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    assert "transaction_read_only=on" in c.data["stats"] and "advisory" in c.data["stats"]
    assert "never-printed" not in json.dumps(asdict(c)) and _asked_only_questions(conn)
    priv = [(s, a) for s, a in conn.statements if s.startswith("SELECT has_table_privilege")]
    assert len(priv) == 1 and priv[0][1][0] == ("public.loans",) * 4 + ("public",)
    assert all(p in priv[0][0] for p in ("'INSERT'", "'UPDATE'", "'DELETE'", "'TRUNCATE'", "has_schema_privilege(%s, 'CREATE')"))
    for kw, needle in (({"roles": ["rolsuper"]}, "rolsuper"),
                       ({"writable": {"public.loans": ["TRUNCATE"]}}, "public.loans: TRUNCATE"),
                       ({"writable": {"public.loans": ["CREATE"]}}, "public.loans: CREATE on schema"),
                       # indirection: role attributes, file/program roles, function EXECUTE, SET ROLE to a writer
                       ({"roles": ["rolcreaterole"]}, "role rolcreaterole"),
                       ({"roles": ["rolcreatedb"]}, "role rolcreatedb"),
                       ({"roles": ["pg_write_server_files"]}, "role pg_write_server_files"),
                       ({"roles": ["pg_execute_server_program"]}, "role pg_execute_server_program"),
                       ({"execute": ["public.post_payment(integer)"]}, "public.post_payment(integer): EXECUTE"),
                       ({"set_role": {"loader": ["public.loans"]}}, "SET ROLE loader: public.loans write"),
                       # nested: granted only `etl`, which is itself a member of the writer `loader`
                       ({"set_role": {"loader": ["public.loans"]}, "direct": ["etl"]}, "SET ROLE loader: public.loans write"),
                       # a SET-only role that may EXECUTE a definer function the session may not: a write path too
                       ({"set_role_execute": {"loader": ["public.post_payment(integer)"]}},
                        "SET ROLE loader: public.post_payment(integer) EXECUTE"),
                       ({"columns": {"public.loans": {"balance": ["INSERT", "UPDATE"]}}},
                        "public.loans: INSERT on column balance, UPDATE on column balance")):
        c = doctor.check_source_principal(["public.loans"], "postgres", "LAKEBASE_SRC",
                                          connect=lambda dsn, kw=kw: FakePrivConn(**kw))
        assert c.status == "fail" and needle in c.detail, (kw, c.detail)
    # BYPASSRLS widens what a read sees, it grants no write path: not a finding
    c = doctor.check_source_principal(["public.loans"], "postgres", "LAKEBASE_SRC", connect=lambda dsn: FakePrivConn(roles=["rolbypassrls"]))
    assert c.status == "ok", c.detail
    # a membership whose role holds no in-scope write is not a finding, but is still asked about per table
    conn = FakePrivConn(set_role={"readers": []})
    c = doctor.check_source_principal(["raw.loans", "raw.payments"], "postgres", "LAKEBASE_SRC", connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    walk = [a[0] for s, a in conn.statements if s.startswith("SELECT has_table_privilege") and len(a[0]) == 4]
    assert walk == [("readers", "raw.loans", "readers", "raw"), ("readers", "raw.payments", "readers", "raw")]
    # and about the in-scope functions it could EXECUTE after SET ROLE (the session's own EXECUTE misses them)
    as_role_fn = [a[0] for s, a in conn.statements if "pg_proc" in s and "has_function_privilege(%s, p.oid" in s]
    assert as_role_fn == [(["raw"], "readers")]
    # the membership list is transitive (pg_has_role), not one hop of pg_auth_members, and on 16+ asks for
    # memberships the session can use (USAGE = inherits, SET = SET ROLE-able); before 16 MEMBER implied both
    assert not any("pg_auth_members" in s for s, _ in conn.statements)
    members = [s for s, _ in conn.statements if "pg_has_role(current_user, oid," in s]
    assert len(members) == 1 and "rolname <> current_user" in members[0]
    assert "server_version_num')::int >= 160000 THEN 'USAGE, SET' ELSE 'MEMBER'" in members[0]
    # a membership granted SET FALSE, INHERIT FALSE cannot be used, so it is not a write path
    c = doctor.check_source_principal(["public.loans"], "postgres", "LAKEBASE_SRC",
                                      connect=lambda dsn: FakePrivConn(set_role={"loader": ["public.loans"]}, unusable=["loader"]))
    assert c.status == "ok", c.detail
    fn = [(s, a[0]) for s, a in conn.statements if "pg_proc" in s and "has_function_privilege(p.oid" in s]
    assert len(fn) == 1 and fn[0][1] == (["raw"],) and "prosecdef" in fn[0][0]
    # column grants: one query per table over pg_attribute, excluding what the table-level grant already covers
    cols = [(s, a[0]) for s, a in conn.statements if "has_column_privilege" in s]
    assert [a for _, a in cols] == [("raw.loans",), ("raw.payments",)]
    assert "pg_attribute" in cols[0][0] and "NOT has_table_privilege" in cols[0][0] and "attisdropped" in cols[0][0]
    assert any("pg_has_role(current_user, 'pg_write_server_files', 'MEMBER')" in s and "rolcreatedb" in s
               and "rolbypassrls" not in s for s, _ in conn.statements)


def test_default_connectors_open_read_only_the_way_each_driver_accepts(monkeypatch):
    """psycopg 3 takes read_only as a connection attribute, not a libpq keyword (that raises)."""
    calls = {}

    class FakePg:
        class Conn:
            read_only = False

        @staticmethod
        def connect(conninfo, **kw):
            calls.update(conninfo=conninfo, kw=kw)
            return FakePg.Conn()

    monkeypatch.setitem(sys.modules, "psycopg", FakePg)
    conn = doctor._psycopg_connect("postgres://u:pw@h/db")
    assert calls == {"conninfo": "postgres://u:pw@h/db", "kw": {"connect_timeout": 15}} and conn.read_only is True
    fake_odbc = types.SimpleNamespace(connect=lambda dsn, **kw: (dsn, kw))
    monkeypatch.setitem(sys.modules, "pyodbc", fake_odbc)
    assert doctor._pyodbc_connect("Driver=x") == ("Driver=x", {"readonly": True, "timeout": 15})


def test_unresolvable_object_and_untestable_families_are_unverified_never_ok(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC",
                                      connect=lambda dsn: FakePrivConn(absent=["raw.payments"]))
    assert c.status == "unverified" and "raw.payments" in c.detail and c.data["unresolved"] == ["raw.payments"]
    # Postgres: has_table_privilege / ::regclass raise for a relation that does not exist, so existence is
    # settled by to_regclass first and no privilege query, table-level, column-level or as a role, names it;
    # the other tables are still evaluated, and a write on one of them still fails the row
    monkeypatch.setenv("LAKEBASE_SRC", "postgres://u:x@h/db")
    conn = FakePrivConn(absent=["raw.payments"], set_role={"readers": []})
    c = doctor.check_source_principal(TABLES, "postgres", "LAKEBASE_SRC", connect=lambda dsn: conn)
    assert c.status == "unverified" and "raw.payments" in c.detail and c.data["unresolved"] == ["raw.payments"]
    assert [a[0] for s, a in conn.statements if "to_regclass(%s)" in s] == [("raw.loans",), ("raw.payments",)]
    named = [a[0] for s, a in conn.statements if "has_table_privilege" in s or "has_column_privilege" in s]
    assert named and all("raw.payments" not in a for a in named)
    assert ("raw.loans",) * 4 + ("raw",) in named and ("readers", "raw.loans", "readers", "raw") in named
    conn = FakePrivConn(absent=["raw.payments"], writable={"raw.loans": ["INSERT"]})
    c = doctor.check_source_principal(TABLES, "postgres", "LAKEBASE_SRC", connect=lambda dsn: conn)
    assert c.status == "fail" and "raw.loans: INSERT" in c.detail and c.data["unresolved"] == ["raw.payments"]
    for family in ("teradata", "oracle", "redshift", "snowflake"):
        c = doctor.check_source_principal(TABLES, family, "LEGACY_ODBC", connect=lambda dsn: FakePrivConn())
        assert c.status == "unverified" and family in c.detail and "privilege query" in c.detail


def test_source_principal_needs_the_secret_and_redacts_driver_errors(monkeypatch):
    monkeypatch.delenv("LEGACY_ODBC", raising=False)
    c = doctor.check_source_principal(TABLES, "sqlserver", None)
    assert c.status == "fail" and "--source-secret" in c.detail
    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC")
    assert c.status == "fail" and "LEGACY_ODBC" in c.detail and "not set" in c.detail
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")

    def boom(dsn):
        raise RuntimeError("login failed for PWD=hunter2token99 at server")

    c = doctor.check_source_principal(TABLES, "sqlserver", "LEGACY_ODBC", connect=boom)
    assert c.status == "fail" and "hunter2token99" not in c.detail and "login failed" in c.detail


def test_run_resolves_the_in_scope_tables_from_the_same_mappings_as_delete_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    monkeypatch.setattr(doctor, "check_delete_evidence",
                        lambda m, s, root, connect=None, params=None: doctor.Check("delete_evidence", "ok", ""))
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans")
    _unit_mapping(ws, "payments", evidence=False)
    seen = {}

    def fake(tables, family, secret, connect=None):
        seen.update(tables=tables, family=family, secret=secret)
        return doctor.Check("source_principal_read_only", "unverified", "stub")

    monkeypatch.setattr(doctor, "check_source_principal", fake)
    report = doctor.run(ws, PLUGIN_ROOT, "child", probed(ws), None, True, units=["loans", "payments"],
                        source_secret="LEGACY_ODBC")
    assert seen == {"tables": ["raw.loans"], "family": "sqlserver", "secret": "LEGACY_ODBC"}
    # only `fail` softens in a child: the row stays unverified but does not block child readiness
    assert by_id(report)["source_principal_read_only"]["status"] == "unverified"
    assert "source_principal_read_only=unverified" not in report["blocking"]
    # --source-family is authoritative; without it the family comes from the mapping's delete_evidence kind
    seen.clear()
    doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True, source_secret="LEGACY_ODBC",
               source_family="postgres")
    assert seen["family"] == "postgres" and seen["tables"] == ["raw.loans"]
    # a mapping that declares no evidence kind and no --source-family cannot be verified, never passed
    monkeypatch.setattr(doctor, "check_source_principal", lambda *a, **k: pytest.fail("must not connect"))
    ws2 = make_workspace(tmp_path / "nokind")
    _unit_mapping(ws2, "loans", evidence=False)
    report = doctor.run(ws2, PLUGIN_ROOT, "orchestrator", "blocked", None, True, source_secret="LEGACY_ODBC")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "unverified" and "--source-family" in row["detail"]
    assert "source_principal_read_only=unverified" in report["blocking"]
    # setup, before any unit mapping exists: not applicable, and not blocking
    setup = make_workspace(tmp_path / "setup")
    report = doctor.run(setup, PLUGIN_ROOT, "orchestrator", probed(setup), None, True)
    assert by_id(report)["source_principal_read_only"]["status"] == "skipped"
    assert report["blocking"] == ["databricks_identity=skipped"]


def test_cli_source_family_choices_and_help(tmp_path):
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--help"], capture_output=True, text=True, check=True)
    assert "--source-family" in r.stdout and "--source-attested" in r.stdout \
        and "--expect-catalogs" in r.stdout and "blocked:<nonce>" in r.stdout
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(make_workspace(tmp_path)),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--source-family", "mysql"],
                       capture_output=True, text=True, check=False)
    assert r.returncode == 2 and "--source-family" in r.stderr


def test_source_families_are_the_harness_families_and_databricks_without_cli_is_unverified(tmp_path, monkeypatch):
    """The doctor accepts exactly the families `dbx-recon run --family` accepts (a Databricks-to-Databricks
    unit is one); a family without a tested privilege query is `unverified` and blocks, never `ok`."""
    cli = (PLUGIN_ROOT / "skills" / "data-reconciliation" / "harness" / "recon" / "cli.py").read_text()
    families = re.search(r"^SOURCE_FAMILIES = \((.*)\)$", cli, re.MULTILINE).group(1)
    assert set(doctor.SOURCE_FAMILIES) == set(re.findall(r'"(\w+)"', families)) and "databricks" in doctor.SOURCE_FAMILIES
    monkeypatch.setenv("SRC_DBX", "token=never-printed")
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "databricks" in c.detail and "never-printed" not in json.dumps(asdict(c))
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True, source_secret="SRC_DBX",
                        source_family="databricks")
    assert by_id(report)["source_principal_read_only"]["status"] == "unverified"
    assert "source_principal_read_only=unverified" in report["blocking"] and report["ready"] is False


# ------------------------------------------------------------------ dictionary_readable (WS3.1)

sys.path.insert(0, str(PLUGIN_ROOT / "skills" / "data-reconciliation" / "harness"))
from recon.adapters import DICTIONARY_OBJECTS  # noqa: E402


class FakeDictConn:
    """A source seen through the doctor's dictionary probes. `fail_views` names catalog views
    whose probe raises; `census` maps a table to (declared, listed) trigger answers. Records
    every statement."""

    def __init__(self, *, fail_views=(), census=None):
        self.fail_views, self.census, self.statements = set(fail_views), census or {}, []

    def cursor(self):
        outer = self

        class Cur:
            def execute(self, sql, params=()):
                outer.statements.append(sql)
                sql_l = sql.lower()
                for label in outer.fail_views:
                    if label.lower() in sql_l:
                        raise RuntimeError(f"permission denied for {label} secret=DSN://leak")
                outer.result = [(1,)]
                if "tablehasinserttrigger" in sql_l or "relhastriggers" in sql_l:
                    outer.result = [(outer.census.get(params[0], (0, 0))[0],)]
                elif "count(*)" in sql_l:
                    outer.result = [(outer.census.get(params[0], (0, 0))[1],)]
                return self

            def fetchall(self):
                return outer.result
        return Cur()

    def close(self):
        pass


def test_dictionary_readable_unverified_for_unprobed_families(monkeypatch):
    for family in ("teradata", "databricks"):
        c = doctor.check_dictionary_readable(TABLES, family, "LEGACY_ODBC")
        assert c.status == "unverified" and "unsupported" in c.detail


def test_dictionary_readable_fails_without_the_secret(monkeypatch):
    c = doctor.check_dictionary_readable(TABLES, "sqlserver", None,
                                         views=DICTIONARY_OBJECTS["sqlserver"])
    assert c.status == "fail" and "--source-secret" in c.detail
    monkeypatch.delenv("LEGACY_ODBC", raising=False)
    c = doctor.check_dictionary_readable(TABLES, "sqlserver", "LEGACY_ODBC",
                                         views=DICTIONARY_OBJECTS["sqlserver"])
    assert c.status == "fail" and "not set" in c.detail


def test_dictionary_readable_fails_when_a_catalog_view_is_unreadable(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn(fail_views={"sys.triggers"})
    c = doctor.check_dictionary_readable(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn,
                                 views=DICTIONARY_OBJECTS["sqlserver"])
    assert c.status == "fail" and "sys.triggers" in c.detail and "DSN" not in c.detail


def test_dictionary_readable_fails_when_sqlserver_hides_triggers(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn(census={"raw.loans": (1, 0), "raw.payments": (0, 0)})
    c = doctor.check_dictionary_readable(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn,
                                 views=DICTIONARY_OBJECTS["sqlserver"])
    assert c.status == "fail" and "raw.loans" in c.detail and "filtered by permission" in c.detail
    assert c.data["trigger_census"]["raw.loans"] == {"declared": 1, "listed": 0}


def test_dictionary_readable_fails_when_a_constraint_view_is_unreadable(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn(fail_views={"pg_index"})
    c = doctor.check_dictionary_readable(TABLES, "postgres", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["postgres"])
    assert c.status == "fail" and "pg_index" in c.detail and "DSN" not in c.detail


def test_dictionary_readable_warns_on_a_postgres_census_lag(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn(census={"public.loans": (1, 0)})
    c = doctor.check_dictionary_readable(["public.loans"], "postgres", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["postgres"])
    assert c.status == "warn" and "relhastriggers" in c.detail


def test_dictionary_readable_ok_with_data(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn(census={"raw.loans": (1, 2), "raw.payments": (0, 0)})
    c = doctor.check_dictionary_readable(TABLES, "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn,
                                 views=DICTIONARY_OBJECTS["sqlserver"])
    assert c.status == "ok"
    assert "sys.triggers" in c.data["views"] and "sys.database_permissions" in c.data["views"]
    assert c.data["trigger_census"]["raw.loans"] == {"declared": 1, "listed": 2}


def test_dictionary_readable_probes_per_table_ddl_for_databricks(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn()
    c = doctor.check_dictionary_readable(["cat.s.loans"], "databricks", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "ok" and "trigger_census" not in {
        k for k, v in c.data.items() if v}
    assert any("SHOW CREATE TABLE `cat`.`s`.`loans`" in q for q in conn.statements)
    assert any("`cat`.information_schema" in q for q in conn.statements)


def test_dictionary_readable_probes_each_catalog_once_for_databricks(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn()
    c = doctor.check_dictionary_readable(["a.s.loans", "a.s.fees", "b.s.loans"],
                                         "databricks", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "ok"
    cat_scoped = [q for q in conn.statements if "information_schema" in q]
    per_catalog = [q for q in cat_scoped if ".information_schema" in q]
    assert per_catalog and all(q.startswith("SELECT 1") for q in per_catalog)
    assert sum("`a`.information_schema" in q for q in per_catalog) == 7
    assert sum("`b`.information_schema" in q for q in per_catalog) == 7
    assert sum("SHOW CREATE TABLE" in q for q in conn.statements) == 3
    assert "SHOW CREATE TABLE `a`.`s`.`loans`" in conn.statements


def test_dictionary_readable_fails_on_a_two_part_databricks_table(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn()
    c = doctor.check_dictionary_readable(["s.loans"], "databricks", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "fail" and "s.loans" in c.detail and "catalog.schema.table" in c.detail


def test_dictionary_readable_registers_a_databricks_connector():
    assert "databricks" in doctor._READ_ONLY_CONNECT


def test_dictionary_readable_databricks_without_the_connector_names_the_package(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "{}")
    monkeypatch.setitem(sys.modules, "databricks", None)  # import fails
    c = doctor.check_dictionary_readable(["cat.s.loans"], "databricks", "LEGACY_ODBC",
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "fail" and "databricks-sql-connector" in c.detail
    assert "{" not in c.detail  # never the secret


def test_dictionary_readable_quotes_probe_identifiers(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn()
    c = doctor.check_dictionary_readable(["cat.`a;b`.loans"], "databricks", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "ok"
    assert conn.statements
    for q in conn.statements:
        assert "a;b" not in q or "`a;b`" in q  # the part only ever appears backtick-quoted
    assert any("`a;b`" in q for q in conn.statements)


def test_dictionary_readable_fails_on_an_empty_databricks_name_part(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn()
    c = doctor.check_dictionary_readable(["a..t"], "databricks", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "fail" and "a..t" in c.detail


def test_dictionary_readable_fails_on_an_unreadable_databricks_table(monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "DSN=x")
    conn = FakeDictConn(fail_views={"SHOW CREATE TABLE"})
    c = doctor.check_dictionary_readable(["cat.s.loans"], "databricks", "LEGACY_ODBC",
                                         connect=lambda dsn: conn,
                                         views=DICTIONARY_OBJECTS["databricks"])
    assert c.status == "fail" and "SHOW CREATE TABLE" in c.detail and "cat.s.loans" in c.detail


def test_dictionary_readable_all_resolves_units_and_children(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_secret="LEGACY_ODBC", source_family="sqlserver")
    row = by_id(report)["dictionary_readable"]
    assert row["status"] == "skipped"
    _unit_mapping(ws, "loans")
    seen = {}
    monkeypatch.setattr(doctor, "check_dictionary_readable",
                        lambda tables, family, secret, **kw: seen.update(tables=tables) or
                        doctor.Check("dictionary_readable", "ok", "stub"))
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_secret="LEGACY_ODBC", source_family="sqlserver")
    assert by_id(report)["dictionary_readable"]["status"] == "ok" and seen["tables"]


def test_dictionary_readable_fail_blocks_the_run(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans")
    monkeypatch.setattr(doctor, "check_dictionary_readable",
                        lambda *a, **k: doctor.Check("dictionary_readable", "fail", "stub"))
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_secret="LEGACY_ODBC", source_family="sqlserver")
    assert "dictionary_readable=fail" in report["blocking"] and report["ready"] is False


# ------------------------------------------------------------------ databricks source principal + attested

DBX_TABLES = ["mig.raw.loans", "mig.raw.payments"]
DBX_READ_GRANTS = {"mig": ["USE_CATALOG"], "mig.raw": ["USE_SCHEMA"],
                   "mig.raw.loans": ["SELECT"], "mig.raw.payments": ["SELECT"]}
DBX_PRINCIPAL = "2e90bc1d-e9a1-4703-8c48-ad28ebb1864d"
DBX_SECRET = json.dumps({"server_hostname": "adb-source.example", "http_path": "/sql/1.0/warehouses/x",
                         "access_token": "test-source-token"})


def _dbx_source_cli(monkeypatch, *, grants, owners=None, groups=(), fail_op=None,
                    raw_get_effective=None):
    """A databricks CLI bound to the --source-secret credential, not the session env: every call
    must arrive with env carrying only the secret's host/token plus auth-type pat — every other
    DATABRICKS_* variable set in the session must not reach the subprocess. `current-user me`
    answers an applicationId in `groups`, `catalogs|schemas|tables get` the owners dict (default
    owner@example.com, since a missing owner is now unverified), `grants get-effective` the
    per-name grants dict (one assignment carried through a GROUP, the way inherited grants
    arrive); `raw_get_effective` overrides the payload per securable for malformed-JSON cases.
    Returns every command asked."""
    asked = []
    monkeypatch.setenv("SRC_DBX", DBX_SECRET)
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "oauth-m2m")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "inherited-client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "inherited-secret")
    monkeypatch.setenv("DATABRICKS_ACCOUNT_ID", "inherited-account")
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "inherited-profile")
    monkeypatch.setenv("DATABRICKS_HOST", "https://wrong-workspace.example")
    monkeypatch.setenv("DATABRICKS_TOKEN", "inherited-token")
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")

    def fake_run(cmd, timeout=0, env=None):
        asked.append(cmd)
        assert env is not None, cmd
        dbx = {k for k in env if k.startswith("DATABRICKS_")}
        assert dbx == {"DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_AUTH_TYPE"}, cmd
        assert env["DATABRICKS_TOKEN"] == "test-source-token"
        assert env["DATABRICKS_HOST"] == "https://adb-source.example"
        assert env["DATABRICKS_AUTH_TYPE"] == "pat"
        op = tuple(cmd[1:3])
        if op == fail_op:
            return 1, "", "Error: default auth: cannot configure default credentials token=never-printed"
        if op == ("current-user", "me"):
            return 0, json.dumps({"applicationId": DBX_PRINCIPAL,
                                  "groups": [{"display": g} for g in groups]}), ""
        if op in (("catalogs", "get"), ("schemas", "get"), ("tables", "get")):
            return 0, json.dumps({"owner": (owners or {}).get(cmd[3], "owner@example.com")}), ""
        if op == ("grants", "get-effective"):
            if raw_get_effective and cmd[4] in raw_get_effective:
                return 0, json.dumps(raw_get_effective[cmd[4]]), ""
            return 0, json.dumps({"privilege_assignments": [
                {"principal": DBX_PRINCIPAL, "inherited_from_type": "GROUP",
                 "privileges": grants.get(cmd[4], [])}]}), ""
        raise AssertionError(cmd)

    monkeypatch.setattr(doctor, "_run", fake_run)
    return asked


def test_databricks_source_principal_needs_and_parses_the_secret(monkeypatch):
    c = doctor.check_source_principal(DBX_TABLES, "databricks", None)
    assert c.status == "fail" and "--source-secret" in c.detail
    monkeypatch.delenv("SRC_DBX", raising=False)
    c = doctor.check_source_principal(DBX_TABLES, "databricks", "SRC_DBX")
    assert c.status == "fail" and "SRC_DBX" in c.detail and "not set" in c.detail
    monkeypatch.setenv("SRC_DBX", "token=never-printed")
    c = doctor.check_source_principal(DBX_TABLES, "databricks", "SRC_DBX")
    assert c.status == "unverified" and "JSON" in c.detail
    assert "never-printed" not in json.dumps(asdict(c))


def test_databricks_source_principal_ok_reads_every_securable(monkeypatch):
    asked = _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS)
    c = doctor.check_source_principal(DBX_TABLES, "databricks", "SRC_DBX")
    assert c.status == "ok", c.detail
    assert c.data["principal"] == DBX_PRINCIPAL and c.data["host"] == "adb-source.example"
    assert c.data["writable"] == {} and "test-source-token" not in json.dumps(asdict(c))
    effective = [cmd[3:5] for cmd in asked if cmd[1:3] == ["grants", "get-effective"]]
    assert effective == [["catalog", "mig"], ["schema", "mig.raw"],
                         ["table", "mig.raw.loans"], ["table", "mig.raw.payments"]]


def test_databricks_source_principal_fails_on_write_privilege(monkeypatch):
    grants = dict(DBX_READ_GRANTS, **{"mig.raw.payments": ["SELECT", "MODIFY"]})
    _dbx_source_cli(monkeypatch, grants=grants)
    c = doctor.check_source_principal(DBX_TABLES, "databricks", "SRC_DBX")
    assert c.status == "fail" and "mig.raw.payments: MODIFY" in c.detail
    assert c.data["writable"]["mig.raw.payments"] == ["MODIFY"]


def test_databricks_source_principal_fails_on_group_ownership(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS, owners={"mig.raw": "data_engineers"},
                    groups=("data_engineers",))
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "fail" and "mig.raw: OWNER" in c.detail
    assert c.data["writable"]["mig.raw"] == ["OWNER"]


def test_databricks_source_principal_fails_on_inherited_write_grant(monkeypatch):
    grants = dict(DBX_READ_GRANTS, **{"mig.raw": ["USE_SCHEMA", "ALL_PRIVILEGES"]})
    _dbx_source_cli(monkeypatch, grants=grants, groups=("data_engineers",))
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "fail" and "mig.raw: ALL_PRIVILEGES" in c.detail


def test_databricks_source_principal_unverified_on_cli_error(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS, fail_op=("grants", "get-effective"))
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "get-effective" in c.detail
    assert "never-printed" not in json.dumps(asdict(c))
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "CLI" in c.detail


def test_databricks_source_principal_unverified_when_owner_is_unknown(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS, fail_op=("schemas", "get"))
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "schemas get mig.raw" in c.detail
    assert "never-printed" not in json.dumps(asdict(c))


def test_databricks_source_principal_unverified_on_malformed_grants_payload(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS, raw_get_effective={"mig": {}})
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "catalog mig" in c.detail
    assert "no privilege_assignments" in c.detail


def test_databricks_source_principal_empty_assignments_are_read_only(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS,
                    raw_get_effective={name: {"privilege_assignments": []}
                                       for name in ("mig", "mig.raw", "mig.raw.loans")})
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "ok", c.detail


def test_databricks_source_principal_unverified_on_string_privileges(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS,
                    raw_get_effective={"mig.raw.loans": {"privilege_assignments": [
                        {"principal": DBX_PRINCIPAL, "privileges": "SELECT"}]}})
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "table mig.raw.loans" in c.detail
    assert "no privilege_assignments" in c.detail


def test_databricks_source_principal_two_part_names_are_unresolved(monkeypatch):
    _dbx_source_cli(monkeypatch, grants=DBX_READ_GRANTS)
    c = doctor.check_source_principal(["raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and c.data["unresolved"] == ["raw.loans"]


def _attest(ws, line):
    p = ws / ".migration" / "06_decisions.md"
    p.write_text(p.read_text() + line)


def test_source_attested_reports_attested_and_does_not_block(tmp_path):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    _attest(ws, "D-7 | source_principal_read_only attested: source is a static export, no principal | user:msg-41\n")
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_family="teradata", source_attested="D-7")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "ok" and row["data"]["attested"] == "D-7"
    assert row["data"]["decision"] == "D-7" and row["data"]["provenance"] == "user:msg-41"
    assert not [b for b in report["blocking"] if b.startswith("source_principal_read_only")]
    assert report["blocking"] == ["hook_guard=unverified", "recon_family_supported=fail",
                                  "databricks_identity=skipped"]


def test_source_attested_fails_without_a_matching_ledger_line(tmp_path):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    _attest(ws, "D-99 | source_principal_read_only: grants confirmed SELECT-only by hand\n")
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_family="teradata", source_attested="D-99")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "fail" and "D-99" in row["detail"]
    assert "source_principal_read_only=fail" in report["blocking"]
    # a decision id is a whole token: D-9 does not match the D-99 line, nor D-990 a D-99 one
    _attest(ws, "D-990 | source_principal_read_only attested: static export\n")
    for wrong in ("D-9", "D-99"):
        report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                            source_family="teradata", source_attested=wrong)
        assert by_id(report)["source_principal_read_only"]["status"] == "fail", wrong


@pytest.mark.parametrize("line", [
    "D-8 | source_principal_read_only attested: static export | default-accepted (soft, 60s, no reply)\n",
    "D-8 | source_principal_read_only attested: static export\n",
    "D-8 | source_principal_read_only attested: static export | user:\n",
    "D-8 | source_principal_read_only attested: static export | reviewer:alice\n",
])
def test_source_attested_requires_user_provenance(tmp_path, line):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    _attest(ws, line)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_family="teradata", source_attested="D-8")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "fail" and "user:" in row["detail"] and "D-8" in row["detail"]
    assert "source_principal_read_only=fail" in report["blocking"]


def test_source_attested_is_rejected_for_families_with_a_privilege_query(tmp_path, monkeypatch):
    monkeypatch.setattr(doctor, "check_source_principal", lambda *a, **k: pytest.fail("must not connect"))
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    _attest(ws, "D-7 | source_principal_read_only attested: source is a static export, no principal | user:msg-41\n")
    for family in ("postgres", "databricks"):
        report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                            source_family=family, source_attested="D-7")
        row = by_id(report)["source_principal_read_only"]
        assert row["status"] == "fail" and "run the query instead" in row["detail"], family


def test_source_principal_unsupported_family_stays_unverified_and_blocking(tmp_path):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True, source_family="teradata")
    row = by_id(report)["source_principal_read_only"]
    assert row["status"] == "unverified" and "--source-attested" in row["detail"]
    assert "source_principal_read_only=unverified" in report["blocking"] and report["ready"] is False


# ------------------------------------------------------------------ recon_family_supported (WS3.11)

sys.path.insert(0, str(PLUGIN_ROOT / "skills" / "data-reconciliation" / "harness"))
from recon.adapters import SOURCE_ADAPTERS, is_untested_source_family  # noqa: E402

_UNTESTED_FAMILIES = [f for f in SOURCE_ADAPTERS if is_untested_source_family(f)]


def test_recon_family_supported_skipped_without_a_family(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    row = by_id(report)["recon_family_supported"]
    assert row["status"] == "skipped" and "--source-family" in row["detail"]
    assert not [b for b in report["blocking"] if b.startswith("recon_family_supported")]


@pytest.mark.parametrize("family", ("sqlserver", "postgres", "databricks"))
def test_recon_family_supported_ok_for_live_tested_families(family):
    c = doctor.check_recon_family_supported(PLUGIN_ROOT, family)
    assert c.status == "ok" and c.data["family"] == family and family in c.data["live_tested"]


@pytest.mark.parametrize("family", _UNTESTED_FAMILIES)
def test_recon_family_supported_fails_for_every_family_the_harness_refuses(family):
    assert _UNTESTED_FAMILIES
    c = doctor.check_recon_family_supported(PLUGIN_ROOT, family)
    assert c.status == "fail"
    assert "Attestation says the principal is read-only; this row says whether we can reconcile the family" in c.detail
    assert f"dbx-recon run --family {family}" in c.detail and "DSN=" not in c.detail
    assert c.data["live_tested"] == ["databricks", "postgres", "sqlserver"]


def test_recon_family_supported_fails_for_an_unknown_family():
    c = doctor.check_recon_family_supported(PLUGIN_ROOT, "mysql")
    assert c.status == "fail" and "no source adapter" in c.detail


def test_recon_family_supported_asks_the_installed_harness_first(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which",
                        lambda name: "/usr/bin/dbx-recon" if name == "dbx-recon" else None)
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, '{"live_tested": ["oracle"], "untested": []}', ""))
    c = doctor.check_recon_family_supported(PLUGIN_ROOT, "oracle")
    assert c.status == "ok" and "dbx-recon" in c.data["harness"]


def test_recon_family_supported_fails_when_the_harness_cannot_answer(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which",
                        lambda name: "/usr/bin/dbx-recon" if name == "dbx-recon" else None)
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (1, "", "boom"))
    c = doctor.check_recon_family_supported(PLUGIN_ROOT, "sqlserver")
    assert c.status == "fail" and "cannot ask the harness" in c.detail


def test_recon_family_supported_fails_on_a_malformed_registry_instead_of_raising(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which",
                        lambda name: "/usr/bin/dbx-recon" if name == "dbx-recon" else None)
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, '{"live_tested": ["sqlserver", 1], "untested": [null]}', ""))
    c = doctor.check_recon_family_supported(PLUGIN_ROOT, "sqlserver")
    assert c.status == "fail" and "cannot ask the harness" in c.detail


def test_recon_family_supported_is_its_own_row_beside_an_attested_principal(tmp_path):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    _attest(ws, "D-7 | source_principal_read_only attested: source is a static export, no principal | user:msg-41\n")
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        source_family="teradata", source_attested="D-7")
    rows = by_id(report)
    assert rows["source_principal_read_only"]["status"] == "ok"
    assert rows["recon_family_supported"]["status"] == "fail"
    assert "recon_family_supported=fail" in report["blocking"] and report["ready"] is False


def test_wave_manifest_family_reaches_recon_family_supported(tmp_path):
    ws = make_workspace(tmp_path)
    manifest = ws / ".migration" / "waves" / "wave-1.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({
        "capabilities": {"identity": "sp-1", "host": "https://h", "catalogs": ["mig_cat"]},
        "source": {"family": "oracle", "secret": "LEGACY_DSN", "params": {"db": "loans"}},
    }))
    result = subprocess.run(
        [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--no-databricks",
         "--hook-probe-result", probed(ws), "--wave", str(manifest)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 1
    record = json.loads(manifest.with_suffix(".doctor.json").read_text())
    row = next(c for c in record["checks"] if c["id"] == "recon_family_supported")
    assert row["status"] == "fail" and "recon_family_supported=fail" in record["blocking"]
    assert any(l.startswith("fail") and "recon_family_supported" in l for l in result.stdout.splitlines())


# ------------------------------------------------------------------ type_map_audit (WS3.5)

def _typed_unit_mapping(ws: Path, unit: str, fields: list[dict]) -> Path:
    p = ws / ".migration" / "units" / unit / "mapping_spec.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": "m1", "objects": [{
        "object": "orders", "root_table": "ORDERS",
        "key": {"source": ["ORDER_ID"], "target": ["order_id"]},
        "fields": fields}]}))
    return p


def _field(source, source_type, target_type):
    return {"source": source, "target": source.lower(),
            "source_type": source_type, "target_type": target_type}


def test_type_map_audit_skipped_at_setup(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], None, PLUGIN_ROOT)
    assert c.status == "skipped" and "not applicable" in c.detail


def test_type_map_audit_unverified_without_a_family(tmp_path):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [_field("ORDER_ID", "NUMBER(18,0)", "bigint")])
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], None, PLUGIN_ROOT)
    assert c.status == "unverified" and "--source-family" in c.detail


def test_type_map_audit_ok_fills_in_the_undeclared_count(tmp_path):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [
        _field("ORDER_ID", "NUMBER(18,0)", "bigint"),
        _field("AMOUNT", "NUMBER(12,2)", "decimal(12,2)"),
        _field("CREATED_AT", "TIMESTAMP(6)", "timestamp_ntz"),
        _field("NOTE", "VARCHAR2(200)", ""),
    ])
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT)
    assert c.status == "ok"
    assert c.data["map"].endswith("oracle-plsql/canonicalization.json")
    assert c.data["undeclared"] == 1 and c.data["fields"] == 4


def test_type_map_audit_fails_on_a_forbidden_target_and_blocks_the_run(tmp_path):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [
        _field("AMOUNT", "NUMBER(12,2)", "double"),
        _field("CREATED_AT", "TIMESTAMP", "timestamptz"),
    ])
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT)
    assert c.status == "fail"
    for needle in ("orders.AMOUNT", "double", "decimal(12,2)", "orders.CREATED_AT"):
        assert needle in c.detail, needle
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True, source_family="oracle")
    assert "type_map_audit=fail" in report["blocking"] and report["ready"] is False


def test_type_map_audit_warns_for_a_family_with_no_map(tmp_path):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [_field("ORDER_ID", "NUMBER(18,0)", "bigint")])
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "sqlserver", PLUGIN_ROOT)
    assert c.status == "warn" and "sqlserver" in c.detail


def test_type_map_audit_audits_against_the_declared_target_kind(tmp_path):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [
        _field("CREATED_AT", "TIMESTAMP WITH TIME ZONE", "timestamp with time zone"),
        _field("NOTE", "VARCHAR2(200)", "text"),
    ])
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT,
                                    target_kind="lakebase")
    assert c.status == "ok" and c.data["target_kind"] == "lakebase"
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT)
    assert c.status == "fail" and c.data["target_kind"] == "databricks"


def test_type_map_audit_warns_when_the_family_maps_a_different_kind(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [_field("ORDER_ID", "NUMBER(18,0)", "bigint")])
    payload = json.dumps({"family_known": True, "target_known": False, "map": None,
                          "findings": [], "error": None})
    monkeypatch.setattr(doctor.shutil, "which",
                        lambda name: "/usr/bin/dbx-recon" if name == "dbx-recon" else None)
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, payload, ""))
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "sqlserver", PLUGIN_ROOT,
                                    target_kind="lakebase")
    assert c.status == "warn" and "sqlserver->lakebase" in c.detail
    assert c.data["harness"] == "dbx-recon"


def test_type_map_audit_fails_when_the_harness_reports_an_error(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [_field("ORDER_ID", "NUMBER(18,0)", "bigint")])
    payload = json.dumps({"family_known": True, "target_known": True, "map": None,
                          "findings": [], "error": "ConfigError: multiple canonicalization "
                          "files carry a type_map for oracle"})
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, payload, ""))
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT)
    assert c.status == "fail" and "multiple canonicalization" in c.detail


def test_type_map_audit_fails_on_malformed_harness_output(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [_field("ORDER_ID", "NUMBER(18,0)", "bigint")])
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, "not json", ""))
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT)
    assert c.status == "fail" and "cannot audit" in c.detail


def test_type_map_audit_asks_the_installed_harness_first(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    _typed_unit_mapping(ws, "loans", [_field("ORDER_ID", "NUMBER(18,0)", "bigint")])
    payload = json.dumps({"family_known": True, "target_known": True,
                          "map": "/x/canonicalization.json",
                          "findings": [{"field": "orders.ORDER_ID", "verdict": "ok",
                                        "detail": "bigint", "source_type": "NUMBER(18,0)",
                                        "target_type": "bigint"}], "error": None})
    monkeypatch.setattr(doctor.shutil, "which",
                        lambda name: "/usr/bin/dbx-recon" if name == "dbx-recon" else None)
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, payload, ""))
    c = doctor.check_type_map_audit(ws, "orchestrator", [], [], "oracle", PLUGIN_ROOT)
    assert c.status == "ok" and c.data["harness"] == "dbx-recon" and c.data["fields"] == 1



# ------------------------------------------------------------------ ledger integrity rows (A2c)

def test_allowlist_committed_is_ok_only_when_both_contract_files_equal_head(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "ok" and "byte-equal to HEAD" in c.detail
    assert c.data == {".migration/allowed_targets.json": "clean",
                      ".migration/03_recon_tolerances.json": "clean"}
    (ws / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat", "prod"]}))
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and c.data[".migration/allowed_targets.json"] == "modified"
    assert "allowed_targets.json modified" in c.detail and "03_recon_tolerances" not in c.detail
    _git(ws, "add", "-A")  # staging is not committing
    assert doctor.check_allowlist_committed(ws).status == "fail"
    _git(ws, "commit", "-qm", "decision")
    assert doctor.check_allowlist_committed(ws).status == "ok"
    _git(ws, "rm", "-q", "--cached", ".migration/03_recon_tolerances.json")
    _git(ws, "commit", "-qm", "oops")
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and c.data[".migration/03_recon_tolerances.json"] == "untracked"
    (ws / ".migration" / "03_recon_tolerances.json").unlink()
    assert doctor.check_allowlist_committed(ws).data[".migration/03_recon_tolerances.json"] == "missing"


def test_allowlist_committed_compares_bytes_not_git_status(tmp_path):
    ws = make_workspace(tmp_path)
    _git(ws, "update-index", "--assume-unchanged", ".migration/03_recon_tolerances.json")
    (ws / ".migration" / "03_recon_tolerances.json").write_text("{}")
    assert _git(ws, "status", "--porcelain").strip() == ""
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and c.data[".migration/03_recon_tolerances.json"] == "modified"


def test_allowlist_committed_compares_to_origin_main_not_a_feature_head(tmp_path):
    ws = make_workspace(tmp_path)
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-q", str(origin)], check=True)
    _git(ws, "remote", "add", "origin", str(origin))
    _git(ws, "push", "-q", "-u", "origin", "HEAD:main")
    _git(ws, "fetch", "-q", "origin")
    _git(ws, "remote", "set-head", "origin", "main")
    # a feature branch may commit whatever it likes: the contract is origin's copy
    _git(ws, "checkout", "-qb", "feature")
    (ws / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat", "prod"]}))
    _git(ws, "commit", "-qam", "widened on a branch")
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and c.data[".migration/allowed_targets.json"] == "modified"
    assert "modified" in c.detail


def test_allowlist_committed_fails_outside_a_repository(tmp_path):
    ws = make_workspace(tmp_path, commit=False)
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and "untracked" in c.detail
    import shutil
    shutil.rmtree(ws / ".git")
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and "git" in c.detail.lower()


def test_allowlist_matches_contract(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_allowlist_matches_contract(ws, None)
    assert c.status == "skipped" and "--expect-catalogs" in c.detail
    c = doctor.check_allowlist_matches_contract(ws, ["mig_cat"])
    assert c.status == "ok" and c.data == {"expected": ["mig_cat"], "allowlist": ["mig_cat"]}
    c = doctor.check_allowlist_matches_contract(ws, ["mig_cat", "prod"])
    assert c.status == "fail" and "prod" in c.detail and c.data["expected"] == ["mig_cat", "prod"]
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True, expect_catalogs=["other"])
    assert "allowed_targets=fail" in report["blocking"]
    # the guard accepts backticked / mixed-case spellings and normalizes them; the contract carries the
    # normalized names, so the comparison must use the guard's rule on both sides
    (ws / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["`Mig_Cat`"], "legacy_sources": []}')
    c = doctor.check_allowlist_matches_contract(ws, [" MIG_CAT "])
    assert c.status == "ok" and c.data == {"expected": ["mig_cat"], "allowlist": ["mig_cat"]}
    assert doctor.check_allowlist_matches_contract(ws, ["mig_cat2"]).status == "fail"
    (ws / ".migration" / "allowed_targets.json").write_text("{}")
    assert doctor.check_allowlist_matches_contract(ws, ["mig_cat"]).status == "fail"


def test_run_blocks_on_an_uncommitted_contract_and_the_cli_parses_expect_catalogs(tmp_path):
    ws = make_workspace(tmp_path)
    (ws / ".migration" / "03_recon_tolerances.json").write_text('{"row_count": 0.5}')
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert "allowlist_committed=fail" in report["blocking"]
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--plugin-root",
                        str(PLUGIN_ROOT), "--no-databricks", "--expect-catalogs", "mig_cat, other", "--out", "-"],
                       capture_output=True, text=True, check=False)
    assert "allowed_targets=fail" in r.stdout and "['mig_cat', 'other']" in r.stdout


# ------------------------------------------------------------------ identity + host (A3)

def _fake_cli(monkeypatch, me, describe):
    def run(cmd, timeout=0):
        if cmd[1:3] == ["current-user", "me"]:
            return 0, json.dumps(me), ""
        if cmd[1:3] == ["auth", "describe"]:
            return 0, json.dumps(describe), ""
        return 0, "v0.2", ""
    monkeypatch.setattr(doctor, "_run", run)
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/bin/databricks")


def test_auth_kind_fails_on_conflicting_pat_and_m2m_env(monkeypatch):
    _fake_cli(monkeypatch, {"userName": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"},
              {"status": "success", "details": {"host": "https://adb-1.azuredatabricks.net"}})
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    for name in doctor.M2M_VARS:
        monkeypatch.setenv(name, name.lower())
    row = {c.id: c for c in doctor.check_databricks(None)}["databricks_auth_kind"]
    assert row.status == "fail"
    assert row.data["auth_kind"] == "conflict (env)"
    assert "DATABRICKS_TOKEN" in row.detail


def test_identity_row_records_the_verified_host_and_the_report_exposes_it(tmp_path, monkeypatch):
    sp = {"userName": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"}
    _fake_cli(monkeypatch, sp, {"status": "success", "details": {"host": "https://adb-1.azuredatabricks.net"}})
    checks = {c.id: c for c in doctor.check_databricks(None)}
    assert checks["databricks_identity"].status == "ok"
    assert checks["databricks_identity"].data == {"userName": sp["userName"], "service_principal": True,
                                                  "host": "https://adb-1.azuredatabricks.net"}
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "orchestrator", "blocked", None, False)
    assert report["identity"] == checks["databricks_identity"].data
    # no host means the workflow has nothing to pin children to: fail closed
    _fake_cli(monkeypatch, sp, {"status": "error"})
    checks = {c.id: c for c in doctor.check_databricks(None)}
    assert checks["databricks_identity"].status == "fail" and "host" in checks["databricks_identity"].detail


def test_identity_row_fails_when_the_workspace_is_not_the_expected_host(tmp_path, monkeypatch):
    """The expected principal can resolve against another workspace (a child's own profile or env):
    the host is compared to --expect-host under one spelling rule, not merely required to be set."""
    sp = {"userName": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"}
    _fake_cli(monkeypatch, sp, {"status": "success", "details": {"host": "https://adb-2.azuredatabricks.net"}})

    def row(host):
        return {c.id: c for c in doctor.check_databricks(sp["userName"], host)}["databricks_identity"]

    c = row("https://adb-1.azuredatabricks.net")
    assert c.status == "fail" and "expected host https://adb-1.azuredatabricks.net" in c.detail
    assert c.data["host"] == "https://adb-2.azuredatabricks.net"  # what was seen, for the report
    for spelled in ("https://adb-2.azuredatabricks.net/", "HTTPS://ADB-2.azuredatabricks.net", " adb-2.azuredatabricks.net "):
        assert row(spelled).status == "ok", spelled
    assert row(None).status == "ok"
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", sp["userName"], False,
                        expect_host="https://adb-1.azuredatabricks.net")
    assert report["ready"] is False and "databricks_identity=fail" in report["blocking"]
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--expect-host", "h", "--out", "-"],
                       capture_output=True, text=True, check=False)
    assert r.returncode == 1 and "unrecognized" not in r.stderr and "databricks_identity" in r.stdout


def test_wave_flag_writes_a_signed_record_beside_the_manifest(tmp_path):
    ws = make_workspace(tmp_path)
    manifest = ws / ".migration" / "waves" / "wave-1.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({
        "capabilities": {"identity": "sp-1", "host": "https://h", "catalogs": ["mig_cat"]},
        "source": {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}},
    }))
    hook_probe = probed(ws)
    result = subprocess.run(
        [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--no-databricks",
         "--hook-probe-result", hook_probe, "--wave", str(manifest)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 1
    record_path = manifest.with_suffix(".doctor.json")
    assert record_path.exists()
    record = json.loads(record_path.read_text())
    manifest_bytes = manifest.read_bytes()
    assert record["ready"] is False
    assert record["manifest_sha"] == doctor.manifest_sha(manifest_bytes)
    assert doctor.datetime.datetime.fromisoformat(record["signed_at"]).tzinfo is not None
    assert record["hook_probe"] == hook_probe
    assert record["source"] == json.loads(manifest.read_text())["source"]
    assert doctor.wave_signature(record, manifest_bytes) == record["signature"]
    changed = {**record, "hook_probe": "unknown" if hook_probe != "unknown" else "not-blocked"}
    assert doctor.wave_signature(changed, manifest_bytes) != record["signature"]
    assert next(c for c in record["checks"] if c["id"] == "allowed_targets")

    default_ws = make_workspace(tmp_path / "default")
    default_manifest = default_ws / ".migration" / "waves" / "wave-1.json"
    default_manifest.parent.mkdir()
    default_manifest.write_text(manifest.read_text())
    subprocess.run(
        [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(default_ws), "--no-databricks",
         "--wave", str(default_manifest)],
        capture_output=True, text=True, check=False,
    )
    assert json.loads(default_manifest.with_suffix(".doctor.json").read_text())["hook_probe"] == "unknown"


def test_wave_rejects_explicit_source_settings(tmp_path):
    ws = make_workspace(tmp_path)
    manifest = ws / ".migration" / "waves" / "wave-1.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({
        "capabilities": {"identity": "sp-1", "host": "https://h", "catalogs": ["mig_cat"]},
        "source": {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}},
    }))
    result = subprocess.run(
        [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--no-databricks",
         "--source-secret", "OTHER", "--wave", str(manifest)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 2
    assert "--wave takes source settings from the manifest" in result.stderr
    assert not manifest.with_suffix(".doctor.json").exists()


def test_wave_signature_binds_the_manifest_bytes_and_identity():
    manifest_bytes = b"{...}"
    record = doctor.sign_wave_report(
        {"ready": True, "identity": {"userName": "sp-1", "host": "h"}, "checks": []},
        manifest_bytes, signed_at="2026-01-01T00:00:00+00:00",
    )
    assert doctor.wave_signature(record, manifest_bytes) == record["signature"]
    assert doctor.wave_signature(record, b"{..x}") != record["signature"]
    changed = {**record, "identity": {**record["identity"], "userName": "sp-2"}}
    assert doctor.wave_signature(changed, manifest_bytes) != record["signature"]
    assert doctor.wave_signature({**record, "ready": False}, manifest_bytes) != record["signature"]
    assert record["signed_at"] == "2026-01-01T00:00:00+00:00"


# ------------------------------------------------------------------ hooks.json (post-hint cut)

def test_hooks_json_registers_only_the_guard():
    data = json.loads((PLUGIN_ROOT / "hooks.json").read_text())
    assert list(data) == ["PreToolUse"]
    assert "hooks/dbx_guard.py" in data["PreToolUse"][0]["hooks"][0]["command"]
    assert data["PreToolUse"][0]["matcher"] == "exec"
    assert not (PLUGIN_ROOT / "hooks" / "dbx_post_hint.py").exists()


def test_hook_guard_functional_requires_the_probe_token_in_the_block_reason(tmp_path):
    """rc=2 plus a generic block is not proof the guard read the probe: the reason must echo the
    full `__dbx_guard_probe__<nonce>` token the doctor sent, with a nonce fresh per invocation (the
    prefix, or a token seen in an earlier run, can be hardcoded)."""
    ws = make_workspace(tmp_path)
    fake_root = tmp_path / "plugin"
    (fake_root / "hooks").mkdir(parents=True)
    (fake_root / "hooks.json").write_text(json.dumps(
        {"PreToolUse": [{"matcher": "exec", "hooks": [{"command": "python hooks/dbx_guard.py"}]}]}))
    guard = fake_root / "hooks" / "dbx_guard.py"
    guard.write_text('import sys, json; cmd = json.load(sys.stdin)["tool_input"]["command"]\n'
                     'print(json.dumps({"decision": "block", "reason": "blocked: " + cmd})); sys.exit(2)\n')
    seen = []
    for _ in range(2):
        c = {x.id: x for x in doctor.check_hooks(fake_root, ws, "not-blocked")}["hook_guard_functional"]
        assert c.status == "ok"
        seen.append(re.search(r"__dbx_guard_probe__(\w+)", c.detail).group(1))
    assert seen[0] != seen[1] and "self" not in seen
    for reason in ("generic deny", "__dbx_guard_probe__ is never allowed", f"__dbx_guard_probe__{seen[1]} replayed"):
        guard.write_text(f'import sys; print(\'{{"decision": "block", "reason": "{reason}"}}\'); sys.exit(2)\n')
        c = {x.id: x for x in doctor.check_hooks(fake_root, ws, "not-blocked")}["hook_guard_functional"]
        assert c.status == "fail" and "__dbx_guard_probe__" in c.detail, reason


def test_child_never_live_probes_the_platform_hook(tmp_path):
    ws = make_workspace(tmp_path)
    first = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    nonce = by_id(first)["hook_guard"]["data"]["probe_nonce"]
    _unit_mapping(ws, "loans", evidence=False)
    report = doctor.run(ws, PLUGIN_ROOT, "child", f"blocked:{nonce}", None, True, units=["loans"])
    sub = sub_by_id(report, "hook_guard")
    assert sub["hook_platform_loaded"]["status"] == "warn"
    assert "orchestrator-only" in sub["hook_platform_loaded"]["detail"]
    assert not [b for b in report["blocking"] if b.startswith("hook_guard")]
    # a workspace with no pending nonce stays without one: a child never mints or writes it
    fresh = make_workspace(tmp_path / "fresh")
    _unit_mapping(fresh, "loans", evidence=False)
    doctor.run(fresh, PLUGIN_ROOT, "child", f"blocked:{nonce}", None, True, units=["loans"])
    assert not (fresh / doctor.HOOK_PROBE_NONCE).exists()


def test_child_inherits_platform_row_from_the_signed_record(tmp_path):
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    reused = {"signed_at": "2026-01-01T00:00:00Z",
              "checks": [{"id": "hook_guard", "status": "ok", "detail": "…",
                          "data": {"sub_results": [{"id": "hook_platform_loaded", "status": "ok",
                                                    "detail": "live probe was BLOCKED…",
                                                    "data": {"probe_nonce": "deadbeef"}}]}}]}
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, True, units=["loans"], reused=reused)
    sub = sub_by_id(report, "hook_guard")["hook_platform_loaded"]
    assert sub["status"] == "ok"
    assert sub["detail"].startswith("reused from the orchestrator's record signed 2026-01-01T00:00:00Z")
    assert sub["data"]["reused_from"] == "2026-01-01T00:00:00Z"


# ------------------------------------------------------------------ playbooks in sync

PLAYBOOKS_DIR = PLUGIN_ROOT / "skills" / "install-dbx-factory" / "playbooks"


def test_repo_playbooks_malformed_index_returns_empty(tmp_path):
    playbooks = tmp_path / "skills" / "install-dbx-factory" / "playbooks"
    playbooks.mkdir(parents=True)
    (playbooks / "1-x.md").write_text("# playbook\n")
    for content in ("[]", '{"playbooks": 3}', '{"playbooks": [null]}', "{"):
        (playbooks / "index.json").write_text(content)
        assert doctor._repo_playbooks(tmp_path) == {}


def test_repo_playbooks_valid_index_returns_macro(tmp_path):
    playbooks = tmp_path / "skills" / "install-dbx-factory" / "playbooks"
    playbooks.mkdir(parents=True)
    body = "# playbook\n"
    (playbooks / "1-x.md").write_text(body)
    (playbooks / "index.json").write_text(json.dumps({
        "playbooks": [{"file": "1-x.md", "macro": "!x", "title": "X"}],
    }))

    expected = hashlib.sha256(body.encode()).hexdigest()
    assert doctor._repo_playbooks(tmp_path) == {"!x": ("1-x.md", expected)}


def test_repo_playbooks_keys_are_macros_only():
    repo = doctor._repo_playbooks(PLUGIN_ROOT)
    assert all(m.startswith("!") for m in repo)
    assert "00_intake_template.md" not in repo and "index.json" not in repo
    assert "!dbx_migrate_pipeline" in repo
    assert len(repo) >= 14
    index = json.loads((PLAYBOOKS_DIR / "index.json").read_text())["playbooks"]
    assert [r["macro"] for r in index] == list(repo)
    assert all(r["title"].startswith("[DBX v1] ") for r in index)


def test_playbooks_in_sync_ok(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    expected = {m for m, (f, _sha) in doctor._repo_playbooks(PLUGIN_ROOT).items()}
    on_disk = {p.name for p in PLAYBOOKS_DIR.glob("*.md")} - doctor._NOT_PLAYBOOKS
    assert {f for f, _ in doctor._repo_playbooks(PLUGIN_ROOT).values()} == on_disk
    sha = hashlib.sha256((PLAYBOOKS_DIR / "9-orchestrator.md").read_bytes()).hexdigest()
    assert doctor._repo_playbooks(PLUGIN_ROOT)["!dbx_migrate_pipeline"] == ("9-orchestrator.md", sha)
    assert c.status == "ok" and c.data["checked"] == len(expected) >= 14
    assert c.data["installed_at"] == "2026-01-01T00:00:00Z"
    assert "last install-dbx-factory sync (2026-01-01T00:00:00Z)" in c.detail
    assert "and the live export (0 min old)" in c.detail


def test_orchestrator_playbook_drift_is_a_warning_not_a_blocker(tmp_path):
    ws = make_workspace(tmp_path, with_lock=False)
    _lock(ws, overrides={"!dbx_migrate_pipeline": "0" * 64})
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn"
    assert "!dbx_migrate_pipeline" in c.detail and "install-dbx-factory" in c.detail
    assert c.data["stale"] == ["!dbx_migrate_pipeline"]
    ws = make_workspace(tmp_path / "missing", with_lock=False)
    _lock(ws, drop=("!dbx_migrate_oltp",))
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "child")
    assert c.status == "warn" and c.data["missing"] == ["!dbx_migrate_oltp"]
    ws = make_workspace(tmp_path / "unknown", with_lock=False)
    _lock(ws, overrides={"!dbx_gone": "f" * 64})
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and c.data["unknown"] == ["!dbx_gone"]
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert not [b for b in report["blocking"] if b.startswith("playbooks_in_sync")]


def test_playbooks_in_sync_lock_missing_and_role(tmp_path):
    ws = make_workspace(tmp_path, with_lock=False)
    assert doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "setup").status == "skipped"
    report = doctor.run(ws, PLUGIN_ROOT, "setup", "blocked", None, True)
    assert by_id(report)["playbooks_in_sync"]["status"] == "skipped"
    assert not [b for b in report["blocking"] if b.startswith("playbooks_in_sync")]
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--role", "setup",
                        "--out", "-"], capture_output=True, text=True, check=False)
    row = next(l for l in r.stdout.splitlines() if "playbooks_in_sync" in l)
    assert row.startswith("skipped"), r.stdout
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "install-dbx-factory" in c.detail
    assert doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "child").status == "warn"
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert not [b for b in report["blocking"] if b.startswith("playbooks_in_sync")]


def test_playbooks_in_sync_malformed_entry_fails_not_crashes(tmp_path):
    ws = make_workspace(tmp_path, with_lock=False)
    entries = _lock(ws)
    entries["!dbx_migration_plan"]["installed_at"] = 1
    (ws / doctor.PLAYBOOKS_LOCK).write_text(json.dumps(entries))
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and c.data["malformed"] == ["!dbx_migration_plan"]
    assert "malformed: !dbx_migration_plan" in c.detail and "install-dbx-factory" in c.detail


def test_playbooks_in_sync_unlisted_repo_file_is_a_finding(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    stray = PLAYBOOKS_DIR / "15-unlisted.md"
    stray.write_text("# stray\n")
    try:
        c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    finally:
        stray.unlink()
    assert c.status == "warn" and "15-unlisted.md" in c.detail and "index.json" in c.detail


def test_playbooks_in_sync_ok_checks_the_live_export(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "ok" and "live export" in c.detail
    assert c.data["live"]["checked"] >= 14 and c.data["live"]["age_minutes"] == 0


def test_playbooks_in_sync_orchestrator_needs_the_live_export(tmp_path):
    ws = make_workspace(tmp_path)
    (ws / doctor.LIVE_PLAYBOOKS).unlink()
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "live_playbooks.json" in c.detail
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert not [b for b in report["blocking"] if b.startswith("playbooks_in_sync")]
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "child")
    assert c.status == "ok" and c.data["live"] is None
    assert doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "setup").status == "ok"


def test_playbooks_in_sync_stale_live_export_fails(tmp_path):
    ws = make_workspace(tmp_path)
    _live(ws, age_minutes=20)
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "stale export" in c.detail
    ws2 = make_workspace(tmp_path / "child")
    _live(ws2, age_minutes=20)
    assert doctor.check_playbooks_in_sync(ws2, PLUGIN_ROOT, "child").status == "warn"


def test_playbooks_in_sync_live_drift_fails(tmp_path):
    ws = make_workspace(tmp_path)
    _live(ws, overrides={"!dbx_migrate_pipeline": "# edited live\n"})
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "!dbx_migrate_pipeline" in c.detail
    assert c.data["live_stale"] == ["!dbx_migrate_pipeline"]


def test_playbooks_in_sync_duplicate_macro_fails(tmp_path):
    ws = make_workspace(tmp_path)
    _live(ws, duplicate="!dbx_migrate_etl")
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "!dbx_migrate_etl" in c.detail
    assert "playbook-dbx_migrate_etl" in c.detail and "playbook-extra-copy" in c.detail
    assert sorted(c.data["duplicate"]["!dbx_migrate_etl"]) == [
        "playbook-dbx_migrate_etl", "playbook-extra-copy"]


def test_playbooks_in_sync_live_missing_macro_fails(tmp_path):
    ws = make_workspace(tmp_path)
    _live(ws, drop=("!dbx_migrate_oltp",))
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and c.data["live_missing"] == ["!dbx_migrate_oltp"]


def test_playbooks_in_sync_malformed_live_export_fails_not_crashes(tmp_path):
    ws = make_workspace(tmp_path)
    (ws / doctor.LIVE_PLAYBOOKS).write_text('{"a": 1}')
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "malformed" in c.detail
    (ws / doctor.LIVE_PLAYBOOKS).write_text('[{"macro": "!dbx_migrate_etl", "content": 7}]')
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "warn" and "malformed" in c.detail


def test_playbooks_in_sync_trailing_newline_is_normalized(tmp_path):
    ws = make_workspace(tmp_path)
    _live(ws, overrides={"!dbx_migrate_pipeline": (
        PLAYBOOKS_DIR / "9-orchestrator.md").read_text().rstrip("\n")})
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "orchestrator")
    assert c.status == "ok", c.detail


def test_playbooks_in_sync_cli_live_playbooks_flag(tmp_path):
    ws = make_workspace(tmp_path)
    export = tmp_path / "elsewhere" / "live.json"
    export.parent.mkdir()
    export.write_text((ws / doctor.LIVE_PLAYBOOKS).read_text())
    (ws / doctor.LIVE_PLAYBOOKS).unlink()
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks",
                        "--live-playbooks", str(export), "--out", "-"],
                       capture_output=True, text=True, check=False)
    row = next(l for l in r.stdout.splitlines() if "playbooks_in_sync" in l)
    assert row.startswith("ok"), r.stdout


def test_playbooks_in_sync_explicit_live_path_must_exist(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_playbooks_in_sync(ws, PLUGIN_ROOT, "child", tmp_path / "nowhere.json")
    assert c.status == "warn" and "nowhere.json" in c.detail


# ------------------------------------------------------------------ named_secrets_exist (WS2.6)

def test_manifest_secret_names_collects_lists_and_brief_references():
    manifest = {
        "secrets": ["app/db-host"],
        "batches": [
            {"id": "b-1", "secrets": ["app/db-user"],
             "brief": "read {{secrets/app/db-password}} then dbutils.secrets.get(scope=\"app\", key=\"db-token\")"},
            {"id": "b-2",
             "brief": "also secrets/warehouse/token and secrets.get(\"app\", \"db-host\")"},
        ],
    }
    assert doctor.manifest_secret_names(manifest) == [
        "app/db-host", "app/db-password", "app/db-token", "app/db-user", "warehouse/token"]


def test_manifest_secret_names_parses_secrets_get_in_any_argument_order():
    manifest = {"batches": [{"id": "b", "brief": (
        'a = dbutils.secrets.get(key="password", scope="payments")\n'
        'b = secrets.get("s", key="k")\n'
        "c = dbutils.secrets.get(scope='app', key='db-token')\n"
        'd = secrets.get("only")\n')}]}
    assert doctor.manifest_secret_names(manifest) == ["app/db-token", "payments/password", "s/k"]


@pytest.mark.parametrize("bad", ["a/b", 7, None, ["ok/k", 3]])
def test_manifest_secret_names_rejects_a_non_list_of_strings(bad):
    with pytest.raises(SystemExit, match="must be a list of scope/key strings"):
        doctor.manifest_secret_names({"secrets": bad, "batches": []})
    with pytest.raises(SystemExit, match="must be a list of scope/key strings"):
        doctor.manifest_secret_names({"batches": [{"id": "b", "secrets": bad}]})


def test_wave_manifest_with_bad_secrets_shape_exits_cleanly(tmp_path):
    ws = make_workspace(tmp_path)
    manifest = ws / ".migration" / "waves" / "wave-1.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({
        "capabilities": {"identity": "sp-1", "host": "https://h", "catalogs": ["mig_cat"]},
        "source": {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}},
        "secrets": "app/db-user",
    }))
    result = subprocess.run(
        [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--no-databricks",
         "--hook-probe-result", probed(ws), "--wave", str(manifest)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 1
    assert "must be a list of scope/key strings" in result.stderr
    assert "Traceback" not in result.stderr
    assert not manifest.with_suffix(".doctor.json").exists()


def test_manifest_secret_names_ignores_other_text():
    manifest = {"batches": [{"id": "b-1", "brief": "no secrets here, just secrets talk"}]}
    assert doctor.manifest_secret_names(manifest) == []


def test_check_named_secrets_skipped_without_names():
    c = doctor.check_named_secrets([])
    assert c.status == "skipped" and "no Databricks secret names" in c.detail
    assert c.data == {"checked": [], "missing": [], "unreadable_scopes": []}


def test_check_named_secrets_fails_on_missing_key():
    values = {"app": {"db-user": "s3cr3t-value"}}  # keys the stub returns; values never seen
    c = doctor.check_named_secrets(["app/db-user", "app/db-password"],
                                   lambda scope: list(values.get(scope, {})))
    assert c.status == "fail"
    assert "STOP C" in c.detail and "app/db-password" in c.detail
    assert c.data["missing"] == ["app/db-password"]
    assert c.data["checked"] == ["app/db-user", "app/db-password"]
    assert "s3cr3t-value" not in json.dumps(asdict(c))


def test_check_named_secrets_fails_on_unreadable_scope():
    c = doctor.check_named_secrets(["hidden/key", "app/ok"],
                                   lambda scope: None if scope == "hidden" else ["ok"])
    assert c.status == "fail"
    assert "scope hidden" in c.detail and "list-secrets" in c.detail
    assert c.data["unreadable_scopes"] == ["hidden"]
    assert "hidden/key" in c.data["missing"]


def test_check_named_secrets_fails_on_malformed_name():
    c = doctor.check_named_secrets(["nokey", "app/db-user"], lambda scope: ["db-user"])
    assert c.status == "fail" and "not a scope/key secret name" in c.detail and "nokey" in c.detail


def test_check_named_secrets_ok_when_all_present():
    calls = []

    def ls(scope):
        calls.append(scope)
        return {"app": ["db-user", "db-password"], "wh": ["token"]}[scope]

    c = doctor.check_named_secrets(["app/db-user", "wh/token", "app/db-password"], ls)
    assert c.status == "ok"
    assert "3 named secret(s) exist in 2 scope(s)" in c.detail
    assert sorted(calls) == ["app", "wh"]
    assert c.data["missing"] == [] and c.data["unreadable_scopes"] == []


def test_list_secrets_parses_key_rows(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")
    seen = []

    def fake_run(cmd, timeout=0):
        seen.append(cmd)
        return (0, '[{"key": "db-user", "last_updated_timestamp": 1}, {"key": "db-password"}]', "")

    monkeypatch.setattr(doctor, "_run", fake_run)
    assert doctor._list_secrets("app") == ["db-user", "db-password"]
    assert seen[0][1:] == ["secrets", "list-secrets", "app", "--output", "json"]


def test_list_secrets_accepts_the_wrapped_shape(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")
    monkeypatch.setattr(doctor, "_run",
                        lambda cmd, **kw: (0, '{"secrets": [{"key": "k"}]}', ""))
    assert doctor._list_secrets("app") == ["k"]


def test_list_secrets_none_without_cli_on_error_or_on_non_json(monkeypatch):
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)
    assert doctor._list_secrets("app") is None
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/local/bin/databricks")
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (1, "", "denied"))
    assert doctor._list_secrets("app") is None
    monkeypatch.setattr(doctor, "_run", lambda cmd, **kw: (0, "not json", ""))
    assert doctor._list_secrets("app") is None


def test_run_named_secrets_skipped_offline_and_never_blocking(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True,
                        secret_names=["app/db-user"])
    row = by_id(report)["named_secrets_exist"]
    assert row["status"] == "skipped" and row["detail"] == "--no-databricks"
    assert not [b for b in report["blocking"] if b.startswith("named_secrets_exist")]


def test_run_named_secrets_fail_is_blocking(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_identity", "ok", "as sp", {"userName": "sp-1"}),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, False,
                        secret_names=["app/db-user"], list_secrets=lambda scope: [])
    row = by_id(report)["named_secrets_exist"]
    assert row["status"] == "fail" and "named_secrets_exist=fail" in report["blocking"]


def test_wave_manifest_secrets_reach_the_report(tmp_path):
    ws = make_workspace(tmp_path)
    manifest = ws / ".migration" / "waves" / "wave-1.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({
        "capabilities": {"identity": "sp-1", "host": "https://h", "catalogs": ["mig_cat"]},
        "source": {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}},
        "secrets": ["app/db-user"],
        "batches": [{"id": "b-1", "secrets": ["app/db-password"],
                     "brief": "uses {{secrets/wh/token}} too"}],
    }))
    result = subprocess.run(
        [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--no-databricks",
         "--hook-probe-result", probed(ws), "--wave", str(manifest)],
        capture_output=True, text=True, check=False,
    )
    record = json.loads(manifest.with_suffix(".doctor.json").read_text())
    row = next(c for c in record["checks"] if c["id"] == "named_secrets_exist")
    assert row["status"] == "skipped" and row["detail"] == "--no-databricks"


def test_secret_flag_passes_names_to_the_check(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_identity", "ok", "as sp", {"userName": "sp-1"}),
    ])
    seen = {}

    def stub(names, ls=None):
        seen["names"] = list(names)
        return doctor.Check("named_secrets_exist", "ok", "captured")

    monkeypatch.setattr(doctor, "check_named_secrets", stub)
    doctor.main(["--workspace", str(ws), "--plugin-root", str(PLUGIN_ROOT),
                 "--secret", "app/db-user", "--secret", "wh/token", "--out", "-"])
    assert seen["names"] == ["app/db-user", "wh/token"]


# --------------------------------------------------------- reused wave records (WS4.1)

def _signed_record(manifest, *, identity="sp-1", host="https://adb-1", rows=None, **overrides):
    """An orchestrator's wave-<N>.doctor.json: sign_wave_report over a ready report whose checks
    carry one row per reusable id, against manifest_bytes of this exact manifest."""
    manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
    rows = rows if rows is not None else [
        {"id": rid, "status": "ok", "detail": f"{rid} done", "data": {"k": rid}}
        for rid in doctor.REUSABLE_ROWS]
    report = {"schema": "dbx-migration-factory/capabilities/1", "role": "orchestrator",
              "ready": True, "identity": {"userName": identity, "host": host}, "inputs_sha": "inputs-a",
              "checks": rows, "summary": {}, "blocking": [], **overrides}
    return doctor.sign_wave_report(report, manifest_bytes,
                                  signed_at=overrides.pop("signed_at", None)), manifest_bytes


def test_reusable_record_accepts_a_fresh_signed_orchestrator_record():
    manifest = {"wave": 1}
    record, manifest_bytes = _signed_record(manifest)
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "SP-1", "https://adb-1")
    assert got is record and why == ""


def test_reusable_record_rejects_a_record_older_than_doctor_max_age():
    manifest = {"wave": 1}
    record, manifest_bytes = _signed_record(
        manifest, signed_at="2020-01-01T00:00:00+00:00")
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None and "age" in why

    manifest5 = {"wave": 1, "doctor_max_age": 5}
    recent = (doctor.datetime.datetime.now(doctor.datetime.timezone.utc)
              - doctor.datetime.timedelta(minutes=10)).isoformat()
    record, manifest_bytes = _signed_record(manifest5, signed_at=recent)
    got, why = doctor.reusable_record(record, manifest5, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None and "age" in why
    record, manifest_bytes = _signed_record({"wave": 1}, signed_at=recent)
    got, why = doctor.reusable_record(record, {"wave": 1}, manifest_bytes, "sp-1", "https://adb-1")
    assert got is record


def test_reusable_record_binds_the_manifest_bytes_and_the_signature():
    manifest = {"wave": 1}
    record, manifest_bytes = _signed_record(manifest)
    got, why = doctor.reusable_record(record, {"wave": 2}, b'{"wave": 2}',
                                      "sp-1", "https://adb-1")
    assert got is None and "manifest" in why
    tampered = json.loads(json.dumps(record))
    tampered["checks"][0]["detail"] = "edited"
    got, why = doctor.reusable_record(tampered, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None and "signature" in why


def test_reusable_record_requires_the_expected_identity_and_host():
    manifest = {"wave": 1}
    record, manifest_bytes = _signed_record(manifest)
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "other", "https://adb-1")
    assert got is None and "identity" in why
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-2")
    assert got is None and "host" in why
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, None, None)
    assert got is None


def test_reusable_record_rejects_non_orchestrator_not_ready_and_malformed():
    manifest = {"wave": 1}
    record, manifest_bytes = _signed_record(manifest, role="child")
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None
    record, manifest_bytes = _signed_record(manifest, ready=False)
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None
    for bad in (None, "x", {"role": "orchestrator"}, {"role": "orchestrator", "ready": True}):
        got, why = doctor.reusable_record(bad, manifest, manifest_bytes, "sp-1", "https://adb-1")
        assert got is None and why, bad
    record, manifest_bytes = _signed_record(manifest, signed_at="not-a-date")
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None
    future = (doctor.datetime.datetime.now(doctor.datetime.timezone.utc)
              + doctor.datetime.timedelta(minutes=1)).isoformat()
    record, manifest_bytes = _signed_record(manifest, signed_at=future)
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert got is None


SAFETY_ROWS = ("source_principal_read_only", "named_secrets_exist", "recon_family_supported")
LOCAL_ROWS = ("recon_harness",)


def test_reusable_rows_are_the_non_safety_checkout_bound_ones():
    """Reuse is policy, not the signature: a record any manifest reader could forge may only stand in
    for rows that read the checkout and the source (type map, dictionary, delete evidence); the rows
    that guard the source and the secrets always run in the child, and so does recon_harness, whose
    driver imports describe the orchestrator's machine, not the child's."""
    assert set(doctor.REUSABLE_ROWS) == {"type_map_audit", "delete_evidence", "dictionary_readable"}
    assert not set(doctor.REUSABLE_ROWS) & set(SAFETY_ROWS + LOCAL_ROWS)


def test_run_reuses_the_signed_source_side_rows_and_keeps_identity_fresh(tmp_path, monkeypatch):
    manifest = {"wave": 1, "capabilities": {"identity": "sp-1", "host": "https://adb-1"}}
    rows = [{"id": rid, "status": "ok", "detail": f"{rid} done",
             "data": {"k": rid, "target_kind": "databricks"}}
            for rid in (*doctor.REUSABLE_ROWS, *SAFETY_ROWS, *LOCAL_ROWS)]
    record, manifest_bytes = _signed_record(manifest, rows=rows)
    reused, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    assert reused is record

    def boom(*a, **k):
        raise AssertionError("computed a reusable row")

    for name in ("check_type_map_audit", "check_delete_evidence_all", "check_dictionary_readable_all"):
        monkeypatch.setattr(doctor, name, boom)
    for name, rid in (("check_recon_family_supported", "recon_family_supported"),
                      ("check_source_principal_all", "source_principal_read_only"),
                      ("check_named_secrets", "named_secrets_exist"),
                      ("check_harness", "recon_harness"),
                      ("check_drivers", "recon_drivers")):
        monkeypatch.setattr(doctor, name, lambda *a, _rid=rid, **k: doctor.Check(_rid, "ok", "fresh"))
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_identity", "ok", "as sp", {"userName": "sp-1",
                                                          "host": "https://adb-1"})])
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "child", "blocked", "sp-1", False,
                        expect_host="https://adb-1", reused=reused)
    rows = by_id(report)
    for rid in doctor.REUSABLE_ROWS:
        assert rows[rid]["status"] == "ok", rid
        assert rows[rid]["detail"].startswith("reused from the orchestrator's record signed"), rid
        assert rows[rid]["data"]["reused_from"] == reused["signed_at"]
        assert rows[rid]["reusable"] is True
    for rid in SAFETY_ROWS:
        assert rows[rid]["detail"] == "fresh" and rows[rid]["reusable"] is False, rid
    assert rows["recon_harness"]["reusable"] is False
    assert "reused from" not in rows["recon_harness"]["detail"]
    assert rows["databricks_identity"]["reusable"] is False
    assert report["reused_doctor"] == reused["signed_at"]
    assert "reused from" not in rows["databricks_identity"]["detail"]


def test_run_recomputes_type_map_audit_when_the_recorded_target_kind_differs(tmp_path, monkeypatch):
    """Type maps are keyed by source family and target kind: a Lakebase child must not stand on the
    orchestrator's Databricks audit, and a recorded row that names no target kind proves nothing."""
    manifest = {"wave": 1, "capabilities": {"identity": "sp-1", "host": "https://adb-1"}}
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_identity", "ok", "as sp", {"userName": "sp-1",
                                                          "host": "https://adb-1"})])
    monkeypatch.setattr(doctor, "check_type_map_audit",
                        lambda *a, target_kind="databricks", **k:
                        doctor.Check("type_map_audit", "ok", "fresh", {"target_kind": target_kind}))
    ws = make_workspace(tmp_path)
    for recorded, child in (("databricks", "lakebase"), (None, "databricks")):
        data = {"k": "x"} if recorded is None else {"k": "x", "target_kind": recorded}
        rows = [{"id": "type_map_audit", "status": "ok", "detail": "audited", "data": data}]
        record, manifest_bytes = _signed_record(manifest, rows=rows)
        reused, _ = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
        assert reused is record
        report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", "sp-1", True,
                            expect_host="https://adb-1", target_kind=child, reused=reused)
        row = by_id(report)["type_map_audit"]
        assert row["detail"] == "fresh" and row["data"]["target_kind"] == child, (recorded, child)
    rows = [{"id": "type_map_audit", "status": "ok", "detail": "audited",
             "data": {"target_kind": "lakebase"}}]
    record, manifest_bytes = _signed_record(manifest, rows=rows)
    reused, _ = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1")
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", "sp-1", True,
                        expect_host="https://adb-1", target_kind="lakebase", reused=reused)
    assert by_id(report)["type_map_audit"]["detail"].startswith("reused from")


def test_run_computes_a_reusable_row_the_record_lacks(tmp_path, monkeypatch):
    manifest = {"wave": 1}
    rows = [r for r in _signed_record(manifest)[0]["checks"] if r["id"] != "dictionary_readable"]
    record, manifest_bytes = _signed_record(manifest, rows=rows)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect, host=None: [
        doctor.Check("databricks_identity", "ok", "as sp", {"userName": "sp-1",
                                                          "host": "https://adb-1"})])
    monkeypatch.setattr(doctor, "check_dictionary_readable_all",
                        lambda *a, **k: doctor.Check("dictionary_readable", "unverified", "fresh"))
    report = doctor.run(make_workspace(tmp_path), PLUGIN_ROOT, "child", "blocked", "sp-1", False,
                        expect_host="https://adb-1", reused=record)
    assert by_id(report)["dictionary_readable"]["detail"] == "fresh"


def _reuse_ws(tmp_path, manifest=None, **record_overrides):
    ws = make_workspace(tmp_path)
    waves = ws / ".migration" / "waves"
    waves.mkdir(exist_ok=True)
    manifest = manifest or {"wave": 1, "capabilities": {"identity": "sp-1", "host": "https://adb-1",
                                                        "catalogs": ["mig_cat"]}}
    manifest_path = waves / "wave-1.json"
    record_overrides.setdefault("inputs_sha", doctor.inputs_sha(ws))
    record, manifest_bytes = _signed_record(manifest, **record_overrides)
    manifest_path.write_bytes(manifest_bytes)
    record_path = waves / "wave-1.doctor.json"
    record_path.write_text(json.dumps(record))
    return ws, manifest_path, record_path


def test_reuse_record_rejects_bad_flag_combinations(tmp_path):
    ws, manifest_path, record_path = _reuse_ws(tmp_path)
    base = [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--role", "child",
            "--reuse-record", str(record_path), "--expect-identity", "sp-1", "--out", "-"]
    for extra in (["--wave", str(manifest_path)], ["--role", "orchestrator"], ["--no-databricks"]):
        r = subprocess.run(base + extra, capture_output=True, text=True, check=False)
        assert r.returncode == 2, (extra, r.stderr)
    r = subprocess.run(base[:-4] + base[-2:], capture_output=True, text=True, check=False)
    assert r.returncode == 2 and "--expect-identity" in r.stderr


def test_reuse_record_reuses_rows_and_reports_it(tmp_path):
    ws, manifest_path, record_path = _reuse_ws(tmp_path)
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--role", "child", "--reuse-record", str(record_path), "--expect-identity", "sp-1",
                        "--expect-host", "https://adb-1", "--out", "-"],
                       capture_output=True, text=True, check=False)
    assert "doctor_record" in r.stdout and "reused" in r.stdout
    assert "reused from the orchestrator's record" in r.stdout
    out = tmp_path / "caps.json"
    subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                    "--role", "child", "--reuse-record", str(record_path), "--expect-identity", "sp-1",
                    "--expect-host", "https://adb-1", "--out", str(out)],
                   capture_output=True, text=True, check=False)
    rows = json.loads(out.read_text())["checks"]
    assert all(isinstance(c.get("reusable"), bool) for c in rows)
    assert next(c for c in rows if c["id"] == "doctor_record")["reusable"] is False


def test_reuse_record_falls_back_to_a_full_run_when_not_reusable(tmp_path):
    ws, manifest_path, record_path = _reuse_ws(tmp_path, signed_at="2020-01-01T00:00:00+00:00")
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--role", "child", "--reuse-record", str(record_path), "--expect-identity", "sp-1",
                        "--expect-host", "https://adb-1", "--out", "-"],
                       capture_output=True, text=True, check=False)
    assert "doctor record not reused:" in r.stderr
    assert "reused from the orchestrator's record" not in r.stdout


def test_reusable_record_requires_the_checkouts_inputs_to_match():
    """The record binds the inputs its source-side rows were computed from; a child whose
    .migration/units or top-level .migration/*.json differ runs in full."""
    manifest = {"wave": 1}
    record, manifest_bytes = _signed_record(manifest)
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1",
                                      inputs_sha="inputs-a")
    assert got is record and why == ""
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1",
                                      inputs_sha="inputs-b")
    assert got is None and "inputs" in why
    record, manifest_bytes = _signed_record(manifest, inputs_sha=None)
    got, why = doctor.reusable_record(record, manifest, manifest_bytes, "sp-1", "https://adb-1",
                                      inputs_sha="inputs-a")
    assert got is None and "inputs_sha" in why


def test_inputs_sha_follows_unit_mappings_and_migration_json_not_the_doctors_own_output(tmp_path):
    ws = make_workspace(tmp_path)
    before = doctor.inputs_sha(ws)
    (ws / ".migration" / "09_capabilities.json").write_text("{}")
    assert doctor.inputs_sha(ws) == before
    unit = ws / ".migration" / "units" / "u1"
    unit.mkdir(parents=True)
    (unit / "mapping_spec.json").write_text('{"target_type": "STRING"}')
    changed = doctor.inputs_sha(ws)
    assert changed != before
    (unit / "mapping_spec.json").write_text('{"target_type": "DOUBLE"}')
    assert doctor.inputs_sha(ws) not in (before, changed)
    (ws / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["other"]}')
    assert doctor.inputs_sha(ws) not in (before, changed)


def test_orchestrator_wave_record_carries_the_inputs_sha(tmp_path):
    ws = make_workspace(tmp_path)
    manifest = ws / ".migration" / "waves" / "wave-1.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({"capabilities": {"identity": "sp-1", "host": "https://h",
                                                      "catalogs": ["mig_cat"]}}))
    subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--no-databricks",
                    "--hook-probe-result", probed(ws), "--wave", str(manifest)],
                   capture_output=True, text=True, check=False)
    record = json.loads(manifest.with_suffix(".doctor.json").read_text())
    assert record["inputs_sha"] == doctor.inputs_sha(ws)
    assert record["signature"] == doctor.wave_signature(record, manifest.read_bytes())


def test_reuse_record_falls_back_to_a_full_run_when_the_checkout_inputs_differ(tmp_path):
    ws, manifest_path, record_path = _reuse_ws(tmp_path)
    unit = ws / ".migration" / "units" / "u1"
    unit.mkdir(parents=True)
    (unit / "mapping_spec.json").write_text('{"target_type": "STRING"}')
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--role", "child", "--reuse-record", str(record_path), "--expect-identity", "sp-1",
                        "--expect-host", "https://adb-1", "--out", "-"],
                       capture_output=True, text=True, check=False)
    assert "doctor record not reused:" in r.stderr and "inputs" in r.stderr
    assert "reused from the orchestrator's record" not in r.stdout


def test_reuse_record_accepts_the_manifests_own_source_flags(tmp_path):
    """The child brief names --source-family/--source-secret/--param from the manifest; the same
    values beside --reuse-record are fine, different ones are the error."""
    manifest = {"wave": 1, "capabilities": {"identity": "sp-1", "host": "https://adb-1", "catalogs": ["mig_cat"]},
                "source": {"family": "oracle", "secret": "LEGACY_DSN", "params": {"db": "loans"}}}
    ws, manifest_path, record_path = _reuse_ws(tmp_path, manifest=manifest)
    base = [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--role", "child",
            "--reuse-record", str(record_path), "--expect-identity", "sp-1", "--expect-host", "https://adb-1",
            "--out", "-"]
    r = subprocess.run(base + ["--source-family", "oracle", "--source-secret", "LEGACY_DSN", "--param", "db=loans"],
                       capture_output=True, text=True, check=False)
    assert r.returncode != 2, r.stderr
    assert "reused from the orchestrator's record" in r.stdout
    for extra in (["--source-family", "sqlserver"], ["--source-secret", "OTHER"], ["--param", "db=cards"]):
        r = subprocess.run(base + extra, capture_output=True, text=True, check=False)
        assert r.returncode == 2 and "manifest" in r.stderr, (extra, r.stderr)
