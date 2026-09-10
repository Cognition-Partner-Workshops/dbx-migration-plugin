import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

SKILL = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = SKILL.parents[1]
sys.path.insert(0, str(SKILL))
sys.path.insert(0, str(PLUGIN_ROOT / "hooks"))

import doctor  # noqa: E402
import dbx_guard  # noqa: E402


def make_workspace(tmp_path: Path, *, allowed=None, stop_mode="hard", omit=()):
    mig = tmp_path / ".migration"
    mig.mkdir()
    for f in doctor.REQUIRED_FILES:
        if f in omit:
            continue
        if f == "allowed_targets.json":
            mig.joinpath(f).write_text(json.dumps(allowed if allowed is not None else
                                                  {"catalogs": ["mig_cat"], "legacy_sources": ["LEGACY_DSN"]}))
        elif f == "00_context.md":
            mig.joinpath(f).write_text(f"# context\n\nstop_mode: {stop_mode}\n")
        else:
            mig.joinpath(f).write_text(f"# {f}\n")
    return tmp_path


def by_id(report):
    return {c["id"]: c for c in report["checks"]}


def test_probe_command_is_blocked_by_guard_and_harmless_otherwise():
    cfg = dbx_guard.GuardConfig.from_dict({"catalogs": ["mig_cat"]})
    v = dbx_guard.evaluate(doctor.HOOK_PROBE_COMMAND, cfg)
    assert v.decision == "block" and "__dbx_guard_probe__" in v.reason
    assert doctor.HOOK_PROBE_COMMAND.startswith("echo ")


def test_offline_run_passes_every_local_check_but_is_never_ready(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, no_databricks=True)
    c = by_id(report)
    assert not [x for x in report["checks"] if x["status"] == "fail"]
    # an unverified identity can never certify a wave, however the check was skipped
    assert not report["ready"] and report["blocking"] == ["databricks_identity=skipped"]
    assert c["workspace"]["status"] == "ok"
    assert c["stop_mode"]["data"]["stop_mode"] == "hard"
    assert c["allowed_targets"]["status"] == "ok" and c["allowed_targets"]["data"]["catalogs"] == ["mig_cat"]
    assert c["hooks_files"]["status"] == "ok"
    assert c["hook_guard_functional"]["status"] == "ok"
    assert c["hook_platform_loaded"]["status"] == "ok"
    assert c["recon_harness"]["status"] == "ok"
    assert c["databricks_identity"]["status"] == "skipped"


def test_unknown_probe_is_unverified_and_carries_command(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, True)
    c = by_id(report)
    assert c["hook_platform_loaded"]["status"] == "unverified"
    assert c["hook_platform_loaded"]["data"]["probe_command"] == doctor.HOOK_PROBE_COMMAND
    assert not report["ready"]
    assert report["blocking"] == ["hook_platform_loaded=unverified", "databricks_identity=skipped"]


def test_human_identity_is_not_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "warn", "pat (env)"),
        doctor.Check("databricks_identity", "warn", "authenticated as someone@example.com (user)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, no_databricks=False)
    assert report["summary"].get("fail", 0) == 0
    assert not report["ready"] and report["blocking"] == ["databricks_identity=warn"]


def test_service_principal_with_advisory_warns_is_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "ok", "oauth-m2m (env)"),
        doctor.Check("databricks_identity", "ok", "authenticated as 1234-sp (service principal)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, no_databricks=False)
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
    """Each probe module is the one the harness adapter actually imports (Lakebase is psycopg 3,
    Redshift is psycopg2); a wrong name would report a working environment as missing a driver."""
    adapters = (PLUGIN_ROOT / "skills" / "data-reconciliation" / "harness" / "recon" / "adapters.py").read_text()
    imported = set(re.findall(r"^\s+import ([A-Za-z_][\w.]*)  # lazy", adapters, re.MULTILINE))
    imported |= {f"{a}.{b}" for a, b in re.findall(r"^\s+from ([\w.]+) import (\w+)  # lazy", adapters, re.MULTILINE)}
    for engine, module in doctor.DRIVERS.items():
        assert module in imported, f"{engine}: doctor probes {module}, adapters never import it"
    assert doctor.DRIVERS["postgres"] == "psycopg" and doctor.DRIVERS["redshift"] == "psycopg2"


def test_not_blocked_probe_fails(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "not-blocked", None, True)
    assert not report["ready"]
    assert by_id(report)["hook_platform_loaded"]["status"] == "fail"


def test_missing_files_and_stop_mode_fail(tmp_path):
    ws = make_workspace(tmp_path, omit=("05_progress.md",))
    (ws / ".migration" / "00_context.md").write_text("# no mode here\n")
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail" and "05_progress.md" in c["workspace"]["data"]["missing"]
    assert c["stop_mode"]["status"] == "fail"


def test_setup_outputs_glossary_and_tolerances_json_are_required(tmp_path):
    ws = make_workspace(tmp_path, omit=("02_glossary.md", "03_recon_tolerances.json"))
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail"
    assert c["workspace"]["data"]["missing"] == ["02_glossary.md", "03_recon_tolerances.json"]


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
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 1, r.stdout + r.stderr  # offline: identity unverified, so not ready
    cap = json.loads((ws / ".migration" / "09_capabilities.json").read_text())
    assert cap["schema"] == "dbx-migration-factory/capabilities/1" and cap["ready"] is False
    assert cap["blocking"] == ["databricks_identity=skipped"]
    assert "ready=False" in r.stdout and "databricks_identity=skipped" in r.stdout

    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "not-blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 1


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

def _mapping(tmp_path: Path, *, evidence=True) -> Path:
    obj = {"object": "loans", "root_table": "raw.loans", "key": {"source": ["Id"], "target": "id"},
           "fields": [{"source": "Id", "target": "id"}],
           "delete_evidence": {"kind": "sqlserver_cdc", "capture": "raw_loans",
                               "applied_position": {"table": "cdc_checkpoint", "column": "lsn"}}}
    if not evidence:
        del obj["delete_evidence"]
    p = tmp_path / "mapping.json"
    p.write_text(json.dumps({"version": "m1", "objects": [obj]}))
    return p


class FakeCdcConn:
    """Read-only SQL Server fixture: answers the doctor's three metadata queries and records
    every statement so a test can prove nothing else (in particular no sp_cdc_enable_*) ran."""

    def __init__(self, *, db_enabled=1, can_read=1, captures=("raw_loans",)):
        self.db_enabled, self.can_read, self.captures = db_enabled, can_read, list(captures)
        self.statements: list[str] = []

    def cursor(self):
        return self

    def execute(self, sql, *params):
        self.statements.append(sql)
        if "is_cdc_enabled" in sql:
            self._rows = [(self.db_enabled,)]
        elif "HAS_PERMS_BY_NAME" in sql:
            self._rows = [(self.can_read,)]
        elif "cdc.change_tables" in sql:
            self._rows = [(c,) for c in self.captures]
        else:
            raise AssertionError(f"unexpected statement: {sql}")
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def test_delete_evidence_skipped_without_mapping_and_ok_when_none_declared(tmp_path):
    c = doctor.check_delete_evidence(None, None, PLUGIN_ROOT)
    assert c.status == "skipped" and "--mapping" in c.detail
    c = doctor.check_delete_evidence(_mapping(tmp_path, evidence=False), None, PLUGIN_ROOT)
    assert c.status == "ok" and "no object declares delete_evidence" in c.detail


def test_delete_evidence_declared_needs_a_named_source_secret(tmp_path, monkeypatch):
    monkeypatch.delenv("LEGACY_ODBC", raising=False)
    c = doctor.check_delete_evidence(_mapping(tmp_path), None, PLUGIN_ROOT)
    assert c.status == "fail" and "--source-secret" in c.detail
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT)
    assert c.status == "fail" and "LEGACY_ODBC" in c.detail and "not set" in c.detail


def test_delete_evidence_ok_reads_only_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x;Server=y;PWD=never-printed")
    conn = FakeCdcConn()
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    assert c.data == {"kind": "sqlserver_cdc", "captures": ["raw_loans"], "missing": []}
    assert "never-printed" not in c.detail
    assert all(s.lstrip().upper().startswith("SELECT") for s in conn.statements)
    assert not any("sp_cdc" in s.lower() for s in conn.statements)


@pytest.mark.parametrize("kw, needle", [
    ({"db_enabled": 0}, "CDC is not enabled"),
    ({"can_read": 0}, "cannot read the cdc schema"),
    ({"captures": ("raw_payments",)}, "raw_loans"),
])
def test_delete_evidence_failures_name_the_source_side_gap(tmp_path, monkeypatch, kw, needle):
    monkeypatch.setenv("LEGACY_ODBC", "Driver=x")
    conn = FakeCdcConn(**kw)
    c = doctor.check_delete_evidence(_mapping(tmp_path), "LEGACY_ODBC", PLUGIN_ROOT, connect=lambda dsn: conn)
    assert c.status == "fail" and needle in c.detail
    assert "sp_cdc_enable" not in c.detail.replace("never runs sp_cdc_enable", "")
    assert not any("sp_cdc" in s.lower() for s in conn.statements)


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
    real = doctor.check_delete_evidence
    monkeypatch.setattr(doctor, "check_delete_evidence",
                        lambda m, s, root: doctor.Check("delete_evidence", "fail", "CDC is not enabled")
                        if m is not None else real(m, s, root))
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, True, mapping=Path("m.json"),
                        source_secret="LEGACY_ODBC")
    assert "delete_evidence=fail" in report["blocking"]
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, True)
    assert by_id(report)["delete_evidence"]["status"] == "skipped"
