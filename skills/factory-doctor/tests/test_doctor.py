import json
import re
import subprocess
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


def make_workspace(tmp_path: Path, *, allowed=None, stop_mode="hard", omit=(), commit=True):
    """A setup-complete workspace, committed as the setup playbook leaves it before STOP A."""
    mig = tmp_path / ".migration"
    mig.mkdir(parents=True)
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
    _git(tmp_path, "init", "-q")
    if commit:
        _git(tmp_path, "add", "-A")
        _git(tmp_path, "commit", "-qm", "setup")
    return tmp_path


def by_id(report):
    return {c["id"]: c for c in report["checks"]}


def probed(ws):
    """What a session does before the live probe: one doctor run (report written) issues the nonce the probe echoes."""
    first = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps(first))
    return "blocked:" + by_id(first)["hook_platform_loaded"]["data"]["probe_nonce"]


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
    assert c["stop_mode"]["data"]["stop_mode"] == "hard"
    assert c["allowed_targets"]["status"] == "ok" and c["allowed_targets"]["data"]["catalogs"] == ["mig_cat"]
    assert c["hooks_files"]["status"] == "ok"
    assert c["hook_guard_functional"]["status"] == "ok"
    assert c["hook_platform_loaded"]["status"] == "ok"
    assert c["recon_harness"]["status"] == "ok"
    assert c["databricks_identity"]["status"] == "skipped"


def test_unknown_probe_is_unverified_and_carries_command(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    c = by_id(report)
    assert c["hook_platform_loaded"]["status"] == "unverified"
    nonce = c["hook_platform_loaded"]["data"]["probe_nonce"]
    assert re.fullmatch(r"[0-9a-f]{8}", nonce)
    assert c["hook_platform_loaded"]["data"]["probe_command"] == doctor.HOOK_PROBE_COMMAND.format(nonce=nonce)
    assert "blocked:<nonce>" in c["hook_platform_loaded"]["detail"]
    assert not report["ready"]
    assert report["blocking"] == ["hook_platform_loaded=unverified", "databricks_identity=skipped"]


def test_platform_probe_is_verified_by_the_nonce_the_last_report_issued(tmp_path):
    ws = make_workspace(tmp_path)
    first = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "unknown", None, True)
    (ws / ".migration" / "09_capabilities.json").write_text(json.dumps(first))
    nonce = by_id(first)["hook_platform_loaded"]["data"]["probe_nonce"]
    ok = doctor.run(ws, PLUGIN_ROOT, "orchestrator", f"blocked:{nonce}", None, True)
    assert by_id(ok)["hook_platform_loaded"]["status"] == "ok"
    assert by_id(ok)["hook_platform_loaded"]["data"] == {"probe_nonce": nonce}
    # a nonce that was never issued for this workspace (typed, stale, or from another report)
    # proves nothing: the row stays unverified and a fresh nonce is issued
    for claim in ("blocked:deadbeef", "blocked:"):
        again = doctor.run(ws, PLUGIN_ROOT, "orchestrator", claim, None, True)
        row = by_id(again)["hook_platform_loaded"]
        assert row["status"] == "unverified" and "did not match" in row["detail"]
        assert row["data"]["probe_nonce"] not in (nonce, "deadbeef")
    # a workspace with no prior report has no nonce to match, so nothing verifies it yet
    fresh = doctor.run(make_workspace(tmp_path / "fresh"), PLUGIN_ROOT, "orchestrator", f"blocked:{nonce}", None, True)
    assert by_id(fresh)["hook_platform_loaded"]["status"] == "unverified"


def test_cli_rejects_a_bare_blocked_claim(tmp_path):
    ws = make_workspace(tmp_path)
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--plugin-root",
                        str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 2 and "blocked:<nonce>" in r.stderr
    assert not (ws / ".migration" / "09_capabilities.json").exists()


def test_human_identity_is_not_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "warn", "pat (env)"),
        doctor.Check("databricks_identity", "warn", "authenticated as someone@example.com (user)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", probed(ws), None, no_databricks=False)
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
    argv = [sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws), "--plugin-root", str(PLUGIN_ROOT),
            "--no-databricks"]
    r = subprocess.run(argv, capture_output=True, text=True, check=False)
    assert r.returncode == 1, r.stdout + r.stderr
    cap = json.loads((ws / ".migration" / "09_capabilities.json").read_text())
    nonce = by_id(cap)["hook_platform_loaded"]["data"]["probe_nonce"]
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
    assert "delete_evidence=fail" in report["blocking"]
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
    table to the write privileges it holds; `absent` tables cannot be resolved. Indirection: `impersonate`
    lists principals the principal may IMPERSONATE, `execute` the procedures/functions it may EXECUTE,
    `set_role` maps a Postgres role it is a member of (directly or through `direct`, the roles it was granted
    itself, when given) to the write it would gain via SET ROLE, `unusable` names memberships granted with
    `SET FALSE, INHERIT FALSE` (Postgres 16: a path exists, but neither SET ROLE nor inheritance can use it);
    `columns` maps a table to {column: [privileges]} granted at column level only (invisible to the table-level query).
    Records every statement so a test can prove the check only ever asked questions."""

    def __init__(self, *, roles=(), writable=None, absent=(), read_only="on", impersonate=(), execute=(),
                 set_role=None, columns=None, direct=None, unusable=()):
        self.roles, self.writable, self.absent, self.read_only = set(roles), writable or {}, set(absent), read_only
        self.impersonate, self.execute_on, self.set_role = list(impersonate), list(execute), set_role or {}
        self.columns, self.direct = columns or {}, list(self.set_role if direct is None else direct)
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
        elif "fn_my_permissions" in low or "has_column_privilege" in low:
            self._rows = [(c, p) for c, privs in self.columns.get(args[0], {}).items() for p in privs]
        elif "has_perms_by_name" in low:
            t = args[0]
            self._rows = [(None,) * 4 if t in self.absent else
                          tuple(int(p in self.writable.get(t, ())) for p in ("INSERT", "UPDATE", "DELETE", "ALTER"))]
        elif "rolsuper" in low and "pg_auth_members" not in low:
            names = re.findall(r"\b(rolsuper|rolcreaterole|rolcreatedb|rolbypassrls)\b|pg_has_role\(current_user, '(\w+)'", sql)
            self._rows = [tuple("".join(n) in self.roles for n in names)]
        elif "pg_auth_members" in low:  # direct memberships only
            self._rows = [(r,) for r in self.direct]
        elif "pg_has_role(current_user, oid," in low:  # transitive: every role reachable through memberships
            reachable = {*self.direct, *self.set_role}
            if "'usage, set'" in low:  # a 16+ server answers only for memberships that inherit or can SET ROLE
                reachable -= self.unusable
            self._rows = [(r,) for r in sorted(reachable)]
        elif "pg_proc" in low:
            self._rows = [(f, "EXECUTE") for f in self.execute_on]
        elif "has_table_privilege" in low and len(args) == 4:  # (role, table, role, schema): the SET ROLE walk
            role, t = args[0], args[1]
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
    # an unresolved table is not asked about column by column either
    conn = FakePrivConn(absent=["raw.loans"])
    doctor.check_source_principal(["raw.loans"], "sqlserver", "LEGACY_ODBC", connect=lambda dsn: conn)
    assert not any("fn_my_permissions" in s for s, _ in conn.statements)


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
                       ({"absent": ["public.loans"]}, "does not exist"),
                       # indirection: role attributes, file/program roles, function EXECUTE, SET ROLE to a writer
                       ({"roles": ["rolcreaterole"]}, "role rolcreaterole"),
                       ({"roles": ["rolcreatedb"]}, "role rolcreatedb"),
                       ({"roles": ["rolbypassrls"]}, "role rolbypassrls"),
                       ({"roles": ["pg_write_server_files"]}, "role pg_write_server_files"),
                       ({"roles": ["pg_execute_server_program"]}, "role pg_execute_server_program"),
                       ({"execute": ["public.post_payment(integer)"]}, "public.post_payment(integer): EXECUTE"),
                       ({"set_role": {"loader": ["public.loans"]}}, "SET ROLE loader: public.loans write"),
                       # nested: granted only `etl`, which is itself a member of the writer `loader`
                       ({"set_role": {"loader": ["public.loans"]}, "direct": ["etl"]}, "SET ROLE loader: public.loans write"),
                       ({"columns": {"public.loans": {"balance": ["INSERT", "UPDATE"]}}},
                        "public.loans: INSERT on column balance, UPDATE on column balance")):
        c = doctor.check_source_principal(["public.loans"], "postgres", "LAKEBASE_SRC",
                                          connect=lambda dsn, kw=kw: FakePrivConn(**kw))
        assert c.status == "fail" and needle in c.detail, (kw, c.detail)
    # a membership whose role holds no in-scope write is not a finding, but is still asked about per table
    conn = FakePrivConn(set_role={"readers": []})
    c = doctor.check_source_principal(["raw.loans", "raw.payments"], "postgres", "LAKEBASE_SRC", connect=lambda dsn: conn)
    assert c.status == "ok", c.detail
    walk = [a[0] for s, a in conn.statements if s.startswith("SELECT has_table_privilege") and len(a[0]) == 4]
    assert walk == [("readers", "raw.loans", "readers", "raw"), ("readers", "raw.payments", "readers", "raw")]
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
    fn = [(s, a[0]) for s, a in conn.statements if "pg_proc" in s]
    assert len(fn) == 1 and fn[0][1] == (["raw"],) and "prosecdef" in fn[0][0] and "has_function_privilege" in fn[0][0]
    # column grants: one query per table over pg_attribute, excluding what the table-level grant already covers
    cols = [(s, a[0]) for s, a in conn.statements if "has_column_privilege" in s]
    assert [a for _, a in cols] == [("raw.loans",), ("raw.payments",)]
    assert "pg_attribute" in cols[0][0] and "NOT has_table_privilege" in cols[0][0] and "attisdropped" in cols[0][0]
    assert any("pg_has_role(current_user, 'pg_write_server_files', 'MEMBER')" in s and "rolbypassrls" in s
               for s, _ in conn.statements)


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
    # unverified for a declared source blocks readiness; a fail does too
    assert "source_principal_read_only=unverified" in report["blocking"]
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
    assert "--source-family" in r.stdout and "--expect-catalogs" in r.stdout and "blocked:<nonce>" in r.stdout
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(make_workspace(tmp_path)),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--source-family", "mysql"],
                       capture_output=True, text=True, check=False)
    assert r.returncode == 2 and "--source-family" in r.stderr


def test_source_families_are_the_harness_families_and_databricks_is_unverified(tmp_path, monkeypatch):
    """The doctor accepts exactly the families `dbx-recon run --family` accepts (a Databricks-to-Databricks
    unit is one); a family without a tested privilege query is `unverified` and blocks, never `ok`."""
    cli = (PLUGIN_ROOT / "skills" / "data-reconciliation" / "harness" / "recon" / "cli.py").read_text()
    families = re.search(r"^SOURCE_FAMILIES = \((.*)\)$", cli, re.MULTILINE).group(1)
    assert set(doctor.SOURCE_FAMILIES) == set(re.findall(r'"(\w+)"', families)) and "databricks" in doctor.SOURCE_FAMILIES
    monkeypatch.setenv("SRC_DBX", "token=never-printed")
    c = doctor.check_source_principal(["mig.raw.loans"], "databricks", "SRC_DBX")
    assert c.status == "unverified" and "databricks" in c.detail and "never-printed" not in json.dumps(asdict(c))
    ws = make_workspace(tmp_path)
    _unit_mapping(ws, "loans", evidence=False)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True, source_secret="SRC_DBX",
                        source_family="databricks")
    assert by_id(report)["source_principal_read_only"]["status"] == "unverified"
    assert "source_principal_read_only=unverified" in report["blocking"] and report["ready"] is False


# ------------------------------------------------------------------ ledger integrity rows (A2c)

def test_allowlist_committed_is_ok_only_when_both_contract_files_equal_head(tmp_path):
    ws = make_workspace(tmp_path)
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "ok" and c.data == {".migration/allowed_targets.json": "clean",
                                           ".migration/03_recon_tolerances.json": "clean"}
    (ws / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat", "prod"]}))
    c = doctor.check_allowlist_committed(ws)
    assert c.status == "fail" and c.data[".migration/allowed_targets.json"] == "modified since HEAD"
    assert "allowed_targets.json modified since HEAD" in c.detail and "03_recon_tolerances" not in c.detail
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
    assert c.status == "fail" and c.data[".migration/03_recon_tolerances.json"] == "modified since HEAD"


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
    assert "allowlist_matches_contract=fail" in report["blocking"]
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
    assert "allowlist_matches_contract=fail" in r.stdout and "['mig_cat', 'other']" in r.stdout


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


# ------------------------------------------------------------------ hooks.json (post-hint cut)

def test_hooks_json_registers_only_the_guard():
    data = json.loads((PLUGIN_ROOT / "hooks.json").read_text())
    assert list(data) == ["PreToolUse"]
    assert "hooks/dbx_guard.py" in data["PreToolUse"][0]["hooks"][0]["command"]
    assert data["PreToolUse"][0]["matcher"] == "exec"
    assert not (PLUGIN_ROOT / "hooks" / "dbx_post_hint.py").exists()


def test_hook_guard_functional_requires_the_probe_token_in_the_block_reason(tmp_path):
    """rc=2 plus a generic block is not proof the guard read the probe: the reason must echo the
    full `__dbx_guard_probe__<nonce>` token the doctor sent (the bare prefix can be hardcoded)."""
    ws = make_workspace(tmp_path)
    fake_root = tmp_path / "plugin"
    (fake_root / "hooks").mkdir(parents=True)
    (fake_root / "hooks.json").write_text(json.dumps(
        {"PreToolUse": [{"matcher": "exec", "hooks": [{"command": "python hooks/dbx_guard.py"}]}]}))
    guard = fake_root / "hooks" / "dbx_guard.py"
    for reason in ("generic deny", "__dbx_guard_probe__ is never allowed"):
        guard.write_text(f'import sys; print(\'{{"decision": "block", "reason": "{reason}"}}\'); sys.exit(2)\n')
        c = {x.id: x for x in doctor.check_hooks(fake_root, ws, "not-blocked")}["hook_guard_functional"]
        assert c.status == "fail" and "__dbx_guard_probe__self" in c.detail, reason
    guard.write_text('import sys, json; cmd = json.load(sys.stdin)["tool_input"]["command"]\n'
                     'print(json.dumps({"decision": "block", "reason": "blocked: " + cmd})); sys.exit(2)\n')
    assert {x.id: x for x in doctor.check_hooks(fake_root, ws, "not-blocked")}["hook_guard_functional"].status == "ok"
