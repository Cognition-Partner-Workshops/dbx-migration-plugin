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
    p.write_text(json.dumps({"version": "m1", "objects": [obj]}))
    return p


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
    mapping = _mapping(tmp_path, root_where="Amount > ${floor}")
    seen = {}

    def fake(m, s, root, params=None):
        seen["params"] = params
        return doctor.Check("delete_evidence", "ok", "")

    monkeypatch.setattr(doctor, "check_delete_evidence", fake)
    argv = ["--workspace", str(ws), "--plugin-root", str(PLUGIN_ROOT), "--no-databricks",
            "--mapping", str(mapping), "--source-secret", "X", "--out", "-"]
    doctor.main([*argv, "--param", "floor=100", "--param", "day=2026-01-01"])
    assert seen["params"] == {"floor": "100", "day": "2026-01-01"}
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
    real = doctor.check_delete_evidence
    monkeypatch.setattr(doctor, "check_delete_evidence",
                        lambda m, s, root, params=None: doctor.Check("delete_evidence", "fail", "CDC is not enabled")
                        if m is not None else real(m, s, root, params=params))
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, True, mapping=Path("m.json"),
                        source_secret="LEGACY_ODBC")
    assert "delete_evidence=fail" in report["blocking"]
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, True)
    assert by_id(report)["delete_evidence"]["status"] == "skipped"
