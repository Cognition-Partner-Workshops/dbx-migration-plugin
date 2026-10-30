#!/usr/bin/env python3
"""tools/tests/test_doctor.py — offline tests for tools/doctor.py (design-0.7.md §5)."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import doctor  # noqa: E402

POLICY = {
    "version": 2,
    "repo": "github.com/example/run-repo",
    "base_branch": "mig/run",
    "migration_principal": "sp-123",
    "targets": {
        "catalog": "mig_cat",
        "schema_prefix": "mig_run_",
        "warehouse_http_path": "/sql/1.0/warehouses/wh1",
        "lakebase": {"project": "lb-proj", "branch": "mig-run",
                     "endpoint_host": "ep-x.database.example.cloud.databricks.com"},
    },
    "forbidden_catalogs": ["prod_cat"],
    "forbidden_targets": ["prod", "production"],
    "bundle_targets": ["migration"],
    "legacy_sources": ["LEGACY_DSN"],
    "width": 3,
    "breaker": 3,
    "source": {"family": "oracle", "secret": "LEGACY_DSN", "access": "fed_cat.legacy"},
    "tolerances": {"version": "tol-v1", "exact": True, "blocking_findings": [], "source_lag_s": 900},
}
SLICES = {
    "version": 1,
    "slices": [
        {"id": "slice-0", "name": "reference", "track": "delta", "tables": ["BILLING.TENANTS"],
         "routines": [], "jobs": [], "consumers": [], "depends_on": [], "routing": "none"},
        {"id": "slice-1", "name": "plans", "track": "lakebase", "tables": ["BILLING.SUBSCRIPTIONS"],
         "routines": ["PKG_PLANS.*"], "jobs": [], "consumers": ["billing-api"],
         "depends_on": ["slice-0"], "routing": "jdbc_url", "route_group": "billing-api"},
    ],
}
GOOD_ENV = {"DATABRICKS_AUTH_TYPE": "oauth-m2m"}

def make_dbx(fixtures: dict):
    calls = []

    def dbx(args):
        calls.append(" ".join(args))
        for key in sorted(fixtures, key=len, reverse=True):
            val = fixtures[key]
            if key in " ".join(args):
                if isinstance(val, Exception):
                    raise val
                return val
        raise AssertionError("unexpected dbx call: " + " ".join(args))

    dbx.calls = calls
    return dbx

def grants(*privs, principal="sp-123"):
    return {"privilege_assignments": [{"principal": principal,
                                       "privileges": [{"privilege": p} for p in privs]}]}

def ok_dbx(**overrides):
    fixtures = {
        "current-user me": {"userName": "sp-123", "servicePrincipalName": "sp-123"},
        "SHOW CATALOGS": {"status": {"state": "SUCCEEDED"},
                          "result": {"data_array": [["mig_cat"], ["fed_cat"], ["prod_cat"]]}},
        "catalogs list": {"catalogs": [{"name": "mig_cat", "owner": "admin"},
                                       {"name": "fed_cat", "owner": "admin"},
                                       {"name": "prod_cat", "owner": "admin"}]},
        "SHOW SCHEMAS": {"status": {"state": "SUCCEEDED"}, "result": {"data_array": []}},
        '"statement": "SELECT 1"': {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [[1]]}},
        "grants get-effective catalog mig_cat": grants("ALL_PRIVILEGES"),
        "grants get-effective catalog fed_cat": grants("USE_CATALOG", "SELECT"),
        "grants get-effective catalog prod_cat": grants("USE_CATALOG", "SELECT"),
        "external-locations list": {"external_locations": []},
        "storage-credentials list": {"storage_credentials": []},
        "connections list": {"connections": []},
        "postgres list-projects": {"projects": [{"name": "projects/lb-proj"}]},
        "postgres list-branches projects/lb-proj": {
            "branches": [{"name": "projects/lb-proj/branches/mig-run"},
                         {"name": "projects/lb-proj/branches/production"}]},
        "list-roles projects/lb-proj/branches/mig-run": {
            "roles": [{"name": "projects/lb-proj/branches/mig-run/roles/sp-123"}]},
        "list-roles projects/lb-proj/branches/production": {"roles": []},
        "psql": "",
    }
    fixtures.update(overrides)
    return make_dbx(fixtures)

def make_run_cmd(fixtures: dict | None = None):
    fixtures = {"dbx-recon selftest": (0, "", ""), "guard.py": (2, '{"decision": "block"}', ""),
                **(fixtures or {})}
    calls = []

    def run_cmd(argv, cwd=None, stdin=""):
        calls.append(argv)
        if argv[0] == "git":
            return doctor.default_run_cmd(argv, cwd=cwd, stdin=stdin)
        for key, val in fixtures.items():
            if key in " ".join(argv):
                return val
        raise AssertionError("unexpected cmd: " + " ".join(argv))

    run_cmd.calls = calls
    return run_cmd

def make_repo(tmp_path, policy=POLICY, slices=SLICES, commit=True):
    repo = tmp_path / "run"
    mig = repo / ".migration"
    mig.mkdir(parents=True)
    (mig / "policy.json").write_text(json.dumps(policy))
    (mig / "slices.json").write_text(json.dumps(slices))
    (mig / "base").write_text("main")
    subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, capture_output=True)

    def git(*a):
        subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True)

    if commit:
        git("add", "-A")
        git("-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qm", "x")
    return repo

def make_plugin(tmp_path, guard_body="", name="dbx_guard.py"):
    plugin = tmp_path / "plugin"
    (plugin / "hooks").mkdir(parents=True)
    (plugin / "hooks" / name).write_text(guard_body)
    return plugin

def run(repo, plugin, dbx=None, run_cmd=None, env=None, run_sql=None):
    return doctor.run_doctor(repo, dbx=dbx or ok_dbx(), run_cmd=run_cmd or make_run_cmd(),
                             env=GOOD_ENV if env is None else env, run_sql=run_sql, plugin_root=plugin)

def statuses(report):
    return {c["check"]: c["status"] for c in report["checks"]}

def test_identity_ok():
    st, data = doctor.check_identity(POLICY, ok_dbx(), GOOD_ENV)
    assert st == "ok" and data["auth_type"] == "oauth-m2m"

def test_identity_pat_fails():
    st, data = doctor.check_identity(POLICY, ok_dbx(), {"DATABRICKS_AUTH_TYPE": "pat"})
    assert st == "fail" and "PAT" in data["reason"]

def test_identity_mismatch_fails():
    st, _ = doctor.check_identity(POLICY, ok_dbx(**{"current-user me": {"userName": "other-sp"}}), GOOD_ENV)
    assert st == "fail"

def test_identity_auth_unset_fails():
    st, _ = doctor.check_identity(POLICY, ok_dbx(), {})
    assert st == "fail"

def test_isolation_ok(tmp_path):
    st, data = doctor.check_isolation(POLICY, ok_dbx())
    assert st == "ok" and data["access_select"] is True

def test_isolation_source_unreadable_fails():
    st, data = doctor.check_isolation(
        POLICY, ok_dbx(**{"grants get-effective catalog fed_cat": {"privilege_assignments": []}}))
    assert st == "fail" and data["access_select"] is False

def test_isolation_access_all_privileges_satisfies_select_manage_alone_does_not():
    for privs in (("ALL_PRIVILEGES",), ("ALL_PRIVILEGES", "MANAGE")):
        st, data = doctor.check_isolation(POLICY, ok_dbx(**{"grants get-effective catalog fed_cat": grants(*privs)}))
        assert data["access_select"] is True
        assert st == "fail" and all(f"fed_cat:{p}" in data["writes_outside_targets"] for p in privs)
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{"grants get-effective catalog fed_cat": grants("MANAGE")}))
    assert st == "fail" and data["access_select"] is False
    same_cat = {**POLICY, "source": {**POLICY["source"], "access": "mig_cat.legacy_fed"}}
    st, data = doctor.check_isolation(same_cat, ok_dbx(**{"grants get-effective catalog mig_cat": grants("MANAGE")}))
    assert st == "fail" and data["access_select"] is False

def test_isolation_read_only_uc_privileges_are_not_writes():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "grants get-effective catalog prod_cat": grants("USE_CATALOG", "USE_SCHEMA", "BROWSE", "READ_METADATA",
                                                        "SELECT"),
        "SHOW SCHEMAS IN `prod_cat`": {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [["s1"]]}},
        "grants get-effective schema prod_cat.s1": grants("USE_SCHEMA", "BROWSE", "READ_METADATA")}))
    assert st == "ok" and data["writes_outside_targets"] == []

def test_isolation_catalog_ownership_fails():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "catalogs list": {"catalogs": [{"name": "prod_cat", "owner": "sp-123"}]}}))
    assert st == "fail" and "catalog prod_cat:owns" in data["writes_outside_targets"]

def test_isolation_schema_write_fails():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "SHOW SCHEMAS IN `prod_cat`": {"status": {"state": "SUCCEEDED"},
                                      "result": {"data_array": [["secret"]]}},
        "grants get-effective schema prod_cat.secret": grants("USE_SCHEMA", "MODIFY")}))
    assert st == "fail" and "prod_cat.secret:MODIFY" in data["writes_outside_targets"]

def test_isolation_write_outside_targets_fails():
    st, data = doctor.check_isolation(
        POLICY, ok_dbx(**{"grants get-effective catalog prod_cat": grants("SELECT", "MODIFY")}))
    assert st == "fail" and any("prod_cat:MODIFY" in w for w in data["writes_outside_targets"])

def test_isolation_external_location_write_fails():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "external-locations list": {"external_locations": [{"name": "ext1"}]},
        "grants get-effective external_location ext1": grants("CREATE_EXTERNAL_TABLE")}))
    assert st == "fail" and any("ext1" in w for w in data["writes_outside_targets"])

def test_isolation_lakebase_production_role_fails():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "list-roles projects/lb-proj/branches/production": {
            "roles": [{"name": "projects/lb-proj/branches/production/roles/sp-123"}]}}))
    assert st == "fail" and data["lakebase_role_violations"]

def test_isolation_extra_read_warns():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "SHOW CATALOGS": {"status": {"state": "SUCCEEDED"},
                          "result": {"data_array": [["mig_cat"], ["fed_cat"], ["weird_cat"]]}},
        "grants get-effective catalog weird_cat": grants("USE_CATALOG", "SELECT")}))
    assert st == "warn" and any("weird_cat" in r for r in data["unexpected_read_grants"])

def test_isolation_unreadable_catalog_records_error_and_continues():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "SHOW CATALOGS": {"status": {"state": "SUCCEEDED"},
                          "result": {"data_array": [["other_cat"], ["mig_cat"], ["fed_cat"], ["prod_cat"]]}},
        "grants get-effective catalog other_cat": doctor.DbxError(1, "PERMISSION_DENIED")}))
    assert st == "warn" and data["access_select"] is True and not data["writes_outside_targets"]
    assert any(e.startswith("grants other_cat:") for e in data["errors"])

def test_isolation_admins_group_fails():
    st, data = doctor.check_isolation(POLICY, ok_dbx(**{
        "current-user me": {"userName": "sp-123", "groups": [{"display": "admins", "value": "g1"}]}}))
    assert st == "fail" and data["admin_groups"] == ["admins"]

def test_policy_committed_ok(tmp_path):
    repo = make_repo(tmp_path)
    st, _ = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "ok"

def test_policy_committed_dirty_fails(tmp_path):
    repo = make_repo(tmp_path)
    (repo / ".migration" / "policy.json").write_text(json.dumps({**POLICY, "width": 9}))
    st, data = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "fail" and "differs" in data["reason"]

def test_policy_committed_uncommitted_fails(tmp_path):
    repo = make_repo(tmp_path, commit=False)
    subprocess.run(["git", "-C", str(repo), "add", ".migration/base"], check=True, capture_output=True)
    st, data = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "fail" and data["reason"] == "uncommitted working copy"

def test_policy_committed_invalid_schema_fails(tmp_path):
    repo = make_repo(tmp_path, policy={**POLICY, "version": 1})
    st, data = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "fail" and any("version" in e for e in data["errors"])

def test_policy_committed_missing_fails(tmp_path):
    repo = tmp_path / "run"
    (repo / ".migration").mkdir(parents=True)
    st, data = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "fail" and "missing" in data["reason"]

def test_policy_committed_waves_warns(tmp_path):
    repo = make_repo(tmp_path)
    (repo / ".migration" / "waves").mkdir()
    st, data = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "warn" and "0.6 workspace" in data["reason"]

def test_policy_committed_lakebase_gap_fails(tmp_path):
    pol = {**POLICY, "targets": {**POLICY["targets"]}}
    del pol["targets"]["lakebase"]
    repo = make_repo(tmp_path, policy=pol)
    st, data = doctor.check_policy_committed(repo, make_run_cmd())
    assert st == "fail" and any("lakebase" in e for e in data["errors"])

def oracle_run_sql(fixtures):
    def run_sql(sql):
        for key, rows in fixtures.items():
            if key in sql:
                return rows
        raise AssertionError("unexpected sql: " + sql)
    return run_sql


ORACLE_OK = {
    "session_privs": [("CREATE SESSION",), ("SELECT ANY TABLE",)],
    "all_tab_privs": [("BILLING", "TENANTS", "SELECT")],
    "session_user": [("MIG_RO",)],
}

def test_source_read_only_oracle_ok():
    st, _ = doctor.check_source_read_only(POLICY, SLICES["slices"], ok_dbx(),
                                          oracle_run_sql(ORACLE_OK), "sp-123")
    assert st == "ok"

def test_source_read_only_oracle_tab_privs_query_uses_table_schema():
    sql = dict(doctor._source_queries("oracle", doctor._objects(SLICES["slices"])))["tab_privs"]
    assert "table_schema" in sql and "owner" not in sql.lower() and "'BILLING'" in sql
    assert "privilege, type FROM all_tab_privs" in sql

def test_source_read_only_oracle_ignores_user_type_privs():
    inherit = [("BILLING", "BILLING", "INHERIT PRIVILEGES", "USER")]
    for rows in (inherit, [{"table_schema": "BILLING", "table_name": "BILLING",
                            "privilege": "INHERIT PRIVILEGES", "type": "USER"}]):
        st, data = doctor.check_source_read_only(
            POLICY, SLICES["slices"], ok_dbx(),
            oracle_run_sql({**ORACLE_OK, "all_tab_privs": [("BILLING", "TENANTS", "SELECT", "TABLE"), *rows]}),
            "sp-123")
        assert st == "ok" and data["violations"] == []
    for priv in ("INSERT", "UPDATE", "DELETE", "ALTER"):
        st, data = doctor.check_source_read_only(
            POLICY, SLICES["slices"], ok_dbx(),
            oracle_run_sql({**ORACLE_OK, "all_tab_privs": inherit + [("BILLING", "TENANTS", priv, "TABLE")]}), "sp-123")
        assert st == "fail" and len(data["violations"]) == 1 and priv in data["violations"][0]

def test_source_read_only_oracle_scoped_to_slice_schemas():
    public_stock = [("SYS", "DBMS_OUTPUT", "EXECUTE"), ("MDSYS", "SDO_GEOM", "EXECUTE"), ("XDB", "XDB$ACL", "SELECT")]
    st, data = doctor.check_source_read_only(POLICY, SLICES["slices"], ok_dbx(),
                                             oracle_run_sql({**ORACLE_OK, "all_tab_privs": public_stock}), "sp-123")
    assert st == "ok" and data["violations"] == []
    st, data = doctor.check_source_read_only(
        POLICY, SLICES["slices"], ok_dbx(),
        oracle_run_sql({**ORACLE_OK, "all_tab_privs": public_stock + [("BILLING", "TENANTS", "INSERT")]}), "sp-123")
    assert st == "fail" and any("INSERT" in v for v in data["violations"])

def failing_adapter(policy):
    raise RuntimeError("DPY-6005: cannot connect to database (timed out)")

def test_main_source_adapter_error_is_a_fail_row(tmp_path, capsys):
    repo, plugin = make_repo(tmp_path), make_plugin(tmp_path)
    rc = doctor.main(["--repo", str(repo), "--source-adapter", f"{__name__}:failing_adapter"],
                     dbx=ok_dbx(), run_cmd=make_run_cmd(), env=GOOD_ENV, plugin_root=plugin)
    assert rc == 1
    report = json.loads(capsys.readouterr().out)
    row = next(c for c in report["checks"] if c["check"] == "source_read_only")
    assert row["status"] == "fail" and "DPY-6005" in row["data"]["error"]
    assert statuses(report)["identity"] == "ok"

def test_source_read_only_oracle_write_priv_fails():
    st, data = doctor.check_source_read_only(
        POLICY, SLICES["slices"], ok_dbx(),
        oracle_run_sql({**ORACLE_OK, "session_privs": [("CREATE SESSION",), ("DELETE ANY TABLE",)]}), "sp-123")
    assert st == "fail" and any("DELETE ANY TABLE" in v for v in data["violations"])

def test_source_read_only_oracle_owns_schema_fails():
    st, data = doctor.check_source_read_only(
        POLICY, SLICES["slices"], ok_dbx(),
        oracle_run_sql({**ORACLE_OK, "session_user": [("BILLING",)]}), "sp-123")
    assert st == "fail" and "owns slice schema" in data["violations"]

def test_source_read_only_no_adapter_warns():
    st, _ = doctor.check_source_read_only(POLICY, SLICES["slices"], ok_dbx(), None, "sp-123")
    assert st == "warn"

def test_source_read_only_unknown_family_warns():
    st, _ = doctor.check_source_read_only({**POLICY, "source": {"family": "teradata", "access": "x"}},
                                          SLICES["slices"], ok_dbx(), None, "sp-123")
    assert st == "warn"

def test_source_read_only_sqlserver_role_fails():
    fixtures = {"fn_my_permissions(NULL,'SERVER'": [("VIEW SERVER STATE",)],
                "fn_my_permissions(NULL,'DATABASE'": [("CONNECT",), ("SELECT",)],
                "IS_SRVROLEMEMBER": [{"sysadmin": 1, "db_owner": 0, "db_datawriter": 0,
                                      "db_ddladmin": 0, "db_securityadmin": 0}],
                "fn_my_permissions('BILLING": [], "fn_my_permissions('PKG_PLANS'": [],
                "sys.objects": []}
    pol = {**POLICY, "source": {"family": "sqlserver", "secret": "D", "access": "fed_cat.legacy"}}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(),
                                             oracle_run_sql(fixtures), "sp-123")
    assert st == "fail" and "role:sysadmin" in data["violations"]

def test_source_read_only_sqlserver_glob_expands():
    fixtures = {"fn_my_permissions(NULL,'SERVER'": [("VIEW SERVER STATE",)],
                "fn_my_permissions(NULL,'DATABASE'": [("CONNECT",), ("SELECT",)],
                "IS_SRVROLEMEMBER": [{"sysadmin": 0, "db_owner": 0, "db_datawriter": 0,
                                      "db_ddladmin": 0, "db_securityadmin": 0}],
                "sys.objects": [("PKG_PLANS.proc_a",)],
                "fn_my_permissions('PKG_PLANS.proc_a'": [("EXECUTE",)],
                "fn_my_permissions('PKG_PLANS'": [],
                "fn_my_permissions('BILLING": []}
    pol = {**POLICY, "source": {"family": "sqlserver", "secret": "D", "access": "fed_cat.legacy"}}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(),
                                             oracle_run_sql(fixtures), "sp-123")
    assert st == "fail" and any("PKG_PLANS.proc_a" in v for v in data["violations"])


PG_BASE = {"pg_roles": [{"rolsuper": False, "rolcreatedb": False, "rolcreaterole": False,
                        "rolbypassrls": False}],
           "has_schema_privilege": [], "pg_proc": [], "pg_class": []}

def test_source_read_only_postgres_can_write_fails():
    fixtures = {**PG_BASE,
                "has_table_privilege": [("BILLING.TENANTS", False), ("BILLING.SUBSCRIPTIONS", True)]}
    pol = {**POLICY, "source": {"family": "postgres", "secret": "D", "access": "fed_cat.legacy"}}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(),
                                             oracle_run_sql(fixtures), "sp-123")
    assert st == "fail" and "table:BILLING.SUBSCRIPTIONS" in data["violations"]

def test_source_read_only_postgres_schema_create_fails():
    fixtures = {**PG_BASE, "has_table_privilege": [],
                "has_schema_privilege": [("BILLING", False, True)]}
    pol = {**POLICY, "source": {"family": "postgres", "secret": "D", "access": "fed_cat.legacy"}}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(),
                                             oracle_run_sql(fixtures), "sp-123")
    assert st == "fail" and "schema-create:BILLING" in data["violations"]

def test_source_read_only_postgres_no_tables_ok():
    fixtures = dict(PG_BASE)   # no has_table_privilege fixture: the query must not run
    pol = {**POLICY, "source": {"family": "postgres", "secret": "D", "access": "fed_cat.legacy"}}
    st, _ = doctor.check_source_read_only(pol, [{**SLICES["slices"][0], "tables": [], "routines": []}],
                                          ok_dbx(), oracle_run_sql(fixtures), "sp-123")
    assert st == "ok"

def test_source_read_only_postgres_routine_execute_fails():
    fixtures = {**PG_BASE, "has_table_privilege": [],
                "pg_proc": [("PKG_PLANS.reset(int)",)], "has_function_privilege": [(True,)]}
    pol = {**POLICY, "source": {"family": "postgres", "secret": "D", "access": "fed_cat.legacy"}}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(),
                                             oracle_run_sql(fixtures), "sp-123")
    assert st == "fail" and "routine:PKG_PLANS.reset(int)" in data["violations"]


EXISTING_GRANTS = {
    "grants get-effective catalog src_cat": grants("USE_CATALOG", "SELECT"),
    "grants get-effective schema src_cat.BILLING": grants("USE_SCHEMA", "SELECT"),
    "grants get-effective schema src_cat.PKG_PLANS": grants("USE_SCHEMA", "SELECT"),
    "grants get-effective table src_cat.BILLING.TENANTS": grants("SELECT"),
    "grants get-effective table src_cat.BILLING.SUBSCRIPTIONS": grants("SELECT"),
}

def test_source_read_only_databricks_existing():
    pol = {**POLICY, "source": {"family": "databricks", "kind": "existing", "access": "src_cat"}}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(**EXISTING_GRANTS),
                                             None, "sp-123")
    assert st == "ok"

def test_source_read_only_databricks_existing_write_fails():
    pol = {**POLICY, "source": {"family": "databricks", "kind": "existing", "access": "src_cat"}}
    bad = {**EXISTING_GRANTS,
           "grants get-effective table src_cat.BILLING.SUBSCRIPTIONS": grants("SELECT", "MODIFY")}
    st, data = doctor.check_source_read_only(pol, SLICES["slices"], ok_dbx(**bad), None, "sp-123")
    assert st == "fail" and any("MODIFY" in w for w in data["write_privileges"])

def test_hook_live_ok(tmp_path):
    st, data = doctor.check_hook_live(make_plugin(tmp_path), make_run_cmd())
    assert st == "ok" and data["decision"] == "block" and data["guard"] == "dbx_guard.py"

def test_hook_live_probes_from_armed_temp_checkout(tmp_path):
    plugin, seen = make_plugin(tmp_path), {}
    inner = make_run_cmd()

    def run_cmd(argv, cwd=None, stdin=""):
        if argv[-1].endswith("dbx_guard.py"):
            event = json.loads(stdin)
            seen.update(cwd=cwd, event_cwd=event["cwd"],
                        armed=(Path(event["cwd"]) / ".migration" / "policy.json").is_file(),
                        committed=doctor.default_run_cmd(["git", "status", "--porcelain"], cwd=cwd)[1].strip() == "")
        return inner(argv, cwd=cwd, stdin=stdin)

    st, _ = doctor.check_hook_live(plugin, run_cmd)
    assert st == "ok" and seen["armed"] and seen["committed"]
    assert Path(seen["cwd"]) == Path(seen["event_cwd"]) and not Path(seen["cwd"]).is_relative_to(plugin)

def test_hook_live_real_guard_blocks_sentinel():
    plugin_root = Path(doctor.__file__).resolve().parents[1]
    st, data = doctor.check_hook_live(plugin_root, doctor.default_run_cmd)
    assert st == "ok" and data["exit"] == 2 and data["decision"] == "block", data

def test_hook_live_missing_fails(tmp_path):
    st, data = doctor.check_hook_live(tmp_path / "plugin", make_run_cmd())
    assert st == "fail" and "missing" in data["reason"]

BLOCKING_GUARD = 'import sys; print(\'{"decision": "block"}\'); sys.exit(2)'

def test_hook_live_relative_plugin_root_resolves_guard(tmp_path, monkeypatch):
    make_plugin(tmp_path, guard_body=BLOCKING_GUARD)
    monkeypatch.chdir(tmp_path)
    st, data = doctor.check_hook_live(Path("plugin"), doctor.default_run_cmd)
    assert st == "ok" and data["decision"] == "block", data

def test_hook_live_exit_2_without_block_decision_fails(tmp_path):
    st, data = doctor.check_hook_live(make_plugin(tmp_path),
                                      make_run_cmd({"guard.py": (2, "python: can't open file", "")}))
    assert st == "fail" and data["decision"] is None

def test_hook_live_not_firing_fails(tmp_path):
    st, data = doctor.check_hook_live(make_plugin(tmp_path), make_run_cmd({"guard.py": (0, "", "")}))
    assert st == "fail" and data["exit"] == 0

def test_harness_ok():
    st, data = doctor.check_harness(POLICY, SLICES["slices"], ok_dbx(), make_run_cmd())
    assert st == "ok" and data["warehouse"] == "ok" and data["lakebase"] == "ok"

def test_harness_selftest_fails():
    st, data = doctor.check_harness(POLICY, SLICES["slices"], ok_dbx(),
                                  make_run_cmd({"dbx-recon selftest": (1, "", "boom")}))
    assert st == "fail" and data["selftest_rc"] == 1

def test_harness_warehouse_unreachable_fails():
    st, data = doctor.check_harness(POLICY, SLICES["slices"], ok_dbx(**{
        '"statement": "SELECT 1"': {"status": {"state": "FAILED"}}}), make_run_cmd())
    assert st == "fail" and "warehouse" in data["reason"]

def test_harness_lakebase_unreachable_fails():
    st, data = doctor.check_harness(POLICY, SLICES["slices"], ok_dbx(**{
        "psql": doctor.DbxError(2, "connection refused")}), make_run_cmd())
    assert st == "fail" and "lakebase" in data["reason"]

def test_harness_delta_only_skips_lakebase():
    st, data = doctor.check_harness(POLICY, [SLICES["slices"][0]], ok_dbx(), make_run_cmd())
    assert st == "ok" and "lakebase" not in data

def test_run_doctor_ready(tmp_path):
    report = run(make_repo(tmp_path), make_plugin(tmp_path),
                 run_sql=oracle_run_sql(ORACLE_OK))
    assert report["ready"] is True
    assert set(statuses(report)) == {"identity", "isolation", "policy_committed",
                                     "source_read_only", "hook_live", "harness"}

def test_run_doctor_not_ready(tmp_path):
    report = run(make_repo(tmp_path), make_plugin(tmp_path),
                 dbx=ok_dbx(**{"current-user me": {"userName": "other-sp"}}),
                 run_sql=oracle_run_sql(ORACLE_OK))
    assert report["ready"] is False
    assert statuses(report)["identity"] == "fail"

def test_main_exit_and_markdown(tmp_path, capsys):
    repo, plugin = make_repo(tmp_path), make_plugin(tmp_path)
    md = tmp_path / "doctor.md"
    rc = doctor.main(["--repo", str(repo), "--markdown", str(md)],
                     dbx=ok_dbx(), run_cmd=make_run_cmd(), env=GOOD_ENV,
                     run_sql=oracle_run_sql(ORACLE_OK), plugin_root=plugin)
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ready"] is True
    text = md.read_text()
    assert "| check | status | data |" in text and "**ready**: true" in text

def test_main_exit_one_on_failure(tmp_path, capsys):
    repo, plugin = make_repo(tmp_path), tmp_path / "no-plugin"
    rc = doctor.main(["--repo", str(repo)],
                     dbx=ok_dbx(), run_cmd=make_run_cmd(), env=GOOD_ENV,
                     run_sql=oracle_run_sql(ORACLE_OK), plugin_root=plugin)
    assert rc == 1
    assert json.loads(capsys.readouterr().out)["checks"][4]["check"] == "hook_live"
