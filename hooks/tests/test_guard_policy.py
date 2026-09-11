"""Unit tests for the fail-closed policy: allowlist schema, host and bundle-target allowlists,
Databricks CLI/REST read shapes, identity swaps, `.migration/` integrity, legacy read shapes,
and the cheap Python/Spark script scan."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOKS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HOOKS))

import dbx_guard as g

CFG = g.GuardConfig.from_dict({
    "catalogs": ["mig_cat"],
    "legacy_sources": ["LEGACY_TD_DSN", "tdprod.corp"],
    "target_hosts": ["fixture-host", "LAKEBASE_DSN"],
    "bundle_targets": ["migration", "dev"],
})
MINIMAL = g.GuardConfig.from_dict({"catalogs": ["mig_cat"], "legacy_sources": ["tdprod.corp"]})


def approve(cmd, cfg=CFG):
    v = g.evaluate(cmd, cfg)
    assert v.decision == "approve", v.reason
    return v


def block(cmd, cfg=CFG):
    v = g.evaluate(cmd, cfg)
    assert v.decision == "block", f"expected block for: {cmd}"
    return v


# ---------------------------------------------------------------- schema

def test_new_keys_are_optional_and_normalised():
    assert MINIMAL.target_hosts == [] and MINIMAL.bundle_targets == []
    assert CFG.target_hosts == ["fixture-host", "lakebase_dsn"]
    assert CFG.bundle_targets == ["migration", "dev"]
    assert CFG.forbidden_bundle_targets == ("prod", "production")
    for bad in ({"catalogs": ["c"], "target_hosts": "h"}, {"catalogs": ["c"], "bundle_targets": {"a": 1}}):
        with pytest.raises(ValueError):
            g.GuardConfig.from_dict(bad)


# ---------------------------------------------------------------- A7 generic-client hosts

@pytest.mark.parametrize("cmd", [
    "psql -h fixture-host -d demo -c 'CREATE TABLE t (id int)'",
    "sqlcmd -S fixture-host -Q 'INSERT dbo.t VALUES (1)'",
    "psql \"$LAKEBASE_DSN\" -c 'INSERT INTO s.t VALUES (1)'",
    "psql -h 10.0.0.5 -c 'SELECT 1'",  # a read goes anywhere
    "mysql -h unknown -e 'SHOW TABLES'",
])
def test_generic_write_to_a_listed_host_or_any_read_is_fine(cmd):
    approve(cmd)


@pytest.mark.parametrize("cmd", [
    "psql -h other-host -d demo -c 'CREATE TABLE t (id int)'",
    "sqlcmd -S $HOST -Q 'INSERT dbo.t VALUES (1)'",
    "sqlcmd -S 10.0.0.5 -Q 'DELETE dbo.t'",
    "psql \"$OTHER_DSN\" -c 'INSERT INTO s.t VALUES (1)'",
    "mysql -e 'DROP TABLE t'",  # no host at all
])
def test_generic_write_to_an_unlisted_host_blocks(cmd):
    assert "target_hosts" in block(cmd).reason


def test_missing_target_hosts_fails_closed():
    assert "target_hosts" in block("psql -h fixture-host -c 'CREATE TABLE t (id int)'", MINIMAL).reason


# ---------------------------------------------------------------- A6 legacy read shapes

@pytest.mark.parametrize("sql", [
    "SELECT 1", "WITH x AS (SELECT 1) SELECT * FROM x", "SHOW TABLES", "DESCRIBE dbo.t", "DESC dbo.t",
    "EXPLAIN SELECT 1", "HELP TABLE t", "SET NOCOUNT ON; SELECT 1", "SET QUOTED_IDENTIFIER OFF",
    ":setvar x 1\nSELECT 1\nGO", "SELECT 1 -- DELETE dbo.t", "SELECT * FROM t WHERE n = 'DELETE dbo.t'",
])
def test_legacy_read_shapes_pass(sql):
    approve(f"sqlcmd -S tdprod.corp -Q \"{sql}\"")


@pytest.mark.parametrize("sql", [
    "DELETE dbo.t", "INSERT dbo.t VALUES (1)", "MERGE dbo.t USING dbo.s ON 1=1 WHEN MATCHED THEN DELETE",
    "SELECT * INTO dbo.bak FROM dbo.t", "UPDATE STATISTICS dbo.t", "sp_rename 'a','b'", "xp_cmdshell 'ls'",
    "KILL 55", "BACKUP DATABASE x TO DISK='/x'", "DBCC SHRINKDATABASE(x)", "CREATE LOGIN x WITH PASSWORD='y'",
    "ALTER USER x WITH NAME = y", "DISABLE TRIGGER ALL ON dbo.t", "WAITFOR DELAY '10:00'", "BEGIN TRAN",
    "SET IDENTITY_INSERT dbo.t ON", "SELECT 1; DELETE dbo.t", "TRUNCATE TABLE dbo.t", "  ; UPDATE dbo.t SET a=1",
])
def test_legacy_non_read_shapes_block(sql):
    v = block(f"sqlcmd -S tdprod.corp -Q \"{sql}\"")
    assert "read-only" in v.reason


@pytest.mark.parametrize("cmd", [
    "sqlldr svc@tdprod.corp control=load.ctl", "mload < job.ml", "fastload < job.fl", "tbuild -f job.tpt",
    "tdload --job x", "bcp dbo.t in data.csv -S tdprod.corp -c",
])
def test_loaders_always_block_on_legacy(cmd):
    assert "loader" in block(cmd).reason


def test_bcp_out_is_a_read():
    approve("bcp dbo.t out data.csv -S tdprod.corp -c")


def test_psql_meta_commands_that_read_pass():
    approve("psql \"$LEGACY_TD_DSN\" -c '\\dt' -c '\\d+ sales.orders'")
    block("psql \"$LEGACY_TD_DSN\" -c '\\copy t FROM x.csv'")


# ---------------------------------------------------------------- A4 Databricks CLI / REST

@pytest.mark.parametrize("cmd", [
    "databricks --version", "databricks -h", "databricks jobs list --help", "databricks current-user me",
    "databricks catalogs list", "databricks schemas get prod.s", "databricks tables list prod s",
    "databricks volumes list prod s", "databricks functions get prod.s.f", "databricks api get /api/2.0/clusters/list",
    "databricks bundle summary -t prod", "databricks fs ls dbfs:/Volumes/prod/s/v", "databricks jobs list-runs --job-id 1",
    "databricks jobs get-run 5", "databricks pipelines get 1", "databricks warehouses list", "databricks clusters get 1",
    "databricks workspace list /Shared", "databricks workspace export /Shared/x", "databricks workspace get-status /Shared/x",
    "databricks secrets list-scopes", "databricks secrets list-secrets s", "databricks auth describe", "databricks auth env",
    "databricks auth token", "databricks sql execute -e 'SELECT 1'", "databricks bundle validate",
])
def test_databricks_read_shapes_pass(cmd):
    approve(cmd)


@pytest.mark.parametrize("cmd", [
    "databricks jobs run-now 1", "databricks jobs submit --json @j.json", "databricks jobs reset 1 --json @j.json",
    "databricks pipelines start-update 1", "databricks workspace import /x --file x.py", "databricks workspace delete /x",
    "databricks permissions update warehouses x --json '{}'", "databricks secrets put-secret s k --string-value v",
    "databricks warehouses delete x", "databricks clusters create --json @c.json", "databricks fs rm dbfs:/Volumes/prod/s/v/f",
    "databricks fs cp x.csv dbfs:/Volumes/prod/s/v/", "databricks fs cp dbfs:/Volumes/mig_cat/s/v/x dbfs:/Volumes/prod/s/v/",
    "databricks api post /api/2.1/jobs/create --json @j.json", "databricks api delete /api/2.1/unity-catalog/tables/prod.s.t",
    "databricks api patch /api/2.1/unity-catalog/catalogs/prod", "databricks catalogs update prod --json '{}'",
    "databricks experimental something new", "databricks tables delete prod.s.t", "databricks volumes create prod s v MANAGED",
])
def test_databricks_unrecognised_or_mutating_shapes_block(cmd):
    block(cmd)


@pytest.mark.parametrize("cmd", [
    "databricks tables delete mig_cat.s.t", "databricks schemas create s mig_cat",
    "databricks volumes create mig_cat s v MANAGED", "databricks volumes delete mig_cat.s.v",
    "databricks fs rm dbfs:/Volumes/mig_cat/s/v/f", "databricks fs cp x.csv dbfs:/Volumes/mig_cat/s/v/",
    "databricks fs cp dbfs:/Volumes/mig_cat/s/v/a.csv ./a.csv",
    "databricks api delete /api/2.1/unity-catalog/tables/mig_cat.s.t",
])
def test_databricks_mutation_of_an_allowlisted_securable_passes(cmd):
    approve(cmd)


@pytest.mark.parametrize("cmd", [
    "databricks schemas delete mig_cat.s",  # probe2 #4: catalog lifecycle is not an object write
    "databricks grants update schema mig_cat.s --json '{}'",  # probe2 #4: permissions are not object writes
    "databricks catalogs update mig_cat --json '{}'",  # probe2 #4: catalog lifecycle is not an object write
    "databricks catalogs delete mig_cat", "databricks grants update catalog mig_cat --json '{}'",
    "databricks sql execute -e 'GRANT USE CATALOG ON CATALOG mig_cat TO `x`'", "databricks sql execute -e 'DROP CATALOG mig_cat'",
])
def test_catalog_lifecycle_and_permissions_block_even_on_the_allowlisted_catalog(cmd):
    assert "lifecycle" in block(cmd).reason


@pytest.mark.parametrize("cmd", [
    "curl -X POST $DATABRICKS_HOST/api/2.0/sql/statements -d '{}'",
    "curl https://adb-1.2.azuredatabricks.net/api/2.1/jobs/run-now --data '{\"job_id\": 1}'",
    "curl -X DELETE https://x.cloud.databricks.com/api/2.1/unity-catalog/tables/prod.s.t",
    "wget --method=PATCH https://x.gcp.databricks.com/api/2.1/unity-catalog/catalogs/prod",
    "http POST $DATABRICKS_HOST/api/2.0/permissions/x",
])
def test_rest_mutations_block(cmd):
    assert "REST" in block(cmd).reason


@pytest.mark.parametrize("cmd", [
    "curl -s $DATABRICKS_HOST/api/2.0/clusters/list -H \"Authorization: Bearer $DATABRICKS_TOKEN\"",
    "curl -X GET https://x.cloud.databricks.com/api/2.1/unity-catalog/catalogs",
    "curl -X POST https://example.com/webhook -d '{}'",
])
def test_rest_reads_and_other_hosts_pass(cmd):
    approve(cmd)


def test_sql_execute_catalog_flag_and_dynamic_sql():
    approve("databricks sql execute --catalog mig_cat -e 'CREATE TABLE s.t AS SELECT 1'")
    block("databricks sql execute --catalog prod -e 'CREATE TABLE s.t AS SELECT 1'")
    block("databricks sql execute -e \"EXECUTE IMMEDIATE 'INSERT INTO prod.s.t VALUES (1)'\"")
    approve("databricks sql execute -e \"EXECUTE IMMEDIATE 'INSERT INTO mig_cat.s.t VALUES (1)'\"")
    block("databricks sql execute -e \"DECLARE s STRING; EXECUTE IMMEDIATE s\"")
    block("databricks sql execute -e \"INSERT INTO IDENTIFIER(concat('pr','od.s.t')) VALUES (1)\"")


# ---------------------------------------------------------------- A5 bundle / dbt targets

@pytest.mark.parametrize("cmd", [
    "databricks bundle deploy -t migration", "databricks bundle run --target=dev nightly", "databricks bundle destroy -t dev",
    "dbt run --target dev", "dbt build -t migration", "dbt seed --target=dev", "dbt compile", "dbt test",
])
def test_bundle_and_dbt_targets_in_the_list_pass(cmd):
    approve(cmd)


@pytest.mark.parametrize("cmd", [
    "databricks bundle deploy", "databricks bundle deploy -t $T", "databricks bundle deploy -t staging",
    "databricks bundle run -t prod job", "dbt run", "dbt run --target prod", "dbt seed -t $T",
])
def test_bundle_and_dbt_targets_outside_the_list_block(cmd):
    assert "bundle_targets" in block(cmd).reason


def test_forbidden_bundle_targets_still_deny_even_when_listed():
    cfg = g.GuardConfig.from_dict({"catalogs": ["mig_cat"], "bundle_targets": ["prod"]})
    assert "forbidden" in block("databricks bundle deploy -t prod", cfg).reason


def test_missing_bundle_targets_fails_closed():
    assert "bundle_targets" in block("databricks bundle deploy -t migration", MINIMAL).reason


# ---------------------------------------------------------------- A3 identity

@pytest.mark.parametrize("cmd", [
    "databricks auth login --host https://x.cloud.databricks.com", "databricks --profile admin jobs list",
    "databricks -p admin jobs list", "databricks jobs list --profile=admin",
    "DATABRICKS_TOKEN=$OTHER databricks jobs list", "export DATABRICKS_HOST=https://y; databricks jobs list",
    "env DATABRICKS_CONFIG_PROFILE=admin databricks jobs list", "DATABRICKS_CLIENT_ID=x DATABRICKS_CLIENT_SECRET=y dbx-recon run --unit u",
    "export DATABRICKS_TOKEN=x && spark-sql -e 'SELECT 1'",
])
def test_identity_swaps_block(cmd):
    assert "identity" in block(cmd).reason


def test_identity_variables_without_a_client_in_the_segment_are_not_the_guards_business():
    block("export DATABRICKS_HOST=https://x; git status")  # probe2 #3: an export outlives the command; the next one runs as it
    approve("DATABRICKS_HOST=https://x git status; databricks jobs list")
    approve("echo $DATABRICKS_TOKEN | wc -c")
    approve("echo DATABRICKS_TOKEN=x; databricks jobs list")


# ---------------------------------------------------------------- A2(a) `.migration/` integrity

@pytest.mark.parametrize("cmd", [
    "echo x > .migration/allowed_targets.json", "echo x >> .migration/06_decisions.md", "sed -i 's/a/b/' .migration/x.json",
    "tee .migration/x.json < y", "cp y .migration/x.json", "mv .migration .m", "rm -rf .migration", "rmdir .migration/units",
    "truncate -s 0 .migration/x", "chmod 000 .migration/allowed_targets.json", "jq . y > .migration/x.json",
    "git checkout -- .migration/allowed_targets.json", "python3 -c \"open('.migration/x','w')\"",
    "python3 - <<'EOF'\nopen('.migration/x', 'w').write('1')\nEOF", "cat y > ./.migration/x", "echo x > /ws/.migration/x",
    "cd .migration && echo x > allowed_targets.json",
])
def test_writes_under_migration_block(cmd):
    assert ".migration" in block(cmd).reason


@pytest.mark.parametrize("cmd", [
    "cat .migration/allowed_targets.json", "jq . .migration/allowed_targets.json", "ls -la .migration/", "git diff .migration/",
    "echo x > .migration/recon/u12/report.json", "tee .migration/waves/wave-1.result.json < r.json",
    "cp r.json .migration/recon/u12/", "mkdir -p .migration/recon/u12", "grep -r prod .migration/",
    "python3 -c \"print(open('.migration/allowed_targets.json').read())\"", "echo x > notes/.migrations/x",
])
def test_reads_and_recon_wave_outputs_pass(cmd):
    approve(cmd)


# ---------------------------------------------------------------- Python / Spark cheap scan

def test_python_and_spark_scripts_are_scanned_for_literal_writes(tmp_path: Path):
    (tmp_path / "w.py").write_text("spark.sql('INSERT INTO prod.s.t SELECT 1')\n")
    (tmp_path / "ok.py").write_text("spark.sql('INSERT INTO mig_cat.s.t SELECT 1')\ncur.execute(query)\n")
    (tmp_path / "legacy.py").write_text("c = pyodbc.connect(os.environ['LEGACY_TD_DSN'])\nc.execute('DELETE dbo.t')\n")
    (tmp_path / "read.py").write_text("c = pyodbc.connect(os.environ['LEGACY_TD_DSN'])\nc.execute('SELECT 1')\n")
    for cmd in ("python3 w.py", "spark-submit w.py", "python w.py --catalog prod"):
        assert "prod" in g.evaluate(cmd, CFG, root=tmp_path).reason, cmd
    assert "read-only" in g.evaluate("python3 legacy.py", CFG, root=tmp_path).reason
    for cmd in ("python3 ok.py", "python3 read.py", "python3 -m pytest tests/", "python3 missing.py"):
        assert g.evaluate(cmd, CFG, root=tmp_path).decision == "approve", cmd
    assert g.evaluate("spark-submit missing.py", CFG, root=tmp_path).decision == "block"


def test_inline_python_with_a_literal_write_blocks():
    block("python3 -c \"w.statement_execution.execute_statement(statement='INSERT INTO prod.s.t VALUES (1)')\"")
    block("python3 -c \"pyodbc.connect(os.environ['LEGACY_TD_DSN']).execute('DROP TABLE x')\"")
    approve("python3 -c \"c.execute('SELECT 1 FROM prod.s.t')\"")


# ---------------------------------------------------------------- wrappers and aliases

def test_wrappers_are_read_through():
    block("ssh tdprod.corp 'sqlcmd -Q \"DELETE FROM dbo.t\"'")
    block("docker exec fixture sqlcmd -S localhost -Q 'DELETE dbo.t' # host not listed")
    approve("docker exec fixture sqlcmd -S fixture-host -Q 'DELETE dbo.t'")
    block("alias d=databricks; d jobs run-now 1")
    block("cp $(which databricks) /tmp/dbx && /tmp/dbx jobs list")


# ---------------------------------------------------------------- doctor compatibility

def test_doctor_probe_command_blocks_via_subprocess(tmp_path: Path):
    sys.path.insert(0, str(HOOKS.parent / "skills" / "factory-doctor"))
    import doctor
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat"]}))
    event = {"tool_name": "exec", "tool_input": {"command": doctor.HOOK_PROBE_COMMAND}}
    r = subprocess.run([sys.executable, str(HOOKS / "dbx_guard.py")], input=json.dumps(event), text=True, check=False,
                       capture_output=True, cwd=tmp_path, env={"PATH": "/usr/bin:/bin", "CLAUDE_PROJECT_DIR": str(tmp_path)})
    assert r.returncode == 2 and "block" in r.stdout
