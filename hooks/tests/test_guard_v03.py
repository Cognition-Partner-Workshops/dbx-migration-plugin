"""Section C tests for the guard rewrite."""

import sys
from pathlib import Path

import pytest

HOOKS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HOOKS))

import dbx_guard as g  # noqa: E402


CFG = g.GuardConfig.from_dict({
    "catalogs": ["mig_cat"],
    "legacy_sources": ["LEGACY_TD_DSN", "tdprod.corp", "OrdersDb"],
    "target_hosts": ["lakebase-host", "LAKEBASE_DSN"],
    "guard_mode": "block",
})


def approve(command: str, root: Path | None = None):
    verdict = g.evaluate(command, CFG, root=root)
    assert verdict.decision == "approve", verdict.reason
    return verdict


def block(command: str, root: Path | None = None):
    verdict = g.evaluate(command, CFG, root=root)
    assert verdict.decision == "block", f"{command}: {verdict.reason}"
    return verdict


@pytest.mark.parametrize("command", [
    "psql -h lakebase-host -d mig_cat -c 'INSERT INTO t VALUES (1)' # cutover from tdprod.corp",
    "psql -h lakebase-host -d mig_cat -c '-- was tdprod.corp\nINSERT INTO t VALUES (1)'",
    'git commit -m "port from tdprod.corp"',
    'echo "tdprod.corp done" >> notes.md',
    "sqlcmd -S lakebase-host -d mig_cat -Q 'SELECT 1 -- DROP TABLE x'",
])
def test_comments_and_prose_do_not_make_a_command_legacy(command):
    approve(command)


def test_sql_block_comment_still_hides_no_write():
    verdict = block("sqlcmd -S tdprod.corp -Q 'SELECT 1 /* x */; DROP TABLE t'")
    assert "legacy source" in verdict.reason


def test_secret_name_matching_is_case_sensitive():
    lower = block('psql "$legacy_td_dsn" -c \'DROP TABLE t\'')
    assert "target_hosts" in lower.reason
    assert "legacy source" not in lower.reason
    assert "legacy source" in block('psql "$LEGACY_TD_DSN" -c \'DROP TABLE t\'').reason


def test_host_matching_is_case_insensitive():
    assert "legacy source" in block("sqlcmd -S TDPROD.CORP -Q 'DELETE FROM t'").reason


def test_database_matching_depends_on_client():
    psql = block("psql -h lakebase-host -d ordersdb -c 'DELETE FROM t'")
    assert "legacy source" not in psql.reason
    assert "allowlist" in psql.reason
    assert "legacy source" in block("sqlcmd -S lakebase-host -d ordersdb -Q 'DELETE FROM t'").reason


@pytest.mark.parametrize("command", [
    "PGHOST=lakebase-host psql -d mig_cat -c 'INSERT INTO t VALUES (1)'",
    "export H=lakebase-host; psql -h $H -d mig_cat -c 'INSERT INTO t VALUES (1)'",
])
def test_resolved_environment_values_are_connections(command):
    approve(command)


def test_legacy_environment_value_is_a_legacy_connection():
    assert "legacy source" in block("H=tdprod.corp; psql -h $H -c 'DELETE FROM t'").reason


@pytest.mark.parametrize("command", [
    "source env.sh && psql -h $H -d $D -c 'INSERT INTO t VALUES (1)'",
    ". ./env.sh; psql -h $H -d $D -c 'INSERT INTO t VALUES (1)'",
])
def test_source_exports_flow_to_later_commands(tmp_path: Path, command: str):
    (tmp_path / "env.sh").write_text("H=lakebase-host\nD=mig_cat\n")
    approve(command, tmp_path)


def test_unresolved_connection_position_on_write_blocks():
    verdict = block("psql -h $UNSET -c 'DELETE FROM t'")
    assert "legacy source" not in verdict.reason


def test_unrelated_unresolved_expansion_does_not_block():
    approve('echo "$UNSET"; psql -h lakebase-host -d mig_cat -c \'INSERT INTO t VALUES (1)\'')


def test_missing_source_script_blocks(tmp_path: Path):
    verdict = block("source missing.env; psql -h lakebase-host -d mig_cat -c 'SELECT 1'", tmp_path)
    assert "cannot be read" in verdict.reason


def test_readable_wrapper_script_is_judged_by_contents(tmp_path: Path):
    (tmp_path / "run.sh").write_text(
        "set -e\n"
        "log() { echo \"$(date) $1\"; }\n"
        "log start\n"
        "psql -h lakebase-host -d mig_cat -c 'INSERT INTO t VALUES (1)'\n"
        "log done\n"
    )
    approve("bash run.sh", tmp_path)


@pytest.mark.parametrize("name, host, needle", [
    ("bad.sh", "tdprod.corp", "legacy source"),
    ("bad2.sh", "other-host", "target_hosts"),
])
def test_wrapper_script_destinations_are_checked(tmp_path: Path, name: str, host: str, needle: str):
    (tmp_path / name).write_text(f"psql -h {host} -c 'DELETE FROM t'\n")
    assert needle in block(f"bash {name}", tmp_path).reason


def test_wrapper_script_with_unresolved_connection_blocks(tmp_path: Path):
    (tmp_path / "bad3.sh").write_text("psql -h $X -c 'DELETE FROM t'\n")
    block("bash bad3.sh", tmp_path)


def test_non_connection_substitution_does_not_block():
    approve("echo $(date); psql -h lakebase-host -d mig_cat -c 'INSERT INTO t VALUES (1)'")


def test_python_without_connection_evidence_is_ignored():
    approve('python3 -c "from psycopg2 import sql; q = sql.SQL(\'ALTER ROLE r NOLOGIN\')"')


@pytest.mark.parametrize("command", [
    (
        'python3 -c "import psycopg2; c = psycopg2.connect(host=\'tdprod.corp\'); '
        'c.cursor().execute(\'GRANT ALL ON t TO x\')"'
    ),
    (
        'python3 -c "import psycopg2; c = psycopg2.connect(os.environ[\'LEGACY_TD_DSN\']); '
        "c.cursor().execute(f'GRANT ALL ON {t} TO x')\""
    ),
])
def test_python_legacy_connections_block(command):
    assert "legacy source" in block(command).reason


def test_python_secret_name_matching_is_case_sensitive():
    verdict = block(
        'python3 -c "import psycopg2; c = psycopg2.connect(os.environ[\'legacy_td_dsn\']); '
        "c.cursor().execute('DROP TABLE t')\""
    )
    assert "legacy source" not in verdict.reason


@pytest.mark.parametrize("command, expected", [
    (
        'python3 -c "c = psycopg2.connect(dsn); c.cursor().execute(\'DROP TABLE t\')"',
        "block",
    ),
    (
        'python3 -c "c = psycopg2.connect(dsn); c.cursor().execute(\'SELECT 1\')"',
        "approve",
    ),
])
def test_python_runtime_connection_only_blocks_writes(command, expected):
    assert g.evaluate(command, CFG).decision == expected


def test_python_variable_sql_on_a_legacy_connection_fails_closed():
    verdict = block('python3 -c "c = psycopg2.connect(host=\'tdprod.corp\'); q = \'DROP TABLE t\'; c.cursor().execute(q)"')
    assert "legacy source" in verdict.reason
    approve('python3 -c "c = psycopg2.connect(host=\'lakebase-host\', dbname=\'mig_cat\'); c.cursor().execute(q)"')


def test_python_foreign_host_is_checked_even_when_databricks_is_imported():
    verdict = block(
        'python3 -c "import databricks; c = psycopg2.connect(host=\'evil.corp\'); '
        "c.cursor().execute('DROP TABLE mig_cat.s.t')\""
    )
    assert "evil.corp" in verdict.reason


def test_python_connection_database_is_the_default_catalog():
    approve('python3 -c "c = psycopg2.connect(host=\'lakebase-host\', dbname=\'mig_cat\'); c.cursor().execute(\'DROP TABLE staging\')"')
    approve('python3 -c "c = psycopg2.connect(\'postgresql://u@lakebase-host:5432/mig_cat\'); c.cursor().execute(\'DROP TABLE staging\')"')
    block('python3 -c "c = psycopg2.connect(host=\'lakebase-host\', dbname=\'other\'); c.cursor().execute(\'DROP TABLE staging\')"')


def test_python_allowlisted_secret_is_a_resolved_target():
    approve('python3 -c "c = psycopg2.connect(os.environ[\'LAKEBASE_DSN\']); c.cursor().execute(\'DROP TABLE mig_cat.s.t\')"')
    block('python3 -c "c = psycopg2.connect(os.environ[\'LAKEBASE_DSN\']); c.cursor().execute(\'DROP TABLE prod.s.t\')"')


def test_spark_sql_dynamic_statement_fails_closed():
    block("spark-sql -e 'EXECUTE IMMEDIATE stmt'")
    block("dbsqlcli -e 'EXECUTE IMMEDIATE stmt'")


def test_python_databricks_destination_still_checks_catalog():
    verdict = block(
        "python3 -c \"from databricks.sdk import WorkspaceClient; "
        "WorkspaceClient().statement_execution.execute_statement("
        "statement='INSERT INTO prod.s.t VALUES (1)', warehouse_id='x')\""
    )
    assert "prod" in verdict.reason


@pytest.mark.parametrize("command", [
    "echo __dbx_guard_probe__abc123",
    "printf '%s' __dbx_guard_probe__zz9 | cat",
])
def test_probe_sentinel_is_blocked(command):
    verdict = block(command)
    assert "__dbx_guard_probe__" in verdict.reason


@pytest.mark.parametrize("command, needle", [
    ("sqlcmd -S tdprod.corp -Q 'EXEC dbo.purge_orders'", "legacy source"),
    ('psql "$LEGACY_TD_DSN" -c "SELECT setval(\'s\', 1)"', "legacy source"),
    ("psql -h lakebase-host -d mig_cat -c 'GRANT ALL ON t TO x'", "permission"),
    ("psql -h lakebase-host -d other_db -c 'INSERT INTO t VALUES (1)'", "allowlist"),
])
def test_keep_list_remains_blocked(command, needle):
    assert needle in block(command).reason


def test_decision_authorizes_the_pinned_legacy_write(tmp_path: Path):
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "06_decisions.md").write_text(
        "# Decisions\n\n"
        "| id | date | decision |\n|---|---|---|\n"
        "| D-7 | 2026-01-01 | legacy_write_authorized: customer DBA approved the CDC "
        "prerequisite `ALTER TABLE dbo.orders ADD cdc_ts DATETIME2` on dbo.orders |\n"
    )
    verdict = g.evaluate(
        "DBX_DECISION=D-7 sqlcmd -S tdprod.corp -Q "
        "'ALTER TABLE dbo.orders ADD cdc_ts DATETIME2'",
        CFG,
        root=tmp_path,
    )
    assert verdict.decision == "approve", verdict.reason


def test_allowlist_tampering_blocks():
    assert ".migration" in block("rm .migration/allowed_targets.json").reason


def test_force_push_decision_is_unchanged():
    verdict = g.evaluate("git push --force origin main", CFG)
    assert verdict.decision == "block"


def test_renamed_client_is_blocked():
    verdict = block("cp $(which sqlcmd) ./x && ./x -S tdprod.corp -Q 'DROP TABLE t'")
    assert "copies or renames client" in verdict.reason
