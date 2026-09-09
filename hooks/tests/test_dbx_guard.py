import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOKS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HOOKS))

import dbx_guard as g  # noqa: E402
import dbx_post_hint as h  # noqa: E402

CFG = g.GuardConfig.from_dict({
    "catalogs": ["mig_cat"],
    "legacy_sources": ["LEGACY_TD_DSN", "tdprod.corp.example", "legacy-prod"],
})


def approve(cmd: str, cfg=CFG):
    v = g.evaluate(cmd, cfg)
    assert v.decision == "approve", v.reason
    return v


def block(cmd: str, cfg=CFG):
    v = g.evaluate(cmd, cfg)
    assert v.decision == "block", f"expected block for: {cmd}"
    return v


# ---------------------------------------------------------------- allowed shapes

@pytest.mark.parametrize("cmd", [
    "git status && git log --oneline -5",
    "pytest skills/data-reconciliation/harness/tests -q",
    "databricks current-user me",
    "databricks experimental aitools tools query \"SELECT count(*) FROM mig_cat.raw.orders\"",
    "databricks experimental aitools tools query \"SELECT * FROM prod_cat.sales.orders LIMIT 10\"",
    "databricks experimental aitools tools query \"CREATE OR REPLACE TABLE mig_cat.wave1_u12.orders AS SELECT * FROM prod_cat.sales.orders\"",
    "databricks experimental aitools tools query \"INSERT INTO `mig_cat`.`wave1_u12`.`orders` SELECT * FROM src\"",
    "databricks experimental aitools tools query \"USE CATALOG mig_cat; CREATE SCHEMA IF NOT EXISTS wave1_u12; CREATE TABLE t (id INT)\"",
    "databricks experimental aitools tools query \"CREATE SCHEMA IF NOT EXISTS mig_cat.wave1_u12\"",
    "databricks experimental aitools tools query \"GRANT SELECT ON SCHEMA mig_cat.wave1_u12 TO `recon-verifier`\"",
    "databricks experimental aitools tools query \"MERGE INTO mig_cat.s.t USING mig_cat.s.stg ON t.id = stg.id WHEN MATCHED THEN UPDATE SET *\"",
    "databricks experimental aitools tools query \"CALL mig_cat.wave1.usp_load_orders()\"",
    "databricks experimental aitools tools query \"SELECT * FROM legacy_fed.dbo.orders WHERE op_type = 'DELETE' AND last_update > '2024-01-01'\"",
    "databricks experimental aitools tools query \"SELECT * FROM prod_cat.audit.log WHERE stmt = 'DROP TABLE prod_cat.s.t' OR stmt = 'UPDATE prod_cat.s.t SET a = 1'\"",
    "sqlcmd -S legacy-prod -Q \"SELECT * FROM dbo.audit WHERE action = 'INSERT INTO loans' AND note = 'it''s an UPDATE dbo.loans SET x'\"",
    "sqlcmd -S legacy-prod -Q \"SELECT 1 -- INSERT INTO dbo.loans SELECT 1\"",
    "dbx-recon run --unit u12 --family teradata --mode live --source-dsn-secret LEGACY_TD_DSN --target-secret DATABRICKS_MIGRATION_SQL --target-catalog mig_cat --allowed-targets-file .migration/allowed_targets.json --target-schema wave1_u12 --out .migration/recon/u12/",
    "databricks bundle validate -t migration",
    "databricks bundle deploy -t migration",
    "databricks bundle run --target migration nightly_orders",
    "databricks schemas create wave1_u12 mig_cat",
    "databricks schemas delete mig_cat.wave1_u12",
    "databricks grants update schema mig_cat.wave1_u12 --json '{\"changes\": []}'",
    "databricks jobs list",
    "bteq <<'EOF'\n.LOGON tdprod.corp.example/svc_ro;\nSELECT COUNT(*) FROM sales.orders;\n.QUIT\nEOF",
    "docker exec -i sybase-fixture isql -Usa -Q 'SELECT TOP 5 * FROM dbo.loans'",
    "psql -h localhost -d fixture -c \"CREATE TABLE loans (id int)\"",
    "psql -h localhost -d fixture -c \"INSERT INTO loans VALUES (1)\"",
    "python3 -c \"print('DROP TABLE is a string in a test fixture name')\"",
    "grep -rn 'INSERT INTO' skills/ | head",
    "echo 'the updated_at column and the deleted flag' > notes.txt",
    "pip install databricks-sql-connector",
])
def test_allowed(cmd):
    approve(cmd)


# ---------------------------------------------------------------- denied shapes: Databricks scope

@pytest.mark.parametrize("cmd,needle", [
    ("databricks experimental aitools tools query \"CREATE TABLE prod_cat.sales.orders_v2 AS SELECT 1\"", "prod_cat"),
    ("databricks experimental aitools tools query \"INSERT INTO prod_cat.sales.orders SELECT * FROM mig_cat.s.orders\"", "prod_cat"),
    ("databricks experimental aitools tools query \"DROP TABLE IF EXISTS `prod_cat`.sales.orders\"", "prod_cat"),
    ("databricks experimental aitools tools query \"MERGE INTO prod_cat.sales.orders t USING mig_cat.s.orders s ON t.id=s.id WHEN MATCHED THEN UPDATE SET *\"", "prod_cat"),
    ("databricks experimental aitools tools query \"UPDATE prod_cat.sales.orders AS o SET o.status = 'x'\"", "prod_cat"),
    ("databricks experimental aitools tools query \"UPDATE prod_cat.sales.orders o SET status = 'x' WHERE note = 'harmless'\"", "prod_cat"),
    ("databricks experimental aitools tools query \"SELECT 1; DROP TABLE prod_cat.s.t\"", "prod_cat"),
    ("databricks experimental aitools tools query \"USE CATALOG prod_cat; CREATE TABLE orders_v2 (id INT)\"", "prod_cat"),
    ("databricks experimental aitools tools query \"CREATE SCHEMA prod_cat.migration_tmp\"", "prod_cat"),
    ("databricks experimental aitools tools query \"GRANT ALL PRIVILEGES ON CATALOG prod_cat TO `migration-sp`\"", "prod_cat"),
    ("databricks experimental aitools tools query \"GRANT SELECT ON SCHEMA prod_cat.sales TO `bi_readers`\"", "prod_cat"),
    ("databricks experimental aitools tools query \"CREATE CATALOG scratch_cat\"", "scratch_cat"),
    ("databricks experimental aitools tools query \"CREATE TABLE orders (id INT)\"", "unresolvable"),
    ("databricks experimental aitools tools query \"CREATE TABLE orders AS SELECT * FROM prod_cat.sales.orders\"", "prod_cat"),
    ("databricks experimental aitools tools query \"MERGE INTO prod_cat.s.t t USING mig_cat.s.stg s ON t.id=s.id WHEN MATCHED THEN DELETE\"", "prod_cat"),
    ("databricks experimental aitools tools query \"DELETE FROM sales.orders WHERE 1=1\"", "unresolvable"),
    ("databricks experimental aitools tools query \"CALL prod_cat.ops.usp_repoint_consumers()\"", "prod_cat"),
    ("databricks experimental aitools tools query \"ALTER TABLE prod_cat.sales.orders ADD COLUMN x INT\"", "prod_cat"),
    ("databricks experimental aitools tools query \"COPY INTO prod_cat.sales.orders FROM '/Volumes/x'\"", "prod_cat"),
    ("dbx-recon run --unit u12 --family teradata --mode live --source-dsn-secret LEGACY_TD_DSN --target-secret T --target-catalog prod_cat --allowed-targets-file .migration/allowed_targets.json --target-schema s --out o", "prod_cat"),
    ("databricks schemas create migration_tmp prod_cat", "prod_cat"),
    ("databricks schemas delete prod_cat.migration_tmp", "prod_cat"),
    ("databricks tables delete prod_cat.sales.orders", "prod_cat"),
    ("databricks grants update catalog prod_cat --json '{}'", "prod_cat"),
    ("databricks catalogs delete scratch_cat", "scratch_cat"),
    ("databricks volumes create prod_cat sales landing MANAGED", "prod_cat"),
    ("databricks bundle deploy -t prod", "prod"),
    ("databricks bundle deploy --target=production", "production"),
    ("databricks bundle run -t prod nightly_orders", "prod"),
    ("databricks bundle destroy --target prod", "prod"),
    ("cd repo && databricks bundle validate && databricks bundle deploy -t prod", "prod"),
    ("databricks bundle deploy \\\n  --target prod \\\n  --var env=live", "prod"),
    ("databricks bundle run \\\n\t-t production nightly_orders", "production"),
    ("databricks bundle deploy -t migration && databricks bundle deploy -t prod", "prod"),
    ("databricks bundle deploy -t migration; databricks bundle run --target production nightly_orders", "production"),
    ("databricks bundle -t prod deploy", "prod"),
    ("databricks experimental aitools tools query \"USE CATALOG mig_cat; CREATE TABLE a (id INT); USE CATALOG prod_cat; CREATE TABLE orders_v2 (id INT)\"", "prod_cat"),
    ("databricks experimental aitools tools query \"USE CATALOG mig_cat; USE CATALOG prod_cat; INSERT INTO orders SELECT 1\"", "prod_cat"),
])
def test_blocked_databricks(cmd, needle):
    v = block(cmd)
    assert needle in v.reason


def test_use_catalog_switch_back_to_allowed_is_fine():
    approve("databricks experimental aitools tools query \"USE CATALOG prod_cat; SELECT 1; USE CATALOG mig_cat; CREATE TABLE t (id INT)\"")


# ---------------------------------------------------------------- script files fed to clients

def test_legacy_script_file_readonly_is_approved(tmp_path: Path):
    (tmp_path / "extract.bteq").write_text(".LOGON tdprod/svc_ro;\nSELECT COUNT(*) FROM sales.orders;\n.QUIT\n")
    assert g.evaluate("bteq < extract.bteq", CFG, root=tmp_path).decision == "approve"
    assert g.evaluate(f"bteq < {tmp_path / 'extract.bteq'}", CFG, root=tmp_path).decision == "approve"


@pytest.mark.parametrize("invocation", [
    "bteq < {f}",
    "sqlplus -S svc_ro@LEGACY_TD_DSN @{f}",
    "snowsql -f {f}",
    "tbuild -f {f}",
    "bteq -i {f}",
])
def test_legacy_script_file_with_write_is_blocked(tmp_path: Path, invocation: str):
    f = tmp_path / "fix.sql"
    f.write_text("SELECT 1;\nUPDATE sales.orders SET status = 'X' WHERE 1 = 1;\n")
    v = g.evaluate(invocation.format(f=f), CFG, root=tmp_path)
    assert v.decision == "block"
    assert "read-only" in v.reason


def test_script_file_literals_are_data_unless_executed(tmp_path: Path):
    f = tmp_path / "audit.sql"
    f.write_text("SELECT * FROM sales.audit WHERE stmt = 'DROP TABLE sales.orders' -- it's history\n"
                 "  OR stmt = 'UPDATE sales.orders SET status = 1';\n")
    assert g.evaluate(f"bteq < {f}", CFG, root=tmp_path).decision == "approve"
    f.write_text("BEGIN EXECUTE IMMEDIATE 'DROP TABLE sales.orders_bak'; END;\n")
    v = g.evaluate(f"sqlplus svc@LEGACY_TD_DSN @{f}", CFG, root=tmp_path)
    assert v.decision == "block" and "read-only" in v.reason


def test_legacy_script_file_unreadable_is_blocked(tmp_path: Path):
    v = g.evaluate("bteq < /nonexistent/extract.bteq", CFG, root=tmp_path)
    assert v.decision == "block"
    assert "cannot read" in v.reason
    v = g.evaluate("psql -h tdprod.corp.example -f missing.sql", CFG, root=tmp_path)
    assert v.decision == "block"


def test_databricks_script_file_is_inspected(tmp_path: Path):
    (tmp_path / "load.sql").write_text("USE CATALOG prod_cat;\nCREATE TABLE orders_v2 (id INT);\n")
    v = g.evaluate("spark-sql -f load.sql", CFG, root=tmp_path)
    assert v.decision == "block" and "prod_cat" in v.reason
    (tmp_path / "ok.sql").write_text("CREATE TABLE mig_cat.wave1.orders (id INT);\n")
    assert g.evaluate("spark-sql -f ok.sql", CFG, root=tmp_path).decision == "approve"


def test_non_client_commands_do_not_read_files(tmp_path: Path):
    approve("rm -f /nonexistent/thing && docker run -i img < /nonexistent/in.txt")


# ---------------------------------------------------------------- denied shapes: legacy writes

@pytest.mark.parametrize("cmd", [
    "bteq <<'EOF'\n.LOGON tdprod.corp.example/svc;\nUPDATE sales.orders SET status='X' WHERE 1=1;\n.QUIT\nEOF",
    "bteq <<'EOF'\n.LOGON tdprod.corp.example/svc;\nCREATE TABLE sales.orders_fix AS (SELECT * FROM sales.orders) WITH DATA;\nEOF",
    "bteq <<'EOF'\n.LOGON tdprod.corp.example/svc;\nDELETE FROM sales.orders_stage;\nEOF",
    "psql \"$LEGACY_TD_DSN\" -c \"INSERT INTO fixes VALUES (1)\"",
    "sqlcmd -S legacy-prod -Q \"EXEC dbo.usp_fix_balances\"",
    "sqlcmd -S legacy-prod -Q \"exec usp_fix_balances\"",
    "isql -S legacy-prod -U sa -Q \"ALTER TABLE dbo.loans ADD fixed bit\"",
    "sqlplus svc@tdprod.corp.example @fix.sql\nTRUNCATE TABLE sales.orders;",
    "sqlplus ro_user/x@ORCL <<EOF\nDROP INDEX sales.ix_orders;\nEOF",
    "snowsql -q \"CREATE OR REPLACE VIEW sales.v_orders AS SELECT 1\"",
    "docker exec -i legacy-prod isql -Usa -Q 'UPDATE dbo.loans SET status = 1 WHERE 1=1'",
    "sqlcmd -S legacy-prod -Q \"UPDATE dbo.loans AS l SET l.status = 1\"",
    "sqlcmd -S legacy-prod -Q \"UPDATE dbo.loans l SET status = 1\"",
    "sqlcmd -S legacy-prod -Q \"UPDATE dbo.loans WITH (TABLOCK) SET status = 1\"",
    "sqlcmd -S legacy-prod -Q \"UPDATE TOP (10) dbo.loans SET status = 1\"",
    "sqlcmd -S legacy-prod -Q \"EXEC sp_executesql N'UPDATE dbo.loans SET status = 1'\"",
    "sqlplus svc@tdprod.corp.example <<EOF\nBEGIN EXECUTE IMMEDIATE 'DROP TABLE sales.orders_bak'; END;\n/\nEOF",
    "python3 - <<'EOF'\nimport pyodbc\nc = pyodbc.connect(os.environ['LEGACY_TD_DSN'])\nc.execute('GRANT SELECT ON sales.orders TO devin')\nEOF",
])
def test_blocked_legacy(cmd):
    v = block(cmd)
    assert "legacy" in v.reason


# ---------------------------------------------------------------- modes & config

def test_warn_mode_approves_with_reason():
    cfg = g.GuardConfig.from_dict({"catalogs": ["mig_cat"], "guard_mode": "warn"})
    v = g.evaluate("databricks experimental aitools tools query \"DROP TABLE prod_cat.s.t\"", cfg)
    assert v.decision == "approve"
    assert v.reason.startswith("WARN")
    assert v.violations


def test_config_validation():
    with pytest.raises(ValueError):
        g.GuardConfig.from_dict({})
    with pytest.raises(ValueError):
        g.GuardConfig.from_dict({"catalogs": []})
    with pytest.raises(ValueError):
        g.GuardConfig.from_dict({"catalogs": ["c"], "guard_mode": "off"})
    cfg = g.GuardConfig.from_dict({"catalogs": ["`Mig_Cat`"]})
    assert cfg.catalogs == ["mig_cat"]
    assert cfg.mode == "block"
    assert cfg.forbidden_bundle_targets == ("prod", "production")


def test_catalog_match_is_case_insensitive_and_backtick_insensitive():
    approve("databricks experimental aitools tools query \"CREATE TABLE `MIG_CAT`.s.t (id INT)\"")


def test_find_config_walks_up(tmp_path: Path):
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat"]}))
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    cfg = g.load_config(nested)
    assert cfg is not None and cfg.catalogs == ["mig_cat"]
    assert g.load_config(tmp_path.parent) is None or g.load_config(tmp_path.parent).path != cfg.path


# ---------------------------------------------------------------- end-to-end via stdin (the real hook contract)

def _run(event: dict, cwd: Path, script="dbx_guard.py"):
    return subprocess.run(
        [sys.executable, str(HOOKS / script)],
        input=json.dumps(event), text=True, capture_output=True, cwd=cwd,
        env={"PATH": "/usr/bin:/bin", "CLAUDE_PROJECT_DIR": str(cwd)},
    )


def test_main_blocks_with_json_and_exit_2(tmp_path: Path):
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat"]}))
    r = _run({"tool_name": "exec", "tool_input": {"command": "databricks experimental aitools tools query \"DROP TABLE prod_cat.s.t\""}}, tmp_path)
    assert r.returncode == 2
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert out["decision"] == "block"
    assert "prod_cat" in out["reason"]
    assert "prod_cat" in r.stderr


def test_main_approves_silently(tmp_path: Path):
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig_cat"]}))
    r = _run({"tool_name": "exec", "tool_input": {"command": "git status"}}, tmp_path)
    assert r.returncode == 0 and r.stdout.strip() == ""


def test_main_noop_outside_migration_workspace(tmp_path: Path):
    r = _run({"tool_name": "exec", "tool_input": {"command": "databricks experimental aitools tools query \"DROP TABLE prod_cat.s.t\""}}, tmp_path)
    assert r.returncode == 0 and r.stdout.strip() == ""


def test_main_blocks_when_allowlist_is_broken(tmp_path: Path):
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text("{not json")
    r = _run({"tool_name": "exec", "tool_input": {"command": "git status"}}, tmp_path)
    assert r.returncode == 2


def test_main_tolerates_garbage_input(tmp_path: Path):
    r = subprocess.run([sys.executable, str(HOOKS / "dbx_guard.py")], input="garbage", text=True, capture_output=True, cwd=tmp_path)
    assert r.returncode == 0


# ---------------------------------------------------------------- PostToolUse hint

@pytest.mark.parametrize("text,kind", [
    ("Error: PERMISSION_DENIED: User does not have USE CATALOG on Catalog 'prod_cat'.", "databricks-scope"),
    ("Error: default auth: cannot configure default credentials", "databricks-auth"),
    ("Error: Invalid access token.", "databricks-auth"),
    ("ORA-01031: insufficient privileges", "legacy-readonly"),
    ("ERROR: cannot execute INSERT in a read-only transaction", "legacy-readonly"),
    ("Msg 229, Level 14, State 5: The INSERT permission was denied", "legacy-readonly"),
    ("All good, 42 rows", None),
])
def test_post_hint_classify(text, kind):
    assert h.classify(text) == kind


def test_post_hint_emits_additional_context(tmp_path: Path):
    ev = {"tool_name": "exec", "tool_input": {"command": "databricks jobs list"},
          "tool_response": {"success": False, "output": "", "error": "Error: PERMISSION_DENIED: User does not have USE CATALOG"}}
    r = _run(ev, tmp_path, script="dbx_post_hint.py")
    assert r.returncode == 0
    out = json.loads(r.stdout)
    assert out["hookSpecificOutput"]["hookEventName"] == "PostToolUse"
    assert "D10" in out["hookSpecificOutput"]["additionalContext"]


def test_post_hint_silent_on_success(tmp_path: Path):
    ev = {"tool_name": "exec", "tool_input": {"command": "ls"}, "tool_response": {"success": True, "output": "a b c", "error": None}}
    r = _run(ev, tmp_path, script="dbx_post_hint.py")
    assert r.returncode == 0 and r.stdout.strip() == ""


def test_hooks_json_registers_both_scripts():
    data = json.loads((HOOKS.parent / "hooks.json").read_text())
    pre = data["PreToolUse"][0]["hooks"][0]["command"]
    post = data["PostToolUse"][0]["hooks"][0]["command"]
    assert "hooks/dbx_guard.py" in pre and "hooks/dbx_post_hint.py" in post
    assert data["PreToolUse"][0]["matcher"] == "exec"
