"""Schema-evolution rerun proof (plan row 3.10): the idempotency proof runs twice, once on a
fresh target and once against a target pre-created in the table's previous committed shape.
The harness grades the two observed shapes against the shape the committed DDL declares and
records `rerun_proof: {fresh, evolved}`; a failing run is `rerun_gap` and never merge-eligible."""

import json
from pathlib import Path

import pytest
from recon import cli
from recon.config import ConfigError
from recon.report import build_result
from recon.rerun import (
    check_proof,
    RERUN_RECORD_KEYS,
    declared_shape,
    grade_rerun,
    load_record,
    load_shape,
    normalize_type,
    rerun_gap,
)
from recon.tiers import TierResult

from tests.fakes import FakeSource, FakeTarget
from tests.loans import _StubConn

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "example_rerun"

DDL = """
-- committed shape: the second wave added `channel`
CREATE TABLE IF NOT EXISTS mig.sales.orders (
  order_id BIGINT NOT NULL,
  amount DECIMAL(18,2),
  channel STRING COMMENT 'added in wave 2',
  tags ARRAY<STRUCT<k: STRING, v: STRING>>,
  CONSTRAINT pk PRIMARY KEY (order_id)
) USING DELTA;
INSERT INTO mig.sales.orders SELECT * FROM src.orders;
"""

NEW_SHAPE = {"tables": {"orders": [
    {"name": "order_id", "type": "bigint", "nullable": False},
    {"name": "amount", "type": "decimal(18,2)", "nullable": True},
    {"name": "channel", "type": "string", "nullable": True},
    {"name": "tags", "type": "array<struct<k:string,v:string>>", "nullable": True},
]}}
OLD_SHAPE = {"tables": {"orders": NEW_SHAPE["tables"]["orders"][:2] + NEW_SHAPE["tables"]["orders"][3:]}}


def _record(run, shape, pre_shape=None, status="pass", evidence="job-run/1"):
    rec = {"run": run, "status": status, "evidence": evidence, "shape": shape}
    if pre_shape is not None:
        rec["pre_shape"] = pre_shape
    return rec


# ---- declared shape from DDL ------------------------------------------------------------------

def test_declared_shape_reads_columns_types_and_nullability_from_create_table():
    shape = declared_shape(DDL)
    cols = shape["tables"]["mig.sales.orders"]
    assert [c["name"] for c in cols] == ["order_id", "amount", "channel", "tags"]
    assert cols[0] == {"name": "order_id", "type": "bigint", "nullable": False}
    assert cols[1]["type"] == "decimal(18,2)" and cols[1]["nullable"] is True
    assert cols[3]["type"] == "array<struct<k:string,v:string>>"
    assert shape["if_not_exists"] == ["mig.sales.orders"]
    assert shape["statements"] == {"create_table": 1, "alter_table": 0, "other": 1}


def test_declared_shape_applies_add_columns_and_replace():
    sql = ("CREATE TABLE t (a INT);\n"
           "ALTER TABLE t ADD COLUMNS (b STRING NOT NULL, c DATE);\n"
           "ALTER TABLE t ADD COLUMN IF NOT EXISTS d INT;\n"
           "CREATE OR REPLACE TABLE u (x INT) ;")
    shape = declared_shape(sql)
    assert [c["name"] for c in shape["tables"]["t"]] == ["a", "b", "c", "d"]
    assert shape["tables"]["t"][1]["nullable"] is False
    assert shape["tables"]["u"] == [{"name": "x", "type": "int", "nullable": True}]
    assert shape["if_not_exists"] == []
    assert shape["statements"]["alter_table"] == 2


def test_declared_shape_handles_quoted_identifiers_and_inline_constraints():
    sql = 'CREATE TABLE `Mig`.`T` (`Id` BIGINT PRIMARY KEY, "Name" VARCHAR(40) DEFAULT \'x\' NOT NULL, n NUMERIC(10, 2) CHECK (n > 0));'
    cols = declared_shape(sql)["tables"]["mig.t"]
    assert cols == [{"name": "id", "type": "bigint", "nullable": False},
                    {"name": "name", "type": "varchar(40)", "nullable": False},
                    {"name": "n", "type": "numeric(10,2)", "nullable": True}]


def test_declared_shape_reads_nullability_only_from_top_level_clauses():
    """Constraint words inside a literal, a comment or a bracketed expression are text, not
    constraints: `DEFAULT 'NOT NULL'` leaves the column nullable."""
    sql = """CREATE TABLE t (
      note STRING DEFAULT 'NOT NULL',
      hint STRING COMMENT 'the PRIMARY KEY of the old system',
      flag STRING DEFAULT ('NOT NULL') NOT NULL,
      n DECIMAL(10, 2) GENERATED ALWAYS AS (CASE WHEN note IS NOT NULL THEN 1 ELSE 0 END),
      code VARCHAR(8) COMMENT 'x' NOT NULL
    );"""
    cols = {c["name"]: c for c in declared_shape(sql)["tables"]["t"]}
    assert cols["note"] == {"name": "note", "type": "string", "nullable": True}
    assert cols["hint"] == {"name": "hint", "type": "string", "nullable": True}
    assert cols["flag"] == {"name": "flag", "type": "string", "nullable": False}
    assert cols["n"] == {"name": "n", "type": "decimal(10,2)", "nullable": True}
    assert cols["code"] == {"name": "code", "type": "varchar(8)", "nullable": False}


def test_declared_shape_refuses_ddl_without_a_create_table():
    with pytest.raises(ConfigError, match="no CREATE TABLE"):
        declared_shape("INSERT INTO t SELECT 1;")


@pytest.mark.parametrize("raw, norm", [
    ("DECIMAL(18, 2)", "decimal(18,2)"),
    (" String ", "string"),
    ("ARRAY<STRUCT<k: STRING, v: STRING>>", "array<struct<k:string,v:string>>"),
    ("character varying(40)", "varchar(40)"),
    ("timestamp without time zone", "timestamp"),
    ("INTEGER", "int"),
])
def test_normalize_type_folds_case_whitespace_and_common_spellings(raw, norm):
    assert normalize_type(raw) == norm


# ---- shape and record files -------------------------------------------------------------------

def test_load_shape_validates_the_file(tmp_path):
    p = tmp_path / "s.json"
    p.write_text(json.dumps(NEW_SHAPE))
    assert load_shape(p)["tables"]["orders"][0]["name"] == "order_id"
    p.write_text(json.dumps({"tables": {"orders": [{"name": "a"}]}}))
    with pytest.raises(ConfigError, match="orders.*type"):
        load_shape(p)
    p.write_text(json.dumps({"tables": {"orders": [{"name": "a", "type": "int", "nullable": "no"}]}}))
    with pytest.raises(ConfigError, match="nullable"):
        load_shape(p)
    p.write_text("[]")
    with pytest.raises(ConfigError, match="tables"):
        load_shape(p)


def test_load_record_requires_every_key_and_a_known_status(tmp_path):
    assert RERUN_RECORD_KEYS == ("run", "status", "evidence", "shape")
    p = tmp_path / "r.json"
    p.write_text(json.dumps(_record("fresh", NEW_SHAPE)))
    assert load_record(p, "fresh")["run"] == "fresh"
    with pytest.raises(ConfigError, match="run.*evolved"):
        load_record(p, "evolved")
    p.write_text(json.dumps(_record("fresh", NEW_SHAPE, status="green")))
    with pytest.raises(ConfigError, match="status"):
        load_record(p, "fresh")
    p.write_text(json.dumps({"run": "fresh", "status": "pass", "shape": NEW_SHAPE}))
    with pytest.raises(ConfigError, match="evidence"):
        load_record(p, "fresh")
    p.write_text(json.dumps(_record("fresh", NEW_SHAPE, evidence="")))
    with pytest.raises(ConfigError, match="evidence"):
        load_record(p, "fresh")


# ---- grading ----------------------------------------------------------------------------------

def test_fresh_and_evolved_both_pass_when_both_runs_land_the_declared_shape():
    expected = declared_shape(DDL)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                      _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE, evidence="job-run/2"))
    assert out["fresh"] == "pass" and out["evolved"] == "pass"
    assert out["findings"] == [] and out["passed"] is True
    assert out["evidence"] == {"fresh": "job-run/1", "evolved": "job-run/2"}
    assert out["tables"] == ["mig.sales.orders"]
    assert out["notes"] == ["mig.sales.orders: CREATE TABLE IF NOT EXISTS with no ALTER TABLE ADD "
                            "COLUMN; the evolved run proves whether the shape still evolves"]


def test_create_if_not_exists_on_an_old_shape_table_is_the_canonical_evolved_failure():
    expected = declared_shape(DDL)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                      _record("evolved", OLD_SHAPE, pre_shape=OLD_SHAPE))
    assert out["fresh"] == "pass" and out["evolved"] == "fail" and out["passed"] is False
    assert out["findings"] == [{"run": "evolved", "table": "mig.sales.orders", "check": "column_missing",
                                "column": "channel", "detail": "declared string, absent after the run"}]


def test_grading_names_type_nullability_extra_and_missing_table_findings():
    expected = declared_shape(DDL)
    drifted = {"tables": {"orders": [
        {"name": "order_id", "type": "bigint", "nullable": True},
        {"name": "amount", "type": "double", "nullable": True},
        {"name": "channel", "type": "string", "nullable": True},
        {"name": "tags", "type": "array<struct<k:string,v:string>>", "nullable": True},
        {"name": "legacy_flag", "type": "string", "nullable": True},
    ]}}
    out = grade_rerun(expected, _record("fresh", drifted), None)
    checks = [(f["check"], f["column"]) for f in out["findings"]]
    assert checks == [("nullability_mismatch", "order_id"), ("type_mismatch", "amount"),
                      ("column_extra", "legacy_flag")]
    assert out["fresh"] == "fail"
    out = grade_rerun(expected, _record("fresh", {"tables": {"other": []}}), None)
    assert out["findings"] == [{"run": "fresh", "table": "mig.sales.orders", "check": "table_missing",
                                "column": None, "detail": "not in the observed shape"}]


def test_a_job_the_child_reports_failed_fails_regardless_of_shape():
    expected = declared_shape(DDL)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE, status="fail"),
                      _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE, status="fail"))
    assert out["fresh"] == "fail" and out["evolved"] == "fail"
    assert [f["check"] for f in out["findings"]] == ["job_failed", "job_failed"]


def test_a_failed_evolved_job_is_fail_even_without_a_usable_pre_shape():
    expected = declared_shape(DDL)
    for rec in (_record("evolved", NEW_SHAPE, status="fail"),
                _record("evolved", NEW_SHAPE, pre_shape=NEW_SHAPE, status="fail")):
        out = grade_rerun(expected, _record("fresh", NEW_SHAPE), rec)
        assert out["evolved"] == "fail" and out["passed"] is False
        assert [f["check"] for f in out["findings"]] == ["job_failed"]
        assert "unsupported_reason" not in out


def test_column_order_drift_is_a_finding():
    expected = declared_shape(DDL)
    cols = NEW_SHAPE["tables"]["orders"]
    reordered = {"tables": {"orders": [cols[0], cols[1], cols[3], cols[2]]}}
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                      _record("evolved", reordered, pre_shape=OLD_SHAPE))
    assert out["fresh"] == "pass" and out["evolved"] == "fail"
    assert out["findings"] == [{"run": "evolved", "table": "mig.sales.orders", "check": "column_order",
                                "column": None,
                                "detail": "declared order_id, amount, channel, tags; observed "
                                          "order_id, amount, tags, channel"}]
    # a missing column is reported as missing, not also as an order drift
    out = grade_rerun(expected, _record("fresh", OLD_SHAPE), None)
    assert [f["check"] for f in out["findings"]] == ["column_missing"]


def test_check_proof_rejects_contradictory_or_malformed_artifacts():
    good = {"unit": "u", "fresh": "pass", "evolved": "unsupported", "passed": True, "findings": [],
            "notes": [], "evidence": {"fresh": "job/1"}, "unsupported_reason": "no evolved record"}
    assert check_proof(good, "u", "x") == good
    bad = [
        {**good, "passed": False},                                   # disagrees with the legs
        {**good, "evolved": "fail", "passed": True},
        {**good, "evolved": "pass", "evidence": {"fresh": "job/1"}},  # no evolved evidence
        {**good, "evidence": {}},                                    # no fresh evidence
        {**good, "evidence": {"fresh": ""}},
        {**good, "findings": [{"check": "job_failed"}]},             # findings shape
        {**good, "findings": {}},
        {**good, "notes": "x"},
        {k: v for k, v in good.items() if k != "unsupported_reason"},
        {**good, "fresh": "fail", "passed": False, "findings": []},  # a failed leg names why
        # a leg that passed, or did not run, cannot carry a finding: a finding is a failure
        {**good, "findings": [{"run": "fresh", "table": "t", "check": "column_missing",
                              "column": "c", "detail": "d"}]},
        {**good, "findings": [{"run": "evolved", "table": "t", "check": "job_failed",
                              "column": None, "detail": "d"}]},
        {**good, "evolved": "pass", "evidence": {"fresh": "j/1", "evolved": "j/2"},
         "findings": [{"run": "evolved", "table": "t", "check": "column_missing",
                       "column": "c", "detail": "d"}]},
        {**good, "findings": [{"run": "warm", "table": "t", "check": "x", "column": None,
                              "detail": "d"}]},                         # unknown leg
    ]
    for proof in bad:
        with pytest.raises(ConfigError):
            check_proof(proof, "u", "x")
    failed = {**good, "evolved": "fail", "passed": False, "evidence": {"fresh": "j/1", "evolved": "j/2"},
              "findings": [{"run": "evolved", "table": "t", "check": "job_failed", "column": None,
                            "detail": "d"}]}
    del failed["unsupported_reason"]
    assert check_proof(failed, "u", "x") == failed


def test_evolved_is_unsupported_never_clean_when_no_prior_shape_was_exercised():
    expected = declared_shape(DDL)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE), None)
    assert out["evolved"] == "unsupported" and out["passed"] is True
    assert out["unsupported_reason"] == ("no evolved record: pre-create the table in its previous "
                                         "committed shape (git history or the prior wave's DDL) and run again")
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE), _record("evolved", NEW_SHAPE))
    assert out["evolved"] == "unsupported" and "pre_shape" in out["unsupported_reason"]
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                      _record("evolved", NEW_SHAPE, pre_shape=NEW_SHAPE))
    assert out["evolved"] == "unsupported"
    assert out["unsupported_reason"] == ("evolved pre_shape equals the declared shape: nothing evolved, "
                                         "so the run proves only what fresh proved")


def test_a_fresh_record_is_required_and_the_run_label_must_match():
    expected = declared_shape(DDL)
    with pytest.raises(ConfigError, match="fresh"):
        grade_rerun(expected, None, None)
    with pytest.raises(ConfigError, match="run.*fresh"):
        grade_rerun(expected, _record("evolved", NEW_SHAPE), None)


def test_tables_match_on_their_trailing_name_when_one_side_is_unqualified():
    expected = declared_shape("CREATE TABLE ORDERS (a INT);")
    out = grade_rerun(expected, _record("fresh", {"tables": {"mig.sales.orders": [
        {"name": "A", "type": "INT", "nullable": True}]}}), None)
    assert out["fresh"] == "pass" and out["findings"] == []


# ---- result.json wiring -----------------------------------------------------------------------

def _ok():
    return TierResult(tier=1, name="row_counts", passed=True, checks_run=1, findings=[], stats={})


def test_build_result_records_the_proof_and_blocks_on_rerun_gap():
    r = build_result("u", "live", "m1", "t1", [_ok()])
    assert r["rerun_proof"] is None and r["merge_eligible"] is True
    proof = {"fresh": "pass", "evolved": "pass", "passed": True, "findings": []}
    r = build_result("u", "live", "m1", "t1", [_ok()], rerun_proof=proof)
    assert r["rerun_proof"] == proof and r["merge_eligible"] is True and r["merge_block_reasons"] == []
    proof = {"fresh": "pass", "evolved": "fail", "passed": False, "findings": [{"check": "column_missing"}]}
    r = build_result("u", "live", "m1", "t1", [_ok()], rerun_proof=proof)
    assert r["merge_eligible"] is False and r["merge_block_reasons"] == ["rerun_gap"]
    assert r["verdict"] == "PASS"  # the tiers passed; the rerun gap is its own reason
    # an unsupported evolved leg is not a failure, but it is not proof either: a supplied proof
    # that did not exercise the previous shape blocks merge under its own reason
    proof = {"fresh": "pass", "evolved": "unsupported", "passed": True, "findings": []}
    r = build_result("u", "live", "m1", "t1", [_ok()], rerun_proof=proof)
    assert r["merge_eligible"] is False and r["merge_block_reasons"] == ["rerun_unsupported"]


def test_a_proof_with_findings_is_a_rerun_gap_whatever_its_legs_say():
    """Belt and braces: check_proof rejects a `pass` leg that carries findings, and the merge
    gate reads the findings too, so a contradictory artifact can never stay merge-eligible."""
    proof = {"fresh": "pass", "evolved": "pass", "passed": True,
             "findings": [{"run": "evolved", "table": "t", "check": "column_missing",
                           "column": "c", "detail": "x"}]}
    assert rerun_gap(proof) is True
    r = build_result("u", "live", "m1", "t1", [_ok()], rerun_proof=proof)
    assert r["merge_eligible"] is False and r["merge_block_reasons"] == ["rerun_gap"]
    with pytest.raises(ConfigError, match="lists a finding"):
        check_proof(dict(proof, unit="u", notes=[], evidence={"fresh": "a", "evolved": "b"}), "u", "x")


def test_run_recon_carries_the_proof_into_result_json(tmp_path):
    from recon.engine import run_recon

    from tests.test_tiers import RULES, SPEC, TOL, make_green
    proof = {"fresh": "fail", "evolved": "unsupported", "passed": False,
             "findings": [{"run": "fresh", "check": "job_failed"}]}
    source, target = make_green()
    result = run_recon("u", "live", SPEC, TOL, RULES, source, target,
                       out_dir=tmp_path, rerun_proof=proof)
    assert result["rerun_proof"] == proof and "rerun_gap" in result["merge_block_reasons"]
    written = json.loads((tmp_path / "result.json").read_text())
    assert written["rerun_proof"] == proof
    assert "rerun proof" in (tmp_path / "recon.summary.md").read_text().lower()


# ---- fixture ----------------------------------------------------------------------------------

def test_fixture_is_the_canonical_failing_case(tmp_path, capsys):
    rc = cli.main(["rerun-proof", "--unit", "orders", "--ddl", str(FIXTURE / "ddl.sql"),
                   "--fresh", str(FIXTURE / "fresh.json"), "--evolved", str(FIXTURE / "evolved.json"),
                   "--out", str(tmp_path)])
    assert rc == 1
    out = json.loads((tmp_path / "rerun_proof.json").read_text())
    assert out["unit"] == "orders" and out["fresh"] == "pass" and out["evolved"] == "fail"
    assert out["findings"][0]["check"] == "column_missing"
    assert json.loads(capsys.readouterr().out)["evolved"] == "fail"
    prior = (FIXTURE / "prior_ddl.sql").read_text()
    assert "channel" in (FIXTURE / "ddl.sql").read_text() and "channel" not in prior
    evolved = json.loads((FIXTURE / "evolved.json").read_text())
    assert evolved["pre_shape"] == evolved["shape"]  # IF NOT EXISTS left the old shape in place


def test_cli_passes_with_an_evolving_ddl_and_reports_unsupported_without_evolved(tmp_path, capsys):
    ddl = tmp_path / "ddl.sql"
    ddl.write_text((FIXTURE / "ddl.sql").read_text()
                   + "\nALTER TABLE mig.sales.orders ADD COLUMN IF NOT EXISTS channel STRING;\n")
    with pytest.raises(SystemExit, match="run is 'fresh', expected 'evolved'"):
        cli.main(["rerun-proof", "--unit", "orders", "--ddl", str(ddl),
                  "--fresh", str(FIXTURE / "fresh.json"), "--evolved", str(FIXTURE / "fresh.json"),
                  "--out", str(tmp_path)])
    rc = cli.main(["rerun-proof", "--unit", "orders", "--ddl", str(ddl),
                   "--fresh", str(FIXTURE / "fresh.json"), "--out", str(tmp_path)])
    assert rc == 0
    out = json.loads((tmp_path / "rerun_proof.json").read_text())
    assert out["fresh"] == "pass" and out["evolved"] == "unsupported" and out["notes"] == []


def test_cli_accepts_an_expected_shape_file_instead_of_ddl(tmp_path):
    shape = tmp_path / "expected.json"
    shape.write_text(json.dumps(NEW_SHAPE))
    fresh = tmp_path / "fresh.json"
    fresh.write_text(json.dumps(_record("fresh", NEW_SHAPE)))
    assert cli.main(["rerun-proof", "--unit", "u", "--expected-shape", str(shape),
                     "--fresh", str(fresh), "--out", str(tmp_path / "out")]) == 0
    out = json.loads((tmp_path / "out" / "rerun_proof.json").read_text())
    assert out["expected_from"] == str(shape) and out["fresh"] == "pass"
    with pytest.raises(SystemExit, match="--ddl or --expected-shape"):
        cli.main(["rerun-proof", "--unit", "u", "--fresh", str(fresh), "--out", str(tmp_path)])


def test_run_reads_a_rerun_proof_file_and_refuses_a_malformed_one(tmp_path, monkeypatch):
    from recon import adapters
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["mig"]}')
    seen = {}

    def fake_run_recon(*a, **kw):
        seen.update(kw)
        return {"verdict": "PASS", "depth": "threshold", "merge_eligible": False}
    monkeypatch.setattr(cli, "run_recon", fake_run_recon)
    monkeypatch.setattr(cli, "_load_spec", lambda *a: (type("S", (), {"version": "m1"})(), None))
    monkeypatch.setattr(cli, "load_tolerances", lambda p: type("T", (), {"version": "t1"})())
    monkeypatch.setattr(cli, "load_canon_rules", lambda p: [])
    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "postgres", lambda s: object())
    monkeypatch.setattr(adapters, "DatabricksTargetAdapter", lambda *a: object())
    proof = tmp_path / "rerun_proof.json"
    proof.write_text(json.dumps({"unit": "u", "fresh": "pass", "evolved": "fail", "passed": False,
                                 "findings": [{"run": "evolved", "table": "t", "check": "job_failed",
                                               "column": None, "detail": "d"}],
                                 "notes": [], "evidence": {"fresh": "j/1", "evolved": "j/2"}}))
    args = ["run", "--unit", "u", "--family", "postgres", "--mapping", "m", "--tolerances", "t",
            "--canonicalization", "c", "--mode", "live", "--source-dsn-secret", "S",
            "--target-secret", "T", "--target-catalog", "mig", "--target-schema", "s",
            "--out", str(tmp_path / "out"), "--rerun-proof", str(proof)]
    assert cli.main(args) == 0
    assert seen["rerun_proof"]["evolved"] == "fail"
    proof.write_text(json.dumps({"fresh": "pass"}))
    with pytest.raises(SystemExit, match="rerun-proof"):
        cli.main(args)
    proof.write_text(json.dumps({"unit": "other", "fresh": "pass", "evolved": "pass", "passed": True,
                                 "findings": [], "notes": [], "evidence": {}}))
    with pytest.raises(SystemExit, match="unit"):
        cli.main(args)


# ---- observed shape readers ------------------------------------------------------------------

def test_databricks_target_reads_the_observed_shape_from_information_schema(monkeypatch, tmp_path, capsys):
    from recon import adapters
    conn = _StubConn([("order_id", "BIGINT", "NO", 1), ("amount", "DECIMAL(18, 2)", "YES", 2)])
    monkeypatch.setattr(adapters, "_databricks_connect", lambda name: conn)
    target = adapters.DatabricksTargetAdapter("D", "mig", "sales")
    assert target.column_shape("orders") == [
        {"name": "order_id", "type": "bigint", "nullable": False},
        {"name": "amount", "type": "decimal(18,2)", "nullable": True}]
    sql, params = conn.executed[-1]
    assert "information_schema.columns" in sql and "ORDER BY ordinal_position" in sql
    assert params == {"catalog": "mig", "schema": "sales", "table": "orders"}
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["mig"]}')
    rc = cli.main(["shape", "--target-kind", "databricks", "--target-secret", "D", "--target-catalog", "mig",
                   "--target-schema", "sales", "--table", "orders", "--out", str(tmp_path / "shape.json")])
    assert rc == 0
    written = json.loads((tmp_path / "shape.json").read_text())
    assert written["tables"]["orders"][0]["name"] == "order_id" and written["target_kind"] == "databricks"
    with pytest.raises(SystemExit, match="not in"):
        cli.main(["shape", "--target-kind", "databricks", "--target-secret", "D", "--target-catalog", "prod",
                  "--target-schema", "sales", "--table", "orders", "--out", str(tmp_path / "s2.json")])


def test_lakebase_target_reads_the_observed_shape_from_pg_attribute(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    from recon import adapters
    monkeypatch.setenv("T", "dsn-under-test")
    conn = _StubConn([("db",)])
    monkeypatch.setattr(psycopg, "connect", lambda dsn: conn)
    target = adapters.LakebaseTargetAdapter("T", "db", "sales")
    conn.rows = [("order_id", "bigint", True, 1), ("amount", "numeric(18,2)", False, 2)]
    assert target.column_shape("orders") == [
        {"name": "order_id", "type": "bigint", "nullable": False},
        {"name": "amount", "type": "numeric(18,2)", "nullable": True}]
    sql, params = conn.executed[-1]
    assert "pg_attribute" in sql and "format_type" in sql and params == ("sales", "orders")
