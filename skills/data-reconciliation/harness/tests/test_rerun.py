"""Schema-evolution rerun proof (plan row 3.10): the idempotency proof runs twice, once on a
fresh target and once against a target pre-created in the table's previous committed shape.
The harness grades the two observed shapes against the shape the committed DDL declares and
records `rerun_proof: {fresh, evolved}`; a failing run is `rerun_gap` and never merge-eligible."""

import json
import re
import time
from pathlib import Path

import pytest
from recon import cli
from recon.config import ConfigError
from recon.report import build_result
from recon.rerun import _resolve_tables
from recon.rerun import (
    _check_shape,
    check_proof,
    expected_digest,
    RERUN_RECORD_KEYS,
    declared_shape,
    grade_rerun,
    load_record,
    load_shape,
    normalize_type,
    rerun_gap,
    rerun_missing,
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
PRIOR = _check_shape({"tables": {"mig.sales.orders": OLD_SHAPE["tables"]["orders"]}}, "prior")
NEW_SHAPE_QUALIFIED = {"tables": {"mig.sales.orders": NEW_SHAPE["tables"]["orders"]}}


def load_shape_dict(shape):
    return _check_shape(shape, "test")


DIGEST = expected_digest(load_shape_dict(NEW_SHAPE_QUALIFIED))


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
                    {"name": "n", "type": "decimal(10,2)", "nullable": True}]


def test_declared_shape_makes_table_level_primary_key_columns_not_null():
    """`PRIMARY KEY (a, b)` as a table constraint is what the catalog reports as NOT NULL on a and
    b, so the declared shape says so too; a key over a column the table lacks is a broken DDL."""
    sql = """CREATE TABLE t (id BIGINT, day DATE, note STRING,
      CONSTRAINT pk_t PRIMARY KEY (id, `day`));
    CREATE TABLE u (id BIGINT, PRIMARY KEY (id));"""
    shape = declared_shape(sql)
    assert shape["tables"]["t"] == [{"name": "id", "type": "bigint", "nullable": False},
                                   {"name": "day", "type": "date", "nullable": False},
                                   {"name": "note", "type": "string", "nullable": True}]
    assert shape["tables"]["u"] == [{"name": "id", "type": "bigint", "nullable": False}]
    with pytest.raises(ConfigError, match="PRIMARY KEY.*missing"):
        declared_shape("CREATE TABLE t (id BIGINT, PRIMARY KEY (id, missing));")


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


@pytest.mark.parametrize("alter", [
    "ALTER TABLE t DROP COLUMN b",
    "ALTER TABLE t RENAME COLUMN b TO c",
    "ALTER TABLE t ALTER COLUMN b TYPE BIGINT",
    "ALTER TABLE t CHANGE COLUMN b b STRING",
    "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (a)",
    "ALTER TABLE t REPLACE COLUMNS (a INT)",
])
def test_declared_shape_fails_closed_on_an_alter_it_cannot_apply(alter):
    """A schema change the parser does not apply would leave a stale declared shape that a broken
    job could match, so it is a refusal (use --expected-shape), never harmless `other` SQL."""
    with pytest.raises(ConfigError, match=r"ALTER TABLE.*--expected-shape"):
        declared_shape(f"CREATE TABLE t (a INT, b INT); {alter};")


@pytest.mark.parametrize("create", [
    "CREATE TABLE derived AS SELECT id FROM anchor",
    "CREATE OR REPLACE TABLE derived USING DELTA AS SELECT id FROM anchor",
    "CREATE TABLE IF NOT EXISTS derived LIKE anchor",
    "CREATE TABLE derived (LIKE anchor INCLUDING ALL)",
    "CREATE TABLE derived (local_id BIGINT, LIKE anchor INCLUDING ALL)",
    "CREATE TABLE derived (local_id BIGINT, CONSTRAINT pk PRIMARY KEY (local_id), LIKE anchor)",
])
def test_declared_shape_fails_closed_on_a_create_table_without_a_column_list(create):
    """CTAS and LIKE create a table whose columns the parser cannot derive; letting them fall
    through as `other` would grade a shape that omits the table."""
    with pytest.raises(ConfigError, match=r"CREATE TABLE.*derived.*--expected-shape"):
        declared_shape(f"CREATE TABLE anchor (id INT); {create};")


def test_declared_shape_lets_property_and_owner_alters_through_as_other():
    shape = declared_shape("CREATE TABLE t (a INT); ALTER TABLE t SET TBLPROPERTIES ('k' = 'v');\n"
                           "ALTER TABLE t UNSET TBLPROPERTIES ('k'); ALTER TABLE t SET OWNER TO `grp`;")
    assert shape["tables"]["t"] == [{"name": "a", "type": "int", "nullable": True}]
    assert shape["statements"] == {"create_table": 1, "alter_table": 0, "other": 3}


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
    ("NUMERIC(18, 2)", "decimal(18,2)"),
    ("numeric", "decimal"),
    ("timestamp(6) without time zone", "timestamp(6)"),
    ("timestamp(3) with time zone", "timestamptz(3)"),
    ("time(3) without time zone", "time(3)"),
    ("time with time zone", "timetz"),
    ("TIMESTAMP(6)", "timestamp(6)"),
    ("TIMESTAMP (6) WITHOUT TIME ZONE", "timestamp(6)"),
    ("time  (3)  with   time   zone", "timetz(3)"),
])
def test_normalize_type_folds_case_whitespace_and_common_spellings(raw, norm):
    assert normalize_type(raw) == norm


def test_declared_shape_expands_postgres_serial_to_what_the_catalog_reports():
    """`SERIAL` is an integer column with NOT NULL and a sequence default once created, and that is
    what format_type and is_nullable report; the declared shape says the same."""
    cols = declared_shape("CREATE TABLE t (id SERIAL, big BIGSERIAL, small SMALLSERIAL NULL, "
                          "n serial4 PRIMARY KEY, at TIMESTAMP(6));")["tables"]["t"]
    assert cols == [{"name": "id", "type": "int", "nullable": False},
                    {"name": "big", "type": "bigint", "nullable": False},
                    {"name": "small", "type": "smallint", "nullable": False},
                    {"name": "n", "type": "int", "nullable": False},
                    {"name": "at", "type": "timestamp(6)", "nullable": True}]
    observed = {"tables": {"t": [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "big", "type": "bigint", "nullable": False},
        {"name": "small", "type": "smallint", "nullable": False},
        {"name": "n", "type": "integer", "nullable": False},
        {"name": "at", "type": "timestamp(6) without time zone", "nullable": True}]}}
    expected = declared_shape("CREATE TABLE t (id SERIAL, big BIGSERIAL, small SMALLSERIAL NULL, "
                              "n serial4 PRIMARY KEY, at TIMESTAMP(6));")
    assert grade_rerun(expected, _record("fresh", observed), None)["findings"] == []


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
                      _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE, evidence="job-run/2"),
                      prior=PRIOR)
    assert out["fresh"] == "pass" and out["evolved"] == "pass"
    assert out["prior_digest"] == expected_digest(PRIOR)
    assert out["findings"] == [] and out["passed"] is True
    assert out["evidence"] == {"fresh": "job-run/1", "evolved": "job-run/2"}
    assert out["tables"] == ["mig.sales.orders"]
    assert out["notes"] == ["mig.sales.orders: CREATE TABLE IF NOT EXISTS with no ALTER TABLE ADD "
                            "COLUMN; the evolved run proves whether the shape still evolves"]


def test_create_if_not_exists_on_an_old_shape_table_is_the_canonical_evolved_failure():
    expected = declared_shape(DDL)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                      _record("evolved", OLD_SHAPE, pre_shape=OLD_SHAPE), prior=PRIOR)
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
                      _record("evolved", reordered, pre_shape=OLD_SHAPE), prior=PRIOR)
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
            "notes": [], "evidence": {"fresh": "job/1"}, "unsupported_reason": "no evolved record",
            "expected_digest": DIGEST}
    ran = {**good, "evolved": "pass", "evidence": {"fresh": "j/1", "evolved": "j/2"},
           "prior_digest": expected_digest(PRIOR)}
    del ran["unsupported_reason"]
    assert check_proof(ran, "u", "x", DIGEST) == ran
    assert check_proof(good, "u", "x", DIGEST) == good
    with pytest.raises(ConfigError, match="stale"):
        check_proof(good, "u", "x", expected_digest(load_shape_dict(OLD_SHAPE)))
    bad = [
        {k: v for k, v in good.items() if k != "expected_digest"},
        {**good, "expected_digest": ""},
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
        {k: v for k, v in ran.items() if k != "prior_digest"},       # evolved pass, no prior shape
        {**ran, "prior_digest": ""},
    ]
    for proof in bad:
        with pytest.raises(ConfigError):
            check_proof(proof, "u", "x", DIGEST)
    failed = {**good, "evolved": "fail", "passed": False, "evidence": {"fresh": "j/1", "evolved": "j/2"},
              "findings": [{"run": "evolved", "table": "t", "check": "job_failed", "column": None,
                            "detail": "d"}]}  # a failed job needs no prior shape to be a failure
    del failed["unsupported_reason"]
    assert check_proof(failed, "u", "x", DIGEST) == failed


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


def test_evolved_is_unsupported_when_the_pre_shape_lacks_a_declared_table():
    """A table absent before the run met an empty target, so that leg was another fresh run;
    only a table that existed in an older shape evolves."""
    expected = declared_shape(DDL)
    for pre in ({"tables": {}}, {"tables": {"other": OLD_SHAPE["tables"]["orders"]}}):
        out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                          _record("evolved", NEW_SHAPE, pre_shape=pre))
        assert out["evolved"] == "unsupported" and out["findings"] == []
        assert out["unsupported_reason"] == ("evolved pre_shape has no mig.sales.orders: the table did "
                                             "not exist before the run, so that leg was a fresh run")


def test_the_proof_carries_a_digest_of_the_shape_it_graded_against():
    expected = declared_shape(DDL)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE), None)
    assert out["expected_digest"] == expected_digest(expected)
    assert expected_digest(expected) != expected_digest(load_shape_dict(OLD_SHAPE))
    assert expected_digest(load_shape_dict(NEW_SHAPE_QUALIFIED)) != expected_digest(load_shape_dict(OLD_SHAPE))


def test_the_digest_binds_the_ddl_text_not_only_the_shape_it_lands():
    """Idempotency is a property of the statements, not of the columns they leave behind: dropping
    the ADD COLUMN that makes IF NOT EXISTS safe leaves the shape (and a shape-only digest) intact,
    so `run` would accept the old proof for a job that no longer evolves the table."""
    safe = DDL + "ALTER TABLE mig.sales.orders ADD COLUMN IF NOT EXISTS channel STRING;"
    assert declared_shape(safe)["tables"] == declared_shape(DDL)["tables"]
    assert expected_digest(declared_shape(safe)) != expected_digest(declared_shape(DDL))
    assert expected_digest(declared_shape(DDL)) != expected_digest(load_shape_dict(NEW_SHAPE_QUALIFIED))
    respaced = "/* note */ " + re.sub(r"\s+", " ", DDL.split("\n", 2)[2]).replace("; ", ";\n\n")
    assert expected_digest(declared_shape(respaced)) == expected_digest(declared_shape(DDL))


def test_the_digest_keeps_whitespace_inside_literals_and_quoted_identifiers():
    """Only whitespace between tokens is insignificant: a DEFAULT literal or a quoted name that
    changes its spaces is a different statement, so the old proof is stale."""
    one = "CREATE TABLE t (note STRING DEFAULT 'x  y', `two  words` INT);"
    two = "CREATE TABLE t (note STRING DEFAULT 'x y', `two  words` INT);"
    three = "CREATE TABLE t (note STRING DEFAULT 'x  y', `two words` INT);"
    assert declared_shape(one)["tables"] == declared_shape(two)["tables"]
    assert expected_digest(declared_shape(one)) != expected_digest(declared_shape(two))
    assert expected_digest(declared_shape(one)) != expected_digest(declared_shape(three))
    assert expected_digest(declared_shape(one)) == expected_digest(declared_shape(
        "CREATE   TABLE t\n(note STRING   DEFAULT 'x  y',\n `two  words`  INT);"))


def test_a_later_guarded_create_keeps_the_first_shape_and_an_unguarded_duplicate_is_refused():
    """SQL creates the first definition and treats a later CREATE TABLE IF NOT EXISTS as a no-op;
    only CREATE OR REPLACE replaces. A second unguarded CREATE of the same table would fail when
    run, so the DDL is refused rather than graded against a shape it never lands."""
    shape = declared_shape("CREATE TABLE t (a INT); CREATE TABLE IF NOT EXISTS t (b INT);")
    assert shape["tables"]["t"] == [{"name": "a", "type": "int", "nullable": True}]
    assert shape["if_not_exists"] == ["t"] and shape["statements"]["create_table"] == 2
    shape = declared_shape("CREATE TABLE t (a INT); ALTER TABLE t ADD COLUMN c INT; "
                           "CREATE TABLE IF NOT EXISTS t (b INT);")
    assert [c["name"] for c in shape["tables"]["t"]] == ["a", "c"]
    shape = declared_shape("CREATE TABLE t (a INT); CREATE OR REPLACE TABLE t (b INT);")
    assert [c["name"] for c in shape["tables"]["t"]] == ["b"]
    with pytest.raises(ConfigError, match="t is created twice"):
        declared_shape("CREATE TABLE t (a INT); CREATE TABLE t (b INT);")
    with pytest.raises(ConfigError, match="t is created twice"):
        declared_shape("CREATE TABLE IF NOT EXISTS t (a INT); CREATE TABLE t (b INT);")


def test_the_evolved_leg_needs_the_prior_committed_shape_and_the_pre_shape_must_equal_it():
    """`pre_shape != declared` only proves something differed; the leg is evolution only when the
    table started in the previous committed shape, so that shape is an input and pre_shape must
    match it exactly."""
    expected = declared_shape(DDL)
    evolved = _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE)
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE), evolved)
    assert out["evolved"] == "unsupported" and out["findings"] == [] and "prior_digest" not in out
    assert out["unsupported_reason"] == ("no prior shape: pass --prior-ddl or --prior-shape (the previous "
                                         "committed shape) so the evolved leg can be checked against it")
    drifted = {"tables": {"orders": [dict(NEW_SHAPE["tables"]["orders"][0]),
                                     {"name": "amount", "type": "double", "nullable": True},
                                     *NEW_SHAPE["tables"]["orders"][2:]]}}
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE),
                      _record("evolved", NEW_SHAPE, pre_shape=drifted), prior=PRIOR)
    assert out["evolved"] == "unsupported" and out["prior_digest"] == expected_digest(PRIOR)
    assert out["unsupported_reason"].startswith("evolved pre_shape is not the prior committed shape")
    assert "channel" in out["unsupported_reason"] and "amount" in out["unsupported_reason"]
    out = grade_rerun(expected, _record("fresh", NEW_SHAPE), evolved, prior=PRIOR)
    assert out["evolved"] == "pass" and out["prior_digest"] == expected_digest(PRIOR)


def test_alter_table_with_several_add_column_actions_adds_each_and_refuses_other_actions():
    shape = declared_shape("CREATE TABLE t (a INT); ALTER TABLE t ADD COLUMN b INT, "
                           "ADD COLUMN IF NOT EXISTS c STRING NOT NULL, ADD d INT AFTER a;")
    assert [(c["name"], c["type"], c["nullable"]) for c in shape["tables"]["t"]] == [
        ("a", "int", True), ("d", "int", True), ("b", "int", True), ("c", "string", False)]
    with pytest.raises(ConfigError, match=r"DROP COLUMN a.*--expected-shape"):
        declared_shape("CREATE TABLE t (a INT); ALTER TABLE t ADD COLUMN b INT, DROP COLUMN a;")


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


def test_one_observed_table_cannot_stand_in_for_two_declared_tables_that_share_a_name():
    """Two declared tables with the same trailing name and one unqualified observation: the match
    is ambiguous, so both are missing rather than both passing on one table."""
    expected = declared_shape("CREATE TABLE sales.orders (id INT); CREATE TABLE archive.orders (id INT);")
    one = {"tables": {"orders": [{"name": "id", "type": "int", "nullable": True}]}}
    out = grade_rerun(expected, _record("fresh", one), None)
    assert out["fresh"] == "fail"
    assert [(f["table"], f["check"]) for f in out["findings"]] == [
        ("sales.orders", "table_missing"), ("archive.orders", "table_missing")]
    assert all("ambiguous" in f["detail"] for f in out["findings"])
    both = {"tables": {"sales.orders": one["tables"]["orders"], "archive.orders": one["tables"]["orders"]}}
    assert grade_rerun(expected, _record("fresh", both), None)["findings"] == []
    # a qualified observation of one and an exact match of the other still resolve one-to-one
    mixed = {"tables": {"mig.sales.orders": one["tables"]["orders"], "archive.orders": one["tables"]["orders"]}}
    assert grade_rerun(expected, _record("fresh", mixed), None)["findings"] == []


def test_table_resolution_stays_fast_when_every_table_has_one_obvious_candidate():
    """Thirty expected tables, each observed once under a catalog prefix, resolve at once; a search
    over every partial assignment would visit 2^30 branches here."""
    cols = [{"name": "id", "type": "int", "nullable": True}]
    expected = {f"s.t{i}": cols for i in range(30)}
    observed = {f"cat.s.t{i}": cols for i in range(30)}
    started = time.monotonic()
    assert _resolve_tables(expected, observed) == {f"s.t{i}": f"cat.s.t{i}" for i in range(30)}
    assert time.monotonic() - started < 2
    observed["orders"] = cols
    expected.update({"sales.orders": cols, "archive.orders": cols})
    out = _resolve_tables(expected, observed)
    assert out["sales.orders"] is None and out["archive.orders"] is None
    assert out["s.t7"] == "cat.s.t7"


def test_a_forced_one_to_one_assignment_resolves_tables_that_share_a_trailing_name():
    """`orders` could be either declared table on its own, but only sales.orders can take
    `mig.sales.orders`, which leaves `orders` for archive.orders: every complete matching agrees, so
    both resolve. A table whose observation varies between matchings stays missing."""
    cols = [{"name": "id", "type": "int", "nullable": True}]
    expected = declared_shape("CREATE TABLE sales.orders (id INT); CREATE TABLE archive.orders (id INT);")
    forced = {"tables": {"mig.sales.orders": cols, "orders": cols}}
    assert grade_rerun(expected, _record("fresh", forced), None)["findings"] == []
    assert _resolve_tables(expected["tables"], forced["tables"]) == {
        "sales.orders": "mig.sales.orders", "archive.orders": "orders"}
    three = declared_shape("CREATE TABLE sales.orders (id INT); CREATE TABLE archive.orders (id INT); "
                           "CREATE TABLE stage.orders (id INT);")
    varies = {"tables": {"mig.sales.orders": cols, "orders": cols, "x.orders": cols}}
    assert _resolve_tables(three["tables"], varies["tables"]) == {
        "sales.orders": "mig.sales.orders", "archive.orders": None, "stage.orders": None}
    out = grade_rerun(three, _record("fresh", varies), None)
    assert [(f["table"], f["check"]) for f in out["findings"]] == [
        ("archive.orders", "table_missing"), ("stage.orders", "table_missing")]


def test_drop_table_lets_a_later_create_land_its_own_shape():
    """A drop-and-recreate job is valid DDL: after DROP TABLE the next CREATE is the first one
    again and its columns are the declared shape."""
    shape = declared_shape("CREATE TABLE t (a INT); DROP TABLE t; CREATE TABLE t (b INT);")
    assert [c["name"] for c in shape["tables"]["t"]] == ["b"]
    assert shape["statements"] == {"create_table": 2, "alter_table": 0, "other": 1}
    shape = declared_shape("CREATE TABLE t (a INT); ALTER TABLE t ADD COLUMN c INT; DROP TABLE IF EXISTS `t`; "
                           "CREATE TABLE IF NOT EXISTS t (b INT);")
    assert [c["name"] for c in shape["tables"]["t"]] == ["b"]
    assert declared_shape("CREATE TABLE t (a INT); DROP TABLE t;")["tables"] == {}
    assert declared_shape("DROP TABLE IF EXISTS t; CREATE TABLE t (a INT);")["tables"]["t"] == [
        {"name": "a", "type": "int", "nullable": True}]
    with pytest.raises(ConfigError, match="t is created twice"):
        declared_shape("CREATE TABLE t (a INT); DROP TABLE other; CREATE TABLE t (b INT);")


def test_drop_table_starts_the_table_over_including_its_create_and_alter_history():
    """The IF NOT EXISTS / ALTER notes describe how the surviving shape came to be; a dropped
    table's earlier statements are not part of that story."""
    shape = declared_shape("CREATE TABLE t (a INT); ALTER TABLE t ADD COLUMN c INT; DROP TABLE t; "
                           "CREATE TABLE IF NOT EXISTS t (b INT);")
    assert shape["if_not_exists"] == ["t"] and shape["altered"] == []
    shape = declared_shape("CREATE TABLE IF NOT EXISTS t (a INT); DROP TABLE t; CREATE TABLE t (b INT); "
                           "ALTER TABLE t ADD COLUMN c INT;")
    assert shape["if_not_exists"] == [] and shape["altered"] == ["t"]
    assert [c["name"] for c in shape["tables"]["t"]] == ["b", "c"]


def test_drop_table_takes_every_table_in_its_list_and_the_dialect_modifiers():
    shape = declared_shape("CREATE TABLE a (x INT); CREATE TABLE b (x INT); DROP TABLE a,b;")
    assert shape["tables"] == {}
    shape = declared_shape("CREATE TABLE a (x INT); CREATE TABLE b (y INT); DROP TABLE a, b; CREATE TABLE b (z INT);")
    assert shape["tables"] == {"b": [{"name": "z", "type": "int", "nullable": True}]}
    shape = declared_shape('CREATE TABLE s.a (x INT); CREATE TABLE `b` (x INT); CREATE TABLE c (x INT); '
                           'DROP TABLE IF EXISTS ONLY s.a, `b` CASCADE; DROP TABLE c RESTRICT;')
    assert shape["tables"] == {}
    for bad in ("DROP TABLE t PURGE;", "DROP TABLE t, ;", "DROP TABLE;", "DROP TABLE t CASCADE CONSTRAINTS;"):
        with pytest.raises(ConfigError, match="DROP TABLE"):
            declared_shape("CREATE TABLE t (a INT); " + bad)


def test_bracket_quoted_names_keep_their_commas_and_escaped_brackets():
    """T-SQL brackets quote like backticks do: a comma or a doubled `]]` inside them is part of
    the name, in a DROP list and in a column list alike."""
    shape = declared_shape("CREATE TABLE [orders,archive] ([a,b] INT, [c]]d] INT); "
                           "DROP TABLE [orders,archive];")
    assert shape["tables"] == {}
    shape = declared_shape("CREATE TABLE [orders,archive] ([a,b] INT, [c]]d] INT NOT NULL);")
    assert shape["tables"] == {"orders,archive": [
        {"name": "a,b", "type": "int", "nullable": True},
        {"name": "c]d", "type": "int", "nullable": False}]}
    shape = declared_shape("CREATE TABLE [x,y] (a INT); CREATE TABLE z (a INT); DROP TABLE [x,y], z;")
    assert shape["tables"] == {}


# ---- result.json wiring -----------------------------------------------------------------------

def _ok():
    return TierResult(tier=1, name="row_counts", passed=True, checks_run=1, findings=[], stats={})


def test_a_result_without_a_rerun_proof_is_not_merge_eligible():
    """Every migrated unit writes its tables, so no proof is a missing control, not a clean one:
    result.json records null and blocks under `rerun_missing`."""
    r = build_result("u", "live", "m1", "t1", [_ok()])
    assert r["rerun_proof"] is None and r["merge_eligible"] is False
    assert r["merge_block_reasons"] == ["rerun_missing"]
    assert rerun_missing(None) is True and rerun_missing({"fresh": "pass"}) is False


def test_build_result_records_the_proof_and_blocks_on_rerun_gap():
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
        check_proof(dict(proof, unit="u", notes=[], evidence={"fresh": "a", "evolved": "b"},
                         expected_digest=DIGEST), "u", "x", DIGEST)


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
                   "--prior-ddl", str(FIXTURE / "prior_ddl.sql"),
                   "--fresh", str(FIXTURE / "fresh.json"), "--evolved", str(FIXTURE / "evolved.json"),
                   "--out", str(tmp_path)])
    assert rc == 1
    out = json.loads((tmp_path / "rerun_proof.json").read_text())
    assert out["unit"] == "orders" and out["fresh"] == "pass" and out["evolved"] == "fail"
    assert out["prior_digest"] == expected_digest(declared_shape((FIXTURE / "prior_ddl.sql").read_text()))
    assert out["prior_from"] == str(FIXTURE / "prior_ddl.sql")
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
    ddl = tmp_path / "ddl.sql"
    ddl.write_text(DDL)
    digest = expected_digest(declared_shape(DDL))
    proof.write_text(json.dumps({"unit": "u", "fresh": "pass", "evolved": "fail", "passed": False,
                                 "findings": [{"run": "evolved", "table": "t", "check": "job_failed",
                                               "column": None, "detail": "d"}],
                                 "notes": [], "evidence": {"fresh": "j/1", "evolved": "j/2"},
                                 "expected_digest": digest}))
    base = ["run", "--unit", "u", "--family", "postgres", "--mapping", "m", "--tolerances", "t",
            "--canonicalization", "c", "--mode", "live", "--source-dsn-secret", "S",
            "--target-secret", "T", "--target-catalog", "mig", "--target-schema", "s",
            "--out", str(tmp_path / "out"), "--rerun-proof", str(proof)]
    args = base + ["--rerun-ddl", str(ddl)]
    assert cli.main(args) == 0
    assert seen["rerun_proof"]["evolved"] == "fail"
    # the proof is bound to the DDL it graded: a DDL that moved on makes it stale
    with pytest.raises(SystemExit, match="rerun-ddl or --rerun-expected-shape"):
        cli.main(base)
    ddl.write_text(DDL + "\nALTER TABLE mig.sales.orders ADD COLUMN region STRING;\n")
    with pytest.raises(SystemExit, match="stale"):
        cli.main(args)
    # a DDL-graded proof is bound to that DDL's text; the same columns from a shape file are
    # another expected shape, so `run` must be given what rerun-proof was given
    shape = tmp_path / "expected.json"
    shape.write_text(json.dumps(NEW_SHAPE_QUALIFIED))
    with pytest.raises(SystemExit, match="stale"):
        cli.main(base + ["--rerun-expected-shape", str(shape)])
    proof.write_text(json.dumps({"fresh": "pass"}))
    with pytest.raises(SystemExit, match="rerun-proof"):
        cli.main(args)
    proof.write_text(json.dumps({"unit": "other", "fresh": "pass", "evolved": "pass", "passed": True,
                                 "findings": [], "notes": [], "evidence": {}, "expected_digest": digest}))
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


def test_shape_refuses_an_absent_table_rather_than_recording_it_empty(monkeypatch, tmp_path):
    """A catalog with no rows for the table means the table is not there; writing `[]` would let a
    pre_shape claim the table existed and turn a fresh run into an `evolved` pass."""
    from recon import adapters
    conn = _StubConn([])
    monkeypatch.setattr(adapters, "_databricks_connect", lambda name: conn)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["mig"]}')
    out = tmp_path / "shape.json"
    with pytest.raises(SystemExit, match=r"shape: .*orders.* not (found|in) .*mig\.sales"):
        cli.main(["shape", "--target-kind", "databricks", "--target-secret", "D", "--target-catalog", "mig",
                  "--target-schema", "sales", "--table", "orders", "--out", str(out)])
    assert not out.exists()


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
        {"name": "amount", "type": "decimal(18,2)", "nullable": True}]
    sql, params = conn.executed[-1]
    assert "pg_attribute" in sql and "format_type" in sql and params == ("sales", "orders")


def test_a_top_level_comparison_does_not_swallow_the_statements_after_it():
    """`<` and `>` nest only inside a type (STRUCT<...>); in a statement body they are operators,
    and treating them as brackets would drop a later ALTER TABLE from the expected shape."""
    shape = declared_shape(
        "CREATE TABLE t (a INT); INSERT INTO audit SELECT 1 WHERE 1 < 2; "
        "ALTER TABLE t ADD COLUMN b INT; "
        "CREATE TABLE u (c INT, d ARRAY<STRUCT<x: INT, y: INT>>, e INT DEFAULT 3 CHECK (e > 1));")
    assert [c["name"] for c in shape["tables"]["t"]] == ["a", "b"]
    assert [c["name"] for c in shape["tables"]["u"]] == ["c", "d", "e"]
    assert shape["tables"]["u"][1]["type"] == "array<struct<x:int,y:int>>"
    assert shape["altered"] == ["t"] and shape["statements"]["other"] == 1


def test_comment_markers_inside_literals_and_quoted_identifiers_are_kept():
    shape = declared_shape(
        "-- header\n"
        "CREATE TABLE t (url STRING DEFAULT 'https://host/a--b', /* real; comment, */ id INT, "
        "note STRING DEFAULT 'it''s /* not */ a comment', `odd--name` INT); -- trailing")
    assert [c["name"] for c in shape["tables"]["t"]] == ["url", "id", "note", "odd--name"]
    assert shape["statements"] == {"create_table": 1, "alter_table": 0, "other": 0}


def test_add_column_first_and_after_place_the_column_and_leave_its_type_clean():
    shape = declared_shape(
        "CREATE TABLE t (a INT, b STRING); "
        "ALTER TABLE t ADD COLUMN c STRING AFTER a; "
        "ALTER TABLE t ADD COLUMNS (d INT FIRST, e DECIMAL(10, 2) NOT NULL AFTER b);")
    assert [(c["name"], c["type"], c["nullable"]) for c in shape["tables"]["t"]] == [
        ("d", "int", True), ("a", "int", True), ("c", "string", True),
        ("b", "string", True), ("e", "decimal(10,2)", False)]
    with pytest.raises(ConfigError, match="AFTER zz"):
        declared_shape("CREATE TABLE t (a INT); ALTER TABLE t ADD COLUMN c INT AFTER zz;")


class _QueuedConn(_StubConn):
    """A stub whose successive fetches answer from a queue (one answer per statement)."""

    def __init__(self, answers):
        super().__init__([])
        self.answers = list(answers)

    def cursor(self):
        cur, conn = super().cursor(), self

        class Cur:
            def execute(self, sql, params=()):
                cur.execute(sql, params)
                conn.rows = conn.answers.pop(0)

            def fetchall(self):
                return conn.rows
        return Cur()


def test_shape_keeps_an_existing_zero_column_table_as_an_empty_list(monkeypatch, tmp_path):
    """`CREATE TABLE markers ()` is legal on Postgres and declared_shape records it as `[]`; the
    shape reader must tell that table apart from an absent one by asking the catalog whether the
    table exists, not by counting its columns."""
    from recon import adapters
    conn = _QueuedConn([[], [("markers",)]])
    monkeypatch.setattr(adapters, "_databricks_connect", lambda name: conn)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["mig"]}')
    out = tmp_path / "shape.json"
    cli.main(["shape", "--target-kind", "databricks", "--target-secret", "D", "--target-catalog", "mig",
              "--target-schema", "sales", "--table", "markers", "--out", str(out)])
    assert json.loads(out.read_text())["tables"] == {"markers": []}
    assert "information_schema.tables" in conn.executed[-1][0]


def test_lakebase_target_table_exists_asks_pg_class(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    from recon import adapters
    monkeypatch.setenv("T", "dsn-under-test")
    conn = _StubConn([("db",)])
    monkeypatch.setattr(psycopg, "connect", lambda dsn: conn)
    target = adapters.LakebaseTargetAdapter("T", "db", "sales")
    conn.rows = [(1,)]
    assert target.table_exists("markers") is True
    sql, params = conn.executed[-1]
    assert "pg_class" in sql and params == ("sales", "markers")
    conn.rows = []
    assert target.table_exists("gone") is False
