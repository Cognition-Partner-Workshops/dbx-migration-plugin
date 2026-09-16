"""Schema-evolution rerun proof (plan row 3.10): the idempotency proof runs twice, once on a
fresh target and once against a target pre-created in the table's previous committed shape.
The catalog is the source of truth: the shape observed after the fresh run is the expected shape,
the evolved run must land the identical shape, and the evolved leg counts only when it started
from the previously committed proof's shape (or the manifest-declared old shape on a first run).
A failing run is `rerun_gap` and never merge-eligible."""

import json
from pathlib import Path

import pytest
from recon import cli
from recon.config import ConfigError
from recon.report import build_result
from recon.rerun import (
    RERUN_RECORD_KEYS,
    _check_shape,
    check_proof,
    ddl_tables,
    grade_rerun,
    load_prior,
    load_record,
    load_shape,
    normalize_type,
    rerun_gap,
    rerun_missing,
    shape_digest,
    source_digest,
)
from recon.tiers import TierResult

from tests.loans import _StubConn

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "example_rerun"

NEW_SHAPE = {"tables": {"orders": [
    {"name": "order_id", "type": "bigint", "nullable": False},
    {"name": "amount", "type": "decimal(18,2)", "nullable": True},
    {"name": "channel", "type": "string", "nullable": True},
    {"name": "tags", "type": "array<struct<k:string,v:string>>", "nullable": True},
]}}
OLD_SHAPE = {"tables": {"orders": NEW_SHAPE["tables"]["orders"][:2] + NEW_SHAPE["tables"]["orders"][3:]}}
PRIOR = _check_shape(OLD_SHAPE, "prior")
DIGEST = "a" * 64


def _record(run, shape, pre_shape=None, status="pass", evidence="job-run/1"):
    rec = {"run": run, "status": status, "evidence": evidence, "shape": shape}
    if pre_shape is not None:
        rec["pre_shape"] = pre_shape
    return rec


def _grade(fresh, evolved=None, prior=None, **kw):
    return grade_rerun(fresh, evolved, prior, digest=DIGEST, **kw)


# ---- types and shapes -------------------------------------------------------------------------

@pytest.mark.parametrize("raw, norm", [
    ("BIGINT", "bigint"),
    ("Decimal(18, 2)", "decimal(18,2)"),
    ("NUMERIC(18,2)", "decimal(18,2)"),
    ("ARRAY<STRUCT<k: STRING, v: STRING>>", "array<struct<k:string,v:string>>"),
    ("integer", "int"),
    ("character varying(20)", "varchar(20)"),
    ("timestamp without time zone", "timestamp"),
    ("timestamp with time zone", "timestamptz"),
    ("TIMESTAMP (6) WITHOUT TIME ZONE", "timestamp(6)"),
    ("time  (3)  with   time   zone", "timetz(3)"),
    ("character(3)", "char(3)"),
    ("CHAR(3)", "char(3)"),
    ("character", "char"),
    ("character varying", "varchar"),
    ("double precision", "double"),
])
def test_normalize_type_folds_case_whitespace_and_common_spellings(raw, norm):
    assert normalize_type(raw) == norm


def test_shapes_keep_the_names_the_catalog_reported():
    """Both legs and the prior come from the same catalog reader, so names compare as reported:
    a quoted `"Orders"` on PostgreSQL is not `orders`, and the harness never folds it."""
    out = _check_shape({"tables": {"Orders": [{"name": "Id", "type": "INTEGER", "nullable": False}]}}, "x")
    assert out == {"tables": {"Orders": [{"name": "Id", "type": "int", "nullable": False}]}}
    assert shape_digest(out) != shape_digest(_check_shape(
        {"tables": {"orders": [{"name": "Id", "type": "INTEGER", "nullable": False}]}}, "x"))


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


def test_source_digest_binds_the_job_files_content_and_names(tmp_path):
    """The proof names the job it graded by hashing the job's source files, so any edit to the
    job (DDL, notebook, SQL) makes an older proof stale; the order the files are given in does not."""
    a, b = tmp_path / "ddl.sql", tmp_path / "job.py"
    a.write_text("CREATE TABLE t (x INT)")
    b.write_text("print('x')")
    d = source_digest([a, b])
    assert len(d) == 64 and d == source_digest([b, a])
    b.write_text("print('y')")
    assert source_digest([a, b]) != d
    with pytest.raises(ConfigError, match="source"):
        source_digest([])
    with pytest.raises(ConfigError, match="cannot read"):
        source_digest([tmp_path / "missing.sql"])


def test_the_prior_shape_comes_from_the_previous_proof_or_a_declared_old_shape(tmp_path):
    proof = tmp_path / "rerun_proof.json"
    proof.write_text(json.dumps({"unit": "u", "shape": OLD_SHAPE}))
    assert load_prior(proof) == PRIOR
    shape = tmp_path / "old_shape.json"
    shape.write_text(json.dumps(OLD_SHAPE))
    assert load_prior(shape) == PRIOR
    proof.write_text(json.dumps({"unit": "u"}))
    with pytest.raises(ConfigError, match="tables"):
        load_prior(proof)


# ---- grading ----------------------------------------------------------------------------------

def test_fresh_and_evolved_both_pass_when_the_evolved_run_lands_the_fresh_shape():
    out = _grade(_record("fresh", NEW_SHAPE),
                 _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE, evidence="job-run/2"), prior=PRIOR)
    assert out["fresh"] == "pass" and out["evolved"] == "pass"
    assert out["prior_digest"] == shape_digest(PRIOR)
    assert out["findings"] == [] and out["passed"] is True
    assert out["evidence"] == {"fresh": "job-run/1", "evolved": "job-run/2"}
    assert out["tables"] == ["orders"]
    # the proof carries the shape it observed, so it is the next run's prior
    assert out["shape"] == _check_shape(NEW_SHAPE, "x") and out["shape_digest"] == shape_digest(out["shape"])
    assert out["source_digest"] == DIGEST and out["notes"] == []


def test_create_if_not_exists_on_an_old_shape_table_is_the_canonical_evolved_failure():
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", OLD_SHAPE, pre_shape=OLD_SHAPE), prior=PRIOR)
    assert out["fresh"] == "pass" and out["evolved"] == "fail" and out["passed"] is False
    assert out["findings"] == [{"run": "evolved", "table": "orders", "check": "column_missing",
                                "column": "channel", "detail": "fresh run landed string, absent after the run"}]


def test_the_evolved_run_must_land_exactly_the_fresh_shape():
    drifted = {"tables": {"orders": [
        {"name": "order_id", "type": "bigint", "nullable": True},
        {"name": "amount", "type": "double", "nullable": True},
        {"name": "channel", "type": "string", "nullable": True},
        {"name": "tags", "type": "array<struct<k:string,v:string>>", "nullable": True},
        {"name": "legacy_flag", "type": "string", "nullable": True},
    ]}}
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", drifted, pre_shape=OLD_SHAPE), prior=PRIOR)
    checks = [(f["check"], f["column"]) for f in out["findings"]]
    assert checks == [("nullability_mismatch", "order_id"), ("type_mismatch", "amount"),
                      ("column_extra", "legacy_flag")]
    assert out["evolved"] == "fail"
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", {"tables": {"other": []}}, pre_shape=OLD_SHAPE),
                 prior=PRIOR)
    assert [f["check"] for f in out["findings"]] == ["table_missing", "column_extra"]
    assert out["findings"][0] == {"run": "evolved", "table": "orders", "check": "table_missing", "column": None,
                                  "detail": "not in the observed shape"}
    assert out["findings"][1]["table"] == "other"


def test_a_fresh_run_that_landed_no_table_is_a_failure_not_an_empty_expectation():
    out = _grade(_record("fresh", {"tables": {}}))
    assert out["fresh"] == "fail" and out["passed"] is False
    assert out["findings"] == [{"run": "fresh", "table": None, "check": "no_tables", "column": None,
                                "detail": "the fresh run recorded no table; nothing to prove a rerun against"}]
    assert out["tables"] == [] and "shape" not in out


def test_a_job_the_child_reports_failed_fails_regardless_of_shape():
    out = _grade(_record("fresh", NEW_SHAPE, status="fail"),
                 _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE, status="fail"))
    assert out["fresh"] == "fail" and out["evolved"] == "fail"
    assert [f["check"] for f in out["findings"]] == ["job_failed", "job_failed"]


def test_a_failed_evolved_job_is_fail_even_without_a_usable_pre_shape():
    for rec in (_record("evolved", NEW_SHAPE, status="fail"),
                _record("evolved", NEW_SHAPE, pre_shape=NEW_SHAPE, status="fail")):
        out = _grade(_record("fresh", NEW_SHAPE), rec)
        assert out["evolved"] == "fail" and out["passed"] is False
        assert [f["check"] for f in out["findings"]] == ["job_failed"]
        assert "unsupported_reason" not in out


def test_a_failed_fresh_leg_leaves_the_evolved_leg_unsupported():
    """With no fresh shape there is nothing the evolved run can be held to."""
    out = _grade(_record("fresh", NEW_SHAPE, status="fail"),
                 _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE), prior=PRIOR)
    assert out["fresh"] == "fail" and out["evolved"] == "unsupported" and out["passed"] is False
    assert out["unsupported_reason"] == "the fresh run failed, so there is no shape the evolved run can be held to"


def test_column_order_drift_is_a_finding():
    cols = NEW_SHAPE["tables"]["orders"]
    reordered = {"tables": {"orders": [cols[0], cols[1], cols[3], cols[2]]}}
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", reordered, pre_shape=OLD_SHAPE), prior=PRIOR)
    assert out["fresh"] == "pass" and out["evolved"] == "fail"
    assert out["findings"] == [{"run": "evolved", "table": "orders", "check": "column_order", "column": None,
                                "detail": "fresh run landed order_id, amount, channel, tags; observed "
                                          "order_id, amount, tags, channel"}]


def test_evolved_is_unsupported_never_clean_when_no_prior_shape_was_exercised():
    out = _grade(_record("fresh", NEW_SHAPE))
    assert out["evolved"] == "unsupported" and out["passed"] is True
    assert out["unsupported_reason"] == ("no evolved record: pre-create the table in its previous "
                                         "committed shape (the prior proof's shape) and run again")
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", NEW_SHAPE))
    assert out["evolved"] == "unsupported" and "pre_shape" in out["unsupported_reason"]
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", NEW_SHAPE, pre_shape=NEW_SHAPE), prior=PRIOR)
    assert out["evolved"] == "unsupported"
    assert out["unsupported_reason"] == ("evolved pre_shape equals the fresh shape: nothing evolved, "
                                         "so the run proves only what fresh proved")


def test_evolved_is_unsupported_when_the_pre_shape_lacks_a_fresh_table():
    """A table absent before the run met an empty target, so that leg was another fresh run;
    only a table that existed in an older shape evolves."""
    for pre in ({"tables": {}}, {"tables": {"other": OLD_SHAPE["tables"]["orders"]}}):
        out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", NEW_SHAPE, pre_shape=pre), prior=PRIOR)
        assert out["evolved"] == "unsupported" and out["findings"] == []
        assert out["unsupported_reason"] == ("evolved pre_shape has no orders: the table did not exist "
                                             "before the run, so that leg was a fresh run")


def test_the_evolved_leg_needs_the_prior_committed_shape_and_the_pre_shape_must_equal_it():
    """`pre_shape != fresh` only proves something differed; the leg is evolution only when the
    table started in the previously committed shape, so that shape is an input and pre_shape must
    match it exactly."""
    evolved = _record("evolved", NEW_SHAPE, pre_shape=OLD_SHAPE)
    out = _grade(_record("fresh", NEW_SHAPE), evolved)
    assert out["evolved"] == "unsupported" and out["findings"] == [] and "prior_digest" not in out
    assert out["unsupported_reason"] == ("no prior shape: pass --prior-proof (the previous committed "
                                         "rerun_proof.json) or --prior-shape (the declared old shape) so "
                                         "the evolved leg can be checked against it")
    drifted = {"tables": {"orders": [dict(NEW_SHAPE["tables"]["orders"][0]),
                                     {"name": "amount", "type": "double", "nullable": True},
                                     *NEW_SHAPE["tables"]["orders"][2:]]}}
    out = _grade(_record("fresh", NEW_SHAPE), _record("evolved", NEW_SHAPE, pre_shape=drifted), prior=PRIOR)
    assert out["evolved"] == "unsupported" and out["prior_digest"] == shape_digest(PRIOR)
    assert out["unsupported_reason"].startswith("evolved pre_shape is not the prior committed shape")
    assert "channel" in out["unsupported_reason"] and "amount" in out["unsupported_reason"]
    out = _grade(_record("fresh", NEW_SHAPE), evolved, prior=PRIOR)
    assert out["evolved"] == "pass" and out["prior_digest"] == shape_digest(PRIOR)


def test_a_fresh_record_is_required_and_the_run_label_must_match():
    with pytest.raises(ConfigError, match="fresh run record is required"):
        _grade(None)
    with pytest.raises(ConfigError, match="expected 'evolved'"):
        _grade(_record("fresh", NEW_SHAPE), _record("fresh", NEW_SHAPE))


def test_the_ddl_is_a_hint_that_only_ever_adds_notes():
    """The parser is best effort: a CREATE TABLE it can read whose table the fresh run did not
    record is a note for the reader, and DDL it cannot read is a note too; neither is a finding
    and neither changes a leg's status."""
    assert ddl_tables("-- c\nCREATE TABLE IF NOT EXISTS mig.sales.orders (a INT);\n"
                      "create or replace table `mig`.`sales`.`Runs` as select 1;") == ["mig.sales.orders", "mig.sales.Runs"]
    assert ddl_tables("INSERT INTO t SELECT 1") == []
    out = _grade(_record("fresh", NEW_SHAPE), ddl="CREATE TABLE mig.sales.orders (a INT); CREATE TABLE audit.log (b INT)")
    assert out["fresh"] == "pass" and out["findings"] == []
    assert out["notes"] == ["DDL hint: audit.log is created by the DDL and was not recorded by the fresh run"]
    out = _grade(_record("fresh", NEW_SHAPE), ddl="$$ not ddl $$")
    assert out["notes"] == ["DDL hint: no CREATE TABLE found in the DDL"]


def test_check_proof_rejects_contradictory_or_malformed_artifacts():
    shape = _check_shape(NEW_SHAPE, "x")
    good = {"unit": "u", "fresh": "pass", "evolved": "unsupported", "passed": True, "findings": [],
            "notes": [], "evidence": {"fresh": "job/1"}, "unsupported_reason": "no evolved record",
            "source_digest": DIGEST, "shape": shape, "shape_digest": shape_digest(shape)}
    ran = {**good, "evolved": "pass", "evidence": {"fresh": "j/1", "evolved": "j/2"},
           "prior_digest": shape_digest(PRIOR)}
    del ran["unsupported_reason"]
    assert check_proof(ran, "u", "x", DIGEST) == ran
    assert check_proof(good, "u", "x", DIGEST) == good
    with pytest.raises(ConfigError, match="stale"):
        check_proof(good, "u", "x", "b" * 64)
    bad = [
        {k: v for k, v in good.items() if k != "source_digest"},
        {**good, "source_digest": ""},
        {**good, "shape_digest": shape_digest(PRIOR)},              # shape and its digest disagree
        {k: v for k, v in good.items() if k != "shape"},
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
    # a failed fresh leg has no shape to carry
    fresh_failed = {**good, "fresh": "fail", "passed": False, "shape": None, "shape_digest": None,
                    "findings": [{"run": "fresh", "table": None, "check": "job_failed", "column": None,
                                  "detail": "d"}]}
    assert check_proof(fresh_failed, "u", "x", DIGEST) == fresh_failed


# ---- merge authority --------------------------------------------------------------------------

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
                         source_digest=DIGEST, shape=_check_shape(NEW_SHAPE, "x"),
                         shape_digest=shape_digest(_check_shape(NEW_SHAPE, "x"))), "u", "x", DIGEST)


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


# ---- fixture and CLI --------------------------------------------------------------------------

FIXTURE_ARGS = ["rerun-proof", "--unit", "orders", "--source", str(FIXTURE / "ddl.sql"),
                "--ddl", str(FIXTURE / "ddl.sql"), "--prior-shape", str(FIXTURE / "prior_shape.json"),
                "--fresh", str(FIXTURE / "fresh.json"), "--evolved", str(FIXTURE / "evolved.json")]


def test_fixture_is_the_canonical_failing_case(tmp_path, capsys):
    rc = cli.main(FIXTURE_ARGS + ["--out", str(tmp_path)])
    assert rc == 1
    out = json.loads((tmp_path / "rerun_proof.json").read_text())
    assert out["unit"] == "orders" and out["fresh"] == "pass" and out["evolved"] == "fail"
    assert out["prior_digest"] == shape_digest(load_shape(FIXTURE / "prior_shape.json"))
    assert out["prior_from"] == str(FIXTURE / "prior_shape.json")
    assert out["source_digest"] == source_digest([FIXTURE / "ddl.sql"])
    assert out["sources"] == [str(FIXTURE / "ddl.sql")]
    assert out["findings"][0]["check"] == "column_missing" and out["notes"] == []
    assert json.loads(capsys.readouterr().out)["evolved"] == "fail"
    assert "channel" in (FIXTURE / "ddl.sql").read_text()
    assert "channel" not in json.dumps(json.loads((FIXTURE / "prior_shape.json").read_text()))
    evolved = json.loads((FIXTURE / "evolved.json").read_text())
    assert evolved["pre_shape"] == evolved["shape"]  # IF NOT EXISTS left the old shape in place
    assert (FIXTURE / "ddl.sql").read_text().count("IF NOT EXISTS") == 1


def test_cli_takes_the_prior_from_the_previous_committed_proof(tmp_path):
    """The proof carries the shape it observed; the next run hands it back as --prior-proof, so the
    old shape comes from the last committed artifact, not from parsing DDL history."""
    first = tmp_path / "first"
    rc = cli.main(["rerun-proof", "--unit", "orders", "--source", str(FIXTURE / "ddl.sql"),
                   "--fresh", str(FIXTURE / "prior_fresh.json"), "--out", str(first)])
    assert rc == 0
    proof = json.loads((first / "rerun_proof.json").read_text())
    assert proof["evolved"] == "unsupported" and proof["shape"] == load_shape(FIXTURE / "prior_shape.json")
    rc = cli.main(["rerun-proof", "--unit", "orders", "--source", str(FIXTURE / "ddl.sql"),
                   "--prior-proof", str(first / "rerun_proof.json"),
                   "--fresh", str(FIXTURE / "fresh.json"), "--evolved", str(FIXTURE / "evolved.json"),
                   "--out", str(tmp_path / "second")])
    assert rc == 1
    out = json.loads((tmp_path / "second" / "rerun_proof.json").read_text())
    assert out["evolved"] == "fail" and out["prior_digest"] == proof["shape_digest"]
    assert out["prior_from"] == str(first / "rerun_proof.json")
    with pytest.raises(SystemExit, match="prior-proof or --prior-shape, not both"):
        cli.main(FIXTURE_ARGS + ["--prior-proof", str(first / "rerun_proof.json"), "--out", str(tmp_path)])


def test_cli_reports_unsupported_without_an_evolved_record_and_refuses_a_mislabelled_one(tmp_path):
    with pytest.raises(SystemExit, match="run is 'fresh', expected 'evolved'"):
        cli.main(["rerun-proof", "--unit", "orders", "--source", str(FIXTURE / "ddl.sql"),
                  "--fresh", str(FIXTURE / "fresh.json"), "--evolved", str(FIXTURE / "fresh.json"),
                  "--out", str(tmp_path)])
    rc = cli.main(["rerun-proof", "--unit", "orders", "--source", str(FIXTURE / "ddl.sql"),
                   "--fresh", str(FIXTURE / "fresh.json"), "--out", str(tmp_path)])
    assert rc == 0
    out = json.loads((tmp_path / "rerun_proof.json").read_text())
    assert out["fresh"] == "pass" and out["evolved"] == "unsupported" and out["notes"] == []
    with pytest.raises(SystemExit, match="--source"):
        cli.main(["rerun-proof", "--unit", "orders", "--fresh", str(FIXTURE / "fresh.json"),
                  "--out", str(tmp_path)])


def test_run_reads_a_rerun_proof_file_and_refuses_a_stale_or_malformed_one(tmp_path, monkeypatch):
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
    ddl, job = tmp_path / "ddl.sql", tmp_path / "job.py"
    ddl.write_text((FIXTURE / "ddl.sql").read_text())
    job.write_text("spark.sql(open('ddl.sql').read())\n")
    digest = source_digest([ddl, job])
    shape = _check_shape(NEW_SHAPE, "x")
    proof.write_text(json.dumps({"unit": "u", "fresh": "pass", "evolved": "fail", "passed": False,
                                 "findings": [{"run": "evolved", "table": "t", "check": "job_failed",
                                               "column": None, "detail": "d"}],
                                 "notes": [], "evidence": {"fresh": "j/1", "evolved": "j/2"},
                                 "source_digest": digest, "shape": shape, "shape_digest": shape_digest(shape)}))
    base = ["run", "--unit", "u", "--family", "postgres", "--mapping", "m", "--tolerances", "t",
            "--canonicalization", "c", "--mode", "live", "--source-dsn-secret", "S",
            "--target-secret", "T", "--target-catalog", "mig", "--target-schema", "s",
            "--out", str(tmp_path / "out"), "--rerun-proof", str(proof)]
    args = base + ["--rerun-source", str(ddl), "--rerun-source", str(job)]
    assert cli.main(args) == 0
    assert seen["rerun_proof"]["evolved"] == "fail"
    with pytest.raises(SystemExit, match="rerun-source"):
        cli.main(base)
    # the proof is bound to the job's files: any edit to any of them makes it stale
    job.write_text("spark.sql(open('ddl.sql').read()); spark.sql('ALTER TABLE mig.sales.orders ADD COLUMN r STRING')\n")
    with pytest.raises(SystemExit, match="stale"):
        cli.main(args)
    job.write_text("spark.sql(open('ddl.sql').read())\n")
    with pytest.raises(SystemExit, match="stale"):
        cli.main(base + ["--rerun-source", str(ddl)])  # a subset of the files is another job
    proof.write_text(json.dumps({"fresh": "pass"}))
    with pytest.raises(SystemExit, match="rerun-proof"):
        cli.main(args)
    proof.write_text(json.dumps({"unit": "other", "fresh": "pass", "evolved": "pass", "passed": True,
                                 "findings": [], "notes": [], "evidence": {}, "source_digest": digest,
                                 "shape": shape, "shape_digest": shape_digest(shape)}))
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
    # a view or materialized view with the same columns is not the table the DDL creates
    assert "information_schema.tables" in sql and "table_type IN ('MANAGED', 'EXTERNAL'" in sql
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
    assert "c.relkind IN ('r', 'p')" in sql

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
    """`CREATE TABLE markers ()` is legal on Postgres and the reader records it as `[]`; the
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
    sql = conn.executed[-1][0]
    assert "information_schema.tables" in sql and "table_type IN ('MANAGED', 'EXTERNAL'" in sql


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
    assert "pg_class" in sql and "c.relkind IN ('r', 'p')" in sql and params == ("sales", "markers")
    conn.rows = []
    assert target.table_exists("gone") is False
