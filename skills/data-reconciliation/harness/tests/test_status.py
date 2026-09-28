"""The status split: parity vs merge policy vs blocker class, the rerun posture per unit, and
privilege visibility as its own outcome. The behavioural case the estate gate needs: a
first-run unit whose rows match and whose evolved rerun leg had nothing to evolve from is
merge-eligible, and a unit whose data matched never prints as FAIL."""

import dataclasses
import json
from pathlib import Path

import pytest
from recon import cli
from recon.adapters import DictionaryError, SchemaFacts, _read_grants, dictionary_error
from recon.config import ConfigError, Tolerances, load_mapping_spec
from recon.engine import run_recon
from recon.report import (
    BLOCKER_CLASSES,
    RERUN_POSTURES,
    build_result,
    data_failed,
    render_report,
    render_summary,
    status_line,
)
from recon.tiers import Finding, TierResult

from tests.fakes import PROVEN_RERUN, FakeSource, FakeTarget
from tests.loans import BORROWER_FACTS, LOANS_FACTS, TARGET_LOANS_FACTS, _facts, _rows, _spec

# what a first migration's proof looks like: the fresh leg landed, the evolved leg had no
# previous committed shape to start from
FIRST_RUN_PROOF = {"fresh": "pass", "evolved": "unsupported", "passed": True, "findings": [],
                   "unsupported_kind": "nothing_evolved",
                   "unsupported_reason": "evolved pre_shape equals the fresh shape: nothing evolved"}
FAILED_EVOLVED = {"fresh": "pass", "evolved": "fail", "passed": False,
                  "findings": [{"table": "t", "column": "c", "check": "type"}]}


def _green(**over):
    tiers = [TierResult(1, "row_count", True, 1, [], {}), TierResult(2, "aggregates", True, 1, [], {})]
    kw = dict(unit="u", mode="live", mapping_version="m", tolerance_version="t", tiers=tiers,
              rerun_proof=PROVEN_RERUN)
    kw.update(over)
    return build_result(**kw)


# ------------------------------------------------------------------ rerun posture

def test_required_posture_keeps_the_old_gate():
    for proof, reason in ((None, "rerun_missing"), (FIRST_RUN_PROOF, "rerun_unsupported"),
                          (FAILED_EVOLVED, "rerun_gap")):
        r = _green(rerun_proof=proof)
        assert r["parity"] == "PASS" and r["verdict"] == "PASS"
        assert r["merge_eligible"] is False and r["merge_policy"] == "blocked"
        assert r["merge_block_reasons"] == [reason]
        assert r["blockers"] == [{"reason": reason, "class": "rerun_policy"}]
        assert r["blocker_classes"] == ["rerun_policy"]
    assert _green()["merge_eligible"] is True


def test_first_run_baseline_accepts_an_evolved_leg_with_nothing_to_evolve_from():
    r = _green(rerun_proof=FIRST_RUN_PROOF, rerun_posture="first_run_baseline")
    assert r["merge_eligible"] is True and r["merge_policy"] == "eligible"
    assert r["blockers"] == [] and r["rerun_posture"] == "first_run_baseline"
    assert r["rerun_proof"] is FIRST_RUN_PROOF  # the evidence is kept as recorded


def test_first_run_baseline_still_needs_a_fresh_leg_and_refuses_a_failed_one():
    assert _green(rerun_proof=None, rerun_posture="first_run_baseline")["merge_block_reasons"] == ["rerun_missing"]
    assert _green(rerun_proof=FAILED_EVOLVED, rerun_posture="first_run_baseline")["merge_block_reasons"] == ["rerun_gap"]
    fresh_failed = {"fresh": "fail", "evolved": "unsupported", "passed": False, "findings": [],
                    "unsupported_kind": "fresh_failed"}
    assert _green(rerun_proof=fresh_failed, rerun_posture="first_run_baseline")["merge_block_reasons"] == [
        "rerun_gap", "rerun_unsupported"]


def test_first_run_baseline_accepts_only_the_leg_that_ran_against_the_fresh_shape():
    for kind in ("no_evolved_record", "no_pre_shape", "pre_shape_missing_table", "no_prior_shape",
                 "pre_shape_not_prior", None):
        proof = {**FIRST_RUN_PROOF, "unsupported_kind": kind, "unsupported_reason": "x"}
        if kind is None:
            del proof["unsupported_kind"]  # a proof from a harness that did not record the kind
        r = _green(rerun_proof=proof, rerun_posture="first_run_baseline")
        assert r["merge_eligible"] is False, kind
        assert r["blockers"] == [{"reason": "rerun_unsupported", "class": "rerun_policy"}], kind


def test_an_unstable_window_is_an_evidence_failure_not_a_row_mismatch():
    tiers = [TierResult(0, "consistency_window", False, 1,
                        [Finding("dbo.loans", "window_unstable", "source count moved")], {}),
             TierResult(1, "row_count", True, 1, [], {})]
    r = _green(tiers=tiers, mode="transactional")
    assert r["verdict"] == "FAIL" and r["parity"] == "PASS"
    assert r["blockers"] == [{"reason": "tier_failed", "class": "evidence"}]
    assert status_line(r) == "parity PASS, merge blocked (evidence)"
    rows_moved = [tiers[0], TierResult(1, "row_count", False, 1, [Finding("dbo.loans", "count", "1 != 2")], {})]
    assert _green(tiers=rows_moved, mode="transactional")["blockers"] == [{"reason": "tier_failed", "class": "data"}]
    window_only = _green(tiers=[tiers[0]], mode="transactional")
    assert window_only["parity"] == "NOT_RUN" and window_only["blockers"][0]["class"] == "evidence"


def test_not_applicable_posture_needs_no_proof_but_refuses_a_failed_one():
    assert _green(rerun_proof=None, rerun_posture="not_applicable")["merge_eligible"] is True
    assert _green(rerun_proof=FIRST_RUN_PROOF, rerun_posture="not_applicable")["merge_eligible"] is True
    r = _green(rerun_proof=FAILED_EVOLVED, rerun_posture="not_applicable")
    assert r["merge_block_reasons"] == ["rerun_gap"] and r["blocker_classes"] == ["rerun_policy"]


def test_posture_never_lifts_a_non_rerun_blocker():
    tiers = [TierResult(1, "row_count", False, 1, [Finding("row_count", "n", "12", "11")], {})]
    for posture in RERUN_POSTURES:
        r = _green(tiers=tiers, rerun_proof=FIRST_RUN_PROOF, rerun_posture=posture)
        assert r["parity"] == "FAIL" and r["verdict"] == "FAIL" and r["merge_eligible"] is False
        assert {"reason": "tier_failed", "class": "data"} in r["blockers"]
    assert _green(mode="fixture", rerun_posture="not_applicable")["blockers"] == [
        {"reason": "mode", "class": "evidence"}]


def test_unknown_posture_is_a_config_error():
    with pytest.raises(ConfigError, match="rerun_posture"):
        _green(rerun_posture="whenever")


# ------------------------------------------------------------------ blocker classes

def _structural(findings=(), stats=None):
    return TierResult(0, "structural_parity", not findings, 3, list(findings), stats or {})


def test_structural_finding_is_structural_and_parity_still_passes():
    t0 = _structural([Finding("triggers", "loans", "trg_audit", "-")])
    r = _green(tiers=[t0, TierResult(1, "row_count", True, 1, [], {})])
    assert r["parity"] == "PASS" and r["verdict"] == "FAIL"
    assert r["blockers"] == [{"reason": "structural_gap", "class": "structural"},
                             {"reason": "tier_failed", "class": "structural"}]
    assert "data" not in r["blocker_classes"]


def test_privilege_hole_without_findings_is_privilege_visibility():
    t0 = _structural(stats={"hole_kinds": ["privilege"],
                            "dictionary_unavailable": ["loans: information_schema.table_privileges read failed (X)"],
                            "structural_checks": {"grants": "unsupported"}})
    r = _green(tiers=[t0, TierResult(1, "row_count", True, 1, [], {})])
    assert r["parity"] == "PASS" and r["verdict"] == "PASS" and r["merge_eligible"] is False
    assert r["blocker_classes"] == ["privilege_visibility"]
    assert [b["reason"] for b in r["blockers"]] == ["structural_gap", "warnings"]


def test_privilege_hole_next_to_a_finding_or_a_read_hole_stays_structural():
    with_finding = _structural([Finding("grants", "loans", "app_ro: select", "-")],
                               {"hole_kinds": ["privilege"], "unverified": ["loans: x"]})
    assert _green(tiers=[with_finding])["blocker_classes"] == ["structural"]
    mixed = _structural(stats={"hole_kinds": ["privilege", "read"], "unverified": ["loans: x"]})
    assert _green(tiers=[mixed])["blocker_classes"] == ["structural"]


def test_every_blocker_class_is_declared():
    r = _green(mode="snapshot", snapshot=None, rerun_proof=None,
               tiers=[_structural([Finding("grants", "loans", "a", "-")]),
                      TierResult(1, "row_count", False, 1, [Finding("row_count", "n", "1", "2")], {})])
    assert set(r["blocker_classes"]) <= set(BLOCKER_CLASSES)
    assert r["blocker_classes"] == ["data", "evidence", "rerun_policy", "structural"]


# ------------------------------------------------------------------ wording

def test_status_line_never_reads_as_a_data_failure_when_rows_matched():
    r = _green(rerun_proof=FIRST_RUN_PROOF)
    assert status_line(r) == "parity PASS, merge blocked (rerun_policy)"
    assert status_line(_green()) == "parity PASS, merge eligible"
    for text in (render_report(r), render_summary(r)):
        assert "parity PASS, merge blocked (rerun_policy)" in text
        assert "**FAIL**" not in text
        assert "Blockers: rerun_policy: rerun_unsupported" in text
        assert "Rerun posture: `required`" in text
    failed = _green(tiers=[TierResult(1, "row_count", False, 1, [Finding("row_count", "n", "1", "2")], {})])
    assert status_line(failed) == "parity FAIL, merge blocked (data)"
    assert status_line({"verdict": "PASS", "merge_eligible": True}) == "parity PASS, merge eligible"


# ------------------------------------------------------------------ privilege classification

def test_dictionary_error_kind_from_driver_text_which_is_then_dropped():
    class InsufficientPrivilege(Exception):
        pass
    denied = dictionary_error("t", "all_triggers",
                              RuntimeError("ORA-01031: insufficient privileges at dsn=user/pw@host"))
    assert denied.kind == "privilege" and str(denied) == "t: all_triggers read failed (RuntimeError)"
    assert dictionary_error("t", "v", InsufficientPrivilege("42501")).kind == "privilege"
    assert dictionary_error("t", "v", RuntimeError("permission denied for table x")).kind == "privilege"
    other = dictionary_error("t", "v", TimeoutError("connection reset at host:5432"))
    assert other.kind == "read" and "host" not in str(other)
    assert DictionaryError("t: v read failed (X)").kind == "read"


def test_read_grants_turns_a_refusal_into_a_denied_category_and_propagates_the_rest():
    facts = SchemaFacts()
    def refused():
        raise DictionaryError("t: information_schema.table_privileges read failed (X)", "privilege")
    _read_grants(facts, refused)
    assert facts.grants == {} and facts.unsupported == {"grants"} and facts.privilege_denied == {"grants"}
    def broken():
        raise DictionaryError("t: information_schema.table_privileges read failed (Timeout)")
    with pytest.raises(DictionaryError):
        _read_grants(SchemaFacts(), broken)
    ok = SchemaFacts()
    _read_grants(ok, lambda: [("APP_RO", "SELECT"), ("app_ro", "INSERT")])
    assert ok.grants == {"app_ro": frozenset({"select", "insert"})} and not ok.unsupported


# ------------------------------------------------------------------ end to end

def _first_run(target_loans=None, rerun_posture="required", **kw):
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": _facts(LOANS_FACTS, grants={"app_ro": frozenset({"select"})}),
                                "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                        schema={"loans": target_loans or _facts(TARGET_LOANS_FACTS,
                                                                grants={"app_ro": frozenset({"select"})}),
                                "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    spec = dataclasses.replace(_spec(), rerun_posture=rerun_posture)
    return run_recon("u1", "live", spec, Tolerances("t1"), [], source, target, **kw)


def test_first_run_estate_reaches_merge_eligibility_end_to_end():
    blocked = _first_run(rerun_proof=FIRST_RUN_PROOF)
    assert blocked["parity"] == "PASS" and blocked["merge_block_reasons"] == ["rerun_unsupported"]
    eligible = _first_run(rerun_proof=FIRST_RUN_PROOF, rerun_posture="first_run_baseline")
    assert eligible["verdict"] == "PASS" and eligible["parity"] == "PASS"
    assert eligible["merge_eligible"] is True and eligible["blockers"] == []
    assert eligible["merge_authority"] == {"kind": "harness", "decision_id": None}


def test_denied_grants_are_a_visibility_blocker_no_caller_can_lift():
    """A refused dictionary read is classed `privilege_visibility`, never `structural` or `data`, and
    it blocks merge on its own: the harness takes no declaration that would mask it, so the only way
    past it is a human merge_override scoped to that class in the ledger."""
    denied = _facts(TARGET_LOANS_FACTS, unsupported=frozenset({"grants"}), privilege_denied=frozenset({"grants"}))
    r = _first_run(target_loans=denied, rerun_proof=PROVEN_RERUN)
    t0 = r["tiers"][0]
    assert t0["passed"] and t0["stats"]["hole_kinds"] == ["privilege"]
    assert t0["stats"]["structural_checks"]["grants"] == "unsupported"
    assert r["parity"] == "PASS" and r["merge_eligible"] is False
    assert r["blocker_classes"] == ["privilege_visibility"]
    assert any("cannot expose grants" in w for w in r["warnings"])
    assert status_line(r) == "parity PASS, merge blocked (privilege_visibility)"
    with pytest.raises(TypeError):
        _first_run(target_loans=denied, rerun_proof=PROVEN_RERUN, structural_blind=["grants"])
    with pytest.raises(SystemExit):
        cli.main(["run", "--unit", "u", "--family", "oracle", "--mode", "live", "--mapping", "m",
                  "--tolerances", "t", "--canonicalization", "c", "--structural-blind", "grants"])


def test_a_refusal_beside_a_real_mismatch_is_structural_and_the_mismatch_is_kept():
    wrong_trigger = _facts(TARGET_LOANS_FACTS, triggers={"trg_extra": ("after", ("insert",), "row")},
                           unsupported=frozenset({"grants"}), privilege_denied=frozenset({"grants"}))
    r = _first_run(target_loans=wrong_trigger, rerun_proof=PROVEN_RERUN)
    assert r["verdict"] == "FAIL" and r["parity"] == "PASS"
    assert r["blocker_classes"] == ["structural"]
    assert any(f["check"] == "trigger_extra" for f in r["tiers"][0]["findings"])


def test_cdc_lag_is_evidence_not_parity_but_a_misordered_row_is_data():
    """Tier 6 fails for the feed being behind and for a row applied out of order alike; only the second
    says the rows the target holds differ from the source."""
    lag = TierResult(6, "cdc_lag_ordering", False, 1,
                     [Finding("t", "cdc_lag_exceeded", "lag 90.000s > cdc_lag_max_s=60.0s (12 source rows in flight)")], {})
    r = _green(mode="transactional", tiers=[TierResult(1, "row_count", True, 1, [], {}), lag])
    assert r["verdict"] == "FAIL" and r["parity"] == "PASS"
    assert r["blockers"] == [{"reason": "tier_failed", "class": "evidence"}]
    for check in ("row_ahead_of_source", "row_behind_applied_watermark", "target_ahead_of_source"):
        order = TierResult(6, "cdc_lag_ordering", False, 1, [lag.findings[0], Finding("t", check, "1 row")], {})
        r = _green(mode="transactional", tiers=[TierResult(1, "row_count", True, 1, [], {}), order])
        assert r["parity"] == "FAIL" and r["blockers"] == [{"reason": "tier_failed", "class": "data"}], check
    assert data_failed(TierResult(6, "cdc_lag_ordering", True, 1, [], {})) is False


def test_rerun_posture_is_declared_in_the_committed_mapping_spec_never_by_the_run(tmp_path):
    """A flag would let the run being graded pick its own gate; the spec is versioned, committed and
    reviewed, and result.json cites its version."""
    spec = {"version": "m1", "objects": [{"object": "t", "root_table": "s.t",
                                          "key": {"source": ["id"], "target": "id"}}]}
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps(spec))
    assert load_mapping_spec(path, {}).rerun_posture == "required"
    path.write_text(json.dumps({**spec, "rerun_posture": "not_applicable"}))
    assert load_mapping_spec(path, {}).rerun_posture == "not_applicable"
    path.write_text(json.dumps({**spec, "rerun_posture": "whenever"}))
    with pytest.raises(ConfigError, match="rerun_posture"):
        load_mapping_spec(path, {})
    assert "--rerun-posture" not in (Path(cli.__file__).read_text())


def test_cli_passes_posture_through(tmp_path, monkeypatch, capsys):
    from recon import adapters
    from tests.test_tiers import GRADED_SPEC, RULES, TOL, make_graded
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig"]}))
    source, target = make_graded()
    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "oracle", lambda secret: source)
    monkeypatch.setattr(adapters, "DatabricksTargetAdapter", lambda *a: target)
    specs = {"m": dataclasses.replace(GRADED_SPEC, rerun_posture="not_applicable"), "m-required": GRADED_SPEC}
    monkeypatch.setattr(cli, "load_mapping_spec", lambda path, params: specs[str(path)])
    monkeypatch.setattr(cli, "load_tolerances", lambda path: TOL)
    monkeypatch.setattr(cli, "load_canon_rules", lambda path: RULES)
    argv = ["run", "--unit", "u", "--family", "oracle", "--mode", "live", "--depth", "full",
            "--mapping", "m", "--tolerances", "t", "--canonicalization", "c",
            "--source-dsn-secret", "SOURCE", "--target-secret", "TARGET",
            "--target-catalog", "mig", "--target-schema", "s", "--out", str(tmp_path / "out")]
    # no committed dependency analysis in this repo: the one blocker left is evidence, not rerun
    assert cli.main(argv) == 0
    assert "dbx-recon PASS: parity PASS, merge blocked (evidence)" in capsys.readouterr().out
    result = json.loads((tmp_path / "out" / "result.json").read_text())
    assert result["rerun_posture"] == "not_applicable"
    assert result["blockers"] == [{"reason": "routine_parity_missing", "class": "evidence"}]
    assert "structural_blind" not in result["tiers"][0]["stats"]
    argv[argv.index("m")] = "m-required"
    assert cli.main(argv) == 0
    assert "parity PASS, merge blocked (evidence, rerun_policy)" in capsys.readouterr().out
    result = json.loads((tmp_path / "out" / "result.json").read_text())
    assert result["rerun_posture"] == "required" and "rerun_missing" in result["merge_block_reasons"]
    for flag in (["--structural-blind", "grants"], ["--rerun-posture", "not_applicable"]):
        with pytest.raises(SystemExit):
            cli.main(argv + flag)


def test_a_refused_grant_read_releases_its_savepoint_either_way():
    """ROLLBACK TO leaves the savepoint defined; without RELEASE one accumulates per refused table
    until the transaction ends, so both paths release it."""
    from recon.adapters import LakebaseTargetAdapter
    inst = LakebaseTargetAdapter.__new__(LakebaseTargetAdapter)
    calls = []
    inst._execute = lambda sql, params=(): calls.append(sql)
    assert inst._in_savepoint(lambda: [1]) == [1]
    with pytest.raises(DictionaryError):
        inst._in_savepoint(lambda: (_ for _ in ()).throw(DictionaryError("refused", "privilege")))
    assert calls == ["SAVEPOINT recon_dictionary", "RELEASE SAVEPOINT recon_dictionary",
                     "SAVEPOINT recon_dictionary", "ROLLBACK TO SAVEPOINT recon_dictionary",
                     "RELEASE SAVEPOINT recon_dictionary"]
