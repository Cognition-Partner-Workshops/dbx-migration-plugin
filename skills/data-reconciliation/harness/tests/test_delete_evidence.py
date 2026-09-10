"""Tombstones: a target-only key is an in-flight delete only when the source's change stream
proves it was deleted after the target's applied position and inside cdc_lag_max_s; every
other target-only key stays `pk_extra_on_target` exactly as without evidence."""

import dataclasses
import json

import pytest
from recon.adapters import DeleteEvent
from recon.config import ConfigError, DeleteEvidenceSpec, Tolerances, load_mapping_spec
from recon.cost import estimate_cost

from tests.fakes import FakeCdcSource, FakeCheckpointTarget, FakeSource, FakeTarget
from tests.test_transactional import (
    BORROWER_FACTS,
    LOANS_FACTS,
    TARGET_LOANS_FACTS,
    _codes,
    _loan,
    _rows,
    _run,
    _spec,
    _tier,
    _ts,
)

EVIDENCE = DeleteEvidenceSpec(kind="sqlserver_cdc", capture="raw_loans", applied_table="cdc_checkpoint",
                              applied_column="applied_lsn", applied_where="source_table = 'dbo.loans'")
CHECKPOINT = ("cdc_checkpoint", "applied_lsn", "source_table = 'dbo.loans'")
TOL = Tolerances("t1", cdc_lag_max_s=60)


class CdcTarget(FakeCheckpointTarget, FakeTarget):
    def __init__(self, objects, positions, **kw):
        super().__init__(objects, **kw)
        self.positions = positions


def _spec_with_evidence(evidence=EVIDENCE):
    spec = _spec()
    loans = dataclasses.replace(spec.objects[0], delete_evidence=evidence)
    return dataclasses.replace(spec, objects=[loans, spec.objects[1]])


def _sides(loans_src, loans_tgt, borrowers, tombstones, applied, horizons=None, source_cls=FakeCdcSource):
    seq = max(r["loan_id"] for r in loans_tgt) + 1
    kw = {"tombstones": tombstones, "horizons": horizons} if source_cls is FakeCdcSource else {}
    source = source_cls({"dbo.loans": loans_src, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): seq}, **kw)
    target = CdcTarget({"loans": loans_tgt, "borrowers": [dict(b) for b in borrowers]},
                       {CHECKPOINT: applied},
                       schema={"loans": TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                       sequences={("loans", "loan_id"): seq})
    return source, target


def _deleted(loans, *ids):
    """Source rows minus the deleted ids; the target still holds every row."""
    tgt = [dict(r) for r in loans]
    return [r for r in loans if r["loan_id"] not in ids], tgt


def _ev(key, position, age_s):
    return DeleteEvent((key,), position, age_s)


def test_a_delete_after_the_applied_position_inside_the_lag_is_in_flight_not_a_defect():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    assert result["merge_eligible"] is True
    counts = _tier(result, "counts_through_mapping")
    assert counts["stats"]["count_gap_within_in_flight"]["loans"] == {
        "gap": -1, "in_flight": 0, "in_flight_deletes": 1}
    pk = _tier(result, "pk_set_diff")
    assert pk["findings"] == []
    assert pk["stats"]["loans"]["in_flight_deletes"] == 1 and pk["stats"]["loans"]["extra_on_target"] == 0
    assert pk["stats"]["loans"]["delete_evidence"]["status"] == "ok"
    assert pk["stats"]["loans"]["delete_evidence"]["applied_position"] == 10
    assert _tier(result, "cdc_lag_ordering")["stats"]["loans"]["in_flight_deletes"] == 1
    assert _tier(result, "keyed_diffs")["stats"]["loans"]["in_flight_deletes"] == 1
    assert source.last_deletes_since == {"capture": "raw_loans", "key_cols": ["loan_id"], "after": 10, "upto": 15,
                                         "where": None}
    # the evidence costs one target and two source statements per object, plus one source read of
    # the tombstoned keys once there are any, and is its own line
    assert result["cost"]["delete_evidence_statements"] == {"source": 3, "target": 1}
    assert source.calls["deletes_since"] == 1 and source.calls["evidence_horizon"] == 1
    assert target.calls["applied_position"] == 1


def test_in_flight_delete_keys_are_excluded_from_the_target_aggregates():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    agg = _tier(result, "per_field_aggregates")
    assert agg["passed"] is True
    assert agg["stats"]["applied_subset"]["loans"] == {"in_flight": 0, "in_flight_deletes": 1, "excluded_keys": 1}
    assert target.last_excluded_keys == [(3,)]


def test_a_stray_target_row_stays_a_finding_when_the_stream_never_deleted_it():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    tgt.append(_loan(500, changed=0, borrower_id=1))
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert _codes(result, "counts_through_mapping") == ["root_count"]
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(500,)" in pk["findings"][0]["detail"] and "(3,)" not in pk["findings"][0]["detail"]
    assert "not deleted on the source" in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["in_flight_deletes"] == 1 and pk["stats"]["loans"]["extra_on_target"] == 1
    assert _codes(result, "cdc_lag_ordering") == []


def test_a_delete_older_than_the_lag_tolerance_is_a_lagging_delete():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3, 5)
    events = [_ev(3, 12, 400.0), _ev(5, 15, 5.0)]
    source, target = _sides(src, tgt, borrowers, {"raw_loans": events}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(3,)" in pk["findings"][0]["detail"] and "(5,)" not in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["delete_evidence"]["aged_deletes"] == 1
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["delete_lag_exceeded"]
    assert "400s" in cdc["findings"][0]["detail"] and "(3,)" in cdc["findings"][0]["detail"]
    # counts: the target is two rows long but only one delete is in flight
    assert _codes(result, "counts_through_mapping") == ["root_count"]
    assert "1 deletes in flight" in _tier(result, "counts_through_mapping")["findings"][0]["detail"]


def test_an_aged_delete_the_target_already_applied_is_not_a_finding():
    loans, borrowers = _rows(12)
    src = [r for r in loans if r["loan_id"] != 3]
    tgt = [dict(r) for r in src]
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 12, 400.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS"
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]["aged_deletes"] == 1


def test_an_in_flight_delete_of_the_newest_row_does_not_put_the_target_ahead_of_the_source():
    # loan 12 carries the source's max watermark; deleting it leaves the target's max newer than
    # the source's, which is the delete in flight, not a replayed or misordered apply
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 12)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(12, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    cdc = _tier(result, "cdc_lag_ordering")
    assert cdc["findings"] == []
    assert cdc["stats"]["loans"]["in_flight_deletes"] == 1
    assert cdc["stats"]["loans"]["target_max_from_in_flight_delete"] is True
    # lag is measured from the newest target row that is not a delete in flight (loan 11)
    assert cdc["stats"]["loans"]["target_max_excluding_deletes"] == _ts(11)
    assert cdc["stats"]["loans"]["lag_s"] == 0.0
    assert target.calls["table_aggregates_excluding"] == 2  # tier 2's, plus this one read


def test_lag_is_still_graded_when_a_delete_in_flight_carries_the_target_max():
    # loan 12 (target max) is deleted in flight; loan 13 was inserted 1000s later and is still
    # unapplied: tier 5 rightly calls it in flight, tier 6 must still see the 989s of lag
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 12)
    src.append(_loan(13, changed=1000, borrower_id=1))
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(12, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_missing"] == 1
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["cdc_lag_exceeded"]
    assert cdc["stats"]["loans"]["target_max_from_in_flight_delete"] is True
    assert cdc["stats"]["loans"]["lag_s"] == 989.0


def test_lag_is_ungraded_not_skipped_when_every_target_row_is_a_delete_in_flight():
    loans, borrowers = _rows(3)
    tgt = [dict(r) for r in loans]
    src = [_loan(13, changed=1000, borrower_id=1)]
    events = [_ev(i, 15, 5.0) for i in (1, 2, 3)]
    source, target = _sides(src, tgt, borrowers, {"raw_loans": events}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["cdc_lag_ungraded"]
    assert "no applied watermark survives" in cdc["findings"][0]["detail"]
    assert cdc["stats"]["loans"]["target_max_excluding_deletes"] is None
    assert cdc["stats"]["loans"]["lag_s"] is None


def test_a_target_row_newer_than_every_source_row_still_reads_ahead_when_no_delete_explains_it():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    tgt.append(_loan(500, changed=99, borrower_id=1))
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["target_ahead_of_source"]
    assert cdc["stats"]["loans"]["target_max_from_in_flight_delete"] is False


def test_an_applied_position_older_than_the_retained_horizon_is_a_retention_gap():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 25, 5.0)]}, applied=10,
                            horizons={"raw_loans": (20, 25)})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert _codes(result, "counts_through_mapping") == ["root_count"]
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(3,)" in pk["findings"][0]["detail"] and "retention_gap" in pk["findings"][0]["detail"]
    assert _codes(result, "cdc_lag_ordering") == ["delete_evidence_retention_gap"]
    assert source.calls["deletes_since"] == 0  # nothing to read: the gap is decided by the horizon


def test_an_applied_position_right_before_the_retained_horizon_is_not_a_gap():
    # the delete read starts at the position after the checkpoint; when that is the oldest
    # retained change nothing between them can have been cleaned up, so the evidence is whole
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 25, 5.0)]}, applied=19,
                            horizons={"raw_loans": (20, 25)})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["delete_evidence"]["status"] == "ok"
    assert pk["stats"]["loans"]["in_flight_deletes"] == 1 and pk["findings"] == []
    assert source.last_deletes_since["after"] == 19 and source.last_deletes_since["upto"] == 25
    # one position further back and a change may have been retained and cleaned up in between
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 25, 5.0)]}, applied=18,
                            horizons={"raw_loans": (20, 25)})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert _codes(result, "cdc_lag_ordering") == ["delete_evidence_retention_gap"]
    assert source.calls["deletes_since"] == 0


def test_a_binary_position_succeeds_by_one_with_carry_like_fn_cdc_increment_lsn():
    # LSNs are binary(10) integers: sys.fn_cdc_increment_lsn(0x..00FF) is 0x..0100, so a
    # checkpoint at ..00FF against a horizon starting at ..0100 is whole, ..00FE is a gap
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)

    def lsn(tail):
        return bytes.fromhex("0000003000004d58" + tail)

    horizon = (lsn("0100"), lsn("0200"))
    for applied, status in ((lsn("00ff"), "ok"), (lsn("00fe"), "retention_gap")):
        source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, lsn("0150"), 5.0)]},
                                applied=applied, horizons={"raw_loans": horizon})
        result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
        assert _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]["status"] == status
        assert (result["verdict"] == "PASS") is (status == "ok")


def test_an_applied_position_past_the_retained_horizon_is_ahead_of_horizon_and_strict():
    # a feed cannot have applied a position the source never produced: wrong capture, wrong
    # database or a corrupt checkpoint; a clean-looking target must not merge on that evidence
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    source, target = _sides(loans, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=30,
                            horizons={"raw_loans": (5, 20)})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["delete_evidence_unusable"]
    assert "ahead_of_horizon" in cdc["findings"][0]["detail"] and "30" in cdc["findings"][0]["detail"]
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]["status"] == "ahead_of_horizon"
    assert source.calls["deletes_since"] == 0
    # and with a target-only key the strict grading applies
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=30,
                            horizons={"raw_loans": (5, 20)})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert [f["check"] for f in _tier(result, "pk_set_diff")["findings"]] == ["pk_extra_on_target"]


def test_an_applied_position_equal_to_the_horizon_is_fully_applied():
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    source, target = _sides(loans, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=20,
                            horizons={"raw_loans": (5, 20)})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS" and result["merge_eligible"] is True
    assert _tier(result, "cdc_lag_ordering")["findings"] == []
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]["status"] == "ok"
    assert source.calls["deletes_since"] == 0


def test_a_capture_that_retains_nothing_is_unavailable_and_strict():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert [f["check"] for f in _tier(result, "pk_set_diff")["findings"]] == ["pk_extra_on_target"]
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["delete_evidence_unusable"]
    assert "unavailable" in cdc["findings"][0]["detail"]


def test_a_target_with_no_applied_position_yet_is_strict():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=None)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert [f["check"] for f in _tier(result, "pk_set_diff")["findings"]] == ["pk_extra_on_target"]
    assert _codes(result, "cdc_lag_ordering") == ["delete_evidence_unusable"]
    assert source.calls["evidence_horizon"] == 0 and source.calls["deletes_since"] == 0


def test_sides_without_the_evidence_protocols_keep_the_drain_before_run_contract():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, None, applied=10, source_cls=FakeSource)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "undrained deletes" in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["delete_evidence"]["status"] == "unsupported"
    assert _codes(result, "cdc_lag_ordering") == ["delete_evidence_unusable"]
    assert target.calls["applied_position"] == 0


def _scoped_spec():
    """The loans object reconciled for borrower 1 only, on both sides."""
    spec = _spec_with_evidence()
    loans = dataclasses.replace(spec.objects[0], root_where="borrower_id = 1", target_where="borrower_id = 1")
    return dataclasses.replace(spec, objects=[loans, spec.objects[1]])


def test_a_scoped_run_reads_tombstones_under_the_same_scope_predicate():
    # loans 3, 6, 9, 12 belong to borrower 1; loan 3 was deleted inside the scope
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    source.images = {"raw_loans": {(3,): {"loan_id": 3, "borrower_id": 1}}}
    result = _run(source, target, spec=_scoped_spec(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_deletes"] == 1
    assert source.last_deletes_since == {"capture": "raw_loans", "key_cols": ["loan_id"], "after": 10,
                                         "upto": 15, "where": "borrower_id = 1"}


def test_a_delete_outside_the_scope_never_vouches_for_a_stray_row_inside_it():
    # borrower 2 deleted loan 7; the target holds a stray loan 7 filed under borrower 1. The
    # tombstone's before-image is borrower 2's, so it is not evidence for the scoped run.
    loans, borrowers = _rows(12)
    src = [r for r in loans if r["loan_id"] != 7]
    tgt = [dict(r, borrower_id=1) if r["loan_id"] == 7 else dict(r) for r in loans]
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(7, 15, 5.0)]}, applied=10)
    source.images = {"raw_loans": {(7,): {"loan_id": 7, "borrower_id": 2}}}
    result = _run(source, target, spec=_scoped_spec(), tol=TOL)
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(7,)" in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["in_flight_deletes"] == 0
    assert pk["stats"]["loans"]["delete_evidence"]["events"] == 0
    assert _codes(result, "counts_through_mapping") == ["root_count"]


def test_a_tombstone_whose_image_cannot_answer_the_scope_is_not_evidence():
    # a capture that did not keep the scope column: the fake's default image is the key alone
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_scoped_spec(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert [f["check"] for f in _tier(result, "pk_set_diff")["findings"]] == ["pk_extra_on_target"]


def test_an_undeclared_object_reports_no_evidence_and_no_tier6_check():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec(), tol=TOL)
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["delete_evidence"]["status"] == "absent"
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert _codes(result, "cdc_lag_ordering") == []
    assert source.calls["evidence_horizon"] == 0 and target.calls["applied_position"] == 0


def test_the_latest_delete_of_a_key_decides_and_events_at_or_before_the_applied_position_are_ignored():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3, 4)
    events = [_ev(3, 12, 400.0), _ev(3, 15, 5.0),   # deleted, reinserted, deleted again: fresh wins
              _ev(4, 10, 3.0), _ev(4, 8, 2.0)]      # over-delivered: at/before the applied position
    source, target = _sides(src, tgt, borrowers, {"raw_loans": events}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["in_flight_deletes"] == 1
    assert pk["stats"]["loans"]["delete_evidence"]["events"] == 2
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(4,)" in pk["findings"][0]["detail"] and "(3,)" not in pk["findings"][0]["detail"]


def test_a_deleted_then_reinserted_source_key_is_never_a_target_only_candidate():
    # loan 3 was deleted and reinserted after the target's applied position: the source holds
    # it again (newer than the target's copy), so it is an in-flight update, not a target-only key
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    loans[2]["modified_date"] = _ts(30)
    source, target = _sides(loans, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["in_flight_deletes"] == 0 and pk["stats"]["loans"]["in_flight_updates"] == 1
    # the tombstone was read, but the key is on the source again: not a target-only candidate
    assert pk["stats"]["loans"]["delete_evidence"]["in_flight_deletes"] == 0
    assert pk["stats"]["loans"]["delete_evidence"]["reinserted"] == 1
    assert _tier(result, "counts_through_mapping")["stats"].get("count_gap_within_in_flight") is None
    # the key is excluded from the target aggregates once (as an in-flight update), not twice
    assert target.last_excluded_keys == [(3,)]
    assert _tier(result, "per_field_aggregates")["stats"]["applied_subset"]["loans"] == {
        "in_flight": 1, "excluded_keys": 1}
    # the reinsert check is one source read of the tombstoned keys, inside the window
    assert result["cost"]["delete_evidence_statements"] == {"source": 3, "target": 1}


class _LoanReads(FakeCdcSource):
    """Records every keyed read of dbo.loans: the tombstone lookup, then tier 3's sample."""

    def fetch_keyed(self, table, key_cols, columns, where=None, keys=None):
        if table == "dbo.loans" and keys is not None:
            self.loan_reads.append([tuple(k) for k in keys])
        yield from super().fetch_keyed(table, key_cols, columns, where, keys)


def test_a_key_restored_with_its_old_watermark_is_graded_as_an_applied_row_on_both_sides():
    # loan 7 was deleted after the applied position and restored as it was (same modified_date),
    # so the target's copy is exactly what the source holds: an applied row, not in flight on
    # either side. Excluding it from the target aggregates alone would compare unequal sets.
    loans, borrowers = _rows(40)
    tgt = [dict(r) for r in loans]
    source, target = _sides(loans, tgt, borrowers, {"raw_loans": [_ev(7, 15, 5.0)]}, applied=10)
    source.__class__ = _LoanReads
    tol = Tolerances("t1", cdc_lag_max_s=60, sample_size=2)

    def run():
        source.loan_reads = []
        result = _run(source, target, spec=_spec_with_evidence(), tol=tol, depth="sampled", seed=3)
        lookup, sample = source.loan_reads
        assert lookup == [(7,)] and source.last_fetch_keyed["where"] is None
        assert (7,) not in sample, "pick a seed whose sample misses key 7"
        return result

    result = run()
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    agg = _tier(result, "per_field_aggregates")
    assert agg["stats"].get("applied_subset", {}).get("loans") is None
    assert target.calls["table_aggregates_excluding"] == 0
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["delete_evidence"]["in_flight_deletes"] == 0
    assert pk["stats"]["loans"]["delete_evidence"]["reinserted"] == 1
    assert _tier(result, "counts_through_mapping")["stats"].get("count_gap_within_in_flight") is None
    # a field defect on that row is graded by tier 2 even though tier 3's sample never visits it
    tgt[6]["current_balance"] = 999_999
    result = run()
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert _codes(result, "keyed_diffs") == []
    t2 = _tier(result, "per_field_aggregates")
    assert {f["check"] for f in t2["findings"]} >= {"aggregate_sum", "aggregate_max"}
    assert all(f["object"] == "loans" and "current_balance" in f["detail"] for f in t2["findings"])
    assert target.calls["table_aggregates_excluding"] == 0


def test_the_exclusion_cap_is_judged_on_the_disjoint_in_flight_and_deleted_key_sets():
    # loans 3 and 4 were deleted and reinserted after the applied position: each is an in-flight
    # update and a reinserted tombstone, so the target statement binds 2 keys, not 4
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    loans[2]["modified_date"] = loans[3]["modified_date"] = _ts(30)
    events = [_ev(3, 15, 5.0), _ev(4, 15, 5.0)]
    source, target = _sides(loans, tgt, borrowers, {"raw_loans": events}, applied=10)
    target.max_params = 3
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    agg = _tier(result, "per_field_aggregates")
    assert agg["stats"]["applied_subset"]["loans"] == {"in_flight": 2, "excluded_keys": 2}
    assert target.last_excluded_keys == [(3,), (4,)]
    # neither set alone exceeds the budget (3 updates, 1 delete) but together the 4 keys do,
    # and that is settled from the counts without reading the in-flight keys
    loans[4]["modified_date"] = _ts(30)
    source.tombstones["raw_loans"].append(_ev(6, 15, 5.0))
    source.tables["dbo.loans"] = [r for r in loans if r["loan_id"] != 6]
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert _codes(result, "per_field_aggregates") == ["aggregates_ungraded_in_flight"]
    detail = _tier(result, "per_field_aggregates")["findings"][0]["detail"]
    assert "3 source rows in flight and 1 deletes in flight exceed the 3-key" in detail


def test_a_source_emptied_by_deletes_in_flight_is_not_a_stray_target():
    loans, borrowers = _rows(1)
    src, tgt = _deleted(loans, 1)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(1, 15, 5.0)]}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS", json.dumps(result["tiers"], default=str, indent=1)
    pk = _tier(result, "pk_set_diff")
    assert pk["findings"] == []
    assert pk["stats"]["loans"]["population"] == 0 and pk["stats"]["loans"]["target_population"] == 1
    assert pk["stats"]["loans"]["in_flight_deletes"] == 1 and pk["stats"]["loans"]["extra_on_target"] == 0
    assert target.calls["keys_in_range"] == 1
    cdc = _tier(result, "cdc_lag_ordering")
    assert cdc["findings"] == [] and cdc["stats"]["loans"]["target_max_from_in_flight_delete"] is True


def test_a_source_emptied_by_deletes_still_grades_the_aged_and_unexplained_ones():
    loans, borrowers = _rows(3)
    src, tgt = _deleted(loans, 1, 2, 3)
    events = [_ev(1, 15, 5.0), _ev(2, 12, 400.0)]  # loan 3 was never deleted on the source
    source, target = _sides(src, tgt, borrowers, {"raw_loans": events}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    detail = pk["findings"][0]["detail"]
    assert "(2,)" in detail and "(3,)" in detail and "(1,)" not in detail
    assert pk["stats"]["loans"]["in_flight_deletes"] == 1 and pk["stats"]["loans"]["extra_on_target"] == 2
    assert "delete_lag_exceeded" in _codes(result, "cdc_lag_ordering")


def test_a_source_emptied_without_usable_evidence_stays_strict_and_reads_no_keys():
    loans, borrowers = _rows(1)
    src, tgt = _deleted(loans, 1)
    source, target = _sides(src, tgt, borrowers, {}, applied=10)
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "source is empty" in pk["findings"][0]["detail"]
    assert target.calls["keys_in_range"] == 0


def test_a_binary_lsn_position_orders_like_the_engine_does():
    lsn = lambda n: n.to_bytes(10, "big")
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, lsn(0x1500), 5.0)]},
                            applied=bytearray(lsn(0x1000)))
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "PASS"
    stats = _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]
    assert stats["applied_position"] == lsn(0x1000).hex()
    assert stats["horizon"] == [bytes(10).hex(), lsn(0x1500).hex()]


@pytest.mark.parametrize("applied, horizon", [
    (10, (b"\x00" * 10, b"\x00" * 9 + b"\x10")),      # integer checkpoint against LSNs
    (b"\x00" * 8, (b"\x00" * 10, b"\x00" * 9 + b"\x10")),  # a binary of another width
    (True, (1, 20)),                                    # a boolean is not a position
])
def test_positions_from_different_mechanisms_are_never_compared(applied, horizon):
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, horizon[1], 5.0)]},
                            applied=applied, horizons={"raw_loans": horizon})
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert result["verdict"] == "FAIL"
    assert [f["check"] for f in _tier(result, "pk_set_diff")["findings"]] == ["pk_extra_on_target"]
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]["status"] == "incompatible"
    assert _codes(result, "cdc_lag_ordering") == ["delete_evidence_unusable"]
    assert source.calls["deletes_since"] == 0


def test_an_event_whose_position_type_differs_from_the_stream_is_incompatible():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10,
                            horizons={"raw_loans": (1, 20)})
    source.tombstones["raw_loans"] = [DeleteEvent((3,), b"\x0f", 5.0)]
    result = _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    stats = _tier(result, "pk_set_diff")["stats"]["loans"]["delete_evidence"]
    assert stats["status"] == "incompatible" and stats["in_flight_deletes"] == 0
    assert _codes(result, "cdc_lag_ordering") == ["delete_evidence_unusable"]


def test_a_source_whose_stream_kind_differs_from_the_declaration_is_a_config_error():
    loans, borrowers = _rows(12)
    src, tgt = _deleted(loans, 3)
    source, target = _sides(src, tgt, borrowers, {"raw_loans": [_ev(3, 15, 5.0)]}, applied=10)
    source.kind = "sqlserver_change_tracking"
    with pytest.raises(ConfigError, match="sqlserver_change_tracking"):
        _run(source, target, spec=_spec_with_evidence(), tol=TOL)
    assert source.window_open is False and target.window_open is False


def test_estimate_counts_the_evidence_statements_as_their_own_line():
    plain = estimate_cost(_spec(), TOL, "full", mode="transactional")
    with_evidence = estimate_cost(_spec_with_evidence(), TOL, "full", mode="transactional")
    assert plain["source_statements"]["delete_evidence"] == 0
    assert with_evidence["source_statements"]["delete_evidence"] == 2
    assert with_evidence["target_statements"]["delete_evidence"] == 1
    assert with_evidence["source_statements"]["total"] == plain["source_statements"]["total"] + 2
    assert with_evidence["target_statements"]["total"] == plain["target_statements"]["total"] + 1
    assert "delete_evidence" not in estimate_cost(_spec(), TOL, "full", mode="live")["source_statements"]


# ---------------------------------------------------------------- mapping block


def _mapping(block, tmp_path):
    spec = {"version": "m1", "objects": [{
        "object": "loans", "root_table": "dbo.loans", "key": {"source": ["loan_id"], "target": "loan_id"},
        "fields": [{"source": "loan_id", "target": "loan_id"}],
        "delete_evidence": block}]}
    p = tmp_path / "mapping.json"
    p.write_text(json.dumps(spec))
    return p


VALID_BLOCK = {"kind": "sqlserver_cdc", "capture": "raw_loans",
               "applied_position": {"table": "cdc_checkpoint", "column": "applied_lsn",
                                    "where": "source_table = 'dbo.loans'"}}


def test_mapping_delete_evidence_block_loads_into_the_object(tmp_path):
    spec = load_mapping_spec(_mapping(VALID_BLOCK, tmp_path))
    assert spec.objects[0].delete_evidence == EVIDENCE


def test_mapping_delete_evidence_where_is_optional_and_takes_params(tmp_path):
    block = dict(VALID_BLOCK, applied_position={"table": "cdc_checkpoint", "column": "applied_lsn"})
    assert load_mapping_spec(_mapping(block, tmp_path)).objects[0].delete_evidence.applied_where is None
    block = dict(VALID_BLOCK, applied_position={"table": "cdc_checkpoint", "column": "applied_lsn",
                                                "where": "source_table = '${src}'"})
    de = load_mapping_spec(_mapping(block, tmp_path), {"src": "dbo.loans"}).objects[0].delete_evidence
    assert de.applied_where == "source_table = 'dbo.loans'"


@pytest.mark.parametrize("block, match", [
    ("sqlserver_cdc", "must be an object"),
    (dict(VALID_BLOCK, kind="debezium"), "kind must be one of"),
    ({k: v for k, v in VALID_BLOCK.items() if k != "capture"}, "capture"),
    (dict(VALID_BLOCK, capture="raw_loans; DROP TABLE x"), "invalid identifier"),
    ({k: v for k, v in VALID_BLOCK.items() if k != "applied_position"}, "applied_position"),
    (dict(VALID_BLOCK, applied_position={"table": "cdc_checkpoint"}), "applied_position"),
    (dict(VALID_BLOCK, applied_position={"table": "cdc_checkpoint", "column": "lsn) --"}), "invalid identifier"),
    (dict(VALID_BLOCK, applied_position={"table": "cdc_checkpoint", "column": "applied_lsn", "where": 1}),
     "where must be a string"),
    (dict(VALID_BLOCK, applied_position={"table": "cdc_checkpoint", "column": "applied_lsn",
                                         "where": "1=1; DELETE FROM cdc_checkpoint"}), "single expression"),
])
def test_malformed_delete_evidence_blocks_are_rejected(block, match, tmp_path):
    with pytest.raises(ConfigError, match=match):
        load_mapping_spec(_mapping(block, tmp_path))
