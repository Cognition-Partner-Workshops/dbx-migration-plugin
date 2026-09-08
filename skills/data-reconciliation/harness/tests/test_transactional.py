"""--mode transactional: consistency window, PK-set diff, CDC lag/ordering, schema parity."""

import datetime as dt
import json

import pytest

from recon.adapters import (
    SOURCE_ADAPTERS,
    LakebaseTargetAdapter,
    SchemaFacts,
    TargetIdentityError,
)
from recon.cli import main
from recon.config import (
    CanonRule,
    ConfigError,
    FieldMapping,
    MappingSpec,
    ObjectMapping,
    Tolerances,
    load_mapping_spec,
    load_tolerances,
)
from recon.cost import estimate_cost
from recon.engine import MODES, PLANNED_MODES, run_recon
from recon.report import render_summary
from recon.transactional import _newer_predicate
from tests.fakes import FakeSource, FakeTarget

T0 = dt.datetime(2026, 9, 1, 12, 0, 0)


def _ts(seconds: int) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def _loan(i: int, changed: int = 0, **over) -> dict:
    row = {"loan_id": i, "loan_number": f"LN{i:05d}", "current_balance": 1000 + i,
           "modified_date": _ts(changed)}
    row.update(over)
    return row


LOANS_FACTS = SchemaFacts(
    primary_key=("loan_id",), unique={("loan_number",)},
    foreign_keys={(("borrower_id",), "dbo.borrowers", ("borrower_id",))},
    not_null={"loan_id", "loan_number", "current_balance", "modified_date", "borrower_id"},
    indexes={("borrower_id",), ("loan_status", "days_past_due")}, check_count=2,
    identity_columns={"loan_id"})

TARGET_LOANS_FACTS = SchemaFacts(
    primary_key=("loan_id",), unique={("loan_number",)},
    foreign_keys={(("borrower_id",), "loan_servicing.borrowers", ("borrower_id",))},
    not_null={"loan_id", "loan_number", "current_balance", "modified_date", "borrower_id"},
    indexes={("borrower_id",), ("loan_status", "days_past_due", "loan_id")}, check_count=2,
    identity_columns={"loan_id"})

BORROWER_FACTS = SchemaFacts(primary_key=("borrower_id",), not_null={"borrower_id"},
                             identity_columns={"borrower_id"})


def _spec(with_watermark: bool = True, with_identity: bool = True) -> MappingSpec:
    loans = ObjectMapping(
        object="loans", root_table="dbo.loans", key_source=["loan_id"], key_target=["loan_id"],
        fields=[FieldMapping("loan_number", "loan_number", "varchar", "string"),
                FieldMapping("current_balance", "current_balance", "money", "decimal(19,4)"),
                FieldMapping("borrower_id", "borrower_id", "int", "int")],
        watermark_source="modified_date" if with_watermark else None,
        watermark_target="modified_date" if with_watermark else None,
        identity_source="loan_id" if with_identity else None,
        identity_target="loan_id" if with_identity else None)
    borrowers = ObjectMapping(
        object="borrowers", root_table="dbo.borrowers", key_source=["borrower_id"],
        key_target=["borrower_id"], fields=[FieldMapping("name", "name", "varchar", "string")])
    return MappingSpec("m1", [loans, borrowers])


def _rows(n: int = 12) -> tuple[list[dict], list[dict]]:
    loans = [_loan(i, changed=i, borrower_id=1 + i % 3) for i in range(1, n + 1)]
    borrowers = [{"borrower_id": b, "name": f"B{b}"} for b in (1, 2, 3)]
    return loans, borrowers


def _sides(loans_src, loans_tgt, borrowers, *, src_seq=None, tgt_seq=None, tgt_facts=None):
    borrowers_tgt = [dict(b) for b in borrowers]
    source = FakeSource({"dbo.loans": loans_src, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): src_seq if src_seq is not None
                                   else max(r["loan_id"] for r in loans_src) + 1})
    target = FakeTarget({"loans": loans_tgt, "borrowers": borrowers_tgt},
                        schema={"loans": tgt_facts or TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): tgt_seq if tgt_seq is not None
                                   else max(r["loan_id"] for r in loans_src) + 1})
    return source, target


def _run(source, target, spec=None, tol=None, **kw):
    return run_recon("u1", "transactional", spec or _spec(), tol or Tolerances("t1"),
                     [CanonRule("decimal_round", "money", {"places": 4})], source, target, **kw)


def _tier(result, name):
    return next(t for t in result["tiers"] if t["name"] == name)


def _codes(result, name):
    return sorted(f["check"] for f in _tier(result, name)["findings"])


def test_transactional_is_a_runnable_mode():
    assert "transactional" in MODES
    assert "transactional" not in PLANNED_MODES


def test_identical_sides_pass_and_are_merge_eligible():
    loans, borrowers = _rows()
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(source, target)
    assert result["verdict"] == "PASS", result
    assert result["merge_eligible"] is True
    assert [t["tier"] for t in result["tiers"]] == [0, 1, 2, 3, 5, 6, 7]
    window = _tier(result, "consistency_window")
    assert window["stats"]["isolation"] == {"source": "fake_snapshot", "target": "fake_snapshot"}
    assert source.calls["open_window"] == 1 and source.calls["close_window"] == 1
    assert target.calls["open_window"] == 1 and target.calls["close_window"] == 1


def test_a_side_that_moves_during_the_run_fails_the_window():
    loans, borrowers = _rows()
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    # the source grows after the opening markers were read
    source.on_open = lambda: None
    original = source.window_marker
    seen = {"n": 0}

    def moving_marker(table, key_cols, watermark, where=None):
        seen["n"] += 1
        if table == "dbo.loans" and seen["n"] > 2:
            loans.append(_loan(99, changed=99, borrower_id=1))
        return original(table, key_cols, watermark, where)
    source.window_marker = moving_marker
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    assert "window_unstable" in _codes(result, "consistency_window")
    assert result["merge_eligible"] is False


def test_pk_set_diff_reports_missing_and_extra_keys_only_for_mismatched_ranges():
    loans, borrowers = _rows(40)
    tgt = [dict(r) for r in loans if r["loan_id"] not in (7, 23)]
    tgt.append(_loan(500, changed=0, borrower_id=1))  # stray key above every source range
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8))
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    codes = [f["check"] for f in pk["findings"]]
    assert codes == ["pk_missing_on_target", "pk_extra_on_target"]
    assert "(7,)" in pk["findings"][0]["detail"] and "(23,)" in pk["findings"][0]["detail"]
    assert "(500,)" in pk["findings"][1]["detail"]
    stats = pk["stats"]["loans"]
    assert stats["ranges"] == 10  # 8 strata + 2 open edges
    assert stats["missing_on_target"] == 2 and stats["extra_on_target"] == 1
    # only the mismatched ranges streamed keys, never the whole table
    assert stats["keys_streamed"] < 2 * len(loans)
    assert source.calls["range_fingerprints"] == 2 and target.calls["range_fingerprints"] == 2
    assert stats["fingerprint"] == "count+key_sum+key_sumsq+watermark_sum+watermark_sumsq"


def test_a_key_swapped_for_another_in_the_same_range_is_caught_when_counts_agree():
    # one source key absent, one stray target key present in the same range: the range counts
    # are equal, only the key digest differs; tier 3 is sampled so it may fetch neither row
    _, borrowers = _rows()
    loans = [_loan(2 * i, changed=i, borrower_id=1 + i % 3) for i in range(1, 41)]  # even keys
    tgt = [dict(r) for r in loans if r["loan_id"] != 74]
    tgt.append(_loan(75, changed=37, borrower_id=1))
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    pk = _tier(result, "pk_set_diff")
    assert result["verdict"] == "FAIL"
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]
    assert "(74,)" in pk["findings"][0]["detail"] and "(75,)" in pk["findings"][1]["detail"]
    stats = pk["stats"]["loans"]
    assert stats["mismatched_ranges"] == 1 and stats["keys_streamed"] < len(loans)
    assert _tier(result, "counts_through_mapping")["passed"] is True  # counts alone saw nothing


def test_two_keys_traded_for_two_with_the_same_sum_are_caught():
    # source keys 14 and 18 are replaced on the target by 15 and 17 inside the same stratum
    # (12..20): same count, same key sum, same watermarks; only the second moment (sum of
    # squares) tells them apart
    _, borrowers = _rows()
    loans = [_loan(2 * i, changed=i, borrower_id=1 + i % 3) for i in range(1, 41)]  # even keys
    tgt = [dict(r) for r in loans if r["loan_id"] not in (14, 18)]
    tgt.append(_loan(15, changed=7, borrower_id=1))
    tgt.append(_loan(17, changed=9, borrower_id=1))
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]
    assert "(14,)" in pk["findings"][0]["detail"] and "(18,)" in pk["findings"][0]["detail"]
    assert "(15,)" in pk["findings"][1]["detail"] and "(17,)" in pk["findings"][1]["detail"]
    stats = pk["stats"]["loans"]
    assert 0 < stats["mismatched_ranges"] < stats["ranges"]
    assert _tier(result, "counts_through_mapping")["passed"] is True


def test_two_watermarks_moved_in_opposite_directions_are_caught():
    # loan 6 applied a second late, loan 8 a second early: the watermark total of the range is
    # unchanged, its sum of squares is not, so both rows are streamed and graded
    loans, borrowers = _rows(40)
    tgt = [dict(r) for r in loans]
    tgt[5]["modified_date"] = _ts(7)
    tgt[7]["modified_date"] = _ts(7)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    assert result["verdict"] == "FAIL"
    cdc = _tier(result, "cdc_lag_ordering")
    codes = {f["check"]: f["detail"] for f in cdc["findings"]}
    assert "(6,)" in codes["row_ahead_of_source"] and "(8,)" in codes["row_behind_applied_watermark"]


def test_an_undrained_source_delete_fails_counts_and_pk_set_even_with_generous_lag():
    # loan 3 was deleted on the source after the target applied it; the target has also applied
    # newer rows, so nothing about the extra row says "pending delete" rather than "stray write"
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    del loans[2]
    source, target = _sides(loans, tgt, borrowers, src_seq=13, tgt_seq=13)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=3600))
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert _codes(result, "counts_through_mapping") == ["root_count"]
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(3,)" in pk["findings"][0]["detail"] and "undrained deletes" in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["in_flight_missing"] == 0


def test_a_stray_target_key_between_two_source_strata_is_counted():
    # source keys are the even numbers; strata of 5 keys end at 10, 20, ...; a target-only key
    # 11 lies in the gap between the stratum ending at 10 and the one starting at 12
    _, borrowers = _rows()
    loans = [_loan(2 * i, changed=i, borrower_id=1 + i % 3) for i in range(1, 41)]
    tgt = [dict(r) for r in loans] + [_loan(11, changed=5, borrower_id=1)]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, cdc_lag_max_s=3600))
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_extra_on_target"]
    assert "(11,)" in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["mismatched_ranges"] == 1


def test_pk_set_stream_every_range_skips_fingerprints_and_streams_everything():
    loans, borrowers = _rows(40)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, pk_set_stream_every_range=True))
    assert result["verdict"] == "PASS", result
    stats = _tier(result, "pk_set_diff")["stats"]["loans"]
    assert stats["fingerprint"] == "not used: every range streamed (pk_set_stream_every_range)"
    assert stats["mismatched_ranges"] == stats["ranges"]
    assert stats["keys_streamed"] >= 2 * len(loans)
    assert source.calls["range_fingerprints"] == 0 and target.calls["range_fingerprints"] == 0


def test_in_flight_rows_are_not_defects_when_lag_is_tolerated():
    loans, borrowers = _rows(12)
    # target applied everything up to loan 10 (watermark _ts(10)); 11 and 12 are still in flight
    tgt = [dict(r) for r in loans if r["loan_id"] <= 10]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == "PASS", result
    t1 = _tier(result, "counts_through_mapping")
    assert t1["stats"]["count_gap_within_in_flight"]["loans"] == {"gap": 2, "in_flight": 2}
    assert _tier(result, "per_field_aggregates")["stats"]["skipped_in_flight"] == ["loans"]
    assert _tier(result, "keyed_diffs")["stats"]["loans"]["in_flight_rows"] == 2
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_missing"] == 2
    cdc = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert cdc["lag_s"] == 2.0 and cdc["in_flight"] == 2
    assert result["merge_eligible"] is True


def test_summary_names_the_window_isolation_and_in_flight_rows():
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans if r["loan_id"] <= 10]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    summary = render_summary(result)
    assert ("- Consistency window: source isolation `fake_snapshot`, target isolation "
            "`fake_snapshot`, held; in flight at open: `{\"loans\": 2}`") in summary


def test_in_flight_predicate_starts_at_the_next_microsecond_for_datetimes():
    # engines that store more precision than the driver returns (datetime2(7)) would otherwise
    # count every row sharing the applied microsecond as in flight
    hwm = dt.datetime(2026, 9, 8, 18, 43, 52, 164112)
    assert _newer_predicate("modified_date", hwm) == \
        "modified_date >= '2026-09-08 18:43:52.164113'"
    assert _newer_predicate("version_no", 41) == "version_no > 41"


def test_lag_beyond_tolerance_is_a_finding():
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans if r["loan_id"] <= 10]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=1))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["cdc_lag_exceeded"]
    # the in-flight rows are still not double-reported as missing keys
    assert _codes(result, "pk_set_diff") == []


def test_a_missing_key_older_than_the_applied_watermark_is_a_defect_even_with_lag():
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans if r["loan_id"] not in (3, 11, 12)]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target"]
    assert "(3,)" in pk["findings"][0]["detail"] and "(11,)" not in pk["findings"][0]["detail"]
    assert pk["stats"]["loans"]["in_flight_missing"] == 2
    # tier 1 cannot be excused either: the gap (3) exceeds the in-flight bound (2)
    assert _codes(result, "counts_through_mapping") == ["root_count"]


def test_target_ahead_of_source_is_an_ordering_violation_not_lag():
    loans, borrowers = _rows(6)
    tgt = [dict(r) for r in loans]
    tgt[2]["modified_date"] = _ts(100)  # replayed / applied out of order
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=1000))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["row_ahead_of_source", "target_ahead_of_source"]
    assert "row_ahead_of_source" in _codes(result, "keyed_diffs")


def test_one_row_ahead_of_its_source_is_caught_when_the_global_max_is_not():
    # loan 3 on the target carries a newer watermark than its source row, but loan 40 (the
    # global max on both sides) is untouched, so max(target) is not ahead of max(source);
    # tier 3 samples two keys and may never fetch loan 3
    loans, borrowers = _rows(40)
    tgt = [dict(r) for r in loans]
    tgt[2]["modified_date"] = _ts(30)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2),
                  depth="sampled")
    assert result["verdict"] == "FAIL"
    cdc = _tier(result, "cdc_lag_ordering")
    assert [f["check"] for f in cdc["findings"]] == ["row_ahead_of_source"]
    assert "(3,)" in cdc["findings"][0]["detail"]
    assert cdc["stats"]["loans"]["lag_s"] == 0.0
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["mismatched_ranges"] == 1


def test_a_row_behind_the_applied_watermark_is_a_lost_change_not_lag():
    # the target applied up to loan 12 (its watermark) but loan 5's row still shows an older
    # modified_date than its source: the change was skipped or applied out of order
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    tgt[4]["modified_date"] = _ts(1)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=1000), depth="full")
    assert result["verdict"] == "FAIL"
    assert "row_behind_applied_watermark" in _codes(result, "cdc_lag_ordering")


def test_an_unapplied_update_in_flight_is_not_an_ordering_finding():
    # loans 11 and 12 changed on the source after the target's applied watermark (_ts(10)): the
    # target still holds their previous versions, which is lag, not a defect
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    tgt[10]["modified_date"] = _ts(9)
    tgt[11]["modified_date"] = _ts(10)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == "PASS", result
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_updates"] == 2


def test_string_keys_stream_every_range_rather_than_trusting_counts():
    loans, borrowers = _rows(12)
    spec = _spec()
    spec.objects[0].key_source[:] = ["loan_number"]
    spec.objects[0].key_target[:] = ["loan_number"]
    tgt = [dict(r) for r in loans if r["loan_id"] != 7]
    tgt.append(_loan(7, changed=7, borrower_id=1) | {"loan_number": "LN99999"})
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=spec, tol=Tolerances("t1", pk_set_ranges=4))
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["fingerprint"] == "unavailable: every range streamed"
    assert pk["stats"]["loans"]["mismatched_ranges"] == pk["stats"]["loans"]["ranges"]
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]


def _marker_only(source):
    source.pin = "none"
    return source


def test_a_side_without_snapshot_or_change_token_is_not_merge_eligible():
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(_marker_only(source), target)
    window = _tier(result, "consistency_window")
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert [f["check"] for f in window["findings"]] == ["window_unproven"]
    assert "source" in window["findings"][0]["detail"]
    assert window["stats"]["strength"] == {"source": "markers", "target": "snapshot"}
    assert window["stats"]["isolation"]["source"] == "none"
    assert ("- Consistency window: source isolation `none` (markers), target isolation "
            "`fake_snapshot`, UNPROVEN") in render_summary(result)


def test_marker_only_window_is_accepted_only_by_a_recorded_tolerance():
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(_marker_only(source), target, tol=Tolerances("t1", accept_marker_only_window=True))
    assert result["verdict"] == "PASS" and result["merge_eligible"] is True
    window = _tier(result, "consistency_window")
    assert window["stats"]["accepted_marker_only"] == ["source"]


@pytest.mark.parametrize("flag", ["accept_marker_only_window", "pk_set_stream_every_range"])
@pytest.mark.parametrize("value", ["false", "true", "no", 0, 1, None, [], {}])
def test_tolerance_switches_must_be_json_booleans(tmp_path, flag, value):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", flag: value}))
    with pytest.raises(ConfigError, match=f"{flag} must be a JSON boolean"):
        load_tolerances(path)


def test_tolerance_switches_load_real_booleans_and_default_off(tmp_path):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", "accept_marker_only_window": True}))
    tol = load_tolerances(path)
    assert tol.accept_marker_only_window is True and tol.pk_set_stream_every_range is False
    path.write_text(json.dumps({"version": "t1"}))
    tol = load_tolerances(path)
    assert tol.accept_marker_only_window is False and tol.pk_set_stream_every_range is False


def _tokened(source, counter):
    """A marker-fallback source whose engine exposes a per-table write counter."""
    source.pin = "none"
    source.change_token = lambda table: counter[table]
    return source


def test_an_update_below_the_max_watermark_during_fallback_is_caught_by_the_change_token():
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    writes = {"dbo.loans": 10, "dbo.borrowers": 3}
    _tokened(source, writes)
    original = source.range_fingerprints

    def write_mid_run(*a, **kw):
        # count and max(modified_date) are unchanged: loan 2 is edited in place
        loans[1]["current_balance"] += 1
        writes["dbo.loans"] += 1
        return original(*a, **kw)
    source.range_fingerprints = write_mid_run
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    window = _tier(result, "consistency_window")
    assert [f["check"] for f in window["findings"]] == ["window_unstable"]
    assert window["stats"]["strength"]["source"] == "change_token"
    assert window["stats"]["markers"]["loans"]["source_open"][:2] == \
        window["stats"]["markers"]["loans"]["source_close"][:2]
    assert "source isolation `none` (change_token), target isolation `fake_snapshot`, MOVED" in \
        render_summary(result)


def test_a_balanced_insert_and_delete_during_fallback_is_caught_by_the_change_token():
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    writes = {"dbo.loans": 10, "dbo.borrowers": 3}
    _tokened(source, writes)
    original = source.range_fingerprints

    def churn_mid_run(*a, **kw):
        # delete loan 5 and insert a new loan 5 twin with an older watermark: count and max hold
        loans[:] = [r for r in loans if r["loan_id"] != 5] + [_loan(13, changed=1, borrower_id=1)]
        writes["dbo.loans"] += 2
        return original(*a, **kw)
    source.range_fingerprints = churn_mid_run
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    assert "window_unstable" in _codes(result, "consistency_window")
    assert result["merge_eligible"] is False


def test_the_same_writes_are_invisible_to_markers_alone_and_that_is_why_they_do_not_pass():
    # sanity check of the premise: without a token the markers hold, and the only thing that
    # stops a merge is the window_unproven finding
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    _marker_only(source)
    original = source.range_fingerprints

    def write_mid_run(*a, **kw):
        loans[1]["current_balance"] += 1
        return original(*a, **kw)
    source.range_fingerprints = write_mid_run
    result = _run(source, target)
    assert _codes(result, "consistency_window") == ["window_unproven"]
    assert result["merge_eligible"] is False


def test_a_shared_tier_that_raises_still_closes_both_windows():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    def failing_target_count(object, where=None):
        raise RuntimeError("connection reset during tier 1")
    target.target_row_count = failing_target_count
    with pytest.raises(RuntimeError, match="tier 1"):
        _run(source, target)
    assert source.calls["close_window"] == 1 and target.calls["close_window"] == 1
    assert not source.window_open and not target.window_open


def test_a_transactional_tier_that_raises_still_closes_both_windows():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.fail_on["range_fingerprints"] = RuntimeError("lakebase branch went away")
    with pytest.raises(RuntimeError, match="lakebase"):
        _run(source, target)
    assert source.calls["close_window"] == 1 and target.calls["close_window"] == 1


def test_a_failing_marker_query_at_open_still_closes_both_windows():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.fail_on["window_marker"] = RuntimeError("permission denied for marker")
    with pytest.raises(RuntimeError, match="marker"):
        _run(source, target)
    assert source.calls["close_window"] == 1 and target.calls["close_window"] == 1


def test_one_side_failing_to_close_does_not_leave_the_other_pinned():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.fail_on["range_fingerprints"] = RuntimeError("tier failure")
    source.fail_on["close_window"] = RuntimeError("source rollback failed")
    with pytest.raises(RuntimeError, match="source rollback failed"):
        _run(source, target)
    assert target.calls["close_window"] == 1 and not target.window_open


def test_a_clean_run_closes_each_window_exactly_once():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(source, target)
    assert result["verdict"] == "PASS"
    assert source.calls["close_window"] == 1 and target.calls["close_window"] == 1


def test_field_diff_is_still_graded_for_applied_rows():
    loans, borrowers = _rows(6)
    tgt = [dict(r) for r in loans]
    tgt[1]["current_balance"] = 0
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target)
    assert "field_diff" in _codes(result, "keyed_diffs")


def test_schema_parity_findings_map_through_the_spec():
    loans, borrowers = _rows(6)
    weak = SchemaFacts(primary_key=("loan_id",), unique=set(), foreign_keys=set(),
                       not_null={"loan_id"}, indexes=set(), check_count=0,
                       identity_columns={"loan_id"})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=weak)
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    assert _codes(result, "schema_parity") == sorted([
        "unique_missing", "foreign_key_missing", "not_null_missing", "not_null_missing",
        "not_null_missing", "not_null_missing", "index_missing", "index_missing",
        "check_constraint_count_lower"])
    fk = next(f for f in _tier(result, "schema_parity")["findings"] if f["check"] == "foreign_key_missing")
    assert "borrowers" in fk["detail"]


def test_index_covered_by_a_longer_target_index_is_parity():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    facts = _tier(result, "schema_parity")["stats"]["loans"]
    assert facts["target"]["indexes"] == [["borrower_id"], ["loan_status", "days_past_due", "loan_id"]]


def test_source_filtered_indexes_are_reported_for_a_manual_check_not_graded():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    source.schema["dbo.loans"] = SchemaFacts(
        primary_key=LOANS_FACTS.primary_key, unique=set(LOANS_FACTS.unique),
        foreign_keys=set(LOANS_FACTS.foreign_keys), not_null=set(LOANS_FACTS.not_null),
        indexes=set(LOANS_FACTS.indexes), check_count=2, identity_columns={"loan_id"},
        partial={("days_past_due",)})
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    parity = _tier(result, "schema_parity")
    assert parity["stats"]["partial_indexes_unverified"] == [
        ("loans: source filtered index ('days_past_due',) carries a predicate the harness cannot "
         "translate; confirm its target counterpart by hand")]
    assert parity["stats"]["loans"]["source"]["partial"] == [["days_past_due"]]


def test_primary_key_mismatch_is_reported():
    loans, borrowers = _rows(6)
    facts = SchemaFacts(primary_key=("loan_number",), unique={("loan_number",)},
                        foreign_keys=TARGET_LOANS_FACTS.foreign_keys,
                        not_null=TARGET_LOANS_FACTS.not_null, indexes=TARGET_LOANS_FACTS.indexes,
                        check_count=2, identity_columns={"loan_id"})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=facts)
    result = _run(source, target)
    assert "primary_key_mismatch" in _codes(result, "schema_parity")


def test_sequence_behind_source_max_key_is_a_cutover_blocker():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_seq=4)
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    assert _codes(result, "schema_parity") == ["sequence_behind_source"]
    identity = _tier(result, "schema_parity")["stats"]["loans"]["identity"]
    assert identity == {"source_next": 7, "source_max": 6, "target_next": 4}


def test_missing_target_sequence_is_a_finding():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.sequences[("loans", "loan_id")] = None
    result = _run(source, target)
    assert _codes(result, "schema_parity") == ["sequence_missing"]


def test_unverifiable_schema_facts_warn_and_block_merge_eligibility():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    del target.schema["loans"]
    result = _run(source, target)
    assert result["verdict"] == "PASS"
    assert result["merge_eligible"] is False
    assert any(w.startswith("UNVERIFIED schema_parity") for w in result["warnings"])


def test_objects_without_watermark_are_graded_strictly():
    loans, borrowers = _rows(6)
    tgt = [dict(r) for r in loans][:-1]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_spec(with_watermark=False),
                  tol=Tolerances("t1", cdc_lag_max_s=1000))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "counts_through_mapping") == ["root_count"]
    assert _tier(result, "cdc_lag_ordering")["stats"]["loans"]["watermark"] is None


def test_adapters_without_transactional_side_are_refused():
    class Plain:
        def row_count(self, table, where=None):
            return 0

        def target_row_count(self, object, where=None):
            return 0

    loans, borrowers = _rows(3)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    with pytest.raises(ConfigError, match="TransactionalSide"):
        _run(source, Plain())
    with pytest.raises(ConfigError, match="TransactionalSide"):
        _run(Plain(), target)


def test_mapping_spec_loads_watermark_and_identity(tmp_path):
    spec = {"version": "m1", "objects": [{
        "object": "loans", "root_table": "dbo.loans",
        "key": {"source": ["loan_id"], "target": "loan_id"},
        "fields": [{"source": "loan_number", "target": "loan_number"}],
        "watermark": {"source": "modified_date", "target": "modified_date"},
        "identity": {"source": "loan_id", "target": "loan_id"}}]}
    p = tmp_path / "m.json"
    p.write_text(json.dumps(spec))
    loaded = load_mapping_spec(p, {})
    c = loaded.objects[0]
    assert (c.watermark_source, c.watermark_target) == ("modified_date", "modified_date")
    assert (c.identity_source, c.identity_target) == ("loan_id", "loan_id")
    spec["objects"][0]["watermark"] = {"source": "modified_date"}
    p.write_text(json.dumps(spec))
    with pytest.raises(ConfigError, match="watermark"):
        load_mapping_spec(p, {})
    spec["objects"][0]["watermark"] = {"source": "modified_date", "target": "x; drop"}
    p.write_text(json.dumps(spec))
    with pytest.raises(ConfigError):
        load_mapping_spec(p, {})


def test_estimate_adds_the_transactional_tiers():
    spec = _spec()
    live = estimate_cost(spec, Tolerances("t1"), row_counts={"dbo.loans": 10, "dbo.borrowers": 3})
    tx = estimate_cost(spec, Tolerances("t1"), row_counts={"dbo.loans": 10, "dbo.borrowers": 3},
                       mode="transactional")
    assert "tier5" not in live["source_statements"]
    assert tx["mode"] == "transactional"
    assert tx["source_statements"]["tier0"] == 2 + 1 + 2  # markers x2 per object, in-flight for loans
    assert tx["source_statements"]["tier5"] == 6 and tx["target_statements"]["tier5"] == 2
    assert tx["source_statements"]["tier7"] == 4 + 2 + 4
    assert tx["source_statements"]["total"] > live["source_statements"]["total"]


def _write_inputs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig"]}))
    (tmp_path / "m.json").write_text(json.dumps({"version": "m1", "objects": [{
        "object": "loans", "root_table": "dbo.loans",
        "key": {"source": ["loan_id"], "target": "loan_id"},
        "fields": [{"source": "loan_number", "target": "loan_number"}],
        "watermark": {"source": "modified_date", "target": "modified_date"}}]}))
    (tmp_path / "t.json").write_text(json.dumps({"version": "t1"}))
    (tmp_path / "c.json").write_text(json.dumps({"version": "c1", "rules": []}))


def test_cli_refuses_transactional_mode_against_a_delta_target(tmp_path, monkeypatch, capsys):
    _write_inputs(tmp_path, monkeypatch)
    with pytest.raises(SystemExit) as exc:
        main(["run", "--unit", "u", "--family", "sqlserver", "--mapping", "m.json",
              "--tolerances", "t.json", "--canonicalization", "c.json",
              "--mode", "transactional", "--source-dsn-secret", "S", "--target-secret", "T",
              "--target-catalog", "mig", "--target-schema", "s", "--out", "o"])
    msg = str(exc.value)
    assert "transactional" in msg and "lakebase" in msg and "not implemented" in msg


class _Conn:
    """Minimal DB-API stand-in whose only answer is the connected database name."""

    def __init__(self, database):
        self.database, self.closed, self.executed = database, False, []

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, params=()):
                conn.executed.append(sql)

            def fetchall(self):
                return [(conn.database,)]
        return Cur()

    def close(self):
        self.closed = True


def test_lakebase_target_binds_the_connection_to_the_allowlisted_database(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("T", "dsn-under-test")
    conns = []

    def connect(dsn):
        conns.append(_Conn("loan_servicing_prod"))
        return conns[-1]
    monkeypatch.setattr(psycopg, "connect", connect)
    with pytest.raises(TargetIdentityError, match="'loan_servicing_prod'.*'lakebase_rehearsal'"):
        LakebaseTargetAdapter("T", "lakebase_rehearsal", "loan_servicing")
    assert conns[0].closed is True
    assert conns[0].executed == ["SELECT current_database()"]
    target = LakebaseTargetAdapter("T", "loan_servicing_prod", "loan_servicing")
    assert target.database == "loan_servicing_prod" and conns[1].closed is False


def test_cli_refuses_a_lakebase_dsn_outside_the_allowlisted_database(tmp_path, monkeypatch):
    _write_inputs(tmp_path, monkeypatch)
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("S", "src")
    monkeypatch.setenv("T", "dsn-under-test")
    monkeypatch.setattr(psycopg, "connect", lambda dsn: _Conn("somewhere_else"))
    monkeypatch.setitem(SOURCE_ADAPTERS, "sqlserver", lambda secret: FakeSource({}))
    with pytest.raises(SystemExit) as exc:
        main(["run", "--unit", "u", "--family", "sqlserver", "--mapping", "m.json",
              "--tolerances", "t.json", "--canonicalization", "c.json",
              "--mode", "transactional", "--source-dsn-secret", "S", "--target-secret", "T",
              "--target-kind", "lakebase", "--target-catalog", "mig", "--target-schema", "s",
              "--out", "o"])
    assert "'somewhere_else'" in str(exc.value) and "'mig'" in str(exc.value)
    assert "dsn-under-test" not in str(exc.value)


def test_cli_estimate_accepts_mode(tmp_path, monkeypatch, capsys):
    _write_inputs(tmp_path, monkeypatch)
    assert main(["estimate", "--mapping", "m.json", "--tolerances", "t.json",
                 "--mode", "transactional"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["mode"] == "transactional" and "tier5" in out["source_statements"]
