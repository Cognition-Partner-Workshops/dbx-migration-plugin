"""--mode transactional: consistency window, PK-set diff, CDC lag/ordering, schema parity."""
import dataclasses
import datetime as dt
import io
import json
from decimal import Decimal

import pytest
from recon.adapters import (
    SOURCE_ADAPTERS,
    SchemaFacts,
)
from recon.cli import main
from recon.config import (
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
from recon.transactional import _common_kind, _digest_kind, _kind, open_window
from recon.watermarks import (
    check_comparable,
    in_form_of,
    instant,
    lag_seconds,
    lag_units,
    later,
    same,
)

from tests.fakes import FakeSource, FakeTarget, FakeTypedSource
from tests.loans import (
    BORROWER_FACTS,
    EPOCH,
    EVEN_KEYS,
    LOANS_FACTS,
    PLUS2,
    T0,
    TARGET_LOANS_FACTS,
    UTC,
    _codes,
    _counter_rows,
    _counter_spec,
    _db,
    _keyed,
    _loan,
    _rows,
    _rowversion,
    _run,
    _sides,
    _spec,
    _tgt,
    _tier,
    _ts,
)


def test_transactional_is_a_runnable_mode():
    assert "transactional" in MODES
    assert "transactional" not in PLANNED_MODES


@pytest.mark.parametrize("aware", [False, True])
def test_identical_sides_pass_and_are_merge_eligible(aware):
    # source: SQL Server datetime (naive, UTC by contract); target: timestamptz rendered +02:00
    loans, borrowers = _rows()
    source, target = _sides(loans, _tgt(loans, aware), borrowers)
    result = _run(source, target)
    assert result["verdict"] == "PASS", result
    assert result["merge_eligible"] is True
    assert [t["tier"] for t in result["tiers"]] == [0, 1, 2, 3, 5, 6, 7]
    window = _tier(result, "consistency_window")
    assert window["stats"]["isolation"] == {"source": "fake_snapshot", "target": "fake_snapshot"}
    assert source.calls["open_window"] == 1 and source.calls["close_window"] == 1
    assert target.calls["open_window"] == 1 and target.calls["close_window"] == 1
    cdc = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert cdc["lag_s"] == 0.0 and cdc["in_flight"] == 0
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["mismatched_ranges"] == 0


def test_a_side_that_moves_during_the_run_fails_the_window():
    loans, borrowers = _rows()
    source, target = _sides(loans, _tgt(loans), borrowers)
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


@pytest.mark.parametrize("n, keys, src_gone, tgt_gone, extra, seq, tol, t1_codes, needles, absent, stats", [
    # 7 and 23 never arrived and 500 lies above every source range
    (40, None, (), (7, 23), {500: 0}, None, Tolerances("t1", pk_set_ranges=8), ["root_count"],
     {"pk_missing_on_target": ["(7,)", "(23,)"], "pk_extra_on_target": ["(500,)"]}, [],
     {"ranges": 10, "missing_on_target": 2, "extra_on_target": 1,   # 8 strata + 2 open edges
      "fingerprint": "count+key_sum+key_sumsq+watermark_sum+watermark_sumsq"}),
    # loan 3 was deleted on the source after the target applied it; the target has also applied
    # newer rows, so nothing about the extra row says "pending delete" rather than "stray write"
    (12, None, (3,), (), {}, 13, Tolerances("t1", cdc_lag_max_s=3600), ["root_count"],
     {"pk_extra_on_target": ["(3,)", "undrained deletes"]}, [], {"in_flight_missing": 0}),
    # source keys are the even numbers; strata of 5 keys end at 10, 20, ...; a target-only key
    # 11 lies in the gap between the stratum ending at 10 and the one starting at 12
    (40, EVEN_KEYS, (), (), {11: 5}, None, Tolerances("t1", pk_set_ranges=8, cdc_lag_max_s=3600), ["root_count"],
     {"pk_extra_on_target": ["(11,)"]}, [], {"mismatched_ranges": 1}),
    # 11 and 12 are in flight; 3 is older than the applied watermark and a defect even with lag,
    # and tier 1 cannot be excused either: the gap (3) exceeds the in-flight bound (2)
    (12, None, (), (3, 11, 12), {}, None, Tolerances("t1", cdc_lag_max_s=5), ["root_count"],
     {"pk_missing_on_target": ["(3,)"]}, ["(11,)"], {"in_flight_missing": 2}),
])
def test_pk_set_diff_reports_missing_and_extra_keys_only_for_mismatched_ranges(
        n, keys, src_gone, tgt_gone, extra, seq, tol, t1_codes, needles, absent, stats):
    loans, borrowers = _rows(n, keys=keys)
    tgt = [dict(r) for r in loans if r["loan_id"] not in tgt_gone]
    tgt += [_loan(k, changed=c, borrower_id=1) for k, c in extra.items()]
    src = [r for r in loans if r["loan_id"] not in src_gone]
    source, target = _sides(src, tgt, borrowers, src_seq=seq, tgt_seq=seq)
    result = _run(source, target, tol=tol)
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert _codes(result, "counts_through_mapping") == t1_codes
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == list(needles)
    for finding in pk["findings"]:
        assert all(n in finding["detail"] for n in needles[finding["check"]])
        assert not any(a in finding["detail"] for a in absent)
    assert {k: pk["stats"]["loans"][k] for k in stats} == stats
    # only the mismatched ranges streamed keys, never the whole table
    assert pk["stats"]["loans"]["keys_streamed"] < 2 * len(loans)
    assert source.calls["range_fingerprints"] == 2 and target.calls["range_fingerprints"] == 2


@pytest.mark.parametrize("missing, extra", [
    # one key swapped inside a range: counts agree, only the key digest differs
    ((74,), {75: 37}),
    # 14 and 18 traded for 15 and 17 inside stratum 12..20: same count, key sum and watermarks;
    # only the second moment (sum of squares) tells them apart
    ((14, 18), {15: 7, 17: 9}),
])
def test_keys_swapped_inside_a_range_are_caught_when_counts_agree(missing, extra):
    # tier 3 is sampled (2 keys) so it may fetch none of the rows involved
    loans, borrowers = _rows(keys=EVEN_KEYS)
    tgt = [dict(r) for r in loans if r["loan_id"] not in missing]
    tgt += [_loan(k, changed=c, borrower_id=1) for k, c in extra.items()]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]
    assert all(f"({k},)" in pk["findings"][0]["detail"] for k in missing)
    assert all(f"({k},)" in pk["findings"][1]["detail"] for k in extra)
    stats = pk["stats"]["loans"]
    assert 0 < stats["mismatched_ranges"] < stats["ranges"] and stats["keys_streamed"] < len(loans)
    assert _tier(result, "counts_through_mapping")["passed"] is True  # counts alone saw nothing


@pytest.mark.parametrize("source_edit, target_edit, ahead, behind", [
    # loan 6 applied a second late, loan 8 a second early: the watermark total of the range is
    # unchanged, its sum of squares is not, so both rows are streamed and graded
    (None, {5: _ts(7), 7: _ts(7)}, (6,), (8,)),
    # loan 3 is newer on the target but loan 40 (the global max on both sides) is untouched, so
    # max(target) is not ahead of max(source); tier 3 samples two keys and may never fetch loan 3
    (None, {2: _ts(30)}, (3,), None),
    # SUM skips NULL, so a NULL watermark used to fold into the same digest as the epoch: the
    # target lost row 6's watermark, no row is the max, tier 3 is sampled and may not fetch it
    ({5: EPOCH}, {5: None}, None, (6,)),
    ({5: None}, {5: EPOCH}, (6,), None),
])
def test_watermark_moments_catch_one_range_that_counts_and_key_sums_cannot(source_edit, target_edit,
                                                                              ahead, behind):
    loans, borrowers = _rows(40)
    for i, wm in (source_edit or {}).items():
        loans[i]["modified_date"] = wm
    tgt = [dict(r) for r in loans]
    for i, wm in target_edit.items():
        tgt[i]["modified_date"] = wm
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")["stats"]["loans"]
    assert pk["fingerprint"].startswith("count+key_sum") and pk["mismatched_ranges"] == 1 < pk["ranges"]
    cdc = _tier(result, "cdc_lag_ordering")
    assert cdc["stats"]["loans"]["lag_s"] == 0.0
    codes = {f["check"]: f["detail"] for f in cdc["findings"]}
    expected = {"row_ahead_of_source": ahead, "row_behind_applied_watermark": behind}
    assert set(codes) == {c for c, key in expected.items() if key}
    assert all(f"{key}" in codes[c] for c, key in expected.items() if key)


@pytest.mark.parametrize("key, source_balance, edited, target_value, missing, extra", [
    # string keys have no portable digest
    ("loan_number", None, 7, "LN99999", "LN00007", "LN99999"),
    # two keys that differ beyond the sixth decimal must never collapse into one digest
    ("current_balance", lambda k: Decimal("1000.0000000") + Decimal(k) * Decimal("0.0000001"),
     7, Decimal("1000.0000099"), "1000.0000007", "1000.0000099"),
    # every stratum bound is whole (1, 3, 6, 9, 12) yet one interior key is fractional on each
    # side; rounded into DECIMAL(38,0) both would digest as 5 and the range would look equal
    ("current_balance", lambda k: Decimal("4.6") if k == 5 else Decimal(k), 5, Decimal("5.4"), "4.6", "5.4"),
])
def test_keys_without_an_exact_digest_stream_every_range_rather_than_trusting_counts(
        key, source_balance, edited, target_value, missing, extra):
    loans, borrowers = _rows(12)
    if source_balance:
        for r in loans:
            r["current_balance"] = source_balance(r["loan_id"])
    tgt = [dict(r, **({key: target_value} if r["loan_id"] == edited else {})) for r in loans]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_keyed(key), tol=Tolerances("t1", pk_set_ranges=4))
    assert result["merge_eligible"] is False
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["fingerprint"] == "unavailable: every range streamed"
    assert pk["stats"]["loans"]["mismatched_ranges"] == pk["stats"]["loans"]["ranges"]
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]
    assert missing in pk["findings"][0]["detail"] and extra in pk["findings"][1]["detail"]
    assert source.calls["whole_number_columns"] == target.calls["whole_number_columns"] == 2


def test_pk_set_stream_every_range_skips_fingerprints_and_streams_everything():
    loans, borrowers = _rows(40)
    source, target = _sides(loans, _tgt(loans), borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, pk_set_stream_every_range=True))
    assert result["verdict"] == "PASS", result
    stats = _tier(result, "pk_set_diff")["stats"]["loans"]
    assert stats["fingerprint"] == "not used: every range streamed (pk_set_stream_every_range)"
    assert stats["mismatched_ranges"] == stats["ranges"]
    assert stats["keys_streamed"] >= 2 * len(loans)
    assert source.calls["range_fingerprints"] == 0 and target.calls["range_fingerprints"] == 0


@pytest.mark.parametrize("aware", [False, True])
def test_in_flight_rows_are_not_defects_when_lag_is_tolerated(aware):
    loans, borrowers = _rows(12)
    # target applied everything up to loan 10 (watermark _ts(10)), stored aware or naive;
    # 11 and 12 are still in flight
    tgt = _tgt([r for r in loans if r["loan_id"] <= 10], aware)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == "PASS", result
    t1 = _tier(result, "counts_through_mapping")
    assert t1["stats"]["count_gap_within_in_flight"]["loans"] == {"gap": 2, "in_flight": 2}
    t2 = _tier(result, "per_field_aggregates")
    assert t2["passed"] and t2["stats"]["applied_subset"]["loans"] == {"in_flight": 2, "excluded_keys": 2}
    assert target.last_excluded_keys == [(11,), (12,)]
    assert _tier(result, "keyed_diffs")["stats"]["loans"]["in_flight_rows"] == 2
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_missing"] == 2
    cdc = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert cdc["lag_s"] == 2.0 and cdc["in_flight"] == 2
    assert result["merge_eligible"] is True
    assert ("- Consistency window: source isolation `fake_snapshot`, target isolation "
            "`fake_snapshot`, held; in flight at open: `{\"loans\": 2}`") in render_summary(result)


@pytest.mark.parametrize("rowversion, unit, step, lag_max_s, in_flight_max, verdict, codes, lag_s, lag_units", [
    # two rows in flight, counter behind by 2: below the 1000 s bound and above the 1 s bound,
    # neither of which means anything for a counter without a declared unit
    (False, None, 1, 1, 0, "FAIL", ["cdc_lag_ungraded"], None, 2),
    (False, None, 1, 1000, 0, "FAIL", ["cdc_lag_ungraded"], None, 2),
    # a counter is graded by unapplied rows, not seconds: 2000 units behind is 2 rows
    (False, "counter", 1000, 0, 2, "PASS", [], None, 2000),
    (False, "counter", 1000, 10_000, 1, "FAIL", ["cdc_in_flight_exceeded"], None, 2000),
    # an epoch scales to seconds by its unit: 2 rows behind = 2000 ms = 2 s, within 5 s...
    (False, "epoch_ms", 1000, 5, 0, "PASS", [], 2.0, 2000),
    (False, "epoch_s", 1000, 5, 0, "FAIL", ["cdc_lag_exceeded"], 2000.0, 2000),   # ...or 2000 s
    (False, "epoch_us", 1000, 5, 0, "PASS", [], 0.002, 2000),
    # a SQL Server rowversion opens the window and is graded like any counter
    (True, "counter", 1, 0, 2, "PASS", [], None, 2),
])
def test_a_numeric_watermark_is_graded_by_its_declared_unit(rowversion, unit, step, lag_max_s,
                                                            in_flight_max, verdict, codes, lag_s, lag_units):
    loans, tgt, borrowers = _counter_rows(behind=2, step=step, rowversion=rowversion)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec(unit),
                  tol=Tolerances("t1", cdc_lag_max_s=lag_max_s, cdc_in_flight_max_rows=in_flight_max))
    assert result["verdict"] == verdict, result
    assert result["merge_eligible"] is (verdict == "PASS")
    assert _codes(result, "cdc_lag_ordering") == codes
    stats = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert (stats["lag_s"], stats["lag_units"], stats["unit"]) == (lag_s, lag_units, unit)
    assert _codes(result, "pk_set_diff") == []


def test_a_rowversion_watermark_binds_as_the_engine_literal_and_meets_a_bigint_target():
    loans, tgt, borrowers = _counter_rows(behind=2, rowversion=True)
    # the in-flight predicate carries the binary literal the engine understands...
    source, target = _sides(loans, tgt, borrowers)
    ctx = open_window(_counter_spec("counter"), source, target)
    assert ctx.applied_where(_counter_spec("counter").objects[0]) == \
        "(version_no <= 0x00000000000003f2 OR version_no IS NULL)"
    # ...and a target that stores the counter as a bigint gets a plain number on its side
    for r in tgt:
        r["version_no"] = instant(r["version_no"])
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_lag_max_s=0, cdc_in_flight_max_rows=2))
    assert result["verdict"] == "PASS", result
    assert _tier(result, "cdc_lag_ordering")["stats"]["loans"]["lag_units"] == 2


@pytest.mark.parametrize("spec, unit", [(_spec(), "counter"), (_counter_spec("datetime"), "datetime")])
def test_a_declared_unit_that_does_not_fit_the_values_is_incomparable(spec, unit):
    loans, tgt, borrowers = _counter_rows(behind=0)
    loans_obj = dataclasses.replace(spec.objects[0], watermark_unit=unit)
    spec = MappingSpec("m1", [loans_obj, spec.objects[1]])
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=spec, tol=Tolerances("t1", cdc_lag_max_s=1000))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["cdc_watermark_incomparable"]


def test_lag_seconds_only_reads_a_number_under_an_epoch_unit():
    assert lag_seconds(10, 9) is None and lag_seconds(10, 9, "counter") is None
    assert lag_seconds(2000, 500, "epoch_ms") == 1.5
    assert lag_seconds(2000, 500, "epoch_s") == 1500.0
    assert lag_seconds(Decimal(3_000_000), 0, "epoch_us") == 3.0
    assert lag_units(10, 9) == 1 and lag_units(9, 10) == -1 and lag_units(None, 9) is None
    assert lag_units(_ts(3), _ts(0)) is None


def test_a_rowversion_watermark_is_the_unsigned_big_endian_counter_everywhere():
    lo, hi = _rowversion(0x7FFF_FFFF_FFFF_FFFF), _rowversion(0x8000_0000_0000_0000)
    # ordering: a signed reading would put `hi` below zero
    assert instant(hi) == 2**63 and later(hi, lo) and not later(lo, hi)
    assert later(bytearray(hi), memoryview(lo))
    # a side that stores the counter as a bigint (a Lakebase target) compares with the bytes
    check_comparable(_rowversion(2001), 2001, "rv")
    assert same(_rowversion(2001), 2001) and same(_rowversion(2001), Decimal(2001))
    assert not same(_rowversion(2001), 2000) and later(_rowversion(2001), 2000)
    assert lag_units(_rowversion(2003), _rowversion(2001)) == 2
    assert lag_units(_rowversion(2003), 2001) == 2 and lag_units(2001, _rowversion(2003)) == -2
    assert lag_seconds(_rowversion(2003), _rowversion(2001), "counter") is None
    # no portable exact digest for the raw bytes, and no catalog claim of wholeness upgrades them
    # (whatever the target made of the counter): such a watermark streams instead
    assert _kind(_rowversion(7)) == "binary"
    assert _digest_kind([_rowversion(7)], "rv", "rv", {"rv"}, {"rv"}) == "binary"
    assert _digest_kind([_rowversion(7), 7], "rv", "rv", {"rv"}, {"rv"}) == "other"
    # the target's bigint high-watermark takes the source column's binary form (and width) for
    # the source-side predicate; a fractional or oversized number stays as it is
    assert in_form_of(2001, _rowversion(9)) == _rowversion(2001)
    assert in_form_of(Decimal(2001), b"\x00\x01") == b"\x07\xd1"
    assert in_form_of(_rowversion(2001), 5) == 2001
    assert in_form_of(Decimal("2001.5"), _rowversion(9)) == Decimal("2001.5")
    assert in_form_of(70000, b"\x00\x01") == 70000
    assert in_form_of(T0, _rowversion(9)) is T0
    # a datetime is still refused against it
    with pytest.raises(ConfigError):
        check_comparable(_rowversion(1), T0, "rv")


def test_mapping_spec_loads_watermark_identity_and_unit(tmp_path):
    spec = {"version": "m1", "objects": [{
        "object": "loans", "root_table": "dbo.loans",
        "key": {"source": ["loan_id"], "target": "loan_id"},
        "fields": [{"source": "loan_number", "target": "loan_number"}],
        "watermark": {"source": "modified_date", "target": "modified_date", "unit": "counter"},
        "identity": {"source": "loan_id", "target": "loan_id"}}]}
    p = tmp_path / "m.json"
    p.write_text(json.dumps(spec))
    c = load_mapping_spec(p, {}).objects[0]
    assert (c.watermark_source, c.watermark_target, c.watermark_unit) == ("modified_date", "modified_date", "counter")
    assert (c.identity_source, c.identity_target) == ("loan_id", "loan_id")
    for watermark, match in [
        ({"source": "modified_date", "target": "modified_date", "unit": "seconds"}, "watermark unit must be one of"),
        ({"source": "modified_date"}, "watermark"),
        ({"source": "modified_date", "target": "x; drop"}, ""),
    ]:
        spec["objects"][0]["watermark"] = watermark
        p.write_text(json.dumps(spec))
        with pytest.raises(ConfigError, match=match):
            load_mapping_spec(p, {})


@pytest.mark.parametrize("target_edit, lag_max_s, depth, verdict, cdc_codes", [
    # target applied everything up to loan 10; 11 and 12 are still in flight, beyond the bound
    ({11: None, 12: None}, 1, "sampled", "FAIL", ["cdc_lag_exceeded"]),
    # replayed / applied out of order
    ({3: _ts(100)}, 1000, "sampled", "FAIL", ["row_ahead_of_source", "target_ahead_of_source"]),
    # the target applied up to loan 12 (its watermark) but loan 5's row still shows an older
    # modified_date than its source: the change was skipped or applied out of order
    ({5: _ts(1)}, 1000, "full", "FAIL", ["row_behind_applied_watermark"]),
    # loans 11 and 12 changed on the source after the target's applied watermark (_ts(10)): the
    # target still holds their previous versions, which is lag, not a defect
    ({11: _ts(9), 12: _ts(10)}, 5, "sampled", "PASS", []),
])
def test_lag_is_tolerated_but_misordering_and_lost_changes_are_findings(target_edit, lag_max_s, depth,
                                                                        verdict, cdc_codes):
    loans, borrowers = _rows(12)
    tgt = [dict(r, modified_date=target_edit.get(r["loan_id"], r["modified_date"]))
           for r in loans if target_edit.get(r["loan_id"], True) is not None]
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=lag_max_s), depth=depth)
    assert result["verdict"] == verdict, result
    assert _codes(result, "cdc_lag_ordering") == cdc_codes
    # in-flight rows are never double-reported as missing keys or ordering findings
    assert _codes(result, "pk_set_diff") == []
    assert ("row_ahead_of_source" in _codes(result, "keyed_diffs")) == ("row_ahead_of_source" in cdc_codes)
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_updates"] == (2 if verdict == "PASS" else 0)


@pytest.mark.parametrize("rowversion, ahead", [(False, 50), (True, _rowversion(5000))])
def test_a_counter_ahead_on_the_target_is_still_an_ordering_violation(rowversion, ahead):
    loans, tgt, borrowers = _counter_rows(behind=0, rowversion=rowversion)
    tgt[-1]["version_no"] = ahead
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_in_flight_max_rows=100))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["row_ahead_of_source", "target_ahead_of_source"]


def test_numeric_keys_digest_exactly_only_when_both_catalogs_declare_them_whole():
    class _UntypedSource(FakeSource):
        def whole_number_columns(self, table):
            raise NotImplementedError

    loans, borrowers = _rows(12)
    source, target = _sides(loans, _tgt(loans), borrowers)
    fp = _tier(_run(source, target), "pk_set_diff")["stats"]["loans"]["fingerprint"]
    assert fp.startswith("count+key_sum+key_sumsq")
    untyped = _UntypedSource(source.tables, schema=source.schema, sequences=source.sequences)
    fp = _tier(_run(untyped, target), "pk_set_diff")["stats"]["loans"]["fingerprint"]
    assert fp == "unavailable: every range streamed"


def test_digest_kind_is_exact_for_whole_numbers_both_catalogs_declare():
    assert _kind(7) == _kind(Decimal(7)) == _kind(Decimal("7E+2")) == "integer"
    assert _kind(Decimal("7.00")) == _kind(Decimal("1.0000001")) == _kind(7.5) == "number"
    assert _kind(True) == _kind("7") == "other"
    assert _kind(T0) == "datetime"
    # a column whose scale varies by row (unconstrained numeric) gets no digest at all
    assert _common_kind([Decimal(7), Decimal("7.5")]) == "other"
    assert _common_kind([None, 3, 4]) == "integer" and _common_kind([]) == "other"
    # datetimes need no catalog proof; numerics are `number` (streamed) until both sides prove them
    assert _digest_kind([T0], "a", "b", None, None) == "datetime"
    assert _digest_kind([1, 2], "a", "b", {"a"}, {"b"}) == "integer"
    assert _digest_kind([1, 2], "a", "b", {"a"}, set()) == "number"
    assert _digest_kind([1, 2], "a", "b", None, {"b"}) == "number"
    assert _digest_kind([Decimal("1.5")], "a", "b", {"a"}, {"b"}) == "integer"  # the catalog, not the sample, decides
    assert _digest_kind(["x"], "a", "b", {"a"}, {"b"}) == "other"


def _marker_only(source):
    source.pin = "none"
    return source


@pytest.mark.parametrize("accepted", [False, True])
def test_a_marker_only_window_is_accepted_only_by_a_recorded_tolerance(accepted):
    loans, borrowers = _rows(12)
    source, target = _sides(loans, _tgt(loans), borrowers)
    result = _run(_marker_only(source), target, tol=Tolerances("t1", accept_marker_only_window=accepted))
    window = _tier(result, "consistency_window")
    assert window["stats"]["strength"] == {"source": "markers", "target": "snapshot"}
    assert window["stats"]["isolation"]["source"] == "none"
    if accepted:
        assert result["verdict"] == "PASS" and result["merge_eligible"] is True
        assert window["stats"]["accepted_marker_only"] == ["source"]
    else:
        assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
        assert [f["check"] for f in window["findings"]] == ["window_unproven"]
        assert "source" in window["findings"][0]["detail"]
        assert ("- Consistency window: source isolation `none` (markers), target isolation "
                "`fake_snapshot`, UNPROVEN") in render_summary(result)


SWITCHES = ["accept_marker_only_window", "pk_set_stream_every_range", "accept_target_only_constraints"]
BOUNDS = ["cdc_lag_max_s", "numeric_abs_tol", "aggregate_rel_tol"]
COUNTS = ["pk_set_ranges", "sample_size", "source_concurrency"]
NOT_INTEGERS = [(-4, "non-negative JSON integer"), (True, "non-negative JSON integer"), ("64", "non-negative JSON integer"),
                (2.5, "non-negative JSON integer"), (None, "non-negative JSON integer")]


@pytest.mark.parametrize("key, value, needle", [
    *[(k, v, "JSON boolean") for k in SWITCHES for v in ["false", "true", "no", 0, 1, None, [], {}]],
    # json emits NaN/Infinity literals
    *[(k, v, n) for k in BOUNDS for v, n in [
        (float("nan"), "finite"), (float("inf"), "finite"), (-1, "finite and >= 0"),
        (True, "JSON number"), ("60", "JSON number"), (None, "JSON number"), ([], "JSON number")]],
    *[(k, v, n) for k in COUNTS for v, n in [(0, "positive JSON integer"), *NOT_INTEGERS]],
    # a zero is a real value for the counters that bound something (0 rows: always sample)
    *[(k, v, n) for k in ["cdc_in_flight_max_rows", "full_diff_row_threshold"] for v, n in NOT_INTEGERS],
])
def test_tolerances_must_be_typed_json_values(tmp_path, key, value, needle):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", key: value}))
    with pytest.raises(ConfigError, match=f"{key} must be .*{needle}"):
        load_tolerances(path)


def test_tolerances_load_typed_values_and_keep_defaults(tmp_path):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", "accept_marker_only_window": True, "pk_set_ranges": 16,
                                "full_diff_row_threshold": 0, "cdc_in_flight_max_rows": 3}))
    tol = load_tolerances(path)
    assert tol.accept_marker_only_window is True and tol.pk_set_stream_every_range is False
    assert tol.pk_set_ranges == 16 and tol.full_diff_row_threshold == 0 and tol.cdc_in_flight_max_rows == 3
    path.write_text(json.dumps({"version": "t1", "cdc_lag_max_s": 60}))
    tol = load_tolerances(path)
    assert tol.accept_marker_only_window is False and tol.pk_set_stream_every_range is False
    assert tol.accept_target_only_constraints is False and tol.cdc_lag_max_s == 60.0
    assert tol.sample_size == 1_000 and tol.source_concurrency == 1 and tol.cdc_in_flight_max_rows == 0


def _tokened(source, counter):
    """A marker-fallback source whose engine exposes a per-table write counter."""
    source.pin = "none"
    source.change_token = lambda table: counter[table]
    return source


def _edit_in_place(loans):
    # count and max(modified_date) are unchanged: loan 2 is edited in place
    loans[1]["current_balance"] += 1
    return 1


def _churn(loans):
    # delete loan 5 and insert a new loan 13 with an older watermark: count and max hold
    loans[:] = [r for r in loans if r["loan_id"] != 5] + [_loan(13, changed=1, borrower_id=1)]
    return 2


@pytest.mark.parametrize("write, markers_hold", [(_edit_in_place, True), (_churn, False)])
def test_a_write_below_the_max_watermark_during_fallback_is_caught_by_the_change_token(write, markers_hold):
    loans, borrowers = _rows(12)
    source, target = _sides(loans, _tgt(loans), borrowers)
    writes = {"dbo.loans": 10, "dbo.borrowers": 3}
    _tokened(source, writes)
    original = source.range_fingerprints

    def write_mid_run(*a, **kw):
        writes["dbo.loans"] += write(loans)
        return original(*a, **kw)
    source.range_fingerprints = write_mid_run
    result = _run(source, target)
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    window = _tier(result, "consistency_window")
    assert [f["check"] for f in window["findings"]] == ["window_unstable"]
    assert window["stats"]["strength"]["source"] == "change_token"
    assert "source isolation `none` (change_token), target isolation `fake_snapshot`, MOVED" in \
        render_summary(result)
    if markers_hold:
        assert window["stats"]["markers"]["loans"]["source_open"][:2] == \
            window["stats"]["markers"]["loans"]["source_close"][:2]


@pytest.mark.parametrize("target_fails, source_fails, discard_fails, raised", [
    ("target_row_count", None, None, "connection reset during tier 1"),   # a shared tier
    ("range_fingerprints", None, None, "lakebase branch went away"),       # a transactional tier
    ("window_marker", None, None, "permission denied for marker"),         # the opening marker read
    ("range_fingerprints", "close_window", None, "source rollback failed"),
    ("range_fingerprints", "close_window", "socket gone", "rollback failed"),  # not releasable at all
])
def test_a_raise_anywhere_in_the_run_still_closes_both_windows(target_fails, source_fails, discard_fails,
                                                               raised, capsys):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, _tgt(loans), borrowers)
    target.fail_on[target_fails] = RuntimeError("tier failure" if source_fails else raised)
    if source_fails:
        source.fail_on[source_fails] = RuntimeError(raised)
    if discard_fails:
        source.fail_on["discard"] = RuntimeError(discard_fails)
    with pytest.raises(RuntimeError) as info:
        _run(source, target)
    # the run's own error propagates; a failed release is attached to it, never in its place
    assert str(info.value) == ("tier failure" if source_fails else raised)
    assert target.calls["close_window"] == 1 and not target.window_open
    assert source.calls["close_window"] == 1 and source.window_open is bool(discard_fails)
    notes = [f"source close_window failed: RuntimeError({raised!r})"] if source_fails else []
    if discard_fails:
        notes.append(f"source connection could not be dropped: RuntimeError({discard_fails!r})")
    assert getattr(info.value, "__notes__", []) == notes
    assert source.calls["discard"] == (1 if source_fails else 0)  # the connection is dropped
    # release failures are printed before the error propagates
    assert capsys.readouterr().err == "".join(f"dbx-recon: {n}\n" for n in notes)


def test_field_diff_is_still_graded_for_applied_rows():
    loans, borrowers = _rows(6)
    tgt = _tgt(loans)
    tgt[1]["current_balance"] = 0
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target)
    assert "field_diff" in _codes(result, "keyed_diffs")


def test_objects_without_watermark_are_graded_strictly():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, _tgt(loans)[:-1], borrowers)
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
    source, target = _sides(loans, _tgt(loans), borrowers)
    with pytest.raises(ConfigError, match="TransactionalSide"):
        _run(source, Plain())
    with pytest.raises(ConfigError, match="TransactionalSide"):
        _run(Plain(), target)


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


def _cli_run(*extra):
    return main(["run", "--unit", "u", "--family", "sqlserver", "--mapping", "m.json",
                 "--tolerances", "t.json", "--canonicalization", "c.json",
                 "--mode", "transactional", "--source-dsn-secret", "S", "--target-secret", "T",
                 *extra, "--target-catalog", "mig", "--target-schema", "s", "--out", "o"])


def test_cli_refuses_transactional_mode_against_a_delta_target(tmp_path, monkeypatch):
    _write_inputs(tmp_path, monkeypatch)
    with pytest.raises(SystemExit) as exc:
        _cli_run()
    msg = str(exc.value)
    assert "transactional" in msg and "lakebase" in msg and "not implemented" in msg


def test_cli_refuses_a_lakebase_dsn_outside_the_allowlisted_database(tmp_path, monkeypatch):
    _write_inputs(tmp_path, monkeypatch)
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("S", "src")
    monkeypatch.setenv("T", "dsn-under-test")
    monkeypatch.setattr(psycopg, "connect", lambda dsn: _db("somewhere_else"))
    monkeypatch.setitem(SOURCE_ADAPTERS, "sqlserver", lambda secret: FakeSource({}))
    with pytest.raises(SystemExit) as exc:
        _cli_run("--target-kind", "lakebase")
    assert "'somewhere_else'" in str(exc.value) and "'mig'" in str(exc.value)
    assert "dsn-under-test" not in str(exc.value)


def test_cli_estimate_accepts_mode(tmp_path, monkeypatch, capsys):
    _write_inputs(tmp_path, monkeypatch)
    assert main(["estimate", "--mapping", "m.json", "--tolerances", "t.json",
                 "--mode", "transactional"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["mode"] == "transactional" and "tier5" in out["source_statements"]


class _ClosedStderr(io.StringIO):
    def write(self, s):
        raise OSError("stderr closed")


@pytest.mark.parametrize("version, stderr", [((3, 10, 0), None), ((3, 12, 0), None), ((3, 12, 0), _ClosedStderr)])
def test_release_failures_reach_the_exception_on_every_runtime_even_with_stderr_gone(monkeypatch, capsys, version,
                                                                                    stderr):
    from recon import engine
    monkeypatch.setattr(engine.sys, "version_info", version)
    if stderr:
        monkeypatch.setattr(engine.sys, "stderr", stderr())
    exc = RuntimeError("tier failure")
    engine._report_release_failure(exc, "source close_window failed")
    engine._report_release_failure(exc, "source connection could not be dropped")
    assert exc.__notes__ == ["source close_window failed", "source connection could not be dropped"]
    assert str(exc) == "tier failure"
    assert capsys.readouterr().err.splitlines() == ([] if stderr else [
        "dbx-recon: source close_window failed", "dbx-recon: source connection could not be dropped"])


def test_drift_in_an_applied_row_fails_aggregates_while_other_rows_are_in_flight():
    # loans 39 and 40 are in flight; loan 7 was applied long ago but its target balance drifted.
    # Tier 3 is sampled (2 keys + range edges) and never visits key 7; tier 2 must still see it.
    loans, borrowers = _rows(40)
    tgt = [dict(r) for r in loans if r["loan_id"] <= 38]
    tgt[6]["current_balance"] = 999_999
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5, sample_size=2),
                  depth="sampled", seed=3)
    assert (7,) not in source.last_fetch_keyed["keys"], "pick a seed whose sample misses key 7"
    assert _codes(result, "keyed_diffs") == []
    t2 = _tier(result, "per_field_aggregates")
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert {f["check"] for f in t2["findings"]} >= {"aggregate_sum", "aggregate_max"}
    assert all(f["object"] == "loans" and "current_balance" in f["detail"] for f in t2["findings"])
    assert t2["stats"]["applied_subset"]["loans"] == {"in_flight": 2, "excluded_keys": 2}
    assert target.last_excluded_keys == [(39,), (40,)]
    # the applied rows agree on every other field: no false findings from the in-flight rows
    assert not any("loan_number" in f["detail"] or "borrower_id" in f["detail"] for f in t2["findings"])


@pytest.mark.parametrize("undeclared, excluding", [(False, 1), (True, 2)])
def test_in_flight_rows_with_stale_target_values_do_not_fail_aggregates(undeclared, excluding):
    # the target still holds the pre-change values of two rows the source has since updated:
    # exactly the case that used to force tier 2 to skip the whole object
    loans, borrowers = _rows(12)
    tgt = _tgt(loans)
    for r in loans[10:]:
        r["current_balance"] += 500
        r["modified_date"] = _ts(20)
    spec = _spec()
    if undeclared:
        # the target side of an undeclared field is probed in isolation; under in-flight exclusion
        # that probe must run over the same applied set as the batched statement, not the whole table
        fields = [dataclasses.replace(f, target_type="") if f.target == "current_balance" else f
                  for f in spec.objects[0].fields]
        spec = MappingSpec("m1", [dataclasses.replace(spec.objects[0], fields=fields), spec.objects[1]])
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=15), spec=spec)
    assert result["verdict"] == "PASS", result
    t2 = _tier(result, "per_field_aggregates")
    assert t2["checks_run"] == 4 and _codes(result, "per_field_aggregates") == []
    assert t2["stats"]["applied_subset"]["loans"]["excluded_keys"] == 2
    assert source.calls["table_aggregates"] == 2
    assert target.calls["table_aggregates_excluding"] == excluding and target.calls["field_aggregates"] == 0


def test_a_target_that_cannot_exclude_keys_leaves_aggregates_ungraded_not_green():
    class NoExclusion(FakeTarget):
        table_aggregates_excluding = None
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans if r["loan_id"] <= 10]
    source, _ = _sides(loans, tgt, borrowers)
    target = NoExclusion({"loans": tgt, "borrowers": [dict(b) for b in borrowers]},
                         schema={"loans": TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                         sequences={("loans", "loan_id"): 13})
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "per_field_aggregates") == ["aggregates_ungraded_in_flight"]


def _wide_key_estate(in_flight: int, width: int = 7):
    cols = [f"k{j}" for j in range(width)]
    rows = [{**{c: (i if j == 0 else 1) for j, c in enumerate(cols)}, "v": i, "modified_date": _ts(i)}
            for i in range(1, 400 + 1)]
    tgt = [dict(r) for r in rows[:len(rows) - in_flight]]
    facts = SchemaFacts(primary_key=tuple(cols), not_null=set(cols))
    spec = MappingSpec("m1", [ObjectMapping(
        object="wide", root_table="dbo.wide", key_source=cols, key_target=cols,
        fields=[FieldMapping("v", "v", "int", "int")],
        watermark_source="modified_date", watermark_target="modified_date")])
    source = FakeSource({"dbo.wide": rows}, schema={"dbo.wide": facts})
    target = FakeTarget({"wide": tgt}, schema={"wide": facts})
    return spec, source, target


@pytest.mark.parametrize("in_flight, graded", [(285, True), (300, False)])
def test_wide_key_exclusion_is_capped_by_bound_parameters_not_row_count(in_flight, graded):
    # 300 seven-column keys are far under IN_FLIGHT_EXCLUSION_CAP but need 2100 parameters,
    # more than one statement carries; the object is graded ungraded, never split or aborted
    spec, source, target = _wide_key_estate(in_flight)
    result = run_recon("u1", "transactional", spec, Tolerances("t1", cdc_lag_max_s=1000), [],
                       source, target)
    t2 = _tier(result, "per_field_aggregates")
    if graded:
        assert result["verdict"] == "PASS", result
        assert target.calls["table_aggregates_excluding"] == 1
        assert t2["stats"]["applied_subset"]["wide"]["excluded_keys"] == in_flight
    else:
        assert result["verdict"] == "FAIL"
        assert _codes(result, "per_field_aggregates") == ["aggregates_ungraded_in_flight"]
        assert "285-key exclusion cap for a 7-column key" in t2["findings"][0]["detail"]
        assert target.calls["table_aggregates_excluding"] == 0


def test_watermark_comparator_treats_naive_as_utc_and_never_compares_lexically():
    naive = dt.datetime(2026, 9, 1, 12, 0, 0)  # noqa: DTZ001  the SQL Server datetime shape
    aware_same = dt.datetime(2026, 9, 1, 14, 0, 0, tzinfo=PLUS2)   # same instant, +02:00
    aware_utc = dt.datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)
    assert instant(aware_same) == instant(aware_utc) == naive
    assert same(naive, aware_same) and same(aware_same, aware_utc)
    assert not later(naive, aware_same) and not later(aware_same, naive)
    assert later(aware_same + dt.timedelta(microseconds=1), naive)
    assert later(naive + dt.timedelta(microseconds=1), aware_same)
    assert lag_seconds(naive, aware_same) == 0.0
    assert lag_seconds(naive + dt.timedelta(seconds=3), aware_same) == 3.0
    # 9 > 10 lexically is not a thing: numbers compare as numbers
    assert not later(9, 10) and later(10, 9)
    # a datetime against a number is a mapping error, not a string comparison
    with pytest.raises(ConfigError, match="not comparable"):
        later(naive, 10)


def test_tier3_and_tier5_ordering_use_instants_across_tz_representations():
    # loan 4's target row is one second NEWER than the source (as instants) though its +02:00
    # wall clock reads two hours ahead of every naive value; loan 6 is one second OLDER
    loans, borrowers = _rows(12)
    tgt = _tgt(loans, aware=True)
    tgt[3]["modified_date"] += dt.timedelta(seconds=1)
    tgt[5]["modified_date"] -= dt.timedelta(seconds=1)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, depth="full")
    assert result["verdict"] == "FAIL"
    t3 = {f["check"]: f["detail"] for f in _tier(result, "keyed_diffs")["findings"]}
    assert "key=(4,)" in t3["row_ahead_of_source"] and "(6,)" not in t3["row_ahead_of_source"]
    cdc = {f["check"]: f["detail"] for f in _tier(result, "cdc_lag_ordering")["findings"]}
    assert "(4,)" in cdc["row_ahead_of_source"] and "(6,)" in cdc["row_behind_applied_watermark"]
    assert "(6,)" not in cdc["row_ahead_of_source"] and "(4,)" not in cdc["row_behind_applied_watermark"]


def test_a_datetime_watermark_against_a_numeric_one_is_refused_before_any_tier_runs():
    loans, borrowers = _rows(12)
    tgt = [dict(r, modified_date=i) for i, r in enumerate(loans, 1)]
    source, target = _sides(loans, tgt, borrowers)
    with pytest.raises(ConfigError, match="loans: modified_date vs modified_date.*not comparable"):
        _run(source, target)
    assert source.calls["range_fingerprints"] == 0
    assert source.window_open is False and target.window_open is False


def test_transactional_predicates_use_the_source_engine_literal(monkeypatch):
    # the in-flight and applied predicates are evaluated by the source, so its rendering wins
    rows, borrowers = _rows(12)
    source, target = _sides(rows, _tgt(rows), borrowers)
    wheres: list[str] = []
    monkeypatch.setattr(source, "watermark_literal", lambda value: "<SRC>")
    monkeypatch.setattr(source, "row_count", lambda table, where=None: wheres.append(where) or 0)
    ctx = open_window(_spec(), source, target)
    assert "modified_date >= <SRC>" in wheres
    assert ctx.applied_where(_spec().objects[0]) == "(modified_date < <SRC> OR modified_date IS NULL)"


def test_range_fingerprints_carry_the_watermark_null_count():
    loans, borrowers = _rows(4)
    loans[1]["modified_date"] = None
    source, _ = _sides(loans, _tgt(loans), borrowers)
    (n, keys, wm), = source.range_fingerprints("dbo.loans", ["loan_id"], ["integer"], "modified_date",
                                              "datetime", [(None, None)])
    assert n == 4 and keys[0][0] == 10 and len(wm) == 3 and wm[2] == 1


def _undeclared_spec() -> MappingSpec:
    # a field with no declared target type: tier 2 has to find out whether it takes a SUM
    loans = _spec().objects[0]
    fields = [*loans.fields, FieldMapping("loan_status", "loan_status", "", "")]
    return MappingSpec("m1", [dataclasses.replace(loans, fields=fields), _spec().objects[1]])


class _ShoutingCatalog(FakeTypedSource):
    """SQL Server's catalog reports the declared column case; the mapping may spell it differently."""

    def numeric_columns(self, table: str) -> set[str]:
        return {c.upper() for c in super().numeric_columns(table)}


@pytest.mark.parametrize("catalog, status, drift", [
    (FakeTypedSource, "ACTIVE", None),
    # applied-row numeric drift only a SUM can see (min/max/nulls agree)
    (_ShoutingCatalog, 3, 4),
])
def test_a_typed_source_is_never_probed_for_undeclared_fields(catalog, status, drift):
    loans, borrowers = _rows(12)
    for r in loans:
        r["loan_status"] = status
    tgt = _tgt(loans)
    if drift:
        tgt[0]["loan_status"] = drift
    source = catalog({"dbo.loans": loans, "dbo.borrowers": borrowers},
                     schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                     sequences={("dbo.loans", "loan_id"): 13})
    _, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_undeclared_spec())
    # the one typing read replaces the per-field probe; the identity bounds read stays
    assert source.calls["numeric_columns"] == 1
    assert source.calls["field_aggregates"] == 1
    assert _tier(result, "consistency_window")["stats"]["strength"]["source"] == "snapshot"
    if drift:
        assert result["verdict"] == "FAIL"
        assert any(f["check"] == "aggregate_sum" and "loan_status" in f["detail"]
                   for f in _tier(result, "per_field_aggregates")["findings"]), result
    else:
        assert result["verdict"] == "PASS", result


def test_a_probe_that_releases_the_source_snapshot_fails_the_window():
    # a source with no column typing falls back to the probe; if that probe rolls the pinned
    # transaction back, the window is lost and the run says so instead of reporting a snapshot
    loans, borrowers = _rows(12)
    for r in loans:
        r["loan_status"] = "ACTIVE"
    source, target = _sides(loans, _tgt(loans), borrowers)
    original = source.sum_probe

    def probing(table, column, where=None):
        if column == "loan_status":
            source.isolation = "none"   # what a libpq rollback does to a REPEATABLE READ window
        return original(table, column, where)
    source.sum_probe = probing
    result = _run(source, target, spec=_undeclared_spec())
    window = _tier(result, "consistency_window")
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert [f["check"] for f in window["findings"]] == ["window_lost", "window_unproven"]
    assert "rolled the transaction back" in window["findings"][0]["detail"]
