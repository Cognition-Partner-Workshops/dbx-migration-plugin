"""--mode transactional: consistency window, PK-set diff, CDC lag/ordering, schema parity."""

import dataclasses
import datetime as dt
import json
from decimal import Decimal

import pytest

from recon.adapters import (
    SOURCE_ADAPTERS,
    LakebaseTargetAdapter,
    SchemaFacts,
    TargetIdentityError,
    _fk_action,
    _index_key_text,
    quote_ident,
    _PostgresBase,
    _SqlAdapterBase,
    SqlServerSourceAdapter,
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
from recon.transactional import (
    _applied_predicate,
    _check_key_text,
    _common_kind,
    _digest_kind,

    _kind,
    _map_expression,
    _newer_predicate,
    open_window,
)
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

T0 = dt.datetime(2026, 9, 1, 12, 0, 0)
EPOCH = dt.datetime(1970, 1, 1)
UTC = dt.timezone.utc
PLUS2 = dt.timezone(dt.timedelta(hours=2))


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
    checks={"([Current_Balance]>=(0))", "([Loan_Status]='FC' OR [Loan_Status]='DL' OR [Loan_Status]='AC')"},
    identity_columns={"loan_id"})

TARGET_LOANS_FACTS = SchemaFacts(
    primary_key=("loan_id",), unique={("loan_number",)},
    foreign_keys={(("borrower_id",), "loan_servicing.borrowers", ("borrower_id",))},
    not_null={"loan_id", "loan_number", "current_balance", "modified_date", "borrower_id"},
    indexes={("borrower_id",), ("loan_status", "days_past_due", "loan_id")}, check_count=2,
    checks={"CHECK ((current_balance >= (0)::numeric))",
            "CHECK (((loan_status)::text = ANY ((ARRAY['AC'::character varying, 'DL'::character varying, "
            "'FC'::character varying])::text[])))"},
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


def _rows(n: int = 12, keys=None) -> tuple[list[dict], list[dict]]:
    keys = list(keys) if keys is not None else range(1, n + 1)
    loans = [_loan(k, changed=i, borrower_id=1 + i % 3) for i, k in enumerate(keys, 1)]
    borrowers = [{"borrower_id": b, "name": f"B{b}"} for b in (1, 2, 3)]
    return loans, borrowers


EVEN_KEYS = [2 * i for i in range(1, 41)]


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
    loans, borrowers = _rows(keys=EVEN_KEYS)
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


def _counter_spec(unit: str | None) -> MappingSpec:
    """The loans mapping keyed on a numeric change column (rowversion / version_no)."""
    spec = _spec()
    loans = dataclasses.replace(spec.objects[0], watermark_source="version_no",
                                watermark_target="version_no", watermark_unit=unit)
    return MappingSpec("m1", [loans, spec.objects[1]])


def _counter_rows(behind: int, step: int = 1):
    """12 source rows versioned 1*step..12*step; the target lacks the last `behind` of them."""
    loans, borrowers = _rows(12)
    for i, r in enumerate(loans, 1):
        r["version_no"] = i * step
    tgt = [dict(r) for r in loans[:12 - behind]]
    return loans, tgt, borrowers


@pytest.mark.parametrize("lag_max_s", [1, 1000])
def test_a_numeric_watermark_without_a_unit_is_never_graded_as_seconds(lag_max_s):
    # two rows in flight, counter behind by 2: below the 1000s bound and above the 1s bound,
    # neither of which means anything for a counter
    loans, tgt, borrowers = _counter_rows(behind=2)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec(None),
                  tol=Tolerances("t1", cdc_lag_max_s=lag_max_s))
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    assert _codes(result, "cdc_lag_ordering") == ["cdc_lag_ungraded"]
    stats = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert stats["lag_s"] is None and stats["lag_units"] == 2 and stats["unit"] is None
    assert _codes(result, "pk_set_diff") == []


def test_a_counter_watermark_is_graded_by_unapplied_rows_not_seconds():
    loans, tgt, borrowers = _counter_rows(behind=2, step=1000)  # 2000 units behind
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_lag_max_s=0, cdc_in_flight_max_rows=2))
    assert result["verdict"] == "PASS", result
    assert _tier(result, "cdc_lag_ordering")["stats"]["loans"]["lag_units"] == 2000
    source, target = _sides(loans, [dict(r) for r in tgt], borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_lag_max_s=10_000, cdc_in_flight_max_rows=1))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["cdc_in_flight_exceeded"]


def test_a_counter_ahead_on_the_target_is_still_an_ordering_violation():
    loans, tgt, borrowers = _counter_rows(behind=0)
    tgt[-1]["version_no"] = 50
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_in_flight_max_rows=100))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["row_ahead_of_source", "target_ahead_of_source"]


@pytest.mark.parametrize("unit, step, verdict", [
    ("epoch_ms", 1000, "PASS"),   # 2 rows behind = 2000 ms = 2 s, within 5 s
    ("epoch_s", 1000, "FAIL"),    # the same numbers read as seconds: 2000 s
    ("epoch_us", 1000, "PASS"),
])
def test_an_epoch_watermark_scales_to_seconds_by_its_declared_unit(unit, step, verdict):
    loans, tgt, borrowers = _counter_rows(behind=2, step=step)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec(unit), tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == verdict, result
    codes = _codes(result, "cdc_lag_ordering")
    assert codes == ([] if verdict == "PASS" else ["cdc_lag_exceeded"])
    lag = _tier(result, "cdc_lag_ordering")["stats"]["loans"]["lag_s"]
    assert lag == {"epoch_ms": 2.0, "epoch_s": 2000.0, "epoch_us": 0.002}[unit]


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


def _rowversion(n: int) -> bytes:
    """A SQL Server rowversion as pyodbc returns it: 8 bytes, unsigned big-endian."""
    return n.to_bytes(8, "big")


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
    assert _NoSnapshotAdapter(_StubConn())._digest_sql("rv", "binary") is None
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


def test_sql_server_renders_a_rowversion_bound_as_a_binary_literal():
    rv = _rowversion(2001)
    mssql, pg = _SqlServerLike(_StubConn()), _PostgresLike(_StubConn())
    assert mssql.watermark_literal(rv) == "0x00000000000007d1"
    assert _newer_predicate("rv", rv, mssql.watermark_literal) == "rv > 0x00000000000007d1"
    assert _applied_predicate("rv", rv, mssql.watermark_literal) == \
        "(rv <= 0x00000000000007d1 OR rv IS NULL)"
    assert mssql.watermark_literal(bytearray(rv)) == mssql.watermark_literal(memoryview(rv))
    # a bytea target column takes the same 8 bytes in Postgres syntax
    assert pg.watermark_literal(rv) == "'\\x00000000000007d1'::bytea"
    # a target that converted the counter to a bigint hands back a plain number
    assert mssql.watermark_literal(2001) == "2001"


def _rowversion_rows(behind: int):
    loans, tgt, borrowers = _counter_rows(behind=behind, step=1)
    for r in loans:
        r["version_no"] = _rowversion(1000 + r["version_no"])
    tgt = [dict(r) for r in loans[:12 - behind]]
    return loans, tgt, borrowers


def test_a_rowversion_watermark_opens_the_window_and_grades_by_unapplied_rows():
    loans, tgt, borrowers = _rowversion_rows(behind=2)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_lag_max_s=0, cdc_in_flight_max_rows=2))
    assert result["verdict"] == "PASS", result
    stats = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert stats["lag_units"] == 2 and stats["lag_s"] is None
    assert _codes(result, "pk_set_diff") == []
    # the in-flight predicate carried the binary literal the engine understands, and a target
    # that stores the counter as a bigint gets a plain number on its side
    source, target = _sides(loans, tgt, borrowers)
    ctx = open_window(_counter_spec("counter"), source, target)
    assert ctx.applied_where(_counter_spec("counter").objects[0]) == \
        "(version_no <= 0x00000000000003f2 OR version_no IS NULL)"
    for r in tgt:
        r["version_no"] = instant(r["version_no"])
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_lag_max_s=0, cdc_in_flight_max_rows=2))
    assert result["verdict"] == "PASS", result
    assert _tier(result, "cdc_lag_ordering")["stats"]["loans"]["lag_units"] == 2


def test_a_rowversion_target_ahead_of_the_source_is_an_ordering_violation():
    loans, tgt, borrowers = _rowversion_rows(behind=0)
    tgt[-1]["version_no"] = _rowversion(5000)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_counter_spec("counter"),
                  tol=Tolerances("t1", cdc_in_flight_max_rows=100))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "cdc_lag_ordering") == ["row_ahead_of_source", "target_ahead_of_source"]


def test_mapping_watermark_unit_is_validated_and_loaded(tmp_path):
    base = {"version": "m1", "objects": [{
        "object": "loans", "root_table": "dbo.loans", "key": {"source": ["id"], "target": ["id"]},
        "fields": [], "watermark": {"source": "rv", "target": "rv", "unit": "counter"}}]}
    path = tmp_path / "m.json"
    path.write_text(json.dumps(base))
    assert load_mapping_spec(path).objects[0].watermark_unit == "counter"
    base["objects"][0]["watermark"]["unit"] = "seconds"
    path.write_text(json.dumps(base))
    with pytest.raises(ConfigError, match="watermark unit must be one of"):
        load_mapping_spec(path)


@pytest.mark.parametrize("value", [True, -1, 1.5, "2", None])
def test_cdc_in_flight_max_rows_must_be_a_non_negative_integer(tmp_path, value):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", "cdc_in_flight_max_rows": value}))
    with pytest.raises(ConfigError, match="cdc_in_flight_max_rows must be a non-negative JSON integer"):
        load_tolerances(path)
    path.write_text(json.dumps({"version": "t1", "cdc_in_flight_max_rows": 3}))
    assert load_tolerances(path).cdc_in_flight_max_rows == 3


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


def test_fractional_numeric_keys_stream_every_range_rather_than_trusting_a_rounded_digest():
    # two keys that differ beyond the sixth decimal must never collapse into one digest
    loans, borrowers = _rows(12)
    for r in loans:
        r["current_balance"] = Decimal("1000.0000000") + Decimal(r["loan_id"]) * Decimal("0.0000001")
    spec = _spec()
    spec.objects[0].key_source[:] = ["current_balance"]
    spec.objects[0].key_target[:] = ["current_balance"]
    tgt = [dict(r) for r in loans]
    tgt[6]["current_balance"] = Decimal("1000.0000099")
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=spec, tol=Tolerances("t1", pk_set_ranges=4))
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["fingerprint"] == "unavailable: every range streamed"
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]
    assert "1000.0000007" in pk["findings"][0]["detail"] and "1000.0000099" in pk["findings"][1]["detail"]


def test_a_fractional_key_between_whole_range_bounds_is_not_digested_as_an_integer():
    # every stratum bound is whole (1, 3, 6, 9, 12) yet one interior key is fractional on each
    # side; rounded into DECIMAL(38,0) both would digest as 5 and the range would look equal
    loans, borrowers = _rows(12)
    for r in loans:
        r["current_balance"] = Decimal(r["loan_id"])
    loans[4]["current_balance"] = Decimal("4.6")
    spec = _spec()
    spec.objects[0].key_source[:] = ["current_balance"]
    spec.objects[0].key_target[:] = ["current_balance"]
    tgt = [dict(r) for r in loans]
    tgt[4]["current_balance"] = Decimal("5.4")
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=spec, tol=Tolerances("t1", pk_set_ranges=4))
    pk = _tier(result, "pk_set_diff")
    assert pk["stats"]["loans"]["fingerprint"] == "unavailable: every range streamed"
    assert [f["check"] for f in pk["findings"]] == ["pk_missing_on_target", "pk_extra_on_target"]
    assert "4.6" in pk["findings"][0]["detail"] and "5.4" in pk["findings"][1]["detail"]
    assert result["merge_eligible"] is False
    assert source.calls["whole_number_columns"] == target.calls["whole_number_columns"] == 2


def test_numeric_keys_digest_exactly_only_when_both_catalogs_declare_them_whole():
    class _UntypedSource(FakeSource):
        def whole_number_columns(self, table):
            raise NotImplementedError

    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    source, target = _sides(loans, tgt, borrowers)
    fp = _tier(_run(source, target), "pk_set_diff")["stats"]["loans"]["fingerprint"]
    assert fp.startswith("count+key_sum+key_sumsq")
    untyped = _UntypedSource(source.tables, schema=source.schema, sequences=source.sequences)
    fp = _tier(_run(untyped, target), "pk_set_diff")["stats"]["loans"]["fingerprint"]
    assert fp == "unavailable: every range streamed"
    # datetimes need no catalog proof; numerics are `number` (streamed) until both sides prove them
    assert _digest_kind([T0], "a", "b", None, None) == "datetime"
    assert _digest_kind([1, 2], "a", "b", {"a"}, {"b"}) == "integer"
    assert _digest_kind([1, 2], "a", "b", {"a"}, set()) == "number"
    assert _digest_kind([1, 2], "a", "b", None, {"b"}) == "number"
    assert _digest_kind([Decimal("1.5")], "a", "b", {"a"}, {"b"}) == "integer"  # the catalog, not the sample, decides
    assert _digest_kind(["x"], "a", "b", {"a"}, {"b"}) == "other"


def test_digest_kind_is_exact_for_whole_numbers_only():
    assert _kind(7) == _kind(Decimal(7)) == _kind(Decimal("7E+2")) == "integer"
    assert _kind(Decimal("7.00")) == _kind(Decimal("1.0000001")) == _kind(7.5) == "number"
    assert _kind(True) == _kind("7") == "other"
    assert _kind(T0) == "datetime"
    # a column whose scale varies by row (unconstrained numeric) gets no digest at all
    assert _common_kind([Decimal(7), Decimal("7.5")]) == "other"
    assert _common_kind([None, 3, 4]) == "integer" and _common_kind([]) == "other"
    base = _NoSnapshotAdapter(_StubConn())
    digest, square = base._digest_sql("k", "integer")
    assert digest == "CAST(k AS DECIMAL(38,0))"
    assert "DECIMAL(38,6)" not in square
    assert base._digest_sql("k", "number") is None is base._digest_sql("k", "other")


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


@pytest.mark.parametrize("flag", ["accept_marker_only_window", "pk_set_stream_every_range",
                                  "accept_target_only_constraints"])
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
    path.write_text(json.dumps({"version": "t1", "cdc_lag_max_s": 60}))
    tol = load_tolerances(path)
    assert tol.accept_marker_only_window is False and tol.pk_set_stream_every_range is False
    assert tol.accept_target_only_constraints is False and tol.cdc_lag_max_s == 60.0


@pytest.mark.parametrize("key", ["cdc_lag_max_s", "numeric_abs_tol", "aggregate_rel_tol"])
@pytest.mark.parametrize("value, needle", [
    (float("nan"), "finite"), (float("inf"), "finite"), (-1, "finite and >= 0"),
    (True, "JSON number"), ("60", "JSON number"), (None, "JSON number"), ([], "JSON number"),
])
def test_tolerance_bounds_must_be_finite_non_negative_numbers(tmp_path, key, value, needle):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", key: value}))  # json emits NaN/Infinity literals
    with pytest.raises(ConfigError, match=f"{key} must be .*{needle}"):
        load_tolerances(path)


@pytest.mark.parametrize("key", ["pk_set_ranges", "sample_size", "source_concurrency"])
@pytest.mark.parametrize("value, needle", [
    (0, "positive JSON integer"), (-4, "non-negative JSON integer"), (True, "non-negative JSON integer"),
    ("64", "non-negative JSON integer"), (2.5, "non-negative JSON integer"), (None, "non-negative JSON integer"),
])
def test_tolerance_plan_counts_must_be_positive_integers(tmp_path, key, value, needle):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", key: value}))
    with pytest.raises(ConfigError, match=f"{key} must be .*{needle}"):
        load_tolerances(path)


def test_tolerance_counts_load_integers_and_keep_defaults(tmp_path):
    path = tmp_path / "tol.json"
    path.write_text(json.dumps({"version": "t1", "pk_set_ranges": 16, "full_diff_row_threshold": 0}))
    tol = load_tolerances(path)
    assert tol.pk_set_ranges == 16 and tol.full_diff_row_threshold == 0  # 0 rows: always sample
    assert tol.sample_size == 1_000 and tol.source_concurrency == 1
    path.write_text(json.dumps({"version": "t1", "full_diff_row_threshold": -1}))
    with pytest.raises(ConfigError, match="full_diff_row_threshold must be a non-negative JSON integer"):
        load_tolerances(path)


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


@pytest.mark.parametrize("target_fails, source_fails, raised", [
    ("target_row_count", None, "connection reset during tier 1"),   # a shared tier
    ("range_fingerprints", None, "lakebase branch went away"),       # a transactional tier
    ("window_marker", None, "permission denied for marker"),         # the opening marker read
    ("range_fingerprints", "close_window", "source rollback failed"),
])
def test_a_raise_anywhere_in_the_run_still_closes_both_windows(target_fails, source_fails, raised):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.fail_on[target_fails] = RuntimeError("tier failure" if source_fails else raised)
    if source_fails:
        source.fail_on[source_fails] = RuntimeError(raised)
    with pytest.raises(RuntimeError) as info:
        _run(source, target)
    # the run's own error propagates; a failed release is attached to it, never in its place
    assert str(info.value) == ("tier failure" if source_fails else raised)
    assert target.calls["close_window"] == 1 and not target.window_open
    assert source.calls["close_window"] == 1
    if source_fails:
        assert info.value.__notes__ == [f"source close_window failed: RuntimeError({raised!r})"]
        assert source.calls["discard"] == 1 and not source.window_open  # the connection is dropped
    else:
        assert not hasattr(info.value, "__notes__") and source.calls["discard"] == 0
        assert not source.window_open


def test_a_side_that_cannot_be_released_at_all_is_reported_on_the_original_error():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.fail_on["range_fingerprints"] = RuntimeError("tier failure")
    source.fail_on["close_window"] = RuntimeError("rollback failed")
    source.fail_on["discard"] = RuntimeError("socket gone")
    with pytest.raises(RuntimeError) as info:
        _run(source, target)
    assert str(info.value) == "tier failure" and info.value.__notes__ == [
        "source close_window failed: RuntimeError('rollback failed')",
        "source connection could not be dropped: RuntimeError('socket gone')"]
    assert target.calls["close_window"] == 1 and not target.window_open


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
        "check_constraint_missing", "check_constraint_missing"])
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


def _facts(base: SchemaFacts, **over) -> SchemaFacts:
    return dataclasses.replace(base, **over)


def _tightened(**over) -> SchemaFacts:
    return _facts(TARGET_LOANS_FACTS, **over)


@pytest.mark.parametrize("src, tgt, code, needle", [
    (_facts(LOANS_FACTS, not_null=LOANS_FACTS.not_null - {"current_balance"}), _tightened(),
     "not_null_extra", "current_balance is NOT NULL but its source column is nullable"),
    (LOANS_FACTS, _tightened(unique=TARGET_LOANS_FACTS.unique | {("borrower_id", "loan_number")}),
     "unique_extra", "('borrower_id', 'loan_number') has no source counterpart"),
    (LOANS_FACTS, _tightened(foreign_keys=TARGET_LOANS_FACTS.foreign_keys
                             | {(("loan_number",), "loan_servicing.borrowers", ("borrower_id",))}),
     "foreign_key_extra", "('loan_number',) -> borrowers('borrower_id',) has no source counterpart"),
    (LOANS_FACTS, _tightened(check_count=3), "check_constraint_count_higher",
     "source 2 CHECK constraints, target 3"),
])
def test_a_target_only_constraint_fails_parity_because_it_rejects_legacy_valid_writes(src, tgt, code, needle):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target)
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    parity = _tier(result, "schema_parity")
    assert [f["check"] for f in parity["findings"]] == [code]
    assert needle in parity["findings"][0]["detail"]

    accepted = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert accepted["verdict"] == "PASS" and accepted["merge_eligible"] is True
    note = _tier(accepted, "schema_parity")["stats"]["accepted_target_only_constraints"]
    assert len(note) == 1 and note[0].startswith(f"loans: {code}: ")


@pytest.mark.parametrize("sqlserver, postgres, canonical", [
    ("([Balance]>=(0))", "CHECK ((balance >= (0)::numeric))", "balance >= 0"),
    ("([Balance]>=(0))", "CHECK (((0)::numeric <= balance))", "balance >= 0"),
    ("([Loan_Status]='FC' OR [Loan_Status]='AC')",
     "CHECK (((loan_status)::text = ANY ((ARRAY['AC'::character varying, 'FC'::character varying])::text[])))",
     "loan_status in ('AC', 'FC')"),
    ("([status]<>'X' AND [status]<>'Y')", "CHECK ((status <> ALL (ARRAY['X'::text, 'Y'::text])))",
     "status not in ('X', 'Y')"),
    ("(len([Code])<=(10))", "CHECK ((length((code)::text) <= 10))", "length(code) <= 10"),
    ("(([a]+[b])*(2)>(0))", "CHECK ((((a + b) * 2) > 0))", "(a + b) * 2 > 0"),
    ("([rate]>=(0.00) AND [rate]<=(1))", "CHECK (((rate <= (1)::numeric) AND (rate >= (0)::numeric)))",
     "rate <= 1 and rate >= 0"),
    ("([x] BETWEEN (1) AND (5))", "CHECK (((x >= 1) AND (x <= 5)))", "x <= 5 and x >= 1"),
    ("([n]>(-(1)))", "CHECK ((n > '-1'::integer))", "n > -1"),
    ("([x] IS NOT NULL OR [y] IS NOT NULL)", "CHECK (((y IS NOT NULL) OR (x IS NOT NULL)))",
     "x is not null or y is not null"),
])
def test_check_predicates_canonicalise_the_same_across_sql_server_and_postgres(sqlserver, postgres, canonical):
    assert _check_key_text(sqlserver, {}) == (canonical, True)
    assert _check_key_text(postgres, {}) == (canonical, True)


def test_check_predicate_canonical_form_keeps_what_distinguishes_rules():
    assert _check_key_text("(upper([x])='A')", {})[0] != _check_key_text("(upper([x])='a')", {})[0]
    assert _check_key_text("([a]-([b]-[c])>(0))", {})[0] == "a - (b - c) > 0"
    assert _check_key_text("(([a]-[b])-[c]>(0))", {})[0] == "a - b - c > 0"
    assert _check_key_text("CHECK ((a + (b * 2) > 0))", {})[0] == "a + b * 2 > 0"
    # source columns are renamed through the mapping; literals and function names are not
    assert _check_key_text("([Cust_ID]>(0) AND upper([memo])<>'cust_id')",
                           {"cust_id": "customer_id", "memo": "note"})[0] == \
        "customer_id > 0 and upper(note) <> 'cust_id'"


@pytest.mark.parametrize("definition", [
    "(datalength([Blob])<(100))",                       # engine-specific function
    "CHECK ((CASE WHEN a THEN 1 ELSE 0 END = 1))",      # outside the grammar
    "CHECK ((e ~~ '%@%'::text))",                       # LIKE: pattern/collation semantics differ
    "([e] like '%@%')",
])
def test_dialect_specific_check_predicates_are_not_portable(definition):
    assert _check_key_text(definition, {})[1] is False


def _checks_run(src_checks, tgt_checks, tol=None):
    loans, borrowers = _rows(6)
    tgt = _tightened(check_count=len(tgt_checks), checks=set(tgt_checks))
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, check_count=len(src_checks), checks=set(src_checks))
    return _run(source, target, tol=tol) if tol else _run(source, target)


def test_equal_check_counts_with_a_predicate_absent_from_the_target_fail_as_missing():
    # two on each side, but the target dropped the status rule and added a rule the source lacks
    # spelled on a column the source rule does not mention: nothing on the target can be it
    result = _checks_run(
        ["([Current_Balance]>=(0))", "([Loan_Status]='AC' OR [Loan_Status]='FC')"],
        ["CHECK ((current_balance >= (0)::numeric))", "CHECK ((current_balance >= (0)::numeric)) "])
    # the second target text canonicalises to the same rule, so the target has one predicate
    assert _codes(result, "schema_parity") == ["check_constraint_missing"]
    f = _tier(result, "schema_parity")["findings"][0]
    assert "([Loan_Status]='AC' OR [Loan_Status]='FC')" in f["detail"]
    assert "canonical: loan_status in ('AC', 'FC')" in f["detail"]
    assert result["merge_eligible"] is False


def test_equal_check_counts_with_different_predicates_are_unverified_and_block_merge():
    result = _checks_run(["([Current_Balance]>=(0))"], ["CHECK ((current_balance <= (0)::numeric))"])
    assert _codes(result, "schema_parity") == ["check_constraint_unverified"]
    detail = _tier(result, "schema_parity")["findings"][0]["detail"]
    assert "canonical: current_balance >= 0" in detail and "canonical: current_balance <= 0" in detail
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    # accept_target_only_constraints is about extra rules, not about unproven equivalence
    still = _checks_run(["([Current_Balance]>=(0))"], ["CHECK ((current_balance <= (0)::numeric))"],
                        tol=Tolerances("t1", accept_target_only_constraints=True))
    assert _codes(still, "schema_parity") == ["check_constraint_unverified"]
    # the recorded hand comparison demotes the pair to a stat
    accepted = _checks_run(["([Current_Balance]>=(0))"], ["CHECK ((current_balance <= (0)::numeric))"],
                           tol=Tolerances("t1", accept_unverified_check_constraints=True))
    assert accepted["verdict"] == "PASS" and accepted["merge_eligible"] is True
    note = _tier(accepted, "schema_parity")["stats"]["accepted_unverified_check_constraints"]
    assert len(note) == 1 and note[0].startswith("loans: 1 source CHECK(s) match no target CHECK")


def test_dialect_specific_check_predicate_with_no_textual_match_is_unverified_not_passed():
    result = _checks_run(["(datalength([Memo])<(100))"], ["CHECK ((octet_length(memo) < 100))"])
    assert _codes(result, "schema_parity") == ["check_constraint_unverified"]
    assert "dialect-specific construct" in _tier(result, "schema_parity")["findings"][0]["detail"]
    # the same dialect spelling on both sides is a match, not a guess
    same = _checks_run(["(datalength([Memo])<(100))"], ["CHECK ((datalength(memo) < 100))"])
    assert _codes(same, "schema_parity") == []


def test_check_predicates_are_compared_through_the_column_mapping():
    loans, borrowers = _rows(6)
    renamed_not_null = (TARGET_LOANS_FACTS.not_null - {"current_balance"}) | {"balance_current"}
    tgt = _tightened(not_null=renamed_not_null,
                     checks={"CHECK ((balance_current >= (0)::numeric))",
                             "CHECK ((loan_status = ANY (ARRAY['AC'::text, 'DL'::text, 'FC'::text])))"})
    source, target = _sides(loans, _renamed_rows(loans), borrowers, tgt_facts=tgt)
    result = _run(source, target, spec=_renamed_spec())
    assert _codes(result, "schema_parity") == []
    # a target rule still written against the old column name is not the mapped source rule
    stale = _tightened(not_null=renamed_not_null)
    unmapped = _run(*_sides(loans, _renamed_rows(loans), borrowers, tgt_facts=stale)[:2], spec=_renamed_spec())
    assert _codes(unmapped, "schema_parity") == ["check_constraint_unverified"]


def test_target_only_check_predicate_is_a_tightening_with_the_existing_decision_knob():
    result = _checks_run(["([Current_Balance]>=(0))"],
                         ["CHECK ((current_balance >= (0)::numeric))", "CHECK ((days_past_due >= 0))"])
    assert _codes(result, "schema_parity") == ["check_constraint_extra"]
    assert "days_past_due >= 0" in _tier(result, "schema_parity")["findings"][0]["detail"]
    accepted = _checks_run(["([Current_Balance]>=(0))"],
                           ["CHECK ((current_balance >= (0)::numeric))", "CHECK ((days_past_due >= 0))"],
                           tol=Tolerances("t1", accept_target_only_constraints=True))
    assert accepted["verdict"] == "PASS" and accepted["merge_eligible"] is True


def test_a_reader_that_only_counts_checks_falls_back_to_counts_and_says_so():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers,
                            tgt_facts=_tightened(checks=set()))
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    assert _tier(result, "schema_parity")["stats"]["check_predicates_unverified"] == [
        "loans: 2 CHECK constraints on each side, but a catalog reader delivered counts only; "
        "the predicates were not compared"]
    assert _tier(result, "schema_parity")["stats"]["loans"]["source"]["checks"] == sorted(LOANS_FACTS.checks)


def test_target_only_constraints_on_unmapped_columns_or_out_of_scope_tables_are_noted_not_graded():
    loans, borrowers = _rows(6)
    facts = _tightened(
        unique=TARGET_LOANS_FACTS.unique | {("servicer_ref",)},
        foreign_keys=TARGET_LOANS_FACTS.foreign_keys
        | {(("servicer_id",), "loan_servicing.servicers", ("servicer_id",)),
           # a mapped parent, but the local column is outside the mapping...
           (("servicer_id",), "loan_servicing.borrowers", ("borrower_id",)),
           # ...or the referenced column is: neither is provably target-only
           (("loan_number",), "loan_servicing.borrowers", ("legacy_ref",))},
        not_null=TARGET_LOANS_FACTS.not_null | {"servicer_ref", "servicer_id"})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=facts)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    stats = _tier(result, "schema_parity")["stats"]
    assert stats["target_only_columns_unverified"] == [
        "loans: unique ('servicer_ref',) covers a column outside the mapping",
        "loans: FK ('loan_number',) -> borrowers('legacy_ref',) covers a column outside the mapping",
        "loans: FK ('servicer_id',) -> borrowers('borrower_id',) covers a column outside the mapping"]
    assert stats["foreign_keys_out_of_scope"] == [
        "loans: target FK ('servicer_id',) -> loan_servicing.servicers"]


def _customer_facts(schema: str, fk_to: str | None = None) -> SchemaFacts:
    facts = SchemaFacts(table=f"{schema}.customer", primary_key=("id",), not_null={"id"})
    if fk_to:
        facts.foreign_keys.add((("parent_id",), fk_to, ("id",)))
    return facts


def _two_schema_sides(source_fk: str, target_fk: str, target_placed: bool = True):
    """`billing.customer` and `crm.customer` are both mapped (to customer_billing / customer_crm);
    `crm.customer.parent_id` references `source_fk`, its target references `target_fk`."""
    rows = [{"id": 1, "parent_id": 1}]
    spec = MappingSpec("m1", [
        ObjectMapping(object="customer_billing", root_table="billing.customer", key_source=["id"],
                      key_target=["id"], fields=[]),
        ObjectMapping(object="customer_crm", root_table="crm.customer", key_source=["id"],
                      key_target=["id"], fields=[FieldMapping("parent_id", "parent_id", "int", "int")]),
    ])
    tschema = "lakebase" if target_placed else ""
    source = FakeSource({"billing.customer": rows, "crm.customer": rows},
                        schema={"billing.customer": _customer_facts("billing"),
                                "crm.customer": _customer_facts("crm", source_fk)})
    target = FakeTarget({"customer_billing": rows, "customer_crm": rows},
                        schema={"customer_billing": _facts(_customer_facts(tschema),
                                                           table=f"{tschema}.customer_billing" if tschema else ""),
                                "customer_crm": _facts(_customer_facts(tschema, target_fk),
                                                       table=f"{tschema}.customer_crm" if tschema else "")})
    return spec, source, target


@pytest.mark.parametrize("source_fk, target_fk, codes", [
    # the same-named table in the other mapped schema is a different parent
    ("billing.customer", "lakebase.customer_billing", []),
    ("crm.customer", "lakebase.customer_crm", []),
    ("billing.customer", "lakebase.customer_crm", ["foreign_key_extra", "foreign_key_missing"]),
    ("crm.customer", "lakebase.customer_billing", ["foreign_key_extra", "foreign_key_missing"]),
    # bracketed catalog spelling is the same table
    ("[billing].[customer]", "lakebase.customer_billing", []),
])
def test_foreign_keys_resolve_on_the_qualified_source_table_not_its_bare_name(source_fk, target_fk, codes):
    spec, source, target = _two_schema_sides(source_fk, target_fk)
    result = run_recon("u1", "transactional", spec, Tolerances("t1"), [], source, target)
    assert _codes(result, "schema_parity") == codes, _tier(result, "schema_parity")["findings"]
    assert result["verdict"] == ("PASS" if not codes else "FAIL")


def test_a_bare_foreign_key_reference_shared_by_two_mapped_schemas_is_unverified_not_passed():
    spec, source, target = _two_schema_sides("customer", "lakebase.customer_billing")
    result = run_recon("u1", "transactional", spec, Tolerances("t1"), [], source, target)
    parity = _tier(result, "schema_parity")
    # neither graded as parity nor as a target-only FK: the run stays PASS but cannot merge
    assert _codes(result, "schema_parity") == []
    assert parity["stats"]["unverified"] == [(
        "customer_crm: FK ('parent_id',) -> customer could reference any of ['customer_billing', "
        "'customer_crm']; qualify the reference (root_table schema) or confirm its target "
        "counterpart by hand")]
    assert result["verdict"] == "PASS" and result["merge_eligible"] is False
    assert any(w.startswith("UNVERIFIED schema_parity") for w in result["warnings"])


@pytest.mark.parametrize("placed, codes", [
    # the target catalog places customer_billing in `lakebase`; a same-named table in another
    # schema is not the mapped object
    (True, ["foreign_key_missing"]),
    # a fake that reports no catalog identity falls back to the bare object name
    (False, []),
])
def test_a_target_foreign_key_into_another_schema_is_not_the_mapped_object(placed, codes):
    spec, source, target = _two_schema_sides("billing.customer", "archive.customer_billing", target_placed=placed)
    result = run_recon("u1", "transactional", spec, Tolerances("t1"), [], source, target)
    parity = _tier(result, "schema_parity")
    assert _codes(result, "schema_parity") == codes, parity["findings"]
    if placed:
        assert parity["stats"]["foreign_keys_out_of_scope"] == [
            "customer_crm: target FK ('parent_id',) -> archive.customer_billing"]


def _sqlserver_cased(f: SchemaFacts) -> SchemaFacts:
    """Catalog identifiers as a case-insensitive SQL Server returns them: the DDL's casing."""
    def up(x: str) -> str:
        return "_".join(p[:1].upper() + p[1:] for p in x.split("_")).replace("Id", "ID")
    return SchemaFacts(
        primary_key=tuple(up(x) for x in f.primary_key),
        unique={tuple(up(x) for x in u) for u in f.unique},
        foreign_keys={(tuple(up(x) for x in c), "dbo.Borrowers", tuple(up(x) for x in rc))
                      for c, _r, rc in f.foreign_keys},
        not_null={up(x) for x in f.not_null}, indexes={tuple(up(x) for x in i) for i in f.indexes},
        check_count=f.check_count, checks=set(f.checks),
        identity_columns={up(x) for x in f.identity_columns})


def _renamed_spec() -> MappingSpec:
    spec = _spec()
    loans = spec.objects[0]
    fields = [FieldMapping("current_balance", "balance_current", "money", "decimal(19,4)")
              if f.source == "current_balance" else f for f in loans.fields]
    return MappingSpec(spec.version, [dataclasses.replace(loans, fields=fields), *spec.objects[1:]])


def _renamed_rows(loans: list[dict]) -> list[dict]:
    return [{("balance_current" if k == "current_balance" else k): v for k, v in r.items()} for r in loans]


@pytest.mark.parametrize("renamed_not_null, codes", [({"balance_current"}, []), (set(), ["not_null_missing"])])
def test_catalog_casing_never_changes_parity_on_a_renamed_target_column(renamed_not_null, codes):
    cased = _sqlserver_cased(LOANS_FACTS)
    assert cased.primary_key == ("Loan_ID",) and "Current_Balance" in cased.not_null
    loans, borrowers = _rows(6)
    tgt = _tightened(not_null=(TARGET_LOANS_FACTS.not_null - {"current_balance"}) | renamed_not_null,
                     checks={c.replace("current_balance", "balance_current") for c in TARGET_LOANS_FACTS.checks})
    source, target = _sides(loans, _renamed_rows(loans), borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = cased
    source.schema["dbo.borrowers"] = _sqlserver_cased(BORROWER_FACTS)
    result = _run(source, target, spec=_renamed_spec())
    parity = _tier(result, "schema_parity")
    assert _codes(result, "schema_parity") == codes, parity["findings"]
    # the catalog's own spelling is what the evidence records
    assert parity["stats"]["loans"]["source"]["primary_key"] == ["Loan_ID"]
    if codes:
        assert "current_balance -> target balance_current is nullable" in parity["findings"][0]["detail"]


def test_primary_key_mismatch_is_reported():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers,
                            tgt_facts=_tightened(primary_key=("loan_number",)))
    result = _run(source, target)
    assert "primary_key_mismatch" in _codes(result, "schema_parity")


DESC_KEYS = [1000 - i for i in range(6)]


@pytest.mark.parametrize("keys, src_seq, tgt_seq, codes, needle, identity", [
    (None, None, 4, ["sequence_behind_source"], "4 <= source max",
     {"source_next": 7, "source_max": 6, "target_next": 4}),
    (None, None, None, ["sequence_missing"], "owns no sequence", None),
    # both count down from 1000: next 994 is below every source key, the safe state
    (DESC_KEYS, (994, -1), (994, -1), [], None,
     {"source_next": 994, "source_max": 1000, "target_next": 994, "source_min": 995, "increment": -1}),
    (DESC_KEYS, (994, -1), (997, -1), ["sequence_behind_source"], "997 >= source min loan_id=995", None),
    (None, (7, 1), (0, -1), ["sequence_direction_mismatch"], "opposite ends", None),
    (None, None, (4, 5), ["sequence_behind_source"], "collide", None),
    (None, (7, 10), (7, 1), ["sequence_increment_mismatch"], "steps by 10", None),
    # rows 7..999 were issued and deleted (or rolled back): the surviving max is 6 but the source
    # identity stands at 1000, so a target seeded from the surviving rows reissues 7..999
    (None, 1000, 7, ["sequence_behind_source"], "7 < source identity next 1000 (source max loan_id=6)",
     {"source_next": 1000, "source_max": 6, "target_next": 7}),
    (None, 1000, 1000, [], None, {"source_next": 1000, "source_max": 6, "target_next": 1000}),
    (None, 1000, 1500, [], None, None),
    # the same on a countdown identity: the source issued 994..100 and deleted them
    (DESC_KEYS, (99, -1), (994, -1), ["sequence_behind_source"],
     "994 > source identity next 99 on a descending identity (source min loan_id=995)", None),
    (DESC_KEYS, (99, -1), (99, -1), [], None, None),
    # a source that steps the other way has no comparable frontier; only the direction is graded
    (None, (-50, -1), (7, 1), ["sequence_direction_mismatch"], "opposite ends", None),
])
def test_identity_parity(keys, src_seq, tgt_seq, codes, needle, identity):
    loans, borrowers = _rows(6, keys=keys)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, src_seq=src_seq, tgt_seq=tgt_seq)
    if tgt_seq is None:
        target.sequences[("loans", "loan_id")] = None
    result = _run(source, target)
    assert result["verdict"] == ("PASS" if not codes else "FAIL"), result
    assert _codes(result, "schema_parity") == codes
    parity = _tier(result, "schema_parity")
    if needle:
        assert needle in parity["findings"][0]["detail"]
    if identity:
        assert parity["stats"]["loans"]["identity"] == identity


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


class _StubConn:
    """DB-API stand-in that records every (sql, params) and answers each fetch with `rows`."""

    def __init__(self, rows=((3, 3, 1, 9, 3, 12),)):
        self.rows, self.closed, self.executed = list(rows), False, []

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, params=()):
                conn.executed.append((sql, params))

            def fetchall(self):
                return conn.rows
        return Cur()

    def close(self):
        self.closed = True


def _db(name):
    return _StubConn([(name,)])


@pytest.mark.parametrize("name, quote, expected", [
    ("loans", '"', '"loans"'),
    ('lo"ans"; DROP TABLE x; --', '"', '"lo""ans""; DROP TABLE x; --"'),
    ("loan`s", "`", "`loan``s`"),
    ("Mixed Case", "`", "`Mixed Case`"),
])
def test_identifiers_are_delimited_with_the_delimiter_doubled(name, quote, expected):
    assert quote_ident(name, quote) == expected


@pytest.mark.parametrize("name", ["", "a.b", "x\x00y"])
def test_identifiers_that_are_not_one_name_are_refused(name):
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        quote_ident(name, '"')


def test_lakebase_target_validates_the_schema_before_it_connects(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("T", "dsn-under-test")
    connects = []
    monkeypatch.setattr(psycopg, "connect", lambda dsn: connects.append(dsn) or _db("db"))
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        LakebaseTargetAdapter("T", "db", "public.loan_servicing")
    assert connects == []


def test_databricks_target_validates_catalog_and_schema_before_it_connects(monkeypatch):
    import recon.adapters as adapters
    connects = []
    monkeypatch.setattr(adapters, "_databricks_connect", lambda name: connects.append(name))
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        adapters.DatabricksTargetAdapter("D", "", "silver")
    assert connects == []


@pytest.mark.parametrize("version", [(3, 10, 0), (3, 12, 0)])
def test_release_failures_reach_stderr_and_the_exception_on_every_runtime(monkeypatch, capsys, version):
    import recon.engine as engine
    monkeypatch.setattr(engine.sys, "version_info", version)
    exc = RuntimeError("tier failure")
    engine._report_release_failure(exc, "source close_window failed")
    engine._report_release_failure(exc, "source connection could not be dropped")
    assert exc.__notes__ == ["source close_window failed", "source connection could not be dropped"]
    assert str(exc) == "tier failure"
    assert capsys.readouterr().err.splitlines() == [
        "dbx-recon: source close_window failed", "dbx-recon: source connection could not be dropped"]


def test_a_failed_run_prints_release_failures_before_the_error_propagates(capsys):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    target.fail_on["range_fingerprints"] = RuntimeError("tier failure")
    source.fail_on["close_window"] = RuntimeError("rollback failed")
    with pytest.raises(RuntimeError):
        _run(source, target)
    assert capsys.readouterr().err == "dbx-recon: source close_window failed: RuntimeError('rollback failed')\n"


def test_lakebase_target_qualifies_objects_with_escaped_identifiers(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("T", "dsn-under-test")
    monkeypatch.setattr(psycopg, "connect", lambda dsn: _db("db"))
    target = LakebaseTargetAdapter("T", "db", 'loan"servicing')
    assert target._q('lo"ans') == '"loan""servicing"."lo""ans"'
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        target._q("public.loans")


def test_lakebase_target_binds_the_connection_to_the_allowlisted_database(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("T", "dsn-under-test")
    conns = []

    def connect(dsn):
        conns.append(_db("loan_servicing_prod"))
        return conns[-1]
    monkeypatch.setattr(psycopg, "connect", connect)
    with pytest.raises(TargetIdentityError, match="'loan_servicing_prod'.*'lakebase_rehearsal'"):
        LakebaseTargetAdapter("T", "lakebase_rehearsal", "loan_servicing")
    assert conns[0].closed is True
    assert [s for s, _ in conns[0].executed] == ["SELECT current_database()"]
    target = LakebaseTargetAdapter("T", "loan_servicing_prod", "loan_servicing")
    assert target.database == "loan_servicing_prod" and conns[1].closed is False


class _LiveTable:
    """DB-API stand-in for an engine with no snapshot: answers the marker aggregate and a
    per-table write counter, and lets a test schedule a write between any two statements."""

    def __init__(self):
        self.count, self.max_wm, self.token = 100, 500, 7
        self.executed: list[str] = []
        self.before_statement: dict = {}

    def write_below_max(self):
        self.token += 1  # an UPDATE that leaves COUNT(*) and MAX(watermark) untouched

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, params=()):
                hook = conn.before_statement.pop(len(conn.executed), None)
                if hook:
                    hook()
                conn.executed.append(sql)
                self.sql = sql

            def fetchall(self):
                if self.sql.startswith("TOKEN"):
                    return [(conn.token,)]
                return [(conn.count, conn.max_wm)]
        return Cur()


class _NoSnapshotAdapter(_SqlAdapterBase):
    change_token_sql = "TOKEN {table}"


def test_a_write_between_the_marker_row_and_its_token_is_not_baked_into_the_baseline():
    conn = _LiveTable()
    conn.before_statement[2] = conn.write_below_max  # after the aggregate, before the token
    adapter = _NoSnapshotAdapter(conn)
    assert adapter.open_window() == "none"
    marker = adapter.window_marker("dbo.loan", ["loan_id"], "modified_date")
    # first bracket (7, row, 8) disagreed and was discarded; the retry read (8, row, 8)
    assert marker == (100, 500, 8)
    assert [s[:5] for s in conn.executed] == ["TOKEN", "SELEC", "TOKEN", "TOKEN", "SELEC", "TOKEN"]
    assert adapter.window_marker("dbo.loan", ["loan_id"], "modified_date") == marker


def test_a_bracket_that_never_settles_yields_a_marker_no_close_can_match():
    conn = _LiveTable()
    for i in (2, 5, 8):  # a write inside every one of the three brackets
        conn.before_statement[i] = conn.write_below_max
    adapter = _NoSnapshotAdapter(conn)
    adapter.open_window()
    opened = adapter.window_marker("dbo.loan", ["loan_id"], "modified_date")
    assert opened == (100, 500, 9, 10)
    closed = adapter.window_marker("dbo.loan", ["loan_id"], "modified_date")
    assert closed == (100, 500, 10) and closed != opened


def test_a_pinned_snapshot_marker_reads_no_token():
    conn = _LiveTable()
    adapter = _NoSnapshotAdapter(conn)
    adapter.isolation = "repeatable_read"
    assert adapter.window_marker("dbo.loan", ["loan_id"], "modified_date") == (100, 500)
    assert all(not s.startswith("TOKEN") for s in conn.executed)


def test_cli_refuses_a_lakebase_dsn_outside_the_allowlisted_database(tmp_path, monkeypatch):
    _write_inputs(tmp_path, monkeypatch)
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("S", "src")
    monkeypatch.setenv("T", "dsn-under-test")
    monkeypatch.setattr(psycopg, "connect", lambda dsn: _db("somewhere_else"))
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


def test_in_flight_rows_with_stale_target_values_do_not_fail_aggregates():
    # the target still holds the pre-change values of two rows the source has since updated:
    # exactly the case that used to force tier 2 to skip the whole object
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    for r in loans[10:]:
        r["current_balance"] += 500
        r["modified_date"] = _ts(20)
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=15))
    assert result["verdict"] == "PASS", result
    t2 = _tier(result, "per_field_aggregates")
    assert t2["checks_run"] == 4 and t2["stats"]["applied_subset"]["loans"]["excluded_keys"] == 2
    assert source.calls["table_aggregates"] == 2 and target.calls["table_aggregates_excluding"] == 1


def test_an_undeclared_target_field_is_probed_with_the_in_flight_keys_excluded():
    # the target side of an undeclared field is probed in isolation; under in-flight exclusion
    # that probe must run over the same applied set as the batched statement, not the whole table
    loans, borrowers = _rows(12)
    tgt = [dict(r) for r in loans]
    for r in loans[10:]:
        r["current_balance"] += 500
        r["modified_date"] = _ts(20)
    spec = _spec()
    fields = [dataclasses.replace(f, target_type="") if f.target == "current_balance" else f
              for f in spec.objects[0].fields]
    spec = MappingSpec("m1", [dataclasses.replace(spec.objects[0], fields=fields), spec.objects[1]])
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=15), spec=spec)
    assert result["verdict"] == "PASS", result
    t2 = _tier(result, "per_field_aggregates")
    assert t2["checks_run"] == 4 and _codes(result, "per_field_aggregates") == []
    assert target.calls["table_aggregates_excluding"] == 2 and target.calls["field_aggregates"] == 0


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


def test_applied_predicate_is_the_complement_of_the_in_flight_predicate_plus_nulls():
    hwm = dt.datetime(2026, 9, 8, 18, 43, 52, 164112)  # noqa: DTZ001  naive = UTC by contract
    assert _applied_predicate("modified_date", hwm) == \
        "(modified_date < '2026-09-08 18:43:52.164113' OR modified_date IS NULL)"
    assert _applied_predicate("version_no", 41) == "(version_no <= 41 OR version_no IS NULL)"


def test_sql_adapter_excludes_in_flight_keys_in_one_bound_statement():
    conn = _StubConn()
    adapter = _NoSnapshotAdapter(conn)
    out = adapter.table_aggregates_excluding("dbo.loans", ["current_balance"], ["current_balance"],
                                             ["loan_id"], [(11,), (12,)], where="status = 'A'")
    sql, params = conn.executed[-1]
    assert sql.endswith("FROM dbo.loans WHERE (status = 'A') AND NOT (loan_id IN (?, ?))")
    assert params == (11, 12)
    assert out["current_balance"]["sum"] == 12 and out["current_balance"]["count"] == 3
    adapter.table_aggregates_excluding("dbo.x", ["v"], [], ["a", "b"], [(1, "p"), (2, "q")])
    sql, params = conn.executed[-1]
    assert sql.endswith("FROM dbo.x WHERE NOT ((a = ? AND b = ?) OR (a = ? AND b = ?))")
    assert params == (1, "p", 2, "q")


def test_sql_adapter_exclusion_capacity_counts_every_key_component():
    conn = _StubConn()
    adapter = _NoSnapshotAdapter(conn)
    width = 7
    cap = adapter.exclusion_capacity(width)
    assert cap == 2000 // width == 285
    keys = [tuple(range(i, i + width)) for i in range(cap)]
    adapter.table_aggregates_excluding("dbo.wide", ["v"], [], [f"k{j}" for j in range(width)], keys)
    sql, params = conn.executed[-1]
    assert sql.count("?") == len(params) == cap * width <= adapter.max_params
    statements = len(conn.executed)
    with pytest.raises(ValueError, match="exceed"):
        adapter.table_aggregates_excluding("dbo.wide", ["v"], [], [f"k{j}" for j in range(width)],
                                           keys + [tuple(range(cap, cap + width))])
    assert len(conn.executed) == statements   # never split into several statements


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


def _aware_target(loans, shift=PLUS2):
    return [dict(r, modified_date=r["modified_date"].replace(tzinfo=UTC).astimezone(shift))
            for r in loans]


def test_equivalent_naive_and_aware_watermarks_pass_every_tier():
    # source: SQL Server datetime (naive, UTC by contract); target: timestamptz rendered +02:00
    loans, borrowers = _rows(12)
    source, target = _sides(loans, _aware_target(loans), borrowers)
    result = _run(source, target)
    assert result["verdict"] == "PASS", result
    assert result["merge_eligible"] is True
    cdc = _tier(result, "cdc_lag_ordering")["stats"]["loans"]
    assert cdc["lag_s"] == 0.0 and cdc["in_flight"] == 0
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["mismatched_ranges"] == 0


def test_mixed_tz_in_flight_rows_and_lag_are_measured_as_instants():
    # target applied up to loan 10, stored aware; loans 11 and 12 (naive) are in flight
    loans, borrowers = _rows(12)
    tgt = _aware_target([r for r in loans if r["loan_id"] <= 10])
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", cdc_lag_max_s=5))
    assert result["verdict"] == "PASS", result
    assert _tier(result, "counts_through_mapping")["stats"]["count_gap_within_in_flight"]["loans"] == \
        {"gap": 2, "in_flight": 2}
    assert _tier(result, "pk_set_diff")["stats"]["loans"]["in_flight_missing"] == 2
    assert _tier(result, "cdc_lag_ordering")["stats"]["loans"]["lag_s"] == 2.0


def test_tier3_and_tier5_ordering_use_instants_across_tz_representations():
    # loan 4's target row is one second NEWER than the source (as instants) though its +02:00
    # wall clock reads two hours ahead of every naive value; loan 6 is one second OLDER
    loans, borrowers = _rows(12)
    tgt = _aware_target(loans)
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


def test_expression_indexes_are_graded_not_dropped():
    src = dataclasses.replace(LOANS_FACTS, expression_unique={"lower(loan_number)"},
                              expression_indexes={"upper(loan_number)"})
    # target: same unique expression, plus a unique expression the source never had
    tgt = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique={"lower(loan_number)", "lower(name)"})
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    t7 = _tier(result, "schema_parity")
    assert _codes(result, "schema_parity") == ["expression_unique_extra"]
    assert "lower(name)" in t7["findings"][0]["detail"]
    assert t7["stats"]["expression_indexes_unverified"] == [
        ("loans: source index on (upper(loan_number)) has no target index on (upper(loan_number)); "
         "confirm the access path by hand")]
    assert t7["stats"]["loans"]["source"]["expression_unique"] == ["lower(loan_number)"]
    assert t7["stats"]["loans"]["target"]["expression_unique"] == ["lower(loan_number)", "lower(name)"]
    # the recorded decision for target-only constraints covers a target-only unique expression
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert result["verdict"] == "PASS", result
    # a source unique expression the target lacks is always a defect
    target.schema["loans"] = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique=set())
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert _codes(result, "schema_parity") == ["expression_unique_missing"]


def test_index_key_text_keeps_nested_calls_whole_and_drops_suffixes():
    assert _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (lower(email))") == "lower(email)"
    assert _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (lower(region)) WHERE active") == \
        "lower(region)"
    assert _index_key_text("CREATE INDEX i ON s.t USING btree (upper(region), id) INCLUDE (code)") == \
        "upper(region), id"
    assert _index_key_text("CREATE INDEX i ON s.t USING gin (to_tsvector('english'::regconfig, "
                           "COALESCE(body, ''::text))) WITH (fastupdate=off)") == \
        "to_tsvector('english'::regconfig, coalesce(body, ''::text))"


def test_index_key_text_keeps_literal_and_quoted_identifier_case():
    upper = _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (((status = 'A'::text)), tenant_id)")
    lower = _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (((status = 'a'::text)), tenant_id)")
    assert upper == "((status = 'A'::text)), tenant_id" and upper != lower
    assert _index_key_text('CREATE INDEX i ON s.t USING btree (lower("Email"), UPPER("email"))') == \
        'lower("Email"), upper("email")'
    # parentheses and doubled quotes inside a literal never close the key list
    assert _index_key_text("CREATE INDEX i ON s.t USING btree (COALESCE(note, 'n/a (''X'')'::text)) WHERE x") == \
        "coalesce(note, 'n/a (''X'')'::text)"


def test_expression_unique_literal_case_is_a_parity_finding():
    loans, borrowers = _rows(12)
    src = dataclasses.replace(LOANS_FACTS, expression_unique={_index_key_text(
        "CREATE UNIQUE INDEX u ON dbo.loans USING btree (((status = 'A'::text)), loan_number)")})
    tgt = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique={_index_key_text(
        "CREATE UNIQUE INDEX u ON public.loans USING btree (((status = 'a'::text)), loan_number)")})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "schema_parity") == ["expression_unique_missing"]
    assert "'A'" in _tier(result, "schema_parity")["findings"][0]["detail"]
    target.schema["loans"] = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique=set(src.expression_unique))
    assert _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))["verdict"] == "PASS"


@pytest.mark.parametrize("expr_src, expr_tgt", [
    ("lower(loan_number)", "lower(loan_no)"),
    # a column name inside a string literal is not a column reference
    ("CASE WHEN loan_number = 'loan_number' THEN NULL ELSE lower(loan_number) END",
     "CASE WHEN loan_no = 'loan_number' THEN NULL ELSE lower(loan_no) END"),
])
def test_expression_index_columns_follow_the_field_mapping(expr_src, expr_tgt):
    spec = _spec()
    loans = dataclasses.replace(spec.objects[0], fields=[
        FieldMapping("loan_number", "loan_no", "varchar", "string"),
        FieldMapping("current_balance", "current_balance", "money", "decimal(19,4)"),
        FieldMapping("borrower_id", "borrower_id", "int", "int")])
    spec = MappingSpec("m1", [loans, spec.objects[1]])
    src = _facts(LOANS_FACTS, unique={("loan_number",)}, expression_unique={expr_src})
    tgt = _tightened(unique={("loan_no",)}, expression_unique={expr_tgt},
                     not_null={"loan_id", "loan_no", "current_balance", "modified_date", "borrower_id"})
    rows, borrowers = _rows(12)
    tgt_rows = [{**{k: v for k, v in r.items() if k != "loan_number"}, "loan_no": r["loan_number"]} for r in rows]
    source, target = _sides(rows, tgt_rows, borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target, spec=spec)
    assert _codes(result, "schema_parity") == [], _tier(result, "schema_parity")["findings"]


def test_expression_mapping_rewrites_column_references_only():
    colmap = {"status": "loan_status", "email": "email_addr", "text": "body", "lower": "lc"}
    # the mapped name inside a string literal, a cast type and a function name stays put
    assert _map_expression("CASE WHEN status = 'status' THEN 'email' ELSE email END", colmap) == \
        "CASE WHEN loan_status = 'status' THEN 'email' ELSE email_addr END"
    assert _map_expression("lower(email::text)", colmap) == "lower(email_addr::text)"
    assert _map_expression("lower((email)::character varying)", colmap) == \
        "lower((email_addr)::character varying)"
    assert _map_expression("COALESCE(status, 'it''s status')", colmap) == \
        "COALESCE(loan_status, 'it''s status')"
    # quoted identifiers are column references too, and keep their quoting
    assert _map_expression('lower("Email"), "text"', colmap) == 'lower("email_addr"), "body"'
    # unchanged when nothing is mapped
    assert _map_expression("to_tsvector('english'::regconfig, coalesce(body, ''::text))", {}) == \
        "to_tsvector('english'::regconfig, coalesce(body, ''::text))"


class _PostgresLike(_PostgresBase):
    pass


def test_watermark_literal_carries_the_utc_offset_only_where_the_engine_needs_it():
    hwm = dt.datetime(2026, 9, 8, 18, 43, 52, 164112, tzinfo=PLUS2)   # 16:43:52.164112 UTC
    pg, generic = _PostgresLike(_StubConn()), _NoSnapshotAdapter(_StubConn())
    # Postgres: a timestamptz column would read a bare literal in the session TimeZone
    assert _newer_predicate("modified_at", hwm, pg.watermark_literal) == \
        "modified_at >= '2026-09-08 16:43:52.164113+00:00'"
    assert _applied_predicate("modified_at", hwm, pg.watermark_literal) == \
        "(modified_at < '2026-09-08 16:43:52.164113+00:00' OR modified_at IS NULL)"
    # SQL Server datetime rejects an offset, so the zone-less engines keep the bare UTC form
    assert _newer_predicate("modified_date", hwm, generic.watermark_literal) == \
        "modified_date >= '2026-09-08 16:43:52.164113'"
    assert pg.watermark_literal(41) == generic.watermark_literal(41) == "41"


class _SqlServerLike(SqlServerSourceAdapter):
    def __init__(self, conn):
        _SqlAdapterBase.__init__(self, conn)


def test_sql_server_types_the_datetime_bound_so_coarse_columns_compare_exactly():
    # a bare literal is converted to the column's type first: datetime (3.33 ms ticks) and
    # smalldatetime (minutes) would round the next-microsecond bound back onto the watermark,
    # so rows equal to the applied HWM would count as in flight and leave the tier 2 aggregates
    hwm = dt.datetime(2026, 1, 1, 10, 0, 0, 167000)  # noqa: DTZ001  naive = UTC by contract
    mssql = _SqlServerLike(_StubConn())
    assert _newer_predicate("modified_date", hwm, mssql.watermark_literal) == \
        "modified_date >= CAST('2026-01-01 10:00:00.167001' AS datetime2(7))"
    assert _applied_predicate("modified_date", hwm, mssql.watermark_literal) == \
        "(modified_date < CAST('2026-01-01 10:00:00.167001' AS datetime2(7)) OR modified_date IS NULL)"
    assert mssql.watermark_literal(41) == "41"
    assert mssql.watermark_literal(dt.date(2026, 1, 1)) == "'2026-01-01'"


def test_transactional_predicates_use_the_source_engine_literal(monkeypatch):
    # the in-flight and applied predicates are evaluated by the source, so its rendering wins
    rows, borrowers = _rows(12)
    source, target = _sides(rows, [dict(r) for r in rows], borrowers)
    wheres: list[str] = []
    monkeypatch.setattr(source, "watermark_literal", lambda value: "<SRC>")
    monkeypatch.setattr(source, "row_count", lambda table, where=None: wheres.append(where) or 0)
    ctx = open_window(_spec(), source, target)
    assert "modified_date >= <SRC>" in wheres
    assert ctx.applied_where(_spec().objects[0]) == "(modified_date < <SRC> OR modified_date IS NULL)"


def test_a_watermark_that_turns_null_on_the_target_changes_the_fingerprint():
    # SUM skips NULL, so a NULL watermark used to fold into the same digest as the epoch: the
    # target lost row 6's watermark, no row is the max, tier 3 is sampled and may not fetch it
    loans, borrowers = _rows(40)
    for r in loans:
        r["modified_date"] = EPOCH if r["loan_id"] == 6 else r["modified_date"]
    tgt = [dict(r) for r in loans]
    tgt[5]["modified_date"] = None
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    assert result["verdict"] == "FAIL"
    pk = _tier(result, "pk_set_diff")["stats"]["loans"]
    assert pk["fingerprint"].startswith("count+key_sum") and 0 < pk["mismatched_ranges"] < pk["ranges"]
    codes = {f["check"]: f["detail"] for f in _tier(result, "cdc_lag_ordering")["findings"]}
    assert "(6,)" in codes["row_behind_applied_watermark"]


def test_a_null_source_watermark_applied_as_the_epoch_is_caught_too():
    loans, borrowers = _rows(40)
    loans[5]["modified_date"] = None
    tgt = [dict(r) for r in loans]
    tgt[5]["modified_date"] = EPOCH
    source, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, tol=Tolerances("t1", pk_set_ranges=8, sample_size=2), depth="sampled")
    assert result["verdict"] == "FAIL"
    codes = {f["check"]: f["detail"] for f in _tier(result, "cdc_lag_ordering")["findings"]}
    assert "(6,)" in codes["row_ahead_of_source"]


def test_range_fingerprints_carry_the_watermark_null_count():
    loans, borrowers = _rows(4)
    loans[1]["modified_date"] = None
    source, _ = _sides(loans, [dict(r) for r in loans], borrowers)
    (n, keys, wm), = source.range_fingerprints("dbo.loans", ["loan_id"], ["integer"], "modified_date",
                                              "datetime", [(None, None)])
    assert n == 4 and keys[0][0] == 10 and len(wm) == 3 and wm[2] == 1


def _contiguous(n, width=2):
    edges = [(i * 10,) * width for i in range(1, n)]
    return list(zip([None] + edges, edges + [None]))


def test_composite_key_fingerprints_stay_under_sql_server_parameter_limit():
    # 64 ranges x 2 key columns x (sum, sum of squares) + a watermark: the old one-CASE-per-term
    # shape bound the bounds ~8 times per range and crossed 2100 parameters on SQL Server
    # GROUP BY rng rows: (rng, count, sum a, sumsq a, sum b, sumsq b, sum wm, sumsq wm, wm nulls)
    conn = _StubConn([(1, 5, 50, 7, 50, 7, 500, 9, 0), (3, 2, 20, 3, 20, 3, 200, 4, 1)])
    adapter = _NoSnapshotAdapter(conn)
    out = adapter.range_fingerprints("dbo.t", ["a", "b"], ["integer", "integer"], "version_no",
                                     "integer", _contiguous(64))
    (sql, params), = conn.executed
    # a lexicographic 2-column bound is 3 parameters; 62 two-sided ranges + 2 open-ended edges
    assert len(params) == 62 * 6 + 2 * 3 and sql.count("?") == len(params)
    assert sql.startswith("SELECT rng, COUNT(*), SUM(") and sql.endswith("WHERE rng IS NOT NULL GROUP BY rng")
    assert sql.count(" THEN 63 END AS rng") == 1 and " THEN 64 " not in sql
    assert len(out) == 64
    assert out[1] == (5, ((50, 7), (50, 7)), (500, 9, 0))
    assert out[3] == (2, ((20, 3), (20, 3)), (200, 4, 1))
    assert out[0] == (0, ((0, 0), (0, 0)), (0, 0, 0)) and out[63] == out[0]


def test_range_fingerprints_split_into_statements_when_ranges_exceed_the_parameter_budget():
    conn = _StubConn([(0, 1, 1, 1, 1, 1)])
    adapter = _NoSnapshotAdapter(conn)
    adapter.max_params = 20
    out = adapter.range_fingerprints("dbo.t", ["a", "b"], ["integer", "integer"], None, None,
                                     _contiguous(50))
    # 6 parameters per range -> 3 ranges per statement -> 17 statements, every range answered
    assert len(conn.executed) == 17 and all(len(p) <= 20 for _, p in conn.executed)
    assert len(out) == 50 and out[0] == (1, ((1, 1), (1, 1)), None)


SRC_FK = ((("borrower_id",), "dbo.borrowers", ("borrower_id",)))
TGT_FK = ((("borrower_id",), "loan_servicing.borrowers", ("borrower_id",)))


@pytest.mark.parametrize("src_act, tgt_act, codes", [
    (("no action", "cascade"), ("no action", "no action"), ["foreign_key_action_mismatch"]),
    (("no action", "cascade"), ("no action", "cascade"), []),
    (("no action", "cascade"), None, []),  # a catalog that does not report actions is not graded
])
def test_foreign_key_referential_actions_are_compared(src_act, tgt_act, codes):
    loans, borrowers = _rows(6)
    src = _facts(LOANS_FACTS, foreign_key_actions={SRC_FK: src_act})
    tgt = _facts(TARGET_LOANS_FACTS, foreign_key_actions={TGT_FK: tgt_act} if tgt_act else {})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target)
    assert _codes(result, "schema_parity") == codes
    if codes:
        (finding,) = _tier(result, "schema_parity")["findings"]
        assert "source acts (update, delete) = ('no action', 'cascade')" in finding["detail"]
        assert _tier(result, "schema_parity")["stats"]["loans"]["source"]["foreign_keys"] == [
            [["borrower_id"], "dbo.borrowers", ["borrower_id"], "no action", "cascade"]]


def test_referential_actions_normalise_across_catalogs():
    assert [_fk_action(x) for x in ("a", "r", "c", "n", "d")] == \
        ["no action", "no action", "cascade", "set null", "set default"]
    assert [_fk_action(x) for x in ("NO_ACTION", "CASCADE", "SET_NULL", "SET_DEFAULT")] == \
        ["no action", "cascade", "set null", "set default"]


@pytest.mark.parametrize("src, tgt, codes, note", [
    # a composite unique is a column set: another declaration order is the same constraint
    ({"unique": {("loan_number",), ("borrower_id", "loan_number")}},
     {"unique": {("loan_number",), ("loan_number", "borrower_id")}}, [], "unique_reordered"),
    ({"unique": {("loan_number",), ("borrower_id", "loan_number")}},
     {"unique": {("loan_number",), ("borrower_id", "current_balance")}}, ["unique_extra", "unique_missing"], None),
    # an index is an access path and keeps its order: (loan_number, borrower_id) does not serve
    # a lookup that leads with borrower_id
    ({"indexes": {("borrower_id", "loan_number")}}, {"indexes": {("loan_number", "borrower_id")}},
     ["index_missing"], None),
])
def test_composite_unique_is_unordered_but_a_composite_index_is_not(src, tgt, codes, note):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=_tightened(**tgt))
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, **src)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == codes
    if note:
        (line,) = _tier(result, "schema_parity")["stats"][note]
        assert "('borrower_id', 'loan_number')" in line and "('loan_number', 'borrower_id')" in line


@pytest.mark.parametrize("src, tgt, codes, accepted", [
    # SQL Server keeps one NULL loan_number; a default Postgres unique keeps any number of them
    ({"unique_nulls_equal": {("loan_number",)}}, {}, ["unique_nulls_equal_missing"], []),
    # the reverse tightens the target: a second NULL the legacy app writes today is rejected
    ({}, {"unique_nulls_equal": {("loan_number",)}}, ["unique_nulls_equal_extra"], ["unique_nulls_equal_extra"]),
    ({"unique_nulls_equal": {("loan_number",)}}, {"unique_nulls_equal": {("loan_number",)}}, [], []),
    ({}, {}, [], []),
])
def test_nullable_unique_keys_must_agree_on_how_nulls_compare(src, tgt, codes, accepted):
    loans, borrowers = _rows(6)
    nullable = LOANS_FACTS.not_null - {"loan_number"}
    source, target = _sides(loans, [dict(r) for r in loans], borrowers,
                            tgt_facts=_tightened(not_null=nullable, **tgt))
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, not_null=nullable, **src)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == codes
    if codes:
        (f,) = _tier(result, "schema_parity")["findings"]
        assert "('loan_number',)" in f["detail"] and {f["source_value"], f["target_value"]} == {repr("nulls equal"), repr("nulls distinct")}
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert _codes(result, "schema_parity") == [c for c in codes if c not in accepted]
    assert result["verdict"] == "PASS" or codes != accepted


def test_null_semantics_are_not_graded_while_every_key_column_is_not_null():
    # loan_number is NOT NULL on both sides: no NULL key can ever exist, so the engines' NULL
    # handling cannot disagree on a real row
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, unique_nulls_equal={("loan_number",)})
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    assert _tier(result, "schema_parity")["stats"]["loans"]["source"]["unique_nulls_equal"] == [["loan_number"]]


def _undeclared_spec() -> MappingSpec:
    # a field with no declared target type: tier 2 has to find out whether it takes a SUM
    loans = _spec().objects[0]
    fields = [*loans.fields, FieldMapping("loan_status", "loan_status", "", "")]
    return MappingSpec("m1", [dataclasses.replace(loans, fields=fields), _spec().objects[1]])


def test_a_typed_source_is_never_probed_for_undeclared_fields():
    loans, borrowers = _rows(12)
    for r in loans:
        r["loan_status"] = "ACTIVE"
    tgt = [dict(r) for r in loans]
    source = FakeTypedSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                             schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                             sequences={("dbo.loans", "loan_id"): 13})
    _, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_undeclared_spec())
    assert result["verdict"] == "PASS", result
    # the one typing read replaces the per-field probe; the identity bounds read stays
    assert source.calls["numeric_columns"] == 1
    assert source.calls["field_aggregates"] == 1
    assert _tier(result, "consistency_window")["stats"]["strength"]["source"] == "snapshot"


def test_a_probe_that_releases_the_source_snapshot_fails_the_window():
    # a source with no column typing falls back to the probe; if that probe rolls the pinned
    # transaction back, the window is lost and the run says so instead of reporting a snapshot
    loans, borrowers = _rows(12)
    for r in loans:
        r["loan_status"] = "ACTIVE"
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
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


def test_catalog_case_does_not_decide_whether_an_undeclared_field_is_summed():
    # SQL Server's catalog reports the declared column case; the mapping may spell it differently
    class ShoutingCatalog(FakeTypedSource):
        def numeric_columns(self, table: str) -> set[str]:
            return {c.upper() for c in super().numeric_columns(table)}

    loans, borrowers = _rows(12)
    for r in loans:
        r["loan_status"] = 3
    tgt = [dict(r) for r in loans]
    tgt[0]["loan_status"] = 4  # applied-row numeric drift only a SUM can see (min/max/nulls agree)
    source = ShoutingCatalog({"dbo.loans": loans, "dbo.borrowers": borrowers},
                             schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                             sequences={("dbo.loans", "loan_id"): 13})
    _, target = _sides(loans, tgt, borrowers)
    result = _run(source, target, spec=_undeclared_spec())
    assert result["verdict"] == "FAIL"
    assert any(f["check"] == "aggregate_sum" and "loan_status" in f["detail"]
               for f in _tier(result, "per_field_aggregates")["findings"]), result
