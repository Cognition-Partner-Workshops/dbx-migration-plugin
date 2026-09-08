"""--mode transactional: consistency window, PK-set diff, CDC lag/ordering, schema parity."""

import datetime as dt
import json

import pytest

from recon.adapters import SchemaFacts
from recon.cli import main
from recon.config import (CanonRule, ConfigError, FieldMapping, MappingSpec, ObjectMapping,
                          Tolerances, load_mapping_spec)
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
    assert source.calls["range_counts"] == 2 and target.calls["range_counts"] == 2


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
    assert _codes(result, "cdc_lag_ordering") == ["target_ahead_of_source"]
    assert "row_ahead_of_source" in _codes(result, "keyed_diffs")


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


def test_cli_estimate_accepts_mode(tmp_path, monkeypatch, capsys):
    _write_inputs(tmp_path, monkeypatch)
    assert main(["estimate", "--mapping", "m.json", "--tolerances", "t.json",
                 "--mode", "transactional"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["mode"] == "transactional" and "tier5" in out["source_statements"]
