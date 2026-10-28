"""Plan-chosen grading: an object the spec grades `by_design_unverified` (a reject-only quarantine
sink, a rollup) is never a copy of its root. The harness keeps the cheap fail-closed facts (target
rows <= root rows, structure read), skips the value tiers for it, records the grading, and blocks
the merge on `by_design_unverified` until the wave manifest declares the same object."""

import json

import pytest
from recon.adapters import SchemaFacts
from recon.config import (
    GRADINGS,
    CanonRule,
    ConfigError,
    FieldMapping,
    MappingSpec,
    ObjectMapping,
    Tolerances,
    load_mapping_spec,
)
from recon.cost import estimate_cost
from recon.engine import run_recon
from recon.report import build_result, by_design_unverified, render_report, render_summary
from recon.tiers import TierResult

from tests.fakes import PROVEN_RERUN, FakeSource, FakeTarget

TOL = Tolerances(version="tol-v1", full_diff_row_threshold=100, sample_size=10)
RULES = [CanonRule("identity", "*")]


def _spec(grading="by_design_unverified"):
    orders = ObjectMapping(object="orders", root_table="ORDERS", key_source=["ORDER_ID"], key_target=["order_id"],
                           fields=[FieldMapping("TOTAL", "total", "NUMBER(10,2)", "decimal(10,2)")])
    quarantine = ObjectMapping(object="quarantine", root_table="ORDERS", key_source=["ORDER_ID"],
                               key_target=["order_id"],
                               fields=[FieldMapping("TOTAL", "total", "NUMBER(10,2)", "decimal(10,2)")],
                               grading=grading)
    return MappingSpec(version="map-v1", objects=[orders, quarantine])


ORDERS = [{"order_id": 1, "total": 10.5}, {"order_id": 2, "total": 20.0}]


def _estate(quarantine_rows, quarantine=None):
    source = FakeSource({"ORDERS": [{"ORDER_ID": 1, "TOTAL": 10.5}, {"ORDER_ID": 2, "TOTAL": 20.0}]})
    target = FakeTarget({"orders": list(ORDERS),
                         "quarantine": quarantine if quarantine is not None else
                         [{"order_id": 9000 + i, "total": -1.0} for i in range(quarantine_rows)]})
    source.schema["ORDERS"] = SchemaFacts(table="ORDERS", primary_key=("ORDER_ID",))
    for name in ("orders", "quarantine"):
        target.schema[name] = SchemaFacts(table=name, primary_key=("order_id",))
    return source, target


def _run(quarantine_rows, spec=None, quarantine=None):
    return run_recon("u", "live", spec or _spec(), TOL, RULES, *_estate(quarantine_rows, quarantine),
                     rerun_proof=PROVEN_RERUN)


def test_by_design_object_blocks_the_merge_on_its_own_reason_and_nothing_else():
    r = _run(quarantine_rows=1)
    assert r["verdict"] == "PASS" and r["parity"] == "PASS"
    assert r["merge_eligible"] is False
    assert r["blockers"] == [{"reason": "by_design_unverified", "class": "evidence"}]
    assert r["grading"] == {"orders": "full", "quarantine": "by_design_unverified"}
    assert by_design_unverified(r["grading"]) == ["quarantine"]
    tier1 = next(t for t in r["tiers"] if t["tier"] == 1)
    assert tier1["stats"]["by_design_unverified"] == ["quarantine"]
    # the value tiers never read the quarantine rows: a reject row has no source twin to differ from
    for t in r["tiers"]:
        if t["tier"] in (2, 3):
            assert not [f for f in t["findings"] if f["object"] == "quarantine"]
    for text in (render_report(r), render_summary(r)):
        assert "`quarantine` unverified(by_design)" in text


def test_a_by_design_object_with_more_rows_than_its_root_fails_tier_1():
    r = _run(quarantine_rows=3)
    assert r["verdict"] == "FAIL" and r["parity"] == "FAIL"
    tier1 = next(t for t in r["tiers"] if t["tier"] == 1)
    assert [(f["object"], f["check"]) for f in tier1["findings"]] == [("quarantine", "root_count_exceeded")]
    assert {"reason": "tier_failed", "class": "data"} in r["blockers"]
    assert {"reason": "by_design_unverified", "class": "evidence"} in r["blockers"]


def test_a_fully_graded_estate_records_full_grading_and_stays_eligible():
    # graded full, the same table is a copy of its root and every tier reads it
    r = _run(quarantine_rows=2, spec=_spec(grading="full"), quarantine=list(ORDERS))
    assert r["grading"] == {"orders": "full", "quarantine": "full"}
    assert r["merge_eligible"] is True and "by_design_unverified" not in r["merge_block_reasons"]
    assert "unverified(by_design)" not in render_summary(r)


def test_structural_tier_still_runs_over_a_by_design_object():
    r = _run(quarantine_rows=1)
    structural = next(t for t in r["tiers"] if t["tier"] == 0)
    assert structural["checks_run"] >= 2


def test_mapping_spec_grading_is_full_by_default_and_refuses_other_values(tmp_path):
    def spec(**obj):
        p = tmp_path / "spec.json"
        p.write_text(json.dumps({"version": "1", "objects": [
            {"object": "quarantine", "root_table": "ORDERS", "key": {"source": ["ORDER_ID"], "target": "order_id"},
             "fields": [], **obj}]}))
        return p
    assert load_mapping_spec(spec()).objects[0].grading == "full"
    assert load_mapping_spec(spec(grading="by_design_unverified")).objects[0].grading == "by_design_unverified"
    with pytest.raises(ConfigError, match="grading must be one of"):
        load_mapping_spec(spec(grading="subset"))
    assert GRADINGS == ("full", "by_design_unverified")


def test_build_result_refuses_an_unknown_grading_and_records_none_by_default():
    tiers = [TierResult(1, "counts_through_mapping", True, 1, [], {})]
    r = build_result("u", "live", "m", "t", tiers, rerun_proof=PROVEN_RERUN)
    assert r["grading"] == {} and r["merge_eligible"] is True
    with pytest.raises(ConfigError, match="grading"):
        build_result("u", "live", "m", "t", tiers, rerun_proof=PROVEN_RERUN, grading={"q": "subset"})


def test_estimate_budgets_only_the_tiers_a_by_design_object_runs():
    counts = {"ORDERS": 1_000_000}
    for mode in ("live", "transactional"):
        full = estimate_cost(_spec(grading="full"), TOL, "full", mode=mode, row_counts=counts)
        graded = estimate_cost(_spec(), TOL, "full", mode=mode, row_counts=counts)
        assert graded["source_rows_fetched"] < full["source_rows_fetched"]
        assert graded["target_rows_fetched"] < full["target_rows_fetched"]
        assert graded["source_statements"]["tier1"] == full["source_statements"]["tier1"]
        for tier in ("tier2", "tier3") + (("tier5",) if mode == "transactional" else ()):
            assert graded["source_statements"][tier] < full["source_statements"][tier]
            assert graded["target_statements"][tier] < full["target_statements"][tier]
        if mode == "transactional":
            for side in ("source_statements", "target_statements"):
                assert graded[side]["tier0"] == full[side]["tier0"] and graded[side]["tier7"] == full[side]["tier7"]
        assert "quarantine" not in graded["tier3_mode"] and graded["tier3_mode"]


# ------------------------------------------------------------------ structural parity

def _estate_with_keys(target_name, target_pk, grading="by_design_unverified", key_target="order_id",
                      rows=None, platform=False):
    """ORDERS keyed by ORDER_ID; a second target object keyed as declared."""
    obj = ObjectMapping(object=target_name, root_table="ORDERS", key_source=["ORDER_ID"], key_target=[key_target],
                        fields=[FieldMapping("TOTAL", "total", "NUMBER(10,2)", "decimal(10,2)")], grading=grading)
    spec = MappingSpec(version="map-v1", objects=[_spec().objects[0], obj])
    source, target = _estate(0)
    target.tables[target_name] = rows if rows is not None else [{key_target: 7, "total": 1.0}]
    target.schema[target_name] = SchemaFacts(table=target_name, primary_key=target_pk)
    if platform:
        target.target_kind = "databricks"
    return spec, source, target


def _structural(spec, source, target):
    r = run_recon("u", "live", spec, TOL, RULES, source, target, rerun_proof=PROVEN_RERUN)
    tier0 = next(t for t in r["tiers"] if t["tier"] == 0)
    return r, [(f["object"], f["check"]) for f in tier0["findings"]]


ROLLUP_ROWS = [{"order_id": 1, "customer_id": 7, "total": 1.0}]


def test_a_rollup_keyed_differently_from_its_root_passes_structural_parity():
    # the root key maps to order_id; the rollup's enforced key is customer_id
    spec, source, target = _estate_with_keys("totals", ("customer_id",), rows=ROLLUP_ROWS)
    r, findings = _structural(spec, source, target)
    assert ("totals", "primary_key_mismatch") not in findings and not findings
    assert r["verdict"] == "PASS" and r["merge_block_reasons"] == ["by_design_unverified"]


def test_a_same_key_reject_sink_grades_structurally_as_before():
    spec, source, target = _estate_with_keys("quarantine", ("order_id",))
    r, findings = _structural(spec, source, target)
    assert not findings and r["verdict"] == "PASS"


def test_a_by_design_target_without_an_enforced_key_must_prove_its_key_unique():
    # no PK on the target and no uniqueness probe available: fail closed
    spec, source, target = _estate_with_keys("totals", (), key_target="customer_id",
                                             rows=[{"customer_id": 7, "total": 1.0}])
    r, findings = _structural(spec, source, target)
    assert findings == [("totals", "target_key_not_unique")] and r["verdict"] == "FAIL"
    # a Databricks target probes the key: duplicates block, a unique key passes
    dup = [{"customer_id": 7, "total": 1.0}, {"customer_id": 7, "total": 2.0}]
    spec, source, target = _estate_with_keys("totals", (), key_target="customer_id", rows=dup, platform=True)
    r, findings = _structural(spec, source, target)
    assert findings == [("totals", "target_key_not_unique")] and r["verdict"] == "FAIL"
    spec, source, target = _estate_with_keys("totals", (), key_target="customer_id",
                                             rows=[{"customer_id": 7, "total": 1.0}], platform=True)
    r, findings = _structural(spec, source, target)
    assert not findings and r["verdict"] == "PASS"


def test_an_object_graded_full_with_a_mismatched_key_still_blocks():
    spec, source, target = _estate_with_keys("totals", ("customer_id",), grading="full", rows=ROLLUP_ROWS)
    r, findings = _structural(spec, source, target)
    assert ("totals", "primary_key_mismatch") in findings and r["verdict"] == "FAIL"
