"""Synthetic-estate tests: a small Oracle-shaped estate with an embed mapping, seeded with
one mismatch per class, proving each tier catches its class and the engine gates on Tier 1.
"""

import copy
import json
from pathlib import Path

from recon.config import (CanonRule, ObjectMapping, EmbedMapping, FieldMapping,
                          MappingSpec, Tolerances, load_canon_rules, load_mapping_spec,
                          load_tolerances)
from recon.engine import run_recon
from tests.fakes import FakeSource, FakeTarget

RULES = [CanonRule("rstrip_spaces", "*"), CanonRule("empty_string_is_null", "*"),
         CanonRule("null_missing_equiv", "*"), CanonRule("identity", "*")]

SPEC = MappingSpec(version="map-v1", objects=[ObjectMapping(
    object="orders", root_table="ORDERS",
    key_source=["ORDER_ID"], key_target="order_id",
    fields=[
        FieldMapping("ORDER_ID", "order_id", "NUMBER(18,0)", "long"),
        FieldMapping("CUST_NAME", "customer.name", "CHAR(20)", "string",
                     rules=["rstrip_spaces", "empty_string_is_null", "null_missing_equiv"]),
        FieldMapping("TOTAL", "total", "NUMBER(10,2)", "Decimal128"),
    ],
    embeds=[EmbedMapping(array_path="items", child_table="ORDER_ITEMS")],
)])

TOL = Tolerances(version="tol-v1", full_diff_row_threshold=100, sample_size=10)


def make_green():
    source = FakeSource({
        "ORDERS": [
            {"ORDER_ID": 1, "CUST_NAME": "Ada   ", "TOTAL": 10.5},
            {"ORDER_ID": 2, "CUST_NAME": "", "TOTAL": 20.0},
        ],
        "ORDER_ITEMS": [{"ORDER_ID": 1, "SKU": "a"}, {"ORDER_ID": 1, "SKU": "b"},
                        {"ORDER_ID": 2, "SKU": "c"}],
    })
    target = FakeTarget({
        "orders": [
            {"order_id": 1, "customer": {"name": "Ada"}, "total": 10.5,
             "items": [{"sku": "a"}, {"sku": "b"}]},
            {"order_id": 2, "customer": {}, "total": 20.0, "items": [{"sku": "c"}]},
        ],
    })
    return source, target


def run(source, target, mode="live"):
    return run_recon("orders-batch-1", mode, SPEC, TOL, RULES, source, target)


def test_green_estate_passes():
    result = run(*make_green())
    assert result["verdict"] == "PASS"
    assert [t["tier"] for t in result["tiers"]] == [1, 2, 3]
    assert result["mapping_version"] == "map-v1" and result["tolerance_version"] == "tol-v1"


def test_tier1_root_count_and_gate():
    source, target = make_green()
    target.objects["orders"] = target.objects["orders"][:1]
    result = run(source, target)
    assert result["verdict"] == "FAIL"
    assert len(result["tiers"]) == 1  # nothing else ran: Tier 1 gates
    assert any(f["check"] == "root_count" for f in result["tiers"][0]["findings"])


def test_tier1_embed_cardinality():
    source, target = make_green()
    target.objects["orders"][0]["items"].pop()
    result = run(source, target)
    checks = {f["check"] for f in result["tiers"][0]["findings"]}
    assert checks == {"embed_cardinality"}


def test_tier2_aggregate_mismatch():
    source, target = make_green()
    target.objects["orders"][0]["total"] = 999.0  # sum/min/max drift
    result = run(source, target)
    t2 = result["tiers"][1]
    assert not t2["passed"]
    assert any(f["check"].startswith("aggregate_") for f in t2["findings"])


def test_tier3_field_diff_reports_rule_evidence():
    source, target = make_green()
    target.objects["orders"][0]["customer"]["name"] = "Bob"
    result = run(source, target)
    t3 = result["tiers"][2]
    diffs = [f for f in t3["findings"] if f["check"] == "field_diff"]
    assert diffs and "rstrip_spaces" in diffs[0]["rules_applied"]


def test_tier3_missing_doc():
    source, target = make_green()
    source.tables["ORDERS"].append({"ORDER_ID": 3, "CUST_NAME": "Eve", "TOTAL": 1.0})
    source.tables["ORDER_ITEMS"].append({"ORDER_ID": 3, "SKU": "d"})
    result = run(source, target)
    assert result["verdict"] == "FAIL"  # tier 1 catches count; force tier 3 view too
    # add matching counts but wrong key to reach tier 3
    source, target = make_green()
    source.tables["ORDERS"][1] = {"ORDER_ID": 99, "CUST_NAME": "", "TOTAL": 20.0}
    result = run(source, target)
    t3 = result["tiers"][2]
    assert {f["check"] for f in t3["findings"]} >= {"missing_doc", "extra_doc"}


def test_tier3_sampling_above_threshold():
    source, target = make_green()
    tol = Tolerances(version="tol-v1", full_diff_row_threshold=1, sample_size=1)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target)
    stats = result["tiers"][2]["stats"]["orders"]
    assert stats["mode"] == "stratified_sample" and 0 < stats["coverage"] <= 1


def test_tier4_parity():
    source, target = make_green()
    ops = [{"name": "top_customers", "object": "orders", "rules": ["rstrip_spaces"]}]
    good = lambda op: [{"name": "Ada   "}]
    bad = lambda op: [{"name": "Zed"}]
    result = run_recon("u", "live", SPEC, TOL, RULES, source, target,
                       ops=ops, run_source=good, run_target=lambda op: [{"name": "Ada"}])
    assert result["verdict"] == "PASS" and len(result["tiers"]) == 4
    result = run_recon("u", "live", SPEC, TOL, RULES, source, target,
                       ops=ops, run_source=good, run_target=bad)
    assert result["tiers"][3]["findings"][0]["check"] == "parity_mismatch"


def test_continuous_mode_samples_tier3_and_skips_tier4():
    source, target = make_green()
    ops = [{"name": "x"}]
    result = run_recon("u", "continuous", SPEC, TOL, RULES, source, target,
                       ops=ops, run_source=lambda o: [], run_target=lambda o: [])
    assert [t["tier"] for t in result["tiers"]] == [1, 2, 3]
    assert result["tiers"][2]["stats"]["orders"]["mode"] == "stratified_sample"


def test_determinism():
    r1 = run(*make_green())
    r2 = run(*make_green())
    r1.pop("generated_at"); r2.pop("generated_at")
    assert r1 == r2


GRADED_SPEC = MappingSpec(version="map-v2", objects=[ObjectMapping(
    object="orders", root_table="ORDERS",
    key_source=["ORDER_ID"], key_target="order_id",
    fields=[FieldMapping("ORDER_ID", "order_id", "NUMBER(18,0)", "long")],
    embeds=[EmbedMapping(
        array_path="items", child_table="ORDER_ITEMS",
        parent_key=["ORDER_ID"], key_source=["SKU"], key_target="sku",
        fields=[FieldMapping("SKU", "sku", "VARCHAR2(10)", "string"),
                FieldMapping("QTY", "qty", "NUMBER(5,0)", "int")],
    )],
)])


def make_graded():
    source = FakeSource({
        "ORDERS": [{"ORDER_ID": 1}, {"ORDER_ID": 2}],
        "ORDER_ITEMS": [{"ORDER_ID": 1, "SKU": "a", "QTY": 2},
                        {"ORDER_ID": 1, "SKU": "b", "QTY": 1},
                        {"ORDER_ID": 2, "SKU": "c", "QTY": 5}],
    })
    target = FakeTarget({"orders": [
        {"order_id": 1, "items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 1}]},
        {"order_id": 2, "items": [{"sku": "c", "qty": 5}]},
    ]})
    return source, target


def test_embed_values_graded_green():
    result = run_recon("u", "live", GRADED_SPEC, TOL, RULES, *make_graded())
    assert result["verdict"] == "PASS"
    assert result["warnings"] == []
    assert result["tiers"][2]["stats"]["embeds_graded"]["orders.items"] == 3


def test_embed_value_diff_caught():
    source, target = make_graded()
    target.objects["orders"][0]["items"][1]["qty"] = 99
    result = run_recon("u", "live", GRADED_SPEC, TOL, RULES, source, target)
    assert result["verdict"] == "FAIL"
    checks = {f["check"] for f in result["tiers"][2]["findings"]}
    assert "embed_field_diff" in checks


def test_missing_embedded_elem_caught():
    source, target = make_graded()
    # same cardinality (Tier 1 green) but wrong element key
    target.objects["orders"][0]["items"][1]["sku"] = "zzz"
    result = run_recon("u", "live", GRADED_SPEC, TOL, RULES, source, target)
    checks = {f["check"] for f in result["tiers"][2]["findings"]}
    assert "missing_embedded_elem" in checks


def test_ungraded_embed_is_loud():
    result = run(*make_green())  # SPEC's embed declares no key/fields
    assert result["verdict"] == "PASS"
    assert result["tiers"][2]["stats"]["embeds_ungraded"] == ["orders.items"]
    assert any("UNGRADED" in w for w in result["warnings"])
    from recon.report import render_summary
    assert "UNGRADED" in render_summary(result)


def test_tier2_sum_skipped_for_non_numeric():
    spec = MappingSpec(version="m", objects=[ObjectMapping(
        object="c", root_table="T", key_source=["ID"], key_target="id",
        fields=[FieldMapping("ID", "id", "NUMBER", "long"),
                FieldMapping("NAME", "name", "VARCHAR2(10)", "string")])])
    source = FakeSource({"T": [{"ID": 1, "NAME": "x"}]})
    # target sum for a string field comes back 0 (Mongo $sum semantics)
    class Target(FakeTarget):
        def field_aggregates(self, object, field_path):
            out = super().field_aggregates(object, field_path)
            if out["sum"] is None:
                out["sum"] = 0
            return out
    target = Target({"c": [{"id": 1, "name": "x"}]})
    result = run_recon("u", "live", spec, TOL, RULES, source, target)
    assert result["verdict"] == "PASS"  # 0-vs-None sum on a string field is not a finding


def test_null_consistent_aggregates_green():
    # Adapter contract: SUM/MIN/MAX/DISTINCT-COUNT over non-null values only (SQL
    # semantics). A nullable numeric field with nulls on both sides must not produce
    # Tier-2 findings, and an all-null field sums to None on both sides.
    spec = MappingSpec(version="m", objects=[ObjectMapping(
        object="c", root_table="T", key_source=["ID"], key_target="id",
        fields=[FieldMapping("ID", "id", "NUMBER", "long"),
                FieldMapping("AMT", "amt", "NUMBER(10,2)", "double"),
                FieldMapping("VOID", "void", "NUMBER", "double")])])
    source = FakeSource({"T": [{"ID": 1, "AMT": 10.0, "VOID": None},
                               {"ID": 2, "AMT": None, "VOID": None}]})
    target = FakeTarget({"c": [{"id": 1, "amt": 10.0, "void": None},
                               {"id": 2, "amt": None, "void": None}]})
    result = run_recon("u", "live", spec, TOL,
                       RULES + [CanonRule("null_missing_equiv", "c.amt"),
                                CanonRule("null_missing_equiv", "c.void")],
                       source, target)
    assert result["verdict"] == "PASS"


def test_seed_and_params_recorded():
    result = run_recon("u", "live", GRADED_SPEC, TOL, RULES, *make_graded(),
                       seed=42, params={"batch": "demo"})
    assert result["seed"] == 42 and result["params"] == {"batch": "demo"}


def test_where_clause_params(tmp_path: Path):
    import pytest
    from recon.config import ConfigError
    (tmp_path / "map.json").write_text(json.dumps({
        "version": "m1", "objects": [{
            "object": "c", "root_table": "T", "root_where": "BATCH = '${batch}'",
            "key": {"source": ["ID"], "target": "id"},
            "fields": [{"source": "ID", "target": "id"}]}]}))
    spec = load_mapping_spec(tmp_path / "map.json", {"batch": "demo"})
    assert spec.objects[0].root_where == "BATCH = 'demo'"
    with pytest.raises(ConfigError):
        load_mapping_spec(tmp_path / "map.json")


def test_config_loaders_and_report(tmp_path: Path):
    (tmp_path / "map.json").write_text(json.dumps({
        "version": "m1", "objects": [{
            "object": "c", "root_table": "T",
            "key": {"source": ["ID"], "target": "id"},
            "fields": [{"source": "ID", "target": "id"}]}]}))
    (tmp_path / "tol.json").write_text(json.dumps({"version": "t1"}))
    (tmp_path / "rules.json").write_text(json.dumps([{"rule": "identity", "applies_to": "*"}]))
    spec = load_mapping_spec(tmp_path / "map.json")
    tol = load_tolerances(tmp_path / "tol.json")
    rules = load_canon_rules(tmp_path / "rules.json")
    source = FakeSource({"T": [{"ID": 1}]})
    target = FakeTarget({"c": [{"id": 1}]})
    result = run_recon("u", "snapshot", spec, tol, rules, source, target,
                       out_dir=tmp_path / "out")
    assert result["verdict"] == "PASS"
    assert (tmp_path / "out/result.json").exists()
    report = (tmp_path / "out/report.md").read_text()
    assert "snapshot" in report and "scoped to the snapshot watermark" in report


def test_unversioned_inputs_rejected(tmp_path: Path):
    import pytest
    from recon.config import ConfigError
    (tmp_path / "map.json").write_text(json.dumps({"objects": []}))
    with pytest.raises(ConfigError):
        load_mapping_spec(tmp_path / "map.json")


def test_missing_comparison_key_rejected(tmp_path: Path):
    import pytest
    from recon.config import ConfigError
    (tmp_path / "map.json").write_text(json.dumps({
        "version": "m1", "objects": [{"object": "c", "root_table": "T", "fields": []}]}))
    with pytest.raises(ConfigError):
        load_mapping_spec(tmp_path / "map.json")
