"""Recon hot path: one statement per table for Tier 2, real key stratification for Tier 3,
the verify-depth knob, and cost accounting in the result."""

import json
import random
import sqlite3

import pytest

from recon.adapters import _SqlAdapterBase
from recon.config import ConfigError, ObjectMapping, Tolerances
from recon.cost import estimate_cost
from recon.engine import run_recon
from recon.tiers import _stratified_keys
from tests.fakes import FakeSource, FakeTarget
from tests.test_tiers import RULES, SPEC


class CountingConn:
    """sqlite3 connection wrapper that counts executed statements."""

    def __init__(self):
        self._c = sqlite3.connect(":memory:")
        self.statements = []

    def cursor(self):
        outer = self

        class Cur:
            def __init__(s):
                s._cur = outer._c.cursor()
                s.description = None

            def execute(s, sql, params=()):
                outer.statements.append(sql)
                s._cur.execute(sql, params)
                s.description = s._cur.description
                return s

            def fetchall(s):
                return s._cur.fetchall()

            def __iter__(s):
                return iter(s._cur)

        return Cur()

    def rollback(self):
        self._c.rollback()


def sqlite_adapter(rows):
    conn = CountingConn()
    conn._c.execute("CREATE TABLE t (id INTEGER, grp INTEGER, amt REAL, name TEXT)")
    conn._c.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
    return _SqlAdapterBase(conn), conn


ROWS = [(i, i % 3, float(i) if i % 7 else None, f"n{i}") for i in range(1, 101)]


# ---------------------------------------------------------------- SQL adapter: one statement per table

def test_table_aggregates_is_one_statement_and_matches_per_column():
    ad, conn = sqlite_adapter(ROWS)
    per_col = {c: ad.field_aggregates("t", c) for c in ("id", "amt", "name")}
    conn.statements.clear()
    batched = ad.table_aggregates("t", ["id", "amt", "name"], numeric=["id", "amt"])
    assert len(conn.statements) == 1
    assert batched["id"]["count"] == 100 and batched["id"]["sum"] == per_col["id"]["sum"]
    assert batched["amt"]["null_rate"] == pytest.approx(per_col["amt"]["null_rate"])
    assert batched["amt"]["distinct_count"] == per_col["amt"]["distinct_count"]
    assert batched["name"]["min"] == "n1" and batched["name"]["max"] == "n99"
    assert "sum" not in batched["name"] or batched["name"]["sum"] is None
    assert ad.statements == 7  # 3 x (metrics + SUM probe) per-column, then 1 batched


def test_table_aggregates_honours_where():
    ad, _ = sqlite_adapter(ROWS)
    out = ad.table_aggregates("t", ["id"], numeric=["id"], where="grp = 0")
    assert out["id"]["count"] == 33


def test_key_strata_covers_range_and_is_contiguous():
    ad, conn = sqlite_adapter(ROWS)
    conn.statements.clear()
    strata = ad.key_strata("t", ["id"], 4)
    assert len(conn.statements) == 1
    assert len(strata) == 4
    assert strata[0].lo == (1,) and strata[-1].hi == (100,)
    assert sum(s.n for s in strata) == 100
    for a, b in zip(strata, strata[1:]):
        assert a.hi < b.lo
    with_where = ad.key_strata("t", ["id"], 2, where="grp = 0")
    assert sum(s.n for s in with_where) == 33


def test_sample_keys_returns_requested_row_numbers_in_range():
    ad, conn = sqlite_adapter(ROWS)
    conn.statements.clear()
    keys = ad.sample_keys("t", ["id"], lo=41, hi=60, row_numbers=[1, 5, 20])
    assert len(conn.statements) == 1
    assert keys == [(41,), (45,), (60,)]
    assert ad.sample_keys("t", ["id"], lo=41, hi=60, row_numbers=[1], where="grp = 1") == [(43,)]


# every row shares the first key column; only the second differs
COMPOSITE_ROWS = [(1, i, float(i), f"n{i}") for i in range(1, 101)]


def test_key_strata_composite_bounds_are_disjoint_when_first_key_is_shared():
    ad, conn = sqlite_adapter(COMPOSITE_ROWS)
    conn.statements.clear()
    strata = ad.key_strata("t", ["id", "grp"], 4)
    assert len(conn.statements) == 1 and len(strata) == 4
    assert strata[0].lo == (1, 1) and strata[-1].hi == (1, 100)
    assert [s.n for s in strata] == [25, 25, 25, 25]
    for a, b in zip(strata, strata[1:]):
        assert a.hi < b.lo
    sampled: set[tuple] = set()
    for s in strata:
        keys = ad.sample_keys("t", ["id", "grp"], s.lo, s.hi, [1, s.n])
        assert keys == [s.lo, s.hi], (s, keys)
        sampled.update(keys)
    assert len(sampled) == 8
    with_where = ad.key_strata("t", ["id", "grp"], 2, where="grp > 50")
    assert [s.n for s in with_where] == [25, 25] and with_where[0].lo == (1, 51)


def test_stratified_keys_cover_every_stratum_of_a_shared_first_key():
    rows = [{"ID": 1, "SEQ": i, "TOTAL": float(i)} for i in range(1, 201)]
    source = FakeSource({"T": rows})
    mapping = ObjectMapping("t", "T", ["ID", "SEQ"], ["id", "seq"], [])
    keys, n_strata = _stratified_keys(mapping, source, 200, 16, random.Random(1))
    assert n_strata == 16
    assert (1, 1) in keys and (1, 200) in keys
    per_stratum = {(k[1] - 1) * n_strata // 200 for k in keys}
    assert per_stratum == set(range(n_strata))  # no stratum left unsampled, no early-row repeats
    assert len(keys) == len(set(keys)) and 16 <= len(keys) <= 16 + 2 * n_strata


def test_duplicate_key_count():
    ad, _ = sqlite_adapter(ROWS + [(1, 1, 1.0, "dup"), (2, 2, 2.0, "dup"), (2, 2, 2.0, "dup")])
    assert ad.duplicate_key_count("t", ["id"]) == 2
    assert ad.duplicate_key_count("t", ["id", "grp"], where="grp = 1") == 1


# ---------------------------------------------------------------- tiers through the fakes

def big_estate(n=200):
    rows = [{"ORDER_ID": i, "CUST_NAME": str(i), "TOTAL": float(i)} for i in range(n)]
    source = FakeSource({"ORDERS": rows, "ORDER_ITEMS": []})
    target = FakeTarget({"orders": [
        {"order_id": i, "customer": {"name": str(i)}, "total": float(i), "items": []}
        for i in range(n)]})
    return source, target


def test_tier2_uses_one_table_statement_per_side():
    source, target = big_estate()
    run_recon("u", "live", SPEC, Tolerances(version="t"), RULES, source, target)
    assert source.calls["table_aggregates"] == 1 and source.calls["field_aggregates"] == 0
    assert target.calls["table_aggregates"] == 1


def test_tier3_stratified_sampling_does_not_stream_all_keys():
    source, target = big_estate()
    tol = Tolerances(version="t", full_diff_row_threshold=1, sample_size=12)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target, seed=3)
    stats = result["tiers"][2]["stats"]["orders"]
    assert stats["mode"] == "stratified_sample" and stats["sampling"] == "stratified"
    assert stats["strata"] > 1
    assert source.calls["iter_keys"] == 0
    keys = source.last_fetch_keyed["keys"]
    assert (0,) in keys and (199,) in keys  # edges always included
    assert 12 <= len(keys) <= 12 + 2 * stats["strata"]
    assert stats["duplicate_source_key_count"] == 0

    source2, target2 = big_estate()
    r2 = run_recon("u", "live", SPEC, tol, RULES, source2, target2, seed=3)
    assert source2.last_fetch_keyed["keys"] == keys and r2["verdict"] == "PASS"
    source3, target3 = big_estate()
    run_recon("u", "live", SPEC, tol, RULES, source3, target3, seed=4)
    assert source3.last_fetch_keyed["keys"] != keys


def test_tier3_stratified_catches_a_seeded_diff_in_every_stratum_edge():
    source, target = big_estate()
    target.objects["orders"][199]["total"] = -1.0  # last key: always sampled as an edge
    tol = Tolerances(version="t", full_diff_row_threshold=1, sample_size=4)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target)
    assert result["verdict"] == "FAIL"
    assert any(f["check"] == "field_diff" and "199" in f["detail"] for f in result["tiers"][2]["findings"])


def test_tier3_stratified_counts_source_duplicates_without_streaming():
    source, target = big_estate()
    # A duplicated key on both sides passes Tier 1 counts; only the key-uniqueness probe sees it.
    source.tables["ORDERS"].append({"ORDER_ID": 5, "CUST_NAME": "5", "TOTAL": 5.0})
    target.objects["orders"].append({"order_id": 5, "customer": {"name": "5"}, "total": 5.0, "items": []})
    tol = Tolerances(version="t", full_diff_row_threshold=1, sample_size=4)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target)
    assert result["tiers"][2]["stats"]["orders"]["duplicate_source_key_count"] == 1
    assert source.calls["iter_keys"] == 0


class ReservoirOnlySource(FakeSource):
    """Adapter without stratification support: the harness must fall back to the key stream."""
    key_strata = None
    sample_keys = None
    duplicate_key_count = None


def test_reservoir_fallback_for_adapters_without_strata():
    rows = [{"ORDER_ID": i, "CUST_NAME": str(i), "TOTAL": float(i)} for i in range(50)]
    source = ReservoirOnlySource({"ORDERS": rows, "ORDER_ITEMS": []})
    target = FakeTarget({"orders": [{"order_id": i, "customer": {"name": str(i)}, "total": float(i),
                                     "items": []} for i in range(50)]})
    tol = Tolerances(version="t", full_diff_row_threshold=1, sample_size=5)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target)
    stats = result["tiers"][2]["stats"]["orders"]
    assert stats["sampling"] == "reservoir" and source.calls["iter_keys"] == 1
    assert result["verdict"] == "PASS"


# ---------------------------------------------------------------- verify depth

def test_depth_full_forces_full_diff_above_threshold():
    source, target = big_estate()
    tol = Tolerances(version="t", full_diff_row_threshold=1, sample_size=4)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target, depth="full")
    assert result["depth"] == "full"
    assert result["tiers"][2]["stats"]["orders"]["mode"] == "full_diff"


def test_depth_sampled_forces_sampling_below_threshold():
    source, target = big_estate(20)
    result = run_recon("u", "live", SPEC, Tolerances(version="t"), RULES, source, target, depth="sampled")
    assert result["depth"] == "sampled"
    assert result["tiers"][2]["stats"]["orders"]["mode"] == "stratified_sample"


def test_depth_default_is_tolerance_threshold_and_invalid_rejected():
    source, target = big_estate(20)
    result = run_recon("u", "live", SPEC, Tolerances(version="t"), RULES, source, target)
    assert result["depth"] == "threshold"
    assert result["tiers"][2]["stats"]["orders"]["mode"] == "full_diff"
    with pytest.raises(ConfigError, match="depth"):
        run_recon("u", "live", SPEC, Tolerances(version="t"), RULES, source, target, depth="deep")


def test_depth_recorded_in_report(tmp_path):
    source, target = big_estate(20)
    run_recon("u", "live", SPEC, Tolerances(version="t"), RULES, source, target, depth="full",
              out_dir=tmp_path)
    assert "full" in (tmp_path / "report.md").read_text()
    assert json.loads((tmp_path / "result.json").read_text())["depth"] == "full"


# ---------------------------------------------------------------- cost

def test_result_carries_cost_actuals():
    source, target = big_estate()
    tol = Tolerances(version="t", full_diff_row_threshold=1, sample_size=4)
    result = run_recon("u", "live", SPEC, tol, RULES, source, target)
    cost = result["cost"]
    assert cost["source_statements"] == source.statements
    assert cost["target_statements"] == target.statements
    assert cost["source_rows_fetched"] == source.rows_fetched > 0
    assert cost["elapsed_s"] >= 0
    # sampled Tier 3 on 200 rows pulls far fewer than the population across the wire
    assert cost["source_rows_fetched"] < 200


def test_estimate_cost_scales_with_depth():
    tol = Tolerances(version="t", full_diff_row_threshold=100, sample_size=10)
    sampled = estimate_cost(SPEC, tol, "sampled", row_counts={"ORDERS": 1_000_000})
    full = estimate_cost(SPEC, tol, "full", row_counts={"ORDERS": 1_000_000})
    threshold = estimate_cost(SPEC, tol, "threshold", row_counts={"ORDERS": 1_000_000})
    assert sampled["source_rows_fetched"] < full["source_rows_fetched"] == 1_000_000
    assert threshold["source_rows_fetched"] == sampled["source_rows_fetched"]  # above threshold -> sampled
    assert sampled["source_statements"]["tier2"] == 1  # one table
    assert full["source_statements"]["total"] < sampled["source_statements"]["total"]
    small = estimate_cost(SPEC, tol, "threshold", row_counts={"ORDERS": 50})
    assert small["source_rows_fetched"] == 50 and small["tier3_mode"]["ORDERS"] == "full_diff"
    unknown = estimate_cost(SPEC, tol, "threshold", row_counts=None)
    assert unknown["source_rows_fetched"] is None and unknown["tier3_mode"]["ORDERS"] == "unknown"
    assert unknown["source_statements"]["total"] > 0
    bounded = estimate_cost(SPEC, tol, "sampled", row_counts=None)
    assert bounded["source_rows_fetched"] == sampled["source_rows_fetched"]  # sample cost needs no population


def test_cli_estimate_subcommand(tmp_path, capsys):
    from recon.cli import main
    spec_path = tmp_path / "map.json"
    spec_path.write_text(json.dumps({"version": "m", "objects": [{
        "object": "orders", "root_table": "ORDERS", "key": {"source": ["ORDER_ID"], "target": "order_id"},
        "fields": [{"source": "ORDER_ID", "target": "order_id", "target_type": "long"}]}]}))
    tol_path = tmp_path / "tol.json"
    tol_path.write_text(json.dumps({"version": "t", "full_diff_row_threshold": 10, "sample_size": 5}))
    counts = tmp_path / "counts.json"
    counts.write_text(json.dumps({"ORDERS": 5000}))
    rc = main(["estimate", "--mapping", str(spec_path), "--tolerances", str(tol_path),
               "--depth", "sampled", "--row-counts", str(counts)])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["depth"] == "sampled" and out["source_rows_fetched"] < 5000


def test_cli_refuses_transactional_mode_by_name(tmp_path, capsys):
    from recon.cli import main
    with pytest.raises(SystemExit) as exc:
        main(["run", "--unit", "u", "--family", "sqlserver", "--mapping", str(tmp_path / "m.json"),
              "--tolerances", str(tmp_path / "t.json"), "--canonicalization", str(tmp_path / "c.json"),
              "--mode", "transactional", "--source-dsn-secret", "S", "--target-secret", "T",
              "--target-catalog", "mig", "--target-schema", "s", "--out", str(tmp_path / "out")])
    msg = str(exc.value)
    assert "transactional" in msg and "not implemented" in msg
    assert "snapshot" in msg
