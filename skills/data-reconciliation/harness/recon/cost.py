"""Pre-run cost estimate for one unit's recon: statements per side per tier and rows that will
cross the wire, from the mapping spec, the tolerances, the depth, and (optionally) known source
row counts. The plan playbook sums these per wave for the STOP C cost line; the actuals land in
result.json["cost"] after the run so the estimate can be corrected next wave.

No connections are opened here. Warehouse cost is proportional to statements x scanned rows on
each side; transfer cost and harness wall time are proportional to rows fetched.
"""

from __future__ import annotations

import math

from .config import MappingSpec, Tolerances
from .tiers import MAX_STRATA, sum_plan


def _tier3_mode(depth: str, n: int | None, tol: Tolerances) -> str:
    if depth == "full":
        return "full_diff"
    if depth == "sampled":
        return "stratified_sample"
    if n is None:
        return "unknown"
    return "stratified_sample" if n > tol.full_diff_row_threshold else "full_diff"


# Catalog statements per side for tier 7 (constraints, indexes, columns, checks).
SCHEMA_FACT_STATEMENTS = 4


def estimate_cost(spec: MappingSpec, tol: Tolerances, depth: str = "threshold",
                  row_counts: dict[str, int] | None = None, ops: int = 0,
                  mode: str = "live") -> dict:
    src: dict[str, int] = {"tier1": 0, "tier2": 0, "tier3": 0, "tier4": ops}
    tgt: dict[str, int] = {"tier1": 0, "tier2": 0, "tier3": 0, "tier4": ops}
    if mode == "transactional":
        for side in (src, tgt):
            side.update({"tier0": 0, "tier5": 0, "tier6": 0, "tier7": 0})
    rows_src: int | None = 0
    rows_tgt: int | None = 0
    modes: dict[str, str] = {}
    for c in spec.objects:
        n = (row_counts or {}).get(c.root_table)
        src["tier1"] += 1 + len(c.embeds)
        tgt["tier1"] += 1 + len(c.embeds)
        # Tier 2: one batched statement per table per side, plus one isolated probe per field
        # whose SUM that side cannot declare safe (SUM may error on strings, so it is not batched).
        src["tier2"] += 1 + sum(1 for f in c.fields if sum_plan(f.source_type, f.target_type) == "probe")
        tgt["tier2"] += 1 + sum(1 for f in c.fields if sum_plan(f.target_type, f.source_type) == "probe")
        t3 = _tier3_mode(depth, n, tol)
        modes[c.root_table] = t3
        src["tier3"] += 1  # row_count
        if t3 == "unknown":
            # threshold depth with no row count: the tier could go either way; statements are
            # estimated as sampled (the cheaper floor), rows cannot be estimated.
            rows_src = rows_tgt = None
            src["tier3"] += 3
            tgt["tier3"] += 1
        elif t3 == "full_diff":
            src["tier3"] += 1
            tgt["tier3"] += 1
            if n is None:
                rows_src = rows_tgt = None
            else:
                if rows_src is not None:
                    rows_src += n
                if rows_tgt is not None:
                    rows_tgt += n
        else:
            strata = max(1, min(MAX_STRATA, tol.sample_size, n if n is not None else MAX_STRATA))
            per = max(1, math.ceil(tol.sample_size / strata))
            sampled = min(n, strata * (per + 2)) if n is not None else strata * (per + 2)
            # strata + one sample_keys per stratum + duplicate count + keyed fetch (500 keys/stmt)
            fetch_stmts = max(1, math.ceil(sampled / 500))
            src["tier3"] += 1 + strata + 1 + fetch_stmts
            tgt["tier3"] += fetch_stmts
            if rows_src is not None:
                rows_src += sampled * 2  # keys, then rows
            if rows_tgt is not None:
                rows_tgt += sampled
        if mode == "transactional":
            # markers at open and close, plus the in-flight count when a watermark is declared
            src["tier0"] += 2 + (1 if c.watermark_source else 0)
            tgt["tier0"] += 2
            # tier 5: row_count + strata + one range-count statement; keys stream only for
            # mismatched ranges (unknown ahead of the run, estimated at zero)
            src["tier5"] += 3
            tgt["tier5"] += 1
            src["tier7"] += SCHEMA_FACT_STATEMENTS + (2 if c.identity_source else 0)
            tgt["tier7"] += SCHEMA_FACT_STATEMENTS + (2 if c.identity_target else 0)
        src["tier3"] += sum(1 for e in c.embeds if e.parent_key and e.fields)
        for e in c.embeds:
            if e.parent_key and e.fields:
                child_n = (row_counts or {}).get(e.child_table)
                if child_n is None:
                    rows_src = None
                elif rows_src is not None:
                    rows_src += child_n
    src["total"] = sum(src.values())
    tgt["total"] = sum(tgt.values())
    return {
        "mode": mode,
        "depth": depth,
        "tier3_mode": modes,
        "source_statements": src,
        "target_statements": tgt,
        "source_rows_fetched": rows_src,
        "target_rows_fetched": rows_tgt,
        "row_counts_known": row_counts is not None,
    }
