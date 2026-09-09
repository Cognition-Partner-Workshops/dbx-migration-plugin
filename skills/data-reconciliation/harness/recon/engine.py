"""Orchestration: run tiers in cost order, gate on Tier 1, produce the result.

Deterministic and idempotent: same inputs produce the same verdict; safe to re-run.
"""

from __future__ import annotations

import time
from pathlib import Path

from .adapters import StatementCounting
from .canon import Canonicalizer
from .config import CanonRule, ConfigError, MappingSpec, Tolerances
from .report import build_result, write_outputs
from .tiers import tier1_counts, tier2_aggregates, tier3_diffs, tier4_parity

# fixture: same checks as live, run against a small fixture copy of the source during
# development and fix rounds. A fixture PASS is never a merge verdict; it only earns the unit
# its one live run.
MODES = ("fixture", "live", "snapshot", "continuous")
# Accepted by the CLI so the refusal names the mode, never run: the Lakebase/OLTP track's
# consistency-window, PK-set, CDC-lag and constraint-parity tiers are not implemented yet.
PLANNED_MODES = ("transactional",)

# Tier 3 depth. threshold: the tolerance file's full_diff_row_threshold decides per table.
# sampled: always stratified sample (the verifier default). full: always keyed full diff
# (cutover-critical units, set by the plan in the wave manifest's verify_depth).
DEPTHS = ("threshold", "sampled", "full")


def _cost(source, target, started: float) -> dict:
    def side(adapter):
        if isinstance(adapter, StatementCounting):
            return adapter.statements, adapter.rows_fetched
        return None, None
    s_stmts, s_rows = side(source)
    t_stmts, t_rows = side(target)
    return {"source_statements": s_stmts, "source_rows_fetched": s_rows,
            "target_statements": t_stmts, "target_rows_fetched": t_rows,
            "elapsed_s": round(time.monotonic() - started, 3)}

def _snapshot_provenance_warnings(snapshot: dict | None, source_family: str | None,
                                  spec: MappingSpec, tier1) -> list[str]:
    if snapshot is None:
        return []
    warnings = []
    if snapshot.get("source") != source_family:
        warnings.append(
            f"SNAPSHOT provenance: manifest source '{snapshot.get('source')}' "
            f"!= run family '{source_family}'")
    observed = tier1.stats.get("source_counts", {})
    manifest_counts = snapshot.get("row_counts", {})
    for c in spec.objects:
        name = c.root_table
        if name not in manifest_counts:
            warnings.append(f"SNAPSHOT provenance: {name} missing from manifest")
        elif manifest_counts[name] != observed.get(name):
            warnings.append(
                f"SNAPSHOT provenance mismatch: {name} manifest={manifest_counts[name]} "
                f"observed={observed.get(name)}")
    return warnings


def run_recon(unit: str, mode: str, spec: MappingSpec, tol: Tolerances,
              rules: list[CanonRule], source, target,
              ops: list[dict] | None = None, run_source=None, run_target=None,
              out_dir: Path | None = None, seed: int = 0,
              params: dict[str, str] | None = None,
              snapshot: dict | None = None,
              source_family: str | None = None,
              depth: str = "threshold") -> dict:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if depth not in DEPTHS:
        raise ConfigError(f"depth must be one of {DEPTHS}, got {depth!r}")
    started = time.monotonic()
    for c in spec.objects:
        if (c.root_where is None) != (c.target_where is None):
            raise ConfigError(f"object {c.object} has root_where but no target_where; scope both sides or neither")
        for e in c.embeds:
            if (e.child_where is None) != (e.target_where is None):
                raise ConfigError(f"object {c.object} embed {e.array_path} has child_where but no target_where; "
                                  "scope both sides or neither")
    if ops and (run_source is None or run_target is None):
        raise ConfigError("--ops given but no query executors; tier 4 cannot run")
    # continuous: per-cycle Tier 1+2 plus sampled Tier 3, appended to the evidence log; the
    # result records the depth tier 3 actually ran at, never the deeper one that was asked for.
    if mode == "continuous":
        depth = "sampled"
    canon = Canonicalizer(rules)
    tiers = [tier1_counts(spec, source, target)]
    provenance_warnings = _snapshot_provenance_warnings(
        snapshot, source_family, spec, tiers[0])
    if tiers[0].passed:
        # Tier 1 failures are load defects or mapping-spec violations; nothing else runs.
        tiers.append(tier2_aggregates(spec, tol, canon, source, target))
        tiers.append(tier3_diffs(spec, tol, canon, source, target, seed, depth=depth))
        if ops and mode != "continuous":
            tiers.append(tier4_parity(ops, canon, tol, run_source, run_target))
    result = build_result(unit, mode, spec.version, tol.version, tiers,
                          seed=seed, params=params, snapshot=snapshot,
                          provenance_warnings=provenance_warnings, depth=depth,
                          cost=_cost(source, target, started))
    if out_dir is not None:
        write_outputs(out_dir, result)
    return result
