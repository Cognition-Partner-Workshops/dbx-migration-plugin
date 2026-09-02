"""Orchestration: run tiers in cost order, gate on Tier 1, produce the result.

Deterministic and idempotent: same inputs produce the same verdict; safe to re-run.
"""

from __future__ import annotations

from pathlib import Path

from .canon import Canonicalizer
from .config import CanonRule, ConfigError, MappingSpec, Tolerances
from .report import build_result, write_outputs
from .tiers import tier1_counts, tier2_aggregates, tier3_diffs, tier4_parity

# fixture: same checks as live, run against a small fixture copy of the source during
# development and fix rounds. A fixture PASS is never a merge verdict; it only earns the unit
# its one live run.
MODES = ("fixture", "live", "snapshot", "continuous")


def run_recon(unit: str, mode: str, spec: MappingSpec, tol: Tolerances,
              rules: list[CanonRule], source, target,
              ops: list[dict] | None = None, run_source=None, run_target=None,
              out_dir: Path | None = None, seed: int = 0,
              params: dict[str, str] | None = None) -> dict:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    for c in spec.objects:
        if (c.root_where is None) != (c.target_where is None):
            raise ConfigError(f"object {c.object} has root_where but no target_where; scope both sides or neither")
        for e in c.embeds:
            if (e.child_where is None) != (e.target_where is None):
                raise ConfigError(f"object {c.object} embed {e.array_path} has child_where but no target_where; "
                                  "scope both sides or neither")
    if ops and (run_source is None or run_target is None):
        raise ConfigError("--ops given but no query executors; tier 4 cannot run")
    canon = Canonicalizer(rules)
    tiers = [tier1_counts(spec, source, target)]
    if tiers[0].passed:
        # Tier 1 failures are load defects or mapping-spec violations; nothing else runs.
        tiers.append(tier2_aggregates(spec, tol, canon, source, target))
        if mode == "continuous":
            # Per-cycle: Tier 1+2 plus sampled Tier 3, appended to the evidence log.
            sampled_tol = Tolerances(**{**tol.__dict__, "full_diff_row_threshold": 0})
            tiers.append(tier3_diffs(spec, sampled_tol, canon, source, target, seed))
        else:
            tiers.append(tier3_diffs(spec, tol, canon, source, target, seed))
        if ops and mode != "continuous":
            tiers.append(tier4_parity(ops, canon, tol, run_source, run_target))
    result = build_result(unit, mode, spec.version, tol.version, tiers,
                          seed=seed, params=params)
    if out_dir is not None:
        write_outputs(out_dir, result)
    return result
