
from __future__ import annotations

import sys
import time
from pathlib import Path

from .adapters import UC_PIPELINE_KINDS, StatementCounting
from .canon import Canonicalizer
from .config import CanonRule, ConfigError, MappingSpec, Tolerances
from .report import build_result, write_outputs
from .structure import tier0_structural_parity
from .tiers import tier1_counts, tier2_aggregates, tier3_diffs, tier4_parity
from .transactional import (
    abandon_window,
    close_window,
    open_window,
    require_transactional,
    tier5_pk_set,
    tier6_cdc,
    tier7_schema_parity,
)

MODES = ("fixture", "live", "snapshot", "continuous", "transactional", "structural")
PLANNED_MODES: tuple[str, ...] = ()

DEPTHS = ("threshold", "sampled", "full")

def _cost(source, target, started: float, ctx=None) -> dict:
    def side(adapter):
        if isinstance(adapter, StatementCounting):
            return adapter.statements, adapter.rows_fetched
        return None, None
    s_stmts, s_rows = side(source)
    t_stmts, t_rows = side(target)
    cost = {"source_statements": s_stmts, "source_rows_fetched": s_rows,
            "target_statements": t_stmts, "target_rows_fetched": t_rows,
            "elapsed_s": round(time.monotonic() - started, 3)}
    if ctx is not None and ctx.deletes:
        cost["delete_evidence_statements"] = dict(ctx.evidence_statements)
    return cost

def _snapshot_provenance_warnings(snapshot: dict | None, source_family: str | None,
                                  spec: MappingSpec, tier1) -> list[str]:
    if snapshot is None or tier1 is None:
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

def _run_tiers(spec: MappingSpec, tol: Tolerances, canon: Canonicalizer, source, target,
               seed: int, depth: str, mode: str, ops: list[dict] | None, run_source, run_target,
               ctx, own_principals: frozenset[str] = frozenset()) -> list:
    if mode == "structural":
        return [tier0_structural_parity(spec, tol, source, target, catalog_only=True,
                                        own_principals=own_principals)]
    tiers = [tier1_counts(spec, source, target, ctx=ctx)]
    if tiers[0].passed:
        tiers.append(tier2_aggregates(spec, tol, canon, source, target, ctx=ctx))
        tiers.append(tier3_diffs(spec, tol, canon, source, target, seed, depth=depth, ctx=ctx))
        if ops and mode != "continuous":
            tiers.append(tier4_parity(ops, canon, tol, run_source, run_target))
    if ctx is not None:
        tiers.append(tier5_pk_set(spec, tol, ctx, source, target))
        tiers.append(tier6_cdc(spec, tol, ctx, source, target))
        tiers.append(tier7_schema_parity(spec, tol, source, target, own_principals=own_principals))
        tiers.insert(0, close_window(spec, tol, ctx, source, target))
    elif mode != "continuous":
        tiers.insert(0, tier0_structural_parity(spec, tol, source, target,
                                                own_principals=own_principals))
    return tiers

def derived_rerun_posture(spec: MappingSpec, target) -> tuple[str, dict[str, str], str | None]:
    table_kind = getattr(target, "table_kind", None)
    if getattr(target, "target_kind", None) != "databricks" or table_kind is None:
        return spec.rerun_posture, {}, None
    kinds = {c.object: table_kind(c.object) for c in spec.objects}
    present = {k for k in kinds.values() if k}
    if present and present <= UC_PIPELINE_KINDS and all(kinds.values()):
        return "not_applicable", kinds, f"not_applicable({'/'.join(sorted(present))})"
    return spec.rerun_posture, kinds, None

def _report_release_failure(exc: BaseException, note: str) -> None:
    try:
        print(f"dbx-recon: {note}", file=sys.stderr, flush=True)
    except Exception:  # noqa: BLE001, S110  reporting must never replace the run's error
        pass
    try:
        exc.add_note(note)
    except AttributeError:  # 3.10
        exc.__dict__.setdefault("__notes__", []).append(note)

def run_recon(unit: str, mode: str, spec: MappingSpec, tol: Tolerances,
              rules: list[CanonRule], source, target,
              ops: list[dict] | None = None, run_source=None, run_target=None,
              out_dir: Path | None = None, seed: int = 0,
              params: dict[str, str] | None = None,
              snapshot: dict | None = None,
              source_family: str | None = None,
              depth: str = "threshold", type_map: dict | None = None,
              routine_parity: list[dict] | None = None,
              routine_writers: list[str] | None = None,
              routine_analysis_missing: bool = False,
              routine_dependencies: str | None = None,
              rerun_proof: dict | None = None,
              own_principals: frozenset[str] = frozenset()) -> dict:
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
    if mode == "continuous":
        depth = "sampled"
    canon = Canonicalizer(rules)
    ctx = None
    if mode == "transactional":
        require_transactional(source, target)
    try:
        if mode == "transactional":
            ctx = open_window(spec, source, target, tol)
        tiers = _run_tiers(spec, tol, canon, source, target, seed, depth, mode, ops,
                           run_source, run_target, ctx, own_principals)
    except BaseException as exc:
        if mode == "transactional":
            for err in abandon_window(source, target):
                _report_release_failure(exc, str(err))
        raise
    provenance_warnings = _snapshot_provenance_warnings(
        snapshot, source_family, spec, next((t for t in tiers if t.tier == 1), None))
    rerun_posture, table_kinds, posture_reason = (
        (spec.rerun_posture, {}, None) if mode == "structural" else derived_rerun_posture(spec, target))
    result = build_result(unit, mode, spec.version, tol.version, tiers,
                          seed=seed, params=params, snapshot=snapshot,
                          provenance_warnings=provenance_warnings, depth=depth,
                          cost=_cost(source, target, started, ctx), type_map=type_map,
                          routine_parity=routine_parity, routine_writers=routine_writers,
                          routine_analysis_missing=routine_analysis_missing,
                          routine_dependencies=routine_dependencies,
                          rerun_proof=rerun_proof, rerun_posture=rerun_posture,
                          grading={c.object: c.grading for c in spec.objects},
                          blocking_findings=tol.blocking_findings)
    if table_kinds:
        result["target_table_kinds"] = table_kinds
    if posture_reason:
        result["rerun_posture_reason"] = posture_reason
    if out_dir is not None:
        write_outputs(out_dir, result)
    return result
