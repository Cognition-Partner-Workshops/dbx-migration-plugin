"""The four check tiers, in order of cost. Each returns a TierResult; the engine gates:
Tier 1 must be green before anything else runs.

All comparisons happen post-canonicalization through the mapping spec, never raw.
"""

from __future__ import annotations

import decimal
import math
import random
from dataclasses import dataclass, field
from typing import Any

from .adapters import (
    BatchAggregates,
    ColumnTypes,
    KeyExcludingAggregates,
    NullKeyCounting,
    StratifiedKeys,
    SumProbe,
)
from .canon import MISSING, Canonicalizer
from .config import MappingSpec, ObjectMapping, Tolerances
from .paths import get_path
from .watermarks import family, instant, later

# Tier 3 sampling: at most this many strata per table; each stratum contributes
# ceil(sample_size / strata) keys plus the range edges (first/last keys are always graded).
MAX_STRATA = 32

# Tier 2 in transactional mode excludes the in-flight source keys from the target's aggregates
# by listing them in one statement; above this many keys the object is graded ungraded instead
# of shipping an oversized predicate (a feed that far behind fails tier 6 on lag anyway).
IN_FLIGHT_EXCLUSION_CAP = 10_000


@dataclass
class Finding:
    object: str
    check: str
    detail: str
    source_value: Any = None
    target_value: Any = None
    rules_applied: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"object": self.object, "check": self.check, "detail": self.detail,
                "source_value": repr(self.source_value), "target_value": repr(self.target_value),
                "rules_applied": self.rules_applied}


@dataclass
class TierResult:
    tier: int
    name: str
    passed: bool
    checks_run: int
    findings: list[Finding]
    stats: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"tier": self.tier, "name": self.name, "passed": self.passed,
                "checks_run": self.checks_run, "stats": self.stats,
                "findings": [f.as_dict() for f in self.findings]}


_get_path = get_path


def tier1_counts(spec: MappingSpec, source, target, ctx=None) -> TierResult:
    """Counts THROUGH the mapping: root docs vs root rows; embedded array cardinality vs
    child-table rows. A naive docs-vs-rows count is wrong by construction for embeds.
    In transactional mode `ctx` bounds how many source rows may not have reached the target yet."""
    findings, checks = [], 0
    stats: dict[str, Any] = {"source_counts": {}}
    for c in spec.objects:
        checks += 1
        src_n = source.row_count(c.root_table, c.root_where)
        tgt_n = target.target_row_count(c.object, c.target_where)
        stats["source_counts"][c.root_table] = src_n
        in_flight = ctx.in_flight(c) if ctx is not None else 0
        in_flight_deletes = ctx.in_flight_deletes(c) if ctx is not None else 0
        # inserts the feed has not applied leave the target short; deletes it has not applied
        # (proved by delete evidence) leave it long; the gap may be anywhere between
        if src_n != tgt_n and -in_flight_deletes <= src_n - tgt_n <= in_flight:
            stats.setdefault("count_gap_within_in_flight", {})[c.object] = {
                "gap": src_n - tgt_n, "in_flight": in_flight,
                **({"in_flight_deletes": in_flight_deletes} if in_flight_deletes else {})}
        elif src_n != tgt_n:
            flight = [f"{in_flight} in flight"] if in_flight else []
            flight += [f"{in_flight_deletes} deletes in flight"] if in_flight_deletes else []
            findings.append(Finding(c.object, "root_count",
                                    f"rows({c.root_table})={src_n} vs target rows={tgt_n}"
                                    + (f" ({', '.join(flight)})" if flight else "")))
        for e in c.embeds:
            checks += 1
            child_n = source.row_count(e.child_table, e.child_where)
            emb_n = target.nested_count(c.object, e.array_path, e.target_where)
            if child_n != emb_n:
                findings.append(Finding(c.object, "embed_cardinality",
                                        f"rows({e.child_table})={child_n} vs sum(len({e.array_path}))={emb_n}"))
    return TierResult(1, "counts_through_mapping", not findings, checks, findings, stats)


def _agg_close(a: Any, b: Any, rel_tol: float) -> bool:
    if a is None and b is None:
        return True
    if (isinstance(a, (int, float, decimal.Decimal)) and
            isinstance(b, (int, float, decimal.Decimal))):
        da, db = decimal.Decimal(str(a)), decimal.Decimal(str(b))
        denom = max(abs(da), abs(db), decimal.Decimal("1e-12"))
        return abs(da - db) <= decimal.Decimal(str(rel_tol)) * denom
    if family(a) == family(b) == "datetime":
        return instant(a) == instant(b)
    return a == b


# Rules that remap what counts as null/present. Aggregates are computed natively on each
# side (pre-canonicalization), so null_rate/distinct/min/max are not comparable for fields
# carrying these rules; they are deferred to Tier 3's keyed post-canonicalization diff.
NULL_SEMANTIC_RULES = {"empty_string_is_null", "null_missing_equiv"}
ORDER_PRESERVING_RULES = {
    "identity", "decimal_round", "datetime_utc_truncate_ms", "datetime_grid_333",
}

# SUM is only meaningful for numeric fields. SUM of a string column errors or returns NULL
# depending on the engine, so comparing sums on non-numeric fields manufactures false
# findings. Each side's numericness comes from its own declared type (source_type in the
# source engine's vocabulary, target_type in the target's); a side with no declaration, or
# one whose declared type disagrees with the other side (a conversion mapping), is probed in
# isolation so a failing SUM costs one statement instead of the batched one.
NUMERIC_TYPES = {
    "int", "integer", "bigint", "long", "smallint", "tinyint", "byteint", "double", "float", "real",
    "double precision", "decimal", "numeric", "number", "money", "smallmoney", "decimal128",
    "int2", "int4", "int8", "float4", "float8", "serial", "bigserial", "binary_float", "binary_double",
}
_NUMERIC_VALUE = (int, float, decimal.Decimal)


def _type_numeric(type_name: str) -> bool | None:
    if not type_name:
        return None
    return type_name.lower().split("(")[0].strip() in NUMERIC_TYPES


def sum_plan(own_type: str, other_type: str) -> str:
    """How one side obtains SUM for a field: 'batch' (declared numeric, in the table statement),
    'probe' (undeclared, or declared non-numeric while the other side is numeric: a conversion
    mapping the engine may still sum), or 'skip'."""
    own = _type_numeric(own_type)
    if own:
        return "batch"
    if own is None or _type_numeric(other_type):
        return "probe"
    return "skip"


def _side_numeric(plan: str, agg: dict) -> bool:
    if plan == "batch":
        return True
    s = agg.get("sum")
    return plan == "probe" and isinstance(s, _NUMERIC_VALUE) and not isinstance(s, bool)


def _is_numeric_field(f, s: dict, t: dict, s_plan: str | None = None, t_plan: str | None = None) -> bool:
    return (_side_numeric(s_plan or sum_plan(f.source_type, f.target_type), s)
            and _side_numeric(t_plan or sum_plan(f.target_type, f.source_type), t))


_PLAN_RANK = {"skip": 0, "probe": 1, "batch": 2}


def _column_plans(plans: list[tuple[str, str]]) -> dict[str, str]:
    """One SUM plan per physical column from the plans of every mapping that touches it: a
    column read once serves all of them, so the strongest request wins (batch > probe > skip)
    and no mapping's requirement is dropped when a column name repeats."""
    out: dict[str, str] = {}
    for col, p in plans:
        if col not in out or _PLAN_RANK[p] > _PLAN_RANK[out[col]]:
            out[col] = p
    return out


def _sum_plans(pairs: list[tuple[str, str, str]], adapter, table: str) -> dict[str, str]:
    """col -> `sum_plan` for one side. A field that would be probed is typed from the catalog
    instead when the adapter exposes it (`ColumnTypes`): the probe's error handling rolls the
    connection back, which on a pinned transactional window silently ends the snapshot."""
    plan = _column_plans([(col, sum_plan(own, other)) for col, own, other in pairs])
    if "probe" in plan.values() and isinstance(adapter, ColumnTypes):
        try:
            typed = adapter.numeric_columns(table)
        except NotImplementedError:  # an engine whose catalog the adapter does not read yet
            typed = None
        if typed is not None:
            typed_names = {name.lower() for name in typed}
            for col, p in plan.items():
                if p == "probe":
                    plan[col] = "batch" if col.lower() in typed_names else "skip"
    return plan


def _object_aggregates(c: ObjectMapping, source, target, source_where: str | None = None,
                       exclude_keys: list[tuple] | None = None
                       ) -> tuple[dict[str, dict], dict[str, dict], dict[str, str], dict[str, str]]:
    """All field aggregates for one object plus each side's plan: one statement per side when the
    adapter batches, one per field otherwise. Each side requests SUM per its own `sum_plan` (its
    declared type, or its catalog); a probed field keeps the batched metrics and adds one isolated
    SUM statement (`SumProbe`) so a SUM that errors never aborts the batched one. In transactional mode the source is bounded to its applied
    rows (`source_where`) and the target excludes the same keys (`exclude_keys`), so both
    aggregates describe one set."""
    s_where = source_where if source_where is not None else c.root_where
    s_plan = _sum_plans([(f.source, f.source_type, f.target_type) for f in c.fields], source, c.root_table)
    cols = list(s_plan)
    if isinstance(source, BatchAggregates):
        s_all = source.table_aggregates(c.root_table, cols, [k for k, p in s_plan.items() if p == "batch"],
                                        s_where)
        for col, p in s_plan.items():
            if p == "probe":
                if isinstance(source, SumProbe):
                    s_all[col]["sum"] = source.sum_probe(c.root_table, col, s_where)
                else:
                    s_all[col] = source.field_aggregates(c.root_table, col, s_where)
    else:
        s_all = {col: source.field_aggregates(c.root_table, col, s_where) for col in cols}
    t_plan = _sum_plans([(f.target, f.target_type, f.source_type) for f in c.fields], target, c.object)
    t_batch = [k for k, p in t_plan.items() if p == "batch"]

    def t_probe(col: str) -> dict:
        if exclude_keys is not None:
            return target.table_aggregates_excluding(c.object, [col], [col], list(c.key_target),
                                                     exclude_keys, c.target_where)[col]
        return (target.field_aggregates(c.object, col, c.target_where)
                if c.target_where is not None else target.field_aggregates(c.object, col))

    if exclude_keys is not None:
        t_all = target.table_aggregates_excluding(c.object, list(t_plan), t_batch, list(c.key_target),
                                                  exclude_keys, c.target_where)
    elif isinstance(target, BatchAggregates):
        t_all = target.table_aggregates(c.object, list(t_plan), t_batch, c.target_where)
    else:
        t_all = {col: t_probe(col) for col in t_plan}
    if exclude_keys is not None or isinstance(target, BatchAggregates):
        for col, p in t_plan.items():
            if p == "probe":
                if exclude_keys is None and isinstance(target, SumProbe):
                    t_all[col]["sum"] = target.sum_probe(c.object, col, c.target_where)
                else:
                    t_all[col] = t_probe(col)
    return s_all, t_all, s_plan, t_plan


def tier2_aggregates(spec: MappingSpec, tol: Tolerances, canon: Canonicalizer,
                     source, target, ctx=None) -> TierResult:
    findings, checks = [], 0
    deferred: list[str] = []
    applied_subset: dict[str, dict[str, Any]] = {}
    for c in spec.objects:
        in_flight = ctx.in_flight(c) if ctx is not None else 0
        in_flight_deletes = ctx.in_flight_deletes(c) if ctx is not None else 0
        if in_flight or in_flight_deletes:
            # aggregate the applied set on both sides: the source bounded by the target's applied
            # watermark, the target minus the very keys that are in flight (their target values
            # are the pre-change ones). Every applied row stays in the comparison, so drift in a
            # row tier 3's sample never visits is still caught
            if not isinstance(target, KeyExcludingAggregates):
                checks += 1
                findings.append(Finding(c.object, "aggregates_ungraded_in_flight",
                                        f"{in_flight} source rows in flight and the target adapter "
                                        "cannot exclude keys from its aggregates"))
                continue
            # the exclusion is one statement on the target: its capacity is the smaller of the
            # tier's own cap and what the adapter can bind for a key this wide. The in-flight
            # source keys exist on the source and the in-flight deletes do not (a tombstoned key
            # the source holds again is `reinserted`, an ordinary row), so the two sets are
            # disjoint and their sum over the cap settles it without reading keys
            cap = min(IN_FLIGHT_EXCLUSION_CAP, target.exclusion_capacity(len(c.key_target)))
            if in_flight + in_flight_deletes > cap:
                checks += 1
                findings.append(Finding(c.object, "aggregates_ungraded_in_flight",
                                        f"{in_flight} source rows in flight and {in_flight_deletes} "
                                        f"deletes in flight exceed the {cap}-key exclusion cap for a "
                                        f"{len(c.key_target)}-column key; let the feed catch up "
                                        "before grading aggregates"))
                continue
            keys = (ctx.in_flight_keys(c, source) if in_flight else []) + ctx.in_flight_delete_keys(c)
            applied_subset[c.object] = {
                "in_flight": in_flight,
                **({"in_flight_deletes": in_flight_deletes} if in_flight_deletes else {}),
                "excluded_keys": len(keys)}
            s_all, t_all, s_plan, t_plan = _object_aggregates(c, source, target, ctx.applied_where(c), keys)
        else:
            s_all, t_all, s_plan, t_plan = _object_aggregates(c, source, target)
        for f in c.fields:
            checks += 1
            s, t = s_all[f.source], t_all[f.target]
            numeric = _is_numeric_field(f, s, t, s_plan[f.source], t_plan[f.target])
            stats_to_check: tuple[str, ...] = ("null_rate", "distinct_count", "min", "max")
            if numeric:
                stats_to_check += ("sum",)
            if NULL_SEMANTIC_RULES & set(f.rules):
                stats_to_check = ("sum",) if numeric else ()
                deferred.append(f"{c.object}.{f.target}")
            rewriting = set(f.rules) - ORDER_PRESERVING_RULES - NULL_SEMANTIC_RULES
            if not (NULL_SEMANTIC_RULES & set(f.rules)) and rewriting:
                stats_to_check = ("null_rate",)
                deferred.append(f"{c.object}.{f.target}")
            elif "decimal_round" in f.rules:
                stats_to_check = tuple(s for s in stats_to_check
                                      if s not in ("sum", "distinct_count"))
                deferred.append(f"{c.object}.{f.target}")
            for stat in stats_to_check:
                sv, tv = s.get(stat), t.get(stat)
                if stat in ("min", "max", "sum"):
                    sv, _ = canon.apply(sv, f.rules)
                    tv, _ = canon.apply(tv, f.rules)
                if not _agg_close(sv, tv, tol.aggregate_rel_tol):
                    findings.append(Finding(c.object, f"aggregate_{stat}",
                                            f"field {f.source}->{f.target}", sv, tv, f.rules))
    stats: dict[str, Any] = {"deferred_to_tier3": deferred} if deferred else {}
    if applied_subset:
        stats["applied_subset"] = applied_subset
    return TierResult(2, "per_field_aggregates", not findings, checks, findings, stats)


def _grade_embeds(c: ObjectMapping, canon: Canonicalizer, tol: Tolerances,
                  source, src_rows: dict, tgt_docs: dict, sampled: bool,
                  findings: list[Finding], stats: dict[str, Any]) -> int:
    """Tier 3 value grading INSIDE embedded arrays. Tier 1 only proves cardinality; an
    embed without declared element keys/fields is loudly reported UNGRADED, never silently
    green."""
    checks = 0
    ungraded = []
    graded = {}
    for e in c.embeds:
        if not (e.parent_key and len(e.key_source) == 1 and e.key_target and e.fields):
            ungraded.append(e.array_path)
            continue
        n_elems = 0
        for row in source.fetch_keyed(e.child_table, e.parent_key + e.key_source,
                                      [f.source for f in e.fields], e.child_where):
            pk = tuple(row[k] for k in e.parent_key)
            if pk not in src_rows:
                if not sampled:
                    checks += 1
                    findings.append(Finding(c.object, "embed_orphan_child",
                                            f"{e.child_table} row parent key={pk} has no root row"))
                continue
            doc = tgt_docs.get(pk)
            if doc is None:
                continue  # missing_doc already reported for the parent
            elems = _get_path(doc, e.array_path)
            elems = elems if isinstance(elems, list) else []
            index = {(_get_path(el, e.key_target),): el for el in elems}
            ek = tuple(row[k] for k in e.key_source)
            el = index.get(ek)
            checks += 1
            n_elems += 1
            if el is None:
                findings.append(Finding(c.object, "missing_embedded_elem",
                                        f"{e.array_path} parent={pk} key={ek}"))
                continue
            for f in e.fields:
                sv = row.get(f.source, MISSING)
                tv = _get_path(el, f.target)
                ok, fired = canon.equal(sv, tv, f.rules, tol.numeric_abs_tol)
                if not ok:
                    findings.append(Finding(c.object, "embed_field_diff",
                                            f"{e.array_path} parent={pk} key={ek} "
                                            f"field {f.source}->{f.target}", sv, tv, fired))
        graded[e.array_path] = n_elems
    if ungraded:
        stats.setdefault("embeds_ungraded", []).extend(
            f"{c.object}.{p}" for p in ungraded)
    if graded:
        stats.setdefault("embeds_graded", {}).update(
            {f"{c.object}.{p}": n for p, n in graded.items()})
    return checks


def _stratified_keys(c: ObjectMapping, source: StratifiedKeys, n: int, sample_size: int,
                     rng: random.Random) -> tuple[list[tuple], int]:
    """Pick ~sample_size keys server-side: n_strata equal-count key ranges, a seeded set of
    positions inside each, plus every range's first and last key. Only chosen keys cross the
    wire (one strata statement + one per stratum)."""
    n_strata = max(1, min(MAX_STRATA, sample_size, n))
    strata = source.key_strata(c.root_table, c.key_source, n_strata, c.root_where)
    per = max(1, math.ceil(sample_size / max(1, len(strata))))
    chosen: set[tuple] = set()
    for s in strata:
        if s.n <= 0:
            continue
        positions = sorted({1, s.n} | set(rng.sample(range(1, s.n + 1), min(per, s.n))))
        chosen.update(source.sample_keys(c.root_table, c.key_source, s.lo, s.hi, positions, c.root_where))
    return sorted(chosen), len(strata)


def _null_key_rows(c: ObjectMapping, source, target) -> dict[str, int]:
    """Rows per side whose comparison key has a NULL component, one statement per side that can
    count them. Every keyed path (MIN/MAX strata, IN-list fetches, dict lookups) skips such rows,
    so a nonzero count means the key does not identify the table and Tier 3 cannot clear it."""
    out: dict[str, int] = {}
    if isinstance(source, NullKeyCounting):
        out["source"] = source.null_key_count(c.root_table, c.key_source, c.root_where)
    if isinstance(target, NullKeyCounting):
        out["target"] = target.null_key_count(c.object, c.key_target, c.target_where)
    return out


def tier3_diffs(spec: MappingSpec, tol: Tolerances, canon: Canonicalizer,
                source, target, seed: int = 0, depth: str = "threshold", ctx=None) -> TierResult:
    """Full keyed diff below the tolerance row threshold; keyed stratified sampling above.
    `depth` overrides the threshold: "full" always diffs every key, "sampled" always samples.
    Embedded arrays with declared element keys/fields are value-graded; the rest are
    reported UNGRADED. In transactional mode `ctx` classifies source rows changed after the
    target's applied watermark as in flight (not graded) and a target row newer than its
    source row as an ordering violation."""
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    rng = random.Random(seed)
    for c in spec.objects:
        wm = bool(ctx is not None and c.watermark_source and c.watermark_target)
        src_cols = [f.source for f in c.fields] + ([c.watermark_source] if wm else [])
        n = source.row_count(c.root_table, c.root_where)
        null_keys = _null_key_rows(c, source, target)
        for side, count in null_keys.items():
            if count:
                checks += 1
                findings.append(Finding(c.object, "null_comparison_key",
                                        f"{side}: {count} row(s) with a NULL component in the comparison key "
                                        f"{c.key_source if side == 'source' else c.key_target}; such rows can be "
                                        "neither matched nor sampled, so the key does not identify every row"))
        if depth == "full":
            sampled = False
        elif depth == "sampled":
            sampled = True
        else:
            sampled = n > tol.full_diff_row_threshold
        keys: list[Any] | None = None
        duplicate_source_runs = []
        previous_source_key = None
        source_run_count = 0

        def record_source_key(key, runs=duplicate_source_runs):
            nonlocal previous_source_key, source_run_count
            if source_run_count and key == previous_source_key:
                source_run_count += 1
                return
            if source_run_count > 1:
                runs.append((previous_source_key, source_run_count))
            previous_source_key = key
            source_run_count = 1

        if sampled and isinstance(source, StratifiedKeys):
            keys, n_strata = _stratified_keys(c, source, n, tol.sample_size, rng)
            fetched = source.fetch_keyed(c.root_table, c.key_source, src_cols,
                                         where=c.root_where, keys=keys)
            src_rows = {tuple(r[k] for k in c.key_source): r for r in fetched}
            dup_keys = source.duplicate_key_count(c.root_table, c.key_source, c.root_where)
            duplicate_source_runs.extend([(None, 2)] * dup_keys)
            stats[c.object] = {"mode": "stratified_sample", "sampling": "stratified",
                               "strata": n_strata, "population": n, "sampled": len(src_rows),
                               "coverage": round(len(src_rows) / n, 6) if n else 1.0}
        elif sampled:
            first, last, reservoir = [], [], []
            for seen, raw_key in enumerate(source.iter_keys(c.root_table, c.key_source, c.root_where), 1):
                key = tuple(raw_key)
                record_source_key(key)
                if len(first) < 2:
                    first.append(key)
                last = (last + [key])[-2:]
                if len(reservoir) < tol.sample_size:
                    reservoir.append(key)
                else:
                    slot = rng.randrange(seen)
                    if slot < tol.sample_size:
                        reservoir[slot] = key
            chosen = set(first + last + reservoir)
            keys = sorted(chosen)
            fetched = source.fetch_keyed(c.root_table, c.key_source, src_cols,
                                         where=c.root_where, keys=keys)
            src_rows = {tuple(r[k] for k in c.key_source): r for r in fetched}
            stats[c.object] = {"mode": "stratified_sample", "sampling": "reservoir",
                               "population": n, "sampled": len(src_rows),
                               "coverage": round(len(src_rows) / n, 6) if n else 1.0}
        else:
            src_rows = {}
            for r in source.fetch_keyed(c.root_table, c.key_source, src_cols, where=c.root_where):
                key = tuple(r[k] for k in c.key_source)
                record_source_key(key)
                src_rows[key] = r
            stats[c.object] = {"mode": "full_diff", "population": n}
        stats[c.object]["null_key_rows"] = null_keys
        if source_run_count > 1:
            duplicate_source_runs.append((previous_source_key, source_run_count))
        stats[c.object]["duplicate_source_key_count"] = len(duplicate_source_runs)
        for key, count in duplicate_source_runs:
            if count > 1:
                checks += 1
                findings.append(Finding(c.object, "duplicate_source_key",
                                        f"key={key} seen {count} times" if key is not None
                                        else "a source key occurs more than once (server-side count)"))
        tgt_docs = {}
        proj = ([f.target for f in c.fields] + [e.array_path for e in c.embeds]
                + ([c.watermark_target] if wm else []))
        target_counts = {}
        in_flight_rows = 0
        in_flight_deletes = set(ctx.in_flight_delete_keys(c)) if ctx is not None else set()
        deleted_in_flight = 0
        for d in target.fetch_keyed(c.object, c.key_target, proj,
                                    where=c.target_where, keys=keys):
            key = tuple(_get_path(d, key_field) for key_field in c.key_target)
            target_counts[key] = target_counts.get(key, 0) + 1
            tgt_docs[key] = d
        for key, count in target_counts.items():
            if count > 1:
                checks += 1
                findings.append(Finding(c.object, "duplicate_target_key",
                                        f"key={key} seen {count} times"))
        for k, row in src_rows.items():
            checks += 1
            doc = tgt_docs.get(k)
            if wm and ctx.row_in_flight(c, row):
                in_flight_rows += 1
                continue
            if doc is None:
                findings.append(Finding(c.object, "missing_doc", f"key={k}"))
                continue
            if wm:
                s_wm, t_wm = row.get(c.watermark_source), _get_path(doc, c.watermark_target)
                if s_wm is not None and t_wm is not None and later(t_wm, s_wm):
                    findings.append(Finding(c.object, "row_ahead_of_source",
                                            f"key={k} target {c.watermark_target} newer than source "
                                            f"{c.watermark_source}", s_wm, t_wm))
            for f in c.fields:
                sv = row.get(f.source, MISSING)
                tv = _get_path(doc, f.target)
                ok, fired = canon.equal(sv, tv, f.rules, tol.numeric_abs_tol)
                if not ok:
                    findings.append(Finding(c.object, "field_diff",
                                            f"key={k} field {f.source}->{f.target}",
                                            sv, tv, fired))
        for k in tgt_docs:
            if k not in src_rows and not sampled:
                checks += 1
                if k in in_flight_deletes:
                    deleted_in_flight += 1
                    continue
                findings.append(Finding(c.object, "extra_doc", f"key={k}"))
        if wm:
            stats[c.object]["in_flight_rows"] = in_flight_rows
        if in_flight_deletes:
            stats[c.object]["in_flight_deletes"] = deleted_in_flight
        checks += _grade_embeds(c, canon, tol, source, src_rows, tgt_docs, sampled,
                                findings, stats)
    return TierResult(3, "keyed_diffs", not findings, checks, findings, stats)


def tier4_parity(ops: list[dict], canon: Canonicalizer, tol: Tolerances,
                 run_source, run_target) -> TierResult:
    """Replay recorded representative operations against both stacks. `run_source` and
    `run_target` execute one recorded op and return a list of result rows/docs; the unit
    supplies them (this is the one tier that is never delegated)."""
    findings, checks = [], 0
    for op in ops:
        checks += 1
        rules = list(op.get("rules", []))
        s = [{k: canon.apply(v, rules)[0] for k, v in row.items()}
             for row in run_source(op)]
        t = [{k: canon.apply(v, rules)[0] for k, v in row.items()}
             for row in run_target(op)]
        unmatched_source = 0
        unmatched_target = [True] * len(t)
        for source_row in s:
            match = None
            for i, target_row in enumerate(t):
                if not unmatched_target[i] or source_row.keys() != target_row.keys():
                    continue
                if all(canon.equal(source_row[k], target_row[k], rules,
                                   tol.numeric_abs_tol)[0] for k in source_row):
                    match = i
                    break
            if match is None:
                unmatched_source += 1
            else:
                unmatched_target[match] = False
        unmatched_target_count = sum(unmatched_target)
        if unmatched_source or unmatched_target_count:
            findings.append(Finding(op.get("object", "?"), "parity_mismatch",
                                    f"op {op.get('name', '?')}: {unmatched_source} source rows "
                                    f"unmatched, {unmatched_target_count} target rows unmatched"))
    return TierResult(4, "app_level_parity", not findings, checks, findings)
