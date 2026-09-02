"""The four check tiers, in order of cost. Each returns a TierResult; the engine gates:
Tier 1 must be green before anything else runs.

All comparisons happen post-canonicalization through the mapping spec, never raw.
"""

from __future__ import annotations

import decimal
import random
from dataclasses import dataclass, field
from typing import Any

from .canon import MISSING, Canonicalizer
from .config import MappingSpec, ObjectMapping, Tolerances
from .paths import get_path


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


def tier1_counts(spec: MappingSpec, source, target) -> TierResult:
    """Counts THROUGH the mapping: root docs vs root rows; embedded array cardinality vs
    child-table rows. A naive docs-vs-rows count is wrong by construction for embeds."""
    findings, checks = [], 0
    for c in spec.objects:
        checks += 1
        src_n = source.row_count(c.root_table, c.root_where)
        tgt_n = target.target_row_count(c.object, c.target_where)
        if src_n != tgt_n:
            findings.append(Finding(c.object, "root_count",
                                    f"rows({c.root_table})={src_n} vs target rows={tgt_n}"))
        for e in c.embeds:
            checks += 1
            child_n = source.row_count(e.child_table, e.child_where)
            emb_n = target.nested_count(c.object, e.array_path, e.target_where)
            if child_n != emb_n:
                findings.append(Finding(c.object, "embed_cardinality",
                                        f"rows({e.child_table})={child_n} vs sum(len({e.array_path}))={emb_n}"))
    return TierResult(1, "counts_through_mapping", not findings, checks, findings)


def _agg_close(a: Any, b: Any, rel_tol: float) -> bool:
    if a is None and b is None:
        return True
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        denom = max(abs(a), abs(b), 1e-12)
        return abs(a - b) <= rel_tol * denom
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
# findings. Numericness comes from the declared target_type (Spark SQL type name), falling
# back to the observed source value.
NUMERIC_TARGET_TYPES = {"int", "integer", "bigint", "long", "smallint", "tinyint", "double",
                        "float", "decimal", "numeric", "number"}


def _is_numeric_field(f, source_sum: Any) -> bool:
    if f.target_type:
        return f.target_type.lower().split("(")[0] in NUMERIC_TARGET_TYPES
    return isinstance(source_sum, (int, float, decimal.Decimal)) and not isinstance(source_sum, bool)


def tier2_aggregates(spec: MappingSpec, tol: Tolerances, canon: Canonicalizer,
                     source, target) -> TierResult:
    findings, checks = [], 0
    deferred: list[str] = []
    for c in spec.objects:
        for f in c.fields:
            checks += 1
            s = source.field_aggregates(c.root_table, f.source, c.root_where)
            t = (target.field_aggregates(c.object, f.target, c.target_where)
                 if c.target_where is not None else target.field_aggregates(c.object, f.target))
            numeric = _is_numeric_field(f, s.get("sum"))
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
                    sv = float(sv) if hasattr(sv, "__float__") and not isinstance(sv, bool) else sv
                    tv = float(tv) if hasattr(tv, "__float__") and not isinstance(tv, bool) else tv
                if not _agg_close(sv, tv, tol.aggregate_rel_tol):
                    findings.append(Finding(c.object, f"aggregate_{stat}",
                                            f"field {f.source}->{f.target}", sv, tv, f.rules))
    stats = {"deferred_to_tier3": deferred} if deferred else {}
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


def tier3_diffs(spec: MappingSpec, tol: Tolerances, canon: Canonicalizer,
                source, target, seed: int = 0) -> TierResult:
    """Full keyed diff below the tolerance row threshold; keyed stratified sampling above.
    Embedded arrays with declared element keys/fields are value-graded; the rest are
    reported UNGRADED."""
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    rng = random.Random(seed)
    for c in spec.objects:
        n = source.row_count(c.root_table, c.root_where)
        sampled = n > tol.full_diff_row_threshold
        keys: list[Any] | None = None
        duplicate_counts = {}
        if sampled:
            first, last, reservoir = [], [], []
            seen = 0
            for raw_key in source.iter_keys(c.root_table, c.key_source, c.root_where):
                key = tuple(raw_key)
                duplicate_counts[key] = duplicate_counts.get(key, 0) + 1
                seen += 1
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
            fetched = source.fetch_keyed(c.root_table, c.key_source,
                                         [f.source for f in c.fields],
                                         where=c.root_where, keys=keys)
            src_rows = {tuple(r[k] for k in c.key_source): r for r in fetched}
            stats[c.object] = {"mode": "stratified_sample", "population": n,
                                   "sampled": len(src_rows),
                                   "coverage": round(len(src_rows) / n, 6) if n else 1.0}
        else:
            src_rows = {}
            for r in source.fetch_keyed(c.root_table, c.key_source,
                                        [f.source for f in c.fields], where=c.root_where):
                key = tuple(r[k] for k in c.key_source)
                duplicate_counts[key] = duplicate_counts.get(key, 0) + 1
                src_rows[key] = r
            stats[c.object] = {"mode": "full_diff", "population": n}
        for key, count in duplicate_counts.items():
            if count > 1:
                checks += 1
                findings.append(Finding(c.object, "duplicate_source_key",
                                        f"key={key} seen {count} times"))
        tgt_docs = {}
        proj = [f.target for f in c.fields] + [e.array_path for e in c.embeds]
        target_counts = {}
        for d in target.fetch_keyed(c.object, c.key_target, proj,
                                    where=c.target_where, keys=keys):
            kv = _get_path(d, c.key_target)
            key = (kv,) if not isinstance(kv, tuple) else kv
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
            if doc is None:
                findings.append(Finding(c.object, "missing_doc", f"key={k}"))
                continue
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
                findings.append(Finding(c.object, "extra_doc", f"key={k}"))
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
        s = [tuple(sorted((k, canon.apply(v, rules)[0]) for k, v in row.items()))
             for row in run_source(op)]
        t = [tuple(sorted((k, canon.apply(v, rules)[0]) for k, v in row.items()))
             for row in run_target(op)]
        if sorted(map(repr, s)) != sorted(map(repr, t)):
            findings.append(Finding(op.get("object", "?"), "parity_mismatch",
                                    f"op '{op.get('name', '?')}' result sets differ "
                                    f"(source {len(s)} rows, target {len(t)} rows)"))
    return TierResult(4, "app_level_parity", not findings, checks, findings)
