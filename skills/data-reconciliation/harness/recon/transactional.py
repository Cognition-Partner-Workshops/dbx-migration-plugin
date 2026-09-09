"""--mode transactional: the operational track's tiers, where both sides are live.

Warehouse tiers assume two still copies. An OLTP source with CDC into a Lakebase branch is
never still, so this mode adds what a set-based diff cannot say:

  tier 0  consistency_window   count + max(watermark) markers on both sides at open and at
                               close; a side that moved during the run is a finding, so a PASS
                               is scoped to a window that provably held. Markers alone cannot
                               see a write below the max or a balanced insert+delete, so a side
                               with neither a pinned snapshot nor an engine change token makes
                               the run ineligible for merge unless the tolerances accept that
  tier 5  pk_set_diff          key-range fingerprints on both sides (one statement each: count,
                               exact key sums and modular sums of squares, the same for the
                               watermark), then the keys and watermarks of every range whose
                               fingerprint differs; a swapped key or a moved watermark is caught
                               even when the counts agree. Missing keys whose source watermark
                               is newer than the target's applied watermark are in flight, not
                               defects. The allowance never covers a target-only key: a deleted
                               source row leaves no watermark to date the delete by, so deletes
                               must be drained before the run and every extra key is a finding
  tier 6  cdc_lag_ordering     max(source watermark) - max(target watermark) against the
                               tolerance, plus the per-key ordering that tier 5 streamed: a
                               target row ahead of its source row is a replay/ordering
                               violation, never lag; one behind an applied watermark is a lost
                               or misordered change
  tier 7  schema_parity        primary key, unique, foreign-key, not-null, index coverage,
                               check-constraint count and identity/sequence headroom, mapped
                               through the spec's column names

Tiers 1-3 still run; the context below tells them how many rows may legitimately differ.
"""

from __future__ import annotations

import datetime as dt
import decimal
import re
from dataclasses import dataclass, field
from typing import Any

from .adapters import SchemaFacts, StratifiedKeys, TransactionalSide
from .config import ConfigError, MappingSpec, ObjectMapping, Tolerances
from .tiers import Finding, TierResult
from .watermarks import check_comparable, family, instant, lag_seconds, later, same

# Keys listed per finding before the rest is summarised as a count.
MAX_KEYS_IN_FINDING = 20


@dataclass
class ObjectWindow:
    hwm_target: Any = None          # target's applied high-watermark at open
    in_flight: int = 0              # source rows changed after hwm_target (upper bound)
    isolation: tuple[str, str] = ("none", "none")


@dataclass
class KeyDiff:
    """What tier 5 learned from the streamed ranges of one object; tier 6 grades the ordering."""
    missing: list[tuple] = field(default_factory=list)
    extra: list[tuple] = field(default_factory=list)
    in_flight_missing: int = 0
    in_flight_updates: int = 0
    ahead: list[tuple] = field(default_factory=list)     # target watermark newer than source
    behind: list[tuple] = field(default_factory=list)    # target older, source already applied


@dataclass
class TransactionalContext:
    """Per-object facts the shared tiers consult in transactional mode."""
    windows: dict[str, ObjectWindow] = field(default_factory=dict)
    open_markers: dict[str, tuple[tuple, tuple]] = field(default_factory=dict)
    key_diffs: dict[str, KeyDiff] = field(default_factory=dict)

    def in_flight(self, c: ObjectMapping) -> int:
        return self.windows.get(c.object, ObjectWindow()).in_flight

    def hwm(self, c: ObjectMapping) -> Any:
        return self.windows.get(c.object, ObjectWindow()).hwm_target

    def row_in_flight(self, c: ObjectMapping, source_row: dict) -> bool:
        """A source row changed after the target's applied watermark is not yet expected there."""
        hwm = self.hwm(c)
        if hwm is None or not c.watermark_source:
            return False
        wm = source_row.get(c.watermark_source)
        return wm is not None and later(wm, hwm)

    def applied_where(self, c: ObjectMapping) -> str | None:
        """Source predicate selecting the rows the target is expected to hold already (watermark at
        or before the applied high-watermark, or no watermark), scoped by root_where."""
        hwm = self.hwm(c)
        if hwm is None or not c.watermark_source:
            return c.root_where
        applied = _applied_predicate(c.watermark_source, hwm)
        return f"({c.root_where}) AND {applied}" if c.root_where else applied

    def in_flight_keys(self, c: ObjectMapping, source) -> list[tuple]:
        """Keys of the source rows changed after the applied high-watermark (one statement)."""
        hwm = self.hwm(c)
        if hwm is None or not c.watermark_source:
            return []
        newer = _newer_predicate(c.watermark_source, hwm)
        where = f"({c.root_where}) AND {newer}" if c.root_where else newer
        return [tuple(r[k] for k in c.key_source)
                for r in source.fetch_keyed(c.root_table, c.key_source, [], where=where)]


def require_transactional(source, target) -> None:
    for side, adapter in (("source", source), ("target", target)):
        if not isinstance(adapter, TransactionalSide):
            raise ConfigError(f"--mode transactional needs a {side} adapter that implements "
                              f"TransactionalSide; {type(adapter).__name__} does not")


def open_window(spec: MappingSpec, source, target) -> TransactionalContext:
    """Pin both sides, read the opening markers, and measure the in-flight set per object."""
    ctx = TransactionalContext()
    iso = (source.open_window(), target.open_window())
    for c in spec.objects:
        win = ObjectWindow(isolation=iso)
        s_mark = source.window_marker(c.root_table, c.key_source, c.watermark_source, c.root_where)
        t_mark = target.window_marker(c.object, c.key_target, c.watermark_target, c.target_where)
        ctx.open_markers[c.object] = (s_mark, t_mark)
        if c.watermark_source and c.watermark_target:
            check_comparable(s_mark[1], t_mark[1],
                             f"{c.object}: {c.watermark_source} vs {c.watermark_target}")
            win.hwm_target = t_mark[1]
            if win.hwm_target is not None:
                newer = _newer_predicate(c.watermark_source, win.hwm_target)
                where = f"({c.root_where}) AND {newer}" if c.root_where else newer
                win.in_flight = source.row_count(c.root_table, where)
        ctx.windows[c.object] = win
    return ctx


def _newer_predicate(column: str, hwm: Any) -> str:
    """Rows changed after the target's applied watermark. Drivers deliver datetimes at microsecond
    precision while the engine may store more (SQL Server datetime2(7)), so a strict `>` against
    the truncated literal would count every row that shares the applied microsecond; compare from
    the next microsecond instead, matching what `later` can see on fetched rows."""
    if isinstance(hwm, dt.datetime):
        return f"{column} >= {_literal(hwm + dt.timedelta(microseconds=1))}"
    return f"{column} > {_literal(hwm)}"


def _applied_predicate(column: str, hwm: Any) -> str:
    """The complement of `_newer_predicate` that also keeps rows with no watermark: those are
    applied as far as the harness can tell, exactly as `row_in_flight` treats them."""
    if isinstance(hwm, dt.datetime):
        bound = f"{column} < {_literal(hwm + dt.timedelta(microseconds=1))}"
    else:
        bound = f"{column} <= {_literal(hwm)}"
    return f"({bound} OR {column} IS NULL)"


def _literal(value: Any) -> str:
    """Watermark literal for a predicate, as a UTC instant (see recon.watermarks). Only datetimes
    and numbers are accepted as watermarks; anything else cannot be compared across engines."""
    if isinstance(value, dt.datetime):
        return "'" + instant(value).isoformat(sep=" ", timespec="microseconds") + "'"
    if isinstance(value, dt.date):
        return f"'{value.isoformat()}'"
    if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
        raise ConfigError(f"watermark values must be datetimes or numbers, got {type(value).__name__}")
    return str(value)


def abandon_window(source, target) -> None:
    """Release both sides after a failed run; one side's failure never keeps the other pinned."""
    errors = []
    for side in (source, target):
        try:
            side.close_window()
        except Exception as exc:  # noqa: BLE001  driver-specific error type
            errors.append(exc)
    if errors:
        raise errors[0]


def close_window(spec: MappingSpec, tol: Tolerances, ctx: TransactionalContext,
                 source, target) -> TierResult:
    findings, checks = [], 0
    strength = {"source": source.window_strength(), "target": target.window_strength()}
    stats: dict[str, Any] = {"isolation": {"source": source.isolation if hasattr(source, "isolation") else "none",
                                           "target": target.isolation if hasattr(target, "isolation") else "none"},
                             "strength": strength, "markers": {}}
    for side, how in strength.items():
        if how == "markers" and not tol.accept_marker_only_window:
            checks += 1
            findings.append(Finding("*", "window_unproven",
                                    f"{side} pinned no snapshot and exposes no change token: "
                                    "(count, max watermark) markers cannot see an update below the "
                                    "max or a balanced insert+delete, so this run cannot be merge-"
                                    "eligible; enable snapshot isolation / grant the usage-stats "
                                    "view, or record accept_marker_only_window in the tolerances"))
        elif how == "markers":
            stats.setdefault("accepted_marker_only", []).append(side)
    for c in spec.objects:
        checks += 1
        s_open, t_open = ctx.open_markers[c.object]
        s_close = source.window_marker(c.root_table, c.key_source, c.watermark_source, c.root_where)
        t_close = target.window_marker(c.object, c.key_target, c.watermark_target, c.target_where)
        stats["markers"][c.object] = {"source_open": list(s_open), "source_close": list(s_close),
                                      "target_open": list(t_open), "target_close": list(t_close),
                                      "in_flight_at_open": ctx.in_flight(c)}
        if s_open != s_close:
            findings.append(Finding(c.object, "window_unstable",
                                    f"source moved during the run: {c.root_table} "
                                    f"open={s_open} close={s_close}", s_open, s_close))
        if t_open != t_close:
            findings.append(Finding(c.object, "window_unstable",
                                    f"target moved during the run: {c.object} "
                                    f"open={t_open} close={t_close}", t_open, t_close))
    source.close_window()
    target.close_window()
    return TierResult(0, "consistency_window", not findings, checks, findings, stats)


def _ranges(source: StratifiedKeys, c: ObjectMapping, n: int, tol: Tolerances) -> list[tuple]:
    """Contiguous key ranges built from the source strata: each range runs from the previous
    stratum's last key to this stratum's last key, plus two open-ended edges below the first
    and above the last source key. Strata alone would leave the gaps between them uncovered,
    and a target key living in a gap (a source row deleted since, or a stray insert) must be
    counted. Neighbouring ranges share their boundary key on both sides alike; the set diff
    dedupes any key seen twice."""
    n_strata = max(1, min(tol.pk_set_ranges, n))
    strata = source.key_strata(c.root_table, c.key_source, n_strata, c.root_where)
    if not strata:
        return []
    inner = [(strata[i - 1].hi if i else s.lo, s.hi) for i, s in enumerate(strata)]
    return [(None, strata[0].lo)] + inner + [(strata[-1].hi, None)]


def _kind(value: Any) -> str:
    """Digest family of a key/watermark value: what portable sum the adapters can compute."""
    return family(value)


def _fingerprint_complete(fps: list[tuple], nk: int, watermark: bool) -> bool:
    """True when every range carries a key digest (and a watermark digest if one is declared),
    so equal fingerprints really do mean equal key sets and equal watermarks."""
    return all(f[1] is not None and len(f[1]) == nk and (not watermark or f[2] is not None)
               for f in fps)


def tier5_pk_set(spec: MappingSpec, tol: Tolerances, ctx: TransactionalContext,
                 source, target) -> TierResult:
    """Full primary-key set comparison at range granularity. Both sides fingerprint every range
    in one statement (count, then exact sum and modular sum of squares per key column and for
    the watermark as epoch microseconds); only ranges whose fingerprints differ stream their
    keys and watermarks, which is where missing/extra keys and per-row ordering are graded. Two
    moments make any one- or two-row difference inside a range visible; a three-or-more-row
    substitution engineered to preserve both is the residual blind spot, and
    `pk_set_stream_every_range` removes it by streaming everything. Keys or watermarks with no
    portable digest (strings, uuids) stream every range as well. Cost: strata + one fingerprint
    statement per side per table, plus one fetch per side per streamed range."""
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    for c in spec.objects:
        n = source.row_count(c.root_table, c.root_where)
        checks += 1
        diff = KeyDiff()
        ctx.key_diffs[c.object] = diff
        if n == 0:
            t_n = target.target_row_count(c.object, c.target_where)
            stats[c.object] = {"ranges": 0, "population": 0, "target_population": t_n}
            if t_n:
                findings.append(Finding(c.object, "pk_extra_on_target",
                                        f"{t_n} target rows, source is empty"))
            continue
        if not isinstance(source, StratifiedKeys):
            raise ConfigError("tier 5 needs a source adapter with server-side key strata")
        ranges = _ranges(source, c, n, tol)
        nk = len(c.key_source)
        has_wm = bool(c.watermark_source and c.watermark_target)
        first = next((r[0] for r in ranges if r[0] is not None), ())
        key_kinds = [_kind(v) for v in first] if len(first) == nk else ["other"] * nk
        s_open_wm = ctx.open_markers[c.object][0][1] if has_wm else None
        wm_kind = _kind(s_open_wm) if has_wm and s_open_wm is not None else None
        complete = False
        if tol.pk_set_stream_every_range:
            fingerprint = "not used: every range streamed (pk_set_stream_every_range)"
            streamed_ranges = list(range(len(ranges)))
        else:
            s_fps = source.range_fingerprints(c.root_table, c.key_source, key_kinds,
                                              c.watermark_source, wm_kind, ranges, c.root_where)
            t_fps = target.range_fingerprints(c.object, c.key_target, key_kinds, c.watermark_target,
                                              wm_kind, ranges, c.target_where)
            complete = (_fingerprint_complete(s_fps, nk, has_wm)
                        and _fingerprint_complete(t_fps, nk, has_wm))
            if complete:
                fingerprint = "count+key_sum+key_sumsq" + ("+watermark_sum+watermark_sumsq" if has_wm else "")
                streamed_ranges = [i for i, (a, b) in enumerate(zip(s_fps, t_fps)) if a != b]
            else:
                fingerprint = "unavailable: every range streamed"
                streamed_ranges = list(range(len(ranges)))
        s_wm_cols = [c.watermark_source] if has_wm else []
        t_wm_cols = [c.watermark_target] if has_wm else []
        hwm = ctx.hwm(c)
        # edge ranges share one key with their neighbouring stratum, so every bucket is a set
        missing: set[tuple] = set()
        extra: set[tuple] = set()
        in_flight_missing: set[tuple] = set()
        in_flight_updates: set[tuple] = set()
        ahead: set[tuple] = set()
        behind: set[tuple] = set()
        streamed = 0
        for i in streamed_ranges:
            lo, hi = ranges[i]
            s_keys = source.keys_in_range(c.root_table, c.key_source, lo, hi, c.root_where, s_wm_cols)
            t_keys = target.keys_in_range(c.object, c.key_target, lo, hi, c.target_where, t_wm_cols)
            streamed += len(s_keys) + len(t_keys)
            s_index = {tuple(k[:nk]): k[nk:] for k in s_keys}
            t_index = {tuple(k[:nk]): k[nk:] for k in t_keys}
            for key, rest in s_index.items():
                s_wm = rest[0] if has_wm and rest else None
                unapplied = has_wm and hwm is not None and s_wm is not None and later(s_wm, hwm)
                if key not in t_index:
                    if unapplied:
                        in_flight_missing.add(key)
                    else:
                        missing.add(key)
                    continue
                if not has_wm:
                    continue
                t_wm = t_index[key][0] if t_index[key] else None
                if same(s_wm, t_wm):
                    continue
                if t_wm is not None and (s_wm is None or later(t_wm, s_wm)):
                    ahead.add(key)
                elif unapplied:
                    in_flight_updates.add(key)
                else:
                    behind.add(key)
            extra |= {k for k in t_index if k not in s_index}
        missing_l, extra_l = sorted(missing, key=repr), sorted(extra, key=repr)
        diff.missing, diff.extra, diff.in_flight_missing = missing_l, extra_l, len(in_flight_missing)
        diff.in_flight_updates = len(in_flight_updates)
        diff.ahead, diff.behind = sorted(ahead, key=repr), sorted(behind, key=repr)
        stats[c.object] = {"ranges": len(ranges), "population": n, "fingerprint": fingerprint,
                           "mismatched_ranges": len(streamed_ranges), "keys_streamed": streamed,
                           "missing_on_target": len(missing_l), "extra_on_target": len(extra_l),
                           "in_flight_missing": len(in_flight_missing),
                           "in_flight_updates": diff.in_flight_updates,
                           "rows_ahead_on_target": len(diff.ahead),
                           "rows_behind_on_target": len(diff.behind)}
        if missing_l:
            findings.append(Finding(c.object, "pk_missing_on_target",
                                    f"{len(missing_l)} source keys absent on target; first "
                                    f"{min(len(missing_l), MAX_KEYS_IN_FINDING)}: "
                                    f"{missing_l[:MAX_KEYS_IN_FINDING]}"))
        if extra_l:
            findings.append(Finding(c.object, "pk_extra_on_target",
                                    f"{len(extra_l)} target keys absent on source (undrained deletes "
                                    f"or stray writes; no tombstone evidence to tell them apart); "
                                    f"first {min(len(extra_l), MAX_KEYS_IN_FINDING)}: "
                                    f"{extra_l[:MAX_KEYS_IN_FINDING]}"))
    return TierResult(5, "pk_set_diff", not findings, checks, findings, stats)


def tier6_cdc(spec: MappingSpec, tol: Tolerances, ctx: TransactionalContext,
              source, target) -> TierResult:
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    for c in spec.objects:
        if not (c.watermark_source and c.watermark_target):
            stats[c.object] = {"watermark": None,
                               "note": "no watermark declared: the window markers alone prove stillness"}
            continue
        checks += 1
        s_open, t_open = ctx.open_markers[c.object]
        s_wm, t_wm = s_open[1], t_open[1]
        lag = lag_seconds(s_wm, t_wm)
        diff = ctx.key_diffs.get(c.object, KeyDiff())
        stats[c.object] = {"watermark": f"{c.watermark_source}->{c.watermark_target}",
                           "source_max": s_wm, "target_max": t_wm, "lag_s": lag,
                           "in_flight": ctx.in_flight(c),
                           "rows_ahead_on_target": len(diff.ahead),
                           "rows_behind_on_target": len(diff.behind)}
        if diff.ahead:
            findings.append(Finding(c.object, "row_ahead_of_source",
                                    f"{len(diff.ahead)} target rows carry a newer {c.watermark_target} "
                                    f"than their source row: replay or out-of-order apply; first "
                                    f"{min(len(diff.ahead), MAX_KEYS_IN_FINDING)}: "
                                    f"{diff.ahead[:MAX_KEYS_IN_FINDING]}"))
        if diff.behind:
            findings.append(Finding(c.object, "row_behind_applied_watermark",
                                    f"{len(diff.behind)} source rows changed at or before the target's "
                                    f"applied watermark {t_wm!r} but the target row is older: lost or "
                                    f"misordered change; first {min(len(diff.behind), MAX_KEYS_IN_FINDING)}: "
                                    f"{diff.behind[:MAX_KEYS_IN_FINDING]}"))
        if s_wm is None and t_wm is None:
            continue
        if lag is None:
            findings.append(Finding(c.object, "cdc_watermark_incomparable",
                                    f"max({c.watermark_source})={s_wm!r} vs max({c.watermark_target})={t_wm!r}",
                                    s_wm, t_wm))
        elif lag < 0:
            findings.append(Finding(c.object, "target_ahead_of_source",
                                    f"target watermark is {-lag:.3f}s newer than the source: replay "
                                    "or out-of-order apply", s_wm, t_wm))
        elif lag > tol.cdc_lag_max_s:
            findings.append(Finding(c.object, "cdc_lag_exceeded",
                                    f"lag {lag:.3f}s > cdc_lag_max_s={tol.cdc_lag_max_s}s "
                                    f"({ctx.in_flight(c)} source rows in flight)", s_wm, t_wm))
    return TierResult(6, "cdc_lag_ordering", not findings, checks, findings, stats)


def _column_map(spec: MappingSpec, c: ObjectMapping) -> dict[str, str]:
    """Source column -> target column, both lower-cased: catalogs return identifiers in stored
    case (SQL Server keeps whatever the DDL said, Postgres folds unquoted names) while the
    mapping spec is written by hand, so parity is judged on case-folded names throughout."""
    m = {s.lower(): t.lower() for s, t in zip(c.key_source, c.key_target)}
    m.update({f.source.lower(): f.target.lower() for f in c.fields})
    if c.watermark_source and c.watermark_target:
        m[c.watermark_source.lower()] = c.watermark_target.lower()
    if c.identity_source and c.identity_target:
        m[c.identity_source.lower()] = c.identity_target.lower()
    return m


def _table_map(spec: MappingSpec) -> dict[str, str]:
    return {o.root_table.split(".")[-1].lower(): o.object.lower() for o in spec.objects}


def _map_cols(cols: tuple, colmap: dict[str, str]) -> tuple:
    return tuple(colmap.get(col.lower(), col.lower()) for col in cols)


def _lower_facts(f: SchemaFacts) -> SchemaFacts:
    return SchemaFacts(
        primary_key=tuple(x.lower() for x in f.primary_key),
        unique={tuple(x.lower() for x in u) for u in f.unique},
        foreign_keys={(tuple(x.lower() for x in cols), ref.split(".")[-1].lower(),
                       tuple(x.lower() for x in rcols)) for cols, ref, rcols in f.foreign_keys},
        not_null={x.lower() for x in f.not_null},
        indexes={tuple(x.lower() for x in i) for i in f.indexes},
        check_count=f.check_count, identity_columns={x.lower() for x in f.identity_columns},
        partial={tuple(x.lower() for x in p) for p in f.partial},
        expression_unique={x.lower() for x in f.expression_unique},
        expression_indexes={x.lower() for x in f.expression_indexes})


def _map_expression(text: str, colmap: dict[str, str]) -> str:
    """Rewrite the source column names inside an index expression to their target names, so
    `lower(email)` on the source is expected as `lower(email_addr)` when the field is renamed."""
    return re.sub(r"[A-Za-z_][A-Za-z0-9_]*",
                  lambda m: colmap.get(m.group(0).lower(), m.group(0)), text)


def _covered(leading: tuple, facts: SchemaFacts) -> bool:
    candidates = [facts.primary_key] + list(facts.unique) + list(facts.indexes)
    return any(cand[:len(leading)] == leading for cand in candidates)


def tier7_schema_parity(spec: MappingSpec, tol: Tolerances, source, target) -> TierResult:
    """Constraints are compared both ways: a source constraint the target lacks lets bad data in,
    a target constraint the source lacks rejects writes the legacy application makes today.
    Indexes stay one-directional (an extra target index changes cost, not acceptance)."""
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    tables = _table_map(spec)

    def tightened(finding: Finding) -> None:
        if tol.accept_target_only_constraints:
            stats.setdefault("accepted_target_only_constraints", []).append(
                f"{finding.object}: {finding.check}: {finding.detail}")
        else:
            findings.append(finding)

    for c in spec.objects:
        colmap = _column_map(spec, c)
        mapped_targets = set(colmap.values())
        try:
            s_raw = source.schema_facts(c.root_table)
            t_raw = target.schema_facts(c.object)
        except NotImplementedError as exc:
            stats.setdefault("unverified", []).append(f"{c.object}: {exc}")
            continue
        checks += 1
        s, t, t_lower = _lower_facts(s_raw), t_raw, _lower_facts(t_raw)
        pk = _map_cols(s.primary_key, colmap)
        if pk != t_lower.primary_key:
            findings.append(Finding(c.object, "primary_key_mismatch",
                                    f"source {s.primary_key} -> expected {pk}, target {t.primary_key}",
                                    s.primary_key, t.primary_key))
        expected_unique = {_map_cols(u, colmap) for u in s.unique}
        for u in sorted(s.unique):
            if _map_cols(u, colmap) not in t_lower.unique:
                findings.append(Finding(c.object, "unique_missing",
                                        f"source unique {u} has no target unique {_map_cols(u, colmap)}"))
        for u in sorted(t_lower.unique - expected_unique - {t_lower.primary_key}):
            if set(u) <= mapped_targets:
                tightened(Finding(c.object, "unique_extra",
                                  f"target unique {u} has no source counterpart: legacy-valid "
                                  "duplicates would be rejected"))
            else:
                stats.setdefault("target_only_columns_unverified", []).append(
                    f"{c.object}: unique {u} covers a column outside the mapping")
        expected_fks: set[tuple] = set()
        for cols, ref, rcols in sorted(s.foreign_keys):
            ref_obj = tables.get(ref.split(".")[-1].lower())
            if ref_obj is None:
                stats.setdefault("foreign_keys_out_of_scope", []).append(f"{c.object}: {cols} -> {ref}")
                continue
            ref_map = next((_column_map(spec, o) for o in spec.objects if o.object.lower() == ref_obj), {})
            want = (_map_cols(cols, colmap), ref_obj, _map_cols(rcols, ref_map))
            expected_fks.add(want)
            if want not in t_lower.foreign_keys:
                findings.append(Finding(c.object, "foreign_key_missing",
                                        f"source FK {cols} -> {ref}{rcols} expected on target as {want}"))
        in_scope = {o.object.lower() for o in spec.objects}
        for cols, ref, rcols in sorted(t_lower.foreign_keys - expected_fks):
            if ref not in in_scope:
                stats.setdefault("foreign_keys_out_of_scope", []).append(
                    f"{c.object}: target FK {cols} -> {ref}")
            else:
                tightened(Finding(c.object, "foreign_key_extra",
                                  f"target FK {cols} -> {ref}{rcols} has no source counterpart: "
                                  "legacy-valid orphans would be rejected"))
        expected_not_null = {colmap[col] for col in s.not_null if col in colmap}
        for col in sorted(s.not_null):
            mapped = colmap.get(col, col)
            if col in colmap and mapped not in t_lower.not_null:
                findings.append(Finding(c.object, "not_null_missing",
                                        f"source NOT NULL {col} -> target {mapped} is nullable"))
        for col in sorted((t_lower.not_null & mapped_targets) - expected_not_null - set(t_lower.primary_key)):
            tightened(Finding(c.object, "not_null_extra",
                              f"target {col} is NOT NULL but its source column is nullable: "
                              "legacy-valid NULLs would be rejected"))
        for idx in sorted(s.indexes):
            if not _covered(_map_cols(idx, colmap), t_lower):
                findings.append(Finding(c.object, "index_missing",
                                        f"no target index leads with {_map_cols(idx, colmap)} (source {idx})"))
        if t.check_count < s.check_count:
            findings.append(Finding(c.object, "check_constraint_count_lower",
                                    f"source {s.check_count} CHECK constraints, target {t.check_count}",
                                    s.check_count, t.check_count))
        elif t.check_count > s.check_count:
            tightened(Finding(c.object, "check_constraint_count_higher",
                              f"source {s.check_count} CHECK constraints, target {t.check_count}: "
                              "the extra checks reject writes the source accepts",
                              s.check_count, t.check_count))
        for idx in sorted(s.partial):
            stats.setdefault("partial_indexes_unverified", []).append(
                f"{c.object}: source filtered index {idx} carries a predicate the harness cannot "
                f"translate; confirm its target counterpart by hand")
        # expression indexes: matched on their rewritten definition text, never dropped. A unique
        # one is a constraint (unique_missing/unique_extra semantics); a plain one is coverage the
        # harness cannot judge column-wise, so it is listed for a hand check
        expected_expr_unique = {_map_expression(e, colmap) for e in s.expression_unique}
        for expr in sorted(s.expression_unique):
            want = _map_expression(expr, colmap)
            if want not in t_lower.expression_unique:
                findings.append(Finding(c.object, "expression_unique_missing",
                                        f"source unique index on ({expr}) has no target unique index "
                                        f"on ({want})"))
        for expr in sorted(t_lower.expression_unique - expected_expr_unique):
            tightened(Finding(c.object, "expression_unique_extra",
                              f"target unique index on ({expr}) has no source counterpart: "
                              "legacy-valid rows that collide under the expression would be rejected"))
        for expr in sorted(s.expression_indexes):
            want = _map_expression(expr, colmap)
            if want not in t_lower.expression_indexes:
                stats.setdefault("expression_indexes_unverified", []).append(
                    f"{c.object}: source index on ({expr}) has no target index on ({want}); "
                    "confirm the access path by hand")
        seq_note = None
        if c.identity_source and c.identity_target:
            checks += 1
            try:
                t_next = target.identity_next(c.object, c.identity_target)
                s_next = source.identity_next(c.root_table, c.identity_source)
            except NotImplementedError as exc:
                stats.setdefault("unverified", []).append(f"{c.object} identity: {exc}")
                t_next = s_next = None
            else:
                (s_max,) = _max_key(source, c.root_table, c.identity_source, c.root_where)
                seq_note = {"source_next": s_next, "source_max": s_max, "target_next": t_next}
                if t_next is None:
                    findings.append(Finding(c.object, "sequence_missing",
                                            f"target column {c.identity_target} owns no sequence/identity"))
                elif s_max is not None and t_next <= int(s_max):
                    findings.append(Finding(c.object, "sequence_behind_source",
                                            f"target next value {t_next} <= source max "
                                            f"{c.identity_source}={s_max}: new inserts would collide",
                                            s_max, t_next))
        stats[c.object] = {"source": _facts_dict(s_raw), "target": _facts_dict(t_raw), "identity": seq_note}
    return TierResult(7, "schema_parity", not findings, checks, findings, stats)


def _max_key(source, table: str, column: str, where: str | None) -> tuple:
    return (source.field_aggregates(table, column, where)["max"],)


def _facts_dict(f: SchemaFacts) -> dict:
    return {"primary_key": list(f.primary_key), "unique": sorted(map(list, f.unique)),
            "foreign_keys": sorted([list(c), r, list(rc)] for c, r, rc in f.foreign_keys),
            "not_null": sorted(f.not_null), "indexes": sorted(map(list, f.indexes)),
            "check_count": f.check_count, "identity_columns": sorted(f.identity_columns),
            "partial": sorted(map(list, f.partial)),
            "expression_unique": sorted(f.expression_unique),
            "expression_indexes": sorted(f.expression_indexes)}
