
from __future__ import annotations

import datetime as dt
import decimal
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from .adapters import (
    AppliedPosition,
    DeleteEvent,
    DeleteEvidence,
    DictionaryError,
    KeyExcludingAggregates,
    SchemaFacts,
    StatementCounting,
    StratifiedKeys,
    TransactionalSide,
    UC_PIPELINE_KINDS,
    WholeNumberColumns,
    normalize_sql_text,
)
from .config import ConfigError, MappingSpec, ObjectMapping, Tolerances
from .structure import (
    CATEGORIES,
    compare_grants,
    compare_identity_columns,
    compare_triggers,
    mask_unsupported,
    diff_by_object,
    structural_checks,
)
from .tiers import Finding, TierResult, blocking
from .watermarks import (
    EPOCH_SCALE,
    check_comparable,
    family,
    in_form_of,
    lag_seconds,
    lag_units,
    later,
    literal,
    same,
)

MAX_KEYS_IN_FINDING = 20

@dataclass
class ObjectWindow:
    hwm_target: Any = None          # target's applied high-watermark at open
    in_flight: int = 0              # source rows changed after hwm_target (upper bound)
    isolation: tuple[str, str] = ("none", "none")

@dataclass
class KeyDiff:
    missing: list[tuple] = field(default_factory=list)
    extra: list[tuple] = field(default_factory=list)
    in_flight_missing: int = 0
    in_flight_updates: int = 0
    ahead: list[tuple] = field(default_factory=list)     # target watermark newer than source
    behind: list[tuple] = field(default_factory=list)    # target older, source already applied
    in_flight_deletes: list[tuple] = field(default_factory=list)  # target-only, deleted in time
    delete_lagging: list[tuple] = field(default_factory=list)     # target-only, deleted too long ago
    in_flight_delete_max_wm: Any = None  # newest target watermark among the in-flight deletes

EVIDENCE_STRICT = ("absent", "unsupported", "no_position", "unavailable", "incompatible",
                   "retention_gap", "ahead_of_horizon")

@dataclass
class DeleteEvidenceResult:
    status: str
    kind: str | None = None
    applied: Any = None
    horizon: tuple = (None, None)
    events: int = 0
    in_flight: dict[tuple, DeleteEvent] = field(default_factory=dict)
    aged: dict[tuple, DeleteEvent] = field(default_factory=dict)
    reinserted: dict[tuple, DeleteEvent] = field(default_factory=dict)
    detail: str = ""

    def as_stats(self) -> dict[str, Any]:
        return {"status": self.status, "kind": self.kind, "applied_position": _pos(self.applied),
                "horizon": [_pos(self.horizon[0]), _pos(self.horizon[1])], "events": self.events,
                "in_flight_deletes": len(self.in_flight), "aged_deletes": len(self.aged),
                "reinserted": len(self.reinserted), "detail": self.detail}

def _pos(p: Any) -> Any:
    return p.hex() if isinstance(p, (bytes, bytearray, memoryview)) else p

def _compatible(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return False
    if isinstance(a, int) and isinstance(b, int):
        return True
    return isinstance(a, bytes) and isinstance(b, bytes) and len(a) == len(b)

def _as_position(value: Any) -> Any:
    return bytes(value) if isinstance(value, (bytearray, memoryview)) else value

def _successor(p: Any) -> Any:
    if isinstance(p, int):
        return p + 1
    return (int.from_bytes(p, "big") + 1).to_bytes(len(p), "big")

def resolve_delete_evidence(c: ObjectMapping, tol: Tolerances, source, target) -> DeleteEvidenceResult:
    de = c.delete_evidence
    if de is None:
        return DeleteEvidenceResult("absent", detail="no delete_evidence declared for this object")
    if not isinstance(source, DeleteEvidence) or not isinstance(target, AppliedPosition):
        return DeleteEvidenceResult(
            "unsupported", de.kind,
            detail=f"{type(source).__name__} / {type(target).__name__} do not expose delete "
                   "evidence and an applied position")
    kind = source.delete_evidence_kind()
    if kind != de.kind:
        raise ConfigError(f"{c.object}: delete_evidence.kind {de.kind!r} but the source adapter "
                          f"reads {kind!r}")
    applied = _as_position(target.applied_position(de.applied_table, de.applied_column,
                                                   de.applied_where))
    if applied is None:
        return DeleteEvidenceResult("no_position", kind,
                                    detail=f"{de.applied_table}.{de.applied_column} holds no applied position")
    lo, hi = (_as_position(p) for p in source.evidence_horizon(de.capture))
    result = DeleteEvidenceResult("ok", kind, applied, (lo, hi))
    if lo is None or hi is None:
        result.status, result.detail = "unavailable", f"capture {de.capture!r} retains no positions"
        return result
    if not (_compatible(applied, lo) and _compatible(applied, hi)):
        result.status = "incompatible"
        result.detail = (f"applied position {type(applied).__name__} cannot be ordered against "
                         f"{kind} positions {type(lo).__name__}")
        return result
    if applied < lo and _successor(applied) < lo:
        result.status = "retention_gap"
        result.detail = (f"target applied position {_pos(applied)} is older than the oldest "
                         f"retained change {_pos(lo)}: deletes between them are unknowable")
        return result
    if applied > hi:
        result.status = "ahead_of_horizon"
        result.detail = (f"target applied position {_pos(applied)} is newer than the newest change "
                         f"the capture has produced {_pos(hi)}: the checkpoint cannot come from this "
                         f"capture")
        return result
    if applied == hi:
        result.detail = "target has applied every retained change"
        return result
    latest: dict[tuple, DeleteEvent] = {}
    for ev in source.deletes_since(de.capture, c.key_source, applied, hi, c.root_where):
        pos = _as_position(ev.position)
        if not _compatible(pos, applied):
            result.status = "incompatible"
            result.detail = f"delete event position {type(pos).__name__} does not match {type(applied).__name__}"
            result.in_flight, result.aged = {}, {}
            return result
        if pos <= applied or pos > hi:
            continue
        result.events += 1
        key = tuple(ev.key)
        if key not in latest or pos > _as_position(latest[key].position):
            latest[key] = ev
    present = set()
    if latest:
        present = {tuple(r[k] for k in c.key_source)
                   for r in source.fetch_keyed(c.root_table, c.key_source, [], where=c.root_where,
                                               keys=list(latest))}
    for key, ev in latest.items():
        if key in present:
            result.reinserted[key] = ev
        else:
            (result.in_flight if ev.age_s <= tol.cdc_lag_max_s else result.aged)[key] = ev
    result.detail = (f"{result.events} delete events after the applied position"
                     + (f", {len(result.reinserted)} keys since reinserted" if result.reinserted else ""))
    return result

@dataclass
class TransactionalContext:
    windows: dict[str, ObjectWindow] = field(default_factory=dict)
    open_markers: dict[str, tuple[tuple, tuple]] = field(default_factory=dict)
    key_diffs: dict[str, KeyDiff] = field(default_factory=dict)
    render: Callable[[Any], str] = literal
    strength_open: dict[str, str] = field(default_factory=dict)
    deletes: dict[str, DeleteEvidenceResult] = field(default_factory=dict)
    evidence_statements: dict[str, int] = field(default_factory=lambda: {"source": 0, "target": 0})

    def in_flight(self, c: ObjectMapping) -> int:
        return self.windows.get(c.object, ObjectWindow()).in_flight

    def delete_evidence(self, c: ObjectMapping) -> DeleteEvidenceResult:
        return self.deletes.get(c.object) or DeleteEvidenceResult("absent")

    def in_flight_deletes(self, c: ObjectMapping) -> int:
        return len(self.delete_evidence(c).in_flight)

    def in_flight_delete_keys(self, c: ObjectMapping) -> list[tuple]:
        return sorted(self.delete_evidence(c).in_flight, key=repr)

    def hwm(self, c: ObjectMapping) -> Any:
        return self.windows.get(c.object, ObjectWindow()).hwm_target

    def row_in_flight(self, c: ObjectMapping, source_row: dict) -> bool:
        hwm = self.hwm(c)
        if hwm is None or not c.watermark_source:
            return False
        wm = source_row.get(c.watermark_source)
        return wm is not None and later(wm, hwm)

    def applied_where(self, c: ObjectMapping) -> str | None:
        hwm = self.hwm(c)
        if hwm is None or not c.watermark_source:
            return c.root_where
        applied = _applied_predicate(c.watermark_source, hwm, self.render)
        return f"({c.root_where}) AND {applied}" if c.root_where else applied

    def in_flight_keys(self, c: ObjectMapping, source) -> list[tuple]:
        hwm = self.hwm(c)
        if hwm is None or not c.watermark_source:
            return []
        newer = _newer_predicate(c.watermark_source, hwm, self.render)
        where = f"({c.root_where}) AND {newer}" if c.root_where else newer
        return [tuple(r[k] for k in c.key_source)
                for r in source.fetch_keyed(c.root_table, c.key_source, [], where=where)]

def require_transactional(source, target) -> None:
    for side, adapter in (("source", source), ("target", target)):
        if not isinstance(adapter, TransactionalSide):
            raise ConfigError(f"--mode transactional needs a {side} adapter that implements "
                              f"TransactionalSide; {type(adapter).__name__} does not")

def open_window(spec: MappingSpec, source, target,
                tol: Tolerances | None = None) -> TransactionalContext:
    ctx = TransactionalContext(render=source.watermark_literal)
    iso = (source.open_window(), target.open_window())
    for c in spec.objects:
        win = ObjectWindow(isolation=iso)
        s_mark = source.window_marker(c.root_table, c.key_source, c.watermark_source, c.root_where)
        t_mark = target.window_marker(c.object, c.key_target, c.watermark_target, c.target_where)
        ctx.open_markers[c.object] = (s_mark, t_mark)
        if c.watermark_source and c.watermark_target:
            check_comparable(s_mark[1], t_mark[1],
                             f"{c.object}: {c.watermark_source} vs {c.watermark_target}")
            win.hwm_target = in_form_of(t_mark[1], s_mark[1])
            if win.hwm_target is not None:
                newer = _newer_predicate(c.watermark_source, win.hwm_target, ctx.render)
                where = f"({c.root_where}) AND {newer}" if c.root_where else newer
                win.in_flight = source.row_count(c.root_table, where)
        ctx.windows[c.object] = win
        if c.delete_evidence is not None:
            before = (_statements(source), _statements(target))
            ctx.deletes[c.object] = resolve_delete_evidence(c, tol or Tolerances("-"), source, target)
            ctx.evidence_statements["source"] += _statements(source) - before[0]
            ctx.evidence_statements["target"] += _statements(target) - before[1]
    ctx.strength_open = {"source": source.window_strength(), "target": target.window_strength()}
    return ctx

def _statements(adapter) -> int:
    return adapter.statements if isinstance(adapter, StatementCounting) else 0

def _newer_predicate(column: str, hwm: Any, render: Callable[[Any], str] = literal) -> str:
    if isinstance(hwm, dt.datetime):
        return f"{column} >= {render(hwm + dt.timedelta(microseconds=1))}"
    return f"{column} > {render(hwm)}"

def _applied_predicate(column: str, hwm: Any, render: Callable[[Any], str] = literal) -> str:
    if isinstance(hwm, dt.datetime):
        bound = f"{column} < {render(hwm + dt.timedelta(microseconds=1))}"
    else:
        bound = f"{column} <= {render(hwm)}"
    return f"({bound} OR {column} IS NULL)"

def abandon_window(source, target) -> list[Exception]:
    errors: list[Exception] = []
    for name, side in (("source", source), ("target", target)):
        try:
            side.close_window()
        except Exception as exc:  # noqa: BLE001  driver-specific error type
            errors.append(RuntimeError(f"{name} close_window failed: {exc!r}"))
            try:
                side.discard()
            except Exception as exc2:  # noqa: BLE001
                errors.append(RuntimeError(f"{name} connection could not be dropped: {exc2!r}"))
    return errors

def close_window(spec: MappingSpec, tol: Tolerances, ctx: TransactionalContext,
                 source, target) -> TierResult:
    findings, checks = [], 0
    strength = {"source": source.window_strength(), "target": target.window_strength()}
    stats: dict[str, Any] = {"isolation": {"source": source.isolation if hasattr(source, "isolation") else "none",
                                           "target": target.isolation if hasattr(target, "isolation") else "none"},
                             "strength": strength, "markers": {}}
    for side, how in strength.items():
        if ctx.strength_open.get(side) == "snapshot" and how != "snapshot":
            checks += 1
            findings.append(Finding("*", "window_lost",
                                    f"{side} pinned a snapshot at open but no longer holds it at "
                                    "close: a statement rolled the transaction back mid-run, so "
                                    "the tiers did not all read one snapshot; rerun"))
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
    n_strata = max(1, min(tol.pk_set_ranges, n))
    strata = source.key_strata(c.root_table, c.key_source, n_strata, c.root_where)
    if not strata:
        return []
    inner = [(strata[i - 1].hi if i else s.lo, s.hi) for i, s in enumerate(strata)]
    return [(None, strata[0].lo)] + inner + [(strata[-1].hi, None)]

def _kind(value: Any) -> str:
    if isinstance(value, bool):
        return family(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "binary"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, decimal.Decimal) and value.is_finite() and value.as_tuple().exponent >= 0:
        return "integer"
    return family(value)

def _common_kind(values: Iterable[Any]) -> str:
    kinds = {_kind(v) for v in values if v is not None}
    return kinds.pop() if len(kinds) == 1 else "other"

def _whole_columns(adapter, table: str) -> set[str] | None:
    if not isinstance(adapter, WholeNumberColumns):
        return None
    try:
        return adapter.whole_number_columns(table)
    except NotImplementedError:
        return None

def _digest_kind(values: Iterable[Any], s_col: str, t_col: str,
                 s_whole: set[str] | None, t_whole: set[str] | None) -> str:
    kind = _common_kind(values)
    if kind not in ("integer", "number"):
        return kind
    if s_whole is not None and t_whole is not None and s_col in s_whole and t_col in t_whole:
        return "integer"
    return "number"

def _fingerprint_complete(fps: list[tuple], nk: int, watermark: bool) -> bool:
    return all(f[1] is not None and len(f[1]) == nk and (not watermark or f[2] is not None)
               for f in fps)

def tier5_pk_set(spec: MappingSpec, tol: Tolerances, ctx: TransactionalContext,
                 source, target) -> TierResult:
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    obj_uns: dict[str, set] = {}  # categories a live read failed for this object
    for c in spec.objects:
        if c.grading != "full":
            continue  # a target-only output has no key set to match; tier 1 bounds its count
        n = source.row_count(c.root_table, c.root_where)
        checks += 1
        diff = KeyDiff()
        ctx.key_diffs[c.object] = diff
        nk = len(c.key_source)
        has_wm = bool(c.watermark_source and c.watermark_target)
        t_wm_cols = [c.watermark_target] if has_wm else []
        evidence = ctx.delete_evidence(c)
        missing: set[tuple] = set()
        extra: set[tuple] = set()
        in_flight_missing: set[tuple] = set()
        in_flight_updates: set[tuple] = set()
        ahead: set[tuple] = set()
        behind: set[tuple] = set()
        extra_wm: dict[tuple, Any] = {}
        streamed = 0
        if n == 0:
            t_n = target.target_row_count(c.object, c.target_where)
            base = {"ranges": 0, "population": 0, "target_population": t_n}
            if not t_n or evidence.status != "ok":
                stats[c.object] = {**base, "delete_evidence": evidence.as_stats()}
                if t_n:
                    findings.append(Finding(c.object, "pk_extra_on_target",
                                            f"{t_n} target rows, source is empty; delete evidence "
                                            f"{evidence.status}"))
                continue
            ranges, streamed_ranges = [], []
            fingerprint = "not used: source empty, every target key read"
            t_keys = target.keys_in_range(c.object, c.key_target, None, None, c.target_where, t_wm_cols)
            streamed = len(t_keys)
            extra = {tuple(k[:nk]) for k in t_keys}
            if has_wm:
                extra_wm = {tuple(k[:nk]): k[nk] for k in t_keys if k[nk] is not None}
        else:
            ranges, fingerprint, streamed_ranges, streamed = _compare_ranges(
                source, target, c, tol, ctx, n, nk, has_wm, t_wm_cols, missing, extra,
                in_flight_missing, in_flight_updates, ahead, behind, extra_wm)
        in_flight_deletes = {k for k in extra if k in evidence.in_flight}
        extra -= in_flight_deletes
        missing_l, extra_l = sorted(missing, key=repr), sorted(extra, key=repr)
        diff.missing, diff.extra, diff.in_flight_missing = missing_l, extra_l, len(in_flight_missing)
        diff.in_flight_updates = len(in_flight_updates)
        diff.ahead, diff.behind = sorted(ahead, key=repr), sorted(behind, key=repr)
        diff.in_flight_deletes = sorted(in_flight_deletes, key=repr)
        diff.delete_lagging = sorted((k for k in extra if k in evidence.aged), key=repr)
        for k in in_flight_deletes:
            wm = extra_wm.get(k)
            if wm is not None and (diff.in_flight_delete_max_wm is None
                                   or later(wm, diff.in_flight_delete_max_wm)):
                diff.in_flight_delete_max_wm = wm
        stats[c.object] = {"ranges": len(ranges), "population": n, "fingerprint": fingerprint,
                           "mismatched_ranges": len(streamed_ranges), "keys_streamed": streamed,
                           "missing_on_target": len(missing_l), "extra_on_target": len(extra_l),
                           "in_flight_missing": len(in_flight_missing),
                           "in_flight_updates": diff.in_flight_updates,
                           "in_flight_deletes": len(in_flight_deletes),
                           "delete_evidence": evidence.as_stats(),
                           "rows_ahead_on_target": len(diff.ahead),
                           "rows_behind_on_target": len(diff.behind)}
        if n == 0:
            stats[c.object]["target_population"] = streamed
        if missing_l:
            findings.append(Finding(c.object, "pk_missing_on_target",
                                    f"{len(missing_l)} source keys absent on target; first "
                                    f"{min(len(missing_l), MAX_KEYS_IN_FINDING)}: "
                                    f"{missing_l[:MAX_KEYS_IN_FINDING]}"))
        if extra_l:
            if evidence.status == "ok":
                why = (f"not deleted on the source after the target's applied position, so stray "
                       f"writes or deletes older than cdc_lag_max_s ({len(diff.delete_lagging)} "
                       f"of the latter)")
            else:
                why = (f"undrained deletes or stray writes; delete evidence {evidence.status}, so "
                       "nothing tells them apart")
            findings.append(Finding(c.object, "pk_extra_on_target",
                                    f"{len(extra_l)} target keys absent on source ({why}); "
                                    f"first {min(len(extra_l), MAX_KEYS_IN_FINDING)}: "
                                    f"{extra_l[:MAX_KEYS_IN_FINDING]}"))
    return TierResult(5, "pk_set_diff", not findings, checks, findings, stats)

def _compare_ranges(source, target, c: ObjectMapping, tol: Tolerances, ctx: TransactionalContext,
                    n: int, nk: int, has_wm: bool, t_wm_cols: list[str],
                    missing: set, extra: set, in_flight_missing: set, in_flight_updates: set,
                    ahead: set, behind: set, extra_wm: dict) -> tuple[list, str, list[int], int]:
    if not isinstance(source, StratifiedKeys):
        raise ConfigError("tier 5 needs a source adapter with server-side key strata")
    ranges = _ranges(source, c, n, tol)
    bounds = [b for r in ranges for b in r if b is not None and len(b) == nk]
    s_whole, t_whole = None, None
    if not tol.pk_set_stream_every_range:
        s_whole, t_whole = _whole_columns(source, c.root_table), _whole_columns(target, c.object)
    key_kinds = [_digest_kind((b[i] for b in bounds), c.key_source[i], c.key_target[i], s_whole, t_whole)
                 for i in range(nk)]
    wm_kind = None
    if has_wm:
        wm_kind = _digest_kind((m[1] for m in ctx.open_markers[c.object] if len(m) > 1),
                               c.watermark_source, c.watermark_target, s_whole, t_whole)
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
    hwm = ctx.hwm(c)
    streamed = 0
    for i in streamed_ranges:
        lo, hi = ranges[i]
        s_keys = source.keys_in_range(c.root_table, c.key_source, lo, hi, c.root_where, s_wm_cols)
        t_keys = target.keys_in_range(c.object, c.key_target, lo, hi, c.target_where, t_wm_cols)
        streamed += len(s_keys) + len(t_keys)
        s_index = {tuple(k[:nk]): k[nk:] for k in s_keys}
        t_index = {tuple(k[:nk]): k[nk:] for k in t_keys}
        if has_wm:
            extra_wm.update((k, rest[0]) for k, rest in t_index.items()
                            if k not in s_index and rest and rest[0] is not None)
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
    return ranges, fingerprint, streamed_ranges, streamed

def tier6_cdc(spec: MappingSpec, tol: Tolerances, ctx: TransactionalContext,
              source, target) -> TierResult:
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    obj_uns: dict[str, set] = {}  # categories a live read failed for this object
    for c in spec.objects:
        if c.grading != "full":
            continue
        diff = ctx.key_diffs.get(c.object, KeyDiff())
        if c.delete_evidence is not None:
            checks += 1
            findings += _grade_deletes(c, tol, ctx.delete_evidence(c), diff)
        if not (c.watermark_source and c.watermark_target):
            stats[c.object] = {"watermark": None,
                               "note": "no watermark declared: the window markers alone prove stillness",
                               "in_flight_deletes": len(diff.in_flight_deletes)}
            continue
        checks += 1
        s_open, t_open = ctx.open_markers[c.object]
        s_wm, t_wm = s_open[1], t_open[1]
        fam = family(s_wm if s_wm is not None else t_wm)
        unit = c.watermark_unit or ("datetime" if fam == "datetime" else None)
        deleted_max = (t_wm is not None and diff.in_flight_delete_max_wm is not None
                       and not later(t_wm, diff.in_flight_delete_max_wm))
        surviving_note = None
        if deleted_max:
            t_wm, surviving_note = _surviving_target_max(c, target, diff)
        lag = lag_seconds(s_wm, t_wm, unit)
        units = lag_units(s_wm, t_wm)
        in_flight = ctx.in_flight(c)
        stats[c.object] = {"watermark": f"{c.watermark_source}->{c.watermark_target}",
                           "unit": unit, "source_max": s_wm, "target_max": t_open[1], "lag_s": lag,
                           "lag_units": units, "in_flight": in_flight,
                           "in_flight_deletes": len(diff.in_flight_deletes),
                           "target_max_from_in_flight_delete": deleted_max,
                           **({"target_max_excluding_deletes": t_wm} if deleted_max else {}),
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
        if deleted_max and t_wm is None:
            findings.append(Finding(c.object, "cdc_lag_ungraded",
                                    f"max({c.watermark_target}) belongs to a source delete in flight and "
                                    f"{surviving_note}: no applied watermark survives to measure lag "
                                    f"against max({c.watermark_source})={s_wm!r}; let the feed apply "
                                    "the deletes and re-run", s_wm, t_open[1]))
            continue
        if s_wm is None or t_wm is None or (fam == "datetime") != (unit == "datetime"):
            findings.append(Finding(c.object, "cdc_watermark_incomparable",
                                    f"max({c.watermark_source})={s_wm!r} vs max({c.watermark_target})={t_wm!r}"
                                    + (f" under watermark unit {unit!r}" if c.watermark_unit else ""),
                                    s_wm, t_wm))
            continue
        if fam == "number" and unit not in EPOCH_SCALE and unit != "counter":
            findings.append(Finding(c.object, "cdc_lag_ungraded",
                                    f"{c.watermark_source} is a number with no declared watermark unit: "
                                    f"its difference {units} is not a duration, so cdc_lag_max_s cannot "
                                    "grade it; declare watermark.unit epoch_s|epoch_ms|epoch_us (graded "
                                    "in seconds) or counter (graded by cdc_in_flight_max_rows)",
                                    s_wm, t_wm))
        elif unit == "counter":
            if units < 0:
                findings.append(Finding(c.object, "target_ahead_of_source",
                                        f"target watermark is {-units} units past the source: replay "
                                        "or out-of-order apply", s_wm, t_wm))
            elif in_flight > tol.cdc_in_flight_max_rows:
                findings.append(Finding(c.object, "cdc_in_flight_exceeded",
                                        f"{in_flight} source rows unapplied > cdc_in_flight_max_rows="
                                        f"{tol.cdc_in_flight_max_rows} (counter behind by {units} units)",
                                        s_wm, t_wm))
        elif lag < 0:
            findings.append(Finding(c.object, "target_ahead_of_source",
                                    f"target watermark is {-lag:.3f}s newer than the source: replay "
                                    "or out-of-order apply", s_wm, t_wm))
        elif lag > tol.cdc_lag_max_s:
            findings.append(Finding(c.object, "cdc_lag_exceeded",
                                    f"lag {lag:.3f}s > cdc_lag_max_s={tol.cdc_lag_max_s}s "
                                    f"({in_flight} source rows in flight)", s_wm, t_wm))
    return TierResult(6, "cdc_lag_ordering", not findings, checks, findings, stats)

def _surviving_target_max(c: ObjectMapping, target, diff: KeyDiff) -> tuple[Any, str]:
    if not isinstance(target, KeyExcludingAggregates):
        return None, "the target adapter cannot exclude keys from its aggregates"
    keys = diff.in_flight_deletes
    if len(keys) > target.exclusion_capacity(len(c.key_target)):
        return None, f"{len(keys)} deletes in flight exceed one statement's key budget"
    agg = target.table_aggregates_excluding(c.object, [c.watermark_target], [], c.key_target,
                                            keys, c.target_where)
    wm = agg[c.watermark_target]["max"]
    return wm, "every remaining target row is one too" if wm is None else ""

def _grade_deletes(c: ObjectMapping, tol: Tolerances, evidence: DeleteEvidenceResult,
                   diff: KeyDiff) -> list[Finding]:
    out: list[Finding] = []
    if evidence.status == "retention_gap":
        out.append(Finding(c.object, "delete_evidence_retention_gap", evidence.detail,
                           _pos(evidence.horizon[0]), _pos(evidence.applied)))
    elif evidence.status in EVIDENCE_STRICT and evidence.status != "absent":
        out.append(Finding(c.object, "delete_evidence_unusable",
                           f"delete_evidence declared but {evidence.status}: {evidence.detail}; "
                           "target-only keys were graded strictly"))
    if diff.delete_lagging:
        oldest = max(evidence.aged[k].age_s for k in diff.delete_lagging)
        out.append(Finding(c.object, "delete_lag_exceeded",
                           f"{len(diff.delete_lagging)} source deletes still present on the target "
                           f"{oldest:.0f}s after commit > cdc_lag_max_s={tol.cdc_lag_max_s}s; first "
                           f"{min(len(diff.delete_lagging), MAX_KEYS_IN_FINDING)}: "
                           f"{diff.delete_lagging[:MAX_KEYS_IN_FINDING]}"))
    return out

def _column_map(spec: MappingSpec, c: ObjectMapping) -> dict[str, str]:
    m = {s.lower(): t.lower() for s, t in zip(c.key_source, c.key_target)}
    m.update({f.source.lower(): f.target.lower() for f in c.fields})
    if c.watermark_source and c.watermark_target:
        m[c.watermark_source.lower()] = c.watermark_target.lower()
    if c.identity_source and c.identity_target:
        m[c.identity_source.lower()] = c.identity_target.lower()
    return m

def _norm_table(name: str) -> str:
    return name.replace("[", "").replace("]", "").replace('"', "").lower()

class _TableIndex:

    def __init__(self) -> None:
        self._exact: dict[str, str] = {}
        self._placed: set[str] = set()  # objects whose catalog identity is in _exact

    def add(self, spelling: str, obj: str, catalog: bool = False) -> None:
        self._exact[_norm_table(spelling)] = obj
        if catalog:
            self._placed.add(obj)

    def resolve(self, ref: str) -> list[str]:
        name = _norm_table(ref)
        if name in self._exact:
            return [self._exact[name]]
        bare = name.rsplit(".", 1)[-1]
        if "." in name:
            found = {o for s, o in self._exact.items() if s == bare and o not in self._placed}
        else:
            found = {o for s, o in self._exact.items() if s.rsplit(".", 1)[-1] == bare}
        return sorted(found)

def _map_cols(cols: tuple, colmap: dict[str, str]) -> tuple:
    return tuple(colmap.get(col.lower(), col.lower()) for col in cols)

def _lower_fk(fk: tuple) -> tuple:
    cols, ref, rcols = fk
    return (tuple(x.lower() for x in cols), _norm_table(ref), tuple(x.lower() for x in rcols))

def _lower_facts(f: SchemaFacts) -> SchemaFacts:
    return SchemaFacts(
        table=_norm_table(f.table),
        primary_key=tuple(x.lower() for x in f.primary_key),
        unique={tuple(x.lower() for x in u) for u in f.unique},
        unique_nulls_equal={tuple(x.lower() for x in u) for u in f.unique_nulls_equal},
        primary_key_informational=tuple(k.lower() for k in f.primary_key_informational),
        foreign_keys={_lower_fk(fk) for fk in f.foreign_keys},
        foreign_keys_informational={_lower_fk(fk) for fk in f.foreign_keys_informational},
        foreign_key_actions={_lower_fk(fk): a for fk, a in f.foreign_key_actions.items()},
        not_null={x.lower() for x in f.not_null},
        indexes={tuple(x.lower() for x in i) for i in f.indexes},
        check_count=f.check_count, checks=set(f.checks),
        identity_columns={x.lower() for x in f.identity_columns},
        partial={tuple(x.lower() for x in p) for p in f.partial},
        expression_unique={normalize_sql_text(x) for x in f.expression_unique},
        expression_indexes={normalize_sql_text(x) for x in f.expression_indexes},
        triggers={n: (t, tuple(e), g) for n, (t, e, g) in f.triggers.items()},
        grants={g.lower(): frozenset(p.lower() for p in ps) for g, ps in f.grants.items()},
        unsupported=f.unsupported,
        declares_not_null=f.declares_not_null,
        table_type=f.table_type)

_EXPR_TOKEN = re.compile(
    r"(?P<string>'(?:[^']|'')*')"
    r"|(?P<quoted>\"(?:[^\"]|\"\")*\")"
    r"|(?P<cast>::\s*[A-Za-z_][A-Za-z0-9_]*"
    r"(?:\s+(?:varying|precision|with(?:out)?\s+time\s+zone))?(?:\s*\([^)]*\))?)"
    r"|(?P<func>[A-Za-z_][A-Za-z0-9_]*)(?=\s*\()"
    r"|(?P<ident>[A-Za-z_][A-Za-z0-9_]*)")

def _map_expression(text: str, colmap: dict[str, str]) -> str:
    def swap(m: re.Match) -> str:
        if m.group("ident"):
            return colmap.get(m.group("ident").lower(), m.group("ident"))
        if m.group("quoted"):
            name = m.group("quoted")[1:-1].replace('""', '"')
            mapped = colmap.get(name.lower())
            return '"' + mapped.replace('"', '""') + '"' if mapped else m.group("quoted")
        return m.group(0)
    return _EXPR_TOKEN.sub(swap, text)

_CHECK_TOKEN = re.compile(
    r"(?P<ws>\s+)"
    r"|(?P<string>'(?:[^']|'')*')"
    r"|(?P<quoted>\"(?:[^\"]|\"\")*\")"
    r"|(?P<cast>::\s*[A-Za-z_][A-Za-z0-9_]*"
    r"(?:\s+(?:varying|precision|with(?:out)?\s+time\s+zone))?"
    r"(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:\s*\[\s*\])*)"
    r"|(?P<number>\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\.\d+)"
    r"|(?P<ident>[A-Za-z_][A-Za-z0-9_$#]*)"
    r"|(?P<op><>|!=|>=|<=|!~~|~~|\|\||[=<>+\-*/%(),\[\]])"
    r"|(?P<other>\S)")
_NUMERIC_CAST = re.compile(r"::\s*(?:integer|int|int2|int4|int8|bigint|smallint|numeric|decimal|real|"
                           r"double precision|float\d*)\b", re.IGNORECASE)
_BRACKET_IDENT = re.compile(r"\[([^\]]+)\]")
_ARRAY_BRACKET = re.compile(r"\barray\s*\[", re.IGNORECASE)
_FUNC_ALIASES = {"len": "length", "char_length": "length", "character_length": "length",
                 "ceil": "ceiling", "isnull": "coalesce", "getdate": "current_timestamp",
                 "getutcdate": "current_timestamp", "now": "current_timestamp",
                 "sysdatetime": "current_timestamp"}
_PORTABLE_FUNCS = {"length", "upper", "lower", "trim", "ltrim", "rtrim", "abs", "coalesce", "nullif",
                   "round", "floor", "ceiling", "substring", "current_timestamp", "current_date"}
_KEYWORDS = {"and", "or", "not", "in", "is", "null", "like", "between", "any", "all", "some", "array",
             "true", "false", "case", "when", "then", "else", "end", "exists", "select", "collate",
             "similar", "to", "escape", "current_timestamp", "current_date"}
_NON_PORTABLE_KEYWORDS = {"case", "exists", "select", "collate", "like", "similar", "escape"}
_COMPARISONS = {"=", "<>", "<", ">", "<=", ">=", "in", "like", "is", "between"}
_LITERAL_KINDS = {"string", "number", "ident"}  # ident covers true/false/null

def _check_tokens(text: str, colmap: dict[str, str]) -> tuple[list[tuple[str, str]], bool]:
    text = re.sub(r"^\s*check\s*", "", text, flags=re.IGNORECASE)
    if not _ARRAY_BRACKET.search(text):  # SQL Server quotes identifiers in brackets
        text = _BRACKET_IDENT.sub(lambda m: '"' + m.group(1).replace('"', '""') + '"', text)
    tokens: list[tuple[str, str]] = []
    portable = True
    for m in _CHECK_TOKEN.finditer(text):
        kind = m.lastgroup
        value = m.group(0)
        if kind == "ws":
            continue
        if kind == "cast":
            if tokens and tokens[-1][0] == "string" and _NUMERIC_CAST.match(value):
                try:
                    num = decimal.Decimal(tokens[-1][1][1:-1].replace("''", "'"))
                    tokens[-1] = ("number", format(num.normalize(), "f") if num.is_finite() else tokens[-1][1])
                except decimal.InvalidOperation:
                    pass
            continue
        if kind == "quoted":
            name = value[1:-1].replace('""', '"').lower()
            tokens.append(("ident", colmap.get(name, name)))
        elif kind == "ident":
            name = value.lower()
            if name in _KEYWORDS:
                if name in _NON_PORTABLE_KEYWORDS:
                    portable = False
                tokens.append(("kw", name))
            else:
                tokens.append(("ident", colmap.get(name, name)))
        elif kind == "number":
            try:
                num = decimal.Decimal(value)
                value = format(num.normalize(), "f") if num.is_finite() else value
            except decimal.InvalidOperation:
                pass
            tokens.append(("number", value))
        elif kind == "op":
            if value in ("~~", "!~~"):  # Postgres spells LIKE as an operator in pg_get_constraintdef
                portable = False
                if value == "!~~":
                    tokens.append(("kw", "not"))
                tokens.append(("kw", "like"))
            else:
                tokens.append(("op", "<>" if value == "!=" else value))
        elif kind == "string":
            tokens.append(("string", value))
        else:
            portable = False
            tokens.append(("other", value))
    for i, (kind, value) in enumerate(tokens):
        if kind == "ident" and i + 1 < len(tokens) and tokens[i + 1] == ("op", "("):
            name = _FUNC_ALIASES.get(value, value)
            if name not in _PORTABLE_FUNCS:
                portable = False
            tokens[i] = ("func", name)
    return tokens, portable

class _Unparsable(Exception):
    pass

class _Parser:

    def __init__(self, tokens: list[tuple[str, str]]) -> None:
        self.toks = tokens
        self.i = 0

    def peek(self, k: int = 0):
        j = self.i + k
        return self.toks[j] if j < len(self.toks) else None

    def take(self, expect=None):
        tok = self.peek()
        if tok is None or (expect is not None and tok != expect):
            raise _Unparsable(f"expected {expect}, got {tok}")
        self.i += 1
        return tok

    def at(self, *values: str) -> bool:
        tok = self.peek()
        return tok is not None and tok[0] in ("kw", "op") and tok[1] in values

    def parse(self):
        node = self.expr()
        if self.peek() is not None:
            raise _Unparsable(f"trailing {self.peek()}")
        return node

    def expr(self):
        return self.chain("or", self.and_expr)

    def and_expr(self):
        return self.chain("and", self.not_expr)

    def chain(self, kw: str, sub):
        items = [sub()]
        while self.at(kw):
            self.take()
            items.append(sub())
        return items[0] if len(items) == 1 else (kw, items)

    def not_expr(self):
        if self.at("not"):
            self.take()
            return ("not", self.not_expr())
        return self.pred()

    def pred(self):
        left = self.arith()
        if self.at("is"):
            self.take()
            neg = self.at("not") and self.take()
            what = self.take()
            if what[0] != "kw" or what[1] not in ("null", "true", "false"):
                raise _Unparsable("IS needs NULL/TRUE/FALSE")
            return ("is", left, ("not " if neg else "") + what[1])
        neg = False
        if self.at("not") and self.peek(1) is not None and self.peek(1)[1] in ("in", "like", "between"):
            self.take()
            neg = True
        if self.at("in"):
            self.take()
            self.take(("op", "("))
            lits = self.literal_list(("op", ")"))
            return ("in", left, lits, neg)
        if self.at("between"):
            self.take()
            lo = self.arith()
            self.take(("kw", "and"))
            hi = self.arith()
            node = ("and", [("cmp", ">=", left, lo), ("cmp", "<=", left, hi)])
            return ("not", node) if neg else node
        if self.at("like"):
            self.take()
            pattern = self.arith()
            if self.at("escape"):
                self.take()
                pattern = ("bin", "escape", pattern, self.arith())
            node = ("cmp", "like", left, pattern)
            return ("not", node) if neg else node
        if self.at("=", "<>", "<", ">", "<=", ">="):
            op = self.take()[1]
            if self.at("any", "all", "some"):
                quant = self.take()[1]
                self.take(("op", "("))
                lits = self.array_literal()
                self.take(("op", ")"))
                if op == "=" and quant in ("any", "some"):
                    return ("in", left, lits, False)
                if op == "<>" and quant == "all":
                    return ("in", left, lits, True)
                raise _Unparsable("quantified comparison")
            right = self.arith()
            if _is_literal(left) and not _is_literal(right):  # `0 <= x` reads as `x >= 0`
                op = {"<": ">", ">": "<", "<=": ">=", ">=": "<="}.get(op, op)
                left, right = right, left
            return ("cmp", op, left, right)
        return left

    def array_literal(self):
        depth = 0
        while self.at("("):
            self.take()
            depth += 1
        self.take(("kw", "array"))
        self.take(("op", "["))
        lits = self.literal_list(("op", "]"))
        for _ in range(depth):
            self.take(("op", ")"))
        return lits

    def literal_list(self, close):
        lits = []
        while True:
            lits.append(self.arith())
            if self.at(","):
                self.take()
                continue
            self.take(close)
            return lits

    def arith(self):
        node = self.term()
        while self.at("+", "-", "||"):
            op = self.take()[1]
            node = ("bin", op, node, self.term())
        return node

    def term(self):
        node = self.factor()
        while self.at("*", "/", "%"):
            op = self.take()[1]
            node = ("bin", op, node, self.factor())
        return node

    def factor(self):
        if self.at("-"):
            self.take()
            inner = self.factor()
            if inner[0] == "atom" and inner[1] == "number":
                return ("atom", "number", "-" + inner[2])
            return ("neg", inner)
        if self.at("+"):
            self.take()
            return self.factor()
        return self.primary()

    def primary(self):
        tok = self.take()
        kind, value = tok
        if kind == "op" and value == "(":
            node = self.expr()
            self.take(("op", ")"))
            return node
        if kind == "func":
            self.take(("op", "("))
            args = []
            if not self.at(")"):
                args = self.literal_list(("op", ")"))
            else:
                self.take()
            return ("call", value, args)
        if kind in ("number", "string", "ident"):
            return ("atom", kind, value)
        if kind == "kw" and value in ("null", "true", "false", "current_timestamp", "current_date"):
            return ("atom", "kw", value)
        raise _Unparsable(f"unexpected {tok}")

def _is_literal(node) -> bool:
    return node[0] == "atom" and node[1] in ("number", "string", "kw")

def _fold(node):
    kind = node[0]
    if kind in ("or", "and"):
        items = []
        for x in node[1]:
            x = _fold(x)
            items.extend(x[1] if x[0] == kind else [x])
        want_op, negated = ("=", False) if kind == "or" else ("<>", True)
        by_col: dict = {}
        rest = []
        for x in items:
            if x[0] == "cmp" and x[1] == want_op and x[2][0] == "atom" and x[2][1] == "ident" \
                    and _is_literal(x[3]):
                by_col.setdefault(x[2], []).append(x[3])
            else:
                rest.append(x)
        for col, lits in by_col.items():
            if len(lits) == 1:
                rest.append(("cmp", want_op, col, lits[0]))
            else:
                rest.append(("in", col, lits, negated))
        rest = [_fold(x) if x[0] == "in" else x for x in rest]
        rest.sort(key=_render)
        return rest[0] if len(rest) == 1 else (kind, rest)
    if kind == "in":
        lits = sorted((_fold(x) for x in node[2]), key=_render)
        return ("in", _fold(node[1]), lits, node[3])
    if kind == "not":
        return ("not", _fold(node[1]))
    if kind == "cmp":
        return ("cmp", node[1], _fold(node[2]), _fold(node[3]))
    if kind == "is":
        return ("is", _fold(node[1]), node[2])
    if kind == "bin":
        return ("bin", node[1], _fold(node[2]), _fold(node[3]))
    if kind == "neg":
        return ("neg", _fold(node[1]))
    if kind == "call":
        return ("call", node[1], [_fold(a) for a in node[2]])
    return node

_PREC = {"or": 1, "and": 2, "not": 3, "cmp": 4, "is": 4, "in": 4, "+": 5, "-": 5, "||": 5,
         "*": 6, "/": 6, "%": 6, "neg": 7, "escape": 8}

def _prec(node) -> int:
    kind = node[0]
    if kind == "bin":
        return _PREC[node[1]]
    return _PREC.get(kind, 9)

def _render(node, parent: int = 0, right: bool = False) -> str:
    kind = node[0]
    if kind == "atom":
        return node[2]
    if kind == "call":
        return f"{node[1]}({', '.join(_render(a) for a in node[2])})"
    if kind in ("or", "and"):
        text = f" {kind} ".join(_render(x, _PREC[kind]) for x in node[1])
    elif kind == "not":
        text = "not " + _render(node[1], _PREC["not"])
    elif kind == "cmp":
        text = f"{_render(node[2], 4)} {node[1]} {_render(node[3], 4, True)}"
    elif kind == "is":
        text = f"{_render(node[1], 4)} is {node[2]}"
    elif kind == "in":
        text = (f"{_render(node[1], 4)} {'not in' if node[3] else 'in'} "
                f"({', '.join(_render(x) for x in node[2])})")
    elif kind == "bin":
        p = _PREC[node[1]]
        text = f"{_render(node[2], p)} {node[1]} {_render(node[3], p, True)}"
    elif kind == "neg":
        text = "-" + _render(node[1], _PREC["neg"])
    else:
        raise _Unparsable(kind)
    own = _prec(node)
    if own < parent or (right and own == parent and kind == "bin"):
        return f"({text})"
    return text

def _check_key_text(definition: str, colmap: dict[str, str]) -> tuple[str, bool]:
    tokens, portable = _check_tokens(definition, colmap)
    try:
        return _render(_fold(_Parser(tokens).parse())), portable
    except (_Unparsable, IndexError):
        return " ".join(v for _, v in tokens), False

def _check_keys(defs: set[str], colmap: dict[str, str]) -> dict[str, tuple[str, bool]]:
    out: dict[str, tuple[str, bool]] = {}
    for d in sorted(defs):
        key, portable = _check_key_text(d, colmap)
        out[key] = (d, portable and out.get(key, (d, True))[1])
    return out

def _category_content(f: SchemaFacts, cat: str) -> int:
    if cat == "triggers":
        return len(f.triggers)
    if cat == "grants":
        return len(f.grants)
    if cat == "indexes":
        return len(f.indexes) + len(f.partial) + len(f.expression_indexes)
    if cat == "sequences_identity":
        return len(f.identity_columns)
    return (bool(f.primary_key) + bool(f.primary_key_informational) + len(f.unique)
            + len(f.foreign_keys) + len(f.foreign_keys_informational) + len(f.checks)
            + len(f.not_null) + len(f.expression_unique))

def _covered(leading: tuple, facts: SchemaFacts) -> bool:
    candidates = [facts.primary_key] + list(facts.unique) + list(facts.indexes)
    return any(cand[:len(leading)] == leading for cand in candidates)

def schema_parity(tier: int, name: str, spec: MappingSpec, tol: Tolerances, source, target,
                  strict: bool, catalog_only: bool = False,
                  own_principals: frozenset[str] = frozenset()) -> TierResult:
    findings, checks = [], 0
    stats: dict[str, Any] = {}
    obj_uns: dict[str, set] = {}  # categories a live read failed for this object
    hole_kinds: set[str] = set()

    def hole(note: str, kind: str = "read", key: str = "unverified") -> None:
        hole_kinds.add(kind)
        stats.setdefault(key, []).append(note)
    platform = getattr(target, "target_kind", None) == "databricks"
    key_uniqueness = getattr(target, "key_uniqueness", None) if platform else None

    def probe(obj: str, key: tuple[str, ...], where: str | None) -> tuple[bool, str]:
        if key_uniqueness is None:
            return False, ""
        try:
            rows, distinct = key_uniqueness(obj, list(key), where)
        except Exception as exc:  # noqa: BLE001 - a failed probe is a blocking finding, not a crash
            return False, f"failed ({exc})"
        return rows == distinct, f"{rows} rows, {distinct} distinct keys"
    tables, targets = _TableIndex(), _TableIndex()
    facts: dict[str, tuple[SchemaFacts, SchemaFacts]] = {}
    for c in spec.objects:
        obj = c.object.lower()
        tables.add(c.root_table, obj)
        targets.add(c.object, obj)
        try:
            s_raw = source.schema_facts(c.root_table)
            t_raw = target.schema_facts(c.object)
        except (NotImplementedError, DictionaryError) as exc:
            hole(f"{c.object}: {exc}",
                 "privilege" if isinstance(exc, DictionaryError) and exc.kind == "privilege" else "read",
                 "unverified" if strict else "dictionary_unavailable")
            continue
        if s_raw.table:
            tables.add(s_raw.table, obj, catalog=True)
        if t_raw.table:
            targets.add(t_raw.table, obj, catalog=True)
        facts[c.object] = (s_raw, t_raw)

    def tightened(finding: Finding) -> None:
        if tol.accept_target_only_constraints:
            stats.setdefault("accepted_target_only_constraints", []).append(
                f"{finding.object}: {finding.check}: {finding.detail}")
        else:
            findings.append(finding)

    for c in spec.objects:
        if c.object not in facts:
            continue
        colmap = _column_map(spec, c)
        mapped_targets = set(colmap.values())
        s_raw, t_raw = facts[c.object]
        checks += 1
        s, t, t_lower = _lower_facts(s_raw), t_raw, _lower_facts(t_raw)
        uns = s_raw.unsupported | t_raw.unsupported | obj_uns.get(c.object, set())
        if uns:
            s, t, t_lower = (mask_unsupported(f_, uns) for f_ in (s, t, t_lower))
        pk = _map_cols(s.primary_key, colmap)
        pk_info = _map_cols(s.primary_key_informational, colmap)
        kind = (t_raw.table_type or "").upper()
        pipeline = platform and kind in UC_PIPELINE_KINDS
        by_design = c.grading == "by_design_unverified"
        if by_design:
            own_key = t_lower.primary_key or tuple(k.lower() for k in c.key_target)
            if not t_lower.primary_key:
                unique, note = probe(c.object, own_key, c.target_where)
                if not unique:
                    findings.append(Finding(c.object, "target_key_not_unique",
                                            f"by_design_unverified target declares no enforced primary key and "
                                            f"its key {own_key} is not proven unique"
                                            + (f"; uniqueness probe: {note}" if note else "; no uniqueness probe")))
        elif pk != t_lower.primary_key:
            if pk and not t_lower.primary_key and t_lower.primary_key_informational == pk:
                unique, note = probe(c.object, pk, c.target_where)
                if unique:
                    findings.append(Finding(c.object, "primary_key_informational_only",
                                            "target PK is informational (Unity Catalog does not "
                                            f"enforce it); uniqueness probe passed: {note}",
                                            severity="info"))
                else:
                    findings.append(Finding(c.object, "primary_key_informational_only",
                                            "target PK is informational, not enforced: duplicate keys "
                                            "would be accepted" + (f"; uniqueness probe: {note}" if note else "")))
            elif pipeline and pk and not t_lower.primary_key and not t_lower.primary_key_informational:
                unique, note = probe(c.object, pk, c.target_where)
                if unique:
                    findings.append(Finding(c.object, "primary_key_not_applicable",
                                            f"not_applicable({kind}): a pipeline output declares no "
                                            f"primary key; uniqueness probe passed: {note}",
                                            severity="info"))
                else:
                    findings.append(Finding(c.object, "primary_key_mismatch",
                                            f"source {s.primary_key} -> expected {pk}, target "
                                            f"{kind} declares none; uniqueness probe: {note}",
                                            s.primary_key, t.primary_key))
            elif not pk and pk_info and pk_info == t_lower.primary_key:
                pass  # the target enforces what the source only declared informationally
            else:
                findings.append(Finding(c.object, "primary_key_mismatch",
                                        f"source {s.primary_key} -> expected {pk}, target "
                                        f"{t.primary_key}", s.primary_key, t.primary_key))
        if (not by_design and pk_info and not s.primary_key and pk_info != t_lower.primary_key
                and pk_info != t_lower.primary_key_informational):
            unique, note = (probe(c.object, pk_info, c.target_where)
                            if pipeline and not t_lower.primary_key and not t_lower.primary_key_informational
                            else (False, ""))
            if unique:
                findings.append(Finding(c.object, "primary_key_not_applicable",
                                        f"not_applicable({kind}): a pipeline output declares no "
                                        f"primary key; uniqueness probe passed: {note}",
                                        severity="info"))
            else:
                findings.append(Finding(c.object, "primary_key_informational_missing",
                                        f"source PK {s.primary_key_informational} is informational and "
                                        "absent on the target: duplicate keys would be accepted"
                                        + (f"; uniqueness probe: {note}" if note else "")))
        expected_unique = {frozenset(_map_cols(u, colmap)) for u in s.unique}
        target_unique = {frozenset(u) for u in t_lower.unique}
        for u in sorted(s.unique):
            want = _map_cols(u, colmap)
            if frozenset(want) not in target_unique:
                findings.append(Finding(c.object, "unique_missing",
                                        f"source unique {u} has no target unique {want}"))
                continue
            have = next(t for t in sorted(t_lower.unique) if frozenset(t) == frozenset(want))
            if have != want:
                stats.setdefault("unique_reordered", []).append(
                    f"{c.object}: source unique {u} is enforced on the target as {have}, not "
                    f"{want}; same constraint, different access path")
            if all(col in s.not_null for col in u):
                continue
            s_eq, t_eq = u in s.unique_nulls_equal, have in t_lower.unique_nulls_equal
            if s_eq and not t_eq:
                findings.append(Finding(c.object, "unique_nulls_equal_missing",
                                        f"source unique {u} allows one NULL key; target unique {have} "
                                        "treats NULLs as distinct, so duplicate NULL keys the source "
                                        "rejects would be accepted", "nulls equal", "nulls distinct"))
            elif t_eq and not s_eq:
                tightened(Finding(c.object, "unique_nulls_equal_extra",
                                  f"target unique {have} allows one NULL key; source unique {u} "
                                  "treats NULLs as distinct, so a second legacy-valid NULL key would "
                                  "be rejected", "nulls distinct", "nulls equal"))
        for u in sorted(t_lower.unique):
            if frozenset(u) in expected_unique or frozenset(u) == frozenset(t_lower.primary_key):
                continue
            if set(u) <= mapped_targets:
                tightened(Finding(c.object, "unique_extra",
                                  f"target unique {u} has no source counterpart: legacy-valid "
                                  "duplicates would be rejected"))
            else:
                stats.setdefault("target_only_columns_unverified", []).append(
                    f"{c.object}: unique {u} covers a column outside the mapping")
        t_fks: dict[tuple, tuple] = {}
        for cols, ref, rcols in t_lower.foreign_keys:
            found = targets.resolve(ref)
            t_fks[(cols, found[0] if len(found) == 1 else ref, rcols)] = (cols, ref, rcols)
        t_fks_info = set()
        for cols, ref, rcols in t_lower.foreign_keys_informational:
            found = targets.resolve(ref)
            t_fks_info.add((cols, found[0] if len(found) == 1 else ref, rcols))
        expected_fks: set[tuple] = set()
        for cols, ref, rcols in sorted(s.foreign_keys_informational):
            found = tables.resolve(ref)
            if not found:
                stats.setdefault("foreign_keys_out_of_scope", []).append(f"{c.object}: {cols} -> {ref}")
                continue
            if len(found) == 1:
                ref_map = next((_column_map(spec, o) for o in spec.objects
                                if o.object.lower() == found[0]), {})
                want = (_map_cols(cols, colmap), found[0], _map_cols(rcols, ref_map))
                if want in t_fks:
                    expected_fks.add(want)
                elif want not in t_fks_info:
                    findings.append(Finding(c.object, "foreign_key_informational_missing",
                                            f"source FK {cols} -> {ref}{rcols} is informational and "
                                            "absent on the target"))
        for cols, ref, rcols in sorted(s.foreign_keys):
            found = tables.resolve(ref)
            if len(found) > 1:
                hole(f"{c.object}: FK {cols} -> {ref} could reference any of {found}; qualify the "
                     "reference (root_table schema) or confirm its target counterpart by hand")
                for cand in found:
                    ref_map = next((_column_map(spec, o) for o in spec.objects if o.object.lower() == cand), {})
                    expected_fks.add((_map_cols(cols, colmap), cand, _map_cols(rcols, ref_map)))
                continue
            if not found:
                stats.setdefault("foreign_keys_out_of_scope", []).append(f"{c.object}: {cols} -> {ref}")
                continue
            ref_obj = found[0]
            ref_map = next((_column_map(spec, o) for o in spec.objects if o.object.lower() == ref_obj), {})
            want = (_map_cols(cols, colmap), ref_obj, _map_cols(rcols, ref_map))
            expected_fks.add(want)
            if want not in t_fks:
                if want in t_fks_info:
                    findings.append(Finding(c.object, "foreign_key_informational_only",
                                            f"target FK {want[0]} -> {want[1]}{want[2]} is "
                                            "informational, not enforced: orphans would be accepted"))
                else:
                    findings.append(Finding(c.object, "foreign_key_missing",
                                            f"source FK {cols} -> {ref}{rcols} expected on target "
                                            f"as {want}"))
                continue
            s_act = s.foreign_key_actions.get((cols, ref, rcols))
            t_act = t_lower.foreign_key_actions.get(t_fks[want])
            if s_act and t_act and s_act != t_act:
                findings.append(Finding(c.object, "foreign_key_action_mismatch",
                                        f"FK {want}: source acts (update, delete) = {s_act}, target "
                                        f"{t_act}: parent changes propagate differently", s_act, t_act))
        mapped_ref_cols = {o.object.lower(): set(_column_map(spec, o).values()) for o in spec.objects}
        for cols, ref, rcols in sorted(set(t_fks) - expected_fks):
            if ref not in mapped_ref_cols:
                stats.setdefault("foreign_keys_out_of_scope", []).append(
                    f"{c.object}: target FK {cols} -> {ref}")
            elif set(cols) <= mapped_targets and set(rcols) <= mapped_ref_cols[ref]:
                tightened(Finding(c.object, "foreign_key_extra",
                                  f"target FK {cols} -> {ref}{rcols} has no source counterpart: "
                                  "legacy-valid orphans would be rejected"))
            else:
                stats.setdefault("target_only_columns_unverified", []).append(
                    f"{c.object}: FK {cols} -> {ref}{rcols} covers a column outside the mapping")
        expected_not_null = {colmap[col] for col in s.not_null if col in colmap}
        if t_lower.declares_not_null:
            for col in sorted(s.not_null):
                mapped = colmap.get(col, col)
                if col in colmap and mapped not in t_lower.not_null:
                    findings.append(Finding(c.object, "not_null_missing",
                                            f"source NOT NULL {col} -> target {mapped} is nullable"))
            for col in sorted((t_lower.not_null & mapped_targets) - expected_not_null - set(t_lower.primary_key)):
                tightened(Finding(c.object, "not_null_extra",
                                  f"target {col} is NOT NULL but its source column is nullable: "
                                  "legacy-valid NULLs would be rejected"))
        else:
            stats.setdefault("not_null_unverified", []).append(
                f"{c.object}: target relation cannot declare NOT NULL; "
                f"{len(expected_not_null)} source NOT NULL columns unverified")
        for idx in sorted(s.indexes):
            if not _covered(_map_cols(idx, colmap), t_lower):
                findings.append(Finding(c.object, "index_missing",
                                        f"no target index leads with {_map_cols(idx, colmap)} (source {idx})"))
        if len(s.checks) == s.check_count and len(t.checks) == t.check_count:
            s_keys, t_keys = _check_keys(s.checks, colmap), _check_keys(t.checks, {})
            s_only, t_only = sorted(set(s_keys) - set(t_keys)), sorted(set(t_keys) - set(s_keys))
            if s_only and not t_only:
                for key in s_only:
                    findings.append(Finding(c.object, "check_constraint_missing",
                                            f"source CHECK {s_keys[key][0]} (canonical: {key}) has no "
                                            "target counterpart: the target admits rows the source rejects"))
            elif s_only:
                detail = (f"{len(s_only)} source CHECK(s) match no target CHECK and {len(t_only)} target "
                          f"CHECK(s) match no source CHECK; source: "
                          + "; ".join(f"{s_keys[k][0]} (canonical: {k})" for k in s_only)
                          + "; target: " + "; ".join(f"{t_keys[k][0]} (canonical: {k})" for k in t_only)
                          + ("" if all(s_keys[k][1] for k in s_only) and all(t_keys[k][1] for k in t_only)
                             else "; a predicate uses a dialect-specific construct"))
                if tol.accept_unverified_check_constraints:
                    stats.setdefault("accepted_unverified_check_constraints", []).append(
                        f"{c.object}: {detail}")
                else:
                    findings.append(Finding(c.object, "check_constraint_unverified",
                                            detail + "; compare them by hand and record "
                                            "accept_unverified_check_constraints"))
            else:
                for key in t_only:
                    tightened(Finding(c.object, "check_constraint_extra",
                                      f"target CHECK {t_keys[key][0]} (canonical: {key}) has no source "
                                      "counterpart: it rejects writes the source accepts"))
        elif t.check_count < s.check_count:
            findings.append(Finding(c.object, "check_constraint_count_lower",
                                    f"source {s.check_count} CHECK constraints, target {t.check_count}",
                                    s.check_count, t.check_count))
        elif t.check_count > s.check_count:
            tightened(Finding(c.object, "check_constraint_count_higher",
                              f"source {s.check_count} CHECK constraints, target {t.check_count}: "
                              "the extra checks reject writes the source accepts",
                              s.check_count, t.check_count))
        else:
            stats.setdefault("check_predicates_unverified", []).append(
                f"{c.object}: {s.check_count} CHECK constraints on each side, but a catalog reader "
                "delivered counts only; the predicates were not compared")
        for idx in sorted(s.partial):
            stats.setdefault("partial_indexes_unverified", []).append(
                f"{c.object}: source filtered index {idx} carries a predicate the harness cannot "
                f"translate; confirm its target counterpart by hand")
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
        if pipeline and "sequences_identity" not in uns and (c.identity_source or s.identity_columns):
            checks += 1
            seq_note = {"not_applicable": kind}
            findings.append(Finding(c.object, "identity_not_applicable",
                                    f"not_applicable({kind}): source identity "
                                    f"{c.identity_source or sorted(s.identity_columns)} has no "
                                    "counterpart on a pipeline output", severity="info"))
        elif "sequences_identity" not in uns and c.identity_source and c.identity_target:
            checks += 1
            try:
                t_state = target.identity_state(c.object, c.identity_target)
                s_state = source.identity_state(c.root_table, c.identity_source)
            except (NotImplementedError, DictionaryError) as exc:
                hole(f"{c.object} identity: {exc}",
                     "privilege" if isinstance(exc, DictionaryError) and exc.kind == "privilege" else "read")
                obj_uns[c.object] = obj_uns.get(c.object, set()) | {"sequences_identity"}
                uns = uns | {"sequences_identity"}
                s, t_lower = (mask_unsupported(f_, uns) for f_ in (s, t_lower))
            else:
                s_min, s_max = ((None, None) if catalog_only else
                                _key_bounds(source, c.root_table, c.identity_source, c.root_where))
                seq_note = {"source_next": None if s_state is None else s_state.next,
                            "source_max": s_max,
                            "target_next": None if t_state is None else t_state.next}
                if catalog_only:
                    seq_note["source_bounds"] = "unread"
                collides = False
                s_next = (s_state.next if s_state is not None and t_state is not None
                          and s_state.descending == t_state.descending else None)
                if t_state is None:
                    findings.append(Finding(c.object, "sequence_missing",
                                            f"target column {c.identity_target} owns no sequence/identity"))
                elif t_state.descending:
                    seq_note.update(source_min=s_min, increment=t_state.increment)
                    if s_min is not None and t_state.next >= int(s_min):
                        collides = True
                        findings.append(Finding(c.object, "sequence_behind_source",
                                                f"target next value {t_state.next} >= source min "
                                                f"{c.identity_source}={s_min} on a descending identity "
                                                f"(increment {t_state.increment}): new inserts would collide",
                                                s_min, t_state.next))
                    elif s_next is not None and t_state.next > s_next:
                        collides = True
                        findings.append(Finding(c.object, "sequence_behind_source",
                                                f"target next value {t_state.next} > source identity next "
                                                f"{s_next} on a descending identity (source min "
                                                f"{c.identity_source}={s_min}): identifiers the source "
                                                "already issued would be reissued", s_next, t_state.next))
                elif s_max is not None and t_state.next <= int(s_max):
                    collides = True
                    findings.append(Finding(c.object, "sequence_behind_source",
                                            f"target next value {t_state.next} <= source max "
                                            f"{c.identity_source}={s_max}: new inserts would collide",
                                            s_max, t_state.next))
                elif s_next is not None and t_state.next < s_next:
                    collides = True
                    findings.append(Finding(c.object, "sequence_behind_source",
                                            f"target next value {t_state.next} < source identity next "
                                            f"{s_next} (source max {c.identity_source}={s_max}): identifiers "
                                            "the source already issued would be reissued",
                                            s_next, t_state.next))
                if t_state is not None and s_state is not None and t_state.increment != s_state.increment:
                    step = (f"source identity {c.identity_source} steps by {s_state.increment}, "
                            f"target {c.identity_target} by {t_state.increment}: ")
                    if t_state.descending != s_state.descending:
                        findings.append(Finding(c.object, "sequence_direction_mismatch",
                                                step + "the two sides hand out keys from opposite ends "
                                                "and will meet", s_state.increment, t_state.increment))
                    elif not collides:
                        findings.append(Finding(c.object, "sequence_increment_mismatch",
                                                step + "target-generated keys follow a different "
                                                "sequence", s_state.increment, t_state.increment))
        if "triggers" not in uns:
            tr_findings, tr_extra = compare_triggers(c.object, s, t_lower)
            findings += tr_findings + tr_extra
        if "grants" not in uns:
            findings += compare_grants(c.object, s, t_lower, spec.principal_map,
                                       own_principals if platform else frozenset())
        if "sequences_identity" not in uns and not pipeline:
            id_findings, id_extra = compare_identity_columns(c.object, s, t_lower, colmap)
            findings += id_findings + id_extra
        for cat in CATEGORIES:
            n = _category_content(s_raw, cat)
            if cat in (t_raw.unsupported | obj_uns.get(c.object, set())):
                if cat == "indexes":
                    if n:
                        stats.setdefault("index_unsupported", []).append(
                            f"{c.object}: {n} source indexes cannot be checked: the target catalog "
                            "has no indexes dictionary")
                else:
                    hole(f"{c.object}: target dictionary cannot expose {cat}: source has {n}",
                         "privilege" if cat in t_raw.privilege_denied else "read")
            m = _category_content(t_raw, cat)
            if cat in (s_raw.unsupported | obj_uns.get(c.object, set())):
                if cat == "indexes":
                    if m:
                        stats.setdefault("index_unsupported", []).append(
                            f"{c.object}: {m} target indexes cannot be checked: the source "
                            "catalog has no indexes dictionary")
                else:
                    hole(f"{c.object}: source dictionary cannot expose {cat}: target has {m}",
                         "privilege" if cat in s_raw.privilege_denied else "read")
        stats[c.object] = {"source": _facts_dict(s_raw), "target": _facts_dict(t_raw), "identity": seq_note}
    checks_map = structural_checks(list(facts.values()))
    for cat in {c for cats in obj_uns.values() for c in cats}:
        checks_map[cat] = "unsupported"
    if not strict and any(c.object not in facts for c in spec.objects):
        checks_map = {cat: "unsupported" for cat in CATEGORIES}
    stats["structural_checks"] = checks_map
    stats["structural_diff"] = diff_by_object(findings)
    stats["platform_inherent"] = [{"object": f.object, "check": f.check, "detail": f.detail}
                                  for f in findings if f.severity == "info"]
    stats["dictionary"] = {"source": getattr(source, "dictionary_label", "live"),
                           "target": getattr(target, "dictionary_label", "live")}
    for label in stats["dictionary"].values():
        if label != "live":
            hole(f"structure read from fixture dictionary {label.removeprefix('fixture:')}, "
                 "not the live catalog", "fixture")
    if hole_kinds:
        stats["hole_kinds"] = sorted(hole_kinds)
    return TierResult(tier, name, not blocking(findings), checks, findings, stats)

def tier7_schema_parity(spec: MappingSpec, tol: Tolerances, source, target,
                        own_principals: frozenset[str] = frozenset()) -> TierResult:
    return schema_parity(7, "schema_parity", spec, tol, source, target, strict=True,
                         own_principals=own_principals)

def _key_bounds(source, table: str, column: str, where: str | None) -> tuple[Any, Any]:
    agg = source.field_aggregates(table, column, where)
    return agg["min"], agg["max"]

def _facts_dict(f: SchemaFacts) -> dict:
    return {"table_type": f.table_type,
            "primary_key": list(f.primary_key),
            "primary_key_informational": list(f.primary_key_informational),
            "unique": sorted(map(list, f.unique)),
            "unique_nulls_equal": sorted(map(list, f.unique_nulls_equal)),
            "foreign_keys": sorted([list(c), r, list(rc), *f.foreign_key_actions.get((c, r, rc), ())]
                                   for c, r, rc in f.foreign_keys),
            "foreign_keys_informational": sorted([list(c), r, list(rc)]
                                                for c, r, rc in f.foreign_keys_informational),
            "not_null": sorted(f.not_null), "indexes": sorted(map(list, f.indexes)),
            "check_count": f.check_count, "checks": sorted(f.checks),
            "identity_columns": sorted(f.identity_columns),
            "partial": sorted(map(list, f.partial)),
            "expression_unique": sorted(f.expression_unique),
            "expression_indexes": sorted(f.expression_indexes),
            "triggers": {n: [tm, list(ev), g] for n, (tm, ev, g) in f.triggers.items()},
            "grants": {g: sorted(p) for g, p in f.grants.items()},
            "unsupported": sorted(f.unsupported),
            "privilege_denied": sorted(f.privilege_denied)}
