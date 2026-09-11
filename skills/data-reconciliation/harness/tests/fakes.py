"""In-memory fake adapters implementing the adapter protocols, for fixture tests."""

from __future__ import annotations

import datetime as dt
import decimal
import json
import operator
from collections import Counter
from collections.abc import Iterable
from decimal import Decimal
from itertools import accumulate, pairwise
from typing import Any

from recon.adapters import DeleteEvent, IdentityState, SchemaFacts, Stratum
from recon.canon import MISSING
from recon.fingerprint import DIGESTIBLE_KINDS, moments
from recon.paths import get_path
from recon.watermarks import instant, literal

_NUMERIC = (int, float, decimal.Decimal)
_OPS = {" >= ": operator.ge, " > ": operator.gt, " <= ": operator.le, " < ": operator.lt}


def _agg_of(vals: list) -> dict[str, Any]:
    nn = [v for v in vals if v is not None]
    nums = [v for v in nn if isinstance(v, _NUMERIC) and not isinstance(v, bool)]
    return {"count": len(vals),
            "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
            "min": min(nn) if nn else None, "max": max(nn) if nn else None,
            "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}


def _matches(row: dict, where: str | None) -> bool:
    """Evaluates the predicates the tiers issue: a JSON scope, `col = literal`, or the
    transactional window's `[(scope) AND ]col OP literal[ OR col IS NULL]`."""
    if not where:
        return True
    if where.lstrip().startswith("{"):
        return all(row.get(k) == v for k, v in json.loads(where).items())
    scope, _, pred = where.rpartition(" AND ") if " AND " in where else ("", "", where)
    bound, _, null_ok = pred.strip("() ").partition(" OR ")
    op = next((o for o in _OPS if o in bound), None)
    if op:
        col, _, lit = bound.partition(op)
        value = row.get(col.strip())
        if value is None:
            return bool(null_ok) and _matches(row, scope.strip("() "))
        lit = lit.strip().strip("'")
        if isinstance(value, dt.datetime):
            edge = dt.datetime.fromisoformat(lit)
        elif isinstance(value, bytes):   # `0x...`, the engine's binary literal
            edge = int(lit, 16)
        else:
            edge = type(value)(lit)
        return _OPS[op](instant(value), edge) and _matches(row, scope.strip("() "))
    key, sep, lit = (s.strip() for s in where.partition("="))
    return not sep or str(row[key] if key in row else get_path(row, key)) == lit.strip("'\"")


def _bounds(lo, hi):
    return tuple(b if b is None or isinstance(b, tuple) else (b,) for b in (lo, hi))


def _within(k, lo, hi):
    return (lo is None or k >= lo) and (hi is None or k <= hi)


class _TransactionalMixin:
    """The adapter surface both fakes share, including TransactionalSide. `tables` maps table ->
    rows, `schema` table -> SchemaFacts, `sequences` (table, column) -> next value or a (next,
    increment) pair. `calls` records the entry points the tiers used, `statements` what a SQL side
    would have issued, `fail_on[method] = exc` raises on that call, `on_open` runs inside open_window
    and `pin` is "fake_snapshot" or "none" (a refused snapshot; a callable `change_token` then feeds
    the markers)."""

    max_params = 2000  # the SQL adapters' bound-parameter budget: one per key component

    def __init__(self, tables: dict[str, list[dict]], schema=None, sequences=None):
        self.tables, self.schema, self.sequences = tables, schema or {}, sequences or {}
        self.calls, self.statements, self.rows_fetched, self.fail_on = Counter(), 0, 0, {}
        self.last_fetch_keyed = self.last_table_aggregates_numeric = self.last_excluded_keys = None
        self.isolation, self.window_open, self.on_open = "none", False, None
        self.pin, self.change_token = "fake_snapshot", None

    def _count(self, method: str, statements: int = 1) -> None:
        self.calls[method] += 1
        self.statements += statements
        if (exc := self.fail_on.get(method)) is not None:
            raise exc

    def _rows(self, table, where):
        return [r for r in self.tables[table] if _matches(r, where)]

    @staticmethod
    def _key(r: dict, key_cols: list[str]) -> tuple:
        return tuple(r[k] if k in r else get_path(r, k) for k in key_cols)

    def _sorted(self, table, key_cols, where, natural: bool = False) -> list[dict]:
        # repr order tolerates mixed-type keys; strata need the engine's natural order (NULL-free)
        rows = self._rows(table, where)
        if natural:
            return sorted((r for r in rows if not any(v is None for v in self._key(r, key_cols))),
                          key=lambda r: self._key(r, key_cols))
        return sorted(rows, key=lambda r: tuple(repr(v) for v in self._key(r, key_cols)))

    @staticmethod
    def _aggs(rows, columns, numeric) -> dict[str, dict[str, Any]]:
        out = {col: _agg_of([None if (v := get_path(d, col)) is MISSING else v for d in rows])
               for col in columns}
        for col in set(columns) - set(numeric):
            out[col]["sum"] = None
        return out

    def whole_number_columns(self, table: str) -> set[str]:
        """Fake catalog: whole when every value is an int or a scale-0 Decimal."""
        self._count("whole_number_columns")
        rows = self.tables[table]
        return {c for c in {c for r in rows for c in r}
                if all(r.get(c) is None or (isinstance(r[c], int) and not isinstance(r[c], bool))
                       or (isinstance(r[c], Decimal) and r[c].as_tuple().exponent >= 0) for r in rows)}

    def null_key_count(self, table, key_cols, where=None) -> int:
        self._count("null_key_count")
        return sum(1 for r in self._rows(table, where) if any(v is None for v in self._key(r, key_cols)))

    def sum_probe(self, table, column, where=None) -> Any:
        self._count("sum_probe")
        return self._aggs(self._rows(table, where), [column], [column])[column]["sum"]

    def table_aggregates(self, table, columns, numeric, where=None) -> dict[str, dict[str, Any]]:
        self._count("table_aggregates")
        self.last_table_aggregates_numeric = list(numeric)
        return self._aggs(self._rows(table, where), columns, numeric)

    def exclusion_capacity(self, key_width: int) -> int:
        return max(1, self.max_params // max(1, key_width))

    def table_aggregates_excluding(self, table, columns, numeric, key_cols, exclude_keys,
                                   where=None) -> dict[str, dict[str, Any]]:
        if len(exclude_keys) > self.exclusion_capacity(len(key_cols)):
            raise ValueError(f"{len(exclude_keys)} keys x {len(key_cols)} columns exceed the "
                             f"{self.max_params}-parameter budget of one statement")
        self._count("table_aggregates_excluding")
        self.last_excluded_keys = list(exclude_keys)
        excluded = {tuple(k) for k in exclude_keys}
        rows = [d for d in self._rows(table, where) if self._key(d, key_cols) not in excluded]
        return self._aggs(rows, columns, numeric)

    def fetch_keyed(self, table, key_cols, columns, where=None, keys=None) -> Iterable[dict]:
        key_cols = [key_cols] if isinstance(key_cols, str) else key_cols
        self._count("fetch_keyed")
        self.last_fetch_keyed = {"table": table, "key_cols": key_cols, "columns": columns,
                                 "where": where, "keys": keys}
        wanted = ({tuple(k) if isinstance(k, (tuple, list)) else (k,) for k in keys}
                  if keys is not None else None)
        for r in self._sorted(table, key_cols, where):
            if wanted is None or self._key(r, key_cols) in wanted:
                self.rows_fetched += 1
                yield r

    # ---- TransactionalSide

    def open_window(self) -> str:
        self.calls["open_window"] += 1
        self.window_open, self.isolation = True, self.pin
        if self.on_open:
            self.on_open()
        return self.isolation

    def close_window(self) -> None:
        self._count("close_window", 0)
        self.window_open = False

    def discard(self) -> None:
        self._count("discard", 0)
        self.window_open = False

    def window_strength(self) -> str:
        return ("snapshot" if self.isolation == "fake_snapshot"
                else "change_token" if self.change_token else "markers")

    watermark_literal = staticmethod(literal)

    def window_marker(self, table, key_cols, watermark, where=None) -> tuple:
        self._count("window_marker")
        rows = self._rows(table, where)
        marker = (len(rows), *((self._max(rows, watermark),) if watermark else
                               (max((self._key(r, key_cols)[i] for r in rows), default=None)
                                for i in range(len(key_cols)))))
        if self.isolation != "fake_snapshot" and self.change_token:
            marker += (self.change_token(table),)
        return marker

    def range_fingerprints(self, table, key_cols, key_kinds, watermark, wm_kind, ranges,
                           where=None) -> list[tuple]:
        self._count("range_fingerprints")
        digest_keys = all(kind in DIGESTIBLE_KINDS for kind in key_kinds)
        digest_wm = bool(watermark and wm_kind in DIGESTIBLE_KINDS)
        norm = [_bounds(lo, hi) for lo, hi in ranges]
        # like the SQL CASE, the first range whose bounds hold wins the row
        hits: list[list] = [[] for _ in norm]
        for r in self._rows(table, where):
            k, wm = self._key(r, key_cols), r.get(watermark) if watermark else None
            if (i := next((i for i, (lo, hi) in enumerate(norm) if _within(k, lo, hi)), None)) is not None:
                hits[i].append((k, wm))
        return [(len(hit),
                 tuple(moments(k[j] for k, _ in hit) for j in range(len(key_cols))) if digest_keys else None,
                 (*moments(wm for _, wm in hit), sum(wm is None for _, wm in hit)) if digest_wm else None)
                for hit in hits]

    def keys_in_range(self, table, key_cols, lo, hi, where=None, extra_cols=None) -> list[tuple]:
        self._count("keys_in_range")
        lo, hi = _bounds(lo, hi)
        out = [self._key(r, key_cols) + tuple(r.get(c) for c in (extra_cols or []))
               for r in self._rows(table, where) if _within(self._key(r, key_cols), lo, hi)]
        self.rows_fetched += len(out)
        return sorted(out, key=lambda k: k[:len(key_cols)])

    @staticmethod
    def _max(rows, watermark):
        return max((r[watermark] for r in rows if r.get(watermark) is not None), default=None)

    def max_watermark(self, table, watermark, where=None):
        self._count("max_watermark")
        return self._max(self._rows(table, where), watermark)

    def schema_facts(self, table) -> SchemaFacts:
        self._count("schema_facts", 4)
        if table not in self.schema:
            raise NotImplementedError(f"fake has no schema facts for {table}")
        return self.schema[table]

    def identity_state(self, table, column) -> IdentityState | None:
        self._count("identity_state")
        if (table, column) not in self.sequences:
            raise NotImplementedError(f"fake has no identity state for {table}.{column}")
        state = self.sequences[(table, column)]
        return (IdentityState(*state) if isinstance(state, tuple)
                else state if state is None or isinstance(state, IdentityState) else IdentityState(state, 1))


class FakeSource(_TransactionalMixin):
    """SourceAdapter plus the optional BatchAggregates / StratifiedKeys / StatementCounting."""

    def row_count(self, table: str, where: str | None = None) -> int:
        self._count("row_count")
        return len(self._rows(table, where))

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        self._count("field_aggregates")
        return self._aggs(self._rows(table, where), [column], [column])[column]

    def iter_keys(self, table, key_cols, where=None):
        self._count("iter_keys")
        for r in self._sorted(table, key_cols, where):
            self.rows_fetched += 1
            yield self._key(r, key_cols)

    def key_strata(self, table, key_cols, n_strata, where=None) -> list[Stratum]:
        self._count("key_strata")
        rows = self._sorted(table, key_cols, where, natural=True)
        if not rows:
            return []
        size, rem = divmod(len(rows), n_strata := max(1, min(n_strata, len(rows))))
        edges = list(accumulate([0, *(size + (b < rem) for b in range(n_strata))]))
        self.rows_fetched += n_strata
        return [Stratum(b + 1, self._key(rows[s], key_cols), self._key(rows[e - 1], key_cols), e - s)
                for b, (s, e) in enumerate(pairwise(edges))]

    def sample_keys(self, table, key_cols, lo, hi, row_numbers, where=None) -> list[tuple]:
        self._count("sample_keys")
        lo, hi = _bounds(lo, hi)
        in_range = [self._key(r, key_cols) for r in self._sorted(table, key_cols, where, natural=True)]
        keys = [k for i, k in enumerate((k for k in in_range if lo <= k <= hi), 1) if i in set(row_numbers)]
        self.rows_fetched += len(keys)
        return keys

    def duplicate_key_count(self, table, key_cols, where=None) -> int:
        self._count("duplicate_key_count")
        return sum(n > 1 for n in Counter(self._key(r, key_cols) for r in self._rows(table, where)).values())


class FakeCdcSource(FakeSource):
    """A source with a change stream (`DeleteEvidence`): `tombstones` maps a capture to its retained
    DeleteEvents; the horizon is (oldest, newest) retained position unless `horizons` pins it
    ((None, None) retains nothing). `images` maps capture -> {key: before-image} for scope predicates."""

    kind = "sqlserver_cdc"

    def __init__(self, tables, tombstones: dict[str, list[DeleteEvent]], horizons=None, images=None, **kw):
        super().__init__(tables, **kw)
        self.tombstones, self.horizons, self.images = tombstones, horizons or {}, images or {}

    def delete_evidence_kind(self) -> str:
        return self.kind

    def evidence_horizon(self, capture):
        self._count("evidence_horizon")
        newest = max((e.position for e in self.tombstones.get(capture, [])), default=None)
        zero = bytes(len(newest)) if isinstance(newest, bytes) else 0  # retention from zero
        return self.horizons.get(capture, (None, None) if newest is None else (zero, newest))

    def deletes_since(self, capture, key_cols, after, upto, where=None):
        self._count("deletes_since")
        self.last_deletes_since = {"capture": capture, "key_cols": key_cols, "after": after, "upto": upto, "where": where}
        images = self.images.get(capture, {})
        # an event of another position type is returned as is: the engine, not the fake, rejects it
        out = [e for e in self.tombstones.get(capture, [])
               if (type(e.position) is not type(after) or after < e.position <= upto)
               and _matches(images.get(e.key, dict(zip(key_cols, e.key, strict=True))), where)]
        self.rows_fetched += len(out)
        return out


class FakeTypedSource(FakeSource):
    """A catalog that says which columns are numeric (`ColumnTypes`), typed from the rows."""

    def numeric_columns(self, table: str) -> set[str]:
        self._count("numeric_columns")
        return {col for r in self.tables[table] for col, v in r.items()
                if isinstance(v, _NUMERIC) and not isinstance(v, bool)}


class FakeCheckpointTarget:
    """Mixin: the `AppliedPosition` protocol over `positions`, {(table, column, where): position}."""
    calls: Counter
    statements: int
    positions: dict

    def applied_position(self, table, column, where):
        self.calls["applied_position"] += 1
        self.statements += 1
        return self.positions[(table, column, where)]


class FakeTarget(_TransactionalMixin):
    objects = property(lambda self: self.tables)

    def target_row_count(self, object: str, where=None) -> int:
        self._count("target_row_count")
        return len(self._rows(object, where))

    def nested_count(self, object: str, array_path: str, where=None) -> int:
        self._count("nested_count")
        return sum(len(get_path(d, array_path) or []) for d in self._rows(object, where))

    def field_aggregates(self, object: str, field_path: str, where=None) -> dict[str, Any]:
        self.calls["field_aggregates"] += 1  # rides on the batched read: no statement of its own
        return self._aggs(self._rows(object, where), [field_path], [field_path])[field_path]
