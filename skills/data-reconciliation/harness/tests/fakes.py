"""In-memory fake adapters implementing the adapter protocols, for fixture tests."""

from __future__ import annotations

import datetime as dt
import decimal
import json
import operator
from collections import Counter
from collections.abc import Iterable
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from recon.adapters import DIGEST_MODULUS, IdentityState, SchemaFacts, Stratum
from recon.canon import MISSING
from recon.paths import get_path
from recon.watermarks import literal

_EPOCH = dt.datetime(1970, 1, 1)  # noqa: DTZ001  fixtures use naive datetimes throughout
_NUMERIC = (int, float, decimal.Decimal)
_OPS = {" >= ": operator.ge, " > ": operator.gt, " <= ": operator.le, " < ": operator.lt}


def _instant(value):
    """Naive-UTC form of a datetime, mirroring recon.watermarks.instant for literal predicates."""
    if isinstance(value, dt.datetime) and value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value


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
        edge = dt.datetime.fromisoformat(lit) if isinstance(value, dt.datetime) else type(value)(lit)
        return _OPS[op](_instant(value), edge) and _matches(row, scope.strip("() "))
    left, sep, right = where.partition("=")
    if not sep:
        return True
    key = left.strip()
    value = row[key] if key in row else get_path(row, key)
    return str(value) == right.strip().strip("'\"")


class _TransactionalMixin:
    """TransactionalSide for the in-memory fakes; `schema` maps table -> SchemaFacts and
    `sequences` maps (table, column) -> next value (or a (next, increment) pair for a stepped
    or descending identity), both optional."""
    tables: dict
    calls: Counter
    statements: int
    rows_fetched: int

    def _init_transactional(self, schema=None, sequences=None):
        self.schema = schema or {}
        self.sequences = sequences or {}
        self.isolation = "none"
        self.window_open = False
        self.on_open = None  # runs inside open_window, for tests that move a side mid-run
        # "fake_snapshot" (default) or "none" for an engine that refused a snapshot; with "none"
        # a callable `change_token` feeds the markers
        self.pin = "fake_snapshot"
        self.change_token = None
        self.fail_on: dict = {}  # {"method": Exception} failure injection

    def _maybe_fail(self, method):
        exc = self.fail_on.get(method)
        if exc is not None:
            raise exc

    def _tx_rows(self, table, where):
        return [r for r in self._all_rows(table) if _matches(r, where)]

    def _key(self, r: dict, key_cols: list[str]) -> tuple:
        return tuple(r[k] if k in r else get_path(r, k) for k in key_cols)

    def open_window(self) -> str:
        self.calls["open_window"] += 1
        self.window_open = True
        self.isolation = self.pin
        if self.on_open:
            self.on_open()
        return self.isolation

    def close_window(self) -> None:
        self.calls["close_window"] += 1
        self._maybe_fail("close_window")
        self.window_open = False

    def window_strength(self) -> str:
        if self.isolation == "fake_snapshot":
            return "snapshot"
        return "change_token" if self.change_token else "markers"

    def watermark_literal(self, value) -> str:
        return literal(value)

    def window_marker(self, table, key_cols, watermark, where=None) -> tuple:
        self.calls["window_marker"] += 1
        self.statements += 1
        self._maybe_fail("window_marker")
        rows = self._tx_rows(table, where)
        if watermark:
            vals = [r.get(watermark) for r in rows if r.get(watermark) is not None]
            marker = (len(rows), max(vals) if vals else None)
        else:
            keys = [self._key(r, key_cols) for r in rows]
            marker = (len(rows),) + tuple(max(k[i] for k in keys) if keys else None
                                         for i in range(len(key_cols)))
        if self.isolation != "fake_snapshot" and self.change_token:
            marker += (self.change_token(table),)
        return marker

    @staticmethod
    def _digest(value):
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, _NUMERIC):
            return Decimal(str(value))
        if isinstance(value, dt.datetime):
            return Decimal(int((_instant(value) - _EPOCH).total_seconds() * 1_000_000))
        if isinstance(value, dt.date):
            return Decimal((value - dt.date(1970, 1, 1)).days * 86_400_000_000)
        return None

    @classmethod
    def _moments(cls, values) -> tuple[Decimal, int]:
        digests = [cls._digest(v) or Decimal(0) for v in values]
        squares = 0
        for d in digests:
            residue = abs(int(d.to_integral_value(rounding=ROUND_HALF_UP))) % DIGEST_MODULUS
            squares += residue * residue
        return sum(digests, Decimal(0)), squares

    @staticmethod
    def _bounds(lo, hi):
        return (lo if lo is None or isinstance(lo, tuple) else (lo,),
                hi if hi is None or isinstance(hi, tuple) else (hi,))

    @staticmethod
    def _within(k, lo, hi):
        return (lo is None or k >= lo) and (hi is None or k <= hi)

    def range_fingerprints(self, table, key_cols, key_kinds, watermark, wm_kind, ranges,
                           where=None) -> list[tuple]:
        self.calls["range_fingerprints"] += 1
        self.statements += 1
        self._maybe_fail("range_fingerprints")
        digest_keys = all(kind in ("integer", "datetime") for kind in key_kinds)
        digest_wm = bool(watermark and wm_kind in ("integer", "datetime"))
        norm = [self._bounds(lo, hi) for lo, hi in ranges]
        # like the SQL CASE, the first range whose bounds hold wins the row
        hits: list[list] = [[] for _ in norm]
        for r in self._tx_rows(table, where):
            k, wm = self._key(r, key_cols), r.get(watermark) if watermark else None
            i = next((i for i, (lo, hi) in enumerate(norm) if self._within(k, lo, hi)), None)
            if i is not None:
                hits[i].append((k, wm))
        out = []
        for hit in hits:
            keys = (tuple(self._moments(k[j] for k, _ in hit) for j in range(len(key_cols)))
                    if digest_keys else None)
            wm = ((*self._moments(wm for _, wm in hit), sum(1 for _, wm in hit if wm is None))
                  if digest_wm else None)
            out.append((len(hit), keys, wm))
        return out

    def keys_in_range(self, table, key_cols, lo, hi, where=None, extra_cols=None) -> list[tuple]:
        self.calls["keys_in_range"] += 1
        self.statements += 1
        self._maybe_fail("keys_in_range")
        lo, hi = self._bounds(lo, hi)
        out = [self._key(r, key_cols) + tuple(r.get(c) for c in (extra_cols or []))
               for r in self._tx_rows(table, where) if self._within(self._key(r, key_cols), lo, hi)]
        self.rows_fetched += len(out)
        return sorted(out, key=lambda k: k[:len(key_cols)])

    def max_watermark(self, table, watermark, where=None):
        self.calls["max_watermark"] += 1
        self.statements += 1
        vals = [r.get(watermark) for r in self._tx_rows(table, where) if r.get(watermark) is not None]
        return max(vals) if vals else None

    def schema_facts(self, table) -> SchemaFacts:
        self.calls["schema_facts"] += 1
        self.statements += 4
        if table not in self.schema:
            raise NotImplementedError(f"fake has no schema facts for {table}")
        return self.schema[table]

    def identity_state(self, table, column) -> IdentityState | None:
        self.calls["identity_state"] += 1
        self.statements += 1
        if (table, column) not in self.sequences:
            raise NotImplementedError(f"fake has no identity state for {table}.{column}")
        state = self.sequences[(table, column)]
        if state is None or isinstance(state, IdentityState):
            return state
        return IdentityState(*state) if isinstance(state, tuple) else IdentityState(state, 1)


class FakeSource(_TransactionalMixin):
    """Implements SourceAdapter plus the optional BatchAggregates / StratifiedKeys /
    StatementCounting / TransactionalSide protocols; `calls` records which entry points the
    tiers used."""

    def __init__(self, tables: dict[str, list[dict]], schema=None, sequences=None):
        self.tables = tables
        self.last_fetch_keyed = None
        self.calls: Counter[str] = Counter()
        self.statements = 0
        self.rows_fetched = 0
        self._init_transactional(schema, sequences)

    def _all_rows(self, table):
        return self.tables[table]

    def _sorted(self, table: str, key_cols: list[str], where: str | None,
                natural: bool = False) -> list[dict]:
        # repr order tolerates mixed-type keys (the historical fake behaviour); strata need the
        # engine's natural key order so lo <= key <= hi range checks hold.
        sort_key = ((lambda r: self._key(r, key_cols)) if natural
                    else (lambda r: tuple(repr(v) for v in self._key(r, key_cols))))
        return sorted(self._tx_rows(table, where), key=sort_key)

    def row_count(self, table: str, where: str | None = None) -> int:
        self.calls["row_count"] += 1
        self.statements += 1
        return len(self._tx_rows(table, where))

    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]:
        self.calls["table_aggregates"] += 1
        self.statements += 1
        self.last_table_aggregates_numeric = list(numeric)
        out = {col: self._aggregates(table, col, where) for col in columns}
        for col in set(columns) - set(numeric):
            out[col]["sum"] = None
        return out

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        self.calls["field_aggregates"] += 1
        self.statements += 1
        return self._aggregates(table, column, where)

    def sum_probe(self, table: str, column: str, where: str | None = None) -> Any:
        self.calls["sum_probe"] += 1
        self.statements += 1
        return self._aggregates(table, column, where)["sum"]

    def _aggregates(self, table: str, column: str, where: str | None) -> dict[str, Any]:
        return _agg_of([(r[column] if column in r else get_path(r, column))
                        for r in self._tx_rows(table, where)])

    def fetch_keyed(self, table, key_cols, columns, where=None, keys=None) -> Iterable[dict]:
        self.calls["fetch_keyed"] += 1
        self.statements += 1
        self.last_fetch_keyed = {"table": table, "key_cols": key_cols, "columns": columns,
                                 "where": where, "keys": keys}
        wanted = {tuple(k) for k in keys} if keys is not None else None
        for r in self._sorted(table, key_cols, where):
            if wanted is None or self._key(r, key_cols) in wanted:
                self.rows_fetched += 1
                yield r

    def iter_keys(self, table, key_cols, where=None):
        self.calls["iter_keys"] += 1
        self.statements += 1
        for r in self._sorted(table, key_cols, where):
            self.rows_fetched += 1
            yield self._key(r, key_cols)

    def key_strata(self, table, key_cols, n_strata, where=None) -> list[Stratum]:
        self.calls["key_strata"] += 1
        self.statements += 1
        rows = self._sorted(table, key_cols, where, natural=True)
        if not rows:
            return []
        n_strata = max(1, min(n_strata, len(rows)))
        size, rem = divmod(len(rows), n_strata)
        out, start = [], 0
        for b in range(n_strata):
            end = start + size + (1 if b < rem else 0)
            chunk = rows[start:end]
            out.append(Stratum(b + 1, self._key(chunk[0], key_cols),
                               self._key(chunk[-1], key_cols), len(chunk)))
            start = end
        self.rows_fetched += len(out)
        return out

    def sample_keys(self, table, key_cols, lo, hi, row_numbers, where=None) -> list[tuple]:
        self.calls["sample_keys"] += 1
        self.statements += 1
        lo, hi = self._bounds(lo, hi)
        in_range = [r for r in self._sorted(table, key_cols, where, natural=True)
                    if lo <= self._key(r, key_cols) <= hi]
        wanted = set(row_numbers)
        keys = [self._key(r, key_cols) for i, r in enumerate(in_range, 1) if i in wanted]
        self.rows_fetched += len(keys)
        return keys

    def duplicate_key_count(self, table, key_cols, where=None) -> int:
        self.calls["duplicate_key_count"] += 1
        self.statements += 1
        counts = Counter(self._key(r, key_cols) for r in self._tx_rows(table, where))
        return sum(1 for n in counts.values() if n > 1)


class FakeTypedSource(FakeSource):
    """A source whose catalog says which columns are numeric (the `ColumnTypes` protocol), as
    the SQL Server and Postgres adapters do; typed from the fixture rows' Python values."""

    def numeric_columns(self, table: str) -> set[str]:
        self.calls["numeric_columns"] += 1
        self.statements += 1
        return {col for r in self.tables[table] for col, v in r.items()
                if isinstance(v, _NUMERIC) and not isinstance(v, bool)}


class FakeTarget(_TransactionalMixin):
    def __init__(self, objects: dict[str, list[dict]], scopes: dict[str, callable] | None = None,
                 schema=None, sequences=None):
        self.objects = objects
        self.scopes = scopes or {}
        self.last_fetch_keyed = None
        self.calls: Counter[str] = Counter()
        self.statements = 0
        self.rows_fetched = 0
        self._init_transactional(schema, sequences)

    def _all_rows(self, table):
        return self.objects[table]

    def _rows(self, object, where):
        scope = self.scopes.get(where) if where else None
        return [d for d in self.objects[object]
                if (scope(d) if scope else _matches(d, where))]

    @staticmethod
    def _aggs(rows, columns, numeric):
        out = {col: _agg_of([None if (v := get_path(d, col)) is MISSING else v for d in rows])
               for col in columns}
        for col in set(columns) - set(numeric):
            out[col]["sum"] = None
        return out

    def target_row_count(self, object: str, where=None) -> int:
        self.calls["target_row_count"] += 1
        self.statements += 1
        self._maybe_fail("target_row_count")
        return len(self._rows(object, where))

    def nested_count(self, object: str, array_path: str, where=None) -> int:
        self.calls["nested_count"] += 1
        self.statements += 1
        return sum(len(get_path(d, array_path) or []) for d in self._rows(object, where))

    def table_aggregates(self, object: str, columns: list[str], numeric: list[str],
                         where=None) -> dict[str, dict[str, Any]]:
        # Delegates per field so subclass overrides of field_aggregates still shape the values;
        # counted as one statement, as a SQL target would issue.
        self.calls["table_aggregates"] += 1
        self.statements += 1
        self.last_table_aggregates_numeric = list(numeric)
        probes = self.calls["field_aggregates"]
        out = {}
        for col in columns:
            agg = (self.field_aggregates(object, col, where) if where is not None
                   else self.field_aggregates(object, col))
            if col not in numeric:
                agg["sum"] = None
            out[col] = agg
        self.calls["field_aggregates"] = probes  # only direct probes count as statements
        return out

    def table_aggregates_excluding(self, object: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where=None) -> dict[str, dict[str, Any]]:
        self.calls["table_aggregates_excluding"] += 1
        self.statements += 1
        self.last_excluded_keys = list(exclude_keys)
        excluded = {tuple(k) for k in exclude_keys}
        rows = [d for d in self._rows(object, where)
                if tuple(get_path(d, k) for k in key_cols) not in excluded]
        return self._aggs(rows, columns, numeric)

    def sum_probe(self, object: str, field_path: str, where=None) -> Any:
        self.calls["sum_probe"] += 1
        self.statements += 1
        return self._aggs(self._rows(object, where), [field_path], [field_path])[field_path]["sum"]

    def field_aggregates(self, object: str, field_path: str, where=None) -> dict[str, Any]:
        self.calls["field_aggregates"] += 1
        return self._aggs(self._rows(object, where), [field_path], [field_path])[field_path]

    def fetch_keyed(self, object, key_fields, fields, where=None, keys=None) -> Iterable[dict]:
        if isinstance(key_fields, str):
            key_fields = [key_fields]
        self.calls["fetch_keyed"] += 1
        self.statements += 1
        self.last_fetch_keyed = {"object": object, "key_fields": key_fields,
                                 "fields": fields, "where": where, "keys": keys}
        wanted = ({k if isinstance(k, tuple) else (k,) for k in keys}
                  if keys is not None else None)
        for d in sorted(self._rows(object, where),
                        key=lambda d: repr(tuple(get_path(d, k) for k in key_fields))):
            if wanted is None or tuple(get_path(d, k) for k in key_fields) in wanted:
                self.rows_fetched += 1
                yield d
