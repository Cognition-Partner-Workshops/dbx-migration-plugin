"""In-memory fake adapters implementing the adapter protocols, for fixture tests."""

from __future__ import annotations

import datetime
import datetime as dt
import json
import decimal
from decimal import ROUND_HALF_UP, Decimal
from collections import Counter
from typing import Any, Iterable

from recon.adapters import DIGEST_MODULUS, SchemaFacts, Stratum
from recon.paths import get_path
from recon.canon import MISSING
from recon.watermarks import literal


_EPOCH = dt.datetime(1970, 1, 1)  # noqa: DTZ001  fixtures use naive datetimes throughout


def _instant(value):
    """Naive-UTC form of a datetime, mirroring recon.watermarks.instant for literal predicates."""
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value


def _agg_of(vals: list) -> dict[str, Any]:
    nn = [v for v in vals if v is not None]
    nums = [v for v in nn if isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool)]
    return {"count": len(vals),
            "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
            "min": min(nn) if nn else None, "max": max(nn) if nn else None,
            "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}


def _matches(row: dict, where: str | None) -> bool:
    if not where:
        return True
    if where.lstrip().startswith("{"):
        parsed = json.loads(where)
        return all(row.get(k) == v for k, v in parsed.items())
    if " IS NULL)" in where:
        # the applied predicate: [(scope) AND ](col < lit OR col IS NULL) | (col <= lit OR ...)
        scope, _, applied = where.rpartition(" AND (") if " AND (" in where else (None, None, where)
        bound, _, _ = applied.strip("() ").partition(" OR ")
        op = " <= " if " <= " in bound else " < "
        col, _, lit = bound.partition(op)
        value = row.get(col.strip())
        if value is None:
            applied_ok = True
        else:
            lit = lit.strip().strip("'")
            bound_v = (datetime.datetime.fromisoformat(lit) if isinstance(value, datetime.datetime)
                       else type(value)(lit))
            applied_ok = _instant(value) <= bound_v if op == " <= " else _instant(value) < bound_v
        return applied_ok and _matches(row, scope.strip("() ") if scope else None)
    if " > " in where or " >= " in where:
        # the in-flight predicate the transactional window issues:
        # [(scope) AND ]col > literal  |  [(scope) AND ]col >= datetime_literal
        scope, _, newer = where.rpartition(" AND ") if " AND " in where else (None, None, where)
        op = " >= " if " >= " in newer else " > "
        col, _, lit = newer.partition(op)
        value = row.get(col.strip())
        if value is None:
            return False
        lit = lit.strip().strip("'")
        if isinstance(value, datetime.datetime):
            bound = datetime.datetime.fromisoformat(lit)
            value = _instant(value)
        else:
            bound = type(value)(lit)
        newer_ok = value >= bound if op == " >= " else value > bound
        return newer_ok and _matches(row, scope.strip("() ") if scope else None)
    left, sep, right = where.partition("=")
    if not sep:
        return True
    right = right.strip().strip("'\"")
    key = left.strip()
    value = row[key] if key in row else get_path(row, key)
    return str(value) == right


class _TransactionalMixin:
    """TransactionalSide for the in-memory fakes; `schema` maps table -> SchemaFacts and
    `sequences` maps (table, column) -> next value, both optional."""
    tables: dict
    calls: Counter
    statements: int
    rows_fetched: int

    def _init_transactional(self, schema=None, sequences=None):
        self.schema = schema or {}
        self.sequences = sequences or {}
        self.isolation = "none"
        self.window_open = False
        # called between open and close by tests that simulate a side moving mid-run
        self.on_open = None
        # what open_window pins: "fake_snapshot" (default) or "none" for a side whose engine
        # refused a snapshot; with "none", `change_token` (a callable) feeds the markers
        self.pin = "fake_snapshot"
        self.change_token = None
        # optional per-method failure injection: {"method": Exception}
        self.fail_on: dict = {}

    def _maybe_fail(self, method):
        exc = self.fail_on.get(method)
        if exc is not None:
            raise exc

    def _tx_rows(self, table, where):
        return [r for r in self._all_rows(table) if _matches(r, where)]

    def _tx_key(self, r, key_cols):
        return tuple(get_path(r, k) if k not in r else r[k] for k in key_cols)

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
            keys = [self._tx_key(r, key_cols) for r in rows]
            marker = (len(rows),) + tuple(max(k[i] for k in keys) if keys else None
                                         for i in range(len(key_cols)))
        if self.isolation != "fake_snapshot" and self.change_token:
            marker += (self.change_token(table),)
        return marker

    @staticmethod
    def _digest(value):
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, (int, float, Decimal)):
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
            whole = int(d.to_integral_value(rounding=ROUND_HALF_UP))
            residue = abs(whole) % DIGEST_MODULUS  # sign vanishes in the square anyway
            squares += residue * residue
        return sum(digests, Decimal(0)), squares

    def range_fingerprints(self, table, key_cols, key_kinds, watermark, wm_kind, ranges,
                           where=None) -> list[tuple]:
        self.calls["range_fingerprints"] += 1
        self.statements += 1
        self._maybe_fail("range_fingerprints")
        rows = self._tx_rows(table, where)
        keyed = [(self._tx_key(r, key_cols), r.get(watermark) if watermark else None) for r in rows]
        digestible_keys = all(kind in ("integer", "datetime") for kind in key_kinds)
        digest_wm = bool(watermark and wm_kind in ("integer", "datetime"))
        out = []
        for lo, hi in ranges:
            lo = lo if lo is None or isinstance(lo, tuple) else (lo,)
            hi = hi if hi is None or isinstance(hi, tuple) else (hi,)
            hit = [(k, wm) for k, wm in keyed if (lo is None or k >= lo) and (hi is None or k <= hi)]
            keys = None
            if digestible_keys:
                keys = tuple(self._moments(k[i] for k, _ in hit) for i in range(len(key_cols)))
            wm = self._moments(wm for _, wm in hit) if digest_wm else None
            out.append((len(hit), keys, wm))
        return out

    def keys_in_range(self, table, key_cols, lo, hi, where=None, extra_cols=None) -> list[tuple]:
        self.calls["keys_in_range"] += 1
        self.statements += 1
        self._maybe_fail("keys_in_range")
        lo = lo if lo is None or isinstance(lo, tuple) else (lo,)
        hi = hi if hi is None or isinstance(hi, tuple) else (hi,)
        out = []
        for r in self._tx_rows(table, where):
            k = self._tx_key(r, key_cols)
            if (lo is None or k >= lo) and (hi is None or k <= hi):
                out.append(k + tuple(r.get(c) for c in (extra_cols or [])))
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

    def identity_next(self, table, column) -> int | None:
        self.calls["identity_next"] += 1
        self.statements += 1
        if (table, column) not in self.sequences:
            raise NotImplementedError(f"fake has no identity state for {table}.{column}")
        return self.sequences[(table, column)]

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

    def _key(self, r: dict, key_cols: list[str]) -> tuple:
        return tuple(r[k] if k in r else get_path(r, k) for k in key_cols)

    def _sorted(self, table: str, key_cols: list[str], where: str | None,
                natural: bool = False) -> list[dict]:
        # repr order tolerates mixed-type keys (the historical fake behaviour); strata need the
        # engine's natural key order so lo <= key <= hi range checks hold.
        sort_key = ((lambda r: self._key(r, key_cols)) if natural
                    else (lambda r: tuple(repr(v) for v in self._key(r, key_cols))))
        return sorted((r for r in self.tables[table] if _matches(r, where)), key=sort_key)

    def row_count(self, table: str, where: str | None = None) -> int:
        self.calls["row_count"] += 1
        self.statements += 1
        return sum(_matches(r, where) for r in self.tables[table])

    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]:
        self.calls["table_aggregates"] += 1
        self.statements += 1
        out = {}
        for col in columns:
            agg = self._aggregates(table, col, where)
            if col not in numeric:
                agg["sum"] = None
            out[col] = agg
        return out

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        self.calls["field_aggregates"] += 1
        self.statements += 1
        return self._aggregates(table, column, where)

    def _aggregates(self, table: str, column: str, where: str | None) -> dict[str, Any]:
        vals = [(r[column] if column in r else get_path(r, column))
                for r in self.tables[table] if _matches(r, where)]
        return _agg_of(vals)

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
        lo = lo if isinstance(lo, tuple) else (lo,)
        hi = hi if isinstance(hi, tuple) else (hi,)
        in_range = [r for r in self._sorted(table, key_cols, where, natural=True)
                    if lo <= self._key(r, key_cols) <= hi]
        wanted = set(row_numbers)
        keys = [self._key(r, key_cols) for i, r in enumerate(in_range, 1) if i in wanted]
        self.rows_fetched += len(keys)
        return keys

    def duplicate_key_count(self, table, key_cols, where=None) -> int:
        self.calls["duplicate_key_count"] += 1
        self.statements += 1
        counts = Counter(self._key(r, key_cols) for r in self.tables[table] if _matches(r, where))
        return sum(1 for n in counts.values() if n > 1)


def _get_path(doc: dict, path: str):
    cur: Any = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


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

    def target_row_count(self, object: str, where=None) -> int:
        self.calls["target_row_count"] += 1
        self.statements += 1
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
        out = {}
        for col in columns:
            agg = (self.field_aggregates(object, col, where) if where is not None
                   else self.field_aggregates(object, col))
            if col not in numeric:
                agg["sum"] = None
            out[col] = agg
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
        out = {}
        for col in columns:
            agg = _agg_of([None if (v := get_path(d, col)) is MISSING else v for d in rows])
            if col not in numeric:
                agg["sum"] = None
            out[col] = agg
        return out

    def field_aggregates(self, object: str, field_path: str, where=None) -> dict[str, Any]:
        vals = [get_path(d, field_path) for d in self._rows(object, where)]
        return _agg_of([None if v is MISSING else v for v in vals])

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
