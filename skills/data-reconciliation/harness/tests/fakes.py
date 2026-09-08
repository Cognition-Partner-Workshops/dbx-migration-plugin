"""In-memory fake adapters implementing the adapter protocols, for fixture tests."""

from __future__ import annotations

import json
import decimal
from collections import Counter
from typing import Any, Iterable

from recon.adapters import Stratum
from recon.paths import get_path
from recon.canon import MISSING


def _matches(row: dict, where: str | None) -> bool:
    if not where:
        return True
    if where.lstrip().startswith("{"):
        parsed = json.loads(where)
        return all(row.get(k) == v for k, v in parsed.items())
    left, sep, right = where.partition("=")
    if not sep:
        return True
    right = right.strip().strip("'\"")
    key = left.strip()
    value = row[key] if key in row else get_path(row, key)
    return str(value) == right

class FakeSource:
    """Implements SourceAdapter plus the optional BatchAggregates / StratifiedKeys /
    StatementCounting protocols; `calls` records which entry points the tiers used."""

    def __init__(self, tables: dict[str, list[dict]]):
        self.tables = tables
        self.last_fetch_keyed = None
        self.calls: Counter[str] = Counter()
        self.statements = 0
        self.rows_fetched = 0

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
        nn = [v for v in vals if v is not None]
        nums = [v for v in nn if isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool)]
        return {"count": len(vals),
                "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
                "min": min(nn) if nn else None, "max": max(nn) if nn else None,
                "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}

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


class FakeTarget:
    def __init__(self, objects: dict[str, list[dict]], scopes: dict[str, callable] | None = None):
        self.objects = objects
        self.scopes = scopes or {}
        self.last_fetch_keyed = None
        self.calls: Counter[str] = Counter()
        self.statements = 0
        self.rows_fetched = 0

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

    def field_aggregates(self, object: str, field_path: str, where=None) -> dict[str, Any]:
        vals = [get_path(d, field_path) for d in self._rows(object, where)]
        vals = [None if v is MISSING else v for v in vals]
        nn = [v for v in vals if v is not None]
        nums = [v for v in nn if isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool)]
        return {"count": len(vals),
                "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
                "min": min(nn) if nn else None, "max": max(nn) if nn else None,
                "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}

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
