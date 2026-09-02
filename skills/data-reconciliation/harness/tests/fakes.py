"""In-memory fake adapters implementing the adapter protocols, for fixture tests."""

from __future__ import annotations

import json
from typing import Any, Iterable

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
    return str(row.get(left.strip())) == right

class FakeSource:
    def __init__(self, tables: dict[str, list[dict]]):
        self.tables = tables
        self.last_fetch_keyed = None

    def row_count(self, table: str, where: str | None = None) -> int:
        return sum(_matches(r, where) for r in self.tables[table])

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        vals = [r.get(column) for r in self.tables[table] if _matches(r, where)]
        nn = [v for v in vals if v is not None]
        nums = [v for v in nn if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return {"count": len(vals),
                "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
                "min": min(nn) if nn else None, "max": max(nn) if nn else None,
                "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}

    def fetch_keyed(self, table, key_cols, columns, where=None, keys=None) -> Iterable[dict]:
        self.last_fetch_keyed = {"table": table, "key_cols": key_cols, "columns": columns,
                                 "where": where, "keys": keys}
        wanted = {tuple(k) for k in keys} if keys is not None else None
        for r in sorted(self.tables[table], key=lambda r: tuple(repr(r[k]) for k in key_cols)):
            if _matches(r, where) and (wanted is None or tuple(r[k] for k in key_cols) in wanted):
                yield r

    def iter_keys(self, table, key_cols, where=None):
        for r in sorted(self.tables[table], key=lambda r: tuple(repr(r[k]) for k in key_cols)):
            if _matches(r, where):
                yield tuple(r[k] for k in key_cols)

    def key_strata(self, table, key_cols, n_strata):
        return []


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

    def _rows(self, object, where):
        scope = self.scopes.get(where) if where else None
        return [d for d in self.objects[object]
                if (scope(d) if scope else _matches(d, where))]

    def target_row_count(self, object: str, where=None) -> int:
        return len(self._rows(object, where))

    def nested_count(self, object: str, array_path: str, where=None) -> int:
        return sum(len(get_path(d, array_path) or []) for d in self._rows(object, where))

    def field_aggregates(self, object: str, field_path: str, where=None) -> dict[str, Any]:
        vals = [get_path(d, field_path) for d in self._rows(object, where)]
        vals = [None if v is MISSING else v for v in vals]
        nn = [v for v in vals if v is not None]
        nums = [v for v in nn if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return {"count": len(vals),
                "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
                "min": min(nn) if nn else None, "max": max(nn) if nn else None,
                "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}

    def fetch_keyed(self, object, key_field, fields, where=None, keys=None) -> Iterable[dict]:
        self.last_fetch_keyed = {"object": object, "key_field": key_field,
                                 "fields": fields, "where": where, "keys": keys}
        for d in sorted(self._rows(object, where), key=lambda d: repr(get_path(d, key_field))):
            if keys is None or get_path(d, key_field) in keys:
                yield d
