"""In-memory fake adapters implementing the adapter protocols, for fixture tests."""

from __future__ import annotations

from typing import Any, Iterable


class FakeSource:
    def __init__(self, tables: dict[str, list[dict]]):
        self.tables = tables

    def row_count(self, table: str, where: str | None = None) -> int:
        return len(self.tables[table])

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        vals = [r.get(column) for r in self.tables[table]]
        nn = [v for v in vals if v is not None]
        nums = [v for v in nn if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return {"count": len(vals),
                "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
                "min": min(nn) if nn else None, "max": max(nn) if nn else None,
                "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}

    def fetch_keyed(self, table, key_cols, columns, where=None, keys=None) -> Iterable[dict]:
        wanted = {tuple(k) for k in keys} if keys is not None else None
        for r in sorted(self.tables[table], key=lambda r: tuple(repr(r[k]) for k in key_cols)):
            if wanted is None or tuple(r[k] for k in key_cols) in wanted:
                yield r

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
    def __init__(self, objects: dict[str, list[dict]]):
        self.objects = objects

    def target_row_count(self, object: str) -> int:
        return len(self.objects[object])

    def nested_count(self, object: str, array_path: str) -> int:
        return sum(len(_get_path(d, array_path) or []) for d in self.objects[object])

    def field_aggregates(self, object: str, field_path: str) -> dict[str, Any]:
        vals = [_get_path(d, field_path) for d in self.objects[object]]
        nn = [v for v in vals if v is not None]
        nums = [v for v in nn if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return {"count": len(vals),
                "null_rate": (len(vals) - len(nn)) / len(vals) if vals else 0.0,
                "min": min(nn) if nn else None, "max": max(nn) if nn else None,
                "sum": sum(nums) if nums else None, "distinct_count": len(set(map(repr, nn)))}

    def fetch_keyed(self, object, key_field, fields, keys=None) -> Iterable[dict]:
        for d in sorted(self.objects[object], key=lambda d: repr(_get_path(d, key_field))):
            if keys is None or _get_path(d, key_field) in keys:
                yield d
