
from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import re
import struct
import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .dsn import oracle_dsn_parts
from .fingerprint import DIGEST_MODULUS
from .fingerprint import normalise as _digest_value
from .rerun import normalize_type
from .watermarks import instant
from .watermarks import literal as watermark_literal

UC_PHYSICAL_KINDS = ("MANAGED", "EXTERNAL", "MANAGED_SHALLOW_CLONE", "EXTERNAL_SHALLOW_CLONE",
                     "MATERIALIZED_VIEW", "STREAMING_TABLE")
UC_PIPELINE_KINDS = frozenset({"MATERIALIZED_VIEW", "STREAMING_TABLE"})
_UC_PHYSICAL = "t.table_type IN (" + ", ".join(f"'{k}'" for k in UC_PHYSICAL_KINDS) + ")"
_PG_PHYSICAL = "c.relkind IN ('r', 'p')"

@dataclass(frozen=True)
class Stratum:
    bucket: int
    lo: tuple
    hi: tuple
    n: int

def _as_key(value: Any) -> tuple:
    return value if isinstance(value, tuple) else (value,)

MARKER_BRACKET_ATTEMPTS = 3

_FK_ACTIONS = {"a": "no action", "r": "no action", "c": "cascade", "n": "set null",
               "d": "set default", "no_action": "no action", "restrict": "no action",
               "cascade": "cascade", "set_null": "set null", "set_default": "set default"}

def _fk_action(raw: str) -> str:
    return _FK_ACTIONS.get(str(raw).lower(), str(raw).lower())

@dataclass
class SchemaFacts:
    table: str = ""
    primary_key: tuple[str, ...] = ()
    unique: set[tuple[str, ...]] = field(default_factory=set)
    unique_nulls_equal: set[tuple[str, ...]] = field(default_factory=set)
    foreign_keys: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = field(default_factory=set)
    foreign_key_actions: dict[tuple[tuple[str, ...], str, tuple[str, ...]], tuple[str, str]] = field(
        default_factory=dict)
    not_null: set[str] = field(default_factory=set)
    indexes: set[tuple[str, ...]] = field(default_factory=set)
    check_count: int = 0
    checks: set[str] = field(default_factory=set)
    identity_columns: set[str] = field(default_factory=set)
    partial: set[tuple[str, ...]] = field(default_factory=set)
    expression_unique: set[str] = field(default_factory=set)
    expression_indexes: set[str] = field(default_factory=set)
    triggers: dict[str, tuple[str, tuple[str, ...], str]] = field(default_factory=dict)
    grants: dict[str, frozenset[str]] = field(default_factory=dict)
    grants_effective: bool = True
    primary_key_informational: tuple[str, ...] = ()
    foreign_keys_informational: set[tuple[tuple[str, ...], str, tuple[str, ...]]] =         field(default_factory=set)  # declared but not enforced (UC foreign keys)
    unsupported: frozenset[str] = frozenset()
    privilege_denied: frozenset[str] = frozenset()
    table_type: str | None = None
    declares_not_null: bool = True

@dataclass(frozen=True)
class IdentityState:
    next: int
    increment: int

    @property
    def descending(self) -> bool:
        return self.increment < 0

class SourceAdapter(Protocol):
    def row_count(self, table: str, where: str | None = None) -> int: ...
    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]: ...
    def fetch_keyed(self, table: str, key_cols: list[str], columns: list[str],
                    where: str | None = None, keys: list[tuple] | None = None) -> Iterable[dict[str, Any]]: ...
    def iter_keys(self, table: str, key_cols: list[str], where: str | None = None) -> Iterable[tuple]: ...

class TargetAdapter(Protocol):
    def target_row_count(self, object: str, where: str | None = None) -> int: ...
    def nested_count(self, object: str, array_path: str, where: str | None = None) -> int: ...
    def field_aggregates(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]: ...
    def fetch_keyed(self, object: str, key_fields: list[str], fields: list[str],
                    where: str | None = None, keys: list[Any] | None = None) -> Iterable[dict[str, Any]]: ...

@runtime_checkable
class BatchAggregates(Protocol):
    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None,
                         *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]: ...

@runtime_checkable
class KeyExcludingAggregates(Protocol):
    def table_aggregates_excluding(self, table: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None,
                                   *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]: ...
    def exclusion_capacity(self, key_width: int) -> int: ...

@runtime_checkable
class ColumnTypes(Protocol):
    def numeric_columns(self, table: str) -> set[str]: ...

@runtime_checkable
class WholeNumberColumns(Protocol):
    def whole_number_columns(self, table: str) -> set[str]: ...

@runtime_checkable
class SumProbe(Protocol):
    def sum_probe(self, table: str, column: str, where: str | None = None) -> Any: ...

@runtime_checkable
class StratifiedKeys(Protocol):
    def key_strata(self, table: str, key_cols: list[str], n_strata: int,
                   where: str | None = None) -> list[Stratum]: ...
    def sample_keys(self, table: str, key_cols: list[str], lo: Any, hi: Any,
                    row_numbers: list[int], where: str | None = None) -> list[tuple]: ...
    def duplicate_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int: ...

WINDOW_STRENGTHS = ("snapshot", "change_token", "markers")

@runtime_checkable
class TransactionalSide(Protocol):
    def open_window(self) -> str: ...
    def close_window(self) -> None: ...
    def discard(self) -> None: ...
    def window_strength(self) -> str: ...
    def window_marker(self, table: str, key_cols: list[str], watermark: str | None,
                      where: str | None = None) -> tuple: ...
    def range_fingerprints(self, table: str, key_cols: list[str], key_kinds: list[str],
                           watermark: str | None, wm_kind: str | None,
                           ranges: list[tuple[tuple | None, tuple | None]],
                           where: str | None = None) -> list[tuple[int, tuple | None, Any]]: ...
    def keys_in_range(self, table: str, key_cols: list[str], lo: tuple | None, hi: tuple | None,
                      where: str | None = None, extra_cols: list[str] | None = None) -> list[tuple]: ...
    def max_watermark(self, table: str, watermark: str, where: str | None = None) -> Any: ...
    def watermark_literal(self, value: Any) -> str: ...
    def schema_facts(self, table: str) -> SchemaFacts: ...
    def identity_state(self, table: str, column: str) -> IdentityState | None: ...

Position = bytes | int

@dataclass(frozen=True)
class DeleteEvent:
    key: tuple
    position: Position
    age_s: float

@runtime_checkable
class DeleteEvidence(Protocol):
    def delete_evidence_kind(self) -> str: ...
    def evidence_horizon(self, capture: str) -> tuple[Position | None, Position | None]: ...
    def deletes_since(self, capture: str, key_cols: list[str], after: Position,
                      upto: Position, where: str | None = None) -> list[DeleteEvent]: ...

@runtime_checkable
class AppliedPosition(Protocol):
    def applied_position(self, table: str, column: str, where: str | None) -> Position | None: ...

@runtime_checkable
class NullKeyCounting(Protocol):
    def null_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int: ...

@runtime_checkable
class StatementCounting(Protocol):
    statements: int
    rows_fetched: int

def _secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"secret '{name}' not found in environment; pass secrets by name only")
    return value

AGG_SQL = ("SELECT COUNT(*) AS n, COUNT({col}) AS nonnull, MIN({col}) AS mn, "
           "MAX({col}) AS mx, COUNT(DISTINCT {col}) AS dc FROM {table}{where}")
PROFILE_SQL = "SELECT COUNT(*) AS n, COUNT({col}) AS nonnull, COUNT(DISTINCT {col}) AS dc FROM {table}{where}"

NTILE_SQL = "NTILE({n}) OVER (ORDER BY {order})"

class DictionaryError(RuntimeError):

    def __init__(self, message: str, kind: str = "read"):
        super().__init__(message)
        self.kind = kind

_PRIVILEGE_ERROR = re.compile(
    r"ORA-0(0942|1031)|\b42501\b|permission[ _]denied|permission was denied|insufficient[ _]"
    r"(privilege|permission)|not authorized|access denied", re.IGNORECASE)

def dictionary_error(table: str, view: str, exc: BaseException) -> DictionaryError:
    kind = "privilege" if _PRIVILEGE_ERROR.search(f"{type(exc).__name__}: {exc}") else "read"
    return DictionaryError(f"{table}: {view} read failed ({type(exc).__name__})", kind)

def _read_grants(facts: SchemaFacts, read) -> None:
    try:
        rows = read()
    except DictionaryError as exc:
        if exc.kind != "privilege":
            raise
        facts.unsupported = facts.unsupported | {"grants"}
        facts.privilege_denied = facts.privilege_denied | {"grants"}
        return
    grants: dict[str, set] = {}
    for grantee, priv in rows:
        grants.setdefault(str(grantee).lower(), set()).add(str(priv).lower())
    facts.grants = {g: frozenset(p) for g, p in grants.items()}

class _SqlAdapterBase:

    paramstyle = "qmark"
    max_params = 2000
    bucket_sql = NTILE_SQL
    snapshot_sql: str | None = None
    snapshot_reset_sql: str | None = None

    change_token_sql: str | None = None
    integer_digest_sql = "CAST({col} AS DECIMAL(38,0))"
    datetime_digest_sql: str | None = None  # whole microseconds since the epoch
    mod_sql = "MOD({x}, {m})"
    square_digest_sql = "CAST({r} * {r} AS DECIMAL(38,0))"
    watermark_literal_utc_offset = False
    datetime_bound_sql = "{lit}"
    binary_literal_sql = "0x{hex}"
    CATALOG_STATEMENTS = {"schema_facts": 0, "identity_state": 0, "session": 0}

    def __init__(self, conn):
        self._conn = conn
        self.statements = 0
        self.rows_fetched = 0
        self.isolation = "none"
        self._token_ok: bool | None = None
        self.window_released: str | None = None

    def _execute(self, sql: str, params=()):
        cur = self._conn.cursor()
        self.statements += 1  # sent is spent: a rejected statement still reached the source
        cur.execute(sql, params)
        return cur

    def _rows(self, sql: str, params=()) -> list[tuple]:
        return self._execute(sql, params).fetchall()

    def _placeholders(self, n: int, offset: int = 0) -> list[str]:
        if self.paramstyle == "format":
            return ["%s"] * n
        if self.paramstyle == "named":
            return [f":{i}" for i in range(offset + 1, offset + n + 1)]
        if self.paramstyle == "pyformat":
            return [f"%(p{i})s" for i in range(offset, offset + n)]
        return ["?"] * n

    def _params(self, values: list[Any]):
        if self.paramstyle == "named":
            return {str(i): value for i, value in enumerate(values, 1)}
        if self.paramstyle == "pyformat":
            return {f"p{i}": value for i, value in enumerate(values)}
        return tuple(values)

    def _dict_rows(self, table: str, view: str, sql: str, params=()) -> list[tuple]:
        try:
            return self._rows(sql, params)
        except (DictionaryError, NotImplementedError):
            raise
        except Exception as exc:
            raise dictionary_error(table, view, exc) from exc

    _WRITE_SQL = re.compile(
        r";|\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|"
        r"call|execute|copy|comment|vacuum|analyze|checkpoint|set|use|show)\b", re.I)

    @staticmethod
    def _unquoted(sql: str) -> str:
        out, i, n, mode, tag = [], 0, len(sql), None, ""
        while i < n:
            ch = sql[i]
            if mode == "line_comment":
                if ch == "\n":
                    mode = None
                    out.append(" ")
                i += 1
            elif mode == "block_comment":
                if sql.startswith("*/", i):
                    mode = None
                    out.append(" ")
                    i += 2
                else:
                    i += 1
            elif mode in ("squote", "esquote", "dquote", "bracket"):
                quote = {"squote": "'", "esquote": "'", "dquote": '"',
                         "bracket": "]"}[mode]
                if mode == "esquote" and ch == "\\":
                    i += 2
                elif sql.startswith(quote + quote, i):
                    i += 2
                elif sql[i] == quote:
                    mode = None
                    i += 1
                else:
                    i += 1
            elif mode == "dollar":
                if sql.startswith(tag, i):
                    mode = None
                    i += len(tag)
                else:
                    i += 1
            elif sql.startswith("--", i):
                mode, i = "line_comment", i + 2
            elif sql.startswith("/*", i):
                mode, i = "block_comment", i + 2
            elif ch == "'":
                prev = sql[i - 1] if i else ""
                before = sql[i - 2] if i > 1 else " "
                mode = ("esquote" if prev in "eE" and not
                        (before.isalnum() or before in "_$") else "squote")
                i += 1
            elif ch == '"':
                mode, i = "dquote", i + 1
            elif ch == "[":
                mode, i = "bracket", i + 1
            elif (ch == "$" and (i == 0 or not
                      (sql[i - 1].isalnum() or sql[i - 1] in "_$"))
                  and (m := re.match(r"\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$", sql[i:]))):
                mode, tag, i = "dollar", m.group(0), i + len(m.group(0))
            else:
                out.append(ch)
                i += 1
        return "".join(out)

    def run_query(self, sql: str) -> list[dict[str, Any]]:
        head = sql.lstrip().lower()
        if not head.startswith(("select", "with")) or self._WRITE_SQL.search(self._unquoted(sql)):
            from .config import ConfigError
            raise ConfigError("recorded SQL ops must be a single read-only SELECT or WITH query")
        cur = self._execute(sql)
        names = self._result_names(cur.description or [])
        rows = cur.fetchall()
        self.rows_fetched += len(rows)
        return [dict(zip(names, row)) for row in rows]

    def _result_names(self, description, requested: Sequence[str] | None = None) -> list[str]:
        return [d[0] for d in description]

    def row_count(self, table: str, where: str | None = None) -> int:
        w = f" WHERE {where}" if where else ""
        return int(self._rows(f"SELECT COUNT(*) FROM {table}{w}")[0][0])

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        w = f" WHERE {where}" if where else ""
        n, nonnull, mn, mx, dc = self._rows(AGG_SQL.format(col=column, table=table, where=w))[0]
        out = {"count": int(n), "null_rate": (int(n) - int(nonnull)) / int(n) if n else 0.0,
               "min": mn, "max": mx, "distinct_count": int(dc)}
        out["sum"] = self.sum_probe(table, column, where)
        return out

    def column_profile(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        w = f" WHERE {where}" if where else ""
        n, nonnull, dc = self._rows(PROFILE_SQL.format(col=column, table=table, where=w))[0]
        return {"count": int(n), "null_rate": (int(n) - int(nonnull)) / int(n) if n else 0.0,
                "distinct_count": int(dc)}

    def sum_probe(self, table: str, column: str, where: str | None = None) -> Any:
        w = f" WHERE {where}" if where else ""
        try:
            (s,) = self._rows(f"SELECT SUM({column}) FROM {table}{w}")[0]
            return s
        except Exception:  # noqa: BLE001  driver-specific error type: SUM on a non-numeric column
            if hasattr(self._conn, "rollback"):
                self._conn.rollback()  # libpq leaves the transaction aborted otherwise
                self._release_window(f"SUM({column}) on {table} failed and rolled back")
            return None

    def _release_window(self, reason: str) -> None:
        if self.isolation != "none":
            self.isolation = "none"
            self.window_released = reason

    def numeric_columns(self, table: str) -> set[str]:
        raise NotImplementedError(f"{type(self).__name__} cannot read column types")

    def whole_number_columns(self, table: str) -> set[str]:
        raise NotImplementedError(f"{type(self).__name__} cannot read column types")

    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None,
                         *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        w = f" WHERE {where}" if where else ""
        return self._table_aggregates(table, columns, numeric, w, [], unordered)

    def exclusion_capacity(self, key_width: int) -> int:
        return max(1, self.max_params // max(1, key_width))

    def table_aggregates_excluding(self, table: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None,
                                   *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        if len(exclude_keys) > self.exclusion_capacity(len(key_cols)):
            raise ValueError(f"{len(exclude_keys)} keys x {len(key_cols)} columns exceed the "
                             f"{self.max_params}-parameter budget of one statement")
        clauses, values = [], []
        if where:
            clauses.append(f"({where})")
        if exclude_keys:
            clauses.append("NOT " + self._keys_clause(key_cols, exclude_keys, values))
        w = " WHERE " + " AND ".join(clauses) if clauses else ""
        return self._table_aggregates(table, columns, numeric, w, values, unordered)

    def _table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                          w: str, values: list[Any],
                          unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        exprs = ["COUNT(*)"]
        for col in columns:
            exprs += ([f"COUNT({col})", "NULL", "NULL", f"COUNT(DISTINCT {col})"]
                      if col in unordered else
                      [f"COUNT({col})", f"MIN({col})", f"MAX({col})", f"COUNT(DISTINCT {col})"])
            if col in numeric:
                exprs.append(f"SUM({col})")
        row = list(self._rows(f"SELECT {', '.join(exprs)} FROM {table}{w}", self._params(values))[0])
        n = int(row.pop(0))
        out: dict[str, dict[str, Any]] = {}
        for col in columns:
            nonnull, mn, mx, dc = row[:4]
            del row[:4]
            agg = {"count": n, "null_rate": (n - int(nonnull)) / n if n else 0.0,
                   "min": mn, "max": mx, "distinct_count": int(dc)}
            if col in numeric:
                agg["sum"] = row.pop(0)
            else:
                agg["sum"] = None
            out[col] = agg
        return out

    def fetch_keyed(self, table: str, key_cols: list[str], columns: list[str],
                    where: str | None = None, keys: list[tuple] | None = None) -> Iterable[dict[str, Any]]:
        cols = ", ".join(dict.fromkeys(key_cols + columns))
        chunks = [keys[i:i + 500] for i in range(0, len(keys), 500)] if keys is not None else [None]
        for chunk in chunks:
            clauses, values = [], []
            if where:
                clauses.append(f"({where})")
            if chunk is not None:
                clauses.append(self._keys_clause(key_cols, chunk, values))
            w = " WHERE " + " AND ".join(clauses) if clauses else ""
            cur = self._execute(f"SELECT {cols} FROM {table}{w} ORDER BY {', '.join(key_cols)}",
                                self._params(values))
            names = self._result_names(cur.description, list(dict.fromkeys(key_cols + columns)))
            for row in cur:
                self.rows_fetched += 1
                yield dict(zip(names, row))

    def _keys_clause(self, key_cols: list[str], keys: list, values: list[Any]) -> str:
        if len(key_cols) == 1:
            offset = len(values)
            values.extend(k[0] if isinstance(k, tuple) else k for k in keys)
            return f"({key_cols[0]} IN ({', '.join(self._placeholders(len(keys), offset))}))"
        alternatives = []
        for key in keys:
            parts = []
            for col, value in zip(key_cols, key):
                parts.append(f"{col} = {self._placeholders(1, len(values))[0]}")
                values.append(value)
            alternatives.append("(" + " AND ".join(parts) + ")")
        return "(" + " OR ".join(alternatives) + ")"

    def iter_keys(self, table: str, key_cols: list[str], where: str | None = None) -> Iterable[tuple]:
        w = f" WHERE {where}" if where else ""
        cur = self._execute(f"SELECT {', '.join(key_cols)} FROM {table}{w} ORDER BY {', '.join(key_cols)}")
        for row in cur:
            self.rows_fetched += 1
            yield tuple(row)

    def key_strata(self, table: str, key_cols: list[str], n_strata: int,
                   where: str | None = None) -> list[Stratum]:
        order = ", ".join(key_cols)
        w = f" WHERE {where}" if where else ""
        bucket = self.bucket_sql.format(n=int(n_strata), order=order)
        inner = f"(SELECT {order}, {bucket} AS b FROM {table}{w}) s"
        if len(key_cols) == 1:
            k = key_cols[0]
            rows = self._rows(f"SELECT b, MIN({k}), MAX({k}), COUNT(*) FROM {inner} GROUP BY b ORDER BY b")
            self.rows_fetched += len(rows)
            return [Stratum(int(b), (lo,), (hi,), int(n)) for b, lo, hi, n in rows]
        desc = ", ".join(f"{k} DESC" for k in key_cols)
        rows = self._rows(
            f"SELECT b, ra, rd, cnt, {order} FROM (SELECT {order}, b, "
            f"ROW_NUMBER() OVER (PARTITION BY b ORDER BY {order}) AS ra, "
            f"ROW_NUMBER() OVER (PARTITION BY b ORDER BY {desc}) AS rd, "
            f"COUNT(*) OVER (PARTITION BY b) AS cnt FROM {inner}) x "
            f"WHERE ra = 1 OR rd = 1 ORDER BY b, ra")
        self.rows_fetched += len(rows)
        by_bucket: dict[int, dict[str, Any]] = {}
        for b, ra, rd, cnt, *key in rows:
            entry = by_bucket.setdefault(int(b), {"n": int(cnt)})
            if int(ra) == 1:
                entry["lo"] = tuple(key)
            if int(rd) == 1:
                entry["hi"] = tuple(key)
        return [Stratum(b, e["lo"], e["hi"], e["n"]) for b, e in sorted(by_bucket.items())]

    def _key_bound(self, key_cols: list[str], bound: tuple, op: str, offset: int) -> tuple[str, list[Any]]:
        strict = op[0]
        terms, values = [], []
        for i in range(len(bound)):
            eq = [f"{k} = {{}}" for k in key_cols[:i]]
            last_op = op if i == len(bound) - 1 else strict
            terms.append("(" + " AND ".join(eq + [f"{key_cols[i]} {last_op} {{}}"]) + ")")
            values += list(bound[:i + 1])
        ph = iter(self._placeholders(len(values), offset))
        sql = " OR ".join(terms).format(*[next(ph) for _ in values])
        return f"({sql})", values

    def sample_keys(self, table: str, key_cols: list[str], lo: Any, hi: Any,
                    row_numbers: list[int], where: str | None = None) -> list[tuple]:
        if not row_numbers:
            return []
        order = ", ".join(key_cols)
        lo_sql, values = self._key_bound(key_cols, _as_key(lo), ">=", 0)
        hi_sql, hi_vals = self._key_bound(key_cols, _as_key(hi), "<=", len(values))
        values += hi_vals
        clauses = [lo_sql, hi_sql]
        if where:
            clauses.append(f"({where})")
        rn_ph = self._placeholders(len(row_numbers), len(values))
        values += [int(r) for r in row_numbers]
        sql = (f"SELECT {order} FROM (SELECT {order}, ROW_NUMBER() OVER (ORDER BY {order}) AS rn "
               f"FROM {table} WHERE {' AND '.join(clauses)}) s WHERE rn IN ({', '.join(rn_ph)}) ORDER BY {order}")
        rows = self._rows(sql, self._params(values))
        self.rows_fetched += len(rows)
        return [tuple(r) for r in rows]

    def duplicate_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int:
        order = ", ".join(key_cols)
        w = f" WHERE {where}" if where else ""
        (n,) = self._rows(f"SELECT COUNT(*) FROM (SELECT {order} FROM {table}{w} "
                          f"GROUP BY {order} HAVING COUNT(*) > 1) d")[0]
        return int(n)

    def open_window(self) -> str:
        self.window_released = None
        if hasattr(self._conn, "rollback"):
            try:
                self._conn.rollback()
            except Exception:  # noqa: BLE001, S110  nothing to roll back on some drivers
                pass
        if self.snapshot_sql:
            try:
                self._execute(self.snapshot_sql)
                self.isolation = "snapshot"
            except Exception:  # noqa: BLE001  driver-specific error type
                if hasattr(self._conn, "rollback"):
                    self._conn.rollback()
                self.isolation = "none"
        return self.isolation

    def close_window(self) -> None:
        if hasattr(self._conn, "rollback"):
            self._conn.rollback()

    def discard(self) -> None:
        self.isolation = "none"
        self._conn.close()

    def window_strength(self) -> str:
        if self.isolation in ("snapshot", "repeatable_read"):
            return "snapshot"
        return "change_token" if self._token_ok else "markers"

    def watermark_literal(self, value: Any) -> str:
        lit = watermark_literal(value, utc_offset=self.watermark_literal_utc_offset,
                                binary=self.binary_literal_sql)
        return self.datetime_bound_sql.format(lit=lit) if isinstance(value, dt.datetime) else lit

    def _change_token(self, table: str) -> Any:
        if not self.change_token_sql or self._token_ok is False:
            return None
        try:
            (token,) = self._rows(self.change_token_sql.format(table=table))[0]
        except Exception:  # noqa: BLE001  driver-specific error type (missing DMV / permission)
            self._token_ok = False
            return None
        self._token_ok = True
        return token

    def window_marker(self, table: str, key_cols: list[str], watermark: str | None,
                      where: str | None = None) -> tuple:
        w = f" WHERE {where}" if where else ""
        marks = [f"MAX({watermark})"] if watermark else [f"MAX({k})" for k in key_cols]
        sql = f"SELECT COUNT(*), {', '.join(marks)} FROM {table}{w}"
        if self.isolation in ("snapshot", "repeatable_read"):
            try:
                return tuple(self._rows(sql)[0])
            except Exception:
                if self.isolation != "snapshot":
                    raise
                self._conn.rollback()
                if self.snapshot_reset_sql:
                    self._execute(self.snapshot_reset_sql)
                self.isolation = "none"
        for _ in range(MARKER_BRACKET_ATTEMPTS):
            before = self._change_token(table)
            row = self._rows(sql)[0]
            after = self._change_token(table)
            if before == after:
                return tuple(row) + (after,)
        return tuple(row) + (before, after)

    def _range_predicate(self, key_cols: list[str], lo: tuple | None, hi: tuple | None,
                         offset: int) -> tuple[str, list[Any]]:
        parts, values = [], []
        if lo is not None:
            sql, vals = self._key_bound(key_cols, _as_key(lo), ">=", offset + len(values))
            parts.append(sql)
            values += vals
        if hi is not None:
            sql, vals = self._key_bound(key_cols, _as_key(hi), "<=", offset + len(values))
            parts.append(sql)
            values += vals
        return (" AND ".join(parts) if parts else "1 = 1"), values

    def _digest_sql(self, col: str, kind: str) -> tuple[str, str] | None:
        if kind == "integer":
            digest = self.integer_digest_sql.format(col=col)
        elif kind == "datetime" and self.datetime_digest_sql:
            digest = self.datetime_digest_sql.format(col=col)
        else:
            return None
        residue = self.mod_sql.format(x=f"CAST({digest} AS DECIMAL(38,0))", m=DIGEST_MODULUS)
        return digest, self.square_digest_sql.format(r=residue)

    def range_fingerprints(self, table: str, key_cols: list[str], key_kinds: list[str],
                           watermark: str | None, wm_kind: str | None,
                           ranges: list[tuple[tuple | None, tuple | None]],
                           where: str | None = None) -> list[tuple[int, tuple | None, Any]]:
        if not ranges:
            return []
        key_digests = [self._digest_sql(k, kind) for k, kind in zip(key_cols, key_kinds)]
        key_digestible = all(d is not None for d in key_digests)
        wm_digest = self._digest_sql(watermark, wm_kind) if watermark and wm_kind else None
        terms = []
        if key_digestible:
            terms += [t for pair in key_digests for t in pair]
        if wm_digest:
            terms += [*wm_digest, f"CASE WHEN {watermark} IS NULL THEN 1 ELSE 0 END"]
        per_range = max(len(self._range_predicate(key_cols, lo, hi, 0)[1]) for lo, hi in ranges)
        batch = max(1, self.max_params // max(1, per_range))
        w = f" WHERE {where}" if where else ""
        out: list[tuple[int, tuple | None, Any]] = []
        for start in range(0, len(ranges), batch):
            chunk = ranges[start:start + batch]
            whens, values = [], []
            for i, (lo, hi) in enumerate(chunk):
                pred, vals = self._range_predicate(key_cols, lo, hi, len(values))
                values += vals
                whens.append(f"WHEN {pred} THEN {i}")
            cols = ", ".join(dict.fromkeys(list(key_cols) + ([watermark] if watermark else [])))
            selected = ", ".join(["rng", "COUNT(*)"] + [f"SUM({t})" for t in terms])
            sql = (f"SELECT {selected} FROM (SELECT CASE {' '.join(whens)} END AS rng, {cols} "
                   f"FROM {table}{w}) r WHERE rng IS NOT NULL GROUP BY rng")
            by_range = {int(r[0]): list(r[1:]) for r in self._rows(sql, self._params(values))}
            for i in range(len(chunk)):
                row = iter(by_range.get(i, [0] + [None] * len(terms)))
                n = int(next(row) or 0)
                keys = (tuple((_digest_value(next(row)), _digest_value(next(row))) for _ in key_cols)
                        if key_digestible else None)
                wm = ((_digest_value(next(row)), _digest_value(next(row)), int(next(row) or 0))
                      if wm_digest else None)
                out.append((n, keys, wm))
        return out

    def keys_in_range(self, table: str, key_cols: list[str], lo: tuple | None, hi: tuple | None,
                      where: str | None = None, extra_cols: list[str] | None = None) -> list[tuple]:
        pred, values = self._range_predicate(key_cols, lo, hi, 0)
        clauses = [pred] + ([f"({where})"] if where else [])
        cols = ", ".join(list(key_cols) + list(extra_cols or []))
        rows = self._rows(f"SELECT {cols} FROM {table} WHERE {' AND '.join(clauses)} "
                          f"ORDER BY {', '.join(key_cols)}", self._params(values))
        self.rows_fetched += len(rows)
        return [tuple(r) for r in rows]

    def max_watermark(self, table: str, watermark: str, where: str | None = None) -> Any:
        w = f" WHERE {where}" if where else ""
        return self._rows(f"SELECT MAX({watermark}) FROM {table}{w}")[0][0]

    def schema_facts(self, table: str) -> SchemaFacts:
        raise NotImplementedError(f"{type(self).__name__} cannot read constraint metadata")

    def identity_state(self, table: str, column: str) -> IdentityState | None:
        raise NotImplementedError(f"{type(self).__name__} cannot read identity state")

    def null_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int:
        nulls = " OR ".join(f"{k} IS NULL" for k in key_cols)
        w = f" AND ({where})" if where else ""
        (n,) = self._rows(f"SELECT COUNT(*) FROM {table} WHERE ({nulls}){w}")[0]
        return int(n)

def _key_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return instant(value).isoformat(sep=" ", timespec="microseconds") + "+00:00"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    return str(value)

def quote_ident(name: str, quote: str) -> str:
    if not name or "." in name or "\x00" in name:
        from .config import ConfigError
        raise ConfigError(f"invalid SQL identifier {name!r}")
    return quote + name.replace(quote, quote * 2) + quote

_PG_TRIGGER_EVENTS = ((4, "insert"), (8, "delete"), (16, "update"), (32, "truncate"))

def _pg_trigger_shape(tgtype: int) -> tuple[str, tuple[str, ...], str]:
    timing = "instead of" if tgtype & 64 else "before" if tgtype & 2 else "after"
    return (timing, tuple(ev for bit, ev in _PG_TRIGGER_EVENTS if tgtype & bit),
            "row" if tgtype & 1 else "statement")

def _uc_schema_facts(run_query, catalog: str, schema: str, table: str) -> SchemaFacts:
    facts = SchemaFacts(table=f"{catalog}.{schema}.{table}",
                        unsupported=frozenset({"indexes"}),
                        grants_effective=False)

    def q(view: str, sql: str, params: dict) -> list[tuple]:
        try:
            return run_query(sql, params)
        except (DictionaryError, NotImplementedError):
            raise
        except Exception as exc:
            raise dictionary_error(table, view, exc) from exc

    rows = q("information_schema.tables",
        f"SELECT t.table_type FROM {catalog}.information_schema.tables t "
        "WHERE t.table_catalog = %(catalog)s AND t.table_schema = %(schema)s AND t.table_name = %(table)s",
        {"catalog": catalog, "schema": schema, "table": table})
    facts.table_type = str(rows[0][0]).upper() if rows else None
    rows = q("information_schema.table_constraints",
        f"SELECT tc.constraint_type, tc.constraint_name, kcu.column_name, "
        "uk.table_schema, uk.table_name, uk.column_name "
        f"FROM {catalog}.information_schema.table_constraints tc "
        f"LEFT JOIN {catalog}.information_schema.key_column_usage kcu "
        "  ON kcu.constraint_catalog = tc.constraint_catalog "
        " AND kcu.constraint_schema = tc.constraint_schema "
        " AND kcu.constraint_name = tc.constraint_name "
        " AND kcu.table_name = tc.table_name "
        f"LEFT JOIN {catalog}.information_schema.referential_constraints rc "
        "  ON rc.constraint_catalog = tc.constraint_catalog "
        " AND rc.constraint_schema = tc.constraint_schema "
        " AND rc.constraint_name = tc.constraint_name "
        f"LEFT JOIN {catalog}.information_schema.key_column_usage uk "
        "  ON uk.constraint_catalog = rc.unique_constraint_catalog "
        " AND uk.constraint_schema = rc.unique_constraint_schema "
        " AND uk.constraint_name = rc.unique_constraint_name "
        " AND uk.ordinal_position = kcu.position_in_unique_constraint "
        "WHERE tc.table_catalog = %(catalog)s AND tc.table_schema = %(schema)s "
        "AND tc.table_name = %(table)s "
        "ORDER BY tc.constraint_name, kcu.ordinal_position",
        {"catalog": catalog, "schema": schema, "table": table})
    by_con: dict[str, list] = {}
    for ctype, cname, col, ref_schema, ref_table, ref_col in rows:
        entry = by_con.setdefault(cname, [ctype, [], None, []])
        if col is not None:
            entry[1].append(col)
        if ref_table is not None:
            entry[2] = f"{ref_schema}.{ref_table}"
            if ref_col is not None:
                entry[3].append(ref_col)
    for ctype, cols, ref, ref_cols in by_con.values():
        kind = str(ctype).upper()
        if kind == "PRIMARY KEY":
            facts.primary_key_informational = tuple(cols)  # UC PKs are RELY, never enforced
        elif kind == "UNIQUE":
            facts.unique.add(tuple(cols))
        elif kind == "FOREIGN KEY" and ref is not None:
            facts.foreign_keys_informational.add((tuple(cols), ref, tuple(ref_cols)))
        elif kind == "CHECK":
            facts.check_count += 1
    rows = q("information_schema.columns",
        f"SELECT column_name, is_nullable FROM {catalog}.information_schema.columns "
        "WHERE table_catalog = %(catalog)s AND table_schema = %(schema)s AND table_name = %(table)s",
        {"catalog": catalog, "schema": schema, "table": table})
    for col, nullable in rows:
        if str(nullable).upper() == "NO":
            facts.not_null.add(col)
    facts.identity_columns = set(_uc_identity_columns(
        q("SHOW CREATE TABLE",
          f"SHOW CREATE TABLE {quote_ident(catalog, '`')}.{quote_ident(schema, '`')}."
          f"{quote_ident(table, '`')}", {})))
    rows = q("information_schema.check_constraints",
        f"SELECT cc.check_clause FROM {catalog}.information_schema.check_constraints cc "
        f"JOIN {catalog}.information_schema.table_constraints tc "
        "  ON tc.constraint_catalog = cc.constraint_catalog "
        " AND tc.constraint_schema = cc.constraint_schema "
        " AND tc.constraint_name = cc.constraint_name "
        "WHERE tc.table_catalog = %(catalog)s AND tc.table_schema = %(schema)s "
        "AND tc.table_name = %(table)s",
        {"catalog": catalog, "schema": schema, "table": table})
    facts.check_count = max(facts.check_count, len(rows))
    facts.checks = {clause for (clause,) in rows}
    _read_grants(facts, lambda: q("information_schema.table_privileges",
        f"SELECT tp.grantee, tp.privilege_type FROM {catalog}.information_schema.table_privileges tp "
        f"JOIN {catalog}.information_schema.tables t "
        "  ON t.table_catalog = tp.table_catalog AND t.table_schema = tp.table_schema "
        " AND t.table_name = tp.table_name "
        "WHERE tp.table_catalog = %(catalog)s AND tp.table_schema = %(schema)s "
        "AND tp.table_name = %(table)s AND tp.grantee <> t.table_owner "
        "ORDER BY tp.grantee",
        {"catalog": catalog, "schema": schema, "table": table}))
    return facts

_UC_IDENTITY_RE = re.compile(
    r"^\s*(?:`(?P<bt>[^`]+)`|\"(?P<dq>[^\"]+)\"|(?P<plain>[A-Za-z_][\w$]*))"
    r".*?\bGENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY"
    r"(?:\s*\((?P<opts>[^)]*)\))?",
    re.IGNORECASE)

def _uc_identity_columns(ddl_rows) -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for (ddl,) in ddl_rows:
        for line in str(ddl).splitlines():
            m = _UC_IDENTITY_RE.match(line)
            if not m:
                continue
            col = m.group("bt") or m.group("dq") or m.group("plain")
            opts = m.group("opts") or ""
            start = re.search(r"START\s+WITH\s+(-?\d+)", opts, re.IGNORECASE)
            inc = re.search(r"INCREMENT\s+BY\s+(-?\d+)", opts, re.IGNORECASE)
            out[col] = (int(start.group(1)) if start else 1,
                        int(inc.group(1)) if inc else 1)
    return out

def _uc_identity_state(run_query, catalog: str, schema: str, table: str,
                       column: str) -> IdentityState | None:
    qual = (f"{quote_ident(catalog, '`')}.{quote_ident(schema, '`')}."
            f"{quote_ident(table, '`')}")

    def q(view: str, sql: str) -> list[tuple]:
        try:
            return run_query(sql, {})
        except (DictionaryError, NotImplementedError):
            raise
        except Exception as exc:
            raise dictionary_error(table, view, exc) from exc

    state = _uc_identity_columns(q("SHOW CREATE TABLE", f"SHOW CREATE TABLE {qual}")).get(column)
    if not state:
        return None
    start, inc = state
    edge = "MIN" if inc < 0 else "MAX"
    (mx,) = q(f"{column} {edge.lower()}",
              f"SELECT {edge}({quote_ident(column, '`')}) FROM {qual}")[0]
    return IdentityState(int(mx) + inc if mx is not None else start, inc)

_TSQL_LENGTH_TYPES = ("char", "varchar", "binary", "varbinary", "nchar", "nvarchar")
_TSQL_PRECISION_TYPES = ("decimal", "numeric")
_TSQL_SCALE_TYPES = ("datetime2", "datetimeoffset", "time")
_TSQL_BITS_TYPES = ("float",)  # float(24) and float(53) are different storage

def _tsql_type(kind, max_length, precision, scale) -> str:
    kind = str(kind).lower()
    if kind in _TSQL_LENGTH_TYPES and max_length is not None:
        if int(max_length) == -1:
            return f"{kind}(max)"
        n = int(max_length) // 2 if kind.startswith("n") else int(max_length)
        return f"{kind}({n})"
    if kind in _TSQL_PRECISION_TYPES and precision is not None:
        return f"{kind}({int(precision)},{int(scale or 0)})"
    if kind in _TSQL_SCALE_TYPES and scale is not None:
        return f"{kind}({int(scale)})"
    if kind in _TSQL_BITS_TYPES and precision is not None:
        return f"{kind}({int(precision)})"
    return normalize_type(kind)

def _split_table(table: str, default_schema: str | None) -> tuple[str | None, str]:
    parts = table.replace("[", "").replace("]", "").replace('"', "").split(".")
    return (parts[-2] if len(parts) > 1 else default_schema), parts[-1]

_INDEX_KEYS_START_RE = re.compile(r"\bUSING\s+\w+\s*\(", re.IGNORECASE)

def normalize_sql_text(text: str) -> str:
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch in "'\"":
            j = i + 1
            while j < n:
                if text[j] == ch:
                    if j + 1 < n and text[j + 1] == ch:  # doubled quote escapes itself
                        j += 2
                        continue
                    break
                j += 1
            out.append(text[i:j + 1])
            i = j + 1
        else:
            j = i
            while j < n and text[j] not in "'\"":
                j += 1
            out.append(re.sub(r"\s+", " ", text[i:j]).lower())
            i = j
    return "".join(out).strip()

def _index_key_text(indexdef: str) -> str:
    m = _INDEX_KEYS_START_RE.search(indexdef)
    if m is None:
        return normalize_sql_text(indexdef)
    depth, start, quote = 1, m.end(), ""
    for i in range(start, len(indexdef)):
        ch = indexdef[i]
        if quote:
            if ch == quote:
                quote = ""
            continue
        if ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return normalize_sql_text(indexdef[start:i])
    return normalize_sql_text(indexdef[start:])

def _datetimeoffset_from_odbc(raw: bytes) -> dt.datetime:
    year, month, day, hour, minute, second, fraction, tz_hour, tz_minute = \
        struct.unpack("<6hI2h", raw)
    return dt.datetime(year, month, day, hour, minute, second, fraction // 1000,
                       tzinfo=dt.timezone(dt.timedelta(hours=tz_hour, minutes=tz_minute)))

class SqlServerSourceAdapter(_SqlAdapterBase):

    snapshot_sql = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
    snapshot_reset_sql = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    mod_sql = "({x} % {m})"  # T-SQL has no MOD(); pyodbc binds with `?`, so `%` is literal
    CATALOG_STATEMENTS = {"schema_facts": 6, "identity_state": 1, "session": 0}
    change_token_sql = ("SELECT ISNULL(SUM(user_updates), 0) FROM sys.dm_db_index_usage_stats "
                        "WHERE database_id = DB_ID() AND object_id = OBJECT_ID('{table}')")
    datetime_digest_sql = "CAST(DATEDIFF_BIG(MICROSECOND, '19700101', {col}) AS DECIMAL(38,0))"
    datetime_bound_sql = "CAST({lit} AS datetime2(7))"

    def __init__(self, dsn_secret: str):
        import pyodbc  # lazy: optional extra
        conn = pyodbc.connect(_secret(dsn_secret))
        conn.add_output_converter(-155, _datetimeoffset_from_odbc)
        super().__init__(conn)

    def schema_facts(self, table: str) -> SchemaFacts:
        schema, name = _split_table(table, "dbo")
        facts = SchemaFacts(table=f"{schema}.{name}")
        rows = self._dict_rows(
            table, "sys.indexes",
            "SELECT i.is_primary_key, i.is_unique, i.has_filter, i.name, ic.key_ordinal, c.name "
            "FROM sys.indexes i JOIN sys.objects o ON o.object_id = i.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE s.name = ? AND o.name = ? AND i.index_id > 0 AND ic.is_included_column = 0 "
            "AND i.is_disabled = 0 AND i.is_hypothetical = 0 "
            "ORDER BY i.index_id, ic.key_ordinal", (schema, name))
        by_index: dict[str, list] = {}
        for is_pk, is_unique, has_filter, iname, _ord, col in rows:
            by_index.setdefault(iname, [bool(is_pk), bool(is_unique), bool(has_filter), []])[3].append(col)
        for is_pk, is_unique, has_filter, cols in by_index.values():
            if is_pk:
                facts.primary_key = tuple(cols)
            elif has_filter:
                facts.partial.add(tuple(cols))
            elif is_unique:
                facts.unique.add(tuple(cols))
                facts.unique_nulls_equal.add(tuple(cols))
            else:
                facts.indexes.add(tuple(cols))
        rows = self._dict_rows(
            table, "sys.foreign_keys",
            "SELECT fk.name, pc.name, rs.name + '.' + ro.name, rc.name, "
            "       fk.update_referential_action_desc, fk.delete_referential_action_desc "
            "FROM sys.foreign_keys fk "
            "JOIN sys.objects o ON o.object_id = fk.parent_object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "JOIN sys.objects ro ON ro.object_id = fk.referenced_object_id "
            "JOIN sys.schemas rs ON rs.schema_id = ro.schema_id "
            "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
            "JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id "
            "JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id "
            "WHERE s.name = ? AND o.name = ? AND fk.is_disabled = 0 "
            "ORDER BY fk.name, fkc.constraint_column_id", (schema, name))
        by_fk: dict[str, list] = {}
        for fk_name, col, ref_table, ref_col, on_update, on_delete in rows:
            entry = by_fk.setdefault(fk_name, [[], ref_table, [], (on_update, on_delete)])
            entry[0].append(col)
            entry[2].append(ref_col)
        for cols, ref_table, ref_cols, actions in by_fk.values():
            fk = (tuple(cols), ref_table, tuple(ref_cols))
            facts.foreign_keys.add(fk)
            facts.foreign_key_actions[fk] = tuple(_fk_action(a) for a in actions)
        rows = self._dict_rows(
            table, "sys.columns",
            "SELECT c.name, c.is_nullable, c.is_identity FROM sys.columns c "
            "JOIN sys.objects o ON o.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = ? AND o.name = ?",
            (schema, name))
        for col, nullable, is_identity in rows:
            if not nullable:
                facts.not_null.add(col)
            if is_identity:
                facts.identity_columns.add(col)
        rows = self._dict_rows(
            table, "sys.check_constraints",
            "SELECT cc.definition FROM sys.check_constraints cc "
            "JOIN sys.objects o ON o.object_id = cc.parent_object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE s.name = ? AND o.name = ? AND cc.is_disabled = 0",
            (schema, name))
        facts.check_count = len(rows)
        facts.checks = {definition for (definition,) in rows}
        rows = self._dict_rows(
            table, "sys.triggers",
            "SELECT tr.name, tr.is_instead_of_trigger, te.type_desc "
            "FROM sys.triggers tr "
            "JOIN sys.objects o ON o.object_id = tr.parent_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "LEFT JOIN sys.trigger_events te ON te.object_id = tr.object_id "
            "WHERE s.name = ? AND o.name = ? AND tr.parent_class = 1 AND tr.is_disabled = 0 "
            "ORDER BY tr.name", (schema, name))
        by_trigger: dict[str, tuple] = {}
        for tname, is_instead, type_desc in rows:
            timing, events = by_trigger.setdefault(
                tname, ("instead of" if is_instead else "after", []))
            if type_desc:
                events.append(str(type_desc).lower())
        for tname, (timing, events) in by_trigger.items():
            facts.triggers[tname] = (timing, tuple(sorted(set(events))), "statement")
        _read_grants(facts, lambda: self._dict_rows(
            table, "sys.database_permissions",
            "WITH m(role_id, member_id) AS ("
            "  SELECT role_principal_id, member_principal_id FROM sys.database_role_members "
            "  UNION ALL "
            "  SELECT m.role_id, rm.member_principal_id FROM m "
            "  JOIN sys.database_role_members rm ON rm.role_principal_id = m.member_id), "
            "direct(grantee_id, perm) AS ("
            "  SELECT p.grantee_principal_id, p.permission_name FROM sys.database_permissions p "
            "  JOIN sys.objects o ON o.object_id = p.major_id "
            "  JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "  WHERE p.class = 1 AND s.name = ? AND o.name = ? AND p.state IN ('G', 'W') "
            "  UNION ALL "
            "  SELECT p.grantee_principal_id, p.permission_name FROM sys.database_permissions p "
            "  JOIN sys.schemas s ON s.schema_id = p.major_id "
            "  WHERE p.class = 3 AND s.name = ? AND p.state IN ('G', 'W') "
            "    AND p.permission_name IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES') "
            "  UNION ALL "
            "  SELECT p.grantee_principal_id, p.permission_name FROM sys.database_permissions p "
            "  WHERE p.class = 0 AND p.state IN ('G', 'W') "
            "    AND p.permission_name IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES')), "
            "denied(grantee_id, perm) AS ("
            "  SELECT p.grantee_principal_id, p.permission_name FROM sys.database_permissions p "
            "  JOIN sys.objects o ON o.object_id = p.major_id "
            "  JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "  WHERE p.class = 1 AND s.name = ? AND o.name = ? AND p.state = 'D' "
            "  UNION ALL "
            "  SELECT p.grantee_principal_id, p.permission_name FROM sys.database_permissions p "
            "  JOIN sys.schemas s ON s.schema_id = p.major_id "
            "  WHERE p.class = 3 AND s.name = ? AND p.state = 'D' "
            "  UNION ALL "
            "  SELECT p.grantee_principal_id, p.permission_name FROM sys.database_permissions p "
            "  WHERE p.class = 0 AND p.state = 'D'), "
            "fixed(grantee_id, perm) AS ("
            "  SELECT m.member_id, v.perm FROM m "
            "  JOIN sys.database_principals r ON r.principal_id = m.role_id "
            "  JOIN (VALUES ('db_datareader', 'SELECT'), ('db_datawriter', 'INSERT'), "
            "               ('db_datawriter', 'UPDATE'), ('db_datawriter', 'DELETE')) v(role_name, perm) "
            "    ON v.role_name = r.name), "
            "eff(grantee_id, perm) AS ("
            "  SELECT grantee_id, perm FROM ("
            "    SELECT grantee_id, perm FROM direct "
            "    UNION SELECT m.member_id, d.perm FROM direct d JOIN m ON m.role_id = d.grantee_id "
            "    UNION SELECT grantee_id, perm FROM fixed) g "
            "  EXCEPT "
            "  SELECT grantee_id, perm FROM ("
            "    SELECT grantee_id, perm FROM denied "
            "    UNION SELECT m.member_id, d.perm FROM denied d JOIN m ON m.role_id = d.grantee_id) x) "
            "SELECT DISTINCT dp.name, e.perm FROM eff e "
            "JOIN sys.database_principals dp ON dp.principal_id = e.grantee_id "
            "WHERE dp.principal_id <> (SELECT principal_id FROM sys.schemas WHERE name = ?) "
            "  AND dp.principal_id NOT IN (SELECT member_id FROM m WHERE role_id = DATABASE_PRINCIPAL_ID('db_owner')) "
            "ORDER BY dp.name", (schema, name, schema, schema, name, schema, schema)))
        return facts

    def column_shape(self, table: str) -> list[dict[str, Any]]:
        schema, name = _split_table(table, "dbo")
        rows = self._rows(
            "SELECT c.name, t.name, c.max_length, c.precision, c.scale, c.is_nullable, c.column_id "
            "FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id "
            "JOIN sys.objects o ON o.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = ? AND o.name = ? "
            "ORDER BY c.column_id", (schema, name))
        return [{"name": str(col).lower(), "type": _tsql_type(kind, max_length, precision, scale),
                 "nullable": bool(nullable)}
                for col, kind, max_length, precision, scale, nullable, _ in rows]

    def numeric_columns(self, table: str) -> set[str]:
        schema, name = _split_table(table, "dbo")
        rows = self._rows(
            "SELECT c.name FROM sys.columns c "
            "JOIN sys.types t ON t.user_type_id = c.system_type_id "  # the base type, not an alias
            "JOIN sys.objects o ON o.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE s.name = ? AND o.name = ? AND t.name IN ('tinyint', 'smallint', 'int', "
            "'bigint', 'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real')",
            (schema, name))
        return {col for (col,) in rows}

    def whole_number_columns(self, table: str) -> set[str]:
        schema, name = _split_table(table, "dbo")
        rows = self._rows(
            "SELECT c.name FROM sys.columns c "
            "JOIN sys.types t ON t.user_type_id = c.system_type_id "
            "JOIN sys.objects o ON o.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE s.name = ? AND o.name = ? AND (t.name IN ('tinyint', 'smallint', 'int', 'bigint') "
            "OR (t.name IN ('decimal', 'numeric') AND c.scale = 0))",
            (schema, name))
        return {col for (col,) in rows}

    def identity_state(self, table: str, column: str) -> IdentityState | None:
        schema, name = _split_table(table, "dbo")
        rows = self._dict_rows(
            table, "sys.identity_columns",
            "SELECT CAST(ic.last_value AS BIGINT), CAST(ic.increment_value AS BIGINT), "
            "       CAST(ic.seed_value AS BIGINT) "
            "FROM sys.identity_columns ic JOIN sys.objects o ON o.object_id = ic.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE s.name = ? AND o.name = ? AND ic.name = ?", (schema, name, column))
        if not rows:
            return None
        last, inc, seed = rows[0]
        step = int(inc or 1)
        if last is None:  # identity never used: the next value is the seed
            return IdentityState(int(seed), step) if seed is not None else None
        return IdentityState(int(last) + step, step)

    def delete_evidence_kind(self) -> str:
        return "sqlserver_cdc"

    def evidence_horizon(self, capture: str) -> tuple[bytes | None, bytes | None]:
        lo, hi = self._rows("SELECT sys.fn_cdc_get_min_lsn(?), sys.fn_cdc_get_max_lsn()", (capture,))[0]
        lo = bytes(lo) if lo is not None else None
        hi = bytes(hi) if hi is not None else None
        if lo is None or hi is None or not any(lo):  # all-zero: no such capture instance
            return None, None
        return lo, hi

    def deletes_since(self, capture: str, key_cols: list[str], after: bytes,
                      upto: bytes, where: str | None = None) -> list[DeleteEvent]:
        from .config import validate_identifier
        validate_identifier(capture)
        cols = ", ".join(validate_identifier(c) for c in key_cols)
        rows = self._rows(
            f"SELECT __$start_lsn, DATEDIFF_BIG(MILLISECOND, sys.fn_cdc_map_lsn_to_time(__$start_lsn), "
            f"GETDATE()), {cols} FROM cdc.fn_cdc_get_all_changes_{capture}"
            "(sys.fn_cdc_increment_lsn(?), ?, N'all') WHERE __$operation = 1"
            + (f" AND ({where})" if where else ""),
            (after, upto))
        self.rows_fetched += len(rows)
        return [DeleteEvent(tuple(r[2:]), bytes(r[0]), max(0.0, float(r[1]) / 1000.0)) for r in rows]

class DatabricksSourceAdapter(_SqlAdapterBase):

    paramstyle = "pyformat"
    CATALOG_STATEMENTS = {"schema_facts": 5, "identity_state": 2, "session": 0}

    def __init__(self, http_path: str | None = None):
        super().__init__(_databricks_connect(http_path))

    def schema_facts(self, table: str) -> SchemaFacts:
        parts = table.replace("`", "").split(".")
        if len(parts) != 3:
            raise NotImplementedError(
                f"{type(self).__name__} needs a catalog.schema.table name, got {table!r}")
        return _uc_schema_facts(lambda sql, params: self._rows(sql, params), *parts)

    def column_shape(self, table: str) -> list[dict[str, Any]]:
        parts = table.replace("`", "").split(".")
        if len(parts) != 3:
            raise NotImplementedError(
                f"{type(self).__name__} needs a catalog.schema.table name, got {table!r}")
        catalog, schema, name = parts
        rows = self._rows(
            f"SELECT column_name, full_data_type, is_nullable, ordinal_position "
            f"FROM {quote_ident(catalog, '`')}.information_schema.columns "
            "WHERE table_catalog = %(catalog)s AND table_schema = %(schema)s AND table_name = %(table)s "
            "ORDER BY ordinal_position", {"catalog": catalog, "schema": schema, "table": name})
        return [{"name": str(col).lower(), "type": normalize_type(dtype),
                 "nullable": str(nullable).upper() != "NO"} for col, dtype, nullable, _ in rows]

    def identity_state(self, table: str, column: str) -> IdentityState | None:
        parts = table.replace("`", "").split(".")
        if len(parts) != 3:
            raise NotImplementedError(
                f"{type(self).__name__} needs a catalog.schema.table name, got {table!r}")
        return _uc_identity_state(
            lambda sql, params: self._rows(sql, params), *parts, column)

def _oracle_dsn_parts(secret: str) -> tuple[str, str, str]:
    raw = os.environ.get(secret)
    if not raw:
        from .config import ConfigError
        raise ConfigError(f"source secret {secret} is not set in the environment")
    try:
        return oracle_dsn_parts(raw)
    except ValueError as e:
        from .config import ConfigError
        raise ConfigError(f"source secret {secret}: {e}") from None

class OracleSourceAdapter(_SqlAdapterBase):

    family = "oracle"
    official = True
    paramstyle = "named"  # oracledb binds :N
    max_params = 1000  # Oracle's hard IN-list cap
    snapshot_sql = "SET TRANSACTION READ ONLY"
    snapshot_reset_sql = "COMMIT"
    change_token_sql = None
    integer_digest_sql = "CAST({col} AS NUMBER(38,0))"
    mod_sql = "MOD({x}, {m})"
    square_digest_sql = "CAST({r} * {r} AS NUMBER(38,0))"
    datetime_bound_sql = "TO_TIMESTAMP({lit}, 'YYYY-MM-DD HH24:MI:SS.FF6')"
    datetime_digest_sql = None
    CATALOG_STATEMENTS = {"schema_facts": 7, "identity_state": 1, "session": 1}

    _NUMERIC_TYPES = ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE")
    _IMPLICIT_NOT_NULL_RE = re.compile(r'^"[^"]+"\s+IS\s+NOT\s+NULL$', re.IGNORECASE)

    def __init__(self, dsn_secret: str):
        import oracledb  # lazy: optional extra (oracledb)

        user, password, dsn = _oracle_dsn_parts(dsn_secret)
        conn = oracledb.connect(user=user, password=password, dsn=dsn)

        def _out_type_handler(cursor, name, default_type, size, precision, scale):
            if default_type is oracledb.DB_TYPE_NUMBER:
                return cursor.var(decimal.Decimal, arraysize=cursor.arraysize)
            tz_types = [t for t in (getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", None),
                                    getattr(oracledb, "DB_TYPE_TIMESTAMP_LTZ", None))
                        if t is not None]
            if default_type in tz_types:
                return cursor.var(default_type, arraysize=cursor.arraysize, outconverter=(
                    lambda v: v if v is None or v.tzinfo is not None
                    else v.replace(tzinfo=dt.timezone.utc)))
            return None

        conn.outputtypehandler = _out_type_handler
        conn.cursor().execute("ALTER SESSION SET TIME_ZONE = '+00:00'")
        conn.cursor().execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        super().__init__(conn)
        self._schema: str | None = None

    def _result_names(self, description, requested: Sequence[str] | None = None) -> list[str]:
        names = [str(d[0]) for d in description]
        if not requested:
            return [n.lower() if n.isupper() else n for n in names]
        want: dict[str, str] = {}
        for r in requested:
            want.setdefault(str(r).lower(), str(r))
        return [want.get(n.lower(), n) for n in names]

    def _current_schema(self) -> str:
        if self._schema is None:
            self._schema = str(self._rows(
                "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual")[0][0])
        return self._schema

    def _owner_name(self, table: str) -> tuple[str, str]:
        schema, name = _split_table(table, self._current_schema())
        if '"' not in table:
            schema, name = schema.upper(), name.upper()
        return schema, name

    def column_shape(self, table: str) -> list[dict[str, Any]]:
        owner, name = self._owner_name(table)
        rows = self._dict_rows(
            name, "all_tab_columns",
            "SELECT column_name, data_type, data_length, char_length, char_used, "
            "       data_precision, data_scale, nullable "
            "FROM all_tab_columns WHERE owner = :1 AND table_name = :2 ORDER BY column_id",
            self._params([owner, name]))
        return [{"name": col.lower(),
                 "type": _oracle_type(dtype, precision, scale,
                                      char_length if char_used == "C" else length),
                 "nullable": nullable == "Y"}
                for col, dtype, length, char_length, char_used, precision, scale, nullable
                in rows]

    def numeric_columns(self, table: str) -> set[str]:
        owner, name = self._owner_name(table)
        rows = self._dict_rows(
            name, "all_tab_columns",
            "SELECT column_name FROM all_tab_columns WHERE owner = :1 AND table_name = :2 "
            "AND data_type IN ('NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE')",
            self._params([owner, name]))
        return {col.lower() for (col,) in rows}

    def whole_number_columns(self, table: str) -> set[str]:
        owner, name = self._owner_name(table)
        rows = self._dict_rows(
            name, "all_tab_columns",
            "SELECT column_name FROM all_tab_columns WHERE owner = :1 AND table_name = :2 "
            "AND data_type = 'NUMBER' AND data_scale = 0",
            self._params([owner, name]))
        return {col.lower() for (col,) in rows}

    def schema_facts(self, table: str) -> SchemaFacts:
        owner, name = self._owner_name(table)
        facts = SchemaFacts(table=f"{owner.lower()}.{name.lower()}")
        if not self._rows(
                "SELECT column_name FROM all_tab_columns WHERE owner = :1 AND table_name = :2 "
                "AND ROWNUM = 1", self._params([owner, name])):
            raise DictionaryError(
                f"{table}: table metadata not visible to this principal "
                f"(all_tab_columns has no rows for {owner}.{name})")
        constraints = self._dict_rows(
            name, "all_constraints",
            "SELECT c.constraint_name, c.constraint_type, cc.column_name, cc.position, "
            "       r.owner AS r_owner, r.table_name AS r_table, rc.column_name AS r_column, "
            "       c.delete_rule, c.search_condition_vc "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc "
            "  ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name "
            " AND cc.table_name = c.table_name "
            "LEFT JOIN all_constraints r "
            "  ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name "
            "LEFT JOIN all_cons_columns rc "
            "  ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name "
            " AND rc.table_name = r.table_name AND rc.position = cc.position "
            "WHERE c.owner = :1 AND c.table_name = :2 "
            "  AND c.constraint_type IN ('P', 'U', 'R', 'C') "
            "  AND c.status = 'ENABLED' AND c.validated = 'VALIDATED' "
            "ORDER BY c.constraint_name, cc.position",
            self._params([owner, name]))
        cols_of: dict[str, dict[int, str]] = {}
        ctype_of: dict[str, str] = {}
        ref_of: dict[str, tuple[str, dict[int, str], str]] = {}  # R: rtable, rcols, delete_rule
        for cname, ctype, col, pos, rowner, rtable, rcol, delete_rule, condition in constraints:
            ctype_of[cname] = ctype
            if ctype in ("P", "U"):
                cols_of.setdefault(cname, {})[int(pos)] = col.lower()
            elif ctype == "R":
                cols_of.setdefault(cname, {})[int(pos)] = col.lower()
                target, rcols, _ = ref_of.setdefault(
                    cname, (f"{rowner}.{rtable}".lower(), {}, delete_rule))
                if rcol:
                    rcols[int(pos)] = rcol.lower()
            elif condition and not self._IMPLICIT_NOT_NULL_RE.match(str(condition).strip()):
                facts.checks.add(normalize_sql_text(str(condition)))
        facts.check_count = len(facts.checks)
        for cname, ctype in ctype_of.items():
            cols = tuple(cols_of[cname][p] for p in sorted(cols_of.get(cname, {})))
            if ctype == "P":
                facts.primary_key = cols
            elif ctype == "U":
                facts.unique.add(cols)
            elif ctype == "R":
                target, rcols, delete_rule = ref_of[cname]
                fkey = (cols, target, tuple(rcols[p] for p in sorted(rcols)))
                facts.foreign_keys.add(fkey)
                facts.foreign_key_actions[fkey] = ("no action", _fk_action(delete_rule))

        rows = self._dict_rows(
            name, "all_tab_columns",
            "SELECT column_name, nullable FROM all_tab_columns "
            "WHERE owner = :1 AND table_name = :2",
            self._params([owner, name]))
        facts.not_null = {col.lower() for col, nullable in rows if nullable == "N"}

        rows = self._dict_rows(
            name, "all_indexes",
            "SELECT i.index_name, i.uniqueness, ic.column_name "
            "FROM all_indexes i "
            "JOIN all_ind_columns ic "
            "  ON ic.index_owner = i.owner AND ic.index_name = i.index_name "
            " AND ic.table_owner = i.table_owner AND ic.table_name = i.table_name "
            "WHERE i.table_owner = :1 AND i.table_name = :2 AND i.status = 'VALID' "
            "  AND i.index_type NOT LIKE 'FUNCTION-BASED%' "
            "  AND NOT EXISTS (SELECT 1 FROM all_constraints c "
            "                  WHERE c.index_owner = i.owner AND c.index_name = i.index_name "
            "                  AND c.constraint_type = 'P') "
            "ORDER BY i.index_name, ic.column_position",
            self._params([owner, name]))
        by_index: dict[tuple[str, str], list[str]] = {}
        for idx, uniqueness, col in rows:
            by_index.setdefault((idx, uniqueness), []).append(col.lower())
        for (idx, uniqueness), cols in by_index.items():
            if uniqueness == "UNIQUE":
                facts.unique.add(tuple(cols))
            else:
                facts.indexes.add(tuple(cols))

        rows = self._dict_rows(
            name, "all_ind_expressions",
            "SELECT ic.index_name, ic.column_position, ic.column_name, "
            "       e.column_expression, i.uniqueness "
            "FROM all_ind_columns ic "
            "JOIN all_indexes i "
            "  ON i.owner = ic.index_owner AND i.index_name = ic.index_name "
            " AND i.table_owner = ic.table_owner AND i.table_name = ic.table_name "
            "LEFT JOIN all_ind_expressions e "
            "  ON e.index_owner = ic.index_owner AND e.index_name = ic.index_name "
            " AND e.table_owner = ic.table_owner AND e.table_name = ic.table_name "
            " AND e.column_position = ic.column_position "
            "WHERE i.table_owner = :1 AND i.table_name = :2 "
            "  AND i.index_type LIKE 'FUNCTION-BASED%' AND i.status = 'VALID' "
            "ORDER BY ic.index_name, ic.column_position",
            self._params([owner, name]))
        by_fbi: dict[str, dict[str, Any]] = {}
        for idx, pos, col, expr, uniqueness in rows:
            parts = by_fbi.setdefault(idx, {"unique": uniqueness == "UNIQUE", "parts": {}})
            parts["parts"][int(pos)] = str(expr) if expr else str(col)
        for parts in by_fbi.values():
            text = normalize_sql_text(", ".join(parts["parts"][p] for p in sorted(parts["parts"])))
            if parts["unique"]:
                facts.expression_unique.add(text)
            else:
                facts.expression_indexes.add(text)

        rows = self._dict_rows(
            name, "all_triggers",
            "SELECT trigger_name, trigger_type, triggering_event FROM all_triggers "
            "WHERE table_owner = :1 AND table_name = :2 AND status = 'ENABLED'",
            self._params([owner, name]))
        for tname, ttype, event in rows:
            facts.triggers[str(tname).lower()] = _oracle_trigger_shape(str(ttype), str(event))

        _read_grants(facts, lambda: self._dict_rows(
            name, "all_tab_privs",
            "SELECT grantee, privilege FROM all_tab_privs "
            "WHERE table_schema = :1 AND table_name = :2 AND grantee <> :1",
            self._params([owner, name])))
        facts.grants_effective = False

        rows = self._dict_rows(
            name, "all_tab_identity_cols",
            "SELECT column_name FROM all_tab_identity_cols "
            "WHERE owner = :1 AND table_name = :2",
            self._params([owner, name]))
        facts.identity_columns = {col.lower() for (col,) in rows}
        return facts

    def identity_state(self, table: str, column: str) -> IdentityState | None:
        owner, name = self._owner_name(table)
        rows = self._dict_rows(
            name, "all_tab_identity_cols",
            "SELECT s.last_number, s.increment_by "
            "FROM all_tab_identity_cols ic "
            "JOIN all_sequences s "
            "  ON s.sequence_owner = ic.owner AND s.sequence_name = ic.sequence_name "
            "WHERE ic.owner = :1 AND ic.table_name = :2 AND ic.column_name = :3",
            self._params([owner, name, column.upper() if '"' not in column else column]))
        if not rows:
            return None
        return IdentityState(int(rows[0][0]), int(rows[0][1]))

def _oracle_type(dtype: str, precision, scale, length) -> str:
    kind = str(dtype).lower()
    if kind == "number" and precision is not None:
        return normalize_type(f"number({int(precision)},{int(scale or 0)})")
    if kind == "float" and precision is not None:
        return normalize_type(f"float({int(precision)})")
    if kind in ("varchar2", "char", "nvarchar2", "nchar") and length is not None:
        return normalize_type(f"{kind}({int(length)})")
    return normalize_type(kind)

def _oracle_trigger_shape(trigger_type: str, event: str) -> tuple[str, tuple[str, ...], str]:
    tt = trigger_type.lower()
    timing = "instead of" if "instead of" in tt else "before" if "before" in tt else "after"
    events = tuple(e for e in ("insert", "update", "delete") if e in event.lower())
    return (timing, events, "row" if "each row" in tt else "statement")

SOURCE_ADAPTERS = {
    "oracle": OracleSourceAdapter,
    "sqlserver": SqlServerSourceAdapter,
    "databricks": DatabricksSourceAdapter,
}

def _databricks_sdk_config():
    import databricks.sdk.core as _sdk_core  # lazy: optional extra (databricks-sdk)
    if os.environ.get("DATABRICKS_AUTH_TYPE") == "env-oidc" and not os.environ.get("DATABRICKS_OIDC_TOKEN"):
        audience = os.environ.get("DATABRICKS_DEVIN_AUDIENCE", "databricks")
        os.environ["DATABRICKS_OIDC_TOKEN"] = subprocess.run(["devin-oidc", "token", "--audience", audience],
            check=True, capture_output=True, text=True).stdout.strip()
    return _sdk_core.Config()

def _databricks_credentials_provider():
    return _databricks_sdk_config().authenticate

def _databricks_connect(http_path: str | None = None):
    from databricks import sql  # lazy: optional extra (databricks-sql-connector)
    host = _secret("DATABRICKS_HOST").removeprefix("https://").removeprefix("http://").rstrip("/")
    http_path = http_path or _secret("DATABRICKS_HTTP_PATH")
    return sql.connect(server_hostname=host, http_path=http_path,
        credentials_provider=_databricks_credentials_provider)

class DatabricksTargetAdapter:

    CATALOG_STATEMENTS = {"schema_facts": 6, "identity_state": 2, "session": 0}
    KEY_PROBE_STATEMENTS = 1
    OWN_PRINCIPAL_STATEMENTS = 1
    target_kind = "databricks"

    def __init__(self, http_path: str | None, catalog: str, schema: str):
        self._catalog, self._schema = catalog, schema
        self._prefix = f"{quote_ident(catalog, '`')}.{quote_ident(schema, '`')}."
        self._conn = _databricks_connect(http_path)
        self._sql = _SqlAdapterBase(self._conn)
        self._sql.paramstyle = "pyformat"

    @property
    def statements(self) -> int:
        return self._sql.statements

    @property
    def rows_fetched(self) -> int:
        return self._sql.rows_fetched

    def _bare(self, object: str) -> str:
        parts = [p[1:-1] if len(p) > 1 and p[0] == p[-1] == "`" else p for p in object.split(".")]
        if len(parts) == 1:
            return object
        from .config import ConfigError
        own = f"{self._catalog}.{self._schema}"
        if len(parts) != 3:
            raise ConfigError(f"target object {object!r} must be a bare table name or {own}.<table>")
        if (parts[0].casefold(), parts[1].casefold()) != (self._catalog.casefold(), self._schema.casefold()):
            raise ConfigError(f"target object {object!r} is outside this target's {own}; the recon target "
                              "is the migration catalog and schema, never another one")
        return parts[2]

    def _q(self, object: str) -> str:
        return self._prefix + quote_ident(self._bare(object), "`")

    def schema_facts(self, object: str) -> SchemaFacts:
        return _uc_schema_facts(lambda sql, params: self._sql._rows(sql, params),
                                self._catalog, self._schema, self._bare(object))

    def identity_state(self, object: str, column: str) -> IdentityState | None:
        return _uc_identity_state(
            lambda sql, params: self._sql._rows(sql, params),
            self._catalog, self._schema, self._bare(object), column)

    def column_shape(self, object: str) -> list[dict[str, Any]]:
        cat = quote_ident(self._catalog, '`')
        rows = self._sql._rows(
            f"SELECT c.column_name, c.full_data_type, c.is_nullable, c.ordinal_position "
            f"FROM {cat}.information_schema.columns c "
            f"JOIN {cat}.information_schema.tables t "
            "ON t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema "
            "AND t.table_name = c.table_name "
            "WHERE c.table_catalog = %(catalog)s AND c.table_schema = %(schema)s AND c.table_name = %(table)s "
            f"AND {_UC_PHYSICAL} ORDER BY ordinal_position",
            {"catalog": self._catalog, "schema": self._schema, "table": self._bare(object)})
        return [{"name": str(name).lower(), "type": normalize_type(dtype),
                 "nullable": str(nullable).upper() != "NO"} for name, dtype, nullable, _ in rows]

    def table_kind(self, object: str) -> str | None:
        rows = self._sql._rows(
            f"SELECT t.table_type FROM {quote_ident(self._catalog, '`')}.information_schema.tables t "
            "WHERE t.table_catalog = %(catalog)s AND t.table_schema = %(schema)s AND t.table_name = %(table)s "
            f"AND {_UC_PHYSICAL}",
            {"catalog": self._catalog, "schema": self._schema, "table": self._bare(object)})
        return str(rows[0][0]).upper() if rows else None

    def table_exists(self, object: str) -> bool:
        return self.table_kind(object) is not None

    def current_principal(self) -> str:
        (name,), = self._sql._rows("SELECT current_user()")
        return str(name)

    def key_uniqueness(self, object: str, key_cols: list[str], where: str | None = None) -> tuple[int, int]:
        w = f" WHERE {where}" if where else ""
        cols = ", ".join(quote_ident(c, "`") for c in key_cols)
        (rows, distinct), = self._sql._rows(
            f"SELECT COUNT(*), COUNT(DISTINCT {cols}) FROM {self._q(object)}{w}")
        return int(rows), int(distinct)

    def table_aggregates(self, object: str, columns: list[str], numeric: list[str],
                         where: str | None = None,
                         *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        return self._sql.table_aggregates(self._q(object), columns, numeric, where,
                                          unordered=unordered)

    def target_row_count(self, object: str, where: str | None = None) -> int:
        return self._sql.row_count(self._q(object), where)

    def nested_count(self, object: str, array_path: str, where: str | None = None) -> int:
        w = f" WHERE {where}" if where else ""
        (n,) = self._sql._rows(f"SELECT COALESCE(SUM(size({array_path})), 0) FROM {self._q(object)}{w}")[0]
        return int(n)

    def field_aggregates(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]:
        return self._sql.field_aggregates(self._q(object), field_path, where)

    def column_profile(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]:
        return self._sql.column_profile(self._q(object), field_path, where)

    def sum_probe(self, object: str, field_path: str, where: str | None = None) -> Any:
        return self._sql.sum_probe(self._q(object), field_path, where)

    def null_key_count(self, object: str, key_fields: list[str], where: str | None = None) -> int:
        return self._sql.null_key_count(self._q(object), key_fields, where)

    def fetch_keyed(self, object: str, key_fields: list[str], fields: list[str],
                    where: str | None = None, keys: list[Any] | None = None) -> Iterable[dict[str, Any]]:
        tops = list(dict.fromkeys([k.split(".")[0] for k in key_fields]
                                  + [f.split(".")[0] for f in fields]))
        chunks = [keys[i:i + 500] for i in range(0, len(keys), 500)] if keys is not None else [None]
        for chunk in chunks:
            clauses = [f"({where})"] if where else []
            values = []
            if chunk is not None:
                if len(key_fields) == 1:
                    clauses.append(f"{key_fields[0]} IN ({', '.join(self._sql._placeholders(len(chunk)))})")
                    values.extend(k[0] if isinstance(k, tuple) else k for k in chunk)
                else:
                    groups = []
                    for key in chunk:
                        parts = []
                        for col, value in zip(key_fields, key):
                            parts.append(f"{col} = {self._sql._placeholders(1, len(values))[0]}")
                            values.append(value)
                        groups.append("(" + " AND ".join(parts) + ")")
                    clauses.append("(" + " OR ".join(groups) + ")")
            w = " WHERE " + " AND ".join(clauses) if clauses else ""
            cur = self._sql._execute(f"SELECT {', '.join(tops)} FROM {self._q(object)}{w} "
                                     f"ORDER BY {', '.join(key_fields)}",
                                     self._sql._params(values))
            for row in cur:
                self._sql.rows_fetched += 1
                rec = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(zip(
                    [d[0] for d in cur.description], row))
                yield rec

    def run_query(self, sql: str) -> list[dict[str, Any]]:
        return self._sql.run_query(sql)

class _PostgresBase(_SqlAdapterBase):

    paramstyle = "format"
    datetime_digest_sql = "TRUNC(EXTRACT(EPOCH FROM {col}) * 1000000)"
    watermark_literal_utc_offset = True
    binary_literal_sql = "'\\x{hex}'::bytea"
    CATALOG_STATEMENTS = {"schema_facts": 7, "identity_state": 2, "session": 1}

    def _pg_column_shape(self, schema: str, name: str, physical: bool = False) -> list[dict[str, Any]]:
        kind = f"AND {_PG_PHYSICAL} " if physical else ""
        rows = self._rows(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attnum "
            "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = %s AND c.relname = %s {kind}AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum", (schema, name))
        return [{"name": str(col), "type": normalize_type(dtype), "nullable": not notnull}
                for col, dtype, notnull, _ in rows]

    def open_window(self) -> str:
        self._conn.rollback()
        try:
            import psycopg  # lazy: optional extra
            self._conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            self._conn.read_only = True
            self.isolation = "repeatable_read"
        except Exception:  # noqa: BLE001  a non-psycopg connection object (tests, other drivers)
            self.isolation = "none"
        return self.isolation

    def schema_facts(self, table: str) -> SchemaFacts:
        schema, name = _split_table(table, "public")
        facts = SchemaFacts(table=f"{schema}.{name}")
        rows = self._dict_rows(
            table, "pg_constraint",
            "SELECT con.contype, con.conname, a.attname, "
            "       CASE WHEN con.contype = 'f' THEN rn.nspname || '.' || rc.relname END, "
            "       ra.attname, k.ord, con.confupdtype, con.confdeltype, "
            "       CASE WHEN con.contype = 'c' THEN pg_get_constraintdef(con.oid) END "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE "
            "LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum "
            "LEFT JOIN pg_class rc ON rc.oid = con.confrelid "
            "LEFT JOIN pg_namespace rn ON rn.oid = rc.relnamespace "
            "LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ord) "
            "       ON fk.ord = k.ord "
            "LEFT JOIN pg_attribute ra ON ra.attrelid = con.confrelid AND ra.attnum = fk.attnum "
            "WHERE n.nspname = %s AND c.relname = %s AND con.contype IN ('p', 'u', 'f', 'c') "
            "ORDER BY con.conname, k.ord", (schema, name))
        by_con: dict[str, list] = {}
        for ctype, cname, col, ref_table, ref_col, _ord, on_update, on_delete, definition in rows:
            entry = by_con.setdefault(cname, [ctype, [], ref_table, [], (on_update, on_delete), definition])
            if col is not None:
                entry[1].append(col)
            if ref_col is not None:
                entry[3].append(ref_col)
        for ctype, cols, ref_table, ref_cols, actions, definition in by_con.values():
            if ctype == "p":
                facts.primary_key = tuple(cols)
            elif ctype == "u":
                facts.unique.add(tuple(cols))
            elif ctype == "f":
                fk = (tuple(cols), ref_table, tuple(ref_cols))
                facts.foreign_keys.add(fk)
                facts.foreign_key_actions[fk] = tuple(_fk_action(a) for a in actions)
            elif ctype == "c":
                facts.check_count += 1
                facts.checks.add(definition)
        nulls_equal = "ix.indnullsnotdistinct" if self._server_version() >= 150000 else "FALSE"
        rows = self._dict_rows(
            table, "pg_index",
            "SELECT ix.indexrelid, ix.indisunique, ix.indpred IS NOT NULL, ix.indexprs IS NOT NULL, "
            f"a.attname, k.ord, pg_get_indexdef(ix.indexrelid), {nulls_equal} "
            "FROM pg_index ix JOIN pg_class c ON c.oid = ix.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON k.ord <= ix.indnkeyatts "
            "LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum AND k.attnum > 0 "
            "WHERE n.nspname = %s AND c.relname = %s AND NOT ix.indisprimary "
            "AND ix.indisvalid AND ix.indisready AND ix.indislive "
            "ORDER BY ix.indexrelid, k.ord", (schema, name))
        by_index: dict[Any, dict[str, Any]] = {}
        for indexrelid, is_unique, is_partial, has_exprs, col, _ord, indexdef, nulls_eq in rows:
            entry = by_index.setdefault(indexrelid, {
                "cols": [], "unique": bool(is_unique), "partial": bool(is_partial),
                "expr": bool(has_exprs), "def": indexdef, "nulls_equal": bool(nulls_eq)})
            entry["cols"].append(col)
        for entry in by_index.values():
            if entry["expr"]:
                text = _index_key_text(entry["def"])
                if entry["unique"] and not entry["partial"]:
                    facts.expression_unique.add(text)
                else:  # a partial unique expression is an access path, not full uniqueness
                    facts.expression_indexes.add(text)
            elif entry["partial"]:
                facts.partial.add(tuple(entry["cols"]))
            elif entry["unique"]:
                facts.unique.add(tuple(entry["cols"]))
                if entry["nulls_equal"]:
                    facts.unique_nulls_equal.add(tuple(entry["cols"]))
            else:
                facts.indexes.add(tuple(entry["cols"]))
        rows = self._dict_rows(
            table, "pg_attribute",
            "SELECT a.attname, a.attnotnull, a.attidentity <> '' OR "
            "       pg_get_serial_sequence(%s, a.attname) IS NOT NULL, c.relkind "
            "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (f'"{schema}"."{name}"', schema, name))
        for col, notnull, has_seq, relkind in rows:
            if relkind in ("v", "m"):
                facts.declares_not_null = False
            elif notnull:
                facts.not_null.add(col)
            if has_seq:
                facts.identity_columns.add(col)
        rows = self._dict_rows(
            table, "pg_trigger",
            "SELECT t.tgname, t.tgtype::int "
            "FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal AND t.tgenabled <> 'D' "
            "ORDER BY t.tgname", (schema, name))
        for tname, tgtype in rows:
            facts.triggers[tname] = _pg_trigger_shape(int(tgtype))
        inherit = "am.inherit_option" if self._server_version() >= 160000 else "u.rolinherit"
        _read_grants(facts, lambda: self._in_savepoint(lambda: self._dict_rows(
            table, "information_schema.table_privileges",
            ("WITH RECURSIVE m(role, member, platform) AS ("
            "  SELECT r.rolname, u.rolname, r.rolsuper OR r.rolname LIKE 'databricks\\_%%' "
            "  FROM pg_auth_members am "
            "  JOIN pg_roles r ON r.oid = am.roleid JOIN pg_roles u ON u.oid = am.member "
            "  WHERE {inherit} "
            "  UNION "
            "  SELECT m.role, u.rolname, m.platform OR r.rolsuper OR r.rolname LIKE 'databricks\\_%%' "
            "  FROM m "
            "  JOIN pg_roles r ON r.rolname = m.member "
            "  JOIN pg_auth_members am ON am.roleid = r.oid JOIN pg_roles u ON u.oid = am.member "
            "  WHERE {inherit}), "
            "rel AS ("
            "  SELECT pg_get_userbyid(c.relowner) AS owner FROM pg_class c "
            "  JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s), "
            "direct(grantee, priv) AS ("
            "  SELECT tp.grantee, tp.privilege_type FROM information_schema.table_privileges tp "
            "  WHERE tp.table_schema = %s AND tp.table_name = %s), "
            "eff(grantee, priv) AS ("
            "  SELECT grantee, priv FROM direct "
            "  UNION SELECT m.member, d.priv FROM direct d JOIN m ON m.role = d.grantee AND NOT m.platform "
            "  UNION SELECT m.member, v.priv FROM m "
            "  JOIN (VALUES ('pg_read_all_data', 'SELECT'), ('pg_write_all_data', 'INSERT'), "
            "               ('pg_write_all_data', 'UPDATE'), ('pg_write_all_data', 'DELETE')) v(role, priv) "
            "    ON v.role = m.role WHERE NOT m.platform) "
            "SELECT e.grantee, e.priv FROM eff e, rel "
            "WHERE e.grantee <> rel.owner "
            "  AND e.grantee NOT IN (SELECT member FROM m WHERE role = rel.owner) "
            "  AND NOT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname = e.grantee "
            "                  AND (r.rolsuper OR r.rolname LIKE 'databricks\\_%%')) "
            "ORDER BY 1, 2").replace("{inherit}", inherit), (schema, name, schema, name))))
        return facts

    def _in_savepoint(self, read):
        self._execute("SAVEPOINT recon_dictionary")
        try:
            rows = read()
        except Exception:
            self._execute("ROLLBACK TO SAVEPOINT recon_dictionary")
            self._execute("RELEASE SAVEPOINT recon_dictionary")
            raise
        self._execute("RELEASE SAVEPOINT recon_dictionary")
        return rows

    ARRAY_EXCLUSION_CAPACITY = 1_000_000

    def exclusion_capacity(self, key_width: int) -> int:
        return self.ARRAY_EXCLUSION_CAPACITY

    def _column_types(self, table: str) -> dict[str, str]:
        schema, name = _split_table(table, "public")
        rows = self._rows(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
            "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (schema, name))
        return {col.lower(): typ for col, typ in rows}

    def table_aggregates_excluding(self, table: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None,
                                   *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        if not exclude_keys:
            return super().table_aggregates_excluding(table, columns, numeric, key_cols, [],
                                                      where, unordered=unordered)
        if len(exclude_keys) > self.exclusion_capacity(len(key_cols)):
            raise ValueError(f"{len(exclude_keys)} keys exceed the "
                             f"{self.ARRAY_EXCLUSION_CAPACITY}-key exclusion budget")
        types = self._column_types(table)
        casts = []
        for col in key_cols:
            typ = types.get(col.strip('"').lower())
            if typ is None:
                raise ValueError(f"{table}: key column {col} is not in the catalog")
            casts.append(typ)
        values: list[Any] = [[_key_text(k[i]) for k in exclude_keys] for i in range(len(key_cols))]
        arrays = [f"{p}::text[]" for p in self._placeholders(len(key_cols))]
        names = ", ".join(f"_k{i}" for i in range(len(key_cols)))
        match = " AND ".join(f"_x._k{i}::{typ} = {col}"
                             for i, (col, typ) in enumerate(zip(key_cols, casts)))
        clauses = [f"({where})"] if where else []
        clauses.append(f"NOT EXISTS (SELECT 1 FROM unnest({', '.join(arrays)}) AS _x({names}) "
                       f"WHERE {match})")
        return self._table_aggregates(table, columns, numeric, " WHERE " + " AND ".join(clauses),
                                      values, unordered)

    def _server_version(self) -> int:
        if not hasattr(self, "_server_version_num"):
            (v,) = self._rows("SELECT current_setting('server_version_num')")[0]
            self._server_version_num = int(v)
        return self._server_version_num

    def numeric_columns(self, table: str) -> set[str]:
        schema, name = _split_table(table, "public")
        rows = self._rows(
            "WITH RECURSIVE col_type AS ("
            "  SELECT a.attname, t.typname, t.typtype, t.typbasetype "
            "  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "  JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_type t ON t.oid = a.atttypid "
            "  WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped "
            "  UNION ALL "
            "  SELECT ct.attname, t.typname, t.typtype, t.typbasetype "
            "  FROM col_type ct JOIN pg_type t ON t.oid = ct.typbasetype WHERE ct.typtype = 'd') "
            "SELECT attname FROM col_type WHERE typtype <> 'd' "
            "AND typname IN ('int2', 'int4', 'int8', 'numeric', 'float4', 'float8', 'money')",
            (schema, name))
        return {col for (col,) in rows}

    def whole_number_columns(self, table: str) -> set[str]:
        schema, name = _split_table(table, "public")
        rows = self._rows(
            "WITH RECURSIVE col_type AS ("
            "  SELECT a.attname, t.typname, t.typtype, t.typbasetype, a.atttypmod AS typmod, t.typtypmod "
            "  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "  JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_type t ON t.oid = a.atttypid "
            "  WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped "
            "  UNION ALL "
            "  SELECT ct.attname, t.typname, t.typtype, t.typbasetype, "
            "         CASE WHEN ct.typmod <> -1 THEN ct.typmod ELSE ct.typtypmod END, t.typtypmod "
            "  FROM col_type ct JOIN pg_type t ON t.oid = ct.typbasetype WHERE ct.typtype = 'd') "
            "SELECT attname FROM col_type WHERE typtype <> 'd' AND (typname IN ('int2', 'int4', 'int8') "
            "OR (typname = 'numeric' AND typmod <> -1 AND ((typmod - 4) & 65535) = 0))",
            (schema, name))
        return {col for (col,) in rows}

    def identity_state(self, table: str, column: str) -> IdentityState | None:
        schema, name = _split_table(table, "public")
        (seq,) = self._dict_rows(table, "pg_get_serial_sequence",
                                 "SELECT pg_get_serial_sequence(%s, %s)",
                                 (f'"{schema}"."{name}"', column))[0]
        if seq is None:
            return None
        (last, is_called, inc) = self._dict_rows(
            table, "pg_sequence",
            f"SELECT s.last_value, s.is_called, p.seqincrement FROM {seq} s, "
            "pg_sequence p WHERE p.seqrelid = %s::regclass", (seq,))[0]
        step = int(inc)
        return IdentityState(int(last) + step if is_called else int(last), step)

class PostgresSourceAdapter(_PostgresBase):

    def __init__(self, dsn_secret: str):
        import psycopg  # lazy: optional extra
        super().__init__(psycopg.connect(_secret(dsn_secret)))

    def column_shape(self, table: str) -> list[dict[str, Any]]:
        return self._pg_column_shape(*_split_table(table, "public"))

SOURCE_ADAPTERS["postgres"] = PostgresSourceAdapter

class TargetIdentityError(RuntimeError):
    pass

class LakebaseTargetAdapter(_PostgresBase):
    target_kind = "lakebase"
    """Target side for the operational track: one schema inside a Lakebase branch database.
    Secret value: the branch endpoint's libpq DSN (OAuth token as the password, minted by
    `databricks postgres` for the migration principal; never the production branch). The DSN
    decides which database the session lands in, so the connection is bound to `database`, the
    allowlisted identity, before any statement runs; a DSN pointing elsewhere is refused. Object
    names in the mapping spec are bare table names, qualified here with the schema."""

    def __init__(self, secret_name: str, database: str, schema: str):
        import psycopg  # lazy: optional extra
        self._schema = schema
        self._schema_q = quote_ident(schema, '"')  # before connecting: a bad name leaks no session
        super().__init__(psycopg.connect(_secret(secret_name)))
        self.database = self._bind_database(database)

    def _bind_database(self, expected: str) -> str:
        (actual,) = self._rows("SELECT current_database()")[0]
        if actual != expected:
            self._conn.close()
            raise TargetIdentityError(
                f"target DSN connects to database {actual!r}, but the allowlisted target is "
                f"{expected!r}; point --target-catalog at the connected database or fix the DSN")
        return actual

    def _q(self, object: str) -> str:
        return f'{self._schema_q}.{quote_ident(object, chr(34))}'

    def target_row_count(self, object: str, where: str | None = None) -> int:
        return self.row_count(self._q(object), where)

    def nested_count(self, object: str, array_path: str, where: str | None = None) -> int:
        from .config import ConfigError
        raise ConfigError(f"{object}.{array_path}: embedded arrays have no Lakebase shape; "
                          "map the child table as its own object")

    def field_aggregates(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]:
        return super().field_aggregates(self._q(object), field_path, where)

    def column_profile(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]:
        return super().column_profile(self._q(object), field_path, where)

    def table_aggregates(self, object: str, columns: list[str], numeric: list[str],
                         where: str | None = None,
                         *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        return super().table_aggregates(self._q(object), columns, numeric, where,
                                        unordered=unordered)

    def table_aggregates_excluding(self, object: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None,
                                   *, unordered: Sequence[str] = ()) -> dict[str, dict[str, Any]]:
        return super().table_aggregates_excluding(self._q(object), columns, numeric, key_cols,
                                                  exclude_keys, where, unordered=unordered)

    def fetch_keyed(self, object: str, key_fields: list[str], fields: list[str],
                    where: str | None = None, keys: list[Any] | None = None) -> Iterable[dict[str, Any]]:
        return super().fetch_keyed(self._q(object), key_fields, fields, where, keys)

    def window_marker(self, object: str, key_cols: list[str], watermark: str | None,
                      where: str | None = None) -> tuple:
        return super().window_marker(self._q(object), key_cols, watermark, where)

    def range_fingerprints(self, object: str, key_cols: list[str], key_kinds: list[str],
                           watermark: str | None, wm_kind: str | None, ranges,
                           where: str | None = None) -> list[tuple[int, tuple | None, Any]]:
        return super().range_fingerprints(self._q(object), key_cols, key_kinds, watermark,
                                          wm_kind, ranges, where)

    def keys_in_range(self, object: str, key_cols: list[str], lo, hi, where: str | None = None,
                      extra_cols: list[str] | None = None) -> list[tuple]:
        return super().keys_in_range(self._q(object), key_cols, lo, hi, where, extra_cols)

    def max_watermark(self, object: str, watermark: str, where: str | None = None) -> Any:
        return super().max_watermark(self._q(object), watermark, where)

    def schema_facts(self, object: str) -> SchemaFacts:
        return super().schema_facts(self._q(object))

    def numeric_columns(self, object: str) -> set[str]:
        return super().numeric_columns(self._q(object))

    def whole_number_columns(self, object: str) -> set[str]:
        return super().whole_number_columns(self._q(object))

    def identity_state(self, object: str, column: str) -> IdentityState | None:
        return super().identity_state(self._q(object), column)

    def column_shape(self, object: str) -> list[dict[str, Any]]:
        return self._pg_column_shape(self._schema, object, physical=True)

    def table_exists(self, object: str) -> bool:
        rows = self._rows(
            "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = %s AND c.relname = %s AND {_PG_PHYSICAL}", (self._schema, object))
        return bool(rows)

    def null_key_count(self, object: str, key_fields: list[str], where: str | None = None) -> int:
        return super().null_key_count(self._q(object), key_fields, where)

    def applied_position(self, table: str, column: str, where: str | None) -> bytes | int | None:
        w = f" WHERE {where}" if where else ""
        col = quote_ident(column, '"')
        rows = self._rows(f"SELECT {col} FROM {self._q(table)}{w} ORDER BY {col} DESC NULLS LAST LIMIT 1")
        value = rows[0][0] if rows else None
        if value is None or isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        from .config import ConfigError
        raise ConfigError(f"{table}.{column}: applied position must be bytea or an integer, "
                          f"got {type(value).__name__}")

TARGET_ADAPTERS = {"databricks": DatabricksTargetAdapter, "lakebase": LakebaseTargetAdapter}
