"""Source and target adapters.

Tiers 1-3 talk only to these two interfaces, so an in-memory fake (tests) or a Lakebridge
reconcile wrapper (future) can plug in without touching tier logic. Aggregates are computed
natively on each side (SQL on the source warehouse, SQL on Databricks) so no bulk data
crosses the wire. Drivers are imported lazily; install only the extras you need.

Connection secrets are read from environment variables BY NAME; the harness never accepts
a literal connection string or token on the CLI.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from .paths import get_path


@dataclass(frozen=True)
class Stratum:
    """One key range of a table: n rows whose full composite key is in [lo, hi] (tuples ordered
    lexicographically over every key column, so strata sharing a first-column value stay disjoint)."""
    bucket: int
    lo: tuple
    hi: tuple
    n: int


def _as_key(value: Any) -> tuple:
    return value if isinstance(value, tuple) else (value,)


def _digest_value(value: Any) -> Any:
    """Normalise an engine's SUM result so equal digests compare equal across drivers
    (Decimal('5.000000') vs int 5 vs float 5.0)."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    return value


@dataclass
class SchemaFacts:
    """Constraint, index and identity shape of one table, in the engine's own column names.
    Column tuples are ordered; unique/index sets hold leading-column tuples."""
    primary_key: tuple[str, ...] = ()
    unique: set[tuple[str, ...]] = field(default_factory=set)
    foreign_keys: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = field(default_factory=set)
    not_null: set[str] = field(default_factory=set)
    indexes: set[tuple[str, ...]] = field(default_factory=set)
    check_count: int = 0
    identity_columns: set[str] = field(default_factory=set)


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
    """One statement per table for Tier 2 (the cost rule: one multi-metric statement, not one per
    metric). `numeric` names the columns that also get a SUM."""
    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]: ...


@runtime_checkable
class StratifiedKeys(Protocol):
    """Server-side key stratification for Tier 3 sampling: the source computes n key ranges and
    returns chosen keys per range, so the harness never streams the whole key column."""
    def key_strata(self, table: str, key_cols: list[str], n_strata: int,
                   where: str | None = None) -> list[Stratum]: ...
    def sample_keys(self, table: str, key_cols: list[str], lo: Any, hi: Any,
                    row_numbers: list[int], where: str | None = None) -> list[tuple]: ...
    def duplicate_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int: ...


# How a side proves the rows it served did not change between open_window and close_window:
#   snapshot      - the engine pinned one snapshot for the whole run (SNAPSHOT / REPEATABLE READ)
#   change_token  - plain reads, but the markers carry an engine counter that moves on every
#                   write to the table (SQL Server sys.dm_db_index_usage_stats.user_updates)
#   markers       - plain reads and only (COUNT, MAX(watermark)) to compare; a write below the
#                   max or a balanced insert+delete is invisible, so a run on this footing is
#                   not merge-eligible unless the tolerances record that decision
WINDOW_STRENGTHS = ("snapshot", "change_token", "markers")


@runtime_checkable
class TransactionalSide(Protocol):
    """What --mode transactional needs from a live side. Both sides implement it; the tiers
    compare the two. `window_marker` is read at open and close to prove the window held;
    `window_strength` says how much that proof is worth (see WINDOW_STRENGTHS)."""
    def open_window(self) -> str: ...
    def close_window(self) -> None: ...
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
    def schema_facts(self, table: str) -> SchemaFacts: ...
    def identity_next(self, table: str, column: str) -> int | None: ...


@runtime_checkable
class StatementCounting(Protocol):
    """Adapters that count what they cost: statements issued and rows pulled across the wire."""
    statements: int
    rows_fetched: int


def _secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"secret '{name}' not found in environment; pass secrets by name only")
    return value


AGG_SQL = ("SELECT COUNT(*) AS n, COUNT({col}) AS nonnull, MIN({col}) AS mn, "
           "MAX({col}) AS mx, COUNT(DISTINCT {col}) AS dc FROM {table}{where}")


# Bucket expression assigning each row to one of {n} equal-count strata ordered by key.
# ANSI NTILE everywhere except Teradata, which only has QUANTILE (0-based).
NTILE_SQL = "NTILE({n}) OVER (ORDER BY {order})"
TERADATA_QUANTILE_SQL = "QUANTILE({n}, {order}) + 1"


class _SqlAdapterBase:
    """Shared SQL implementation; subclasses provide a DB-API connection."""

    paramstyle = "qmark"
    bucket_sql = NTILE_SQL
    # Statement that pins a repeatable snapshot for the rest of the transaction; None when the
    # engine has none we can rely on (the window markers then carry the proof alone).
    snapshot_sql: str | None = None
    # Statement that undoes snapshot_sql when the engine rejects it at the first table read
    # (the level is session-scoped on SQL Server, so a rollback alone leaves it in force).
    snapshot_reset_sql: str | None = None

    # Cumulative per-table write counter read alongside the markers when no snapshot is pinned
    # ({table} is formatted in); None when the engine exposes none.
    change_token_sql: str | None = None
    # Portable digests for range fingerprints: exact decimal sums compare equal across engines.
    number_digest_sql = "CAST({col} AS DECIMAL(38,6))"
    datetime_digest_sql: str | None = None  # whole microseconds since the epoch

    def __init__(self, conn):
        self._conn = conn
        self.statements = 0
        self.rows_fetched = 0
        self.isolation = "none"
        self._token_ok: bool | None = None

    def _execute(self, sql: str, params=()):
        cur = self._conn.cursor()
        cur.execute(sql, params)
        self.statements += 1
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

    def run_query(self, sql: str) -> list[dict[str, Any]]:
        if not sql.lstrip().lower().startswith(("select", "with")):
            from .config import ConfigError
            raise ConfigError("recorded SQL ops must be read-only SELECT or WITH queries")
        cur = self._execute(sql)
        names = [d[0] for d in cur.description or []]
        rows = cur.fetchall()
        self.rows_fetched += len(rows)
        return [dict(zip(names, row)) for row in rows]

    def row_count(self, table: str, where: str | None = None) -> int:
        w = f" WHERE {where}" if where else ""
        return int(self._rows(f"SELECT COUNT(*) FROM {table}{w}")[0][0])

    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]:
        w = f" WHERE {where}" if where else ""
        n, nonnull, mn, mx, dc = self._rows(AGG_SQL.format(col=column, table=table, where=w))[0]
        out = {"count": int(n), "null_rate": (int(n) - int(nonnull)) / int(n) if n else 0.0,
               "min": mn, "max": mx, "distinct_count": int(dc)}
        try:
            (s,) = self._rows(f"SELECT SUM({column}) FROM {table}{w}")[0]
            out["sum"] = s
        except Exception:  # noqa: BLE001  driver-specific error type: SUM on a non-numeric column
            out["sum"] = None
            if hasattr(self._conn, "rollback"):
                self._conn.rollback()  # libpq leaves the transaction aborted otherwise
        return out

    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]:
        w = f" WHERE {where}" if where else ""
        exprs = ["COUNT(*)"]
        for col in columns:
            exprs += [f"COUNT({col})", f"MIN({col})", f"MAX({col})", f"COUNT(DISTINCT {col})"]
            if col in numeric:
                exprs.append(f"SUM({col})")
        row = list(self._rows(f"SELECT {', '.join(exprs)} FROM {table}{w}")[0])
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
                if len(key_cols) == 1:
                    clauses.append(f"{key_cols[0]} IN ({', '.join(self._placeholders(len(chunk)))})")
                    values.extend(k[0] if isinstance(k, tuple) else k for k in chunk)
                else:
                    for key in chunk:
                        parts = []
                        for col, value in zip(key_cols, key):
                            parts.append(f"{col} = {self._placeholders(1, len(values))[0]}")
                            values.append(value)
                        clauses.append("(" + " AND ".join(parts) + ")")
                    clauses[-len(chunk):] = ["(" + " OR ".join(clauses[-len(chunk):]) + ")"]
            w = " WHERE " + " AND ".join(clauses) if clauses else ""
            cur = self._execute(f"SELECT {cols} FROM {table}{w} ORDER BY {', '.join(key_cols)}",
                                self._params(values))
            names = [d[0] for d in cur.description]
            for row in cur:
                self.rows_fetched += 1
                yield dict(zip(names, row))

    def iter_keys(self, table: str, key_cols: list[str], where: str | None = None) -> Iterable[tuple]:
        w = f" WHERE {where}" if where else ""
        cur = self._execute(f"SELECT {', '.join(key_cols)} FROM {table}{w} ORDER BY {', '.join(key_cols)}")
        for row in cur:
            self.rows_fetched += 1
            yield tuple(row)

    def key_strata(self, table: str, key_cols: list[str], n_strata: int,
                   where: str | None = None) -> list[Stratum]:
        """n equal-count key ranges, computed server-side in one statement. Bounds are full composite
        keys: MIN/MAX for a single key column, the first and last row per bucket otherwise."""
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
        """Lexicographic `(k1, k2, ...) op (v1, v2, ...)` for op in {>=, <=}, spelled without row-value
        constructors so every dialect accepts it and the first-column range stays prunable."""
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
        """Keys at the given 1-based positions within the composite key range [lo, hi]: ROW_NUMBER
        over the range, filtered by position, so only the chosen keys cross the wire."""
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

    # ---- transactional mode -----------------------------------------------------------

    def open_window(self) -> str:
        """Best effort: pin a snapshot for the run. Engines that refuse (SQL Server without
        ALLOW_SNAPSHOT_ISOLATION, Sybase) fall back to plain reads; the markers still decide."""
        if hasattr(self._conn, "rollback"):
            try:
                self._conn.rollback()
            except Exception:  # noqa: BLE001  nothing to roll back on some drivers
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

    def window_strength(self) -> str:
        if self.isolation in ("snapshot", "repeatable_read"):
            return "snapshot"
        return "change_token" if self._token_ok else "markers"

    def _change_token(self, table: str) -> Any:
        """Engine write counter for the table, or None when the engine has none or the login
        cannot read it; both cases leave the window on markers alone."""
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
        try:
            row = self._rows(sql)[0]
        except Exception:  # noqa: BLE001  driver-specific error type
            if self.isolation != "snapshot":
                raise
            # SQL Server accepts SET ... SNAPSHOT and only fails on the first table read when the
            # database has ALLOW_SNAPSHOT_ISOLATION off; drop to plain reads, markers still decide.
            self._conn.rollback()
            if self.snapshot_reset_sql:
                self._execute(self.snapshot_reset_sql)
            self.isolation = "none"
            row = self._rows(sql)[0]
        marker = tuple(row)
        if self.isolation not in ("snapshot", "repeatable_read"):
            marker += (self._change_token(table),)
        return marker

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

    def _digest_sql(self, col: str, kind: str) -> str | None:
        if kind == "number":
            return self.number_digest_sql.format(col=col)
        if kind == "datetime" and self.datetime_digest_sql:
            return self.datetime_digest_sql.format(col=col)
        return None

    def range_fingerprints(self, table: str, key_cols: list[str], key_kinds: list[str],
                           watermark: str | None, wm_kind: str | None,
                           ranges: list[tuple[tuple | None, tuple | None]],
                           where: str | None = None) -> list[tuple[int, tuple | None, Any]]:
        """Per key range: (row count, per-key-column sums, watermark sum) in one statement (SUM
        over CASE). Sums are exact decimals so a key swapped for another or one row's watermark
        moved inside a range changes the fingerprint even when the count does not. A digest is
        None when the column kind has no portable sum (strings, uuids)."""
        if not ranges:
            return []
        key_digests = [self._digest_sql(k, kind) for k, kind in zip(key_cols, key_kinds)]
        key_digestible = all(d is not None for d in key_digests)
        wm_digest = self._digest_sql(watermark, wm_kind) if watermark and wm_kind else None
        exprs, values = [], []
        for lo, hi in ranges:
            # each CASE binds its own copy of the bounds: positional drivers cannot reuse them
            terms = ["1"] + (key_digests if key_digestible else []) + ([wm_digest] if wm_digest else [])
            for term in terms:
                pred, vals = self._range_predicate(key_cols, lo, hi, len(values))
                values += vals
                exprs.append(f"SUM(CASE WHEN {pred} THEN {term} ELSE 0 END)")
        w = f" WHERE {where}" if where else ""
        row = list(self._rows(f"SELECT {', '.join(exprs)} FROM {table}{w}", self._params(values))[0])
        out: list[tuple[int, tuple | None, Any]] = []
        for _ in ranges:
            n = int(row.pop(0) or 0)
            keys = None
            if key_digestible:
                keys = tuple(_digest_value(row.pop(0)) for _ in key_cols)
            wm = _digest_value(row.pop(0)) if wm_digest else None
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

    def identity_next(self, table: str, column: str) -> int | None:
        raise NotImplementedError(f"{type(self).__name__} cannot read identity state")


# ---- Source warehouses ------------------------------------------------------------------

class RedshiftSourceAdapter(_SqlAdapterBase):
    """Secret value: a libpq DSN, e.g. postgresql://user:pw@host:5439/db (read-only user)."""

    paramstyle = "format"
    def __init__(self, dsn_secret: str):
        import psycopg2  # lazy: optional extra
        super().__init__(psycopg2.connect(_secret(dsn_secret)))


class SnowflakeSourceAdapter(_SqlAdapterBase):
    """Secret value: JSON with account, user, password, warehouse, database, schema, role."""

    paramstyle = "format"
    def __init__(self, dsn_secret: str):
        import snowflake.connector  # lazy: optional extra
        super().__init__(snowflake.connector.connect(**json.loads(_secret(dsn_secret))))


class TeradataSourceAdapter(_SqlAdapterBase):
    """Secret value: JSON accepted by teradatasql.connect (host, user, password, ...)."""

    bucket_sql = TERADATA_QUANTILE_SQL

    def __init__(self, dsn_secret: str):
        import teradatasql  # lazy: optional extra
        super().__init__(teradatasql.connect(_secret(dsn_secret)))


class OracleSourceAdapter(_SqlAdapterBase):
    """Secret value: user/password/dsn."""

    paramstyle = "named"
    def __init__(self, dsn_secret: str):
        import oracledb  # lazy: optional extra
        user, password, dsn = _secret(dsn_secret).split("/", 2)
        super().__init__(oracledb.connect(user=user, password=password, dsn=dsn))


def _split_table(table: str, default_schema: str | None) -> tuple[str | None, str]:
    parts = table.replace("[", "").replace("]", "").replace('"', "").split(".")
    return (parts[-2] if len(parts) > 1 else default_schema), parts[-1]


class SqlServerSourceAdapter(_SqlAdapterBase):
    """Secret value: an ODBC connection string. Also the Sybase ASE stand-in for the OLTP track
    (same T-SQL catalog shape through sys.* views on SQL Server; ASE itself has no snapshot
    isolation and no usage-stats DMV, so there the window rests on markers alone)."""

    snapshot_sql = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
    snapshot_reset_sql = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    # Every INSERT/UPDATE/DELETE statement against the table bumps user_updates, committed or
    # not, so a window whose token held saw no write at all. Needs VIEW SERVER STATE (2019) /
    # VIEW DATABASE PERFORMANCE STATE (2022+) on the read-only login; without it the token is
    # unreadable and the window is markers-only.
    change_token_sql = ("SELECT ISNULL(SUM(user_updates), 0) FROM sys.dm_db_index_usage_stats "
                        "WHERE database_id = DB_ID() AND object_id = OBJECT_ID('{table}')")
    datetime_digest_sql = "CAST(DATEDIFF_BIG(MICROSECOND, '19700101', {col}) AS DECIMAL(38,0))"

    def __init__(self, dsn_secret: str):
        import pyodbc  # lazy: optional extra
        super().__init__(pyodbc.connect(_secret(dsn_secret)))

    def schema_facts(self, table: str) -> SchemaFacts:
        schema, name = _split_table(table, "dbo")
        facts = SchemaFacts()
        rows = self._rows(
            "SELECT i.is_primary_key, i.is_unique, i.name, ic.key_ordinal, c.name "
            "FROM sys.indexes i JOIN sys.objects o ON o.object_id = i.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE s.name = ? AND o.name = ? AND i.index_id > 0 AND ic.is_included_column = 0 "
            "ORDER BY i.index_id, ic.key_ordinal", (schema, name))
        by_index: dict[str, list] = {}
        for is_pk, is_unique, iname, _ord, col in rows:
            by_index.setdefault(iname, [bool(is_pk), bool(is_unique), []])[2].append(col)
        for is_pk, is_unique, cols in by_index.values():
            if is_pk:
                facts.primary_key = tuple(cols)
            elif is_unique:
                facts.unique.add(tuple(cols))
            else:
                facts.indexes.add(tuple(cols))
        rows = self._rows(
            "SELECT fk.name, pc.name, rs.name + '.' + ro.name, rc.name "
            "FROM sys.foreign_keys fk "
            "JOIN sys.objects o ON o.object_id = fk.parent_object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "JOIN sys.objects ro ON ro.object_id = fk.referenced_object_id "
            "JOIN sys.schemas rs ON rs.schema_id = ro.schema_id "
            "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
            "JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id "
            "JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id "
            "WHERE s.name = ? AND o.name = ? ORDER BY fk.name, fkc.constraint_column_id", (schema, name))
        by_fk: dict[str, list] = {}
        for fk_name, col, ref_table, ref_col in rows:
            entry = by_fk.setdefault(fk_name, [[], ref_table, []])
            entry[0].append(col)
            entry[2].append(ref_col)
        for cols, ref_table, ref_cols in by_fk.values():
            facts.foreign_keys.add((tuple(cols), ref_table, tuple(ref_cols)))
        rows = self._rows(
            "SELECT c.name, c.is_nullable, c.is_identity FROM sys.columns c "
            "JOIN sys.objects o ON o.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = ? AND o.name = ?",
            (schema, name))
        for col, nullable, is_identity in rows:
            if not nullable:
                facts.not_null.add(col)
            if is_identity:
                facts.identity_columns.add(col)
        (n,) = self._rows(
            "SELECT COUNT(*) FROM sys.check_constraints cc "
            "JOIN sys.objects o ON o.object_id = cc.parent_object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = ? AND o.name = ?",
            (schema, name))[0]
        facts.check_count = int(n)
        return facts

    def identity_next(self, table: str, column: str) -> int | None:
        schema, name = _split_table(table, "dbo")
        rows = self._rows(
            "SELECT CAST(ic.last_value AS BIGINT), CAST(ic.increment_value AS BIGINT), "
            "       CAST(ic.seed_value AS BIGINT) "
            "FROM sys.identity_columns ic JOIN sys.objects o ON o.object_id = ic.object_id "
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE s.name = ? AND o.name = ? AND ic.name = ?", (schema, name, column))
        if not rows:
            return None
        last, inc, seed = rows[0]
        if last is None:  # identity never used: the next value is the seed
            return int(seed) if seed is not None else None
        return int(last) + int(inc or 1)


class DatabricksSourceAdapter(_SqlAdapterBase):
    """Databricks as the SOURCE (workspace-to-workspace or Hive-to-UC moves)."""

    def __init__(self, dsn_secret: str):
        super().__init__(_databricks_connect(dsn_secret))


SOURCE_ADAPTERS = {
    "redshift": RedshiftSourceAdapter,
    "snowflake": SnowflakeSourceAdapter,
    "teradata": TeradataSourceAdapter,
    "oracle": OracleSourceAdapter,
    "sqlserver": SqlServerSourceAdapter,
    "databricks": DatabricksSourceAdapter,
}


# ---- Databricks target ------------------------------------------------------------------

def _databricks_connect(secret_name: str):
    """Secret value: JSON {"server_hostname": ..., "http_path": ..., "access_token": ...}.
    Convention: DATABRICKS_MIGRATION_SQL (the migration-catalog principal, never prod)."""
    from databricks import sql  # lazy: optional extra (databricks-sql-connector)
    cfg = json.loads(_secret(secret_name))
    return sql.connect(server_hostname=cfg["server_hostname"], http_path=cfg["http_path"],
                       access_token=cfg["access_token"])


class DatabricksTargetAdapter:
    """Target side: Unity Catalog tables under one catalog.schema. Nested (ARRAY<STRUCT>)
    columns are supported for cardinality via size(); dotted field paths address STRUCT
    fields. Object names in the mapping spec are bare table names; the adapter qualifies
    them with the migration catalog and schema so a spec never points at production."""

    def __init__(self, secret_name: str, catalog: str, schema: str):
        self._conn = _databricks_connect(secret_name)
        self._sql = _SqlAdapterBase(self._conn)
        self._sql.paramstyle = "pyformat"
        self._prefix = f"`{catalog}`.`{schema}`."

    @property
    def statements(self) -> int:
        return self._sql.statements

    @property
    def rows_fetched(self) -> int:
        return self._sql.rows_fetched

    def _q(self, object: str) -> str:
        return self._prefix + f"`{object}`"

    def table_aggregates(self, object: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]:
        return self._sql.table_aggregates(self._q(object), columns, numeric, where)

    def target_row_count(self, object: str, where: str | None = None) -> int:
        return self._sql.row_count(self._q(object), where)

    def nested_count(self, object: str, array_path: str, where: str | None = None) -> int:
        w = f" WHERE {where}" if where else ""
        (n,) = self._sql._rows(f"SELECT COALESCE(SUM(size({array_path})), 0) FROM {self._q(object)}{w}")[0]
        return int(n)

    def field_aggregates(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]:
        return self._sql.field_aggregates(self._q(object), field_path, where)

    def fetch_keyed(self, object: str, key_fields: list[str], fields: list[str],
                    where: str | None = None, keys: list[Any] | None = None) -> Iterable[dict[str, Any]]:
        # Select top-level columns only; STRUCT/ARRAY columns come back as dicts/lists via
        # Row.asDict(recursive=True) so tier logic can walk dotted paths.
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


# ---- Lakebase (Postgres) target -----------------------------------------------------------

class _PostgresBase(_SqlAdapterBase):
    """Postgres wire protocol via psycopg 3: Lakebase branches and any stand-in Postgres."""

    paramstyle = "format"
    # EXTRACT returns an exact numeric on Postgres 14+ (Lakebase is 16), so no float rounding
    datetime_digest_sql = "TRUNC(EXTRACT(EPOCH FROM {col}) * 1000000)"

    def open_window(self) -> str:
        """Every statement until close_window reads one REPEATABLE READ snapshot."""
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
        facts = SchemaFacts()
        rows = self._rows(
            "SELECT con.contype, con.conname, a.attname, "
            "       CASE WHEN con.contype = 'f' THEN rn.nspname || '.' || rc.relname END, "
            "       ra.attname, k.ord "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum "
            "LEFT JOIN pg_class rc ON rc.oid = con.confrelid "
            "LEFT JOIN pg_namespace rn ON rn.oid = rc.relnamespace "
            "LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ord) "
            "       ON fk.ord = k.ord "
            "LEFT JOIN pg_attribute ra ON ra.attrelid = con.confrelid AND ra.attnum = fk.attnum "
            "WHERE n.nspname = %s AND c.relname = %s AND con.contype IN ('p', 'u', 'f', 'c') "
            "ORDER BY con.conname, k.ord", (schema, name))
        by_con: dict[str, list] = {}
        for ctype, cname, col, ref_table, ref_col, _ord in rows:
            entry = by_con.setdefault(cname, [ctype, [], ref_table, []])
            entry[1].append(col)
            if ref_col is not None:
                entry[3].append(ref_col)
        for ctype, cols, ref_table, ref_cols in by_con.values():
            if ctype == "p":
                facts.primary_key = tuple(cols)
            elif ctype == "u":
                facts.unique.add(tuple(cols))
            elif ctype == "f":
                facts.foreign_keys.add((tuple(cols), ref_table, tuple(ref_cols)))
            elif ctype == "c":
                facts.check_count += 1
        rows = self._rows(
            "SELECT ix.indisunique, ix.indisprimary, a.attname, k.ord "
            "FROM pg_index ix JOIN pg_class c ON c.oid = ix.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON k.ord <= ix.indnkeyatts "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum "
            "WHERE n.nspname = %s AND c.relname = %s AND NOT ix.indisprimary "
            "ORDER BY ix.indexrelid, k.ord", (schema, name))
        # indexes are grouped by their order of appearance (indexrelid), so a change in the
        # ordinal back to 1 starts a new index
        current: list[str] = []
        current_unique = False
        for is_unique, _is_pk, col, ord_ in rows:
            if int(ord_) == 1 and current:
                (facts.unique if current_unique else facts.indexes).add(tuple(current))
                current = []
            current.append(col)
            current_unique = bool(is_unique)
        if current:
            (facts.unique if current_unique else facts.indexes).add(tuple(current))
        rows = self._rows(
            "SELECT a.attname, a.attnotnull, a.attidentity <> '' OR "
            "       pg_get_serial_sequence(%s, a.attname) IS NOT NULL "
            "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (f'"{schema}"."{name}"', schema, name))
        for col, notnull, has_seq in rows:
            if notnull:
                facts.not_null.add(col)
            if has_seq:
                facts.identity_columns.add(col)
        return facts

    def identity_next(self, table: str, column: str) -> int | None:
        schema, name = _split_table(table, "public")
        (seq,) = self._rows("SELECT pg_get_serial_sequence(%s, %s)",
                            (f'"{schema}"."{name}"', column))[0]
        if seq is None:
            return None
        (last, is_called, inc) = self._rows(
            f"SELECT s.last_value, s.is_called, p.seqincrement FROM {seq} s, "
            "pg_sequence p WHERE p.seqrelid = %s::regclass", (seq,))[0]
        return int(last) + int(inc) if is_called else int(last)


class PostgresSourceAdapter(_PostgresBase):
    """Secret value: a libpq DSN for a read-only role (on-prem Postgres/MySQL-compatible OLTP
    sources on the operational track; also the stand-in source in rehearsals)."""

    def __init__(self, dsn_secret: str):
        import psycopg  # lazy: optional extra
        super().__init__(psycopg.connect(_secret(dsn_secret)))


SOURCE_ADAPTERS["postgres"] = PostgresSourceAdapter


class LakebaseTargetAdapter(_PostgresBase):
    """Target side for the operational track: one schema inside a Lakebase branch database.
    Secret value: the branch endpoint's libpq DSN (OAuth token as the password, minted by
    `databricks postgres` for the migration principal; never the production branch). Object
    names in the mapping spec are bare table names, qualified here with the schema."""

    def __init__(self, secret_name: str, schema: str):
        import psycopg  # lazy: optional extra
        super().__init__(psycopg.connect(_secret(secret_name)))
        self._schema = schema

    def _q(self, object: str) -> str:
        return f'"{self._schema}"."{object}"'

    def target_row_count(self, object: str, where: str | None = None) -> int:
        return self.row_count(self._q(object), where)

    def nested_count(self, object: str, array_path: str, where: str | None = None) -> int:
        from .config import ConfigError
        raise ConfigError(f"{object}.{array_path}: embedded arrays have no Lakebase shape; "
                          "map the child table as its own object")

    def field_aggregates(self, object: str, field_path: str, where: str | None = None) -> dict[str, Any]:
        return super().field_aggregates(self._q(object), field_path, where)

    def table_aggregates(self, object: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]:
        return super().table_aggregates(self._q(object), columns, numeric, where)

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

    def identity_next(self, object: str, column: str) -> int | None:
        return super().identity_next(self._q(object), column)
