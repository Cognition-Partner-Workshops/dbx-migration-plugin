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
from dataclasses import dataclass
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
class SumProbe(Protocol):
    """One isolated `SELECT SUM(col)` for a field whose numeric type is undeclared on this side;
    None when the engine rejects it. Pairs with `BatchAggregates` so a probed field costs one
    extra statement, not a second full metrics pass."""
    def sum_probe(self, table: str, column: str, where: str | None = None) -> Any: ...


@runtime_checkable
class StratifiedKeys(Protocol):
    """Server-side key stratification for Tier 3 sampling: the source computes n key ranges and
    returns chosen keys per range, so the harness never streams the whole key column."""
    def key_strata(self, table: str, key_cols: list[str], n_strata: int,
                   where: str | None = None) -> list[Stratum]: ...
    def sample_keys(self, table: str, key_cols: list[str], lo: Any, hi: Any,
                    row_numbers: list[int], where: str | None = None) -> list[tuple]: ...
    def duplicate_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int: ...


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

    def __init__(self, conn):
        self._conn = conn
        self.statements = 0
        self.rows_fetched = 0

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
        out["sum"] = self.sum_probe(table, column, where)
        return out

    def sum_probe(self, table: str, column: str, where: str | None = None) -> Any:
        w = f" WHERE {where}" if where else ""
        try:
            (s,) = self._rows(f"SELECT SUM({column}) FROM {table}{w}")[0]
            return s
        except Exception:  # noqa: BLE001  driver-specific error type: SUM on a non-numeric column
            if hasattr(self._conn, "rollback"):
                self._conn.rollback()  # libpq leaves the transaction aborted otherwise
            return None

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


class SqlServerSourceAdapter(_SqlAdapterBase):
    """Secret value: an ODBC connection string."""

    def __init__(self, dsn_secret: str):
        import pyodbc  # lazy: optional extra
        super().__init__(pyodbc.connect(_secret(dsn_secret)))


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

    def sum_probe(self, object: str, field_path: str, where: str | None = None) -> Any:
        return self._sql.sum_probe(self._q(object), field_path, where)

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
