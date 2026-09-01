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
from typing import Any, Protocol


class SourceAdapter(Protocol):
    def row_count(self, table: str, where: str | None = None) -> int: ...
    def field_aggregates(self, table: str, column: str, where: str | None = None) -> dict[str, Any]: ...
    def fetch_keyed(self, table: str, key_cols: list[str], columns: list[str],
                    where: str | None = None, keys: list[tuple] | None = None) -> Iterable[dict[str, Any]]: ...
    def key_strata(self, table: str, key_cols: list[str], n_strata: int) -> list[tuple]: ...


class TargetAdapter(Protocol):
    def target_row_count(self, object: str) -> int: ...
    def nested_count(self, object: str, array_path: str) -> int: ...
    def field_aggregates(self, object: str, field_path: str) -> dict[str, Any]: ...
    def fetch_keyed(self, object: str, key_field: str, fields: list[str],
                    keys: list[Any] | None = None) -> Iterable[dict[str, Any]]: ...


def _secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"secret '{name}' not found in environment; pass secrets by name only")
    return value


AGG_SQL = ("SELECT COUNT(*) AS n, COUNT({col}) AS nonnull, MIN({col}) AS mn, "
           "MAX({col}) AS mx, COUNT(DISTINCT {col}) AS dc FROM {table}{where}")


class _SqlAdapterBase:
    """Shared SQL implementation; subclasses provide a DB-API connection."""

    def __init__(self, conn):
        self._conn = conn

    def _rows(self, sql: str, params: tuple = ()) -> list[tuple]:
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()

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

    def fetch_keyed(self, table: str, key_cols: list[str], columns: list[str],
                    where: str | None = None, keys: list[tuple] | None = None) -> Iterable[dict[str, Any]]:
        cols = ", ".join(dict.fromkeys(key_cols + columns))
        w = f" WHERE {where}" if where else ""
        cur = self._conn.cursor()
        cur.execute(f"SELECT {cols} FROM {table}{w} ORDER BY {', '.join(key_cols)}")
        names = [d[0] for d in cur.description]
        wanted = {tuple(k) for k in keys} if keys is not None else None
        for row in cur:
            rec = dict(zip(names, row))
            if wanted is None or tuple(rec[k] for k in key_cols) in wanted:
                yield rec

    def key_strata(self, table: str, key_cols: list[str], n_strata: int) -> list[tuple]:
        key = key_cols[0]
        rows = self._rows(f"SELECT MIN({key}), MAX({key}) FROM {table}")
        return [rows[0]] if rows else []


# ---- Source warehouses ------------------------------------------------------------------

class RedshiftSourceAdapter(_SqlAdapterBase):
    """Secret value: a libpq DSN, e.g. postgresql://user:pw@host:5439/db (read-only user)."""

    def __init__(self, dsn_secret: str):
        import psycopg2  # lazy: optional extra
        super().__init__(psycopg2.connect(_secret(dsn_secret)))


class SnowflakeSourceAdapter(_SqlAdapterBase):
    """Secret value: JSON with account, user, password, warehouse, database, schema, role."""

    def __init__(self, dsn_secret: str):
        import snowflake.connector  # lazy: optional extra
        super().__init__(snowflake.connector.connect(**json.loads(_secret(dsn_secret))))


class TeradataSourceAdapter(_SqlAdapterBase):
    """Secret value: JSON accepted by teradatasql.connect (host, user, password, ...)."""

    def __init__(self, dsn_secret: str):
        import teradatasql  # lazy: optional extra
        super().__init__(teradatasql.connect(_secret(dsn_secret)))


class OracleSourceAdapter(_SqlAdapterBase):
    """Secret value: user/password/dsn."""

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
        self._prefix = f"`{catalog}`.`{schema}`."

    def _q(self, object: str) -> str:
        return self._prefix + f"`{object}`"

    def target_row_count(self, object: str) -> int:
        return self._sql.row_count(self._q(object))

    def nested_count(self, object: str, array_path: str) -> int:
        (n,) = self._sql._rows(f"SELECT COALESCE(SUM(size({array_path})), 0) FROM {self._q(object)}")[0]
        return int(n)

    def field_aggregates(self, object: str, field_path: str) -> dict[str, Any]:
        return self._sql.field_aggregates(self._q(object), field_path)

    def fetch_keyed(self, object: str, key_field: str, fields: list[str],
                    keys: list[Any] | None = None) -> Iterable[dict[str, Any]]:
        # Select top-level columns only; STRUCT/ARRAY columns come back as dicts/lists via
        # Row.asDict(recursive=True) so tier logic can walk dotted paths.
        tops = list(dict.fromkeys([key_field.split(".")[0]] + [f.split(".")[0] for f in fields]))
        cur = self._conn.cursor()
        cur.execute(f"SELECT {', '.join(tops)} FROM {self._q(object)} ORDER BY {key_field}")
        wanted = set(keys) if keys is not None else None
        for row in cur:
            rec = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(zip(
                [d[0] for d in cur.description], row))
            if wanted is None or rec.get(key_field) in wanted:
                yield rec
