"""Source and target adapters.

Tiers 1-3 talk only to these two interfaces, so an in-memory fake (tests) or a Lakebridge
reconcile wrapper (future) can plug in without touching tier logic. Aggregates are computed
natively on each side (SQL on the source warehouse, SQL on Databricks) so no bulk data
crosses the wire. Drivers are imported lazily; install only the extras you need.

Connection secrets are read from environment variables BY NAME; the harness never accepts
a literal connection string or token on the CLI.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from .paths import get_path
from .watermarks import instant
from .watermarks import literal as watermark_literal


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


# Range fingerprints carry two moments per digested column: the exact sum and the sum of
# squared residues modulo this Mersenne prime (2^31 - 1). Two multisets that agree on count,
# sum and sum of squares must differ in at least three elements, so any one- or two-key
# substitution inside a range is provably visible; the modulus keeps a bigint key's or an
# epoch-microsecond datetime's square inside DECIMAL(38,0) on every engine.
DIGEST_MODULUS = 2_147_483_647

# How often a fallback window marker re-reads (token, count/max, token) before giving up on a
# quiet bracket and recording the unstable pair instead.
MARKER_BRACKET_ATTEMPTS = 3


_FK_ACTIONS = {"a": "no action", "r": "no action", "c": "cascade", "n": "set null",
               "d": "set default", "no_action": "no action", "restrict": "no action",
               "cascade": "cascade", "set_null": "set null", "set_default": "set default"}


def _fk_action(raw: str) -> str:
    """One spelling for a referential action across pg_constraint codes and sys.foreign_keys
    descriptions."""
    return _FK_ACTIONS.get(str(raw).lower(), str(raw).lower())


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
    Column tuples are ordered; unique/index sets hold leading-column tuples. Only indexes the
    engine enforces over the whole table belong in `unique`/`indexes`: disabled, invalid or
    still-building ones are left out, and filtered/partial ones (a row predicate) go to
    `partial`, which is reported but not graded because the predicate is dialect-bound.
    `table` is the schema-qualified name the catalog knows the table by, so a foreign key that
    references it can be resolved even when the mapping spec spells the table bare."""
    table: str = ""
    primary_key: tuple[str, ...] = ()
    unique: set[tuple[str, ...]] = field(default_factory=set)
    # the unique keys whose engine treats NULL keys as equal, so a second row with a NULL key is
    # rejected (SQL Server/ASE always; Postgres only under UNIQUE NULLS NOT DISTINCT). A key
    # absent here follows the SQL standard: NULLs are distinct and any number of them pass.
    unique_nulls_equal: set[tuple[str, ...]] = field(default_factory=set)
    foreign_keys: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = field(default_factory=set)
    # (on update, on delete) per foreign key, spelled `no action` / `cascade` / `set null` /
    # `set default`; RESTRICT is folded into `no action` (same outcome, engine-specific timing)
    foreign_key_actions: dict[tuple[tuple[str, ...], str, tuple[str, ...]], tuple[str, str]] = field(
        default_factory=dict)
    not_null: set[str] = field(default_factory=set)
    indexes: set[tuple[str, ...]] = field(default_factory=set)
    check_count: int = 0
    identity_columns: set[str] = field(default_factory=set)
    partial: set[tuple[str, ...]] = field(default_factory=set)
    # indexes keyed on expressions rather than columns, as their normalised definition text
    # (e.g. "lower(email)"); a unique one is a constraint the column-wise facts cannot see
    expression_unique: set[str] = field(default_factory=set)
    expression_indexes: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class IdentityState:
    """Where a sequence/identity stands: the value its next insert takes and the signed step it
    moves by. A negative step counts down, so its headroom is checked against the other side's
    minimum key rather than its maximum."""
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
    """One statement per table for Tier 2 (the cost rule: one multi-metric statement, not one per
    metric). `numeric` names the columns that also get a SUM."""
    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]: ...


@runtime_checkable
class KeyExcludingAggregates(Protocol):
    """Tier 2 in transactional mode: the same one-statement aggregates over every row except the
    listed keys (the source rows still in flight), so both sides describe one applied set.
    `exclusion_capacity` is how many keys of the given width one such statement can carry; the
    caller grades nothing rather than split the aggregate across statements."""
    def table_aggregates_excluding(self, table: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None) -> dict[str, dict[str, Any]]: ...
    def exclusion_capacity(self, key_width: int) -> int: ...


@runtime_checkable
class ColumnTypes(Protocol):
    """Catalog-backed column typing, so Tier 2 learns which undeclared fields take a SUM from
    metadata instead of probing with a statement that errors on strings. The probe's error
    handling rolls the connection back, which on a pinned transactional window silently ends
    the snapshot every later tier believes it is still reading."""
    def numeric_columns(self, table: str) -> set[str]: ...


@runtime_checkable
class WholeNumberColumns(Protocol):
    """Columns whose declared type guarantees whole-number values (integer types, exact decimals
    of scale 0). Tier 5 fingerprints a numeric key or watermark exactly only on this evidence:
    the sampled range bounds may all be whole while an interior key is fractional, and rounding
    it into a DECIMAL(38,0) digest would let two distinct keys sum alike."""
    def whole_number_columns(self, table: str) -> set[str]: ...


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
    def watermark_literal(self, value: Any) -> str: ...
    def schema_facts(self, table: str) -> SchemaFacts: ...
    def identity_state(self, table: str, column: str) -> IdentityState | None: ...


@runtime_checkable
class NullKeyCounting(Protocol):
    """Rows whose comparison key has a NULL component. Such rows cannot be matched, bounded by
    MIN/MAX or reached by a keyed fetch, so Tier 3 reports them instead of silently skipping them."""
    def null_key_count(self, table: str, key_cols: list[str], where: str | None = None) -> int: ...


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
    # Bound parameters one statement may carry; SQL Server's hard limit is 2100.
    max_params = 2000
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
    # Only whole-number keys digest; a fractional decimal or float has no scale the harness can
    # cast to without rounding two distinct keys together, so those kinds return no digest and
    # the caller streams every range.
    integer_digest_sql = "CAST({col} AS DECIMAL(38,0))"
    datetime_digest_sql: str | None = None  # whole microseconds since the epoch
    # Remainder of {x} divided by {m}, sign of the dividend (the square below removes it). The
    # function form is the default because a literal `%` is a placeholder to pyformat drivers
    # (psycopg, databricks-sql); engines without MOD() override with their operator.
    mod_sql = "MOD({x}, {m})"
    # Second moment of a digest {d}: the residue is rounded to a whole number first so engines
    # that shorten the scale of a decimal product still agree with those that keep it.
    square_digest_sql = "CAST({r} * {r} AS DECIMAL(38,0))"
    # Whether datetime watermark literals carry an explicit +00:00 (see watermarks.literal).
    watermark_literal_utc_offset = False

    def __init__(self, conn):
        self._conn = conn
        self.statements = 0
        self.rows_fetched = 0
        self.isolation = "none"
        self._token_ok: bool | None = None
        # set when a statement rolled the pinned window back mid-run (see field_aggregates)
        self.window_released: str | None = None

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
                self._release_window(f"SUM({column}) on {table} failed and rolled back")
            return None

    def _release_window(self, reason: str) -> None:
        """A rollback ends the transaction that carried the pinned snapshot; say so rather than
        keep reporting an isolation the later reads no longer have."""
        if self.isolation != "none":
            self.isolation = "none"
            self.window_released = reason

    def numeric_columns(self, table: str) -> set[str]:
        raise NotImplementedError(f"{type(self).__name__} cannot read column types")

    def whole_number_columns(self, table: str) -> set[str]:
        raise NotImplementedError(f"{type(self).__name__} cannot read column types")

    def table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                         where: str | None = None) -> dict[str, dict[str, Any]]:
        w = f" WHERE {where}" if where else ""
        return self._table_aggregates(table, columns, numeric, w, [])

    def exclusion_capacity(self, key_width: int) -> int:
        """The membership test binds one parameter per key component."""
        return max(1, self.max_params // max(1, key_width))

    def table_aggregates_excluding(self, table: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None) -> dict[str, dict[str, Any]]:
        if len(exclude_keys) > self.exclusion_capacity(len(key_cols)):
            raise ValueError(f"{len(exclude_keys)} keys x {len(key_cols)} columns exceed the "
                             f"{self.max_params}-parameter budget of one statement")
        clauses, values = [], []
        if where:
            clauses.append(f"({where})")
        if exclude_keys:
            clauses.append("NOT " + self._keys_clause(key_cols, exclude_keys, values))
        w = " WHERE " + " AND ".join(clauses) if clauses else ""
        return self._table_aggregates(table, columns, numeric, w, values)

    def _table_aggregates(self, table: str, columns: list[str], numeric: list[str],
                          w: str, values: list[Any]) -> dict[str, dict[str, Any]]:
        exprs = ["COUNT(*)"]
        for col in columns:
            exprs += [f"COUNT({col})", f"MIN({col})", f"MAX({col})", f"COUNT(DISTINCT {col})"]
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
            names = [d[0] for d in cur.description]
            for row in cur:
                self.rows_fetched += 1
                yield dict(zip(names, row))

    def _keys_clause(self, key_cols: list[str], keys: list, values: list[Any]) -> str:
        """A parenthesised membership test for `keys`, appending its bind values to `values`."""
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
        self.window_released = None
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

    def watermark_literal(self, value: Any) -> str:
        """How this engine wants a watermark bound in a predicate against its own column."""
        return watermark_literal(value, utc_offset=self.watermark_literal_utc_offset)

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
        if self.isolation in ("snapshot", "repeatable_read"):
            try:
                return tuple(self._rows(sql)[0])
            except Exception:  # noqa: BLE001  driver-specific error type
                if self.isolation != "snapshot":
                    raise
                # SQL Server accepts SET ... SNAPSHOT and only fails on the first table read when
                # the database has ALLOW_SNAPSHOT_ISOLATION off; drop to plain reads, markers
                # still decide.
                self._conn.rollback()
                if self.snapshot_reset_sql:
                    self._execute(self.snapshot_reset_sql)
                self.isolation = "none"
        # No pinned snapshot: the count/max row and the change token are separate reads, so a
        # write landing between them would enter the baseline unseen. Bracket the row with two
        # token reads and accept it only when both agree; a bracket that never settles carries
        # both tokens, a shape no later marker can equal.
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
        """(sum term, sum-of-squares term) for one column, or None when the kind has no exact
        portable digest (strings, uuids, fractional numbers)."""
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
        """Per key range: (row count, per-key-column (sum, sum of squares), watermark (sum, sum
        of squares, null count)) in one statement (SUM over CASE). Sums are exact decimals and
        the squares are taken modulo DIGEST_MODULUS, so a key swapped for another, two keys
        traded for two others with the same total, or a row's watermark moved inside a range all
        change the fingerprint even when the count does not. SUM skips NULL, which would make a
        NULL watermark indistinguishable from zero / the epoch; the null count keeps them apart.
        A digest is None when the column kind has no exact portable sum (strings, uuids,
        fractional numbers)."""
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
        # one CASE assigns each row its range, so every bound is bound once; a key on a shared
        # boundary lands in the lower range on both sides alike. Ranges are batched so one
        # statement never carries more parameters than the engine accepts.
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
    mod_sql = "({x} MOD {m})"

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


def _key_text(value: Any) -> str | None:
    """A key component as the text the target engine casts back to the column type; datetimes as
    UTC instants with an explicit offset, matching the watermark literal contract."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return instant(value).isoformat(sep=" ", timespec="microseconds") + "+00:00"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    return str(value)


def _split_table(table: str, default_schema: str | None) -> tuple[str | None, str]:
    parts = table.replace("[", "").replace("]", "").replace('"', "").split(".")
    return (parts[-2] if len(parts) > 1 else default_schema), parts[-1]


_INDEX_KEYS_START_RE = re.compile(r"\bUSING\s+\w+\s*\(", re.IGNORECASE)


def normalize_sql_text(text: str) -> str:
    """Lower-case and collapse whitespace outside quotes; a single-quoted literal or a
    double-quoted identifier keeps its exact contents, so `lower(x) = 'A'` and `= 'a'` differ."""
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
    """The key list of a `CREATE INDEX` definition ("lower(email), tenant_id"), normalized with
    `normalize_sql_text`: the balanced parenthesis after `USING <method>`, walked outside quotes
    so nested calls and literals containing parentheses stay whole and the INCLUDE/WHERE/WITH
    suffixes never enter."""
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


class SqlServerSourceAdapter(_SqlAdapterBase):
    """Secret value: an ODBC connection string. Also the Sybase ASE stand-in for the OLTP track
    (same T-SQL catalog shape through sys.* views on SQL Server; ASE itself has no snapshot
    isolation and no usage-stats DMV, so there the window rests on markers alone)."""

    snapshot_sql = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
    snapshot_reset_sql = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    mod_sql = "({x} % {m})"  # T-SQL has no MOD(); pyodbc binds with `?`, so `%` is literal
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
        facts = SchemaFacts(table=f"{schema}.{name}")
        rows = self._rows(
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
        rows = self._rows(
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
            "JOIN sys.schemas s ON s.schema_id = o.schema_id "
            "WHERE s.name = ? AND o.name = ? AND cc.is_disabled = 0",
            (schema, name))[0]
        facts.check_count = int(n)
        return facts

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
        rows = self._rows(
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

    def null_key_count(self, object: str, key_fields: list[str], where: str | None = None) -> int:
        return self._sql.null_key_count(self._q(object), key_fields, where)

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
    # A timestamptz column reads an offset-less literal in the session TimeZone; a timestamp
    # column ignores the offset, so the explicit +00:00 is right for both under the UTC contract.
    watermark_literal_utc_offset = True

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
        facts = SchemaFacts(table=f"{schema}.{name}")
        rows = self._rows(
            "SELECT con.contype, con.conname, a.attname, "
            "       CASE WHEN con.contype = 'f' THEN rn.nspname || '.' || rc.relname END, "
            "       ra.attname, k.ord, con.confupdtype, con.confdeltype "
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
        # a CHECK that names no column (a constant, or only functions) has an empty conkey and
        # arrives as one row with a NULL column; it still counts
        by_con: dict[str, list] = {}
        for ctype, cname, col, ref_table, ref_col, _ord, on_update, on_delete in rows:
            entry = by_con.setdefault(cname, [ctype, [], ref_table, [], (on_update, on_delete)])
            if col is not None:
                entry[1].append(col)
            if ref_col is not None:
                entry[3].append(ref_col)
        for ctype, cols, ref_table, ref_cols, actions in by_con.values():
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
        # attnum 0 in indkey marks an expression key; a LEFT JOIN keeps those indexes visible
        nulls_equal = "ix.indnullsnotdistinct" if self._server_version() >= 150000 else "FALSE"
        rows = self._rows(
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

    # In-flight keys travel as one text[] per key column and are cast to the column's declared
    # type server-side, so one statement excludes any number of keys of any width: the row cap
    # is the caller's (IN_FLIGHT_EXCLUSION_CAP), never the bind-parameter limit.
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
                                   where: str | None = None) -> dict[str, dict[str, Any]]:
        if not exclude_keys:
            return super().table_aggregates_excluding(table, columns, numeric, key_cols, [], where)
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
                                      values)

    def _server_version(self) -> int:
        if not hasattr(self, "_server_version_num"):
            (v,) = self._rows("SELECT current_setting('server_version_num')")[0]
            self._server_version_num = int(v)
        return self._server_version_num

    def numeric_columns(self, table: str) -> set[str]:
        schema, name = _split_table(table, "public")
        # a domain (typtype 'd') is classified by the base type at the bottom of its chain
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
        # numeric's typmod packs (precision << 16 | scale) + 4; -1 is unconstrained, so its scale
        # is unknown and it is not whole. A domain carries the modifier on its own pg_type row.
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
        (seq,) = self._rows("SELECT pg_get_serial_sequence(%s, %s)",
                            (f'"{schema}"."{name}"', column))[0]
        if seq is None:
            return None
        (last, is_called, inc) = self._rows(
            f"SELECT s.last_value, s.is_called, p.seqincrement FROM {seq} s, "
            "pg_sequence p WHERE p.seqrelid = %s::regclass", (seq,))[0]
        step = int(inc)
        return IdentityState(int(last) + step if is_called else int(last), step)


class PostgresSourceAdapter(_PostgresBase):
    """Secret value: a libpq DSN for a read-only role (on-prem Postgres/MySQL-compatible OLTP
    sources on the operational track; also the stand-in source in rehearsals)."""

    def __init__(self, dsn_secret: str):
        import psycopg  # lazy: optional extra
        super().__init__(psycopg.connect(_secret(dsn_secret)))


SOURCE_ADAPTERS["postgres"] = PostgresSourceAdapter


class TargetIdentityError(RuntimeError):
    """The database a target connection landed in is not the one the allowlist names."""


class LakebaseTargetAdapter(_PostgresBase):
    """Target side for the operational track: one schema inside a Lakebase branch database.
    Secret value: the branch endpoint's libpq DSN (OAuth token as the password, minted by
    `databricks postgres` for the migration principal; never the production branch). The DSN
    decides which database the session lands in, so the connection is bound to `database`, the
    allowlisted identity, before any statement runs; a DSN pointing elsewhere is refused. Object
    names in the mapping spec are bare table names, qualified here with the schema."""

    def __init__(self, secret_name: str, database: str, schema: str):
        import psycopg  # lazy: optional extra
        super().__init__(psycopg.connect(_secret(secret_name)))
        self._schema = schema
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

    def table_aggregates_excluding(self, object: str, columns: list[str], numeric: list[str],
                                   key_cols: list[str], exclude_keys: list[tuple],
                                   where: str | None = None) -> dict[str, dict[str, Any]]:
        return super().table_aggregates_excluding(self._q(object), columns, numeric, key_cols,
                                                  exclude_keys, where)

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

    def null_key_count(self, object: str, key_fields: list[str], where: str | None = None) -> int:
        return super().null_key_count(self._q(object), key_fields, where)
