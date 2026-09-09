"""Catalog reads that only a real Postgres can exercise. Skipped unless RECON_TEST_POSTGRES_DSN
names a throwaway database (the rehearsal stand-in); the test owns one temporary schema."""

import datetime as dt
import os
import uuid
from decimal import Decimal

import pytest

from recon.adapters import (
    LakebaseTargetAdapter,
    PostgresSourceAdapter,
    TargetIdentityError,
)
from recon.transactional import _applied_predicate, _newer_predicate
from recon.watermarks import literal

psycopg = pytest.importorskip("psycopg")

DSN_VAR = "RECON_TEST_POSTGRES_DSN"
pytestmark = pytest.mark.skipif(DSN_VAR not in os.environ, reason=f"{DSN_VAR} not set")


@pytest.fixture
def schema():
    name = f"recon_test_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(os.environ[DSN_VAR], autocommit=True)
    conn.execute(f"CREATE SCHEMA {name}")
    conn.execute(f"CREATE TABLE {name}.t (id INT PRIMARY KEY, code TEXT NOT NULL, region TEXT, "
                 f"active BOOLEAN NOT NULL, dup INT)")
    conn.execute(f"INSERT INTO {name}.t VALUES (1, 'a', 'eu', true, 1), (2, 'b', 'us', true, 1)")
    conn.execute(f"CREATE UNIQUE INDEX t_code_u ON {name}.t (code)")
    conn.execute(f"CREATE UNIQUE INDEX t_region_partial_u ON {name}.t (region) WHERE active")
    conn.execute(f"CREATE INDEX t_region_ix ON {name}.t (region, code)")
    conn.execute(f"CREATE INDEX t_active_partial_ix ON {name}.t (active) WHERE region = 'eu'")
    conn.execute(f"CREATE UNIQUE INDEX t_code_lower_u ON {name}.t (lower(code))")
    conn.execute(f"CREATE INDEX t_region_expr_ix ON {name}.t (upper(region), id) INCLUDE (code)")
    conn.execute(f"CREATE UNIQUE INDEX t_expr_partial_u ON {name}.t (lower(region)) WHERE active")
    conn.execute(f"ALTER TABLE {name}.t ADD CONSTRAINT t_dup_chk CHECK (dup > 0)")
    conn.execute(f"ALTER TABLE {name}.t ADD CONSTRAINT t_const_chk CHECK (1 < 2)")   # names no column
    conn.execute(f"CREATE TABLE {name}.c (id INT PRIMARY KEY, t_id INT REFERENCES {name}.t (id) "
                 f"ON UPDATE RESTRICT ON DELETE SET NULL)")
    # a concurrent unique build over duplicate values fails and leaves the index INVALID
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(f"CREATE UNIQUE INDEX CONCURRENTLY t_dup_invalid_u ON {name}.t (dup)")
    (valid,) = conn.execute("SELECT indisvalid FROM pg_index WHERE indexrelid = "
                            f"'{name}.t_dup_invalid_u'::regclass").fetchone()
    assert valid is False
    try:
        yield name
    finally:
        conn.execute(f"DROP SCHEMA {name} CASCADE")
        conn.close()


def _database() -> str:
    with psycopg.connect(os.environ[DSN_VAR]) as conn:
        return conn.execute("SELECT current_database()").fetchone()[0]


def test_partial_and_invalid_indexes_never_count_as_parity(schema, monkeypatch):
    monkeypatch.setenv("RECON_TEST_TARGET", os.environ[DSN_VAR])
    target = LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), schema)
    facts = target.schema_facts("t")
    assert facts.primary_key == ("id",)
    assert facts.unique == {("code",)}                       # partial unique is not full uniqueness
    assert facts.indexes == {("region", "code")}             # invalid and partial indexes excluded
    assert facts.partial == {("region",), ("active",)}       # reported for a manual check
    assert facts.not_null == {"id", "code", "active"}
    assert facts.check_count == 2                            # a column-free CHECK still counts
    # expression keys (attnum 0) stay visible as the key text of pg_get_indexdef
    assert facts.expression_unique == {"lower(code)"}           # partial unique is not uniqueness
    assert facts.expression_indexes == {"upper(region), id", "lower(region)"}  # INCLUDE dropped
    child = target.schema_facts("c")
    fk = (("t_id",), f"{schema}.t", ("id",))
    assert child.foreign_keys == {fk}
    assert child.foreign_key_actions == {fk: ("no action", "set null")}  # RESTRICT folds in


def test_range_fingerprints_bind_through_psycopg(schema, monkeypatch):
    # the modular square must render as MOD(), not `%`, or psycopg reads it as a placeholder
    monkeypatch.setenv("RECON_TEST_TARGET", os.environ[DSN_VAR])
    target = LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), schema)
    fps = target.range_fingerprints("t", ["id"], ["integer"], None, None, [(None, (1,)), ((1,), None)])
    assert [(n, k[0][0]) for n, k, _ in fps] == [(1, 1), (1, 2)]   # boundary key 1 lands once
    assert [k[0][1] for _, k, _ in fps] == [1, 4]        # 1^2 and 2^2


def test_numeric_keys_beyond_six_decimals_are_never_collapsed_into_one_digest(schema, monkeypatch):
    dsn = os.environ[DSN_VAR]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE {schema}.n (k NUMERIC(20,8) PRIMARY KEY, big BIGINT)")
        conn.execute(f"INSERT INTO {schema}.n VALUES (1.0000001, 4611686018427387904), "
                     "(1.0000002, 4611686018427387905)")
    monkeypatch.setenv("RECON_TEST_TARGET", dsn)
    target = LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), schema)
    whole = [(None, None)]
    # a fractional key gets no digest, so tier 5 streams it instead of trusting a rounded sum
    (n, keys, _), = target.range_fingerprints("n", ["k"], ["number"], None, None, whole)
    assert (n, keys) == (2, None)
    # whole numbers digest exactly up to bigint range: neighbours near 2^62 stay distinct
    (_, keys, _), = target.range_fingerprints("n", ["big"], ["integer"], None, None, whole)
    (_, keys_lo, _), = target.range_fingerprints("n", ["big"], ["integer"], None, None,
                                                 [(None, (4611686018427387904,))])
    assert keys[0][0] == 2 * 4611686018427387904 + 1
    assert keys_lo[0][0] == 4611686018427387904 and keys[0][1] != keys_lo[0][1]


def test_target_identity_is_checked_against_the_live_database(monkeypatch):
    monkeypatch.setenv("RECON_TEST_TARGET", os.environ[DSN_VAR])
    with pytest.raises(TargetIdentityError, match="allowlisted target is 'not_this_database'"):
        LakebaseTargetAdapter("RECON_TEST_TARGET", "not_this_database", "public")
    assert LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), "public").database == _database()


def test_watermark_predicates_hold_for_timestamptz_in_a_non_utc_session(schema, monkeypatch):
    dsn = os.environ[DSN_VAR]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE {schema}.w (id INT PRIMARY KEY, at_tz TIMESTAMPTZ, at_naive TIMESTAMP)")
        conn.execute(f"INSERT INTO {schema}.w VALUES "
                     "(1, '2026-09-08 12:00:00+00', '2026-09-08 12:00:00'), "
                     "(2, '2026-09-08 13:00:00+00', '2026-09-08 13:00:00'), "
                     "(3, '2026-09-08 14:00:00+00', '2026-09-08 14:00:00'), "
                     "(4, NULL, NULL)")
    monkeypatch.setenv("RECON_TEST_SOURCE",
                       psycopg.conninfo.make_conninfo(dsn, options="-c TimeZone=America/New_York"))
    source = PostgresSourceAdapter("RECON_TEST_SOURCE")
    try:
        (tz,) = source._rows("SHOW TimeZone")[0]
        assert tz == "America/New_York"
        hwm = dt.datetime(2026, 9, 8, 15, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))  # 13:00 UTC
        for col in ("at_tz", "at_naive"):
            newer = _newer_predicate(col, hwm, source.watermark_literal)
            applied = _applied_predicate(col, hwm, source.watermark_literal)
            assert source.row_count(f"{schema}.w", newer) == 1, (col, newer)       # row 3 only
            assert source.row_count(f"{schema}.w", applied) == 3, (col, applied)   # rows 1, 2 and the NULL
        # the bare form the zone-less engines use is read as 13:00 New York (17:00 UTC) here, so
        # the in-flight row 3 would be misfiled as applied
        bare = _newer_predicate("at_tz", hwm, literal)
        assert source.row_count(f"{schema}.w", bare) == 0
    finally:
        source._conn.close()   # release the table lock before the schema is dropped


def test_catalog_typing_spares_the_repeatable_read_window_that_a_sum_probe_ends(schema, monkeypatch):
    dsn = os.environ[DSN_VAR]
    table = f"{schema}.a"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, amount NUMERIC(12,2), status TEXT, "
                     "modified_at TIMESTAMPTZ)")
        conn.execute(f"INSERT INTO {table} VALUES (1, 10, 'A', '2026-09-08 12:00:00+00'), "
                     "(2, 20, 'A', '2026-09-08 13:00:00+00'), (3, 30, 'A', '2026-09-08 14:00:00+00')")
    monkeypatch.setenv("RECON_TEST_SOURCE", dsn)
    source = PostgresSourceAdapter("RECON_TEST_SOURCE")
    try:
        assert source.open_window() == "repeatable_read"
        assert source.row_count(table) == 3                    # the first read pins the snapshot
        with psycopg.connect(dsn, autocommit=True) as writer:   # a below-max write: count and max unchanged
            writer.execute(f"UPDATE {table} SET amount = 1000 WHERE id = 1")
        # tier 2 reads the catalog instead of probing, so the string column is never summed
        assert source.numeric_columns(table) == {"id", "amount"}
        assert source.field_aggregates(table, "amount")["sum"] == Decimal("60.00")
        assert source.window_strength() == "snapshot" and source.window_released is None
        # the probe an untyped source falls back to: SUM(text) aborts the transaction, the rollback
        # ends the snapshot, and the adapter must stop claiming it
        assert source.field_aggregates(table, "status")["sum"] is None
        assert source.window_strength() != "snapshot"
        assert source.window_released == f"SUM(status) on {table} failed and rolled back"
        assert source.field_aggregates(table, "amount")["sum"] == Decimal("1050.00")  # the write shows
    finally:
        source._conn.close()


def test_unique_null_semantics_follow_the_index_declaration(schema, monkeypatch):
    dsn = os.environ[DSN_VAR]
    table = f"{schema}.u"
    with psycopg.connect(dsn, autocommit=True) as conn:
        (version,) = conn.execute("SELECT current_setting('server_version_num')::int").fetchone()
        conn.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, a INT, b INT, UNIQUE (a))")
        if version >= 150000:
            conn.execute(f"CREATE UNIQUE INDEX u_b_nnd ON {table} (b) NULLS NOT DISTINCT")
    monkeypatch.setenv("RECON_TEST_TARGET", dsn)
    target = LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), schema)
    facts = target.schema_facts("u")
    # a default unique lets any number of NULL keys through, so it is not in the nulls-equal set
    assert ("a",) in facts.unique and ("a",) not in facts.unique_nulls_equal
    if version >= 150000:
        assert facts.unique_nulls_equal == {("b",)}
    else:
        assert facts.unique_nulls_equal == set()
    assert facts.unique == {("a",), ("b",)} if version >= 150000 else facts.unique == {("a",)}


def test_numeric_domains_are_numeric_columns_and_take_a_sum(schema, monkeypatch):
    # a domain over NUMERIC (and a domain over that domain) is still a number for tier 2
    dsn = os.environ[DSN_VAR]
    table = f"{schema}.d"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE DOMAIN {schema}.amount_d AS NUMERIC(12,2) CHECK (VALUE >= 0)")
        conn.execute(f"CREATE DOMAIN {schema}.fee_d AS {schema}.amount_d")
        conn.execute(f"CREATE DOMAIN {schema}.code_d AS TEXT")
        conn.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, amount {schema}.amount_d, "
                     f"fee {schema}.fee_d, code {schema}.code_d)")
        conn.execute(f"INSERT INTO {table} VALUES (1, 10.50, 1, 'x'), (2, 20, 2.25, 'y')")
    monkeypatch.setenv("RECON_TEST_SOURCE", dsn)
    source = PostgresSourceAdapter("RECON_TEST_SOURCE")
    try:
        assert source.numeric_columns(table) == {"id", "amount", "fee"}
        aggs = source.table_aggregates(table, ["amount", "fee", "code"], ["amount", "fee"])
        assert aggs["amount"]["sum"] == Decimal("30.50") and aggs["fee"]["sum"] == Decimal("3.25")
        assert "sum" not in aggs["code"] or aggs["code"]["sum"] is None
    finally:
        source._conn.close()

