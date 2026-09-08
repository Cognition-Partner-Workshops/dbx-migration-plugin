"""Catalog reads that only a real Postgres can exercise. Skipped unless RECON_TEST_POSTGRES_DSN
names a throwaway database (the rehearsal stand-in); the test owns one temporary schema."""

import os
import uuid

import pytest

from recon.adapters import LakebaseTargetAdapter, TargetIdentityError

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


def test_range_fingerprints_bind_through_psycopg(schema, monkeypatch):
    # the modular square must render as MOD(), not `%`, or psycopg reads it as a placeholder
    monkeypatch.setenv("RECON_TEST_TARGET", os.environ[DSN_VAR])
    target = LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), schema)
    fps = target.range_fingerprints("t", ["id"], ["number"], None, None, [(None, (1,)), ((1,), None)])
    assert [(n, k[0][0]) for n, k, _ in fps] == [(1, 1), (2, 3)]
    assert [k[0][1] for _, k, _ in fps] == [1, 5]        # 1^2 and 1^2 + 2^2


def test_target_identity_is_checked_against_the_live_database(monkeypatch):
    monkeypatch.setenv("RECON_TEST_TARGET", os.environ[DSN_VAR])
    with pytest.raises(TargetIdentityError, match="allowlisted target is 'not_this_database'"):
        LakebaseTargetAdapter("RECON_TEST_TARGET", "not_this_database", "public")
    assert LakebaseTargetAdapter("RECON_TEST_TARGET", _database(), "public").database == _database()
