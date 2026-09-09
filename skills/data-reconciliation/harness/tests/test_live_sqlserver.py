"""Catalog reads that only a real SQL Server can exercise. Skipped unless RECON_TEST_SQLSERVER_ODBC
names a throwaway database (the rehearsal fixture, never a legacy estate); the test owns one
temporary schema and drops it. The fixture is the only thing written to."""

import os
import uuid

import pytest

from recon.adapters import SqlServerSourceAdapter
from recon.transactional import _applied_predicate, _newer_predicate

pyodbc = pytest.importorskip("pyodbc")

DSN_VAR = "RECON_TEST_SQLSERVER_ODBC"
pytestmark = pytest.mark.skipif(DSN_VAR not in os.environ, reason=f"{DSN_VAR} not set")


@pytest.fixture
def schema():
    name = f"recon_test_{uuid.uuid4().hex[:8]}"
    conn = pyodbc.connect(os.environ[DSN_VAR], autocommit=True)
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA {name}")
    cur.execute(f"CREATE TABLE {name}.Parent (Parent_ID INT PRIMARY KEY, Other_ID INT UNIQUE)")
    cur.execute(f"CREATE TABLE {name}.Child (Child_ID INT PRIMARY KEY, Parent_ID INT NOT NULL, "
                f"Other_ID INT, Amount DECIMAL(10,2), Status VARCHAR(10), "
                f"CONSTRAINT FK_Child_Parent FOREIGN KEY (Parent_ID) REFERENCES {name}.Parent(Parent_ID) "
                f"ON UPDATE CASCADE ON DELETE NO ACTION, "
                f"CONSTRAINT FK_Child_Other FOREIGN KEY (Other_ID) REFERENCES {name}.Parent(Other_ID), "
                f"CONSTRAINT CK_Child_Amount CHECK (Amount >= 0), "
                f"CONSTRAINT CK_Child_Status CHECK (Status IN ('open', 'closed')))")
    # disabled constraints are catalogued but enforce nothing: the legacy app writes past them
    cur.execute(f"ALTER TABLE {name}.Child NOCHECK CONSTRAINT FK_Child_Other")
    cur.execute(f"ALTER TABLE {name}.Child NOCHECK CONSTRAINT CK_Child_Status")
    cur.execute(f"INSERT INTO {name}.Parent VALUES (1, 10)")
    cur.execute(f"INSERT INTO {name}.Child VALUES (1, 1, 999, 5.00, 'weird')")  # violates both disabled ones
    try:
        yield name
    finally:
        cur.execute(f"DROP TABLE {name}.Child")
        cur.execute(f"DROP TABLE {name}.Parent")
        cur.execute(f"DROP SCHEMA {name}")
        conn.close()


def test_disabled_constraints_never_count_as_enforced(schema, monkeypatch):
    monkeypatch.setenv("RECON_TEST_SOURCE", os.environ[DSN_VAR])
    source = SqlServerSourceAdapter("RECON_TEST_SOURCE")
    facts = source.schema_facts(f"{schema}.Child")
    assert facts.primary_key == ("Child_ID",)
    fk = (("Parent_ID",), f"{schema}.Parent", ("Parent_ID",))
    assert facts.foreign_keys == {fk}
    assert facts.foreign_key_actions == {fk: ("cascade", "no action")}
    assert facts.check_count == 1
    assert facts.not_null == {"Child_ID", "Parent_ID"}


def test_whole_number_columns_come_from_the_declared_scale(schema, monkeypatch):
    # integer types and DECIMAL/NUMERIC of scale 0 are whole; MONEY (scale 4), a scaled
    # DECIMAL and FLOAT are not, however whole their current values happen to be
    conn = pyodbc.connect(os.environ[DSN_VAR], autocommit=True)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {schema}.W (Id BIGINT PRIMARY KEY, N0 NUMERIC(18,0), D2 DECIMAL(18,2), "
                f"M MONEY, F FLOAT, T TINYINT, Code VARCHAR(10))")
    cur.execute(f"INSERT INTO {schema}.W VALUES (1, 1, 1, 1, 1, 1, 'x')")
    monkeypatch.setenv("RECON_TEST_SOURCE", os.environ[DSN_VAR])
    source = SqlServerSourceAdapter("RECON_TEST_SOURCE")
    try:
        assert source.whole_number_columns(f"{schema}.W") == {"Id", "N0", "T"}
        assert source.numeric_columns(f"{schema}.W") == {"Id", "N0", "D2", "M", "F", "T"}
    finally:
        cur.execute(f"DROP TABLE {schema}.W")
        conn.close()


def test_a_nullable_unique_key_keeps_one_null_row(schema, monkeypatch):
    # SQL Server treats NULL as a value for uniqueness: a unique index admits one NULL key,
    # unlike a default Postgres unique which admits any number of them
    monkeypatch.setenv("RECON_TEST_SOURCE", os.environ[DSN_VAR])
    source = SqlServerSourceAdapter("RECON_TEST_SOURCE")
    facts = source.schema_facts(f"{schema}.Parent")
    assert facts.unique == {("Other_ID",)}
    assert facts.unique_nulls_equal == {("Other_ID",)}
    assert "Other_ID" not in facts.not_null



def test_datetime_bounds_are_exact_on_every_datetime_family_column(schema, monkeypatch):
    # rows equal to the applied watermark are applied, the next tick is in flight, on datetime
    # (3.33 ms ticks), smalldatetime (minutes) and datetime2(7) alike; a bare literal would be
    # converted to the column's type first (datetime rejects the 6-digit form outright)
    monkeypatch.setenv("RECON_TEST_SOURCE", os.environ[DSN_VAR])
    source = SqlServerSourceAdapter("RECON_TEST_SOURCE")
    conn = pyodbc.connect(os.environ[DSN_VAR], autocommit=True)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {schema}.Stamps (Id INT PRIMARY KEY, D DATETIME, SD SMALLDATETIME, D2 DATETIME2(7))")
    cur.execute(f"INSERT INTO {schema}.Stamps VALUES (1, '2026-01-01 10:00:00.163', '2026-01-01 10:00:00', '2026-01-01 10:00:00.1630000')")
    cur.execute(f"INSERT INTO {schema}.Stamps VALUES (2, '2026-01-01 10:00:00.167', '2026-01-01 10:01:00', '2026-01-01 10:00:00.1670000')")
    cur.execute(f"INSERT INTO {schema}.Stamps VALUES (3, '2026-01-01 10:00:00.170', '2026-01-01 10:02:00', '2026-01-01 10:00:00.1700000')")
    try:
        (d, sd, d2) = cur.execute(f"SELECT D, SD, D2 FROM {schema}.Stamps WHERE Id = 2").fetchone()
        for column, hwm in (("D", d), ("SD", sd), ("D2", d2)):
            newer = _newer_predicate(column, hwm, source.watermark_literal)
            applied = _applied_predicate(column, hwm, source.watermark_literal)
            assert source.row_count(f"{schema}.Stamps", newer) == 1, column
            assert source.row_count(f"{schema}.Stamps", applied) == 2, column
    finally:
        cur.execute(f"DROP TABLE {schema}.Stamps")
        conn.close()
