"""Catalog reads that only a real SQL Server can exercise. Skipped unless RECON_TEST_SQLSERVER_ODBC
names a throwaway database (the rehearsal fixture, never a legacy estate); the test owns one
temporary schema and drops it. The fixture is the only thing written to."""

import os
import time
import uuid

import pytest
from recon.adapters import SqlServerSourceAdapter
from recon.transactional import _applied_predicate, _newer_predicate, _successor
from recon.watermarks import in_form_of, instant

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
    assert facts.checks == {"([Amount]>=(0))"}                 # the NOCHECK one is not enforced
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


def test_rowversion_bounds_compare_the_counter_byte_for_byte(schema, monkeypatch):
    # pyodbc delivers rowversion as 8 bytes; the predicate must carry them as a binary literal
    # of the same width, whether the applied watermark came back as bytes (target keeps the
    # bytes) or as the bigint a Lakebase target stores instead (in_form_of widens it again)
    monkeypatch.setenv("RECON_TEST_SOURCE", os.environ[DSN_VAR])
    source = SqlServerSourceAdapter("RECON_TEST_SOURCE")
    conn = pyodbc.connect(os.environ[DSN_VAR], autocommit=True)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {schema}.RV (Id INT PRIMARY KEY, RV ROWVERSION)")
    for i in (1, 2, 3):
        cur.execute(f"INSERT INTO {schema}.RV (Id) VALUES ({i})")
    cur.execute(f"UPDATE {schema}.RV SET Id = Id WHERE Id = 1")  # row 1 now carries the newest counter
    try:
        marks = dict(cur.execute(f"SELECT Id, RV FROM {schema}.RV").fetchall())
        assert all(isinstance(v, bytes) and len(v) == 8 for v in marks.values())
        assert source.max_watermark(f"{schema}.RV", "RV") == marks[1]
        assert instant(marks[1]) > instant(marks[3]) > instant(marks[2])
        for hwm in (marks[3], in_form_of(instant(marks[3]), marks[1])):
            newer = _newer_predicate("RV", hwm, source.watermark_literal)
            applied = _applied_predicate("RV", hwm, source.watermark_literal)
            assert newer == f"RV > 0x{marks[3].hex()}"
            assert source.row_count(f"{schema}.RV", newer) == 1, hwm
            assert source.row_count(f"{schema}.RV", applied) == 2, hwm
            assert [r["Id"] for r in source.fetch_keyed(f"{schema}.RV", ["Id"], [], where=newer)] == [1]
    finally:
        cur.execute(f"DROP TABLE {schema}.RV")
        conn.close()


def test_cdc_delete_evidence_reads_tombstones_after_a_position(schema, monkeypatch):
    # The capture instance is created on the throwaway schema only (CDC is already enabled on
    # the fixture database; a legacy estate is never enabled from the harness). Deletes are
    # listed with binary(10) positions and a server-clock age; inserts are not tombstones, a
    # capture that does not exist has no horizon, and a position past the last delete reads nothing.
    monkeypatch.setenv("RECON_TEST_SOURCE", os.environ[DSN_VAR])
    source = SqlServerSourceAdapter("RECON_TEST_SOURCE")
    conn = pyodbc.connect(os.environ[DSN_VAR], autocommit=True)
    cur = conn.cursor()
    if not cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()").fetchone()[0]:
        pytest.skip("fixture database has no CDC")
    capture = f"{schema}_T"
    cur.execute(f"CREATE TABLE {schema}.T (Id INT PRIMARY KEY, Seq INT)")
    cur.execute("EXEC sys.sp_cdc_enable_table @source_schema = ?, @source_name = 'T', "
                "@role_name = NULL, @capture_instance = ?, @supports_net_changes = 0", (schema, capture))
    try:
        for i in (1, 2, 3):
            cur.execute(f"INSERT INTO {schema}.T VALUES ({i}, {i})")
        cur.execute(f"DELETE FROM {schema}.T WHERE Id IN (1, 2)")
        cur.execute(f"DELETE FROM {schema}.T WHERE Id = 3")
        deadline = time.time() + 60
        while time.time() < deadline:  # the capture job polls the log every few seconds
            (n,) = cur.execute(f"SELECT COUNT(*) FROM cdc.{capture}_CT WHERE __$operation = 1").fetchone()
            if n == 3:
                break
            time.sleep(1)
        assert n == 3, "capture job did not harvest the deletes"
        assert source.evidence_horizon("no_such_capture") == (None, None)
        # the harness' successor is the server's: the retention check and the inclusive lower
        # bound of the delete read agree on which position follows the checkpoint (with carry)
        for tail in ("0005", "00ff", "ffff"):
            lsn = bytes.fromhex("0000003100006ac0" + tail)
            (nxt,) = cur.execute("SELECT sys.fn_cdc_increment_lsn(?)", (lsn,)).fetchone()
            assert _successor(lsn) == bytes(nxt)
        lo, hi = source.evidence_horizon(capture)
        assert isinstance(lo, bytes) and isinstance(hi, bytes) and len(lo) == len(hi) == 10 and lo < hi
        events = source.deletes_since(capture, ["Id"], lo, hi)
        assert sorted(e.key for e in events) == [(1,), (2,), (3,)]
        assert all(isinstance(e.position, bytes) and len(e.position) == 10 and lo < e.position <= hi
                   for e in events)
        assert all(0.0 <= e.age_s < 120 for e in events)
        by_key = {e.key: e.position for e in events}
        assert by_key[(1,)] == by_key[(2,)] < by_key[(3,)]  # one statement, one commit position
        later = source.deletes_since(capture, ["Id"], by_key[(2,)], hi)
        assert [(e.key, e.position) for e in later] == [((3,), by_key[(3,)])]
        if by_key[(3,)] < hi:  # only when the database logged something after the last delete
            assert source.deletes_since(capture, ["Id"], by_key[(3,)], hi) == []
        # the scope predicate is evaluated on the deleted row's before-image (Seq is the row's
        # value at delete time), and a scope the capture cannot answer errors instead of widening
        assert sorted(e.key for e in source.deletes_since(capture, ["Id"], lo, hi, "Seq >= 2")) == [(2,), (3,)]
        assert source.deletes_since(capture, ["Id"], lo, hi, "Seq > 3") == []
        with pytest.raises(pyodbc.Error, match="Invalid column name 'NotCaptured'"):
            source.deletes_since(capture, ["Id"], lo, hi, "NotCaptured = 1")
    finally:
        source.close_window()  # release the read transaction: disabling the capture drops its table
        cur.execute("EXEC sys.sp_cdc_disable_table @source_schema = ?, @source_name = 'T', "
                    "@capture_instance = ?", (schema, capture))
        cur.execute(f"DROP TABLE {schema}.T")
        conn.close()
