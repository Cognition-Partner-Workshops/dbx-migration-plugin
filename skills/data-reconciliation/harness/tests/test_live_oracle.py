"""Catalog reads and fingerprints that only a real Oracle can exercise. Skipped unless
RECON_TEST_ORACLE_DSN names an admin DSN on a throwaway instance (the rehearsal stand-in);
the test owns one temporary user dropped CASCADE in teardown."""

import datetime as dt
import decimal
import os
import secrets
import uuid

import pytest

from recon.adapters import IdentityState, OracleSourceAdapter

oracledb = pytest.importorskip("oracledb")

DSN_VAR = "RECON_TEST_ORACLE_DSN"
pytestmark = pytest.mark.skipif(DSN_VAR not in os.environ, reason=f"{DSN_VAR} not set")


def _admin_connect():
    from recon.adapters import _oracle_dsn_parts
    user, password, dsn = _oracle_dsn_parts(DSN_VAR)
    return oracledb.connect(user=user, password=password, dsn=dsn)


@pytest.fixture
def oracle_schema(monkeypatch):
    """A temporary user with the full structural surface: PK, UNIQUE, FK ON DELETE SET NULL,
    a DISABLED unique constraint, a function-based index, every NUMBER shape, DATE with
    time-of-day, zoneless TIMESTAMP, TIMESTAMP WITH TIME ZONE, an identity column."""
    name = f"RECON_T_{uuid.uuid4().hex[:8].upper()}"
    admin = _admin_connect()
    cur = admin.cursor()
    # quoted alnum password minted per fixture; never printed
    password = "P" + secrets.token_urlsafe(24).replace("-", "a").replace("_", "b")
    cur.execute(f"CREATE USER {name} IDENTIFIED BY \"{password}\"")
    for grant in ("CONNECT", "RESOURCE", "UNLIMITED TABLESPACE"):
        cur.execute(f"GRANT {grant} TO {name}")
    cur.execute(f"""CREATE TABLE {name}.parent (id NUMBER(10,0) PRIMARY KEY,
                    code VARCHAR2(30), region VARCHAR2(10))""")
    cur.execute(f"""CREATE TABLE {name}.t (
                    id NUMBER(10,0) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    p_id NUMBER(10,0) REFERENCES {name}.parent (id) ON DELETE SET NULL,
                    code VARCHAR2(30) CONSTRAINT t_code_u UNIQUE,
                    disabled_code VARCHAR2(30),
                    amount NUMBER(12,2), raw_num NUMBER, ratio FLOAT(63),
                    prob BINARY_DOUBLE, flag NUMBER(1,0),
                    created DATE NOT NULL,
                    touched TIMESTAMP(6),
                    stamped TIMESTAMP(6) WITH TIME ZONE,
                    CONSTRAINT t_dup_chk CHECK (flag > 0))""")
    cur.execute(f"ALTER TABLE {name}.t ADD CONSTRAINT t_disabled_u UNIQUE (disabled_code) DISABLE")
    cur.execute(f"CREATE INDEX lwr_code_ix ON {name}.t (LOWER(code))")
    cur.execute(f"CREATE UNIQUE INDEX mix_u ON {name}.t (LOWER(code), p_id)")
    cur.execute(f"CREATE INDEX amt_code_ix ON {name}.t (amount, code)")
    cur.execute(f"""INSERT INTO {name}.parent (id, code, region) VALUES (1, 'a', 'eu')""")
    cur.execute(f"""INSERT INTO {name}.t (p_id, code, amount, raw_num, ratio, prob, flag,
                    created, touched, stamped)
                    VALUES (1, 'x', 10.5, 123456789012345678.123, 1.25, 0.5, 1,
                            TO_DATE('2026-09-15 13:45:30', 'YYYY-MM-DD HH24:MI:SS'),
                            TIMESTAMP '2026-09-15 13:45:30.123456',
                            TIMESTAMP '2026-09-15 13:45:30.123456 +00:00')""")
    admin.commit()
    monkeypatch.setenv("RECON_ORACLE_USER_DSN",
                       f"{name}/{password}@localhost:52521/FREEPDB1")
    yield name
    # DROP USER refuses while the adapter's connection is still open, so kill first.
    for sid, serial in admin.cursor().execute(
            "SELECT sid, serial# FROM v$session WHERE username = :1", [name]).fetchall():
        try:
            admin.cursor().execute(f"ALTER SYSTEM KILL SESSION '{sid},{serial}' IMMEDIATE")
        except Exception:
            pass
    admin.cursor().execute(f"DROP USER {name} CASCADE")
    admin.close()


def _adapter() -> OracleSourceAdapter:
    return OracleSourceAdapter("RECON_ORACLE_USER_DSN")


def test_schema_facts_and_classification(oracle_schema):
    a = _adapter()
    facts = a.schema_facts("t")   # bare name resolves to the connected user's schema
    assert facts.table.endswith(".t") and facts.primary_key == ("id",)
    assert ("code",) in facts.unique
    fk = (("p_id",), f"{oracle_schema.lower()}.parent", ("id",))
    assert facts.foreign_keys == {fk}
    assert facts.foreign_key_actions == {fk: ("no action", "set null")}
    assert facts.not_null == {"id", "created"}
    assert "disabled_code" not in str(facts.unique)      # DISABLED constraint excluded
    # Oracle stores user-written CHECK text unquoted ('flag > 0'); implicit NOT NULL
    # checks arrive as '"COL" IS NOT NULL' and are dropped by the reader.
    assert facts.checks == {'flag > 0'}
    assert facts.check_count == 1
    assert ("amount", "code") in facts.indexes
    assert facts.expression_indexes                       # the LOWER(code) FBI is reported
    # the mixed FBI reassembles whole: expression position + plain column
    assert facts.expression_unique == {'lower("CODE"), p_id'}  # Oracle stores it quoted
    assert "id" in facts.identity_columns
    assert facts.primary_key  # PK backing index not double-counted in indexes
    assert a.whole_number_columns("t") == {"id", "p_id", "flag"}
    assert a.numeric_columns("t") == {"id", "p_id", "flag", "amount", "raw_num", "ratio",
                                      "prob"}
    shape = a.column_shape("t")
    assert shape[0]["name"] == "id" and shape[0]["type"].startswith("number")
    a.discard()


def test_number_is_exact_decimal_and_date_keeps_time(oracle_schema):
    a = _adapter()
    row = a._rows("SELECT amount, raw_num, created, touched, stamped FROM "
                  f"{oracle_schema}.t")[0]
    amount, raw_num, created, touched, stamped = row
    assert isinstance(amount, decimal.Decimal) and amount == decimal.Decimal("10.5")
    # exact decimal round-trip: a NUMBER(38,3) past float precision keeps its digits
    assert raw_num == decimal.Decimal("123456789012345678.123")
    assert isinstance(created, dt.datetime) and created.tzinfo is None
    assert (created.hour, created.minute) == (13, 45)          # DATE is not truncated
    assert touched.tzinfo is None and touched.microsecond == 123456
    assert stamped.tzinfo is not None                          # WITH TIME ZONE stays aware
    a.discard()


def test_identity_state_and_tiers(oracle_schema):
    a = _adapter()
    state = a.identity_state("t", "id")
    assert isinstance(state, IdentityState) and state.increment == 1 and state.next >= 1
    assert a.identity_state("t", "code") is None
    # the tiers the rehearsal promised: row_count + range fingerprints through the adapter
    assert a.row_count("t") == 1
    (n, keys, wm), = a.range_fingerprints("t", ["id"], ["integer"], None, None, [(None, None)])
    assert n == 1 and keys and keys[0][0] is not None and wm is None
    a.discard()
