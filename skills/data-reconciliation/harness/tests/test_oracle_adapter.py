"""Oracle source adapter: dialect, DSN parsing and catalog classification, no network."""

from __future__ import annotations

import decimal
import re
import sys
import types

import pytest

import recon.adapters as adapters
from recon.adapters import IdentityState, OracleSourceAdapter, _SqlAdapterBase, _oracle_dsn_parts
from recon.config import ConfigError


_NUMERIC = ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE")


class _OracleCatalogConn:
    """DB-API stand-in answering the ALL_* catalog reads by SQL substring. all_tab_columns rows
    are stored in full (name, dtype, length, precision, scale, nullable) and sliced to the
    SELECT list the query actually asks for, like the real view."""

    def __init__(self, tables=None):
        # keyed by the view name appearing in the query
        self.tables = dict(tables or {})
        self.executed = []

    def cursor(self):
        conn = self

        class Cur:
            description = None

            def execute(self, sql, params=None):
                conn.executed.append((sql, params))

            def fetchall(self):
                sql = conn.executed[-1][0]
                m = re.search(r"FROM\s+(\w+)", sql)
                if not m or m[1] not in conn.tables:
                    return []
                view, rows = m[1], conn.tables[m[1]]
                if view == "all_tab_columns":
                    # stored rows: (name, dtype, data_length, char_length, char_used,
                    #               precision, scale, nullable)
                    if "data_scale = 0" in sql:
                        return [(r[0],) for r in rows if r[1] == "NUMBER" and r[6] == 0]
                    if "data_type IN" in sql:
                        return [(r[0],) for r in rows if r[1] in _NUMERIC]
                    if "column_name, nullable" in sql:
                        return [(r[0], r[7]) for r in rows]
                return [tuple(r) for r in rows]
        return Cur()

    def close(self):
        pass


class _OracleLike(OracleSourceAdapter):
    """The catalog-reader half of the adapter with the connect skipped (the psycopg/pyodbc
    tests use the same bypass for _PostgresBase/SqlServerSourceAdapter)."""

    def __init__(self, conn):
        _SqlAdapterBase.__init__(self, conn)
        self._schema = "APP"


def test_oracle_dsn_parses_both_secret_forms(monkeypatch):
    monkeypatch.setenv("ORA_DSN", "oracle://scott:tiger@db.example.com:1521/FREEPDB1")
    assert _oracle_dsn_parts("ORA_DSN") == ("scott", "tiger", "db.example.com:1521/FREEPDB1")
    monkeypatch.setenv("ORA_DSN", "scott/tiger@db.example.com:1521/FREEPDB1")
    assert _oracle_dsn_parts("ORA_DSN") == ("scott", "tiger", "db.example.com:1521/FREEPDB1")


def test_oracle_dsn_rejects_malformed_and_never_leaks_the_secret(monkeypatch):
    monkeypatch.delenv("MISSING_DSN", raising=False)
    with pytest.raises(ConfigError):
        _oracle_dsn_parts("MISSING_DSN")
    monkeypatch.setenv("ORA_DSN", "definitely-not-a-dsn-with-a-password")
    with pytest.raises(ConfigError) as exc:
        _oracle_dsn_parts("ORA_DSN")
    assert "definitely-not" not in str(exc.value)


def test_oracle_dialect_strings():
    a = OracleSourceAdapter
    assert a.family == "oracle" and a.paramstyle == "named" and a.max_params == 1000
    assert a.snapshot_sql == "SET TRANSACTION READ ONLY" and a.snapshot_reset_sql == "COMMIT"
    assert a.change_token_sql is None and a.datetime_digest_sql is None
    assert a.integer_digest_sql == "CAST({col} AS NUMBER(38,0))"
    assert a.mod_sql == "MOD({x}, {m})"
    assert a.datetime_bound_sql == "TO_TIMESTAMP({lit}, 'YYYY-MM-DD HH24:MI:SS.FF6')"
    assert not adapters.is_untested_source_family("oracle")


def test_numeric_and_whole_number_classification():
    conn = _OracleCatalogConn({
        "all_tab_columns": [
            # (name, dtype, data_length, char_length, char_used, precision, scale, nullable)
            ("ID", "NUMBER", 22, 0, "B", 38, 0, "N"),          # whole
            ("CNT", "NUMBER", 22, 0, "B", 10, 0, "N"),         # whole
            ("AMOUNT", "NUMBER", 22, 0, "B", 12, 2, "Y"),      # numeric, not whole
            ("RAW_NUM", "NUMBER", 22, 0, "B", None, None, "Y"),  # unscaled: NOT whole
            ("RATIO", "FLOAT", 22, 0, "B", 126, None, "Y"),    # numeric, not whole
            ("PROB", "BINARY_DOUBLE", 8, 0, "B", None, None, "Y"),
            ("NAME", "VARCHAR2", 100, 25, "C", None, None, "N"),  # neither
        ]})
    a = _OracleLike(conn)
    assert a.whole_number_columns("APP.T") == {"id", "cnt"}
    assert a.numeric_columns("APP.T") == {"id", "cnt", "amount", "raw_num", "ratio", "prob"}
    joined = " ".join(sql for sql, _ in conn.executed)
    assert "all_tab_columns" in joined and "data_scale = 0" in joined


def _schema_facts_conn():
    return _OracleCatalogConn({
        "all_constraints": [
            # P on ID
            ("PK_T", "P", "ID", 1, None, None, None, None, None),
            # U on CODE
            ("U_T_CODE", "U", "CODE", 1, None, None, None, None, None),
            # R on P_ID -> APP.P(ID) ON DELETE SET NULL
            ("FK_T_P", "R", "P_ID", 1, "APP", "P", "ID", "SET NULL", None),
            # implicit NOT NULL check (dropped) and a real CHECK (kept)
            ("SYS_C001", "C", None, None, None, None, None, None, '"DUP" IS NOT NULL'),
            ("CK_T_DUP", "C", None, None, None, None, None, None, '"DUP" > 0'),
        ],
        "all_tab_columns": [("ID", "NUMBER", 22, 0, "B", 10, 0, "N"),
                              ("CODE", "VARCHAR2", 30, 30, "C", None, None, "Y"),
                              ("P_ID", "NUMBER", 22, 0, "B", 10, 0, "Y"),
                              ("DUP", "NUMBER", 22, 0, "B", 3, 0, "N")],
        "all_indexes": [
            ("T_CODE_U", "UNIQUE", "CODE"),
            ("T_REGION_IX", "NONUNIQUE", "REGION"),
        ],
        # one mixed FBI (its FROM is all_ind_columns joined to all_ind_expressions):
        # position 1 an expression (SYS_NC col), position 2 a plain column
        "all_ind_columns": [("MIXED_U", 1, "SYS_NC00005$", "LOWER(CODE)", "UNIQUE"),
                            ("MIXED_U", 2, "TENANT_ID", None, "UNIQUE")],
        "all_triggers": [("TRG_T", "AFTER EACH ROW", "INSERT")],
        "all_tab_privs": [("READER1", "SELECT")],
        "all_tab_identity_cols": [("ID",)],
    })


def test_schema_facts_from_catalog_rows():
    conn = _schema_facts_conn()
    a = _OracleLike(conn)
    facts = a.schema_facts("APP.T")
    assert facts.table == "app.t"
    assert facts.primary_key == ("id",)
    assert ("code",) in facts.unique          # U constraint
    assert facts.unique_nulls_equal == set()  # Oracle all-NULL keys are distinct
    fk = (("p_id",), "app.p", ("id",))
    assert facts.foreign_keys == {fk}
    assert facts.foreign_key_actions == {fk: ("no action", "set null")}
    assert facts.not_null == {"id", "dup"}    # implicit NOT NULL check not double-counted
    assert facts.checks == {'"DUP" > 0'} and facts.check_count == 1
    assert facts.indexes == {("region",)}
    # the composite FBI is reassembled whole: expression position + plain column
    assert facts.expression_unique == {'lower(code), tenant_id'}
    assert facts.triggers == {"trg_t": ("after", ("insert",), "row")}
    assert facts.grants == {"reader1": frozenset({"select"})}
    assert facts.grants_effective is False
    assert facts.identity_columns == {"id"}
    joined = " ".join(sql for sql, _ in conn.executed)
    # the filtering that excludes disabled/unvalidated constraints and FBIs lives in the SQL
    assert "status = 'ENABLED'" in joined and "validated = 'VALIDATED'" in joined
    assert "FUNCTION-BASED" in joined


def test_identity_state_from_all_sequences():
    conn = _OracleCatalogConn({"all_tab_identity_cols": [(41, 1)]})
    a = _OracleLike(conn)
    assert a.identity_state("APP.T", "ID") == IdentityState(41, 1)
    conn.tables["all_tab_identity_cols"] = []
    assert a.identity_state("APP.T", "ID") is None


def test_column_shape_orders_by_column_id():
    conn = _OracleCatalogConn({"all_tab_columns": [
        ("ID", "NUMBER", 22, 0, "B", 38, 0, "N"),
        # CHAR_USED='C' uses char_length (chars), not data_length (bytes)
        ("NAME", "VARCHAR2", 100, 25, "C", None, None, "Y"),
        ("DUE", "TIMESTAMP(6) WITH TIME ZONE", 11, 0, "B", None, None, "Y"),
    ]})
    a = _OracleLike(conn)
    shape = a.column_shape("APP.T")
    assert [c["name"] for c in shape] == ["id", "name", "due"]
    assert shape[0]["type"] == "number(38,0)" and shape[0]["nullable"] is False
    assert shape[1]["type"] == "varchar2(25)"
    assert shape[2]["type"] == "timestamptz(6)"  # WITH TIME ZONE normalizes to the tz form
    sql = conn.executed[-1][0]
    assert "ORDER BY column_id" in sql


def test_bare_table_uses_the_connected_schema_and_names_fold_upper():
    conn = _OracleCatalogConn()
    conn.tables["all_tab_columns"] = [("ID", "NUMBER", 22, 0, "B", 38, 0, "N")]
    a = _OracleLike(conn)
    a.numeric_columns("T")
    sql, params = conn.executed[-1]
    assert params == {"1": "APP", "2": "T"}   # unquoted folds upper, no quotes -> catalog case


def test_connect_wires_the_typehandler_and_never_logs_the_secret(monkeypatch):
    seen = {}

    class _Var:
        def __init__(self, typ, **kw):
            self.typ, self.kw = typ, kw

    class _Cur:
        def execute(self, sql, params=None):
            seen.setdefault("session_sql", []).append(sql)

        @property
        def arraysize(self):
            return 100

        def var(self, typ, **kw):
            return _Var(typ, **kw)

    class _Conn:
        def __init__(self):
            self.outputtypehandler = None

        def cursor(self):
            return _Cur()

    fake = types.SimpleNamespace(
        DB_TYPE_NUMBER=object(), DB_TYPE_TIMESTAMP_TZ=object(),
        DB_TYPE_TIMESTAMP_LTZ=object(), connect=lambda **kw: seen.setdefault("connect", kw) or _Conn())
    fake.connect = lambda **kw: (seen.update(connect=kw), conn_holder[0])[1]
    conn_holder = [_Conn()]
    monkeypatch.setitem(sys.modules, "oracledb", fake)
    monkeypatch.setenv("ORA_SECRET", "oracle://scott:s3cr3t@host:1521/SVC")
    adapter = OracleSourceAdapter("ORA_SECRET")
    assert seen["connect"] == {"user": "scott", "password": "s3cr3t", "dsn": "host:1521/SVC"}
    handler = conn_holder[0].outputtypehandler
    assert handler is not None
    cur = _Cur()
    v = handler(cur, "AMOUNT", fake.DB_TYPE_NUMBER, 22, 12, 2)
    assert v.typ is decimal.Decimal
    assert handler(cur, "NAME", object(), 10, None, None) is None
    assert any("TIME_ZONE" in s for s in seen["session_sql"])
    adapter._conn.close = lambda: None  # nothing to clean up on the fake


class _IterConn:
    """Minimal iterable cursor returning UPPER-cased description names, as Oracle does."""

    def __init__(self, rows, names):
        self.rows, self.names, self.executed = list(rows), names, []

    def cursor(self):
        conn = self

        class Cur:
            description = [(n,) for n in conn.names]

            def execute(self, sql, params=None):
                conn.executed.append((sql, params))
                return self

            def __iter__(self):
                return iter(conn.rows)

            def fetchall(self):
                return conn.rows
        return Cur()

    def close(self):
        pass


def test_fetch_keyed_remaps_upper_names_to_the_specs_spelling():
    for spec_cols, want in ((["id", "amount"], {"id", "amount"}),
                            (["ID", "AMOUNT"], {"ID", "AMOUNT"})):
        conn = _IterConn([(1, 5.0), (2, 7.0)], names=["ID", "AMOUNT"])
        a = _OracleLike(conn)
        rows = list(a.fetch_keyed("APP.T", ["id"], spec_cols))
        assert rows and all(set(r) == want for r in rows)
        sql = conn.executed[-1][0]
        assert "ORDER BY id" in sql


def test_result_names_default_hook_is_identity():
    a = _OracleLike(_IterConn([], []))
    assert a._result_names([("X",), ("Y",)]) == ["X", "Y"]
    assert a._result_names([("X",)], ["x"]) == ["x"]


def test_run_query_names_use_the_hook_too():
    conn = _IterConn([(7,)], names=["AMOUNT"])
    a = _OracleLike(conn)
    out = a.run_query("SELECT amount FROM APP.T")
    assert out == [{"AMOUNT": 7}]
