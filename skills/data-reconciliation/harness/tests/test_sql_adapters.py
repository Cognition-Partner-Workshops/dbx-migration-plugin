"""SQL adapter statements, identifiers, literals and the source-family registry, offline."""
import datetime as dt

import pytest
from recon import adapters, cli
from recon.adapters import (
    SOURCE_ADAPTERS,
    LakebaseTargetAdapter,
    TargetIdentityError,
    quote_ident,
)
from recon.cli import SOURCE_FAMILIES
from recon.config import ConfigError
from recon.transactional import _applied_predicate, _newer_predicate

from tests.loans import (
    PLUS2,
    _db,
    _NoSnapshotAdapter,
    _PostgresLike,
    _rowversion,
    _SqlServerLike,
    _StubConn,
)

UNTESTED_FAMILIES = ("redshift", "snowflake", "teradata", "oracle")


def test_every_cli_family_is_either_live_tested_or_refused():
    assert set(SOURCE_FAMILIES) == set(SOURCE_ADAPTERS)
    assert set(SOURCE_FAMILIES) - set(UNTESTED_FAMILIES) == {"sqlserver", "postgres", "databricks"}


@pytest.mark.parametrize("family", UNTESTED_FAMILIES)
def test_untested_source_families_fail_fast_before_any_driver_or_the_target_is_touched(family, tmp_path, monkeypatch):
    monkeypatch.delenv("SOURCE_DSN", raising=False)
    with pytest.raises(NotImplementedError, match=f"^{family} source adapter is untested; see SKILL.md$"):
        SOURCE_ADAPTERS[family]("SOURCE_DSN")
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text('{"catalogs": ["mig"]}')
    for loader in ("load_mapping_spec", "load_tolerances", "load_canon_rules"):
        monkeypatch.setattr(cli, loader, lambda *a: None)
    monkeypatch.setattr(adapters, "DatabricksTargetAdapter", lambda *a: pytest.fail("target was built"))
    with pytest.raises(SystemExit, match=f"^--family {family}: {family} source adapter is untested; see SKILL.md$"):
        cli.main(["run", "--unit", "u", "--family", family, "--mapping", "m", "--tolerances", "t",
                  "--canonicalization", "c", "--mode", "fixture", "--source-dsn-secret", "SOURCE",
                  "--target-secret", "TARGET", "--target-catalog", "mig", "--target-schema", "s",
                  "--out", str(tmp_path / "out")])


@pytest.mark.parametrize("name, quote, expected", [
    ("loans", '"', '"loans"'),
    ('lo"ans"; DROP TABLE x; --', '"', '"lo""ans""; DROP TABLE x; --"'),
    ("loan`s", "`", "`loan``s`"),
    ("Mixed Case", "`", "`Mixed Case`"),
])
def test_identifiers_are_delimited_with_the_delimiter_doubled(name, quote, expected):
    assert quote_ident(name, quote) == expected


@pytest.mark.parametrize("name", ["", "a.b", "x\x00y"])
def test_identifiers_that_are_not_one_name_are_refused(name):
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        quote_ident(name, '"')


def test_lakebase_target_validates_and_escapes_identifiers_before_it_connects(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("T", "dsn-under-test")
    connects = []
    monkeypatch.setattr(psycopg, "connect", lambda dsn: connects.append(dsn) or _db("db"))
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        LakebaseTargetAdapter("T", "db", "public.loan_servicing")
    assert connects == []
    target = LakebaseTargetAdapter("T", "db", 'loan"servicing')
    assert target._q('lo"ans') == '"loan""servicing"."lo""ans"'
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        target._q("public.loans")


def test_databricks_target_validates_catalog_and_schema_before_it_connects(monkeypatch):
    connects = []
    monkeypatch.setattr(adapters, "_databricks_connect", lambda name: connects.append(name))
    with pytest.raises(ConfigError, match="invalid SQL identifier"):
        adapters.DatabricksTargetAdapter("D", "", "silver")
    assert connects == []


def test_lakebase_target_binds_the_connection_to_the_allowlisted_database(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    monkeypatch.setenv("T", "dsn-under-test")
    conns = []

    def connect(dsn):
        conns.append(_db("loan_servicing_prod"))
        return conns[-1]
    monkeypatch.setattr(psycopg, "connect", connect)
    with pytest.raises(TargetIdentityError, match="'loan_servicing_prod'.*'lakebase_rehearsal'"):
        LakebaseTargetAdapter("T", "lakebase_rehearsal", "loan_servicing")
    assert conns[0].closed is True
    assert [s for s, _ in conns[0].executed] == ["SELECT current_database()"]
    target = LakebaseTargetAdapter("T", "loan_servicing_prod", "loan_servicing")
    assert target.database == "loan_servicing_prod" and conns[1].closed is False


class _LiveTable:
    """DB-API stand-in for an engine with no snapshot: answers the marker aggregate and a
    per-table write counter, and lets a test schedule a write between any two statements."""

    def __init__(self):
        self.count, self.max_wm, self.token = 100, 500, 7
        self.executed: list[str] = []
        self.before_statement: dict = {}

    def write_below_max(self):
        self.token += 1  # an UPDATE that leaves COUNT(*) and MAX(watermark) untouched

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, params=()):
                hook = conn.before_statement.pop(len(conn.executed), None)
                if hook:
                    hook()
                conn.executed.append(sql)
                self.sql = sql

            def fetchall(self):
                if self.sql.startswith("TOKEN"):
                    return [(conn.token,)]
                return [(conn.count, conn.max_wm)]
        return Cur()


def test_a_write_between_the_marker_row_and_its_token_is_not_baked_into_the_baseline():
    conn = _LiveTable()
    conn.before_statement[2] = conn.write_below_max  # after the aggregate, before the token
    adapter = _NoSnapshotAdapter(conn)
    assert adapter.open_window() == "none"
    marker = adapter.window_marker("dbo.loan", ["loan_id"], "modified_date")
    # first bracket (7, row, 8) disagreed and was discarded; the retry read (8, row, 8)
    assert marker == (100, 500, 8)
    assert [s[:5] for s in conn.executed] == ["TOKEN", "SELEC", "TOKEN", "TOKEN", "SELEC", "TOKEN"]
    assert adapter.window_marker("dbo.loan", ["loan_id"], "modified_date") == marker


def test_a_bracket_that_never_settles_yields_a_marker_no_close_can_match():
    conn = _LiveTable()
    for i in (2, 5, 8):  # a write inside every one of the three brackets
        conn.before_statement[i] = conn.write_below_max
    adapter = _NoSnapshotAdapter(conn)
    adapter.open_window()
    opened = adapter.window_marker("dbo.loan", ["loan_id"], "modified_date")
    assert opened == (100, 500, 9, 10)
    closed = adapter.window_marker("dbo.loan", ["loan_id"], "modified_date")
    assert closed == (100, 500, 10) and closed != opened


def test_a_pinned_snapshot_marker_reads_no_token():
    conn = _LiveTable()
    adapter = _NoSnapshotAdapter(conn)
    adapter.isolation = "repeatable_read"
    assert adapter.window_marker("dbo.loan", ["loan_id"], "modified_date") == (100, 500)
    assert all(not s.startswith("TOKEN") for s in conn.executed)


def test_sql_adapter_excludes_in_flight_keys_in_one_bound_statement():
    conn = _StubConn()
    adapter = _NoSnapshotAdapter(conn)
    out = adapter.table_aggregates_excluding("dbo.loans", ["current_balance"], ["current_balance"],
                                             ["loan_id"], [(11,), (12,)], where="status = 'A'")
    sql, params = conn.executed[-1]
    assert sql.endswith("FROM dbo.loans WHERE (status = 'A') AND NOT (loan_id IN (?, ?))")
    assert params == (11, 12)
    assert out["current_balance"]["sum"] == 12 and out["current_balance"]["count"] == 3
    adapter.table_aggregates_excluding("dbo.x", ["v"], [], ["a", "b"], [(1, "p"), (2, "q")])
    sql, params = conn.executed[-1]
    assert sql.endswith("FROM dbo.x WHERE NOT ((a = ? AND b = ?) OR (a = ? AND b = ?))")
    assert params == (1, "p", 2, "q")


def test_sql_adapter_exclusion_capacity_counts_every_key_component():
    conn = _StubConn()
    adapter = _NoSnapshotAdapter(conn)
    width = 7
    cap = adapter.exclusion_capacity(width)
    assert cap == 2000 // width == 285
    keys = [tuple(range(i, i + width)) for i in range(cap)]
    adapter.table_aggregates_excluding("dbo.wide", ["v"], [], [f"k{j}" for j in range(width)], keys)
    sql, params = conn.executed[-1]
    assert sql.count("?") == len(params) == cap * width <= adapter.max_params
    statements = len(conn.executed)
    with pytest.raises(ValueError, match="exceed"):
        adapter.table_aggregates_excluding("dbo.wide", ["v"], [], [f"k{j}" for j in range(width)],
                                           keys + [tuple(range(cap, cap + width))])
    assert len(conn.executed) == statements   # never split into several statements


HWM = dt.datetime(2026, 9, 8, 18, 43, 52, 164112)  # noqa: DTZ001  naive = UTC by contract
RV = _rowversion(2001)


@pytest.mark.parametrize("engine, hwm, bound, strict", [
    # a datetime bound starts at the next microsecond: an engine that stores more precision than
    # the driver returns (datetime2(7)) would otherwise count every row sharing the applied
    # microsecond as in flight; a counter is compared as it is
    (None, HWM, "'2026-09-08 18:43:52.164113'", False),
    (None, 41, "41", True),
    (_PostgresLike, 41, "41", True),
    (_NoSnapshotAdapter, 41, "41", True),
    # Postgres: a timestamptz column would read a bare literal in the session TimeZone
    (_PostgresLike, HWM.replace(tzinfo=PLUS2), "'2026-09-08 16:43:52.164113+00:00'", False),
    # SQL Server datetime rejects an offset, so the zone-less engines keep the bare UTC form...
    (_NoSnapshotAdapter, HWM.replace(tzinfo=PLUS2), "'2026-09-08 16:43:52.164113'", False),
    # ...and convert the literal to the column's type first: datetime (3.33 ms ticks) and
    # smalldatetime (minutes) would round the next-microsecond bound back onto the watermark, so
    # rows equal to the applied HWM would count as in flight and leave the tier 2 aggregates
    (_SqlServerLike, HWM.replace(microsecond=167000), "CAST('2026-09-08 18:43:52.167001' AS datetime2(7))", False),
    # a rowversion is a binary literal in the engine's own syntax; a bigint counter a plain number
    (_SqlServerLike, RV, "0x00000000000007d1", True),
    (_PostgresLike, RV, "'\\x00000000000007d1'::bytea", True),
    (_SqlServerLike, 2001, "2001", True),
])
def test_in_flight_and_applied_predicates_render_the_bound_for_the_engine(engine, hwm, bound, strict):
    render = {"render": engine(_StubConn()).watermark_literal} if engine else {}
    newer, applied = (">", "<=") if strict else (">=", "<")
    assert _newer_predicate("wm", hwm, **render) == f"wm {newer} {bound}"
    assert _applied_predicate("wm", hwm, **render) == f"(wm {applied} {bound} OR wm IS NULL)"


def test_literal_and_digest_shapes_outside_a_predicate():
    mssql, base = _SqlServerLike(_StubConn()), _NoSnapshotAdapter(_StubConn())
    assert mssql.watermark_literal(bytearray(RV)) == mssql.watermark_literal(memoryview(RV)) == "0x00000000000007d1"
    assert mssql.watermark_literal(dt.date(2026, 1, 1)) == "'2026-01-01'"
    # only whole numbers (and datetimes) digest exactly: a fractional, binary or text key streams
    digest, square = base._digest_sql("k", "integer")
    assert digest == "CAST(k AS DECIMAL(38,0))" and "DECIMAL(38,6)" not in square
    assert base._digest_sql("k", "number") is base._digest_sql("k", "binary") is base._digest_sql("k", "other") is None


def _contiguous(n, width=2):
    edges = [(i * 10,) * width for i in range(1, n)]
    return list(zip([None] + edges, edges + [None]))


def test_composite_key_fingerprints_stay_under_sql_server_parameter_limit():
    # 64 ranges x 2 key columns x (sum, sum of squares) + a watermark: the old one-CASE-per-term
    # shape bound the bounds ~8 times per range and crossed 2100 parameters on SQL Server
    # GROUP BY rng rows: (rng, count, sum a, sumsq a, sum b, sumsq b, sum wm, sumsq wm, wm nulls)
    conn = _StubConn([(1, 5, 50, 7, 50, 7, 500, 9, 0), (3, 2, 20, 3, 20, 3, 200, 4, 1)])
    adapter = _NoSnapshotAdapter(conn)
    out = adapter.range_fingerprints("dbo.t", ["a", "b"], ["integer", "integer"], "version_no",
                                     "integer", _contiguous(64))
    (sql, params), = conn.executed
    # a lexicographic 2-column bound is 3 parameters; 62 two-sided ranges + 2 open-ended edges
    assert len(params) == 62 * 6 + 2 * 3 and sql.count("?") == len(params)
    assert sql.startswith("SELECT rng, COUNT(*), SUM(") and sql.endswith("WHERE rng IS NOT NULL GROUP BY rng")
    assert sql.count(" THEN 63 END AS rng") == 1 and " THEN 64 " not in sql
    assert len(out) == 64
    assert out[1] == (5, ((50, 7), (50, 7)), (500, 9, 0))
    assert out[3] == (2, ((20, 3), (20, 3)), (200, 4, 1))
    assert out[0] == (0, ((0, 0), (0, 0)), (0, 0, 0)) and out[63] == out[0]


def test_range_fingerprints_split_into_statements_when_ranges_exceed_the_parameter_budget():
    conn = _StubConn([(0, 1, 1, 1, 1, 1)])
    adapter = _NoSnapshotAdapter(conn)
    adapter.max_params = 20
    out = adapter.range_fingerprints("dbo.t", ["a", "b"], ["integer", "integer"], None, None,
                                     _contiguous(50))
    # 6 parameters per range -> 3 ranges per statement -> 17 statements, every range answered
    assert len(conn.executed) == 17 and all(len(p) <= 20 for _, p in conn.executed)
    assert len(out) == 50 and out[0] == (1, ((1, 1), (1, 1)), None)
