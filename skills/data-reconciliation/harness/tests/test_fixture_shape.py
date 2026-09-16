"""Wave 0 fixture shape check: the fixture copy children develop against must have the source's
shape (column names, types, nullability) and a sample cardinality that exercises the mapping,
not merely exist. A gap is a listed finding; what could not be read is `unsupported`."""

import json
from pathlib import Path

import pytest
from recon import cli
from recon.config import ConfigError, FieldMapping, MappingSpec, ObjectMapping
from recon.fixture_shape import compare_fixture, load_check

from tests.fakes import FakeSource

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "example_fixture_shape"

SPEC = MappingSpec(version="map-v1", objects=[ObjectMapping(
    object="orders", root_table="app.orders", key_source=["order_id"], key_target="order_id",
    fields=[FieldMapping("order_id", "order_id", "bigint", "long"),
            FieldMapping("status", "status", "varchar(12)", "string"),
            FieldMapping("occurred_at", "occurred_at", "timestamp", "timestamp")])])

SOURCE_SHAPE = [{"name": "order_id", "type": "bigint", "nullable": False},
                {"name": "status", "type": "varchar(12)", "nullable": True},
                {"name": "occurred_at", "type": "timestamp", "nullable": False}]

SOURCE_ROWS = [{"order_id": i, "status": s, "occurred_at": f"2024-01-0{i}"}
               for i, s in enumerate(["new", "paid", "paid", "shipped", None], start=1)]


class ShapedSource(FakeSource):
    """FakeSource plus the read-only column_shape the SQL source adapters grew."""

    def __init__(self, rows, shapes: dict[str, list[dict]] | None):
        super().__init__(rows)
        self.shapes = shapes

    def column_shape(self, table: str) -> list[dict]:
        self._count("column_shape")
        if self.shapes is None:
            raise NotImplementedError("no catalog")
        return self.shapes[table]


def _source():
    return ShapedSource({"app.orders": SOURCE_ROWS}, {"app.orders": SOURCE_SHAPE})


def test_a_faithful_fixture_passes_and_records_what_was_read():
    out = compare_fixture(SPEC, _source(), _source())
    assert out["status"] == "pass" and out["findings"] == []
    assert out["tables"]["app.orders"] == {"status": "pass", "shape": "checked",
                                           "cardinality": "checked"}
    assert out["source_statements"] == 1 + 3  # one catalog read, one aggregate per column


def test_column_name_type_and_nullability_gaps_are_findings():
    fixture = ShapedSource({"app.orders": [
        {"order_id": 1, "state": "new", "occurredAt": "2024-01-01"}]}, {"app.orders": [
            {"name": "order_id", "type": "int", "nullable": True},
            {"name": "state", "type": "varchar(12)", "nullable": True},
            {"name": "occurredAt", "type": "timestamp", "nullable": False}]})
    out = compare_fixture(SPEC, _source(), fixture)
    assert out["status"] == "fail"
    checks = [(f["check"], f.get("column")) for f in out["findings"]]
    assert ("type_mismatch", "order_id") in checks
    assert ("nullable_mismatch", "order_id") in checks
    assert ("column_missing", "status") in checks
    assert ("column_missing", "occurred_at") in checks
    assert ("column_extra", "state") in checks
    assert ("column_extra", "occurredat") in checks
    detail = next(f for f in out["findings"] if f["check"] == "type_mismatch")["detail"]
    assert detail == "source bigint, fixture int"
    assert out["tables"]["app.orders"]["status"] == "fail"


def test_types_compare_after_normalisation_and_names_case_insensitively():
    fixture = ShapedSource({"app.orders": SOURCE_ROWS}, {"app.orders": [
        {"name": "ORDER_ID", "type": "BIGINT", "nullable": False},
        {"name": "Status", "type": "CHARACTER VARYING(12)", "nullable": True},
        {"name": "occurred_at", "type": "TIMESTAMP WITHOUT TIME ZONE", "nullable": False}]})
    assert compare_fixture(SPEC, _source(), fixture)["findings"] == []


def test_only_mapped_columns_are_compared_but_extra_fixture_columns_are_named():
    src = ShapedSource({"app.orders": SOURCE_ROWS},
                       {"app.orders": SOURCE_SHAPE + [{"name": "legacy_flag", "type": "char(1)",
                                                       "nullable": True}]})
    out = compare_fixture(SPEC, src, _source())
    assert out["findings"] == []  # an unmapped source column the fixture lacks is not a gap
    out = compare_fixture(SPEC, _source(), src)
    assert [f["check"] for f in out["findings"]] == ["column_extra"]


def test_sample_cardinality_gaps_are_findings():
    flat = [{"order_id": i, "status": "new", "occurred_at": None} for i in range(1, 6)]
    fixture = ShapedSource({"app.orders": flat}, {"app.orders": SOURCE_SHAPE})
    out = compare_fixture(SPEC, _source(), fixture)
    by = {(f["check"], f["column"]): f for f in out["findings"]}
    assert by[("cardinality_collapsed", "status")]["detail"] == \
        "source 3 distinct in 5 rows, fixture 1 distinct in 5 rows"
    assert by[("null_profile", "occurred_at")]["detail"] == \
        "source null rate 0.0, fixture null rate 1.0"
    assert ("cardinality_collapsed", "order_id") not in by
    assert out["status"] == "fail"


def test_an_empty_fixture_table_is_a_finding_not_a_pass():
    fixture = ShapedSource({"app.orders": []}, {"app.orders": SOURCE_SHAPE})
    out = compare_fixture(SPEC, _source(), fixture)
    assert [f["check"] for f in out["findings"]] == ["empty_fixture"]
    assert out["status"] == "fail"


def test_a_side_without_a_catalog_reader_is_unsupported_never_clean():
    src = ShapedSource({"app.orders": SOURCE_ROWS}, None)
    out = compare_fixture(SPEC, src, _source())
    assert out["status"] == "unsupported"
    assert out["tables"]["app.orders"]["shape"] == "unsupported"
    assert out["tables"]["app.orders"]["cardinality"] == "checked"
    assert "column_shape" in out["tables"]["app.orders"]["reason"]


def test_a_table_the_fixture_lacks_is_a_finding_and_stops_its_reads():
    fixture = ShapedSource({}, {})
    out = compare_fixture(SPEC, _source(), fixture)
    assert [f["check"] for f in out["findings"]] == ["table_missing"]
    assert out["tables"]["app.orders"] == {"status": "fail", "shape": "unsupported",
                                           "cardinality": "unsupported",
                                           "reason": "fixture has no app.orders"}


def test_the_source_statement_cap_bounds_the_legacy_reads():
    spec = MappingSpec(version="m", objects=SPEC.objects + [ObjectMapping(
        object="lines", root_table="app.lines", key_source=["id"], key_target="id",
        fields=[FieldMapping("id", "id", "bigint", "long")])])
    lines_shape = [{"name": "id", "type": "bigint", "nullable": False}]
    rows = {"app.orders": SOURCE_ROWS, "app.lines": [{"id": 1}, {"id": 2}]}
    shapes = {"app.orders": SOURCE_SHAPE, "app.lines": lines_shape}
    src, fx = ShapedSource(rows, shapes), ShapedSource(rows, shapes)
    out = compare_fixture(spec, src, fx, source_statement_cap=3)
    # shapes first for every table (2), then cardinality until the cap: orders needs 3 more
    # reads and does not fit, lines needs 1 and does
    assert out["source_statements"] == 3 and src.statements == 3
    assert out["tables"]["app.orders"]["shape"] == "checked"
    assert out["tables"]["app.lines"]["shape"] == "checked"
    assert out["tables"]["app.orders"]["cardinality"] == "unsupported"
    assert "cap 3 reached after 2" in out["tables"]["app.orders"]["reason"]
    assert out["tables"]["app.lines"] == {"status": "pass", "shape": "checked",
                                          "cardinality": "checked"}
    assert out["status"] == "unsupported"
    with pytest.raises(ConfigError, match="cap"):
        compare_fixture(spec, src, fx, source_statement_cap=1)


def test_findings_are_ordered_and_reference_the_unit_table():
    fixture = ShapedSource({"app.orders": []}, {"app.orders": SOURCE_SHAPE[:1]})
    out = compare_fixture(SPEC, _source(), fixture)
    assert [(f["table"], f["check"]) for f in out["findings"]] == [
        ("app.orders", "column_missing"), ("app.orders", "column_missing"),
        ("app.orders", "empty_fixture")]


def test_load_check_validates_the_written_shape():
    good = {"status": "fail", "findings": [{"table": "t", "check": "column_missing"}],
            "tables": {}, "source_statements": 1}
    assert load_check(good, "x")["status"] == "fail"
    for bad in ({}, {"status": "clean", "findings": [], "tables": {}},
                {"status": "pass", "findings": [{}], "tables": {}},
                {"status": "pass", "findings": "none", "tables": {}}):
        with pytest.raises(ConfigError):
            load_check(bad, "x")


# ---- fixture (the committed example) -------------------------------------------------------

def _sample(name):
    d = json.loads((FIXTURE / name).read_text())
    return ShapedSource({d["table"]: d["rows"]}, {d["table"]: d["shape"]})


def test_example_fixture_shows_a_mismatch_in_the_recorded_shape():
    """The example is a recorded pair (source sample vs fixture copy): the classic wave 0 gap
    where the fixture spells a column differently, loosens a type and flattens a status."""
    from recon.config import load_mapping_spec
    spec = load_mapping_spec(FIXTURE / "mapping_spec.json")
    out = compare_fixture(spec, _sample("source_sample.json"), _sample("fixture_sample.json"))
    assert out == json.loads((FIXTURE / "expected.json").read_text())
    assert out["status"] == "fail"
    assert [(f["check"], f.get("column")) for f in out["findings"]] == [
        ("type_mismatch", "event_id"), ("column_missing", "occurred_at"),
        ("column_extra", "timestamp"), ("cardinality_collapsed", "status")]


# ---- CLI ----------------------------------------------------------------------------------

def test_cli_fixture_shape_writes_the_check_and_exits_non_zero_on_a_gap(tmp_path, monkeypatch, capsys):
    from recon import adapters
    src, fx = _source(), ShapedSource({"app.orders": []}, {"app.orders": SOURCE_SHAPE})
    made = []

    def make(secret):
        made.append(secret)
        return src if secret == "SRC_DSN" else fx

    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "postgres", make)
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps({"version": "m1", "objects": [
        {"object": "orders", "root_table": "app.orders",
         "key": {"source": ["order_id"], "target": "order_id"},
         "fields": [{"source": "order_id", "target": "order_id", "source_type": "bigint",
                     "target_type": "long"}]}]}))
    rc = cli.main(["fixture-shape", "--family", "postgres", "--mapping", str(mapping),
                   "--source-dsn-secret", "SRC_DSN", "--fixture-dsn-secret", "FIX_DSN",
                   "--source-statement-cap", "10", "--out", str(tmp_path / "w0")])
    assert rc == 1 and made == ["SRC_DSN", "FIX_DSN"]
    out = json.loads((tmp_path / "w0" / "fixture_shape.json").read_text())
    assert out["status"] == "fail"
    assert [f["check"] for f in out["findings"]] == ["empty_fixture"]
    assert out["source_statements"] == 2 and out["source_statement_cap"] == 10
    assert out["family"] == "postgres" and out["mapping_version"] == "m1"
    line = capsys.readouterr().out
    assert "fail" in line and "SRC_DSN" not in line.replace("secret=SRC_DSN", "")


def test_cli_fixture_shape_refuses_an_untested_family(tmp_path):
    with pytest.raises(SystemExit, match="untested"):
        cli.main(["fixture-shape", "--family", "oracle", "--mapping", str(tmp_path / "m.json"),
                  "--source-dsn-secret", "S", "--fixture-dsn-secret", "F",
                  "--source-statement-cap", "10", "--out", str(tmp_path)])
    assert not (tmp_path / "fixture_shape.json").exists()


def test_cli_fixture_shape_requires_a_cap(tmp_path):
    with pytest.raises(SystemExit):
        cli.main(["fixture-shape", "--family", "postgres", "--mapping", str(tmp_path / "m.json"),
                  "--source-dsn-secret", "S", "--fixture-dsn-secret", "F", "--out", str(tmp_path)])


# ---- adapters: read-only column_shape on the live-tested source families ------------------

class _Cur:
    def __init__(self, rows):
        self._rows, self.description = rows, None

    def execute(self, sql, params=()):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self.rows, self.cursors = rows, []

    def cursor(self):
        c = _Cur(self.rows)
        self.cursors.append(c)
        return c


def test_sqlserver_source_column_shape_reads_sys_columns_in_order():
    from recon.adapters import SqlServerSourceAdapter
    conn = _Conn([("Id", "bigint", None, None, None, False, 1),
                  ("Name", "nvarchar", 80, None, None, True, 2),
                  ("Amt", "decimal", None, 18, 2, True, 3)])
    ad = SqlServerSourceAdapter.__new__(SqlServerSourceAdapter)
    ad._conn, ad.statements, ad.rows_fetched = conn, 0, 0
    assert ad.column_shape("dbo.Orders") == [
        {"name": "id", "type": "bigint", "nullable": False},
        {"name": "name", "type": "nvarchar(40)", "nullable": True},
        {"name": "amt", "type": "decimal(18,2)", "nullable": True}]
    cur = conn.cursors[0]
    assert "sys.columns" in cur.sql and "ORDER BY c.column_id" in cur.sql
    assert cur.params == ("dbo", "Orders") and ad.statements == 1


def test_databricks_source_column_shape_reads_information_schema():
    from recon.adapters import DatabricksSourceAdapter
    conn = _Conn([("id", "BIGINT", "NO", 1), ("tags", "ARRAY<STRING>", "YES", 2)])
    ad = DatabricksSourceAdapter.__new__(DatabricksSourceAdapter)
    ad._conn, ad.statements, ad.rows_fetched = conn, 0, 0
    assert ad.column_shape("cat.sch.t") == [
        {"name": "id", "type": "bigint", "nullable": False},
        {"name": "tags", "type": "array<string>", "nullable": True}]
    cur = conn.cursors[0]
    assert "information_schema.columns" in cur.sql and "ORDER BY ordinal_position" in cur.sql
    assert cur.params == {"catalog": "cat", "schema": "sch", "table": "t"}


def test_databricks_source_column_shape_needs_a_three_part_name():
    from recon.adapters import DatabricksSourceAdapter
    ad = DatabricksSourceAdapter.__new__(DatabricksSourceAdapter)
    ad._conn, ad.statements, ad.rows_fetched = _Conn([]), 0, 0
    with pytest.raises(NotImplementedError, match="catalog.schema.table"):
        ad.column_shape("sch.t")


def test_postgres_source_column_shape_reads_pg_attribute():
    from recon.adapters import PostgresSourceAdapter
    conn = _Conn([("id", "bigint", True, 1), ("note", "character varying(40)", False, 2)])
    ad = PostgresSourceAdapter.__new__(PostgresSourceAdapter)
    ad._conn, ad.statements, ad.rows_fetched = conn, 0, 0
    assert ad.column_shape("app.t") == [
        {"name": "id", "type": "bigint", "nullable": False},
        {"name": "note", "type": "varchar(40)", "nullable": True}]
    cur = conn.cursors[0]
    assert "pg_attribute" in cur.sql and cur.params == ("app", "t")
