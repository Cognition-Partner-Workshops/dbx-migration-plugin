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
    src = _source()
    out = compare_fixture(SPEC, src, _source())
    assert out["status"] == "pass" and out["findings"] == []
    assert out["tables"]["app.orders"] == {"status": "pass", "shape": "checked",
                                           "cardinality": "checked"}
    assert out["source_statements"] == 1 + 3  # one catalog read, one profile per column
    assert src.calls["column_profile"] == 3


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


def test_an_empty_source_population_is_unsupported_not_a_pass():
    """No source rows in scope means the cardinality check compared nothing; that is not evidence
    the fixture has the source's profile."""
    src = ShapedSource({"app.orders": []}, {"app.orders": SOURCE_SHAPE})
    out = compare_fixture(SPEC, src, _source())
    assert out["findings"] == []
    assert out["status"] == "unsupported"
    assert out["tables"]["app.orders"] == {
        "status": "unsupported", "shape": "checked", "cardinality": "unsupported",
        "reason": "source has 0 rows in scope, nothing to compare the fixture's profile with"}
    assert src.calls["column_profile"] == 1  # stops at the first empty profile


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
    assert out["tables"]["app.orders"]["cardinality"] == "unsupported"
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


class TwoStatementSource(ShapedSource):
    """A SQL-style side whose field_aggregates costs two statements (aggregate plus SUM probe)."""

    def field_aggregates(self, table, column, where=None):
        self._count("field_aggregates", statements=2)
        return self._aggs(self._rows(table, where), [column], [column])[column]


def test_cardinality_uses_one_statement_per_column_and_the_cap_counts_real_statements():
    src, fx = TwoStatementSource({"app.orders": SOURCE_ROWS}, {"app.orders": SOURCE_SHAPE}), _source()
    out = compare_fixture(SPEC, src, fx, source_statement_cap=4)
    assert out["source_statements"] == 4 and src.statements == 4
    assert src.calls["column_profile"] == 3 and src.calls["field_aggregates"] == 0
    assert out["tables"]["app.orders"]["cardinality"] == "checked"


class OvershootingSource(ShapedSource):
    """An adapter that spends more statements than the check budgeted for a column."""

    def column_profile(self, table, column, where=None):
        self._count("column_profile", statements=2)
        return super().column_profile(table, column, where)


def test_an_adapter_that_overspends_the_cap_stops_the_source_reads_and_is_unsupported():
    src = OvershootingSource({"app.orders": SOURCE_ROWS}, {"app.orders": SOURCE_SHAPE})
    out = compare_fixture(SPEC, src, _source(), source_statement_cap=4)
    # catalog read (1) + first profile (2) = 3; the second profile would need 1 but costs 2
    # and is not issued once the counter shows the cap cannot hold it
    assert src.statements <= 4 and out["source_statements"] == src.statements
    assert out["tables"]["app.orders"]["cardinality"] == "unsupported"
    assert "cap 4" in out["tables"]["app.orders"]["reason"]
    assert out["status"] == "unsupported"


def test_a_scoped_mapping_profiles_only_its_population():
    spec = MappingSpec(version="m", objects=[ObjectMapping(
        object="orders", root_table="app.orders", key_source=["order_id"], key_target="order_id",
        root_where="region = 'eu'", target_where="region = 'eu'",
        fields=[FieldMapping("order_id", "order_id", "bigint", "long"),
                FieldMapping("status", "status", "varchar(12)", "string")])])
    shape = SOURCE_SHAPE[:2] + [{"name": "region", "type": "varchar(2)", "nullable": False}]
    src_rows = [{"order_id": 1, "status": "new", "region": "eu"},
                {"order_id": 2, "status": "new", "region": "eu"},
                {"order_id": 3, "status": "paid", "region": "us"},
                {"order_id": 4, "status": "shipped", "region": "us"}]
    fx_rows = [r for r in src_rows if r["region"] == "eu"]
    src = ShapedSource({"app.orders": src_rows}, {"app.orders": shape})
    fx = ShapedSource({"app.orders": fx_rows}, {"app.orders": shape})
    out = compare_fixture(spec, src, fx)
    assert out["status"] == "pass" and out["findings"] == []  # eu has one status on both sides
    out = compare_fixture(SPEC, src, ShapedSource({"app.orders": fx_rows}, {"app.orders": shape}))
    assert [f["check"] for f in out["findings"]] == ["cardinality_collapsed"]  # unscoped: 3 vs 1


def test_a_table_the_source_lacks_is_unsupported_and_not_aggregated():
    src = ShapedSource({"app.orders": SOURCE_ROWS}, {"app.orders": []})
    fx = _source()
    out = compare_fixture(SPEC, src, fx)
    assert out["status"] == "unsupported" and out["findings"] == []
    assert out["tables"]["app.orders"] == {"status": "unsupported", "shape": "unsupported",
                                           "cardinality": "unsupported",
                                           "reason": "source no app.orders"}
    assert src.calls["column_profile"] == 0 and fx.calls["column_profile"] == 0
    # the same when only the source catalog reader is missing: nothing is aggregated blind
    src = ShapedSource({}, None)
    out = compare_fixture(SPEC, src, fx)
    assert out["tables"]["app.orders"]["cardinality"] == "unsupported"
    assert src.calls["column_profile"] == 0


class FailingCatalog(ShapedSource):
    """A source whose catalog read fails the way a live driver does (denied, dropped, dialect)."""

    def column_shape(self, table):
        self._count("column_shape")
        raise RuntimeError("permission denied for schema information_schema")


def test_a_failed_catalog_read_is_unsupported_evidence_not_a_crash(tmp_path, monkeypatch):
    src, fx = FailingCatalog({"app.orders": SOURCE_ROWS}, None), _source()
    out = compare_fixture(SPEC, src, fx)
    assert out["status"] == "unsupported" and out["findings"] == []
    assert out["tables"]["app.orders"]["shape"] == "unsupported"
    assert out["tables"]["app.orders"]["cardinality"] == "unsupported"
    assert "RuntimeError" in out["tables"]["app.orders"]["reason"]
    assert "permission denied" in out["tables"]["app.orders"]["reason"]
    assert src.calls["column_profile"] == 0
    # the CLI still writes the evidence for the wave gate to read
    from recon import adapters
    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "postgres",
                        lambda secret: src if secret == "SRC_DSN" else fx)
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps({"version": "m1", "objects": [
        {"object": "orders", "root_table": "app.orders",
         "key": {"source": ["order_id"], "target": "order_id"},
         "fields": [{"source": "order_id", "target": "order_id", "source_type": "bigint",
                     "target_type": "long"}]}]}))
    rc = cli.main(["fixture-shape", "--family", "postgres", "--mapping", str(mapping),
                   "--source-dsn-secret", "SRC_DSN", "--fixture-dsn-secret", "FIX_DSN",
                   "--source-statement-cap", "10", "--out", str(tmp_path / "w0")])
    written = json.loads((tmp_path / "w0" / "fixture_shape.json").read_text())
    assert rc != 0 and written["status"] == "unsupported"


def test_a_failed_catalog_read_does_not_swallow_interrupts():
    class Interrupted(ShapedSource):
        def column_shape(self, table):
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        compare_fixture(SPEC, Interrupted({}, None), _source())


def test_comparison_keys_are_checked_even_when_not_mapped_as_fields():
    spec = MappingSpec(version="map-v1", objects=[ObjectMapping(
        object="orders", root_table="app.orders", key_source=["order_id"], key_target="order_id",
        fields=[FieldMapping("status", "status", "varchar(12)", "string")])])
    fixture = ShapedSource({"app.orders": [{"status": s} for s in ["new", "paid", None]]},
                           {"app.orders": SOURCE_SHAPE[1:2]})
    out = compare_fixture(spec, _source(), fixture)
    assert [(f["check"], f["column"]) for f in out["findings"]] == [("column_missing", "order_id")]
    assert out["status"] == "fail"
    src = _source()
    out = compare_fixture(spec, src, _source())
    assert out["status"] == "pass"
    assert src.calls["column_profile"] == 2  # the key column is profiled too


def test_a_mapped_column_the_source_lacks_is_unsupported_and_never_profiled():
    """The mapping names a column the source does not have (tier 7's finding, not this check's),
    so nothing about that column can be proven here: it is left out of the profile plan rather
    than queried into a crash, and the table cannot be clean."""
    rows = [{k: v for k, v in r.items() if k != "status"} for r in SOURCE_ROWS]
    shape = [c for c in SOURCE_SHAPE if c["name"] != "status"]
    src, fx = ShapedSource({"app.orders": rows}, {"app.orders": shape}), ShapedSource(
        {"app.orders": rows}, {"app.orders": shape})
    out = compare_fixture(SPEC, src, fx)
    assert out["status"] == "unsupported" and out["findings"] == []
    assert out["tables"]["app.orders"]["status"] == "unsupported"
    assert out["tables"]["app.orders"]["reason"] == "source has no mapped column status (a mapping defect)"
    assert out["tables"]["app.orders"]["cardinality"] == "partial"
    assert src.calls["column_profile"] == 2  # order_id and occurred_at, never status


def test_the_cap_floor_counts_catalog_reads_per_table_not_per_object():
    spec = MappingSpec(version="m", objects=[
        ObjectMapping(object="a", root_table="app.orders", key_source=["order_id"], key_target="order_id",
                      fields=[FieldMapping("status", "status", "varchar(12)", "string")]),
        ObjectMapping(object="b", root_table="app.orders", key_source=["order_id"], key_target="order_id",
                      fields=[FieldMapping("occurred_at", "occurred_at", "timestamp", "timestamp")])])
    out = compare_fixture(spec, _source(), _source(), source_statement_cap=1)
    assert out["tables"]["app.orders"]["shape"] == "checked"
    assert out["tables"]["app.orders"]["cardinality"] == "unsupported"
    with pytest.raises(ConfigError, match="cap 0 is below the 1"):
        compare_fixture(spec, _source(), _source(), source_statement_cap=0)


def test_two_objects_on_one_root_table_each_keep_their_own_coverage():
    """A second object on the same root table (a different slice or projection) must not
    overwrite the first one's plan; each object's mapped columns and scope are checked."""
    spec = MappingSpec(version="map-v1", objects=[
        ObjectMapping(object="orders", root_table="app.orders", key_source=["order_id"],
                      key_target="order_id",
                      fields=[FieldMapping("status", "status", "varchar(12)", "string")]),
        ObjectMapping(object="order_times", root_table="app.orders", key_source=["order_id"],
                      key_target="order_id", root_where="status = 'paid'",
                      fields=[FieldMapping("occurred_at", "occurred_at", "timestamp",
                                           "timestamp")])])
    src = _source()
    # the fixture flattens status (fails `orders`) but keeps occurred_at faithful (`order_times`)
    flat = [dict(r, status="paid" if r["status"] else None) for r in SOURCE_ROWS]
    fx = ShapedSource({"app.orders": flat}, {"app.orders": SOURCE_SHAPE})
    out = compare_fixture(spec, src, fx)
    assert [(f["check"], f["column"]) for f in out["findings"]] == [
        ("cardinality_collapsed", "status")]
    assert out["tables"]["app.orders"]["status"] == "fail"
    assert out["source_statements"] == 1 + 2 + 2  # one catalog read, both objects profiled
    assert src.calls["column_shape"] == 1
    # and the fixture that drops the second object's column is caught by the shape check
    fx = ShapedSource({"app.orders": [{"order_id": r["order_id"], "status": r["status"]}
                                      for r in SOURCE_ROWS]},
                      {"app.orders": SOURCE_SHAPE[:2]})
    out = compare_fixture(spec, src, fx)
    assert ("column_missing", "occurred_at") in [(f["check"], f["column"])
                                                 for f in out["findings"]]


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


def test_sql_column_profile_is_one_scoped_statement_with_no_sum_probe():
    from recon.adapters import SqlServerSourceAdapter
    conn = _Conn([(10, 8, 3)])
    ad = SqlServerSourceAdapter.__new__(SqlServerSourceAdapter)
    ad._conn, ad.statements, ad.rows_fetched = conn, 0, 0
    assert ad.column_profile("dbo.Orders", "status", "region = 'eu'") == {
        "count": 10, "null_rate": 0.2, "distinct_count": 3}
    assert ad.statements == 1 and len(conn.cursors) == 1
    sql = conn.cursors[0].sql
    assert "WHERE region = 'eu'" in sql and "SUM(" not in sql
    # counts only: MIN/MAX are undefined for booleans on some engines and would abort the check
    assert "MIN(" not in sql and "MAX(" not in sql
    assert ad.column_profile("dbo.Orders", "status") == {"count": 10, "null_rate": 0.2, "distinct_count": 3}
    assert "WHERE" not in conn.cursors[1].sql


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


def test_sqlserver_float_keeps_its_declared_precision():
    """float(24) and float(53) are different storage; folding both to `float` would let a
    lower-precision fixture pass the shape gate."""
    from recon.adapters import SqlServerSourceAdapter
    conn = _Conn([("Rate", "float", 4, 24, None, True, 1),
                  ("Amt", "float", 8, 53, None, True, 2),
                  ("Real", "real", 4, 24, None, True, 3)])
    ad = SqlServerSourceAdapter.__new__(SqlServerSourceAdapter)
    ad._conn, ad.statements, ad.rows_fetched = conn, 0, 0
    assert [c["type"] for c in ad.column_shape("dbo.Orders")] == ["float(24)", "float(53)", "real"]


class UnprofilableSource(ShapedSource):
    """A side whose profile query fails on one column (a type with no equality operator)."""

    def column_profile(self, table, column, where=None):
        if column == "status":
            self._count("column_profile")
            raise RuntimeError("could not identify an equality operator for type json")
        return super().column_profile(table, column, where)


def test_a_profile_read_that_fails_leaves_the_table_unsupported_with_evidence():
    """A COUNT(DISTINCT) the source cannot run must end that table's cardinality check as
    `unsupported` (reason recorded), never escape and leave wave 0 without fixture_shape.json."""
    src = UnprofilableSource({"app.orders": SOURCE_ROWS}, {"app.orders": SOURCE_SHAPE})
    out = compare_fixture(SPEC, src, _source())
    row = out["tables"]["app.orders"]
    assert out["status"] == "unsupported" and out["findings"] == []
    assert row["status"] == "unsupported" and row["cardinality"] == "unsupported"
    assert "status" in row["reason"] and "RuntimeError" in row["reason"]
    assert src.calls["column_profile"] < 3  # profiling that table stops at the failure
    with pytest.raises(KeyboardInterrupt):
        class Interrupting(ShapedSource):
            def column_profile(self, table, column, where=None):
                raise KeyboardInterrupt
        compare_fixture(SPEC, Interrupting({"app.orders": SOURCE_ROWS}, {"app.orders": SOURCE_SHAPE}),
                        _source())
