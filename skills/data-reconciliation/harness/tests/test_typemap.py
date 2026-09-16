"""The dialect skill's type_map: parse, match, audit, apply; cli wiring and result recording."""

import json
from pathlib import Path

import pytest
from recon.config import (
    ConfigError,
    EmbedMapping,
    FieldMapping,
    MappingSpec,
    ObjectMapping,
)
from recon.typemap import (
    apply_type_map,
    audit_field,
    audit_spec,
    expected_target,
    load_type_map,
    type_map_families,
)

ORACLE_CANON = Path(__file__).resolve().parents[3] / "oracle-plsql" / "canonicalization.json"


@pytest.fixture
def oracle_map():
    return load_type_map(ORACLE_CANON, "oracle", "databricks")


def test_load_type_map_reads_the_real_oracle_file(oracle_map):
    assert oracle_map.family == "oracle" and oracle_map.rules
    assert load_type_map(ORACLE_CANON, "sqlserver", "databricks") is None
    assert load_type_map(ORACLE_CANON, "oracle", "lakebase") is not None
    assert type_map_families(ORACLE_CANON) == ["oracle"]


def test_load_type_map_none_for_a_list_shaped_file(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps([{"rule": "identity"}]))
    assert load_type_map(p, "oracle", "databricks") is None
    assert type_map_families(p) == []


def test_load_type_map_refuses_a_malformed_entry(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps({"type_map": {"oracle": {"databricks": {"types": [{"source": "NUMBER"}]}}}}))
    with pytest.raises(ConfigError):
        load_type_map(p, "oracle", "databricks")


@pytest.mark.parametrize("source_type, expected", [
    ("NUMBER(10,2)", "decimal(10,2)"),
    ("NUMBER(18,0)", "bigint"),
    ("NUMBER(19,0)", "decimal(19,0)"),
    ("NUMBER(5)", "bigint"),
    ("NUMBER(20)", "decimal(20,0)"),
    ("NUMBER", "decimal(38,10)"),
    ("NUMBER(*,4)", "decimal(38,4)"),
    ("TIMESTAMP", "timestamp_ntz"),
    ("TIMESTAMP(6)", "timestamp_ntz"),
    ("timestamp(6) with local time zone", "timestamp"),
    ("VARCHAR2(30 CHAR)", "string"),
    ("INTERVAL DAY(2) TO SECOND(6)", "interval day to second"),
    ("SDO_GEOMETRY", None),
])
def test_expected_target(oracle_map, source_type, expected):
    found = expected_target(oracle_map, source_type)
    if expected is None:
        assert found is None
    else:
        assert found[0] == expected


@pytest.mark.parametrize("source_type, target_type, status", [
    ("NUMBER(10,2)", "DECIMAL(10,2)", "ok"),
    ("NUMBER(10,2)", "double", "contradiction"),
    ("NUMBER(10,2)", "float", "contradiction"),
    ("NUMBER(10,2)", "decimal", "contradiction"),
    ("NUMBER", "decimal(38,12)", "ok"),
    ("NUMBER", "decimal(38,0)", "contradiction"),
    ("NUMBER", "bigint", "contradiction"),
    ("NUMBER(5)", "decimal(5,0)", "ok"),
    ("NUMBER(5)", "BIGINT", "ok"),
    ("NUMBER(20)", "bigint", "contradiction"),
    ("TIMESTAMP", "timestamp", "ok"),
    ("TIMESTAMP", "TIMESTAMP_NTZ", "ok"),
    ("TIMESTAMP", "timestamptz", "contradiction"),
    ("TIMESTAMP", "timestamp with time zone", "contradiction"),
    ("TIMESTAMP", "timestamp_ltz", "ok"),
    ("NUMBER(10,2)", "NUMERIC(10,2)", "ok"),
    ("NUMBER(10,2)", "", "undeclared"),
    ("SDO_GEOMETRY", "string", "unmapped"),
    ("", "string", "unmapped"),
])
def test_audit_field(oracle_map, source_type, target_type, status):
    assert audit_field(oracle_map, source_type, target_type)[0] == status


def _spec(fields, embed_fields=()):
    return MappingSpec(version="m1", objects=[ObjectMapping(
        object="orders", root_table="ORDERS",
        key_source=["ORDER_ID"], key_target="order_id",
        fields=fields,
        embeds=[EmbedMapping(array_path="items", child_table="ORDER_ITEMS", fields=list(embed_fields))]
        if embed_fields else [],
    )])


def test_apply_type_map_fills_undeclared_and_records_unmapped(oracle_map):
    spec = _spec(
        [FieldMapping("ORDER_ID", "order_id", "NUMBER(5)", ""),
         FieldMapping("AMOUNT", "amount", "NUMBER(10,2)", "decimal(10,2)"),
         FieldMapping("SHAPE", "shape", "SDO_GEOMETRY", "string")],
        embed_fields=[FieldMapping("QTY", "qty", "NUMBER(3)", "bigint")],
    )
    new_spec, summary = apply_type_map(oracle_map, spec)
    assert summary == {"family": "oracle", "target_kind": "databricks",
                       "filled": ["orders.ORDER_ID"], "unmapped": ["orders.SHAPE"]}
    fields = new_spec.objects[0].fields
    assert fields[0].target_type == "bigint"
    assert fields[1].target_type == "decimal(10,2)"  # declared and accepted: untouched
    assert new_spec.objects[0].embeds[0].fields[0].target_type == "bigint"
    assert spec.objects[0].fields[0].target_type == ""  # original spec unchanged


def test_apply_type_map_raises_every_contradiction(oracle_map):
    spec = _spec([
        FieldMapping("AMOUNT", "amount", "NUMBER(10,2)", "double"),
        FieldMapping("CREATED_AT", "created_at", "TIMESTAMP", "timestamptz"),
    ])
    with pytest.raises(ConfigError) as exc:
        apply_type_map(oracle_map, spec)
    msg = str(exc.value)
    for needle in ("orders.AMOUNT", "NUMBER(10,2)", "double", "decimal(10,2)",
                   "orders.CREATED_AT", "TIMESTAMP", "timestamptz", "timestamp_ntz"):
        assert needle in msg, needle


def test_audit_spec_rows_cover_root_and_embed_fields(oracle_map):
    spec = _spec([FieldMapping("AMOUNT", "amount", "NUMBER(10,2)", "double")],
                 embed_fields=[FieldMapping("QTY", "qty", "NUMBER(3)", "")])
    rows = audit_spec(oracle_map, spec)
    assert {(r["object"], r["source"], r["status"]) for r in rows} == {
        ("orders", "AMOUNT", "contradiction"), ("orders.items", "QTY", "undeclared")}
    assert rows[0]["expected"] == "decimal(10,2)"


# ------------------------------------------------------------------ cli wiring

def _cli_run(tmp_path, monkeypatch, spec_fields, canon_data):
    from recon import adapters, cli
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir()
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig"]}))
    (tmp_path / "m.json").write_text(json.dumps({
        "version": "m1", "objects": [{
            "object": "orders", "root_table": "ORDERS",
            "key": {"source": ["ORDER_ID"], "target": ["order_id"]},
            "fields": spec_fields}]}))
    (tmp_path / "t.json").write_text(json.dumps({"version": "t1"}))
    (tmp_path / "c.json").write_text(json.dumps(canon_data))
    captured = {}
    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "sqlserver", lambda secret: object())
    monkeypatch.setattr(adapters, "DatabricksTargetAdapter", lambda *a: object())

    def fake_run(unit, mode, spec, tol, rules, source, target, **kw):
        captured.update(kw)
        captured["spec"] = spec
        return {"verdict": "PASS", "depth": "threshold", "merge_eligible": True}
    monkeypatch.setattr(cli, "run_recon", fake_run)
    argv = ["run", "--unit", "u", "--family", "sqlserver", "--mode", "fixture",
            "--mapping", str(tmp_path / "m.json"), "--tolerances", str(tmp_path / "t.json"),
            "--canonicalization", str(tmp_path / "c.json"), "--source-dsn-secret", "SOURCE",
            "--target-secret", "TARGET", "--target-catalog", "mig", "--target-schema", "s",
            "--out", str(tmp_path / "out")]
    return cli, argv, captured


SQLSERVER_MAP = {"types": [{"source": "BIGINT", "target": "bigint"},
                           {"source": "INT", "target": "int"}]}


def test_cli_refuses_a_spec_the_type_map_contradicts(tmp_path, monkeypatch):
    cli, argv, _ = _cli_run(tmp_path, monkeypatch,
                            [{"source": "ORDER_ID", "target": "order_id",
                              "source_type": "BIGINT", "target_type": "string"}],
                            {"rules": [], "type_map": {"sqlserver": {"databricks": SQLSERVER_MAP}}})
    with pytest.raises(SystemExit, match="^type map:"):
        cli.main(argv)


def test_cli_fills_undeclared_targets_and_records_the_summary(tmp_path, monkeypatch):
    cli, argv, captured = _cli_run(tmp_path, monkeypatch,
                                   [{"source": "ORDER_ID", "target": "order_id",
                                     "source_type": "BIGINT", "target_type": ""},
                                    {"source": "QTY", "target": "qty",
                                     "source_type": "INT", "target_type": "int"}],
                                   {"rules": [], "type_map": {"sqlserver": {"databricks": SQLSERVER_MAP}}})
    assert cli.main(argv) == 0
    assert captured["type_map"] == {"family": "sqlserver", "target_kind": "databricks",
                                    "filled": ["orders.ORDER_ID"], "unmapped": []}
    assert captured["spec"].objects[0].fields[0].target_type == "bigint"
    assert captured["spec"].objects[0].fields[1].target_type == "int"


def test_cli_without_a_type_map_passes_none(tmp_path, monkeypatch):
    cli, argv, captured = _cli_run(tmp_path, monkeypatch,
                                   [{"source": "ORDER_ID", "target": "order_id",
                                     "source_type": "BIGINT", "target_type": "string"}],
                                   {"rules": []})
    assert cli.main(argv) == 0
    assert captured["type_map"] is None


def test_build_result_records_type_map():
    from recon.report import build_result
    marker = {"family": "sqlserver", "filled": [], "unmapped": []}
    assert build_result("u", "fixture", "m1", "t1", [])["type_map"] is None
    assert build_result("u", "fixture", "m1", "t1", [], type_map=marker)["type_map"] is marker


@pytest.fixture
def oracle_lakebase_map():
    return load_type_map(ORACLE_CANON, "oracle", "lakebase")


@pytest.mark.parametrize("source_type, expected", [
    ("NUMBER(10,2)", "numeric(10,2)"),
    ("NUMBER(18,0)", "bigint"),
    ("NUMBER(20)", "numeric(20,0)"),
    ("NUMBER", "numeric"),
    ("TIMESTAMP WITH TIME ZONE", "timestamp with time zone"),
    ("TIMESTAMP(6)", "timestamp(6)"),
    ("VARCHAR2(30)", "varchar(30)"),
    ("CLOB", "text"),
    ("DATE", "timestamp(0)"),
])
def test_expected_target_lakebase(oracle_lakebase_map, source_type, expected):
    assert expected_target(oracle_lakebase_map, source_type)[0] == expected


@pytest.mark.parametrize("source_type, target_type, status", [
    ("TIMESTAMP WITH TIME ZONE", "timestamp with time zone", "ok"),
    ("TIMESTAMP WITH TIME ZONE", "timestamptz", "ok"),
    ("TIMESTAMP WITH TIME ZONE", "timestamp", "contradiction"),
    ("VARCHAR2(30)", "text", "ok"),
    ("VARCHAR2(30)", "string", "contradiction"),
    ("NUMBER(10,2)", "numeric(10,2)", "ok"),
    ("NUMBER(10,2)", "double precision", "contradiction"),
])
def test_audit_field_lakebase(oracle_lakebase_map, source_type, target_type, status):
    assert audit_field(oracle_lakebase_map, source_type, target_type)[0] == status


def test_the_same_spec_disagrees_across_target_kinds(oracle_map, oracle_lakebase_map):
    assert audit_field(oracle_map, "TIMESTAMP WITH TIME ZONE", "timestamp with time zone")[0] == "contradiction"
    assert audit_field(oracle_lakebase_map, "TIMESTAMP WITH TIME ZONE", "timestamp with time zone")[0] == "ok"


@pytest.mark.parametrize("source_type, expected", [
    ("NUMBER(5,-2)", "bigint"),       # holds up to 7 integer digits: whole-number rule
    ("NUMBER(20,-3)", "decimal(23,0)"),
    ("NUMBER(4,5)", "decimal(5,5)"),  # |x| < 0.1 with 5 fractional digits
])
def test_expected_target_normalises_extreme_scales(oracle_map, source_type, expected):
    assert expected_target(oracle_map, source_type)[0] == expected


@pytest.mark.parametrize("source_type, target_type, status, expected", [
    ("NUMBER(5,-2)", "double", "contradiction", "bigint"),
    ("NUMBER(4,5)", "decimal(4,5)", "contradiction", "decimal(5,5)"),
    ("NUMBER(4,5)", "decimal(5,5)", "ok", "decimal(5,5)"),
])
def test_audit_field_normalises_extreme_scales(oracle_map, source_type, target_type, status, expected):
    assert audit_field(oracle_map, source_type, target_type) == (status, expected)


def test_apply_reports_the_normalisation(oracle_map):
    from recon.typemap import read_as
    assert read_as("NUMBER(5,-2)") == "NUMBER(7,0)" and read_as("NUMBER(10,2)") is None
    spec = _spec([FieldMapping("AMOUNT", "amount", "NUMBER(5,-2)", "double")])
    with pytest.raises(ConfigError, match="NUMBER\\(5,-2\\) -> read as NUMBER\\(7,0\\)"):
        apply_type_map(oracle_map, spec)


def test_estimate_applies_the_same_type_map_as_run(tmp_path, monkeypatch, capsys):
    from recon import cli
    (tmp_path / "t.json").write_text(json.dumps({"version": "t1"}))
    canon = {"rules": [], "type_map": {"oracle": {"databricks": {"types": [
        {"source": "NUMBER(p,s)", "target": "decimal(p,s)"}]}}}}
    (tmp_path / "c.json").write_text(json.dumps(canon))

    def estimate(target_type):
        m = {"version": "m1", "objects": [{
            "object": "orders", "root_table": "ORDERS",
            "key": {"source": ["ORDER_ID"], "target": ["order_id"]},
            "fields": [{"source": "AMOUNT", "target": "amount",
                        "source_type": "NUMBER(10,2)", "target_type": target_type}]}]}
        (tmp_path / "m.json").write_text(json.dumps(m))
        assert cli.main(["estimate", "--mapping", str(tmp_path / "m.json"),
                         "--tolerances", str(tmp_path / "t.json"), "--depth", "full",
                         "--family", "oracle", "--canonicalization", str(tmp_path / "c.json")]) == 0
        return json.loads(capsys.readouterr().out)

    assert estimate("") == estimate("decimal(10,2)")


def test_unrepresentable_precision_fails_instead_of_filling(oracle_map, oracle_lakebase_map):
    # NUMBER(38,-84) normalises to decimal(122,0): beyond databricks' 38, within postgres' 1000
    status, detail = audit_field(oracle_map, "NUMBER(38,-84)", "decimal(122,0)")
    assert status == "unrepresentable" and "decimals stop at 38" in detail
    assert expected_target(oracle_lakebase_map, "NUMBER(38,-84)")[0] == "numeric(122,0)"
    assert audit_field(oracle_lakebase_map, "NUMBER(38,-84)", "numeric(122,0)")[0] == "ok"
    spec = _spec([FieldMapping("BIG", "big", "NUMBER(38,-84)", "")])
    with pytest.raises(ConfigError, match="decimals stop at 38"):
        apply_type_map(oracle_map, spec)


def test_conditional_targets_need_their_census_rule(oracle_map):
    status, detail = audit_field(oracle_map, "INTEGER", "bigint")
    assert status == "contradiction" and "census_fits_int64" in detail
    assert audit_field(oracle_map, "INTEGER", "bigint", rules=["census_fits_int64"])[0] == "ok"
    assert audit_field(oracle_map, "INT", "bigint", rules=["census_fits_int64"])[0] == "ok"
    assert audit_field(oracle_map, "SMALLINT", "bigint", rules=["census_fits_int64"])[0] == "ok"
    status, detail = audit_field(oracle_map, "DATE", "date")
    assert status == "contradiction" and "census_midnight_only" in detail
    assert audit_field(oracle_map, "DATE", "date", rules=["census_midnight_only"])[0] == "ok"
    with pytest.raises(ConfigError, match="census_midnight_only"):
        apply_type_map(oracle_map, _spec([FieldMapping("D", "d", "DATE", "date")]))
    # declared with the rule audits ok through the whole-spec path too
    spec = _spec([FieldMapping("N", "n", "INTEGER", "bigint", rules=["census_fits_int64"])])
    apply_type_map(oracle_map, spec)
    assert audit_spec(oracle_map, spec)[0]["status"] == "ok"


def test_lakebase_parses_timestamp_without_time_zone_as_timestamp(oracle_map, oracle_lakebase_map):
    assert audit_field(oracle_lakebase_map, "TIMESTAMP(6)", "TIMESTAMP(6) WITHOUT TIME ZONE")[0] == "ok"
    # databricks unchanged: the spelling is timestamp_ntz there, and a bare timestamp_ntz stays ok
    assert audit_field(oracle_map, "TIMESTAMP(6)", "timestamp_ntz")[0] == "ok"
