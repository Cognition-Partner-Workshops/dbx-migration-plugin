"""Tier 0 structural parity: fixture dictionaries, the DictionaryOverlay, the trigger/grant/
identity comparators, per-category checked/unsupported accounting, and merge_block_reasons."""

import json
from pathlib import Path

import pytest
from recon.adapters import IdentityState, SchemaFacts
from recon.config import ConfigError, Tolerances, load_mapping_spec
from recon.engine import run_recon
from recon.report import build_result
from recon.structure import (
    CATEGORIES,
    DictionaryOverlay,
    compare_grants,
    compare_identity_columns,
    compare_triggers,
    diff_by_object,
    load_dictionary,
    structural_checks,
)
from recon.tiers import Finding

from tests.fakes import FakeSource, FakeTarget
from tests.loans import (
    BORROWER_FACTS,
    LOANS_FACTS,
    TARGET_LOANS_FACTS,
    _facts,
    _rows,
    _spec,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.mark.parametrize("family", ["sqlserver", "postgres", "databricks", "oracle"])
def test_load_dictionary_reads_each_family_fixture(family):
    d = load_dictionary(FIXTURES / f"example_{family}" / "dictionary.json")
    assert d.family == family and d.tables
    assert all(isinstance(f, SchemaFacts) for f in d.tables.values())
    if family == "databricks":
        assert d.unsupported == frozenset({"indexes", "triggers"})
        assert all(f.unsupported == d.unsupported for f in d.tables.values())
    loans = next(f for f in d.tables.values() if f.primary_key)
    assert loans.identity_columns
    if family != "databricks":
        assert ("after", ("insert", "update")) in loans.triggers.values()


def test_load_dictionary_refuses_a_bad_foreign_key_shape(tmp_path):
    p = tmp_path / "d.json"
    p.write_text(json.dumps({"family": "x", "tables": {"t": {"foreign_keys": [{"columns": ["a"]}]}}}))
    with pytest.raises(ConfigError):
        load_dictionary(p)


def test_overlay_serves_fixture_facts_and_delegates_the_rest():
    d = load_dictionary(FIXTURES / "example_sqlserver" / "dictionary.json")
    src = FakeSource({"dbo.loans": [{"loan_id": 1}]})
    overlay = DictionaryOverlay(src, d)
    assert overlay.schema_facts("dbo.loans") is d.tables["dbo.loans"]
    assert overlay.row_count("dbo.loans") == 1
    assert overlay.dictionary_label == f"fixture:{FIXTURES / 'example_sqlserver' / 'dictionary.json'}"
    assert overlay.identity_state("dbo.loans", "loan_id") == IdentityState(1001, 1)
    with pytest.raises(NotImplementedError):
        overlay.identity_state("dbo.loans", "loan_number")
    with pytest.raises(NotImplementedError):
        overlay.schema_facts("dbo.missing")


def test_structural_checks_mark_a_reader_hole():
    full = SchemaFacts()
    assert set(structural_checks([(full, full)]).values()) == {"checked"}
    d = load_dictionary(FIXTURES / "example_databricks" / "dictionary.json")
    t = next(iter(d.tables.values()))
    sc = structural_checks([(full, t)])
    assert sc["triggers"] == "unsupported" and sc["indexes"] == "unsupported"
    assert sc["constraints"] == sc["grants"] == sc["sequences_identity"] == "checked"
    assert set(structural_checks([]).values()) == {"unsupported"}


def test_compare_triggers_by_timing_and_event():
    s = SchemaFacts(triggers={"trg_a": ("after", ("insert", "update"))})
    t = SchemaFacts(triggers={"renamed": ("after", ("insert",))})
    findings, tightened = compare_triggers("o", s, t)
    assert len(findings) == 1 and findings[0].check == "trigger_missing"
    assert "update" in findings[0].detail
    # renamed but equal -> nothing
    findings, tightened = compare_triggers("o", s, SchemaFacts(
        triggers={"x": ("after", ("insert", "update"))}))
    assert findings == [] and tightened == []
    _, tightened = compare_triggers("o", SchemaFacts(), SchemaFacts(
        triggers={"t2": ("before", ("delete",))}))
    assert [f.check for f in tightened] == ["trigger_extra"]


def test_compare_grants_with_principal_map():
    s = SchemaFacts(grants={"app_rw": frozenset({"select", "insert"})})
    missing = compare_grants("o", s, SchemaFacts(grants={"app_rw": frozenset({"select"})}), {})
    assert [f.check for f in missing] == ["grant_missing"] and "insert" in missing[0].detail
    assert compare_grants("o", s, SchemaFacts(grants={"svc_app": frozenset({"select", "insert"})}),
                          {"app_rw": "svc_app"}) == []
    extra = compare_grants("o", s, SchemaFacts(
        grants={"app_rw": frozenset({"select", "insert"}), "analyst": frozenset({"select"})}), {})
    assert [f.check for f in extra] == ["grant_extra"]
    extra_priv = compare_grants("o", s, SchemaFacts(
        grants={"app_rw": frozenset({"select", "insert", "delete"})}), {})
    assert [f.check for f in extra_priv] == ["grant_extra"]


def test_compare_identity_columns():
    colmap = {"loan_id": "loan_id"}
    s = SchemaFacts(identity_columns={"loan_id"})
    findings, _ = compare_identity_columns("o", s, SchemaFacts(), colmap)
    assert [f.check for f in findings] == ["identity_missing"]
    _, tight = compare_identity_columns("o", SchemaFacts(), s, colmap)
    assert [f.check for f in tight] == ["identity_extra"]


def test_diff_by_object_groups_by_category():
    d = diff_by_object([Finding("loans", "trigger_missing", "d1"),
                        Finding("loans", "grant_missing", "d2"),
                        Finding("x", "row_count_diff", "unrelated")])
    assert d == {"loans": {"triggers": ["d1"], "grants": ["d2"]}}


# ------------------------------------------------------------------ end-to-end, live mode

def _live(loans_src_facts=LOANS_FACTS, loans_tgt_facts=None):
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": loans_src_facts,
                                "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                        schema={"loans": loans_tgt_facts or TARGET_LOANS_FACTS,
                                "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    return run_recon("u1", "live", _spec(), Tolerances("t1"), [], source, target)


def test_tier0_fails_on_missing_trigger_and_grant():
    src_facts = _facts(LOANS_FACTS,
                       triggers={"trg_loans_audit": ("after", ("insert", "update"))},
                       grants={"app_rw": frozenset({"select", "insert"})})
    result = _live(loans_src_facts=src_facts)
    t0 = result["tiers"][0]
    assert t0["tier"] == 0 and t0["name"] == "structural_parity" and t0["passed"] is False
    codes = sorted(f["check"] for f in t0["findings"])
    assert "trigger_missing" in codes and "grant_missing" in codes
    assert result["merge_eligible"] is False
    assert result["merge_block_reasons"][0] == "structural_gap"
    assert t0["stats"]["structural_checks"] == {c: "checked" for c in CATEGORIES}
    assert t0["stats"]["structural_diff"]["loans"]["triggers"]


def test_tier0_passes_and_tiers_shift_when_structure_matches():
    result = _live()
    t0 = result["tiers"][0]
    assert t0["name"] == "structural_parity" and t0["passed"] is True
    assert result["merge_block_reasons"] == []
    assert [t["name"] for t in result["tiers"][1:4]] == ["row_counts", "aggregates", "diffs"] or \
        [t["tier"] for t in result["tiers"][1:4]] == [1, 2, 3]


def test_tier0_records_target_unsupported_categories_as_unchecked():
    src_facts = _facts(LOANS_FACTS, triggers={"trg": ("after", ("insert",))})
    tgt_facts = _facts(TARGET_LOANS_FACTS, unsupported=frozenset({"triggers"}))
    result = _live(loans_src_facts=src_facts, loans_tgt_facts=tgt_facts)
    t0 = result["tiers"][0]
    assert t0["passed"] is True
    assert t0["stats"]["structural_checks"]["triggers"] == "unsupported"
    assert t0["stats"]["structural_checks"]["constraints"] == "checked"
    assert any("triggers" in n for n in t0["stats"]["unverified"])
    assert result["merge_eligible"] is False
    assert result["merge_block_reasons"] == ["structural_gap", "warnings"]
    assert any(w.startswith("UNVERIFIED structural_parity:") and "trigger" in w
               for w in result["warnings"])


def test_tier0_unsupported_indexes_are_not_a_warning():
    src_facts = _facts(LOANS_FACTS, indexes={("borrower_id",)})
    tgt_facts = _facts(TARGET_LOANS_FACTS, unsupported=frozenset({"indexes"}),
                       indexes=set())
    result = _live(loans_src_facts=src_facts, loans_tgt_facts=tgt_facts)
    t0 = result["tiers"][0]
    assert t0["stats"]["indexes_unsupported"]
    assert not t0["stats"].get("unverified")
    assert result["merge_eligible"] is (result["verdict"] == "PASS")


def test_tier0_unreadable_catalog_is_a_hole_not_a_warning():
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers})
    result = run_recon("u1", "live", _spec(), Tolerances("t1"), [], source, target)
    t0 = result["tiers"][0]
    assert t0["name"] == "structural_parity" and t0["passed"] is True
    assert t0["stats"]["structural_checks"] == {c: "unsupported" for c in CATEGORIES}
    assert t0["stats"]["dictionary_unavailable"]
    assert not t0["stats"].get("unverified")
    assert result["merge_eligible"] is True
    assert result["merge_block_reasons"] == []


def test_tier0_fixture_dictionary_never_merges():
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers})
    d = load_dictionary(FIXTURES / "example_sqlserver" / "dictionary.json")
    overlay = DictionaryOverlay(source, d)
    result = run_recon("u1", "live", _spec(), Tolerances("t1"), [], overlay, target)
    t0 = result["tiers"][0]
    assert t0["stats"]["dictionary"]["source"].startswith("fixture:")
    assert any("fixture dictionary" in n for n in t0["stats"]["unverified"])
    assert result["merge_eligible"] is False


# ------------------------------------------------------------------ spec / report / adapters

def test_principal_map_loads_and_validates(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({
        "version": "m1",
        "principal_map": {"app_rw": "svc_app"},
        "objects": [{"object": "loans", "root_table": "dbo.loans",
                     "key": {"source": ["loan_id"], "target": "loan_id"},
                     "fields": []}]}))
    spec = load_mapping_spec(p)
    assert spec.principal_map == {"app_rw": "svc_app"}
    p.write_text(json.dumps({
        "version": "m1", "principal_map": {"app_rw": 3},
        "objects": [{"object": "loans", "root_table": "dbo.loans",
                     "key": {"source": ["loan_id"], "target": "loan_id"}, "fields": []}]}))
    with pytest.raises(ConfigError):
        load_mapping_spec(p)


def test_sqlserver_schema_facts_reads_triggers_and_grants():
    from tests.loans import _StubConn

    class Conn(_StubConn):
        def cursor(self):
            outer = self

            class Cur:
                def execute(self, sql, params=()):
                    outer.executed.append((sql, params))
                    sql_l = sql.lower()
                    if "sys.triggers" in sql_l:
                        outer.rows = [("trg_a", 0, "INSERT"), ("trg_a", 0, "UPDATE"),
                                      ("trg_i", 1, "DELETE")]
                    elif "database_permissions" in sql_l:
                        outer.rows = [("app_rw", "SELECT"), ("app_rw", "INSERT")]
                    elif "is_nullable" in sql_l:
                        outer.rows = [("loan_id", False, True)]
                    else:
                        outer.rows = []

                def fetchall(self):
                    return outer.rows
            return Cur()
    from recon.adapters import SqlServerSourceAdapter
    a = SqlServerSourceAdapter.__new__(SqlServerSourceAdapter)
    a._conn = Conn()
    a.statements = a.rows_fetched = 0
    facts = a.schema_facts("dbo.loans")
    assert facts.triggers == {"trg_a": ("after", ("insert", "update")),
                              "trg_i": ("instead of", ("delete",))}
    assert facts.grants == {"app_rw": frozenset({"select", "insert"})}
    sql = " ".join(s.lower() for s, _ in a._conn.executed)
    for view in ("sys.triggers", "sys.trigger_events", "sys.database_permissions",
                 "sys.database_principals"):
        assert view in sql


def test_postgres_schema_facts_reads_triggers_and_grants():
    from tests.loans import _StubConn

    class Conn(_StubConn):
        def cursor(self):
            outer = self

            class Cur:
                def execute(self, sql, params=()):
                    outer.executed.append((sql, params))
                    sql_l = sql.lower()
                    if "pg_trigger" in sql_l:
                        outer.rows = [("trg_a", 2 + 4 + 16), ("trg_i", 64 + 8)]  # before insert,update / instead of delete
                    elif "table_privileges" in sql_l:
                        outer.rows = [("app_rw", "SELECT"), ("reporting_ro", "SELECT")]
                    elif "pg_attribute" in sql_l and "attidentity" in sql_l:
                        outer.rows = [("loan_id", True, False)]
                    elif "server_version" in sql_l or "current_setting" in sql_l:
                        outer.rows = [(150000,)]
                    else:
                        outer.rows = []

                def fetchall(self):
                    return outer.rows
            return Cur()
    from recon.adapters import PostgresSourceAdapter
    a = PostgresSourceAdapter.__new__(PostgresSourceAdapter)
    a._conn = Conn()
    a.statements = a.rows_fetched = 0
    facts = a.schema_facts("public.loans")
    assert facts.triggers["trg_a"] == ("before", ("insert", "update"))
    assert facts.triggers["trg_i"] == ("instead of", ("delete",))
    assert facts.grants == {"app_rw": frozenset({"select"}), "reporting_ro": frozenset({"select"})}


def test_databricks_schema_facts_maps_information_schema():
    from recon.adapters import DatabricksTargetAdapter
    a = DatabricksTargetAdapter.__new__(DatabricksTargetAdapter)
    a._catalog, a._schema = "mig", "s"
    answers = []

    class Sql:
        def _rows(self, sql, params):
            answers.append(sql)
            if "table_constraints" in sql and "referential_constraints" in sql:
                return [("PRIMARY KEY", "pk", "loan_id", None, None, None),
                        ("FOREIGN KEY", "fk", "borrower_id", "s", "borrowers", "borrower_id")]
            if "check_constraints" in sql:
                return [("current_balance >= 0",)]
            if "table_privileges" in sql:
                return [("svc_app", "SELECT"), ("svc_app", "MODIFY")]
            if "columns" in sql:
                return [("loan_id", "NO", "YES"), ("note", "YES", "NO")]
            return []
    a._sql = Sql()
    facts = a.schema_facts("loans")
    assert facts.primary_key == ("loan_id",)
    assert facts.foreign_keys == {(("borrower_id",), "s.borrowers", ("borrower_id",))}
    assert facts.not_null == {"loan_id"} and facts.identity_columns == {"loan_id"}
    assert facts.checks == {"current_balance >= 0"} and facts.check_count == 1
    assert facts.grants == {"svc_app": frozenset({"select", "modify"})}
    assert facts.unsupported == frozenset({"indexes", "triggers"})
    joined = " ".join(answers)
    for view in ("table_constraints", "key_column_usage", "referential_constraints",
                 "columns", "check_constraints", "table_privileges"):
        assert f"information_schema.{view}" in joined


def test_build_result_merge_block_reasons():
    from recon.tiers import TierResult
    ok = TierResult(0, "structural_parity", True, 1, [], {})
    assert build_result("u", "live", "m1", "t1", [ok])["merge_block_reasons"] == []
    failed = TierResult(1, "row_counts", False, 1, [Finding("o", "row_count_diff", "d")], {})
    assert build_result("u", "live", "m1", "t1", [ok, failed])["merge_block_reasons"] == ["tier_failed"]
    assert build_result("u", "fixture", "m1", "t1", [ok])["merge_block_reasons"] == ["mode"]
    gap = TierResult(0, "structural_parity", False, 1, [Finding("o", "trigger_missing", "d")], {})
    r = build_result("u", "live", "m1", "t1", [gap])
    assert r["merge_block_reasons"][0] == "structural_gap" and "tier_failed" in r["merge_block_reasons"]
