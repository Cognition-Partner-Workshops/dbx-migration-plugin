"""Tier 0 structural parity: fixture dictionaries, the DictionaryOverlay, the trigger/grant/
identity comparators, per-category checked/unsupported accounting, and merge_block_reasons."""

import json
from pathlib import Path

import pytest
from recon.adapters import (DatabricksTargetAdapter, IdentityState, PostgresSourceAdapter,
                            SchemaFacts, SqlServerSourceAdapter, _uc_identity_state,
                            _uc_schema_facts)
from recon.config import ConfigError, Tolerances, load_mapping_spec
from recon.cost import estimate_cost
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

from tests.fakes import PROVEN_RERUN, FakeSource, FakeTarget
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
        assert d.unsupported == frozenset({"indexes"})
        assert all(f.unsupported == d.unsupported for f in d.tables.values())
    loans = next(f for f in d.tables.values() if f.primary_key or f.primary_key_informational)
    assert loans.identity_columns
    if family != "databricks":
        assert ("after", ("insert", "update"),
                "statement" if family == "sqlserver" else "row") in loans.triggers.values()


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
    assert set(structural_checks([(full, full)]).values()) == {"checked", "effective"}
    d = load_dictionary(FIXTURES / "example_databricks" / "dictionary.json")
    t = next(iter(d.tables.values()))
    sc = structural_checks([(full, t)])
    assert sc["indexes"] == "unsupported" and sc["grants"] == "direct_only"
    assert sc["triggers"] == sc["constraints"] == "checked"
    assert set(structural_checks([]).values()) == {"unsupported"}


def test_compare_triggers_by_timing_and_event():
    s = SchemaFacts(triggers={"trg_a": ("after", ("insert", "update"), "row")})
    t = SchemaFacts(triggers={"renamed": ("after", ("insert",), "row")})
    findings, tightened = compare_triggers("o", s, t)
    assert len(findings) == 1 and findings[0].check == "trigger_missing"
    assert "update" in findings[0].detail
    # renamed but equal -> nothing
    findings, tightened = compare_triggers("o", s, SchemaFacts(
        triggers={"x": ("after", ("insert", "update"), "row")}))
    assert findings == [] and tightened == []
    _, tightened = compare_triggers("o", SchemaFacts(), SchemaFacts(
        triggers={"t2": ("before", ("delete",), "row")}))
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


def test_compare_grants_expands_compound_privileges():
    s = SchemaFacts(grants={"app_rw": frozenset({"select", "insert", "update", "delete"})})
    # UC MODIFY stands for the same capabilities
    assert compare_grants("o", s, SchemaFacts(grants={"app_rw": frozenset({"select", "modify"})}),
                          {}) == []
    thin = compare_grants("o", SchemaFacts(grants={"ro": frozenset({"select"})}),
                          SchemaFacts(grants={"ro": frozenset({"select", "modify"})}), {})
    assert [f.check for f in thin] == ["grant_extra"]
    assert "insert" in thin[0].detail and "update" in thin[0].detail and "delete" in thin[0].detail


def test_compare_grants_folds_many_to_one_principal_map():
    s = SchemaFacts(grants={"reader": frozenset({"select"}), "writer": frozenset({"insert"})})
    t = SchemaFacts(grants={"app": frozenset({"select", "insert"})})
    assert compare_grants("o", s, t, {"reader": "app", "writer": "app"}) == []


def test_compare_grants_missing_folds_many_to_one():
    s = SchemaFacts(grants={"reader": frozenset({"select"}), "writer": frozenset({"insert"})})
    t = SchemaFacts(grants={"app": frozenset({"select"})})
    missing = compare_grants("o", s, t, {"reader": "app", "writer": "app"})
    assert [f.check for f in missing] == ["grant_missing"]
    assert "insert" in missing[0].detail
    assert "reader" in missing[0].detail and "writer" in missing[0].detail


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

def _live(loans_src_facts=LOANS_FACTS, loans_tgt_facts=None, *, target_kind=None, table_types=None,
          extra_target_loans=(), mode="live", rerun_proof=PROVEN_RERUN, own_principals=frozenset()):
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": loans_src_facts,
                                "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    target = FakeTarget({"loans": [dict(r) for r in loans] + [dict(r) for r in extra_target_loans],
                         "borrowers": borrowers},
                        schema={"loans": loans_tgt_facts or TARGET_LOANS_FACTS,
                                "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    target.target_kind, target.table_types = target_kind, table_types or {}
    return run_recon("u1", mode, _spec(), Tolerances("t1"), [], source, target, rerun_proof=rerun_proof,
                     own_principals=own_principals)


def test_tier0_fails_on_missing_trigger_and_grant():
    src_facts = _facts(LOANS_FACTS,
                       triggers={"trg_loans_audit": ("after", ("insert", "update"), "row")},
                       grants={"app_rw": frozenset({"select", "insert"})})
    result = _live(loans_src_facts=src_facts)
    t0 = result["tiers"][0]
    assert t0["tier"] == 0 and t0["name"] == "structural_parity" and t0["passed"] is False
    codes = sorted(f["check"] for f in t0["findings"])
    assert "trigger_missing" in codes and "grant_missing" in codes
    assert result["merge_eligible"] is False
    assert result["merge_block_reasons"][0] == "structural_gap"
    assert t0["stats"]["structural_checks"] == {
        c: "effective" if c == "grants" else "checked" for c in CATEGORIES}
    assert t0["stats"]["structural_diff"]["loans"]["triggers"]


def test_tier0_passes_and_tiers_shift_when_structure_matches():
    result = _live()
    t0 = result["tiers"][0]
    assert t0["name"] == "structural_parity" and t0["passed"] is True
    assert result["merge_block_reasons"] == []
    assert [t["name"] for t in result["tiers"][1:4]] == ["row_counts", "aggregates", "diffs"] or \
        [t["tier"] for t in result["tiers"][1:4]] == [1, 2, 3]


def test_tier0_records_target_unsupported_categories_as_unchecked():
    src_facts = _facts(LOANS_FACTS, triggers={"trg": ("after", ("insert",), "row")})
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
    assert t0["stats"]["index_unsupported"]
    assert "index_missing" not in {f["check"] for f in t0["findings"]}
    assert not t0["stats"].get("unverified")
    assert result["merge_eligible"] is (result["verdict"] == "PASS")


def test_structural_mode_runs_tier0_only_and_reads_no_source_rows():
    """`--mode structural` is the declared-DEGRADED verifier's run: the structural tier against both
    catalogs, no row tier, never merge evidence."""
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                        schema={"loans": TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    result = run_recon("u1", "structural", _spec(), Tolerances("t1"), [], source, target)
    assert [t["name"] for t in result["tiers"]] == ["structural_parity"]
    assert result["verdict"] == "PASS" and result["mode"] == "structural"
    assert result["merge_eligible"] is False and result["merge_block_reasons"] == ["mode"]
    assert source.rows_fetched == 0 and not {"count", "fetch_keyed", "sample_keys"} & set(source.calls)
    bad = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                     schema={"loans": BORROWER_FACTS, "borrowers": BORROWER_FACTS},
                     sequences={("loans", "loan_id"): 13})
    assert run_recon("u1", "structural", _spec(), Tolerances("t1"), [], source, bad)["verdict"] == "FAIL"


def test_structural_mode_never_reads_identity_row_bounds():
    """A degraded wave's structural run is catalog-only: the identity frontier-vs-rows collision
    check needs source MIN/MAX, so `--mode structural` must not issue that row read at all."""
    from recon.transactional import schema_parity
    loans, borrowers = _rows(12)
    kw = dict(schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
              sequences={("dbo.loans", "loan_id"): 13})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                        schema={"loans": TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers}, **kw)
    source.fail_on["field_aggregates"] = AssertionError("row read in structural mode")
    result = run_recon("u1", "structural", _spec(), Tolerances("t1"), [], source, target)
    assert [t["name"] for t in result["tiers"]] == ["structural_parity"]
    assert result["merge_block_reasons"] == ["mode"]
    assert source.calls["field_aggregates"] == 0

    # the live tier-0 run still reads the bounds — catalog_only is a flag, not a removal
    live_src = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers}, **kw)
    t0 = schema_parity(0, "structural_parity", _spec(), Tolerances("t1"), live_src, target,
                       strict=False)
    assert live_src.calls["field_aggregates"] > 0


def test_catalog_only_identity_note_marks_the_bounds_unread():
    """catalog_only still runs the catalog-backed identity checks but reports the skipped row
    read, and cannot raise the collision finding the bounds would carry."""
    from recon.transactional import schema_parity
    loans, borrowers = _rows(12)
    # source identity next (7) is already past the target's (10): only the row-backed source
    # max (12) would collide — the catalog-visible checks alone see nothing wrong
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 7})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                        schema={"loans": TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 10})
    t0 = schema_parity(0, "structural_parity", _spec(), Tolerances("t1"), source, target,
                       strict=False, catalog_only=True)
    assert "sequence_behind_source" not in {f.check for f in t0.findings}
    assert t0.stats["loans"]["identity"]["source_bounds"] == "unread"
    live = schema_parity(0, "structural_parity", _spec(), Tolerances("t1"), source, target,
                         strict=False)
    assert "sequence_behind_source" in {f.check for f in live.findings}


def test_structural_estimate_counts_each_adapters_catalog_reads():
    """`estimate --mode structural` describes the run it names: Tier 0's catalog statements per
    object and side from the adapters' own CATALOG_STATEMENTS (schema_facts per object,
    identity_state only when both identities are declared, one session read), no row tier, no
    rows transferred."""
    spec = _spec()
    n = len(spec.objects)
    n_id = sum(1 for c in spec.objects if c.identity_source and c.identity_target)
    est = estimate_cost(spec, Tolerances("t1"),
                        row_counts={"dbo.loans": 1_000_000, "dbo.borrowers": 3},
                        mode="structural", family="sqlserver", target_kind="databricks")
    assert est["mode"] == "structural" and est["tier3_mode"] == {}
    assert est["source_statements"]["tier0"] == 6 * n + n_id
    # + one key probe per object + the one current_user() read of `run --capabilities`
    assert est["target_statements"]["tier0"] == 6 * n + 2 * n_id + n + 1
    assert est["target_rows_fetched"] == n + 1
    lb = estimate_cost(spec, Tolerances("t1"), mode="structural", family="sqlserver", target_kind="lakebase")
    assert lb["target_rows_fetched"] == 0  # no probe off Databricks
    for side in ("source_statements", "target_statements"):
        assert est[side]["total"] == est[side]["tier0"]
        assert {k for k, v in est[side].items() if v and k not in ("tier0", "total")} == set()
    assert est["source_rows_fetched"] == 0 and est["target_rows_fetched"] == n + 1
    pg = estimate_cost(spec, Tolerances("t1"), mode="structural",
                       family="postgres", target_kind="lakebase")
    assert pg["source_statements"]["tier0"] == 7 * n + 2 * n_id + 1
    assert pg["target_statements"]["tier0"] == 7 * n + 2 * n_id + 1
    with pytest.raises(ValueError, match="--family"):
        estimate_cost(spec, Tolerances("t1"), mode="structural")
    assert estimate_cost(spec, Tolerances("t1"), mode="structural",
                         family="redshift")["source_statements"]["tier0"] == 0


def test_catalog_statements_match_what_the_adapters_issue():
    """CATALOG_STATEMENTS is bound to the code that issues the statements: two schema_facts plus
    one identity_state plus the session read cost exactly the counted statements."""
    for cls in (SqlServerSourceAdapter, PostgresSourceAdapter):
        calls = []
        inst = object.__new__(cls)

        def stub(sql, params=()):
            calls.append(sql)
            if "server_version_num" in sql:
                return [("150000",)]
            if sql.startswith("SELECT pg_get_serial_sequence"):
                return [("seq1",)]
            if "FROM seq1" in sql:
                return [(5, True, 1)]
            return []

        inst._rows = stub
        inst._execute = lambda sql, params=(): calls.append(sql)  # savepoint statements: sent
        inst.schema_facts("dbo.loans")
        inst.schema_facts("dbo.loans")
        inst.identity_state("dbo.loans", "loan_id")
        st = cls.CATALOG_STATEMENTS
        assert len(calls) == 2 * st["schema_facts"] + st["identity_state"] + st["session"], cls

    calls = []

    def run_query(sql, params=None):
        calls.append(sql)
        if "SHOW CREATE TABLE" in sql:
            return [("CREATE TABLE `c`.`s`.`t` (\n"
                     "  `col` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)\n)",)]
        if "MAX(`col`)" in sql:
            return [(7,)]
        return []

    _uc_schema_facts(run_query, "c", "s", "t")
    _uc_schema_facts(run_query, "c", "s", "t")
    _uc_identity_state(run_query, "c", "s", "t", "col")
    st = DatabricksTargetAdapter.CATALOG_STATEMENTS
    assert len(calls) == 2 * st["schema_facts"] + st["identity_state"] + st["session"]


def test_estimate_cli_structural_needs_family(tmp_path, capsys, monkeypatch):
    """`estimate --mode structural` refuses without --family (the counts are per adapter) and
    prints tier0 with one."""
    import recon.cli as cli
    monkeypatch.setattr(cli, "_load_spec", lambda *a: (_spec(), None))
    monkeypatch.setattr(cli, "load_tolerances", lambda p: Tolerances("t1"))
    with pytest.raises(SystemExit, match="--family"):
        cli.main(["estimate", "--mapping", "m", "--tolerances", "t", "--mode", "structural"])
    rc = cli.main(["estimate", "--mapping", "m", "--tolerances", "t", "--mode", "structural",
                   "--family", "sqlserver"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out)["source_statements"]["tier0"] == 13


def test_tier0_unreadable_catalog_blocks_merge():
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers})
    result = run_recon("u1", "live", _spec(), Tolerances("t1"), [], source, target, rerun_proof=PROVEN_RERUN)
    t0 = result["tiers"][0]
    assert t0["name"] == "structural_parity" and t0["passed"] is True
    assert t0["stats"]["structural_checks"] == {c: "unsupported" for c in CATEGORIES}
    assert t0["stats"]["dictionary_unavailable"]
    assert not t0["stats"].get("unverified")
    assert any(w.startswith("UNVERIFIED structural_parity: structure unavailable:")
               for w in result["warnings"])
    assert result["merge_eligible"] is False
    assert result["merge_block_reasons"] == ["structural_gap", "warnings"]


class _DictErrorTarget(FakeTarget):
    def schema_facts(self, table):
        from recon.adapters import DictionaryError
        raise DictionaryError(f"{table}: sys.triggers read failed (RuntimeError)")


def test_tier0_dictionary_error_records_view_not_credential():
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    target = _DictErrorTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers})
    result = run_recon("u1", "live", _spec(), Tolerances("t1"), [], source, target)  # run completes
    t0 = result["tiers"][0]
    notes = t0["stats"]["dictionary_unavailable"]
    assert any("sys.triggers read failed (RuntimeError)" in n for n in notes)
    assert not any("DSN" in n or "password" in n for n in notes)
    assert result["merge_eligible"] is False and "structural_gap" in result["merge_block_reasons"]


class _IdErrorTarget(FakeTarget):
    def identity_state(self, table, column):
        from recon.adapters import DictionaryError
        raise DictionaryError(f"{table}: SHOW CREATE TABLE read failed (RuntimeError)")


def test_tier0_identity_read_error_is_unverified_and_blocks_merge():
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    target = _IdErrorTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                            schema={"loans": TARGET_LOANS_FACTS, "borrowers": BORROWER_FACTS})
    result = run_recon("u1", "live", _spec(), Tolerances("t1"), [], source, target, rerun_proof=PROVEN_RERUN)
    t0 = result["tiers"][0]
    assert any("SHOW CREATE TABLE read failed" in n for n in t0["stats"]["unverified"])
    assert any("UNVERIFIED" in w for w in result["warnings"])
    assert result["merge_eligible"] is False and "structural_gap" in result["merge_block_reasons"]


def test_build_result_structural_gap_from_checks_alone():
    from recon.tiers import TierResult
    t = TierResult(0, "structural_parity", True, 1, [],
                   {"structural_checks": {"constraints": "checked", "triggers": "unsupported",
                                          "indexes": "checked", "sequences_identity": "checked",
                                          "grants": "effective"}})
    r = build_result("u", "live", "m1", "t1", [t], rerun_proof=PROVEN_RERUN)
    assert "structural_gap" in r["merge_block_reasons"] and r["merge_eligible"] is False
    t = TierResult(0, "structural_parity", True, 1, [],
                   {"structural_checks": {"indexes": "unsupported", "triggers": "checked"}})
    r = build_result("u", "live", "m1", "t1", [t], rerun_proof=PROVEN_RERUN)
    assert "structural_gap" not in r["merge_block_reasons"] and r["merge_eligible"] is True


def test_tier0_target_without_identity_reader_marks_category_unchecked():
    tgt_facts = _facts(TARGET_LOANS_FACTS, unsupported=frozenset({"sequences_identity"}))
    result = _live(loans_tgt_facts=tgt_facts)
    t0 = result["tiers"][0]
    assert "sequence_missing" not in {f["check"] for f in t0["findings"]}
    assert "identity_missing" not in {f["check"] for f in t0["findings"]}
    assert t0["stats"]["structural_checks"]["sequences_identity"] == "unsupported"


def test_tier0_informational_target_fk_is_a_finding_not_a_pass():
    tgt_facts = _facts(TARGET_LOANS_FACTS,
                       foreign_keys=set(),
                       foreign_keys_informational={(("borrower_id",), "borrowers", ("borrower_id",))})
    result = _live(loans_tgt_facts=tgt_facts)
    t0 = result["tiers"][0]
    codes = {f["check"] for f in t0["findings"]}
    assert "foreign_key_informational_only" in codes and "foreign_key_missing" not in codes
    assert t0["stats"]["structural_diff"]["loans"]["constraints"]
    assert result["merge_eligible"] is False
    assert "structural_gap" in result["merge_block_reasons"]


def test_tier0_source_unsupported_category_warns_even_when_target_is_empty():
    src = _facts(LOANS_FACTS, unsupported=frozenset({"triggers"}), triggers={})
    result = _live(loans_src_facts=src)  # target has no triggers either
    t0 = result["tiers"][0]
    assert result["verdict"] == "PASS" and t0["passed"] is True
    assert any("source dictionary cannot expose triggers" in n
               for n in t0["stats"]["unverified"])
    assert result["merge_eligible"] is False


def test_databricks_fixture_marks_triggers_checked_not_a_hole():
    d = load_dictionary(FIXTURES / "example_databricks" / "dictionary.json")
    t = next(iter(d.tables.values()))
    sc = structural_checks([(t, t)])
    # Delta reads every category but indexes (identity comes from SHOW CREATE TABLE DDL)
    assert sc == {c: ("unsupported" if c == "indexes" else
                      "direct_only" if c == "grants" else "checked") for c in sc}


def test_tier0_informational_source_keys_must_exist_on_the_target():
    src = _facts(LOANS_FACTS, primary_key=(), primary_key_informational=("loan_id",),
                 foreign_keys=set(),
                 foreign_keys_informational={(("borrower_id",), "borrowers", ("borrower_id",))})
    empty_tgt = _facts(TARGET_LOANS_FACTS, primary_key=(), foreign_keys=set())
    result = _live(loans_src_facts=src, loans_tgt_facts=empty_tgt)
    t0 = result["tiers"][0]
    codes = {f["check"] for f in t0["findings"]}
    assert {"primary_key_informational_missing", "foreign_key_informational_missing"} <= codes
    assert t0["stats"]["structural_diff"]["loans"]["constraints"]
    assert result["merge_eligible"] is False

    info_tgt = _facts(empty_tgt, primary_key_informational=("loan_id",),
                      foreign_keys_informational={(("borrower_id",), "borrowers", ("borrower_id",))})
    assert "informational_missing" not in " ".join(
        f["check"] for f in _live(loans_src_facts=src, loans_tgt_facts=info_tgt)["tiers"][0]["findings"])

    enforced_tgt = _facts(TARGET_LOANS_FACTS)  # enforced pk + fk cover the informational source
    assert "informational_missing" not in " ".join(
        f["check"] for f in _live(loans_src_facts=src, loans_tgt_facts=enforced_tgt)["tiers"][0]["findings"])


def test_compare_triggers_flags_a_granularity_mismatch():
    s = SchemaFacts(triggers={"trg": ("after", ("insert",), "row")})
    t = SchemaFacts(triggers={"x": ("after", ("insert",), "statement")})
    findings, tight = compare_triggers("o", s, t)
    assert [f.check for f in findings] == ["trigger_granularity_mismatch"] and tight == []


def test_compare_triggers_counts_multiplicity_by_shape():
    s = SchemaFacts(triggers={"a": ("after", ("insert",), "row"),
                              "b": ("after", ("insert",), "row")})
    t = SchemaFacts(triggers={"x": ("after", ("insert",), "row")})
    findings, extra = compare_triggers("o", s, t)
    assert [f.check for f in findings] == ["trigger_missing"]
    assert "after insert row: source 2, target 1" == findings[0].detail and extra == []
    findings, extra = compare_triggers("o", s,
                                       SchemaFacts(triggers=dict(s.triggers)))
    assert findings == [] and extra == []


def test_compare_triggers_passes_when_both_granularities_are_covered():
    both = {"a": ("after", ("insert",), "row"), "b": ("after", ("insert",), "statement")}
    findings, tight = compare_triggers("o", SchemaFacts(triggers=both),
                                       SchemaFacts(triggers=dict(both)))
    assert findings == [] and tight == []


def test_tier0_informational_source_fk_met_by_an_enforced_target_fk_is_clean():
    src = _facts(LOANS_FACTS, foreign_keys=set(),
                 foreign_keys_informational={(("borrower_id",), "borrowers", ("borrower_id",))})
    tgt = _facts(TARGET_LOANS_FACTS)
    result = _live(loans_src_facts=src, loans_tgt_facts=tgt)
    t0 = result["tiers"][0]
    assert t0["passed"] is True and not t0["stats"].get("structural_diff")


def test_tier0_target_only_trigger_fails_despite_accept_target_only_constraints():
    loans, borrowers = _rows(12)
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    tgt_facts = _facts(TARGET_LOANS_FACTS, triggers={"x": ("after", ("delete",), "row")})
    target = FakeTarget({"loans": [dict(r) for r in loans], "borrowers": borrowers},
                        schema={"loans": tgt_facts, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    result = run_recon("u1", "live", _spec(),
                       Tolerances("t1", accept_target_only_constraints=True), [], source, target)
    t0 = result["tiers"][0]
    assert "trigger_extra" in {f["check"] for f in t0["findings"]} and t0["passed"] is False
    assert not t0["stats"].get("accepted_target_only_constraints")


def test_tier0_target_unsupported_category_warns_even_when_source_is_empty():
    tgt = _facts(TARGET_LOANS_FACTS, unsupported=frozenset({"triggers"}), triggers={})
    result = _live(loans_tgt_facts=tgt)  # source has no triggers either
    t0 = result["tiers"][0]
    assert t0["passed"] is True
    assert any("target dictionary cannot expose triggers" in n
               for n in t0["stats"]["unverified"])
    assert result["merge_eligible"] is False


def test_tier0_informational_target_pk_still_counts_for_a_blind_source():
    src = _facts(LOANS_FACTS, unsupported=frozenset({"constraints"}),
                 primary_key=(), unique=frozenset(), foreign_keys=set(),
                 foreign_keys_informational=set(), not_null=frozenset(),
                 checks=frozenset(), check_count=0, expression_unique=frozenset())
    tgt = _facts(TARGET_LOANS_FACTS, primary_key=(),
                 primary_key_informational=("loan_id",))
    result = _live(loans_src_facts=src, loans_tgt_facts=tgt)
    t0 = result["tiers"][0]
    assert t0["passed"] is True
    assert any("target has" in n and "constraints" in n for n in t0["stats"]["unverified"])
    assert result["merge_eligible"] is False
    assert t0["stats"]["loans"]["target"]["primary_key_informational"] == ["loan_id"]


_INFO_PK = _facts(TARGET_LOANS_FACTS, primary_key=(), primary_key_informational=("loan_id",))


def test_tier0_informational_target_pk_blocks_on_lakebase_unchanged():
    """No target_kind (Lakebase/Postgres path): an informational PK is a blocking finding, as
    before the Databricks carve-out; the carve-out is gated on target_kind, never on its absence."""
    result = _live(loans_tgt_facts=_INFO_PK)
    t0 = result["tiers"][0]
    codes = {f["check"] for f in t0["findings"]}
    assert "primary_key_informational_only" in codes and "primary_key_mismatch" not in codes
    assert t0["stats"]["structural_diff"]["loans"]["constraints"]
    assert all(f["severity"] == "error" for f in t0["findings"])
    assert t0["stats"]["platform_inherent"] == []
    assert result["merge_eligible"] is False
    assert "structural_gap" in result["merge_block_reasons"]


def test_tier0_informational_uc_pk_with_unique_keys_is_info_and_merges():
    """Databricks target (flipped from `..._is_a_finding_not_a_pass`): Unity Catalog cannot
    enforce a PK, so an informational PK whose key the uniqueness probe proves unique is a
    platform-inherent info finding: shown in the diff, never a blocker."""
    result = _live(loans_tgt_facts=_INFO_PK, target_kind="databricks")
    t0 = result["tiers"][0]
    pk = [f for f in t0["findings"] if f["check"] == "primary_key_informational_only"]
    assert len(pk) == 1 and pk[0]["severity"] == "info" and "12 rows, 12 distinct keys" in pk[0]["detail"]
    assert t0["passed"] is True
    assert t0["stats"]["structural_diff"]["loans"]["constraints"]  # diff_by_object shows it
    assert t0["stats"]["platform_inherent"] == [{"object": "loans", "check": "primary_key_informational_only",
                                                 "detail": pk[0]["detail"]}]
    assert result["merge_eligible"] is True and result["merge_block_reasons"] == []


def test_tier0_informational_uc_pk_with_duplicate_keys_still_blocks():
    """Structural mode (the DEGRADED verifier path, no row tier to catch the duplicate): the
    probe finds two rows per key, so the unenforced PK stays a blocking finding."""
    loans, _ = _rows(12)
    result = _live(loans_tgt_facts=_INFO_PK, target_kind="databricks", mode="structural",
                   extra_target_loans=[loans[0]])
    t0 = result["tiers"][0]
    pk = [f for f in t0["findings"] if f["check"] == "primary_key_informational_only"]
    assert len(pk) == 1 and pk[0]["severity"] == "error" and "13 rows, 12 distinct keys" in pk[0]["detail"]
    assert t0["passed"] is False and result["merge_eligible"] is False
    assert "structural_gap" in result["merge_block_reasons"]


def test_tier0_informational_uc_pk_carve_out_holds_in_structural_mode():
    result = _live(loans_tgt_facts=_INFO_PK, target_kind="databricks", mode="structural", rerun_proof=None)
    t0 = result["tiers"][0]
    assert t0["passed"] is True and [f["severity"] for f in t0["findings"]] == ["info"]
    assert result["merge_block_reasons"] == ["mode"]  # exactly what a clean structural run blocks on


def test_tier0_uniqueness_probe_is_scoped_to_the_mappings_target_where():
    """A duplicate key in another unit's partition of a shared target table must not decide
    this unit's probe: `key_uniqueness` gets the mapping's `target_where`."""
    import dataclasses
    from recon.structure import tier0_structural_parity
    loans, borrowers = _rows(12)
    spec = _spec()
    scoped = dataclasses.replace(spec.objects[0], target_where="borrower_id = 1")
    spec = dataclasses.replace(spec, objects=[scoped, spec.objects[1]])
    source = FakeSource({"dbo.loans": loans, "dbo.borrowers": borrowers},
                        schema={"dbo.loans": LOANS_FACTS, "dbo.borrowers": BORROWER_FACTS},
                        sequences={("dbo.loans", "loan_id"): 13})
    dup = dict(loans[0])  # borrower_id 2: outside this unit's partition
    assert dup["borrower_id"] != 1
    target = FakeTarget({"loans": [dict(r) for r in loans] + [dup], "borrowers": borrowers},
                        schema={"loans": _INFO_PK, "borrowers": BORROWER_FACTS},
                        sequences={("loans", "loan_id"): 13})
    target.target_kind = "databricks"
    t0 = tier0_structural_parity(spec, Tolerances("t1"), source, target)
    pk = [f for f in t0.findings if f.check == "primary_key_informational_only"]
    assert len(pk) == 1 and pk[0].severity == "info" and "4 rows, 4 distinct keys" in pk[0].detail
    # the same table unscoped sees the duplicate
    unscoped = tier0_structural_parity(_spec(), Tolerances("t1"), source, target)
    pk = [f for f in unscoped.findings if f.check == "primary_key_informational_only"]
    assert len(pk) == 1 and pk[0].severity == "error" and "13 rows, 12 distinct keys" in pk[0].detail


def test_tier0_informational_source_key_on_a_pipeline_output_uses_the_probe():
    """A Databricks source declares its PK informationally (`_uc_schema_facts`), so `pk` is empty
    and `pk_info` carries the key: an MV/ST target with unique keys is not_applicable, not
    `primary_key_informational_missing`; a duplicate keeps the blocking finding."""
    src = _facts(LOANS_FACTS, primary_key=(), primary_key_informational=("loan_id",))
    mv = _facts(TARGET_LOANS_FACTS, primary_key=(), primary_key_informational=(), identity_columns=set(),
                table_type="MATERIALIZED_VIEW")
    result = _live(loans_src_facts=src, loans_tgt_facts=mv, target_kind="databricks", rerun_proof=None,
                   mode="structural", table_types={"loans": "MATERIALIZED_VIEW"})
    codes = {f["check"]: f["severity"] for f in result["tiers"][0]["findings"] if f["object"] == "loans"}
    assert codes["primary_key_not_applicable"] == "info" and "primary_key_informational_missing" not in codes
    assert result["merge_block_reasons"] == ["mode"]
    loans, _ = _rows(12)
    result = _live(loans_src_facts=src, loans_tgt_facts=mv, target_kind="databricks", rerun_proof=None,
                   mode="structural", table_types={"loans": "MATERIALIZED_VIEW"}, extra_target_loans=[loans[0]])
    codes = {f["check"]: f["severity"] for f in result["tiers"][0]["findings"] if f["object"] == "loans"}
    assert codes["primary_key_informational_missing"] == "error" and "primary_key_not_applicable" not in codes
    # off Databricks the informational source key is still missing on a PK-less target
    result = _live(loans_src_facts=src, loans_tgt_facts=mv, mode="structural", rerun_proof=None)
    codes = {f["check"]: f["severity"] for f in result["tiers"][0]["findings"] if f["object"] == "loans"}
    assert codes["primary_key_informational_missing"] == "error"


def test_tier0_grant_extra_for_own_principal_is_info_any_other_grantee_blocks():
    tgt = _facts(TARGET_LOANS_FACTS, grants={**TARGET_LOANS_FACTS.grants, "sp-migration": frozenset({"select", "modify"})})
    own = frozenset({"SP-Migration"})
    result = _live(loans_tgt_facts=tgt, target_kind="databricks", own_principals=own)
    t0 = result["tiers"][0]
    extra = [f for f in t0["findings"] if f["check"] == "grant_extra"]
    assert len(extra) == 1 and extra[0]["severity"] == "info" and "sp-migration" in extra[0]["detail"]
    assert result["merge_eligible"] is True
    # another group holding the same extra grant is a blast-radius defect: blocks
    tgt = _facts(TARGET_LOANS_FACTS, grants={**TARGET_LOANS_FACTS.grants, "analysts": frozenset({"select"})})
    result = _live(loans_tgt_facts=tgt, target_kind="databricks", own_principals=own)
    extra = [f for f in result["tiers"][0]["findings"] if f["check"] == "grant_extra"]
    assert len(extra) == 1 and extra[0]["severity"] == "error"
    assert result["merge_eligible"] is False and "structural_gap" in result["merge_block_reasons"]
    # Lakebase: the own principal's extra still blocks (nothing is softened off-Databricks)
    tgt = _facts(TARGET_LOANS_FACTS, grants={**TARGET_LOANS_FACTS.grants, "sp-migration": frozenset({"select"})})
    result = _live(loans_tgt_facts=tgt, own_principals=own)
    assert result["merge_eligible"] is False
    assert [f["severity"] for f in result["tiers"][0]["findings"] if f["check"] == "grant_extra"] == ["error"]


def test_tier0_grant_missing_is_never_softened_on_databricks():
    src = _facts(LOANS_FACTS, grants={"app_role": frozenset({"select"})})
    tgt = _facts(TARGET_LOANS_FACTS, grants={"sp-migration": frozenset({"select"})})
    result = _live(loans_src_facts=src, loans_tgt_facts=tgt, target_kind="databricks",
                   own_principals=frozenset({"sp-migration"}))
    missing = [f for f in result["tiers"][0]["findings"] if f["check"] == "grant_missing"]
    assert missing and all(f["severity"] == "error" for f in missing)
    assert result["merge_eligible"] is False


def test_materialized_view_target_with_unique_keys_is_eligible_and_rerun_not_applicable():
    """A Lakeflow MATERIALIZED_VIEW declares no PK and no identity: with the probe passing both
    are recorded not_applicable(<kind>) (info), and the rerun posture derives not_applicable
    from the kind (no proof needed); a MANAGED table keeps the spec's `required`."""
    mv = _facts(TARGET_LOANS_FACTS, primary_key=(), primary_key_informational=(), identity_columns=set(),
                table_type="MATERIALIZED_VIEW")
    result = _live(loans_tgt_facts=mv, target_kind="databricks", rerun_proof=None,
                   table_types={"loans": "MATERIALIZED_VIEW", "borrowers": "STREAMING_TABLE"})
    t0 = result["tiers"][0]
    by_check = {f["check"]: f for f in t0["findings"] if f["object"] == "loans"}
    assert by_check["primary_key_not_applicable"]["severity"] == "info"
    assert by_check["primary_key_not_applicable"]["detail"].startswith("not_applicable(MATERIALIZED_VIEW)")
    assert by_check["identity_not_applicable"]["severity"] == "info"
    assert "primary_key_mismatch" not in by_check and "identity_missing" not in by_check
    assert t0["stats"]["loans"]["identity"] == {"not_applicable": "MATERIALIZED_VIEW"}
    assert t0["stats"]["loans"]["target"]["table_type"] == "MATERIALIZED_VIEW"
    assert result["rerun_posture"] == "not_applicable"
    assert result["rerun_posture_reason"] == "not_applicable(MATERIALIZED_VIEW/STREAMING_TABLE)"
    assert result["target_table_kinds"] == {"loans": "MATERIALIZED_VIEW", "borrowers": "STREAMING_TABLE"}
    assert result["merge_eligible"] is True and result["merge_block_reasons"] == []
    # duplicate keys in the view: the missing PK is a real mismatch again
    loans, _ = _rows(12)
    result = _live(loans_tgt_facts=mv, target_kind="databricks", rerun_proof=None, mode="structural",
                   extra_target_loans=[loans[0]], table_types={"loans": "MATERIALIZED_VIEW"})
    codes = {f["check"]: f["severity"] for f in result["tiers"][0]["findings"] if f["object"] == "loans"}
    assert codes["primary_key_mismatch"] == "error" and "primary_key_not_applicable" not in codes
    # MANAGED (the default kind) keeps the spec posture: no proof blocks
    result = _live(target_kind="databricks", rerun_proof=None)
    assert result["rerun_posture"] == "required" and "rerun_missing" in result["merge_block_reasons"]
    assert result["target_table_kinds"] == {"loans": "MANAGED", "borrowers": "MANAGED"}
    assert "rerun_posture_reason" not in result
    # a unit mixing a MANAGED table with an MV keeps `required`: the table still needs the proof
    result = _live(target_kind="databricks", rerun_proof=None, table_types={"loans": "MATERIALIZED_VIEW"})
    assert result["rerun_posture"] == "required"


def test_tier0_source_unsupported_category_with_target_content_warns():
    src_facts = _facts(LOANS_FACTS, unsupported=frozenset({"triggers"}), triggers={})
    tgt_facts = _facts(TARGET_LOANS_FACTS, triggers={"trg": ("after", ("insert",), "row")})
    result = _live(loans_src_facts=src_facts, loans_tgt_facts=tgt_facts)
    t0 = result["tiers"][0]
    assert result["verdict"] == "PASS" and t0["passed"] is True
    assert any("source dictionary cannot expose triggers: target has 1" in n
               for n in t0["stats"]["unverified"])
    assert result["merge_eligible"] is False


def test_tier0_absent_target_fk_is_still_missing():
    result = _live(loans_tgt_facts=_facts(TARGET_LOANS_FACTS, foreign_keys=set()))
    assert "foreign_key_missing" in {f["check"] for f in result["tiers"][0]["findings"]}


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
    assert facts.triggers == {"trg_a": ("after", ("insert", "update"), "statement"),
                              "trg_i": ("instead of", ("delete",), "statement")}
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
                        outer.rows = [("trg_a", 1 + 2 + 4 + 16), ("trg_i", 64 + 8)]  # row before insert,update / instead of delete (statement)
                    elif "table_privileges" in sql_l:
                        outer.rows = [("app_rw", "SELECT"), ("reporting_ro", "SELECT")]
                    elif "pg_attribute" in sql_l and "attidentity" in sql_l:
                        outer.rows = [("loan_id", True, False, "r")]
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
    assert facts.triggers["trg_a"] == ("before", ("insert", "update"), "row")
    assert facts.triggers["trg_i"] == ("instead of", ("delete",), "statement")
    assert facts.grants == {"app_rw": frozenset({"select"}), "reporting_ro": frozenset({"select"})}


def test_sqlserver_grants_query_expands_role_membership():
    from tests.loans import _StubConn

    class Conn(_StubConn):
        def cursor(self):
            outer = self

            class Cur:
                def execute(self, sql, params=()):
                    outer.executed.append((sql, params))
                    outer.rows = []

                def fetchall(self):
                    return outer.rows
            return Cur()
    from recon.adapters import SqlServerSourceAdapter
    a = SqlServerSourceAdapter.__new__(SqlServerSourceAdapter)
    a._conn = Conn()
    a.statements = a.rows_fetched = 0
    a.schema_facts("dbo.loans")
    grants = next((s, p) for s, p in a._conn.executed if "database_permissions" in s)
    sql, params = grants
    for fragment in ("sys.database_role_members", "db_datareader", "db_datawriter", "db_owner",
                     "p.class = 3", "p.class = 0", "p.state = 'D'", "EXCEPT"):
        assert fragment in sql
    assert params == ("dbo", "loans", "dbo", "dbo", "loans", "dbo", "dbo")


def test_postgres_grants_query_expands_role_membership():
    from tests.loans import _StubConn

    class Conn(_StubConn):
        def cursor(self):
            outer = self

            class Cur:
                def execute(self, sql, params=()):
                    outer.executed.append((sql, params))
                    sql_l = sql.lower()
                    if "server_version" in sql_l or "current_setting" in sql_l:
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
    a.schema_facts("public.loans")
    grants = next((s, p) for s, p in a._conn.executed if "table_privileges" in s)
    sql, params = grants
    for fragment in ("pg_auth_members", "pg_read_all_data", "pg_write_all_data",
                     "rolsuper", "relowner"):
        assert fragment in sql
    assert params == ("public", "loans", "public", "loans")


def _pg_grants_sql(version_num):
    from tests.loans import _StubConn

    class Conn(_StubConn):
        def cursor(self):
            outer = self

            class Cur:
                def execute(self, sql, params=()):
                    outer.executed.append((sql, params))
                    sql_l = sql.lower()
                    if "server_version" in sql_l or "current_setting" in sql_l:
                        outer.rows = [(version_num,)]
                    else:
                        outer.rows = []

                def fetchall(self):
                    return outer.rows
            return Cur()
    from recon.adapters import PostgresSourceAdapter
    a = PostgresSourceAdapter.__new__(PostgresSourceAdapter)
    a._conn = Conn()
    a.statements = a.rows_fetched = 0
    a.schema_facts("public.loans")
    return next(s for s, _ in a._conn.executed if "table_privileges" in s)


def test_postgres_grants_query_honours_pg16_inherit_option():
    sql16 = _pg_grants_sql(160000)
    assert "am.inherit_option" in sql16 and "u.rolinherit" not in sql16
    sql15 = _pg_grants_sql(150000)
    assert "u.rolinherit" in sql15 and "am.inherit_option" not in sql15


def test_structural_checks_marks_direct_only_grants():
    s = SchemaFacts()
    t = SchemaFacts(grants_effective=False)
    assert structural_checks([(s, t)])["grants"] == "direct_only"
    assert structural_checks([(s, s)])["grants"] == "effective"


def test_dictionary_grants_effective_follows_family():
    d = load_dictionary(FIXTURES / "example_databricks" / "dictionary.json")
    assert all(f.grants_effective is False for f in d.tables.values())
    for family in ("example_sqlserver", "example_postgres"):
        p = FIXTURES / family / "dictionary.json"
        if p.exists():
            assert all(f.grants_effective for f in load_dictionary(p).tables.values()), family


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
                return [("loan_id", "NO"), ("note", "YES")]
            if "SHOW CREATE" in sql:
                return [("CREATE TABLE `mig`.`s`.`loans` (\n"
                         "  `loan_id` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 5 "
                         "INCREMENT BY 1),\n  `note` STRING)",)]
            if "MAX(" in sql:
                return [(1000,)]
            return []
    a._sql = Sql()
    facts = a.schema_facts("loans")
    # UC constraints are informational, never enforced
    assert facts.primary_key_informational == ("loan_id",) and not facts.primary_key
    # UC foreign keys are informational, never enforced
    assert facts.foreign_keys_informational == {(("borrower_id",), "s.borrowers", ("borrower_id",))}
    assert not facts.foreign_keys
    assert facts.not_null == {"loan_id"} and facts.identity_columns == {"loan_id"}
    assert facts.checks == {"current_balance >= 0"} and facts.check_count == 1
    assert facts.grants == {"svc_app": frozenset({"select", "modify"})}
    assert facts.unsupported == frozenset({"indexes"})
    assert facts.triggers == {}
    state = a.identity_state("loans", "loan_id")
    assert (state.next, state.increment) == (1001, 1)  # MAX(1000) + increment
    assert a.identity_state("loans", "note") is None
    joined = " ".join(answers)
    for view in ("table_constraints", "key_column_usage", "referential_constraints",
                 "columns", "check_constraints", "table_privileges"):
        assert f"information_schema.{view}" in joined
    assert "t.table_owner" in joined  # UC information_schema.tables has table_owner, not owner


def test_build_result_merge_block_reasons():
    from recon.tiers import TierResult
    ok = TierResult(0, "structural_parity", True, 1, [], {})
    assert build_result("u", "live", "m1", "t1", [ok], rerun_proof=PROVEN_RERUN)["merge_block_reasons"] == []
    failed = TierResult(1, "row_counts", False, 1, [Finding("o", "row_count_diff", "d")], {})
    assert build_result("u", "live", "m1", "t1", [ok, failed],
                        rerun_proof=PROVEN_RERUN)["merge_block_reasons"] == ["tier_failed"]
    assert build_result("u", "fixture", "m1", "t1", [ok], rerun_proof=PROVEN_RERUN)["merge_block_reasons"] == ["mode"]
    gap = TierResult(0, "structural_parity", False, 1, [Finding("o", "trigger_missing", "d")], {})
    r = build_result("u", "live", "m1", "t1", [gap])
    assert r["merge_block_reasons"][0] == "structural_gap" and "tier_failed" in r["merge_block_reasons"]


def test_databricks_source_adapter_reads_uc_dictionary():
    from recon.adapters import DatabricksSourceAdapter
    a = DatabricksSourceAdapter.__new__(DatabricksSourceAdapter)
    answers = []

    class Conn:
        def cursor(self):
            class Cur:
                def execute(self, sql, params=()):
                    answers.append(sql)
                    if "check_constraints" in sql:
                        self.rows = []
                    elif "table_constraints" in sql:
                        self.rows = [("PRIMARY KEY", "pk", "loan_id", None, None, None)]
                    else:
                        self.rows = []
                def fetchall(self):
                    return self.rows
            return Cur()
    a._conn = Conn()
    a.statements = a.rows_fetched = 0
    facts = a.schema_facts("cat.s.loans")
    assert facts.primary_key_informational == ("loan_id",)
    assert "information_schema.table_constraints" in " ".join(answers)
    assert a.identity_state("cat.s.loans", "loan_id") is None


def test_uc_identity_state_uses_min_for_a_negative_increment():
    from recon.adapters import DatabricksTargetAdapter
    a = DatabricksTargetAdapter.__new__(DatabricksTargetAdapter)
    a._catalog, a._schema = "mig", "s"

    class Sql:
        def _rows(self, sql, params):
            if "SHOW CREATE" in sql:
                return [("`loan_id` BIGINT GENERATED ALWAYS AS IDENTITY "
                         "(START WITH 100 INCREMENT BY -1)",)]
            assert "MIN(" in sql.upper()
            return [(98,)]
    a._sql = Sql()
    state = a.identity_state("loans", "loan_id")
    assert (state.next, state.increment) == (97, -1)


def test_uc_identity_state_uses_the_declared_start_on_an_empty_table():
    from recon.adapters import DatabricksTargetAdapter
    a = DatabricksTargetAdapter.__new__(DatabricksTargetAdapter)
    a._catalog, a._schema = "mig", "s"

    class Sql:
        def _rows(self, sql, params):
            if "SHOW CREATE" in sql:
                return [("`loan_id` BIGINT GENERATED BY DEFAULT AS IDENTITY",)]
            if "MAX(" in sql:
                return [(None,)]
            return []
    a._sql = Sql()
    state = a.identity_state("loans", "loan_id")
    assert (state.next, state.increment) == (1, 1)


def test_schema_facts_driver_errors_are_dictionary_errors_without_secrets():
    from tests.loans import _StubConn

    class Conn(_StubConn):
        def cursor(self):
            class Cur:
                def execute(self, sql, params=()):
                    if "sys.triggers" in sql.lower():
                        raise RuntimeError("connection DSN=prod-host;password=hunter2 failed")
                    self.rows = []
                def fetchall(self):
                    return self.rows
            return Cur()
    from recon.adapters import DictionaryError, SqlServerSourceAdapter
    a = SqlServerSourceAdapter.__new__(SqlServerSourceAdapter)
    a._conn = Conn()
    a.statements = a.rows_fetched = 0
    with pytest.raises(DictionaryError, match="sys.triggers read failed \\(RuntimeError\\)"):
        a.schema_facts("dbo.loans")
    try:
        a.schema_facts("dbo.loans")
    except DictionaryError as exc:
        assert "hunter2" not in str(exc) and "prod-host" not in str(exc)
