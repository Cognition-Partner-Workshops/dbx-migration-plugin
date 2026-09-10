"""Tier 7 schema parity: constraints, indexes, foreign keys, identity/sequence parity."""
import dataclasses

import pytest
from recon.adapters import SchemaFacts, _fk_action, _index_key_text
from recon.config import FieldMapping, MappingSpec, ObjectMapping, Tolerances
from recon.engine import run_recon
from recon.transactional import _check_key_text, _map_expression

from tests.fakes import FakeSource, FakeTarget
from tests.loans import (
    BORROWER_FACTS,
    LOANS_FACTS,
    TARGET_LOANS_FACTS,
    _codes,
    _facts,
    _rows,
    _run,
    _sides,
    _spec,
    _tier,
    _tightened,
)


def test_schema_parity_findings_map_through_the_spec():
    loans, borrowers = _rows(6)
    weak = SchemaFacts(primary_key=("loan_id",), unique=set(), foreign_keys=set(),
                       not_null={"loan_id"}, indexes=set(), check_count=0,
                       identity_columns={"loan_id"})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=weak)
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    assert _codes(result, "schema_parity") == sorted([
        "unique_missing", "foreign_key_missing", "not_null_missing", "not_null_missing",
        "not_null_missing", "not_null_missing", "index_missing", "index_missing",
        "check_constraint_missing", "check_constraint_missing"])
    fk = next(f for f in _tier(result, "schema_parity")["findings"] if f["check"] == "foreign_key_missing")
    assert "borrowers" in fk["detail"]


def test_index_covered_by_a_longer_target_index_is_parity():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    facts = _tier(result, "schema_parity")["stats"]["loans"]
    assert facts["target"]["indexes"] == [["borrower_id"], ["loan_status", "days_past_due", "loan_id"]]


def test_source_filtered_indexes_are_reported_for_a_manual_check_not_graded():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    source.schema["dbo.loans"] = SchemaFacts(
        primary_key=LOANS_FACTS.primary_key, unique=set(LOANS_FACTS.unique),
        foreign_keys=set(LOANS_FACTS.foreign_keys), not_null=set(LOANS_FACTS.not_null),
        indexes=set(LOANS_FACTS.indexes), check_count=2, identity_columns={"loan_id"},
        partial={("days_past_due",)})
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    parity = _tier(result, "schema_parity")
    assert parity["stats"]["partial_indexes_unverified"] == [
        ("loans: source filtered index ('days_past_due',) carries a predicate the harness cannot "
         "translate; confirm its target counterpart by hand")]
    assert parity["stats"]["loans"]["source"]["partial"] == [["days_past_due"]]



@pytest.mark.parametrize("src, tgt, code, needle", [
    (_facts(LOANS_FACTS, not_null=LOANS_FACTS.not_null - {"current_balance"}), _tightened(),
     "not_null_extra", "current_balance is NOT NULL but its source column is nullable"),
    (LOANS_FACTS, _tightened(unique=TARGET_LOANS_FACTS.unique | {("borrower_id", "loan_number")}),
     "unique_extra", "('borrower_id', 'loan_number') has no source counterpart"),
    (LOANS_FACTS, _tightened(foreign_keys=TARGET_LOANS_FACTS.foreign_keys
                             | {(("loan_number",), "loan_servicing.borrowers", ("borrower_id",))}),
     "foreign_key_extra", "('loan_number',) -> borrowers('borrower_id',) has no source counterpart"),
    (LOANS_FACTS, _tightened(check_count=3), "check_constraint_count_higher",
     "source 2 CHECK constraints, target 3"),
])
def test_a_target_only_constraint_fails_parity_because_it_rejects_legacy_valid_writes(src, tgt, code, needle):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target)
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    parity = _tier(result, "schema_parity")
    assert [f["check"] for f in parity["findings"]] == [code]
    assert needle in parity["findings"][0]["detail"]

    accepted = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert accepted["verdict"] == "PASS" and accepted["merge_eligible"] is True
    note = _tier(accepted, "schema_parity")["stats"]["accepted_target_only_constraints"]
    assert len(note) == 1 and note[0].startswith(f"loans: {code}: ")


@pytest.mark.parametrize("sqlserver, postgres, canonical", [
    ("([Balance]>=(0))", "CHECK ((balance >= (0)::numeric))", "balance >= 0"),
    ("([Balance]>=(0))", "CHECK (((0)::numeric <= balance))", "balance >= 0"),
    ("([Loan_Status]='FC' OR [Loan_Status]='AC')",
     "CHECK (((loan_status)::text = ANY ((ARRAY['AC'::character varying, 'FC'::character varying])::text[])))",
     "loan_status in ('AC', 'FC')"),
    ("([status]<>'X' AND [status]<>'Y')", "CHECK ((status <> ALL (ARRAY['X'::text, 'Y'::text])))",
     "status not in ('X', 'Y')"),
    ("(len([Code])<=(10))", "CHECK ((length((code)::text) <= 10))", "length(code) <= 10"),
    ("(([a]+[b])*(2)>(0))", "CHECK ((((a + b) * 2) > 0))", "(a + b) * 2 > 0"),
    ("([rate]>=(0.00) AND [rate]<=(1))", "CHECK (((rate <= (1)::numeric) AND (rate >= (0)::numeric)))",
     "rate <= 1 and rate >= 0"),
    ("([x] BETWEEN (1) AND (5))", "CHECK (((x >= 1) AND (x <= 5)))", "x <= 5 and x >= 1"),
    ("([n]>(-(1)))", "CHECK ((n > '-1'::integer))", "n > -1"),
    ("([x] IS NOT NULL OR [y] IS NOT NULL)", "CHECK (((y IS NOT NULL) OR (x IS NOT NULL)))",
     "x is not null or y is not null"),
])
def test_check_predicates_canonicalise_the_same_across_sql_server_and_postgres(sqlserver, postgres, canonical):
    assert _check_key_text(sqlserver, {}) == (canonical, True)
    assert _check_key_text(postgres, {}) == (canonical, True)


def test_check_predicate_canonical_form_keeps_what_distinguishes_rules():
    assert _check_key_text("(upper([x])='A')", {})[0] != _check_key_text("(upper([x])='a')", {})[0]
    assert _check_key_text("([a]-([b]-[c])>(0))", {})[0] == "a - (b - c) > 0"
    assert _check_key_text("(([a]-[b])-[c]>(0))", {})[0] == "a - b - c > 0"
    assert _check_key_text("CHECK ((a + (b * 2) > 0))", {})[0] == "a + b * 2 > 0"
    # source columns are renamed through the mapping; literals and function names are not
    assert _check_key_text("([Cust_ID]>(0) AND upper([memo])<>'cust_id')",
                           {"cust_id": "customer_id", "memo": "note"})[0] == \
        "customer_id > 0 and upper(note) <> 'cust_id'"


@pytest.mark.parametrize("definition", [
    "(datalength([Blob])<(100))",                       # engine-specific function
    "CHECK ((CASE WHEN a THEN 1 ELSE 0 END = 1))",      # outside the grammar
    "CHECK ((e ~~ '%@%'::text))",                       # LIKE: pattern/collation semantics differ
    "([e] like '%@%')",
])
def test_dialect_specific_check_predicates_are_not_portable(definition):
    assert _check_key_text(definition, {})[1] is False


def _checks_run(src_checks, tgt_checks, tol=None):
    loans, borrowers = _rows(6)
    tgt = _tightened(check_count=len(tgt_checks), checks=set(tgt_checks))
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, check_count=len(src_checks), checks=set(src_checks))
    return _run(source, target, tol=tol) if tol else _run(source, target)


def test_equal_check_counts_with_a_predicate_absent_from_the_target_fail_as_missing():
    # two on each side, but the target dropped the status rule and added a rule the source lacks
    # spelled on a column the source rule does not mention: nothing on the target can be it
    result = _checks_run(
        ["([Current_Balance]>=(0))", "([Loan_Status]='AC' OR [Loan_Status]='FC')"],
        ["CHECK ((current_balance >= (0)::numeric))", "CHECK ((current_balance >= (0)::numeric)) "])
    # the second target text canonicalises to the same rule, so the target has one predicate
    assert _codes(result, "schema_parity") == ["check_constraint_missing"]
    f = _tier(result, "schema_parity")["findings"][0]
    assert "([Loan_Status]='AC' OR [Loan_Status]='FC')" in f["detail"]
    assert "canonical: loan_status in ('AC', 'FC')" in f["detail"]
    assert result["merge_eligible"] is False


def test_equal_check_counts_with_different_predicates_are_unverified_and_block_merge():
    result = _checks_run(["([Current_Balance]>=(0))"], ["CHECK ((current_balance <= (0)::numeric))"])
    assert _codes(result, "schema_parity") == ["check_constraint_unverified"]
    detail = _tier(result, "schema_parity")["findings"][0]["detail"]
    assert "canonical: current_balance >= 0" in detail and "canonical: current_balance <= 0" in detail
    assert result["verdict"] == "FAIL" and result["merge_eligible"] is False
    # accept_target_only_constraints is about extra rules, not about unproven equivalence
    still = _checks_run(["([Current_Balance]>=(0))"], ["CHECK ((current_balance <= (0)::numeric))"],
                        tol=Tolerances("t1", accept_target_only_constraints=True))
    assert _codes(still, "schema_parity") == ["check_constraint_unverified"]
    # the recorded hand comparison demotes the pair to a stat
    accepted = _checks_run(["([Current_Balance]>=(0))"], ["CHECK ((current_balance <= (0)::numeric))"],
                           tol=Tolerances("t1", accept_unverified_check_constraints=True))
    assert accepted["verdict"] == "PASS" and accepted["merge_eligible"] is True
    note = _tier(accepted, "schema_parity")["stats"]["accepted_unverified_check_constraints"]
    assert len(note) == 1 and note[0].startswith("loans: 1 source CHECK(s) match no target CHECK")


def test_dialect_specific_check_predicate_with_no_textual_match_is_unverified_not_passed():
    result = _checks_run(["(datalength([Memo])<(100))"], ["CHECK ((octet_length(memo) < 100))"])
    assert _codes(result, "schema_parity") == ["check_constraint_unverified"]
    assert "dialect-specific construct" in _tier(result, "schema_parity")["findings"][0]["detail"]
    # the same dialect spelling on both sides is a match, not a guess
    same = _checks_run(["(datalength([Memo])<(100))"], ["CHECK ((datalength(memo) < 100))"])
    assert _codes(same, "schema_parity") == []


def test_check_predicates_are_compared_through_the_column_mapping():
    loans, borrowers = _rows(6)
    renamed_not_null = (TARGET_LOANS_FACTS.not_null - {"current_balance"}) | {"balance_current"}
    tgt = _tightened(not_null=renamed_not_null,
                     checks={"CHECK ((balance_current >= (0)::numeric))",
                             "CHECK ((loan_status = ANY (ARRAY['AC'::text, 'DL'::text, 'FC'::text])))"})
    source, target = _sides(loans, _renamed_rows(loans), borrowers, tgt_facts=tgt)
    result = _run(source, target, spec=_renamed_spec())
    assert _codes(result, "schema_parity") == []
    # a target rule still written against the old column name is not the mapped source rule
    stale = _tightened(not_null=renamed_not_null)
    unmapped = _run(*_sides(loans, _renamed_rows(loans), borrowers, tgt_facts=stale)[:2], spec=_renamed_spec())
    assert _codes(unmapped, "schema_parity") == ["check_constraint_unverified"]


def test_target_only_check_predicate_is_a_tightening_with_the_existing_decision_knob():
    result = _checks_run(["([Current_Balance]>=(0))"],
                         ["CHECK ((current_balance >= (0)::numeric))", "CHECK ((days_past_due >= 0))"])
    assert _codes(result, "schema_parity") == ["check_constraint_extra"]
    assert "days_past_due >= 0" in _tier(result, "schema_parity")["findings"][0]["detail"]
    accepted = _checks_run(["([Current_Balance]>=(0))"],
                           ["CHECK ((current_balance >= (0)::numeric))", "CHECK ((days_past_due >= 0))"],
                           tol=Tolerances("t1", accept_target_only_constraints=True))
    assert accepted["verdict"] == "PASS" and accepted["merge_eligible"] is True


def test_a_reader_that_only_counts_checks_falls_back_to_counts_and_says_so():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers,
                            tgt_facts=_tightened(checks=set()))
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    assert _tier(result, "schema_parity")["stats"]["check_predicates_unverified"] == [
        ("loans: 2 CHECK constraints on each side, but a catalog reader delivered counts only; "
         "the predicates were not compared")]
    assert _tier(result, "schema_parity")["stats"]["loans"]["source"]["checks"] == sorted(LOANS_FACTS.checks)


def test_target_only_constraints_on_unmapped_columns_or_out_of_scope_tables_are_noted_not_graded():
    loans, borrowers = _rows(6)
    facts = _tightened(
        unique=TARGET_LOANS_FACTS.unique | {("servicer_ref",)},
        foreign_keys=TARGET_LOANS_FACTS.foreign_keys
        | {(("servicer_id",), "loan_servicing.servicers", ("servicer_id",)),
           # a mapped parent, but the local column is outside the mapping...
           (("servicer_id",), "loan_servicing.borrowers", ("borrower_id",)),
           # ...or the referenced column is: neither is provably target-only
           (("loan_number",), "loan_servicing.borrowers", ("legacy_ref",))},
        not_null=TARGET_LOANS_FACTS.not_null | {"servicer_ref", "servicer_id"})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=facts)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    stats = _tier(result, "schema_parity")["stats"]
    assert stats["target_only_columns_unverified"] == [
        "loans: unique ('servicer_ref',) covers a column outside the mapping",
        "loans: FK ('loan_number',) -> borrowers('legacy_ref',) covers a column outside the mapping",
        "loans: FK ('servicer_id',) -> borrowers('borrower_id',) covers a column outside the mapping"]
    assert stats["foreign_keys_out_of_scope"] == [
        "loans: target FK ('servicer_id',) -> loan_servicing.servicers"]


def _customer_facts(schema: str, fk_to: str | None = None) -> SchemaFacts:
    facts = SchemaFacts(table=f"{schema}.customer", primary_key=("id",), not_null={"id"})
    if fk_to:
        facts.foreign_keys.add((("parent_id",), fk_to, ("id",)))
    return facts


def _two_schema_sides(source_fk: str, target_fk: str, target_placed: bool = True):
    """`billing.customer` and `crm.customer` are both mapped (to customer_billing / customer_crm);
    `crm.customer.parent_id` references `source_fk`, its target references `target_fk`."""
    rows = [{"id": 1, "parent_id": 1}]
    spec = MappingSpec("m1", [
        ObjectMapping(object="customer_billing", root_table="billing.customer", key_source=["id"],
                      key_target=["id"], fields=[]),
        ObjectMapping(object="customer_crm", root_table="crm.customer", key_source=["id"],
                      key_target=["id"], fields=[FieldMapping("parent_id", "parent_id", "int", "int")]),
    ])
    tschema = "lakebase" if target_placed else ""
    source = FakeSource({"billing.customer": rows, "crm.customer": rows},
                        schema={"billing.customer": _customer_facts("billing"),
                                "crm.customer": _customer_facts("crm", source_fk)})
    target = FakeTarget({"customer_billing": rows, "customer_crm": rows},
                        schema={"customer_billing": _facts(_customer_facts(tschema),
                                                           table=f"{tschema}.customer_billing" if tschema else ""),
                                "customer_crm": _facts(_customer_facts(tschema, target_fk),
                                                       table=f"{tschema}.customer_crm" if tschema else "")})
    return spec, source, target


@pytest.mark.parametrize("source_fk, target_fk, codes", [
    # the same-named table in the other mapped schema is a different parent
    ("billing.customer", "lakebase.customer_billing", []),
    ("crm.customer", "lakebase.customer_crm", []),
    ("billing.customer", "lakebase.customer_crm", ["foreign_key_extra", "foreign_key_missing"]),
    ("crm.customer", "lakebase.customer_billing", ["foreign_key_extra", "foreign_key_missing"]),
    # bracketed catalog spelling is the same table
    ("[billing].[customer]", "lakebase.customer_billing", []),
])
def test_foreign_keys_resolve_on_the_qualified_source_table_not_its_bare_name(source_fk, target_fk, codes):
    spec, source, target = _two_schema_sides(source_fk, target_fk)
    result = run_recon("u1", "transactional", spec, Tolerances("t1"), [], source, target)
    assert _codes(result, "schema_parity") == codes, _tier(result, "schema_parity")["findings"]
    assert result["verdict"] == ("PASS" if not codes else "FAIL")


def test_a_bare_foreign_key_reference_shared_by_two_mapped_schemas_is_unverified_not_passed():
    spec, source, target = _two_schema_sides("customer", "lakebase.customer_billing")
    result = run_recon("u1", "transactional", spec, Tolerances("t1"), [], source, target)
    parity = _tier(result, "schema_parity")
    # neither graded as parity nor as a target-only FK: the run stays PASS but cannot merge
    assert _codes(result, "schema_parity") == []
    assert parity["stats"]["unverified"] == [(
        "customer_crm: FK ('parent_id',) -> customer could reference any of ['customer_billing', "
        "'customer_crm']; qualify the reference (root_table schema) or confirm its target "
        "counterpart by hand")]
    assert result["verdict"] == "PASS" and result["merge_eligible"] is False
    assert any(w.startswith("UNVERIFIED schema_parity") for w in result["warnings"])


@pytest.mark.parametrize("placed, codes", [
    # the target catalog places customer_billing in `lakebase`; a same-named table in another
    # schema is not the mapped object
    (True, ["foreign_key_missing"]),
    # a fake that reports no catalog identity falls back to the bare object name
    (False, []),
])
def test_a_target_foreign_key_into_another_schema_is_not_the_mapped_object(placed, codes):
    spec, source, target = _two_schema_sides("billing.customer", "archive.customer_billing", target_placed=placed)
    result = run_recon("u1", "transactional", spec, Tolerances("t1"), [], source, target)
    parity = _tier(result, "schema_parity")
    assert _codes(result, "schema_parity") == codes, parity["findings"]
    if placed:
        assert parity["stats"]["foreign_keys_out_of_scope"] == [
            "customer_crm: target FK ('parent_id',) -> archive.customer_billing"]


def _sqlserver_cased(f: SchemaFacts) -> SchemaFacts:
    """Catalog identifiers as a case-insensitive SQL Server returns them: the DDL's casing."""
    def up(x: str) -> str:
        return "_".join(p[:1].upper() + p[1:] for p in x.split("_")).replace("Id", "ID")
    return SchemaFacts(
        primary_key=tuple(up(x) for x in f.primary_key),
        unique={tuple(up(x) for x in u) for u in f.unique},
        foreign_keys={(tuple(up(x) for x in c), "dbo.Borrowers", tuple(up(x) for x in rc))
                      for c, _r, rc in f.foreign_keys},
        not_null={up(x) for x in f.not_null}, indexes={tuple(up(x) for x in i) for i in f.indexes},
        check_count=f.check_count, checks=set(f.checks),
        identity_columns={up(x) for x in f.identity_columns})


def _renamed_spec() -> MappingSpec:
    spec = _spec()
    loans = spec.objects[0]
    fields = [FieldMapping("current_balance", "balance_current", "money", "decimal(19,4)")
              if f.source == "current_balance" else f for f in loans.fields]
    return MappingSpec(spec.version, [dataclasses.replace(loans, fields=fields), *spec.objects[1:]])


def _renamed_rows(loans: list[dict]) -> list[dict]:
    return [{("balance_current" if k == "current_balance" else k): v for k, v in r.items()} for r in loans]


@pytest.mark.parametrize("renamed_not_null, codes", [({"balance_current"}, []), (set(), ["not_null_missing"])])
def test_catalog_casing_never_changes_parity_on_a_renamed_target_column(renamed_not_null, codes):
    cased = _sqlserver_cased(LOANS_FACTS)
    assert cased.primary_key == ("Loan_ID",) and "Current_Balance" in cased.not_null
    loans, borrowers = _rows(6)
    tgt = _tightened(not_null=(TARGET_LOANS_FACTS.not_null - {"current_balance"}) | renamed_not_null,
                     checks={c.replace("current_balance", "balance_current") for c in TARGET_LOANS_FACTS.checks})
    source, target = _sides(loans, _renamed_rows(loans), borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = cased
    source.schema["dbo.borrowers"] = _sqlserver_cased(BORROWER_FACTS)
    result = _run(source, target, spec=_renamed_spec())
    parity = _tier(result, "schema_parity")
    assert _codes(result, "schema_parity") == codes, parity["findings"]
    # the catalog's own spelling is what the evidence records
    assert parity["stats"]["loans"]["source"]["primary_key"] == ["Loan_ID"]
    if codes:
        assert "current_balance -> target balance_current is nullable" in parity["findings"][0]["detail"]


def test_primary_key_mismatch_is_reported():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers,
                            tgt_facts=_tightened(primary_key=("loan_number",)))
    result = _run(source, target)
    assert "primary_key_mismatch" in _codes(result, "schema_parity")


DESC_KEYS = [1000 - i for i in range(6)]


@pytest.mark.parametrize("keys, src_seq, tgt_seq, codes, needle, identity", [
    (None, None, 4, ["sequence_behind_source"], "4 <= source max",
     {"source_next": 7, "source_max": 6, "target_next": 4}),
    (None, None, None, ["sequence_missing"], "owns no sequence", None),
    # both count down from 1000: next 994 is below every source key, the safe state
    (DESC_KEYS, (994, -1), (994, -1), [], None,
     {"source_next": 994, "source_max": 1000, "target_next": 994, "source_min": 995, "increment": -1}),
    (DESC_KEYS, (994, -1), (997, -1), ["sequence_behind_source"], "997 >= source min loan_id=995", None),
    (None, (7, 1), (0, -1), ["sequence_direction_mismatch"], "opposite ends", None),
    (None, None, (4, 5), ["sequence_behind_source"], "collide", None),
    (None, (7, 10), (7, 1), ["sequence_increment_mismatch"], "steps by 10", None),
    # rows 7..999 were issued and deleted (or rolled back): the surviving max is 6 but the source
    # identity stands at 1000, so a target seeded from the surviving rows reissues 7..999
    (None, 1000, 7, ["sequence_behind_source"], "7 < source identity next 1000 (source max loan_id=6)",
     {"source_next": 1000, "source_max": 6, "target_next": 7}),
    (None, 1000, 1000, [], None, {"source_next": 1000, "source_max": 6, "target_next": 1000}),
    (None, 1000, 1500, [], None, None),
    # the same on a countdown identity: the source issued 994..100 and deleted them
    (DESC_KEYS, (99, -1), (994, -1), ["sequence_behind_source"],
     "994 > source identity next 99 on a descending identity (source min loan_id=995)", None),
    (DESC_KEYS, (99, -1), (99, -1), [], None, None),
    # a source that steps the other way has no comparable frontier; only the direction is graded
    (None, (-50, -1), (7, 1), ["sequence_direction_mismatch"], "opposite ends", None),
])
def test_identity_parity(keys, src_seq, tgt_seq, codes, needle, identity):
    loans, borrowers = _rows(6, keys=keys)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, src_seq=src_seq, tgt_seq=tgt_seq)
    if tgt_seq is None:
        target.sequences[("loans", "loan_id")] = None
    result = _run(source, target)
    assert result["verdict"] == ("PASS" if not codes else "FAIL"), result
    assert _codes(result, "schema_parity") == codes
    parity = _tier(result, "schema_parity")
    if needle:
        assert needle in parity["findings"][0]["detail"]
    if identity:
        assert parity["stats"]["loans"]["identity"] == identity


def test_unverifiable_schema_facts_warn_and_block_merge_eligibility():
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    del target.schema["loans"]
    result = _run(source, target)
    assert result["verdict"] == "PASS"
    assert result["merge_eligible"] is False
    assert any(w.startswith("UNVERIFIED schema_parity") for w in result["warnings"])



def test_expression_indexes_are_graded_not_dropped():
    src = dataclasses.replace(LOANS_FACTS, expression_unique={"lower(loan_number)"},
                              expression_indexes={"upper(loan_number)"})
    # target: same unique expression, plus a unique expression the source never had
    tgt = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique={"lower(loan_number)", "lower(name)"})
    loans, borrowers = _rows(12)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target)
    assert result["verdict"] == "FAIL"
    t7 = _tier(result, "schema_parity")
    assert _codes(result, "schema_parity") == ["expression_unique_extra"]
    assert "lower(name)" in t7["findings"][0]["detail"]
    assert t7["stats"]["expression_indexes_unverified"] == [
        ("loans: source index on (upper(loan_number)) has no target index on (upper(loan_number)); "
         "confirm the access path by hand")]
    assert t7["stats"]["loans"]["source"]["expression_unique"] == ["lower(loan_number)"]
    assert t7["stats"]["loans"]["target"]["expression_unique"] == ["lower(loan_number)", "lower(name)"]
    # the recorded decision for target-only constraints covers a target-only unique expression
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert result["verdict"] == "PASS", result
    # a source unique expression the target lacks is always a defect
    target.schema["loans"] = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique=set())
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert _codes(result, "schema_parity") == ["expression_unique_missing"]


def test_index_key_text_keeps_nested_calls_whole_and_drops_suffixes():
    assert _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (lower(email))") == "lower(email)"
    assert _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (lower(region)) WHERE active") == \
        "lower(region)"
    assert _index_key_text("CREATE INDEX i ON s.t USING btree (upper(region), id) INCLUDE (code)") == \
        "upper(region), id"
    assert _index_key_text("CREATE INDEX i ON s.t USING gin (to_tsvector('english'::regconfig, "
                           "COALESCE(body, ''::text))) WITH (fastupdate=off)") == \
        "to_tsvector('english'::regconfig, coalesce(body, ''::text))"


def test_index_key_text_keeps_literal_and_quoted_identifier_case():
    upper = _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (((status = 'A'::text)), tenant_id)")
    lower = _index_key_text("CREATE UNIQUE INDEX u ON s.t USING btree (((status = 'a'::text)), tenant_id)")
    assert upper == "((status = 'A'::text)), tenant_id" and upper != lower
    assert _index_key_text('CREATE INDEX i ON s.t USING btree (lower("Email"), UPPER("email"))') == \
        'lower("Email"), upper("email")'
    # parentheses and doubled quotes inside a literal never close the key list
    assert _index_key_text("CREATE INDEX i ON s.t USING btree (COALESCE(note, 'n/a (''X'')'::text)) WHERE x") == \
        "coalesce(note, 'n/a (''X'')'::text)"


def test_expression_unique_literal_case_is_a_parity_finding():
    loans, borrowers = _rows(12)
    src = dataclasses.replace(LOANS_FACTS, expression_unique={_index_key_text(
        "CREATE UNIQUE INDEX u ON dbo.loans USING btree (((status = 'A'::text)), loan_number)")})
    tgt = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique={_index_key_text(
        "CREATE UNIQUE INDEX u ON public.loans USING btree (((status = 'a'::text)), loan_number)")})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert result["verdict"] == "FAIL"
    assert _codes(result, "schema_parity") == ["expression_unique_missing"]
    assert "'A'" in _tier(result, "schema_parity")["findings"][0]["detail"]
    target.schema["loans"] = dataclasses.replace(TARGET_LOANS_FACTS, expression_unique=set(src.expression_unique))
    assert _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))["verdict"] == "PASS"


@pytest.mark.parametrize("expr_src, expr_tgt", [
    ("lower(loan_number)", "lower(loan_no)"),
    # a column name inside a string literal is not a column reference
    ("CASE WHEN loan_number = 'loan_number' THEN NULL ELSE lower(loan_number) END",
     "CASE WHEN loan_no = 'loan_number' THEN NULL ELSE lower(loan_no) END"),
])
def test_expression_index_columns_follow_the_field_mapping(expr_src, expr_tgt):
    spec = _spec()
    loans = dataclasses.replace(spec.objects[0], fields=[
        FieldMapping("loan_number", "loan_no", "varchar", "string"),
        FieldMapping("current_balance", "current_balance", "money", "decimal(19,4)"),
        FieldMapping("borrower_id", "borrower_id", "int", "int")])
    spec = MappingSpec("m1", [loans, spec.objects[1]])
    src = _facts(LOANS_FACTS, unique={("loan_number",)}, expression_unique={expr_src})
    tgt = _tightened(unique={("loan_no",)}, expression_unique={expr_tgt},
                     not_null={"loan_id", "loan_no", "current_balance", "modified_date", "borrower_id"})
    rows, borrowers = _rows(12)
    tgt_rows = [{**{k: v for k, v in r.items() if k != "loan_number"}, "loan_no": r["loan_number"]} for r in rows]
    source, target = _sides(rows, tgt_rows, borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target, spec=spec)
    assert _codes(result, "schema_parity") == [], _tier(result, "schema_parity")["findings"]


def test_expression_mapping_rewrites_column_references_only():
    colmap = {"status": "loan_status", "email": "email_addr", "text": "body", "lower": "lc"}
    # the mapped name inside a string literal, a cast type and a function name stays put
    assert _map_expression("CASE WHEN status = 'status' THEN 'email' ELSE email END", colmap) == \
        "CASE WHEN loan_status = 'status' THEN 'email' ELSE email_addr END"
    assert _map_expression("lower(email::text)", colmap) == "lower(email_addr::text)"
    assert _map_expression("lower((email)::character varying)", colmap) == \
        "lower((email_addr)::character varying)"
    assert _map_expression("COALESCE(status, 'it''s status')", colmap) == \
        "COALESCE(loan_status, 'it''s status')"
    # quoted identifiers are column references too, and keep their quoting
    assert _map_expression('lower("Email"), "text"', colmap) == 'lower("email_addr"), "body"'
    # unchanged when nothing is mapped
    assert _map_expression("to_tsvector('english'::regconfig, coalesce(body, ''::text))", {}) == \
        "to_tsvector('english'::regconfig, coalesce(body, ''::text))"


SRC_FK = ((("borrower_id",), "dbo.borrowers", ("borrower_id",)))
TGT_FK = ((("borrower_id",), "loan_servicing.borrowers", ("borrower_id",)))


@pytest.mark.parametrize("src_act, tgt_act, codes", [
    (("no action", "cascade"), ("no action", "no action"), ["foreign_key_action_mismatch"]),
    (("no action", "cascade"), ("no action", "cascade"), []),
    (("no action", "cascade"), None, []),  # a catalog that does not report actions is not graded
])
def test_foreign_key_referential_actions_are_compared(src_act, tgt_act, codes):
    loans, borrowers = _rows(6)
    src = _facts(LOANS_FACTS, foreign_key_actions={SRC_FK: src_act})
    tgt = _facts(TARGET_LOANS_FACTS, foreign_key_actions={TGT_FK: tgt_act} if tgt_act else {})
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=tgt)
    source.schema["dbo.loans"] = src
    result = _run(source, target)
    assert _codes(result, "schema_parity") == codes
    if codes:
        (finding,) = _tier(result, "schema_parity")["findings"]
        assert "source acts (update, delete) = ('no action', 'cascade')" in finding["detail"]
        assert _tier(result, "schema_parity")["stats"]["loans"]["source"]["foreign_keys"] == [
            [["borrower_id"], "dbo.borrowers", ["borrower_id"], "no action", "cascade"]]


def test_referential_actions_normalise_across_catalogs():
    assert [_fk_action(x) for x in ("a", "r", "c", "n", "d")] == \
        ["no action", "no action", "cascade", "set null", "set default"]
    assert [_fk_action(x) for x in ("NO_ACTION", "CASCADE", "SET_NULL", "SET_DEFAULT")] == \
        ["no action", "cascade", "set null", "set default"]


@pytest.mark.parametrize("src, tgt, codes, note", [
    # a composite unique is a column set: another declaration order is the same constraint
    ({"unique": {("loan_number",), ("borrower_id", "loan_number")}},
     {"unique": {("loan_number",), ("loan_number", "borrower_id")}}, [], "unique_reordered"),
    ({"unique": {("loan_number",), ("borrower_id", "loan_number")}},
     {"unique": {("loan_number",), ("borrower_id", "current_balance")}}, ["unique_extra", "unique_missing"], None),
    # an index is an access path and keeps its order: (loan_number, borrower_id) does not serve
    # a lookup that leads with borrower_id
    ({"indexes": {("borrower_id", "loan_number")}}, {"indexes": {("loan_number", "borrower_id")}},
     ["index_missing"], None),
])
def test_composite_unique_is_unordered_but_a_composite_index_is_not(src, tgt, codes, note):
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers, tgt_facts=_tightened(**tgt))
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, **src)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == codes
    if note:
        (line,) = _tier(result, "schema_parity")["stats"][note]
        assert "('borrower_id', 'loan_number')" in line and "('loan_number', 'borrower_id')" in line


@pytest.mark.parametrize("src, tgt, codes, accepted", [
    # SQL Server keeps one NULL loan_number; a default Postgres unique keeps any number of them
    ({"unique_nulls_equal": {("loan_number",)}}, {}, ["unique_nulls_equal_missing"], []),
    # the reverse tightens the target: a second NULL the legacy app writes today is rejected
    ({}, {"unique_nulls_equal": {("loan_number",)}}, ["unique_nulls_equal_extra"], ["unique_nulls_equal_extra"]),
    ({"unique_nulls_equal": {("loan_number",)}}, {"unique_nulls_equal": {("loan_number",)}}, [], []),
    ({}, {}, [], []),
])
def test_nullable_unique_keys_must_agree_on_how_nulls_compare(src, tgt, codes, accepted):
    loans, borrowers = _rows(6)
    nullable = LOANS_FACTS.not_null - {"loan_number"}
    source, target = _sides(loans, [dict(r) for r in loans], borrowers,
                            tgt_facts=_tightened(not_null=nullable, **tgt))
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, not_null=nullable, **src)
    result = _run(source, target)
    assert _codes(result, "schema_parity") == codes
    if codes:
        (f,) = _tier(result, "schema_parity")["findings"]
        assert "('loan_number',)" in f["detail"] and {f["source_value"], f["target_value"]} == {repr("nulls equal"), repr("nulls distinct")}
    result = _run(source, target, tol=Tolerances("t1", accept_target_only_constraints=True))
    assert _codes(result, "schema_parity") == [c for c in codes if c not in accepted]
    assert result["verdict"] == "PASS" or codes != accepted


def test_null_semantics_are_not_graded_while_every_key_column_is_not_null():
    # loan_number is NOT NULL on both sides: no NULL key can ever exist, so the engines' NULL
    # handling cannot disagree on a real row
    loans, borrowers = _rows(6)
    source, target = _sides(loans, [dict(r) for r in loans], borrowers)
    source.schema["dbo.loans"] = _facts(LOANS_FACTS, unique_nulls_equal={("loan_number",)})
    result = _run(source, target)
    assert _codes(result, "schema_parity") == []
    assert _tier(result, "schema_parity")["stats"]["loans"]["source"]["unique_nulls_equal"] == [["loan_number"]]

