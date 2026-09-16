"""Behavioural parity for writing routines: every converted routine whose dependency row writes gets
one committed run on a dedicated target branch against a fixture snapshot, its written rows compared
to a golden set. A routine without such a run is `unproven`, never silently clean; a run whose rows
differ is `failed` and blocks merge (`routine_gap`)."""

import json
from pathlib import Path

import pytest
from recon import cli
from recon.config import ConfigError
from recon.config import FieldMapping, MappingSpec, ObjectMapping
from recon.engine import run_recon
from recon.routines import check_parity, grade_routines, load_runs, routine_gap

from tests.test_tiers import RULES, SPEC, TOL, make_green

# a flat mapping: the embedded `items` in the shared green fixture are an ungraded-embed warning
# that would block merge on its own and hide what routine parity does
FLAT = MappingSpec(version="m", objects=[ObjectMapping(
    object="orders", root_table="ORDERS", key_source=["ORDER_ID"], key_target="order_id",
    fields=SPEC.objects[0].fields)])

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "example_routine_parity"

DEPS = {"routines": [
    {"routine": "app_pkg.close_period", "reads": ["app.period"], "writes": ["app.ledger_balance"],
     "calls": ["app_pkg.write_run_log"]},
    {"routine": "app_pkg.write_run_log", "reads": [], "writes": ["app.run_log"], "calls": []},
    {"routine": "app_pkg.period_status", "reads": ["app.period"], "writes": [], "calls": []}]}

GOLDEN = {"app.ledger_balance": [{"period_id": 1, "balance": "10.00"},
                                 {"period_id": 2, "balance": "-3.50"}]}


def _run(routine="app_pkg.close_period", family="lakebase", branch="mig-ledger-exec",
         observed=None, evidence="pr-42/recon/ledger/close_period.run.json", **extra):
    return {"routine": routine, "target_family": family, "target_branch": branch,
            "snapshot": "fixture:ledger-2024q1", "golden": GOLDEN,
            "observed": GOLDEN if observed is None else observed, "evidence": evidence, **extra}


def test_a_matching_run_on_a_dedicated_branch_proves_the_routine():
    out = grade_routines(DEPS, [_run()])
    assert out["routine_parity"] == [
        {"routine": "app_pkg.close_period", "status": "proven",
         "evidence": "pr-42/recon/ledger/close_period.run.json"},
        {"routine": "app_pkg.write_run_log", "status": "unproven",
         "evidence": None, "reason": "no committed run"}]
    assert out["unproven"] == ["app_pkg.write_run_log"] and out["failed"] == []


def test_read_only_routines_are_out_of_scope():
    out = grade_routines(DEPS, [_run("app_pkg.period_status")])
    assert [r["routine"] for r in out["routine_parity"]] == ["app_pkg.close_period",
                                                             "app_pkg.write_run_log"]


def test_rows_compare_as_sets_after_canonical_json_order():
    shuffled = {"app.ledger_balance": [{"balance": "-3.50", "period_id": 2},
                                       {"balance": "10.00", "period_id": 1}]}
    assert grade_routines(DEPS, [_run(observed=shuffled)])["routine_parity"][0]["status"] == "proven"


def test_differing_rows_fail_and_name_the_table_and_counts():
    observed = {"app.ledger_balance": [{"period_id": 1, "balance": "10.00"},
                                       {"period_id": 2, "balance": "-3.5"}]}
    out = grade_routines(DEPS, [_run(observed=observed)])
    row = out["routine_parity"][0]
    assert row["status"] == "failed" and out["failed"] == ["app_pkg.close_period"]
    assert row["findings"] == [{"table": "app.ledger_balance", "check": "rows_differ",
                                "detail": "golden 2 rows, observed 2 rows; 1 only in golden, "
                                          "1 only in observed"}]


def test_a_written_table_absent_from_the_golden_or_observed_set_fails():
    out = grade_routines(DEPS, [_run(observed={})])
    assert out["routine_parity"][0]["findings"] == [
        {"table": "app.ledger_balance", "check": "table_unobserved",
         "detail": "written by the routine but not in the observed set"}]
    run = _run()
    run["golden"] = {}
    out = grade_routines(DEPS, [run])
    assert out["routine_parity"][0]["findings"][0]["check"] == "no_golden"


def test_a_run_outside_a_dedicated_exec_branch_is_unproven_not_proven():
    for family, branch in (("lakebase", "main"), ("lakebase", "mig-ledger"),
                           ("databricks", "prod.ledger"), ("databricks", "mig.ledger")):
        row = grade_routines(DEPS, [_run(family=family, branch=branch)])["routine_parity"][0]
        assert row["status"] == "unproven", (family, branch)
        assert "dedicated" in row["reason"]
    ok = grade_routines(DEPS, [_run(family="databricks", branch="mig.ledger_exec")])
    assert ok["routine_parity"][0]["status"] == "proven"


def test_an_unknown_target_family_is_unproven_with_the_reason():
    row = grade_routines(DEPS, [_run(family="mainframe", branch="x")])["routine_parity"][0]
    assert row["status"] == "unproven" and "mainframe" in row["reason"]


def test_a_run_without_evidence_or_snapshot_is_unproven():
    row = grade_routines(DEPS, [_run(evidence="")])["routine_parity"][0]
    assert row["status"] == "unproven" and "evidence" in row["reason"]
    run = _run()
    del run["snapshot"]
    row = grade_routines(DEPS, [run])["routine_parity"][0]
    assert row["status"] == "unproven" and "snapshot" in row["reason"]


def test_a_snapshot_that_is_not_a_fixture_leaves_the_routine_unproven():
    """Only a run against a committed fixture snapshot proves a routine; a production or ad hoc
    snapshot is not evidence, however non-empty the string is."""
    for bad in ("production-2026-09-16", "prod", "fixture:", "fixture: ", "Fixture:ledger", "/tmp/snap", 7):
        run = _run()
        run["snapshot"] = bad
        row = grade_routines(DEPS, [run])["routine_parity"][0]
        assert row["status"] == "unproven" and "fixture:" in row["reason"], bad
    for ok in ("fixture:ledger-2024q1", "fixture:ledger/2024q1.v2"):
        run = _run()
        run["snapshot"] = ok
        assert grade_routines(DEPS, [run])["routine_parity"][0]["status"] == "proven", ok


def test_table_names_in_golden_and_observed_compare_case_insensitively():
    upper = {"APP.Ledger_Balance": GOLDEN["app.ledger_balance"]}
    out = grade_routines(DEPS, [_run(golden=upper, observed=upper)])
    assert out["routine_parity"][0]["status"] == "proven"
    out = grade_routines(DEPS, [_run(golden=upper)])
    assert out["routine_parity"][0]["status"] == "proven"
    both = {"app.ledger_balance": GOLDEN["app.ledger_balance"], "APP.LEDGER_BALANCE": []}
    with pytest.raises(ConfigError, match="app.ledger_balance.*twice"):
        grade_routines(DEPS, [_run(observed=both)])


def test_a_run_for_a_routine_the_analysis_lacks_is_a_config_error():
    with pytest.raises(ConfigError, match="not in the dependency analysis"):
        grade_routines(DEPS, [_run("app_pkg.nobody")])
    with pytest.raises(ConfigError, match="twice"):
        grade_routines(DEPS, [_run(), _run()])


def test_routine_gap_is_only_a_failed_run():
    assert routine_gap(None) is False
    assert routine_gap([{"routine": "r", "status": "unproven", "evidence": None}]) is False
    assert routine_gap([{"routine": "r", "status": "failed", "evidence": "e"}]) is True


def test_check_parity_against_the_dependency_analysis_materializes_missing_writers_as_unproven():
    """A parity file that omits a writing routine is not clean: the routine is `unproven`, and a
    row for a routine the analysis lacks (another unit's file) is refused."""
    proven = [{"routine": "app_pkg.close_period", "status": "proven", "evidence": "e"}]
    out = check_parity(proven, "x", dependencies=DEPS)
    assert out == proven + [{"routine": "app_pkg.write_run_log", "status": "unproven", "evidence": None,
                             "reason": "no row in x"}]
    assert check_parity([], "x", dependencies=DEPS)[0]["status"] == "unproven"
    assert check_parity([], "x", dependencies={"routines": []}) == []
    with pytest.raises(ConfigError, match="other_pkg.nobody.*not in the dependency analysis"):
        check_parity(proven + [{"routine": "other_pkg.nobody", "status": "proven", "evidence": "e"}],
                     "x", dependencies=DEPS)
    with pytest.raises(ConfigError, match="routines"):
        check_parity(proven, "x", dependencies={})


def test_check_parity_validates_a_written_result():
    good = [{"routine": "r", "status": "proven", "evidence": "e"}]
    assert check_parity(good, "x") == good
    for bad in ({}, [{"routine": "r", "status": "clean", "evidence": "e"}],
                [{"routine": "r", "status": "proven", "evidence": None}], [{"status": "proven"}]):
        with pytest.raises(ConfigError):
            check_parity(bad, "x")


def test_load_runs_accepts_a_file_or_a_directory(tmp_path):
    (tmp_path / "a.run.json").write_text(json.dumps(_run()))
    (tmp_path / "b.run.json").write_text(json.dumps(_run("app_pkg.write_run_log")))
    (tmp_path / "notes.txt").write_text("ignored")
    assert [r["routine"] for r in load_runs(tmp_path)] == ["app_pkg.close_period",
                                                           "app_pkg.write_run_log"]
    assert load_runs(tmp_path / "a.run.json")[0]["routine"] == "app_pkg.close_period"
    (tmp_path / "c.run.json").write_text("{")
    with pytest.raises(ConfigError, match="c.run.json"):
        load_runs(tmp_path)


# ---- result.json / merge -------------------------------------------------------------------

def test_result_carries_routine_parity_and_a_failed_run_blocks_merge(tmp_path):
    source, target = make_green()
    failed = [{"routine": "r", "status": "failed", "evidence": "e",
               "findings": [{"table": "t", "check": "rows_differ", "detail": "d"}]}]
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target, out_dir=tmp_path,
                       routine_parity=failed)
    assert result["verdict"] == "PASS" and result["merge_eligible"] is False
    assert result["merge_block_reasons"] == ["routine_gap"]
    assert result["routine_parity"] == failed
    written = json.loads((tmp_path / "result.json").read_text())
    assert written["routine_parity"] == failed
    text = (tmp_path / "recon.summary.md").read_text()
    assert "Routine parity" in text and "rows_differ" in text


def test_unproven_routines_do_not_block_merge_but_are_listed(tmp_path):
    source, target = make_green()
    parity = [{"routine": "r", "status": "unproven", "evidence": None, "reason": "no committed run"}]
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target, out_dir=tmp_path,
                       routine_parity=parity)
    assert result["merge_eligible"] is True and result["merge_block_reasons"] == []
    assert "unproven" in (tmp_path / "recon.summary.md").read_text()


def test_result_without_parity_omits_it():
    source, target = make_green()
    result = run_recon("orders", "live", SPEC, TOL, RULES, source, target)
    assert result["routine_parity"] is None and "routine_gap" not in result["merge_block_reasons"]


def _cli_run(tmp_path, monkeypatch, *extra):
    from recon import adapters
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".migration").mkdir(exist_ok=True)
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig"]}))
    source, target = make_green()
    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "oracle", lambda secret: source)
    monkeypatch.setattr(adapters, "DatabricksTargetAdapter", lambda *a: target)
    monkeypatch.setattr(cli, "load_mapping_spec", lambda path, params: FLAT)
    monkeypatch.setattr(cli, "load_tolerances", lambda path: TOL)
    monkeypatch.setattr(cli, "load_canon_rules", lambda path: RULES)
    rc = cli.main(["run", "--unit", "u", "--family", "oracle", "--mode", "live", "--mapping", "m", "--tolerances", "t",
                   "--canonicalization", "c", "--source-dsn-secret", "SOURCE", "--target-secret", "TARGET",
                   "--target-catalog", "mig", "--target-schema", "s", "--out", str(tmp_path / "out"), *extra])
    return rc, json.loads((tmp_path / "out" / "result.json").read_text())


def test_cli_run_grades_the_parity_file_against_the_unit_dependency_analysis(tmp_path, monkeypatch):
    """`run --routine-parity` needs the unit's dependency analysis: a parity file missing a writer,
    or no parity file at all, lists that writer as `unproven` instead of leaving the unit clean."""
    deps = tmp_path / "dependencies.json"
    deps.write_text(json.dumps(DEPS))
    parity = tmp_path / "routine_parity.json"
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": "app_pkg.close_period", "status": "proven", "evidence": "e"}]}))
    with pytest.raises(SystemExit, match="--routine-dependencies"):
        _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity))
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity),
                          "--routine-dependencies", str(deps))
    assert rc == 0 and result["merge_eligible"] is True
    assert [(r["routine"], r["status"]) for r in result["routine_parity"]] == [
        ("app_pkg.close_period", "proven"), ("app_pkg.write_run_log", "unproven")]
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-dependencies", str(deps))
    assert [r["status"] for r in result["routine_parity"]] == ["unproven", "unproven"]
    assert "unproven" in (tmp_path / "out" / "recon.summary.md").read_text()
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": "other_pkg.x", "status": "proven", "evidence": "e"}]}))
    with pytest.raises(SystemExit, match="other_pkg.x.*not in the dependency analysis"):
        _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity), "--routine-dependencies", str(deps))


# ---- fixture -----------------------------------------------------------------------------

def test_example_fixture_has_one_of_each_status():
    deps = json.loads((FIXTURE / "dependencies.json").read_text())
    out = grade_routines(deps, load_runs(FIXTURE / "runs"))
    assert out == json.loads((FIXTURE / "expected.json").read_text())
    assert sorted(r["status"] for r in out["routine_parity"]) == ["failed", "proven", "unproven"]


# ---- CLI ---------------------------------------------------------------------------------

def test_cli_routine_parity_writes_the_result_and_exits_non_zero_on_failed(tmp_path, capsys):
    rc = cli.main(["routine-parity", "--dependencies", str(FIXTURE / "dependencies.json"),
                   "--runs", str(FIXTURE / "runs"), "--out", str(tmp_path)])
    assert rc == 1
    out = json.loads((tmp_path / "routine_parity.json").read_text())
    assert out == json.loads((FIXTURE / "expected.json").read_text())
    assert "1 failed" in capsys.readouterr().out


def test_cli_routine_parity_is_clean_only_when_every_writer_is_proven(tmp_path):
    (tmp_path / "deps.json").write_text(json.dumps(DEPS))
    (tmp_path / "runs").mkdir()
    (tmp_path / "runs" / "a.run.json").write_text(json.dumps(_run()))
    rc = cli.main(["routine-parity", "--dependencies", str(tmp_path / "deps.json"),
                   "--runs", str(tmp_path / "runs"), "--out", str(tmp_path / "o")])
    assert rc == 2  # unproven: not a failure, not clean
    (tmp_path / "runs" / "b.run.json").write_text(json.dumps(_run(
        "app_pkg.write_run_log", observed={"app.run_log": []},
        golden={"app.run_log": []})))
    rc = cli.main(["routine-parity", "--dependencies", str(tmp_path / "deps.json"),
                   "--runs", str(tmp_path / "runs"), "--out", str(tmp_path / "o")])
    assert rc == 0
