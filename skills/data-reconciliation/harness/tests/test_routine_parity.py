"""Behavioural parity for writing routines: every converted routine whose dependency row writes gets
one committed run on a dedicated target branch against a fixture snapshot, its written rows compared
to a golden set. A routine without such a run is `unproven`, never silently clean; a run whose rows
differ is `failed` and blocks merge (`routine_gap`)."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from recon import cli
from recon.config import ConfigError
from recon.config import FieldMapping, MappingSpec, ObjectMapping
from recon.engine import run_recon
from recon.routines import (check_parity, git_committed, grade_routines, load_runs, routine_gap,
                            writers)

from recon.rerun import shape_digest, source_digest
from tests.fakes import PROVEN_RERUN
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

# close_period writes ledger_balance itself and run_log through the routine it calls
GOLDEN = {"app.ledger_balance": [{"period_id": 1, "balance": "10.00"},
                                 {"period_id": 2, "balance": "-3.50"}],
          "app.run_log": [{"run_id": 7, "routine": "close_period"}]}
EVIDENCE = "pr-42/recon/ledger/close_period.run.json"
SNAPSHOT = "fixture:fixtures/ledger-2024q1"
COMMITTED = {EVIDENCE, SNAPSHOT[len("fixture:"):], "fixtures/ledger/2024q1.v2"}.__contains__


def _run(routine="app_pkg.close_period", family="lakebase", branch="mig-ledger-exec",
         observed=None, evidence=EVIDENCE, record=EVIDENCE, **extra):
    return {"routine": routine, "target_family": family, "target_branch": branch,
            "snapshot": SNAPSHOT, "golden": GOLDEN,
            "observed": GOLDEN if observed is None else observed, "evidence": evidence,
            "record": record, **extra}


def grade_routines(deps, runs, committed=COMMITTED):
    from recon import routines
    return routines.grade_routines(deps, runs, committed)


def test_a_matching_run_on_a_dedicated_branch_proves_the_routine():
    out = grade_routines(DEPS, [_run()])
    assert out["routine_parity"] == [
        {"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE},
        {"routine": "app_pkg.write_run_log", "status": "unproven",
         "evidence": None, "reason": "no committed run"}]
    assert out["unproven"] == ["app_pkg.write_run_log"] and out["failed"] == []


# ---- the writer list --------------------------------------------------------------------------

def test_writers_include_what_the_callees_write_transitively():
    """A routine that calls a writer writes: its run must show every table the call graph below
    it touches, not only the ones its own body names."""
    deps = {"routines": DEPS["routines"] + [
        {"routine": "app_pkg.month_end", "reads": [], "writes": [], "calls": ["APP_PKG.Close_Period"]},
        {"routine": "app_pkg.loop_a", "writes": ["app.a"], "calls": ["app_pkg.loop_b"]},
        {"routine": "app_pkg.loop_b", "writes": [], "calls": ["app_pkg.loop_a", "app_pkg.period_status"]}]}
    assert writers(deps) == {
        "app_pkg.close_period": ["app.ledger_balance", "app.run_log"],
        "app_pkg.write_run_log": ["app.run_log"],
        "app_pkg.month_end": ["app.ledger_balance", "app.run_log"],
        "app_pkg.loop_a": ["app.a"],
        "app_pkg.loop_b": ["app.a"]}
    out = grade_routines(DEPS, [_run(observed={"app.ledger_balance": GOLDEN["app.ledger_balance"]})])
    assert out["routine_parity"][0]["status"] == "failed"
    assert out["routine_parity"][0]["findings"] == [
        {"table": "app.run_log", "check": "table_unobserved",
         "detail": "written by the routine but not in the observed set"}]


@pytest.mark.parametrize("rows, match", [
    ([{"routine": "a", "writes": "app.t", "calls": []}], "a: writes must be a list"),
    ([{"routine": "a", "writes": ["app.t", 3], "calls": []}], "a: writes must be a list of table names"),
    ([{"routine": "a", "writes": ["app.t"], "calls": []}, {"routine": "A", "writes": [], "calls": []}],
     "routine a appears twice"),
    ([{"routine": "a", "calls": []}], "a: writes must be present"),
    ([{"routine": "a", "writes": []}], "a: calls must be present"),
    ([{"routine": "a", "writes": ["app.t"], "calls": "b"}], "a: calls must be a list"),
    ([{"routine": "a", "writes": ["app.t"], "calls": ["b"]}], "a calls b, which has no row"),
    ([{"writes": ["app.t"]}], "every row is"),
    ([{"routine": "", "writes": ["app.t"]}], "every row is"),
    (["app_pkg.x"], "every row is"),
    ({"routine": "a"}, "routines: \\["),
])
def test_a_malformed_dependency_analysis_is_refused_not_guessed_at(rows, match):
    """A string `writes` would iterate as characters, a duplicate row would silently win, an
    unknown callee would hide its writes, an omitted `writes` or `calls` would read as a routine that
    writes and calls nothing: each is an error the analysis has to fix."""
    deps = rows if isinstance(rows, dict) else {"routines": rows}
    with pytest.raises(ConfigError, match=match):
        writers(deps)
    with pytest.raises(ConfigError, match=match):
        grade_routines(deps, [])
    with pytest.raises(ConfigError, match=match):
        check_parity([], "x", dependencies=deps)


# ---- committed artifacts ----------------------------------------------------------------------

def test_proven_needs_the_evidence_and_the_fixture_snapshot_committed():
    """A non-empty evidence string proves nothing: the run log and the fixture snapshot the run
    used must both be paths in the committed tree, otherwise the routine stays `unproven`."""
    row = grade_routines(DEPS, [_run()], committed=lambda p: p != EVIDENCE)["routine_parity"][0]
    assert row["status"] == "unproven" and row["evidence"] == EVIDENCE
    assert row["reason"] == f"evidence {EVIDENCE} is not a committed file"
    row = grade_routines(DEPS, [_run()], committed=lambda p: p == EVIDENCE)["routine_parity"][0]
    assert row["status"] == "unproven"
    assert row["reason"] == "fixture snapshot fixtures/ledger-2024q1 is not a committed file"
    row = grade_routines(DEPS, [_run(observed={})], committed=lambda p: False)["routine_parity"][0]
    assert row["status"] == "unproven"  # provenance before rows: an uncommitted run is not a failed one
    assert grade_routines(DEPS, [_run()])["routine_parity"][0]["status"] == "proven"


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _committed_repo(path, *files):
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q")
    _git(path, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "--allow-empty", "-m", "init")
    for f in files:
        (path / f).parent.mkdir(parents=True, exist_ok=True)
        (path / f).write_text("{}\n")
        _git(path, "add", f)
    if files:
        _git(path, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "artifacts")
    return path


def test_git_committed_is_true_only_for_a_file_present_on_disk_and_in_head(tmp_path):
    repo = _committed_repo(tmp_path / "r", "recon/a.run.json", "fixtures/snap")
    committed = git_committed(repo)
    assert committed("recon/a.run.json") and committed("fixtures/snap")
    (repo / "recon" / "b.run.json").write_text("{}")
    assert not committed("recon/b.run.json")  # untracked
    _git(repo, "add", "recon/b.run.json")
    assert not committed("recon/b.run.json")  # staged, not in HEAD
    (repo / "recon" / "a.run.json").write_text('{"edited": true}\n')
    assert not committed("recon/a.run.json")  # in HEAD, edited on disk
    _git(repo, "add", "recon/a.run.json")
    assert not committed("recon/a.run.json")  # the edit staged, HEAD still differs
    (repo / "recon" / "a.run.json").write_text("{}\n")
    assert not committed("recon/a.run.json")  # disk back to HEAD, but the index still holds the edit
    _git(repo, "add", "recon/a.run.json")
    assert committed("recon/a.run.json")  # index and disk both back to the committed blob
    (repo / "fixtures" / "snap").unlink()
    assert not committed("fixtures/snap")  # in HEAD, gone from disk
    assert not committed("../outside") and not committed("/etc/hostname") and not committed("")
    assert not git_committed(tmp_path / "not-a-repo")("recon/a.run.json")


def test_git_committed_refuses_symlinks_even_when_the_link_itself_is_committed(tmp_path):
    """A committed symlink is a committed *pointer*: what it reads is whatever the link target holds
    now, so neither a linked file nor a file under a linked directory is committed evidence."""
    repo = _committed_repo(tmp_path / "r", "recon/a.run.json")
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / "b.run.json").write_text("{}\n")
    (repo / "recon" / "link.run.json").symlink_to(tmp_path / "elsewhere" / "b.run.json")
    (repo / "linked_dir").symlink_to(tmp_path / "elsewhere")
    (repo / "recon" / "inner.run.json").symlink_to("a.run.json")
    _git(repo, "add", "recon/link.run.json", "linked_dir", "recon/inner.run.json")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "links")
    committed = git_committed(repo)
    assert committed("recon/a.run.json")
    assert not committed("recon/link.run.json")  # symlink to a file outside the tree
    assert not committed("recon/inner.run.json")  # symlink to a committed file: still a link
    assert not committed("linked_dir/b.run.json")  # regular file reached through a linked directory


def test_rows_must_be_objects_so_malformed_evidence_cannot_compare_equal(tmp_path):
    """golden and observed tables are lists of row objects; a scalar or a list on both sides would
    serialize and compare equal, so anything but an object row is refused, never graded."""
    for bad in ([1], ["a"], [[1, 2]], [None], [{"period_id": 1}, 2]):
        with pytest.raises(ConfigError, match="lists of row objects"):
            grade_routines(DEPS, [_run(golden={**GOLDEN, "app.run_log": bad}, observed={**GOLDEN, "app.run_log": bad})])
    with pytest.raises(ConfigError, match="lists of row objects"):
        grade_routines(DEPS, [_run(observed={**GOLDEN, "app.run_log": {"run_id": 7}})])
    assert grade_routines(DEPS, [_run()])["failed"] == []


def test_a_run_record_is_graded_only_as_the_evidence_file_it_names():
    """`--runs` content proves nothing by pointing at some other committed path: the record is
    graded only when it is the committed evidence file itself (`record`, set by load_runs, equals
    `evidence`), so a fabricated run cannot borrow another run's evidence."""
    row = grade_routines(DEPS, [_run(record="scratch/close_period.run.json")])["routine_parity"][0]
    assert row["status"] == "unproven" and row["evidence"] == EVIDENCE
    assert row["reason"] == f"run record scratch/close_period.run.json is not its evidence file {EVIDENCE}"
    run = _run()
    del run["record"]
    row = grade_routines(DEPS, [run])["routine_parity"][0]
    assert row["status"] == "unproven" and row["reason"] == "run record location unknown (load it with load_runs)"
    row = grade_routines(DEPS, [_run(record="scratch/x.run.json", observed={})])["routine_parity"][0]
    assert row["status"] == "unproven"  # location before rows, like the committed-file checks


def test_read_only_routines_are_out_of_scope():
    out = grade_routines(DEPS, [_run("app_pkg.period_status")])
    assert [r["routine"] for r in out["routine_parity"]] == ["app_pkg.close_period",
                                                             "app_pkg.write_run_log"]


def test_rows_compare_as_sets_after_canonical_json_order():
    shuffled = {"app.ledger_balance": [{"balance": "-3.50", "period_id": 2},
                                       {"balance": "10.00", "period_id": 1}],
               "app.run_log": GOLDEN["app.run_log"]}
    assert grade_routines(DEPS, [_run(observed=shuffled)])["routine_parity"][0]["status"] == "proven"


def test_differing_rows_fail_and_name_the_table_and_counts():
    observed = {"app.ledger_balance": [{"period_id": 1, "balance": "10.00"},
                                       {"period_id": 2, "balance": "-3.5"}],
               "app.run_log": GOLDEN["app.run_log"]}
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
         "detail": "written by the routine but not in the observed set"},
        {"table": "app.run_log", "check": "table_unobserved",
         "detail": "written by the routine but not in the observed set"}]
    run = _run()
    run["golden"] = {}
    out = grade_routines(DEPS, [run])
    assert [f["check"] for f in out["routine_parity"][0]["findings"]] == ["no_golden", "no_golden"]


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
    for ok in (SNAPSHOT, "fixture:fixtures/ledger/2024q1.v2"):
        run = _run()
        run["snapshot"] = ok
        assert grade_routines(DEPS, [run])["routine_parity"][0]["status"] == "proven", ok


def test_table_names_in_golden_and_observed_compare_case_insensitively():
    upper = {"APP.Ledger_Balance": GOLDEN["app.ledger_balance"], "app.run_log": GOLDEN["app.run_log"]}
    out = grade_routines(DEPS, [_run(golden=upper, observed=upper)])
    assert out["routine_parity"][0]["status"] == "proven"
    out = grade_routines(DEPS, [_run(golden=upper)])
    assert out["routine_parity"][0]["status"] == "proven"
    both = {**GOLDEN, "APP.LEDGER_BALANCE": []}
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


def test_check_parity_against_the_dependency_analysis_materializes_missing_writers_as_unproven(tmp_path):
    """A parity file that omits a writing routine is not clean: the routine is `unproven`, and a
    row for a routine the analysis lacks (another unit's file) is refused."""
    repo = _committed_repo(tmp_path / "r")
    _committed_run(repo)
    proven = [{"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE}]
    out = check_parity(proven, "x", dependencies=DEPS, committed=git_committed(repo), repo=repo)
    assert out == proven + [{"routine": "app_pkg.write_run_log", "status": "unproven", "evidence": None,
                             "reason": "no row in x"}]
    assert check_parity([], "x", dependencies=DEPS)[0]["status"] == "unproven"
    assert check_parity([], "x", dependencies={"routines": []}) == []
    with pytest.raises(ConfigError, match="other_pkg.nobody.*not in the dependency analysis"):
        check_parity(proven + [{"routine": "other_pkg.nobody", "status": "proven", "evidence": EVIDENCE}],
                     "x", dependencies=DEPS, committed=git_committed(repo), repo=repo)
    with pytest.raises(ConfigError, match="routines"):
        check_parity(proven, "x", dependencies={})


def test_check_parity_downgrades_a_claim_whose_evidence_is_not_committed(tmp_path):
    """The parity file is a claim; `run` re-reads the run it names from the committed tree, so an
    absent, uncommitted or edited file cannot carry `proven` (or a made-up `failed`) into result.json."""
    repo = _committed_repo(tmp_path / "r")
    proven = [{"routine": "app_pkg.close_period", "status": "proven", "evidence": "scratch/run.json"}]
    out = check_parity(proven, "x", dependencies=DEPS, committed=git_committed(repo), repo=repo)
    assert out[0] == {"routine": "app_pkg.close_period", "status": "unproven", "evidence": "scratch/run.json",
                      "reason": "x: cannot read evidence scratch/run.json: not a committed file"}
    failed = [{"routine": "app_pkg.close_period", "status": "failed", "evidence": "scratch/run.json",
               "findings": []}]
    assert check_parity(failed, "x", dependencies=DEPS, committed=git_committed(repo), repo=repo)[0] == out[0]
    with pytest.raises(ConfigError, match="committed"):
        check_parity(proven, "x")


def test_check_parity_refuses_a_routine_listed_twice():
    """Two rows for one routine (case aside) would let a `proven` row shadow a `failed` one."""
    rows = [{"routine": "app_pkg.close_period", "status": "failed", "evidence": EVIDENCE, "findings": []},
            {"routine": "APP_PKG.Close_Period", "status": "proven", "evidence": EVIDENCE}]
    with pytest.raises(ConfigError, match="x: app_pkg.close_period appears twice"):
        check_parity(rows, "x", dependencies=DEPS, committed=COMMITTED)
    with pytest.raises(ConfigError, match="x: app_pkg.close_period appears twice"):
        check_parity(rows, "x", committed=COMMITTED)


def test_check_parity_validates_a_written_result(tmp_path):
    repo = _committed_repo(tmp_path / "r")
    _committed_run(repo)
    good = [{"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE}]
    assert check_parity(good, "x", DEPS, git_committed(repo), repo)[0] == good[0]
    for bad in ({}, [{"routine": "r", "status": "clean", "evidence": "e"}],
                [{"routine": "r", "status": "proven", "evidence": None}], [{"status": "proven"}]):
        with pytest.raises(ConfigError):
            check_parity(bad, "x", DEPS, git_committed(repo), repo)
    with pytest.raises(ConfigError, match="dependency analysis"):
        check_parity(good, "x", committed=git_committed(repo), repo=repo)


def test_load_runs_accepts_a_file_or_a_directory(tmp_path):
    runs = tmp_path / "recon" / "runs"
    runs.mkdir(parents=True)
    (runs / "a.run.json").write_text(json.dumps(_run(record="lies")))
    (runs / "b.run.json").write_text(json.dumps(_run("app_pkg.write_run_log")))
    (runs / "notes.txt").write_text("ignored")
    loaded = load_runs(runs, tmp_path)
    assert [r["routine"] for r in loaded] == ["app_pkg.close_period", "app_pkg.write_run_log"]
    # `record` is where the file sits in the repo, never what the file says about itself
    assert [r["record"] for r in loaded] == ["recon/runs/a.run.json", "recon/runs/b.run.json"]
    assert load_runs(runs / "a.run.json", tmp_path)[0]["record"] == "recon/runs/a.run.json"
    with pytest.raises(ConfigError, match="a.run.json: is outside"):
        load_runs(runs / "a.run.json", tmp_path / "elsewhere")
    (runs / "c.run.json").write_text("{")
    with pytest.raises(ConfigError, match="c.run.json"):
        load_runs(runs, tmp_path)


def test_load_runs_refuses_a_symlinked_record_before_reading_it(tmp_path):
    """A link inside the repository can point anywhere; what it points at is nobody's committed
    evidence, so the record is refused by its link path and the target is never read."""
    repo, elsewhere = tmp_path / "repo", tmp_path / "elsewhere"
    (repo / "recon" / "runs").mkdir(parents=True)
    elsewhere.mkdir()
    (elsewhere / "x.run.json").write_text(json.dumps(_run()))
    (repo / "recon" / "runs" / "a.run.json").symlink_to(elsewhere / "x.run.json")
    with pytest.raises(ConfigError, match="recon/runs/a.run.json: is a symlink"):
        load_runs(repo / "recon" / "runs" / "a.run.json", repo)
    with pytest.raises(ConfigError, match="recon/runs/a.run.json: is a symlink"):
        load_runs(repo / "recon" / "runs", repo)
    (repo / "runs").symlink_to(repo / "recon" / "runs", target_is_directory=True)
    (repo / "recon" / "runs" / "a.run.json").unlink()
    (repo / "recon" / "runs" / "b.run.json").write_text(json.dumps(_run()))
    with pytest.raises(ConfigError, match="runs/b.run.json: is a symlink"):
        load_runs(repo / "runs" / "b.run.json", repo)
    assert load_runs(repo / "recon" / "runs" / "b.run.json", repo)[0]["record"] == "recon/runs/b.run.json"
    # `link/../x` normalizes to a repo path on paper while the filesystem follows the link out
    (repo / "link").symlink_to(elsewhere, target_is_directory=True)
    (elsewhere / "recon").mkdir()
    (elsewhere / "recon" / "b.run.json").write_text(json.dumps(_run("app_pkg.write_run_log")))
    with pytest.raises(ConfigError, match=r"link/\.\./recon/runs/b.run.json: has a \.\. component"):
        load_runs(repo / "link" / ".." / "recon" / "runs" / "b.run.json", repo)
    with pytest.raises(ConfigError, match=r"has a \.\. component"):
        load_runs(repo / "link" / ".." / "recon" / "runs", repo)


# ---- result.json / merge -------------------------------------------------------------------

def test_result_carries_routine_parity_and_a_failed_run_blocks_merge(tmp_path):
    source, target = make_green()
    failed = [{"routine": "r", "status": "failed", "evidence": "e",
               "findings": [{"table": "t", "check": "rows_differ", "detail": "d"}]}]
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target, out_dir=tmp_path,
                       routine_parity=failed, rerun_proof=PROVEN_RERUN)
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
                       routine_parity=parity, rerun_proof=PROVEN_RERUN)
    assert result["merge_eligible"] is True and result["merge_block_reasons"] == []
    assert "unproven" in (tmp_path / "recon.summary.md").read_text()


def test_result_without_parity_omits_it():
    source, target = make_green()
    result = run_recon("orders", "live", SPEC, TOL, RULES, source, target)
    assert result["routine_parity"] is None and "routine_gap" not in result["merge_block_reasons"]


def test_a_unit_with_writing_routines_and_no_complete_parity_list_is_not_merge_eligible(tmp_path):
    """Absent parity is not clean parity at the result level either: given the unit's writers,
    `build_result` blocks with `routine_parity_missing` unless every writer has a row; `unproven`
    rows in a complete list stay eligible (they are cutover exceptions, not merge blocks)."""
    source, target = make_green()
    writers_ = ["app_pkg.close_period", "app_pkg.write_run_log"]
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target, out_dir=tmp_path,
                       routine_writers=writers_, rerun_proof=PROVEN_RERUN)
    assert result["verdict"] == "PASS" and result["merge_eligible"] is False
    assert result["merge_block_reasons"] == ["routine_parity_missing"]
    assert "routine_parity_missing" in (tmp_path / "recon.summary.md").read_text()
    partial = [{"routine": "APP_PKG.close_period", "status": "proven", "evidence": EVIDENCE}]
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target,
                       routine_parity=partial, routine_writers=writers_, rerun_proof=PROVEN_RERUN)
    assert result["merge_eligible"] is False and result["merge_block_reasons"] == ["routine_parity_missing"]
    complete = partial + [{"routine": "app_pkg.write_run_log", "status": "unproven", "evidence": None,
                           "reason": "no committed run"}]
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target,
                       routine_parity=complete, routine_writers=writers_, rerun_proof=PROVEN_RERUN)
    assert result["merge_eligible"] is True and result["merge_block_reasons"] == []
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target, routine_writers=[],
                       rerun_proof=PROVEN_RERUN, routine_dependencies=".migration/units/orders/dependencies.json")
    assert result["merge_eligible"] is True  # the analysis says nothing writes
    assert result["routine_dependencies"] == ".migration/units/orders/dependencies.json"


def test_a_unit_with_no_dependency_analysis_at_all_is_not_merge_eligible(tmp_path):
    """Not knowing whether the unit has writers is not the same as knowing it has none: with no
    dependency analysis `build_result` blocks with `routine_parity_missing` and says what to commit."""
    source, target = make_green()
    result = run_recon("orders", "live", FLAT, TOL, RULES, source, target, out_dir=tmp_path,
                       routine_analysis_missing=True, rerun_proof=PROVEN_RERUN)
    assert result["verdict"] == "PASS" and result["merge_eligible"] is False
    assert result["merge_block_reasons"] == ["routine_parity_missing"]
    text = (tmp_path / "recon.summary.md").read_text()
    assert "routine_parity_missing" in text and "dependencies.json" in text


def _committed_run(repo, evidence=EVIDENCE, **overrides):
    """A run record committed at the path its evidence names, plus its fixture snapshot."""
    run = {k: v for k, v in _run(evidence=evidence, **overrides).items() if k != "record"}
    for f, text in ((evidence, json.dumps(run) + "\n"), (SNAPSHOT[len("fixture:"):], "-- snapshot\n")):
        (repo / f).parent.mkdir(parents=True, exist_ok=True)
        (repo / f).write_text(text)
        _git(repo, "add", f)
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "run")
    return run


def test_check_parity_regrades_every_claim_from_its_committed_run_record(tmp_path):
    """A `proven` or `failed` row is a claim about a committed run; `check_parity` re-reads that run
    and grades it again with `_grade_run`, so a parity file cannot relabel a failed run as proven,
    and the row carried is the recomputed one (its findings too)."""
    repo = _committed_repo(tmp_path / "r")
    _committed_run(repo, observed={**GOLDEN, "app.run_log": []})  # the committed run fails
    claim = [{"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE},
             {"routine": "app_pkg.write_run_log", "status": "unproven", "evidence": None, "reason": "none"}]
    with pytest.raises(ConfigError, match="close_period.*claims proven.*grades failed"):
        check_parity(claim, "p", DEPS, git_committed(repo), repo)
    claim[0] = {**claim[0], "status": "failed",
                "findings": [{"table": "app.ledger_balance", "check": "rows_differ", "detail": "made up"}]}
    rows = check_parity(claim, "p", DEPS, git_committed(repo), repo)
    assert rows[0]["status"] == "failed"
    assert [(f["table"], f["check"]) for f in rows[0]["findings"]] == [("app.run_log", "rows_differ")]
    _committed_run(repo)  # the run now matches its golden set
    claim[0] = {"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE}
    rows = check_parity(claim, "p", DEPS, git_committed(repo), repo)
    assert rows[0] == {"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE}
    (repo / EVIDENCE).write_text(json.dumps({**_run(), "observed": {}}))  # edited after commit
    rows = check_parity(claim, "p", DEPS, git_committed(repo), repo)
    assert rows[0]["status"] == "unproven" and "not a committed file" in rows[0]["reason"]
    claim[0] = {"routine": "app_pkg.close_period", "status": "proven", "evidence": "nowhere/x.run.json"}
    rows = check_parity(claim, "p", DEPS, git_committed(repo), repo)
    assert rows[0]["status"] == "unproven" and "cannot read" in rows[0]["reason"]
    # an `unproven` row that names evidence is graded from it too: a failed committed run cannot
    # hide behind the softer label
    _committed_run(repo, observed={**GOLDEN, "app.run_log": []})
    claim[0] = {"routine": "app_pkg.close_period", "status": "unproven", "evidence": EVIDENCE, "reason": "tbd"}
    rows = check_parity(claim, "p", DEPS, git_committed(repo), repo)
    assert rows[0]["status"] == "failed" and rows[0]["findings"][0]["table"] == "app.run_log"
    assert routine_gap(rows)
    with pytest.raises(ConfigError, match="repository root"):
        check_parity(claim, "p", DEPS, git_committed(repo))


def _cli_run(tmp_path, monkeypatch, *extra):
    from recon import adapters
    monkeypatch.chdir(tmp_path)
    if not (tmp_path / ".git").exists():
        _committed_repo(tmp_path, EVIDENCE)
    (tmp_path / ".migration").mkdir(exist_ok=True)
    (tmp_path / ".migration" / "allowed_targets.json").write_text(json.dumps({"catalogs": ["mig"]}))
    source, target = make_green()
    monkeypatch.setitem(adapters.SOURCE_ADAPTERS, "oracle", lambda secret: source)
    monkeypatch.setattr(adapters, "DatabricksTargetAdapter", lambda *a: target)
    monkeypatch.setattr(cli, "load_mapping_spec", lambda path, params: FLAT)
    monkeypatch.setattr(cli, "load_tolerances", lambda path: TOL)
    monkeypatch.setattr(cli, "load_canon_rules", lambda path: RULES)
    job = tmp_path / "job.sql"
    job.write_text("CREATE TABLE IF NOT EXISTS mig.s.orders (id INT)\n")
    shape = {"tables": {"orders": [{"name": "id", "type": "int", "nullable": True}]}}
    proof = tmp_path / "rerun_proof.json"
    proof.write_text(json.dumps({**PROVEN_RERUN, "unit": "u", "notes": [], "prior_digest": "p",
                                 "evidence": {"fresh": "j/1", "evolved": "j/2"}, "source_digest": source_digest([job]),
                                 "shape": shape, "shape_digest": shape_digest(shape)}))
    rc = cli.main(["run", "--unit", "u", "--family", "oracle", "--mode", "live", "--mapping", "m", "--tolerances", "t",
                   "--canonicalization", "c", "--source-dsn-secret", "SOURCE", "--target-secret", "TARGET",
                   "--target-catalog", "mig", "--target-schema", "s", "--out", str(tmp_path / "out"),
                   "--rerun-proof", str(proof), "--rerun-source", str(job), *extra])
    return rc, json.loads((tmp_path / "out" / "result.json").read_text())


def _commit_deps(repo, deps=DEPS, path="analysis/dependencies.json"):
    """The unit's dependency analysis committed at a repository path; returns that path."""
    if not (repo / ".git").exists():
        _committed_repo(repo, EVIDENCE)
    (repo / path).parent.mkdir(parents=True, exist_ok=True)
    (repo / path).write_text(json.dumps(deps) + "\n")
    _git(repo, "add", path)
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "deps")
    return path


def test_cli_run_grades_the_parity_file_against_the_unit_dependency_analysis(tmp_path, monkeypatch):
    """`run --routine-parity` needs the unit's dependency analysis: a parity file missing a writer,
    or no parity file at all, lists that writer as `unproven` instead of leaving the unit clean."""
    deps = _commit_deps(tmp_path)
    parity = tmp_path / "routine_parity.json"
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE}]}))
    with pytest.raises(SystemExit, match="--routine-dependencies"):
        _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity))
    with pytest.raises(SystemExit, match="--routine-dependencies analysis/missing.json is not a file"):
        _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity),
                 "--routine-dependencies", "analysis/missing.json")
    _committed_run(tmp_path)
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity),
                          "--routine-dependencies", deps)
    assert rc == 0 and result["merge_eligible"] is True
    assert [(r["routine"], r["status"]) for r in result["routine_parity"]] == [
        ("app_pkg.close_period", "proven"), ("app_pkg.write_run_log", "unproven")]
    assert result["routine_dependencies"] == deps
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": "other_pkg.x", "status": "proven", "evidence": EVIDENCE}]}))
    with pytest.raises(SystemExit, match="other_pkg.x.*not in the dependency analysis"):
        _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity), "--routine-dependencies", deps)


def test_cli_run_takes_the_dependency_analysis_only_as_a_committed_file_of_the_repository(tmp_path, monkeypatch):
    """The analysis decides who the writers are, so it must be the committed one: a path outside the
    repository, an untracked file, or an edited copy of the committed file is refused (never read
    as an empty writer set), and the result names the committed file it used."""
    deps = _commit_deps(tmp_path)
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-dependencies", deps)
    assert result["routine_dependencies"] == deps and result["routine_writers"] == [
        "app_pkg.close_period", "app_pkg.write_run_log"]
    outside = tmp_path.parent / f"{tmp_path.name}-outside.json"
    outside.write_text(json.dumps({"routines": []}))
    with pytest.raises(SystemExit, match="not a committed file of the repository"):
        _cli_run(tmp_path, monkeypatch, "--routine-dependencies", str(outside))
    (tmp_path / "scratch.json").write_text(json.dumps({"routines": []}))
    with pytest.raises(SystemExit, match="scratch.json.*not a committed file of the repository"):
        _cli_run(tmp_path, monkeypatch, "--routine-dependencies", "scratch.json")
    (tmp_path / deps).write_text(json.dumps({"routines": []}))
    with pytest.raises(SystemExit, match="not a committed file of the repository"):
        _cli_run(tmp_path, monkeypatch, "--routine-dependencies", deps)
    (tmp_path / deps).write_text(json.dumps(DEPS) + "\n")
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-dependencies", str(tmp_path / deps))
    assert result["routine_dependencies"] == deps  # an absolute path inside the repository is recorded relative
    unit = _commit_deps(tmp_path, {"routines": []}, ".migration/units/u/dependencies.json")
    (tmp_path / unit).write_text(json.dumps(DEPS))
    with pytest.raises(SystemExit, match="not a committed file of the repository"):
        _cli_run(tmp_path, monkeypatch)  # the default analysis is held to the same rule


def test_cli_run_with_no_parity_file_blocks_and_names_every_writer_from_the_analysis(tmp_path, monkeypatch):
    """Absent parity is not clean parity: with only the dependency analysis, every routine that
    writes (directly or through a callee) is a writer without a row, so the unit blocks with
    `routine_parity_missing` and the summary names them; `unproven` rows stay eligible only when a
    parity file carries them."""
    deps = _commit_deps(tmp_path, {"routines": DEPS["routines"] + [
        {"routine": "app_pkg.month_end", "writes": [], "calls": ["app_pkg.close_period"]}]})
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-dependencies", deps)
    assert result["merge_eligible"] is False and result["merge_block_reasons"] == ["routine_parity_missing"]
    assert result["routine_parity"] is None
    text = (tmp_path / "out" / "recon.summary.md").read_text()
    assert "routine_parity_missing" in text
    assert all(r in text for r in ("app_pkg.close_period", "app_pkg.write_run_log", "app_pkg.month_end"))
    parity = tmp_path / "routine_parity.json"
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": r, "status": "unproven", "evidence": None, "reason": "no committed run"}
        for r in ("app_pkg.close_period", "app_pkg.write_run_log", "app_pkg.month_end")]}))
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-dependencies", deps, "--routine-parity", str(parity))
    assert rc == 0 and result["merge_eligible"] is True


def test_cli_run_loads_the_units_committed_dependency_analysis_by_default(tmp_path, monkeypatch):
    """Omitting --routine-dependencies never leaves the gate off: `run` reads
    `.migration/units/<unit>/dependencies.json`; with writers and no parity the unit blocks with
    `routine_parity_missing`; only an analysis with zero writers lets the unit through; no analysis
    at all blocks too, naming the file to commit."""
    rc, result = _cli_run(tmp_path, monkeypatch)
    assert result["merge_eligible"] is False and result["merge_block_reasons"] == ["routine_parity_missing"]
    assert result["routine_dependencies"] is None
    assert ".migration/units/u/dependencies.json" in (tmp_path / "out" / "recon.summary.md").read_text()
    unit = _commit_deps(tmp_path, DEPS, ".migration/units/u/dependencies.json")
    rc, result = _cli_run(tmp_path, monkeypatch)
    assert result["merge_eligible"] is False and result["merge_block_reasons"] == ["routine_parity_missing"]
    assert result["routine_parity"] is None and result["routine_writers"] == ["app_pkg.close_period", "app_pkg.write_run_log"]
    assert result["routine_dependencies"] == unit
    _commit_deps(tmp_path, {"routines": [DEPS["routines"][2]]}, unit)
    rc, result = _cli_run(tmp_path, monkeypatch)
    assert rc == 0 and result["merge_eligible"] is True and result["routine_parity"] == []


def test_cli_run_refuses_a_parity_row_whose_committed_run_grades_differently(tmp_path, monkeypatch):
    deps = _commit_deps(tmp_path)
    _committed_run(tmp_path, observed={**GOLDEN, "app.run_log": []})
    parity = tmp_path / "routine_parity.json"
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": "app_pkg.close_period", "status": "proven", "evidence": EVIDENCE}]}))
    with pytest.raises(SystemExit, match="claims proven.*grades failed"):
        _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity), "--routine-dependencies", deps)


def test_cli_run_downgrades_proven_rows_whose_evidence_is_not_in_the_committed_tree(tmp_path, monkeypatch):
    deps = _commit_deps(tmp_path)
    parity = tmp_path / "routine_parity.json"
    parity.write_text(json.dumps({"routine_parity": [
        {"routine": "app_pkg.close_period", "status": "proven", "evidence": "scratch/close_period.run.json"}]}))
    rc, result = _cli_run(tmp_path, monkeypatch, "--routine-parity", str(parity),
                          "--routine-dependencies", deps)
    assert result["routine_parity"][0]["status"] == "unproven"
    assert "not a committed file" in result["routine_parity"][0]["reason"]


# ---- fixture -----------------------------------------------------------------------------

# the example is laid out like a unit's repository: each run record sits at the path its
# `evidence` names, next to the fixture snapshot it ran against
FIXTURE_RUNS = "recon/ledger"
FIXTURE_ARTIFACTS = (f"{FIXTURE_RUNS}/close_period.run.json", f"{FIXTURE_RUNS}/archive_entries.run.json",
                     "fixtures/ledger-2024q1.sql")


def test_example_fixture_has_one_of_each_status():
    deps = json.loads((FIXTURE / "dependencies.json").read_text())
    out = grade_routines(deps, load_runs(FIXTURE / FIXTURE_RUNS, FIXTURE),
                         committed=set(FIXTURE_ARTIFACTS).__contains__)
    assert out == json.loads((FIXTURE / "expected.json").read_text())
    assert sorted(r["status"] for r in out["routine_parity"]) == ["failed", "proven", "unproven"]


def _fixture_repo(path):
    """The example committed as a repository: the artifacts are the fixture's own files."""
    repo = _committed_repo(path)
    for f in FIXTURE_ARTIFACTS:
        (repo / f).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE / f, repo / f)
        _git(repo, "add", f)
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "artifacts")
    return repo


# ---- CLI ---------------------------------------------------------------------------------

def test_cli_routine_parity_writes_the_result_and_exits_non_zero_on_failed(tmp_path, capsys):
    repo = _fixture_repo(tmp_path / "repo")
    rc = cli.main(["routine-parity", "--dependencies", str(FIXTURE / "dependencies.json"),
                   "--runs", str(repo / FIXTURE_RUNS), "--out", str(tmp_path), "--repo", str(repo)])
    assert rc == 1
    out = json.loads((tmp_path / "routine_parity.json").read_text())
    assert out == json.loads((FIXTURE / "expected.json").read_text())
    assert "1 failed" in capsys.readouterr().out
    # the same records read from outside the repository cannot be its committed evidence
    with pytest.raises(SystemExit, match="is outside"):
        cli.main(["routine-parity", "--dependencies", str(FIXTURE / "dependencies.json"),
                  "--runs", str(FIXTURE / FIXTURE_RUNS), "--out", str(tmp_path / "o2"), "--repo", str(repo)])
    # and read from another place inside it, they are not the evidence they name
    shutil.copytree(repo / FIXTURE_RUNS, repo / "scratch")
    rc = cli.main(["routine-parity", "--dependencies", str(FIXTURE / "dependencies.json"),
                   "--runs", str(repo / "scratch"), "--out", str(tmp_path / "o2"), "--repo", str(repo)])
    assert rc == 2
    out = json.loads((tmp_path / "o2" / "routine_parity.json").read_text())
    assert [r["status"] for r in out["routine_parity"]] == ["unproven"] * 3
    assert all("is not its evidence file" in r["reason"] for r in out["routine_parity"][:2])


def test_cli_routine_parity_checks_artifacts_against_the_repo_it_runs_in(tmp_path, monkeypatch, capsys):
    """Without --repo the current directory is the repository; artifacts the fixture names but the
    tree does not hold leave every run `unproven`."""
    repo = _committed_repo(tmp_path / "empty")
    monkeypatch.chdir(repo)
    for f in FIXTURE_ARTIFACTS[:2]:  # the records are in place, but nothing is committed
        (repo / f).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE / f, repo / f)
    rc = cli.main(["routine-parity", "--dependencies", str(FIXTURE / "dependencies.json"),
                   "--runs", FIXTURE_RUNS, "--out", str(tmp_path / "o")])
    assert rc == 2
    out = json.loads((tmp_path / "o" / "routine_parity.json").read_text())
    assert [r["status"] for r in out["routine_parity"]] == ["unproven"] * 3
    assert all("not a committed file" in r["reason"] for r in out["routine_parity"][:2])


def test_cli_routine_parity_is_clean_only_when_every_writer_is_proven(tmp_path):
    repo = _committed_repo(tmp_path / "repo", SNAPSHOT[len("fixture:"):])
    (tmp_path / "deps.json").write_text(json.dumps(DEPS))
    runs = repo / "pr-42" / "recon" / "ledger"
    runs.mkdir(parents=True)

    def commit(name, run):
        (runs / name).write_text(json.dumps(run))
        _git(repo, "add", f"pr-42/recon/ledger/{name}")
        _git(repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", name)

    commit("close_period.run.json", _run())
    args = ["routine-parity", "--dependencies", str(tmp_path / "deps.json"),
            "--runs", str(runs), "--out", str(tmp_path / "o"), "--repo", str(repo)]
    assert cli.main(args) == 2  # unproven: not a failure, not clean
    commit("write_run_log.run.json", _run(
        "app_pkg.write_run_log", observed={"app.run_log": []}, golden={"app.run_log": []},
        evidence="pr-42/recon/ledger/write_run_log.run.json"))
    assert cli.main(args) == 0
    (runs / "write_run_log.run.json").write_text(json.dumps(_run(
        "app_pkg.write_run_log", observed={"app.run_log": [{"run_id": 1}]}, golden={"app.run_log": []},
        evidence="pr-42/recon/ledger/write_run_log.run.json")))
    assert cli.main(args) == 2  # edited after the commit: not the committed run any more
