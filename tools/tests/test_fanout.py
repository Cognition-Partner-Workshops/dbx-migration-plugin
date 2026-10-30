"""Tests for tools/fanout.py (design-0.7 section 7); children faked."""
import json

import pytest

from tools import fanout


def mk_slice(sid, **kw):
    s = {"id": sid, "name": sid, "track": "delta", "tables": [], "routines": [],
         "jobs": [], "consumers": [], "depends_on": [], "routing": "none"}
    s.update(kw)
    return s


def mk_doc(*slices):
    return {"version": 1, "slices": list(slices)}


def mk_policy(**kw):
    p = {"version": 2, "repo": "github.com/acme/estate", "base_branch": "mig/run1",
         "targets": {"catalog": "mig_cat"}, "width": 4, "breaker": 3}
    p.update(kw)
    return p


def migrate_pass(sid, pr=None):
    return {"status": "PASS", "slice": sid, "pr_url": pr or f"https://x/{sid}",
            "branch": f"b-{sid}", "parity": "PASS", "findings": [],
            "changed_paths": [], "recon_cost": 1, "one_line_summary": "ok"}


def verify_pass(sid, head):
    return {"status": "PASS", "slice": sid, "parity": "PASS", "findings": [],
            "blocking": False, "verified_head": head, "one_line_summary": "ok"}


class FakeOps:
    def __init__(self, reports=(), states=None, diffs=None):
        self.reports = list(reports)
        self.states = dict(states or {})
        self.diffs = dict(diffs or {})
        self.launches = []
        self.merges = []

    def launch(self, brief):
        self.launches.append(brief)
        assert self.reports, f"unexpected launch: {brief}"
        return self.reports.pop(0)

    def pr_state(self, pr_url):
        return dict(self.states.get(pr_url, {"state": "open", "head": "h0"}))

    def merge(self, pr_url, sha):
        self.merges.append((pr_url, sha))
        self.states[pr_url] = {"state": "merged", "head": sha}

    def changed_paths(self, pr_url):
        return list(self.diffs.get(pr_url, []))


def seed_record(tmp_path, sid, **fields):
    rec = {"slice": sid, "status": "new"}
    rec.update(fields)
    path = tmp_path / "recon" / sid / "fanout.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rec))


def test_dependency_ready_launch_order(tmp_path):
    doc = mk_doc(mk_slice("slice-1"),
                 mk_slice("slice-2", depends_on=["slice-1"]),
                 mk_slice("slice-3", depends_on=["slice-2"]))
    # slice-1 merges in the same pass; slice-2's migrate then fails, so slice-3
    # must stay waiting and its migrate child is never launched.
    ops = FakeOps([
        migrate_pass("slice-1"), verify_pass("slice-1", "h1"),
        migrate_pass("slice-2"), verify_pass("slice-2", "h2"),
        {"status": "FAIL", "slice": "slice-3", "failure_class": "migrate",
         "one_line_summary": "nope"},
    ])
    ops.states["https://x/slice-1"] = {"state": "open", "head": "h1"}
    ops.states["https://x/slice-2"] = {"state": "open", "head": "h2"}
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert [b.split()[1] for b in ops.launches] == ["slice-1", "slice-1",
                                                  "slice-2", "slice-2",
                                                  "slice-3"]
    assert summary["slices"]["slice-1"]["status"] == "merged"
    assert summary["slices"]["slice-2"]["status"] == "merged"
    assert summary["slices"]["slice-3"]["status"] == "failed"


def test_dependent_waits_until_merged(tmp_path):
    doc = mk_doc(mk_slice("slice-1"), mk_slice("slice-2", depends_on=["slice-1"]))
    ops = FakeOps([{"status": "launched"}])  # migrate child still in flight
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 1
    assert summary["slices"]["slice-1"]["status"] == "launched"
    assert summary["slices"]["slice-2"]["status"] == "waiting"


def test_merges_only_at_verified_head(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h2"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert ops.merges == []
    assert summary["slices"]["slice-1"]["status"] == "verify_pending"


def test_merge_uses_verified_head(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert ops.merges == [("https://x/slice-1", "h1")]
    assert summary["slices"]["slice-1"]["status"] == "merged"


@pytest.mark.parametrize("bad", [".migration/policy.json",
                                 ".migration/slices.json",
                                 ".migration/recon/slice-1/inputs/mapping.json",
                                 ".migration/recon/slice-1/fanout.json"])
def test_protected_paths_fail_child(tmp_path, bad):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  diffs={"https://x/slice-1": ["src/ok.py", bad]})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert ops.merges == []
    assert summary["slices"]["slice-1"]["failure_class"] == "protected_files_tampered"


def test_allowed_paths_merge(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}},
                  diffs={"https://x/slice-1": ["src/a.py",
                                               ".migration/recon/slice-1/out/result.json",
                                               ".migration/authorized/slice-1/fix.sql"]})
    assert fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))["slices"]["slice-1"]["status"] == "merged"


def test_breaker_stops_launching(tmp_path):
    doc = mk_doc(mk_slice("slice-1"), mk_slice("slice-2"), mk_slice("slice-3"))
    ops = FakeOps([
        {"status": "FAIL", "slice": "s", "failure_class": "migrate"},
        {"status": "FAIL", "slice": "s", "failure_class": "migrate"},
    ])
    summary = fanout.run(ops, mk_policy(breaker=2), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 2
    assert summary["halted"]
    assert summary["slices"]["slice-3"]["status"] == "halted"


def test_width_caps_launches(tmp_path):
    doc = mk_doc(mk_slice("slice-1"), mk_slice("slice-2"))
    ops = FakeOps([{"status": "launched"}])
    summary = fanout.run(ops, mk_policy(width=1), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 1
    assert summary["slices"]["slice-2"]["status"] == "waiting"


def test_canary_awaits_human(tmp_path):
    doc = mk_doc(mk_slice("slice-1", canary=True, auto_merge=False))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert ops.merges == []
    assert summary["slices"]["slice-1"]["status"] == "awaiting_human"


def test_rerun_skips_merged_and_progresses(tmp_path):
    seed_record(tmp_path, "slice-1", status="merged", pr_url="https://x/slice-1")
    seed_record(tmp_path, "slice-2", status="verify_pending",
                pr_url="https://x/slice-2")
    doc = mk_doc(mk_slice("slice-1"),
                 mk_slice("slice-2", depends_on=["slice-1"]))
    ops = FakeOps([verify_pass("slice-2", "h2")],
                  states={"https://x/slice-2": {"state": "open", "head": "h2"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 1  # the verify child only
    assert ops.merges == [("https://x/slice-2", "h2")]
    assert summary["slices"]["slice-1"]["status"] == "merged"


def test_rerun_detects_external_merge(tmp_path):
    seed_record(tmp_path, "slice-1", status="awaiting_human",
                pr_url="https://x/slice-1")
    doc = mk_doc(mk_slice("slice-1"),
                 mk_slice("slice-2", depends_on=["slice-1"]))
    ops = FakeOps([migrate_pass("slice-2"), verify_pass("slice-2", "h2")],
                  states={"https://x/slice-1": {"state": "merged", "head": "h1"},
                          "https://x/slice-2": {"state": "open", "head": "h2"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-1"]["status"] == "merged"
    assert summary["slices"]["slice-2"]["status"] == "merged"


def test_closed_pr_relaunches_migrate(tmp_path):
    seed_record(tmp_path, "slice-1", status="verify_pending",
                pr_url="https://x/old")
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1", "https://x/new"),
                   verify_pass("slice-1", "h1")],
                  states={"https://x/old": {"state": "closed", "head": "h0"},
                          "https://x/new": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-1"]["status"] == "merged"
    assert ops.merges == [("https://x/new", "h1")]


def test_collision_refusal(tmp_path):
    seed_record(tmp_path, "slice-1", status="verify_pending",
                pr_url="https://x/slice-1")
    doc = mk_doc(mk_slice("slice-1", tables=["A.T1"]),
                 mk_slice("slice-2", tables=["A.T1"]))
    # verify report for slice-1 is never consumed: collision is checked per
    # slice in order, slice-2 collides with the still-running slice-1.
    ops = FakeOps([{"status": "launched"}],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-2"]["failure_class"] == "collision"


def test_collision_cleared_by_disposition(tmp_path):
    seed_record(tmp_path, "slice-1", status="verify_pending",
                pr_url="https://x/slice-1")
    doc = mk_doc(mk_slice("slice-1", tables=["A.T1"],
                          shared_writes={"A.T1": "dual_write:slice-1"}),
                 mk_slice("slice-2", tables=["A.T1"]))
    ops = FakeOps([{"status": "launched"},
                   migrate_pass("slice-2"), verify_pass("slice-2", "h2")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"},
                          "https://x/slice-2": {"state": "open", "head": "h2"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-2"]["status"] == "merged"


def test_fanout_json_written(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    rec = json.loads((tmp_path / "recon" / "slice-1" / "fanout.json").read_text())
    assert rec["status"] == "merged" and rec["verified_head"] == "h1"
