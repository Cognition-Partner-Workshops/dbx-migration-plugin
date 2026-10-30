"""Tests for tools/fanout.py (design-0.7 section 7); children faked."""
import json
from collections import Counter

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
    def __init__(self, reports=(), states=None, diffs=None, polls=()):
        self.reports = list(reports)
        self.states = dict(states or {})
        self.diffs = dict(diffs or {})
        self.polls = list(polls)
        self.launches = []
        self.merges = []
        self.diff_calls = []

    def launch(self, brief):
        self.launches.append(brief)
        assert self.reports, f"unexpected launch: {brief}"
        return self.reports.pop(0)

    def poll(self, session_id):
        return self.polls.pop(0) if self.polls else None

    def pr_state(self, pr_url):
        return dict(self.states.get(pr_url, {"state": "open", "head": "h0"}))

    def merge(self, pr_url, sha):
        self.merges.append((pr_url, sha))
        self.states[pr_url] = {"state": "merged", "head": sha}

    def changed_paths(self, pr_url):
        self.diff_calls.append(pr_url)
        return list(self.diffs.get(pr_url, []))


def seed_record(tmp_path, sid, **fields):
    rec = {"slice": sid, "status": "new", **fields}
    path = tmp_path / "recon" / sid / "fanout.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rec))


def test_dependency_ready_launch_order(tmp_path):
    doc = mk_doc(mk_slice("slice-1"), mk_slice("slice-2", depends_on=["slice-1"]),
                 mk_slice("slice-3", depends_on=["slice-2"]))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1"),
                   migrate_pass("slice-2"), verify_pass("slice-2", "h2"),
                   {"status": "FAIL", "slice": "slice-3", "failure_class": "migrate",
                    "one_line_summary": "nope"}],
                  states={f"https://x/slice-{i}": {"state": "open", "head": f"h{i}"}
                          for i in (1, 2)})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 5
    assert summary["slices"]["slice-1"]["status"] == "merged"
    assert summary["slices"]["slice-2"]["status"] == "merged"
    assert summary["slices"]["slice-3"]["status"] == "failed"


def test_merge_uses_verified_head(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert ops.merges == [("https://x/slice-1", "h1")]
    assert summary["slices"]["slice-1"]["status"] == "merged"


@pytest.mark.parametrize("bad", [".migration/policy.json", ".migration/slices.json",
                                 ".migration/recon/slice-1/inputs/mapping.json",
                                 ".migration/recon/slice-1/fanout.json"])
def test_protected_paths_fail_child(tmp_path, bad):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}},
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
    ops = FakeOps([{"status": "FAIL", "slice": "s", "failure_class": "migrate"}] * 2)
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


def test_slice0_not_gated_by_canary(tmp_path):
    doc = mk_doc(mk_slice("slice-0"),
                 mk_slice("slice-1", canary=True, auto_merge=False))
    ops = FakeOps([migrate_pass("slice-0"), verify_pass("slice-0", "h0"),
                   migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={f"https://x/slice-{i}": {"state": "open", "head": f"h{i}"}
                          for i in (0, 1)})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-0"]["status"] == "merged"
    assert summary["slices"]["slice-1"]["status"] == "awaiting_human"


def test_failed_slice_no_same_invocation_retry(tmp_path):
    seed_record(tmp_path, "slice-1", status="failed", failure_class="migrate")
    summary = fanout.run(FakeOps(), mk_policy(), mk_doc(mk_slice("slice-1")),
                         migration_dir=str(tmp_path), retry_failed=False)
    assert summary["slices"]["slice-1"]["status"] == "failed"


def test_failures_shared_across_passes(tmp_path):
    ops = FakeOps([{"status": "FAIL", "slice": "s", "failure_class": "migrate"}] * 4)
    fails = Counter()
    for _ in range(4):
        summary = fanout.run(ops, mk_policy(), mk_doc(mk_slice("slice-1")),
                             migration_dir=str(tmp_path), failures=fails)
    assert fails["migrate"] == 3
    assert summary["slices"]["slice-1"]["status"] == "halted"


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


def test_canary_prereq_is_not_gated(tmp_path):
    doc = mk_doc(mk_slice("slice-1", canary=True, auto_merge=False,
                          depends_on=["slice-2"]),
                 mk_slice("slice-2"))
    ops = FakeOps([migrate_pass("slice-2"), verify_pass("slice-2", "h2"),
                   migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={f"https://x/slice-{i}": {"state": "open", "head": f"h{i}"}
                          for i in (1, 2)})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-2"]["status"] == "merged"
    assert summary["slices"]["slice-1"]["status"] == "awaiting_human"


def test_canary_gates_later_slices(tmp_path):
    doc = mk_doc(mk_slice("slice-1", canary=True, auto_merge=False),
                 mk_slice("slice-2"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-1"]["status"] == "awaiting_human"
    assert summary["slices"]["slice-2"]["status"] == "waiting"
    assert len(ops.launches) == 2
    ops.states["https://x/slice-1"] = {"state": "merged", "head": "h1"}
    ops.reports += [migrate_pass("slice-2"), verify_pass("slice-2", "h2")]
    ops.states["https://x/slice-2"] = {"state": "open", "head": "h2"}
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-2"]["status"] == "merged"


def test_parity_fail_blocks_merge(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    report = dict(verify_pass("slice-1", "h1"), parity="FAIL")
    ops = FakeOps([migrate_pass("slice-1"), report],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert ops.merges == []
    assert summary["slices"]["slice-1"]["failure_class"] == "parity"


def test_head_move_before_diff_reverifies(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h2"}})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-1"]["status"] == "verify_pending"
    assert ops.diff_calls == [] and ops.merges == []


def test_merge_refusal_same_head_retries(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})
    ops.merge = lambda url, sha: 1 / 0
    assert fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))[
        "slices"]["slice-1"]["status"] == "merge_ready"
    assert fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))[
        "slices"]["slice-1"]["status"] == "awaiting_human"
    assert len(ops.launches) == 2


def test_merge_refusal_moved_head_reverifies(tmp_path):
    doc = mk_doc(mk_slice("slice-1"))
    ops = FakeOps([migrate_pass("slice-1"), verify_pass("slice-1", "h1")],
                  states={"https://x/slice-1": {"state": "open", "head": "h1"}})

    def refuse(url, sha):
        ops.states[url]["head"] = "h2"  # head moved between verify and merge
        raise RuntimeError("refused")
    ops.merge = refuse
    assert fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))[
        "slices"]["slice-1"]["status"] == "verify_pending"


def test_async_children_run_in_parallel(tmp_path):
    doc = mk_doc(mk_slice("slice-1"), mk_slice("slice-2"))
    ops = FakeOps([{"status": "launched", "session_id": s}
                   for s in ("m1", "m2", "v1", "v2")])
    fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 2  # both migrates in flight at once
    ops.polls = [migrate_pass("slice-1"), migrate_pass("slice-2")]
    fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert len(ops.launches) == 4  # two verifies launched
    ops.polls = [verify_pass("slice-1", "h1"), verify_pass("slice-2", "h2")]
    ops.states.update({f"https://x/slice-{i}": {"state": "open", "head": f"h{i}"}
                       for i in (1, 2)})
    summary = fanout.run(ops, mk_policy(), doc, migration_dir=str(tmp_path))
    assert all(summary["slices"][f"slice-{i}"]["status"] == "merged" for i in (1, 2))


@pytest.mark.parametrize("extra", [[], ["bad/id"]])
def test_missing_dependency_fails(tmp_path, extra):
    doc = mk_doc(*[mk_slice(x) for x in extra],
                 mk_slice("slice-1", depends_on=[extra[0] if extra else "ghost"]))
    summary = fanout.run(FakeOps(), mk_policy(), doc, migration_dir=str(tmp_path))
    assert summary["slices"]["slice-1"]["failure_class"] == "deps"
