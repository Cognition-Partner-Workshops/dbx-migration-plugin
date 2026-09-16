import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")
DOCTOR = Path(__file__).parents[1] / "factory-doctor" / "doctor.py"
GATE = {"id": "g-rows", "kind": "row_parity", "status": "pending", "evidence": ""}


def _gates_sha(batches, wave=0):
    """The recipe the fanout skill documents: sha256 of the compact JSON {wave, batches: {id: {units (sorted),
    gates: [[id, kind, status, evidence, decision_id or null], ...]}}} with sorted keys."""
    declared = {"wave": wave, "batches": {
        b["id"]: {"units": sorted(b["units"]),
                  "gates": [[g["id"], g["kind"], g["status"], g["evidence"], g.get("decision_id")] for g in b["gates"]]}
        for b in batches}}
    return hashlib.sha256(json.dumps(declared, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _workspace(tmp_path, *, mode="start", run_id=None, doctor=True, tamper=None,
               pointer_at=None, smoke=False, hook_probe="blocked:0123abcd",
               doctor_hook_probe=None, doctor_source=None, decisions=None, units=("u",), recon=None,
               gates=None, gates_sha=None, stop_c=True, prior_result=None, stop_mode="soft"):
    ws = tmp_path / "ws"
    waves = ws / ".migration" / "waves"
    waves.mkdir(parents=True)
    recon = {u: True for u in units} if recon is None else recon
    for u, eligible in recon.items():
        d = ws / ".migration" / "recon" / u
        d.mkdir(parents=True)
        (d / "result.json").write_text(eligible if isinstance(eligible, str) else json.dumps(
            {"verdict": "PASS", "merge_eligible": eligible, "merge_authority": {"kind": "harness", "decision_id": None}}))
    source = {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}}
    manifest = {
        "wave": 0,
        "width": 1,
        "repo": "github.com/acme/target",
        "child_macro": "child",
        "verify_macro": "verify",
        "base_branch": "migration/x",
        "source": source,
        "capabilities": {
            "identity": "sp-1",
            "host": "https://adb-1.azuredatabricks.net",
            "catalogs": ["mig"],
            "guard_mode": "block",
            "stop_mode": stop_mode,
            "ready": True,
        },
        "batches": [{"id": "b-1", "units": list(units), "write_targets": ["mig.t"], "brief": "brief",
                     "gates": gates if gates is not None else [GATE]}],
    }
    manifest["gates_sha"] = gates_sha or _gates_sha(manifest["batches"])
    manifest["stop_c"] = "D-2"
    ledger = f"| D-2 | 2026-01-05 | user:U0 | STOP C wave-0 gates_sha {manifest['gates_sha']} | plan approved |\n" if stop_c else ""
    if prior_result is not None:
        waves.joinpath("wave-0.result.json").write_text(json.dumps(prior_result))
    if decisions is not None or stop_c:
        (ws / ".migration" / "06_decisions.md").write_text(ledger + (decisions or ""))
    if smoke:
        manifest["smoke"] = True
    manifest_path = waves / "wave-0.json"
    manifest_path.write_text(json.dumps(manifest))
    manifest_bytes = manifest_path.read_bytes()
    doctor_hook_probe = hook_probe if doctor_hook_probe is None else doctor_hook_probe
    doctor_source = source if doctor_source is None else doctor_source
    if doctor:
        sys.path.insert(0, str(DOCTOR.parent))
        import doctor as doctor_module
        report = {
            "schema": "dbx-migration-factory/capabilities/1",
            "ready": not smoke,
            "identity": None if smoke else {
                "userName": "sp-1", "service_principal": True,
                "host": "https://adb-1.azuredatabricks.net",
            },
            "checks": [
                {"id": "allowed_targets", "status": "ok",
                 "data": {"catalogs": ["mig"], "guard_mode": "block"}},
                {"id": "workspace", "status": "ok", "data": {"stop_mode": stop_mode}},
            ],
            "hook_probe": doctor_hook_probe,
            "source": doctor_source,
        }
        signed_at = None
        if tamper == "stale":
            signed_at = "2020-01-01T00:00:00+00:00"
        record = doctor_module.sign_wave_report(report, manifest_bytes, signed_at=signed_at)
        if tamper == "not_ready":
            report["ready"] = False
            record = doctor_module.sign_wave_report(report, manifest_bytes)
        if tamper == "wrong_sha":
            manifest_path.write_bytes(manifest_bytes + b"x")
        if tamper == "signature":
            record["signature"] = ("0" if record["signature"][0] != "0" else "1") + record["signature"][1:]
        if tamper != "missing":
            waves.joinpath("wave-0.doctor.json").write_text(json.dumps(record))
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", str(origin)], check=True)
    subprocess.run(["git", "-C", str(ws), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(ws), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(ws), "config", "user.email", "t@example.com"], check=True)
    subprocess.run(["git", "-C", str(ws), "remote", "add", "origin", str(origin)], check=True)
    subprocess.run(["git", "-C", str(ws), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(ws), "commit", "-qm", "setup"], check=True)
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", "HEAD:migration/x"], check=True)
    pointer_root = tmp_path / "home" if pointer_at == "home" else ws
    pointer_dir = pointer_root / ".migration" / "waves"
    pointer_dir.mkdir(parents=True, exist_ok=True)
    pointer = {"manifest": "wave-0.json", "mode": mode, "run_id": run_id,
               "hook_probe": hook_probe}
    if pointer_at == "home":
        pointer["workspace"] = str(ws)
    (pointer_dir / "current.json").write_text(json.dumps(pointer))
    cwd = tmp_path / "home" / "sub" / "dir" if pointer_at == "home" else ws
    cwd.mkdir(parents=True, exist_ok=True)
    return ws, cwd


def _run(cwd, tmp_path, agent_reports):
    copy = tmp_path / f"copy-{len(list(tmp_path.glob('copy-*')))}"
    copy.mkdir()
    reports = tmp_path / "reports.json"
    calls = tmp_path / "calls.json"
    reports.write_text(json.dumps(agent_reports))
    shim = f"""
import json
from pathlib import Path
REPORTS = Path({str(reports)!r})
CALLS = Path({str(calls)!r})
class WorkflowAgentError(Exception):
    pass
async def register_workflow(meta):
    CALLS.write_text(json.dumps([{{"kind": "register", "meta": meta}}]))
async def agent(prompt, **kwargs):
    calls = json.loads(CALLS.read_text()) if CALLS.exists() else []
    calls.append({{"kind": "agent", "label": kwargs.get("label"), "prompt": prompt}})
    CALLS.write_text(json.dumps(calls))
    reports = json.loads(REPORTS.read_text())
    report = reports.pop(0)
    REPORTS.write_text(json.dumps(reports))
    return report
def log(message):
    print(message)
"""
    script = copy / "script.py"
    script.write_text(shim + "\n" + WORKFLOW.read_text())
    proc = subprocess.run([sys.executable, str(script)], cwd=cwd, env={},
                          capture_output=True, text=True)
    calls = json.loads(calls.read_text()) if calls.exists() else []
    return proc, calls


def _pass_report(pr_url="", **extra):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
            "pr_url": pr_url, "branch": "feature/x", "changed_paths": [],
            "gates": [{"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/result.json"}],
            "write_targets": ["mig.t"], "one_line_summary": "ok", **extra}


def _result(ws):
    return json.loads((ws / ".migration/waves/wave-0.result.json").read_text())


def test_wave_closes_only_when_every_declared_gate_is_passed_or_waived_in_the_ledger(tmp_path):
    ws, cwd = _workspace(tmp_path / "pending")
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "pending", [_pass_report(pr, gates=[])])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["batches"][0]["status"] == "FAIL" and result["batches"][0]["failure_class"] == "gates"
    assert result["batches"][0]["gates"][0]["status"] == "pending" and result["closed"] is False
    assert "g-rows" in (ws / ".migration/waves/wave-0.brief.md").read_text()

    waived = {"id": "g-export", "kind": "export_file", "status": "waived", "evidence": "", "decision_id": "D-4"}
    ws, cwd = _workspace(tmp_path / "waived", gates=[GATE, waived],
                         decisions="| D-4 | user:U1 waive g-export for u, the downstream feed is retired |\n")
    pr = _push_pr(ws)
    proc, calls = _run(cwd, tmp_path / "waived", [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["closed"] is True
    assert [g["status"] for g in result["batches"][0]["gates"]] == ["passed", "waived"]
    assert result["waived_gates"] == [{"batch": "b-1", "units": ["u"], "gate": "g-export", "decision_id": "D-4"}]
    assert "D-4" in (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert "g-rows" in [c for c in calls if c.get("label") == "verify-wave-0"][0]["prompt"]

    ws, cwd = _workspace(tmp_path / "unwaived", gates=[GATE, waived])
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "unwaived", [_pass_report(pr)])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["batches"][0]["failure_class"] == "gates"


@pytest.mark.parametrize("ledger", [
    None,                                                                   # no ledger at all
    "",
    "| D-2 | user:U0 | STOP C wave-0 gates_sha {other} |\n",                     # a different approved list
    "| D-2 | user:U0 | STOP C wave-0 {sha} |\n",                                 # the hash without its name
    "| D-2 | user:U0 | tolerance note, gates_sha {sha} |\n",                     # a human row that is not STOP C
    "| D-2 | user:U0 | STOP C wave-1 gates_sha {sha} |\n",                       # another wave's approval
    "| D-3 | user:U0 | STOP C wave-0 gates_sha {sha} |\n",                       # not the row the manifest names
    "| D-2 | user:U0 STOP C wave-0 gates_sha {sha} approved |\n",                # prose, not a parsed row
])
def test_gates_sha_must_be_the_one_a_human_approved_in_the_ledger(tmp_path, ledger):
    ws, cwd = _workspace(tmp_path, stop_c=False)
    sha = json.loads((ws / ".migration/waves/wave-0.json").read_text())["gates_sha"]
    if ledger is not None:
        (ws / ".migration" / "06_decisions.md").write_text(ledger.format(sha=sha, other="0" * 64))
    proc, calls = _run(cwd, tmp_path, [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "gates_sha" in proc.stderr and "06_decisions.md" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert not (ws / ".migration/waves/wave-0.result.json").exists()


@pytest.mark.parametrize("stop_mode,launches", [("soft", True), ("hard", False)])
def test_a_default_accepted_stop_c_row_launches_only_a_soft_stop_mode_wave(tmp_path, stop_mode, launches):
    """STOP C resolves per stop_mode, like the other stops: the orchestrator's default-accepted row approves
    the gate plan in soft mode; hard mode needs a human's row."""
    ws, cwd = _workspace(tmp_path, stop_c=False, stop_mode=stop_mode)
    sha = json.loads((ws / ".migration/waves/wave-0.json").read_text())["gates_sha"]
    (ws / ".migration" / "06_decisions.md").write_text(
        f"| D-2 | 2026-01-05 | default-accepted (soft, 60s) | STOP C wave-0 gates_sha {sha} |\n")
    proc, calls = _run(cwd, tmp_path, [_pass_report(_push_pr(ws)), _verify_report()])
    if launches:
        assert proc.returncode == 0, proc.stderr
        assert _result(ws)["batches"][0]["status"] == "PASS"
    else:
        assert proc.returncode != 0 and "gates_sha" in proc.stderr and "user:<id>" in proc.stderr
        assert not [c for c in calls if c["kind"] == "agent"]


def test_gates_subcommand_applies_the_wave_close_rule_to_hand_gathered_results(tmp_path):
    ws, cwd = _workspace(tmp_path, doctor=False, gates=[GATE, {**GATE, "id": "g-w", "kind": "export_file",
                                                                 "status": "waived", "decision_id": "D-4"}],
                         decisions="| D-4 | user:U1 waive g-w for u |\n")
    results = tmp_path / "results.json"

    def run(reports):
        results.write_text(json.dumps(reports))
        return subprocess.run([sys.executable, str(WORKFLOW), "gates", str(results)], cwd=cwd, env={},
                              capture_output=True, text=True)

    pr = _push_pr(ws)
    passed = {"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/result.json"}
    proc = run([{"batch": "b-1", "pr_url": pr, "gates": [passed]}])
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out["closed"] is True and out["batches"]["b-1"]["unmet"] == []
    assert [g["status"] for g in out["batches"]["b-1"]["gates"]] == ["passed", "waived"]
    for reports in ([{"batch": "b-1", "pr_url": pr, "gates": []}],                                   # g-rows still pending
                    [],                                                                               # batch not gathered
                    [{"batch": "b-1", "pr_url": pr, "gates": [{**passed, "evidence": ""}]}],
                    [{"batch": "b-1", "pr_url": pr, "gates": [{**passed, "evidence": ".migration/recon/u/absent.md"}]}],
                    [{"batch": "b-1", "pr_url": pr, "gates": [{**passed, "evidence": "recon/u/result.json"}]}],
                    [{"batch": "b-1", "pr_url": "https://github.com/acme/target/pull/7", "gates": [passed]}],  # no such PR
                    [{"batch": "b-1", "gates": [passed]}],                                            # no PR named
                    [{"batch": "b-9", "pr_url": pr, "gates": []}]):                                  # not in the manifest
        proc = run(reports)
        assert proc.returncode != 0 and "Traceback" not in proc.stderr, reports
        assert "g-rows" in proc.stdout or "b-1" in proc.stdout or "b-9" in proc.stderr or "pr_url" in proc.stderr, reports
    proc = run([{"batch": "b-1", "pr_url": "https://github.com/acme/target/pull/7", "gates": [passed]}])
    assert "pull/7 is not a PR of github.com/acme/target whose head git can fetch" in json.dumps(json.loads(proc.stdout)["batches"]["b-1"]["unmet"])
    proc = run("not a list")
    assert proc.returncode != 0 and "{batch, pr_url, gates}" in proc.stderr
    assert not (ws / ".migration/waves/wave-0.result.json").exists()


@pytest.mark.parametrize("changed", [{"gates": [{**GATE, "kind": "custom"}]}, {"units": ("other_unit",)}])
def test_gate_list_changed_after_stop_c_halts_before_launch(tmp_path, changed):
    approved = [{"id": "b-1", "units": ["u"], "gates": [GATE]}]
    ws, cwd = _workspace(tmp_path, gates_sha=_gates_sha(approved), **changed)
    proc, calls = _run(cwd, tmp_path, [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "gates_sha" in proc.stderr and "STOP C" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert not (ws / ".migration/waves/wave-0.result.json").exists()


def test_a_gate_marked_passed_in_the_manifest_after_stop_c_halts_before_launch(tmp_path):
    approved = [{"id": "b-1", "units": ["u"], "gates": [GATE]}]
    ws, cwd = _workspace(tmp_path, gates_sha=_gates_sha(approved),
                         gates=[{**GATE, "status": "passed", "evidence": "note.txt"}])
    proc, calls = _run(cwd, tmp_path, [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "gates_sha" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert not (ws / ".migration/waves/wave-0.result.json").exists()


def test_a_stop_c_approval_launches_one_run_of_its_wave(tmp_path):
    """The manifest's stop_c row is the one STOP C occurrence that approved this wave; a rerun after a run
    that recorded it needs STOP C to fire again (a new row, named in the manifest), not the old approval."""
    spent = {"closed": False, "run_id": "wfr-old", "base_sha": "a" * 40, "stop_c": "D-2"}
    ws, cwd = _workspace(tmp_path / "spent", mode="rerun", prior_result=spent)
    proc, calls = _run(cwd, tmp_path / "spent", [_pass_report()])
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert json.loads((ws / ".migration/waves/wave-0.result.json").read_text()) == spent

    ws, cwd = _workspace(tmp_path / "fresh", mode="rerun", prior_result={**spent, "stop_c": "D-1"})
    proc, calls = _run(cwd, tmp_path / "fresh", [_pass_report()])
    assert proc.returncode == 0, proc.stderr
    assert [c["label"] for c in calls if c["kind"] == "agent"] == ["b-1"]
    assert _result(ws)["stop_c"] == "D-2"

    ws, cwd = _workspace(tmp_path / "resume", mode="resume", run_id="wfr-old", prior_result=spent)
    (ws / ".migration/waves/wave-0.run_id").write_text("wfr-old\n")
    base = subprocess.run(["git", "-C", str(ws), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout
    (ws / ".migration/waves/wave-0.base_sha").write_text(base)
    proc, calls = _run(cwd, tmp_path / "resume", [_pass_report()])
    assert proc.returncode == 0, proc.stderr  # the same run continuing is not a second run


@pytest.mark.parametrize("prior", ["{not json", '"a string"', "[]", "null"])
def test_a_rerun_over_a_result_that_cannot_say_which_stop_c_it_spent_halts(tmp_path, prior):
    """A prior result the workflow cannot read is not proof the approval is unspent; the rerun halts until a
    human inspects or restores it, rather than launching on the old row."""
    ws, cwd = _workspace(tmp_path / "ws", mode="rerun")
    (ws / ".migration/waves/wave-0.result.json").write_text(prior)
    proc, calls = _run(cwd, tmp_path / "ws", [_pass_report()])
    assert proc.returncode != 0 and "wave-0.result.json" in proc.stderr and "STOP C" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert (ws / ".migration/waves/wave-0.result.json").read_text() == prior


def _push_pr(ws, n=1):
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", f"HEAD:refs/pull/{n}/head", "HEAD:recon/wave-0"], check=True)
    return f"https://github.com/acme/target/pull/{n}"


def _verify_report(**extra):
    return {"wave_verdict": "PASS", "unit_verdicts": {"b-1": "PASS"}, "merged_prs": [],
            "findings": [], "changed_paths": [], **extra}


def test_merge_eligible_false_is_recorded_as_merged_only_by_a_ledger_override(tmp_path):
    ledger = "| D-3 | user:U1 merge_override for u, watermark gap accepted |\n"
    override = {"kind": "human_override", "decision_id": "D-3"}
    ws, cwd = _workspace(tmp_path, decisions=ledger)
    pr = _push_pr(ws)
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr, merge_eligible=False, merge_authority=override),
                                       _verify_report()])
    assert proc.returncode == 0, proc.stderr
    result = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())
    assert result["batches"][0]["status"] == "PASS"
    assert result["batches"][0]["merge_authority"] == override
    assert result["merge_overrides"] == [{"batch": "b-1", "units": ["u"], "decision_id": "D-3"}]
    assert result["closed"] is True
    assert "D-3" in (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert "D-3" in [c for c in calls if c.get("label") == "verify-wave-0"][0]["prompt"]

    ws, cwd = _workspace(tmp_path / "no_row", decisions="| D-3 | user:U1 widen tolerance for u |\n")
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "no_row", [_pass_report(pr, merge_eligible=False, merge_authority=override)])
    assert proc.returncode == 0, proc.stderr
    result = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())
    assert result["batches"][0]["status"] == "FAIL" and result["batches"][0]["failure_class"] == "merge_authority"
    assert result["merge_overrides"] == [] and result["closed"] is False

    ws, cwd = _workspace(tmp_path / "no_ledger")
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "no_ledger", [_pass_report(pr, merge_eligible=False, merge_authority=override)])
    assert proc.returncode == 0, proc.stderr
    result = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())
    assert result["batches"][0]["failure_class"] == "merge_authority"


def test_every_unit_of_the_batch_must_be_merge_eligible_in_its_own_result_json(tmp_path):
    ws, cwd = _workspace(tmp_path, units=("u", "u2"), recon={"u": True, "u2": False})
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr)])
    assert proc.returncode == 0, proc.stderr
    batch = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())["batches"][0]
    assert batch["status"] == "FAIL" and batch["failure_class"] == "merge_authority"
    assert "u2" in batch["one_line_summary"] and "merge_authority" not in batch

    ledger = "| D-3 | user:U1 merge_override for u, u2 |\n"
    override = {"kind": "human_override", "decision_id": "D-3"}
    ws, cwd = _workspace(tmp_path / "override", units=("u", "u2"), recon={"u": True, "u2": False}, decisions=ledger)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "override", [_pass_report(pr, merge_authority=override), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    result = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())
    assert result["batches"][0]["status"] == "PASS" and result["batches"][0]["merge_authority"] == override
    assert result["merge_overrides"] == [{"batch": "b-1", "units": ["u", "u2"], "decision_id": "D-3"}]


@pytest.mark.parametrize("recon", [{}, {"u": "not json"}, {"u": json.dumps({"verdict": "PASS"})}])
def test_a_unit_without_readable_recon_evidence_in_the_pr_is_not_merge_eligible(tmp_path, recon):
    ws, cwd = _workspace(tmp_path, recon=recon)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr)])
    assert proc.returncode == 0, proc.stderr
    batch = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())["batches"][0]
    assert batch["status"] == "FAIL" and batch["failure_class"] == "merge_authority"
    assert ".migration/recon/u/result.json" in batch["one_line_summary"]


def test_harness_pass_records_harness_authority_and_no_overrides(tmp_path):
    ws, cwd = _workspace(tmp_path)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    result = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())
    assert result["batches"][0]["merge_authority"] == {"kind": "harness", "decision_id": None}
    assert result["merge_overrides"] == [] and result["closed"] is True


def test_start_launches_children_and_writes_the_result(tmp_path):
    ws, cwd = _workspace(tmp_path)
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode == 0
    assert [c["label"] for c in calls if c["kind"] == "agent"] == ["b-1"]
    result = json.loads((ws / ".migration/waves/wave-0.result.json").read_text())
    assert result["batches"][0]["status"] == "FAIL"
    assert result["mode"] == "start" and result["run_id"] is None
    assert result["hook_probe"] == "blocked:0123abcd"
    assert (ws / ".migration/waves/wave-0.brief.md").exists()
    assert not (ws / ".migration/waves/wave-0.run_id").exists()
    ws2, cwd2 = _workspace(tmp_path / "second", run_id="wfr-x")
    proc2, calls2 = _run(cwd2, tmp_path / "second", [_pass_report()])
    assert proc2.returncode != 0
    assert "Traceback" not in proc2.stderr
    assert "run_id must be null unless mode is resume" in proc2.stderr
    assert not [c for c in calls2 if c["kind"] == "agent"]
    assert not (ws2 / ".migration/waves/wave-0.run_id").exists()


def test_pointer_above_the_cwd_names_the_workspace(tmp_path):
    ws, cwd = _workspace(tmp_path, pointer_at="home")
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode == 0
    assert [c["label"] for c in calls if c["kind"] == "agent"] == ["b-1"]


@pytest.mark.parametrize("tamper", ["missing", "not_ready", "wrong_sha", "stale", "signature", "hook_probe", "source"])
def test_invalid_doctor_record_launches_nothing(tmp_path, tamper):
    kwargs = {"tamper": tamper}
    if tamper == "hook_probe":
        kwargs.update(hook_probe="not-blocked", doctor_hook_probe="unknown")
    if tamper == "source":
        kwargs["doctor_source"] = {"family": "sqlserver", "secret": "OTHER_DSN", "params": {"db": "loans"}}
    ws, cwd = _workspace(tmp_path, **kwargs)
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode != 0
    assert not [c for c in calls if c["kind"] == "agent"]
    assert not (ws / ".migration/waves/wave-0.result.json").exists()
    assert any(word in proc.stderr.lower() for word in ("doctor", "manifest", "signature", "ready"))


def test_smoke_mode_skips_readiness_only(tmp_path):
    ws, cwd = _workspace(tmp_path, mode="smoke", smoke=True)
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode == 0 and len([c for c in calls if c["kind"] == "agent"]) == 1
    ws, cwd = _workspace(tmp_path / "start", mode="start", smoke=True)
    proc, _ = _run(cwd, tmp_path / "start", [_pass_report()])
    assert proc.returncode != 0 and "smoke" in proc.stderr
    ws, cwd = _workspace(tmp_path / "bad", mode="smoke", smoke=False)
    proc, _ = _run(cwd, tmp_path / "bad", [_pass_report()])
    assert proc.returncode != 0


def test_start_refuses_a_halted_result_and_resume_needs_the_recorded_run_id(tmp_path):
    ws, cwd = _workspace(tmp_path)
    waves = ws / ".migration/waves"
    waves.joinpath("wave-0.result.json").write_text(json.dumps(
        {"closed": False, "run_id": "wfr-a", "base_sha": "a" * 40}))
    waves.joinpath("wave-0.run_id").write_text("wfr-a\n")
    proc, _ = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode != 0 and "mode: resume" in proc.stderr
    ws, cwd = _workspace(tmp_path / "wrong", mode="resume", run_id="wfr-b")
    waves = ws / ".migration/waves"
    waves.joinpath("wave-0.result.json").write_text(json.dumps(
        {"closed": False, "run_id": "wfr-a", "base_sha": "a" * 40}))
    waves.joinpath("wave-0.run_id").write_text("wfr-a\n")
    proc, _ = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode != 0

    ws, cwd = _workspace(tmp_path / "rerun", mode="rerun")
    waves = ws / ".migration/waves"
    waves.joinpath("wave-0.result.json").write_text(json.dumps(
        {"closed": False, "run_id": "wfr-old", "base_sha": "a" * 40}))
    waves.joinpath("wave-0.run_id").write_text("wfr-old\n")
    proc, _ = _run(cwd, tmp_path / "rerun", [_pass_report()])
    assert proc.returncode == 0
    assert not waves.joinpath("wave-0.run_id").exists()


def test_script_reads_no_environment_and_no_file_path():
    src = WORKFLOW.read_text()
    assert "os.environ" not in src
    assert "__file__" not in src
    assert "import os" not in src
    assert "W" + "AVE_" not in src


def test_docs_describe_the_pointer_and_doctor_wave_steps():
    skill = (WORKFLOW.parent / "SKILL.md").read_text()
    orchestrator = (WORKFLOW.parents[1] / "install-dbx-factory" / "playbooks" / "9-orchestrator.md").read_text()
    for doc in (skill, orchestrator):
        assert "current.json" in doc
        assert "--wave" in doc
        assert "run_id" in doc
        assert '"workspace"' in doc or "workspace" in doc
        assert "W" + "AVE_" not in doc
