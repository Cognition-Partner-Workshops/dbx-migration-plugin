import fcntl
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")
DOCTOR = Path(__file__).parents[1] / "factory-doctor" / "doctor.py"
PLUGIN = Path(__file__).parents[2]
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
               gates=None, gates_sha=None, stop_c=True, prior_result=None, stop_mode="soft",
               other_waves=None, mappings=None, namespace=None, dependencies=None, write_targets=("mig.t",),
               deploy_objects=None, max_minutes=None, batch_max_minutes=None, manifest_name="wave-0.json", wave=0,
               pipelines=None, auto_merge=None, close_minutes=None, other_batch=None,
               lakeflow_pipelines=(), extra_batches=(), serialized_pipelines=None, plugin=PLUGIN, width=1,
               manifest_extra=None):
    ws = tmp_path / "ws"
    waves = ws / ".migration" / "waves"
    waves.mkdir(parents=True)
    recon = {u: True for u in units} if recon is None else recon
    for u, eligible in recon.items():
        d = ws / ".migration" / "recon" / u
        d.mkdir(parents=True)
        (d / "result.json").write_text(eligible if isinstance(eligible, str) else json.dumps(
            {"verdict": "PASS", "merge_eligible": eligible, "merge_authority": {"kind": "harness", "decision_id": None}}))
    for name, text in (other_waves or {}).items():
        (waves / name).write_text(text)
    for unit, spec in (mappings or {}).items():
        path = ws / ".migration" / "units" / unit / "mapping_spec.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(spec))
    for unit, text in (dependencies or {}).items():
        path = ws / ".migration" / "units" / unit / "dependencies.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    source = {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}}
    manifest = {
        "wave": wave,
        "width": width,
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
        "batches": [{"id": "b-1", "units": list(units), "write_targets": list(write_targets), "brief": "brief",
                     "gates": gates if gates is not None else [GATE], "lakeflow_pipelines": list(lakeflow_pipelines)},
                    *extra_batches],
    }
    if serialized_pipelines is not None:
        manifest["serialized_pipelines"] = dict(serialized_pipelines)
    if deploy_objects is not None:
        manifest["batches"][0]["deploy_objects"] = list(deploy_objects)
    if max_minutes is not None:
        manifest["max_minutes"] = max_minutes
    if batch_max_minutes is not None:
        manifest["batches"][0]["max_minutes"] = batch_max_minutes
    if auto_merge is not None:
        manifest["auto_merge"] = auto_merge
    if close_minutes is not None:
        manifest["close_minutes"] = close_minutes
    if other_batch is not None:
        manifest["batches"].append(other_batch)
    manifest.update(manifest_extra or {})
    if namespace is not None:
        manifest["target_namespace"] = namespace
    if pipelines is not None:
        manifest["pipelines"] = dict(pipelines)
    manifest["gates_sha"] = gates_sha or _gates_sha(manifest["batches"], wave)
    manifest["stop_c"] = "D-2"
    ledger = f"| D-2 | 2026-01-05 | user:U0 | STOP C wave-{wave} gates_sha {manifest['gates_sha']} | plan approved |\n" if stop_c else ""
    if prior_result is not None:
        waves.joinpath(manifest_name.replace(".json", ".result.json")).write_text(json.dumps(prior_result))
    if decisions is not None or stop_c:
        (ws / ".migration" / "06_decisions.md").write_text(ledger + (decisions or ""))
    if smoke:
        manifest["smoke"] = True
    manifest_path = waves / manifest_name
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
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
            manifest_path.with_suffix(".doctor.json").write_text(json.dumps(record))
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
    pointer = {"manifest": manifest_name, "mode": mode, "run_id": run_id,
               "hook_probe": hook_probe}
    if plugin is not None:
        pointer["plugin"] = str(plugin)
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
import subprocess
from pathlib import Path
REPORTS = Path({str(reports)!r})
CALLS = Path({str(calls)!r})
class WorkflowAgentError(Exception):
    pass
async def register_workflow(meta):
    CALLS.write_text(json.dumps([{{"kind": "register", "meta": meta}}]))
async def agent(prompt, **kwargs):
    calls = json.loads(CALLS.read_text()) if CALLS.exists() else []
    calls.append({{"kind": "agent", "label": kwargs.get("label"), "prompt": prompt, "kwargs": kwargs}})
    CALLS.write_text(json.dumps(calls))
    reports = json.loads(REPORTS.read_text())
    report = reports.pop(0)
    REPORTS.write_text(json.dumps(reports))
    for cmd in report.pop("__run__", []):
        subprocess.run(cmd, check=True, capture_output=True)
    if report.get("error"):
        raise WorkflowAgentError(report["error"])
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
            "write_targets": ["mig.t"],
            "one_line_summary": "ok", **extra}


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
                         decisions="| D-4 | user:U1 | waive g-export for u, the downstream feed is retired |\n")
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


def test_a_waiver_a_human_wrote_for_an_earlier_run_does_not_waive_the_gate_after_a_new_stop_c(tmp_path):
    """A rerun's STOP C is a new row the manifest names; a post-STOP C waiver counts only when it is written
    after that row, so the earlier run's waiver of the same gate and units is not silently reused."""
    waiver = "| D-4 | user:U1 | waive g-rows for u |\n"
    ws, cwd = _workspace(tmp_path / "before")
    ledger = ws / ".migration" / "06_decisions.md"
    ledger.write_text(waiver + ledger.read_text())
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "before", [_pass_report(pr, gates=[])])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["batches"][0]["failure_class"] == "gates" and result["waived_gates"] == []
    assert "D-4" not in json.dumps(result["batches"][0]["gates"])

    ws, cwd = _workspace(tmp_path / "after", decisions=waiver)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "after", [_pass_report(pr, gates=[]), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["waived_gates"] == [{"batch": "b-1", "units": ["u"], "gate": "g-rows", "decision_id": "D-4"}]


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
                         decisions="| D-4 | user:U1 | waive g-w for u |\n")
    results = tmp_path / "results.json"

    def run(reports):
        results.write_text(json.dumps(reports))
        return subprocess.run([sys.executable, str(WORKFLOW), "gates", str(results)], cwd=cwd, env={},
                              capture_output=True, text=True)

    pr = _push_pr(ws)
    passed = {"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/result.json"}
    proc = run([{"batch": "b-1", "pr_url": pr, "gates": [passed]}])
    assert proc.returncode != 0 and "workflow.py reserve" in proc.stderr and "D-2" in proc.stderr  # no reservation
    assert _workflow(cwd, "reserve").returncode == 0
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
    runs = ws / ".migration/waves/wave-0.runs.jsonl"
    assert [json.loads(l)["mode"] for l in runs.read_text().splitlines()] == ["reserve"]  # unmet gates: the run stays open
    proc = run([{"batch": "b-1", "pr_url": pr, "gates": [passed]}])
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out["closed"] is True and out["batches"]["b-1"]["unmet"] == [] and "review_waiver" not in out["batches"]["b-1"]
    assert [g["status"] for g in out["batches"]["b-1"]["gates"]] == ["passed", "waived"]
    assert [(json.loads(l)["stop_c"], json.loads(l)["mode"]) for l in runs.read_text().splitlines()] == [("D-2", "reserve"), ("D-2", "gates")]
    proc = run([{"batch": "b-1", "pr_url": pr, "gates": [passed]}])
    assert proc.returncode != 0 and "closed" in proc.stderr and "D-2" in proc.stderr and "STOP C" in proc.stderr
    assert not (ws / ".migration/waves/wave-0.result.json").exists()


def _workflow(cwd, *args, **kw):
    return subprocess.run([sys.executable, str(WORKFLOW), *args], cwd=cwd, env={}, capture_output=True, text=True, **kw)


def test_a_hand_run_wave_reserves_its_stop_c_row_before_launch_and_a_spent_row_reserves_nothing(tmp_path):
    """`workflow.py reserve` is the hand-run path's launch record: it spends the manifest's STOP C row in the
    run log as a workflow start does, so a second hand launch, or a workflow start over it, halts."""
    ws, cwd = _workspace(tmp_path / "hand", doctor=False)
    runs = ws / ".migration/waves/wave-0.runs.jsonl"
    proc = _workflow(cwd, "reserve")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == {"wave": 0, "stop_c": "D-2", "reserved": True}
    assert [json.loads(l) for l in runs.read_text().splitlines()] == [{"stop_c": "D-2", "mode": "reserve", "run_id": None}]
    proc = _workflow(cwd, "reserve")
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr
    assert len(runs.read_text().splitlines()) == 1

    ws, cwd = _workspace(tmp_path / "then_workflow")
    assert _workflow(cwd, "reserve").returncode == 0
    proc, calls = _run(cwd, tmp_path / "then_workflow", [_pass_report()])
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "after_workflow", doctor=False)
    (ws / ".migration/waves/wave-0.runs.jsonl").write_text(json.dumps({"stop_c": "D-2", "mode": "start", "run_id": None}) + "\n")
    proc = _workflow(cwd, "reserve")
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr
    (tmp_path / "after_workflow" / "results.json").write_text("[]")
    proc = _workflow(cwd, "gates", str(tmp_path / "after_workflow" / "results.json"))
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr and "Traceback" not in proc.stderr


def test_the_stop_c_row_is_spent_under_the_run_log_lock_so_two_starts_cannot_both_take_it(tmp_path):
    """The check that a row is unspent and the record that spends it happen under an exclusive lock on the
    run log: a start that finds the log locked waits, then sees the other start's record and halts."""
    ws, cwd = _workspace(tmp_path / "ws", doctor=False)
    runs = ws / ".migration/waves/wave-0.runs.jsonl"
    runs.touch()
    with runs.open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        waiting = subprocess.Popen([sys.executable, str(WORKFLOW), "reserve"], cwd=cwd, env={},
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        with pytest.raises(subprocess.TimeoutExpired):
            waiting.wait(timeout=2)
        held.write(json.dumps({"stop_c": "D-2", "mode": "start", "run_id": None}) + "\n")
        held.flush()
        fcntl.flock(held, fcntl.LOCK_UN)
    _, err = waiting.communicate(timeout=30)
    assert waiting.returncode != 0 and "D-2" in err and "STOP C" in err
    assert len(runs.read_text().splitlines()) == 1


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


def test_every_run_is_appended_to_the_wave_run_log_and_a_stop_c_row_spent_by_any_of_them_is_not_reused(tmp_path):
    """The result file holds the last run only, so alternating two STOP C rows would reuse each in turn; the
    wave's run log is append-only and every run's stop_c is checked against all of it."""
    ws, cwd = _workspace(tmp_path / "log")
    proc, _ = _run(cwd, tmp_path / "log", [_pass_report()])
    assert proc.returncode == 0, proc.stderr
    runs = ws / ".migration/waves/wave-0.runs.jsonl"
    assert [json.loads(l)["stop_c"] for l in runs.read_text().splitlines()] == ["D-2"]

    prior = {"closed": False, "run_id": "wfr-old", "base_sha": "a" * 40, "stop_c": "D-1"}
    log = json.dumps({"stop_c": "D-2", "mode": "start"}) + "\n" + json.dumps({"stop_c": "D-1", "mode": "rerun"}) + "\n"
    ws, cwd = _workspace(tmp_path / "alternating", mode="rerun", prior_result=prior)
    (ws / ".migration/waves/wave-0.runs.jsonl").write_text(log)
    proc, calls = _run(cwd, tmp_path / "alternating", [_pass_report()])
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr and "runs.jsonl" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert (ws / ".migration/waves/wave-0.runs.jsonl").read_text() == log

    ws, cwd = _workspace(tmp_path / "deleted_result")  # start mode, result removed, log remembers
    (ws / ".migration/waves/wave-0.runs.jsonl").write_text(json.dumps({"stop_c": "D-2", "mode": "start"}) + "\n")
    proc, calls = _run(cwd, tmp_path / "deleted_result", [_pass_report()])
    assert proc.returncode != 0 and "D-2" in proc.stderr and "STOP C" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "fresh", mode="rerun", prior_result=prior)
    (ws / ".migration/waves/wave-0.runs.jsonl").write_text(json.dumps({"stop_c": "D-1", "mode": "start"}) + "\n")
    proc, calls = _run(cwd, tmp_path / "fresh", [_pass_report()])
    assert proc.returncode == 0, proc.stderr
    assert [json.loads(l)["stop_c"] for l in (ws / ".migration/waves/wave-0.runs.jsonl").read_text().splitlines()] == ["D-1", "D-2"]

    ws, cwd = _workspace(tmp_path / "resume", mode="resume", run_id="wfr-old", prior_result={**prior, "stop_c": "D-2"})
    (ws / ".migration/waves/wave-0.runs.jsonl").write_text(json.dumps({"stop_c": "D-2", "mode": "start", "run_id": "wfr-old"}) + "\n")
    (ws / ".migration/waves/wave-0.run_id").write_text("wfr-old\n")
    base = subprocess.run(["git", "-C", str(ws), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout
    (ws / ".migration/waves/wave-0.base_sha").write_text(base)
    proc, _ = _run(cwd, tmp_path / "resume", [_pass_report()])
    assert proc.returncode == 0, proc.stderr  # continuing the run that spent it
    assert len((ws / ".migration/waves/wave-0.runs.jsonl").read_text().splitlines()) == 1


@pytest.mark.parametrize("log", ["{not json\n", "[]\n", '{"mode": "start"}\n', ""])
def test_a_run_log_that_cannot_say_which_stop_c_rows_were_spent_halts(tmp_path, log):
    ws, cwd = _workspace(tmp_path / "ws", mode="rerun", prior_result={"closed": False, "stop_c": "D-1"})
    (ws / ".migration/waves/wave-0.runs.jsonl").write_text(log)
    proc, calls = _run(cwd, tmp_path / "ws", [_pass_report()])
    if log == "":
        assert proc.returncode == 0, proc.stderr
        return
    assert proc.returncode != 0 and "runs.jsonl" in proc.stderr and "STOP C" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert (ws / ".migration/waves/wave-0.runs.jsonl").read_text() == log


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


WAVE_1 = json.dumps({"wave": 1, "batches": [{"id": "b-2", "units": ["v"], "write_targets": ["mig.t"], "brief": "b"}]})
MAPPING = {"objects": [{"object": "mig.t", "root_table": "dbo.t", "key": ["id"], "scope_columns": ["run_date"]}]}
BOUNDED_MAPPING = {"objects": [{**MAPPING["objects"][0], "root_where": "run_date = '${as_of}'",
                                "target_where": "run_date = '${as_of}'"}]}
PRIOR_MAPPING = {"objects": [{**MAPPING["objects"][0], "root_where": "run_date = '${prior_as_of}'",
                              "target_where": "run_date = '${prior_as_of}'"}]}


def test_shared_table_across_waves_halts_before_launch_unless_every_mapping_is_bounded(tmp_path):
    ws, cwd = _workspace(tmp_path / "open", other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": MAPPING, "v": BOUNDED_MAPPING})
    proc, calls = _run(cwd, tmp_path / "open", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0
    assert "'mig.t'" in proc.stderr and "b-1" in proc.stderr and "b-2" in proc.stderr and "target_where" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert not (ws / ".migration/waves/wave-0.result.json").exists()

    tautology = {"objects": [{**MAPPING["objects"][0], "root_where": "1 = 1", "target_where": "1 = 1"}]}
    ws, cwd = _workspace(tmp_path / "taut", other_waves={"wave-1.json": WAVE_1.replace("mig.t", "MIG.T")},
                         mappings={"u": tautology, "v": BOUNDED_MAPPING})
    proc, calls = _run(cwd, tmp_path / "taut", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "'mig.t'" in proc.stderr and "target_where" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    bounded = {"objects": [{**BOUNDED_MAPPING["objects"][0], "object": "T"}]}
    unbounded = {"objects": [{**MAPPING["objects"][0], "object": "T"}]}
    ws, cwd = _workspace(tmp_path / "bare", other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": unbounded, "v": BOUNDED_MAPPING})
    proc, calls = _run(cwd, tmp_path / "bare", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "(unit u) reads it without a target_where" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    ws, cwd = _workspace(tmp_path / "elsewhere", other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": {"objects": [{**MAPPING["objects"][0], "object": "other.t"}]}, "v": BOUNDED_MAPPING})
    proc, calls = _run(cwd, tmp_path / "elsewhere", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "no unit of b-1 reads 'mig.t'" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    sibling_bare = WAVE_1.replace('"mig.t"', '"t"').replace('"wave": 1', '"wave": 1, "target_namespace": "mig"')
    ws, cwd = _workspace(tmp_path / "prior", other_waves={"wave-1.json": sibling_bare},
                         mappings={"u": bounded}, namespace="MIG")
    proc, calls = _run(cwd, tmp_path / "prior", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "units/v/mapping_spec.json is missing" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "other_ns", other_waves={"wave-1.json": WAVE_1.replace('"mig.t"', '"t"')},
                         mappings={"u": bounded}, namespace="MIG")
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "other_ns", [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["closed"] is True

    ws, cwd = _workspace(tmp_path / "bad_ns", other_waves={"wave-1.json": sibling_bare.replace('"mig"', '"cat."')},
                         mappings={"u": bounded}, namespace="MIG")
    proc, calls = _run(cwd, tmp_path / "bad_ns", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "wave-1.json" in proc.stderr and "target_namespace" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "overlap", other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": bounded, "v": BOUNDED_MAPPING})
    proc, calls = _run(cwd, tmp_path / "overlap", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "'mig.t'" in proc.stderr and "unit u and unit v (wave-1.json b-2)" in proc.stderr
    assert "overlap" in proc.stderr and not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "bounded", other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": bounded, "v": PRIOR_MAPPING})
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "bounded", [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["closed"] is True


def test_a_hand_run_wave_reserves_nothing_while_a_shared_table_is_unbounded(tmp_path):
    """`reserve` is the small wave's launch: it runs the same cross-wave collision check the workflow does
    before it spends the STOP C row, so an unbounded mapping on a shared table launches nothing by hand either."""
    ws, cwd = _workspace(tmp_path / "open", doctor=False, other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": MAPPING, "v": BOUNDED_MAPPING})
    proc = _workflow(cwd, "reserve")
    assert proc.returncode != 0 and "Traceback" not in proc.stderr
    assert "'mig.t'" in proc.stderr and "b-1" in proc.stderr and "b-2" in proc.stderr and "target_where" in proc.stderr
    assert not (ws / ".migration/waves/wave-0.runs.jsonl").exists()

    ws, cwd = _workspace(tmp_path / "overlap", doctor=False, other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": BOUNDED_MAPPING, "v": BOUNDED_MAPPING})
    proc = _workflow(cwd, "reserve")
    assert proc.returncode != 0 and "overlap" in proc.stderr
    assert not (ws / ".migration/waves/wave-0.runs.jsonl").exists()

    ws, cwd = _workspace(tmp_path / "bounded", doctor=False, other_waves={"wave-1.json": WAVE_1},
                         mappings={"u": BOUNDED_MAPPING, "v": PRIOR_MAPPING})
    proc = _workflow(cwd, "reserve")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["reserved"] is True


def test_a_hand_run_wave_reserves_nothing_while_its_declared_targets_differ_from_the_call_graph(tmp_path):
    """`preflight` is advice about a manifest that may change before `reserve` runs; `reserve` checks the
    manifest it spends the STOP C row on, call graph included."""
    ws, cwd = _workspace(tmp_path / "drift", doctor=False, dependencies={"u": _analysis("mig.t", "mig.audit")})
    proc = _workflow(cwd, "reserve")
    assert proc.returncode != 0 and "Traceback" not in proc.stderr
    assert "b-1" in proc.stderr and "mig.audit" in proc.stderr
    assert not (ws / ".migration/waves/wave-0.runs.jsonl").exists()

    ws, cwd = _workspace(tmp_path / "ok", doctor=False, dependencies={"u": _analysis("MIG.T")},
                         write_targets=("mig.t", "mig.run"), deploy_objects=("mig.run",), namespace="mig")
    proc = _workflow(cwd, "reserve")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["reserved"] is True


def _second_batch(pipelines, unit="v"):
    return {"id": "b-2", "units": [unit], "write_targets": [f"mig.{unit}"], "brief": "brief", "gates": [GATE],
            "lakeflow_pipelines": list(pipelines)}


def test_a_pipeline_two_batches_share_halts_before_any_child_launches(tmp_path):
    """The one-active-update check is the plugin's pipeline_updates.py run as a subprocess on the manifest
    (the pointer's `plugin` names the plugin root); its non-zero exit is a halt before launch, and a wave
    whose batches do not declare their pipelines is unchecked, so it halts too."""
    ws, cwd = _workspace(tmp_path / "shared", units=("u",), recon={"u": True, "v": True}, manifest_name="wave-1.json",
                         wave=1, width=2, lakeflow_pipelines=("p",), extra_batches=[_second_batch(["p"])])
    proc, calls = _run(cwd, tmp_path / "shared", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "Traceback" not in proc.stderr
    assert "pipeline_updates" in proc.stderr and "'p'" in proc.stderr and "b-1" in proc.stderr and "b-2" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "undeclared", recon={"u": True, "v": True},
                         extra_batches=[{k: v for k, v in _second_batch([]).items() if k != "lakeflow_pipelines"}])
    proc, calls = _run(cwd, tmp_path / "undeclared", [])
    assert proc.returncode != 0 and "unsupported" in proc.stderr and "b-2" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "noplugin", plugin=None)
    proc, calls = _run(cwd, tmp_path / "noplugin", [])
    assert proc.returncode != 0 and "plugin" in proc.stderr and "pipeline_updates.py" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


def test_a_serialized_shared_pipeline_launches_its_batches_one_after_the_other(tmp_path):
    """With a pipeline_serialized decision the wave launches, and b-2 starts only after b-1 finished even at
    width 2: the pipeline_updates `order` is what run_batch waits on."""
    ledger = "| D-7 | 2026-01-06 | user:U1 | pipeline_serialized p: b-1 then b-2 |\n"
    ws, cwd = _workspace(tmp_path, recon={"u": True, "v": True}, decisions=ledger, manifest_name="wave-1.json", wave=1,
                         lakeflow_pipelines=("p",), extra_batches=[_second_batch(["p"])], serialized_pipelines={"p": "D-7"},
                         width=2)
    proc, calls = _run(cwd, tmp_path, [_pass_report(""), _pass_report("")])
    assert proc.returncode == 0, proc.stderr
    launched = [c["label"] for c in calls if c["kind"] == "agent"]
    assert launched == ["b-1", "b-2"]
    out = proc.stdout
    assert out.index("done   b-1") < out.index("launch b-2")
    assert json.loads((ws / ".migration/waves/wave-1.result.json").read_text())["pipeline_order"] == {"b-2": ["b-1"]}


def test_malformed_sibling_wave_manifest_halts_before_launch(tmp_path):
    ws, cwd = _workspace(tmp_path, other_waves={"wave-1.json": "{"})
    proc, calls = _run(cwd, tmp_path, [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "wave-1.json" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


def _analysis(*writes):
    return json.dumps({"routines": [
        {"routine": "app.run", "reads": ["src.t"], "writes": [], "calls": ["app.write"]},
        {"routine": "app.write", "reads": [], "writes": list(writes), "calls": []}]})


def test_declared_write_targets_must_equal_the_call_graphs_transitive_writes(tmp_path):
    ws, cwd = _workspace(tmp_path / "drift", dependencies={"u": _analysis("mig.t", "mig.audit")})
    proc, calls = _run(cwd, tmp_path / "drift", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0
    assert "b-1" in proc.stderr and "missing" in proc.stderr and "mig.audit" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]
    assert not (ws / ".migration/waves/wave-0.result.json").exists()

    ws, cwd = _workspace(tmp_path / "same", dependencies={"u": _analysis("MIG.T")}, write_targets=("mig.t", "mig.run"),
                         deploy_objects=("mig.run",), namespace="mig")
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "same", [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["closed"] is True

    ws, cwd = _workspace(tmp_path / "undeployed", dependencies={"u": _analysis("MIG.T")})
    proc, calls = _run(cwd, tmp_path / "undeployed", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "['app.run']" in proc.stderr and "deploy_objects" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


def test_call_graph_writes_are_compared_as_the_mapping_specs_target_names(tmp_path):
    """The analysis names legacy tables; the manifest names deployed targets. A source write resolves
    through the unit's mapping object (root_table -> object) before the comparison, and a deployed
    procedure listed in deploy_objects is a write target no DML has to produce."""
    spec = {"objects": [{"object": "t", "root_table": "SRC.LEDGER", "key": ["id"]}]}
    ws, cwd = _workspace(tmp_path / "ok", dependencies={"u": _analysis("src.ledger")}, mappings={"u": spec},
                         namespace="mig", write_targets=("mig.t", "mig.run"), deploy_objects=("MIG.run",))
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "ok", [_pass_report(pr, write_targets=["mig.t", "mig.run"]), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["closed"] is True

    ws, cwd = _workspace(tmp_path / "raw", dependencies={"u": _analysis("src.ledger")}, mappings={"u": spec},
                         namespace="mig", write_targets=("src.ledger",))
    proc, calls = _run(cwd, tmp_path / "raw", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "missing from the declaration: ['mig.t']" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "unmapped", dependencies={"u": _analysis("src.ledger", "src.other")},
                         mappings={"u": spec}, namespace="mig", write_targets=("mig.t", "mig.other"))
    proc, calls = _run(cwd, tmp_path / "unmapped", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "'src.other'" in proc.stderr and "mapping_spec.json" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "proc", dependencies={"u": _analysis("mig.t")}, write_targets=("mig.t", "mig.run"))
    proc, calls = _run(cwd, tmp_path / "proc", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "extra in the declaration: ['mig.run']" in proc.stderr

    ws, cwd = _workspace(tmp_path / "outside", write_targets=("mig.t",), deploy_objects=("mig.run",))
    proc, calls = _run(cwd, tmp_path / "outside", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "deploy_objects" in proc.stderr and "write_targets" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


def test_child_reported_targets_compare_under_the_manifests_namespace_after_the_run(tmp_path):
    """The manifest declares `orders` under target_namespace `cat.mig`; a child reports the same table
    as `CAT.MIG.orders`. That is one declared target, not an undeclared write and not an overlap."""
    ws, cwd = _workspace(tmp_path / "ok", namespace="cat.mig", write_targets=("orders",))
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "ok", [_pass_report(pr, write_targets=["CAT.MIG.orders", "orders"]),
                                           _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert "outside their declared targets" not in proc.stdout and "overlapping" not in proc.stdout
    assert _result(ws)["closed"] is True

    ws, cwd = _workspace(tmp_path / "other", namespace="cat.mig", write_targets=("orders",))
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "other", [_pass_report(pr, write_targets=["cat.mig.orders", "cat.mig.other"]),
                                              _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert "outside their declared targets" in proc.stdout and "cat.mig.other" in proc.stdout
    assert "cat.mig.orders" not in proc.stdout.split("outside their declared targets")[1].splitlines()[0]
    assert "cat.mig.other" in (ws / ".migration/waves/wave-0.brief.md").read_text()


def test_read_only_batch_declares_no_targets_only_with_an_analysis_that_writes_nothing(tmp_path):
    ws, cwd = _workspace(tmp_path / "bare", write_targets=())
    proc, calls = _run(cwd, tmp_path / "bare", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "b-1" in proc.stderr and "write_targets" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "ro", write_targets=(), dependencies={"u": json.dumps({"routines": []})})
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path / "ro", [_pass_report(pr, write_targets=[]), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["closed"] is True

    ws, cwd = _workspace(tmp_path / "view", write_targets=(), dependencies={"u": _analysis()})
    proc, calls = _run(cwd, tmp_path / "view", [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "['app.run']" in proc.stderr and "deploy_objects" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


def test_malformed_dependency_analysis_halts_before_launch(tmp_path):
    ws, cwd = _workspace(tmp_path, dependencies={"u": '{"routines": [{"routine": "a"}]}'})
    proc, calls = _run(cwd, tmp_path, [_pass_report("https://github.com/acme/target/pull/1")])
    assert proc.returncode != 0 and "u/dependencies.json" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


_PR_HEADS = {}


def _push_pr(ws, n=1):
    head = subprocess.run(["git", "-C", str(ws), "rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", f"HEAD:refs/pull/{n}/head", "HEAD:recon/wave-0"], check=True)
    url = f"https://github.com/acme/target/pull/{n}"
    _PR_HEADS[url] = head
    return url


def _unproven_pr(ws, n=1):
    """A pushed PR whose head is not yet an ancestor of origin's base tip."""
    (ws / f"pr-{n}.sql").write_text("select 1")
    subprocess.run(["git", "-C", str(ws), "add", f"pr-{n}.sql"], check=True)
    subprocess.run(["git", "-C", str(ws), "commit", "-qm", "x"], check=True)
    head = subprocess.run(["git", "-C", str(ws), "rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", f"HEAD:refs/pull/{n}/head",
                    "HEAD~1:refs/heads/recon/wave-0"], check=True)
    url = f"https://github.com/acme/target/pull/{n}"
    _PR_HEADS[url] = head
    return url


def _squash_merge_to_base(ws):
    """What a host's squash-merge leaves: a new commit on the base tip carrying the PR head's tree."""
    git = ["git", "-C", str(ws)]
    tip = subprocess.run(git + ["rev-parse", "origin/migration/x"],
                         check=True, capture_output=True, text=True).stdout.strip()
    tree = subprocess.run(git + ["rev-parse", "HEAD^{tree}"],
                          check=True, capture_output=True, text=True).stdout.strip()
    merged = subprocess.run(git + ["commit-tree", tree, "-p", tip, "-m", "squash"],
                            check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", f"{merged}:refs/heads/migration/x"], check=True)


def _merge_commit(ws, head, second_parent=None):
    """The merge commit a host records: a commit on top of the base tip whose second (PR-side) parent
    is the PR head."""
    git = ["git", "-C", str(ws)]
    tip = subprocess.run(git + ["rev-parse", "origin/migration/x"],
                         check=True, capture_output=True, text=True).stdout.strip()
    tree = subprocess.run(git + ["rev-parse", f"{head}^{{tree}}"],
                          check=True, capture_output=True, text=True).stdout.strip()
    return subprocess.run(git + ["commit-tree", tree, "-p", tip, "-p", second_parent or head, "-m", "merge"],
                          check=True, capture_output=True, text=True).stdout.strip()


def _merge_row(url, mc, head=None):
    return {"pr_url": url, "merge_commit_sha": mc, "merged_head": head or _PR_HEADS[url]}


def _merged(ws, pr, **extra):
    """A close report that merged pr through a real merge commit pushed to origin/migration/x."""
    mc = _merge_commit(ws, _PR_HEADS[pr])
    close = _close_report(merged_prs=[_merge_row(pr, mc)], **extra)
    close["__run__"] = [["git", "-C", str(ws), "push", "-q", "origin",
                         f"{mc}:refs/heads/migration/x"]]
    return close


def _verify_report(**extra):
    return {"wave_verdict": "PASS", "unit_verdicts": {"b-1": "PASS"},
            "findings": [], "changed_paths": [], **extra}


def test_merge_eligible_false_is_recorded_as_merged_only_by_a_ledger_override(tmp_path):
    ledger = "| D-3 | user:U1 | merge_override for u, watermark gap accepted |\n"
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

    ws, cwd = _workspace(tmp_path / "no_row", decisions="| D-3 | user:U1 | widen tolerance for u |\n")
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

    ledger = "| D-3 | user:U1 | merge_override for u, u2 |\n"
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


def _migrate_limit(calls):
    register = [c for c in calls if c["kind"] == "register"][0]
    return next(p for p in register["meta"]["phases"] if p["title"] == "migrate")["soft_time_limit_minutes"]


def test_child_prompt_names_the_default_time_budget(tmp_path):
    ws, cwd = _workspace(tmp_path)
    proc, calls = _run(cwd, tmp_path, [_pass_report(), _verify_report()])
    assert proc.returncode == 0
    assert "Time budget: 45 minutes" in [c for c in calls if c.get("label") == "b-1"][0]["prompt"]
    assert _migrate_limit(calls) == 45


def test_batch_max_minutes_overrides_the_manifest_for_its_child(tmp_path):
    ws, cwd = _workspace(tmp_path / "sixty", max_minutes=60)
    proc, calls = _run(cwd, tmp_path / "sixty", [_pass_report(), _verify_report()])
    assert proc.returncode == 0
    assert "Time budget: 60 minutes" in [c for c in calls if c.get("label") == "b-1"][0]["prompt"]
    assert _migrate_limit(calls) == 60

    ws, cwd = _workspace(tmp_path / "thirty", max_minutes=60, batch_max_minutes=30)
    proc, calls = _run(cwd, tmp_path / "thirty", [_pass_report(), _verify_report()])
    assert proc.returncode == 0
    assert "Time budget: 30 minutes" in [c for c in calls if c.get("label") == "b-1"][0]["prompt"]
    assert _migrate_limit(calls) == 30


def test_migrate_agent_call_carries_the_batch_soft_time_limit(tmp_path):
    ws, cwd = _workspace(tmp_path / "default")
    proc, calls = _run(cwd, tmp_path / "default", [_pass_report()])
    assert proc.returncode == 0
    assert [c for c in calls if c.get("label") == "b-1"][0]["kwargs"]["soft_time_limit_minutes"] == 45

    ws, cwd = _workspace(tmp_path / "thirty", batch_max_minutes=30)
    proc, calls = _run(cwd, tmp_path / "thirty", [_pass_report()])
    assert proc.returncode == 0
    assert [c for c in calls if c.get("label") == "b-1"][0]["kwargs"]["soft_time_limit_minutes"] == 30


def test_pointer_above_the_cwd_names_the_workspace(tmp_path):
    ws, cwd = _workspace(tmp_path, pointer_at="home")
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode == 0
    assert [c["label"] for c in calls if c["kind"] == "agent"] == ["b-1"]


def test_pipeline_manifest_tags_the_verifier_branch_and_workflow(tmp_path):
    ws, cwd = _workspace(tmp_path, manifest_name="wave-p2-1.json", wave=1, pipelines={"p2": 1})
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", "HEAD:refs/pull/1/head",
                    "HEAD:recon/wave-p2-1"], check=True)
    _PR_HEADS["https://github.com/acme/target/pull/1"] = subprocess.run(
        ["git", "-C", str(ws), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    proc, calls = _run(cwd, tmp_path, [_pass_report("https://github.com/acme/target/pull/1"),
                                     _verify_report()])
    assert proc.returncode == 0, proc.stderr
    register = [c for c in calls if c["kind"] == "register"][0]
    assert register["meta"]["name"] == "migration-wave-p2-1"
    verify = [c for c in calls if c.get("label") == "verify-wave-p2-1"]
    assert verify and "recon/wave-p2-1" in verify[0]["prompt"]


@pytest.mark.parametrize("bad", ["batch-1.json", "wave-p2/1.json"])
def test_manifest_name_must_tag_a_wave_or_pipeline(tmp_path, bad):
    ws, cwd = _workspace(tmp_path, manifest_name=bad)
    proc, calls = _run(cwd, tmp_path, [_pass_report(), _verify_report()])
    assert proc.returncode != 0
    assert "must be named wave-" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


@pytest.mark.parametrize("name,wave", [("wave-2.json", 1), ("wave-payments-1.json", 2), ("wave-payments.json", 1)])
def test_the_name_wave_number_must_equal_the_manifest_wave(tmp_path, name, wave):
    ws, cwd = _workspace(tmp_path, manifest_name=name, wave=wave)
    proc, calls = _run(cwd, tmp_path, [_pass_report(), _verify_report()])
    assert proc.returncode != 0
    assert "the wave number in the file name" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]


def test_a_pipeline_manifest_must_name_every_sibling_in_pipelines(tmp_path):
    ws, cwd = _workspace(tmp_path, manifest_name="wave-orders-1.json", wave=1)
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode != 0 and "pipelines" in proc.stderr and "wave-<pipeline>-<N>" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "foreign", manifest_name="wave-orders-1.json", wave=1,
                         pipelines={"payments": 1, "ledger": 1})
    proc, calls = _run(cwd, tmp_path / "foreign", [_pass_report()])
    assert proc.returncode != 0 and "orders" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "over", manifest_name="wave-orders-3.json", wave=3,
                         pipelines={"orders": 2})
    proc, calls = _run(cwd, tmp_path / "over", [_pass_report()])
    assert proc.returncode != 0 and "orders" in proc.stderr
    assert not [c for c in calls if c["kind"] == "agent"]

    ws, cwd = _workspace(tmp_path / "plain", manifest_name="wave-1.json", wave=1)
    pr = _push_pr(ws)
    proc, calls = _run(cwd, tmp_path / "plain", [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr


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


def test_preflight_subcommand_runs_the_launch_checks_for_a_hand_launched_wave(tmp_path):
    def run(ws_dir, **kw):
        ws, cwd = _workspace(ws_dir, **kw)
        proc = subprocess.run([sys.executable, str(WORKFLOW), "preflight"], cwd=cwd, env={},
                              capture_output=True, text=True)
        assert not (ws / ".migration/waves/wave-0.result.json").exists()
        assert not (ws / ".migration/waves/wave-0.base_sha").exists()
        return proc

    proc = run(tmp_path / "ok", dependencies={"u": _analysis("MIG.T")}, write_targets=("mig.t", "mig.run"),
               deploy_objects=("mig.run",), namespace="mig")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == {"wave": 0, "ready": True, "batches": ["b-1"], "pipeline_order": {}}
    proc = run(tmp_path / "drift", dependencies={"u": _analysis("mig.t", "mig.audit")})
    assert proc.returncode != 0 and "b-1" in proc.stderr and "mig.audit" in proc.stderr
    proc = run(tmp_path / "shared", other_waves={"wave-1.json": WAVE_1}, mappings={"u": MAPPING})
    assert proc.returncode != 0 and "'mig.t'" in proc.stderr and "target_where" in proc.stderr
    proc = run(tmp_path / "nodoctor", doctor=False)
    assert proc.returncode != 0 and "doctor" in proc.stderr
    proc = run(tmp_path / "stopc", stop_c=False)
    assert proc.returncode != 0 and "gates_sha" in proc.stderr


def _close_report(**extra):
    return {"merged_prs": [], "unmerged": [], "changed_paths": [], **extra}


RESYNC = {"command": "python3 load/resync_identity.py --unit u --unit v", "units": ["u", "v"]}
SEQ = {"object": "mig.u.orders_id_seq", "before": 10, "after": 42}


def _resync_report(**extra):
    return {"status": "ok", "sequences": [SEQ], "changed_paths": [], "one_line_summary": "reseeded 1 sequence", **extra}


def _two_batch_workspace(tmp_path, **kw):
    return _workspace(tmp_path, recon={"u": True, "v": True}, manifest_extra={"resync": RESYNC},
                      other_batch={"id": "b-2", "units": ["v"], "write_targets": ["mig.u"],
                                   "brief": "b", "gates": [GATE], "lakeflow_pipelines": []}, **kw)


def _pass2(pr2):
    return _pass_report(pr2, write_targets=["mig.u"], gates=[{"id": "g-rows", "status": "passed",
                                                             "evidence": ".migration/recon/v/result.json"}])


def test_a_manifest_resync_runs_once_after_the_children_and_before_the_verifier(tmp_path):
    ws, cwd = _two_batch_workspace(tmp_path)
    pr, pr2 = _push_pr(ws), _push_pr(ws, 2)
    fail = {**_pass_report(pr), "status": "FAIL", "failure_class": "sequence_behind_source"}
    proc, calls = _run(cwd, tmp_path, [fail, _pass2(pr2), _resync_report(),
                                       {"wave_verdict": "PASS", "unit_verdicts": {"v": "PASS"}, "findings": [], "changed_paths": []}])
    assert proc.returncode == 0, proc.stderr
    labels = [c.get("label") for c in calls if c["kind"] == "agent"]
    assert labels == ["b-1", "b-2", "resync-wave-0", "verify-wave-0"]
    resync = [c for c in calls if c.get("label") == "resync-wave-0"][0]
    assert resync["kwargs"]["phase"] == "resync" and RESYNC["command"] in resync["prompt"]
    assert "migration/x" in resync["prompt"] and "setval" in resync["prompt"] and "before" in resync["prompt"]
    register = [c for c in calls if c["kind"] == "register"][0]
    assert [p["title"] for p in register["meta"]["phases"]] == ["migrate", "resync", "verify", "close"]
    result = _result(ws)
    assert result["resync"]["command"] == RESYNC["command"] and result["resync"]["units"] == ["u", "v"]
    assert result["resync"]["report"]["sequences"] == [SEQ] and result["resync"]["problems"] == []
    assert result["verify"]["unit_verdicts"] == {"b-2": "PASS"}
    brief = (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert "Identity resync" in brief and "mig.u.orders_id_seq" in brief and "10" in brief and "42" in brief
    assert f"Awaiting manual merge: {pr2}" in brief


def test_a_resync_that_wrote_files_or_died_is_a_recorded_problem_that_turns_auto_merge_off(tmp_path):
    verify = {"wave_verdict": "PASS", "unit_verdicts": {"b-1": "PASS", "b-2": "PASS"},
              "findings": [], "changed_paths": []}
    ws, cwd = _two_batch_workspace(tmp_path, auto_merge=True)
    pr, pr2 = _push_pr(ws), _push_pr(ws, 2)
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr), _pass2(pr2), _resync_report(changed_paths=["load/x.sql"]),
                                       dict(verify)])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert any("load/x.sql" in p for p in result["resync"]["problems"])
    assert result["verify"]["wave_verdict"] == "PASS"
    assert "load/x.sql" in (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert result["auto_merge"] is False
    assert not [c for c in calls if str(c.get("label", "")).startswith("close")]

    ws, cwd = _two_batch_workspace(tmp_path / "dead", auto_merge=True)
    pr, pr2 = _push_pr(ws), _push_pr(ws, 2)
    proc, calls = _run(cwd, tmp_path / "dead", [_pass_report(pr), _pass2(pr2), {"error": "boom"}, dict(verify)])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["resync"]["report"] is None and any("boom" in p for p in result["resync"]["problems"])
    assert "boom" in (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert result["auto_merge"] is False
    assert not [c for c in calls if str(c.get("label", "")).startswith("close")]

    ws, cwd = _two_batch_workspace(tmp_path / "failed", auto_merge=True)
    pr, pr2 = _push_pr(ws), _push_pr(ws, 2)
    proc, calls = _run(cwd, tmp_path / "failed", [_pass_report(pr), _pass2(pr2),
                                                  _resync_report(status="failed"), dict(verify)])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert any("resync command failed" in p for p in result["resync"]["problems"])
    assert "resync command failed" in (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert result["auto_merge"] is False
    assert not [c for c in calls if str(c.get("label", "")).startswith("close")]


def test_a_resume_after_a_resync_reruns_only_identity_failures_and_unreported_children(tmp_path):
    """Run 1: b-1 fails on sequence_behind_source, b-2 passes, resync reseeds. Run 2 (resume): b-1 is
    re-launched with the resync report in its prompt, b-2 replays its recorded PASS, the resync step and
    the verifier see the new state."""
    ws, cwd = _two_batch_workspace(tmp_path)
    pr, pr2 = _push_pr(ws), _push_pr(ws, 2)
    fail = {**_pass_report(pr), "status": "FAIL", "failure_class": "sequence_behind_source"}
    proc, calls = _run(cwd, tmp_path, [fail, _pass2(pr2), _resync_report(),
                                       {"wave_verdict": "PASS", "unit_verdicts": {"b-2": "PASS"}, "findings": [], "changed_paths": []}])
    assert proc.returncode == 0, proc.stderr
    first = {c["label"]: c["prompt"] for c in calls if c["kind"] == "agent"}
    result = _result(ws)
    assert result["batches"][0]["status"] == "FAIL" and result["batches"][1]["status"] == "PASS"

    pointer = ws / ".migration/waves/current.json"
    pointer.write_text(json.dumps({"manifest": "wave-0.json", "mode": "resume", "run_id": "wfr-1",
                                   "hook_probe": "blocked:0123abcd", "plugin": str(PLUGIN)}))
    (ws / ".migration/waves/wave-0.run_id").write_text("wfr-1\n")
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr), _pass2(pr2), _resync_report(),
                                       {"wave_verdict": "PASS", "unit_verdicts": {"b-1": "PASS", "b-2": "PASS"},
                                        "findings": [], "changed_paths": []}])
    assert proc.returncode == 0, proc.stderr
    second = {c["label"]: c["prompt"] for c in calls if c["kind"] == "agent"}
    assert second["b-2"] == first["b-2"], "an unaffected PASS keeps its prompt so the runtime replays it"
    assert second["b-1"] != first["b-1"] and "resync" in second["b-1"] and "mig.u.orders_id_seq" in second["b-1"]
    assert second["resync-wave-0"] == first["resync-wave-0"]
    result = _result(ws)
    assert [b["status"] for b in result["batches"]] == ["PASS", "PASS"] and result["resync"]["report"]["sequences"] == [SEQ]
    assert result["batches"][0]["prompt_sha"] != result["batches"][1]["prompt_sha"]


def test_the_wave_close_step_merges_the_verifier_pass_prs(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), _merged(ws, pr)])
    assert proc.returncode == 0, proc.stderr
    register = [c for c in calls if c["kind"] == "register"][0]
    close_phase = [p for p in register["meta"]["phases"] if p["title"] == "close"][0]
    assert close_phase["soft_time_limit_minutes"] == 10
    verify_prompt = [c for c in calls if c.get("label") == "verify-wave-0"][0]["prompt"]
    assert "Merge every PR" not in verify_prompt and "wave-close" in verify_prompt
    assert verify_prompt.count("commit it on branch recon/wave-0, push, and give") == 1
    close = [c for c in calls if c.get("label") == "close-wave-0"]
    assert close and close[0]["kwargs"]["soft_time_limit_minutes"] == 10 and pr in close[0]["prompt"]
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["close_minutes"] == 10
    assert result["closed"] is True


def test_close_minutes_from_the_manifest_propagates_and_a_bad_one_halts(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True, close_minutes=20)
    pr = _unproven_pr(ws)
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), _merged(ws, pr)])
    assert proc.returncode == 0, proc.stderr
    close = [c for c in calls if c.get("label") == "close-wave-0"][0]
    assert close["kwargs"]["soft_time_limit_minutes"] == 20 and "20 minutes" in close["prompt"]
    assert _result(ws)["close_minutes"] == 20
    for i, bad in enumerate((0, "10", True, 61)):
        ws, cwd = _workspace(tmp_path / f"bad{i}", auto_merge=True, close_minutes=bad)
        proc, calls = _run(cwd, tmp_path / f"bad{i}", [_pass_report()])
        assert proc.returncode != 0 and "close_minutes" in proc.stderr
        assert not [c for c in calls if c["kind"] == "agent"]


def test_hard_mode_runs_no_close_step_and_the_brief_lists_the_prs(tmp_path):
    ws, cwd = _workspace(tmp_path)
    pr = _push_pr(ws)
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr), _verify_report()])
    assert proc.returncode == 0, proc.stderr
    assert not [c for c in calls if c.get("label") == "close-wave-0"]
    brief = (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert f"Awaiting manual merge: {pr}" in brief
    assert _result(ws)["close"] is None


def test_a_failed_sibling_does_not_hold_back_a_verified_pr_merge(tmp_path):
    """The verifier sees only PASS children; a wave FAIL on one still leaves the other's PR to merge."""
    ws, cwd = _workspace(tmp_path, auto_merge=True, recon={"u": True, "v": True},
                         other_batch={"id": "b-2", "units": ["v"], "write_targets": ["mig.u"],
                                      "brief": "b", "gates": [GATE], "lakeflow_pipelines": []})
    pr, pr2 = _unproven_pr(ws), _push_pr(ws, 2)
    pass2 = _pass_report(pr2, write_targets=["mig.u"], gates=[{"id": "g-rows", "status": "passed",
                                                             "evidence": ".migration/recon/v/result.json"}])
    proc, calls = _run(cwd, tmp_path, [_pass_report(pr), pass2,
                                       {"wave_verdict": "FAIL", "unit_verdicts": {"b-1": "PASS", "b-2": "FAIL"},
                                        "findings": [], "changed_paths": []},
                                       _merged(ws, pr)])
    assert proc.returncode == 0, proc.stderr
    close = [c for c in calls if c.get("label") == "close-wave-0"]
    assert close and pr in close[0]["prompt"] and "b-2" not in close[0]["prompt"] and pr2 not in close[0]["prompt"]
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["closed"] is False


def test_wave_close_output_is_validated(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    close = _merged(ws, pr)
    close["merged_prs"].append({"pr_url": "https://github.com/acme/target/pull/9",
                                "merge_commit_sha": "f" * 40, "merged_head": "e" * 40})
    close["changed_paths"] = ["x.sql"]
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    result = _result(ws)
    assert result["closed"] is False
    findings = " ".join(result["verify"]["findings"])
    assert "wave close invalid" in findings and "outside the wave" in findings and "x.sql" in findings


def test_a_verified_pr_the_close_step_dropped_is_a_finding(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), _close_report()])
    result = _result(ws)
    assert result["closed"] is False
    assert any("wave close invalid" in f and pr in f for f in result["verify"]["findings"])


def test_an_unmerged_verified_pr_keeps_the_wave_open_and_lands_in_the_brief(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(),
                                     _close_report(unmerged=[{"pr_url": pr, "reason": "head moved"}])])
    result = _result(ws)
    assert result["closed"] is False
    brief = (ws / ".migration/waves/wave-0.brief.md").read_text()
    assert f"Not merged: {pr}" in brief and f"Awaiting manual merge: {pr}" in brief


def test_a_malformed_close_reply_still_writes_the_brief(tmp_path):
    for i, bad in enumerate(({"merged_prs": [], "unmerged": [{"pr_url": "x"}], "changed_paths": []},
                            {"merged_prs": [], "unmerged": "nope", "changed_paths": []})):
        ws, cwd = _workspace(tmp_path / f"bad{i}", auto_merge=True)
        pr = _unproven_pr(ws)
        proc, _ = _run(cwd, tmp_path / f"bad{i}", [_pass_report(pr), _verify_report(), bad])
        assert proc.returncode == 0, proc.stderr
        result = _result(ws)
        assert result["closed"] is False
        assert any("wave close invalid" in f for f in result["verify"]["findings"])
        brief = (ws / ".migration/waves/wave-0.brief.md").read_text()
        assert f"Awaiting manual merge: {pr}" in brief


def test_a_dead_close_session_leaves_every_verified_pr_unmerged(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), {"error": "boom"}])
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["close"]["unmerged"][0]["pr_url"] == pr
    assert result["closed"] is False


def test_the_close_reply_is_reconciled_against_git(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(),
                                 _close_report(merged_prs=[_merge_row(pr, "f" * 40)])])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    unmerged = result["close"]["unmerged"]
    assert unmerged[0]["pr_url"] == pr and "not on origin" in unmerged[0]["reason"]

    ws, cwd = _workspace(tmp_path / "proven", auto_merge=True)
    pr = _unproven_pr(ws)
    mc = _merge_commit(ws, _PR_HEADS[pr])
    proc, _ = _run(cwd, tmp_path / "proven",
                   [_pass_report(pr), _verify_report(),
                    {"error": "died mid-merge",
                     "__run__": [["git", "-C", str(ws), "push", "-q", "origin",
                                  f"{mc}:refs/heads/migration/x"]]}])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["close"]["unmerged"] == []
    assert result["closed"] is True


def test_a_pr_head_that_moved_after_gating_is_not_a_merge(tmp_path):
    """Resume replays the PASS gated at head A; the PR has since moved to B (B merged, A's verdict stands
    for nothing): the wave cannot close over a commit it never gated."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr),
                                 {"wave_verdict": "FAIL", "unit_verdicts": {"b-1": "FAIL"},
                                  "findings": [], "changed_paths": []}])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["closed"] is False and result["batches"][0]["status"] == "PASS"

    pointer = ws / ".migration/waves/current.json"
    pointer.write_text(json.dumps({"manifest": "wave-0.json", "mode": "resume", "run_id": "wfr-1",
                                   "hook_probe": "blocked:0123abcd", "plugin": str(PLUGIN)}))
    (ws / ".migration/waves/wave-0.run_id").write_text("wfr-1\n")
    subprocess.run(["git", "-C", str(ws), "commit", "-q", "--allow-empty", "-m", "b"], check=True)
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", "HEAD:refs/pull/1/head",
                    "HEAD:migration/x"], check=True)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(),
                                 _close_report(merged_prs=[_merge_row(pr, "b" * 40)])])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["batches"][0]["status"] == "PASS"
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    unmerged = result["close"]["unmerged"]
    assert unmerged[0]["pr_url"] == pr and "PR head moved" in unmerged[0]["reason"]


def test_a_close_reply_is_reconciled_against_git_per_pr(tmp_path):
    """The close step died after merging b-1's PR: git proves that one merged, b-2's stays unmerged."""
    ws, cwd = _workspace(tmp_path, auto_merge=True, recon={"u": True, "v": True},
                         other_batch={"id": "b-2", "units": ["v"], "write_targets": ["mig.u"],
                                      "brief": "b", "gates": [GATE], "lakeflow_pipelines": []})
    pr = _unproven_pr(ws)
    pr2 = _unproven_pr(ws, 2)
    pass2 = _pass_report(pr2, write_targets=["mig.u"], gates=[{"id": "g-rows", "status": "passed",
                                                             "evidence": ".migration/recon/v/result.json"}])
    mc = _merge_commit(ws, _PR_HEADS[pr])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), pass2,
                                   {"wave_verdict": "PASS", "unit_verdicts": {"b-1": "PASS", "b-2": "PASS"},
                                    "findings": [], "changed_paths": []},
                                   {"error": "died mid-merge",
                                    "__run__": [["git", "-C", str(ws), "push", "-q", "origin",
                                                 f"{mc}:refs/heads/migration/x"]]}])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr]
    assert result["close"]["unmerged"] == [{"pr_url": pr2, "reason": result["close"]["unmerged"][0]["reason"]}]
    assert "not recorded" in result["close"]["unmerged"][0]["reason"]
    assert result["closed"] is False


def test_a_merge_commit_whose_pr_side_parent_is_the_gated_head_is_proven(tmp_path):
    """The close step's `gh pr view` record plus a merge commit on origin's base that names the gated
    head as its PR-side parent proves the merge."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), _merged(ws, pr)])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["closed"] is True
    mc = result["close"]["merges"][0]["merge_commit_sha"]
    parents = subprocess.run(["git", "-C", str(ws), "rev-list", "--parents", "-n1", mc],
                             check=True, capture_output=True, text=True).stdout.split()
    assert parents[2] == _PR_HEADS[pr]


def test_a_merge_commit_on_the_base_proves_a_pr_the_close_step_never_recorded(tmp_path):
    """The close step reported no merge for the PR, but a merge commit on origin's base names the gated
    head as its PR-side parent: the merge is proven from git alone."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    mc = _merge_commit(ws, _PR_HEADS[pr])
    close = _close_report(unmerged=[{"pr_url": pr, "reason": "merge failed to confirm"}])
    close["__run__"] = [["git", "-C", str(ws), "push", "-q", "origin",
                         f"{mc}:refs/heads/migration/x"]]
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["closed"] is True


def test_a_merge_commit_whose_pr_side_parent_is_a_later_commit_is_not_proven(tmp_path):
    """B appended after gating A and a merge commit naming B as its PR-side parent merged something the
    wave never verified."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    subprocess.run(["git", "-C", str(ws), "commit", "-q", "--allow-empty", "-m", "b"], check=True)
    head_b = subprocess.run(["git", "-C", str(ws), "rev-parse", "HEAD"],
                            check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(["git", "-C", str(ws), "reset", "-q", "--hard", "HEAD~1"], check=True)
    mc = _merge_commit(ws, _PR_HEADS[pr], second_parent=head_b)
    close = _close_report(merged_prs=[_merge_row(pr, mc)])
    close["__run__"] = [["git", "-C", str(ws), "push", "-q", "origin",
                         f"{mc}:refs/heads/migration/x"]]
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    unmerged = result["close"]["unmerged"]
    assert unmerged[0]["pr_url"] == pr and "PR-side parent" in unmerged[0]["reason"]


def test_a_squash_merge_is_proven_only_from_the_close_steps_record(tmp_path):
    """A squash/rebase merge leaves a single-parent commit: git alone cannot tie it to the PR, so only
    the close step's record (merged_head == the gated head) proves it."""
    for i, merged in enumerate((True, False)):
        ws, cwd = _workspace(tmp_path / f"sq{i}", auto_merge=True)
        (ws / "x.sql").write_text("select 1")
        subprocess.run(["git", "-C", str(ws), "add", "x.sql"], check=True)
        subprocess.run(["git", "-C", str(ws), "commit", "-qm", "x"], check=True)
        pr = _push_pr(ws)
        _squash_merge_to_base(ws)
        mc = subprocess.run(["git", "-C", str(ws), "rev-parse", "origin/migration/x"],
                            check=True, capture_output=True, text=True).stdout.strip()
        close = (_close_report(merged_prs=[_merge_row(pr, mc)]) if merged else
                 _close_report(unmerged=[{"pr_url": pr, "reason": "squash merged, record lost"}]))
        proc, _ = _run(cwd, tmp_path / f"sq{i}", [_pass_report(pr), _verify_report(), close])
        assert proc.returncode == 0, proc.stderr
        result = _result(ws)
        if merged:
            assert result["close"]["merged_prs"] == [pr] and result["closed"] is True
        else:
            assert result["close"]["merged_prs"] == [] and result["closed"] is False
            assert "not recorded" in result["close"]["unmerged"][0]["reason"]


def test_a_single_parent_record_must_carry_the_gated_heads_change(tmp_path):
    """A record naming some other single-parent commit on the base as the squash of the gated head is
    not proof: the commit's change against its parent must be the PR's change against its merge base."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    (ws / "x.sql").write_text("select 1")
    subprocess.run(["git", "-C", str(ws), "add", "x.sql"], check=True)
    subprocess.run(["git", "-C", str(ws), "commit", "-qm", "x"], check=True)
    pr = _push_pr(ws)
    git = ["git", "-C", str(ws)]
    tip = subprocess.run(git + ["rev-parse", "origin/migration/x"],
                         check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["checkout", "-q", tip], check=True)
    (ws / "y.sql").write_text("select 2")
    subprocess.run(git + ["add", "y.sql"], check=True)
    subprocess.run(git + ["commit", "-qm", "unrelated"], check=True)
    other = subprocess.run(git + ["rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", f"{other}:refs/heads/migration/x"], check=True)
    subprocess.run(git + ["checkout", "-q", _PR_HEADS[pr]], check=True)
    close = _close_report(merged_prs=[_merge_row(pr, other)])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "does not carry the gated head's change" in result["close"]["unmerged"][0]["reason"]


def _three_commit_pr(ws):
    """A pushed PR of three commits (a.sql, b.sql, c.sql) still off the base; returns (url, A, head)."""
    git = ["git", "-C", str(ws)]
    for name, text in (("a.sql", "select 1"), ("b.sql", "select 2"), ("c.sql", "select 3")):
        (ws / name).write_text(text)
        subprocess.run(git + ["add", name], check=True)
        subprocess.run(git + ["commit", "-qm", name], check=True)
        if name == "a.sql":
            sha_a = subprocess.run(git + ["rev-parse", "HEAD"],
                                   check=True, capture_output=True, text=True).stdout.strip()
    head = subprocess.run(git + ["rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", "HEAD:refs/pull/1/head", "HEAD:recon/wave-0"],
                   check=True)
    url = "https://github.com/acme/target/pull/1"
    _PR_HEADS[url] = head
    return url, sha_a, head


def _advance_base(ws, commits):
    """Move origin/migration/x past the PR's fork point with unrelated commits; returns the new tip."""
    git = ["git", "-C", str(ws)]
    subprocess.run(git + ["reset", "-q", "--hard", "origin/migration/x"], check=True)
    for name, text in commits:
        (ws / name).write_text(text)
        subprocess.run(git + ["add", name], check=True)
        subprocess.run(git + ["commit", "-qm", name], check=True)
    tip = subprocess.run(git + ["rev-parse", "HEAD"],
                         check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
    return tip


def test_a_rebase_merge_of_several_commits_is_proven_by_the_rebased_range(tmp_path):
    """A host's rebase merge lands the PR's commits one by one on the base and records the last as
    mergeCommit: the N commits ending at the record must carry the gated head's whole change."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    git = ["git", "-C", str(ws)]
    fork = subprocess.run(git + ["rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True).stdout.strip()
    pr, sha_a, head = _three_commit_pr(ws)
    _advance_base(ws, [("u.sql", "select 0")])
    subprocess.run(git + ["cherry-pick", f"{fork}..{head}"], check=True, capture_output=True)
    mc = subprocess.run(git + ["rev-parse", "HEAD"],
                        check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
    close = _close_report(merged_prs=[_merge_row(pr, mc)])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["closed"] is True


def test_a_rebase_range_that_is_not_the_prs_change_is_not_proof(tmp_path):
    """The last N first-parent commits ending at the record carry a different change than the gated
    head's, so the record proves nothing."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    git = ["git", "-C", str(ws)]
    pr, sha_a, head = _three_commit_pr(ws)
    _advance_base(ws, [("u.sql", "select 0"), ("v.sql", "select -1")])
    subprocess.run(git + ["cherry-pick", sha_a], check=True, capture_output=True)
    (ws / "d.sql").write_text("select 9")
    subprocess.run(git + ["add", "d.sql"], check=True)
    subprocess.run(git + ["commit", "-qm", "d"], check=True)
    mc = subprocess.run(git + ["rev-parse", "HEAD"],
                        check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
    close = _close_report(merged_prs=[_merge_row(pr, mc)])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "does not carry the gated head's change" in result["close"]["unmerged"][0]["reason"]


def test_a_multi_commit_squash_is_still_proven(tmp_path):
    """A squash of a multi-commit PR is one commit carrying the head's whole tree; the single-parent
    check compares its full change, not the head's last commit."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr, sha_a, head = _three_commit_pr(ws)
    _squash_merge_to_base(ws)
    mc = subprocess.run(["git", "-C", str(ws), "rev-parse", "origin/migration/x"],
                        check=True, capture_output=True, text=True).stdout.strip()
    close = _close_report(merged_prs=[_merge_row(pr, mc)])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["closed"] is True


def _commit_file(ws, name, data, msg=None):
    git = ["git", "-C", str(ws)]
    (ws / name).write_bytes(data if isinstance(data, bytes) else data.encode())
    subprocess.run(git + ["add", name], check=True)
    subprocess.run(git + ["commit", "-qm", msg or name], check=True)


def _tip(ws):
    return subprocess.run(["git", "-C", str(ws), "rev-parse", "origin/migration/x"],
                          check=True, capture_output=True, text=True).stdout.strip()


def test_a_squash_that_differs_only_by_whitespace_is_not_proof(tmp_path):
    """patch-id ignores whitespace; the landed commit's tree must be the gated head's exact change
    applied to what precedes it, so a squash adding one space is no merge of this PR."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    git = ["git", "-C", str(ws)]
    _commit_file(ws, "q.sql", "SELECT a\n")
    subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
    _commit_file(ws, "q.sql", "SELECT a, b\n")
    pr = _push_pr(ws)
    subprocess.run(git + ["reset", "-q", "--hard", "origin/migration/x"], check=True)
    _commit_file(ws, "q.sql", "SELECT  a, b\n")
    subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
    close = _close_report(merged_prs=[_merge_row(pr, _tip(ws))])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "does not carry the gated head's change" in result["close"]["unmerged"][0]["reason"]


def test_a_rebase_range_that_differs_only_by_whitespace_is_not_proof(tmp_path):
    """The rebased range must carry the gated head's change exactly; a last commit landing 'select  3'
    where the head has 'select 3' is not the PR's merge."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    git = ["git", "-C", str(ws)]
    fork = subprocess.run(git + ["rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True).stdout.strip()
    pr, sha_a, head = _three_commit_pr(ws)
    _advance_base(ws, [("u.sql", "select 0")])
    subprocess.run(git + ["cherry-pick", f"{fork}..{head}~1"], check=True, capture_output=True)
    _commit_file(ws, "c.sql", "select  3\n")
    mc = subprocess.run(git + ["rev-parse", "HEAD"],
                        check=True, capture_output=True, text=True).stdout.strip()
    subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
    close = _close_report(merged_prs=[_merge_row(pr, mc)])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "does not carry the gated head's change" in result["close"]["unmerged"][0]["reason"]


def test_a_binary_change_is_proven_exactly(tmp_path):
    """A squash landing the gated head's binary bytes proves; the same commit shape landing different
    bytes does not."""
    for i, landed in enumerate((b"\x00\x02", b"\x00\x03")):
        ws, cwd = _workspace(tmp_path / f"bin{i}", auto_merge=True)
        git = ["git", "-C", str(ws)]
        _commit_file(ws, "m.bin", b"\x00\x01")
        subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
        _commit_file(ws, "m.bin", b"\x00\x02")
        pr = _push_pr(ws)
        subprocess.run(git + ["reset", "-q", "--hard", "origin/migration/x"], check=True)
        # a same-bytes/same-message commit would dedupe to the PR head's sha; the landed commit must differ
        _commit_file(ws, "m.bin", landed, msg="squash merge")
        subprocess.run(git + ["push", "-q", "origin", "HEAD:migration/x"], check=True)
        close = _close_report(merged_prs=[_merge_row(pr, _tip(ws))])
        proc, _ = _run(cwd, tmp_path / f"bin{i}", [_pass_report(pr), _verify_report(), close])
        assert proc.returncode == 0, proc.stderr
        result = _result(ws)
        if landed == b"\x00\x02":
            assert result["close"]["merged_prs"] == [pr] and result["closed"] is True
        else:
            assert result["close"]["merged_prs"] == [] and result["closed"] is False
            assert "does not carry" in result["close"]["unmerged"][0]["reason"]


def test_a_record_whose_merged_head_is_not_the_gated_head_is_not_proven(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    close = _close_report(merged_prs=[_merge_row(pr, _merge_commit(ws, _PR_HEADS[pr]), head="e" * 40)])
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "is not the gated head" in result["close"]["unmerged"][0]["reason"]


def test_a_record_whose_merge_commit_is_not_on_the_base_is_not_proven(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    mc = _merge_commit(ws, _PR_HEADS[pr])  # never pushed to origin/migration/x
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(),
                                 _close_report(merged_prs=[_merge_row(pr, mc)])])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "is not on origin/migration/x" in result["close"]["unmerged"][0]["reason"]


def test_a_moved_pr_head_is_not_proven_even_with_a_record(tmp_path):
    """Resume replays the PASS gated at head A; the PR head has since moved to B, so even a well-formed
    merge record for A cannot prove the merge."""
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _unproven_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr),
                                 {"wave_verdict": "FAIL", "unit_verdicts": {"b-1": "FAIL"},
                                  "findings": [], "changed_paths": []}])
    assert proc.returncode == 0, proc.stderr
    assert _result(ws)["closed"] is False

    subprocess.run(["git", "-C", str(ws), "commit", "-q", "--allow-empty", "-m", "b"], check=True)
    subprocess.run(["git", "-C", str(ws), "push", "-q", "origin", "HEAD:refs/pull/1/head",
                    "HEAD:migration/x"], check=True)
    mc = _merge_commit(ws, _PR_HEADS[pr])
    pointer = ws / ".migration/waves/current.json"
    pointer.write_text(json.dumps({"manifest": "wave-0.json", "mode": "resume", "run_id": "wfr-1",
                                   "hook_probe": "blocked:0123abcd", "plugin": str(PLUGIN)}))
    (ws / ".migration/waves/wave-0.run_id").write_text("wfr-1\n")
    close = _close_report(merged_prs=[_merge_row(pr, mc)])
    close["__run__"] = [["git", "-C", str(ws), "push", "-q", "origin",
                         f"{mc}:refs/heads/migration/x"]]
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(), close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [] and result["closed"] is False
    assert "PR head moved" in result["close"]["unmerged"][0]["reason"]


def test_proven_merges_are_recorded_before_the_result_and_reused_on_resume(tmp_path):
    """The run writes .migration/waves/wave-0.merges.json with the proven {pr_url: merge_commit_sha}; a
    resume whose close step dies with no record still proves the PR from that file."""
    ws, cwd = _workspace(tmp_path, auto_merge=True, recon={"u": True, "v": True},
                         other_batch={"id": "b-2", "units": ["v"], "write_targets": ["mig.u"],
                                      "brief": "b", "gates": [GATE], "lakeflow_pipelines": []})
    pr = _unproven_pr(ws)
    pr2 = _unproven_pr(ws, 2)
    pass2 = _pass_report(pr2, write_targets=["mig.u"], gates=[{"id": "g-rows", "status": "passed",
                                                             "evidence": ".migration/recon/v/result.json"}])
    verify = {"wave_verdict": "PASS", "unit_verdicts": {"b-1": "PASS", "b-2": "PASS"},
              "findings": [], "changed_paths": []}
    mc = _merge_commit(ws, _PR_HEADS[pr])
    close = _close_report(merged_prs=[_merge_row(pr, mc)],
                          unmerged=[{"pr_url": pr2, "reason": "not mergeable"}])
    close["__run__"] = [["git", "-C", str(ws), "push", "-q", "origin",
                         f"{mc}:refs/heads/migration/x"]]
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), pass2, verify, close])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr] and result["closed"] is False
    merges = ws / ".migration/waves/wave-0.merges.json"
    assert json.loads(merges.read_text()) == {pr: mc}

    pointer = ws / ".migration/waves/current.json"
    pointer.write_text(json.dumps({"manifest": "wave-0.json", "mode": "resume", "run_id": "wfr-1",
                                   "hook_probe": "blocked:0123abcd", "plugin": str(PLUGIN)}))
    (ws / ".migration/waves/wave-0.run_id").write_text("wfr-1\n")
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), pass2, verify, {"error": "boom"}])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["close"]["merged_prs"] == [pr]
    assert result["close"]["merges"] == [{"merge_commit_sha": mc, "pr_url": pr}]
    assert result["close"]["unmerged"][0]["pr_url"] == pr2


def test_validate_close_rejects_bare_url_rows_in_merged_prs(tmp_path):
    ws, cwd = _workspace(tmp_path, auto_merge=True)
    pr = _push_pr(ws)
    proc, _ = _run(cwd, tmp_path, [_pass_report(pr), _verify_report(),
                                 _close_report(merged_prs=[pr])])
    assert proc.returncode == 0, proc.stderr
    result = _result(ws)
    assert result["closed"] is False
    findings = " ".join(result["verify"]["findings"])
    assert "wave close invalid" in findings and "merged_prs rows" in findings


def test_the_playbooks_produce_the_pipeline_check_inputs_the_workflow_needs():
    """The launch halts on what the planner and orchestrator omit, so the two playbooks that write the
    manifest and the pointer must name `lakeflow_pipelines`/`serialized_pipelines` and `plugin`; and a
    manifest shaped as 4-migration_plan.md says (every batch declares, `[]` when it updates none) is clean."""
    playbooks = PLUGIN / "skills" / "install-dbx-factory" / "playbooks"
    plan = (playbooks / "4-migration_plan.md").read_text()
    assert "`lakeflow_pipelines`" in plan and "`serialized_pipelines`" in plan
    orchestrator = (playbooks / "9-orchestrator.md").read_text()
    assert "`plugin`" in orchestrator and "current.json" in orchestrator


def test_a_pointer_with_the_plugin_root_and_declared_pipelines_launches(tmp_path):
    ws, cwd = _workspace(tmp_path, recon={"u": True, "v": True}, manifest_name="wave-1.json", wave=1, width=2,
                         lakeflow_pipelines=("p",), extra_batches=[_second_batch([])])
    proc, calls = _run(cwd, tmp_path, [_pass_report(""), _pass_report("")])
    assert proc.returncode == 0, proc.stderr
    assert sorted(c["label"] for c in calls if c["kind"] == "agent") == ["b-1", "b-2"]
    assert json.loads((ws / ".migration/waves/wave-1.result.json").read_text())["pipeline_order"] == {}
