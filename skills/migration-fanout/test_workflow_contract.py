import json
import subprocess
import sys
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")
DOCTOR = Path(__file__).parents[1] / "factory-doctor" / "doctor.py"


def _workspace(tmp_path, *, mode="start", run_id=None, doctor=True, tamper=None,
               pointer_at=None, smoke=False):
    ws = tmp_path / "ws"
    waves = ws / ".migration" / "waves"
    waves.mkdir(parents=True)
    manifest = {
        "wave": 0,
        "width": 1,
        "repo": "github.com/acme/target",
        "child_macro": "child",
        "verify_macro": "verify",
        "base_branch": "migration/x",
        "capabilities": {
            "identity": "sp-1",
            "host": "https://adb-1.azuredatabricks.net",
            "catalogs": ["mig"],
            "guard_mode": "block",
            "stop_mode": "soft",
            "ready": True,
        },
        "batches": [{"id": "b-1", "units": ["u"], "write_targets": ["mig.t"], "brief": "brief"}],
    }
    if smoke:
        manifest["smoke"] = True
    manifest_path = waves / "wave-0.json"
    manifest_path.write_text(json.dumps(manifest))
    manifest_bytes = manifest_path.read_bytes()
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
                {"id": "stop_mode", "status": "ok", "data": {"stop_mode": "soft"}},
            ],
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
               "hook_probe": "blocked:0123abcd"}
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
    return reports.pop(0)
def log(message):
    print(message)
"""
    script = copy / "script.py"
    script.write_text(shim + "\n" + WORKFLOW.read_text())
    proc = subprocess.run([sys.executable, str(script)], cwd=cwd, env={},
                          capture_output=True, text=True)
    calls = json.loads(calls.read_text()) if calls.exists() else []
    return proc, calls


def _pass_report(pr_url=""):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live",
            "pr_url": pr_url, "branch": "feature/x", "changed_paths": [],
            "write_targets": ["mig.t"], "one_line_summary": "ok"}


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
    ws2, _ = _workspace(tmp_path / "second", run_id="wfr-x")
    assert not (ws2 / ".migration/waves/wave-0.run_id").exists()


def test_pointer_above_the_cwd_names_the_workspace(tmp_path):
    ws, cwd = _workspace(tmp_path, pointer_at="home")
    proc, calls = _run(cwd, tmp_path, [_pass_report()])
    assert proc.returncode == 0
    assert [c["label"] for c in calls if c["kind"] == "agent"] == ["b-1"]


@pytest.mark.parametrize("tamper", ["missing", "not_ready", "wrong_sha", "stale", "signature"])
def test_invalid_doctor_record_launches_nothing(tmp_path, tamper):
    ws, cwd = _workspace(tmp_path, tamper=tamper)
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
