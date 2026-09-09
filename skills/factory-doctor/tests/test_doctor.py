import json
import subprocess
import sys
from pathlib import Path

import pytest

SKILL = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = SKILL.parents[1]
sys.path.insert(0, str(SKILL))
sys.path.insert(0, str(PLUGIN_ROOT / "hooks"))

import doctor  # noqa: E402
import dbx_guard  # noqa: E402


def make_workspace(tmp_path: Path, *, allowed=None, stop_mode="hard", omit=()):
    mig = tmp_path / ".migration"
    mig.mkdir()
    for f in doctor.REQUIRED_FILES:
        if f in omit:
            continue
        if f == "allowed_targets.json":
            mig.joinpath(f).write_text(json.dumps(allowed if allowed is not None else
                                                  {"catalogs": ["mig_cat"], "legacy_sources": ["LEGACY_DSN"]}))
        elif f == "00_context.md":
            mig.joinpath(f).write_text(f"# context\n\nstop_mode: {stop_mode}\n")
        else:
            mig.joinpath(f).write_text(f"# {f}\n")
    return tmp_path


def by_id(report):
    return {c["id"]: c for c in report["checks"]}


def test_probe_command_is_blocked_by_guard_and_harmless_otherwise():
    cfg = dbx_guard.GuardConfig.from_dict({"catalogs": ["mig_cat"]})
    v = dbx_guard.evaluate(doctor.HOOK_PROBE_COMMAND, cfg)
    assert v.decision == "block" and "__dbx_guard_probe__" in v.reason
    assert doctor.HOOK_PROBE_COMMAND.startswith("echo ")


def test_ready_offline_with_probe_blocked(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, no_databricks=True)
    c = by_id(report)
    assert report["ready"], [x for x in report["checks"] if x["status"] == "fail"]
    assert c["workspace"]["status"] == "ok"
    assert c["stop_mode"]["data"]["stop_mode"] == "hard"
    assert c["allowed_targets"]["status"] == "ok" and c["allowed_targets"]["data"]["catalogs"] == ["mig_cat"]
    assert c["hooks_files"]["status"] == "ok"
    assert c["hook_guard_functional"]["status"] == "ok"
    assert c["hook_platform_loaded"]["status"] == "ok"
    assert c["recon_harness"]["status"] == "ok"
    assert c["databricks_identity"]["status"] == "skipped"


def test_unknown_probe_is_unverified_and_carries_command(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "child", "unknown", None, True)
    c = by_id(report)
    assert c["hook_platform_loaded"]["status"] == "unverified"
    assert c["hook_platform_loaded"]["data"]["probe_command"] == doctor.HOOK_PROBE_COMMAND
    assert not report["ready"] and report["blocking"] == ["hook_platform_loaded=unverified"]


def test_human_identity_is_not_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "warn", "pat (env)"),
        doctor.Check("databricks_identity", "warn", "authenticated as someone@example.com (user)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, no_databricks=False)
    assert report["summary"].get("fail", 0) == 0
    assert not report["ready"] and report["blocking"] == ["databricks_identity=warn"]


def test_service_principal_with_advisory_warns_is_ready(tmp_path, monkeypatch):
    ws = make_workspace(tmp_path)
    monkeypatch.setattr(doctor, "check_databricks", lambda expect: [
        doctor.Check("databricks_cli", "ok", "v0.2"),
        doctor.Check("databricks_auth_kind", "ok", "oauth-m2m (env)"),
        doctor.Check("databricks_identity", "ok", "authenticated as 1234-sp (service principal)"),
        doctor.Check("databricks_warehouse", "warn", "none"),
    ])
    report = doctor.run(ws, PLUGIN_ROOT, "child", "blocked", None, no_databricks=False)
    assert report["ready"] and report["blocking"] == []


def test_driver_probe_requires_full_module(monkeypatch):
    import importlib.util
    real = importlib.util.find_spec

    def fake(name):
        if name == "databricks.sql":
            raise ModuleNotFoundError("No module named 'databricks.sql'")
        if name == "databricks":
            return real("json")
        return real(name)

    monkeypatch.setattr(importlib.util, "find_spec", fake)
    c = doctor.check_drivers()
    assert c.data["drivers"]["databricks"] is False and c.status == "warn"
    assert "databricks-sql-connector missing" in c.detail


def test_not_blocked_probe_fails(tmp_path):
    ws = make_workspace(tmp_path)
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "not-blocked", None, True)
    assert not report["ready"]
    assert by_id(report)["hook_platform_loaded"]["status"] == "fail"


def test_missing_files_and_stop_mode_fail(tmp_path):
    ws = make_workspace(tmp_path, omit=("05_progress.md",))
    (ws / ".migration" / "00_context.md").write_text("# no mode here\n")
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail" and "05_progress.md" in c["workspace"]["data"]["missing"]
    assert c["stop_mode"]["status"] == "fail"


def test_setup_outputs_glossary_and_tolerances_json_are_required(tmp_path):
    ws = make_workspace(tmp_path, omit=("02_glossary.md", "03_recon_tolerances.json"))
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["workspace"]["status"] == "fail"
    assert c["workspace"]["data"]["missing"] == ["02_glossary.md", "03_recon_tolerances.json"]


@pytest.mark.parametrize("who, expected", [
    ({"userName": "someone@example.com", "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"]}, False),
    ({"userName": "svc_migration", "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"]}, False),  # no '@' is not an SP
    ({"displayName": "Local Admin"}, False),
    ({"userName": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"}, True),  # application id as userName
    ({"userName": "svc", "applicationId": "8f3c2a1e-4b6d-4c2a-9e1f-0a1b2c3d4e5f"}, True),
    ({"displayName": "mig-sp", "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"]}, True),
])
def test_identity_classification_needs_positive_service_principal_evidence(who, expected):
    _, is_sp = doctor.classify_identity(who)
    assert is_sp is expected


def test_warn_mode_and_empty_legacy_sources_are_warned(tmp_path):
    ws = make_workspace(tmp_path, allowed={"catalogs": ["mig_cat"], "guard_mode": "warn"})
    c = by_id(doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True))
    assert c["allowed_targets"]["status"] == "warn"
    assert "legacy_sources empty" in c["allowed_targets"]["detail"]


def test_bad_allowlist_fails(tmp_path):
    ws = make_workspace(tmp_path, allowed={"catalogs": []})
    report = doctor.run(ws, PLUGIN_ROOT, "orchestrator", "blocked", None, True)
    assert by_id(report)["allowed_targets"]["status"] == "fail" and not report["ready"]


def test_cli_writes_capabilities_json_and_exit_codes(tmp_path):
    ws = make_workspace(tmp_path)
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    cap = json.loads((ws / ".migration" / "09_capabilities.json").read_text())
    assert cap["schema"] == "dbx-migration-factory/capabilities/1" and cap["ready"] is True
    assert "ready=True" in r.stdout

    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(ws),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks", "--hook-probe-result", "not-blocked"],
                       capture_output=True, text=True)
    assert r.returncode == 1


def test_no_workspace_does_not_write(tmp_path):
    r = subprocess.run([sys.executable, str(SKILL / "doctor.py"), "--workspace", str(tmp_path),
                        "--plugin-root", str(PLUGIN_ROOT), "--no-databricks"], capture_output=True, text=True)
    assert r.returncode == 1
    assert not (tmp_path / ".migration").exists()


@pytest.mark.parametrize("secretish,expected", [
    ("Error: token dapi1234567890abcdef1234567890abcdef expired", "<redacted>"),
    ("Error: cannot configure default credentials", "credentials"),
])
def test_redact(secretish, expected):
    assert expected in doctor._redact(secretish)
