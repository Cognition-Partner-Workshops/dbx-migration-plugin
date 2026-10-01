"""PreToolUse file-edit event probes for the guard's write scope."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

GUARD = Path(__file__).resolve().parents[1] / "dbx_guard.py"
ALLOWLIST = {
    "catalogs": ["mig_cat"],
    "legacy_sources": ["tdprod.corp"],
    "guard_mode": "block",
    "target_hosts": ["lakebase-host"],
    "bundle_targets": ["migration"],
}


AUTHORIZATIONS = json.dumps({"version": 1, "authorizations": [
    {"id": "D-7", "kind": "tolerance_change", "objects": ["dbo.orders"], "by": "user:t", "at": "2026-01-01"},
]})


def _make_ws(tmp_path: Path) -> Path:
    ws = tmp_path / "edit_ws"
    (ws / ".migration" / "recon" / "u1").mkdir(parents=True)
    (ws / ".migration" / "waves").mkdir()
    (ws / ".migration").joinpath("allowed_targets.json").write_text(json.dumps(ALLOWLIST))
    (ws / ".migration" / "authorizations.json").write_text(AUTHORIZATIONS)
    (ws / "src").mkdir()
    (ws / "src" / "etl.py").write_text("print('ok')\n")
    return ws


def _make_nested_ws(tmp_path: Path) -> Path:
    ws = tmp_path / "project"
    sub = ws / "sub"
    (sub / ".migration" / "recon" / "u1").mkdir(parents=True)
    (sub / ".migration" / "waves").mkdir()
    (sub / ".migration" / "allowed_targets.json").write_text(json.dumps(ALLOWLIST))
    (sub / ".migration" / "authorizations.json").write_text(AUTHORIZATIONS)
    return ws


def run_hook(tool: str, tool_input: dict, ws: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    event = {"tool_name": tool, "tool_input": tool_input, "cwd": str(ws)}
    hook_env = {"PATH": "/usr/bin:/bin", "CLAUDE_PROJECT_DIR": str(ws)}
    hook_env.update(env or {})
    return subprocess.run([sys.executable, str(GUARD)], input=json.dumps(event), text=True, capture_output=True,
                          check=False, cwd=ws, env=hook_env)


def decide(tool: str, tool_input: dict, ws: Path, env: dict[str, str] | None = None) -> str:
    result = run_hook(tool, tool_input, ws, env)
    return "block" if result.returncode == 2 else "approve"


ADD_LWA = '{"id": "D-8", "kind": "legacy_write_authorized", "objects": ["dbo.orders"], "by": "user:t", "at": "x"}'


@pytest.mark.parametrize("tool,path,tool_input,expected", [
    ("write", ".migration/allowed_targets.json", {"content": "{}"}, "approve"),
    ("edit", ".migration/units/u1/mapping_spec.json", {"old_string": "a", "new_string": "b"}, "approve"),
    ("MultiEdit", ".migration/waves/wave-1.json", {"edits": [{"old_string": "a", "new_string": "b"}]}, "approve"),
    ("write", ".migration/recon/u1/result.json", {"content": "{}"}, "approve"),
    ("write", ".migration/waves/wave-1.json", {"content": "{}"}, "approve"),
    ("edit", "src/etl.py", {"old_string": "ok", "new_string": "better"}, "approve"),
    # benign edits to the authorization file pass like any other .migration/ file
    ("write", ".migration/authorizations.json", {"content": "{}"}, "approve"),
    ("edit", ".migration/authorizations.json", {"old_string": "D-7", "new_string": "D-9"}, "approve"),
    ("write", ".migration/authorizations.json", {
        "content": '{"version": 1, "authorizations": [{"id": "D-8", "kind": "tolerance_change", "objects": ["dbo.orders"], "by": "user:t", "at": "x"}]}',
    }, "approve"),
    # a legacy_write_authorized entry enters only through a reviewed PR, never from a session
    ("edit", ".migration/authorizations.json", {
        "old_string": '"kind": "tolerance_change"', "new_string": '"kind": "legacy_write_authorized"',
    }, "block"),
    ("edit", ".migration/authorizations.json", {
        "old_string": ']\n}', "new_string": ', ' + ADD_LWA + ']\n}',
    }, "block"),
    ("edit", ".migration/authorizations.json", {
        "old_string": '"kind": "tolerance_change"', "new_string": '"kind": "gate_waived"',
    }, "block"),
    ("edit", ".migration/authorizations.json", {
        "old_string": '"kind": "tolerance_change"', "new_string": '"kind": "merge_override"',
    }, "block"),
    ("edit", ".migration/authorizations.json", {
        "old_string": '"tolerance_change"', "new_string": '"Legacy_Write_Authorized"',
    }, "block"),
    ("write", ".migration/authorizations.json", {
        "content": '{"version": 1, "authorizations": [' + ADD_LWA + ']}',
    }, "block"),
    ("MultiEdit", ".migration/authorizations.json", {
        "edits": [{"old_string": '"tolerance_change"', "new_string": '"legacy_write_authorized"'}],
    }, "block"),
])
def test_edit_event(tool, path, tool_input, expected, tmp_path):
    ws = _make_ws(tmp_path)
    assert decide(tool, {**tool_input, "file_path": str(ws / path)}, ws) == expected


def test_edit_identity_store_blocks(tmp_path):
    ws = _make_ws(tmp_path)
    assert decide("write", {"file_path": str(ws / ".databrickscfg"), "content": "token"}, ws, {"HOME": str(ws)}) == "block"


def test_edit_event_without_command_or_path_passes(tmp_path):
    ws = _make_ws(tmp_path)
    assert decide("edit", {}, ws) == "approve"


def test_edit_outside_workspace_passes(tmp_path):
    ws = tmp_path / "outside"
    ws.mkdir()
    assert decide("write", {"file_path": str(ws / "notes.md"), "content": "x"}, ws) == "approve"


def test_nested_workspace_edit_falls_back_to_file_config(tmp_path):
    ws = _make_nested_ws(tmp_path)
    auth = ws / "sub" / ".migration" / "authorizations.json"
    allowlist = ws / "sub" / ".migration" / "allowed_targets.json"
    added = run_hook("edit", {
        "file_path": str(auth),
        "old_string": '"version": 1',
        "new_string": '"version": 2',
    }, ws, {"CLAUDE_PROJECT_DIR": str(ws)})
    edited = run_hook("write", {"file_path": str(allowlist), "content": "{}"}, ws, {"CLAUDE_PROJECT_DIR": str(ws)})
    assert added.returncode == 0
    assert edited.returncode == 0   # .migration/ is writable; the allowlist in force is upstream


def test_hooks_json_has_exec_then_edit_matcher():
    hooks = json.loads((GUARD.parents[1] / "hooks.json").read_text())
    entries = hooks["PreToolUse"]
    assert len(entries) == 2
    assert entries[0]["matcher"] == "exec"
    assert entries[1]["matcher"] == "^(edit|write|MultiEdit)$"
    import re
    pattern = entries[1]["matcher"]
    assert all(re.fullmatch(pattern, value) for value in ("edit", "write", "MultiEdit"))
    assert not any(re.fullmatch(pattern, value) for value in ("exec", "read"))
