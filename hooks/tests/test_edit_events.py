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


def _make_ws(tmp_path: Path) -> Path:
    ws = tmp_path / "edit_ws"
    (ws / ".migration" / "recon" / "u1").mkdir(parents=True)
    (ws / ".migration" / "waves").mkdir()
    (ws / ".migration").joinpath("allowed_targets.json").write_text(json.dumps(ALLOWLIST))
    (ws / ".migration" / "06_decisions.md").write_text("# Decisions\n")
    (ws / "src").mkdir()
    (ws / "src" / "etl.py").write_text("print('ok')\n")
    return ws


def _make_nested_ws(tmp_path: Path) -> Path:
    ws = tmp_path / "project"
    sub = ws / "sub"
    (sub / ".migration" / "recon" / "u1").mkdir(parents=True)
    (sub / ".migration" / "waves").mkdir()
    (sub / ".migration" / "allowed_targets.json").write_text(json.dumps(ALLOWLIST))
    (sub / ".migration" / "06_decisions.md").write_text("# Decisions\n")
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


@pytest.mark.parametrize("tool,path,tool_input,expected", [
    ("write", ".migration/allowed_targets.json", {"content": "{}"}, "block"),
    ("edit", ".migration/00_context.md", {"old_string": "a", "new_string": "b"}, "block"),
    ("MultiEdit", ".migration/05_progress.md", {"edits": [{"old_string": "a", "new_string": "b"}]}, "block"),
    ("write", ".migration/recon/u1/result.json", {"content": "{}"}, "approve"),
    ("write", ".migration/waves/wave-1.json", {"content": "{}"}, "approve"),
    ("edit", "src/etl.py", {"old_string": "ok", "new_string": "better"}, "approve"),
    ("edit", ".migration/06_decisions.md", {"old_string": "# Decisions\n", "new_string": "| D-7 | 2026-01-01 | accept tolerances |"}, "approve"),
    ("edit", ".migration/06_decisions.md", {"old_string": "# Decisions\n", "new_string": "# Decisions\nmore prose\n"}, "block"),
    ("edit", ".migration/06_decisions.md", {"old_string": "| D-7 | old | decision |\n", "new_string": "| D-7 | new | changed |\n"}, "block"),
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
    decisions = ws / "sub" / ".migration" / "06_decisions.md"
    allowlist = ws / "sub" / ".migration" / "allowed_targets.json"
    added = run_hook("edit", {
        "file_path": str(decisions),
        "old_string": "# Decisions\n",
        "new_string": "| D-8 | 2026-01-01 | accept tolerances |\n",
    }, ws, {"CLAUDE_PROJECT_DIR": str(ws)})
    blocked = run_hook("write", {"file_path": str(allowlist), "content": "{}"}, ws, {"CLAUDE_PROJECT_DIR": str(ws)})
    assert added.returncode == 0
    assert blocked.returncode == 2


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
