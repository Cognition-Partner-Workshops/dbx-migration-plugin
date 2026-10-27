"""Shipped instructions and file payloads must be usable under the write-scope guard."""
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

PLUGIN = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLUGIN / "hooks"))

import dbx_guard as g


@pytest.fixture(scope="module")
def workspace(tmp_path_factory):
    ws = tmp_path_factory.mktemp("shipped")
    subprocess.run(["git", "init", "-q", str(ws)], check=True)
    (ws / ".migration").mkdir()
    (ws / g.CONFIG_REL).write_text(json.dumps({
        "catalogs": ["mig_cat"], "legacy_sources": ["ORACLE_DSN"], "target_hosts": ["DATABRICKS_HOST"],
    }))
    subprocess.run(["git", "-C", str(ws), "add", ".migration"], check=True)
    subprocess.run(["git", "-C", str(ws), "-c", "user.name=t", "-c", "user.email=t@example.com",
                    "commit", "-qm", "policy"], check=True)
    return ws, g.load_config(ws)


def _commands():
    docs = sorted(set(PLUGIN.glob("skills/*/SKILL.md")) | set(PLUGIN.glob("skills/*/references/*.md")))
    for path in docs:
        text = path.read_text()
        for block in re.finditer(r"^```(?:sh|bash)\s*\n(.*?)^```", text, re.MULTILINE | re.DOTALL):
            for line in re.sub(r"\\\n\s*", " ", block.group(1)).splitlines():
                line = re.sub(r"\s+#.*$", "", line).strip()
                if not line or line.startswith("#") or "__dbx_guard_probe__" in line:
                    continue
                while re.search(r"\[[^\[\]]*\]", line):
                    line = re.sub(r"\[[^\[\]]*\]", "", line)
                yield str(path.relative_to(PLUGIN)), line


COMMANDS = list(_commands())
FILES = sorted(path for tree in ("skills", "hooks") for path in (PLUGIN / tree).rglob("*")
               if path.suffix in (".py", ".md") and not path.is_relative_to(PLUGIN / "hooks" / "tests"))


@pytest.mark.parametrize("source,command", COMMANDS, ids=[f"{source}: {command}" for source, command in COMMANDS])
def test_shipped_command_approves(workspace, source, command):
    ws, cfg = workspace
    command = command.replace("<plugin>", str(PLUGIN)).replace("<repo root>", str(ws)).replace("<repo>", str(ws))
    command = re.sub(r"<[^<>\n]+>", "x", command)
    verdict = g.evaluate_with_workdirs(command, cfg, ws, cwd=str(ws))
    assert verdict.decision == "approve", f"{source}: {command}\n{verdict.reason}"


@pytest.mark.parametrize("path", FILES, ids=[str(path.relative_to(PLUGIN)) for path in FILES])
def test_shipped_file_payload(workspace, path):
    ws, cfg = workspace
    rel, content = path.relative_to(PLUGIN), path.read_text()
    command = f"cat > {shlex.quote(str(ws / rel))} <<'EOF'\n{content}\nEOF"
    verdict = g.evaluate_with_workdirs(command, cfg, ws, cwd=str(ws))
    if "__dbx_guard_probe__" in content:
        assert verdict.decision == "block" and all("hook probe" in v for v in verdict.violations), verdict.reason
    else:
        assert verdict.decision == "approve", verdict.reason
    verdict = g.evaluate_edit("write", {"file_path": str(ws / rel), "content": content}, cfg, ws, str(ws))
    assert verdict.decision == "approve", verdict.reason
