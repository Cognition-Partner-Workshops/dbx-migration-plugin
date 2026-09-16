"""Every SKILL.md frontmatter must parse as YAML with a name matching its directory; an
unquoted `: ` in a description is what the installer reports as "Skill malformed"."""
import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
SKILLS = sorted(p for p in (ROOT / "skills").glob("*/SKILL.md"))
# https://docs.devinenterprise.com/cli/extensibility/skills/creating-skills#frontmatter-reference
FRONTMATTER_FIELDS = {
    "name", "description", "argument-hint", "model", "subagent", "agent",
    "allowed-tools", "permissions", "triggers",
}


@pytest.mark.parametrize("path", SKILLS, ids=lambda p: p.parent.name)
def test_frontmatter_parses(path):
    text = path.read_text()
    assert text.startswith("---\n"), "missing frontmatter"
    end = text.find("\n---", 4)
    assert end > 0, "unterminated frontmatter"
    meta = yaml.safe_load(text[4:end])
    assert meta["name"] == path.parent.name
    assert isinstance(meta["description"], str) and meta["description"].strip()
    assert set(meta) <= FRONTMATTER_FIELDS, set(meta) - FRONTMATTER_FIELDS


def test_required_databricks_plugin_is_pinned():
    manifest = json.loads((ROOT / ".devin-plugin" / "plugin.json").read_text())
    (dep,) = manifest["requiredPlugins"]
    assert dep["url"] == "https://github.com/databricks/databricks-agent-skills"
    assert dep["path"] == "plugins/databricks/claude"
    assert len(dep.get("sha", "")) == 40


def test_source_access_folded_into_recon():
    assert not (ROOT / "skills" / "lakehouse-federation").exists()
    assert not (ROOT / "skills" / "backfill-planner").exists()
    recon = (ROOT / "skills" / "data-reconciliation" / "SKILL.md").read_text()
    assert "## Source access" in recon
    assert "### Live mode prerequisites (Lakehouse Federation)" in recon
    assert "### Load posture (materialize)" in recon
    forbidden = ("lakehouse-federation", "backfill-planner")
    allowed = ROOT / "skills" / "install-dbx-factory" / "playbooks" / "0-README.md"
    for path in ROOT.glob("**/*.md"):
        if path == allowed or ".git" in path.parts:
            continue
        text = path.read_text()
        assert not any(term in text for term in forbidden), path


def test_source_access_keeps_live_and_materialize_rules():
    recon = (ROOT / "skills" / "data-reconciliation" / "SKILL.md").read_text()
    for phrase in (
        "CREATE CONNECTION ... OPTIONS (... secret(...))",
        "legacy-query concurrency cap",
        "CTAS from the foreign catalog",
        "machine-readable table of object, class, method",
        "drop and recopy any unverified partial partition",
        "connector output never self-certifies",
        "timestamp/SCN/LSN",
    ):
        assert phrase in recon
