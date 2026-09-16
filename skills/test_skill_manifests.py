"""Every SKILL.md frontmatter must parse as YAML with a name matching its directory; an
unquoted `: ` in a description is what the installer reports as "Skill malformed"."""
import json
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills" / "data-reconciliation" / "harness"))

from recon.config import load_canon_rules


SKILLS = sorted((ROOT / "skills").glob("*/SKILL.md"))
EXTRA_SKILLS = sorted((ROOT / "skills-extra").glob("*/SKILL.md"))
ALL_SKILLS = SKILLS + EXTRA_SKILLS
# https://docs.devinenterprise.com/cli/extensibility/skills/creating-skills#frontmatter-reference
FRONTMATTER_FIELDS = {
    "name", "description", "argument-hint", "model", "subagent", "agent",
    "allowed-tools", "permissions", "triggers",
}


@pytest.mark.parametrize(
    "path",
    ALL_SKILLS,
    ids=lambda p: f"extra/{p.parent.name}" if p.parent.parent.name == "skills-extra" else p.parent.name,
)
def test_frontmatter_parses(path):
    text = path.read_text()
    assert text.startswith("---\n"), "missing frontmatter"
    end = text.find("\n---", 4)
    assert end > 0, "unterminated frontmatter"
    meta = yaml.safe_load(text[4:end])
    assert meta["name"] == path.parent.name
    assert isinstance(meta["description"], str) and meta["description"].strip()
    assert set(meta) <= FRONTMATTER_FIELDS, set(meta) - FRONTMATTER_FIELDS


def test_extra_skills_are_not_in_core():
    core = {p.name for p in (ROOT / "skills").iterdir() if p.is_dir()}
    extra = {p.name for p in (ROOT / "skills-extra").iterdir() if p.is_dir()}
    assert core.isdisjoint(extra)
    assert {"teradata-bteq", "informatica-xml", "tsql-ssis", "lakebridge"} <= extra
    assert "redshift-" + "sql" not in core | extra


def test_extra_canonicalization_rules_load():
    paths = sorted((ROOT / "skills-extra").glob("*/canonicalization.json"))
    paths += sorted((ROOT / "skills").glob("*/canonicalization.json"))
    for path in paths:
        load_canon_rules(path)


def test_required_databricks_plugin_is_pinned():
    manifest = json.loads((ROOT / ".devin-plugin" / "plugin.json").read_text())
    (dep,) = manifest["requiredPlugins"]
    assert dep["url"] == "https://github.com/databricks/databricks-agent-skills"
    assert dep["path"] == "plugins/databricks/claude"
    assert len(dep.get("sha", "")) == 40
