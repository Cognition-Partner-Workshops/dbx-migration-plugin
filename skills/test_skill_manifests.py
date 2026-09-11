"""Every SKILL.md frontmatter must parse as YAML with a name matching its directory; an
unquoted `: ` in a description is what the installer reports as "Skill malformed"."""
import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
SKILLS = sorted(p for p in (ROOT / "skills").glob("*/SKILL.md"))


@pytest.mark.parametrize("path", SKILLS, ids=lambda p: p.parent.name)
def test_frontmatter_parses(path):
    text = path.read_text()
    assert text.startswith("---\n"), "missing frontmatter"
    end = text.find("\n---", 4)
    assert end > 0, "unterminated frontmatter"
    meta = yaml.safe_load(text[4:end])
    assert meta["name"] == path.parent.name
    assert isinstance(meta["description"], str) and meta["description"].strip()


def test_required_databricks_plugin_is_pinned():
    manifest = json.loads((ROOT / ".devin-plugin" / "plugin.json").read_text())
    (dep,) = manifest["requiredPlugins"]
    assert dep["path"] == "plugins/databricks/claude"
    assert len(dep.get("sha", "")) == 40
