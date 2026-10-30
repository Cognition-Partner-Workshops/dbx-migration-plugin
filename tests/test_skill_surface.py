"""Plugin-surface checks for 0.7 (design spec section 13):

- every SKILL.md in the plugin has YAML frontmatter with `name` and `description`;
- every relative markdown link a SKILL.md makes resolves to a file that exists;
- the combined line count of the core skills (plan, analyze, migrate, verify,
  target-routing, dialects/*) stays within the 0.7 surface budget.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORE_SKILL_GLOBS = (
    "skills/plan/SKILL.md",
    "skills/analyze/SKILL.md",
    "skills/migrate/SKILL.md",
    "skills/verify/SKILL.md",
    "skills/target-routing/SKILL.md",
    "skills/dialects/*/SKILL.md",
)
CORE_LINE_CAP = 900
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
FM_KEY_RE = re.compile(r"^(name|description):", re.MULTILINE)


def skill_files():
    return sorted(
        p
        for p in ROOT.rglob("SKILL.md")
        if ".git" not in p.parts and "__pycache__" not in p.parts
    )


def core_skill_files():
    out = []
    for glob in CORE_SKILL_GLOBS:
        out.extend(ROOT.glob(glob))
    return sorted(set(out))


@pytest.mark.parametrize("path", skill_files(), ids=lambda p: str(p.relative_to(ROOT)))
def test_skill_has_frontmatter(path):
    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\n"), f"{path.relative_to(ROOT)}: missing frontmatter block"
    end = text.find("\n---", 4)
    assert end != -1, f"{path.relative_to(ROOT)}: unclosed frontmatter block"
    front = text[4:end]
    keys = set(FM_KEY_RE.findall(front))
    assert keys == {"name", "description"}, (
        f"{path.relative_to(ROOT)}: frontmatter keys {sorted(keys)}, want name + description"
    )


@pytest.mark.parametrize("path", skill_files(), ids=lambda p: str(p.relative_to(ROOT)))
def test_skill_links_resolve(path):
    text = path.read_text(encoding="utf-8")
    broken = []
    for target in LINK_RE.findall(text):
        target = target.split("#", 1)[0]
        if not target or "://" in target or target.startswith(("mailto:", "#")):
            continue
        candidate = Path(target) if target.startswith("/") else path.parent / target
        if not candidate.exists():
            broken.append(target)
    assert not broken, f"{path.relative_to(ROOT)}: links to missing files: {broken}"


def test_core_skill_line_budget():
    files = core_skill_files()
    missing = [g for g in CORE_SKILL_GLOBS if "*" not in g and not (ROOT / g).exists()]
    assert not missing, f"missing core skills: {missing}"
    assert files, "no core SKILL.md files found"
    total = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in files)
    assert total <= CORE_LINE_CAP, (
        f"core skills total {total} lines > {CORE_LINE_CAP}: "
        + ", ".join(f"{p.relative_to(ROOT)}={len(p.read_text(encoding='utf-8').splitlines())}" for p in files)
    )
