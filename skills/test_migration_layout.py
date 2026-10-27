"""The shipped plugin mentions only machine-file locations under `.migration/`."""
import re
import sys
from pathlib import Path

import pytest

from skills.repo_text import read_text

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "build", "node_modules", "tests"}
SKIP_SUFFIXES = {".pyc", ".png", ".jpg", ".gif", ".pdf", ".zip", ".gz", ".parquet"}
SEGMENT = re.compile(r"\.migration/([^/\s`'\"<>)]+)")
ROOT_PLACEHOLDER = re.compile(r"\.migration/(<[^/\s`'\"]+>)")

sys.path.insert(0, str(ROOT / "skills" / "factory-doctor"))
from doctor import LEGACY_ALIASES, MIGRATION_LAYOUT  # noqa: E402


def repo_files():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or path.suffix in SKIP_SUFFIXES:
            continue
        rel = path.relative_to(ROOT)
        if SKIP_DIRS & set(rel.parts):
            continue
        if path.name == "CHANGES.md" or path.name.startswith("test_"):
            continue
        if path.suffix not in {".md", ".py", ".json"}:
            continue
        if any(part.endswith(".egg-info") for part in rel.parts):
            continue
        if "fixtures" in rel.parts and "harness" in rel.parts:
            continue
        yield path, rel


@pytest.mark.parametrize("path,rel", list(repo_files()), ids=lambda p: str(p))
def test_migration_paths_use_layout_entries(path, rel):
    text = read_text(path)
    if text is None:
        return
    hits = []
    for line_number, line in enumerate(text.splitlines(), 1):
        for match in SEGMENT.finditer(line):
            segment = match.group(1)
            if segment.endswith("\\n"):
                segment = segment[:-2]
            segment = segment.rstrip(".,;:!?\\")
            if not segment:
                continue
            if segment in MIGRATION_LAYOUT:
                continue
            if segment in LEGACY_ALIASES and rel.as_posix() == "skills/factory-doctor/doctor.py":
                continue
            hits.append(f"{rel}:{line_number}: unexpected .migration/ segment {segment!r}")
        for match in ROOT_PLACEHOLDER.finditer(line):
            hits.append(f"{rel}:{line_number}: placeholder {match.group(1)!r} is not a layout directory")
    assert not hits, "; ".join(hits)
