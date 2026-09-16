"""Generality gate: no engagement-specific identifier appears in the plugin outside
`**/fixtures/example_*`. Lessons from a run are written as rules about a class of estate;
the deny-list lives in `estate_denylist.json`."""
import json
import re
from pathlib import Path

import pytest

from skills.repo_text import read_text

ROOT = Path(__file__).resolve().parents[1]
DENYLIST = json.loads((Path(__file__).parent / "estate_denylist.json").read_text())
TERMS = [t for k, v in DENYLIST.items() if not k.startswith("_") for t in v]
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "build"}
SKIP_SUFFIXES = {".pyc", ".png", ".jpg", ".gif", ".pdf", ".zip", ".gz", ".parquet"}


def _in_example_fixture(path: Path) -> bool:
    parts = path.relative_to(ROOT).parts
    return any(
        parts[i] == "fixtures" and parts[i + 1].startswith("example_")
        for i in range(len(parts) - 1)
    )


def repo_files():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or path.suffix in SKIP_SUFFIXES:
            continue
        if SKIP_DIRS & set(path.relative_to(ROOT).parts):
            continue
        if any(part.endswith(".egg-info") for part in path.relative_to(ROOT).parts):
            continue
        if path.name == "estate_denylist.json" or _in_example_fixture(path):
            continue
        yield path


PATTERN = re.compile("|".join(re.escape(t) for t in TERMS), re.IGNORECASE)


def test_denylist_is_nonempty():
    assert TERMS


@pytest.mark.parametrize("path", list(repo_files()), ids=lambda p: str(p.relative_to(ROOT)))
def test_no_estate_strings(path):
    text = read_text(path)
    if text is None:
        return
    hits = {m.group(0) for m in PATTERN.finditer(text)}
    assert not hits, f"engagement-specific identifiers in {path.relative_to(ROOT)}: {sorted(hits)}"
