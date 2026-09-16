"""A rule lives in exactly one file. No sentence of eight or more words from `AGENTS.md`
may appear verbatim anywhere else in the repo; point at AGENTS.md instead of restating it."""
import re
from pathlib import Path

import pytest

from skills.repo_text import read_text

ROOT = Path(__file__).resolve().parents[1]
AGENTS = ROOT / "AGENTS.md"
MIN_WORDS = 8
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "build"}


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def agents_sentences():
    body = _normalize(AGENTS.read_text().replace("\n", " "))
    out = []
    for sentence in re.split(r"(?<=[.;:])\s+", body):
        sentence = sentence.strip(" -*#")
        if len(sentence.split()) >= MIN_WORDS:
            out.append(sentence)
    return out


def other_files():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        if SKIP_DIRS & set(path.relative_to(ROOT).parts):
            continue
        if any(part.endswith(".egg-info") for part in path.relative_to(ROOT).parts):
            continue
        if path == AGENTS:
            continue
        yield path


def test_other_files_include_sql_and_self():
    paths = list(other_files())
    assert any(path.suffix == ".sql" for path in paths)
    assert Path(__file__) in paths


def test_agents_has_rule_sentences():
    assert len(agents_sentences()) >= 6


@pytest.mark.parametrize("path", list(other_files()), ids=lambda p: str(p.relative_to(ROOT)))
def test_no_agents_sentence_restated(path):
    text = read_text(path)
    if text is None:
        return
    text = _normalize(text)
    dupes = [s for s in agents_sentences() if s in text]
    assert not dupes, f"{path.relative_to(ROOT)} restates AGENTS.md: {dupes}"
