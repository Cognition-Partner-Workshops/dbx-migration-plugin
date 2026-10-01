"""No vocabulary from the deleted playbook/macros/stops model may survive in shipped prose or
code (CHANGES.md is allowed to name what it deleted). Findings print file:line so a hit is
fixed, not argued with."""
import re
from pathlib import Path

import pytest

from skills.repo_text import read_text

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "build", "node_modules"}
SKIP_SUFFIXES = {".pyc", ".png", ".jpg", ".gif", ".pdf", ".zip", ".gz", ".parquet"}
ALLOWED = {ROOT / "CHANGES.md", Path(__file__).resolve()}

# case-sensitive identifiers and file names of the old model
CASE_TERMS = (
    "stop_mode", "stop_c", "gates_sha", "default-accepted", "auto-accept",
    "!dbx_", "install-dbx-factory", "06_decisions", "05_progress", "00_context",
    "01_conventions", "02_glossary", "04_dependency_register", "07_access_checklist",
    "03_recon_tolerances.md", "_plan.md", ".brief.md", "notification contract",
    "ledger_tampered", "child_macro", "verify_macro", "decision_ledger",
)
# words banned whatever their case
CI_TERMS = (r"STOP [A-E]\b", r"playbook", r"macro", r"orchestrator", r"\bledger\b")
# The doctor keeps `--role orchestrator` as a CLI value; Teradata "macros" in skills-extra are
# the source engine's own feature name, not the deleted model's macro mechanism.
EXEMPTIONS = {r"orchestrator": ("skills/factory-doctor/",), r"macro": ("skills-extra/",)}


def repo_files():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or path.suffix in SKIP_SUFFIXES:
            continue
        rel = path.relative_to(ROOT)
        if SKIP_DIRS & set(rel.parts):
            continue
        if path in ALLOWED or path.suffix not in {".md", ".py", ".json"}:
            continue
        if any(part.endswith(".egg-info") for part in rel.parts):
            continue
        # harness fixtures keep the estate's own fixture names
        if "fixtures" in rel.parts and "harness" in rel.parts:
            continue
        yield path, rel


@pytest.mark.parametrize("path,rel", list(repo_files()), ids=lambda p: str(p))
def test_no_legacy_process_terms(path, rel):
    text = read_text(path)
    if text is None:
        return
    hits = []
    for i, line in enumerate(text.splitlines(), 1):
        for term in CASE_TERMS:
            if term in line:
                hits.append(f"{rel}:{i}: {term!r}")
        for pattern in CI_TERMS:
            if rel.as_posix().startswith(EXEMPTIONS.get(pattern, ())):
                continue
            if re.search(pattern, line, re.IGNORECASE):
                hits.append(f"{rel}:{i}: /{pattern}/")
    assert not hits, "; ".join(hits)
