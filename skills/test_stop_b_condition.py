import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "skills/install-dbx-factory/references/contract.md"
POINTERS = [
    ROOT / "OVERVIEW.md",
    ROOT / "skills/install-dbx-factory/playbooks/2-estate_inventory.md",
    ROOT / "skills/install-dbx-factory/playbooks/9-orchestrator.md",
]


def _row_b():
    for line in CONTRACT.read_text().splitlines():
        if line.startswith("| **B** |"):
            return line
    raise AssertionError("contract.md has no STOP B row")


def test_stop_b_skipped_only_when_pipeline_and_boundary_fixed():
    row = _row_b()
    assert "skipped only when intake fixed both the first pipeline and its scope boundary and exclusions" in row
    assert "still runs for boundary approval" in row


def test_pointers_do_not_restate_stop_b_condition():
    for path in POINTERS:
        text = path.read_text()
        assert "STOP B" in text, path
        assert not re.search(r"did not fix pipeline order", text), path
        assert "contract.md" in text or "references/contract.md" in text, path
