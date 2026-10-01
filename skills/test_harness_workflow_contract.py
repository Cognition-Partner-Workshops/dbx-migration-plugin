"""The harness writes result.json; the workflow reads it at the PR head. Every key the workflow
reads must be a key `build_result` emits, so a rename on one side fails here and not in a
wave. The strings are found by pattern in workflow.py: `result.json['key']` (prompts and
docstrings) and `.get("key")` on the parsed file (`git show <head>:...result.json`)."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "skills" / "data-reconciliation" / "harness"
FANOUT = ROOT / "skills" / "migration-fanout"
WORKFLOW = "".join((FANOUT / n).read_text() for n in ("workflow.py", "decisions.py", "manifest.py", "report.py"))

sys.path.insert(0, str(HARNESS))
from recon.report import build_result  # noqa: E402
from recon.tiers import TierResult  # noqa: E402


def emitted():
    return build_result("unit", "live", "m1", "t1", [TierResult(1, "row_count", True, 1, [])])


def workflow_reads():
    keys = set(re.findall(r"result\.json\[['\"](\w+)['\"]\]", WORKFLOW))
    for m in re.finditer(r"result\.json\"\][^\n]*\n(?:[^\n]*\n){0,3}", WORKFLOW):
        keys.update(re.findall(r"\.get\(\"(\w+)\"\)", m.group(0)))
    return keys


def test_the_workflow_reads_only_keys_the_harness_writes():
    reads = workflow_reads()
    assert {"merge_eligible", "cost"} <= reads, reads
    missing = sorted(reads - set(emitted()))
    assert not missing, f"workflow.py reads result.json keys build_result never writes: {missing}"


def test_verdict_vocabularies_agree():
    result = emitted()
    child_verdicts = set(re.search(r'"recon_verdict": \{"type": "string", "enum": \[([^\]]*)\]', WORKFLOW)
                         .group(1).replace('"', "").replace(" ", "").split(","))
    assert result["verdict"] in child_verdicts
    assert isinstance(result["merge_eligible"], bool)
