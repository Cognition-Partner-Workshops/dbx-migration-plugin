import ast
from collections import Counter
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")


def _functions():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name in {"validate_manifest", "validate_verify"}]
    namespace = {"Counter": Counter}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    return namespace


def test_validate_verify_missing_and_extra_verdicts():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "pr_url": "https://example/pr/3"}]
    missing = validate_verify({"wave_verdict": "PASS", "unit_verdicts": {},
                               "merged_prs": [], "findings": []}, passed, False)
    extra = validate_verify({"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "PASS", "other": "PASS"},
                             "merged_prs": [], "findings": []}, passed, False)
    assert "missing verdicts for w2-b03" in missing[0]
    assert any("unexpected verdicts" in problem for problem in extra)


def test_validate_verify_contradiction_and_missing_merge():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "pr_url": "https://example/pr/3"}]
    problems = validate_verify({"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "FAIL"},
                                "merged_prs": [], "findings": []}, passed, True)
    assert any("contradict" in problem for problem in problems)
    assert any("missing https://example/pr/3" in problem for problem in problems)


@pytest.mark.parametrize("value", [0, True, "3"])
def test_validate_manifest_rejects_invalid_positive_integer(value):
    validate_manifest = _functions()["validate_manifest"]
    manifest = {"wave": 1, "repo": "repo", "child_macro": "child",
                "verify_macro": "verify", "batches": [{"id": "b", "units": ["u"],
                "write_targets": ["t"], "brief": "brief"}], "width": value}
    with pytest.raises(SystemExit, match="width"):
        validate_manifest(manifest)
