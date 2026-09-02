import ast
import asyncio
from collections import Counter
import os
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


def _batch_runtime():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.ClassDef) and node.name == "Breaker")
                or (isinstance(node, ast.AsyncFunctionDef) and node.name == "run_batch")]
    namespace = {
        "asyncio": asyncio,
        "Counter": Counter,
        "REPLAYED": {},
        "CHILD_SCHEMA": {},
        "REPO": ".",
        "WorkflowAgentError": RuntimeError,
        "child_prompt": lambda batch: batch,
        "log": lambda message: None,
    }
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


def test_replayed_failures_do_not_refill_breaker():
    namespace = _batch_runtime()
    namespace["REPLAYED"] = {f"b{i}": "FAIL" for i in range(3)}

    async def agent(prompt, **kwargs):
        if kwargs["label"] in namespace["REPLAYED"]:
            return {"status": "FAIL", "recon_verdict": "NOT_RUN",
                    "failure_class": "same", "one_line_summary": "replayed"}
        return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live",
                "pr_url": "https://example/pr/held", "branch": "feature/held",
                "one_line_summary": "held passed"}

    namespace["agent"] = agent

    async def exercise():
        breaker = namespace["Breaker"](3)
        sem = asyncio.Semaphore(1)
        outputs = []
        for batch_id in ("b0", "b1", "b2", "b3"):
            outputs.append(await namespace["run_batch"](
                {"id": batch_id, "units": ["u"], "write_targets": ["t"], "brief": "b"},
                sem, breaker))
        return outputs, breaker

    outputs, breaker = asyncio.run(exercise())
    assert outputs[-1]["status"] == "PASS"
    assert breaker.tripped_on is None


def test_pass_without_pr_is_downgraded():
    namespace = _batch_runtime()

    async def agent(prompt, **kwargs):
        return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live",
                "branch": "feature/no-url", "one_line_summary": "passed"}

    namespace["agent"] = agent

    async def exercise():
        breaker = namespace["Breaker"](3)
        return await namespace["run_batch"](
            {"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "b"},
            asyncio.Semaphore(1), breaker)

    output = asyncio.run(exercise())
    assert output["status"] == "FAIL"
    assert output["failure_class"] == "missing_pr"


def test_first_run_requires_wave_run_id():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == "main"]
    namespace = {
        "resume": False,
        "os": os,
        "RUN_ID_PATH": Path(".migration/waves/wave-1.run_id"),
    }
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    old = os.environ.pop("WAVE_RUN_ID", None)
    try:
        with pytest.raises(SystemExit, match="WAVE_RUN_ID is required on the first run"):
            asyncio.run(namespace["main"]())
    finally:
        if old is not None:
            os.environ["WAVE_RUN_ID"] = old
