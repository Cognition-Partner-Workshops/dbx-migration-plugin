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
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"validate_manifest", "validate_verify"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"VERIFY_DEPTHS", "GUARD_MODES", "STOP_MODES"}
                    for t in node.targets))]
    namespace = {"Counter": Counter}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    return namespace


def _batch_runtime():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.ClassDef) and node.name == "Breaker")
                or (isinstance(node, ast.AsyncFunctionDef) and node.name == "run_batch")
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "MERGE_EVIDENCE_MODES" for t in node.targets))]
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


CAPS = {"identity": "sp-1", "catalogs": ["mig"], "ready": True, "guard_mode": "block", "stop_mode": "soft"}


def _caps(**changes):
    return {**CAPS, **changes}


def _manifest(**extra):
    m = {"wave": 1, "repo": "repo", "child_macro": "child", "verify_macro": "verify",
         "capabilities": CAPS,
         "batches": [{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "brief"}]}
    m.update(extra)
    return m


@pytest.mark.parametrize("caps", [
    "sp@x",                                   # not a dict
    {k: v for k, v in CAPS.items() if k != "identity"},
    _caps(identity=""),
    _caps(catalogs=[]),
    _caps(catalogs="mig"),
    _caps(catalogs=["mig", ""]),
    _caps(ready=False),
    {k: v for k, v in CAPS.items() if k != "ready"},
    _caps(ready=None),
    _caps(ready="true"),
    _caps(ready=1),
    {k: v for k, v in CAPS.items() if k != "guard_mode"},
    _caps(guard_mode="off"),
    {k: v for k, v in CAPS.items() if k != "stop_mode"},
    _caps(stop_mode="medium"),
])
def test_validate_manifest_rejects_bad_capability_contract(caps):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="capabilities"):
        validate_manifest(_manifest(capabilities=caps))


def test_validate_manifest_rejects_missing_capabilities():
    validate_manifest = _functions()["validate_manifest"]
    m = _manifest()
    del m["capabilities"]
    with pytest.raises(SystemExit, match="capabilities"):
        validate_manifest(m)


@pytest.mark.parametrize("manifest", [
    _manifest(capabilities=_caps(stop_mode="hard")),                    # auto_merge defaults to true
    _manifest(capabilities=_caps(stop_mode="hard"), auto_merge=True),
    _manifest(auto_merge="false"),
])
def test_validate_manifest_hard_stop_mode_forbids_auto_merge(manifest):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="auto_merge"):
        validate_manifest(manifest)


def test_validate_manifest_accepts_capability_contract():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest())
    validate_manifest(_manifest(auto_merge=True))
    validate_manifest(_manifest(capabilities=_caps(stop_mode="hard", guard_mode="warn"), auto_merge=False))


def test_child_prompt_embeds_capability_contract():
    ns = _prompt_ns(_manifest())
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "--expect-identity sp-1" in text
    assert '"catalogs": ["mig"]' in text
    assert '"guard_mode": "block"' in text and '"stop_mode": "soft"' in text
    assert "BLOCKED" in text


@pytest.mark.parametrize("manifest", [
    _manifest(verify_depth="threshold"),   # verifier depth is a plan decision, never tolerance-driven
    _manifest(verify_depth="deep"),
    _manifest(batches=[{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "brief",
                        "verify_depth": "none"}]),
    _manifest(cost_estimate="cheap"),
])
def test_validate_manifest_rejects_bad_depth_or_cost(manifest):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="verify_depth|cost_estimate"):
        validate_manifest(manifest)


def test_validate_manifest_accepts_depth_knob_and_estimate():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest(verify_depth="full", cost_estimate={"source_statements": 12}))
    validate_manifest(_manifest(batches=[{"id": "b", "units": ["u"], "write_targets": ["t"],
                                          "brief": "brief", "verify_depth": "sampled"}]))


def _prompt_ns(manifest):
    tree = ast.parse(WORKFLOW.read_text())
    names = {"verify_prompt", "batch_verify_depth", "child_prompt", "capability_block",
             "sum_cost", "cost_line"}
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef) and node.name in names)
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"COST_KEYS", "MERGE_EVIDENCE_MODES"}
                    for t in node.targets))]
    ns = {"json": __import__("json"), "WAVE": 1, "REPO": "repo", "MANIFEST": manifest,
          "BATCHES": manifest["batches"], "VERIFY_DEPTH": manifest.get("verify_depth", "sampled")}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), ns)
    return ns


def test_verifier_prompt_carries_per_batch_depth_defaulting_to_sampled():
    m = _manifest(batches=[
        {"id": "b1", "units": ["u"], "write_targets": ["t1"], "brief": "x"},
        {"id": "b2", "units": ["v"], "write_targets": ["t2"], "brief": "y", "verify_depth": "full"}])
    ns = _prompt_ns(m)
    # the shape main() hands the verifier: {"batch": id, ...}, no verify_depth on the record
    passed = [{"batch": b["id"], "units": b["units"], "pr_url": "", "branch": ""} for b in m["batches"]]
    text = ns["verify_prompt"](passed, True)
    assert '"b1": "sampled"' in text and '"b2": "full"' in text
    assert "--depth" in text and "Never lower" in text and "recon_cost" in text
    ns2 = _prompt_ns(_manifest(verify_depth="full"))
    assert '"b": "full"' in ns2["verify_prompt"]([{"batch": "b", "units": ["u"]}], True)


def test_child_prompt_asks_for_recon_cost():
    ns = _prompt_ns(_manifest())
    assert "recon_cost" in ns["child_prompt"](ns["MANIFEST"]["batches"][0])


def test_cost_line_compares_estimate_with_summed_actuals():
    m = _manifest(cost_estimate={"source_statements": 10, "source_rows_fetched": 1000})
    ns = _prompt_ns(m)
    results = [{"recon_cost": {"source_statements": 4, "target_statements": 2,
                               "source_rows_fetched": 300, "target_rows_fetched": 300, "elapsed_s": 1.2}},
               {"status": "BLOCKED"}]
    verify = {"recon_cost": {"source_statements": 3, "target_statements": None,
                             "source_rows_fetched": 100, "target_rows_fetched": 100, "elapsed_s": 0.8}}
    line = ns["cost_line"](results, verify)
    assert "estimated source_statements=10, source_rows_fetched=1000" in line
    assert "source_statements=7" in line and "source_rows_fetched=400" in line
    assert "target_statements=" not in line.split("actual")[1].split(",")[0]  # None side is omitted
    assert "harness time 2s" in line and "Verifier depth sampled" in line
    assert _prompt_ns(_manifest())["cost_line"]([{"status": "FAIL"}], None).startswith("Cost: no estimate")


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


def _run_one(namespace, report):
    async def agent(prompt, **kwargs):
        return dict(report)

    namespace["agent"] = agent

    async def exercise():
        return await namespace["run_batch"](
            {"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "b"},
            asyncio.Semaphore(1), namespace["Breaker"](3))

    return asyncio.run(exercise())


@pytest.mark.parametrize("mode", ["live", "snapshot", "transactional"])
def test_pass_with_merge_evidence_mode_is_kept(mode):
    out = _run_one(_batch_runtime(), {"status": "PASS", "recon_verdict": "PASS", "recon_mode": mode,
                                      "pr_url": "https://example/pr/1", "branch": "f", "one_line_summary": "ok"})
    assert out["status"] == "PASS" and "failure_class" not in out


@pytest.mark.parametrize("mode", ["fixture", "continuous", None])
def test_pass_without_merge_evidence_is_downgraded(mode):
    out = _run_one(_batch_runtime(), {"status": "PASS", "recon_verdict": "PASS", "recon_mode": mode,
                                      "pr_url": "https://example/pr/1", "branch": "f", "one_line_summary": "ok"})
    assert out["status"] == "FAIL" and out["failure_class"] == "non_merge_evidence"


def test_prompts_name_every_merge_evidence_mode():
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](_manifest()["batches"][0])
    verify = ns["verify_prompt"]([{"batch": "b", "pr_url": "https://example/pr/1"}], False)
    for mode in ("live", "snapshot", "transactional"):
        assert mode in child and mode in verify
    assert "Fixture evidence is never PASS" in child


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
