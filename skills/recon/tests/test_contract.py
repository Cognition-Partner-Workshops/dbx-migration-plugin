"""The result.json contract (design §8): parity/mode/tiers/findings/cost, exit 0|1|3, and
the .migration/ inputs layout the run command reads."""
import json
from dataclasses import replace
from pathlib import Path

import pytest

from recon import cli
from recon.config import MappingSpec, Tolerances, load_mapping_spec, load_slice_inputs
from recon.engine import run_recon
from recon.report import build_result

from .fakes import FakeSource, FakeTarget

ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
MAPPING = {
    "version": "m1",
    "objects": [{
        "object": "t", "root_table": "s.t",
        "key": {"source": ["id"], "target": ["id"]},
        "fields": [{"source": "id", "target": "id", "source_type": "int",
                    "target_type": "int"},
                   {"source": "v", "target": "v", "source_type": "text",
                    "target_type": "text"}],
    }],
}


def spec(tmp_path: Path) -> MappingSpec:
    p = tmp_path / "mapping.json"
    p.write_text(json.dumps(MAPPING))
    return load_mapping_spec(p)


def tol(**kw) -> Tolerances:
    return replace(Tolerances(version="t1"), **kw)


def run(tmp_path: Path, **kw):
    args = dict(spec=spec(tmp_path), tol=tol(), rules=[],
                source=FakeSource({"s.t": ROWS}), target=FakeTarget({"t": ROWS}),
                mode="live")
    args.update(kw)
    return run_recon("slice-1", args.pop("mode"), args.pop("spec"), args.pop("tol"),
                     args.pop("rules"), args.pop("source"), args.pop("target"), **args)


def test_pass_contract(tmp_path):
    r = run(tmp_path)
    assert r["parity"] == "PASS"
    for key in ("mode", "tiers", "routine_parity", "findings", "cost", "mapping_version",
                    "tolerance_version", "depth", "seed", "grading", "rerun_posture"):
        assert key in r
    for removed in ("verdict", "merge_eligible", "merge_policy", "blockers",
                    "blocker_classes", "merge_block_reasons", "merge_authority"):
        assert removed not in r
    assert all(f["class"] in ("structural", "privilege_visibility", "evidence",
                              "rerun_policy") for f in r["findings"])


def test_row_diff_is_parity_fail(tmp_path):
    r = run(tmp_path, target=FakeTarget({"t": ROWS[:-1]}))
    assert r["parity"] == "FAIL"


def test_structural_mode_not_run(tmp_path):
    r = run(tmp_path, mode="structural")
    assert r["parity"] == "NOT_RUN"


def test_missing_analysis_is_not_run(tmp_path):
    r = run(tmp_path, routine_analysis_missing=True)
    assert r["parity"] == "NOT_RUN"
    assert any(f["code"] == "routine_analysis_missing" for f in r["findings"])


def test_missing_routine_row_is_not_run(tmp_path):
    r = run(tmp_path, routine_writers=["pkg_a"])
    assert r["parity"] == "NOT_RUN"
    assert any(f["code"] == "routine_parity_missing" and f["object"] == "pkg_a"
               for f in r["findings"])


def test_unproven_routine_is_not_run(tmp_path):
    r = run(tmp_path, routine_writers=["pkg_a"],
            routine_parity=[{"routine": "pkg_a", "status": "unproven", "evidence": None}])
    assert r["parity"] == "NOT_RUN"


def test_proven_routine_passes(tmp_path):
    r = run(tmp_path, routine_writers=["pkg_a"],
            routine_parity=[{"routine": "pkg_a", "status": "proven", "evidence": "x"}])
    assert r["parity"] == "PASS"


def test_failed_routine_is_fail(tmp_path):
    r = run(tmp_path, routine_writers=["pkg_a"],
            routine_parity=[{"routine": "pkg_a", "status": "failed", "evidence": "x"}])
    assert r["parity"] == "FAIL"


def test_non_merge_mode_is_evidence_finding(tmp_path):
    r = run(tmp_path, mode="fixture")
    assert any(f["code"] == "mode" and f["class"] == "evidence" and not f["blocking"]
               for f in r["findings"])


def test_blocking_findings_flag(tmp_path):
    r = run_recon("slice-1", "fixture", spec(tmp_path), tol(blocking_findings=("evidence",)),
                  [], FakeSource({"s.t": ROWS}), FakeTarget({"t": ROWS}))
    blocking = [f for f in r["findings"] if f["blocking"]]
    assert blocking and all(f["class"] == "evidence" for f in blocking)


def test_by_design_unverified_finding(tmp_path):
    s = spec(tmp_path)
    s = replace(s, objects=[replace(o, grading="by_design_unverified") for o in s.objects])
    r = run_recon("slice-1", "live", s, tol(), [],
                  FakeSource({"s.t": ROWS}), FakeTarget({"t": ROWS}))
    assert any(f["code"] == "by_design_unverified" and f["class"] == "evidence"
               for f in r["findings"])


def _migration(root: Path) -> Path:
    inputs = root / ".migration" / "recon" / "slice-1" / "inputs"
    inputs.mkdir(parents=True)
    (inputs / "mapping.json").write_text(json.dumps(MAPPING))
    (inputs / "dependencies.json").write_text(json.dumps({"routines": []}))
    (root / ".migration" / "policy.json").write_text(json.dumps({
        "version": 2,
        "source": {"family": "oracle", "secret": "SRC_DSN", "access": "mig_cat.legacy_fed"},
        "targets": {"catalog": "mig_cat", "schema_prefix": "mig_run_",
                    "warehouse_http_path": "/sql/1.0/warehouses/x"},
        "tolerances": {"version": "t1"},
    }))
    return inputs


def test_run_cli_writes_result(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    inputs = _migration(tmp_path)
    (inputs / "snapshot_manifest.json").write_text(json.dumps(
        {"source": "oracle", "row_counts": {"t": 2}}))
    monkeypatch.setattr(cli, "DatabricksSourceAdapter",
                        lambda http_path=None: FakeSource({"mig_cat.legacy_fed.t": ROWS}))
    monkeypatch.setattr(cli, "DatabricksTargetAdapter",
                        lambda http_path, catalog, schema: FakeTarget({"t": ROWS}))
    out = tmp_path / ".migration" / "recon" / "slice-1" / "out"
    rc = cli.main(["run", "--unit", "slice-1", "--mode", "snapshot", "--out", str(out)])
    assert rc == 0
    result = json.loads((out / "result.json").read_text())
    assert result["parity"] == "PASS" and result["mode"] == "snapshot"
    assert (out / "report.md").exists()


def test_run_cli_out_must_be_under_slice_out(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _migration(tmp_path)
    rc = cli.main(["run", "--unit", "slice-1", "--mode", "snapshot",
                   "--out", str(tmp_path / "elsewhere")])
    assert rc == 3


def test_run_cli_snapshot_needs_manifest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _migration(tmp_path)
    monkeypatch.setattr(cli, "DatabricksSourceAdapter",
                        lambda http_path=None: FakeSource({"mig_cat.legacy_fed.t": ROWS}))
    monkeypatch.setattr(cli, "DatabricksTargetAdapter",
                        lambda http_path, catalog, schema: FakeTarget({"t": ROWS}))
    out = tmp_path / ".migration" / "recon" / "slice-1" / "out"
    rc = cli.main(["run", "--unit", "slice-1", "--mode", "snapshot", "--out", str(out)])
    assert rc == 3
    result = json.loads((out / "result.json").read_text())
    assert result["parity"] == "NOT_RUN"
    assert any(f["code"] == "snapshot_missing" for f in result["findings"])


def test_unit_name_rejects_traversal():
    with pytest.raises(Exception):
        cli._slice_paths("../escape")


def test_param_rejects_expression():
    with pytest.raises(Exception):
        cli.parse_params(["cutoff=1 OR 1=1"])
    assert cli.parse_params(["cutoff=2024-01-01"]) == {"cutoff": "2024-01-01"}


def test_run_cli_missing_inputs_is_not_run_exit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _migration(tmp_path)
    (tmp_path / ".migration" / "recon" / "slice-1" / "inputs" / "mapping.json").unlink()
    out = tmp_path / ".migration" / "recon" / "slice-1" / "out"
    rc = cli.main(["run", "--unit", "slice-1", "--mode", "snapshot", "--out", str(out)])
    assert rc == 3


def test_inputs_overlays(tmp_path):
    inputs = _migration(tmp_path)
    (inputs / "grading.json").write_text(json.dumps({"t": "by_design_unverified"}))
    (inputs / "rerun_posture.json").write_text(json.dumps({"posture": "not_applicable"}))
    spec, deps, rules, ops, snapshot = load_slice_inputs(inputs)
    assert spec.objects[0].grading == "by_design_unverified"
    assert spec.rerun_posture == "not_applicable"
    assert deps == {"routines": []} and rules == [] and ops is None and snapshot is None


def test_blocking_findings_validation(tmp_path):
    from recon.config import ConfigError, tolerances_from_dict
    with pytest.raises(ConfigError):
        tolerances_from_dict({"version": "t", "blocking_findings": ["nope"]}, tmp_path / "p")
    with pytest.raises(ConfigError):
        tolerances_from_dict({"version": "t", "exact": True, "per_type": {"int": 0.1}},
                             tmp_path / "p")


def test_selftest(capsys):
    assert cli.main(["selftest"]) == 0
    assert "selftest PASS" in capsys.readouterr().out
