import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import progress
from progress import main, refresh_merged, render_progress


def _write_manifest(mig, wave, batches, **extra):
    waves = mig / "waves"
    waves.mkdir(parents=True, exist_ok=True)
    manifest = {"wave": wave, "batches": batches, **extra}
    manifest_path = waves / f"wave-{wave}.json"
    manifest_bytes = json.dumps(manifest).encode()
    manifest_path.write_bytes(manifest_bytes)
    return hashlib.sha256(manifest_bytes).hexdigest()[:12]


def _write_result(mig, name, result):
    waves = mig / "waves"
    waves.mkdir(parents=True, exist_ok=True)
    wave = result.get("wave")
    if wave is None:
        wave = int(name.split("-", 1)[1].split(".", 1)[0])
    manifest_path = waves / f"wave-{wave}.json"
    if not manifest_path.exists():
        batches = [
            {
                "id": batch.get("id"),
                **({"units": batch["units"]} if isinstance(batch, dict) and isinstance(batch.get("units"), list) else {}),
            }
            for batch in result.get("batches", [])
            if isinstance(batch, dict)
        ] if isinstance(result.get("batches"), list) else []
        _write_manifest(mig, wave, batches)
    result["manifest_sha"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()[:12]
    (waves / name).write_text(json.dumps(result))


def test_render_progress_uses_manifest_units_when_result_omits_them(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "w1-b01", "units": ["u_a", "u_b"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "width": 1,
        "run_id": "run",
        "base_sha": "base",
        "mode": "start",
        "hook_probe": "blocked:1234abcd",
        "doctor_signed_at": "now",
        "breaker_tripped_on": None,
        "auto_merge": False,
        "closed": False,
        "write_target_overlaps": [],
        "undeclared_write_targets": [],
        "unreported_write_targets": [],
        "batches": [{
            "id": "w1-b01",
            "status": "PASS",
            "recon_verdict": "PASS",
            "pr_url": "https://example.invalid/1",
            "recon_cost": {"rows": 2},
        }],
        "verify": {"unit_verdicts": {"w1-b01": "PASS"}, "merged_prs": []},
    })

    text = render_progress(mig)

    assert "| 1 | w1-b01 | u_a | PASS (unmerged) | PASS | PASS | https://example.invalid/1 | pending | {\"rows\":2} |" in text
    assert "| 1 | w1-b01 | u_b | PASS (unmerged) | PASS | PASS | https://example.invalid/1 | pending |  |" in text


def test_render_progress_normalizes_numeric_batch_ids(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": 1, "units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": 1, "status": "PASS", "recon_verdict": "PASS"}],
        "verify": {"unit_verdicts": {"1": "FAIL"}},
    })

    text = render_progress(mig)

    assert "| 1 | 1 | u1 | FAIL | PASS | FAIL |  |  |" in text


def test_manifest_batch_ids_colliding_after_normalization_are_rejected(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [
        {"id": 1, "units": ["u1"]},
        {"id": "1", "units": ["u2"]},
    ])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [
            {"id": 1, "units": ["u1"]},
            {"id": "1", "units": ["u2"]},
        ],
    })

    with pytest.raises(ValueError, match=r"wave-1\.json: duplicate batch id"):
        render_progress(mig)


def test_result_batch_ids_colliding_after_normalization_are_rejected(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "1", "units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [
            {"id": 1, "units": ["u1"]},
            {"id": "1", "units": ["u1"]},
        ],
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: duplicate batch id"):
        render_progress(mig)


def test_render_progress_rejects_empty_batch_id(tmp_path):
    mig = tmp_path / ".migration"
    manifest_sha = _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    (mig / "waves" / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": manifest_sha,
        "batches": [{"id": "", "units": ["u1"]}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: result batch has no id"):
        render_progress(mig)


def test_render_progress_rejects_stale_manifest_units(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    manifest = waves / "wave-1.json"
    manifest.write_text(json.dumps({
        "wave": 1,
        "batches": [{"id": "w1-b01", "units": ["u_a"]}],
    }))
    manifest_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()[:12]
    manifest.write_text(json.dumps({
        "wave": 1,
        "batches": [{"id": "w1-b01", "units": ["u_a", "u_b"]}],
    }))
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": manifest_sha,
        "batches": [{"id": "w1-b01", "status": "PASS"}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: manifest_sha does not match the manifest \(regenerate the result\)"):
        render_progress(mig)


def test_render_progress_rejects_missing_manifest_sha(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    (waves / "wave-1.json").write_text(json.dumps({
        "wave": 1,
        "batches": [{"id": "w1-b01", "units": ["u_a"]}],
    }))
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "batches": [{"id": "w1-b01", "status": "PASS"}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: manifest_sha does not match the manifest \(regenerate the result\)"):
        render_progress(mig)


def test_render_progress_rejects_stale_manifest_with_embedded_units(tmp_path):
    mig = tmp_path / ".migration"
    manifest_sha = _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "manifest_sha": manifest_sha,
        "batches": [{"id": "b1", "units": ["u1"], "status": "PASS"}],
    })
    (mig / "waves" / "wave-1.json").write_text(json.dumps({
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1", "u2"]}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: manifest_sha does not match the manifest \(regenerate the result\)"):
        render_progress(mig)


def test_render_progress_rejects_missing_manifest_sha_with_embedded_units(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    waves = mig / "waves"
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "PASS"}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: manifest_sha does not match the manifest \(regenerate the result\)"):
        render_progress(mig)


def test_render_progress_rejects_missing_manifest_units(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "w1-b01", "status": "PASS"}],
    })

    with pytest.raises(ValueError, match=r"wave-1\.json: manifest batch 'w1-b01' has no units"):
        render_progress(mig)


def test_render_progress_rejects_manifest_without_batch_units(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    manifest_text = json.dumps({
        "wave": 1,
        "batches": [{"id": "w1-b01"}],
    })
    (waves / "wave-1.json").write_text(manifest_text)
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": hashlib.sha256(manifest_text.encode()).hexdigest()[:12],
        "batches": [{"id": "w1-b01", "status": "PASS"}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.json: manifest batch 'w1-b01' has no units"):
        render_progress(mig)


def test_render_progress_rejects_manifest_batch_without_id(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "FAIL"}],
    })

    with pytest.raises(ValueError, match=r"wave-1\.json: manifest batch has no id"):
        render_progress(mig)


def test_render_progress_rejects_result_missing_manifest_batch(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [
        {"id": "b1", "units": ["u1"]},
        {"id": "b2", "units": ["u2"]},
    ])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "PASS"}],
        "verify": {"unit_verdicts": {"b1": "PASS"}},
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | b2 | u2 | MISSING |  |  |  |  |  |" in lines


def test_render_progress_rejects_result_extra_manifest_batch(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [
            {"id": "b1", "units": ["u1"], "status": "PASS"},
            {"id": "b2", "units": ["u2"], "status": "PASS"},
        ],
        "verify": {"unit_verdicts": {"b1": "PASS"}},
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | b2 | u2 | UNPLANNED |  |  |  |  |  |" in lines


def test_render_progress_rejects_embedded_units_different_from_manifest(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u2"], "status": "PASS"}],
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | b1 | u1 | MISSING |  |  |  |  |  |" in lines
    assert "| 1 | b1 | u2 | UNPLANNED |  |  |  |  |  |" in lines


def test_render_progress_rejects_unplanned_batch_without_units(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b2", "status": "PASS"}],
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: batch 'b2' is not in the manifest"):
        render_progress(mig)


def test_render_progress_renders_matching_embedded_units_from_manifest(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1", "u2"]}])
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u2", "u1"], "status": "FAIL"}],
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | b1 | u1 | FAIL |  |  |  |  |  |" in lines
    assert "| 1 | b1 | u2 | FAIL |  |  |  |  |  |" in lines
    assert lines.index("| 1 | b1 | u1 | FAIL |  |  |  |  |  |") < lines.index(
        "| 1 | b1 | u2 | FAIL |  |  |  |  |  |"
    )


def test_render_progress_rejects_result_wave_mismatch(tmp_path):
    mig = tmp_path / ".migration"
    manifest_sha = _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    (mig / "waves" / "wave-1.result.json").write_text(json.dumps({
        "wave": 2,
        "manifest_sha": manifest_sha,
        "batches": [{"id": "b1", "units": ["u1"], "status": "FAIL"}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: wave does not match the manifest"):
        render_progress(mig)


def test_render_progress_rejects_non_integer_manifest_wave(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    manifest_path = mig / "waves" / "wave-1.json"
    manifest_path.write_text(json.dumps({
        "wave": "1",
        "batches": [{"id": "b1", "units": ["u1"]}],
    }))
    _write_result(mig, "wave-1.result.json", {
        "batches": [{"id": "b1", "units": ["u1"], "status": "FAIL"}],
    })

    with pytest.raises(ValueError, match=r"manifest wave is not a non-negative integer"):
        render_progress(mig)


def test_render_progress_uses_manifest_wave_without_result_wave(tmp_path):
    mig = tmp_path / ".migration"
    _write_manifest(mig, 3, [{"id": "b1", "units": ["u1"]}])
    _write_result(mig, "wave-3.result.json", {
        "batches": [{"id": "b1", "units": ["u1"], "status": "FAIL"}],
    })

    assert "| 3 | b1 | u1 | FAIL |  |  |  |  |  |" in render_progress(mig)


def test_render_progress_rejects_non_list_result_batches(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": {},
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: result batches is not a list"):
        render_progress(mig)


def test_result_without_batches_raises(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "closed": True,
        "manifest_sha": "x",
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: result batches is not a list"):
        render_progress(mig)


def test_render_progress_preserves_manifest_order_and_lists_wave_status(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-2.result.json", {
        "wave": 2,
        "closed": False,
        "auto_merge": True,
        "breaker_tripped_on": "timeout",
        "batches": [
            {"id": "b", "units": ["u2", "u1"], "status": "PASS",
             "recon_verdict": "PASS", "recon_cost": {"rows": 2}},
        ],
        "verify": {"unit_verdicts": {"b": "PASS"}, "merged_prs": []},
    })
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "closed": True,
        "auto_merge": False,
        "batches": [
            {"id": "z", "units": ["u3"], "status": "FAIL", "recon_verdict": "FAIL"},
            {"id": "a", "units": ["u2"], "status": "PASS", "recon_verdict": "PASS",
             "pr_url": "https://example.invalid/2"},
        ],
        "verify": {"unit_verdicts": {"z": "FAIL", "a": "PASS"}, "merged_prs": []},
    })

    lines = render_progress(mig).splitlines()

    assert lines[0].startswith("<!-- generated by skills/migration-fanout/progress.py")
    assert lines[1] == "| wave | batch | unit | status | recon_verdict | verifier_verdict | pr_url | merged | batch_recon_cost |"
    assert "| 1 | a | u2 | PASS (unmerged) | PASS | PASS | https://example.invalid/2 | pending |  |" in lines
    assert '| 2 | b | u2 | PASS (unmerged) | PASS | PASS |  | pending | {"rows":2} |' in lines
    assert '| 2 | b | u1 | PASS (unmerged) | PASS | PASS |  | pending |  |' in lines
    assert lines.index("| 1 | z | u3 | FAIL | FAIL | FAIL |  |  |  |") < \
        lines.index("| 1 | a | u2 | PASS (unmerged) | PASS | PASS | https://example.invalid/2 | pending |  |")
    assert "wave 1: closed=true, auto_merge=false, breaker_tripped_on=-" in lines
    assert "wave 2: closed=false, auto_merge=true, breaker_tripped_on=timeout" in lines


def test_render_progress_includes_verifier_verdicts(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [
            {"id": "pass", "units": ["u1"], "status": "PASS", "recon_verdict": "PASS"},
            {"id": "other", "units": ["u2"], "status": "PASS", "recon_verdict": "PASS"},
        ],
        "verify": {"unit_verdicts": {"pass": "FAIL", "other": "PASS"}},
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | pass | u1 | FAIL | PASS | FAIL |  |  |  |" in lines
    assert "| 1 | other | u2 | PASS (unmerged) | PASS | PASS |  | pending |  |" in lines


def test_verifier_verdict_does_not_revive_failed_batch(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "FAIL"}],
        "verify": {"unit_verdicts": {"b1": "PASS"}},
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | b1 | u1 | FAIL |  | PASS |  |  |  |" in lines


def test_render_progress_marks_missing_verifier_batch_unverified(tmp_path):
    mig = tmp_path / ".migration"
    result = {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "PASS", "recon_verdict": "PASS"}],
        "verify": {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": ["b1"]},
    }
    _write_result(mig, "wave-1.result.json", result)

    assert "| 1 | b1 | u1 | UNVERIFIED | PASS |  |  |  |" in render_progress(mig)

    result.pop("verify")
    (mig / "waves" / "wave-1.result.json").write_text(json.dumps(result))

    assert "| 1 | b1 | u1 | UNVERIFIED | PASS |  |  |  |" in render_progress(mig)


def test_render_progress_marks_merged_and_pending_prs(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "auto_merge": True,
        "batches": [
            {"id": "merged", "units": ["u1"], "status": "PASS",
             "recon_verdict": "PASS", "pr_url": "https://example.invalid/1"},
            {"id": "pending", "units": ["u2"], "status": "PASS",
             "recon_verdict": "PASS", "pr_url": "https://example.invalid/2"},
            {"id": "failed", "units": ["u3"], "status": "FAIL",
             "recon_verdict": "FAIL", "pr_url": "https://example.invalid/3"},
        ],
        "verify": {
            "unit_verdicts": {"merged": "PASS", "pending": "PASS", "failed": "FAIL"},
            "merged_prs": ["https://example.invalid/1"],
        },
    })

    lines = render_progress(mig).splitlines()

    assert "| 1 | merged | u1 | PASS (unmerged) | PASS | PASS | https://example.invalid/1 | REPORTED, UNPROVEN |  |" in lines
    assert "| 1 | pending | u2 | PASS (unmerged) | PASS | PASS | https://example.invalid/2 | pending |  |" in lines
    assert "| 1 | failed | u3 | FAIL | FAIL | FAIL | https://example.invalid/3 |  |  |" in lines


def test_render_reports_unproven_verifier_merge(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{
            "id": "b1", "units": ["u1"], "status": "PASS",
            "pr_url": "https://example.invalid/1", "pr_head": "head-1",
        }],
        "verify": {
            "unit_verdicts": {"b1": "PASS"},
            "merged_prs": ["https://example.invalid/1"],
        },
    })

    assert (
        "| 1 | b1 | u1 | PASS (unmerged) |  | PASS | https://example.invalid/1 | "
        "REPORTED, UNPROVEN |  |"
    ) in render_progress(mig)


def test_render_reports_a_wave_close_merge_as_reported_unproven(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{
            "id": "b1", "units": ["u1"], "status": "PASS",
            "pr_url": "https://example.invalid/1", "pr_head": "head-1",
        }],
        "verify": {"unit_verdicts": {"b1": "PASS"}},
        "close": {"merged_prs": ["https://example.invalid/1"], "unmerged": [], "changed_paths": []},
    })

    assert (
        "| 1 | b1 | u1 | PASS (unmerged) |  | PASS | https://example.invalid/1 | "
        "REPORTED, UNPROVEN |  |"
    ) in render_progress(mig)


def test_render_rejects_a_malformed_close_record(tmp_path):
    for i, close in enumerate(("nope", {"merged_prs": "nope", "unmerged": [], "changed_paths": []})):
        mig = tmp_path / str(i) / ".migration"
        _write_result(mig, "wave-1.result.json", {
            "wave": 1,
            "batches": [{"id": "b1", "units": ["u1"], "status": "PASS",
                         "pr_url": "https://example.invalid/1", "pr_head": "head-1"}],
            "verify": {"unit_verdicts": {"b1": "PASS"}},
            "close": close,
        })
        with pytest.raises(ValueError, match="close"):
            render_progress(mig)


def test_render_progress_uses_matching_merged_record_head(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "auto_merge": False,
        "batches": [{
            "id": "b1", "units": ["u1"], "status": "PASS",
            "pr_url": "https://example.invalid/1", "pr_head": "head-1",
        }],
        "verify": {"unit_verdicts": {"b1": "PASS"}, "merged_prs": []},
    })
    (mig / "waves" / "wave-1.merged.json").write_text(json.dumps({
        "base": "base-1",
        "merged": {"https://example.invalid/1": "head-1"},
    }))

    assert "| 1 | b1 | u1 | PASS |  | PASS | https://example.invalid/1 | yes |  |" in render_progress(mig)


def test_render_progress_ignores_stale_merged_record_head(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "auto_merge": False,
        "batches": [{
            "id": "b1", "units": ["u1"], "status": "PASS",
            "pr_url": "https://example.invalid/1", "pr_head": "head-2",
        }],
        "verify": {"unit_verdicts": {"b1": "PASS"}, "merged_prs": []},
    })
    (mig / "waves" / "wave-1.merged.json").write_text(json.dumps({
        "base": "base-1",
        "merged": {"https://example.invalid/1": "head-1"},
    }))

    assert "| 1 | b1 | u1 | PASS (unmerged) |  | PASS | https://example.invalid/1 | pending |  |" in render_progress(mig)


def test_render_progress_rejects_malformed_merged_record(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "PASS"}],
    })
    (mig / "waves" / "wave-1.merged.json").write_text(json.dumps({
        "base": "base-1",
        "merged": [],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.merged\.json: merged record merged is not a dict"):
        render_progress(mig)


def test_refresh_merged_records_ancestral_heads(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "test@example.invalid")
    git("config", "user.name", "Progress Test")
    (repo / "tracked.txt").write_text("base\n")
    git("add", "tracked.txt")
    git("commit", "-m", "base")
    (repo / "tracked.txt").write_text("pr\n")
    git("commit", "-am", "pr")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-b", "unrelated", "HEAD~1")
    (repo / "unrelated.txt").write_text("unrelated\n")
    git("add", "unrelated.txt")
    git("commit", "-m", "unrelated")
    unrelated_head = git("rev-parse", "HEAD")
    git("checkout", "main")

    bare = tmp_path / "origin.git"
    subprocess.run(["git", "clone", "--bare", str(repo), str(bare)], check=True, capture_output=True, text=True)
    git("remote", "add", "origin", str(bare))

    mig = repo / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    url_1 = "https://example.invalid/1"
    url_2 = "https://example.invalid/2"
    _write_manifest(mig, 1, [
        {"id": "b1", "units": ["u1"]},
        {"id": "b2", "units": ["u2"]},
    ], base_branch="main")
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "auto_merge": False,
        "batches": [
            {"id": "b1", "units": ["u1"], "status": "PASS", "pr_url": url_1, "pr_head": pr_head},
            {"id": "b2", "units": ["u2"], "status": "PASS", "pr_url": url_2, "pr_head": unrelated_head},
        ],
        "verify": {"unit_verdicts": {"b1": "PASS", "b2": "PASS"}, "merged_prs": []},
    })

    refresh_merged(mig)

    record = json.loads((waves / "wave-1.merged.json").read_text())
    assert record["merged"] == {url_1: pr_head}
    lines = render_progress(mig).splitlines()
    assert f"| 1 | b1 | u1 | PASS |  | PASS | {url_1} | yes |  |" in lines
    assert f"| 1 | b2 | u2 | PASS (unmerged) |  | PASS | {url_2} | pending |  |" in lines


def test_refresh_merged_rebuilds_stale_record(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    base_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    git("merge", "--no-ff", "-q", "feature", "-m", "merge feature")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/stale")

    refresh_merged(mig)
    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {
        "https://example.invalid/stale": pr_head,
    }

    git("branch", "other", base_head)
    git("push", "-q", "origin", "other")
    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}], base_branch="other")
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "auto_merge": False,
        "batches": [{
            "id": "b1",
            "units": ["u1"],
            "status": "PASS",
            "recon_verdict": "PASS",
            "pr_url": "https://example.invalid/stale",
            "pr_head": pr_head,
            "recon_cost": {},
        }],
        "verify": {"unit_verdicts": {"b1": "PASS"}, "merged_prs": []},
    })

    refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {}
    assert "| 1 | b1 | u1 | PASS (unmerged) | PASS | PASS | https://example.invalid/stale | pending | {} |" in render_progress(mig)


def test_refresh_merged_leaves_record_when_manifest_is_stale(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    git("merge", "--no-ff", "-q", "feature", "-m", "merge feature")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/stale-manifest")

    refresh_merged(mig)
    original = json.loads((waves / "wave-1.merged.json").read_text())

    _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}], base_branch="other")

    with pytest.raises(ValueError, match=r"manifest_sha does not match"):
        refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text()) == original


def _refresh_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "-q", "-b", "base")
    git("config", "user.email", "test@example.invalid")
    git("config", "user.name", "Progress Test")
    (repo / "base.txt").write_text("base\n")
    git("add", "base.txt")
    git("commit", "-q", "-m", "base")
    bare = tmp_path / "origin.git"
    subprocess.run(
        ["git", "clone", "--bare", str(repo), str(bare)],
        check=True,
        capture_output=True,
        text=True,
    )
    git("remote", "add", "origin", str(bare))
    git("push", "-q", "origin", "base")
    return repo, git


def _refresh_result(repo, pr_head, pr_url):
    mig = repo / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    _write_manifest(
        mig,
        1,
        [{"id": "b1", "units": ["u1"]}],
        base_branch="base",
    )
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "closed": True,
        "auto_merge": False,
        "breaker_tripped_on": None,
        "batches": [{
            "id": "b1",
            "units": ["u1"],
            "status": "PASS",
            "recon_verdict": "PASS",
            "pr_url": pr_url,
            "pr_head": pr_head,
            "recon_cost": {},
        }],
        "verify": {"unit_verdicts": {"b1": "PASS"}, "merged_prs": []},
    })
    return mig, waves


def test_refresh_merged_detects_merge_commit(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    git("merge", "--no-ff", "-q", "feature", "-m", "merge feature")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/merge")

    refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {
        "https://example.invalid/merge": pr_head,
    }
    assert "| 1 | b1 | u1 | PASS | PASS | PASS | https://example.invalid/merge | yes | {} |" in render_progress(mig)


def test_refresh_merged_verifies_reported_merges(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    git("merge", "--no-ff", "-q", "feature", "-m", "merge feature")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/reported")
    result_path = waves / "wave-1.result.json"
    result = json.loads(result_path.read_text())
    result["auto_merge"] = True
    result["verify"]["merged_prs"] = ["https://example.invalid/reported"]
    result_path.write_text(json.dumps(result))

    refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {
        "https://example.invalid/reported": pr_head,
    }
    assert "| 1 | b1 | u1 | PASS | PASS | PASS | https://example.invalid/reported | yes | {} |" in render_progress(mig)


def test_refresh_merged_detects_rebase_merge(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    (repo / "base.txt").write_text("base update\n")
    git("commit", "-q", "-am", "base update")
    git("checkout", "-q", "feature")
    git("rebase", "-q", "base")
    git("checkout", "-q", "base")
    git("merge", "--ff-only", "-q", "feature")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/rebase")

    refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {
        "https://example.invalid/rebase": pr_head,
    }


def test_refresh_merged_detects_squash_merge(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("part one\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature one")
    (repo / "feature.txt").write_text("part one\npart two\n")
    git("commit", "-q", "-am", "feature two")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    git("merge", "--squash", "-q", "feature")
    git("commit", "-q", "-m", "squash feature")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/squash")

    refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {
        "https://example.invalid/squash": pr_head,
    }


def test_refresh_merged_accepts_partial_then_squash(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "a.txt").write_text("a\n")
    (repo / "b.txt").write_text("b\n")
    git("add", "a.txt", "b.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    (repo / "a.txt").write_text("a\n")
    git("add", "a.txt")
    git("commit", "-q", "-m", "base a")
    (repo / "b.txt").write_text("b\n")
    git("add", "b.txt")
    git("commit", "-q", "-m", "base b")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/partial-squash")

    refresh_merged(mig)

    assert json.loads((waves / "wave-1.merged.json").read_text())["merged"] == {
        "https://example.invalid/partial-squash": pr_head,
    }


def test_refresh_merged_rejects_whitespace_equivalent_commit(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "shared.txt").write_text("before\n    x = 1\n")
    git("add", "shared.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    (repo / "shared.txt").write_text("before\nx = 1\n")
    git("add", "shared.txt")
    git("commit", "-q", "-m", "base equivalent")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/whitespace")

    refresh_merged(mig)

    assert not (waves / "wave-1.merged.json").exists()


def test_refresh_merged_rejects_whitespace_equivalent_commit_with_quoted_path(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "café.sql").write_text("before\n    x = 1\n")
    git("add", "café.sql")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    (repo / "café.sql").write_text("before\nx = 1\n")
    git("add", "café.sql")
    git("commit", "-q", "-m", "base equivalent")
    git("push", "-q", "origin", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/quoted-whitespace")

    refresh_merged(mig)

    assert not (waves / "wave-1.merged.json").exists()


def test_refresh_merged_leaves_unmerged_pr_unrecorded(tmp_path):
    repo, git = _refresh_repo(tmp_path)
    git("checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    git("add", "feature.txt")
    git("commit", "-q", "-m", "feature")
    pr_head = git("rev-parse", "HEAD")
    git("checkout", "-q", "base")
    mig, waves = _refresh_result(repo, pr_head, "https://example.invalid/open")

    refresh_merged(mig)

    assert not (waves / "wave-1.merged.json").exists()


def test_refresh_merged_unknown_head_does_not_consult_gh(tmp_path, monkeypatch):
    repo, git = _refresh_repo(tmp_path)
    marker = tmp_path / "gh-called"
    fake_gh = tmp_path / "gh"
    fake_gh.write_text(f"#!/bin/sh\n: > {marker}\n")
    fake_gh.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    mig, waves = _refresh_result(
        repo,
        "ffffffffffffffffffffffffffffffffffffffff",
        "https://github.com/example/repo/pull/1",
    )

    refresh_merged(mig)

    assert not (waves / "wave-1.merged.json").exists()
    assert not marker.exists()


def test_render_progress_rejects_invalid_verifier_unit_verdicts(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b", "units": ["u"], "status": "PASS"}],
        "verify": {"unit_verdicts": []},
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: verify unit_verdicts is not a dict"):
        render_progress(mig)


def test_render_progress_rejects_non_object_batch(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"]}, None],
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: result batch is not an object"):
        render_progress(mig)


def test_render_progress_rejects_batch_without_id(tmp_path):
    mig = tmp_path / ".migration"
    manifest_sha = _write_manifest(mig, 1, [{"id": "b1", "units": ["u1"]}])
    (mig / "waves" / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": manifest_sha,
        "batches": [{"units": ["u1"]}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: result batch has no id"):
        render_progress(mig)


def test_render_progress_rejects_empty_batch_units(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "b1", "units": []}],
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: batch 'b1' has no units"):
        render_progress(mig)


def test_render_progress_empty_waves_has_header_and_table(tmp_path):
    text = render_progress(tmp_path / ".migration")

    assert text.splitlines() == [
        "<!-- generated by skills/migration-fanout/progress.py from waves/*.result.json; do not edit -->",
        "| wave | batch | unit | status | recon_verdict | verifier_verdict | pr_url | merged | batch_recon_cost |",
        "|---|---|---|---|---|---|---|---|---|",
    ]


def test_render_progress_rejects_invalid_json(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    (waves / "wave-1.result.json").write_text("{not json")
    (waves / "wave-2.result.json").write_text(json.dumps({
        "wave": 2, "batches": [], "closed": True, "auto_merge": False,
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: not a JSON object"):
        render_progress(mig)


def test_main_writes_progress_file(tmp_path, capsys):
    mig = tmp_path / ".migration"
    (mig / "waves").mkdir(parents=True)

    assert main([str(mig)]) == 0
    assert (mig / "05_progress.md").read_text().startswith(
        "<!-- generated by skills/migration-fanout/progress.py"
    )
    assert capsys.readouterr().out.strip() == str(mig / "05_progress.md")


def test_main_does_not_write_progress_file_for_invalid_result(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    (waves / "wave-1.result.json").write_text("{not json")

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: not a JSON object"):
        main([str(mig)])

    assert not (mig / "05_progress.md").exists()
