import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from progress import main, render_progress


def _write_result(mig, name, result):
    waves = mig / "waves"
    waves.mkdir(parents=True, exist_ok=True)
    (waves / name).write_text(json.dumps(result))


def test_render_progress_uses_manifest_units_when_result_omits_them(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    manifest_text = json.dumps({
        "wave": 1,
        "batches": [{"id": "w1-b01", "units": ["u_a", "u_b"]}],
    })
    (waves / "wave-1.json").write_text(manifest_text)
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": hashlib.sha256(manifest_text.encode()).hexdigest()[:12],
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
        "verify": None,
    }))

    text = render_progress(mig)

    assert "| 1 | w1-b01 | u_a | PASS | PASS |  | https://example.invalid/1 | {\"rows\":2} |" in text
    assert "| 1 | w1-b01 | u_b | PASS | PASS |  | https://example.invalid/1 | {\"rows\":2} |" in text


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

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: batch 'w1-b01' has no units and manifest_sha does not match"):
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

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: batch 'w1-b01' has no units and manifest_sha does not match"):
        render_progress(mig)


def test_render_progress_rejects_missing_manifest_units(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "w1-b01", "status": "PASS"}],
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: batch 'w1-b01' has no units and no readable manifest entry"):
        render_progress(mig)


def test_render_progress_rejects_manifest_without_batch_units(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    manifest_text = json.dumps({
        "wave": 1,
        "batches": [{"id": "other", "units": ["u_a"]}],
    })
    (waves / "wave-1.json").write_text(manifest_text)
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": hashlib.sha256(manifest_text.encode()).hexdigest()[:12],
        "batches": [{"id": "w1-b01", "status": "PASS"}],
    }))

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: batch 'w1-b01' has no units and no readable manifest entry"):
        render_progress(mig)


def test_render_progress_rejects_non_list_result_batches(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": {},
    })

    with pytest.raises(ValueError, match=r"wave-1\.result\.json: result batches is not a list"):
        render_progress(mig)


def test_render_progress_sorts_rows_and_lists_wave_status(tmp_path):
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
    })

    lines = render_progress(mig).splitlines()

    assert lines[0].startswith("<!-- generated by skills/migration-fanout/progress.py")
    assert lines[1] == "| wave | batch | unit | status | recon_verdict | verifier_verdict | pr_url | recon_cost |"
    assert "| 1 | a | u2 | PASS | PASS |  | https://example.invalid/2 |  |" in lines
    assert '| 2 | b | u1 | PASS | PASS |  |  | {"rows":2} |' in lines
    assert lines.index("| 1 | a | u2 | PASS | PASS |  | https://example.invalid/2 |  |") < \
        lines.index("| 1 | z | u3 | FAIL | FAIL |  |  |  |")
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

    assert "| 1 | pass | u1 | FAIL | PASS | FAIL |  |  |" in lines
    assert "| 1 | other | u2 | PASS | PASS | PASS |  |  |" in lines


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
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"units": ["u1"]}],
    })

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
        "| wave | batch | unit | status | recon_verdict | verifier_verdict | pr_url | recon_cost |",
        "|---|---|---|---|---|---|---|---|",
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
