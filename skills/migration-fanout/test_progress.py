import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from progress import main, refresh_merged, render_progress


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

    assert "| 1 | w1-b01 | u_a | PASS | PASS |  | https://example.invalid/1 | pending | {\"rows\":2} |" in text
    assert "| 1 | w1-b01 | u_b | PASS | PASS |  | https://example.invalid/1 | pending |  |" in text


def test_render_progress_normalizes_numeric_batch_ids(tmp_path):
    mig = tmp_path / ".migration"
    waves = mig / "waves"
    waves.mkdir(parents=True)
    manifest_text = json.dumps({
        "wave": 1,
        "batches": [{"id": 1, "units": ["u1"]}],
    })
    (waves / "wave-1.json").write_text(manifest_text)
    (waves / "wave-1.result.json").write_text(json.dumps({
        "wave": 1,
        "manifest_sha": hashlib.sha256(manifest_text.encode()).hexdigest()[:12],
        "batches": [{"id": 1, "status": "PASS", "recon_verdict": "PASS"}],
        "verify": {"unit_verdicts": {"1": "FAIL"}},
    }))

    text = render_progress(mig)

    assert "| 1 | 1 | u1 | FAIL | PASS | FAIL |  |  |" in text


def test_render_progress_rejects_empty_batch_id(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{"id": "", "units": ["u1"]}],
    })

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


def test_result_without_batches_raises(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "closed": True,
        "manifest_sha": "x",
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
    assert lines[1] == "| wave | batch | unit | status | recon_verdict | verifier_verdict | pr_url | merged | batch_recon_cost |"
    assert "| 1 | a | u2 | PASS | PASS |  | https://example.invalid/2 | pending |  |" in lines
    assert '| 2 | b | u1 | PASS | PASS |  |  | pending | {"rows":2} |' in lines
    assert '| 2 | b | u2 | PASS | PASS |  |  | pending |  |' in lines
    assert lines.index("| 1 | a | u2 | PASS | PASS |  | https://example.invalid/2 | pending |  |") < \
        lines.index("| 1 | z | u3 | FAIL | FAIL |  |  |  |  |")
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
    assert "| 1 | other | u2 | PASS | PASS | PASS |  | pending |  |" in lines


def test_render_progress_marks_missing_verifier_batch_unverified(tmp_path):
    mig = tmp_path / ".migration"
    result = {
        "wave": 1,
        "batches": [{"id": "b1", "units": ["u1"], "status": "PASS", "recon_verdict": "PASS"}],
        "verify": {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": ["b1"]},
    }
    _write_result(mig, "wave-1.result.json", result)

    assert "| 1 | b1 | u1 | UNVERIFIED | PASS |  |  |  |" in render_progress(mig)

    result["verify"] = None
    (mig / "waves" / "wave-1.result.json").write_text(json.dumps(result))

    assert "| 1 | b1 | u1 | PASS | PASS |  |  | pending |  |" in render_progress(mig)


def test_render_progress_marks_merged_and_pending_prs(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
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

    assert "| 1 | merged | u1 | PASS | PASS | PASS | https://example.invalid/1 | yes |  |" in lines
    assert "| 1 | pending | u2 | PASS | PASS | PASS | https://example.invalid/2 | pending |  |" in lines
    assert "| 1 | failed | u3 | FAIL | FAIL | FAIL | https://example.invalid/3 |  |  |" in lines


def test_render_progress_uses_matching_merged_record_head(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{
            "id": "b1", "units": ["u1"], "status": "PASS",
            "pr_url": "https://example.invalid/1", "pr_head": "head-1",
        }],
    })
    (mig / "waves" / "wave-1.merged.json").write_text(json.dumps({
        "base": "base-1",
        "merged": {"https://example.invalid/1": "head-1"},
    }))

    assert "| 1 | b1 | u1 | PASS |  |  | https://example.invalid/1 | yes |  |" in render_progress(mig)


def test_render_progress_ignores_stale_merged_record_head(tmp_path):
    mig = tmp_path / ".migration"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "batches": [{
            "id": "b1", "units": ["u1"], "status": "PASS",
            "pr_url": "https://example.invalid/1", "pr_head": "head-2",
        }],
    })
    (mig / "waves" / "wave-1.merged.json").write_text(json.dumps({
        "base": "base-1",
        "merged": {"https://example.invalid/1": "head-1"},
    }))

    assert "| 1 | b1 | u1 | PASS |  |  | https://example.invalid/1 | pending |  |" in render_progress(mig)


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
    (waves / "wave-1.json").write_text(json.dumps({"wave": 1, "base_branch": "main"}))
    url_1 = "https://example.invalid/1"
    url_2 = "https://example.invalid/2"
    _write_result(mig, "wave-1.result.json", {
        "wave": 1,
        "auto_merge": False,
        "batches": [
            {"id": "b1", "units": ["u1"], "status": "PASS", "pr_url": url_1, "pr_head": pr_head},
            {"id": "b2", "units": ["u2"], "status": "PASS", "pr_url": url_2, "pr_head": unrelated_head},
        ],
    })

    refresh_merged(mig)

    record = json.loads((waves / "wave-1.merged.json").read_text())
    assert record["merged"] == {url_1: pr_head}
    lines = render_progress(mig).splitlines()
    assert f"| 1 | b1 | u1 | PASS |  |  | {url_1} | yes |  |" in lines
    assert f"| 1 | b2 | u2 | PASS |  |  | {url_2} | pending |  |" in lines


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
