import json
import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).with_name("derive_waves.py")


def _unit(root, name, reads=(), writes=(), mapping=True):
    path = root / ".migration" / "units" / name
    path.mkdir(parents=True, exist_ok=True)
    row = {"routine": f"{name}_routine", "reads": list(reads), "writes": list(writes), "calls": []}
    (path / "dependencies.json").write_text(json.dumps({"routines": [row]}))
    if mapping:
        objects = [{"object": table, "root_table": table} for table in writes]
        (path / "mapping_spec.json").write_text(json.dumps({"objects": objects}))


def _run(root, *args):
    return subprocess.run([sys.executable, str(SCRIPT), "--root", str(root), *args],
                          capture_output=True, text=True)


def _derived(root, *args):
    result = _run(root, *args)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _shared_fixture(root):
    _unit(root, "s", writes=["ts"])
    _unit(root, "a", reads=["ts"], writes=["ta"])
    _unit(root, "b", reads=["ts"], writes=["tb"])


def test_chain_is_scheduled_leaf_first_with_dependency_edges(tmp_path):
    _unit(tmp_path, "a", reads=["tb"], writes=["ta"])
    _unit(tmp_path, "b", reads=["tc"], writes=["tb"])
    _unit(tmp_path, "c", writes=["tc"])

    result = _derived(tmp_path)
    waves = {unit: entry["wave"] for entry in result["waves"] for batch in entry["batches"]
             for unit in batch["units"]}
    assert waves == {"c": 1, "b": 2, "a": 3}
    assert result["units"]["a"]["depends_on"] == ["b"]
    assert result["units"]["b"]["depends_on"] == ["c"]
    assert result["units"]["c"]["depends_on"] == []


def test_two_unit_cycle_stays_in_one_batch(tmp_path):
    _unit(tmp_path, "a", reads=["tb"], writes=["ta"])
    _unit(tmp_path, "b", reads=["ta"], writes=["tb"])

    result = _derived(tmp_path)
    assert result["waves"] == [{"wave": 1, "batches": [
        {"id": "w1-b01", "units": ["a", "b"], "write_targets": ["ta", "tb"]}
    ]}]


def test_cycle_over_maximum_refuses_and_names_every_unit(tmp_path):
    units = ["a", "b", "c", "d", "e"]
    for i, unit in enumerate(units):
        _unit(tmp_path, unit, reads=[f"t{(i + 1) % len(units)}"], writes=[f"t{i}"])

    result = _run(tmp_path, "--max-cycle", "4")
    assert result.returncode != 0
    assert all(unit in result.stderr for unit in units)


def test_next_fit_packing_is_stable(tmp_path):
    for unit in ("c", "a", "b"):
        _unit(tmp_path, unit, writes=[f"t{unit}"])

    first = _run(tmp_path, "--batch-max", "2")
    second = _run(tmp_path, "--batch-max", "2")
    assert first.returncode == second.returncode == 0
    assert first.stdout.encode() == second.stdout.encode()
    waves = json.loads(first.stdout)["waves"]
    assert [batch["id"] for batch in waves[0]["batches"]] == ["w1-b01", "w1-b02"]
    assert [batch["units"] for batch in waves[0]["batches"]] == [["a", "b"], ["c"]]


def test_units_writing_the_same_target_share_a_batch(tmp_path):
    _unit(tmp_path, "a", writes=["shared"])
    _unit(tmp_path, "b", writes=["shared"])

    result = _derived(tmp_path)
    assert result["waves"][0]["batches"][0]["units"] == ["a", "b"]


def test_shared_leaf_stays_in_wave_one_without_the_opt_in_flag(tmp_path):
    _shared_fixture(tmp_path)

    result = _derived(tmp_path)
    assert "shared_not_wave_0" not in result
    assert result["waves"][0]["wave"] == 1
    assert result["waves"][0]["batches"][0]["units"] == ["s"]


def test_shared_leaf_uses_wave_zero_and_its_readers_start_at_wave_one(tmp_path):
    _shared_fixture(tmp_path)

    result = _derived(tmp_path, "--shared-wave-0")
    assert result["waves"][0] == {"wave": 0, "batches": [
        {"id": "w0-b01", "units": ["s"], "write_targets": ["ts"]}
    ]}
    assert result["waves"][1]["wave"] == 1
    assert result["waves"][1]["batches"][0]["units"] == ["a", "b"]
    assert result["shared_not_wave_0"] == []


def test_multiple_shared_leaves_get_one_wave_zero_batch_each(tmp_path):
    _unit(tmp_path, "s1", writes=["ts1"])
    _unit(tmp_path, "s2", writes=["ts2"])
    _unit(tmp_path, "a", reads=["ts1"], writes=["ta"])
    _unit(tmp_path, "b", reads=["ts1"], writes=["tb"])
    _unit(tmp_path, "c", reads=["ts2"], writes=["tc"])
    _unit(tmp_path, "d", reads=["ts2"], writes=["td"])

    result = _derived(tmp_path, "--shared-wave-0")
    assert result["waves"][0] == {"wave": 0, "batches": [
        {"id": "w0-b01", "units": ["s1"], "write_targets": ["ts1"]},
        {"id": "w0-b02", "units": ["s2"], "write_targets": ["ts2"]},
    ]}


def test_shared_unit_with_dependencies_stays_normal_and_is_reported(tmp_path):
    _unit(tmp_path, "base", writes=["tbase"])
    _unit(tmp_path, "s", reads=["tbase"], writes=["ts"])
    _unit(tmp_path, "a", reads=["ts"], writes=["ta"])
    _unit(tmp_path, "b", reads=["ts"], writes=["tb"])

    result = _derived(tmp_path, "--shared-wave-0")
    assert all(entry["wave"] != 0 for entry in result["waves"])
    assert result["shared_not_wave_0"] == [
        {"unit": "s", "readers": ["a", "b"], "reason": "depends on base"}
    ]


def test_first_wave_zero_requires_shared_wave_zero_flag(tmp_path):
    _shared_fixture(tmp_path)

    result = _run(tmp_path, "--first-wave", "0")
    assert result.returncode != 0
    assert "wave 0 is the serial shared-objects wave; pass --shared-wave-0 instead" in result.stderr


def test_check_accepts_wave_zero_and_normal_wave_layout(tmp_path):
    _shared_fixture(tmp_path)
    derived = _derived(tmp_path, "--shared-wave-0")
    waves_dir = tmp_path / ".migration" / "waves"
    waves_dir.mkdir(parents=True)
    for entry in derived["waves"]:
        manifest = {"wave": entry["wave"], "batches": entry["batches"]}
        (waves_dir / f"wave-{entry['wave']}.json").write_text(json.dumps(manifest))

    result = _run(tmp_path, "--check", ".migration/waves/wave-0.json", "--shared-wave-0")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "consistent with the derived dependency order" in result.stdout


def test_check_finds_dependency_units_split_across_batches_in_one_wave(tmp_path):
    _unit(tmp_path, "a", reads=["tb"], writes=["ta"])
    _unit(tmp_path, "b", writes=["tb"])
    waves_dir = tmp_path / ".migration" / "waves"
    waves_dir.mkdir(parents=True)
    manifest = waves_dir / "wave-1.json"
    manifest.write_text(json.dumps({
        "wave": 1,
        "batches": [
            {"id": "batch-a", "units": ["a"]},
            {"id": "batch-b", "units": ["b"]},
        ],
    }))

    result = _run(tmp_path, "--check", ".migration/waves/wave-1.json")
    assert result.returncode == 1
    assert all(value in result.stdout for value in ("a", "b", "wave 1", "batch-a", "batch-b"))


def test_check_accepts_derived_batches_across_sibling_waves(tmp_path):
    _unit(tmp_path, "a", reads=["tb"], writes=["ta"])
    _unit(tmp_path, "b", writes=["tb"])
    derived = _derived(tmp_path)
    waves_dir = tmp_path / ".migration" / "waves"
    waves_dir.mkdir(parents=True)
    for entry in derived["waves"]:
        manifest = {"wave": entry["wave"], "batches": entry["batches"]}
        (waves_dir / f"wave-{entry['wave']}.json").write_text(json.dumps(manifest))

    result = _run(tmp_path, "--check", ".migration/waves/wave-1.json")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "consistent with the derived dependency order" in result.stdout


def test_out_under_migration_is_refused(tmp_path):
    _unit(tmp_path, "a", writes=["ta"])

    result = _run(tmp_path, "--out", str(tmp_path / ".migration" / "waves.json"))
    assert result.returncode != 0
    assert "write it outside .migration/" in result.stderr


def test_unmapped_write_refuses_with_unit_routine_and_table(tmp_path):
    _unit(tmp_path, "unit_a", writes=["missing"], mapping=False)
    (tmp_path / ".migration" / "units" / "unit_a" / "mapping_spec.json").write_text(
        json.dumps({"objects": []}))

    result = _run(tmp_path)
    assert result.returncode != 0
    assert all(value in result.stderr for value in ("unit_a", "unit_a_routine", "missing"))


def test_writing_unit_without_mapping_spec_is_refused(tmp_path):
    _unit(tmp_path, "unit_a", writes=["missing"], mapping=False)

    result = _run(tmp_path)
    assert result.returncode != 0
    assert all(value in result.stderr for value in ("unit_a", "missing", "mapping_spec.json"))


@pytest.mark.parametrize(("option", "value"), [
    ("--namespace", "not a namespace"),
    ("--batch-max", "0"),
    ("--max-cycle", "0"),
    ("--first-wave", "-1"),
])
def test_invalid_options_refuse_with_actionable_messages(tmp_path, option, value):
    _unit(tmp_path, "a", writes=["ta"])

    result = _run(tmp_path, option, value)
    assert result.returncode != 0
    assert option in result.stderr
