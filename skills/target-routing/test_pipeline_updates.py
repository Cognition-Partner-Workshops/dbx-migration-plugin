import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from pipeline_updates import check_manifest, main  # noqa: E402


def _manifest(batches, **extra):
    return {"wave": 2, "width": 20, "batches": batches, **extra}


def _batch(bid, pipelines):
    return {"id": bid, "units": [f"{bid}_u"], "write_targets": ["mig.t"], "lakeflow_pipelines": pipelines}


def test_distinct_pipelines_pass():
    r = check_manifest(_manifest([_batch("b1", ["mig.orders"]), _batch("b2", ["mig.ledger"])]))
    assert r["status"] == "pass"
    assert r["shared"] == [] and r["unchecked_batches"] == []


def test_one_pipeline_in_two_batches_of_a_wide_wave_halts_naming_both():
    r = check_manifest(_manifest([_batch("b1", ["mig.orders"]), _batch("b2", ["MIG.Orders", "mig.x"])]))
    assert r["status"] == "halt"
    assert r["shared"] == [{"pipeline": "mig.orders", "batches": ["b1", "b2"], "serialized": False}]


def test_a_serial_wave_may_share_a_pipeline():
    m = _manifest([_batch("b1", ["p"]), _batch("b2", ["p"])], width=1)
    r = check_manifest(m)
    assert r["status"] == "pass"
    assert r["shared"] == [{"pipeline": "p", "batches": ["b1", "b2"], "serialized": True}]
    assert r["order"] == {"b2": ["b1"]}


LEDGER = ("| D-31 | 2026-02-01 | user:U1 | pipeline_serialized p: b1 then b2 | one update at a time |\n"
          "| D-32 | 2026-02-01 | user:U1 | pipeline_serialized q: b3 then b4 |\n"
          "| D-33 | 2026-02-01 | user:U1 | merge_override for b9_u |\n")


def test_a_recorded_serialization_decision_orders_the_batches_that_share_the_pipeline():
    m = _manifest([_batch("b1", ["p"]), _batch("b2", ["p"]), _batch("b3", ["q"]), _batch("b4", ["q"])],
                  serialized_pipelines={"p": "D-31"})
    r = check_manifest(m, LEDGER)
    assert r["status"] == "halt"
    assert [s["pipeline"] for s in r["shared"]] == ["p", "q"]
    assert r["shared"][0]["serialized"] == "D-31" and r["shared"][1]["serialized"] is False
    r = check_manifest(_manifest(m["batches"], serialized_pipelines={"p": "D-31", "Q": "D-32"}), LEDGER)
    assert r["status"] == "pass"
    assert r["order"] == {"b2": ["b1"], "b4": ["b3"]}


def test_serialization_order_follows_the_manifest_and_chains_every_batch_of_the_pipeline():
    m = _manifest([_batch("b3", ["p", "q"]), _batch("b1", ["p"]), _batch("b2", ["p", "q"])],
                  serialized_pipelines={"p": "D-31", "q": "D-32"})
    r = check_manifest(m, LEDGER.replace("b3 then b4", "b3 then b2"))
    assert r["status"] == "pass"
    assert r["order"] == {"b1": ["b3"], "b2": ["b1", "b3"]}


@pytest.mark.parametrize("bad", [{"p": ""}, {"p": "yes"}, {"p": 31}, ["p"], {"p": "D31"}, {"p": "decided"}])
def test_a_serialization_entry_must_name_a_decision_row(bad):
    m = _manifest([_batch("b1", ["p"]), _batch("b2", ["p"])], serialized_pipelines=bad)
    with pytest.raises(SystemExit, match="serialized_pipelines"):
        check_manifest(m, LEDGER)


@pytest.mark.parametrize("ledger", [
    "",
    LEDGER.replace("D-31", "D-30"),
    LEDGER.replace("pipeline_serialized p", "serialized p"),
    LEDGER.replace("pipeline_serialized p:", "pipeline_serialized pp:"),
    "D-31 pipeline_serialized p\n",
])
def test_the_decision_row_must_exist_and_be_tagged_pipeline_serialized_for_that_pipeline(ledger):
    m = _manifest([_batch("b1", ["p"]), _batch("b2", ["p"])], serialized_pipelines={"p": "D-31"})
    with pytest.raises(SystemExit, match="D-31"):
        check_manifest(m, ledger)


def test_a_serialization_entry_for_a_pipeline_no_two_batches_share_is_ignored():
    r = check_manifest(_manifest([_batch("b1", ["p"])], serialized_pipelines={"p": "D-31"}), LEDGER)
    assert r["status"] == "pass" and r["order"] == {}


def test_a_batch_that_declares_no_pipelines_is_unsupported_not_clean():
    m = _manifest([_batch("b1", ["p"]), {"id": "b2", "units": ["u"], "write_targets": []}])
    r = check_manifest(m)
    assert r["status"] == "unsupported"
    assert r["unchecked_batches"] == ["b2"]


def test_a_halt_outranks_unsupported():
    m = _manifest([_batch("b1", ["p"]), _batch("b2", ["p"]), {"id": "b3", "units": ["u"], "write_targets": []}])
    r = check_manifest(m)
    assert r["status"] == "halt" and r["unchecked_batches"] == ["b3"]


def test_an_empty_pipelines_list_is_a_checked_batch_with_none():
    r = check_manifest(_manifest([_batch("b1", []), _batch("b2", ["p"])]))
    assert r["status"] == "pass" and r["unchecked_batches"] == []


@pytest.mark.parametrize("pipelines", ["p", [1], ["p", "p"], [""]])
def test_pipelines_must_be_a_list_of_distinct_names(pipelines):
    with pytest.raises(SystemExit, match="lakeflow_pipelines"):
        check_manifest(_manifest([_batch("b1", pipelines)]))


@pytest.mark.parametrize("m", [[], {"batches": {}}, {"batches": [{"units": []}]}, {"batches": [{"id": "b"}, {"id": "b"}]}])
def test_a_malformed_manifest_halts(m):
    with pytest.raises(SystemExit):
        check_manifest(m)


def test_cli_writes_the_result_and_exit_codes(tmp_path, capsys):
    man = tmp_path / "wave-2.json"
    out = tmp_path / "wave-2.pipelines.json"
    man.write_text(json.dumps(_manifest([_batch("b1", ["p"]), _batch("b2", ["p"])])))
    assert main([str(man), "--out", str(out)]) == 1
    assert json.loads(out.read_text())["status"] == "halt"
    assert "b1" in capsys.readouterr().out
    man.write_text(json.dumps(_manifest([_batch("b1", ["p"]), {"id": "b2", "units": [], "write_targets": []}])))
    assert main([str(man), "--out", str(out)]) == 2
    man.write_text(json.dumps(_manifest([_batch("b1", ["p"]), _batch("b2", ["q"])])))
    assert main([str(man), "--out", str(out)]) == 0
    assert json.loads(out.read_text())["wave"] == 2


def test_cli_reads_the_decision_ledger_beside_the_waves_directory_or_where_told(tmp_path):
    waves = tmp_path / ".migration" / "waves"
    waves.mkdir(parents=True)
    man = waves / "wave-2.json"
    man.write_text(json.dumps(_manifest([_batch("b1", ["p"]), _batch("b2", ["p"])], serialized_pipelines={"p": "D-31"})))
    with pytest.raises(SystemExit, match="D-31"):
        main([str(man)])
    (tmp_path / ".migration" / "06_decisions.md").write_text(LEDGER)
    assert main([str(man)]) == 0
    other = tmp_path / "elsewhere.md"
    other.write_text("")
    with pytest.raises(SystemExit, match="D-31"):
        main([str(man), "--decisions", str(other)])


def test_script_is_standard_library_only_and_runs_as_a_file(tmp_path):
    src = (Path(__file__).parent / "pipeline_updates.py").read_text()
    for line in src.splitlines():
        if line.startswith(("import ", "from ")):
            assert line.split()[1].split(".")[0] in {"argparse", "json", "re", "sys", "pathlib", "__future__"}, line
    man = tmp_path / "wave-1.json"
    man.write_text(json.dumps(_manifest([_batch("b1", ["p"])])))
    proc = subprocess.run([sys.executable, str(Path(__file__).parent / "pipeline_updates.py"), str(man)],
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["status"] == "pass"
