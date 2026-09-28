"""Cards are the only shape a stop, wave close or halt is posted in: six lines, under 90 words, decision
first, one quoted reply, and never a FAIL for rows that matched."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import cards  # noqa: E402

CARDS = Path(__file__).with_name("cards.py")


def _batch(bid, *, status="PASS", parity="PASS", eligible=True, classes=None, pr=None):
    b = {"id": bid, "status": status, "recon_verdict": parity if parity != "NOT_RUN" else "NOT_RUN",
         "parity": parity, "merge_eligible": eligible, "pr_url": pr or f"https://example.test/pr/{bid}"}
    if classes is not None:
        b["blocker_classes"] = classes
    return b


def _result(batches, *, verify="PASS", closed=False, auto_merge=False, overrides=(), breaker=None,
            unit_verdicts=None, **extra):
    if verify and unit_verdicts is None:  # the verifier grades every passed batch; the wave verdict follows
        unit_verdicts = {b["id"]: verify for b in batches if b.get("status") == "PASS"}
    return {"wave": 1, "closed": closed, "auto_merge": auto_merge, "breaker_tripped_on": breaker,
            "verify": {"wave_verdict": verify, "unit_verdicts": unit_verdicts, "findings": []} if verify else None,
            "merge_overrides": list(overrides), "batches": batches,
            "write_target_overlaps": [], "undeclared_write_targets": {}, "unreported_write_targets": [],
            "resync": None, "close": None, **extra}


def _lines(text):
    return text.rstrip("\n").split("\n")


def test_a_card_is_six_lines_under_ninety_words():
    text = cards.stop_card("C", "wave plan", "r1", "approve 3 waves", "writes only to mig_*", "D-12", "approve",
                           "approve STOP C")
    lines = _lines(text)
    assert len(lines) == 6 and lines[0].startswith("STOP C") and lines[1].startswith("Decision:")
    assert lines[-1] == "Reply: `approve STOP C`  (or `halt`)"
    assert sum(len(l.split()) for l in lines) <= cards.MAX_WORDS
    with pytest.raises(ValueError, match="6 non-empty lines"):
        cards.card(["a", "b", "c", "d", "e"])
    with pytest.raises(ValueError, match="6 non-empty lines"):
        cards.card(["a", "b", "", "d", "e", "f"])
    with pytest.raises(ValueError, match="words"):
        cards.card(["w " * 20] * 6)


def test_parity_pass_with_a_blocked_merge_policy_never_reads_as_fail():
    phrase = cards.status_phrase("PASS", False, ["rerun_policy"])
    assert phrase == "parity PASS, merge policy BLOCKED (rerun_policy)"
    assert "FAIL" not in phrase
    assert cards.status_phrase("PASS", True) == "parity PASS, merge policy eligible"
    assert cards.status_phrase("PASS", False) == "parity PASS, merge policy BLOCKED (class not reported)"


def test_wave_card_counts_parity_and_merge_policy_separately_and_groups_blockers_by_class():
    result = _result([
        _batch("b-1", eligible=False, classes=["rerun_policy"]),
        _batch("b-2", eligible=False, classes=["rerun_policy", "privilege_visibility"]),
        _batch("b-3"),
    ], overrides=[{"batch": "b-1", "units": ["u"], "decision_id": "D-23"},
                  {"batch": "b-2", "units": ["v"], "decision_id": "D-23"}])
    lines = _lines(cards.wave_card(result))
    assert len(lines) == 6
    assert lines[0] == "WAVE 1  open  parity PASS 3/3  merge-eligible 1/3  verify PASS"
    assert lines[1] == "Blockers: rerun_policy x2, privilege_visibility x1"
    assert lines[2] == ("Decision: merge 3 verified PRs, then start wave 2. "
                        "Override D-23 lifts merge policy only; parity stays as measured")
    assert lines[3] == "Not done: nothing; wave complete"
    assert lines[4].startswith("PRs: https://example.test/pr/b-1 https://example.test/pr/b-2 https://example.test/pr/b-3")
    assert lines[4].endswith("Evidence: .migration/waves/wave-1.result.json")
    assert lines[5] == "Reply: `accept wave 1`  (or `halt`)"
    assert "FAIL" not in "\n".join(lines)


def test_wave_card_says_when_blocker_classes_were_not_reported_and_falls_back_to_recon():
    batches = [_batch("b-1", eligible=False), _batch("b-2")]
    for b in batches:
        del b["parity"]
    lines = _lines(cards.wave_card(_result(batches)))
    assert lines[0].startswith("WAVE 1  open  recon PASS 2/2  merge-eligible 1/2")
    assert lines[1] == "Blockers: 1 batch(es) blocked, class not reported"


def test_wave_card_with_a_failed_batch_and_a_tripped_breaker_asks_for_a_relaunch():
    result = _result([
        _batch("b-1", status="FAIL", parity="FAIL", eligible=False, classes=["data"]),
        _batch("b-2", status="NOT_LAUNCHED", parity="NOT_RUN", eligible=False, pr=""),
    ], verify=None, breaker="data")
    lines = _lines(cards.wave_card(result))
    assert lines[0] == "WAVE 1  HALTED  parity PASS 0/2  merge-eligible 0/2  verify NOT RUN"
    assert lines[1] == "Blockers: data x1; 1 batch(es) blocked, class not reported; breaker tripped on data"
    assert lines[2] == "Decision: fix b-1, b-2 and relaunch"
    assert lines[3] == "Not done: independent verifier (not run); failed: b-1; held by breaker: b-2"
    assert lines[4].startswith("PRs: none")
    assert lines[5] == "Reply: `relaunch`  (or `halt`)"


def test_wave_card_with_a_verifier_fail_merges_nothing():
    lines = _lines(cards.wave_card(_result([_batch("b-1")], verify="FAIL")))
    assert lines[2] == "Decision: nothing merges: verifier FAIL; fix its findings and relaunch"
    assert lines[3] == "Not done: verifier FAIL: b-1"


def test_a_closed_auto_merged_wave_needs_no_reply():
    result = _result([_batch("b-1"), _batch("b-2")], closed=True, auto_merge=True,
                     close={"merged_prs": ["https://example.test/pr/b-1", "https://example.test/pr/b-2"],
                            "unmerged": []})
    lines = _lines(cards.wave_card(result))
    assert lines[0].startswith("WAVE 1  closed")
    assert lines[2] == "Decision: none; 2 PRs merged, wave 2 may launch"
    assert lines[5] == "Reply: none needed"


def test_links_are_capped_so_a_wide_wave_still_fits_the_card():
    result = _result([_batch(f"b-{i}") for i in range(12)])
    text = cards.wave_card(result)
    assert "+8 more" in text
    assert sum(len(l.split()) for l in _lines(text)) <= cards.MAX_WORDS


def test_halt_card_and_relaunch_line():
    text = cards.halt_card(0, "D-2", "ledger cell D-7 is not JSON", "wave 0, nothing launched",
                           "fix the cell and relaunch under the same STOP C", relaunch=3)
    lines = _lines(text)
    assert lines[0] == "WAVE 0  HALTED  under STOP C D-2  relaunch 3"
    assert lines[1:4] == ["Halt: ledger cell D-7 is not JSON", "Paused: wave 0, nothing launched",
                          "Unblocks: fix the cell and relaunch under the same STOP C"]
    assert lines[5] == "Reply: none needed"
    assert cards.halt_card(0, "D-2", "x", "y", "z", reply="halt").endswith("Reply: `halt`\n")
    assert cards.relaunch_line(0, 3, "report parser fix") == "wave 0 relaunch 3: report parser fix, no new decision\n"


def test_cli_renders_each_kind(tmp_path):
    result = tmp_path / "wave-1.result.json"
    result.write_text(json.dumps(_result([_batch("b-1")])))
    out = subprocess.run([sys.executable, str(CARDS), "wave", str(result)], check=True, capture_output=True, text=True)
    assert out.stdout == cards.wave_card(json.loads(result.read_text()))
    out = subprocess.run([sys.executable, str(CARDS), "halt", "--wave", "0", "--stop-c", "D-2", "--what", "w",
                          "--paused", "p", "--unblocks", "u", "--relaunch", "2"], check=True,
                         capture_output=True, text=True)
    assert out.stdout.startswith("WAVE 0  HALTED  under STOP C D-2  relaunch 2\n")
    out = subprocess.run([sys.executable, str(CARDS), "relaunch", "--wave", "0", "--relaunch", "2", "--fix", "f"],
                         check=True, capture_output=True, text=True)
    assert out.stdout == "wave 0 relaunch 2: f, no new decision\n"


def test_the_contract_quotes_the_same_reply_phrases_the_cards_emit():
    contract = (Path(__file__).parents[1] / "install-dbx-factory" / "references" / "contract.md").read_text()
    for phrase in ("`approve STOP C`", "`accept wave <N>`", "`override wave <N>`", "`relaunch`", "`halt`",
                   "`authorize cutover`", "`decline cutover`", "Reply: none needed"):
        assert phrase in contract, phrase
    assert "wave-<N>.card.md" in contract and "cards.py relaunch" in contract
    packet = Path(__file__).parents[1] / "install-dbx-factory" / "references" / "stop_e_packet.md"
    assert packet.exists()
    playbook = (Path(__file__).parents[1] / "install-dbx-factory" / "playbooks" / "8-cutover_signoff.md").read_text()
    assert "stop_e_packet.md" in playbook


def test_wave_card_names_the_batches_the_verifier_passed_and_what_the_close_step_merged():
    """The close step merges per-batch verifier PASS even when the wave verdict is FAIL; the card must say so."""
    batches = [_batch("b-1"), _batch("b-2")]
    partial = {"b-1": "PASS", "b-2": "FAIL"}
    lines = _lines(cards.wave_card(_result(batches, verify="FAIL", unit_verdicts=partial)))
    assert lines[2] == "Decision: merge 1 verified PRs, then start wave 2; b-2 relaunch separately"
    assert lines[3] == "Not done: verifier FAIL: b-2"
    assert lines[4].startswith("PRs: https://example.test/pr/b-1   Evidence:")
    assert lines[5] == "Reply: `accept wave 1`  (or `halt`)"

    close = {"merged_prs": ["https://example.test/pr/b-1"], "unmerged": [], "changed_paths": []}
    lines = _lines(cards.wave_card(_result(batches, verify="FAIL", unit_verdicts=partial, auto_merge=True,
                                           close=close)))
    assert lines[2] == "Decision: 1 PRs merged; nothing else may merge; b-2 relaunch separately"
    assert lines[4].startswith("PRs: merged https://example.test/pr/b-1   Evidence:")
    assert lines[5] == "Reply: `relaunch`  (or `halt`)"

    close = {"merged_prs": ["https://example.test/pr/b-1"], "unmerged": [{"pr_url": "https://example.test/pr/b-2",
                                                                          "reason": "checks red"}], "changed_paths": []}
    lines = _lines(cards.wave_card(_result(batches, verify="PASS", auto_merge=True, close=close)))
    assert lines[2] == "Decision: 1 PRs merged; 1 verified await merge"
    assert "1 PRs not merged" in lines[3]
    assert lines[4].startswith("PRs: merged https://example.test/pr/b-1; to merge https://example.test/pr/b-2   ")


def test_wave_card_never_overflows_however_many_batches_failed_or_were_held():
    """Rendering runs before the result is written; a card that could raise would strand the wave."""
    batches = [_batch(f"batch-{i:02d}", status="FAIL", parity="FAIL", eligible=False, classes=["data"])
               for i in range(3)]
    batches += [_batch(f"batch-{i:02d}", status="NOT_LAUNCHED", parity="NOT_RUN", eligible=False)
                for i in range(3, 20)]
    text = cards.wave_card(_result(batches, verify=None, breaker="batch-02"))
    lines = _lines(text)
    assert len(lines) == 6 and sum(len(l.split()) for l in lines) <= cards.MAX_WORDS
    assert lines[2].startswith("Decision: fix batch-00, batch-01, batch-02 +17 more and relaunch")
    assert "held by breaker: batch-03, batch-04, batch-05 +14 more" in lines[3]

    long_ids = [_batch("unit_" + "x" * 60 + f"_{i}", status="FAIL", parity="FAIL", eligible=False,
                       classes=[f"class_{i}"]) for i in range(40)]
    long_ids += [_batch("pass_" + "y" * 60 + f"_{i}", pr="https://example.test/" + "z" * 80 + f"/{i}")
                 for i in range(40)]
    verdicts = {b["id"]: ("PASS" if i % 2 else "FAIL") for i, b in enumerate(long_ids[40:])}
    text = cards.wave_card(_result(long_ids, verify="FAIL", unit_verdicts=verdicts))
    lines = _lines(text)
    assert len(lines) == 6 and sum(len(l.split()) for l in lines) <= cards.MAX_WORDS
    assert "in the result" in lines[4] or "+" in lines[4]
