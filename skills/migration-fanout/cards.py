"""The messages the notification contract allows: a six-line card for every stop, wave close and
halt, and a one-line update for a plumbing relaunch. One shape so the reader answers from a
phone without opening anything: decision first, why it is safe, evidence, recommendation, the
exact reply. `parity PASS` is never rendered as FAIL; merge policy is a separate word.

    python3 cards.py wave <wave-N.result.json>
    python3 cards.py halt --wave N --stop-c D-n --what ... --paused ... --unblocks ... [--relaunch R]
    python3 cards.py relaunch --wave N --relaunch R --fix ...
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

LINES = 6
MAX_WORDS = 90
MAX_LINKS = 4
BLOCKER_CLASSES = ("data", "structural", "privilege_visibility", "rerun_policy", "evidence")


def status_phrase(parity: str, merge_eligible: bool, classes=()) -> str:
    """`parity PASS, merge policy BLOCKED (rerun_policy)`: rows that matched are never a FAIL."""
    if merge_eligible:
        return f"parity {parity}, merge policy eligible"
    named = ", ".join(sorted(set(classes))) or "class not reported"
    return f"parity {parity}, merge policy BLOCKED ({named})"


def card(lines) -> str:
    """Exactly six non-empty lines, at most 90 words (a link counts as one)."""
    lines = [str(line).strip() for line in lines]
    if len(lines) != LINES or not all(lines):
        raise ValueError(f"a card is {LINES} non-empty lines, got {len(lines)}: {lines}")
    words = sum(len(line.split()) for line in lines)
    if words > MAX_WORDS:
        raise ValueError(f"card is {words} words; the limit is {MAX_WORDS}")
    return "\n".join(lines) + "\n"


def reply_line(phrase: str | None, alt: str | None = "halt") -> str:
    if not phrase:
        return "Reply: none needed"
    return f"Reply: `{phrase}`" + (f"  (or `{alt}`)" if alt and alt != phrase else "")


def stop_card(stop: str, subject: str, run: str, decision: str, safe: str, evidence: str,
              recommend: str, reply: str, alt: str | None = "halt") -> str:
    """STOP A/B/C/E share one shape; `reply` is the canonical phrase the contract lists."""
    return card([
        f"STOP {stop}  {subject}  run {run}",
        f"Decision: {decision}",
        f"Why it is safe: {safe}",
        f"Evidence: {evidence}",
        f"Recommend: {recommend}",
        reply_line(reply, alt),
    ])


def relaunch_line(wave, relaunch: int, fix: str) -> str:
    """The one post a plumbing relaunch earns: nothing the human decided changed."""
    return f"wave {wave} relaunch {relaunch}: {fix}, no new decision\n"


def halt_card(wave, stop_c: str, what: str, paused: str, unblocks: str, relaunch: int = 1,
              reply: str | None = None, alt: str | None = "halt") -> str:
    """A fan-out halt (collision, breaker, preflight): what stopped, what waits, what restarts it.
    `reply` only when the human decides something; a plumbing fix the orchestrator makes itself needs none."""
    head = f"WAVE {wave}  HALTED  under STOP C {stop_c}" + (f"  relaunch {relaunch}" if relaunch > 1 else "")
    return card([
        head,
        f"Halt: {what}",
        f"Paused: {paused}",
        f"Unblocks: {unblocks}",
        "Production untouched: children write only to declared migration targets",
        reply_line(reply, alt),
    ])


def _links(urls) -> str:
    urls = [u for u in urls if isinstance(u, str) and u]
    shown = urls[:MAX_LINKS]
    more = len(urls) - len(shown)
    return " ".join(shown) + (f" +{more} more" if more > 0 else "") if shown else "none"


def _tally(items) -> str:
    counts = Counter(items)
    return ", ".join(f"{k} x{n}" for k, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def wave_card(result: dict) -> str:
    """The wave-close card from `wave-N.result.json`: parity and merge policy counted separately,
    blockers grouped by class, the one decision left to a human, and what did not run."""
    wave = result.get("wave")
    batches = [b for b in result.get("batches", []) if isinstance(b, dict)]
    n = len(batches)
    graded = [b for b in batches if b.get("recon_verdict") in ("PASS", "FAIL")]
    label = "parity" if graded and all(isinstance(b.get("parity"), str) for b in graded) else "recon"
    matched = sum(1 for b in batches if (b.get("parity") if label == "parity" else b.get("recon_verdict")) == "PASS")
    eligible = sum(1 for b in batches if b.get("merge_eligible") is True)
    passed = [b for b in batches if b.get("status") == "PASS"]
    verify = result.get("verify") if isinstance(result.get("verify"), dict) else None
    verdict = verify.get("wave_verdict", "NOT RUN") if verify else "NOT RUN"
    closed = result.get("closed") is True
    breaker = result.get("breaker_tripped_on")
    head = (f"WAVE {wave}  {'closed' if closed else 'HALTED' if breaker else 'open'}  "
            f"{label} PASS {matched}/{n}  merge-eligible {eligible}/{n}  verify {verdict}")

    classes = [c for b in batches if b.get("merge_eligible") is False
               for c in (b.get("blocker_classes") if isinstance(b.get("blocker_classes"), list) else [])
               if isinstance(c, str)]
    blocked = [b for b in batches if b.get("merge_eligible") is False]
    unclassed = [b for b in blocked if not isinstance(b.get("blocker_classes"), list)]
    parts = []
    if classes:
        parts.append(_tally(classes))
    if unclassed:
        parts.append(f"{len(unclassed)} batch(es) blocked, class not reported")
    if breaker:
        parts.append(f"breaker tripped on {breaker}")
    if result.get("write_target_overlaps"):
        parts.append("write-target overlap")
    if result.get("undeclared_write_targets"):
        parts.append("undeclared write targets")
    if result.get("unreported_write_targets"):
        parts.append("write targets not reported")
    blockers = "Blockers: " + ("; ".join(parts) if parts else "none")

    overrides = [o for o in result.get("merge_overrides", []) if isinstance(o, dict)]
    failed = [b["id"] for b in batches if b.get("status") in ("FAIL", "BLOCKED")]
    held = [b["id"] for b in batches if b.get("status") == "NOT_LAUNCHED"]
    urls = [b.get("pr_url") for b in passed]
    auto_merge = result.get("auto_merge") is True
    close = result.get("close") if isinstance(result.get("close"), dict) else None
    merged = close.get("merged_prs", []) if close else []
    to_start = f"wave {wave + 1}" if isinstance(wave, int) else "the next wave"

    if closed and auto_merge:
        decision, reply = f"none; {len(merged)} PRs merged, {to_start} may launch", None
    elif passed and verdict == "PASS":
        decision = f"merge {len(urls)} verified PRs, then start {to_start}"
        if failed or held:
            decision += f"; {', '.join(failed + held)} relaunch separately"
        reply = f"accept wave {wave}"
    elif passed:
        decision, reply = f"nothing merges: verifier {verdict}; fix its findings and relaunch", "relaunch"
    else:
        decision, reply = f"fix {', '.join(failed + held) or 'the halt'} and relaunch", "relaunch"
    if overrides:
        ids = ", ".join(sorted({o.get("decision_id", "?") for o in overrides}))
        decision += f". Override {ids} lifts merge policy only; parity stays as measured"

    not_done = []
    if verify is None:
        not_done.append("independent verifier (not run)")
    elif verdict != "PASS":
        not_done.append(f"verifier {verdict}")
    if failed:
        not_done.append("failed: " + ", ".join(failed))
    if held:
        not_done.append("held by breaker: " + ", ".join(held))
    if close and close.get("unmerged") and auto_merge:
        not_done.append(f"{len(close['unmerged'])} PRs not merged")
    if result.get("resync") and (result["resync"] or {}).get("held_batches"):
        not_done.append("resync held: " + ", ".join(result["resync"]["held_batches"]))

    return card([
        head,
        blockers,
        f"Decision: {decision}",
        "Not done: " + ("; ".join(not_done) if not_done else "nothing; wave complete"),
        f"PRs: {_links(urls)}   Evidence: .migration/waves/wave-{wave}.result.json",
        reply_line(reply, "halt" if reply else None),
    ])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="kind", required=True)
    w = sub.add_parser("wave")
    w.add_argument("result", type=Path)
    h = sub.add_parser("halt")
    for name in ("--wave", "--stop-c", "--what", "--paused", "--unblocks"):
        h.add_argument(name, required=True)
    h.add_argument("--relaunch", type=int, default=1)
    h.add_argument("--reply", default=None, help="the phrase that decides it, when a human must")
    r = sub.add_parser("relaunch")
    r.add_argument("--wave", required=True)
    r.add_argument("--relaunch", type=int, required=True)
    r.add_argument("--fix", required=True)
    a = p.parse_args(argv)
    if a.kind == "wave":
        sys.stdout.write(wave_card(json.loads(a.result.read_text())))
    elif a.kind == "halt":
        sys.stdout.write(halt_card(a.wave, a.stop_c, a.what, a.paused, a.unblocks, a.relaunch, a.reply))
    else:
        sys.stdout.write(relaunch_line(a.wave, a.relaunch, a.fix))
    return 0


if __name__ == "__main__":
    sys.exit(main())
