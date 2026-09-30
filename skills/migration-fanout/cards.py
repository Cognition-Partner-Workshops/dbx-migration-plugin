"""The wave card: the six lines the wave ticket's worker posts with `wave-N.result.json`, one shape so
the manager answers from a phone without opening anything: decision first, blockers by class, what
did not run, the PRs and evidence, the exact reply. `parity PASS` is never rendered as FAIL; merge
policy is a separate word. The reply phrases are the ones `SKILL.md` (Wave card) lists.

    python3 cards.py <wave-N.result.json>
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
MAX_IDS = 3
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


def _links(urls) -> str:
    urls = [u for u in urls if isinstance(u, str) and u]
    if not urls:
        return "none"
    more = len(urls) - MAX_LINKS
    return " ".join(urls[:MAX_LINKS]) + (f" +{more} more" if more > 0 else "")


def _ids(ids) -> str:
    """`b-1, b-2, b-3 +17 more`: a card names a few and the result names them all."""
    ids = [i for i in ids if isinstance(i, str)]
    more = len(ids) - MAX_IDS
    return ", ".join(ids[:MAX_IDS]) + (f" +{more} more" if more > 0 else "")


def _tally(items) -> str:
    counts = sorted(Counter(items).items(), key=lambda kv: (-kv[1], kv[0]))
    more = len(counts) - MAX_IDS
    return ", ".join(f"{k} x{n}" for k, n in counts[:MAX_IDS]) + (f" +{more} more classes" if more > 0 else "")


def _fit(head, parts, prs, reply) -> str:
    """The one truncation rule: while the card is over budget, the longest clause line drops its last
    clause (never its first) for a `+N more in the result` marker. The result names everything."""
    dropped = [0] * len(parts)
    while True:
        body = [prefix + "; ".join(clauses[:len(clauses) - n]) + (f" +{n} more in the result" if n else "")
                for (prefix, clauses), n in zip(parts, dropped)]
        lines = [head, *body, prs, reply]
        droppable = [i for i, (_, clauses) in enumerate(parts) if len(clauses) - dropped[i] > 1]
        if sum(len(line.split()) for line in lines) <= MAX_WORDS or not droppable:
            return card(lines)
        dropped[max(droppable, key=lambda i: len(body[i].split()))] += 1


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
    blockers = []
    if classes:
        blockers.append(_tally(classes))
    if unclassed:
        blockers.append(f"{len(unclassed)} batch(es) blocked, class not reported")
    if breaker:
        blockers.append(f"breaker tripped on {breaker}")
    if result.get("write_target_overlaps"):
        blockers.append("write-target overlap")
    if result.get("undeclared_write_targets"):
        blockers.append("undeclared write targets")
    if result.get("unreported_write_targets"):
        blockers.append("write targets not reported")

    overrides = [o for o in result.get("merge_overrides", []) if isinstance(o, dict)]
    failed = [b["id"] for b in batches if b.get("status") in ("FAIL", "BLOCKED")]
    held = [b["id"] for b in batches if b.get("status") == "NOT_LAUNCHED"]
    unit_verdicts = verify.get("unit_verdicts") if verify and isinstance(verify.get("unit_verdicts"), dict) else {}
    verified = [b for b in passed if unit_verdicts.get(b.get("id")) == "PASS"]
    unverified = [b["id"] for b in passed if unit_verdicts.get(b.get("id")) != "PASS"]
    auto_merge = result.get("auto_merge") is True
    close = result.get("close") if isinstance(result.get("close"), dict) else None
    merged = [u for u in (close.get("merged_prs", []) if close else []) if isinstance(u, str)]
    resync = result.get("resync") if isinstance(result.get("resync"), dict) else None
    resync_held = {i for i in (resync or {}).get("held_batches") or [] if isinstance(i, str)}
    # the same merge gate the close step applies: verifier PASS, not merged yet, not held by the resync
    awaiting = [b.get("pr_url") for b in verified if b.get("pr_url") not in merged and b.get("id") not in resync_held]
    held_verified = [b["id"] for b in verified if b.get("id") in resync_held]
    to_start = f"wave {wave + 1}" if isinstance(wave, int) else "the next wave"
    rest = _ids(unverified + failed + held)
    # a verified batch the resync holds is not merged by anyone until the hold is fixed and the
    # wave relaunched: it keeps the reply on `relaunch` and the next wave out of the decision, and
    # rides in the first Decision clause so truncation cannot drop it
    resync_hold = f"resync held {_ids(held_verified)}: fix it and relaunch" if held_verified else ""
    if closed and auto_merge and not held_verified:
        decision, reply = [f"none; {len(merged)} PRs merged, {to_start} may launch"], None
    elif merged:
        decision = [f"{len(merged)} PRs merged; " + (f"{len(awaiting)} verified await merge" if awaiting
                                                     else "nothing else may merge")
                    + (f"; {resync_hold}" if resync_hold else "")]
        decision += [f"{rest} relaunch separately"] if rest else []
        reply = "relaunch" if rest or awaiting or held_verified else None
    elif awaiting:
        decision = [f"merge {len(awaiting)} verified PRs; " + (
            resync_hold if held_verified else f"{to_start} once they are recorded merged and green")]
        decision += [f"{rest} relaunch separately"] if rest else []
        reply = "relaunch" if held_verified else f"accept wave {wave}"
    elif held_verified:
        decision, reply = [f"nothing merges: {resync_hold}"], "relaunch"
    elif passed:
        decision, reply = [f"nothing merges: verifier {verdict}; fix its findings and relaunch"], "relaunch"
    else:
        decision, reply = [f"fix {rest or 'the halt'} and relaunch"], "relaunch"
    if overrides:
        decision[-1] += (f". Override {_ids(sorted({o.get('decision_id', '?') for o in overrides}))} lifts merge "
                         "policy only; parity stays as measured")

    not_done = []
    if verify is None:
        not_done.append("independent verifier (not run)")
    elif unverified:
        not_done.append(f"verifier {verdict}: " + _ids(unverified))
    elif verdict != "PASS":
        not_done.append(f"verifier {verdict}")
    if failed:
        not_done.append("failed: " + _ids(failed))
    if held:
        not_done.append("held by breaker: " + _ids(held))
    if close and close.get("unmerged") and auto_merge:
        not_done.append(f"{len(close['unmerged'])} PRs not merged")
    if resync_held:
        not_done.append("resync held: " + _ids(sorted(resync_held)))

    if merged:
        prs = f"merged {_links(merged)}" + (f"; to merge {_links(awaiting)}" if awaiting else "")
    else:
        prs = _links(awaiting or [b.get("pr_url") for b in passed if b.get("id") not in resync_held])
    return _fit(head,
                [("Blockers: ", blockers or ["none"]),
                 ("Decision: ", decision),
                 ("Not done: ", not_done or ["nothing; wave complete"])],
                f"PRs: {prs}   Evidence: .migration/waves/wave-{wave}.result.json",
                reply_line(reply, "halt" if reply else None))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("result", type=Path, help="wave-N.result.json")
    sys.stdout.write(wave_card(json.loads(p.parse_args(argv).result.read_text())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
