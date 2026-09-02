"""Migration fan-out workflow: run one wave of unit-migration children, then one
independent verifier, and write the wave result the orchestrator gates on.

Run with the `run_workflow` tool. Set WAVE_MANIFEST (env var) to the wave file the plan
wrote, e.g. .migration/waves/wave-2.json. Same script for every kit: the manifest names
the child playbook macro, so nothing here is DBX- or Mongo-specific.

What this script guarantees, so the orchestrator does not have to:
  - Two batches in the same wave never share a write target (checked BEFORE launch).
  - At most `width` children run at once.
  - Circuit breaker: after `breaker_threshold` children fail with the same failure class,
    no new children launch; already-running ones finish. Nothing is retried blindly.
  - Children never edit shared ledger files. This script is the single writer of
    <manifest>.result.json and the ledger rows the orchestrator appends from it.
  - The verifier is a different session from every child. Only PRs the verifier marks
    PASS are merged, and only if the manifest says auto_merge.
  - Re-running with the same run_id replays finished children and only launches the rest.

Manifest shape (written by the plan playbook, read here):
{
  "wave": 2,
  "repo": "github.com/acme/dbx-target",
  "child_macro": "!dbx_unit_migration",       # or "!mongo_unit_migration"
  "verify_macro": "!dbx_data_reconciliation", # or "!mongo_reconciliation"
  "width": 20,
  "breaker_threshold": 3,
  "auto_merge": true,
  "child_minutes": 45,                        # soft time limit per child
  "batches": [
    {"id": "w2-b01", "units": ["orders_load", "orders_dim"],
     "write_targets": ["mig.orders", "mig.orders_dim"],
     "brief": "...complete hand-off text for this batch..."}
  ]
}
"""

import asyncio
import hashlib
import json
import os
from collections import Counter
from pathlib import Path

MANIFEST_PATH = Path(os.environ.get("WAVE_MANIFEST", ".migration/waves/wave-1.json"))
if not MANIFEST_PATH.exists():
    raise SystemExit(f"no wave manifest at {MANIFEST_PATH}. Set WAVE_MANIFEST to the file the "
                     "plan playbook wrote, then re-run.")
MANIFEST_TEXT = MANIFEST_PATH.read_text()
MANIFEST = json.loads(MANIFEST_TEXT)
MANIFEST_SHA = hashlib.sha256(MANIFEST_TEXT.encode()).hexdigest()[:12]
RESULT_PATH = MANIFEST_PATH.with_suffix(".result.json")
BRIEF_PATH = MANIFEST_PATH.with_suffix(".brief.md")
RUN_ID_PATH = MANIFEST_PATH.with_suffix(".run_id")

if RESULT_PATH.exists() and os.environ.get("WAVE_RERUN") != "1":
    resume = os.environ.get("WAVE_RESUME") == "1"
    try:
        prior = json.loads(RESULT_PATH.read_text())
        if not isinstance(prior, dict):
            raise ValueError("result is not a JSON object")
    except ValueError:
        if not resume:
            raise SystemExit(f"{RESULT_PATH} is not valid JSON (interrupted write?). Inspect it; to resume "
                             "the same run set WAVE_RESUME=1 with the recorded run_id, or set WAVE_RERUN=1 "
                             "to redo the wave.") from None
    else:
        if prior.get("closed"):
            raise SystemExit(f"{RESULT_PATH} says wave {prior.get('wave')} closed clean. To redo it on "
                             "purpose, set WAVE_RERUN=1.")
        if not resume:
            raise SystemExit(f"{RESULT_PATH} records a halted or failed run. To continue it, re-run with the "
                             "recorded run_id AND WAVE_RESUME=1 (finished children replay). To redo the wave "
                             "from scratch, set WAVE_RERUN=1.")
    if resume:
        run_id = os.environ.get("WAVE_RUN_ID")
        if not run_id:
            raise SystemExit("WAVE_RESUME=1 requires WAVE_RUN_ID; pass the recorded run_id")
        if RUN_ID_PATH.exists() and RUN_ID_PATH.read_text().strip() != run_id:
            raise SystemExit(f"WAVE_RUN_ID does not match {RUN_ID_PATH}; pass the recorded run_id to "
                             "run_workflow and WAVE_RUN_ID, or WAVE_RERUN=1 for a fresh run")


def validate_manifest(m):
    """Fail here, in one line, instead of 20 children failing on a missing field."""
    for key in ("wave", "repo", "child_macro", "verify_macro", "batches"):
        if key not in m:
            raise SystemExit(f"manifest is missing '{key}'")
    if not m["batches"]:
        raise SystemExit("manifest has no batches")
    ids = Counter(b.get("id") for b in m["batches"])
    dupes = [i for i, c in ids.items() if c > 1 or not i]
    if dupes:
        raise SystemExit(f"batch ids must be unique and non-empty: {dupes}")
    for b in m["batches"]:
        for key in ("units", "write_targets", "brief"):
            if not b.get(key):
                raise SystemExit(f"batch {b['id']} is missing '{key}' (a child with no brief or "
                                 "no declared write targets cannot be launched safely)")


validate_manifest(MANIFEST)

WAVE = MANIFEST["wave"]
REPO = MANIFEST["repo"]
BATCHES = sorted(MANIFEST["batches"], key=lambda b: b["id"])
WIDTH = int(MANIFEST.get("width", 20))
BREAKER = int(MANIFEST.get("breaker_threshold", 3))
AUTO_MERGE = bool(MANIFEST.get("auto_merge", True))
CHILD_MINUTES = int(MANIFEST.get("child_minutes", 45))

META = {
    "name": f"migration-wave-{WAVE}",
    "description": f"Wave {WAVE}: {len(BATCHES)} unit batches in parallel, then one independent verifier",
    "phases": [
        {"title": "migrate", "detail": "one child per batch: convert, load, recon, open PR",
         "labels": [b["id"] for b in BATCHES], "soft_time_limit_minutes": CHILD_MINUTES},
        {"title": "verify", "detail": "independent recon over the wave, merge green PRs",
         "count": 1, "soft_time_limit_minutes": 60},
    ],
}

CHILD_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["PASS", "FAIL", "BLOCKED"]},
        "pr_url": {"type": "string"},
        "branch": {"type": "string"},
        "recon_verdict": {"type": "string", "enum": ["PASS", "FAIL", "NOT_RUN"]},
        "recon_mode": {"type": "string"},
        "failure_class": {"type": "string"},
        "write_targets": {"type": "array", "items": {"type": "string"}},
        "skill_feedback": {"type": "array", "items": {"type": "string"}},
        "one_line_summary": {"type": "string"},
    },
    "required": ["status", "recon_verdict", "recon_mode", "write_targets", "one_line_summary"],
}

VERIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "wave_verdict": {"type": "string", "enum": ["PASS", "FAIL"]},
        "unit_verdicts": {"type": "object"},
        "merged_prs": {"type": "array", "items": {"type": "string"}},
        "findings": {"type": "array", "items": {"type": "string"}},
        "report_path": {"type": "string"},
    },
    "required": ["wave_verdict", "unit_verdicts", "findings"],
}


def check_write_targets(batches):
    """Two batches writing the same table or collection means the lineage missed an edge.
    Refuse to launch anything; the plan must be fixed first."""
    owners = {}
    for b in batches:
        for t in b.get("write_targets", []):
            if t in owners:
                raise SystemExit(f"write-target collision before launch: '{t}' is claimed by "
                                 f"{owners[t]} and {b['id']}. Fix the wave plan, then re-run.")
            owners[t] = b["id"]


def child_prompt(batch):
    return (
        f"You are one fan-out child in wave {WAVE} of a migration. Repo: {REPO}.\n"
        f"Run the playbook {MANIFEST['child_macro']} for batch {batch['id']} exactly as written.\n\n"
        f"BATCH BRIEF (your complete hand-off; if anything is missing, report status=BLOCKED "
        f"with the missing item in one_line_summary, do not improvise):\n"
        f"{batch['brief']}\n\n"
        f"Units: {json.dumps(batch['units'], sort_keys=True)}\n"
        f"Write targets you own (never write anywhere else): "
        f"{json.dumps(batch.get('write_targets', []), sort_keys=True)}\n\n"
        "Rules that override anything else:\n"
        "- Do not edit files under .migration/. The workflow writes the ledger from your report.\n"
        "- Do not merge your own PR.\n"
        "- status=PASS requires a live or snapshot recon PASS (result.json merge_eligible=true). "
        "Fixture evidence is never PASS.\n"
        "- If the recon harness fails 3 full runs, stop and report status=FAIL with a short "
        "failure_class (for example 'timestamp_precision', 'decimal_rounding', 'missing_rule').\n"
        "- Report every rule you had to derive yourself in skill_feedback.\n"
        "- one_line_summary is for a human skimming 20 of these: what landed, or why not."
    )


def verify_prompt(passed, auto_merge):
    merge_line = (
        "If your wave verdict is PASS, merge the PRs you marked PASS and list them in merged_prs."
        if auto_merge else
        "Do not merge anything; return per-unit verdicts. The orchestrator merges the PASS PRs at wave close.")
    return (
        f"You are the independent verifier for wave {WAVE}. Repo: {REPO}. You did not write "
        f"any of this code.\nRun the playbook {MANIFEST['verify_macro']} exactly as written over "
        f"these batches:\n{json.dumps(passed, sort_keys=True, indent=1)}\n\n"
        "Re-run the recon harness yourself. Do not trust the PR's pasted evidence. "
        "Mark a unit PASS only if you re-ran the harness in live or snapshot mode and result.json says "
        "merge_eligible=true. "
        f"{merge_line}\nWrite the wave recon report to .migration/recon/wave-{WAVE}/report.md, "
        f"commit it on branch recon/wave-{WAVE}, push, and give '<branch>:<path>' in "
        "report_path. Do not edit any other file under .migration/. Each finding is one plain "
        "sentence a lead can read without opening anything."
    )


class Breaker:
    def __init__(self, threshold):
        self.threshold = threshold
        self.classes = Counter()
        self.tripped_on = None

    def record(self, failure_class):
        if not failure_class:
            return
        self.classes[failure_class] += 1
        if self.classes[failure_class] >= self.threshold and not self.tripped_on:
            self.tripped_on = failure_class
            log(f"CIRCUIT BREAKER: {self.threshold} children failed with '{failure_class}'. "
                "No new children will launch this run.")


async def run_batch(batch, sem, breaker):
    async with sem:
        if breaker.tripped_on:
            return {"status": "NOT_LAUNCHED", "recon_verdict": "NOT_RUN",
                    "one_line_summary": f"held back: breaker tripped on '{breaker.tripped_on}'"}
        log(f"launch {batch['id']} ({len(batch['units'])} units)")
        try:
            out = await agent(child_prompt(batch), phase="migrate", schema=CHILD_SCHEMA,
                              label=batch["id"], repos=[REPO])
        except WorkflowAgentError as e:
            out = {"status": "FAIL", "recon_verdict": "NOT_RUN", "failure_class": "session_died",
                   "one_line_summary": f"child session died: {e}"}
        if (out["status"] == "PASS"
                and (out["recon_verdict"] != "PASS"
                     or out.get("recon_mode") not in ("live", "snapshot"))):
            out["status"] = "FAIL"
            out["failure_class"] = "non_merge_evidence"
            out["one_line_summary"] = (
                f"PASS downgraded: recon evidence was {out.get('recon_mode')}/"
                f"{out.get('recon_verdict')}; " + out["one_line_summary"])
        if out["status"] != "PASS":
            breaker.record(out.get("failure_class") or "unclassified")
        log(f"done   {batch['id']}: {out['status']} / recon {out['recon_verdict']}: "
            f"{out['one_line_summary']}")
        return out


def write_brief(results, verify, surprises, undeclared, unreported):
    """Ten lines a lead reads in one minute. The orchestrator posts this at wave close."""
    n = len(BATCHES)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "FAIL"]
    blocked = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "BLOCKED"]
    held = [b["id"] for b, r in zip(BATCHES, results) if r["status"] == "NOT_LAUNCHED"]
    feedback = sorted({s for r in results for s in r.get("skill_feedback", [])})
    lines = [
        f"# Wave {WAVE} close",
        "",
        f"Landed: {passed} of {n} batches passed their own recon.",
        f"Independent verify: {verify['wave_verdict'] if verify else 'NOT RUN'}"
        + (f", {len(verify.get('merged_prs', []))} PRs merged." if verify else "."),
        f"Failed: {', '.join(failed) or 'none'}.",
        f"Blocked on missing inputs: {', '.join(blocked) or 'none'}.",
        f"Held back by circuit breaker: {', '.join(held) or 'none'}.",
    ]
    if surprises:
        lines.append(f"Merges held: two children reported the same write target "
                     f"({', '.join(surprises)}). A human decides which PR lands.")
    if undeclared:
        lines.append("Merges held: children wrote outside their declared targets: "
                     + "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(undeclared.items()))
                     + ". A human decides which PR lands.")
    if unreported:
        lines.append(f"Merges held: {', '.join(unreported)} passed but reported no write targets; "
                     "a human confirms what they wrote before any PR lands.")
    lines += [
        "",
        "Verifier findings:" if verify and verify["findings"] else "Verifier findings: none.",
    ]
    lines += [f"- {f}" for f in (verify or {}).get("findings", [])]
    lines += ["", "Skill feedback to fold in before the next wave:" if feedback
              else "Skill feedback: none."]
    lines += [f"- {s}" for s in feedback]
    lines += ["", "Per batch:"]
    lines += [f"- {b['id']}: {r['status']}. {r['one_line_summary']}"
              + (f" {r['pr_url']}" if r.get("pr_url") else "")
              for b, r in zip(BATCHES, results)]
    brief_tmp = BRIEF_PATH.with_suffix(".brief.md.tmp")
    brief_tmp.write_text("\n".join(lines) + "\n")
    os.replace(brief_tmp, BRIEF_PATH)


async def main():
    await register_workflow(META)
    check_write_targets(BATCHES)
    log(f"wave {WAVE}: {len(BATCHES)} batches, width {WIDTH}, breaker at {BREAKER}")

    sem = asyncio.Semaphore(WIDTH)
    breaker = Breaker(BREAKER)
    results = await asyncio.gather(*(run_batch(b, sem, breaker) for b in BATCHES))

    reported = Counter(t for r in results for t in r.get("write_targets", []))
    surprises = [t for t, c in reported.items() if c > 1]
    undeclared = {}
    for b, r in zip(BATCHES, results):
        extra = sorted(set(r.get("write_targets", [])) - set(b["write_targets"]))
        if extra:
            undeclared[b["id"]] = extra
    unreported = [b["id"] for b, r in zip(BATCHES, results)
                  if r["status"] == "PASS" and not r.get("write_targets")]
    auto_merge = AUTO_MERGE
    if surprises:
        auto_merge = False
        log(f"WARNING: children reported overlapping write targets after the fact: {surprises}. "
            "Auto-merge is off for this wave; a human decides at wave close.")
    if undeclared:
        auto_merge = False
        log(f"HALT: children wrote outside their declared targets: {undeclared}. "
            "Auto-merge is off for this wave; a human decides at wave close.")
    if unreported:
        auto_merge = False
        log(f"HALT: PASS children did not report write targets: {unreported}. "
            "Auto-merge is off for this wave; a human decides at wave close.")

    passed = [{"batch": b["id"], "units": b["units"], "pr_url": r.get("pr_url", ""),
               "branch": r.get("branch", "")}
              for b, r in zip(BATCHES, results) if r["status"] == "PASS"]
    verify = None
    if passed:
        log(f"verify: {len(passed)} batches to an independent session")
        try:
            verify = await agent(verify_prompt(passed, auto_merge), phase="verify", schema=VERIFY_SCHEMA,
                                 label=f"verify-wave-{WAVE}", repos=[REPO])
        except WorkflowAgentError as e:
            verify = {"wave_verdict": "FAIL", "unit_verdicts": {},
                      "findings": [f"verifier session died: {e}"]}
    else:
        log("verify: skipped, no batch passed")

    closed = (breaker.tripped_on is None and not surprises and not undeclared and not unreported
              and verify is not None and verify["wave_verdict"] == "PASS"
              and all(r["status"] == "PASS" for r in results))
    result_tmp = RESULT_PATH.with_suffix(".result.json.tmp")
    result_tmp.write_text(json.dumps({
        "wave": WAVE, "manifest_sha": MANIFEST_SHA, "width": WIDTH,
        "run_id": os.environ.get("WAVE_RUN_ID"),
        "breaker_tripped_on": breaker.tripped_on, "auto_merge": auto_merge,
        "closed": closed,
        "write_target_overlaps": surprises,
        "undeclared_write_targets": undeclared,
        "unreported_write_targets": unreported,
        "batches": [{"id": b["id"], **r} for b, r in zip(BATCHES, results)],
        "verify": verify,
    }, indent=2, sort_keys=True) + "\n")
    os.replace(result_tmp, RESULT_PATH)
    write_brief(results, verify, surprises, undeclared, unreported)
    log(f"wrote {RESULT_PATH} and {BRIEF_PATH}")
    log(f"wave {WAVE} verdict: {verify['wave_verdict'] if verify else 'NO PASSING BATCHES'}")


asyncio.run(main())
