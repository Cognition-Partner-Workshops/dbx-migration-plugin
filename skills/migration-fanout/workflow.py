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
    PASS are merged, and only if the manifest says auto_merge (true by default; soft stop_mode keeps it true).
  - Re-running with the same run_id (also passed as WAVE_RUN_ID) replays finished children and only
    launches the rest.

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
  "verify_depth": "sampled",                  # optional; verifier Tier 3 depth for the wave:
                                              # sampled (default) | full. Per-batch "verify_depth"
                                              # overrides it (plan sets full on D4/finance-critical).
  "cost_estimate": {                          # optional; STOP C figures from `dbx-recon estimate`
    "source_statements": 240, "target_statements": 96,   # summed over the wave; actuals from
    "source_rows_fetched": 180000, "warehouse_hours": 1.5  # result.json["cost"] land in the brief
  },
  "capabilities": {                           # optional; copied from .migration/09_capabilities.json
    "identity": "<migration SP userName>",   # (factory-doctor). Children run the doctor with
    "catalogs": ["mig"],                       # --expect-identity and report BLOCKED on mismatch.
    "guard_mode": "block", "stop_mode": "hard", "ready": true
  },
  "batches": [
    {"id": "w2-b01", "units": ["orders_load", "orders_dim"],
     "write_targets": ["mig.orders", "mig.orders_dim"],
     "verify_depth": "full",                  # optional per-batch override
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

WAVES_DIR = Path(".migration/waves").resolve()
MANIFEST_PATH = Path(os.environ.get("WAVE_MANIFEST", ".migration/waves/wave-1.json")).resolve()
if MANIFEST_PATH.suffix != ".json" or not MANIFEST_PATH.is_relative_to(WAVES_DIR):
    raise SystemExit(f"WAVE_MANIFEST must be a .json file inside {WAVES_DIR}")
if not MANIFEST_PATH.exists():
    raise SystemExit(f"no wave manifest at {MANIFEST_PATH}. Set WAVE_MANIFEST to the file the "
                     "plan playbook wrote, then re-run.")
MANIFEST_TEXT = MANIFEST_PATH.read_text()
MANIFEST = json.loads(MANIFEST_TEXT)
MANIFEST_SHA = hashlib.sha256(MANIFEST_TEXT.encode()).hexdigest()[:12]
RESULT_PATH = MANIFEST_PATH.with_suffix(".result.json")
BRIEF_PATH = MANIFEST_PATH.with_suffix(".brief.md")
RUN_ID_PATH = MANIFEST_PATH.with_suffix(".run_id")
resume = os.environ.get("WAVE_RESUME") == "1"
prior = None

if RESULT_PATH.exists() and os.environ.get("WAVE_RERUN") != "1":
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
    if not RUN_ID_PATH.exists():
        raise SystemExit(f"no run record at {RUN_ID_PATH}; cannot verify WAVE_RUN_ID belongs to this wave "
                         "— use WAVE_RERUN=1 for a fresh run")
    if RUN_ID_PATH.read_text().strip() != run_id:
        raise SystemExit(f"WAVE_RUN_ID does not match {RUN_ID_PATH}; pass the recorded run_id to "
                         "run_workflow and WAVE_RUN_ID, or WAVE_RERUN=1 for a fresh run")
    if isinstance(prior, dict) and prior.get("run_id") and prior["run_id"] != run_id:
        raise SystemExit(f"WAVE_RUN_ID does not match prior result at {RESULT_PATH}; pass the recorded "
                         "run_id to run_workflow and WAVE_RUN_ID, or WAVE_RERUN=1 for a fresh run")

REPLAYED = {
    b["id"]: b["status"] for b in (prior or {}).get("batches", [])
    if b.get("status") in ("PASS", "FAIL", "BLOCKED")
} if resume and isinstance(prior, dict) else {}


# Verifier Tier 3 depth. sampled: Tier 1+2 plus a differently-seeded stratified Tier 3 (catches
# a child that fabricated or misread results at a fraction of the cost). full: keyed full diff,
# for units the plan flags cutover-critical (D4 external feed, finance). Never "threshold":
# the verifier's depth is a plan decision, not a tolerance-file side effect.
VERIFY_DEPTHS = ("sampled", "full")


def validate_manifest(m):
    """Fail here, in one line, instead of 20 children failing on a missing field."""
    for key in ("wave", "repo", "child_macro", "verify_macro", "batches"):
        if key not in m:
            raise SystemExit(f"manifest is missing '{key}'")
    if not m["batches"]:
        raise SystemExit("manifest has no batches")
    for key in ("width", "breaker_threshold", "child_minutes"):
        if key in m and (isinstance(m[key], bool) or not isinstance(m[key], int) or m[key] <= 0):
            raise SystemExit(f"manifest key '{key}' must be a positive integer")
    ids = Counter(b.get("id") for b in m["batches"])
    dupes = [i for i, c in ids.items() if c > 1 or not i]
    if dupes:
        raise SystemExit(f"batch ids must be unique and non-empty: {dupes}")
    for b in m["batches"]:
        for key in ("units", "write_targets", "brief"):
            if not b.get(key):
                raise SystemExit(f"batch {b['id']} is missing '{key}' (a child with no brief or "
                                 "no declared write targets cannot be launched safely)")
    if "verify_depth" in m and m["verify_depth"] not in VERIFY_DEPTHS:
        raise SystemExit(f"manifest 'verify_depth' must be one of {VERIFY_DEPTHS}")
    for b in m["batches"]:
        if "verify_depth" in b and b["verify_depth"] not in VERIFY_DEPTHS:
            raise SystemExit(f"batch {b['id']} 'verify_depth' must be one of {VERIFY_DEPTHS}")
    if "cost_estimate" in m and not isinstance(m["cost_estimate"], dict):
        raise SystemExit("manifest 'cost_estimate' must be an object (output of `dbx-recon estimate`, "
                         "summed over the wave)")
    if "capabilities" in m:
        caps = m["capabilities"]
        if not isinstance(caps, dict) or not isinstance(caps.get("identity"), str) or not caps["identity"]:
            raise SystemExit("manifest 'capabilities' must be an object with a non-empty 'identity' "
                             "(the migration principal's userName from 09_capabilities.json)")
        if not isinstance(caps.get("catalogs"), list) or not caps["catalogs"]:
            raise SystemExit("manifest 'capabilities.catalogs' must be the non-empty allowlist")
        if caps.get("ready") is False:
            raise SystemExit("manifest 'capabilities.ready' is false: the factory-doctor preflight failed; "
                             "fix the D10 and re-run the doctor before launching a wave")


validate_manifest(MANIFEST)


def validate_verify(verify, passed, auto_merge) -> list[str]:
    """Return verifier-output problems without reading files or mutating input."""
    problems = []
    if not isinstance(verify, dict):
        return ["verifier output invalid: expected an object"]
    expected = {p.get("batch") for p in passed}
    if None in expected:
        problems.append("verifier output invalid: passed batch is missing its id")
        expected.discard(None)
    verdicts = verify.get("unit_verdicts")
    if not isinstance(verdicts, dict):
        problems.append("verifier output invalid: unit_verdicts must be a dict")
        verdicts = {}
    missing = sorted(expected - set(verdicts))
    extra = sorted(set(verdicts) - expected)
    if missing:
        problems.append("verifier output invalid: missing verdicts for " + ", ".join(missing))
    if extra:
        problems.append("verifier output invalid: unexpected verdicts for " + ", ".join(extra))
    wave_verdict = verify.get("wave_verdict")
    if wave_verdict not in ("PASS", "FAIL"):
        problems.append("verifier output invalid: wave_verdict must be PASS or FAIL")
    for batch in sorted(expected):
        verdict = verdicts.get(batch)
        if verdict not in ("PASS", "FAIL"):
            problems.append(f"verifier output invalid: verdict for {batch} is {verdict!r}")
        elif wave_verdict == "PASS" and verdict != "PASS":
            problems.append(f"verifier output invalid: wave PASS contradicts {batch}={verdict}")
    if wave_verdict == "FAIL" and expected and all(verdicts.get(b) == "PASS" for b in expected):
        problems.append("verifier output invalid: wave FAIL contradicts all unit verdicts PASS")
    merged = verify.get("merged_prs")
    if merged is not None and not isinstance(merged, list):
        problems.append("verifier output invalid: merged_prs must be a list")
        merged = []
    if auto_merge:
        if merged is None:
            problems.append("verifier output invalid: merged_prs must be a list when auto_merge is on")
            merged = []
        for batch in passed:
            url = batch.get("pr_url")
            if url and url not in merged:
                problems.append(f"verifier output invalid: merged_prs is missing {url} for {batch['batch']}")
    if not isinstance(verify.get("findings"), list):
        problems.append("verifier output invalid: findings must be a list")
    return problems

WAVE = MANIFEST["wave"]
REPO = MANIFEST["repo"]
BATCHES = sorted(MANIFEST["batches"], key=lambda b: b["id"])
WIDTH = int(MANIFEST.get("width", 20))
BREAKER = int(MANIFEST.get("breaker_threshold", 3))
AUTO_MERGE = bool(MANIFEST.get("auto_merge", True))
CHILD_MINUTES = int(MANIFEST.get("child_minutes", 45))
VERIFY_DEPTH = MANIFEST.get("verify_depth", "sampled")


def batch_verify_depth(batch) -> str:
    return batch.get("verify_depth", VERIFY_DEPTH)

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
        "recon_cost": {"type": "object",
                       "description": "result.json['cost'] of the final live/snapshot run"},
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
        "recon_cost": {"type": "object",
                       "description": "summed result.json['cost'] over the verifier's re-runs"},
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
        + capability_block()
        + "Rules that override anything else:\n"
        "- Do not edit files under .migration/. The workflow writes the ledger from your report.\n"
        "- Do not merge your own PR.\n"
        "- status=PASS requires a live or snapshot recon PASS (result.json merge_eligible=true). "
        "Fixture evidence is never PASS.\n"
        "- If the recon harness fails 3 full runs, stop and report status=FAIL with a short "
        "failure_class (for example 'timestamp_precision', 'decimal_rounding', 'missing_rule').\n"
        "- Report every rule you had to derive yourself in skill_feedback.\n"
        "- Copy result.json['cost'] of your final live/snapshot run into recon_cost; the wave brief "
        "compares it with the STOP C estimate.\n"
        "- one_line_summary is for a human skimming 20 of these: what landed, or why not."
    )


def capability_block():
    caps = MANIFEST.get("capabilities")
    if not caps:
        return ""
    return (
        "CAPABILITY CONTRACT (from the orchestrator's factory-doctor run): "
        f"{json.dumps(caps, sort_keys=True)}\n"
        f"Before converting anything run the factory-doctor skill with --role child "
        f"--expect-identity {caps['identity']} and complete its hook probe. Any 'fail' row "
        "(identity mismatch, harness missing, hooks not applied, allowlist differs from the contract) "
        "means status=BLOCKED with the check id in one_line_summary. Never continue as a different "
        "identity, never run `databricks auth login`, never edit .migration/allowed_targets.json.\n\n"
    )


def verify_prompt(passed, auto_merge):
    depths = {b["id"]: batch_verify_depth(b) for b in passed}
    merge_line = (
        "Merge every PR you mark PASS and list it in merged_prs, even if another unit in the wave failed; "
        "failed units are reopened next launch."
        if auto_merge else
        "Do not merge anything; return per-unit verdicts. The orchestrator surfaces the PASS PRs in the "
        "wave brief, merges them at wave close (or records the human's decision in the kit's decision log "
        "under .migration/), "
        "and the next wave does not launch until that is done.")
    return (
        f"You are the independent verifier for wave {WAVE}. Repo: {REPO}. You did not write "
        f"any of this code.\nRun the playbook {MANIFEST['verify_macro']} exactly as written over "
        f"these batches:\n{json.dumps(passed, sort_keys=True, indent=1)}\n\n"
        "Re-run the recon harness yourself. Do not trust the PR's pasted evidence. "
        "Mark a unit PASS only if you re-ran the harness in live or snapshot mode and result.json says "
        "merge_eligible=true. "
        f"Run with `--depth <d>` per batch, exactly as listed here: {json.dumps(depths, sort_keys=True)} "
        "(sampled = Tier 1+2 plus a stratified Tier 3 with a seed different from the child's; full = keyed "
        "full diff). Never lower a batch's depth; raising it is allowed and noted in findings. "
        "Sum result.json['cost'] over your runs into recon_cost.\n"
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
        if (out["status"] == "PASS"
                and (not out.get("pr_url") or not out.get("branch"))):
            out["status"] = "FAIL"
            out["failure_class"] = "missing_pr"
            out["one_line_summary"] = (
                "PASS downgraded: no PR URL/branch reported; " + out["one_line_summary"])
        if out["status"] != "PASS" and batch["id"] not in REPLAYED:
            breaker.record(out.get("failure_class") or "unclassified")
        log(f"done   {batch['id']}: {out['status']} / recon {out['recon_verdict']}: "
            f"{out['one_line_summary']}")
        return out


COST_KEYS = ("source_statements", "target_statements", "source_rows_fetched", "target_rows_fetched")


def sum_cost(costs) -> dict:
    """Sum result.json['cost'] dicts; a side whose adapter did not count stays None."""
    total: dict = {k: 0 for k in COST_KEYS} | {"elapsed_s": 0.0}
    for c in costs:
        if not isinstance(c, dict):
            continue
        for k in COST_KEYS:
            v = c.get(k)
            if v is None:
                total[k] = None
            elif total[k] is not None and isinstance(v, (int, float)) and not isinstance(v, bool):
                total[k] += v
        if isinstance(c.get("elapsed_s"), (int, float)):
            total["elapsed_s"] += c["elapsed_s"]
    return total


def cost_line(results, verify) -> str:
    """Estimate (STOP C) against actuals (children + verifier), so the next wave's estimate
    can be corrected instead of guessed again."""
    est = MANIFEST.get("cost_estimate")
    actual = sum_cost([r.get("recon_cost") for r in results]
                      + ([verify.get("recon_cost")] if isinstance(verify, dict) else []))
    if est is None and all(actual[k] in (None, 0) for k in COST_KEYS):
        return "Cost: no estimate in the manifest and no recon_cost reported."
    def fmt(d):
        return ", ".join(f"{k}={d.get(k)}" for k in COST_KEYS if d.get(k) is not None) or "n/a"
    return (f"Cost: estimated {fmt(est or {})}; actual {fmt(actual)}, "
            f"harness time {round(actual['elapsed_s'])}s. Verifier depth {VERIFY_DEPTH}"
            + (", overrides: " + ", ".join(f"{b['id']}={b['verify_depth']}" for b in BATCHES if "verify_depth" in b)
               if any("verify_depth" in b for b in BATCHES) else "") + ".")


def write_brief(results, verify, surprises, undeclared, unreported, auto_merge):
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
    if not auto_merge:
        urls = [r["pr_url"] for r in results
                if r["status"] == "PASS" and r.get("pr_url")]
        lines.append("Awaiting manual merge: " + (", ".join(urls) or "none reported"))
    lines.append(cost_line(results, verify))
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
    if not resume:
        run_id = os.environ.get("WAVE_RUN_ID")
        if not run_id:
            raise SystemExit("WAVE_RUN_ID is required on the first run so the wave can be resumed "
                             "(pass the run_workflow run_id)")
        run_id_tmp = RUN_ID_PATH.with_suffix(".run_id.tmp")
        run_id_tmp.write_text(run_id + "\n")
        os.replace(run_id_tmp, RUN_ID_PATH)
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

    verify_problems = validate_verify(verify, passed, auto_merge) if verify is not None else []
    if verify_problems:
        if not isinstance(verify, dict):
            verify = {"wave_verdict": "FAIL", "unit_verdicts": {}, "findings": []}
        verify["wave_verdict"] = "FAIL"
        if not isinstance(verify.get("findings"), list):
            verify["findings"] = []
        verify["findings"].extend(verify_problems)
    closed = (breaker.tripped_on is None and not surprises and not undeclared and not unreported
              and not verify_problems and verify is not None and verify["wave_verdict"] == "PASS"
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
    write_brief(results, verify, surprises, undeclared, unreported, auto_merge)
    log(f"wrote {RESULT_PATH} and {BRIEF_PATH}")
    log(f"wave {WAVE} verdict: {verify['wave_verdict'] if verify else 'NO PASSING BATCHES'}")


asyncio.run(main())
