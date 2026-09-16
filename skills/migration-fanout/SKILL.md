---
name: migration-fanout
description: "Run one migration wave as a dynamic workflow: N unit-migration children in parallel, then one independent verifier, with write-target collision checks, a circuit breaker, and a ten-line wave brief. Use from the orchestrator for every wave with more than one batch. Never hand-manage child sessions when this exists."
---

# migration-fanout

One wave, one command. The orchestrator writes a wave manifest, runs this workflow, and
reads back a result file and a brief. It does not launch, poll, or collect children by hand.

## Why

Hand-managing 20 sessions is the expensive path: the orchestrator burns compute waiting,
misses a collision, and retries the same failure 20 times. The workflow does the launch,
wait, gate, and collect loop mechanically and records every result, so a resumed run never
redoes finished work.

## How to use it

1. The plan playbook writes `wave-<N>.json` (shape at the top of `workflow.py`). Every
   batch carries its complete brief and its write targets.
2. Commit it. Children clone the repo and read `.migration/` from there.
3. In THIS session's shell run the doctor:
   `python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo root> --wave .migration/waves/wave-<N>.json --hook-probe-result blocked:<nonce>`.
   Identity, host, catalogs, and the source block default from the manifest. It writes
   `wave-<N>.doctor.json`, signed over the manifest bytes and accepted for 15 minutes;
   do not commit it.
4. Write the pointer at `~/.migration/waves/current.json` — the `run_workflow` sandbox's
   cwd is the session home directory, not the repo, and the script looks for the pointer
   at or above its cwd — with
   `{"manifest": "wave-<N>.json", "mode": "start", "run_id": null, "hook_probe": "blocked:<nonce>", "workspace": "/abs/path/to/repo"}`.
   `run_id` is null on a first run: the tool only reports it once the run starts.
5. Run:
   `run_workflow(workflow_name="migration-wave-<N>", script_path="<plugin>/skills/migration-fanout/workflow.py")`.
6. Record the returned `run_id` in `.migration/waves/wave-<N>.run_id`, commit, read
   `result.json`/`brief.md`, and post the brief. At every wave close,
   `skills/migration-fanout/progress.py` renders `.migration/05_progress.md` from the wave result.
   Use `--refresh-merged` to record manual merges from git in `wave-<N>.merged.json`.
   Detects merge-commit, rebase, and squash merges from git; falls back to `gh pr view` when installed.
   For resume, rewrite the pointer with
   `mode: "resume"` and that run ID, re-run the doctor with `--wave` (fresh signature),
   and call `run_workflow` with the same run ID. Start, rerun, and smoke pointers carry
   `run_id: null`; only resume carries a run ID. For rerun, use `mode: "rerun"`, call
   `run_workflow` without `run_id`, and let it delete the old `.run_id` record before
   creating a fresh execution.

## What it enforces

| Guard | What it does |
|---|---|
| Manifest check | Refuses to start if the pointer or manifest is missing, malformed, or names another path; if a batch has no brief or no write targets, or batch ids repeat; if a unit id is not a plain directory name or belongs to two batches; or if the required `base_branch` or a `source` value is not one plain word (the engagement feature branch is required; `main`/`master` need a recorded trunk decision; `source.secret` remains a secret name). `wave: 0` is the serial shared-objects wave and requires `width: 1`; `auto_merge` defaults to false and may be enabled only by a recorded STOP A decision. It then launches only from `<manifest>.doctor.json`, the record the doctor signed in the orchestrator's shell (sha of the manifest bytes, signed within 15 minutes, HMAC keyed on manifest + identity + host, record carries the manifest's `source` and the hook probe result; tamper-evident, `.migration/` is review-protected; the key is derivable on purpose because the sandbox holds no secret to verify one with, so the record's job is binding and freshness, and the gate against a lying orchestrator is each child's own `--expect-identity` doctor run plus PR review), and refuses if that record is not `ready` or its identity, host, catalogs, guard_mode or stop_mode differ from the manifest's `capabilities`. |
| Collision check | Refuses to start if two batches claim the same write target. If children report an overlap after the fact, merges are held and the brief says so. |
| Closed-wave guard | Refuses to start only if the wave closed clean (`closed: true` in the result); a halted or failed wave resumes only with the same pointer `run_id` and `mode: resume`. Invalid result JSON also requires resume mode. Redo on purpose with `mode: rerun`. |
| Width | At most `width` children at once (default 20). |
| Circuit breaker | After `breaker_threshold` (default 3) children fail with the same `failure_class`, no new children launch. Running ones finish. |
| Single ledger writer | Children never edit `.migration/`. The result file is written here, once. |
| Ledger gate | The workflow reads each PR's diff from git itself, by the head the host holds for that PR of this repo (`git fetch origin refs/pull/N/head` from `pr_url`; `git diff --name-only <base>...<head>`, where `<base>` is `origin/<base_branch>` fetched now for a head it does not yet contain (a child launched on a resume forked after the verifier merged earlier units into the base; those are not its diff), and for a head it already contains the `base_sha` resolved once at launch and persisted in `<manifest>.base_sha` before anything runs, reused on resume, since a merged PR would otherwise diff to nothing; a replayed PASS keeps the head gated in the run being resumed only when its record carries the hash of the very prompt the child answered (`prompt_sha`, persisted per batch) and names the same `pr_url`; a record from before the brief changed or naming another PR is gated afresh), and the verifier's from `recon/wave-N` (a passed unit's `.migration/recon/<unit_id>/` drops out only where the verifier's tree is byte-identical to the gated PR head it merged or to the launch base it left untouched, so evidence the verifier rewrote stays); the branch name a child reports never selects what is inspected, and reported `changed_paths` can only add to it. A PASS whose PR touches anything under `.migration/` other than its own `.migration/recon/<unit_id>/` (verifier: `.migration/recon/wave-N/`) becomes FAIL with `failure_class: ledger_tampered`, which the breaker counts; a missing list, a `pr_url` that is not a PR of this repo, or a ref git cannot diff is the same. |
| Merge authority | A child's PASS is recorded only when it reports `merge_eligible: true` and every unit's own `.migration/recon/<unit>/result.json` at the gated PR head says `merge_eligible: true` (the batch reports one boolean for up to five units; the per-unit files decide), or with `merge_authority: {kind: human_override, decision_id: D-<n>}` where the row of `.migration/06_decisions.md` whose id cell is that `D-<n>` (a row that only cites it in prose is another decision) has a provenance cell that is exactly `user:<id>` (the reply's message or event id; a user mentioned in the row's text is not its author, and a `default-accepted` cell makes it the orchestrator's), names every unit of the batch and says `merge_override`. Anything else (a unit file missing or without a boolean `merge_eligible`, a `harness` claim on `false`, an id the ledger lacks, a `default-accepted` row, a row naming other units) becomes FAIL with `failure_class: merge_authority`. The result's `merge_overrides` and the brief list every override with its decision id. |
| Acceptance gates | Each batch declares `gates: [{id, kind: byte_compare\|export_file\|publish_leg\|row_parity\|structural\|custom, status: pending\|passed\|failed\|waived, evidence, decision_id?}]`, the gates STOP C approved for its units, and the manifest carries `stop_c` (the `D-<n>` row that approved this wave) and `gates_sha` = sha256 of `{wave, batches: {id: {units (sorted), gates: [[id, kind, status, evidence, decision_id or null], ...]}}}` as compact JSON with sorted keys. The approval is that one row of `.migration/06_decisions.md`, read as a markdown table: a cell holding the decision id, a provenance cell (`user:<event-id>`; under soft `stop_mode` the orchestrator's `default-accepted` once the stop elapsed, as at every stop) and a cell reading exactly `STOP C wave-<N> gates_sha <value>` for this wave. The workflow halts before launch unless the named row approves the manifest's value (the manifest cannot approve its own list; tokens scattered through prose, another wave's row, or a `default-accepted` row under hard `stop_mode` do not count), and again if the declaration no longer hashes to it (a gate renamed, added, dropped, re-kinded, or a status or evidence edited by hand after STOP C); the error prints the hash the current declaration has. One approval launches one run: every start or rerun appends `{stop_c, mode, run_id}` to `wave-N.runs.jsonl` before launching, and a run whose `stop_c` any line of that log or the last result already records halts until STOP C fires again and the manifest names the new row (a `resume` of the recorded run continues); a log line that is not such a record halts too. A child reports `gates: [{id, status: passed\|failed, evidence}]`: `passed` needs evidence that is a file under `.migration/recon/<unit>/` of one of its units at the PR head the workflow fetched from `refs/pull/N/head` (any other string, a path elsewhere, or a file the head lacks is unmet) and it cannot waive, rename or re-kind a gate or touch a `waived` one. The plan's `passed` is what STOP C expects, not proof: every gate but a waived one needs the child's result, and one the child did not report is unmet. A gate the child did not prove (no result, or `failed`) is `waived` if a human's `D-<n>` row of `.migration/06_decisions.md` (a provenance cell of exactly `user:<event-id>`, as for a merge override) says `waive` and names the gate id and every unit of the batch, and sits below the manifest's `stop_c` row (a waiver written for an earlier run's STOP C was that run's); that row is how a waiver decided after STOP C is recorded, since editing the manifest would change `gates_sha`. A PASS with any gate not `passed` or so `waived` becomes FAIL with `failure_class: gates`, so the wave cannot close over it. A `rerun` halts if the prior result file cannot be read as a JSON object, since it cannot then show which `stop_c` row was spent. The result's `waived_gates` and the brief list every waiver with its decision id. A small wave gathered by hand spends the row the same way, with `python3 skills/migration-fanout/workflow.py reserve` before its children launch (a `{stop_c, mode: reserve}` log line; a second `reserve`, or a workflow start, under a spent row halts), and applies the same rule with `python3 skills/migration-fanout/workflow.py gates <results.json>` (the children's `[{batch, pr_url, gates}]` rows; evidence is checked at each PR's fetched head; exits non-zero on any unmet gate, halts without an open reservation, and once every gate is met records the close as a `mode: gates` line, after which the row runs nothing more). The check that a row is unspent and the line that spends it happen under an exclusive lock on the log, so two starts cannot both take one approval. |
| Independent verify | A separate session re-runs recon over every PASS batch with `03_recon_tolerances.json` and `allowed_targets.json` from the base branch, never the PR's, and, if `auto_merge` is true, merges only what it marks PASS. |
| Resume | Same `run_id` replays finished agents. |

## After the run

- Breaker tripped: fix the dialect skill or knowledge note once, then update the pointer
  to `mode: resume` with the same `run_id`. Held-back batches launch; finished ones do not repeat.
- A batch is BLOCKED: its brief was incomplete. Fix the manifest (the prompt changes, so
  only that child re-runs).
- Verifier FAIL: reopen the named units as fresh children with the finding attached.

## Smoke mode

`mode: "smoke"` exists only to exercise the runner; it is honoured only when the manifest
has `"smoke": true`, `"wave": 0`, and `"width": 1`. It still checks the record's sha,
freshness, and signature and skips only the readiness/identity comparison (an offline
`--no-databricks` doctor record has neither); a smoke manifest never runs in any other mode.

## Small waves

The orchestrator playbook sets the threshold below which a wave is run in-session (launch the
whole batch, gather once, one fresh verify session) instead of through this workflow. Follow
that; the evidence is the same either way.
