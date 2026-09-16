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
