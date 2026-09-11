---
name: migration-fanout
description: Run one migration wave as a dynamic workflow: N unit-migration children in parallel, then one independent verifier, with write-target collision checks, a circuit breaker, and a ten-line wave brief. Use from the orchestrator for every wave with more than one batch. Never hand-manage child sessions when this exists.
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

1. The plan playbook writes `.migration/waves/wave-<N>.json` (shape at the top of
   `workflow.py`). Every batch carries its complete brief and its write targets.
2. Commit it. Children clone the repo and read `.migration/` from there.
3. Run:
   `run_workflow(workflow_name="migration-wave-<N>", script_path="<this dir>/workflow.py")`
   with `WAVE_MANIFEST=.migration/waves/wave-<N>.json`, the workflow's `run_id` also
   passed as `WAVE_RUN_ID`, and `WAVE_HOOK_PROBE=blocked:<nonce>`, the outcome of the
   factory-doctor's probe command run in *this* session's shell (the workflow cannot run it, and
   nothing recorded earlier stands in for it), on both the first run and any resume.
4. When it returns, read `.migration/waves/wave-<N>.result.json` (machine) and
   `.migration/waves/wave-<N>.brief.md` (human). Post the brief as the wave-close message.
5. If it timed out, halted, or the session slept, run it again with the same `run_id` and
   `WAVE_RUN_ID=<run_id>` plus `WAVE_RESUME=1`. Finished children replay; only the rest launch.

## What it enforces

| Guard | What it does |
|---|---|
| Manifest check | Refuses to start if the manifest is missing, a batch has no brief or no write targets, or batch ids repeat; if a unit id is not a plain directory name or belongs to two batches; or if `base_branch` or a `source` value is not one plain word (they reach git and the children's command lines). It then re-runs the factory-doctor itself (a fresh `<manifest>.doctor.json`, never a stale one; the manifest's `source` block supplies `--source-family/--source-secret/--param`, `WAVE_HOOK_PROBE` the hook probe result) rather than trusting the editable `.migration/09_capabilities.json`, and refuses if that run is not `ready` or its identity, host, catalogs, guard_mode or stop_mode differ from the manifest's `capabilities`. |
| Collision check | Refuses to start if two batches claim the same write target. If children report an overlap after the fact, merges are held and the brief says so. |
| Closed-wave guard | Refuses to start only if the wave closed clean (`closed: true` in the result); a halted or failed wave resumes only with the same `run_id` and `WAVE_RESUME=1`. Invalid result JSON also requires `WAVE_RESUME=1` to continue. Redo on purpose with `WAVE_RERUN=1`. |
| Width | At most `width` children at once (default 20). |
| Circuit breaker | After `breaker_threshold` (default 3) children fail with the same `failure_class`, no new children launch. Running ones finish. |
| Single ledger writer | Children never edit `.migration/`. The result file is written here, once. |
| Ledger gate | The workflow reads each PR's diff from git itself, by the head the host holds for that PR of this repo (`git fetch origin refs/pull/N/head` from `pr_url`; `git diff --name-only <base_sha>...<head>`, where `base_sha` is `origin/<base_branch>` resolved once at launch and persisted in `<manifest>.base_sha` before anything runs, reused on resume: a PR already merged into a current base would diff to nothing), and the verifier's from `recon/wave-N` (the PRs it merged are in that diff; a passed unit's `.migration/recon/<unit_id>/` drops out only where the verifier's tree is byte-identical to the gated PR head, so evidence the verifier rewrote stays); the branch name a child reports never selects what is inspected, and reported `changed_paths` can only add to it. A PASS whose PR touches anything under `.migration/` other than its own `.migration/recon/<unit_id>/` (verifier: `.migration/recon/wave-N/`) becomes FAIL with `failure_class: ledger_tampered`, which the breaker counts; a missing list, a `pr_url` that is not a PR of this repo, or a ref git cannot diff is the same. |
| Independent verify | A separate session re-runs recon over every PASS batch with `03_recon_tolerances.json` and `allowed_targets.json` from the base branch, never the PR's, and, if `auto_merge` is true, merges only what it marks PASS. |
| Resume | Same `run_id` replays finished agents. |

## After the run

- Breaker tripped: fix the dialect skill or knowledge note once, then re-run with the same
  `run_id` and `WAVE_RESUME=1`. Held-back batches launch; finished ones do not repeat.
- A batch is BLOCKED: its brief was incomplete. Fix the manifest (the prompt changes, so
  only that child re-runs).
- Verifier FAIL: reopen the named units as fresh children with the finding attached.

## Small waves

The orchestrator playbook sets the threshold below which a wave is run in-session (launch the
whole batch, gather once, one fresh verify session) instead of through this workflow. Follow
that; the evidence is the same either way.
