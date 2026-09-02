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
   with `WAVE_MANIFEST=.migration/waves/wave-<N>.json` and the workflow's `run_id` also
   passed as `WAVE_RUN_ID` in the environment, on both the first run and any resume.
4. When it returns, read `.migration/waves/wave-<N>.result.json` (machine) and
   `.migration/waves/wave-<N>.brief.md` (human). Post the brief as the wave-close message.
5. If it timed out, halted, or the session slept, run it again with the same `run_id` and
   `WAVE_RUN_ID=<run_id>` plus `WAVE_RESUME=1`. Finished children replay; only the rest launch.

## What it enforces

| Guard | What it does |
|---|---|
| Manifest check | Refuses to start if the manifest is missing, a batch has no brief or no write targets, or batch ids repeat. |
| Collision check | Refuses to start if two batches claim the same write target. If children report an overlap after the fact, merges are held and the brief says so. |
| Closed-wave guard | Refuses to start only if the wave closed clean (`closed: true` in the result); a halted or failed wave resumes only with the same `run_id` and `WAVE_RESUME=1`. Invalid result JSON also requires `WAVE_RESUME=1` to continue. Redo on purpose with `WAVE_RERUN=1`. |
| Width | At most `width` children at once (default 20). |
| Circuit breaker | After `breaker_threshold` (default 3) children fail with the same `failure_class`, no new children launch. Running ones finish. |
| Single ledger writer | Children never edit `.migration/`. The result file is written here, once. |
| Independent verify | A separate session re-runs recon over every PASS batch and, if `auto_merge` is true, merges only what it marks PASS. |
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
