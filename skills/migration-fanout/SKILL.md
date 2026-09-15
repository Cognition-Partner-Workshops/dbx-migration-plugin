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

1. The plan playbook writes `.migration/waves/wave-<N>.json` (shape at the top of
   `workflow.py`). Every batch carries its complete brief and its write targets.
2. Run `factory-doctor --wave .migration/waves/wave-<N>.json` in the orchestrator
   shell. It writes a signed `.doctor.json` beside the manifest. The record binds the
   manifest bytes and observed identity, and is accepted for 15 minutes.
3. Commit the manifest and doctor record. Write `.migration/waves/current.json` with
   `manifest: "wave-<N>.json"`, `mode: "start"`, `run_id` from the workflow tool,
   and `hook_probe` set to `blocked:<nonce>`, `not-blocked`, or `unknown` from the
   probe run in this shell. Include `workspace` when the pointer is outside the repo.
4. Run:
   `run_workflow(workflow_name="migration-wave-<N>", script_path="<this dir>/workflow.py")`.
   The workflow discovers the pointer from its cwd and parents; it reads no environment
   variables. Record the returned run ID in the pointer before a resume.
5. When it returns, read `.migration/waves/wave-<N>.result.json` (machine) and
   `.migration/waves/wave-<N>.brief.md` (human). Post the brief as the wave-close message.
6. If it timed out or halted, update the pointer to `mode: "resume"` with the recorded
   `run_id`. Finished children replay; only the rest launch. Use `mode: "rerun"` to
   deliberately redo a wave. A smoke manifest uses `mode: "smoke"` and must have
   `smoke: true`, `wave: 0`, and `width: 1`; it still verifies the signed record,
   digest, timestamp, and hook probe while skipping readiness and identity comparison.

## What it enforces

| Guard | What it does |
|---|---|
| Manifest check | Refuses to start if the pointer or manifest is missing, malformed, or names another path; if a batch has no brief or no write targets, or batch ids repeat; if a unit id is not a plain directory name or belongs to two batches; or if the required `base_branch` or a `source` value is not one plain word (the engagement feature branch is required; `main`/`master` need a recorded trunk decision; `source.secret` remains a secret name). `wave: 0` is the serial shared-objects wave and requires `width: 1`; `auto_merge` defaults to false and may be enabled only by a recorded STOP A decision. |
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

## Small waves

The orchestrator playbook sets the threshold below which a wave is run in-session (launch the
whole batch, gather once, one fresh verify session) instead of through this workflow. Follow
that; the evidence is the same either way.
