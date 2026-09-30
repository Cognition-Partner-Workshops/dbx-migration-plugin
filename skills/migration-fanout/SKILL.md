---
name: migration-fanout
description: "Run one migration wave as a dynamic workflow: N unit-migration children in parallel, then one independent verifier, with write-target collision checks, a circuit breaker, and a ten-line wave brief. Use from the wave ticket's worker for every wave with more than one batch. Never hand-manage child sessions when this exists."
---

# migration-fanout

The workflow owns one wave from launch through result writing. It launches children,
checks their reports, runs independent verification, and writes the result.

## How to use it

1. The "Run wave N" ticket's worker reads the committed `wave-<N>.json` manifest and the signed
   `wave-<N>.doctor.json` record the wave-plan ticket left beside it (the record is committed
   with the manifest — it holds no secret values, so commit it). The manifest carries
   `plan_step` (the plan step id of this ticket), `child_skill`, `verify_skill`, complete batch
   briefs, targets, gates, and any `merge_overrides` / gate `decision_id`s the human selected.
2. Write `~/.migration/waves/current.json` — its `plugin` key names the plugin root the
   skills and `pipeline_updates.py` resolve from:
   `{"manifest": "wave-<N>.json", "hook_probe": "blocked:<nonce>|not-blocked|unknown", "workspace": "/abs/repo", "plugin": "<plugin>"}`.
3. Run `run_workflow(workflow_name="migration-wave-<N>", script_path="<plugin>/skills/migration-fanout/workflow.py")`.
4. Post `.migration/waves/wave-<N>.result.json` and its `brief` lines on the ticket; the manager
   ticks the gate.

A result file means the wave will not relaunch. To rerun deliberately, delete the result, update
the manifest if needed, and re-dispatch the ticket for the same plan step.

## Smoke check

The credential-free smoke manifest exercises pointer discovery, registration, and result
writing without a doctor, git origin, or child launch:

```sh
mkdir -p /tmp/fanout-smoke/.migration/waves && printf '{"smoke": true, "wave": 0, "width": 1, "batches": []}' > /tmp/fanout-smoke/.migration/waves/wave-0.json && mkdir -p ~/.migration/waves && printf '{"manifest": "wave-0.json", "hook_probe": "unknown", "workspace": "/tmp/fanout-smoke", "plugin": "<plugin>"}' > ~/.migration/waves/current.json
```

Then run `run_workflow(workflow_name="smoke-wave-0", script_path="<plugin>/skills/migration-fanout/workflow.py")`.
Expect `/tmp/fanout-smoke/.migration/waves/wave-0.result.json` with `"smoke": true`.

## What it enforces

| Guard | Summary |
|---|---|
| Manifest check | Validates shape, plan step, source names, batches, units, width, and migration contract. |
| Signed doctor gate | Requires an HMAC-bound doctor record with 15-minute freshness, hook probe, and matching capabilities. |
| Plan step + run log | Locks `wave-N.runs.jsonl` and refuses a manifest that already ran. |
| Duplicate wave | Refuses any existing result, including halted or unreadable files. |
| Manifest name / pipelines barrier | Checks tags and waits for declared sibling manifests on origin. |
| Collision check | Rejects overlapping declared targets within and across waves. |
| Declared targets match call graph | Checks unit mapping targets against observed writes. |
| Shared tables across waves | Requires bounded predicates for shared readers. |
| Width | Limits concurrent child launches to the manifest width. |
| Time budget | Passes each child its declared execution budget. |
| Circuit breaker | Stops launching after the configured repeated failure threshold. |
| Single result writer | Keeps workflow-owned artifacts out of child writes. |
| Child report | Validates schema, status, recon evidence, and changed paths. |
| Protected files gate | Reclassifies unauthorized `.migration/` changes. |
| Merge authority | Requires harness evidence or a manifest `merge_overrides` entry. |
| Acceptance gates | Requires every declared gate to pass; a waived gate carries its plan `decision_id` in the manifest. |
| Resync | Runs only the declared parent-owned resync and holds affected merges on trouble. |
| Independent verify | Rechecks passing batches from the base branch; a `degraded: true` wave runs the harness in `--mode structural`, never merge-eligible. |
| Verifier verdicts | Normalizes verdicts and rejects missing, extra, or contradictory results. |
| Wave close | Proves merges against the gated PR head before closing the wave. |

Detailed guard behavior is in [references/guards.md](references/guards.md).
