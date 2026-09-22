---
name: migration-fanout
description: "Run one migration wave as a dynamic workflow: N unit-migration children in parallel, then one independent verifier, with write-target collision checks, a circuit breaker, and a ten-line wave brief. Use from the orchestrator for every wave with more than one batch. Never hand-manage child sessions when this exists."
---

# migration-fanout

The workflow owns one wave from launch through result writing. It launches children,
checks their reports, runs independent verification, and writes the result and brief.

## How to use it

1. Write and commit `wave-<N>.json`, including complete batch briefs, targets, gates,
   `stop_c`, and `gates_sha`.
2. Run the signed doctor over the manifest:
   `python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo> --wave .migration/waves/wave-<N>.json --hook-probe-result blocked:<nonce>`.
   It writes `wave-<N>.doctor.json`, signed over the manifest bytes and accepted for 15 minutes.
   Commit it beside the manifest: it carries no secret values (manifest hash, timestamp,
   principal name, host, readiness, `inputs_sha`, signature), and children reuse it per
   `skills/factory-doctor/SKILL.md`, running the doctor in full when it is stale or absent.
3. Write `~/.migration/waves/current.json`:
   `{"manifest": "wave-<N>.json", "hook_probe": "blocked:<nonce>|not-blocked|unknown", "workspace": "/abs/repo", "plugin": "<plugin>"}`.
4. Run `run_workflow(workflow_name="migration-wave-<N>", script_path="<plugin>/skills/migration-fanout/workflow.py")`.
5. Read `.migration/waves/wave-<N>.result.json` and `.migration/waves/wave-<N>.brief.md`,
   then render progress with `python3 <plugin>/skills/migration-fanout/progress.py .migration`.

A result file means the wave will not relaunch. To rerun deliberately, delete the result,
obtain a new STOP C ledger row, put that row in the manifest's `stop_c`, refresh the doctor,
and run the workflow again.

## Smoke check

The credential-free smoke manifest exercises pointer discovery, registration, and result
writing without a doctor, ledger approval, git origin, or child launch:

```sh
mkdir -p /tmp/fanout-smoke/.migration/waves && printf '{"smoke": true, "wave": 0, "width": 1, "batches": []}' > /tmp/fanout-smoke/.migration/waves/wave-0.json && mkdir -p ~/.migration/waves && printf '{"manifest": "wave-0.json", "hook_probe": "unknown", "workspace": "/tmp/fanout-smoke", "plugin": "<plugin>"}' > ~/.migration/waves/current.json
```

Then run `run_workflow(workflow_name="smoke-wave-0", script_path="<plugin>/skills/migration-fanout/workflow.py")`.
Expect `/tmp/fanout-smoke/.migration/waves/wave-0.result.json` with `"smoke": true`.

## What it enforces

| Guard | Summary |
|---|---|
| Manifest check | Validates shape, source names, batches, units, width, and migration contract. |
| Signed doctor gate | Requires an HMAC-bound doctor record with 15-minute freshness, hook probe, and matching capabilities. |
| STOP C gates_sha approval | Requires the ledger row named by `stop_c` to approve the exact gate hash. |
| One approval one run | Locks `wave-N.runs.jsonl` and refuses a spent STOP C row. |
| Duplicate wave | Refuses any existing result, including halted or unreadable files. |
| Manifest name / pipelines barrier | Checks tags and waits for declared sibling manifests on origin. |
| Collision check | Rejects overlapping declared targets within and across waves. |
| Declared targets match call graph | Checks unit mapping targets against observed writes. |
| Shared tables across waves | Requires bounded predicates for shared readers. |
| Width | Limits concurrent child launches to the manifest width. |
| Time budget | Passes each child its declared execution budget. |
| Circuit breaker | Stops launching after the configured repeated failure threshold. |
| Single ledger writer | Keeps workflow-owned ledger artifacts out of child writes. |
| Child report | Validates schema, status, recon evidence, and changed paths. |
| Ledger gate | Reclassifies unauthorized migration-ledger changes. |
| Merge authority | Requires harness evidence or an explicit human override. |
| Acceptance gates | Requires every declared gate to pass or have a valid waiver. |
| Resync | Runs only the declared parent-owned resync and holds affected merges on trouble. |
| Independent verify | Rechecks passing batches from the base branch; a `degraded: true` wave runs the harness in `--mode structural`, never merge-eligible. |
| Verifier verdicts | Normalizes verdicts and rejects missing, extra, or contradictory results. |
| Wave close | Proves merges against the gated PR head before closing the wave. |

Detailed guard behavior is in [references/guards.md](references/guards.md).
