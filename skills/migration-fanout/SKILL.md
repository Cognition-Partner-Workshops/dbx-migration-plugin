---
name: migration-fanout
description: "Run one migration wave as a dynamic workflow: N unit-migration children in parallel, then one independent verifier, with write-target collision checks, a circuit breaker, and the wave card. Use from the wave ticket's worker for every wave with more than one batch. Never hand-manage child sessions when this exists."
---

# migration-fanout

The workflow owns one wave from launch through result writing. It launches children, checks
their reports, runs independent verification, and writes the result and the wave card
(`cards.py`). Its pure parts live beside it: `decisions.py` (plan hash, `merge_overrides`
selection and scope), `manifest.py` (manifest grammar, skill paths, gate shapes, write-target and
predicate checks), `report.py` (child, verifier, close and resync report schemas, protected-file
checks); the sandbox imports them from the pointer's plugin root.

## How to use it

1. The "Run wave N" ticket's worker reads the committed `wave-<N>.json` the wave-plan ticket
   wrote: `plan_step` (this ticket's plan step id), `child_skill` / `verify_skill` (skill names
   under `<plugin>/skills/`, embedded verbatim in the child and verifier prompts), batch briefs
   (`references/brief_template.md`: under 4000 chars, pointing at the manifest and capabilities
   file rather than restating them), targets, gates, and the `merge_overrides` / gate
   `decision_id`s the human selected in the plan.
2. Refresh the signed doctor record when it is older than `doctor_max_age`:
   `python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo> --wave .migration/waves/wave-<N>.json --hook-probe-result blocked:<nonce>`
   writes `wave-<N>.doctor.json`, signed over the manifest bytes. Commit it beside the manifest:
   it carries no secret values (manifest hash, timestamp, principal name, host, readiness,
   `inputs_sha`, signature), and children reuse it per `skills/factory-doctor/SKILL.md`, running
   the doctor in full when it is stale or absent.
3. Write `~/.migration/waves/current.json`; its `plugin` key names the plugin root the skills and
   `pipeline_updates.py` resolve from:
   `{"manifest": "wave-<N>.json", "hook_probe": "blocked:<nonce>|not-blocked|unknown", "workspace": "/abs/repo", "plugin": "<plugin>"}`.
4. Run `run_workflow(workflow_name="migration-wave-<N>", script_path="<plugin>/skills/migration-fanout/workflow.py")`.
5. Post `.migration/waves/wave-<N>.card.md` on the ticket with the result's `brief` lines and a link
   to `wave-<N>.result.json`; the manager ticks the gate.

A result file means the wave will not relaunch. To rerun deliberately, delete the result and
re-dispatch the ticket for the same plan step: `wave-<N>.runs.jsonl` records
`{plan_step, manifest_sha, started}` per launch, so the same manifest bytes launch once, and a
changed manifest launches again only with a fresh doctor signature over the new bytes.

`merge_overrides` entries are `{"decision": "<slug>", "units": ["u1", "u2"], "blocker_classes":
["rerun_policy"]}`. An entry clears a batch only when it is the single entry covering every unit
of the batch and the child claims its decision. `blocker_classes` scopes it to the harness blocker
classes it forgives; a unit with any other class stays blocked. An entry without `blocker_classes`
forgives every policy class (`rerun_policy`, `privilege_visibility`, `structural`, `evidence`)
and never `data`: rows that differ are fixed in converted code, and only an entry naming `data`
says otherwise. A unit whose result.json records no blocker classes fits no override. A waived
gate carries the `decision_id` of the plan decision that waived it; nothing is looked up at run
time.

A child reports gate evidence as the bare path under `.migration/recon/<unit>/`, or as
`{"path": ..., "label": ..., "verdict": ..., "rows": ...}` to annotate it; a path with a note
appended fails the gate with a message that says so.

## Wave card

`cards.py <wave-N.result.json>` renders the six lines the worker posts (`wave-<N>.card.md`):
head, blockers by class, decision, not done, PRs and evidence, reply. `parity PASS` is never
rendered as FAIL; merge policy is a separate word. The last line quotes the one reply the
manager gives: `accept wave <N>` when verified PRs await a manual merge, `relaunch` when a
batch failed, was held or the verifier failed, `Reply: none needed` when everything merged;
`halt` is always the alternative.

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
| Manifest check | Validates shape, plan step, skills, source names, batches, units, width, and migration contract. |
| Signed doctor gate | Requires an HMAC-bound doctor record with 15-minute freshness, hook probe, and matching capabilities. |
| One manifest one run | Holds `.wave-N.lock` for the run (a second launch of the same wave halts) and refuses a manifest whose sha is already in `wave-N.runs.jsonl`. |
| Repo preflight | Halts before anything launches when `repo` is not `host/owner/name` or origin points elsewhere. |
| Duplicate wave | Refuses any existing result, including halted or unreadable files. |
| Manifest name / pipelines barrier | Checks tags and waits for declared sibling manifests on origin. |
| Collision check | Rejects overlapping declared targets within and across waves. |
| Declared targets match call graph | Checks unit mapping targets against observed writes. |
| Shared tables across waves | Requires bounded predicates for shared readers. |
| Width | Limits concurrent child launches to the manifest width. |
| Time budget | Passes each child its declared execution budget. |
| Circuit breaker | Stops launching after the configured repeated failure threshold. |
| Single result writer | Keeps the result, card and run log out of child writes. |
| Child report | Validates schema, status, recon evidence, and changed paths. |
| Protected files gate | Reclassifies unauthorized `.migration/` changes as `protected_files_tampered`. |
| Merge authority | Requires harness evidence or the one committed `merge_overrides` entry covering the batch. |
| Acceptance gates | Requires every declared gate to pass; a waived gate carries its plan `decision_id` in the manifest. |
| Resync | Runs only the declared parent-owned resync and holds affected merges on trouble. |
| Independent verify | Rechecks passing batches from the base branch; a `degraded: true` wave runs the harness in `--mode structural`, never merge-eligible. |
| Verifier verdicts | Normalizes verdicts and rejects missing, extra, or contradictory results. |
| Wave close | Proves merges against the gated PR head before closing the wave. |

Detailed guard behavior is in [references/guards.md](references/guards.md).
