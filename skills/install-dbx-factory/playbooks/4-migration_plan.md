Playbook: Turn one pipeline's analysis into a decided, costed execution plan and wave manifests for STOP C.

## What's Needed From User
- Current analysis, target profiles, committed tolerances, and every UNDECIDED dependency.
- Proposed width, gate posture, data-load posture, reviewer contract, and artifact destination.

## Procedure
1. Run factory-doctor with the hook probe, expected catalogs, source secret/parameters, and all unit mappings. Refresh `09_capabilities.json`; `fail`, `unverified` hook state, or an uncommitted allowlist/tolerance is a D10 blocker.
2. Decide every dependency, fire its lead-time request, and leave no UNDECIDED entry past STOP C.
3. Specify wave-0 scaffolding: catalog/schemas, federation or backfill, CI, recon harness, and bundle/job shells. Where the data-load posture is materialized backfill, **classify every table with the load-posture table in `skills/data-reconciliation/SKILL.md`** and give each class beyond CTAS its own line in the wall-clock math. A backfill too big for CTAS is a wave-0 workstream, not a footnote.
4. Write the schedule: unit batches, branches, profiles, recon rows, isolated namespaces, idempotency, base branch, width, legacy-query cap, breaker threshold, serial floor, reviewer throughput, parallel-run tier, and projected cost. XL units use the decision-first 2-PR split; small units batch 4–5, complex units 1–2.
5. Write each `.migration/waves/wave-<N>.json` with repo, child/verifier macros, width, breaker, `auto_merge`, source family/secret/params, batch ids/units/write targets/briefs, capability contract, `verify_depth`, and `cost_estimate`. Commit manifests before STOP C.
6. Specify mechanical recon commands, populations, source-volume assertions, fixture ownership, review/full-rerun caps, governance mapping and GAP rows. Children use fixture first; the independent verifier supplies live/snapshot/transactional merge evidence.
7. Write `<Pipeline>_plan.md` with risks, unresolved blockers, fired requests, schedule, gate, governance map, and scaffolding. Resolve STOP C before execution.

| Plan control | Required rule |
|---|---|
| Cost | Estimate `dbx-recon` statements/rows/warehouse hours per unit and verifier depth; compare estimate with actual wave cost. |
| Sizing | Batch S/M units at 4–5, complex units at 1–2, and split XL units decision-first; recalibrate after the pilot. |
| Gate | State exact commands, populations, baseline, fixture seed/ownership, source volume, live budget, three full-runs, and three review rounds. |
| Pilot | Wave 1 is width <= 5; one unit calibrates each new pattern class before parallel fan-out and feedback lands in the skill. |
| Governance | Map source grants/policies to approved target statements; unmatched semantics are GAP rows with an owner, never silently approximated. |

## Specifications
- Deliverable: plan with every dependency DECIDED, manifests committed, and STOP C approval.
- Validation: no UNDECIDED entries, self-contained batches, executable gates, narrow pilot, explicit approval on this run.

## Pointers
Manifest schema and fan-out enforcement are in `skills/migration-fanout/workflow.py`; stops, branch/merge, and D1–D10 are in `references/contract.md`.
