Playbook: Verify production readiness, obtain independent sign-off, and execute the customer-authorized cutover.

## Entry criteria
- The parallel-run ledger shows the plan's agreed N consecutive green cycles; no dependency entry is UNDECIDED or IMPLEMENTED without evidence; no D10 is open. Any gap routes back before STOP E is presented.
- All waves are merged and green; recon reports, target/governance parity, costs, and the unverified-path register are complete.
- In DEGRADED mode, attach source/export manifests, coverage limits, and a customer-run in-perimeter recon as a STOP E entry criterion.
- The deployable exists in the target catalog, scheduled jobs are paused or controlled, and the DEGRADED criterion is explicitly accepted or closed.

## Procedure
1. Assemble the evidence pack: inventory, analysis, plan, wave manifests/results, recon JSON, parallel-run ledger, audit memo, costs, open risks, and regenerated `05_progress.md`.
2. Verify every path end to end with a fresh session: deployability, schema and governance parity, secrets, schedules, consumers, rollback, and idempotent rerun.
3. Run an independent audit and a consumer rehearsal against the target; record failures as converted-code fixes or explicit exceptions.
4. Regenerate `05_progress.md`, update `.migration/06_decisions.md`, and confirm no unverified path blocks cutover. Do not self-authorize the production flip.
5. Present the evidence pack at STOP E; only the customer-held cutover principal authorizes the flip.
6. Execute the approved consumer repoint, smoke test, rollback watch, and decommission clock; record timestamps, owners, and evidence.

## Specifications
- Deliverable: signed evidence pack, STOP E authorization, cutover record, rollback watch, and decommission plan.
- Validation: independent audit and rehearsal pass, all gates are cited, and no source modification occurred.

## Pointers
Cutover gates and notification behavior are in `references/contract.md`; source and target safety rules are in `AGENTS.md`.
