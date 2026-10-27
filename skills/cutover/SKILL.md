---
name: cutover
description: The final ticket — assembles the evidence pack, runs the independent audit and consumer rehearsal, presents it for the human's explicit authorization, then executes the approved repoint. Load it for cutover, sign-off, or go-live work.
---

# cutover

You verify production readiness end to end, obtain independent sign-off, and execute the
customer-authorized cutover. Devin never self-authorizes the production flip (`AGENTS.md`).

## Ticket inputs (cold start)

- The plan's cutover step and its gates; the agreed green-cycle count and live window.
- All wave manifests and `wave-<N>.result.json` results; the recon reports and
  `recon_tolerances.json`; the parallel-run verdict.
- `skills/data-reconciliation/SKILL.md` — routine parity and DEGRADED entry criteria.

## Procedure

1. Check entry criteria: the parallel run shows the plan's agreed consecutive green cycles; no
   blocker or dependency is open without evidence; all waves merged and green; every writing
   routine's `routine_parity` is `proven` (each `unproven` one is listed by name as an exception
   in the evidence pack; a `failed` one routes back). In DEGRADED mode, attach source/export
   manifests, coverage limits, and a customer-run in-perimeter recon.
2. Assemble the evidence pack: inventory, analysis, plan, wave manifests/results, recon JSON,
   parallel-run records, audit memo, costs, open risks.
3. Verify every path end to end with a fresh pass: deployability, schema and governance parity,
   secrets, schedules, consumers, rollback, and idempotent rerun.
4. Run an independent audit and a consumer rehearsal against the target; record failures as
   converted-code fixes or explicit exceptions.
5. Render the one-screen packet (`references/cutover-packet.md`) from the evidence pack and
   present it for authorization; do not self-authorize the production flip.
6. On authorization, execute the approved consumer repoint, smoke test, rollback watch, and
   decommission clock; record timestamps, owners, and evidence.

## Evidence the ticket must attach

- The evidence pack (paths), the rendered packet, and the unverified-path register.
- The audit and rehearsal results; the authorization record.
- The cutover record: timestamps, owners, rollback watch, decommission plan.
