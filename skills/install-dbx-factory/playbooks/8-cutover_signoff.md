Playbook: Close out one pipeline: end-to-end verification, consumer cutover plan executed, independent skeptical audit, dependency closure, evidence pack, and the user's explicit cutover authorization at STOP E.

## Overview
Everything before this was construction; this is acceptance. The pipeline's converted code is merged, the parallel-run ledger shows sustained green, and the question is now: flip the consumers, close the dependencies, and agree what turns the legacy pipeline off. The audit is performed by a session that did not migrate anything in this pipeline, reading artifacts skeptically rather than trusting the ledger.

```
[waves merged + parallel-run green]  ->  end-to-end run on schedule  ->  evidence pack
                                     ->  independent audit (fresh session)
                                     ->  consumer cutover per D4 decisions
                                     ->  dependency closure sweep  ->  STOP E (blocking)
                                     ->  authorized: flip routing points, start decommission clock
```

## What's Needed From User
- The cutover window and rollback posture (how long both systems stay warm after the flip).
- Presence at STOP E: cutover authorization is never inferred, never carried over from a prior approval.

## Procedure
1. **Entry criteria check**: all waves merged and green, parallel-run ledger shows the agreed N consecutive green cycles, no dependency entry UNDECIDED or IMPLEMENTED-without-evidence, no unclosed D10. **The unverified-path register is closed**: every unverified path carried on unit PRs has its owner, severity, and gate, and every line gated at or before STOP E is resolved or explicitly user-accepted; that register is the sign-off checklist finance/ops/audit sign, not a footnote. **The deployables exist**: every unit's job/Workflow is present in the workspace as a bundle/IaC-owned object, schedule PAUSED, named per convention; the flip unpauses tested objects, it deploys nothing new. **Governance parity** (via the `governance-mapping` skill): the effective-access diff legacy vs UC is green, meaning every (principal, object) pair matched or its lost/gained delta is user-accepted with a reason, and every source masking/row policy has a verified UC counterpart or a documented accepted gap; the parity report joins the evidence pack. Production-catalog grants from the approved mapping execute only as part of the STOP E flip, under the cutover principal. **If the engagement ran in DEGRADED recon mode**, one additional criterion: an in-perimeter recon run against production data, executed by the customer with the delivered harness, green and attached; sample parity alone never authorizes a production cutover. Any gap routes back before anything else happens.
2. **End-to-end verification run**: execute the full converted pipeline on its production schedule shape (real trigger, real parameters, full volume), then run the complete recon suite over every terminal output. This is the only place the whole pipeline is proven as a system rather than as units.
3. **Verify the evidence pack** in DOCS (waves append to it as they close; this step verifies completeness and fills gaps, it does not rebuild history): per-wave recon reports, the parallel-run ledger, the end-to-end run results, the dependency register's closure states, the tolerance record, and the deviations log (every accepted PROPOSED, every DRIFT-EXPLAINED, every deferral with its condition).
4. **Independent skeptical audit**: a fresh session that performed no migration work walks the evidence pack against the plan: re-runs a sample of recon gates, checks the coverage arithmetic still closes (no unit silently dropped between inventory and merge), verifies every routing point exists and flips where the register says, and writes an audit memo with findings. Findings are fixed or explicitly accepted by the user, never quietly absorbed.
5. **Consumer cutover rehearsal**: per D4 decision, verify each consumer's re-point mechanism against the non-production environment (dashboard renders from UC, extract byte-matches, API contract holds), and write the ordered cutover runbook with per-consumer rollback steps.
6. **STOP E (blocking)**: attach the evidence pack, audit memo, and cutover runbook. Present the decommission condition (what must stay true for how long before legacy turns off) and the parallel-run disposition (keep during rollback window, then decommission). Get explicit authorization.
7. **Execute the flip** per the runbook: routing points in register order, per-consumer verification after each, ledger updated after each. Start the decommission clock, update `.migration/05_progress.md` and `06_decisions.md`, and record what the next pipeline inherits (shared objects now live, tuned skills, updated knowledge notes).

## Specifications
- Deliverable: merged, cut-over pipeline; evidence pack and audit memo in DOCS; executed runbook; decommission clock started with a written condition.
- Validation: (1) audit performed by a non-migrating session; (2) every consumer verified post-flip; (3) every dependency closed or deferred-with-condition-and-owner; (4) STOP E authorization explicit, on this run.

## Advice and Pointers
- The end-to-end run catches what unit recon cannot: orchestration wiring, parameter passing, schedule semantics, and cross-unit interactions merged since the last wave report.
- Cut consumers over in blast-radius order: internal reports first, then dashboards, then external/partner feeds. Verify after each, not after all.
- The decommission condition is the forcing function customers thank you for later; without a written condition, legacy runs forever and the migration never financially closes.
- What this pipeline learned (dialect fixes, tolerance clarifications, skill updates) is the next pipeline's head start; spend ten minutes writing it down before closing.

## Forbidden Actions
- Do NOT let the migrating sessions audit their own work.
- Do NOT cut over any consumer whose D4 mechanism lacks rehearsal evidence.
- Do NOT proceed past STOP E on a prior session's approval, and do NOT infer authorization from silence.
- Do NOT decommission or stop the parallel run before the written rollback window elapses.
- Do NOT absorb audit findings silently; each is fixed or user-accepted on the record.
