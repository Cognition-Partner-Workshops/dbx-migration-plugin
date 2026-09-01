Playbook: Stand up the coexistence window: a scheduled reconciliation job that dual-runs migrated outputs against live legacy on every cycle, and an event-driven automation that spawns a remediation session the moment a run goes red.

## Overview
Between "the pipeline is converted" and "the legacy system is decommissioned" lies the parallel-run window: both systems live, consumers still on legacy (or split), and drift a constant threat. This playbook makes that window autonomous instead of a human vigil. It is also the piece with no in-IDE equivalent: the recon job goes red at 2am, the webhook spawns a session, and a diagnosed fix PR with evidence is waiting when the team comes online.

```
converted pipeline  ->  scheduled recon job (Databricks Workflows, every legacy cycle)
                    ->  green: append to evidence ledger, nothing to do
                    ->  red:   webhook -> automation -> remediation session
                               -> triage: drift vs conversion defect vs legacy change
                               -> fix PR (converted code only) + evidence, or escalation
```

## What's Needed From User
- The recon cadence (default: mirror the legacy pipeline's schedule) and the window's intended duration/exit condition (feeds the cutover plan). Windows are **risk-tiered per the plan** (short for reference lookups, full plus period-end boundary for money-path and ML scoring); a unit's cutover eligibility arrives when its tier's window closes green, not when the slowest unit's does.
- Alert routing: who is notified on red runs and on remediation PRs (Slack channel, on-call).
- Approval to create the Workflows job and the event-driven automation (both are persistent, billable resources; name them per convention and record them in the register as D5 entries).

## Procedure
1. **Build the recon job** from the pipeline's recon spec: one Workflows job, one task per terminal output, each task running the gate queries (row counts, aggregates, row-level diffs on keys, report comparisons) against live legacy via the coexistence mechanism, failing the task on any tolerance breach. Job name, schedule, and notification settings per the ORCHESTRATION profile.
2. **Wire the automation**: recon job failure event -> spawn a remediation session carrying this playbook's remediation brief (below). Verify the trigger fires by staging one controlled red run before trusting it.
3. **Remediation brief (what the spawned session does)**: triage first: (a) legacy-side drift (new data, both engines will converge next cycle): document, no code change; (b) conversion defect that recon-at-merge missed: fix converted code only, re-run to green, PR with the FAIL -> diagnosis -> PASS arc; (c) legacy-side change (schema or logic changed upstream): never chase it silently, escalate as a new dependency-register entry, because legacy changing during coexistence is a governance event, not a bug.
4. **Evidence ledger**: every run, green or red, appends one row (timestamp, verdict, drift note, PR link if any) to `.migration/08_parallel_run_ledger.md`. This ledger is the cutover evidence: N consecutive green cycles is the STOP E entry criterion.
5. Hand the job ID, automation ID, and ledger path to the orchestrator and record them in the register.

## Specifications
- Deliverable: running scheduled recon job, verified event-driven remediation automation, evidence ledger accumulating.
- Validation: (1) one staged red run demonstrated the full arc (red -> session -> PR or documented drift) before the window is declared open; (2) every red run in the window ends in a green re-run plus a ledger row; (3) no remediation ever modified legacy source.

## Advice and Pointers
- Mirror the legacy schedule exactly; comparing a fresh Databricks run to a stale legacy cycle manufactures false reds.
- The triage order matters: drift is the common case, defect the valuable one, legacy-change the dangerous one. Teach the brief to prove drift before touching code.
- Keep the job paused while waves are still merging into the pipeline, or it will red-run against half-migrated state; unpause at window open.
- Cost note for the user: the window burns scheduled compute and occasional sessions; state the run cost per cycle at setup and remind them at STOP E to decommission or keep deliberately.

## Forbidden Actions
- Do NOT modify legacy source in any remediation, and do NOT auto-adjust tolerances to clear a red run.
- Do NOT declare the window open without the staged red-run verification.
- Do NOT let the recon job run against a pipeline still receiving wave merges.
- Do NOT silently absorb a legacy-side change; it escalates as a governance event.
- Do NOT create duplicate jobs/automations on re-runs; reuse the registered ones.
