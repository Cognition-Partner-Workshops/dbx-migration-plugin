---
name: parallel-run
description: The coexistence ticket after migration waves merge — runs scheduled recon through a paused Lakeflow job, classifies findings, prepares remediation PRs on isolated branches, and keeps the green-cycle count the cutover gate reads. Load it for parallel-run, monitoring, or coexistence work.
---

# parallel-run

You operate the coexistence window: scheduled independent recon on the migrated surface, and
remediation of converted code without touching legacy. No duplicate resource, schedule change,
or consumer flip happens here.

## Ticket inputs (cold start)

- The plan's parallel-run tier: agreed recon tier, live window, green-cycle count the cutover
  gate needs, alert route, concurrency cap.
- The deployed recon/remediation job (paused) and its bundle.
- `skills/data-reconciliation/SKILL.md` — tiers, DEGRADED wording, finding codes.

## Procedure

1. Deploy one paused Lakeflow recon/remediation job from the bundle; pin source
   snapshot/window, target namespace, secret names, concurrency, and alert route.
2. Open the job only after the plan's live window and its conditions are satisfied; record its
   run ID and evidence location.
3. Run independent scheduled recon at the agreed tier. Classify each finding as source drift,
   conversion defect, target/governance defect, or infrastructure failure.
4. For a conversion defect, create an isolated remediation branch and PR; automation may
   prepare evidence, but cannot merge or widen targets.
5. Keep a run log of run ID, snapshot, populations, verdict, cost, finding, owner, PR,
   redeploy, and recheck. Stage one red-run fixture and one narrow live recheck before
   widening.
6. Reconcile connector-fed or federated tables at a recorded snapshot; report CDC lag and
   DEGRADED paths rather than hiding them.
7. Pause the job on a collision, repeated same-class failure, or unowned drift; report once
   with the evidence and the unblock condition.

## Evidence the ticket must attach

- The paused/deployed job, its run ID, and the run log rows for every cycle.
- Remediation PRs with their branches and the findings they close.
- The current recon verdict and the consecutive green-cycle count for the cutover gate.
