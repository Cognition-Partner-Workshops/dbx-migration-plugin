Playbook: Operate coexistence after migration waves, reconcile on schedule, and remediate converted code without changing legacy.

## Procedure
1. Deploy one paused Lakeflow recon/remediation job from the bundle; pin source snapshot/window, target namespace, secret names, concurrency, and alert route.
2. Open the job only after the plan's live window and STOP C conditions are satisfied; record its run ID and evidence location.
3. Run independent scheduled recon at the agreed tier. Classify each finding as source drift, conversion defect, target/governance defect, or infrastructure failure.
4. For a conversion defect, create an isolated remediation branch and PR; event-driven automation may prepare evidence, but cannot merge or widen targets.
5. Keep an evidence ledger of run ID, snapshot, populations, verdict, cost, finding, owner, PR, redeploy, and recheck. Stage one red-run fixture and one narrow live recheck before widening.
6. Reconcile connector-fed or federated tables at a recorded snapshot; report CDC lag and DEGRADED paths rather than hiding them.
7. No duplicate resource, schedule change, or consumer flip happens here; legacy stays as `AGENTS.md` says.
8. Pause the job on collision, repeated same-class failure, or unowned drift; notify once with the evidence and unblock condition.

## Specifications
- Deliverable: paused/deployed job, evidence ledger, remediation PRs, and current recon verdict.
- Validation: no duplicate resources, every finding has an owner and recheck, and the target remains inside `allowed_targets.json`.

## Pointers
The data-reconciliation skill owns parity evidence; `references/contract.md` owns stops, notifications, and fan-out guards.
