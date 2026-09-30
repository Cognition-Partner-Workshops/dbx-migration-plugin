---
name: estate-inventory
description: The census ticket — enumerates every object in the legacy estate, proves coverage arithmetic, builds the lineage DAG, and produces the pipeline catalog and shared-object map the plan is built on. Load it for inventory, census, or pipeline catalog work.
---

# estate-inventory

You census the legacy estate, prove coverage, build the lineage DAG, and present the pipeline
catalog. Nothing is silently dropped: objects with no schedule, consumer, or recent run evidence
go in a PROPOSED-unused set.

## Ticket inputs (cold start)

- Complete estate export/repository/catalog metadata and scheduler definitions; register
  partial exports as incomplete.
- Explicit exclusions with reasons and the committed `.migration/` workspace from intake.
- The source-dialect skill for enumeration methods (`skills/oracle-plsql` or a `skills-extra/`
  dialect skill).

## Procedure

1. Enumerate every object with the source-dialect method; capture identifier, type, location,
   complexity, and run evidence.
2. Extract reads, writes, parameters, scheduler edges, and discoverable consumers. Mark every
   edge FACT or INFERRED.
3. Put objects with no schedule, consumer, or recent run evidence in a PROPOSED-unused set;
   never silently drop them.
4. Partition coherent, independently cutoverable pipelines and record counts, complexity,
   lineage depth, edges, and dialect risk.
5. Prove `N = pipelines + shared + PROPOSED-unused + confirmed exclusions`; cross-check every
   available external count and mark completeness UNVERIFIABLE when no check exists.
6. Build the shared-object ownership map. Append governance rows (grantee, privilege, role,
   service account, masking policy, cited query) to the dependency table; credentials never
   enter the inventory.
7. Register every D3–D9 crossing with a complete contract; unresolved fields stay explicit
   blockers on the plan.
8. Record per-pipeline width, serial floor, and D10-constrained concurrency. Write
   `<Estate>_inventory.md`, render the DAG, and hand the recommendation to the manager.

## Evidence the ticket must attach

- `<Estate>_inventory.md`: the pipeline catalog, shared-object map, DAG, PROPOSED-unused set,
  governance section.
- The exact coverage arithmetic (`N = pipelines + shared + PROPOSED-unused + confirmed
  exclusions`) and which counts are UNVERIFIABLE.
- The dependency table: every crossing with a complete contract.
- The parallelism profile: per-pipeline width, serial floor, D10-constrained concurrency.
