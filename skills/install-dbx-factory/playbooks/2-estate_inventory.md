Playbook: Census the legacy estate, prove coverage, build the lineage DAG, and present a pipeline catalog.

## What's Needed From User
- Complete estate export/repository/catalog metadata and scheduler definitions; register partial exports as incomplete.
- Explicit exclusions with reasons and the `.migration/` workspace from setup.

## Procedure
1. Enumerate every object with the source-dialect method; capture identifier, type, location, complexity, and run evidence.
2. Extract reads, writes, parameters, scheduler edges, and discoverable consumers. Mark every edge FACT or INFERRED.
3. Put objects with no schedule, consumer, or recent run evidence in a PROPOSED-unused set; never silently drop them.
4. Partition coherent, independently cutoverable pipelines and record counts, complexity, lineage depth, edges, and dialect risk.
5. Prove `N = pipelines + shared + PROPOSED-unused + confirmed exclusions`; cross-check every available external count and mark completeness UNVERIFIABLE when no check exists.
6. Build the shared-object ownership map. Append governance rows (grantee, privilege, role, service account, masking policy, cited query) to `.migration/04_dependency_register.md`; credentials never enter the inventory.
7. Run dependency resolution in register mode for D3–D9 crossings; append complete UNDECIDED contracts.
8. Record per-pipeline width, serial floor, and D10-constrained concurrency. Write `<Estate>_inventory.md`, render the DAG, and present the recommendation at STOP B unless intake fixed both pipeline and boundary (`references/contract.md`).

## Specifications
- Deliverable: inventory, exact coverage arithmetic, pipeline catalog, shared-object map, parallelism profile, PROPOSED-unused set, governance section, and dependency entries.
- Validation: coverage closes; edges are marked; shared objects have owners; every claim is cited; the pipeline choice follows the contract.

## Pointers
The STOP B condition and all stop behavior live in `references/contract.md`.
