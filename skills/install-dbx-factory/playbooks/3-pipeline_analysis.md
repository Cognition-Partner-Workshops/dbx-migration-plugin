Playbook: Analyze one chosen pipeline into units, waves, dictionaries, dependency entries, and fan-out batches.

## What's Needed From User
- The pinned pipeline boundary, entry feeds, terminal outputs, exclusions, inventory, shared-object map, and `.migration/` context.
- CORE, matching workload profiles, DATA / DEPENDENCY, and the destination for `<Pipeline>_analysis.md`.

## Procedure
1. Trace the pinned scope from feeds to terminal outputs with cites; report absent or unreachable sources instead of widening scope.
2. Inventory each mapping, job, SQL object, script, or consumer with location, workload type, reads/writes, complexity, shared flag, and dialect risks.
3. Build the field/type dictionary for every written table; mark FACT/INFERRED and name numeric, timestamp, collation, and nondeterminism risks.
4. Register every D2–D10 crossing with a complete contract through `!dbx_dependency_resolution`; unresolved fields remain explicit blockers.
5. Topologically group units into wave 0 shared objects, then leaf-first waves. Put INFERRED edges in one batch or serialize them; no same-wave batches share targets.
6. Add a size-aware recon row per unit: gates, live/snapshot source, threshold/sampling rule, determinism rule, projected legacy load, and cap check.
7. Write `<Pipeline>_analysis.md` with scope, inventory, DAG, dictionary, dependency table, waves/batches, recon plan, and risks.

## Specifications
- Deliverable: analysis plus appended dependency-register entries; no plan or code.
- Validation: every reachable unit is inventoried, waves are topological, targets do not collide, claims are cited, and every unit has a recon row.

## Pointers
Dependency classes and stop behavior are in `references/contract.md`; source-specific enumeration is in the dialect skill.
