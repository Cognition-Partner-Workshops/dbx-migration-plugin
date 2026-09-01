Playbook: Deep, source-grounded analysis of ONE user-chosen pipeline, producing the unit inventory, the wave structure, the field/type dictionary, the dependency entries, and the fan-out batch plan that everything downstream consumes.

## Overview
The user picked one pipeline at STOP B. This playbook analyzes exactly that pipeline: what executes, in what order, over what data, for which consumers, and above all **where it crosses a dependency and how wide each wave can fan out**. Analysis only: no plan, no code, no child sessions.

```
[user-chosen pipeline]  ->  pinned scope  ->  unit inventory + lineage DAG  ->  per-unit workload type
                        ->  field/type dictionary  ->  DEPENDENCY REGISTER ENTRIES
                        ->  waves 1..N with fan-out batches  ->  <Pipeline>_analysis.md + rendered DAG
```

## What's Needed From User (or from the orchestrator)
- **The pipeline (REQUIRED, never chosen here)**: its boundary from the inventory (entry feeds, terminal outputs, exclusions). Help pin ambiguity from source; the decision stays with the user.
- `.migration/` context and `<Estate>_inventory.md` (shared-object map, first-pass register). If the inventory is missing, say so: analyzing a pipeline without knowing which objects are shared produces an unsafe fan-out plan.
- **Target state**, read-only here: CORE plus the profiles matching this pipeline's workload types plus DATA/DEPENDENCY. If a needed profile is missing or N/A, flag now rather than at the plan stop.
- Where to write (default `docs/migration/<Pipeline>_analysis.md`).

## Procedure
1. **Pin the scope against source.** Trace from the entry feeds through every transformation to the terminal outputs, with cites. State exclusions. Do not widen the slice. If an object is unreachable or its source is absent from the export, report it and mark it, never invent it.
2. **Unit inventory.** Enumerate every migration unit (mapping, job, SQL object, script) the pipeline executes. Per unit: source location, workload type (SQL / PIPELINE / ORCHESTRATION / CONSUMER / ML-SCORING), reads and writes, complexity signal, shared-with-other-pipelines flag (from the inventory map), and the dialect skill's risk flags (dynamic SQL, parameter indirection, engine-specific functions, nondeterministic outputs).
3. **Field/type dictionary.** For every table the pipeline writes: legacy column -> target Delta type, with the mapping rule cited from DDL/metadata and marked FACT or INFERRED. Engine type quirks (Redshift/Teradata numerics, timestamp zones, collation) are named risks. Inferred types feed the recon plan directly: they are where parity breaks first.
4. **Dependency sweep (the headline output).** Run `!dbx_dependency_resolution` in register mode over the pipeline: every D2-D10 crossing with full contract, appended UNDECIDED. The analysis file carries the dependency table inline, not just a pointer, so a reviewer sees the pipeline's true cost in one document.
5. **Waves and fan-out batches.** Partition the unit DAG by lineage depth: wave 0 is shared objects (D2, serial, migrated on behalf of the estate), then waves 1..N leaf-first. **Within each wave, partition units into fan-out batches** (default 1-5 units per child session) such that no two batches in flight touch the same target table or shared object. **Batching treats every INFERRED edge as if it were FACT**: units joined by an INFERRED edge go into the same batch or are serialized across waves, never split into parallel batches on the hope the edge is false. An extraction gap must cost width, not correctness. Record per wave: width (batch count), the serial floor, and the projected concurrency against the D10 limits. This table is what the plan and the orchestrator execute.
6. **Recon plan per unit, size-aware.** From the tolerance record and the unit's outputs: which recon queries apply (row counts, per-column aggregates, row-level diff keys, report-output comparison), and what the dual-run source is (federated live legacy vs snapshot). Apply the size tiers from the tolerance record: full row-level diff below the agreed row threshold; above it, keyed stratified sampling plus full aggregate coverage, with the sampling rule written into the unit's recon row. State each unit's projected legacy-side query cost, and check the wave's summed legacy load against the legacy-query concurrency cap (separate from session width, from the tolerance record). Units whose outputs are nondeterministic in the legacy engine get their determinism rule stated now.
7. **Write `<Pipeline>_analysis.md`**: pinned scope with cites; unit inventory table (shared and absent units marked); rendered DAG image plus source; field/type dictionary with FACT/INFERRED marks; the dependency table with class, contract, and lead-time exposure; waves with fan-out batches and width; per-unit recon plan; risk list. Update the ledger.

## Specifications
- Deliverable: `<Pipeline>_analysis.md` plus appended dependency-register entries. No plan, no code.
- Validation: (1) every unit reachable within the pinned scope is inventoried; (2) wave order is a valid topological sort; (3) no two same-wave batches share a write target; (4) every claim cites source; (5) every mechanical-sweep crossing is in the table with a complete contract or an explicit unresolved flag; (6) every unit has a recon plan row.

## Advice and Pointers
- **The fan-out batch table is what buys the speed.** Waves are forced by lineage; width within a wave is engineered here. Err toward more, smaller batches: children are cheap, collisions are not.
- Shared objects are estate property, not pipeline property. Wave 0 migrates them once; say which pipelines inherit them.
- The dictionary's INFERRED rows predict the recon failures. Price them in the risk list rather than discovering them one red diff at a time.
- Nondeterministic legacy outputs (unordered SELECTs, ties in ranking) need a determinism rule agreed before recon, or the diff will cry wolf.
- ML-SCORING units keep their feature pipeline and scoring job in the same batch, so prediction parity is testable per child.

## Forbidden Actions
- Do NOT pick, enumerate, or widen the pipeline; analyze exactly the pinned scope.
- Do NOT decide dependency options here; register with contracts, decisions belong to the plan stop.
- Do NOT put two units writing the same target table into different same-wave batches.
- Do NOT batch across an INFERRED edge as if the units were independent; conservative merge or serialization only.
- Do NOT write the plan, convert any code, or launch child sessions here.
- Do NOT treat INFERRED types or lineage as FACT, and do NOT silently drop absent or blocked units.
- Do NOT ship the DAG as a fenced code block only; render and embed it.
