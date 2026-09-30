---
name: pipeline-analysis
description: The analysis ticket for one chosen pipeline — inventories its units, builds the field/type dictionary, registers every dependency crossing, and lays out the wave/batch grouping the plan manifests come from. Load it for pipeline analysis, unit inventory, or wave grouping.
---

# pipeline-analysis

You analyze one pinned pipeline into units, waves, dictionaries, dependency entries, and fan-out
batches. No plan and no code: the output is the analysis the manager plans from.

## Ticket inputs (cold start)

- The pinned pipeline boundary, entry feeds, terminal outputs, and exclusions; the estate
  inventory and shared-object map; the committed `.migration/` workspace.
- CORE, matching workload profiles, DATA/DEPENDENCY, and the destination for
  `<Pipeline>_analysis.md`.
- The source-dialect skill for per-unit enumeration and the dependency-analysis shape it emits
  (`.migration/units/<unit>/dependencies.json`).

## Procedure

1. Trace the pinned scope from feeds to terminal outputs with cites; report absent or
   unreachable sources instead of widening scope.
2. Inventory each mapping, job, SQL object, script, or consumer with location, workload type,
   reads/writes, complexity, shared flag, and dialect risks.
3. Build the field/type dictionary for every written table; mark FACT/INFERRED and name
   numeric, timestamp, collation, and nondeterminism risks.
4. Register every D2–D10 crossing with a complete contract; unresolved fields remain explicit
   blockers on the plan.
5. Topologically group units into wave-0 shared objects, then leaf-first waves. Put INFERRED
   edges in one batch or serialize them; no two same-wave batches share targets.
6. Add a size-aware recon row per unit: gates, live/snapshot source, threshold/sampling rule,
   determinism rule, projected legacy load, and cap check.
7. Write `<Pipeline>_analysis.md` with scope, inventory, DAG, dictionary, dependency table,
   waves/batches, recon plan, and risks.

## Evidence the ticket must attach

- `<Pipeline>_analysis.md` with the wave/batch grouping, the type dictionary, and the
  dependency table.
- Per-unit `dependencies.json` rows the dialect skill emits (transitive reads/writes/calls) —
  the wave manifest's `write_targets` come from these.
- The recon plan row for every unit.
