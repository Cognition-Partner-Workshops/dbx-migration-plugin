---
name: analyze
description: "The analyze ticket: one child session turns the source estate into a slices.json proposal — Lakebridge analyze + a catalog census + the consumer map, then the dialect skill's dependency scan, checked by `slices.py --check`. Load it when the plan's analyze step runs."
---

# Analyze: estate → slices.json proposal

Output: `.migration/slices.json` (a proposal the human approves with the plan at STOP 1), plus
the evidence behind every grouping decision. The legacy source is read-only throughout
(`AGENTS.md` rule 1); work from the catalog metadata, the code export, and the consumer map —
never a live write.

## Steps

1. **Census.** Enumerate every object in scope — tables, views, routines, jobs — with the dialect
   skill's catalog queries for `source.family`, cross-checked with
   `databricks labs lakebridge analyze --source-directory <export dir> --source-tech <tech>
   --report-file <path>.xlsx --generate-json true` on the code export. The analyzer is an input,
   not the census: a file it cannot parse is still an asset.
2. **Consumer map.** Name every consumer (app, report, feed) and which objects it reads; a
   consumer names its `route_group` — the set of slices repointed together at one STOP 3.
3. **Dependency scan.** Run the dialect skill's dependency scan over the routines: one
   `{routine: {writes, reads}}` map per slice at `.migration/recon/<slice>/inputs/`.
4. **Slice.** Group objects into slices: a slice is a consumer-facing capability owning tables,
   routines, jobs and consumers, with `track` (`delta`|`lakebase`) and `routing`
   (`none`|`jdbc_url`|`synonym`|`api`). Shared reference tables read by several slices and owned
   by none form `slice-0`, always first. The first non-zero slice carries `canary: true,
   auto_merge: false`. Merge slices whose routines share writes (`shared_writes:
   {table: "merged:<owner>"}`), or record `dual_write` where the plan accepts it. Unassigned
   objects need an `excluded` row carrying a decision id.
5. **Check.** `tools/slices.py --check` must pass before the proposal goes in the plan: a
   consumer in two slices requires a shared `route_group`; a table in two slices requires a
   `shared_writes` disposition; a routine's transitive writes must land inside its slice or
   `slice-0`; a routable slice's tables must not be read by another `route_group`'s routines;
   every census object is assigned or excluded; no `depends_on` cycle; the `canary` is the first
   non-zero slice.
6. **Hand off.** Per slice, the recon inputs the manager commits under
   `.migration/recon/<slice>/inputs/` (`mapping`, `dependencies`, `canonicalization`, `grading`,
   `rerun_posture`) come out of steps 1–3. Anything the proposal cannot decide — an estate whose
   read closure is everything, a route_group covering all consumers — goes to STOP 1 as a
   `decisions` entry, not a silent choice.
