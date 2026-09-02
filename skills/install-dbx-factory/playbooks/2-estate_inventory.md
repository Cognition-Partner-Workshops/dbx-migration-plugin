Playbook: Census the legacy estate, prove 100% coverage, build the lineage DAG, and present a pipeline catalog so the user can pick the first (or next) migration slice.

## Overview
Before anything migrates, the estate is enumerated and every object is accounted for. The unit of enumeration depends on the source family and is defined by the attached source-dialect skill: Informatica mappings/workflows/sessions, Ab Initio graphs, Redshift/Teradata DDL + views + procedures + scheduled queries, SAS programs, notebooks. The output is a catalog of **pipelines** (coherent slices from source feeds to consumed outputs), a coverage proof, the shared-object map that makes parallel fan-out safe, and a first-pass dependency register.

```
[estate export / repo / catalog metadata]  ->  asset census  ->  lineage DAG  ->  pipeline catalog
                                           ->  coverage arithmetic  ->  shared-object map
                                           ->  first-pass dependency register  ->  STOP B
```

Analysis of one pipeline's internals happens later (`!dbx_pipeline_analysis`). This playbook is breadth, not depth.

## What's Needed From User
- The estate source of truth: repository export, XML/parameter dumps for ETL tools, `information_schema`/system-catalog access for warehouses, scheduler definitions. If the export is partial, say so and get the rest; a census over a partial export is a wrong census.
- Explicit exclusions (deprecated folders, sandbox schemas), each recorded with a reason.
- `.migration/` workspace from `!dbx_migration_setup`.

## Procedure
1. **Census every object** with the source-dialect skill's enumeration method. Per object: identifier, type, source path/location, size/complexity signal (lines, transformation count, table count), last-modified/last-run evidence where available.
2. **Extract lineage mechanically**: reads and writes per object (tables, files, parameters), scheduler edges (which job triggers which), and consumer edges where discoverable (BI extracts, exports). Build the estate-level DAG. Mark every edge FACT (cited) or INFERRED (named risk).
3. **Detect dead weight**: objects with no inbound schedule, no consumer, and no recent run evidence go to a PROPOSED-unused set, never silently dropped. Customers routinely cut 20-40% of scope here; that is a user decision, so present it.
4. **Partition into pipelines**: coherent, independently-cutoverable slices from source feeds to final consumed outputs, typically following the customer's own folder/subject-area structure. Per pipeline: object count, complexity total, lineage depth (this drives wave count later), upstream/downstream pipeline edges, and a difficulty rank from the dialect skill's risk heuristics.
5. **Prove coverage**: every object in the census is assigned to exactly one pipeline, to the shared set (used by 2+ pipelines), to the PROPOSED-unused set, or to the user-confirmed exclusion set. State the arithmetic (`N = a + b + c + d`) with cites. Nothing may be unaccounted for. **Then triangulate export completeness**, because the arithmetic closes perfectly over an incomplete census: cross-check the census count against every external count that exists (scheduler job count vs census jobs, repo object count vs export objects, query-history distinct-object sweep vs warehouse census, tool repository metadata counts vs export). Record each cross-check with its result; where no external count exists, mark completeness UNVERIFIABLE rather than implying the export is whole.
6. **Build the shared-object map**: for every shared object, which pipelines use it and which pipeline should own its migration (normally the first to need it). This map is what prevents two parallel children from migrating the same object twice or divergently.
7. **Governance inventory**: with the `governance-mapping` skill and the dialect skill's governance-discovery entry, extract every grant, role membership, service account, and masking/row policy on in-scope objects into `.migration/08_governance_inventory.md`, one FACT row each with its metadata query cited. These are census objects: the coverage arithmetic includes them, so no grant silently vanishes between inventory and cutover.
8. **First-pass dependency sweep**: run `!dbx_dependency_resolution` in register mode at estate level for the obvious D3-D9 crossings (external feeds, BI consumers, schedulers, shared tables). Append to `.migration/04_dependency_register.md` as UNDECIDED.
9. **Estimate the parallelism profile per pipeline**: width at each lineage depth (how many units could fan out concurrently) and the serial floor (shared objects + deepest chain). This is the honest speed story: present it per pipeline so the user can weigh speed against risk when picking.
10. **Write `<Estate>_inventory.md`** with all of the above, render the DAG to an embedded image, and update the ledger. **STOP B (per stop_mode)**: attach the inventory, recommend a first pipeline (smallest honest slice that exercises every workload surface, not the easiest), and have the **user pick the pipeline** and confirm its boundary and exclusions. Never choose for them.

## Specifications
- Deliverable: `<Estate>_inventory.md` with census, coverage arithmetic, pipeline catalog, shared-object map, parallelism profile, PROPOSED-unused set, and first-pass dependency entries; `.migration/08_governance_inventory.md` with the grant/role/policy census.
- Validation: (1) coverage arithmetic closes exactly; (2) every lineage edge is FACT or INFERRED, never implicit; (3) every shared object has a proposed owner; (4) the recommended pipeline is justified against the catalog, and the user made the choice at STOP B.

## Advice and Pointers
- **The shared-object map is the parallelization keystone.** Fan-out safety comes from proving non-overlap here, once, rather than trusting 50 children to notice collisions.
- Recommend a first pipeline that touches every workload surface (some SQL, a pipeline, a schedule, a consumer) so the target profiles and skills get tuned before the wide fan-out; the second pipeline is where the speed shows.
- ETL-tool estates hide lineage in parameter files and indirect table names; the dialect skill defines how to resolve them, and unresolvable ones are INFERRED risks, not guesses.
- Last-run evidence (scheduler logs, query history) is the cheapest scope-cutter; ask for it explicitly.
- Keep the census re-runnable: estates drift during long engagements, and a re-census diff is the cheapest drift detector.

## Forbidden Actions
- Do NOT pick the pipeline; enumerate, rank, recommend, and let the user choose at STOP B.
- Do NOT silently drop any object; unaccounted-for is a validation failure.
- Do NOT treat INFERRED lineage as FACT, and do NOT present a DAG without its evidence marks.
- Do NOT analyze pipeline internals, write FRs, plan waves in detail, or write code here.
- Do NOT declare an object unused without evidence; PROPOSED-unused is a user decision.
