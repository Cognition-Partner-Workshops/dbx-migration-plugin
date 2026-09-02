# DBX Migration Factory

**What this is**: a repeatable system for moving a legacy data estate (a SQL warehouse like Redshift or Teradata, an ETL tool like Informatica, or a SAS/Hadoop code estate) onto Databricks, run by Devin sessions working in parallel, with a human approving at exactly five checkpoints.

**Why it works, in one paragraph**: the target (Databricks) is always the same, so all the target knowledge is built once and reused. The source varies, so source specifics live in small plug-in profiles. Nothing merges on trust: every converted piece must produce provably identical results to the legacy system (an automated data comparison called the recon gate) before it counts as done. And because most pieces of a data estate are independent of each other, Devin migrates them 20 at a time instead of one at a time.

**What the operator actually does**:
1. Install one plugin into the Devin org and run one setup session ("set up the DBX migration factory").
2. Start a migration by typing one command: `!dbx_migrate_etl`, `!dbx_migrate_warehouse`, or `!dbx_migrate_code`.
3. Answer five approval stops when pinged (Slack/Teams optional). Everything else is automatic.

**The five stops** (the only decisions a human makes):

| Stop | Plain-language question |
|---|---|
| **A** | "This is what 'migrated' will mean, these are the accuracy tolerances, and this is the access we need. Correct?" |
| **B** | "Here is everything in your estate. Which pipeline do we migrate first?" |
| **C** | "Here is the plan: order, dependencies, how many parallel sessions, cost. Approved?" |
| **D** | "A batch is done, here is the evidence it matches the old system. Any concerns?" (notification, not blocking) |
| **E** | "The new system has matched the old one in production for weeks. Authorize cutover?" |

**Why it is safe**: Devin never touches the legacy system except to read it. Migration work writes only to an isolated area. The credentials that can repoint production are held by the customer and used exactly once, at stop E, never by the automated sessions. Every claim of correctness is machine-checked data comparison, re-verified by an independent session that did not do the migration.

**Why it is fast and cost-effective**: independent pieces migrate 20 at a time; a pilot batch runs first so systematic mistakes are found once, not twenty times; long-running data copies run as Databricks jobs rather than tying up sessions; and everything learned in one batch is saved so later batches never re-learn it.

---

Everything below is the engineering detail behind that summary.

## The chain

```
PRE-MIGRATION (once per engagement)
  1  !dbx_migration_setup          Phase 1: CORE + per-workload target profiles -> doc +
                                   knowledge note. Phase 2: .migration/ workspace: context,
                                   recon tolerances, access checklist, dependency register,
                                   ledger                                      STOP A (blocking)
ESTATE LEVEL
  2  !dbx_estate_inventory         asset census, lineage DAG, 100% coverage proof,
                                   shared-object map, first-pass dependency register
                                                                               STOP B (blocking:
                                                                               user picks pipeline)
PIPELINE LEVEL
  3  !dbx_pipeline_analysis        unit inventory, lineage waves, field/type dictionary,
                                   dependency sweep, fan-out batches
  4  !dbx_migration_plan           plan, every dependency DECIDED, access requests fired,
                                   fan-out width and gates confirmed           STOP C (blocking)
EXECUTION (parallel fan-out)
  5  !dbx_unit_migration           N children per wave IN PARALLEL, one per unit batch
  6  !dbx_data_reconciliation      independent dual-run diff per unit batch    STOP D (notify)
COEXISTENCE + CLOSE
  7  !dbx_parallel_run             scheduled recon job + event-driven fix sessions
  8  !dbx_cutover_signoff          end-to-end run, evidence, independent audit,
                                   cutover + decommission plan                 STOP E (blocking)

DRIVER
  9  !dbx_migrate_pipeline         the orchestrator that drives all of the above

FRONT DOORS (thin entry points, pre-select skills and vocabulary)
 10  !dbx_migrate_etl              Informatica / Talend / Ab Initio / DataStage estates
 11  !dbx_migrate_warehouse        Redshift / Teradata / BigQuery / Synapse estates
 12  !dbx_migrate_code             SAS / Hadoop / on-prem Spark / ML scoring estates

INTERNAL SUBROUTINE (never operator-invoked; called by 2, 3, 4 and 5)
 13  !dbx_dependency_resolution    register / decide / implement modes
```

Migrating a whole estate means running the orchestrator once per pipeline, in the order the inventory recommends; independent pipelines may themselves run as parallel orchestrator sessions once the first pipeline has validated the target profiles.

## What stops a bad day

Each row is a way a migration goes wrong and the one thing in the kit that catches it. Nothing here needs a human watching.

| If this happens | What catches it |
|---|---|
| A stop is skipped or an old approval is reused | Every stop is a row in the decisions file with a date. The orchestrator re-reads it on resume and re-asks if the inputs changed. |
| A child gets an incomplete brief | It reports BLOCKED, does nothing, and the brief says which item was missing. It never guesses. |
| Two children write the same table | The wave refuses to launch. If it only shows up afterwards, merges are held and the brief says so. |
| The same wave is launched twice | The workflow refuses if the wave's result file already exists. Resume uses the run_id; redo needs an explicit flag. |
| One mistake repeats across 20 children | Circuit breaker: 3 same-class failures and no new children launch. Fix once, resume, held-back batches run. |
| A child keeps retrying a red recon | Hard cap of 3 full runs, then it reports FAIL with a one-word failure class. |
| Children hammer the live source | Fixture first. Each child reads the real source once, inside the cap agreed at the first stop. |
| A child grades its own homework | A separate session that wrote none of the code re-runs the harness. Only its PASS merges. |
| Fixture PASS gets mistaken for done | The report says so in the verdict line. Only live or snapshot PASS can merge. |
| The source moved during the check | Live comparisons are timestamped and re-run on the source side to separate drift from a real defect. |
| Someone loosens a tolerance to go green | Tolerance changes need a dated approval in the decisions file. Grading-only fixes are the one exception. |
| A secret ends up in a PR or log | Everything takes secret names; values are read from the environment at run time and never printed. |
| Work lands outside the migration area | Write targets come from the brief only. The migration catalog or cluster is the only place with write grants. |
| Cutover runs by accident | It needs a customer-held principal Devin never has, plus a current approval. Children cannot do it. |
| Too many messages | One message per stop, wave close, or halt. Never per child or per PR. The wave brief is ten lines. |

## Design principles

1. **Target is constant, source varies.** Every engagement lands on the same target shape (Unity Catalog, Delta, Databricks SQL/PySpark, Jobs or DLT, Asset Bundles). All source-stack specifics live in pluggable skills, never in the playbooks.
2. **Pre-migration is knowledge ingestion.** The engagement's target state and working context are captured as committed artifacts plus knowledge notes before any migration work, so child and resumed sessions start from fact instead of chat history.
3. **Estate coverage is proven, not assumed.** The inventory proves object-level arithmetic: every mapping, job, SQL object, and script in scope is assigned to a pipeline, the shared set, or the explicitly-excluded set, with cites.
4. **The user picks the pipeline.** Devin enumerates and recommends; it never chooses the slice or widens the scope.
5. **Dependencies are first-class.** Every point where the pipeline touches something not migrating with it is classified, contract-specified, decided with the user, and its lead-time request fired at plan time, not at cutover.
6. **Parallel by default, serial only where lineage forces it.** After shared-object dedup, units at the same lineage depth are independent and fan out as parallel child sessions.
7. **The recon gate is the contract.** No converted unit merges without a green dual-run diff against the legacy system, with tolerances agreed up front. Legacy source is never modified to make reconciliation pass.
8. **The orchestrator is thin and scriptable.** It sequences, fans out, gates, and holds the human stops; substantive method lives in the unit playbooks. Large estates run as dynamic workflows, not hand-managed sessions.

## Files

| File | Playbook title | Macro |
|---|---|---|
| `1-migration_setup.md` | [DBX v1] Migration Setup (target state + workspace + tolerances + access) | `!dbx_migration_setup` |
| `2-estate_inventory.md` | [DBX v1] Estate Inventory & Coverage | `!dbx_estate_inventory` |
| `3-pipeline_analysis.md` | [DBX v1] Pipeline Analysis | `!dbx_pipeline_analysis` |
| `4-migration_plan.md` | [DBX v1] Pipeline Migration Planning | `!dbx_migration_plan` |
| `5-unit_migration.md` | [DBX v1] Unit Migration (fan-out child) | `!dbx_unit_migration` |
| `6-data_reconciliation.md` | [DBX v1] Data Reconciliation | `!dbx_data_reconciliation` |
| `7-parallel_run_monitoring.md` | [DBX v1] Parallel Run & Event-Driven Remediation | `!dbx_parallel_run` |
| `8-cutover_signoff.md` | [DBX v1] Cutover Verification & Sign-off | `!dbx_cutover_signoff` |
| `9-orchestrator.md` | [DBX v1] Pipeline Migration Orchestrator | `!dbx_migrate_pipeline` |
| `10-front_door_etl.md` | [DBX v1] Front Door: ETL Tool Estate | `!dbx_migrate_etl` |
| `11-front_door_warehouse.md` | [DBX v1] Front Door: SQL Warehouse Estate | `!dbx_migrate_warehouse` |
| `12-front_door_code.md` | [DBX v1] Front Door: Code & ML Scoring Estate | `!dbx_migrate_code` |
| `13-dependency_resolution.md` | [DBX v1] Dependency Resolution (Register / Decide / Implement) | `!dbx_dependency_resolution` |
| `00_intake_template.md` | (not a playbook: the pre-kickoff intake form the customer fills; consumed by the front doors) | n/a |

The number prefix is reading order, not an execution requirement. `13-dependency_resolution.md` is an internal subroutine invoked from playbooks 2, 3, 4 and 5, never run in sequence and never presented on the operator surface.

## Human stop points

| Stop | When | Default | What the user decides |
|---|---|---|---|
| **A** | after pre-migration | blocking | target profiles per workload (and which are N/A), recon tolerances, access checklist status, repo topology |
| **B** | after estate inventory | blocking | **which pipeline to migrate**, its scope boundary and exclusions |
| **C** | after plan | blocking | analysis, plan, every dependency decision, fan-out width, wave gates, data target |
| **D** | after each wave | notify | review the wave's PRs and recon evidence in batch; optionally pause the fan-out |
| **E** | before cutover | blocking | sign-off, evidence, independent audit, cutover authorization |

Blocking stops fire even on resumed runs where prior sessions already produced the artifacts. Prior work is acknowledged and re-presented; a previous approval never carries over. At every stop, the full markdown artifacts are attached, not summarized.

## Dependency taxonomy

`!dbx_dependency_resolution` classifies every crossing into exactly one class:

| Class | What it is |
|---|---|
| D1 | intra-pipeline lineage edge (ordering constraint, not a decision) |
| D2 | shared object used by several pipelines (migrate once, first pipeline owns it) |
| D3 | upstream feed owned by a system not migrating (federation or ingestion contract) |
| D4 | downstream consumer (BI dashboard, report, extract, API) reading the legacy output |
| D5 | scheduler / orchestration dependency (Control-M, Autosys, Airflow, cron) |
| D6 | shared table written by both migrated and non-migrated writers |
| D7 | external hand-off (SFTP drop, message queue, partner feed) |
| D8 | security / governance contract (row-level security, PII masking, retention) |
| D9 | ML model or scoring consumer of the data |
| D10 | environment/access dependency (network path, service principal, sample data approval) |

Each entry records the full contract, then a decision (federate / re-point / dual-write during coexistence / documented deferral), the routing point that flips traffic, the cutover and decommission condition, and the fired lead-time request. D10 entries fire at STOP A, because access requests routinely outlast the code work.

## Parallelization model

- The estate inventory computes lineage depth per unit. Units at the same depth with no shared-object conflicts form a **wave**; a wave is partitioned into **unit batches** (default 1-5 units per child) that run as parallel child sessions.
- Default fan-out width is 20 concurrent children, configurable at STOP C. Waves above the plan's threshold run through the `migration-fanout` workflow (shipped in the plugin) with structured outputs and resume; smaller waves are launched as one batch in-session, gathered once, and gated the same way. Nobody reviews children one at a time.
- Each child self-grades with the `dbx-recon` CLI; the wave is then re-verified by one independent session and gated on `result.json`. Humans read the ten-line wave brief at STOP D and act only on exceptions.
- Two runtime backstops guard the fan-out: children register write targets in the ledger before deploying (a collision halts launches: it means the lineage extraction missed an edge), and a circuit breaker pauses remaining launches when N children (default 3) report the same failure class, so a new systematic trap is fixed once, not twenty times.
- Shared objects (D2) migrate in wave 0, serially, before any fan-out, so parallel children never collide on them.
- The first small wave (5-10 units) runs before full fan-out to tune the source-dialect skill and knowledge notes; every later session inherits the corrections.

## Deployment into a customer environment

The kit ships as two packages with different lifecycles:

**The plugin (`dbx-migration-factory`)** carries everything environment-shaped and customer-invariant, versioned centrally and installed into the customer's Devin org in one step:
- the target-convention skills and harness skills (installed with this plugin);
- MCP servers: a Databricks MCP (workspace, jobs, Unity Catalog metadata, Statement Execution) and source-side MCPs where they exist;
- always-on rules that mirror the kit's guardrails (never modify legacy source, reference secrets by name only, write only to the migration catalog), so even off-playbook sessions inherit them;
- accelerator tool wrappers, chiefly the `lakebridge` skill (see the catalog): install, dialect coverage, output interpretation;
- the `install-dbx-factory` bootstrap skill, which imports the playbooks and proposes the environment blueprint.

**The playbook library** carries the process: the 13 playbooks above, imported with their titles and macros, tuned per customer. Playbooks hold everything with customer values in it; the plugin holds nothing customer-specific.

Deployment order at engagement start:
1. Install the plugin into the customer's Devin org.
2. Run one setup session ("set up the DBX migration factory"): it imports the 13 playbooks and proposes the environment blueprint; the admin approves the suggestion.
3. Attach the source-dialect skill(s) matching the customer's legacy stack (installed with this plugin, or repo-level `.agents/skills/`).
4. Provision the three access principals from the access model in `1-migration_setup.md` (assessment read-only, migration sandbox, customer-held cutover) and load their secrets by name.
5. Adjust shop-specific conventions only where the customer genuinely differs: artifact paths in `1-migration_setup.md` (default `docs/migration/...`), catalog/schema naming in the target profiles.
6. Run `!dbx_migration_setup` first. Nothing downstream is allowed to guess the target stack, and later playbooks stop and ask if it has not run.
7. Start a migration with a front door (`!dbx_migrate_etl`, `!dbx_migrate_warehouse`, `!dbx_migrate_code`) or directly with `!dbx_migrate_pipeline`.

The `.migration/` workspace created in step 1 of the chain is the engagement's durable memory: dependency register, progress ledger, recon tolerance record, and decision log live there and are appended by later playbooks, never rewritten.

## What is deliberately not here

No bespoke parity scoreboard app and no assumption of production data access. Correctness rests on the reconciliation harness (dual-run diffs against the live legacy system via federation or exported snapshots), captured evidence in every PR, a CI regression gate, an event-driven parallel-run window, and a skeptical independent audit run by a session that did not perform the migration. Where production access is denied (a common pattern), phase 0 falls back to metadata-only assessment plus masked or representative sample data, recorded as a D10 dependency with an explicit recon-scope caveat.
