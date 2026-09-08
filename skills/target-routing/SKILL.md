---
name: target-routing
description: Routes every Databricks-side step of a migration to the official `databricks` plugin skill that owns it, and carries only the migration-specific deltas (unattended service-principal auth, isolated per-batch targets, PAUSED schedules, never-prod-from-a-child). Use whenever a playbook step touches Databricks: auth, SQL, pipelines, jobs, bundles, Unity Catalog, ingestion, Lakebase.
---

# Target routing

The factory owns the *migration* problem (source dialects, lineage, recon, fan-out, stops). The official
`databricks` plugin (`databricks/databricks-agent-skills`, declared in `.devin-plugin/plugin.json`
as a required plugin) owns *how Databricks works today*: current API names, CLI verbs, product
decision trees. This skill does not duplicate that content. It says which official skill to load for
each factory step, and lists the few rules that are true only inside a migration.

Always load `databricks-core` first, then the product skill named below. If an official skill and a
factory playbook disagree on a Databricks API or CLI detail, the official skill is right and the
playbook needs a fix; if they disagree on *process* (stops, recon authority, write scope), the
playbook is right.

## Step → official skill

| Factory step (playbook) | Load | What it decides |
|---|---|---|
| Auth, CLI sanity, SQL/table exploration (every playbook, every child) | `databricks-core` | CLI presence, identity check (`databricks current-user me`), `experimental aitools tools query` / `discover-schema` / `get-default-warehouse` for ad-hoc SQL. Do not hand-roll Statement Execution API polling. |
| Converted views, procedures, functions, scheduled queries (`5-unit_migration`, warehouse family) | `databricks-dbsql` | SQL scripting (`BEGIN…END`, `DECLARE`, `IF/WHILE/FOR`, DBR 16.3+), `CREATE PROCEDURE`/`CALL` (DBR 17+), `WITH RECURSIVE`, temp tables, MVs, `COLLATE UTF8_LCASE` for case-insensitive legacy semantics. Like-for-like procedure ports go here first; PySpark is the fallback, not the default. |
| Converted ETL mapping chains / graphs (`3-pipeline_analysis`, `5-unit_migration`, ETL family) | `databricks-pipelines` | Lakeflow Spark Declarative Pipelines (formerly DLT). Batch full-scan step → materialized view; continuously growing source → streaming table; CDC/update-strategy → `create_auto_cdc_flow`; reject rows → expectations. Modern API only: `from pyspark import pipelines as dp`, no `import dlt`, no `LIVE.` prefix, `cluster_by` not `partition_cols` (see its `references/dlt-migration.md`). |
| Scheduler edges, task orchestration, converted batch runners (`5-unit_migration`, D5 decisions) | `databricks-jobs` | Lakeflow Jobs task types, triggers, schedules, notifications, run-as. |
| Deploying anything (`5-unit_migration`, `8-cutover_signoff`) | `databricks-dabs` | Declarative Automation Bundles (`databricks.yml`, `resources/`, targets, `bundle validate/deploy/run`). |
| Catalog/schema layout, grants, row filters, column masks, lineage, system tables (`1-migration_setup`, `governance-mapping`, D8) | `databricks-unity-catalog` | Securable DDL, access control, fine-grained access, external locations, volumes, system tables for evidence. |
| Upstream feeds, coexistence ingestion, CDC catch-up (D3 decisions, `backfill-planner`, `lakehouse-federation`) | `databricks-lakeflow-connect` | Managed connectors: SQL Server CDC (GA), query-based Oracle/Teradata/SQL Server/PG/MySQL, Foreign Catalog Redshift/Synapse/BigQuery/Snowflake, SaaS connectors. First option before any hand-built ingestion. |
| Operational-database track (`14-front_door_oltp`) | `databricks-lakebase` | Lakebase (managed Postgres): instances, synced tables, Lakehouse Sync CDC into UC, connectivity. |
| Converted PySpark/Scala code that must run serverless (`12-front_door_code`) | `databricks-serverless-migration`, `databricks-execution-compute` | Serverless compatibility checks and fixes; how to execute code on compute. |
| Streaming legacy jobs (Kafka consumers, CDC streams) | `databricks-spark-structured-streaming` | Trigger modes, checkpoints, sinks. |
| Verifying a migrated model/scoring job (`prediction-parity`) | `databricks-model-serving`, `databricks-ml-training` | Endpoints, MLflow, batch inference. |

## Migration-only deltas (these override nothing in the official skills; they narrow them)

### Auth for unattended sessions
- Children and the orchestrator run as the engagement's **migration service principal** via
  environment-variable OAuth M2M: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
  populated from named org secrets. No interactive `databricks auth login`, no PAT, no
  `~/.databrickscfg` with credential values. The official "never auto-select a profile" rule is
  satisfied because there is exactly one identity and it is recorded in `.migration/00_context.md`.
- Every session verifies identity once (`databricks current-user me`) and stops if the identity is
  not the migration principal (an admin or a human user is a halt, not a convenience).
- Principal tiers: assessment (metadata + read-only, phase 0), migration (full rights on the
  migration catalog, USE elsewhere, no admin roles), cutover (customer-held, STOP E only, never in
  a child). Never request or use account-admin or workspace-admin permissions; escalate instead.

### Write scope
- Migration work writes only to the migration catalog recorded in `.migration/00_context.md`, and a
  child writes only to the targets in its brief (`.migration/allowed_targets.json` is the allowlist
  the enforcement hooks check). Per-batch isolated areas: `<catalog>.<schema>__wave<N>_<unit>` or a
  batch-scoped schema, dropped and recreated idempotently.
- Preserve legacy table/column names in like-for-like migrations even when ugly; renames break
  consumers and recon. Forced renames (reserved words, illegal characters) go in the unit's mapping
  table so recon joins on the mapping, not on name equality.
- Medallion applies to re-architected pipelines. A like-for-like migration lands the legacy shape
  first (this is what makes Tier 1–3 recon trivially defined) and defers medallion refactors to a
  named follow-up wave, unless the STOP A target profile says otherwise.

### Deploy and schedule
- One bundle per pipeline (or per unit batch during fan-out). Bundle targets: `migration`
  (migration catalog + engagement warehouse) and `prod` (deployed only at STOP E by the cutover
  principal). Redeploys must converge; children redeploy after a partial failure rather than
  patching live resources.
- Every deployed job/pipeline lands with its schedule **PAUSED**. The STOP E flip unpauses tested
  objects; it never deploys anything new.
- Never deploy to the `prod` target, create grants on production catalogs, or repoint a consumer
  from a migration or fan-out session.

### Pipelines
- A legacy pipeline that relies on side-effect ordering (audit rows, sequence numbers) needs the
  ordering made explicit or the unit flagged; declarative pipelines reorder by inferred dependency.
- Expectations that drop rows change row counts; recon compares against legacy reject behaviour,
  not raw input counts.
- Legacy staging tables become temporary views only when nothing external reads them (check D4/D6
  first); otherwise they stay tables.

### Governance
- Legacy row-level security, masking, and retention are D8 dependencies: capture the legacy
  contract, implement as UC row filters / column masks, and include a masked-vs-unmasked recon
  check. Grants on published schemas are executed only at STOP E under the cutover principal.
- Managed vs external tables is decided per target profile before backfill; converting later
  moves data.
