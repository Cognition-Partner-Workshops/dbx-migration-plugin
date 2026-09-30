---
name: target-routing
description: "Routes every Databricks-side step of a migration to the official `databricks` plugin skill that owns it, and carries only the migration-specific deltas (unattended service-principal auth, isolated per-batch targets, PAUSED schedules, never-prod-from-a-child). Use whenever a plan step touches Databricks: auth, SQL, pipelines, jobs, bundles, Unity Catalog, ingestion, Lakebase."
---

# Target routing

The factory owns the *migration* problem (source dialects, lineage, recon, fan-out, plan gates). The official
`databricks` plugin (`databricks/databricks-agent-skills`, declared in `.devin-plugin/plugin.json`
as a required plugin) owns *how Databricks works today*: current API names, CLI verbs, product
decision trees. This skill does not duplicate that content. It says which official skill to load for
each factory step, and lists the few rules that are true only inside a migration.

Always load `databricks-core` first, then the product skill named below. If an official skill and a
plugin rule disagree on a Databricks API or CLI detail, the official skill is right and the
plugin rule needs a fix; if they disagree on *process* (gates, recon authority, write scope), the
plugin rule is right.

## Step → official skill

| Factory step | Load | What it decides |
|---|---|---|
| Auth, CLI sanity, SQL/table exploration (every worker session) | `databricks-core` | CLI presence, identity check (`databricks current-user me`), `experimental aitools tools query` / `discover-schema` / `get-default-warehouse` for ad-hoc SQL. Do not hand-roll Statement Execution API polling. |
| Converted views, procedures, functions, scheduled queries (`unit-migration`, warehouse family) | `databricks-dbsql` | SQL scripting (`BEGIN…END`, `DECLARE`, `IF/WHILE/FOR`, DBR 16.3+), `CREATE PROCEDURE`/`CALL` (DBR 17+), `WITH RECURSIVE`, temp tables, MVs, `COLLATE UTF8_LCASE` for case-insensitive legacy semantics. Like-for-like procedure ports go here first; PySpark is the fallback, not the default. |
| Converted ETL mapping chains / graphs (`pipeline-analysis`, `unit-migration`, ETL family) | `databricks-pipelines` | Lakeflow Spark Declarative Pipelines (formerly DLT). Batch full-scan step → materialized view; continuously growing source → streaming table; CDC/update-strategy → `create_auto_cdc_flow`; reject rows → expectations. Modern API only: `from pyspark import pipelines as dp`, no `import dlt`, no `LIVE.` prefix, `cluster_by` not `partition_cols` (see its `references/dlt-migration.md`). |
| Scheduler edges, task orchestration, converted batch runners (`unit-migration`, D5 decisions) | `databricks-jobs` | Lakeflow Jobs task types, triggers, schedules, notifications, run-as. |
| Deploying anything (`unit-migration`, `cutover`) | `databricks-dabs` | Declarative Automation Bundles (`databricks.yml`, `resources/`, targets, `bundle validate/deploy/run`). |
| Catalog/schema layout, grants, row filters, column masks, lineage, system tables (`intake`, governance steps of `estate-inventory`, `migration-planning` and `cutover`, D8) | `databricks-unity-catalog` | Securable DDL, access control, fine-grained access, external locations, volumes, system tables for evidence. |
| Upstream feeds, coexistence ingestion, CDC catch-up (D3 decisions, `data-reconciliation` source access) | `databricks-lakeflow-connect` | Managed connectors: SQL Server CDC (GA), query-based Oracle/Teradata/SQL Server/PG/MySQL, Foreign Catalog Redshift/Synapse/BigQuery/Snowflake, SaaS connectors. First option before any hand-built ingestion. |
| Operational-database track (the OLTP estate track) | `databricks-lakebase` | Lakebase Postgres Autoscaling (`databricks postgres`; the Provisioned tier is retired, never create it): projects, branches, endpoints, synced tables, Lakehouse Sync into UC, OAuth connectivity. Factory delta: one branch per wave batch with a TTL is the isolated namespace; migration sessions never write the `production` branch. |
| Converted PySpark/Scala code that must run serverless (the code estate track) | `databricks-serverless-migration`, `databricks-execution-compute` | Serverless compatibility checks and fixes; how to execute code on compute. |
| Streaming legacy jobs (Kafka consumers, CDC streams) | `databricks-spark-structured-streaming` | Trigger modes, checkpoints, sinks. |
| Verifying a migrated model/scoring job (prediction parity, the wave-verify ML-SCORING step) | `databricks-model-serving`, `databricks-ml-training` | Endpoints, MLflow, batch inference. |

## Route by call graph

Track assignment follows the dependency analysis a source-dialect skill emits
(`.migration/units/<unit>/dependencies.json`, `{routine, reads, writes, calls}` rows; shape and fixture in
`skills/oracle-plsql/SKILL.md`), not the table's home schema. Walk each routine's `calls` transitively and
union its `reads` and `writes`:

- A routine on the operational-database track (Lakebase, the OLTP estate track) pulls every table it reads
  onto that track as well: the converted PL/pgSQL runs inside a Postgres transaction and can only read
  Postgres tables, so a lookup left analytical-only breaks the routine. Such a table lands on both tracks
  (the Lakebase copy fed by the synced-table path in `databricks-lakebase`), with one side recorded as owner.
- A routine on the analytical track routes its reads and writes to the DBSQL/Lakeflow skills above; a
  table it writes that an OLTP routine also writes is a routing conflict the plan must decide (`plan.yaml` decisions), not
  something to split silently.
- The transitive `writes` of a unit's routines, each taken to the target its `mapping_spec.json` names for
  that source table, are its write targets; the routines, views and jobs it deploys are its
  `deploy_objects`. The fan-out workflow refuses a wave whose declared `write_targets` differ from them
  (rule in `skills/migration-fanout/SKILL.md`).

## Migration-only deltas (these override nothing in the official skills; they narrow them)

Analytical-track deltas (deploy/schedule, pipelines, governance): [references/analytical-deltas.md](references/analytical-deltas.md); load for warehouse/ETL/code units, not for Lakebase-only units.

### Auth for unattended sessions

This section is the only home for the Databricks auth rules; other files point here.

- Worker sessions run as the engagement's dedicated **migration service principal**,
  configured by the org blueprint: OIDC token federation preferred (`DATABRICKS_AUTH_TYPE=env-oidc` —
  the `databricks` binary is a wrapper that exports a fresh `DATABRICKS_OIDC_TOKEN` per call), OAuth
  M2M fallback (`DATABRICKS_AUTH_TYPE=oauth-m2m` with `DATABRICKS_CLIENT_SECRET`). Auth arrives only
  from env vars the blueprint sets (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`); optional
  `DATABRICKS_DEVIN_AUDIENCE` sets a per-tier OIDC audience. No PATs (the doctor fails a `pat`
  session; there is no waiver), no profiles, no config files, no interactive `databricks auth login`. The
  official "never auto-select a profile" rule is satisfied because there is exactly one identity and
  it is recorded in the wave manifest's `capabilities`.
- The federation policy that lets the workspace accept the session's OIDC token is a deployment
  prerequisite the customer's workspace admin creates before intake; its issuer, subject and host
  values belong to the engagement's blueprint and its plan blockers, never to this repo.
- Every session verifies identity once: the doctor reads `databricks auth describe` (env-oidc or
  oauth-m2m is `ok`; `pat` is a fail) plus `databricks current-user me`,
  and stops if the identity is not the migration principal (an admin or a human user is a halt, not
  a convenience). The recon harness connects with the same session identity; `DATABRICKS_HTTP_PATH`
  (or `--target-http-path`) names the SQL warehouse.
- Principal tiers: assessment (metadata + read-only, phase 0), migration (full rights on the
  migration catalog, USE elsewhere, no admin roles), cutover (customer-held; `AGENTS.md`). Never
  request or use account-admin or workspace-admin permissions; escalate instead.

### Platform 5xx on bundle deploy and run

A `databricks bundle deploy` or `databricks bundle run` that fails with an HTTP 5xx or
platform-unavailable response is retried at most twice — three attempts total — with a backoff of
30 s then 120 s. Nothing else is retried: a 4xx, a validation error, a failing job run, or a guard
block is a finding, not a retry. After the third 5xx the session stops and reports `status=BLOCKED`
with failure class `platform_5xx` and the last request id in `one_line_summary`. A retry repeats
the same command as the same identity, and the three attempts count as one for the circuit breaker (rule in `AGENTS.md`).

### Write scope
- Write scope and the allowlist are `AGENTS.md`; a child writes only to the targets in its brief.
  Per-batch isolated areas: `<catalog>.<schema>__wave<N>_<unit>` or a
  batch-scoped schema, dropped and recreated idempotently. The promotion schema is created by the
  migration principal at setup when absent, so it owns it; a pre-existing schema under another
  owner needs the grants the factory doctor names.
- Preserve legacy table/column names in like-for-like migrations even when ugly; renames break
  consumers and recon. Forced renames (reserved words, illegal characters) go in the unit's mapping
  table so recon joins on the mapping, not on name equality.
- Medallion applies to re-architected pipelines. A like-for-like migration lands the legacy shape
  first (this is what makes Tier 1–3 recon trivially defined) and defers medallion refactors to a
  named follow-up wave, unless the intake target profile says otherwise.
- One active update per Lakeflow pipeline per wave: each batch lists the pipelines it updates as
  `lakeflow_pipelines` in the wave manifest, and `python3 skills/target-routing/pipeline_updates.py
  .migration/waves/wave-N.json` runs before launch (the fan-out workflow runs it itself from the
  pointer's `plugin` root and halts on any non-zero exit). Two batches of a wave naming the same
  pipeline is a halt unless the wave is serial (`width` 1) or `serialized_pipelines` maps that pipeline
  to the plan decision slug (`serialized_pipelines` in the manifest) whose selected option serializes it; then the workflow
  launches those batches in manifest order, each after the previous one finished. A batch that lists
  nothing makes the result `unsupported`, not clean.
