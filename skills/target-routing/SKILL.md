---
name: target-routing
description: "Routes every Databricks-side step of a migration to the official `databricks` plugin skill that owns it, plus the migration-only auth rules (one migration principal, env-based OIDC/M2M, never a PAT, never the production Lakebase branch). Use whenever a slice step touches Databricks: auth, SQL, pipelines, jobs, bundles, Unity Catalog, ingestion, Lakebase."
---

# Target routing

The factory owns the *migration* problem (slices, dialects, recon, fan-out, the three stops). The
official `databricks` plugin (`databricks/databricks-agent-skills`, declared in
`.devin-plugin/plugin.json` as a required plugin) owns *how Databricks works today*. This skill
never duplicates that content; it says which official skill to load for each step. Always load
`databricks-core` first, then the product skill named below. If an official skill and a plugin
rule disagree on an API or CLI detail, the official skill is right; on *process* (parity
authority, the stops, write scope) the plugin rule is right.

## Step → official skill

| Slice step | Load | What it decides |
|---|---|---|
| Auth, CLI sanity, ad-hoc SQL (every session) | `databricks-core` | CLI presence, `databricks current-user me`, `experimental aitools tools query` / `discover-schema` / `get-default-warehouse`. Never hand-roll Statement Execution API polling. |
| Access — federation (`source.access` kind `federation`) | `databricks-dbsql` + `databricks-unity-catalog` | Lakehouse Federation connection + foreign catalog (`remote_query` for probing). |
| Access — landed schema (`kind: landed`) or export volume (`kind: loaded_from_export`) | `databricks-lakeflow-connect`, `databricks-unity-catalog` | Managed connectors (query-based Oracle/Teradata/SQL Server/PG/MySQL, SQL Server CDC, Foreign Catalog); volumes + `COPY INTO` in FK order for an export. |
| Convert views, procedures, functions, scheduled queries (`delta` track) | `databricks-dbsql` | SQL scripting (`BEGIN…END`, handlers, `SIGNAL`), `CREATE PROCEDURE`/`CALL`, `WITH RECURSIVE`, MVs, `COLLATE` for legacy comparison semantics. |
| Convert ETL/mapping chains to pipelines (`delta` track) | `databricks-pipelines` | Lakeflow Spark Declarative Pipelines: streaming tables vs materialized views, `create_auto_cdc_flow`, expectations. Modern API only. |
| `lakebase` track (tables, routines, synced reference data) | `databricks-lakebase` | Lakebase Postgres Autoscaling: projects, branches, endpoints, `create-synced-table`. One migration branch (`policy.targets.lakebase.branch`); never `production`. |
| Jobs: scheduler edges, the shadow job, task graphs | `databricks-jobs` | Lakeflow Jobs task types, `quartz_cron_expression` + `timezone_id`, `run_if`, `max_concurrent_runs`, notifications. |
| Deploy anything | `databricks-dabs` | `databricks.yml`, targets, `bundle validate/deploy/run`. One bundle per slice named `mig_<run>_<slice>`, bundle target `migration`, schedules `PAUSED`. |
| Catalog/schema layout, grants, volumes, row filters, system tables | `databricks-unity-catalog` | Securable DDL, access control, external locations, volumes, system tables for evidence. |
| Converted PySpark/Scala that must run serverless | `databricks-serverless-migration`, `databricks-execution-compute` | Serverless compatibility checks and fixes. |
| Verifier's second opinion on `delta` slices | `databricks labs lakebridge reconcile` | Optional; `dbx-recon` stays authoritative. |

## Auth rules for migration sessions

- Every session runs as the engagement's **migration principal** from env vars only: OIDC token
  federation (`DATABRICKS_AUTH_TYPE=env-oidc`) preferred, OAuth M2M (`oauth-m2m` with
  `DATABRICKS_CLIENT_SECRET`) fallback. No PATs (the doctor's `identity` check fails a `pat`
  session), no profiles, no config files, no interactive `databricks auth login`.
- The principal must equal `policy.migration_principal`; the doctor proves it and `isolation`
  proves it writes nowhere else. An admin or human-user identity is a halt, not a convenience.
- `DATABRICKS_HTTP_PATH` (or `--target-http-path`) names the warehouse recon runs against;
  `DBX_VERIFY_TOKEN` reaches verifier sessions only.
- Never request or use account-admin or workspace-admin permissions; escalate instead.
- A `databricks bundle deploy|run` that fails with HTTP 5xx is retried at most twice (backoff
  30 s, then 120 s); a third 5xx is `status=BLOCKED` with `failure_class: platform_5xx`. A 4xx,
  a validation error, a failing job run, or a guard block is a finding, never a retry.
