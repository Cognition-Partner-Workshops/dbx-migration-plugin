---
name: backfill-planner
description: Table-level backfill planning and restart-safe execution for Databricks migrations. Use when the data-load posture is materialize (bulk-copying legacy data into Delta), during wave 0 or unit migration, to classify each table and execute its load safely.
---

# Backfill Planner and Executor

Emit a per-table load plan (the decision matrix), then execute each load restart-safe. The posture (federation-first, backfill-first, CDC/coexistence) comes from the plan approved at STOP C; this skill implements it.

## Decision matrix (classify every in-scope table)
| Class | Signal | Method |
|---|---|---|
| Small and static | below the size threshold, no in-flight writes | CTAS through federation, single shot |
| Large and partitionable | above threshold, natural partition key (date, id range) | partitioned incremental copy with checkpointing and per-partition verification |
| Very large or mutable, engine has a Lakeflow Connect connector | SQL Server (CT/CDC gateway, GA), Postgres/MySQL CDC, or query-based Oracle/Teradata/SQL Server/PG/MySQL | Lakeflow Connect ingestion pipeline: managed initial snapshot plus continuous CDC into the migration catalog; the connector is the watermark and the catch-up |
| Very large or mutable, no connector | CDC-fed or continuously written | initial copy to a recorded watermark, then CDC catch-up (own extract, Auto Loader over exported change files); ordering rule against in-flight changes |
| Restricted | no live read access | customer-run export or in-perimeter execution; DEGRADED recon rules apply |

Prefer the connector row over the hand-rolled row whenever the engine has one and its D10 prerequisites can close in time: it removes the per-partition copy job, the watermark bookkeeping, and the catch-up runbook from the factory's surface. Build it via `target-routing` → `databricks-lakeflow-connect` (CLI `databricks pipelines create --json` with an `ingestion_definition`, or the bundle form); the pipeline's destination is the batch's isolated schema in the migration catalog, its schedule stays PAUSED until STOP E, and the query-based variant's polling counts against the legacy-query concurrency cap.

The output artifact is a machine-readable table: object, class, method, partition key, watermark rule, verification rule, projected legacy-side cost. It attaches to the plan and each unit's handoff.

## Execution rules
- Checkpoint per partition: record each completed partition (partition id, row count, aggregate checksum) in a load ledger before starting the next. A relaunched load resumes from the ledger, never recopies verified partitions, and never assumes an unverified partial partition is good (drop and recopy it).
- Verify per partition: row count plus aggregate checksum against the source at copy time, using the data-reconciliation harness's aggregate check.
- Throttle: partition-copy parallelism counts against the legacy-query concurrency cap, shared with recon queries.
- Launch, don't babysit: loads run as Databricks jobs (checkpointed, restart-safe). The session launches the job, records the run id in the load ledger, and moves on; a later verify pass reads the ledger and job output. Never poll a long copy in-session.
- Legacy prerequisites are never the migration principal's to perform: enabling change tracking/CDC, creating the connector's database user, or opening the gateway's network path are D10 entries owned by the customer DBA/platform team. If they are not closed when the wave launches, the table falls back to the hand-rolled row; do not run `ALTER DATABASE`/`sp_cdc_enable_table` yourself, in any mode.
- Connector-fed tables still reconcile: Tier 1/2 against the source at a recorded snapshot point, and the CDC lag at recon time is a stated finding, not a hidden tolerance. Lakeflow Connect output never self-certifies.
- Watermark: for mutable tables loaded without a connector, record the exact watermark (timestamp/SCN/LSN per engine) at copy start; the delta between watermark and cutover is applied as catch-up and re-verified in the cutover runbook.
- Freeze-window honesty: if the engine offers no usable watermark and the table cannot be frozen, say so in the plan; do not fake consistency.
