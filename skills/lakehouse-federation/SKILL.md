---
name: lakehouse-federation
description: Set up and use Databricks Lakehouse Federation to a legacy source (Redshift, Teradata, Synapse, BigQuery, SQL Server, Postgres, MySQL, Snowflake). Use for read-only live access to legacy data during migration backfill and reconciliation.
---

# Lakehouse Federation

Federation gives Databricks a read-only live view of the legacy system through a connection plus a foreign catalog. It is the default coexistence and reconciliation mechanism whenever a JDBC path exists.

## Setup
1. Confirm the source engine is supported for federation and network path exists (private link/VPN requirements are D10 dependencies; fire them early).
2. Store legacy credentials in a Databricks secret scope; reference by scope/key name only.
3. `CREATE CONNECTION <name> TYPE <engine> OPTIONS (host ..., port ..., user secret(...), password secret(...))` with the read-only legacy principal, never an admin login.
4. `CREATE FOREIGN CATALOG <name> USING CONNECTION <name>` and grant USE to the migration principal only.
5. Verify with a trivial `SELECT COUNT(*)` on a known in-scope table and record the evidence in the access checklist.
6. Reuse existing connections and foreign catalogs; never create duplicates of ones that already exist.

## Usage rules
- Federation is read-only by policy even where the engine would allow writes: never write through it.
- Pushdown limits matter: aggregates and filters push down; wide row-level pulls drag data across the wire. Route large comparisons through the size tiers in the data-reconciliation skill.
- Every federated query is real load on the legacy production engine: respect the legacy-query concurrency cap from the tolerance record.
- Small-table backfill: `CREATE TABLE ... AS SELECT` from the foreign catalog is the cheapest correct method. Large tables go through the backfill-planner skill instead.

## When not to use it
- No JDBC path or security denies live access: use exported snapshots (DEGRADED recon mode) or in-perimeter dual-run.
- Engine unsupported by federation: use the engine's export/unload plus cloud storage, or JDBC via Spark with the same read-only principal.
