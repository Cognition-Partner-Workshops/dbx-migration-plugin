---
name: dlt-pipelines
description: Choosing and building Delta Live Tables (Lakeflow Declarative Pipelines) vs Jobs vs notebooks for migrated ETL. Use when deciding the target form for a converted pipeline or implementing DLT expectations as data-quality gates.
---

# DLT Pipelines

## When DLT vs Jobs vs notebooks
- **DLT**: multi-step transformation DAGs with clear table-to-table lineage (the common shape of a converted ETL mapping chain), where expectations, incremental processing, and automatic dependency ordering pay for themselves.
- **Jobs + SQL/PySpark tasks**: single-step or scheduler-shaped work (a converted stored procedure, an extract job), and anything the legacy scheduler must keep triggering during coexistence.
- **Notebooks**: exploration and evidence only; migrated production code does not land as ad hoc notebooks.
- The engagement target state decides the default; do not mix forms within one pipeline without recording the decision.

## Migration mapping patterns
- Legacy mapping/graph step -> DLT table or materialized view; legacy staging tables -> DLT views where nothing external reads them, tables where something does (check D4/D6 first).
- Legacy data-quality checks and reject flows -> DLT expectations (`expect_or_drop`, `expect_or_fail`) with the reject counts compared in recon, since reject semantics differ between engines.
- Incremental loads -> streaming tables with Auto Loader or CDC per the backfill plan, only after the parity of the batch logic is proven.

## Traps
- DLT reorders execution by inferred dependency; a legacy pipeline relying on side-effect ordering (audit rows, sequence numbers) needs the ordering made explicit or the unit flagged.
- Expectations that drop rows change row counts; recon must compare against legacy reject behavior, not raw input counts.
