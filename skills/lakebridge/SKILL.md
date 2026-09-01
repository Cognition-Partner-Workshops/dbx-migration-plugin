---
name: lakebridge
description: Databricks Labs Lakebridge (BladeBridge/Remorph lineage) as a migration accelerator. Use during estate assessment (analyzer) and unit conversion (transpiler) for supported source dialects. Accelerator only, never the merge gate.
---

# Lakebridge

Lakebridge is the Databricks Labs migration toolkit: an **analyzer** for estate profiling, a **transpiler** for SQL/ETL conversion to Databricks SQL, and a **reconciler**. Position in this kit: it accelerates `!dbx_estate_inventory` and `!dbx_unit_migration`; its output always passes our own data-reconciliation harness, and the independent recon session remains the merge authority.

## Install and run
- Install via the Databricks CLI labs mechanism: `databricks labs install lakebridge`. Verify with `databricks labs lakebridge --help`. (Confirm the current command surface against the Lakebridge docs at first use per engagement; the labs CLI evolves.)
- Analyzer: run over the estate export to size and profile the source; feed the output into the inventory's census and risk ranking, cross-checked against the coverage arithmetic (the analyzer is an input, not the census).
- Transpiler: run per unit with the correct source dialect flag; output lands in the unit's working area for review, never straight to deploy.

## Coverage table (maintain per engagement, like Known Traps)
Track per dialect: constructs it converts reliably, constructs it mangles silently, constructs it rejects. Seed rule: trust it more on ANSI-ish SQL, less on procedural code (stored procedures, macro-heavy scripts) and vendor-specific functions; those need hand review line by line.

## Rules
- Transpiled output is a draft: review against the source-dialect skill's conversion rules, then prove it with the data-reconciliation harness like hand-converted code.
- The Lakebridge reconciler may run as a second opinion; it never replaces the kit's independent recon gate.
- A transpiler failure or mangle on a construct goes into the coverage table and, if systematic, into SKILL FEEDBACK so the wave learns it once.
