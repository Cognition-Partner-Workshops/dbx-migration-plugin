# Optional source-dialect skills

These optional source-dialect skills and the Lakebridge accelerator wrapper are not loaded by the core plugin; Devin loads only `skills/`.
Their intended home is a separate `dbx-dialects-extra` plugin with the same layout once that repository exists.
Until then, a fork that needs one copies `skills-extra/<name>` into `skills/`.
Each `canonicalization.json` still loads with `recon.config.load_canon_rules` (`dbx-recon run --canonicalization <path>`).

- `teradata-bteq` — Source-dialect skill for Teradata estates (Teradata SQL, BTEQ scripts, SPL stored procedures and macros, TPT/MLOAD/FASTLOAD control files).
- `informatica-xml` — Source-dialect skill for Informatica PowerCenter estates.
- `tsql-ssis` — Source-dialect skill for SQL Server T-SQL estates (with Sybase ASE deltas) and SSIS packages.
- `lakebridge` — Databricks Labs Lakebridge (analyzer, profiler, BladeBridge/Morpheus/Switch transpilers, reconciler) as a migration accelerator.
