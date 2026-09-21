# dbx-migration-dialects (optional plugin)

This directory is its own Devin plugin (`.devin-plugin/plugin.json`, name `dbx-migration-dialects`). The core
`dbx-migration-factory` plugin loads only `skills/` at the repo root and ships `oracle-plsql` as its one example
dialect; nothing under `skills-extra/` is in a session's context unless this plugin is installed too.

Install it from the subdirectory URL, next to the core plugin:

```
https://github.com/Cognition-Partner-Workshops/dbx-migration-plugin/tree/main/skills-extra
```

- `teradata-bteq` — Teradata SQL, BTEQ scripts, SPL stored procedures and macros, TPT/MLOAD/FASTLOAD control files.
- `informatica-xml` — Informatica PowerCenter repository XML exports (workflows, sessions, mappings, mapplets).
- `tsql-ssis` — SQL Server T-SQL (with Sybase ASE deltas) and SSIS `.dtsx` packages.
- `lakebridge` — Databricks Labs Lakebridge (analyzer, profiler, transpilers, reconciler) as an accelerator; never the merge gate.

The dialect skills cite Lakebridge by the relative path `../lakebridge/SKILL.md`, and references to core skills such as
`skills/target-routing` resolve against the core plugin. Each `canonicalization.json` loads with
`recon.config.load_canon_rules` (`dbx-recon run --canonicalization <path>`).
