# Dialect skill template (for `skills/<dialect>/`)

Not a skill: the specification a child session follows to write or deepen one. A dialect skill is the *source-side* half of the factory; everything about Databricks lives in the official plugin behind `target-routing`. It must be usable by the four playbooks that call it (`2-estate_inventory`, `3-pipeline_analysis`, `5-unit_migration`, `6-data_reconciliation`) and by the recon harness, which reads its `canonicalization.json`.

## Deliverables
```
skills/<dialect>/SKILL.md                 the skill (>= 300 lines, sections 1-12 below, in order)
skills/<dialect>/canonicalization.json    harness rules, validated by `dbx-recon selftest`-style load
skills/<dialect>/examples/                >= 5 before/after pairs (source file + converted file + note)
skills/lakebridge/SKILL.md                append the dialect's coverage-table delta (SEEDED rows)
```
Front matter: `name`, `description` (when to load it: enumerating, analyzing, converting, or reconciling this dialect), no `triggers` (playbooks load it by name).

## Sections
1. **Enumeration**. System-catalog queries (read-only, one statement per object class, paginated) and repository/file patterns; census key per object class; size and complexity signals available per object. Every query names the minimum privilege it needs, so `07_access_checklist.md` can be filled from it.
2. **Lineage extraction**. Reads/writes per object class; how parameter files, dynamic SQL, synonyms, linked servers, and temp objects are resolved; what is FACT (cited) vs INFERRED (named risk). Scheduler edges where the dialect owns a scheduler (SQL Agent, BTEQ under cron, Informatica workflows).
3. **Unit definition**. What one migration unit is for this dialect (procedure + tables it writes; mapping + its session; package + its tasks), and what makes a unit `shared` (two pipelines write or read it).
4. **Type map** (table). Every source type -> Delta type (analytical track) and, where the dialect feeds the OLTP front door, -> Lakebase Postgres type; a `loss` column (none / precision / semantics) and the canonicalization rule that neutralizes the loss in recon.
5. **Function and operator map** (table, >= 60 rows). Source function -> Databricks SQL expression, with a `semantics` column (same / edge case / no equivalent) and the edge case spelled out (null handling, 1-based indexes, month-end, integer division, implicit casts, collation).
6. **Procedural-construct map** (table). Cursor, loop, IF/CASE flow, exception/error handling, transactions, dynamic SQL, temp tables, output parameters, return codes, triggers -> DBSQL SQL scripting / `CREATE PROCEDURE` first, Lakeflow Jobs task control flow second, PySpark last; each row cites the official skill section (`databricks-dbsql` `references/sql-scripting.md`, `databricks-jobs`, `databricks-pipelines`) it relies on.
7. **Known traps with recon signature**. Each trap: what the legacy engine does, what Databricks does, which tier and metric shows it (e.g. "Tier 2 distinct-count drift on string keys"), the fix in converted code, and the canonicalization or `COLLATE` decision it forces before the first run.
8. **Canonicalization rules**. The `canonicalization.json` content explained row by row; only rule names the harness implements (`decimal_round`, `datetime_utc_truncate_ms`, `datetime_grid_333`, `rstrip_spaces`, `empty_string_is_null`, `null_missing_equiv`, `collation_casefold`, `uuid_normalize`, `identity`); a rule the dialect needs and the harness lacks is filed as a harness change, never faked in the mapping.
9. **Governance discovery**. Queries for grants, roles and memberships, ownership-implied rights, row/column security, masking, PUBLIC grants, and audit settings, feeding `governance-mapping` and D8.
10. **Lakebridge coverage delta**. Which `--source-dialect` flag (or none) applies; SEEDED converts / mangles / rejects rows for this dialect, mirrored into `skills/lakebridge/SKILL.md`.
11. **Risk heuristics**. Per-object scoring inputs for the inventory's complexity rank (lines, procedural depth, dynamic SQL, vendor-function density, external calls, trigger fan-out); thresholds are suggestions, the census computes them.
12. **Worked examples**. Index of `examples/` with, per pair, the constructs it exercises and the recon tier that would catch a wrong conversion.

## Acceptance
- Every conversion or construct rule cites an official Databricks skill section or a docs URL; no Databricks API or CLI detail is asserted from memory.
- The fixture estate named in the child brief round-trips `!dbx_estate_inventory` and `!dbx_pipeline_analysis` with zero UNVERIFIABLE lineage edges caused by the skill (INFERRED edges from genuinely dynamic constructs are fine and are listed).
- `canonicalization.json` loads with the harness loader and every rule name exists in `recon/canon.py`.
- The five examples convert under the skill's rules and the note per example says which recon tier proves it.
- Nothing in the skill instructs a write to, or a settings change on, the legacy system; enumeration and governance queries are read-only and cite their privilege.
- No duplicated Databricks product content: if a paragraph would fit in `databricks-dbsql`/`-pipelines`/`-jobs`/`-lakebase`, replace it with a pointer through `target-routing`.

## Child brief (the parent fills the placeholders)
```
Dialect: <name>           Skill dir: skills/<dialect>/        Deepen existing: yes|no
Fixture estate: <org repo or "build a fixture of N objects covering sections 4-7">
Sub-profiles: <e.g. Sybase ASE deltas under tsql-ssis>
Official skills to cite: databricks-core, databricks-dbsql, databricks-jobs, databricks-pipelines, <+ databricks-lakebase if OLTP>
Branch: devin/<ts>-<dialect>-skill off <spine branch>; PR into <spine branch>
Do not touch: hooks/, harness/recon/ (file harness gaps as a list in the PR body), playbooks/
```
