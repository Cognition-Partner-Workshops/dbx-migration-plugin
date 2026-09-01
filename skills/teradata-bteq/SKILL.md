---
name: teradata-bteq
description: Source-dialect skill for Teradata estates. Use when enumerating a Teradata estate, parsing BTEQ scripts and stored procedures, or converting Teradata SQL/BTEQ/utilities to Databricks. v0 stub, hardened on first engagement.
---

# Teradata / BTEQ Dialect (v0)

## 1. Enumeration
Units: BTEQ scripts (from the scheduler's script library, usually the real estate), stored procedures and macros (`DBC.TablesV` where `TableKind` in P/M), views, and load-utility jobs (MLOAD/FASTLOAD/TPT control files). Census key: script path or database.object.

## 2. Lineage
BTEQ is imperative: parse each script's SQL statements in order for reads/writes, including volatile/global temp tables (script-local, not shared edges). Mark INFERRED: dynamic SQL built from BTEQ variables or shell parameter substitution around the script, and macro parameters resolved at call time.

## 3. Type map
Loss-prone rows: `DECIMAL(38)` precision at the boundary; `BYTEINT` -> tinyint; `PERIOD` types have no Delta equivalent (decompose to start/end columns, record the mapping); `CHAR` blank padding; Teradata's `NOT CASESPECIFIC` default makes string comparison case-insensitive, the single most common recon trap; `TIMESTAMP(6)` precision vs the engagement rule; `TITLE`/`FORMAT` clauses are display-only, drop with a note.

## 4. Conversion rules
- BTEQ control flow (`.IF ERRORCODE`, `.LABEL`, `.QUIT`) -> job task dependencies and error handling in Workflows, not emulated line by line.
- `MERGE`/`UPD ... ELSE INS` upsert idioms -> Delta MERGE; `QUALIFY` is supported in Databricks SQL (keep it); `SAMPLE` -> TABLESAMPLE with different semantics (flag for recon).
- Teradata-specific functions: `INDEX` -> `instr`, `SUBSTR` 1-based edge cases, `TRIM(BOTH FROM ...)`, `ADD_MONTHS` end-of-month behavior, integer division truncates, implicit `FORMAT`-based casting must become explicit casts.
- MLOAD/FASTLOAD/TPT -> Auto Loader or `COPY INTO` per the backfill plan; error-table semantics map to expectations/quarantine tables with reject counts reconciled.
- PI/PPI (primary/partitioned primary index) -> liquid clustering or partitioning per target state.

## 5. Known traps (append per engagement)
- NOT CASESPECIFIC: legacy joins and DISTINCT collapse case variants that Databricks keeps distinct; decide the normalization rule before recon, not after the first red diff.
- Teradata rounds decimals half-even in some contexts where Spark rounds half-up; the tolerance record decides.
- SET tables silently deduplicate full-row duplicates; MULTISET semantics on Delta will show extra rows in recon on estates relying on SET dedup.

## 6. Governance discovery
Grants: `DBC.AllRightsV` (explicit) plus `DBC.AllRoleRightsV` (role-held); roles and memberships: `DBC.RoleMembersV` / `DBC.RolesV`; profiles and users: `DBC.ProfileInfoV` / `DBC.UsersV`; row-level security constraints: `DBC.SecConstraintsV`. Watch: ownership-implied rights (creator/owner carries implicit privileges `AllRightsV` does not list as grants), `WITH GRANT OPTION` chains, and PUBLIC grants.
