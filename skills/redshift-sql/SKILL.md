---
name: redshift-sql
description: Source-dialect skill for Amazon Redshift estates. Use when enumerating a Redshift estate, extracting lineage from Redshift SQL/views/procedures, or converting Redshift SQL to Databricks SQL. v0 stub seeded from prior engagement work.
status: stub — no harness, guidance only
---

# Redshift SQL Dialect (v0)

This skill is a stub: guidance seeded from prior engagement work, with no harness, fixture or rehearsal behind it (the `dbx-recon` Redshift source adapter is untested and refuses to run; see `data-reconciliation/SKILL.md`). Verify every rule below against the customer's engine before relying on it.

## 1. Enumeration
Units: tables/views per schema (`pg_catalog`/`svv_` system views), stored procedures (`pg_proc`), scheduled/ETL SQL from the repo, and query-history-derived report queries (`sys_query_history` / `stl_query`) for the hidden-consumer sweep. Census key: schema.object.

## 2. Lineage
View and procedure DDL parse cleanly for reads/writes. Mark INFERRED: dynamic SQL in plpgsql procedures, EXECUTE statements, and anything driven by external orchestration variables. Late-binding views hide dependency errors until query time; enumerate their references explicitly.

## 3. Type map
Loss-prone rows: `DECIMAL/NUMERIC` precision preserved exactly; `CHAR(n)` blank-padding semantics differ (Databricks does not pad; recon string compares need the padding rule); `SUPER` -> VARIANT/struct per target state; `IDENTITY` gap semantics differ; timestamps are timezone-naive in Redshift (`TIMESTAMP`) vs the engagement's UC timezone rule.

## 4. Conversion rules
- Dist/sort keys -> liquid clustering (or partitioning per target state); do not carry the keys as comments and call it done, record the performance-parity expectation.
- plpgsql procedures -> DBSQL stored procedures first (`CREATE PROCEDURE` + SQL scripting: `DECLARE`, `IF/WHILE/FOR`, `EXECUTE IMMEDIATE`, condition handlers; see `databricks-dbsql` `references/sql-scripting.md`); PySpark only when the procedure calls out of SQL. Cursor loops usually become set-based operations (flag for hand review). Redshift's implicit per-procedure transaction has no equivalent: multi-statement atomicity must be made explicit or the unit flagged.
- Redshift-specific functions: `LISTAGG` (watch delimiter/order), `DATEADD/DATEDIFF` argument order, `NVL2`, `DECODE`, `TO_CHAR` format strings, `GETDATE()` -> `current_timestamp()`. Integer division truncates in Redshift; Databricks follows standard SQL, a classic recon failure.
- `COPY`/`UNLOAD` -> Auto Loader / `COPY INTO` / write to cloud storage, per the backfill plan.

## 5. Known traps (append per engagement)
- AVG over integers truncates in Redshift and not in Databricks; the tolerance record's rounding rule decides.
- ORDER BY in Redshift puts NULLs last ascending by default like Databricks, but window-frame defaults and `IGNORE NULLS` support differ; verify per function.
- Case-insensitive collation (`CASE_INSENSITIVE` databases / `COLLATE CASE_INSENSITIVE` columns) -> declare the target column `STRING COLLATE UTF8_LCASE` (DBR 16.1+) rather than sprinkling `lower()`; record the collation in the mapping so recon compares under the same rule. Note UTF8_LCASE is case-insensitive but accent-sensitive; Redshift CI is byte-lowercase, so non-ASCII edge cases still need a Tier 3 check.

## 6. Governance discovery
Grants: `SVV_RELATION_PRIVILEGES` / `HAS_TABLE_PRIVILEGE` sweeps plus `pg_default_acl` for defaults; roles and memberships: `SVV_ROLES` / `SVV_USER_GRANTS` / `SVV_ROLE_GRANTS`; users/groups: `pg_user` / `pg_group`; column-level and RLS policies: `SVV_ATTACHED_MASKING_POLICY` and `SVV_RLS_ATTACHED_POLICY`. Watch: group-based grants and PUBLIC defaults; datashare grants are a separate surface (`SVV_DATASHARE_PRIVILEGES`).
