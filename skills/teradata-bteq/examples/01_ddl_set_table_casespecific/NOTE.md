# 01 — SET table DDL with NOT CASESPECIFIC, CHAR, BYTEINT, IDENTITY, FORMAT, COMPRESS, PI/PPI

Source: fixture `ddl/tables/01_dim_customer.sql` (abridged). Target: Delta table, like-for-like names.

## Constructs exercised
- `CREATE SET TABLE` (silent full-row dedup) -> Delta has no SET; loader dedups explicitly. Skill §7 trap "SET dedup".
- `NOT CASESPECIFIC` on 9 string columns -> `STRING COLLATE UTF8_LCASE` (skill §4, §7). Columns without the clause
  (`GENDER`, `CREDIT_RATING`, `CURRENT_FLAG`) stay default `UTF8_BINARY`; the mapping records which is which.
- `CHAR(1)`, `CHAR(3)` blank padding -> `STRING` + `rstrip_spaces` canonicalization (skill §8).
- `BYTEINT` -> `TINYINT`; `INTEGER` -> `INT`; `TIMESTAMP(0)` -> `TIMESTAMP` (skill §4).
- `GENERATED ALWAYS AS IDENTITY` -> Delta identity column; values are not guaranteed to match the source, so the
  surrogate key is compared through the mapping's natural-key join, never by value equality.
- `FORMAT 'YYYY-MM-DD'` and `COMPRESS (...)` are display/storage clauses -> dropped with a mapping note.
- `UNIQUE PRIMARY INDEX`, `INDEX` (NUSI), `PARTITION BY RANGE_N` -> `CLUSTER BY`; uniqueness is not enforced.
- `COLLECT STATISTICS`, `COMMENT ON` -> `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` per target profile, `COMMENT` clauses kept.
- Column `DEFAULT` clauses -> supplied by the loader (example 04) rather than the DDL, so the converted table makes no
  claim about Delta column-default table features; if the target profile wants DDL defaults, confirm through
  `target-routing` -> `databricks-dbsql` first.

## Recon tier that catches a wrong conversion
- Forgetting `COLLATE UTF8_LCASE` on `CUSTOMER_SEGMENT`/`KYC_STATUS`: **Tier 2** distinct-count drift on those
  columns (Teradata reports 4 segments, Databricks reports every case variant), and downstream **Tier 1** row-count
  excess on any join that uses them.
- Loading without dedup onto a table that was SET on the source: **Tier 1** row-count excess.
- Leaving `CHAR(3)` padding un-canonicalized: **Tier 3** keyed diff on `CREDIT_RATING` (`'AA '` vs `'AA'`), fixed by
  `rstrip_spaces`, not by trimming the data.
- Comparing `CUSTOMER_KEY` by value: false **Tier 3** diffs; the mapping spec must join on `CUSTOMER_ID` +
  `EFFECTIVE_FROM`.

## Citations
- Column `COLLATE`, `UTF8_LCASE` semantics: `databricks-dbsql` `references/geospatial-collations.md` "Part 2: Collations".
- Liquid clustering over partitioning, `GENERATED ALWAYS AS IDENTITY` surrogate keys, `ANALYZE TABLE`: `databricks-dbsql` `references/best-practices.md` ("Dimension Table Patterns", "Liquid Clustering vs Traditional Partitioning", "OPTIMIZE, VACUUM, and ANALYZE").
