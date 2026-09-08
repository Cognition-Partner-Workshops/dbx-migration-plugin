# 01 — SET table DDL with NOT CASESPECIFIC, CHAR, BYTEINT, IDENTITY, FORMAT, COMPRESS, PI/PPI

Source: fixture `ddl/tables/01_dim_customer.sql` (abridged). Target: Delta table, like-for-like names.

## Constructs exercised
- `CREATE SET TABLE` (silent full-row dedup) -> Delta has no SET; loader dedups explicitly. Skill §7 trap "SET dedup".
- `NOT CASESPECIFIC` on 9 string columns -> `STRING COLLATE UTF8_LCASE` (skill §4, §7). Columns without the clause
  (`GENDER`, `CREDIT_RATING`, `CURRENT_FLAG`) stay case-sensitive (`UTF8_BINARY*`); the mapping records which is which.
- `CHAR(n)` blank padding -> the `RTRIM` collation modifier so production comparisons stay trailing-blank-insensitive
  the way Teradata's are: `COUNTRY_CODE CHAR(3) NOT CASESPECIFIC` -> `UTF8_LCASE_RTRIM`; `CREDIT_RATING CHAR(3)`,
  `GENDER CHAR(1)`, `CURRENT_FLAG CHAR(1)` -> `UTF8_BINARY_RTRIM` (skill §4 row `CHAR(n)`, §7 "Trailing-blank
  equality"). Loaders still write the value unpadded; recon applies `rstrip_spaces` to *verify* the load, it is not
  what makes `WHERE CREDIT_RATING = 'AA'` match a padded `'AA '` (the collation is).
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
- Dropping the `_RTRIM` modifier on `COUNTRY_CODE`/`CREDIT_RATING`: recon stays green (both sides stripped) but a
  downstream unit filtering `WHERE COUNTRY_CODE = 'NOR'` against a padded load loses rows -> **Tier 1** row-count
  shortfall on that consumer, and **Tier 2** distinct-count excess (`'AA'` and `'AA '` counted twice) on the
  dimension itself when the source and target loads padded differently.
- Comparing `CUSTOMER_KEY` by value: false **Tier 3** diffs; the mapping spec must join on `CUSTOMER_ID` +
  `EFFECTIVE_FROM`.

## Citations
- Column `COLLATE`, `UTF8_LCASE` semantics: `databricks-dbsql` `references/geospatial-collations.md` "Part 2: Collations";
  `RTRIM` modifier and the `UTF8_BINARY_RTRIM`/`UTF8_LCASE_RTRIM` names: same file, "Collation Modifiers".
- Liquid clustering over partitioning, `GENERATED ALWAYS AS IDENTITY` surrogate keys, `ANALYZE TABLE`: `databricks-dbsql` `references/best-practices.md` ("Dimension Table Patterns", "Liquid Clustering vs Traditional Partitioning", "OPTIMIZE, VACUUM, and ANALYZE").
