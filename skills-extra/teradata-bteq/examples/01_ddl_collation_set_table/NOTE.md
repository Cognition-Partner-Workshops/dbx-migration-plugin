# 01 — SET table DDL: NOT CASESPECIFIC, CHAR padding, BYTEINT, IDENTITY, FORMAT, COMPRESS, PI/PPI

Source: fixture `ddl/tables/01_dim_customer.sql` (abridged). Target: Delta table, like-for-like names.

## Constructs
- `CREATE SET TABLE` -> Delta has no silent full-row dedup; the loader dedups (`SELECT DISTINCT` / `QUALIFY`).
- `NOT CASESPECIFIC` -> `STRING COLLATE UTF8_LCASE`; columns without it stay `UTF8_BINARY*`.
- `CHAR(n)` -> `RTRIM` collation modifier (`UTF8_LCASE_RTRIM` / `UTF8_BINARY_RTRIM`) so production comparisons stay
  trailing-blank-insensitive; recon's `rstrip_spaces` verifies the load, the collation is the fix.
- `BYTEINT` -> `TINYINT`; `TIMESTAMP(0)` -> `TIMESTAMP`; `GENERATED ALWAYS AS IDENTITY` kept (values differ).
- `FORMAT`, `COMPRESS`, `FALLBACK`/`JOURNAL`/`CHECKSUM`, NUSI, `COLLECT STATISTICS` dropped; UPI/NUPI/`RANGE_N` ->
  `CLUSTER BY`; column `DEFAULT`s move to the loader.

## Recon tier that catches a wrong conversion
- Missing `UTF8_LCASE` on `CUSTOMER_SEGMENT`/`KYC_STATUS`: **Tier 2** distinct-count drift, **Tier 1** join excess.
- Loading a former `SET` table without dedup, or trusting the UPI: **Tier 1** `count(*) > count(distinct key)`.
- Missing `_RTRIM`: recon green (both sides stripped) but a consumer `WHERE COUNTRY_CODE = 'NOR'` loses padded rows:
  **Tier 1** shortfall on the consumer.
- Comparing `CUSTOMER_KEY` by value: false **Tier 3** diffs; join on `CUSTOMER_ID` + `EFFECTIVE_FROM`.

Citations: `databricks-dbsql` `references/geospatial-collations.md` "Part 2: Collations", "Collation Modifiers";
`references/best-practices.md` "Dimension Table Patterns", "Liquid Clustering vs Traditional Partitioning".
