-- Target: Databricks SQL / Delta, like-for-like shape (legacy names kept; see target-routing "Write scope").
-- ${catalog}.${schema} are the unit's declared write target from .migration/allowed_targets.json.
-- Collation syntax: databricks-dbsql references/geospatial-collations.md "Part 2: Collations" (column-level COLLATE);
-- the RTRIM modifier ("Collation Modifiers": `'Hello' == 'Hello '`, UTF8_BINARY_RTRIM / UTF8_LCASE_RTRIM) carries
-- Teradata's trailing-blank-insensitive CHAR(n) comparison into every production join/filter/GROUP BY, so that the
-- semantics do not depend on how a loader happened to pad the value. Recon's rstrip_spaces is the *check*, not the fix.
-- Clustering + identity + ANALYZE: databricks-dbsql references/best-practices.md ("Dimension Table Patterns",
-- "Liquid Clustering vs Traditional Partitioning", "OPTIMIZE, VACUUM, and ANALYZE").
-- Teradata column DEFAULTs are not carried into the DDL: the SCD2 loader (example 04) supplies them explicitly,
-- so the converted table does not depend on Delta column-default table features (verify via target-routing if wanted).

CREATE OR REPLACE TABLE ${catalog}.${schema}.DIM_CUSTOMER (
    CUSTOMER_ID         INT              NOT NULL,
    CUSTOMER_KEY        BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
                                         COMMENT 'Surrogate key for SCD Type 2',
    -- NOT CASESPECIFIC -> UTF8_LCASE so joins/GROUP BY/DISTINCT collapse case the way Teradata did.
    FIRST_NAME          STRING COLLATE UTF8_LCASE NOT NULL,
    LAST_NAME           STRING COLLATE UTF8_LCASE NOT NULL,
    DATE_OF_BIRTH       DATE,                              -- FORMAT 'YYYY-MM-DD' was display-only: dropped
    GENDER              STRING COLLATE UTF8_BINARY_RTRIM,  -- CHAR(1) case-specific; RTRIM for an empty-string load ('' vs ' ')
    EMAIL_ADDRESS       STRING COLLATE UTF8_LCASE,
    ADDRESS_LINE_2      STRING COLLATE UTF8_LCASE,         -- COMPRESS '' dropped; '' stays '' (Teradata does not NULL it)
    COUNTRY_CODE        STRING COLLATE UTF8_LCASE_RTRIM,   -- CHAR(3) NOT CASESPECIFIC: case- and padding-insensitive; DEFAULT 'NOR' in the loader
    CUSTOMER_SEGMENT    STRING COLLATE UTF8_LCASE,
    RISK_SCORE          DECIMAL(5,2),
    CREDIT_RATING       STRING COLLATE UTF8_BINARY_RTRIM,  -- CHAR(3) case-specific: 'AA ' = 'AA' but 'aa' <> 'AA', as on Teradata
    KYC_STATUS          STRING COLLATE UTF8_LCASE,         -- DEFAULT 'PENDING' moved to the loader
    ONBOARDING_DATE     DATE             NOT NULL,
    IS_ACTIVE           TINYINT,                           -- BYTEINT -> TINYINT (same -128..127 range); DEFAULT 1 in loader
    EFFECTIVE_FROM      TIMESTAMP,                         -- TIMESTAMP(0); loader sets current_timestamp()
    EFFECTIVE_TO        TIMESTAMP,                         -- loader sets TIMESTAMP '9999-12-31 23:59:59'
    CURRENT_FLAG        STRING COLLATE UTF8_BINARY_RTRIM,  -- CHAR(1); loader sets 'Y'
    ETL_BATCH_ID        BIGINT,
    ETL_INSERT_TS       TIMESTAMP
)
USING DELTA
COMMENT 'SCD Type 2 customer dimension with KYC and risk attributes'
-- UPI (CUSTOMER_KEY) + NUPI (CUSTOMER_ID) + PPI RANGE_N(ONBOARDING_DATE) -> liquid clustering keys, most
-- selective first. RANGE_N yearly partitions are not carried as Delta partitions (see NOTE.md).
CLUSTER BY (CUSTOMER_KEY, CUSTOMER_ID, ONBOARDING_DATE);

-- Teradata SET semantics (silent full-row dedup on INSERT) have no Delta equivalent.
-- The loader must dedup explicitly; the recon Tier 1 count is against the SET-deduplicated source.
-- UNIQUE PRIMARY INDEX uniqueness is not enforced by Delta: covered by a Tier 2 distinct-count check on
-- CUSTOMER_KEY and a duplicate-key probe in Tier 3.

-- Dropped, recorded in the unit mapping (no runtime effect on the target):
--   NO FALLBACK / NO BEFORE|AFTER JOURNAL / CHECKSUM / MERGEBLOCKRATIO (Teradata storage options)
--   COMPRESS (...) value-list compression (Delta columnar encoding replaces it)
--   COLLECT STATISTICS ... (run ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS per the target profile instead)
--   column DEFAULT clauses (moved to the loader, see header)
--   secondary INDEX IDX_CUST_SEGMENT (no secondary indexes on Delta; clustering key or nothing)
