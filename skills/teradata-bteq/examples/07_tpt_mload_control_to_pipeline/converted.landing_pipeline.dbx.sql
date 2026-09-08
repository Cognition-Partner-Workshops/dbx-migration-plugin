-- Target: Lakeflow Spark Declarative Pipeline (SQL). Two load-utility jobs become one pipeline with two flows.
-- Auto Loader:      databricks-pipelines references/auto-loader-sql.md ("FROM STREAM read_files(...)", options table)
-- CSV options:      databricks-pipelines references/options-csv.md (sep, header, nullValue, rescuedDataColumn, skipRows)
-- File metadata:    databricks-pipelines references/dlt-migration.md (_metadata.file_path)
-- Streaming temp view feeding AUTO CDC: databricks-pipelines references/temporary-view-sql.md (FROM STREAM(view_name))
-- Expectations:     databricks-pipelines references/expectations-sql.md (warn / DROP ROW / FAIL UPDATE)
-- Quarantine:       databricks-pipelines references/streaming-patterns.md "Rescue-Data Quarantine"
-- Keep-first dedup: databricks-pipelines references/streaming-patterns.md "Deduplication / By key (keep first)" (ROW_NUMBER over STREAM)
-- Expectations on a materialized view: databricks-pipelines references/expectations-sql.md (MATERIALIZED VIEW form)
-- SELECT * EXCEPT:  databricks-dbsql references/materialized-views-pipes.md "DROP -- Remove columns"
-- Upsert (MLOAD):   databricks-pipelines references/auto-cdc-sql.md ("AUTO CDC INTO ... KEYS ... SEQUENCE BY", SCD TYPE 1,
--                   "Multi-column sequencing": SEQUENCE BY STRUCT(ts, tiebreaker))
-- Landing paths are UC external locations / volumes ("Unity Catalog pipelines must use external locations to load files",
-- auto-loader-sql.md "Rules"); the TPT/MLOAD credentials (@UserPassword, $password) do not exist on the target: the
-- pipeline runs as the migration service principal (target-routing "Auth").

---------------------------------------------------------------------------------------------------------------------
-- (1) TPT LOAD_STG_TRANSACTIONS  ->  bronze (typed as the TPT schema: all VARCHAR) + quarantine + STG_TRANSACTIONS
---------------------------------------------------------------------------------------------------------------------

-- DEFINE SCHEMA: every field VARCHAR -> inferColumnTypes false; the APPLY casts are applied in the silver step so that
-- conversion errors are visible rows, not FastLoad ErrorTable1 rows nobody re-reads.
CREATE OR REFRESH STREAMING TABLE bronze_txn_extract
COMMENT 'Raw daily transaction extract (TPT FILE_READER equivalent), one row per source line'
AS SELECT *,
          _metadata.file_path  AS _source_file,           -- FileName variable of the TPT job (dlt-migration.md: _metadata.file_path)
          current_timestamp()  AS _ingested_at
   FROM STREAM read_files(
          '/Volumes/${catalog}/${schema}/landing/txn/',   -- @LandingDir
          format            => 'csv',
          sep               => '|',                        -- TextDelimiter '|'
          header            => true,                       -- SkipRows = 1
          nullValue         => '',                         -- NullColumns = 'Y'
          inferColumnTypes  => false,                      -- DEFINE SCHEMA is all VARCHAR
          rescuedDataColumn => '_rescued_data'             -- AcceptMissingColumns / extra columns
        );

-- Classification step: every bronze row gets exactly one ERROR_REASON (or NULL). ErrorTable1 (conversion/constraint)
-- and ErrorTable2 (UPI violation) are the non-NULL reasons; the two tables below are complementary filters over this
-- one table, so quarantine + STG_TRANSACTIONS = bronze row-for-row (Tier 1 conservation).
--   * All casts are try_cast here, so a malformed amount/date is a classified row, never a failed update.
--   * FastLoad UPI on TRANSACTION_ID: first row per key is kept, later rows are ErrorTable2 -> 'DUP_KEY'
--     (streaming-patterns.md "Deduplication / By key (keep first)": ROW_NUMBER over the stream). Rows already
--     rejected for parsing/casting do not occupy the key, as in FastLoad (ET1 rows never reach the UV check).
--     Ordering inside a single file is not observable (only _metadata.file_path is documented), so the kept row is
--     the first by (_ingested_at, _source_file); FastLoad kept the first in file order -- NOTE.md "Not verified live".
CREATE OR REFRESH STREAMING TABLE stg_transactions_classified
COMMENT 'Bronze rows with the TPT reject reason (NULL = loadable): PARSE, NULL_KEY, CAST_*, DUP_KEY'
AS SELECT *,
          CASE
            WHEN _reject_reason IS NOT NULL THEN _reject_reason
            WHEN _upi_rn > 1               THEN 'DUP_KEY'                  -- ErrorTable2 (UV) equivalent
          END AS ERROR_REASON
   FROM (
     SELECT *,
            ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID, coalesce(_reject_reason, '')
                               ORDER BY _ingested_at, _source_file) AS _upi_rn
     FROM (
       SELECT *,
              CASE
                WHEN _rescued_data IS NOT NULL                                            THEN 'PARSE'
                WHEN TRANSACTION_ID IS NULL                                               THEN 'NULL_KEY'
                WHEN try_cast(TRANSACTION_AMOUNT AS DECIMAL(15,2)) IS NULL                THEN 'CAST_AMOUNT'   -- NOT NULL on target
                WHEN try_cast(TRANSACTION_DATE   AS DATE)          IS NULL                THEN 'CAST_DATE'     -- NOT NULL on target
                WHEN POSTING_DATE IS NOT NULL AND try_cast(POSTING_DATE AS DATE) IS NULL  THEN 'CAST_POSTING_DATE'
                WHEN VALUE_DATE   IS NOT NULL AND try_cast(VALUE_DATE   AS DATE) IS NULL  THEN 'CAST_VALUE_DATE'
              END AS _reject_reason
       FROM STREAM(bronze_txn_extract)
     )
   );

-- ErrorTable1 + ErrorTable2 -> one quarantine table (the reason column tells them apart).
CREATE OR REFRESH STREAMING TABLE stg_transactions_quarantine
COMMENT 'TPT ErrorTable1/ErrorTable2 equivalent: rows that failed parsing, casting, or the UPI on TRANSACTION_ID'
AS SELECT * EXCEPT (_upi_rn, _reject_reason)
   FROM STREAM(stg_transactions_classified)
   WHERE ERROR_REASON IS NOT NULL;

-- ErrorLimit = 1000: the job aborts past 1000 rejects. Expectations are per row, so the limit becomes a FAIL UPDATE
-- on the invariant that must never be violated (null key) plus warn-level metrics on the typed columns; the
-- count-based abort is a job-level check on stg_transactions_quarantine (skill §7 trap "ErrorLimit").
-- The typed columns are try_cast again here (same expressions as the classifier): with ERROR_REASON IS NULL they cannot
-- be NULL for a non-NULL source value, so the expectations below are metrics on the classifier, not row filters.
CREATE OR REFRESH STREAMING TABLE STG_TRANSACTIONS (
    CONSTRAINT key_present        EXPECT (TRANSACTION_ID IS NOT NULL)                  ON VIOLATION FAIL UPDATE,
    CONSTRAINT amount_parses      EXPECT (TRANSACTION_AMOUNT IS NOT NULL),             -- warn (metric only)
    CONSTRAINT date_parses        EXPECT (TRANSACTION_DATE IS NOT NULL),               -- warn (metric only)
    CONSTRAINT currency_is_iso    EXPECT (length(CURRENCY_CODE) = 3)                   -- warn (metric only)
)
COMMENT 'BANKING_DW.STG_TRANSACTIONS: typed per the TPT APPLY clause'
AS SELECT
      TRANSACTION_ID,
      try_cast(TRANSACTION_DATE AS DATE)                               AS TRANSACTION_DATE,     -- (DATE, FORMAT 'YYYY-MM-DD')
      TRANSACTION_TIME,                                                                        -- TIME(0) -> STRING 'HH:MM:SS'
      ACCOUNT_ID,
      TRANSACTION_TYPE,
      TRANSACTION_SUBTYPE,
      CHANNEL,
      try_cast(TRANSACTION_AMOUNT AS DECIMAL(15,2))                    AS TRANSACTION_AMOUNT,
      CURRENCY_CODE,
      MERCHANT_ID,
      MERCHANT_NAME,
      MERCHANT_CATEGORY,
      COUNTERPARTY_ACCT,
      REFERENCE_NUMBER,
      DESCRIPTION_TEXT,
      try_cast(POSTING_DATE AS DATE)                                   AS POSTING_DATE,
      try_cast(VALUE_DATE AS DATE)                                     AS VALUE_DATE,
      current_date()                                                   AS LOAD_DATE            -- CURRENT_DATE in APPLY
   FROM STREAM(stg_transactions_classified)
   WHERE ERROR_REASON IS NULL;
-- FastLoad protocol requires an empty target and loads once; the streaming table appends per file. The consumer
-- (SP_LOAD_DAILY_TRANSACTIONS, example 04) already filters on LOAD_DATE, so no truncate step is needed.
-- The UPI on TRANSACTION_ID is enforced upstream (DUP_KEY rows never reach this table); the recon check
-- count(*) = count(distinct TRANSACTION_ID) stays as the Tier 1 witness of that, not as the mechanism.

---------------------------------------------------------------------------------------------------------------------
-- (2) MLOAD UPSERT_FX  ->  bronze FX feed + AUTO CDC INTO DIM_EXCHANGE_RATES (SCD Type 1 == DO INSERT FOR MISSING UPDATE ROWS)
---------------------------------------------------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_fx_rates
COMMENT 'Raw FX feed (MLOAD .LAYOUT FX_LAYOUT equivalent)'
AS SELECT *,
          current_timestamp()  AS _ingested_at,           -- SEQUENCE BY: micro-batch time, file path as tie-breaker (below)
          _metadata.file_path  AS _source_file
   FROM STREAM read_files(
          '/Volumes/${catalog}/${schema}/landing/fx/',
          format            => 'csv',
          sep               => '|',                        -- FORMAT VARTEXT '|'
          header            => false,                      -- .LAYOUT is positional, no header
          schemaHints       => 'FROM_CURRENCY STRING, TO_CURRENCY STRING, RATE_DATE STRING, EXCHANGE_RATE STRING, SOURCE_SYSTEM STRING, RATE_SOURCE STRING',
          inferColumnTypes  => false,
          rescuedDataColumn => '_rescued_data'
        );

-- .FILLER SOURCE_SYSTEM -> excluded from the applied columns; typed per the .DML casts (try_cast: a malformed rate
-- or date is an MLOAD ET-table row, not a failed update).
CREATE OR REFRESH TEMPORARY VIEW fx_rates_typed AS
SELECT
    FROM_CURRENCY,
    TO_CURRENCY,
    try_cast(RATE_DATE AS DATE)               AS RATE_DATE,       -- (DATE, FORMAT 'YYYY-MM-DD')
    try_cast(EXCHANGE_RATE AS DECIMAL(18,8))  AS EXCHANGE_RATE,   -- (DECIMAL(18,8))
    RATE_SOURCE,
    _ingested_at,
    _source_file
FROM STREAM(bronze_fx_rates)
WHERE _rescued_data IS NULL
  AND try_cast(RATE_DATE AS DATE) IS NOT NULL
  AND try_cast(EXCHANGE_RATE AS DECIMAL(18,8)) IS NOT NULL;

-- MLOAD ERRORTABLES for the FX feed: the complement of fx_rates_typed.
CREATE OR REFRESH STREAMING TABLE fx_rates_quarantine
COMMENT 'MLOAD ET-table equivalent for the FX feed: parse or cast failures'
AS SELECT *,
          CASE
            WHEN _rescued_data IS NOT NULL                          THEN 'PARSE'
            WHEN try_cast(RATE_DATE AS DATE) IS NULL                THEN 'CAST_DATE'
            WHEN try_cast(EXCHANGE_RATE AS DECIMAL(18,8)) IS NULL   THEN 'CAST_RATE'
          END AS ERROR_REASON
   FROM STREAM(bronze_fx_rates)
   WHERE _rescued_data IS NOT NULL
      OR try_cast(RATE_DATE AS DATE) IS NULL
      OR try_cast(EXCHANGE_RATE AS DECIMAL(18,8)) IS NULL;

-- MLOAD applied rows in file order, so a key repeated inside one file resolved to the LAST occurrence. Nothing
-- documented exposes the row position inside a file (only _metadata.file_path), so that case cannot be reproduced:
-- it is rejected instead. FAIL UPDATE stops the pipeline update with the offending key/file visible, the feed is fixed
-- at source, and the update is re-run (MLOAD ERRLIMIT analogue). Across files the order is deterministic (below).
CREATE OR REFRESH MATERIALIZED VIEW fx_rates_same_file_key_check (
    CONSTRAINT one_row_per_key_per_file EXPECT (n = 1) ON VIOLATION FAIL UPDATE
)
COMMENT 'Guard: a (FROM_CURRENCY, TO_CURRENCY, RATE_DATE) key may appear at most once per source file'
AS SELECT FROM_CURRENCY, TO_CURRENCY, RATE_DATE, _source_file, COUNT(*) AS n
   FROM bronze_fx_rates
   WHERE _rescued_data IS NULL                               -- same admission predicate as fx_rates_typed
     AND try_cast(RATE_DATE AS DATE) IS NOT NULL
     AND try_cast(EXCHANGE_RATE AS DECIMAL(18,8)) IS NOT NULL
   GROUP BY FROM_CURRENCY, TO_CURRENCY, RATE_DATE, _source_file;

CREATE OR REFRESH STREAMING TABLE DIM_EXCHANGE_RATES
COMMENT 'BANKING_DW.DIM_EXCHANGE_RATES: MLOAD UPSERT_FX target';

-- DO INSERT FOR MISSING UPDATE ROWS on (FROM_CURRENCY, TO_CURRENCY, RATE_DATE) == SCD Type 1 keyed on the same columns.
-- SEQUENCE BY STRUCT(_ingested_at, _source_file) (auto-cdc-sql.md "Multi-column sequencing"): later micro-batch wins;
-- inside one micro-batch the lexically later file path wins, which is the feed's own date-stamped naming order.
-- current_timestamp() alone is query-scoped, so two files in one micro-batch would tie and the winner would be arbitrary.
-- ETL_INSERT_TS / ETL_UPDATE_TS from CURRENT_TIMESTAMP(0) are not reproducible as-is; they are excluded from recon
-- (mapping records them as operational columns) rather than emulated.
CREATE FLOW fx_upsert AS AUTO CDC INTO DIM_EXCHANGE_RATES
FROM STREAM(fx_rates_typed)
KEYS (FROM_CURRENCY, TO_CURRENCY, RATE_DATE)
SEQUENCE BY STRUCT(_ingested_at, _source_file)
COLUMNS * EXCEPT (_ingested_at, _source_file)
STORED AS SCD TYPE 1;
