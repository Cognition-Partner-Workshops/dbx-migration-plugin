-- Target: Lakeflow Spark Declarative Pipeline (SQL). Two load-utility jobs become one pipeline with two flows.
-- Auto Loader:      databricks-pipelines references/auto-loader-sql.md ("FROM STREAM read_files(...)", options table)
-- CSV options:      databricks-pipelines references/options-csv.md (sep, header, nullValue, rescuedDataColumn, skipRows)
-- File metadata:    databricks-pipelines references/dlt-migration.md (_metadata.file_path)
-- Streaming temp view feeding AUTO CDC: databricks-pipelines references/temporary-view-sql.md (FROM STREAM(view_name))
-- Expectations:     databricks-pipelines references/expectations-sql.md (warn / DROP ROW / FAIL UPDATE)
-- Quarantine:       databricks-pipelines references/streaming-patterns.md "Rescue-Data Quarantine"
-- Upsert (MLOAD):   databricks-pipelines references/auto-cdc-sql.md ("AUTO CDC INTO ... KEYS ... SEQUENCE BY", SCD TYPE 1)
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

-- ErrorTable1 (conversion/constraint) + ErrorTable2 (unique violation) -> one quarantine table with a reason column.
CREATE OR REFRESH STREAMING TABLE stg_transactions_quarantine
COMMENT 'TPT ErrorTable1/ErrorTable2 equivalent: rows that failed parsing, casting, or the UPI on TRANSACTION_ID'
AS SELECT *,
          CASE
            WHEN _rescued_data IS NOT NULL                                          THEN 'PARSE'
            WHEN try_cast(TRANSACTION_AMOUNT AS DECIMAL(15,2)) IS NULL              THEN 'CAST_AMOUNT'
            WHEN try_cast(TRANSACTION_DATE   AS DATE)          IS NULL              THEN 'CAST_DATE'
            WHEN TRANSACTION_ID IS NULL                                             THEN 'NULL_KEY'
          END AS ERROR_REASON
   FROM STREAM(bronze_txn_extract)
   WHERE _rescued_data IS NOT NULL
      OR try_cast(TRANSACTION_AMOUNT AS DECIMAL(15,2)) IS NULL
      OR try_cast(TRANSACTION_DATE   AS DATE)          IS NULL
      OR TRANSACTION_ID IS NULL;

-- ErrorLimit = 1000: the job aborts past 1000 rejects. Expectations are per row, so the limit becomes a FAIL UPDATE
-- on the invariant that must never be violated (null key) plus a warn-level metric on the rest; the count-based abort
-- is a job-level check on stg_transactions_quarantine (skill §7 trap "ErrorLimit").
CREATE OR REFRESH STREAMING TABLE STG_TRANSACTIONS (
    CONSTRAINT key_present        EXPECT (TRANSACTION_ID IS NOT NULL)                  ON VIOLATION FAIL UPDATE,
    CONSTRAINT amount_parses      EXPECT (TRANSACTION_AMOUNT IS NOT NULL)              ON VIOLATION DROP ROW,
    CONSTRAINT date_parses        EXPECT (TRANSACTION_DATE IS NOT NULL)                ON VIOLATION DROP ROW,
    CONSTRAINT currency_is_iso    EXPECT (length(CURRENCY_CODE) = 3)                   -- warn (metric only)
)
COMMENT 'BANKING_DW.STG_TRANSACTIONS: typed per the TPT APPLY clause'
AS SELECT
      TRANSACTION_ID,
      CAST(TRANSACTION_DATE AS DATE)                                   AS TRANSACTION_DATE,     -- (DATE, FORMAT 'YYYY-MM-DD')
      TRANSACTION_TIME,                                                                        -- TIME(0) -> STRING 'HH:MM:SS'
      ACCOUNT_ID,
      TRANSACTION_TYPE,
      TRANSACTION_SUBTYPE,
      CHANNEL,
      CAST(TRANSACTION_AMOUNT AS DECIMAL(15,2))                        AS TRANSACTION_AMOUNT,
      CURRENCY_CODE,
      MERCHANT_ID,
      MERCHANT_NAME,
      MERCHANT_CATEGORY,
      COUNTERPARTY_ACCT,
      REFERENCE_NUMBER,
      DESCRIPTION_TEXT,
      CAST(POSTING_DATE AS DATE)                                       AS POSTING_DATE,
      CAST(VALUE_DATE AS DATE)                                         AS VALUE_DATE,
      current_date()                                                   AS LOAD_DATE            -- CURRENT_DATE in APPLY
   FROM STREAM(bronze_txn_extract)
   WHERE _rescued_data IS NULL;
-- FastLoad protocol requires an empty target and loads once; the streaming table appends per file. The consumer
-- (SP_LOAD_DAILY_TRANSACTIONS, example 04) already filters on LOAD_DATE, so no truncate step is needed.
-- Unique-violation rows (ErrorTable2): FastLoad drops duplicate UPI rows; Delta does not enforce uniqueness, so the
-- UPI on TRANSACTION_ID is a recon check (Tier 1 distinct vs total) and a downstream QUALIFY in the consumer if needed.

---------------------------------------------------------------------------------------------------------------------
-- (2) MLOAD UPSERT_FX  ->  bronze FX feed + AUTO CDC INTO DIM_EXCHANGE_RATES (SCD Type 1 == DO INSERT FOR MISSING UPDATE ROWS)
---------------------------------------------------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_fx_rates
COMMENT 'Raw FX feed (MLOAD .LAYOUT FX_LAYOUT equivalent)'
AS SELECT *,
          current_timestamp()  AS _ingested_at,           -- SEQUENCE BY needs an ordering; MLOAD applied file order
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

-- .FILLER SOURCE_SYSTEM -> excluded from the applied columns; typed per the .DML casts.
CREATE OR REFRESH TEMPORARY VIEW fx_rates_typed AS
SELECT
    FROM_CURRENCY,
    TO_CURRENCY,
    CAST(RATE_DATE AS DATE)               AS RATE_DATE,       -- (DATE, FORMAT 'YYYY-MM-DD')
    CAST(EXCHANGE_RATE AS DECIMAL(18,8))  AS EXCHANGE_RATE,   -- (DECIMAL(18,8))
    RATE_SOURCE,
    _ingested_at
FROM STREAM(bronze_fx_rates)
WHERE _rescued_data IS NULL;

CREATE OR REFRESH STREAMING TABLE DIM_EXCHANGE_RATES
COMMENT 'BANKING_DW.DIM_EXCHANGE_RATES: MLOAD UPSERT_FX target';

-- DO INSERT FOR MISSING UPDATE ROWS on (FROM_CURRENCY, TO_CURRENCY, RATE_DATE) == SCD Type 1 keyed on the same columns.
-- ETL_INSERT_TS / ETL_UPDATE_TS from CURRENT_TIMESTAMP(0) are not reproducible as-is; they are excluded from recon
-- (mapping records them as operational columns) rather than emulated.
CREATE FLOW fx_upsert AS AUTO CDC INTO DIM_EXCHANGE_RATES
FROM STREAM(fx_rates_typed)
KEYS (FROM_CURRENCY, TO_CURRENCY, RATE_DATE)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_ingested_at)
STORED AS SCD TYPE 1;
