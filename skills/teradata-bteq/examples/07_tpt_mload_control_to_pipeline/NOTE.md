# 07 — TPT (FastLoad protocol) and MLOAD control files -> declarative pipeline with quarantine + AUTO CDC

Source: two skill-authored minimal control files (the estate has none): `source.stg_transactions_load.tpt`
(TPT job, DataConnector producer -> Load operator into `STG_TRANSACTIONS`) and `source.dim_exchange_rates_upsert.mload`
(MLOAD upsert into `DIM_EXCHANGE_RATES`). Target: one Lakeflow Spark Declarative Pipeline (SQL).

## Constructs exercised
- TPT `DEFINE SCHEMA` (all VARCHAR) -> `read_files(... inferColumnTypes => false)`; typing moved to the silver step.
- `DEFINE OPERATOR ... TYPE DATACONNECTOR PRODUCER` attributes -> `read_files` options: `TextDelimiter` -> `sep`,
  `SkipRows = 1` -> `header => true`, `NullColumns = 'Y'` -> `nullValue => ''`, `AcceptMissingColumns` ->
  `rescuedDataColumn`; `DirectoryPath`/`FileName` -> volume path + `_metadata.file_path`.
- `DEFINE OPERATOR ... TYPE LOAD` (FastLoad protocol) -> streaming table append; `TargetTable` keeps its name;
  `LogTable` -> pipeline event log (no artifact); `MaxSessions/MinSessions` -> no equivalent (dropped).
- `ErrorTable1` / `ErrorTable2` -> one classification streaming table (`stg_transactions_classified`, every bronze row
  gets one `ERROR_REASON` or NULL) split into a quarantine table and `STG_TRANSACTIONS` by that column. ErrorTable1 =
  `PARSE` / `NULL_KEY` / `CAST_*` (all four typed columns are `try_cast`-checked: amount, transaction, posting, value
  dates); ErrorTable2 (UPI on `TRANSACTION_ID`) = `DUP_KEY` via `ROW_NUMBER() OVER (PARTITION BY key ORDER BY
  _ingested_at, _source_file) > 1`, first row kept. `ErrorLimit` -> `FAIL UPDATE` on the non-negotiable invariant
  (null key) + job-level count check on the quarantine table (skill §7 "ErrorLimit").
- `APPLY ('INSERT ... VALUES (:f (DATE, FORMAT ''YYYY-MM-DD''), :a (DECIMAL(15,2)) ...)')` -> `try_cast`s in the
  silver `SELECT` (a strict `CAST` would abort the whole update on one malformed line, the opposite of the FastLoad
  ET-table contract); `CURRENT_DATE` -> `current_date()`; `TIME(0)` -> `STRING`.
- `@Variable` job variables / `$tdpid/$user/$password` -> pipeline configuration + service principal; no secret inline.
- MLOAD `.LOGTABLE`, `.BEGIN IMPORT MLOAD ... WORKTABLES/ERRORTABLES/ERRLIMIT/CHECKPOINT/SESSIONS` -> pipeline
  bookkeeping (no artifacts); `.LAYOUT` `.FIELD`/`.FILLER` -> `schemaHints` + a typed temporary view excluding the
  filler.
- `.DML LABEL ... DO INSERT FOR MISSING UPDATE ROWS; UPDATE ...; INSERT ...` (upsert) -> `AUTO CDC INTO ... KEYS (...)
  SEQUENCE BY STRUCT(_ingested_at, _source_file) STORED AS SCD TYPE 1`: later micro-batch wins, file path breaks ties
  inside a micro-batch (`current_timestamp()` alone is query-scoped and would tie). MLOAD's *last row in the file wins*
  for a key repeated inside one file has no documented row-position metadata to reproduce it, so that case is
  rejected by a `FAIL UPDATE` expectation on a per-(key, file) count materialized view (`fx_rates_same_file_key_check`)
  rather than silently resolved.
- MLOAD `ERRORTABLES` for the FX feed -> `fx_rates_quarantine` (parse / cast failures), `try_cast` in the typed view.
- `.IMPORT INFILE ... FORMAT VARTEXT '|' LAYOUT ... APPLY ...` -> `FROM STREAM read_files(... sep => '|', header => false)`.
- `ETL_INSERT_TS`/`ETL_UPDATE_TS` = `CURRENT_TIMESTAMP(0)` -> excluded from recon as operational columns.

## Recon tier that catches a wrong conversion
- Header row loaded as data (`header => false` on the TPT feed): **Tier 1** row-count excess of exactly one per file
  and a `CAST_DATE` quarantine row per file.
- Delimiter/quote mismatch shifting columns: **Tier 3** keyed diff on `STG_TRANSACTIONS` by `TRANSACTION_ID`
  (`MERCHANT_NAME` containing `|` is the usual culprit); **Tier 1** on the quarantine table catches the gross case.
- Rejects silently dropped instead of quarantined (no `_rescued_data`): **Tier 1** `legacy ET rows + loaded rows =
  source lines` conservation check per file.
- Duplicate `TRANSACTION_ID` kept (FastLoad UPI dropped it to ErrorTable2): **Tier 1** `count(*)` vs
  `count(distinct TRANSACTION_ID)` on `STG_TRANSACTIONS`; downstream `FACT_TRANSACTION` row-count excess. The
  `DUP_KEY` quarantine rows are the mechanism; this check is the witness.
- Wrong duplicate kept (FastLoad kept the first in file order; the target keeps the first by `_ingested_at,
  _source_file`): **Tier 3** keyed diff on `STG_TRANSACTIONS` for the duplicated keys only -- the two rows differ in
  non-key columns or the diff is empty.
- Malformed `POSTING_DATE`/`VALUE_DATE` reaching a strict `CAST` (update aborts, nothing lands): **Tier 1** `bronze =
  quarantine + STG_TRANSACTIONS` conservation per file, and the pipeline update failure itself.
- `AUTO CDC` keyed on fewer columns than the MLOAD `WHERE` (e.g. missing `RATE_DATE`): **Tier 1** on
  `DIM_EXCHANGE_RATES` (rows collapse to one per pair); **Tier 2** `sum(BASE_CURRENCY_AMOUNT)` drift in example 04.
- `SEQUENCE BY` ordering differing from MLOAD file order when two files carry the same key: **Tier 3** keyed diff on
  `EXCHANGE_RATE`, neutralised by `decimal_round` only if within tolerance — otherwise a real ordering defect. Same
  key twice in one file is a `FAIL UPDATE` on `fx_rates_same_file_key_check`, not a recon signature.
- `DECIMAL(18,8)` cast vs Teradata implicit conversion of `CHAR(18)`: **Tier 3** last-digit diffs; `decimal_round`.

## Citations
- `FROM STREAM read_files(...)`, option names, "Unity Catalog pipelines must use external locations": `databricks-pipelines`
  `references/auto-loader-sql.md`.
- CSV option names (`sep`, `header`, `nullValue`, `skipRows`, `rescuedDataColumn`): `references/options-csv.md`.
- `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW | FAIL UPDATE`, warn default, "No subqueries": `references/expectations-sql.md`.
- Quarantine branch on `_rescued_data`: `references/streaming-patterns.md` "Rescue-Data Quarantine"; keep-first
  dedup with `ROW_NUMBER() OVER (PARTITION BY key ORDER BY ...)` over `STREAM(...)`: same file, "Deduplication / By
  key (keep first)".
- `SEQUENCE BY STRUCT(ts_col, tiebreaker_col)`: `references/auto-cdc-sql.md` "Multi-column sequencing".
- Expectations on `CREATE OR REFRESH MATERIALIZED VIEW`: `references/expectations-sql.md` (materialized view form).
- `SELECT * EXCEPT (...)`: `databricks-dbsql` `references/materialized-views-pipes.md` "DROP -- Remove columns".
- `_metadata.file_path`: `references/dlt-migration.md` (file metadata row).
- Streaming temporary view read via `FROM STREAM(view_name)`: `references/temporary-view-sql.md`.
- `AUTO CDC INTO ... KEYS ... SEQUENCE BY ... COLUMNS * EXCEPT ... STORED AS SCD TYPE 1`, pre-filter via temporary view,
  "FROM STREAM(...) accepts only a table/view identifier": `references/auto-cdc-sql.md`.
- Backfill alternative (`COPY INTO`) named in the v0 stub is not documented in the official skills read; route through
  `target-routing` before using it.

## Not verified live
- `SEQUENCE BY STRUCT(_ingested_at, _source_file)` reproduces MLOAD file-order semantics when the feed's file names
  sort in arrival order (date-stamped names); a file-modification-time metadata column would be stricter but is not
  documented in the official skills read. Row position inside a file is not exposed, hence the per-(key, file) guard.
- Whether `ROW_NUMBER()` over `STREAM(...)` (the documented keep-first pattern) orders rows within one file the way
  FastLoad did; the kept duplicate may differ from legacy ErrorTable2 contents (Tier 3 on duplicated keys only).
- `try_cast` inside a streaming table select and `length()` inside an expectation (both plain SQL functions, allowed
  per `expectations-sql.md`, but not exercised here).
- Whether a `FAIL UPDATE` violation leaves the quarantine table populated for the same update (the reference says the
  transaction rolls back).
