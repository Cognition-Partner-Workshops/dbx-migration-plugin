# ssis-dataflow-lookup: `LoadPaymentFact.dtsx`

Source: `source.dtsx` — a hand-written, `.dtsx`-shaped package description (the Sybase fixture has no SSIS packages; element names follow the DTS namespace so the §1/§2 XPath rules apply to it unchanged). Converted: `converted.sql` (Lakeflow Spark Declarative Pipelines SQL, run as a `pipeline_task`).

## Constructs exercised
- Data Flow Task with **OLE DB Source** (`AccessMode=2` SQL command, two `?` parameters bound to `User::LoadDate`) -> `CREATE TEMPORARY VIEW` with pipeline `configuration` key `${load_date}` (SKILL §2b "SSIS Data Flow" lineage row; `databricks-pipelines` temporary-view-sql.md, pipeline-configuration.md).
- **Lookup** (`CacheType=0` Full cache, `NoMatchBehavior=1` redirect, parameterised reference query on `Package::ServicerId`) -> `LEFT JOIN` + `lkp_loan_id IS NULL` routing (SKILL §6 "SSIS Lookup", §7 "Lookup cache mode vs collation").
- **Derived Column** expressions `YEAR()*100+MONTH()`, `? :` ternary, nested ternary, `(DT_WSTR,4)TRIM()` -> `year()/month()`, boolean expression, `CASE`, `trim()` (SKILL §6 "SSIS Derived Column").
- **OLE DB Destination** fast load (`FastLoadOptions`, `FastLoadKeepNulls=false`) -> `CREATE OR REFRESH MATERIALIZED VIEW ... CLUSTER BY` with `coalesce` for the DW column default (`databricks-pipelines` materialized-view-sql.md).
- **Lookup No Match Output -> second destination** -> `CONSTRAINT ... ON VIOLATION DROP ROW` on the fact plus a quarantine dataset with the inverse predicate (`databricks-pipelines` expectations-sql.md; SKILL §6 "SSIS error output").
- **OnError event handler** (Execute SQL Task into `dw.etl_log`) -> job `on_failure` notification on the `pipeline_task` (`databricks-jobs` notifications-monitoring.md; SKILL §6 "SSIS Event Handlers").
- Two **connection managers** (`LOANSQL01/loan_servicing`, `LOANDW01/loan_mart`) -> two catalog/schema names in the mapping (SKILL §6 "SSIS Connection Managers"); `ProtectionLevel=DontSaveSensitive` recorded in §9 governance discovery.
- Package parameter (`ServicerId`, required) and variables (`LoadDate`, `RowsRead`) -> configuration keys; `RowsRead` (a row-count variable) has no consumer in the package and is dropped.
- SSIS type codes `cy` (currency = `MONEY`), `str`/`wstr` with `codePage`, `dbTimeStamp`, `dbDate`, `i4`, `bool` -> §4 type map rows.

## Lineage (FACT)
Reads: `loan_servicing.dbo.payments` (SqlCommand), `loan_servicing.dbo.loans` (Lookup reference). Writes: `loan_mart.dw.fact_payment`, `loan_mart.dw.err_payment_no_loan`, `loan_mart.dw.etl_log` (event handler). Unit = the package + both destinations; `fact_payment` is shared if another package or procedure writes it (none in this fixture).

## Recon tier that catches a wrong conversion
- **Tier 1**: `count(fact_payment) + count(err_payment_no_loan)` for the load date must equal the source row count of the OLE DB Source query; the split between them is the Lookup's match/no-match count. An `INNER JOIN` instead of `LEFT JOIN` silently drops the no-match rows (`err_*` empty, fact short).
- **Tier 2**: null-rate on `investor_code`/`property_state` (0 by construction after DROP ROW), `sum(total_amt)` (catches a `coalesce` default applied to the wrong column), distinct-count of `amt_bucket` (3) and of `payment_month` (1 per load date).
- **Tier 3** keyed on `payment_id`: `amt_bucket` boundaries (`< 500` vs `<= 500`), `is_reversal`, `loan_type` trimmed vs padded (`rstrip_spaces` masks the padding difference only if applied — the tolerance record must say which side trims).
- Lookup cache-mode trap: if the real package were Partial/No cache on a CI database, the join would need `collation_casefold`; Full cache does not (§7).

## INFERRED edges
The OnError handler's second `?` (message) is bound to a system variable not shown; the `etl_log` write itself is FACT. Connection strings are literal here; in a real project they are usually project parameters (INFERRED until the `.params` file is read).

## Lakebridge
`--source-dialect ssis` (experimental) converts Source/Derived Column/Destination into SparkSQL scaffolding; Lookup redirect and event handlers are mangled or dropped (SKILL §10). The SDP shape above is hand-derived from the transpiler scaffold.
