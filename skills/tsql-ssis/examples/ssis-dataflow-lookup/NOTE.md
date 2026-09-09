# ssis-dataflow-lookup: `LoadPaymentFact.dtsx`

Source: `source.dtsx`, a hand-written `.dtsx`-shaped package (the Sybase fixture has no SSIS).
Converted: `converted.sql` (Jobs `sql_task` file) + `converted.yml` (job wrapper) + `log_onerror.sql` (OnError task).

Constructs: OLE DB Source with `?` bound to `User::LoadDate`; Lookup (Full cache, no-match
redirected, reference query on `$Package::ServicerId`); Derived Column expressions (`YEAR()*100+MONTH()`,
`? :`, nested `? :`, `(DT_WSTR,4)TRIM()`); OLE DB Destination fast load with `FastLoadKeepNulls=false`;
no-match output to a second destination; OnError handler writing `dw.etl_log`; two connection
managers (`loan_servicing` source, `loan_mart` target) -> `src_*` / `tgt_*` job parameters.

Execution model: a per-execution parameter selects the row set, so the target is a **bounded
batch** (job parameters, one window read, idempotent `MERGE`s), not a streaming table (its global
checkpoint cannot re-evaluate consumed rows under a new `servicer_id`) and not a pipeline
`configuration` key (frozen at deploy). The window and its match verdict are read once into a
temp table so both outputs partition one `loans` snapshot, as the Full-cache Lookup did.

Recorded per-unit decisions: same-day rerun is now a no-op (source appended twice);
`err_payment_no_loan` gains a `servicer_id` key column; one `etl_log` row per failed run
(SSIS: one per error event); `User::RowsRead` dropped (no consumer). Not verified live: a temp
table persisting across the statements of one `sql_task` file.

Recon: **Tier 1** `count(fact) + count(err WHERE servicer_id = S)` per `load_date` = source window
count, unchanged by a rerun; a two-servicer run (7 then 12) must leave servicer 12's facts in
`fact_payment`; a run for a servicer with no loans puts every row in `err_payment_no_loan`; a
failure-injection run adds exactly one `etl_log` row. **Tier 2** `sum(total_amt)`, distinct
`amt_bucket` = 3. **Tier 3** on `payment_id`: bucket boundaries, `is_reversal`, trimmed `loan_type`.
Full cache compares in .NET (case- and space-sensitive): no `collation_casefold` on the join.
