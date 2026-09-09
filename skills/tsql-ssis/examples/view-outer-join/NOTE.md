# view-outer-join: `vw_active_loan_portfolio`

Source: fixture `schema/views/vw_active_loan_portfolio.sql` (Sybase ASE 16), column list trimmed.

Constructs: ASE `*=` outer join in a comma FROM list with an inner-side WHERE predicate
(`m.status = 'A' OR m.status IS NULL`) that must move into `ON`; correlated latest-payment
subquery; scalar UDFs `fn_format_loan_type` / `fn_get_delinquency_bucket` inlined;
`CHAR(4)` padding (`'FHA '`), `+` -> `||`.

Lineage: reads `loans`, `borrowers`, `loan_modifications`, `payments`; shared (read by
`sp_delinquency_snapshot` and the reporting procs).

Recon: **Tier 1** row count catches a comma join left as an inner join, or the status predicate
kept in WHERE (drops every loan without an active modification or without a payment).
Tier 2 null-rate on `modification_id` / `last_payment_date` must be non-zero and equal;
Tier 3 on `loan_id` confirms `loan_type_desc` / `delinq_bucket` (`rstrip_spaces` on `loan_type`).

Lakebridge `mssql` mangles `*=`; rewrite before transpiling.
