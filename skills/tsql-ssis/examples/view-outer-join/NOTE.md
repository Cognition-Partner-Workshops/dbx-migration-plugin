# view-outer-join: `vw_active_loan_portfolio`

Source: fixture `schema/views/vw_active_loan_portfolio.sql` (Sybase ASE 16). Converted: `converted.sql` (Databricks SQL view).

## Constructs exercised
- ASE proprietary outer join `*=` twice, with a WHERE predicate on the inner side (`(m.status = 'A' OR m.status IS NULL)`) that must move into `ON` (SKILL §5 row 71, §7 trap 1, delta list item 1).
- Comma-join FROM list mixed with the `*=` operator.
- Correlated subquery for "latest non-reversed payment" (rewritten as a window, same rows).
- Scalar UDF calls `dbo.fn_format_loan_type`, `dbo.fn_get_delinquency_bucket` (inlined; SKILL §6 "Scalar UDF").
- `CHAR(4)` / `CHAR(2)` keys: `'FHA '` padding (SKILL §7 "Trailing-space padding"; `rstrip_spaces`).
- String concatenation `+` -> `||` (SKILL §5 row 7; no NULL operands here because `RTRIM(@loan_type)` is NOT NULL).

## Lineage (FACT)
Reads: `loans`, `borrowers`, `loan_modifications`, `payments`; calls `fn_format_loan_type`, `fn_get_delinquency_bucket`. Writes: none. Shared: yes (read by `sp_delinquency_snapshot` and the reporting procedures).

## Recon tier that catches a wrong conversion
- **Tier 1 (row count)**: a naive conversion that leaves the comma join, or that keeps `m.status = 'A'` in WHERE without the `IS NULL` arm, drops every loan with no modification history and every new origination with no payment. The fixture README calls this the planted bug; Tier 1 alone shows it.
- **Tier 2**: null-rate on `modification_id` and `last_payment_date` must equal the source's (non-zero); distinct-count on `loan_type` after `rstrip_spaces` must match.
- Tier 3 keyed on `loan_id` confirms `delinq_bucket` and `loan_type_desc` strings.

## Lakebridge
`mssql` flag mangles `*=`; this rewrite is done before transpile (SKILL §10).
