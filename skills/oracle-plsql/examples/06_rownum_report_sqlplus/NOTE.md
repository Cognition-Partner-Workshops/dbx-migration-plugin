# 06 — SQL*Plus paged spool report with `ROWNUM` sandwich -> parameterised `sql_task`

**Source**: `fixture/14_rpt_policy_page.sql`. `SET` directives, `DEFINE`/`&1`/`&2` substitution, `WHENEVER SQLERROR`,
`SPOOL`, `ROWNUM <= hi` / `rn > lo` pagination, `LISTAGG ... WITHIN GROUP`, `TO_CHAR` date and `FM` number formats,
`CHAR(8)` column, `IS NULL` predicate that silently covers `''` inserts, `EXIT SUCCESS`.

**Profile / track**: analytical (Tier 4 report output). Lakeflow Jobs `sql_task` with job parameters.

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| `SET PAGESIZE/LINESIZE/...`, `SPOOL`, `WHENEVER`, `EXIT` | dropped / task semantics; table stated per directive | §6 SQL*Plus row; §7 trap 17 |
| `&1`, `&2`, `DEFINE page_size` | named parameter markers `:page_no`, `:as_of`, `:page_size` via `sql_task.parameters` | §6 SQL*Plus row |
| `ROWNUM` sandwich | `ORDER BY ... LIMIT ... OFFSET` in the inner query + `row_number()` for `rn` | §5 #68; §7 trap 7 |
| `LISTAGG(x,';') WITHIN GROUP (ORDER BY ...)` | `listagg(...) WITHIN GROUP (...)` | §5 #72 |
| `TO_CHAR(d,'DD-MON-YYYY')` | `upper(date_format(d,'dd-MMM-yyyy'))` | §5 #45 |
| `TO_CHAR(n,'FM999999990.00')` | `cast(cast(n AS DECIMAL(18,2)) AS STRING)` (no `FM` in Databricks `to_char`) | §5 #44 |
| `CHAR(8) postcode` | `rtrim()` | §7 trap 5 |
| `cover_note_ref IS NULL` | `IS NULL OR = ''` | §7 trap 1 |
| `surname || ', ' || forename` | `concat(surname, ', ', coalesce(forename,''))` | §7 trap 23 |
| `TO_DATE('&as_of','YYYY-MM-DD')` | `to_date(:as_of, 'yyyy-MM-dd')` | §5 #46 |

## Recon tier that catches a wrong conversion

- **Tier 4** (report output, byte-level on the rendered columns, keyed by `(page_no, rn)`): this unit *is* its
  output. A `LIMIT`/`OFFSET` placed outside the ordered subquery, or an `ORDER BY` on a non-total key, produces a
  different set of rows per page (Oracle: `ROWNUM` after the inner `ORDER BY`). `MMM` without `upper()` gives `Jan`
  vs `JAN`; a bare `cast(annual_premium AS STRING)` gives `1234.5` vs `1234.50`; a missing `rtrim` gives a
  trailing-blank `postcode`.
- **Tier 1** on the page: exactly `page_size` rows except the last page. Off-by-one in `OFFSET :page_size *
  (:page_no - 1)` shows as a duplicated/skipped row between consecutive pages.
- **Tier 3** on the underlying `SELECT` (before formatting), keyed by `policy_no`: catches the `''` rows that Oracle
  includes via `IS NULL` and Databricks excludes if the `OR = ''` is dropped (with `empty_string_is_null` the
  canonicalized target set matches, which is the intended fix).

## Canonicalization used

`rstrip_spaces` (`postcode`), `empty_string_is_null` (`cover_note_ref`), `datetime_utc_truncate_ms`
(`expiry_dt` before `date_format`), `decimal_round` (`annual_premium` before the 2-dp cast).

## Not verified live

`sql_task.parameters` -> named-marker binding for a `.sql` file task; `LIMIT`/`OFFSET` with parameter markers in the
expression; `listagg` inside a correlated scalar subquery on DBSQL; `date_format` `MMM` locale.
