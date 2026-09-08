# InvestorFileExport.dtsx — HAND-CONVERT plan and target shape

**Status: hand-convert.** The Script Component `SCR Format and enrich` is not parsed by this skill or by Lakebridge (`--source-dialect ssis` leaves a `/* Script Component */` stub). This file is the conversion *plan* the template requires for a hand-convert unit: each statement of the C# body is classified, given a target, and given a recon check. Nothing here is generated automatically.

## 1. Classification of the script body (SKILL §6 "SSIS Script Task / Script Component")

| Block | C# behavior | Class | Target | Notes |
|---|---|---|---|---|
| (A) | `current_upb.ToString("N2", CultureInfo(culture_name))`, `(rate/100).ToString("P3", ci)` | pure expression, **culture-dependent** | SQL `format_number` + `CASE` on `culture_name` `[fn:format_number]` | .NET `N2` under `de-DE` gives `1.234.567,89`; Databricks `format_number(x, 2)` gives `1,234,567.89` (invariant). Only the cultures present in `dbo.investors` need a `CASE` branch (`translate(format_number(x,2), ',.', '.,')` for the comma-decimal group) `[fn:translate]`. Every culture value not covered -> `FAIL UPDATE` expectation. |
| (B) | weighted check digit over the digits of `loan_number` (weights 3,1,3,1... from the right, `(10 - sum % 10) % 10`) | pure expression, deterministic | SQL over `split`/`transform`/`aggregate` `[fn:split]` `[fn:transform]` `[fn:aggregate]` or a **SQL UDF** `CREATE FUNCTION check_digit(s STRING) RETURNS STRING` (`[dbsql:scripting]` routes UDF creation to `databricks-dbsql`) | Byte-for-byte reproducible; Tier 3 on `check_digit` is the certification. |
| (C) | per-row `HttpClient.GetStringAsync(RateServiceUrl + "/index/" + code)` with an in-memory cache keyed by `rate_index_code` | **external side effect** | out of the SQL path: a `notebook_task` (Python) that fetches the distinct `rate_index_code` values once, writes `ref_index_rate(rate_index_code, index_rate, as_of)`; the export joins it `[jobs:tasks]` | The cache means the source already used one value per code per run; the reference table makes that explicit and auditable. URL comes from `$Project::RateServiceUrl` -> job parameter; credentials, if any, via secret name only. |
| (D) | `if (current_upb <= 0) DirectRow...Skip` (silent drop, no error output) | row-level filter | `WHERE current_upb > 0` on the export view, **plus** a quarantine count so the drop is visible (`CONSTRAINT upb_positive EXPECT (current_upb > 0) ON VIOLATION DROP ROW` `[sdp:expect]`) | SSIS dropped silently; the converted unit must not (SKILL §6 "SSIS error output": every dropped row is counted). |
| — | `Variables.RateServiceUrl` read-only variable | parameter | job parameter `{{job.parameters.rate_service_url}}` `[jobs:params]` | |

## 2. Target shape

Three job tasks in sequence (`[jobs]` `depends_on`), replacing the Data Flow + FTP Task:

```yaml
tasks:
  - task_key: refresh_index_rates            # block (C)
    notebook_task:
      notebook_path: ../src/refresh_index_rates.py     # Python: one HTTP call per distinct rate_index_code -> ref_index_rate
      base_parameters:
        rate_service_url: "{{job.parameters.rate_service_url}}"
  - task_key: build_investor_export          # blocks (A), (B), (D)
    depends_on: [{task_key: refresh_index_rates}]
    sql_task:
      file: {path: ../sql/investor_export.sql, source: WORKSPACE}
      warehouse_id: ${var.warehouse_id}
  - task_key: deliver_investor_file          # Flat File Destination + FTP Task
    depends_on: [{task_key: build_investor_export}]
    notebook_task:
      notebook_path: ../src/deliver_investor_file.py   # writes fixed-width cp1252 CRLF file to a UC Volume, then SFTP with secret-scoped credentials
```

`investor_export.sql` (DBSQL; `[fn:lpad]` `[fn:rpad]` `[fn:format_number]` `[fn:translate]` `[fn:split]` `[fn:reverse]` `[fn:regexp_replace]` `[fn:transform]` `[fn:aggregate]` `[fn:if]`):

```sql
-- job parameters catalog / schema / investor_code are pushed down to the SQL task and read as :name [jobs:params]
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:schema);

CREATE OR REPLACE TABLE IDENTIFIER('investor_export_' || :investor_code) AS
WITH src AS (                                            -- SRC loans (OLE DB Source SqlCommand, ? -> :investor_code)
  SELECT l.loan_number, l.borrower_name, l.current_upb, l.interest_rate, l.rate_index_code,
         i.investor_code, i.culture_name
  FROM loans l
  JOIN investors i ON i.investor_id = l.investor_id
  WHERE l.loan_status = 'AC' AND i.investor_code = :investor_code
    AND l.current_upb > 0                                -- block (D), made explicit
),
fmt AS (
  SELECT s.*,
         -- block (A): culture-specific number formats; only cultures present in dbo.investors are handled
         CASE WHEN s.culture_name IN ('de-DE','nl-NL','es-ES','it-IT')
              THEN translate(format_number(s.current_upb, 2), ',.', '.,')
              ELSE format_number(s.current_upb, 2) END                          AS upb_formatted,
         CASE WHEN s.culture_name IN ('de-DE','nl-NL','es-ES','it-IT')
              THEN translate(format_number(s.interest_rate, 3), ',.', '.,') || ' %'
              ELSE format_number(s.interest_rate, 3) || '%' END                  AS rate_formatted,
         -- block (B): weighted check digit, weights 3,1,3,1,... from the rightmost digit
         CAST((10 - aggregate(
                 transform(
                   split(reverse(regexp_replace(s.loan_number, '[^0-9]', '')), ''),
                   (d, i) -> CAST(d AS INT) * IF(i % 2 = 0, 3, 1)),
                 0, (acc, x) -> acc + x) % 10) % 10 AS STRING)                    AS check_digit,
         -- block (C): reference table refreshed by the upstream notebook task
         format_number(r.index_rate, 4)                                          AS index_rate
  FROM src s
  LEFT JOIN ref_index_rate r ON r.rate_index_code = s.rate_index_code
)
SELECT rpad(loan_number, 20)  AS loan_number,     -- Flat File fixed-width layout (DTS:ColumnWidth)
       rpad(borrower_name, 60) AS borrower_name,
       lpad(upb_formatted, 18) AS upb_formatted,
       lpad(rate_formatted, 8) AS rate_formatted,
       lpad(index_rate, 8)     AS index_rate,
       check_digit
FROM fmt;
```

Open points that make this **hand-convert** rather than a rule (record each in `.migration/06_decisions.md`):
1. `split(..., '')` empty-separator behavior and the exact `format_number` grouping for every `culture_name` value present must be confirmed on a warehouse (Not verified live).
2. `ToString("P3")` inserts a culture-specific space before `%` in some cultures; the `CASE` above hard-codes the cultures seen in the fixture-shaped data.
3. Fixed-width file encoding (cp1252, CRLF, no header) and SFTP delivery are outside SQL; they live in the delivery notebook and are certified by the file hash (Tier 2 `sha2` `[fn:sha2]`), not by SQL recon.
4. `EncryptSensitiveWithPassword`: the package password is needed only to read the sensitive `FtpPassword`; the migration never reads it — the target uses a secret scope by name.
