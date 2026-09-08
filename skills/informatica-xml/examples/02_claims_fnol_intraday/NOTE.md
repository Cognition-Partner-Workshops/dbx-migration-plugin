# 02 - m_CLAIMS_FNOL_INTRADAY / wf_CLAIMS_FNOL_INTRADAY

Fixture: `informatica/XML/wf_CLAIMS_FNOL_INTRADAY.xml` (verbatim as `source.xml`). Related, not copied:
`orchestration/controlm/ALBION_CLAIMS_INTRADAY.xml` job `ALB-CLM-0107` (cyclic, every 30 minutes,
06:00-22:00 Europe/London); `docs/` incident `INC0067812` (Guidewire date-format drift).

## Constructs exercised

| Construct | Where in source | Handled by (SKILL.md) |
|---|---|---|
| `IIF` selecting a source-specific `TO_DATE` format (`MM/DD/YYYY` vs `DD/MM/YYYY`) | `EXP_CLAIM_DATES` | section 5 rows 1, 9; trap 6 |
| `TO_DATE` row-error semantics (unparsable input rejects the row, no NULL) | `EXP_CLAIM_DATES` | row 9, row 61 (row error -> quarantine), section 6 "Row error handling" |
| `DECODE` with explicit `NULL` default and a **documented semantic defect** (`'S' -> 'N'` while BTEQ maps `'S' -> 'Y'`) | `EXP_CLAIM_FLAGS` | row 3; section 11 "Estate-level defects surfaced in descriptions" |
| `DECODE` whose default is the input port (pass-through) | `EXP_CLAIM_STATUS` | row 3 |
| Union transformation with two input groups | `UN_CLAIM_SOURCES` | row 73 (`UNION ALL`, never `UNION`) |
| Export **without** `SOURCE`/`TARGET`/`CONNECTOR` rows: lineage recovered from `DESCRIPTION` text and marked INFERRED | whole mapping | section 2 (`CONFIDENCE`), section 11 |
| 30-minute Control-M cyclic schedule with a daily window -> Quartz cron with seconds field, `timezone_id` | Control-M (not copied) | section 6 (scheduler edges), `databricks-jobs` `references/triggers-schedules.md` |
| SQL-only mapping routed to a DBSQL `CREATE OR REPLACE PROCEDURE` with `identifier()` table parameters and an `EXIT HANDLER` | converted.sql | section 6 (order: DBSQL first), `databricks-dbsql` SKILL.md "Stored Procedure with Error Handling" |

## Recon tier that catches a wrong conversion

- **Tier 3 keyed diff on `LOSS_DT`** (key `CLAIM_ID`) with `datetime_utc_truncate_ms`: if the converter "fixes"
  the Guidewire format to `dd/MM/yyyy`, every Guidewire row with day <= 12 differs from the legacy store by a
  month/day swap - the whole point of reproducing the defect as written during parallel run. A Quartz/Java token
  mistake (`DD` = day-of-year, `mm` = minutes) shows the same way on all rows.
- **Tier 1 row count plus quarantine count**: Informatica rejects rows whose `TO_DATE` fails; a conversion that
  emits NULL and loads the row shows as a count excess equal to the legacy reject-file size.
- **Tier 2 distribution of `FRAUD_FLAG` and `CLAIM_STATUS`**: a `DECODE` translated to a Boolean chain, or a missed
  pass-through default in `EXP_CLAIM_STATUS`, moves rows between categories (`NULL` vs `'N'`, raw code vs label).
- **Tier 1 count on the Union**: a `UNION` (distinct) instead of `UNION ALL` drops duplicate FNOL events that the
  legacy target legitimately holds twice.

## Findings recorded, not converted

- Guidewire has sent `DD/MM/YYYY` since the 2023 upgrade; the mapping still parses `MM/DD/YYYY`. Converted code
  keeps the defect so the recon can pass against the legacy target; correcting it is a decision in `06_decisions.md`.
- `'S'` (suspected) maps to `'N'` here and to `'Y'` in the BTEQ path. Cross-dialect finding for the assessment.
- The fixture export omits `SOURCE`, `TARGET`, `CONNECTOR`, `SESSION` and `WORKFLOW` elements; source/target
  names, column names and the `INSERT` load type are INFERRED from `DESCRIPTION` attributes.

## Not verified live

- Source/target table names and columns, `Treat source rows as` for the session, and whether the target is
  insert-only or data-driven.
- Real reject-file volume under the row-error rule.
- Control-M `ALB-CLM-0107` calendar exceptions (bank holidays) and its actual `pmcmd` command line.
