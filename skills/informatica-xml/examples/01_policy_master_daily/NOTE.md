# 01 - m_POLICY_MASTER_DAILY / wf_POLICY_MASTER_DAILY

Fixture: `informatica/XML/wf_POLICY_MASTER_DAILY.xml` (copied verbatim as `source.xml`) and
`informatica/parameter_files/wf_POLICY_MASTER_DAILY.par` (`source.par`). Related but not copied:
`informatica/scripts/run_wf_policy_master`, `orchestration/controlm/ALBION_DWH_DAILY.xml` job
`ALB-DWH-0032`, `orchestration/cron/crontab_prod.txt` line `15 3 * * *`.

## Constructs exercised

| Construct | Where in source | Handled by (SKILL.md) |
|---|---|---|
| Fixed-width flat-file source: `FLATFILE DELIMITED=NO`, `SOURCEFIELD OFFSET/LENGTH/PICTURETEXT`, `NULL_CHARACTER='*'`, `STRIPTRAILINGBLANKS=YES`, codepage `MS1252` | `SOURCE PLCYMSTR_DAILY` | section 4 (fixed-width, implied decimals `9(09)V99`), trap 21 |
| Julian `YYDDD` with Y2K pivot 49: `TO_INTEGER(SUBSTR())`, `IIF`, `TO_CHAR`, `TO_DATE(...,'YYYYMMDD')`, `ADD_TO_DATE(...,'DD',n)` | `EXP_POLICY_DATES` | section 5 rows 1, 9, 12, 14, 21, 34; trap 22 |
| Variable port (`PORTTYPE=LOCAL VARIABLE`) feeding output ports | `v_YEAR`, `v_PC_STD` | section 5 row 68 |
| `INSTR`, `LENGTH`, `LTRIM(RTRIM())`, `SUBSTR` with computed bounds, `UPPER`, `\|\|`, `REG_MATCH` | `EXP_POSTCODE_DQ` | rows 31, 35, 36, 37, 39, 42 |
| `IIF(a = x OR a = y, 'Y', 'N')` | `EXP_POLICY_FLAGS` | row 1 |
| Connected static Lookup, `Lookup policy on multiple match = Use First Value`, `decimal(10,0)` lookup port against a `string` source field | `LKP_XREF_CLIENT_PARTY` (`REUSABLE=YES`, shared object) | row 74, trap 9, section 3 |
| Event Wait file watcher with `$$RUNDATE` in the path; per-run `$$RUNDATE` rewritten by the wrapper -> date read from the arrived file name (`plcymstr_raw` Auto Loader table + newest-`RUNDATE` view), NOT from a deployment-time bundle variable; `informatica.RUNDATE` kept only as an explicit-rerun override | `ew_WAIT_PLCYMSTR`, `source.par` | section 6 (Event Wait -> `trigger.file_arrival`), section 2 (parameter precedence: per-run values never become deploy-time constants) |
| Conditional link `$s.Status = FAILED` -> Email task; `Fail parent if this task fails` | `WORKFLOWLINK`, `email_FAIL_POLICY_LOAD` | section 6 (link conditions, Email, Control) |
| Parameter file: `$$RUNDATE`, `$DBConnection_TGT/_LKP`, `$InputFile_*`, `$BadFileName`, `$$COMMIT_INTERVAL` | `source.par` | section 2 (parameter precedence), section 6 (`$$` parameters, commit interval) |
| Dual schedule ownership (Control-M `ALB-DWH-0032` and cron `15 3 * * *`), wrapper `pmcmd ... -wait` + `$?` + BTEQ kick-off | not copied (see paths above) | section 2 "Scheduler edges", trap 24 |

## Recon tier that catches a wrong conversion

- **Tier 3 keyed diff on `INCEPTION_DT`** (key `POLICY_NO`): an off-by-one pivot (50 instead of 49) or a
  `TO_INTEGER` implemented as truncation on a value with a fractional part shows as century flips / day shifts
  on a subset of rows. `datetime_utc_truncate_ms` is irrelevant here (DATE), so the tier is exact.
- **Tier 3 on `POSTCODE_STD` and `POSTCODE_DQ_STATUS`**: a regex translated with POSIX classes or a `substr`
  off-by-one (0-based vs 1-based) flips `VALID`/`INVALID` for the ~5-7 character postcodes. `rstrip_spaces` is
  applied on both sides because the legacy CHAR-origin field is trimmed in the mapping anyway.
- **Tier 3 on `PARTY_ID`**: the wrong `row_number()` order for `Use First Value` shows as a stable subset of
  differing values among clients with more than one crosswalk row; Tier 1 row excess if the dedupe is omitted.
- **Tier 2 `sum(ANNUAL_PREMIUM_GBP)`** with `decimal_round`: a missed implied-decimal scaling (`9(09)V99`) is a
  factor-100 aggregate mismatch, visible before any per-row work.
- **Tier 1 row count vs the expectation metrics**: rows the legacy session sent to `$BadFileName` must equal the
  `expect_or_drop` drop count; a difference means an unmodelled row-error condition.

## Findings recorded, not converted

- The export contains `CONNECTOR` rows for only 5 of the 11 target columns (`INCEPTION_DT`, `POSTCODE_STD`,
  `POSTCODE_DQ_STATUS`, `ACTIVE_POLICY_FLAG`, `PARTY_ID`). `POLICY_NO` (primary key), `CLIENT_NO`, `PRODUCT_CD`,
  `ANNUAL_PREMIUM_GBP`, `POLICY_STATUS`, `LOAD_TS` have no inbound connector. In a real export an unconnected
  `NOTNULL` target port would fail every row, so this is treated as an abbreviated fixture; the converted
  `stg_policy_master` fills them from the obvious source fields and marks the mapping **INFERRED** in the unit brief.
- `PLCY_ANNL_PREM` is `string(11)` with `PICTURETEXT 9(09)V99`; the implied 2-decimal scaling is nowhere in the
  mapping text and is INFERRED from the picture clause and the target `decimal(12,2)`.
- Pivot 49 vs the actuarial SAS macro's 50 (`EXP_POLICY_DATES` description) is an estate finding for the
  assessment; the conversion keeps 49.
- `wf_PARTY_MDM_SYNC` must finish before this workflow (its description says so) but no dependency exists in
  Informatica or Control-M; the job graph above does not invent one - the decision goes to D5.
- The target load type of `STG_POLICY_MASTER` (truncate/insert vs append) is not in the export. The converted
  `stg_policy_master` is recomputed from the newest `RUNDATE` ingested (replace-with-today), which is INFERRED; the
  `plcymstr_raw` table keeps every day's lines, so an explicit-day rerun sets `informatica.RUNDATE` and needs no
  file restore. The legacy landing directory was swept by NDM, so "newest file" and "the file that fired the
  watcher" were the same thing; if two files land between two runs the newer one wins and the older is a Tier 1 gap.

## Not verified live

- Actual `REP_*` view names and the persisted value of `$$RUNDATE` at run time.
- Whether `LKP_XREF_CLIENT_PARTY`'s cache order matches the `ORDER BY PARTY_ID` chosen above.
- Row counts, reject counts, and the `email_FAIL_POLICY_LOAD` delivery path.
- The `MS1252` codepage's effect on `PLCY_SURNAME` characters 0x80-0x9F (needs a real extract).
