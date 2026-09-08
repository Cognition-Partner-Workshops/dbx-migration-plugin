# 05 - m_RI_BORDEREAUX_MONTHLY / wf_REINSURANCE_BORDEREAUX_MONTHLY + scripts/bdx_transfer

Fixture: `informatica/XML/wf_REINSURANCE_BORDEREAUX_MONTHLY.xml` (`source.xml`), `informatica/scripts/bdx_transfer`
(`source.bdx_transfer.ksh`, mailbox value redacted in this copy). Related, not copied:
`orchestration/cron/crontab_prod.txt` line `0 6 1-5 * *` and its "nobody is sure which scheduler owns bdx_transfer"
comment; `docs/` SAS `08_solvency_ii_qrt_prep.sas` (the drifted SII LoB copy).

## Constructs exercised

| Construct | Where in source | Handled by (SKILL.md) |
|---|---|---|
| `REPLACESTR(0, s, 'AL/', 'ALB-')` case-insensitive multi-char replace -> `(?i)` regex | `EXP_REKEY_POLICY` | row 41, trap 17 |
| `REPLACECHR(0, s, '/', '-')` and `REPLACECHR(0, s, CHR(163), '')` -> `translate()` | `EXP_REKEY_POLICY`, `EXP_AMT_CLEAN` | row 40 |
| `CHR(163)` under repository `CODEPAGE="Latin1"` (0xA3 = `£`; UTF-8 needs the file read with the right encoding) | `EXP_AMT_CLEAN` | row 45, trap 18, section 4 (codepage) |
| `TO_DECIMAL(text)` into `decimal(12,2)`: non-numeric -> 0 in Informatica vs NULL in Spark | `EXP_AMT_CLEAN` | row 13 |
| Reusable cached Lookup on reference data that has **drifted** from another engine's copy | `LKP_SII_LOB` (`REUSABLE=YES`, `REF_DB.SII_LOB_MAP`) | row 74, trap 10, section 3 |
| Many hand-maintained broker-specific `SOURCE` definitions (14) -> one Auto Loader source + per-broker schema decision | described, not exported | section 1 (object-size signals), section 11 |
| SFTP pre-step wrapper called by cron, with per-broker credentials and a `mailx` completion mail | `source.bdx_transfer.ksh` | section 2 "Scheduler edges" (wrapper scripts), section 9 (secrets by name), section 6 (Command task -> ingestion decision) |
| Per-broker file watcher + WD3 + habitual late broker -> `trigger.file_arrival` wakes the job; a read-only gate task (`broker_completeness_gate.py`: expected-broker table vs files landed for the target month) + `condition_task` is the only path to the publishing `pipeline_task`, so a partial month is never published and a late file simply re-triggers | `WORKFLOW DESCRIPTION` | section 6 (Event Wait, file arrival; Link condition on counts -> task value) |
| Row-preserving Expression transformations converted as `withColumn` on the same raw row (never as separate views re-joined on a repeatable key) and a Lookup forced to one row per key | `EXP_REKEY_POLICY`, `EXP_AMT_CLEAN`, `LKP_SII_LOB` | row 74, section 6 "Row error handling", trap 10 |
| Ownership ambiguity of a cron entry | crontab comment | trap 24 |

## Recon tier that catches a wrong conversion

- **Tier 3 on `POLICY_NO`** (key `BROKER_ID`, `CLAIM_REF`): a case-sensitive `replace('AL/', 'ALB-')` leaves
  lower-case broker references (`al/mot/...`) un-rekeyed - a small, broker-correlated subset of diffs.
  `collation_casefold` must be **off** for this column or the defect is masked.
- **Tier 2 `sum(AMT)` and null rate on `AMT`**: reading the CSV as UTF-8 while it is Latin1 leaves `Â` in front of
  every amount, `try_cast` NULLs the whole column, and the 0-vs-NULL rule then turns them into zeros: the sum
  collapses and the null rate is unchanged - which is exactly why the `amt_unparsable` expectation count is part of
  the Tier 1 check, not only the sum. `decimal_round` is not involved (the values are text-parsed, no arithmetic).
- **Tier 3 on `SII_LOB`**: pointing the converted lookup at the SAS-side mapping (or a "corrected" copy) changes
  every `PET` product row - the recon sees it; the drift is a finding to be decided, not fixed in conversion.
- **Tier 1 count per `BROKER_ID`**: a run that fires before BRK0007's late file has landed is a count shortfall on
  one broker only; the legacy behaviour (manual restart) has no Databricks equivalent by default, so the completeness
  gate is an explicit task in the job, and a duplicated `CLAIM_REF` inside one broker file must show as exactly the
  legacy count (n), not n*n from a self-join of derived views.

## Findings recorded, not converted

- The SII LoB reference data disagrees between Informatica (`PET -> 'Other motor'`, a 2023 edit) and SAS
  (`PET -> 'Miscellaneous financial loss'`); QRT and outward bordereau therefore disagree today.
- `bdx_transfer` credentials are per-broker SFTP accounts; only the secret **names** may appear in any artifact.
- Ceded-claims computation and the Lloyd's-format output are described but not exported: separate census units.
- Accounting negatives `(1,234.56)` parse to 0 in both engines - a latent legacy defect, reproduced.

## Not verified live

- The 14 broker `SOURCE` definitions (column names, delimiters, encodings, header rows).
- Whether the session code page is `Latin1` or per-connection `MS1252` for the flat-file sources.
- `LKP_SII_LOB` condition and multiple-match policy; the `SII_LOB_MAP` table's key.
- Which scheduler actually starts `bdx_transfer` in production (cron vs Control-M vs manual).
