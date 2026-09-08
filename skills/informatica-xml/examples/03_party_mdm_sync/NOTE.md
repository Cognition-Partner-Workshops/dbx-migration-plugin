# 03 - m_PARTY_MDM_SYNC + mplt_DQ_PARTY_STANDARDISE (shared mapplet)

Fixture: `informatica/XML/wf_PARTY_MDM_SYNC.xml` (`source.xml`), `informatica/mapplets/mplt_DQ_PARTY_STANDARDISE.xml`
(`source.mapplet.xml`), `informatica/parameter_files/wf_PARTY_MDM_SYNC.par` (`source.par`). Related, not copied:
`orchestration/controlm/ALBION_DWH_DAILY.xml` job `ALB-MDM-0003` (01:30, and the comment about the missing `INCOND`),
`docs/architecture_overview.md` (Oracle MDM hub as target).

## Constructs exercised

| Construct | Where in source | Handled by (SKILL.md) |
|---|---|---|
| Shared mapplet consumed by two workflows -> converted once as a function module (wave 0) | `mplt_DQ_PARTY_STANDARDISE` (folder `SHARED="SHARED"`) | section 3 (shared objects, mapplet as wave-0 view/module), section 1 (`MAPPLET` element) |
| `INITCAP` word-boundary semantics (any non-alphanumeric) | `EXP_NAME_STD` | row 38, trap 15 |
| `REPLACESTR(0, ...)` single-pass replace, `REPLACECHR(0, s, ' ', '')` deletion, `LTRIM(RTRIM())` | `EXP_NAME_STD`, `EXP_PHONE_STD` | rows 31, 40, 41, trap 17 |
| `LOWER`, `REG_MATCH` with a Perl-style pattern (portable to Java regex) | `EXP_EMAIL_DQ` | rows 37, 42 |
| `ISNULL`, `LENGTH` on a CHAR-origin port, `SUBSTR`, `\|\|` NULL-skipping | `EXP_NINO_MASK` | rows 4, 34, 36, 39; traps 4, 14 |
| `SOUNDEX(UPPER())`, `TO_DATE(x, 'DD/MM/YYYY')` on a text DOB into a `date/time(29,9)` port | `EXP_XMATCH_APF` | rows 9, 47; section 4 (`date/time` -> `TIMESTAMP_NTZ`); row 61 (row error) |
| Oracle target folding `''` to NULL | `$DBConnection_TGT=ORA_MDM_HUB_PROD` | trap 5; canonicalization `empty_string_is_null` |
| `$$` parameters that are thresholds, not connections (`$$MATCH_CONFIDENCE_FLOOR`, `$$SUSPECT_QUEUE_CAP`) | `source.par` | section 2 (parameter precedence), row 64 |
| Three connections across two source engines (Teradata policy admin, Teradata core banking) and one target (Oracle) | `source.par` | section 9 (connections -> UC connections / external locations) |
| Undeclared cross-workflow dependency ("must finish before wf_POLICY_MASTER_DAILY, relies on runtime luck") | `WORKFLOW DESCRIPTION`, Control-M comment | section 11 (risk heuristics), converted.job.yml |

## Recon tier that catches a wrong conversion

- **Tier 3 on `FIRST_NAME_STD` / `LAST_NAME_STD`** (key `PARTY_ID`): plain `initcap()` leaves `o'brien` as
  `O'brien` and `smith-jones` as `Smith-jones`; the diff is confined to names with apostrophes/hyphens and is the
  signature of trap 15. `collation_casefold` must stay **off** for these columns or the defect is hidden.
- **Tier 3 on `NINO_MASKED`** with `rstrip_spaces`: if the converted code trims `NINO` before `LENGTH`, blank
  CHAR(9) NINOs become NULL on Databricks but `'  *****  '` (-> `''` after `rstrip_spaces`) on Teradata-fed legacy
  output; **Tier 2 null rate** on the column jumps by the blank-NINO share.
- **Tier 3 on `EMAIL_DQ_STATUS`**: a POSIX/ICU regex flavour swap or an `rlike` without anchors flips VALID/INVALID
  for a minority of addresses.
- **Tier 3 on `BIRTH_DT`** with `datetime_utc_truncate_ms`: `dd/MM/yyyy` vs `MM/dd/yyyy` token errors transpose
  rows with day <= 12; a `TIMESTAMP` (zoned) target instead of `TIMESTAMP_NTZ` shifts values by the workspace
  offset (trap 7).
- **Tier 2 null rate on `EMAIL_STD`**: if `''` is not folded to NULL at the write boundary the Delta table holds
  empty strings the Oracle hub never had (trap 5) - unless STOP A set `empty_string_is_null` to equivalent, in
  which case the harness hides it by design.
- **Tier 1 count on the reject path**: unparsable `BIRTH_DT_TXT` rows are legacy row errors; loading them as NULL
  shows as a count excess.

## Findings recorded, not converted

- Match order (NINO exact, then Soundex+DOB fuzzy), survivorship ("most-recent wins except email longest wins"),
  `$$MATCH_CONFIDENCE_FLOOR`, and the suspect queue are only in `DESCRIPTION` text; no Lookup/Joiner/Router
  transformations are exported. The join logic in `converted.py` is INFERRED and the confidence threshold and queue
  cap are surfaced as pipeline configuration but not wired to any exported expression.
- NINO is masked here but read raw by the SAS fraud pipeline: a governance finding for section 9 (column mask in UC),
  not something the conversion changes.
- The mapplet's postcode logic named in its description (DQR-014 variant A) is not in the exported mapplet XML;
  it appears in `EXP_POSTCODE_DQ` of example 01 instead. Four postcode variants across engines is an assessment
  finding.
- The dependency on `wf_POLICY_MASTER_DAILY` is unmodelled everywhere; `converted.job.yml` shows the two options and
  leaves the choice to D5.

## Not verified live

- Source column names in `PARTY` and `CUSTOMERS`, and whether `NINO` is `CHAR(9)` (padding) or `VARCHAR`.
- Oracle hub's actual `''`-to-NULL behaviour for each string column (it is per-column type, `VARCHAR2` folds).
- Informatica `INITCAP` treatment of digits adjacent to letters (`4th`) - the rule used is the documented one, not
  observed.
- `SOUNDEX` of non-ASCII surnames under the `Latin1` codepage.
