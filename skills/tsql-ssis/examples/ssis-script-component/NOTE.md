# ssis-script-component: `InvestorFileExport.dtsx` — **hand-convert**

Source: `source.dtsx` — hand-written `.dtsx`-shaped description with an abridged C# Script Component body. Converted: `converted.md` — a hand-conversion *plan* (statement classification, target task graph, DBSQL export query, open points). There is no automatic conversion for this unit and the skill must never claim one (SKILL §6 row "SSIS Script Task / Script Component", §11 `hand_convert` heuristic).

## Constructs exercised
- **Script Component** (`Microsoft.ManagedComponentHost`, synchronous transformation, `ReadOnlyVariables`) with four behavior classes: culture-dependent formatting (pure expression), custom check-digit algorithm (pure expression), per-row HTTP call with in-memory cache (external side effect), silent row skip (row filter). Each is classified and routed separately in `converted.md` §1.
- **Flat File Destination** fixed-width, `CodePage=1252`, CRLF, no header -> `rpad`/`lpad` layout in SQL plus a delivery notebook that writes the encoded file to a UC Volume (SKILL §6 "SSIS Connection Managers": Flat File -> Volume path).
- **FTP Task** with `Sensitive=True` project parameter -> delivery notebook using a secret scope **by name**; `EncryptSensitiveWithPassword` recorded as a §9 finding; no sensitive value is read or reproduced.
- **OLE DB Source** with `?` bound to `$Package::InvestorCode` -> `:investor_code` named parameter (`[jobs:params]`).
- `EvaluateAsExpression` variable building `FileDate` (`YYYYMMDD`) -> `date_format(current_date(), 'yyyyMMdd')` in the delivery notebook (`[fn:date_format]`, `[docs:datetime]`).
- SSIS type codes `wstr` with `length`, Flat File `DataType="129"` (`DT_STR`) -> §4 rows `NVARCHAR`/`VARCHAR`.

## Lineage
FACT: reads `loan_servicing.dbo.loans`, `dbo.investors`; writes a file (`\\fileshare\investor_out\INV_<code>_<date>.txt`) and an SFTP endpoint. INFERRED (Script Component is inherently unparsed, SKILL §2b): the external rate service URL `$Project::RateServiceUrl` is a read edge that only the script reveals; the census records it as `external_call` with the connection manager list as candidate edges. Unit = package + the new `ref_index_rate` table + the export table + the delivery step; `ref_index_rate` is shared if other packages call the same service.

## Recon tier that catches a wrong conversion
- **Tier 3** keyed on `loan_number` between the source query replayed on Sybase/SQL Server (with the C# formatting reproduced in a harness fixture) and `investor_export_<code>`: catches check-digit weight/direction errors (block B) and culture-format errors (block A) column by column. This is the certification tier for hand-convert units.
- **Tier 2**: `sha2(concat_ws of the canonical file bytes)` on the delivered fixed-width file vs the source file for the same run date (`[fn:sha2]`); `count(*)` of dropped `current_upb <= 0` rows (block D) must equal the source rows that the script skipped — SSIS gave no count, the converted unit must.
- **Tier 1**: rows in `investor_export_<code>` = source rows with `loan_status='AC' AND current_upb > 0` for the investor.
- **Tier 4** replay is not applicable (no consumer query on the target); the consumer is the investor's file intake.
- Canonicalization: `rstrip_spaces` must be **off** for this unit — fixed-width padding is the contract; `collation_casefold` off (`loan_number` compares exactly in .NET).

## Not verified live
`split(..., '')` element behavior, `format_number` output for every `culture_name` value, and the Python delivery step need a warehouse and the real rate service. Listed in the PR body.

## Lakebridge
`--source-dialect ssis` emits a stub for the Script Component and nothing for the FTP Task (SKILL §10); everything in `converted.md` is hand-derived.
