# 01 — Albion `pkg_policy_inquiry`: package + `SYS_REFCURSOR` -> SQL table functions

**Source**: `albion-insurance-data-estate/api_legacy/plsql/pkg_policy_inquiry.sql` (read-only; body only, no spec in the
repo). Chain per `docs/architecture_overview.md`: GoldenGate replica -> Oracle ODS -> this package -> SOAP
`PolicyInquiryService` (26h-stale data presented as real-time).

**Profile / track**: warehouse-ODS read path, analytical DBSQL (SKILL.md §3 "shared unit": the ODS tables are the
unit; the package is a consumer contract). Lakebase variant noted inline for an OLTP repoint.

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| `CREATE OR REPLACE PACKAGE BODY` with two public functions | schema `pkg_policy_inquiry` + two `CREATE FUNCTION ... RETURNS TABLE` | §6 package -> schema namespace; read-only `SYS_REFCURSOR` function -> SQL table function |
| `l_cur SYS_REFCURSOR; OPEN l_cur FOR SELECT ...; RETURN l_cur` | `RETURN SELECT ...` body | §6 `SYS_REFCURSOR` result contract; §7 trap 12 |
| `NVL(TO_CHAR(p.legacy_customer_id), p.party_id)` | `nvl(cast(... AS STRING), ...)` | §5 #1, #44 (`TO_CHAR(number)` no format = default integer text) |
| `UPPER(TRIM(x))`, nested `REPLACE` | same names, same semantics | §5 #17, #23, #25 |
| `TO_CHAR(c.legacy_client_no) = p_party_ref` | explicit `cast(col AS STRING)` kept on the column side | §5 #51 implicit conversion direction |
| heap-ordered cursor | `ORDER BY` added on the natural key | §5 #67 / §3 recon requires a total order |

Not converted (out of scope of the unit, recorded as inventory gaps by `round_trip.py`): `ods_policy_360`,
`ods_claims` DDL is not in the estate repo (nodes `not-in-census`); the `TERADATA.STG_POLICY_360 -> ODS_POLICY_360`
replication edge is INFERRED `risk=freshness`.

## Recon tier that catches a wrong conversion

- **Tier 4** (report/output): the SOAP contract is the column list. Dropping `customer_ref`'s alias, changing
  `annual_premium_gbp`'s scale, or emitting `loss_dt` as text instead of `DATE` breaks consumers that parse the
  prefix / the `DD/MM/YYYY` rendering.
- **Tier 3** (row-level on the projected result, keyed by `policy_no` / `claim_no`): a missing second `REPLACE`
  (`'/' -> '-'`) makes `AL/0001234`-style lookups return zero rows on the target and one on Oracle.
- **Tier 2**: `COUNT(DISTINCT policy_no)` per `product_cd` over the function's output for a sample of bordereaux-style
  refs detects the same normalisation loss at aggregate level.
- **Tier 1** cannot see this unit: it has no table of its own.

## Canonicalization used

`empty_string_is_null` on all `STRING` columns (Albion's `postcode_dq_status` / `policy_status` arrive as `''` from
some feeds), `decimal_round` (`annual_premium_gbp`, `incurred_amt`, `paid_amt` are `NUMBER` without scale upstream),
`datetime_utc_truncate_ms` for `loss_dt` if it lands as `TIMESTAMP_NTZ` instead of `DATE`.

## Not verified live

Function creation/`SELECT * FROM fn(...)` on DBSQL; the SOAP layer's driver-level handling of a table function vs a
ref cursor; Lakebase `RETURNS TABLE` variant.
