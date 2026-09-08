# Oracle fixture estate (synthetic)

Fifteen files, ~30 catalog objects, two schemas: `POLADM` (OLTP, Lakebase track) and `ODS`
(analytical, Delta/DBSQL track). Everything is static SQL/PL-SQL text so the enumeration and
lineage rules in `../../SKILL.md` §1-2 can be exercised without a live engine
(`python3 ../round_trip.py` runs them). No customer data; all names are invented.

| File | Objects | Section 4-7 constructs carried |
|---|---|---|
| `01_seq_policy.sql` | `POLICY_SEQ`, `AUDIT_SEQ` | sequence `CACHE`/`NOORDER` gaps |
| `02_tbl_party_broker.sql` | `PARTY`, `BROKER` | `CHAR` padding, `NVARCHAR2`, `DATE` DOB, self-FK hierarchy, `NUMBER` no scale |
| `03_tbl_policy.sql` | `POLICY` | `NUMBER` without scale x3, `''`-is-NULL column, `CLOB`, `DEFAULT SYSDATE`/`USER` |
| `04_tbl_premium_txn.sql` | `PREMIUM_TXN` (interval-partitioned) | `DATE` with time, `TIMESTAMP WITH [LOCAL] TIME ZONE`, `INTERVAL DAY TO SECOND`, `RAW(16)`, `BINARY_DOUBLE` |
| `05_tbl_policy_audit_log.sql` | `POLICY_AUDIT_LOG` | `SYS_CONTEXT` defaults, `TIMESTAMP(6)` |
| `06_prc_log_event.sql` | `PRC_LOG_EVENT` | `PRAGMA AUTONOMOUS_TRANSACTION`, `NEXTVAL` in DML, swallowed `WHEN OTHERS` |
| `07_trg_policy_biu.sql` | `TRG_POLICY_BIU` | `:NEW`/`:OLD`, `INSERTING`/`UPDATING`, `NEXTVAL`, `= ''` dead branch, calls logger |
| `08_pkg_policy_renewal.sql` | `PKG_POLICY_RENEWAL` spec+body | package state, explicit cursor `FOR UPDATE SKIP LOCKED`, `BULK COLLECT LIMIT`, `FORALL`, `SAVEPOINT`, `RAISE_APPLICATION_ERROR`, `EXECUTE IMMEDIATE`, `SYS_REFCURSOR`, `ADD_MONTHS`, `DBMS_OUTPUT` |
| `09_mrg_policy_from_stg.sql` | `STG_POLICY_FEED` (GTT), `MERGE` | `NEXTVAL` in `MERGE INSERT`, `DELETE WHERE`, `NULLIF(TRIM(x),'')`, duplicate-source-key risk |
| `10_qry_broker_hierarchy.sql` | `V_BROKER_HIERARCHY` | `CONNECT BY NOCYCLE PRIOR`, `LEVEL`, `SYS_CONNECT_BY_PATH`, `CONNECT_BY_ROOT`, `ORDER SIBLINGS BY`, `PRIOR` in select list, `PUBLIC` synonym |
| `11_qry_premium_pivot.sql` | `V_PREMIUM_BY_TXN_TYPE` | `PIVOT` two aggregates + aliases, `TRUNC(date,'MM')` |
| `12_mv_policy_premium_summary.sql` | 2 MV logs, `MV_POLICY_PREMIUM_SUMMARY` | `REFRESH FAST ON DEMAND`, aggregate MV eligibility rules, `ENABLE QUERY REWRITE` |
| `13_job_nightly_renewal.sql` | `PRG_NIGHTLY_RENEWAL`, `JOB_NIGHTLY_RENEWAL` | `DBMS_SCHEDULER` calendar string in `Europe/London`, `max_failures`, email notification, `DBMS_MVIEW.REFRESH` |
| `14_rpt_policy_page.sql` | `RPT_POLICY_PAGE` (SQL*Plus) | `SET`/`DEFINE`/`SPOOL`/`WHENEVER`/`&var`, `ROWNUM` pagination, `LISTAGG`, `TO_CHAR` formats, `COALESCE` vs `NVL` |
| `15_syn_dblink_grants.sql` | synonyms, db link, remote view, roles, grants, VPD, redaction | `PUBLIC` grant, column-level grant, `@claims_link` external ref, `DBMS_RLS`, `DBMS_REDACT` |

Reading order for a human: 01 → 05 (storage), 06 → 08 (procedural), 09 → 12 (set-based /
analytical), 13 → 15 (orchestration and governance).
