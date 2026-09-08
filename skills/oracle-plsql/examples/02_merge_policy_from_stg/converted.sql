-- Converted: fixture 09_mrg_policy_from_stg.sql (GTT + MERGE with DELETE WHERE + NEXTVAL) -> Databricks SQL.
-- Track: analytical Delta (the POLADM.POLICY copy in the migration catalog). Lakebase variant at the bottom.
-- Rules: SKILL.md §5 #83 (MERGE), #87 (NEXTVAL), #6/#25 (NULLIF/TRIM), §7 trap 8, trap 1, trap 3, §2 trigger fan-out.
--
-- Trigger fan-out (census edge 09_MRG_POLICY_FROM_STG -> TRG_POLICY_BIU -> PRC_LOG_EVENT): poladm.policy carries a BEFORE
-- INSERT OR UPDATE trigger (fixture 07). Delta has no triggers, so its per-row effects are folded in below:
--   INSERT rows : policy_id (sequence -> identity), created_dt/created_by, row_version = 1, policy_no normalised (the feed
--                 already is), '' cover_note_ref -> NULL (nullif already), active_policy_flag, one 'INSERT' audit row.
--   UPDATE rows : updated_dt/updated_by, row_version + 1, active_policy_flag recomputed from the NEW status/expiry, one
--                 'UPDATE' audit row when status or premium changed. Oracle applies the UPDATE before DELETE WHERE, so
--                 rows the 'D' branch removes still fire the UPDATE trigger and their autonomous audit row survives.
--   DELETE rows : nothing; the trigger has no DELETE event, so no audit row is written for the delete itself.

-- GTT ON COMMIT PRESERVE ROWS -> session-scoped temporary table [dbsql:materialized-views-pipes.md#Temporary Tables]
-- (CREATE OR REPLACE TEMP TABLE is not supported; DROP first when re-running in the same session)
CREATE TEMPORARY TABLE stg_policy_feed (
  policy_no        STRING,
  party_id         STRING,
  broker_ref       STRING,
  product_cd       STRING,
  policy_status    STRING,
  inception_dt     TIMESTAMP_NTZ,      -- Oracle DATE (may carry a time) §4
  expiry_dt        TIMESTAMP_NTZ,
  annual_premium   DECIMAL(38,10),     -- NUMBER without scale §7 trap 2
  cover_note_ref   STRING,
  feed_action      STRING              -- CHAR(1): rstrip_spaces on load
);

-- Oracle MERGE evaluation order: WHEN MATCHED UPDATE (WHERE) then DELETE WHERE on the *updated* row;
-- Databricks evaluates WHEN clauses top-down and takes the first match [docs:delta-merge-into],
-- so the DELETE branch must come first and carry its own predicate.
--
-- Duplicate source keys: Oracle raises ORA-30926 and the whole MERGE rolls back; Databricks raises
-- DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE only when the duplicate keys also match a *target* row, and
-- inserts both rows when they do not. The failure contract is preserved with an explicit pre-check that fails the
-- unit on any duplicate normalised key (assert_true raises USER_RAISED_EXCEPTION [docs:functions/assert_true]);
-- picking one row with QUALIFY would turn Oracle's all-or-nothing failure into a silent, order-dependent mutation
-- (§7 trap 8). If the business wants the feed deduped instead, that is a 06_decisions.md row with a stated ORDER BY.

-- The MERGE's USING subquery, materialised once so the pre-check, the :OLD snapshot, the MERGE and the audit INSERT see
-- the same rows (a temp view is re-executed on every access [dbsql:materialized-views-pipes.md#Temporary Tables vs Temporary Views]).
DROP TABLE IF EXISTS stg_policy_src;
CREATE TEMPORARY TABLE stg_policy_src AS
  SELECT replace(upper(trim(s.policy_no)), 'AL/', 'ALB-')  AS policy_no,   -- same normalisation the trigger applies to :NEW.policy_no
         s.party_id,
         b.broker_id,
         s.product_cd,
         s.policy_status,
         date_trunc('DAY', s.inception_dt)                   AS inception_dt,   -- TRUNC(date) §5 #61
         s.expiry_dt,
         s.annual_premium,
         nullif(trim(s.cover_note_ref), '')                  AS cover_note_ref, -- '' must become NULL explicitly on Databricks (§7 trap 1)
         rtrim(s.feed_action)                                AS feed_action
    FROM stg_policy_feed s
    LEFT JOIN ${catalog}.poladm.broker b ON b.broker_ref = s.broker_ref;      -- broker_ref is UNIQUE (02_tbl_party_broker.sql), so no fan-out

SELECT assert_true(count(*) = 0,
                   concat('ORA-30926 parity: ', count(*), ' duplicate policy_no key(s) in stg_policy_feed'))
  FROM (SELECT policy_no FROM stg_policy_src GROUP BY 1 HAVING count(*) > 1);

-- :OLD image of the target rows the feed touches, taken before the MERGE: TRG_POLICY_BIU decides whether to log by
-- comparing :OLD.policy_status / :OLD.annual_premium with :NEW, and its WHERE row_version >= 1 gate decides whether
-- the UPDATE (and so the trigger) ran at all. policy_id is kept so rows deleted by the 'D' branch can still be logged.
DROP TABLE IF EXISTS policy_pre_image;
CREATE TEMPORARY TABLE policy_pre_image AS
  SELECT t.policy_id, t.policy_no, t.policy_status, t.annual_premium, t.row_version
    FROM ${catalog}.poladm.policy t
   WHERE t.policy_no IN (SELECT policy_no FROM stg_policy_src);

MERGE INTO ${catalog}.poladm.policy AS tgt
USING stg_policy_src AS src
ON tgt.policy_no = src.policy_no
WHEN MATCHED AND src.feed_action = 'D' AND tgt.row_version >= 1 THEN
  DELETE                                                                       -- no DELETE trigger on poladm.policy: no side effects
WHEN MATCHED AND tgt.row_version >= 1 THEN
  UPDATE SET tgt.policy_status      = src.policy_status,
             tgt.annual_premium     = src.annual_premium,
             tgt.expiry_dt          = src.expiry_dt,
             tgt.broker_id          = nvl(src.broker_id, tgt.broker_id),
             tgt.cover_note_ref     = src.cover_note_ref,
             -- TRG_POLICY_BIU, UPDATING branch (§2 trigger row; example 04). policy_no re-normalisation is a no-op here:
             -- the ON clause already equates tgt.policy_no with the normalised feed key.
             tgt.row_version        = tgt.row_version + 1,                      -- NVL(:OLD.row_version, 0) + 1; >= 1 here
             tgt.updated_dt         = current_timestamp(),                     -- SYSDATE
             tgt.updated_by         = current_user(),                          -- SYS_CONTEXT('USERENV','SESSION_USER')
             -- :NEW.active_policy_flag from the NEW status and NEW expiry; inception_dt is not updated by this MERGE
             tgt.active_policy_flag = CASE WHEN src.policy_status = 'LIVE'
                                            AND current_date() BETWEEN to_date(tgt.inception_dt) AND to_date(src.expiry_dt)
                                           THEN 'Y' ELSE 'N' END
WHEN NOT MATCHED AND src.feed_action <> 'D' THEN
  INSERT (policy_no, party_id, broker_id, product_cd, policy_status,
          inception_dt, expiry_dt, annual_premium, cover_note_ref,
          active_policy_flag, row_version, created_dt, created_by)
  VALUES (src.policy_no, src.party_id, src.broker_id, src.product_cd, src.policy_status,
          src.inception_dt, src.expiry_dt, src.annual_premium, src.cover_note_ref,
          CASE WHEN src.policy_status = 'LIVE'
                AND current_date() BETWEEN to_date(src.inception_dt) AND to_date(src.expiry_dt)
               THEN 'Y' ELSE 'N' END,
          1, current_timestamp(), current_user());
-- policy_id: poladm.policy_seq.NEXTVAL is not listable in a Databricks INSERT; the analytical copy declares
-- policy_id BIGINT GENERATED ALWAYS AS IDENTITY [docs:sql-ref-syntax-ddl-create-table-using] and Tier 3 keys on
-- policy_no (the business key), never on policy_id (§7 trap 10).

-- TRG_POLICY_BIU -> prc_log_event: fires for every INSERTING row and for UPDATING rows whose status or premium changed
-- (NVL(:OLD.policy_status,'~') <> :NEW.policy_status OR NVL(:OLD.annual_premium,-1) <> :NEW.annual_premium).
-- audit_id = audit_seq.NEXTVAL -> identity on the Delta copy. Inserted rows take their identity policy_id from the
-- post-MERGE table; updated-then-deleted rows take it from the pre-image (the row is gone). PRAGMA AUTONOMOUS_TRANSACTION
-- is not reproduced: these rows commit with the MERGE, not independently of it (accepted difference, example 04).
INSERT INTO ${catalog}.poladm.policy_audit_log
  (policy_id, event_cd, old_status, new_status, old_premium, new_premium, event_ts, session_user)
SELECT coalesce(pre.policy_id, p.policy_id),
       CASE WHEN pre.policy_no IS NULL THEN 'INSERT' ELSE 'UPDATE' END,
       pre.policy_status, src.policy_status, pre.annual_premium, src.annual_premium,
       current_timestamp(), current_user()
  FROM stg_policy_src src
  LEFT JOIN policy_pre_image pre     ON pre.policy_no = src.policy_no
  LEFT JOIN ${catalog}.poladm.policy p ON p.policy_no = src.policy_no AND pre.policy_no IS NULL   -- rows this MERGE inserted
 WHERE (pre.policy_no IS NULL AND src.feed_action <> 'D')                     -- INSERTING: always logged
    OR (pre.row_version >= 1                                                  -- UPDATING ran (incl. rows the 'D' branch then deleted)
        AND (coalesce(pre.policy_status, '~') <> src.policy_status
             OR coalesce(pre.annual_premium, -1) <> src.annual_premium));

-- No COMMIT: each statement is its own Delta transaction. The pre-check, the pre-image, the MERGE and the audit INSERT
-- are separate statements, so a row inserted into the temp table between them is not covered; run them inside one
-- sql_task, or wrap GTT load + pre-check + pre-image + MERGE + audit INSERT in BEGIN ATOMIC ... END
-- [dbsql:sql-scripting.md#SQL Scripting Atomic Blocks] (catalogManaged tables required) when the load and the MERGE
-- must be one unit as in Oracle (the Oracle trigger's autonomous audit rows would still differ on a failed run).

-- ---------------------------------------------------------------------------------------------
-- Lakebase (OLTP profile) variant, PostgreSQL 17 syntax [pg17:sql-merge]:
--   CREATE TEMP TABLE stg_policy_feed (...) ON COMMIT PRESERVE ROWS;   -- same clause exists in Postgres
--   MERGE INTO poladm.policy tgt USING (...same src, no dedupe...) ON tgt.policy_no = src.policy_no
--   WHEN MATCHED AND src.feed_action = 'D' THEN DELETE
--   WHEN MATCHED THEN UPDATE SET ...
--   WHEN NOT MATCHED AND src.feed_action <> 'D' THEN
--     INSERT (policy_id, ...) VALUES (nextval('poladm.policy_seq'), ...);   -- sequence survives as-is (§4, [pg17:sql-createsequence])
--   Postgres raises on duplicate source keys that hit one target row ("MERGE command cannot affect row a second
--   time" [pg17:sql-merge]) and aborts the transaction, which is the Oracle contract; keep the same pre-check
--   (RAISE EXCEPTION in a DO block or the calling function) so duplicates that do NOT match a target row also fail
--   instead of inserting twice. The BEFORE trigger from example 04 fires here, so row_version / updated_* /
--   active_policy_flag are NOT set in the MERGE and the pre-image + audit INSERT above are NOT reproduced on Lakebase.
