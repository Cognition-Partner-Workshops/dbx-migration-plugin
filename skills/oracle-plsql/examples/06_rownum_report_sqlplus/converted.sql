-- Converted: fixture 14_rpt_policy_page.sql (SQL*Plus paged spool report) -> Databricks SQL, run as a Lakeflow Jobs
-- sql_task with job parameters. Track: analytical (Tier 4 report). Rules: SKILL.md §5 #68, #72, #44, #45; §6 SQL*Plus
-- row; §7 traps 7, 17, 5, 1.
--
-- SQL*Plus directives and their disposition (§7 trap 17):
--   SET PAGESIZE/LINESIZE/TRIMSPOOL/FEEDBACK/VERIFY/HEADING/ECHO  -> none (formatting; the consumer reads a file/table, not a terminal)
--   SET DEFINE '&' / DEFINE page_size / &1 &2 positional args     -> job parameters [jobs:SKILL.md#Job Parameters] passed as sql_task
--                                                                    `parameters` [jobs:references/task-types.md#SQL Task]; referenced in
--                                                                    the file as named parameter markers :page_size, :page_no, :as_of
--                                                                    [docs:sql-ref-parameter-marker]
--   WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK                   -> a failed sql_task fails the run; nothing to roll back (single SELECT)
--   WHENEVER OSERROR EXIT FAILURE                                  -> none (no OS interaction)
--   SPOOL &spool_file / SPOOL OFF                                  -> the report lands in a Delta table (below) or, when a
--                                                                    file is contractually required, a Volume path written by the
--                                                                    same task; the file layout is a Tier 4 contract
--   EXIT SUCCESS                                                   -> task success
--
-- ROWNUM sandwich (§5 #68, §7 trap 7): Oracle assigns ROWNUM after the inner ORDER BY, so the page is stable only
-- because the inner query is ordered on a total key (expiry_dt, policy_no). Databricks: ORDER BY ... LIMIT ... OFFSET
-- [docs:sql-ref-syntax-qry-select-limit], [docs:sql-ref-syntax-qry-select-offset]; the ORDER BY must be on the same
-- total key or pages overlap/skip rows.

CREATE TABLE IF NOT EXISTS ${catalog}.rpt.policy_page (
  page_no        INT,
  rn             BIGINT,
  policy_no      STRING,
  party_name     STRING,
  postcode       STRING,        -- Oracle CHAR(8): trailing blanks trimmed here; Tier 4 consumers of fixed-width spools need rpad(postcode, 8)
  cover_note_ref STRING,
  broker_refs    STRING,
  inception_dt   STRING,        -- report renders text; same formats as the spool
  expiry_dt      STRING,
  premium        STRING,
  run_ts         TIMESTAMP
);

INSERT INTO ${catalog}.rpt.policy_page
SELECT :page_no                                                            AS page_no,
       row_number() OVER (ORDER BY q.expiry_dt, q.policy_no)               AS rn,          -- ROWNUM over the ordered inner query
       q.policy_no,
       q.party_name,
       rtrim(q.postcode)                                                    AS postcode,    -- §7 trap 5
       q.cover_note_ref,
       q.broker_refs,
       upper(date_format(q.inception_dt, 'dd-MMM-yyyy'))                   AS inception_dt,   -- TO_CHAR(d,'DD-MON-YYYY'): Oracle MON is upper-case §5 #45
       date_format(q.expiry_dt, 'yyyy-MM-dd HH:mm')                        AS expiry_dt,      -- TO_CHAR(d,'YYYY-MM-DD HH24:MI')
       cast(cast(q.annual_premium AS DECIMAL(18,2)) AS STRING)             AS premium,        -- TO_CHAR(n,'FM999999990.00'): no FM modifier in Databricks to_char, so cast instead §5 #44
       current_timestamp()                                                  AS run_ts
  FROM (
        SELECT p.policy_no,
               nvl(pt.org_name, concat(pt.surname, ', ', coalesce(pt.forename, '')))  AS party_name,   -- Oracle || treats NULL as '' (§7 trap 23) -> see note
               pt.postcode,
               coalesce(p.cover_note_ref, 'NONE')                          AS cover_note_ref,
               (SELECT listagg(b.broker_ref, ';') WITHIN GROUP (ORDER BY b.broker_ref)      -- §5 #72 [docs:functions/listagg]
                  FROM ${catalog}.poladm.broker b
                 WHERE b.broker_id = p.broker_id)                          AS broker_refs,
               p.inception_dt,
               p.expiry_dt,
               p.annual_premium
          FROM ${catalog}.poladm.policy p
          JOIN ${catalog}.poladm.party  pt ON pt.party_id = p.party_id
         WHERE p.policy_status = 'LIVE'
           AND (p.cover_note_ref IS NULL OR p.cover_note_ref = '')          -- Oracle: IS NULL alone catches '' inserts; Databricks needs both (§7 trap 1)
           AND p.inception_dt <= to_date(:as_of, 'yyyy-MM-dd')              -- TO_DATE('&as_of','YYYY-MM-DD') §5 #46
         ORDER BY p.expiry_dt, p.policy_no
         LIMIT  :page_size
         OFFSET :page_size * (:page_no - 1)
       ) q;

-- Concatenation caveat (§7 trap 23): Oracle `surname || ', ' || forename` treats NULL as '' and yields 'Smith, ';
-- Databricks `concat` returns NULL if any argument is NULL [docs:functions/concat], so each nullable operand is wrapped
-- in coalesce(x, '') to reproduce the spool byte-for-byte (forename is nullable in fixture/02_tbl_party_broker.sql).
-- `concat_ws(', ', surname, forename)` [docs:functions/concat_ws] ignores NULL operands and drops the separator ('Smith'),
-- which is a Tier 4 text difference: only use it when the consumer agrees, recorded in 06_decisions.md.
