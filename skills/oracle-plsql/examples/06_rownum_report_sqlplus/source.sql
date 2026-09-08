-- Object class: SQL*Plus SCRIPT (report unit). Census key: POLADM.RPT_POLICY_PAGE (file)
-- SQL*Plus directives (SET, DEFINE, COLUMN, SPOOL, WHENEVER, substitution variables, EXIT),
-- classic ROWNUM pagination (nested-subquery form, ORDER BY must be inside), '' -> NULL trap,
-- CHAR padding on postcode, LISTAGG, NVL vs COALESCE, and TO_CHAR date formatting.

SET PAGESIZE 0 LINESIZE 400 TRIMSPOOL ON FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
SET DEFINE '&'
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
WHENEVER OSERROR  EXIT FAILURE

DEFINE page_size = 100
DEFINE page_no   = &1
DEFINE as_of     = &2      -- passed as 'YYYY-MM-DD'

COLUMN policy_no   FORMAT A20
COLUMN premium     FORMAT 999,999,990.00
COLUMN spool_name  NEW_VALUE spool_file NOPRINT
SELECT 'policy_page_' || TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS') || '.lst' AS spool_name FROM dual;

SPOOL &spool_file

SELECT policy_no,
       party_name,
       postcode,                                     -- CHAR(8): trailing blanks in the spool
       cover_note_ref,                               -- NULL for every row the app wrote as ''
       broker_refs,
       TO_CHAR(inception_dt, 'DD-MON-YYYY')          AS inception_dt,
       TO_CHAR(expiry_dt,    'YYYY-MM-DD HH24:MI')   AS expiry_dt,
       TO_CHAR(annual_premium, 'FM999999990.00')     AS premium,
       rn
  FROM (
        SELECT q.*, ROWNUM AS rn                     -- ROWNUM assigned AFTER the inner ORDER BY
          FROM (
                SELECT p.policy_no,
                       NVL(pt.org_name, pt.surname || ', ' || pt.forename)          AS party_name,
                       pt.postcode,
                       COALESCE(p.cover_note_ref, 'NONE')                            AS cover_note_ref,
                       (SELECT LISTAGG(b.broker_ref, ';') WITHIN GROUP (ORDER BY b.broker_ref)
                          FROM poladm.broker b
                         WHERE b.broker_id = p.broker_id)                            AS broker_refs,
                       p.inception_dt,
                       p.expiry_dt,
                       p.annual_premium
                  FROM poladm.policy p
                  JOIN poladm.party  pt ON pt.party_id = p.party_id
                 WHERE p.policy_status = 'LIVE'
                   AND p.cover_note_ref IS NULL                                     -- catches '' inserts too
                   AND p.inception_dt <= TO_DATE('&as_of', 'YYYY-MM-DD')
                 ORDER BY p.expiry_dt, p.policy_no
               ) q
         WHERE ROWNUM <= &page_size * &page_no        -- upper bound must be on the middle query
       )
 WHERE rn > &page_size * (&page_no - 1);

SPOOL OFF
EXIT SUCCESS
