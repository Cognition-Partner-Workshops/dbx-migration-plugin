-- Converted: fixture 10_qry_broker_hierarchy.sql (CONNECT BY view over a PUBLIC synonym) -> Databricks SQL view.
-- Track: analytical DBSQL (ODS view). Rules: SKILL.md §5 #81, #82, §7 trap 6, trap 18.
-- Recursive CTE per [dbsql:sql-scripting.md#Recursive CTEs]: anchor = START WITH rows, recursive member = CONNECT BY,
-- UNION ALL required, default depth 100 (MAX RECURSION LEVEL set explicitly from the census-measured depth),
-- not usable inside UPDATE/DELETE/MERGE, so it stays a view.
-- Synonym BROKER is resolved at conversion time to its base object POLADM.BROKER (§7 trap 18; UC has no synonyms).

CREATE OR REPLACE VIEW ${catalog}.ods.v_broker_hierarchy AS
WITH RECURSIVE h (
  depth, region_ref, broker_id, parent_broker_id, broker_ref, broker_name, tier_cd,
  path_refs, effective_commission_pct, parent_commission_pct, path_ids, is_cycle, sort_key
) MAX RECURSION LEVEL 20 AS (
  -- START WITH b.parent_broker_id IS NULL
  SELECT 1                                          AS depth,
         b.broker_ref                               AS region_ref,          -- CONNECT_BY_ROOT broker_ref
         b.broker_id, b.parent_broker_id, b.broker_ref, b.broker_name, b.tier_cd,
         b.broker_ref                               AS path_refs,           -- LTRIM(SYS_CONNECT_BY_PATH(ref,'/'),'/')
         b.commission_pct                           AS effective_commission_pct,   -- PRIOR is NULL at the root
         cast(NULL AS DECIMAL(38,10))               AS parent_commission_pct,
         array(b.broker_id)                         AS path_ids,            -- for NOCYCLE detection
         0                                          AS is_cycle,
         lpad(b.broker_name, 200, ' ')              AS sort_key             -- ORDER SIBLINGS BY broker_name
    FROM ${catalog}.poladm.broker b
   WHERE b.parent_broker_id IS NULL
  UNION ALL
  -- CONNECT BY NOCYCLE PRIOR b.broker_id = b.parent_broker_id AND b.active_flag = 'Y'
  SELECT h.depth + 1,
         h.region_ref,
         b.broker_id, b.parent_broker_id, b.broker_ref, b.broker_name, b.tier_cd,
         concat(h.path_refs, '/', b.broker_ref),
         nvl(b.commission_pct, h.effective_commission_pct),                  -- NVL(col, PRIOR col): §5 #82
         h.effective_commission_pct,
         array_append(h.path_ids, b.broker_id),
         CASE WHEN array_contains(h.path_ids, b.broker_id) THEN 1 ELSE 0 END,
         concat(h.sort_key, '|', lpad(b.broker_name, 200, ' '))
    FROM h
    JOIN ${catalog}.poladm.broker b
      ON b.parent_broker_id = h.broker_id
     AND b.active_flag = 'Y'
   WHERE NOT array_contains(h.path_ids, b.broker_id)                         -- NOCYCLE: stop expanding, keep the row that closed the loop
)
SELECT h.depth,
       h.region_ref,
       h.broker_id,
       h.parent_broker_id,
       h.broker_ref,
       h.broker_name,
       h.tier_cd,
       h.path_refs,
       -- CONNECT_BY_ISLEAF: 1 when no active child exists (computed after the walk, not during it)
       CASE WHEN EXISTS (SELECT 1 FROM ${catalog}.poladm.broker c
                          WHERE c.parent_broker_id = h.broker_id AND c.active_flag = 'Y')
            THEN 0 ELSE 1 END                        AS is_leaf,
       h.is_cycle,
       h.effective_commission_pct,
       h.sort_key                                    -- exposed so consumers can reproduce ORDER SIBLINGS BY; a view has no order (§7 trap 6)
  FROM h;

-- Oracle-side row for CONNECT_BY_ISCYCLE: Oracle emits the row whose child would repeat with ISCYCLE=1 and does not
-- descend; the WHERE NOT array_contains() filter above drops that repeating child instead, so is_cycle is always 0
-- unless the anchor itself is on a loop. Tier 3 must therefore compare with is_cycle excluded or the rule agreed in
-- 06_decisions.md (§7 trap 6 "cycles").
--
-- Lakebase variant: identical shape with Postgres `WITH RECURSIVE ... ` [pg17:queries-with] (`CYCLE broker_id SET
-- is_cycle USING path_ids` replaces the manual array bookkeeping; `array_append` -> `||`, `array_contains(a, x)`
-- -> `x = ANY(a)`). Postgres has no MAX RECURSION LEVEL: bound depth with `WHERE depth < 20` in the recursive member.
