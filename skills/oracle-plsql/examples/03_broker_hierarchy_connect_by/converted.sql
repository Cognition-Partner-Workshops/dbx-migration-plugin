-- Converted: fixture 10_qry_broker_hierarchy.sql (CONNECT BY view over a PUBLIC synonym) -> Databricks SQL view.
-- Track: analytical DBSQL (ODS view). Rules: SKILL.md §5 #81, #82, §7 trap 6, trap 18.
-- Recursive CTE per [dbsql:sql-scripting.md#Recursive CTEs]: anchor = START WITH rows, recursive member = CONNECT BY,
-- UNION ALL required, default depth 100 (MAX RECURSION LEVEL set explicitly from the census-measured depth),
-- not usable inside UPDATE/DELETE/MERGE, so it stays a view.
-- Synonym BROKER is resolved at conversion time to its base object POLADM.BROKER (§7 trap 18; UC has no synonyms).

CREATE OR REPLACE VIEW ${catalog}.ods.v_broker_hierarchy AS
WITH RECURSIVE h (
  depth, region_ref, broker_id, parent_broker_id, broker_ref, broker_name, tier_cd,
  path_refs, commission_pct, effective_commission_pct, path_ids, sort_key
) MAX RECURSION LEVEL 20 AS (
  -- START WITH b.parent_broker_id IS NULL
  SELECT 1                                          AS depth,
         b.broker_ref                               AS region_ref,          -- CONNECT_BY_ROOT broker_ref
         b.broker_id, b.parent_broker_id, b.broker_ref, b.broker_name, b.tier_cd,
         b.broker_ref                               AS path_refs,           -- LTRIM(SYS_CONNECT_BY_PATH(ref,'/'),'/')
         b.commission_pct                           AS commission_pct,       -- the row's own (raw) value, what PRIOR reads one level down
         b.commission_pct                           AS effective_commission_pct,   -- NVL(col, PRIOR col): PRIOR is NULL at the root
         array(b.broker_id)                         AS path_ids,            -- for NOCYCLE detection
         lpad(b.broker_name, 200, ' ')              AS sort_key             -- ORDER SIBLINGS BY broker_name
    FROM ${catalog}.poladm.broker b
   WHERE b.parent_broker_id IS NULL
  UNION ALL
  -- CONNECT BY NOCYCLE PRIOR b.broker_id = b.parent_broker_id AND b.active_flag = 'Y'
  SELECT h.depth + 1,
         h.region_ref,
         b.broker_id, b.parent_broker_id, b.broker_ref, b.broker_name, b.tier_cd,
         concat(h.path_refs, '/', b.broker_ref),
         b.commission_pct,
         -- NVL(b.commission_pct, PRIOR b.commission_pct): PRIOR reads the parent's RAW column, not the parent's already
         -- derived effective value, so a NULL parent under a non-NULL grandparent yields NULL here, as in Oracle (§5 #82)
         nvl(b.commission_pct, h.commission_pct),
         array_append(h.path_ids, b.broker_id),
         concat(h.sort_key, '|', lpad(b.broker_name, 200, ' '))
    FROM h
    JOIN ${catalog}.poladm.broker b
      ON b.parent_broker_id = h.broker_id
     AND b.active_flag = 'Y'
   WHERE NOT array_contains(h.path_ids, b.broker_id)                         -- NOCYCLE: do not descend into an ancestor again
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
       -- CONNECT_BY_ISCYCLE: 1 when the row has an active child that is also one of its ancestors (the row Oracle
       -- emits and stops at under NOCYCLE); the ancestor itself is not re-emitted, same as Oracle
       CASE WHEN EXISTS (SELECT 1 FROM ${catalog}.poladm.broker c
                          WHERE c.parent_broker_id = h.broker_id AND c.active_flag = 'Y'
                            AND array_contains(h.path_ids, c.broker_id))
            THEN 1 ELSE 0 END                        AS is_cycle,
       h.effective_commission_pct,                   -- one level of inheritance only; see NOTE.md for the 3-level case
       h.sort_key                                    -- exposed so consumers can reproduce ORDER SIBLINGS BY; a view has no order (§7 trap 6)
  FROM h;

-- Cycle semantics (§7 trap 6): under NOCYCLE Oracle emits the row whose child is already its ancestor with
-- CONNECT_BY_ISCYCLE = 1 and does not descend into that child. The recursive member reproduces the "do not descend"
-- half (WHERE NOT array_contains), and is_cycle is computed after the walk on the parent row, so Tier 3 can compare
-- is_cycle directly. A cycle that starts at a root (root reachable from its own descendant) is flagged on the
-- deepest row of the loop, exactly as Oracle does, because the root is in path_ids from the anchor.
--
-- Lakebase variant: identical shape with Postgres `WITH RECURSIVE ... ` [pg17:queries-with] (`CYCLE broker_id SET
-- is_cycle USING path_ids` replaces the manual array bookkeeping; `array_append` -> `||`, `array_contains(a, x)`
-- -> `x = ANY(a)`). Postgres has no MAX RECURSION LEVEL: bound depth with `WHERE depth < 20` in the recursive member.
