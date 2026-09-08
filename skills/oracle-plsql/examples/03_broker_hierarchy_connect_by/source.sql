-- Object class: VIEW (hierarchical). Census key: ODS.V_BROKER_HIERARCHY
-- CONNECT BY PRIOR with LEVEL, SYS_CONNECT_BY_PATH, CONNECT_BY_ROOT, CONNECT_BY_ISLEAF,
-- NOCYCLE and ORDER SIBLINGS BY. Reads POLADM.BROKER through the PUBLIC synonym BROKER.

CREATE OR REPLACE VIEW ods.v_broker_hierarchy AS
SELECT LEVEL                                          AS depth,
       CONNECT_BY_ROOT broker_ref                     AS region_ref,
       b.broker_id,
       b.parent_broker_id,
       b.broker_ref,
       b.broker_name,
       b.tier_cd,
       LTRIM(SYS_CONNECT_BY_PATH(b.broker_ref, '/'), '/') AS path_refs,
       CONNECT_BY_ISLEAF                              AS is_leaf,
       CONNECT_BY_ISCYCLE                             AS is_cycle,
       NVL(b.commission_pct, PRIOR b.commission_pct)  AS effective_commission_pct  -- inherit from parent
  FROM broker b                                        -- PUBLIC synonym -> POLADM.BROKER
 START WITH b.parent_broker_id IS NULL
CONNECT BY NOCYCLE PRIOR b.broker_id = b.parent_broker_id
       AND b.active_flag = 'Y'
 ORDER SIBLINGS BY b.broker_name;

CREATE OR REPLACE PUBLIC SYNONYM broker FOR poladm.broker;              -- consumed by ods.v_broker_hierarchy
