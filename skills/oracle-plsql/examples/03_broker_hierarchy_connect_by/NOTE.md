# 03 — `CONNECT BY` hierarchy view over a `PUBLIC` synonym -> recursive CTE view

**Source**: `fixture/10_qry_broker_hierarchy.sql` + the `PUBLIC SYNONYM broker` line from `fixture/15_syn_dblink_grants.sql`.
`LEVEL`, `CONNECT_BY_ROOT`, `SYS_CONNECT_BY_PATH`, `CONNECT_BY_ISLEAF`, `CONNECT_BY_ISCYCLE`, `NOCYCLE`,
`PRIOR` in the select list, `ORDER SIBLINGS BY`, and a `FROM broker` that only resolves through the synonym.

**Profile / track**: analytical DBSQL view (ODS). Lakebase variant sketched inline (Postgres `WITH RECURSIVE ... CYCLE`).

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| `START WITH ... CONNECT BY NOCYCLE PRIOR ...` | `WITH RECURSIVE h ... MAX RECURSION LEVEL 20` anchor + recursive member | §5 #81; §6 hierarchical queries |
| `LEVEL` | `depth` counter column | §5 #81 |
| `CONNECT_BY_ROOT broker_ref` | carried from the anchor | §5 #81 |
| `LTRIM(SYS_CONNECT_BY_PATH(ref,'/'),'/')` | `concat(path, '/', ref)` seeded with the root ref | §5 #81 |
| `NVL(col, PRIOR col)` | `nvl(col, h.effective_commission_pct)` | §5 #82 |
| `CONNECT_BY_ISLEAF` | post-walk `EXISTS` on active children | §7 trap 6 |
| `CONNECT_BY_ISCYCLE` / `NOCYCLE` | `array_contains(path_ids, id)` guard | §7 trap 6 (semantics differ, see below) |
| `ORDER SIBLINGS BY` | `sort_key` column exposed; no order in a view | §7 trap 6, trap 22 |
| `FROM broker` (PUBLIC synonym) | resolved to `poladm.broker` | §7 trap 18; §9 synonyms |

## Recon tier that catches a wrong conversion

- **Tier 1** on the view: row count equals the number of active brokers reachable from roots. Forgetting
  `AND b.active_flag = 'Y'` in the join (Oracle puts it in `CONNECT BY`) inflates the count.
- **Tier 2** `COUNT(*)` grouped by `depth` and `SUM(effective_commission_pct)` grouped by `region_ref`: catches
  the depth being off by one (anchor at 0 instead of 1), the `PRIOR` inheritance being reversed, and the default
  recursion depth truncating deep trees (Oracle has no cap; the census records the max `LEVEL` and the converted
  view must set `MAX RECURSION LEVEL` above it).
- **Tier 3** keyed on `(region_ref, broker_id)`: catches `path_refs` separator/leading-slash differences and the
  `is_cycle` semantic mismatch, which is expected and must be excluded from the compare set or agreed in
  `06_decisions.md`.
- Ordering is not compared (a view has no order); consumers that relied on `ORDER SIBLINGS BY` are Tier 4 and
  must add `ORDER BY sort_key`.

## Canonicalization used

`decimal_round` on `effective_commission_pct` (`NUMBER(5,2)` source, `DECIMAL(38,10)` in the CTE); `null_missing_equiv`
for `parent_broker_id` at the root.

## Not verified live

`WITH RECURSIVE` inside `CREATE VIEW` on DBSQL 2025.20+; `MAX RECURSION LEVEL` syntax acceptance; Postgres 17 `CYCLE`
clause on Lakebase.
