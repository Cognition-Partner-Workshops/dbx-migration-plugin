Playbook: Intake a warehouse/reporting workload and route it to migration analysis.

| Default | Rule |
|---|---|
| Intake | engine/version, catalogs/schemas, query history, views/procs/UDFs, BI consumers, schedules, SLAs, security |
| Access | prefer Lakehouse Federation for supported read-only discovery; record source-query cost and concurrency |
| Fallback | if federation is denied or unsupported, use customer export or connector path and record DEGRADED/D10 |
| Dialect | route to an installed optional dialect skill; an adapter or Lakebridge flag does not prove a skill exists |
| Orchestration | invoke `!dbx_migrate_pipeline` in this session after intake and profile selection |

## Routing
Use `CORE` + `SQL` + `CONSUMER` + `DATA / DEPENDENCY`; preserve query semantics, governance, and consumer contracts. Continue through setup, inventory, analysis, and plan.
