Playbook: Intake an ETL or batch pipeline and route it to the right migration profile.

| Default | Rule |
|---|---|
| Intake | source dialect, extractor/runtime, scheduler, mappings, rejects, restart, parameters, logging, output contracts |
| Hardening | preserve boundaries, retries, idempotency, quarantine, and observability before conversion |
| Scheduling | map D5 dependencies to Lakeflow Jobs or retain the scheduler with an explicit contract |
| Dialect | invoke an optional dialect skill only when installed; never infer skill existence from an adapter flag |
| Orchestration | invoke `!dbx_migrate_pipeline` in this session after intake and profile selection |

## Routing
Use `CORE` + `PIPELINE` + `ORCHESTRATION` + `DATA / DEPENDENCY`; route source-specific behavior to the installed dialect skill. Continue through `!dbx_migration_setup`, inventory, analysis, and plan; do not convert from this front door.
