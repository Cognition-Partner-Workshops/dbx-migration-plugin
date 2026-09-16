Playbook: Intake OLTP workloads, split operational and analytical tracks, and route them to the right target.

| Default | Rule |
|---|---|
| Track split | operational transactions and analytical/CDC consumers get separate units, owners, cutover gates, and parity checks |
| Target | Lakebase project/branch for operational state; Delta/SQL warehouse for analytical state; declare both in `allowed_targets.json` |
| CDC | customer-owned D10 setup, source read-only principal, lag evidence, ordering, deletes, replay, and rollback |
| Transaction | preserve constraints, identity, isolation, retries, uniqueness, timestamps, and atomic boundaries; never invent a weaker contract |
| Dialect | route only to an installed optional dialect skill; do not infer one from an adapter flag |
| Orchestration | invoke `!dbx_migrate_pipeline` in this session after intake and profile selection |

## Routing
Use `CORE` + `LAKEBASE` for the operational track and `DATA / DEPENDENCY` + `CONSUMER` for the analytical track. Continue through setup, inventory, analysis, plan, reconciliation, and cutover signoff.
