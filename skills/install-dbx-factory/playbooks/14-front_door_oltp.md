Playbook: Intake OLTP workloads, split operational and analytical tracks, and route them to the right target.

| Default | Rule |
|---|---|
| Track split | operational transactions and analytical/CDC consumers get separate units, owners, cutover gates, and parity checks |
| Target | Lakebase project/branch for operational state; Delta/SQL warehouse for analytical state; declare both in `allowed_targets.json` |
| CDC | customer-owned D10 setup, source read-only principal, lag evidence, ordering, deletes, replay, and rollback |
| Transaction | preserve constraints, identity, isolation, retries, uniqueness, timestamps, and atomic boundaries; never invent a weaker contract |
| Dialect | route only to an installed optional dialect skill; do not infer one from an adapter flag |
| Orchestration | invoke `!dbx_migrate_pipeline` in this session after intake and profile selection |
| Pipelines | ask which pipelines share write targets or source objects; disjoint ones run as sibling orchestrator sessions after STOP A (rule in `9-orchestrator.md`) |
| Routine EXECUTE | ask at intake whether the read-only principal may hold EXECUTE on the packages and procedures under test; without it every writing routine stays `unproven` (`skills/data-reconciliation/SKILL.md`, "Routine parity") and the answer is a D10 entry |
| Allowlist | write `.migration/allowed_targets.json` (catalogs, legacy_sources) before any source probe; authorized legacy writes carry `DBX_DECISION=D-<id>` — see contract.md |

## Routing
Use `CORE` + `LAKEBASE` + `DATA / DEPENDENCY` for the operational track. Use `CORE` + the matching `SQL`, `PIPELINE`, or `CONSUMER` profile + `DATA / DEPENDENCY` for the analytical track. Continue through setup, inventory, analysis, plan, reconciliation, and cutover signoff.
