Playbook: Intake code, models, and prediction consumers before selecting migration tracks.

| Default | Rule |
|---|---|
| Partition | separate data movement, application code, model training, scoring, and consumers; give each an owner and target |
| Parity | capture feature definitions, seeds, model version, numeric tolerance, prediction distributions, and a legacy bit-stability probe |
| Scope | inventory repositories, jobs, packages, secrets by name, inputs/outputs, schedules, tests, and runtime assumptions |
| Dialect | route only to an installed optional dialect skill; never claim a skill exists because an adapter/Lakebridge flag exists |
| Orchestration | invoke `!dbx_migrate_pipeline` in this session after intake and profile selection |
| Allowlist | write `.migration/allowed_targets.json` (catalogs, legacy_sources) before any source probe; authorized legacy writes carry `DBX_DECISION=D-<id>` — see contract.md |

## Routing
Use `CORE` + the matching `PIPELINE`, `ML-SCORING`, or `CONSUMER` profile + `DATA / DEPENDENCY`. Preserve data/model split and prediction parity through setup, inventory, analysis, and plan.
