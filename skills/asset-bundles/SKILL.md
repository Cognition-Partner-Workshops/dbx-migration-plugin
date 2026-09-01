---
name: asset-bundles
description: Databricks Asset Bundles for deploying migrated code. Use when deploying converted jobs, pipelines, or notebooks to a Databricks workspace during a migration, or wiring CI for migrated code.
---

# Asset Bundles

Migrated code deploys through Asset Bundles so every deploy is declarative, versioned, and idempotent, which the fan-out's rerun-safety rule depends on.

## Layout
- One bundle per pipeline (or per unit batch during fan-out), in the target repo: `databricks.yml` at the bundle root, resources under `resources/` (jobs, DLT pipelines), code under `src/`.
- Bundle targets map to environments: a `migration` target pointing at the migration catalog and engagement warehouse (IDs from `.migration/00_context.md`), a `prod` target that is only ever deployed at cutover by the cutover principal.

## Commands
- `databricks bundle validate` before every deploy; `databricks bundle deploy -t migration`; `databricks bundle run -t migration <job>`.
- Deploys must be idempotent: a redeploy of the same bundle converges to the same state. Children rerunning after a partial failure redeploy rather than patching live resources.

## Rules
- No hardcoded workspace hosts, warehouse IDs, or catalog names in code; they enter through bundle target variables.
- CI wiring: validate + deploy-to-migration + run recon as the PR gate where the customer has CI; the recon evidence contract still applies.
- Never deploy to the prod target from a migration or fan-out session.
