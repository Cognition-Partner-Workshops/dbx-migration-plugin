---
name: databricks-auth-cli
description: Databricks CLI, SDK, and API auth patterns for migration sessions. Use when connecting to a Databricks workspace, running SQL via the Statement Execution API, deploying jobs or bundles, or setting up service principals and secret scopes.
---

# Databricks Auth and CLI

## Auth pattern
- Sessions authenticate as the engagement's **migration service principal**, never a human user's PAT. Host and token come from named org secrets (e.g. `DATABRICKS_HOST_<ENGAGEMENT>`, `DATABRICKS_TOKEN_<ENGAGEMENT>`); reference by name, never print values.
- Configure via environment: `DATABRICKS_HOST` and `DATABRICKS_TOKEN` env vars are honored by the CLI and SDK; avoid writing `~/.databrickscfg` with token values where env vars suffice.
- Verify identity before work: `databricks current-user me` and confirm it is the migration principal, not an admin.

## Common operations
- SQL: Statement Execution API (`databricks api post /api/2.0/sql/statements` or the SDK `statement_execution`) against the engagement's designated warehouse ID from `.migration/00_context.md`. Poll async for long statements.
- Jobs/Workflows: `databricks jobs ...`; deploys of migrated code go through Asset Bundles (see the asset-bundles skill) for idempotency.
- Secret scopes: `databricks secrets create-scope` / `put-secret` for legacy-source credentials used by federation; scope names in artifacts, values never.

## Principal tiers (from the engagement access model)
- Assessment principal: metadata and read-only, phase 0 only.
- Migration principal: full rights on the migration catalog, USE-only elsewhere, no admin roles, no grants on production catalogs. This is what sessions run as.
- Cutover principal: customer-held, production repoint rights, used once at STOP E, never available to fan-out children.

## Rules
- Never create workspaces, warehouses, or duplicate connections/scopes that already exist for the engagement.
- Never request or use account-admin or workspace-admin permissions; if an operation seems to need them, escalate to the user instead.
