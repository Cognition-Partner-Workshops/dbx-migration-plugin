---
name: factory-doctor
description: Preflight for a DBX migration workspace. Verifies setup, hooks, contracts, identity, harness, source access, and optional target grants, then writes .migration/09_capabilities.json.
---

# factory-doctor

Run before STOP A, before every wave, and before a child converts its first unit. A fail is a D10,
not something to work around.

## Run

```bash
python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo root> [--role orchestrator|child|setup] \
    [--expect-identity <migration SP userName>] [--expect-host <workspace URL>] [--expect-catalogs a,b] \
    [--hook-probe-result blocked:<nonce>|not-blocked] [--unit <unit_id> ...] [--mapping <mapping_spec.json> ...] \
    [--source-secret <ENV VAR NAME> [--source-family sqlserver|postgres|...] --param name=value ...] \
    [--source-attested D-<id>] [--lakebase-project NAME --lakebase-parent-branch NAME] \
    [--lakebase-dsn ENV_VAR_NAME] [--lakebase-schema NAME] [--analytical-schema CATALOG.SCHEMA] \
    [--live-playbooks PATH] [--out PATH]
```

Writes `.migration/09_capabilities.json` and prints one line per row. Exit 0 means `ready`.
`ready` requires no `fail`, the security rows `hook_guard` and `databricks_identity` to have every
security sub-result at `ok`, and `source_principal_read_only` not `unverified` once mappings exist.
Other warnings are advisory. Sub-results live in `data.sub_results`; `--no-databricks` leaves
`databricks_identity=skipped` and never authorizes a wave. The report identity, host, catalogs,
guard mode, and stop mode are the workflow capability contract.

## The hook probe

1. Run the exact `probe_command` in the `hook_guard` row, from the workspace.
2. A guard refusal naming `__dbx_guard_probe__<nonce>` proves hooks are live.
3. Re-run with `--hook-probe-result blocked:<nonce>`; the nonce is in the `hook_guard` row.
4. If the echo prints, re-run with `not-blocked`, register a D10, and do not launch children.

| row | fails when | fix |
|---|---|---|
| `workspace` | setup files or `stop_mode` are missing | rerun `1-migration_setup` |
| `allowed_targets` | allowlist is invalid or differs from `--expect-catalogs` | fix the recorded contract |
| `allowlist_committed` | allowlist or tolerances differ from `HEAD` | commit through a recorded decision |
| `playbooks_in_sync` | lock/live playbooks are missing, stale, malformed, duplicated, or differ from repo | rerun `install-dbx-factory` |
| `hook_guard` | hooks are missing, direct guard fails, or the live probe is unverified/unblocked | load hooks and complete the nonce probe |
| `official_databricks_plugin` | routed official skills are missing or not visible locally | install/load the official skills |
| `recon_harness` | harness self-test/import or a required driver fails | install the harness extras |
| `delete_evidence` | mapped CDC evidence is absent, incomplete, or unreadable | provide source CDC evidence; never enable it here |
| `source_principal_read_only` | source grants permit writes or cannot be verified | remove writes or record a user-attested decision |
| `databricks_identity` | CLI/auth/host/identity is missing, human, or mismatched | use the expected OAuth M2M principal and host |
| `lakebase_branch_create` | optional branch probe cannot create/delete a one-hour child | fix Lakebase project/parent permissions |
| `lakebase_target_grants` | optional DSN role lacks database/schema `CREATE` | grant target create permission |
| `analytical_target_grants` | optional promotion schema lacks required UC privileges | grant `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` |

Long form: `references/checks.md`.
