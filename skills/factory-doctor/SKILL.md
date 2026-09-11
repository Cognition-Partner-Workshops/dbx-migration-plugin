---
name: factory-doctor
description: Preflight for a DBX migration workspace. Verifies the Databricks CLI and the migration principal's identity and host, the dbx-recon harness self-test, .migration/ integrity, the write-scope allowlist (committed, and equal to the wave's contract), that the source principal cannot write in-scope objects, hook loading (nonce probe), and stop_mode, then writes .migration/09_capabilities.json. Run at setup before STOP A, by the plan playbook before every wave, and by every fan-out child before its first unit. A red row is a D10, not something to work around.
---

# factory-doctor

Seventeen checks, one JSON, no warehouse spend. The point is to find out *before* fifty children
launch that the session is a human identity, the harness is not installed, the hooks are not
being applied, the source credential can write, or the tolerances on disk are not the committed ones.

## Run

```bash
python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo root> [--role orchestrator|child] \
    [--expect-identity <migration SP userName>] [--expect-catalogs a,b] \
    [--hook-probe-result blocked:<nonce>|not-blocked] \
    [--unit <unit_id> ...] [--mapping <candidate mapping_spec.json> ...] \
    [--source-secret <ENV VAR NAME of the source DSN> [--source-family sqlserver|postgres|...] --param name=value ...]
# --role child: one --unit per unit in the batch brief (the doctor resolves and checks
# .migration/units/<id>/mapping_spec.json itself); an orchestrator checks every unit mapping in the
# workspace. --source-secret/--param: the same values the recon run will get. --expect-catalogs: the
# catalogs the wave's capability contract names.
```

Writes `.migration/09_capabilities.json` and prints one line per check. Exit 0 = `ready`.
`ready` needs no `fail` anywhere *and* the three security controls (`hook_guard_functional`,
`hook_platform_loaded`, `databricks_identity`) at `ok`, *and* `source_principal_read_only` not
`unverified` once a unit mapping exists: an `unverified` probe, a human identity or a source
principal whose grants could not be read leaves `ready: false` with the offending ids in
`blocking`. Other `warn`/`unverified` rows are advisory. `--no-databricks` skips CLI/identity
checks for offline use; the report it writes is never `ready` (identity `skipped` stays in
`blocking`), so it cannot authorize a wave. The report's top-level `identity` (`userName`,
`service_principal`, `host`) is what the wave manifest's `capabilities` must repeat; the workflow
refuses a manifest whose identity, host, catalogs, guard_mode or stop_mode differ from it.

## The hook probe (the one manual step)

Devin runs plugin hooks fail-open: if `hooks.json` is not loaded, nothing tells the session. The
doctor therefore reports `hook_platform_loaded: unverified` until you prove it, and the proof is
not your word: each report issues a fresh 8-hex `probe_nonce` and embeds it in the probe's catalog
name, so the block message the platform shows is the only place the nonce can be read from.

1. Run, in the session shell, exactly the `probe_command` printed in that row of the report the
   doctor just wrote. It is an `echo` whose *text* looks like a Databricks write to
   `__dbx_guard_probe__<nonce>`, a catalog that is not allowlisted; it makes no Databricks call
   whatever happens.
2. If the shell tool refuses it with a `dbx-migration-factory guard` reason naming
   `__dbx_guard_probe__<nonce>`, hooks are live: re-run the doctor with
   `--hook-probe-result blocked:<nonce>`. A nonce the last report did not issue (typed, stale, or
   from another workspace) keeps the row `unverified` and issues a new one; a bare `blocked` is a
   CLI error.
3. If it prints the line, hooks are **not** applied in this session: re-run with
   `--hook-probe-result not-blocked`, which fails the run. Register a D10 (plugin not installed at
   the org level, or hooks disabled) and do not launch children until it is `blocked:<nonce>`.

## Checks

| id | fail means | source of truth |
|---|---|---|
| `workspace` | `.migration/` missing one of the eight setup files | `1-migration_setup` |
| `stop_mode` | `stop_mode: hard\|soft` not recorded | `00_context.md` |
| `allowed_targets` | `allowed_targets.json` missing, invalid, or rejected by the guard; `warn` if `guard_mode: warn` or `legacy_sources` empty | `hooks/dbx_guard.py` |
| `allowlist_committed` | `.migration/allowed_targets.json` or `.migration/03_recon_tolerances.json` is not byte-equal to `git show HEAD:<path>`: the row names the file and its state (`modified since HEAD`, `untracked`, `missing`). A tolerance or catalog changes through a recorded decision and a commit, never on a working copy | `git` |
| `allowlist_matches_contract` | the `--expect-catalogs` list (the wave's capability contract) differs from the allowlist's `catalogs`; `skipped` without the flag | `allowed_targets.json` |
| `hooks_files` | `hooks.json` missing/malformed or does not register `hooks/dbx_guard.py` as `PreToolUse` | plugin root |
| `hook_guard_functional` | the guard, invoked directly, fails to block the probe with a reason naming `__dbx_guard_probe__` (a blanket deny is not proof it read the command) | `hooks/dbx_guard.py` |
| `hook_platform_loaded` | live probe ran unblocked; `unverified` until `--hook-probe-result blocked:<nonce>` repeats the nonce this workspace's last report issued (`data.probe_nonce`, `data.probe_command`) | this session |
| `official_databricks_plugin` | `warn` if some routed official skills are missing on disk; `unverified` if none visible locally (they are platform-loaded via `requiredPlugins`) | `target-routing` |
| `recon_harness` | `dbx-recon selftest` fails or the harness is not importable | `data-reconciliation` |
| `recon_drivers` | `warn` if `databricks-sql-connector` is missing (live/snapshot recon impossible) | harness `pyproject.toml` extras |
| `delete_evidence` | a mapping object declares `delete_evidence` but the source has CDC off, a declared capture instance is missing or not visible to the identity, a capture does not capture every mapped `key.source` column (named in the row), or the identity cannot call that capture's `fn_cdc_get_all_changes_<capture>` with the key columns and `root_where`; the set of mappings is resolved by the doctor, never typed: `--role child` must name its batch with `--unit` (omitted: `fail`; a named unit without `.migration/units/<id>/mapping_spec.json`: `fail`, hand-off incomplete) and an orchestrator covers every `.migration/units/*/mapping_spec.json` in the workspace, so a subset can never pass as the whole; `--mapping` adds a candidate spec on top; `skipped` (not applicable) only at setup, before a unit mapping exists. Three metadata reads plus one bounded read-only probe per object; no `SELECT` on the `cdc` schema is required, and the doctor never runs `sp_cdc_enable_*` (a source-side change is the customer's decision) | `data-reconciliation` |
| `source_principal_read_only` | the `--source-secret` principal can write an in-scope source table, directly or through indirection. SQL Server: server roles `sysadmin`/`securityadmin`/`serveradmin`/`dbcreator`/`bulkadmin`, database roles `db_owner`/`db_ddladmin`/`db_datawriter`/`db_securityadmin`, `HAS_PERMS_BY_NAME(NULL,NULL,...)` for `CONTROL SERVER`/`ALTER ANY DATABASE`/`IMPERSONATE ANY LOGIN`/`ALTER ANY LOGIN`, `HAS_PERMS_BY_NAME(<table>,'OBJECT',INSERT\|UPDATE\|DELETE\|ALTER)`, a column-level `UPDATE` grant on any in-scope table (`fn_my_permissions(<table>,'OBJECT')`, which the table-level check does not see), `IMPERSONATE` on any login or user, and `EXECUTE` on any user stored procedure in the source database (every procedure is treated as writing). Postgres: `rolsuper`/`rolcreaterole`/`rolcreatedb`/`rolbypassrls`, membership in `pg_write_server_files`/`pg_execute_server_program`, `has_table_privilege(<table>,'INSERT,UPDATE,DELETE,TRUNCATE')`, `has_column_privilege(...,'INSERT'\|'UPDATE')` on any column of an in-scope table, `has_schema_privilege(<schema>,'CREATE')`, `EXECUTE` on a `SECURITY DEFINER` or explicitly granted function in an in-scope schema, and a membership path (every role `pg_has_role(current_user, <role>, ...)` reaches, nested memberships included; on Postgres 16+ only memberships that inherit or can `SET ROLE`, so a `SET FALSE, INHERIT FALSE` grant does not count) to a role holding either table or schema privilege. Tables come from the same mappings `delete_evidence` resolves. The row names the object and privilege, never the credential. `unverified` (also blocks `ready`) for Databricks/Teradata/Oracle/Redshift/Snowflake (no tested privilege query; check the source grants by hand), when the family cannot be inferred (pass `--source-family`), or when the connection fails; `skipped` only at setup, before a unit mapping exists. `stats` says whether the connection was opened read-only (`readonly=True` / `default_transaction_read_only`): that is a driver hint the server may ignore, so it never substitutes for the grant check | the source's own catalog views |
| `databricks_cli` | CLI not on PATH | `databricks-core` |
| `databricks_auth_kind` | `warn` unless OAuth M2M env (`DATABRICKS_HOST/CLIENT_ID/CLIENT_SECRET`) | `target-routing` auth rules |
| `databricks_identity` | `current-user me` fails, differs from `--expect-identity`, or `auth describe` resolves no workspace host; `warn` if a human user (still blocks `ready`). `data` records `userName`, `service_principal`, `host`, copied to the report's `identity` | `07_access_checklist.md` |
| `databricks_warehouse` | `warn` if `aitools get-default-warehouse` resolves nothing | `databricks-core` |

## Where it runs in the factory

- **Setup (`1-migration_setup` step 7)**: orchestrator runs it after committing `allowed_targets.json`,
  completes the hook probe, commits `09_capabilities.json`. `ready: false` (a `fail` anywhere, or
  identity/hooks/source principal not `ok`) is a D10 and blocks STOP A.
- **Plan (`4-migration_plan`)**: re-run before every wave manifest is committed, with
  `--expect-catalogs` naming the contract's catalogs; the manifest's `capabilities` copy the report's
  `identity.userName`, `identity.host`, catalogs, `guard_mode` and `stop_mode`, and `workflow.py`
  compares them with `09_capabilities.json` before launching anything.
- **Unit (`5-unit_migration` step 1)**: each child runs
  `doctor.py --role child --expect-identity <userName from brief>` and does the hook probe before
  converting anything. Any `fail` -> report `status=BLOCKED` with the check id; never proceed as a
  different identity or with hooks unverified.
- **Verifier**: runs it the same way; a verifier that cannot prove its identity produces no verdict.

## Rules

- Never read or print secret values; the doctor reports which env var *names* are set, nothing else.
- Never "fix" a red row by widening permissions, switching to a personal identity, or editing
  `allowed_targets.json` outside a recorded decision.
- Never edit `09_capabilities.json` by hand; it is doctor output, and a child does not commit it.
- A source DSN that *can* write is a `fail` even if every write is opened `readonly=True`: the
  read-only guarantee is the principal's grants, verified here, not a driver flag.
