## Where it runs in the factory

- **Setup (`1-migration_setup` step 7)**: orchestrator runs it after committing `allowed_targets.json`,
  completes the hook probe, commits `09_capabilities.json`. `ready: false` (a `fail` anywhere, or
  identity/hooks/source principal not `ok`) is a D10 and blocks STOP A.
- **Plan (`4-migration_plan`)**: re-run before every wave manifest is committed, with
  `--expect-catalogs` naming the contract's catalogs; the manifest's `capabilities` copy the report's
  `identity.userName`, `identity.host`, catalogs, `guard_mode` and `stop_mode`, and `workflow.py`
  compares them with `09_capabilities.json` before launching anything.
- **Unit (`5-unit_migration` step 1)**: each child runs
  `doctor.py --role child --expect-identity <userName from brief> --expect-host <host from brief>` and does the hook probe before
  converting anything. Any `fail` -> report `status=BLOCKED` with the check id; never proceed as a
  different identity or with hooks unverified.
- **Verifier**: runs it the same way; a verifier that cannot prove its identity produces no verdict.

## Check details

### `workspace`

`.migration/` missing one of the eight setup files.

### `stop_mode`

`stop_mode: hard|soft` not recorded.

### `allowed_targets`

`allowed_targets.json` missing, invalid, or rejected by the guard; `warn` if `guard_mode: warn` or `legacy_sources` empty.

### `allowlist_committed`

`.migration/allowed_targets.json` or `.migration/03_recon_tolerances.json` is not byte-equal to `git show HEAD:<path>`: the row names the file and its state (`modified since HEAD`, `untracked`, `missing`). A tolerance or catalog changes through a recorded decision and a commit, never on a working copy.

### `allowlist_matches_contract`

The `--expect-catalogs` list (the wave's capability contract) differs from the allowlist's `catalogs`; `skipped` without the flag.

### `playbooks_in_sync`

`.migration/playbooks.lock.json` (written by `install-dbx-factory` after it syncs the org playbook
library) holds every macro's repo-file sha256; the row fails when a macro is `stale` (hash differs),
`missing` from the lock, or `unknown` in the lock but gone from the repo, or when a `*.md` in
`playbooks/` is not in `playbooks/index.json`. An entry whose `sha256`/`repo_file`/`installed_at`
is not all strings is `malformed` (named first). Missing lock: `skipped` under `--role setup` (the
setup step of `1-migration_setup` runs with it), `fail` for orchestrator/child — re-run
`install-dbx-factory`, the only fix.

The lock alone proves repo == last sync receipt. Under `--role orchestrator` the row also requires
`.migration/live_playbooks.json` — the `[{"macro","playbook_id","content"}]` export the orchestrator
writes with `devin_playbook_manage` right before the doctor (see `9-orchestrator` step 6) —
absent, older than 15 minutes, or malformed is `fail`; `child`/`setup` only check it when present.
Each repo macro's live body is compared to the repo file (line-ending and trailing-newline
normalized): a mismatch is `live_stale`, no record is `live_missing`, more than one record is
`duplicate` (the fix is to archive the extra live playbook, then re-sync). With a fresh export,
`ok` proves live == repo, not just repo == receipt.

### `hooks_files`

`hooks.json` missing/malformed or does not register `hooks/dbx_guard.py` as `PreToolUse`.

### `hook_guard_functional`

The guard, invoked directly, fails to block the probe with a reason naming the full `__dbx_guard_probe__<nonce>` token the doctor sent, with a nonce fresh per run.

### `hook_platform_loaded`

Live probe ran unblocked; `unverified` until `--hook-probe-result blocked:<nonce>` repeats the pending nonce (`data.probe_nonce`, `data.probe_command`).

### `lakebase_branch_create`

Optional Lakebase project/parent branch probe cannot create and delete a one-hour child branch.

### `lakebase_target_grants`

Optional Lakebase DSN role lacks database or requested schema `CREATE`.

### `analytical_target_grants`

The optional Unity Catalog promotion-schema check is `ok` when the schema does not exist (setup
creates it owned by the migration principal) or when the principal owns it. If the schema exists
under another owner and the principal lacks `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`,
or `SELECT`, the row is red and its `detail` contains the exact `GRANT` statement or statements
to paste into the D10 request. An unreadable schema (no `USE SCHEMA`) is reported as the full
schema grant with owner unknown.

### `source_principal_read_only`

The `--source-secret` principal can write an in-scope source table, directly or through indirection. SQL Server and Postgres privilege checks include server, database, table, column, schema, role-membership, procedure, and CDC capture permissions; for the Databricks family the check authenticates as the `--source-secret` credential (the `{server_hostname,http_path,access_token}` JSON the recon adapter uses) with a subprocess env stripped of every inherited `DATABRICKS_*` variable except that host, that token and `DATABRICKS_AUTH_TYPE=pat`, and reads `databricks grants get-effective` on every in-scope catalog/schema/table on that host (ownership, direct or through a group, counts; an owner lookup that fails or returns no owner is `unverified`, as is a response without a well-formed `privilege_assignments` list) and fails on any privilege outside SELECT/USE_CATALOG/USE_SCHEMA/BROWSE/READ_VOLUME; for a source with no principal to query, `--source-attested D-<id>` reports `attested` (which does not block `ready`) when a `06_decisions.md` line names the decision id, `source_principal_read_only` and `attested` — it is rejected for families that have a query. The row names the object and privilege, never the credential. `unverified` also blocks `ready` when the family is unsupported, cannot be inferred, or the check cannot run. Driver read-only hints never substitute for grant checks.

### `databricks_identity`

`current-user me` fails, differs from `--expect-identity`, `auth describe` resolves no workspace host, or that host is not `--expect-host`; a human user remains a warning that blocks `ready`.

## Privilege and remediation notes

The detailed privilege and remediation text remains here so the invocation contract can keep the checks table concise. Red rows are D10 findings, not permission-widening workarounds; report the exact check id and use the source of truth named in the table.

## Detailed check contract

| id | fail means | source of truth |
|---|---|---|
| `workspace` | `.migration/` missing one of the eight setup files | `1-migration_setup` |
| `stop_mode` | `stop_mode: hard\|soft` not recorded | `00_context.md` |
| `allowed_targets` | `allowed_targets.json` missing, invalid, or rejected by the guard; `warn` if `guard_mode: warn` or `legacy_sources` empty | `hooks/dbx_guard.py` |
| `allowlist_committed` | `.migration/allowed_targets.json` or `.migration/03_recon_tolerances.json` is not byte-equal to `git show HEAD:<path>`: the row names the file and its state (`modified since HEAD`, `untracked`, `missing`). A tolerance or catalog changes through a recorded decision and a commit, never on a working copy | `git` |
| `allowlist_matches_contract` | the `--expect-catalogs` list (the wave's capability contract) differs from the allowlist's `catalogs`; `skipped` without the flag | `allowed_targets.json` |
| `playbooks_in_sync` | the lock `.migration/playbooks.lock.json` is missing (fail; `skipped` only under `--role setup`), unreadable, has a `malformed` entry (non-string `sha256`/`repo_file`/`installed_at`), or differs from the repo playbook files — `stale` sha, `missing` macro, `unknown` macro in the lock but not the repo, or a `*.md` under `playbooks/` absent from `playbooks/index.json`. Under `--role orchestrator` a fresh (<15 min) `.migration/live_playbooks.json` export is also required: absent/stale/malformed fails, and each live body must equal its repo file — mismatched is `live_stale`, no record `live_missing`, more than one `duplicate` (archive the extra). `ok` with the export proves live == repo; without it, repo == last sync receipt. Re-run `install-dbx-factory` | `.migration/playbooks.lock.json`, `.migration/live_playbooks.json` |
| `hooks_files` | `hooks.json` missing/malformed or does not register `hooks/dbx_guard.py` as `PreToolUse` | plugin root |
| `hook_guard_functional` | the guard, invoked directly, fails to block the probe with a reason naming the full `__dbx_guard_probe__<nonce>` token the doctor sent, with a nonce fresh per run (a blanket deny, even one that hardcodes the prefix or a token seen before, is not proof it read the command) | `hooks/dbx_guard.py` |
| `hook_platform_loaded` | live probe ran unblocked; `unverified` until `--hook-probe-result blocked:<nonce>` repeats the pending nonce (`data.probe_nonce`, `data.probe_command`) | this session |
| `lakebase_branch_create` | optional Lakebase project/parent branch probe cannot create and delete a one-hour child branch | Lakebase project permissions and parent expiry |
| `lakebase_target_grants` | optional Lakebase DSN role lacks database or requested schema `CREATE` | Postgres privileges |
| `analytical_target_grants` | optional promotion schema is absent/owned or lacks required Unity Catalog privileges | Unity Catalog grants on the promotion schema |
| `official_databricks_plugin` | `warn` if some routed official skills are missing on disk; `unverified` if none visible locally (they are platform-loaded via `requiredPlugins`) | `target-routing` |
| `recon_harness` | `dbx-recon selftest` fails or the harness is not importable | `data-reconciliation` |
| `recon_drivers` | `warn` if `databricks-sql-connector` is missing (live/snapshot recon impossible) | harness `pyproject.toml` extras |
| `delete_evidence` | a mapping object declares `delete_evidence` but the source has CDC off, a declared capture instance is missing or not visible to the identity, a capture does not capture every mapped `key.source` column (named in the row), or the identity cannot call that capture's `fn_cdc_get_all_changes_<capture>` with the key columns and `root_where`; the set of mappings is resolved by the doctor, never typed: `--role child` must name its batch with `--unit` (omitted: `fail`; a named unit without `.migration/units/<id>/mapping_spec.json`: `fail`, hand-off incomplete) and an orchestrator covers every `.migration/units/*/mapping_spec.json` in the workspace, so a subset can never pass as the whole; `--mapping` adds a candidate spec on top; `skipped` (not applicable) only at setup, before a unit mapping exists. Three metadata reads plus one bounded read-only probe per object; no `SELECT` on the `cdc` schema is required, and the doctor never runs `sp_cdc_enable_*` (a source-side change is the customer's decision) | `data-reconciliation` |
| `source_principal_read_only` | the `--source-secret` principal can write an in-scope source table, directly or through indirection. SQL Server: server roles `sysadmin`/`securityadmin`/`serveradmin`/`dbcreator`/`bulkadmin`, database roles `db_owner`/`db_ddladmin`/`db_datawriter`/`db_securityadmin`, `HAS_PERMS_BY_NAME(NULL,NULL,...)` for `CONTROL SERVER`/`ALTER ANY DATABASE`/`IMPERSONATE ANY LOGIN`/`ALTER ANY LOGIN`, `HAS_PERMS_BY_NAME(<table>,'OBJECT',INSERT\|UPDATE\|DELETE\|ALTER)`, a column-level `UPDATE` grant on any in-scope table (`fn_my_permissions(<table>,'OBJECT')`, which the table-level check does not see), `IMPERSONATE` on any login or user, and `EXECUTE` on any user stored procedure in the source database (every procedure is treated as writing). Postgres: `rolsuper`/`rolcreaterole`/`rolcreatedb` (not `BYPASSRLS`: it widens reads, it grants no write), membership in `pg_write_server_files`/`pg_execute_server_program`, `has_table_privilege(<table>,'INSERT,UPDATE,DELETE,TRUNCATE')`, `has_column_privilege(...,'INSERT'\|'UPDATE')` on any column of an in-scope table, `has_schema_privilege(<schema>,'CREATE')`, `EXECUTE` on a `SECURITY DEFINER` or explicitly granted function in an in-scope schema, and a membership path (every role `pg_has_role(current_user, <role>, ...)` reaches, nested memberships included; on Postgres 16+ only memberships that inherit or can `SET ROLE`, so a `SET FALSE, INHERIT FALSE` grant does not count) to a role holding either table or schema privilege. Tables come from the same mappings `delete_evidence` resolves. The row names the object and privilege, never the credential. Databricks: `grants get-effective` on every in-scope catalog/schema/table for the `current-user me` principal the `--source-secret` token authenticates as on the secret's host, ownership (direct or via a `current-user me` group) counting as a write, an owner lookup failure or owner-less payload being `unverified`, every privilege outside SELECT/USE_CATALOG/USE_SCHEMA/BROWSE/READ_VOLUME failing; `attested` (does not block `ready`) via `--source-attested D-<id>` when a `06_decisions.md` line names the decision id, `source_principal_read_only` and `attested` — rejected for families that have a query, Databricks included. `unverified` (also blocks `ready`) for Teradata/Oracle/Redshift/Snowflake (no tested privilege query; check the source grants by hand, record the decision, then `--source-attested D-<id>`), when the family cannot be inferred (pass `--source-family`), or when the check cannot run; `skipped` only at setup, before a unit mapping exists. `stats` says whether the connection was opened read-only (`readonly=True` / `default_transaction_read_only`): that is a driver hint the server may ignore, so it never substitutes for the grant check | the source's own catalog views |
| `databricks_cli` | CLI not on PATH | `databricks-core` |
| `databricks_auth_kind` | `warn` unless OAuth M2M env (`DATABRICKS_HOST/CLIENT_ID/CLIENT_SECRET`) | `target-routing` auth rules |
| `databricks_identity` | `current-user me` fails, differs from `--expect-identity`, `auth describe` resolves no workspace host, or that host is not `--expect-host` (compared without scheme, case or trailing slash); `warn` if a human user (still blocks `ready`). `data` records `userName`, `service_principal`, `host`, copied to the report's `identity` | `07_access_checklist.md` |
| `databricks_warehouse` | `warn` if `aitools get-default-warehouse` resolves nothing | `databricks-core` |
