## Placement
Setup runs the doctor after committing the allowlist, tolerances, and hook probe. Plan reruns it
with `--expect-catalogs` before each wave; the manifest copies identity, host, catalogs, guard mode,
and stop mode, which `workflow.py` compares before launch. Children and the verifier use expected
identity and host and stop on any fail or unverified hook/security result.

### `workspace`
Sub-checks: `workspace` verifies every required `.migration` setup file from `1-migration_setup`;
`stop_mode` verifies `hard|soft` in `00_context.md` or `01_conventions.md`.
### `allowed_targets`
Sub-checks: `allowed_targets` parses and passes `hooks/dbx_guard.py` (warn for `guard_mode: warn`
or empty `legacy_sources`); `allowlist_matches_contract` compares normalized `--expect-catalogs`
to `allowed_targets.json` and is skipped without that flag.
### `allowlist_committed`
Verifies byte equality of `allowed_targets.json` and `03_recon_tolerances.json` with
`git show HEAD:<path>`, naming modified, untracked, or missing files. Changes require a recorded
decision and commit, never a working-copy edit.
### `playbooks_in_sync`
Verifies string `sha256`, `repo_file`, and `installed_at` for every lock macro; flags stale,
missing, unknown, malformed, or playbooks absent from `playbooks/index.json`. A missing lock skips only for setup;
orchestrator/child rerun `install-dbx-factory`. Orchestrators also require fresh (<15 minutes)
`.migration/live_playbooks.json`; absent/stale/malformed, missing, or duplicate records fail, and
a fresh export proves live bodies equal repo files after line-ending/trailing-newline normalization
(`live_stale`, `live_missing`, and `duplicate` identify those failures).
### `hook_guard`
Sub-checks: `hooks_files` verifies `hooks.json` registers `hooks/dbx_guard.py` as `PreToolUse`;
`hook_guard_functional` directly sends a fresh nonce probe and requires a block naming the full
`__dbx_guard_probe__<nonce>`; `hook_platform_loaded` is `unverified` until the session blocks the
pending nonce, or fails when the echo runs. The nonce and command are in `data`.
### `official_databricks_plugin`
Verifies routed official skills are visible locally: partial visibility is `warn`, none is
`unverified`; platform `requiredPlugins` is the source of truth.
### `recon_harness`
Sub-checks: `recon_harness` runs the `dbx-recon` self-test/import; `recon_drivers` checks lazy
adapters' Databricks SQL, `pyodbc`, and `psycopg` drivers, warning when the
`databricks-sql-connector` live/snapshot extra is absent.
### `delete_evidence`
Checks every resolved mapping: CDC is enabled, the capture is visible and captures every mapped
source key column, and a bounded read-only `fn_cdc_get_all_changes_<capture>` probe works. Children
name units; orchestrators cover every mapping; setup skips before mappings exist. The doctor never
runs `sp_cdc_enable_*`; source-side changes are customer decisions.
### `source_principal_read_only`
Verifies the source principal cannot write mapped objects. SQL Server checks server roles
`sysadmin/securityadmin/serveradmin/dbcreator/bulkadmin`, database roles
`db_owner/db_ddladmin/db_datawriter/db_securityadmin`, `HAS_PERMS_BY_NAME` server controls
`CONTROL SERVER/ALTER ANY DATABASE/IMPERSONATE ANY LOGIN/ALTER ANY LOGIN`, table
`INSERT/UPDATE/DELETE/ALTER`, column `UPDATE`, `fn_my_permissions`, `IMPERSONATE`, `EXECUTE`
on every source procedure, and CDC. Postgres checks `rolsuper/rolcreaterole/rolcreatedb`,
`pg_write_server_files/pg_execute_server_program`, table `INSERT/UPDATE/DELETE/TRUNCATE`, column
`INSERT/UPDATE`, schema `CREATE`, security-definer/granted-function `EXECUTE`, and nested
`pg_has_role` paths (only inheriting or `SET ROLE` memberships on Postgres 16+). Databricks uses
the secret's host/token with inherited `DATABRICKS_*` stripped except host/token and `PAT`, then
`grants get-effective` on each catalog/schema/table; ownership or anything beyond `SELECT`,
`USE_CATALOG`, `USE_SCHEMA`, `BROWSE`, and `READ_VOLUME` fails, owner-less/malformed assignments
are `unverified`. Unsupported/uninferred families are `unverified`; `--source-attested D-<id>`
is `attested` only for families without a query and a human-provenance decision line. Rows name
objects/privileges, never credentials; `readonly=True`/`default_transaction_read_only` are hints.
### `databricks_identity`
Sub-checks: `databricks_cli` verifies CLI on `PATH`; `databricks_auth_kind` warns unless
`DATABRICKS_HOST/CLIENT_ID/CLIENT_SECRET` OAuth M2M is present; `databricks_identity` verifies
`current-user me`, expected principal, workspace host, and service-principal status (human warns
and blocks `ready`); `databricks_warehouse` warns when the default warehouse is absent. Identity
data records `userName`, `service_principal`, and `host`.
### `lakebase_branch_create`
Optional check creates and deletes a one-hour Lakebase child branch; failure means project or parent
expiry permissions are insufficient.
### `lakebase_target_grants`
Optional check verifies the Lakebase DSN role has database and requested schema `CREATE`.
### `analytical_target_grants`
Optional Unity Catalog promotion-schema check passes when absent or owned by the principal.
Otherwise it requires `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, and `SELECT`; failure
detail contains exact `GRANT` statements, or a full schema grant when unreadable/owner unknown.
Every red row is a D10. Never widen permissions, edit the allowlist outside a decision, enable
source CDC here, or substitute a client read-only flag for source grants.
