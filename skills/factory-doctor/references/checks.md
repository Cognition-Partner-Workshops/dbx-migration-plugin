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
missing, unknown, malformed, or README-unlisted playbooks. A missing lock skips only for setup;
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
### `recon_family_supported`

`fail` when `--source-family` names a family the harness has no live-tested source adapter for —
asked of the same harness `recon_harness` ran (`dbx-recon` on PATH, else the checkout's
`python -m recon.cli families`); a missing harness or unreadable output also fails.
`skipped` when no family is declared. Kept apart from `source_principal_read_only` on purpose:
attestation says the principal is read-only; this row says whether we can reconcile the family.
### `dictionary_readable`

The `--source-secret` principal can read the catalog views the recon structural tier queries. SQL Server and Postgres: every view probe the adapter's `schema_facts` issues must return a row, plus a trigger census per in-scope table — SQL Server `OBJECTPROPERTY(...,'TableHas*Trigger')` (declared) against `sys.triggers WHERE parent_id` (listed), Postgres `pg_class.relhastriggers` against `pg_trigger`. A failing view probe fails naming the view: the tier would grade on an incomplete dictionary. A sqlserver table with declared=1/listed=0 fails (sys.triggers is filtered by permission, so the trigger check would pass on an empty view); the same mismatch on Postgres warns (relhastriggers can stay true after a drop until vacuum). Families without a dictionary probe report `unverified` — structural parity will record their categories as unsupported. `skipped` only at setup; `fail` when the secret is absent like `source_principal_read_only`.

### `databricks_identity`
### `type_map_audit`

`fail` when a resolved unit mapping declares a `target_type` the source family's
`type_map.<family>.<target_kind>` forbids — including a `conditional` alternative whose
token is not in the field's `evidence` list — or when two `canonicalization.json` files claim
the family or the spec will not load; `unverified` when no family is declared;
`warn` when no dialect skill carries a map for the family or the chosen `--target-kind`;
`ok` counts typed fields and records unmapped source types plus undeclared targets the
harness fills; `skipped` only at setup. The audit runs on the same harness `recon_harness`
ran (`dbx-recon` on PATH, else the checkout's `python -m recon.cli type-map-audit`); a
missing harness or unreadable output also fails.
