---
name: factory-doctor
description: Preflight for a DBX migration workspace. Verifies setup, hooks, contracts, identity, harness, source access, and optional target grants, then writes .migration/09_capabilities.json.
---

# factory-doctor

| `workspace` | setup files or `stop_mode` are missing | rerun `1-migration_setup` |
| `allowed_targets` | allowlist is invalid or differs from `--expect-catalogs` | fix the recorded contract |
| `allowlist_committed` | allowlist or tolerances differ from `HEAD` | commit through a recorded decision |
| `playbooks_in_sync` | lock/live playbooks are missing, stale, malformed, duplicated, or differ from repo | rerun `install-dbx-factory` |
| `hook_guard` | hooks are missing, direct guard fails, or the live probe is unverified/unblocked | load hooks and complete the nonce probe |
| `official_databricks_plugin` | routed official skills are missing or not visible locally | install/load the official skills |
| `recon_harness` | harness self-test/import or a required driver fails | install the harness extras |
| `type_map_audit` | a unit mapping declares a `target_type` the source family's `type_map.<family>.<target_kind>` forbids | fix the declared type or add the required `evidence` token |
| `delete_evidence` | mapped CDC evidence is absent, incomplete, or unreadable | provide source CDC evidence; never enable it here |
| `source_principal_read_only` | source grants permit writes or cannot be verified | remove writes or record a user-attested decision |
| `databricks_identity` | CLI/auth/host/identity is missing, human, or mismatched | use the expected OAuth M2M principal and host |
| `lakebase_branch_create` | optional branch probe cannot create/delete a one-hour child | fix Lakebase project/parent permissions |
| `lakebase_target_grants` | optional DSN role lacks database/schema `CREATE` | grant target create permission |
| `analytical_target_grants` | optional promotion schema lacks required UC privileges | grant `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` |

## Run

```bash
python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo root> [--role orchestrator|child|setup] \
    [--expect-identity <migration SP userName>] [--expect-host <workspace URL>] [--expect-catalogs a,b] \
    [--hook-probe-result blocked:<nonce>|not-blocked] [--unit <unit_id> ...] [--mapping <mapping_spec.json> ...] \
    [--source-secret <ENV VAR NAME> [--source-family sqlserver|postgres|...] --param name=value ...] \
    [--source-attested D-<id>] [--lakebase-project NAME --lakebase-parent-branch NAME] \
    [--lakebase-dsn ENV_VAR_NAME] [--lakebase-schema NAME] [--analytical-schema CATALOG.SCHEMA] \
    [--live-playbooks PATH] [--reuse-record PATH] [--out PATH]
```

Writes `.migration/09_capabilities.json` and prints one line per row. Exit 0 means `ready`.

A `--role child` run may pass `--reuse-record <manifest>.doctor.json` (the orchestrator's signed
record beside the wave manifest). When the record is an orchestrator's, `ready`, signed for the
same manifest bytes, fresher than the manifest's `doctor_max_age` minutes (default 15), and signed
for the `--expect-identity`/`--expect-host` principal, the source-side rows (`recon_harness`,
`recon_family_supported`, `type_map_audit`, `delete_evidence`, `source_principal_read_only`,
`dictionary_readable`, `named_secrets_exist`) are taken from it — anything else in the report, and
the whole run when the record fails any check, is computed fresh. `databricks_identity` and the
workspace/hook/allowlist rows always run fresh. The `doctor_record` row says what happened; a
rejected record prints `doctor record not reused: <why>` on stderr and does not change the exit
code. `--reuse-record` refuses `--wave`, `--no-databricks`, a non-child role, and a missing
`--expect-identity`.
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
| `recon_family_supported` | the harness refuses the declared source family (asked of the same harness `recon_harness` ran) | reconcile through a family the harness supports |
| `delete_evidence` | mapped CDC evidence is absent, incomplete, or unreadable | provide source CDC evidence; never enable it here |
| `source_principal_read_only` | source grants permit writes or cannot be verified | remove writes or record a user-attested decision |
| `dictionary_readable` | the source principal cannot read a catalog view the structural tier needs, or hides declared triggers | fix the principal's catalog visibility |
| `named_secrets_exist` | a Databricks secret name (`scope/key`) the manifest or a brief references is missing or its scope is unreadable | create the named secret before STOP C; a missing name is a STOP C blocker, not a per-unit discovery |
| `databricks_identity` | CLI/auth/host/identity is missing, human, or mismatched | use the expected OAuth M2M principal and host |
| `lakebase_branch_create` | optional branch probe cannot create/delete a one-hour child | fix Lakebase project/parent permissions |
| `lakebase_target_grants` | optional DSN role lacks database/schema `CREATE` | grant target create permission |
| `analytical_target_grants` | optional promotion schema lacks required UC privileges | grant `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` |

Long form: `references/checks.md`.
