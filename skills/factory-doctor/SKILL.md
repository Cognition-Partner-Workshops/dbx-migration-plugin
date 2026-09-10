---
name: factory-doctor
description: Preflight for a DBX migration workspace. Verifies the Databricks CLI and the migration principal's identity, the dbx-recon harness self-test, .migration/ integrity, the write-scope allowlist, hook loading, and stop_mode, then writes .migration/09_capabilities.json. Run at setup before STOP A, by the plan playbook before every wave, and by every fan-out child before its first unit. A red row is a D10, not something to work around.
---

# factory-doctor

Eleven checks, one JSON, no warehouse spend. The point is to find out *before* fifty children
launch that the session is a human identity, the harness is not installed, or the hooks are not
being applied.

## Run

```bash
python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo root> [--role orchestrator|child] \
    [--expect-identity <migration SP userName>] [--hook-probe-result blocked|not-blocked] \
    [--mapping <recon mapping.json> --source-secret <ENV VAR NAME of the read-only source DSN>
     --param name=value ...]   # the same --param values the recon run will get
```

Writes `.migration/09_capabilities.json` and prints one line per check. Exit 0 = `ready`.
`ready` needs no `fail` anywhere *and* the three security controls (`hook_guard_functional`,
`hook_platform_loaded`, `databricks_identity`) at `ok`: an `unverified` probe or a human identity
leaves `ready: false` with the offending ids in `blocking`. Other `warn`/`unverified` rows are
advisory. `--no-databricks` skips CLI/identity checks for offline use; the report it writes is
never `ready` (identity `skipped` stays in `blocking`), so it cannot authorize a wave.

## The hook probe (the one manual step)

Devin runs plugin hooks fail-open: if `hooks.json` is not loaded, nothing tells the session. The
doctor therefore reports `hook_platform_loaded: unverified` until you prove it:

1. Run, in the session shell, exactly the `probe_command` printed in that row. It is an `echo`
   whose *text* looks like a Databricks write to a catalog that is not allowlisted; it makes no
   Databricks call whatever happens.
2. If the shell tool refuses it with a `dbx-migration-factory guard` reason, hooks are live:
   re-run the doctor with `--hook-probe-result blocked`.
3. If it prints the line, hooks are **not** applied in this session: re-run with
   `--hook-probe-result not-blocked`, which fails the run. Register a D10 (plugin not installed at
   the org level, or hooks disabled) and do not launch children until it is `blocked`.

## Checks

| id | fail means | source of truth |
|---|---|---|
| `workspace` | `.migration/` missing one of the eight setup files | `1-migration_setup` |
| `stop_mode` | `stop_mode: hard\|soft` not recorded | `00_context.md` |
| `allowed_targets` | `allowed_targets.json` missing, invalid, or rejected by the guard; `warn` if `guard_mode: warn` or `legacy_sources` empty | `hooks/dbx_guard.py` |
| `hooks_files` | `hooks.json` or a hook script missing/malformed | plugin root |
| `hook_guard_functional` | the guard, invoked directly, fails to block the probe | `hooks/dbx_guard.py` |
| `hook_platform_loaded` | live probe ran unblocked; `unverified` until the probe is run | this session |
| `official_databricks_plugin` | `warn` if some routed official skills are missing on disk; `unverified` if none visible locally (they are platform-loaded via `requiredPlugins`) | `target-routing` |
| `recon_harness` | `dbx-recon selftest` fails or the harness is not importable | `data-reconciliation` |
| `recon_drivers` | `warn` if `databricks-sql-connector` is missing (live/snapshot recon impossible) | harness `pyproject.toml` extras |
| `delete_evidence` | a mapping object declares `delete_evidence` but the source has CDC off, a declared capture instance is missing or not visible to the identity, a capture does not capture every mapped `key.source` column (named in the row), or the identity cannot call that capture's `fn_cdc_get_all_changes_<capture>` with the key columns and `root_where`; `skipped` without `--mapping`. Three metadata reads plus one bounded read-only probe per object; no `SELECT` on the `cdc` schema is required, and the doctor never runs `sp_cdc_enable_*` (a source-side change is the customer's decision) | `data-reconciliation` |
| `databricks_cli` | CLI not on PATH | `databricks-core` |
| `databricks_auth_kind` | `warn` unless OAuth M2M env (`DATABRICKS_HOST/CLIENT_ID/CLIENT_SECRET`) | `target-routing` auth rules |
| `databricks_identity` | `current-user me` fails, or differs from `--expect-identity`; `warn` if a human user (still blocks `ready`) | `07_access_checklist.md` |
| `databricks_warehouse` | `warn` if `aitools get-default-warehouse` resolves nothing | `databricks-core` |

## Where it runs in the factory

- **Setup (`1-migration_setup` step 7)**: orchestrator runs it after writing `allowed_targets.json`,
  completes the hook probe, commits `09_capabilities.json`. `ready: false` (a `fail` anywhere, or
  identity/hooks not `ok`) is a D10 and blocks STOP A.
- **Plan (`4-migration_plan`)**: re-run before every wave manifest is committed; the manifest's briefs
  quote the `summary` line and the migration principal's `userName` so children can compare.
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
