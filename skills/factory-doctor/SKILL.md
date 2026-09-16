---
name: factory-doctor
description: Preflight for a DBX migration workspace. Verifies the Databricks CLI and the migration principal's identity and host, the dbx-recon harness self-test, .migration/ integrity, the write-scope allowlist (committed, and equal to the wave's contract), that the source principal cannot write in-scope objects, hook loading (nonce probe), and stop_mode, then writes .migration/09_capabilities.json. Run at setup before STOP A, by the plan playbook before every wave, and by every fan-out child before its first unit. A red row is a D10, not something to work around.
---

# factory-doctor

Twenty checks, one JSON, no warehouse spend. The point is to find out *before* fifty children
launch that the session is a human identity, the harness is not installed, the hooks are not
being applied, the source credential can write, or the tolerances on disk are not the committed ones.

## Run

```bash
python3 <plugin>/skills/factory-doctor/doctor.py --workspace <repo root> [--role orchestrator|child|setup] \
    [--expect-identity <migration SP userName>] [--expect-host <workspace URL>] [--expect-catalogs a,b] \
    [--hook-probe-result blocked:<nonce>|not-blocked] \
    [--unit <unit_id> ...] [--mapping <candidate mapping_spec.json> ...] \
    [--source-secret <ENV VAR NAME of the source DSN> [--source-family sqlserver|postgres|...] --param name=value ...] \
    [--source-attested D-<id>] \
    [--lakebase-project NAME --lakebase-parent-branch NAME] [--lakebase-dsn ENV_VAR_NAME] [--lakebase-schema NAME] \
    [--analytical-schema CATALOG.SCHEMA] [--live-playbooks PATH]
# --role child: one --unit per unit in the batch brief (the doctor resolves and checks
# .migration/units/<id>/mapping_spec.json itself); an orchestrator checks every unit mapping in the
# workspace. --role setup: the setup step of 1-migration_setup runs with it; there a missing
# playbooks.lock.json is `skipped` (a warning), not `fail`.
# --source-secret/--param: the same values the recon run will get. --expect-catalogs: the
# catalogs the wave's capability contract names. --source-attested D-<id>: a decision in
# .migration/06_decisions.md attesting the source has no principal to query (files in object
# storage, a read-only share, a static dump); only for families without a privilege query, and
# only a row a human replied to (`user:<id>` provenance) attests — `default-accepted` does not.
```

Writes `.migration/09_capabilities.json` and prints one line per check. Exit 0 = `ready`.
`ready` needs no `fail` anywhere *and* the three security controls (`hook_guard_functional`,
`hook_platform_loaded`, `databricks_identity`) at `ok`, *and* `source_principal_read_only` at `ok`
or `attested` once a unit mapping exists: an `unverified` probe, a human identity or a source
principal whose grants could not be read leaves `ready: false` with the offending ids in
`blocking`. Other `warn`/`unverified` rows are advisory. `--no-databricks` skips CLI/identity
checks for offline use; the report it writes is never `ready` (identity `skipped` stays in
`blocking`), so it cannot authorize a wave. The report's top-level `identity` (`userName`,
`service_principal`, `host`) is what the wave manifest's `capabilities` must repeat; the workflow
refuses a manifest whose identity, host, catalogs, guard_mode or stop_mode differ from it.

## The hook probe (the one manual step)

Devin runs plugin hooks fail-open: if `hooks.json` is not loaded, nothing tells the session. The
doctor therefore reports `hook_platform_loaded: unverified` until you prove it, and the proof is
not your word: each report issues a fresh 8-hex nonce for the direct guard check, while the platform
probe's pending nonce is persisted in `.migration/.hook_probe_nonce` and reused for 8 hours until accepted.

1. Run, in the session shell, exactly the `probe_command` printed in that row of the report the
   doctor just wrote. It is an `echo` whose *text* looks like a Databricks write to
   `__dbx_guard_probe__<nonce>`, a catalog that is not allowlisted; it makes no Databricks call
   whatever happens. Run the probe with the shell tool's working directory set to the workspace, or
   prefixed with `cd <workspace> &&`; a bare probe run from elsewhere finds no allowlist and is not a
   valid result.
2. If the shell tool refuses it with a `dbx-migration-factory guard` reason naming
   `__dbx_guard_probe__<nonce>`, hooks are live: re-run the doctor with
   `--hook-probe-result blocked:<nonce>`. A nonce that does not match the pending nonce keeps the row
   `unverified`; a bare `blocked` is a
   CLI error.
3. If it prints the line, hooks are **not** applied in this session: re-run with
   `--hook-probe-result not-blocked`, which fails the run. Register a D10 (plugin not installed at
   the org level, or hooks disabled) and do not launch children until it is `blocked:<nonce>`.

## Checks

| id | fail means | source of truth |
|---|---|---|
| `workspace` | setup files missing | `1-migration_setup` |
| `stop_mode` | stop mode absent | `00_context.md` |
| `allowed_targets` | allowlist invalid or rejected | `hooks/dbx_guard.py` |
| `allowlist_committed` | allowlist/tolerances differ from HEAD | `git` |
| `allowlist_matches_contract` | catalogs differ from the wave contract | `allowed_targets.json` |
| `playbooks_in_sync` | installed playbooks differ from repo files, the lock is missing, or (orchestrator) the live export is missing/stale/mismatched/duplicated | `.migration/playbooks.lock.json`, `.migration/live_playbooks.json` |
| `hooks_files` | hook registration missing or malformed | plugin root |
| `hook_guard_functional` | probe is not blocked by the guard | `hooks/dbx_guard.py` |
| `hook_platform_loaded` | live probe unblocked or nonce unverified | this session |
| `lakebase_branch_create` | optional branch probe failed | Lakebase project permissions and parent expiry |
| `lakebase_target_grants` | optional DSN role lacks CREATE | Postgres privileges |
| `analytical_target_grants` | optional analytical schema: principal neither owns it nor has USE SCHEMA/CREATE TABLE/MODIFY/SELECT | Unity Catalog grants on the promotion schema |
| `official_databricks_plugin` | routed official skills are missing/unverified | `target-routing` |
| `recon_harness` | harness self-test/import failed | `data-reconciliation` |
| `recon_drivers` | required driver missing | harness extras |
| `delete_evidence` | CDC evidence is missing or unusable | `data-reconciliation` |
| `source_principal_read_only` | source principal can write, or grants unverified and not attested | source catalog views / `databricks grants get-effective` / `06_decisions.md` |
| `databricks_cli` | CLI is not on PATH | `databricks-core` |
| `databricks_auth_kind` | OAuth M2M env is absent | `target-routing` auth rules |
| `databricks_identity` | identity/host is invalid or human | `07_access_checklist.md` |
| `databricks_warehouse` | default warehouse is unavailable | `databricks-core` |

Reference details and factory placement: [references/checks.md](references/checks.md).

## Rules

- Never read or print secret values; the doctor reports which env var *names* are set, nothing else.
- Never "fix" a red row by widening permissions, switching to a personal identity, or editing
  `allowed_targets.json` outside a recorded decision.
- Never edit `09_capabilities.json` by hand; it is doctor output, and a child does not commit it.
- A source DSN that *can* write is a `fail` even if every write is opened `readonly=True`: the
  read-only guarantee is the principal's grants, verified here, not a driver flag.
- `--analytical-schema CATALOG.SCHEMA` checks the promotion schema; a red row's `detail` is the
  ready-to-paste `GRANT` statement or statements for the D10 request.
- `source_principal_read_only` statuses: `ok` when a privilege query or the Databricks CLI proves
  read-only; for `--source-family databricks` the check runs as the `--source-secret` credential
  (the `{server_hostname,http_path,access_token}` JSON the recon adapter uses, in a subprocess
  env carrying only that host, that token and `DATABRICKS_AUTH_TYPE=pat` — no inherited
  `DATABRICKS_*` variables) and reads `grants get-effective` on every in-scope
  catalog/schema/table on that host (ownership, direct or via a group, counts; a response
  without a well-formed `privilege_assignments` list is `unverified`) and fails on any
  privilege outside
  SELECT/USE_CATALOG/USE_SCHEMA/BROWSE/READ_VOLUME; `attested` when `--source-attested D-<id>`
  matches a `06_decisions.md` line containing the id, `source_principal_read_only`, `attested` and a
  `user:<id>` provenance (a `default-accepted` row fails: no human attested; rejected for families
  that have a query — Databricks included); `unverified` for the other
  families, and it blocks `ready`.
- `playbooks_in_sync` compares `.migration/playbooks.lock.json` against the repo playbook files
  (sha256 each): a stale, missing or unknown macro, or a playbook file absent from `playbooks/index.json`,
  fails the row. Re-running `install-dbx-factory` is the only fix; at setup (before
  the lock exists, `--role setup`) the row is `skipped` with the warning that the live library is
  unverified. With `.migration/live_playbooks.json` present (required under `--role orchestrator`,
  rejected after 15 minutes; written per run and gitignored) the row also compares each live
  playbook body against the repo file and fails on a mismatched, missing or duplicate macro —
  `ok` there proves live == repo, not just repo == last sync receipt.
