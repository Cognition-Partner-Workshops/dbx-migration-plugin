---
name: factory-doctor
description: Preflight for a DBX migration workspace. Verifies setup, hooks, contracts, identity, harness, source access, and optional target grants, then writes .migration/capabilities.json.
---

# factory-doctor

| `workspace` | required `.migration/` contract files are missing | rerun the intake step |
| `authorizations_file` | `authorizations.json` is malformed or the working copy differs from the committed one | fix the file or land the authorization PR |
| `allowed_targets` | allowlist is invalid or differs from `--expect-catalogs` | fix the recorded contract |
| `allowlist_committed` | allowlist or tolerances differ from the upstream ref (`origin/HEAD`, else `origin/main`/`master`, else `HEAD`) | merge the allowlist PR into the protected branch and `git fetch` |
| `hook_guard` | hooks are missing, direct guard fails, or the live probe is unverified/unblocked | load hooks and complete the nonce probe |
| `official_databricks_plugin` | routed official skills are missing or not visible locally | install/load the official skills |
| `recon_harness` | harness self-test/import or a required driver fails | install the harness extras |
| `type_map_audit` | a unit mapping declares a `target_type` the source family's `type_map.<family>.<target_kind>` forbids, or declares a top-level `family` other than the wave's `source.family` | fix the declared type or add the required `evidence` token; a spec's top-level `target_kind` (databricks\|lakebase) overrides --target-kind for that spec; a spec of another family belongs in its own pipeline's wave |
| `delete_evidence` | mapped CDC evidence is absent, incomplete, or unreadable | provide source CDC evidence; never enable it here |
| `source_principal_read_only` | source grants permit writes or cannot be verified | remove writes or record a plan decision attesting the source |
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
    [--source-attested <id>] [--http-path /sql/1.0/warehouses/<id>] \
    [--lakebase-project NAME --lakebase-parent-branch NAME] \
    [--lakebase-dsn ENV_VAR_NAME] [--lakebase-schema NAME] [--analytical-schema CATALOG.SCHEMA] \
    [--reuse-record PATH] [--out PATH]
```

A Databricks-family source is probed as the session identity (env-oidc or oauth-m2m, through the
recon harness's `credentials_provider`, with a socket timeout) on the SQL warehouse named by
`--http-path`, else `DATABRICKS_HTTP_PATH`. The doctor runs `databricks warehouses get <id>` on
that path before it connects, so a deleted warehouse fails `dictionary_readable` with
`DATABRICKS_HTTP_PATH names warehouse <id> which does not exist` instead of hanging. The effective
path is recorded as `http_path` in the report and signed into `wave-N.doctor.json`. A manifest may
pin the warehouse in `capabilities.http_path`; under `--wave` and `--reuse-record` the doctor uses
it, and a differing `--http-path` is an error. A child reusing the orchestrator's record reuses its
`dictionary_readable` row only for the warehouse that row was probed on (its recorded `http_path`);
on another warehouse it probes afresh. Pin the warehouse this way, never by editing `DATABRICKS_*`
in the environment (the guard treats that as an identity swap).

`--plugin-root` is optional. The guard accepts it only as the literal path of the running plugin
(not a shell variable or another plugin tree).

`--role orchestrator` is kept as the CLI value for the wave ticket's worker (the session that proves
the platform hook once per wave and signs `wave-N.doctor.json`); `--role setup` is the intake /
wave-plan worker before mappings exist. Setup runs the read-only source probe from
`--source-secret NAME --source-family F` alone; `--mapping` is not needed (the row is
principal-level; table grants are checked per unit later). `--role child` is a batch worker. Writes
`.migration/capabilities.json` and prints one line per row. Exit 0 means `ready`.
`--wave` takes a manifest in `<workspace>/.migration/waves/` (a relative path is the workspace's) and, when ready and not `--role child`,
writes the fan-out pointer `~/.dbx-migration-factory/current-wave.json` (`pointer -> <path>`).
With `--wave`, the orchestrator's per-unit rows (`type_map_audit`, `delete_evidence`,
`source_principal_read_only`, `dictionary_readable`) audit the manifest's units (the union of
`batches[].units`), not every spec under `.migration/units/`: sibling pipelines sharing that directory
are verified by their own wave doctors, and a wave unit without a spec is a hand-off failure. Without
`--wave` (setup, or an orchestrator before the manifest exists) every unit mapping is audited. Each
row records the unit set it covered as `data.units`.

A `--role child` run may pass `--reuse-record <manifest>.doctor.json` (the wave worker's signed
record, committed beside the wave manifest). When the record is the wave worker's, `ready`, signed
for the same manifest bytes, fresher than the manifest's `doctor_max_age` minutes (default 15),
signed for the `--expect-identity`/`--expect-host` principal, and its `inputs_sha` equals this
checkout's (sha256 over `.migration/units/**` and top-level contract files except `capabilities.json`,
by relative path), the rows marked `reusable` (`type_map_audit`, `delete_evidence`,
`dictionary_readable`) are taken from it — every other row, and the whole run when the record fails
any check, is computed fresh; a row is reused only when its recorded `data.units` covers the child's
`--unit`s, and `type_map_audit` only when its recorded `data.target_kind` also equals the child's
`--target-kind`. The signature's key is derivable from the manifest and the identity
(see the module docstring of `doctor.py`), so reuse is a cost policy, not trust: the rows that guard
the source and the secrets (`source_principal_read_only`, `named_secrets_exist`,
`recon_family_supported`), `databricks_identity`, `recon_harness` (its driver imports describe the
machine running the doctor), and the workspace/hook/allowlist rows always run in the child; each row
of the report carries `reusable: true|false`. The `doctor_record` row says
what happened; a rejected record prints `doctor record not reused: <why>` on stderr and does not
change the exit code. `--reuse-record` takes the source block from the manifest: pass the same
`--source-family`/`--source-secret`/`--param` values or none (different values are an error), and
it refuses `--wave`, `--no-databricks`, a non-child role, and a missing `--expect-identity`.
`ready` requires no `fail`, the security rows `hook_guard` and `databricks_identity` to have every
security sub-result at `ok`, and `source_principal_read_only` not `unverified` once mappings exist.

The wave ticket's worker proves the platform once per wave: `hook_platform_loaded` is live-probed
only by `--role orchestrator` and signed into `wave-N.doctor.json`. A child proves the guard script
and its identity (`hook_guard_functional`, `databricks_identity`) and inherits
`hook_platform_loaded` through `--reuse-record`, which is the normal child path. In a child report
every non-security `fail` is softened to an advisory `warn` (the wave worker gated it before
launch), so a child is ready when its two security controls are `ok` and no unit-mapping problem
blocks it; a missing or unreadable unit mapping still blocks, and so does a required security
sub-result that never ran. A `source_principal_read_only=fail` (a writable source principal) also
blocks a child; `unverified` stays advisory.
`authorizations_file` is `ok` when the file is absent; `warn` when the working copy differs from
the committed one (on origin/`base_branch` when run with `--wave` or `--reuse-record`, else the
protected branch; authorizations change only by PR); `fail` on malformed JSON or an entry without a
slug `id`, a `kind` in `legacy_write_authorized` / `gate_waived` / `merge_override` /
`source_principal_attested`, a non-empty `objects` list of strings, and `by: user:...`. A
`merge_override` entry also needs `wave: wave-<tag>` (the manifest file stem) and may scope
`blocker_classes` to the harness policy classes (`recon.report.BLOCKER_CLASSES` minus `data`; an
entry naming `data` fails the file).
`--source-attested <id>` yields an `ok` row whose data carries `attested`/`decision` for a family
with no privilege query (the plan decision is human-owned, so the doctor records it rather than
verifying it); it is rejected for oracle/sqlserver/postgres. For a `databricks` source the live
`grants get-effective` check still runs (its payload lists each privilege as `{privilege,
inherited_from_type, ...}`; `data.writable` records the origin of every write privilege), and the
id must be a committed `source_principal_attested` entry whose `objects` cover every in-scope
`catalog.schema.table` (or its catalog): the row is then `warn`, never `ok`, with `data.attested`
and `data.offending` (the live write privileges it waives); a live check that is `unverified` stays
`unverified` (an attestation waives known grants, not unreadable ones), and any other databricks
attestation is `fail`. The fan-out child prompt repeats the record's `--source-attested` id so a
child's own doctor run does not re-block on the waived grants. Sub-results live in
`data.sub_results`; `--no-databricks` leaves `databricks_identity=skipped` and never authorizes a
wave. The report identity, host, catalogs, and guard mode are the wave manifest's capability
contract.

## The hook probe

1. Run the exact `probe_command` in the `hook_guard` row — the guard blocks it from any shell or directory, not only inside the workspace.
2. A guard refusal naming `__dbx_guard_probe__<nonce>` proves hooks are live.
3. Re-run with `--hook-probe-result blocked:<nonce>`; the nonce is in the `hook_guard` row.
4. If the echo prints, re-run with `not-blocked`, record a plan blocker, and do not launch children.

One nonce per report serves both the functional row and the probe command, so `blocked:<nonce>`
can only come from a session that saw the live block. While `hook_platform_loaded` is `unverified`,
the run's last stdout line is `PROBE_COMMAND (run in the lead session's exec tool, not a sidekick
shell): <probe_command>` — run it in the lead session's exec tool, never a sidekick shell.

| row | fails when | fix |
|---|---|---|
| `workspace` | required `.migration/` contract files are missing | rerun the intake step |
| `workspace_layout` | a top-level `.migration/` entry is not a machine-file layout entry or legacy alias | move prose to the plan step's ticket or `plan.yaml` decisions/blockers |
| `authorizations_file` | `authorizations.json` is malformed or the working copy differs from the committed one | fix the file or land the authorization PR |
| `allowed_targets` | allowlist is invalid or differs from `--expect-catalogs` | fix the recorded contract |
| `allowlist_committed` | allowlist or tolerances differ from the upstream ref (`origin/HEAD`, else `origin/main`/`master`, else `HEAD`) | merge the allowlist PR into the protected branch and `git fetch` |
| `hook_guard` | hooks are missing, direct guard fails, or the live probe is unverified/unblocked | load hooks and complete the nonce probe |
| `official_databricks_plugin` | routed official skills are missing or not visible locally | install/load the official skills |
| `recon_harness` | harness self-test/import or a required driver fails | install the harness extras |
| `recon_family_supported` | the harness refuses the declared source family (asked of the same harness `recon_harness` ran) | reconcile through a family the harness supports |
| `delete_evidence` | mapped CDC evidence is absent, incomplete, or unreadable | provide source CDC evidence; never enable it here |
| `source_principal_read_only` | source grants permit writes or cannot be verified | remove writes or record a plan decision attesting the source |
| `dictionary_readable` | the source principal cannot read a catalog view the structural tier needs, or hides declared triggers | fix the principal's catalog visibility |
| `named_secrets_exist` | a Databricks secret name (`scope/key`) the manifest or a ticket references is missing or its scope is unreadable | create the named secret before the wave launches; a missing name blocks the wave, it is not a per-unit discovery |
| `databricks_identity` | CLI/auth/host/identity is missing, human, or mismatched | use the expected OAuth M2M principal and host |
| `lakebase_branch_create` | optional branch probe cannot create/delete a one-hour child | fix Lakebase project/parent permissions |
| `lakebase_target_grants` | optional DSN role lacks database/schema `CREATE` | grant target create permission |
| `analytical_target_grants` | optional promotion schema lacks required UC privileges | grant `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` |

Long form: `references/checks.md`.

The `workspace` check reads the legacy tolerance alias with a `warn` until 0.7.0 when the
canonical tolerance file is absent; both names together fail as ambiguous.
`workspace_layout` warns for unexpected top-level entries but never blocks readiness. Human-readable
material belongs on the plan step's ticket or in `plan.yaml` decisions/blockers, not in `.migration/`.
