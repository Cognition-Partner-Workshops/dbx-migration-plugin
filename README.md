# dbx-migration-factory (Devin plugin)

Private, installable Devin plugin for Databricks migrations: source-dialect skills (Redshift,
Teradata BTEQ, Informatica XML), a reconciliation harness, a Lakebridge
wrapper, enforcement hooks, a preflight doctor, always-on guardrail rules, and a bootstrap skill
that imports the DBX playbook chain into the org.

The factory owns the *migration* problem: source dialects, lineage, reconciliation, fan-out, human
stops, and the analytical (Delta + Unity Catalog) and operational (Lakebase) tracks. Databricks
product knowledge is **not** duplicated here: the manifest declares the official
[`databricks` plugin](https://github.com/databricks/databricks-agent-skills) as a required plugin,
and the `target-routing` skill maps every Databricks-side step to the official skill that owns it
(DBSQL, Lakeflow Pipelines/Jobs/Connect, bundles, Unity Catalog, Lakebase, serverless).

The repo root *is* the plugin, so the repo itself is the installable unit.

```
.devin-plugin/plugin.json   plugin manifest (name, version, requiredPlugins -> official databricks plugin)
AGENTS.md                   always-on guardrails
hooks.json, hooks/          PreToolUse write-scope guard (fail closed, see below)
skills/                     one directory per skill
skills/_dialect-skill-template.md  spec + acceptance criteria for new source-dialect skills (child-session brief)
skills/lakebridge/          analyzer/transpiler invocation, dialect flags, seeded coverage table
skills/target-routing/      step -> official databricks skill map, plus migration-only deltas
skills/factory-doctor/      read-only preflight: CLI, identity + host, harness, .migration/ integrity,
                            committed allowlist/tolerances, source principal cannot write, hooks (nonce probe)
skills/install-dbx-factory/ bootstrap skill; carries the 14 DBX playbooks in playbooks/
```

## Install (private repo is fine)

A private repo works as-is: cloud sessions fetch it through the org's Git integration, and
CLI users fetch with their own git credentials — so both just need read access to this repo.
Make sure the Devin GitHub App installation includes this repo.

**Org/enterprise-wide (recommended):** at Settings → Resources → Plugins, add to the managed
manifest:

```json
{
  "requiredPlugins": ["Cognition-Partner-Workshops/dbx-migration-plugin"]
}
```

Pin a version instead of tracking the default branch:

```json
{
  "requiredPlugins": [
    { "source": "github", "repo": "Cognition-Partner-Workshops/dbx-migration-plugin", "ref": "v0.2.0" }
  ]
}
```

**Per user (CLI):**

```bash
devin plugins install Cognition-Partner-Workshops/dbx-migration-plugin
```

## After installing

Run the `install-dbx-factory` skill once per org in a dedicated setup session: it imports the
14 playbooks into the org playbook library and proposes the migration environment blueprint —
the two things a plugin cannot carry itself.

Then start an engagement with one front door: `!dbx_migrate_etl`, `!dbx_migrate_warehouse`,
`!dbx_migrate_code`, or `!dbx_migrate_oltp` (operational databases; splits the estate into a
Lakebase operational track and a Delta analytical track). Operational-track units reconcile with
`dbx-recon --mode transactional --target-kind lakebase`: both sides under a consistency window,
in-flight CDC rows tolerated up to `cdc_lag_max_s`, PK-set diff, lag/ordering, and
constraint/index/sequence parity on top of the set-based tiers. Deletes must be drained before
the run unless the mapping declares `delete_evidence` (SQL Server CDC first) that lets the
harness tell an in-flight delete from a stray target row; the factory verifies CDC is on and
readable but never enables it.

## Write-scope guard (`hooks/dbx_guard.py`)

The PreToolUse hook recognises the client a shell command runs and lets only known read shapes
through; everything else it recognises blocks. It is a no-op outside a workspace (no
`.migration/allowed_targets.json` up the tree) and reads its policy from that file:

```json
{
  "catalogs": ["migration_cat"],
  "legacy_sources": ["LEGACY_TD_DSN", "tdprod.corp"],
  "guard_mode": "block",
  "target_hosts": ["fixture-host", "LAKEBASE_MIGRATION_DSN"],
  "bundle_targets": ["migration", "dev"],
  "forbidden_bundle_targets": ["prod", "production"]
}
```

| key | required | meaning |
|---|---|---|
| `catalogs` | yes | Unity Catalog catalogs a Databricks write (`sql execute`, `spark-sql`, `tables delete mig_cat.s.t`, `fs rm dbfs:/Volumes/mig_cat/...`, `api delete .../tables/mig_cat.s.t`) may target; SQL is read from flags, positional text and positional `.sql` files alike. Also the catalog / database a generic-client write on a target host must resolve to (three-part name, `USE [CATALOG|DATABASE] x` before the write, else the one `-d`/`--dbname`/URI/conninfo database on the line or in a reconnect meta-command such as psql `\c db`, mysql `connect db`; two distinct databases resolve to none and block; `CREATE|DROP|ALTER DATABASE` and `GRANT ... ON DATABASE` always block). Also read by `dbx-recon`. |
| `legacy_sources` | no | secret names, hosts, DSNs and profiles of the legacy estate. A generic SQL client whose command mentions one, and every legacy-only client (`bteq`, `sqlplus`, `snowsql`, ...), is held to read shapes only; loaders always block. |
| `guard_mode` | no | `block` (default) or `warn` (approve with the reason attached). |
| `target_hosts` | no | hosts / DSN names a generic SQL client (`psql`, `sqlcmd`, `isql`, `mysql`, ...) may run a non-read statement against. Every host candidate on the line (`-h`/`-S`/`--host`, `PGHOST=`, a positional or `-d` URI / conninfo) or in the script's reconnect meta-commands (psql `\c db user host`, `\c 'host=...'`, sqlcmd `:connect host`, mysql `connect db host`) must be a literal in the list, and there must be at least one; the write's container must still be in `catalogs`. **Missing or empty: every generic-client write blocks.** |
| `bundle_targets` | no | targets `databricks bundle deploy\|run\|destroy` and `dbt run\|build\|seed` may use with a literal `-t/--target`. **Missing or empty: every deploy blocks.** |
| `forbidden_bundle_targets` | no | extra denylist on top of `bundle_targets`; default `["prod", "production"]`. |

Always blocked regardless of config: `databricks` commands outside the read allowlist whose
securable is not in `catalogs`, non-GET or bodied REST calls to a Databricks host, identity swaps
(`auth login`, `--profile`, `DATABRICKS_TOKEN=`... around a Databricks client, writes to
`.databrickscfg` / `~/.databricks/` / `~/.config/databricks/`, `auth token|env` which print the
token), `EXPLAIN ANALYZE <write>` and side-effecting functions (`nextval`, `pg_terminate_backend`,
`dblink`, `DBMS_*`, `OPENROWSET`, ...) and lock / transaction tokens that hold the source (`WITH (TABLOCKX|XLOCK|UPDLOCK|HOLDLOCK|SERIALIZABLE)`,
`FOR UPDATE|SHARE`, `LOCKING ... FOR WRITE|EXCLUSIVE`, `SET TRANSACTION READ WRITE`) on a legacy source -- `SET TRANSACTION
ISOLATION LEVEL <any>` / `READ ONLY`, `NOLOCK`-style hints and Teradata `LOCKING ... FOR ACCESS|READ` are reads --, writes under `.migration/` except
`recon/` and `waves/` (including the git forms that rewrite the whole working copy: `stash [push|save]`, `checkout|switch -f`, `reset
--hard|--merge|--keep`, `clean`, `restore .`), edits to the running guard's own plugin tree, a program the guard has no rule for in front of a SQL
client (`strace`, `chroot`, `firejail`, ...; `env`, `nice`, `nohup`, `timeout`, `sudo`, `ssh host`, `docker exec|run`, `kubectl exec` are modelled), and anything the guard cannot
read (unreadable scripts, `eval`, `$(...)`, decoder pipes, `sh -c "$X"`, `xargs`, a relative script after a `cd` it cannot resolve). Every relative
script or SQL file is read from the directory the command runs in (event `cwd`, `cd`, `pushd`, `env -C`, `git -C`). Python/JDBC/Spark programs are
only cheaply inspected for literal SQL; the factory-doctor's read-only-principal row is the control
for them. `hooks/tests/test_probe_table.py` is the red-team table: add a row there to pin a new shape.

The official `databricks` plugin is installed automatically as a dependency (tracking its default
branch). To pin it, add `"ref"` or `"sha"` to the `requiredPlugins` entry in
`.devin-plugin/plugin.json`. If the org's managed manifest uses `"forbiddenPlugins": ["*"]`, list
`databricks/databricks-agent-skills` explicitly; transitive dependencies are not exempt.
