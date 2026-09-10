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
hooks.json, hooks/          PreToolUse write-scope guard (hard block) + PostToolUse auth/scope hint
skills/                     one directory per skill
skills/_dialect-skill-template.md  spec + acceptance criteria for new source-dialect skills (child-session brief)
skills/lakebridge/          analyzer/transpiler invocation, dialect flags, seeded coverage table
skills/target-routing/      step -> official databricks skill map, plus migration-only deltas
skills/factory-doctor/      read-only preflight: CLI, identity, harness, .migration/ integrity, hooks
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

The official `databricks` plugin is installed automatically as a dependency (tracking its default
branch). To pin it, add `"ref"` or `"sha"` to the `requiredPlugins` entry in
`.devin-plugin/plugin.json`. If the org's managed manifest uses `"forbiddenPlugins": ["*"]`, list
`databricks/databricks-agent-skills` explicitly; transitive dependencies are not exempt.
