# dbx-migration-factory (Devin plugin)

Private, installable Devin plugin for Databricks migrations: target-convention skills,
reconciliation and prediction-parity harnesses, a Lakebridge wrapper, source-dialect skills
(Redshift, Teradata BTEQ, Informatica XML), always-on guardrail rules, and a bootstrap skill
that imports the DBX v1 playbook chain into the org.

The repo root *is* the plugin, so the repo itself is the installable unit.

```
.devin-plugin/plugin.json   plugin manifest (name, version, description)
AGENTS.md                   always-on guardrails
skills/                     one directory per skill
skills/install-dbx-factory/ bootstrap skill; carries the 13 DBX v1 playbooks in playbooks/
```

## Install (private repo is fine)

A private repo works as-is: cloud sessions fetch it through the org's Git integration, and
CLI users fetch with their own git credentials — so both just need read access to this repo.
Make sure the Devin GitHub App installation includes this repo.

**Org/enterprise-wide (recommended):** at Settings → Resources → Plugins, add to the managed
manifest:

```json
{
  "requiredPlugins": ["Cognition-Partner-Workshops/dbx-migration-factory"]
}
```

Pin a version instead of tracking the default branch:

```json
{
  "requiredPlugins": [
    { "source": "github", "repo": "Cognition-Partner-Workshops/dbx-migration-factory", "ref": "v0.1.0" }
  ]
}
```

**Per user (CLI):**

```bash
devin plugins install Cognition-Partner-Workshops/dbx-migration-factory
```

## After installing

Run the `install-dbx-factory` skill once per org in a dedicated setup session: it imports the
13 playbooks into the org playbook library and proposes the migration environment blueprint —
the two things a plugin cannot carry itself.
