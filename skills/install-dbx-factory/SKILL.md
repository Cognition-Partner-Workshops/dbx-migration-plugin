---
name: install-dbx-factory
description: Bootstrap the DBX Migration Factory in this org. Imports the 13 DBX v1 playbooks into the org playbook library and proposes the migration environment blueprint. Run once per org, in a dedicated setup session, when the user asks to set up or install the migration factory.
triggers: ["user"]
---

# Install the DBX Migration Factory

You are bootstrapping the Databricks migration kit into this org. The plugin (skills, rules, harnesses) is already installed since you can read this skill. Your job is the two pieces a plugin cannot carry: the playbook library and the environment blueprint.

## Step 1: Import the playbooks

The 13 playbook bodies are in this skill's `playbooks/` directory (find the skill's install path on disk first). Titles and macros are defined in `playbooks/0-README.md` in the Files table. `0-README.md` itself is documentation, not a playbook; import files 1 through 13. `00_intake_template.md` is also not a playbook: it is the pre-kickoff form the customer fills; commit it to the engagement docs repo (or hand it to the operator) so the front doors can consume it. File 13 (`dependency_resolution`) is an internal subroutine invoked by other playbooks; import it, but never present its macro as part of the operator surface.

For each playbook, in numeric order:
1. Read the file body verbatim. Do not edit, summarize, or re-wrap it.
2. Create an org-level playbook with the exact title (e.g. `[DBX v1] Migration Setup`) and macro (e.g. `!dbx_migration_setup`) from the README table, using your builtin playbook-management tools (list them first to confirm the exact tool names and schemas).
3. If a playbook with the same macro already exists, do not create a duplicate: this is a refresh after a plugin update, so update the existing playbook's body in place with the new file contents, and report the list of updated macros at the end.

If the builtin playbook tools are unavailable or lack permission, fall back to the Playbooks REST API (`POST /v3/playbooks` for org scope) with a service-user key the user provides, and if that is also unavailable, attach the playbook files to a message and ask the user to import them via the UI. Never silently skip a playbook.

## Step 2: Propose the environment blueprint

Propose an update to the org (or repo, if the user names one) environment blueprint so migration sessions start with the toolchain ready:
- Databricks CLI installed and configured for auth via secret names (never values).
- Python with the recon harness installed: `pip install -e "skills/data-reconciliation/harness[databricks,<source family>]"`, then `dbx-recon selftest` must print PASS. Add `databricks-sdk`.
- Databricks Labs Lakebridge installed (see the `lakebridge` skill for the install command).
- Any source-system client the engagement needs (note as TODO until the front-door intake names the stack).

Use your environment-config tools to submit this as a blueprint suggestion for the user to approve. Do not claim the blueprint is active until the user approves it.

## Step 3: Verify and report

1. List org playbooks and confirm all 13 macros resolve.
2. Report to the user: playbooks imported (with macros), blueprint suggestion status, and the one-line operator guide: start an engagement with `!dbx_migrate_etl`, `!dbx_migrate_warehouse`, or `!dbx_migrate_code`, then answer the five approval stops.

## Forbidden
- Do not modify the playbook bodies during import.
- Do not create playbooks at enterprise scope unless the user explicitly asks.
- Do not start any migration work in this session; it is setup only.
