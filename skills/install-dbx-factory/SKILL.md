---
name: install-dbx-factory
description: Bootstrap or re-sync the DBX Migration Factory in this org. Idempotently syncs the 14 DBX v1 playbooks into the org playbook library (create missing, update stale) and proposes the migration environment blueprint. Run in a dedicated setup session, when the user asks to set up or install the migration factory, and again after any plugin update that changes the playbook files.
triggers: ["user"]
---

# Install the DBX Migration Factory

You are bootstrapping the Databricks migration kit into this org. The plugin (skills, rules, harnesses) is already installed since you can read this skill. Your job is the two pieces a plugin cannot carry: the playbook library and the environment blueprint.

The library is org-scoped. Run it once per org to install, and again after any plugin update that changes the playbook files. When the live library already matches the repo files it makes no `create`/`update` call and only verifies and reports, so re-running is cheap and safe.

## Step 1: Sync the playbooks

The 14 playbook bodies are in this skill's `playbooks/` directory (find the skill's install path on disk first). `playbooks/index.json` lists each file with its title and macro, in sync order; sync exactly those files. `00_intake_template.md` is not a playbook: it is the pre-kickoff form the customer fills; commit it to the engagement docs repo (or hand it to the operator) so the front doors can consume it. File 13 (`dependency_resolution`) is an internal subroutine invoked by other playbooks; import it, but never present its macro as part of the operator surface. `references/contract.md` is not imported as a playbook: it is the process contract read by every playbook.

This step is idempotent — it is both the initial import and the re-sync after a plugin update, and it converges the live library to the repo files byte-for-byte:

1. List the live library: call the builtin `devin_playbook_manage` (via the `devin_mcp` tool: `command="call_tool"`, `tool_name="devin_playbook_manage"`) with `{"action":"list","first":200}`; follow the `after`/end_cursor pagination until exhausted. Group the records by macro. A repo macro with more than one live record is a halt: stop before any create/update and report every duplicate `playbook_id` for the operator to remove — the platform does not say which duplicate a `!macro` resolves to, so updating one of them proves nothing. Only then build macro -> {playbook_id, title, content}.
2. For each repo playbook, in `index.json` order, compute the sha256 of the file bytes. Then:
   - No live playbook has the macro: `{"action":"create","title":<index title>,"content":<file body verbatim>,"macro":<macro>}`.
   - The live `content` differs byte-for-byte from the file: `{"action":"update","playbook_id":<id>,"title":<index title>,"content":<file body>,"macro":<macro>}` (v3 update is full-replace, so always pass title+content+macro). Never edit, summarize, or re-wrap a body.
   - Identical: no call. If the tool returned the live `content` truncated (the `get` output is capped, and every playbook here is longer than the cap), you cannot prove identity: treat it as differing and update — the update is a full replace of the same bytes, so it is harmless when nothing changed.
   Pass long content via a `file:///` path — the `devin_mcp` tool substitutes it.
3. If any create/update needs an approval that is not granted, or fails: stop and report the exact stale macros — never bypass.

If the builtin playbook tools are unavailable or lack permission, fall back to the Playbooks REST API (`POST /v3/playbooks` for org scope) with a service-user key the user provides, and if that is also unavailable, attach the playbook files to a message and ask the user to import them via the UI. Never silently skip a playbook.

## Step 2: Propose the environment blueprint

Propose an update to the org (or repo, if the user names one) environment blueprint so migration sessions start with the toolchain ready:
- Databricks CLI installed, authenticated as the migration service principal per the Databricks auth guide's Option A (OIDC token federation, preferred) or Option B (OAuth M2M) blueprint snippet — env vars only, never values.
- Python with the recon harness installed: `pip install -e "skills/data-reconciliation/harness[databricks,<source family>]"`, then `dbx-recon selftest` must print PASS. Add `databricks-sdk`.
- (optional, from the dialects plugin) Databricks Labs Lakebridge, if the `lakebridge` skill from `dbx-migration-dialects` (`skills-extra/`) is in use.
- Any source-system client the engagement needs (note as TODO until the front-door intake names the stack).

Use your environment-config tools to submit this as a blueprint suggestion for the user to approve. Do not claim the blueprint is active until the user approves it.

## Step 3: Verify and report

1. List org playbooks again (`devin_playbook_manage {"action":"list"}`, then `get` per macro) and confirm every one of the 14 macros resolves to exactly one record whose live content matches the repo file byte-for-byte; when `get` returns the content capped, the macro passes only if this run's create/update for it returned success (a full replace of the repo bytes) and the returned prefix matches the file. Report any macro that fails.
2. Report to the user: playbooks created/updated/unchanged (with macros), blueprint suggestion status, and the one-line operator guide: start an engagement with `!dbx_migrate_etl`, `!dbx_migrate_warehouse`, `!dbx_migrate_code`, or `!dbx_migrate_oltp`, then answer the five approval stops.

## Forbidden
- Do not modify the playbook bodies during import.
- Do not create playbooks at enterprise scope unless the user explicitly asks.
- Do not start any migration work in this session; it is setup only.
