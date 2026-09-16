---
name: install-dbx-factory
description: Bootstrap or re-sync the DBX Migration Factory in this org. Idempotently syncs the 14 DBX v1 playbooks into the org playbook library (create missing, update stale), writes .migration/playbooks.lock.json that factory-doctor verifies, and proposes the migration environment blueprint. Run in a dedicated setup session, when the user asks to set up or install the migration factory, and again after any plugin update that changes the playbook files.
triggers: ["user"]
---

# Install the DBX Migration Factory

You are bootstrapping the Databricks migration kit into this org. The plugin (skills, rules, harnesses) is already installed since you can read this skill. Your job is the two pieces a plugin cannot carry: the playbook library and the environment blueprint.

The library is org-scoped; the lock this skill writes is a receipt for one engagement workspace (`.migration/` of the repo you run in). Run it once per org to install, and again in every engagement repo that has no `.migration/playbooks.lock.json` or a red `playbooks_in_sync` row (`1-migration_setup` step 7 does this before its doctor run): when the live library already matches the repo files it makes no `create`/`update` call and only verifies and writes the lock, so re-running in each engagement is cheap and safe. An org re-sync does not refresh other engagements' locks; their next doctor run goes red and its `install-dbx-factory` re-run writes a fresh receipt.

## Step 1: Sync the playbooks and write the lock

The 14 playbook bodies are in this skill's `playbooks/` directory (find the skill's install path on disk first). `playbooks/index.json` lists each file with its title and macro, in sync order; sync exactly those files. `00_intake_template.md` is not a playbook: it is the pre-kickoff form the customer fills; commit it to the engagement docs repo (or hand it to the operator) so the front doors can consume it. File 13 (`dependency_resolution`) is an internal subroutine invoked by other playbooks; import it, but never present its macro as part of the operator surface. `references/contract.md` is not imported as a playbook: it is the process contract read by every playbook.

This step is idempotent — it is both the initial import and the re-sync after a plugin update, and it converges the live library to the repo files byte-for-byte:

1. List the live library: call the builtin `devin_playbook_manage` (via the `devin_mcp` tool: `command="call_tool"`, `tool_name="devin_playbook_manage"`) with `{"action":"list","first":200}`; follow the `after`/end_cursor pagination until exhausted. Group the records by macro. A repo macro with more than one live record is a halt: stop before any create/update, do not write the lock, delete any pre-existing `.migration/playbooks.lock.json` and commit that deletion (a receipt must not survive a live library that cannot be verified), and report every duplicate `playbook_id` for the operator to remove — the platform does not say which duplicate a `!macro` resolves to, so updating one of them proves nothing. Only then build macro -> {playbook_id, title, content}.
2. For each repo playbook, in `index.json` order, compute the sha256 of the file bytes. Then:
   - No live playbook has the macro: `{"action":"create","title":<index title>,"content":<file body verbatim>,"macro":<macro>}`.
   - The live `content` differs byte-for-byte from the file: `{"action":"update","playbook_id":<id>,"title":<index title>,"content":<file body>,"macro":<macro>}` (v3 update is full-replace, so always pass title+content+macro). Never edit, summarize, or re-wrap a body.
   - Identical: no call. If the tool returned the live `content` truncated (the `get` output is capped, and every playbook here is longer than the cap), you cannot prove identity: treat it as differing and update — the update is a full replace of the same bytes, so it is harmless when nothing changed.
   Pass long content via a `file:///` path — the `devin_mcp` tool substitutes it.
3. After the loop, build the lock data in memory only:
   `{"<macro>": {"sha256": "<hex>", "repo_file": "<file name>", "installed_at": "<UTC ISO-8601 of this run>"}}`
   for every playbook — created, updated, or unchanged (`installed_at` is this run for all, so the lock is the record of the last confirmed sync). Do not write it yet: `.migration/playbooks.lock.json` is written in Step 3 only after the live re-read confirms every macro. `factory-doctor`'s `playbooks_in_sync` row compares this lock to the repo files, and re-running this skill is the only fix for a red row.
4. If any create/update needs an approval that is not granted, or fails: stop, do NOT write the lock, delete any pre-existing `.migration/playbooks.lock.json` (a stale receipt must not survive a failed sync), and report the exact stale macros — never bypass.

If the builtin playbook tools are unavailable or lack permission, fall back to the Playbooks REST API (`POST /v3/playbooks` for org scope) with a service-user key the user provides, and if that is also unavailable, attach the playbook files to a message and ask the user to import them via the UI. Never silently skip a playbook.

## Step 2: Propose the environment blueprint

Propose an update to the org (or repo, if the user names one) environment blueprint so migration sessions start with the toolchain ready:
- Databricks CLI installed and configured for auth via secret names (never values).
- Python with the recon harness installed: `pip install -e "skills/data-reconciliation/harness[databricks,<source family>]"`, then `dbx-recon selftest` must print PASS. Add `databricks-sdk`.
- Databricks Labs Lakebridge installed (see the `lakebridge` skill for the install command).
- Any source-system client the engagement needs (note as TODO until the front-door intake names the stack).

Use your environment-config tools to submit this as a blueprint suggestion for the user to approve. Do not claim the blueprint is active until the user approves it.

## Step 3: Verify and report

1. List org playbooks again (`devin_playbook_manage {"action":"list"}`, then `get` per macro) and confirm every one of the 14 macros resolves to exactly one record whose live content hashes to the sha256 in the in-memory lock data; when `get` returns the content capped, the macro passes only if this run's create/update for it returned success (a full replace of the repo bytes) and the returned prefix matches the file. Only when all 14 pass, write `.migration/playbooks.lock.json` (sorted keys, 2-space indent) and commit it with the workspace. If any macro fails, do not write the lock, delete any pre-existing one, and report the failing macros.
2. Report to the user: playbooks created/updated/unchanged (with macros), lock written, blueprint suggestion status, and the one-line operator guide: start an engagement with `!dbx_migrate_etl`, `!dbx_migrate_warehouse`, `!dbx_migrate_code`, or `!dbx_migrate_oltp`, then answer the five approval stops.

## Forbidden
- Do not modify the playbook bodies during import.
- Do not create playbooks at enterprise scope unless the user explicitly asks.
- Do not start any migration work in this session; it is setup only.
