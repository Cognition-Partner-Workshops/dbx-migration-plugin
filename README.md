# dbx-migration-factory (Devin plugin)

Private Devin plugin that runs a database migration to Databricks (Delta for the analytical track,
Lakebase Postgres for the OLTP track) as a **strangler fig**: the estate is replaced one
consumer-facing capability at a time while the two systems coexist. A migration is a set of
**slices** — a slice owns source tables, routines, jobs and the consumers that read it, plus its
target track (`delta` or `lakebase`); shared reference tables form `slice-0`, always first. Each
slice runs the same five steps and ends in one PR whose merge authority is one `dbx-recon` parity
verdict from an independent verifier session. Design spec: `docs/design-0.7.md`.

The repo root *is* the plugin:

```
.devin-plugin/plugin.json   manifest (name, version, requiredPlugins -> official databricks plugin)
AGENTS.md                   the eight always-on rules
hooks.json, hooks/          PreToolUse guard for the three irreversible classes
tools/                      doctor (preflight), slices (contract check), fanout (launch/merge)
harness/                    dbx-recon: the parity engine and merge authority
skills/plan                 manager: assess -> slices -> plan.yaml -> policy PR -> doctor -> tickets
skills/analyze              one ticket: census + consumer map -> slices.json proposal
skills/migrate              one slice child: access -> convert -> prove -> shadow -> route
skills/verify               independent verifier -> the dbx-recon/verify PR status check
skills/dialects/<family>/   oracle, sqlserver, postgres, databricks source-dialect skills
skills/target-routing       step -> official databricks plugin skill table + auth rules
.migration/policy.json      the one committed contract (targets, principal, tolerances)
.migration/slices.json      the slices, written by analyze, approved with the plan
```

## Install (private repo is fine)

A private repo works as-is: cloud sessions fetch it through the org's Git integration, and CLI
users fetch with their own git credentials. Make sure the Devin GitHub App installation includes
this repo.

**Org/enterprise-wide (recommended):** at Settings → Resources → Plugins, add to the managed
manifest:

```json
{ "requiredPlugins": ["Cognition-Partner-Workshops/dbx-migration-plugin"] }
```

Pin a version instead of tracking the default branch:

```json
{
  "requiredPlugins": [
    { "source": "github", "repo": "Cognition-Partner-Workshops/dbx-migration-plugin", "ref": "v0.7.0" }
  ]
}
```

**Per user (CLI):** `devin plugins install Cognition-Partner-Workshops/dbx-migration-plugin`.

The official `databricks` plugin is installed automatically as a dependency, pinned by `"sha"` in
`.devin-plugin/plugin.json`. The pin must equal the sha the org's managed manifest pins the same
plugin to, or installation fails with "Conflicting version pins"; bump both in one change. If the
managed manifest uses `"forbiddenPlugins": ["*"]`, list `databricks/databricks-agent-skills`
explicitly. Databricks auth: `skills/target-routing/SKILL.md`.

Dialects not in core (Teradata BTEQ, Informatica PowerCenter XML, SQL Server SSIS packages, the
Lakebridge accelerator notes) live in `skills-extra/`, a separate plugin
(`dbx-migration-dialects`) the core manifest never loads.

## The three stops

The human is asked exactly three kinds of question, nowhere else:

| Stop | What the human approves | Where |
|---|---|---|
| 1 Plan | slices and their order, targets, tolerances, coexistence window, cutover owner, `routing` per slice, access prerequisites | Plan view `plan.yaml`; merging the policy PR is the approval |
| 2 Canary | the first non-zero slice's PR: conversion pattern, evidence shape, bill | PR review + Plan view gate `g-canary` |
| 3 Route | repoint of one slice's consumers (`d-route-<slice>`), run by the customer-held principal | Plan view decision |

Everything else merges on evidence or halts with a card whose last line is the exact reply that
resumes it.

## How a slice runs

`skills/migrate` is embedded in every slice child's prompt. Five steps:

1. **Access** (slice-0 only): make `policy.source.access` resolvable — federation foreign
   catalog, Lakeflow Connect landed schema, or an export volume loaded in FK order — through the
   official databricks skills, prove it with row counts, record `access.json`.
2. **Convert**: Lakebridge `transpile --source-dialect <family>` for static SQL; procedural code
   by hand against the dialect skill's traps table; one DAB per slice, target `migration`,
   schedules `PAUSED`.
3. **Prove**: `dbx-recon run --unit <slice>` to `parity: PASS` (retry cap 3), then one PR with
   parity on line 1, the findings table, and the bill last.
4. **Shadow**: a paused Lakeflow job that replays the slice on the legacy cadence with
   `mode: continuous` recon; `green_cycles` consecutive PASS ticks gate the route.
5. **Route** (after STOP 3): route packet + the exact repoint change; the customer-held principal
   applies it. Legacy stays as rollback inside `rollback_window_h`.

An independent `verify` session re-runs recon on the PR head and posts the `dbx-recon/verify`
commit status; `fanout.py` merges only at the verified head, and `auto_merge` is false only for
the canary slice.

## How safety works

Safety lives in platform identity and isolation, not in the plugin: every session runs as the
engagement's migration principal, the doctor's `isolation` check proves it holds write or
ownership privilege on nothing but `policy.targets` and `SELECT`-class on `source.access`, and
`fanout.py` refuses to launch while that check is not `ok`. The PreToolUse guard
(`hooks/guard.py`) is the second layer and enforces only the three irreversible classes the
platform cannot — a legacy-only client used for a write, a deploy to a production target, an edit
to the credential store or the running plugin tree; everything else is `warn` and logged. A guard
block is a halt to report, never something to re-shape or retry. `.migration/policy.json` and
`slices.json` are fenced by CODEOWNERS on the protected base branch plus a
`protected_files_tampered` check on every slice PR, and the guard is a no-op in any checkout with
no committed policy, so it never fires in an unrelated repo.
