---
name: intake
description: Manager-facing intake checklist — what the human's request and attachments must contain, and where each answer lands in plan.yaml (phase-1 decisions, blockers, phase-skeleton changes). Load it when scoping a new migration engagement, before plan.yaml exists.
---

# intake

You are the manager session scoping a migration engagement. This skill is a checklist of what
you must find out and where in `plan.yaml` it lands — not a form to send the human, not a setup
ticket. You do not write `.migration/` files yourself; a `workspace-setup` worker ticket does
that after the human approves the plan. Nothing is imported from an org library anywhere — the
plugin arrives through the plugin system and every contract lives in the plan or a ticket.

## What the request and its attachments should contain

- Source family and engine versions (incl. driver/ODBC needs), and the estate type from
  `references/estate-types.md` (ETL, warehouse, code/ML, OLTP).
- Target side: Unity Catalog catalog names for the migration catalog and cutover catalog(s),
  or Lakebase project/branch for the operational track.
- Coexistence posture: how long systems run in parallel and which parallel-run tier applies.
- Correctness contract: exact match or the deviations (per-type/per-surface tolerances, row and
  aggregate thresholds, nondeterminism rules, legacy-query concurrency cap, live vs degraded
  mode). Deviations go in as an `important` plan decision, never a silent default.
- Scope: first pipeline (or "let the inventory recommend"), boundary, and explicit exclusions.
- Access posture: every credential as a **named secret** (`LEGACY_DSN`, `MIG_DSN`, service
  principal, notification route) — never a value; whether the source read can be direct,
  federated, exported, or only customer-run.
- The connected repo and the protected/base branch the engagement works from.
- Timeline: hard dates, freeze windows, and lead-time items the customer must start now.
- Who holds the cutover principal and how cutover authorization will be recorded.

## Old intake field -> where it lands in the plan

| Intake answer | Lands in |
|---|---|
| Source system, versions, dialect | Estate-type routing (below); dialect skill on every worker ticket |
| Named secret for legacy read-only principal | Phase `foundation` blocker `{kind: secret, secret: <name>}` |
| Named secret for the migration principal | Phase `foundation` blocker `{kind: secret}` + `09_capabilities.json` identity |
| Cutover principal holder | `cutover` phase blocker `{kind: secret}` + the `authorized` gate |
| First pipeline / scope / exclusions | Phase-1 decisions `first-pipeline` and `scope-exclusions` |
| Coexistence mode | Phase-1 decision `coexistence-mode` (recommended option `selected`) |
| Correctness deviations | Phase-1 decision `recon-tolerances` — `important: true, selected: null` — then `03_recon_tolerances.json` |
| Access posture (federation / snapshot / export) | Phase-1 decision `source-access` |
| Target catalogs / Lakebase project | Phase-1 decision `target-catalogs` → `allowed_targets.json` |
| Notification route (Slack/webhook) | Blocker `{kind: mcp}` or `{kind: secret}` on the step that posts |
| Connected repo / base branch | Blocker `{kind: repo, repo: <owner>/<name>}` + `base-branch` decision (`selected`) |
| Timeline / freeze windows | `depends_on` ordering and gates on the cutover phase |
| Customer-side lead-time work (network path, CDC enablement) | Blockers with no `check` — a human owns them |

## Blueprint proposal (relayed to the human)

When the org has no blueprint yet, propose: install the Databricks CLI, `uv`, and the source
drivers the estate needs; add `.hook_probe_nonce` to `.gitignore`. The plugin itself arrives
via the plugin system — do not ask for any import, submodule, or library copy step.

## Estate-type routing -> `references/estate-types.md`

Each estate type changes the phase skeleton: ETL adds an orchestration track and
restart/quarantine decisions; warehouse adds federation-vs-export source decisions and
consumer/BI gates; code/ML splits data movement, application code, training, and scoring into
separate steps with bit-stability probes; OLTP adds a Lakebase track, CDC blockers, and
routine-parity constraints (a denied EXECUTE grant is a blocker, not a skip). The reference
lists which `skills-extra/` dialect skills apply per type.

## Explicitly out of scope

- The manager writes `plan.yaml` — never `allowed_targets.json`, tolerances, capabilities, or
  authorizations; those are the `workspace-setup` ticket's, entered on the protected branch.
- Legacy write authorizations are human PRs into `.migration/authorizations.json`; intake only
  names the objects the plan's `legacy_write_authorized` decision will cover.
