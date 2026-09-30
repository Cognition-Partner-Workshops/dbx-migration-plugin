---
name: workspace-setup
description: The phase-1 worker ticket that turns the approved plan's intake decisions into the committed .migration/ workspace (allowed_targets.json, recon tolerances, capabilities) and proves every access path with WORKS/BLOCKED probes. Load it for the `workspace-setup` plan step.
---

# workspace-setup

You write the committed `.migration/` workspace every later ticket reads, and you prove the
access paths before any wave plans. Nothing here is invented: every field cites the plan's
decisions, a probe result, or a referenced standards document.

## Ticket inputs (cold start)

- The approved `plan.yaml` phase-1 decisions: target catalogs, source access path, coexistence
  mode, guard_mode, recon tolerance set, base branch, and any legacy-write objects a
  `legacy_write_authorized` decision names.
- The intake attachments the ticket links (named secrets, timeline, cutover principal holder).
- `skills/target-routing/SKILL.md` — auth rules and the bounded 5xx retry rule.
- `skills/data-reconciliation/SKILL.md` — the tolerance fields the recon harness reads.
- `skills/factory-doctor/SKILL.md` — the doctor posture and its record format.

## Procedure

1. Write `.migration/allowed_targets.json` before any source probe: `catalogs`,
   `legacy_sources`, `guard_mode`, `target_hosts`, `bundle_targets`, `lakebase_projects`,
   `lakebase_branches`, `run_mode`, `fixture_endpoints` (key semantics: `README.md`; who may
   widen it: `AGENTS.md`).
2. Pin the plan's tolerance decision into `.migration/03_recon_tolerances.json`: exact-match or
   per-type/per-surface tolerances, row and aggregate thresholds, populations, nondeterminism,
   the legacy-query concurrency cap, live vs degraded mode, and the amendment procedure (every
   amendment preserves the old row and names its re-verification scope).
3. Write the target-state artifact the workers consume: per surface (CORE, SQL, PIPELINE,
   ORCHESTRATION, CONSUMER, LAKEBASE, ML-SCORING, DATA/DEPENDENCY) cite a reference
   implementation or a standards document, or mark the surface N/A with a reason. A reference
   implementation outranks a document; cite real files.
4. Probe legacy read, Databricks query, and target write (the write probe covers the promotion
   schema the target profile names); record each result as WORKS or BLOCKED with output, and
   report every blocked item on the ticket so the manager can fire it as a plan blocker.
5. Run `factory-doctor` in setup posture and commit `.migration/09_capabilities.json` with the
   `source_principal`; the recorded identity is the one every worker session expects.
6. Record which secret holds each of the three principal tiers (`skills/target-routing/SKILL.md`).
7. Keep `.migration/.hook_probe_nonce` out of git (`.gitignore` entry).
8. Legacy writes a worker must make: list the exact objects on the ticket for the human's
   `.migration/authorizations.json` PR (`AGENTS.md`); never write that file yourself.

## Evidence the ticket must attach

- Committed `.migration/allowed_targets.json`, `03_recon_tolerances.json`, and
  `09_capabilities.json` on the protected branch.
- The doctor record and its identity.
- The probe results table: WORKS/BLOCKED per access path, with output, plus the list of items
  handed to the manager as blockers.
- The target-state artifact with every field cited.
