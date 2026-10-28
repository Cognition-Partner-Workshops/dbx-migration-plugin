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
- The intake attachments the ticket links (named secrets, timeline, cutover principal holder,
  and on a Lakebase track the adjacent-session list: every pipeline and every follow-on
  session named at intake that will need its own `mig-*` branch).
- `skills/target-routing/SKILL.md` — auth rules and the bounded 5xx retry rule.
- `skills/data-reconciliation/SKILL.md` — the tolerance fields the recon harness reads.
- `skills/factory-doctor/SKILL.md` — the doctor posture and its record format.

Source DSN secret formats: oracle — `oracle://user:pass@host:port/service`,
`user/pass@host:port/service`, or JSON `{user, password, host, port, service}`; sqlserver — an
ODBC connection string; postgres — a libpq URI or conninfo. Save the secret in one of these forms;
never build it in the shell (the guard refuses reassigning a `legacy_sources` name).

## Procedure

1. Write `.migration/allowed_targets.json` before any source probe: `catalogs`,
   `legacy_sources`, `guard_mode`, `target_hosts`, `bundle_targets`, `lakebase_projects`,
   `lakebase_branches`, `run_mode`, `fixture_endpoints` (key semantics: `README.md`; who may
   widen it: `AGENTS.md`). `lakebase_branches` globs are bare branch ids (`mig-*`).
   On a Lakebase track, create every `mig-*` branch the plan declares now — one per pipeline
   plus one per adjacent session named at intake (`databricks postgres create-branch
   projects/<project> mig-<name>`, then `create-endpoint`) — and put each branch's endpoint
   host (`ep-*.database.<region>.cloud.databricks.com`, read from `get-endpoint`) into
   `target_hosts` as a literal in this same PR. The run must not need a mid-run allowlist
   edit for a branch the plan already names; a branch that appears later is a plan decision
   and a new PR. Never use a host glob: `target_hosts` entries are exact hosts.
2. Pin the plan's tolerance decision into `.migration/recon_tolerances.json`: exact-match or
   per-type/per-surface tolerances, row and aggregate thresholds, populations, nondeterminism,
   the legacy-query concurrency cap, live vs degraded mode, and the amendment procedure (every
   amendment preserves the old row and names its re-verification scope).
3. Propose one `plan.yaml` decision per target surface: `target-core`, `target-sql`,
   `target-pipeline`, `target-orchestration`, `target-consumer`, `target-lakebase`,
   `target-ml-scoring`, and `target-data-dependency`. Each selected option names the target and
   cites a reference implementation (which outranks a document) or a standards document, or says
   N/A with a reason. Attach the proposals to this ticket; the manager writes them into
   `plan.yaml`. Do not create a target-state file.
4. Probe legacy read, Databricks query, and target write (the write probe covers the promotion
   schema the target profile names); record each result as WORKS or BLOCKED with output in a
   table attached to the ticket. Report each BLOCKED item so the manager can file it as a plan
   blocker. Do not create a probe-table file.
5. Run `factory-doctor` in setup posture and commit `.migration/capabilities.json` with the
   `source_principal`; pass `--source-secret NAME --source-family F` (no `--mapping` is needed).
   The recorded identity is the one every worker session expects.
6. Record which secret holds each of the three principal tiers (`skills/target-routing/SKILL.md`).
7. Keep `.migration/.hook_probe_nonce` out of git (`.gitignore` entry).
8. Legacy writes a worker must make: list the exact objects on the ticket for the human's
   `.migration/authorizations.json` PR (`AGENTS.md`); never write that file yourself.

## Evidence the ticket must attach

- Committed `.migration/allowed_targets.json`, `recon_tolerances.json`, and
  `capabilities.json` on the protected branch; on a Lakebase track, the `get-branch` output
  for every declared `mig-*` branch and the endpoint host each one contributed to
  `target_hosts`.
- The doctor record and its identity.
- The WORKS/BLOCKED probe table with output, attached to the ticket; each BLOCKED item is
  reported for the manager to file as a plan blocker.
- The proposed `target-*` decisions with every selected option cited; the manager records them
  in `plan.yaml`.
