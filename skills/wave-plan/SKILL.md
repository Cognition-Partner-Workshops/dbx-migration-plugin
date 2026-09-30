---
name: wave-plan
description: The worker ticket that writes and commits one wave manifest (`.migration/waves/wave-<N>.json`) from the approved plan — batches, write targets, gates, capability contract, pipeline declarations — and refreshes the signed doctor record for it. Load it when a ticket asks for a wave manifest.
---

# wave-plan

You write the wave manifest the fan-out workflow executes, and prove it launches. The manifest
is a committed artifact: the run log launches each manifest's bytes exactly once, and the
doctor signs `wave-<N>.doctor.json` over them.

## Ticket inputs (cold start)

- The approved `plan.yaml` and the `Run wave N` step id this manifest is for — it is the
  manifest's required `plan_step`.
- The pipeline analysis (`<Pipeline>_analysis.md`, per-unit `dependencies.json` and
  `mapping_spec.json`) and the committed `.migration/` contracts.
- `skills/migration-fanout/SKILL.md` — the manifest schema and every launch guard.
- `skills/target-routing/SKILL.md` — the one-active-update-per-pipeline rule.

## Procedure

1. Write `.migration/waves/wave-<N>.json` with: `wave`, `repo`, `child_skill`
   (`unit-migration`), `verify_skill` (`wave-verify`), `plan_step` (the plan step id, a
   lowercase slug), `width`, `breaker_threshold`, `auto_merge`, `base_branch` (the engagement
   feature branch; `main`/`master` need `trunk_base_decision`), `source` family/secret/params,
   `capabilities` (identity, host, catalogs, `ready`, `guard_mode` — matching
   `09_capabilities.json`), `verify_depth`, `max_minutes`, `cost_estimate`, `degraded` when the
   wave is export-only, `target_namespace` when the harness runs under a fixed catalog.schema,
   `resync` when identity/sequence drift needs a parent-owned reseed, and `secrets` for
   scope/key names a brief references.
2. Each batch: `id`, `units`, `write_targets` (from the dependency analysis's transitive
   writes, each taken to the target its `mapping_spec.json` names — the workflow refuses a
   differing list), `deploy_objects`, `brief`, `gates` (`{id, kind, status, evidence}` rows;
   `waived` rows carry `decision_id` of the plan decision that waived them), optional
   `max_minutes`, `secrets`, `verify_depth`, `lakeflow_pipelines`.
3. `merge_overrides`: when a unit cannot reach `merge_eligible` on evidence alone, one manifest
   entry per covered unit set — `{decision: <plan decision slug>, units: [...]}` — citing the
   plan decision that authorizes the merge. The child clears a batch only when exactly one
   entry covers all its units and it claims that entry's decision.
4. Pipelines: each batch lists the pipelines it updates as `lakeflow_pipelines` (`[]` when
   none). Two batches sharing one pipeline is a launch halt unless the wave is serial
   (`width` 1) or `serialized_pipelines` maps the pipeline to the plan decision slug that
   serializes it — then the workflow launches those batches in manifest order.
5. Sibling pipelines: name manifests `wave-<pipeline>-<N>.json`, list every pipeline with its
   wave count in each manifest's `pipelines` (identical in every sibling), and push them to the
   integration branch — the planning barrier; the workflow halts until origin holds every
   numbered manifest declaring the same `pipelines`.
6. Refresh the doctor: run `factory-doctor` for the wave (`--wave` posture with the hook
   probe) so it signs `.migration/waves/wave-<N>.doctor.json` over these exact manifest bytes;
   the workflow requires it fresher than `doctor_max_age` (default 15 minutes).
7. Commit the manifest (and regenerate nothing else under `.migration/`); a rerun is new
   manifest bytes plus a fresh signature — the run log halts a second launch of identical
   bytes.

## Evidence the ticket must attach

- The committed `wave-<N>.json` (or `wave-<pipeline>-<N>.json` set) and its signed
  `wave-<N>.doctor.json`.
- `pipeline_updates.py` output for the manifest (pass), proving `lakeflow_pipelines` and
  `serialized_pipelines` are consistent.
- The `plan_step` id the manifest records and the batch/unit coverage table.
