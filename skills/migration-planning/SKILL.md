---
name: migration-planning
description: The manager session that turns intake answers into a decided, costed migration plan (plan.yaml) plus the wave manifests and committed .migration/ contracts the worker tickets run from. Load it when the request asks for the plan, wave manifests, or a planning decision.
---

# migration-planning

You are the manager session for a migration engagement. You do not convert code: you write
`plan.yaml`, decide every crossing, file the `.migration/` contracts, and cut the worker tickets.
Nothing you write launches until the human approves `plan.yaml` in the Plan view (`AGENTS.md`).

## Ticket inputs (cold start)

- The intake artifacts: committed `.migration/` workspace (`allowed_targets.json`,
  `recon_tolerances.json`, `capabilities.json`), the target profiles, the estate inventory
  and the chosen pipeline boundary.
- The dialect skill's per-unit analysis under `.migration/units/<unit_id>/` (`dependencies.json`,
  `mapping_spec.json`) or a blocker when it is absent.
- The board's current `plan.yaml` (read it; edit it, never replace it blindly).
- `references/decisions.md` — the decision/blocker template catalog (D1–D10) with the option
  shapes `plan.yaml` decisions use.
- `references/phase1-steps.md` — the canonical phase/step skeleton a new plan starts from.
- `references/plan.example.yaml` — a valid example; `plan.yaml` must pass
  `devin_common.migration_plan.validate`.

## Procedure

1. Run `factory-doctor` with the hook probe, expected catalogs, the source secret/parameters and
   all unit mappings; refresh `capabilities.json`. A `fail` row, an unverified hook state, or
   an uncommitted allowlist/tolerance is a plan blocker (`kind: repo`/`secret`/`mcp` on the step
   that needs it) — a red `named_secrets_exist` row means the secret must be created before the
   wave launches, never left for a worker to discover. On a cold start with no `.migration/`
   workspace yet, the doctor's missing `allowed_targets.json` / `recon_tolerances.json` rows
   are the `workspace-setup` step's deliverables, not blockers: write the plan with that step
   first in `foundation` and record as blockers only the access and lead-time gaps the doctor's
   other rows show.
2. Fill `plan.yaml`: `title`, `summary`, phases, steps with `depends_on` between step ids,
   `decisions` with `options` for every dependency crossing (template catalog:
   `references/decisions.md`), `gates` for the checks a wave or the engagement must pass, and
   `blockers` for every access/lead-time gap with its machine check. `important: true` marks the
   decisions the human should weigh before approving; its `selected` stays null until they do.
3. Decide every dependency: fire each lead-time request (network path, service principal,
   secrets, sample-data approval) now. Every fired request is a `plan.yaml` blocker carrying the
   exact request text and its check; record the reply or pending state on that blocker. Do not
   create a separate register.
4. Specify wave-0 scaffolding: catalog/schemas, federation or backfill, CI, recon harness,
   bundle/job shells. Where the load posture is materialized backfill, classify every table with
   the load-posture table in `skills/data-reconciliation/SKILL.md` and give each class beyond
   CTAS its own line in the wall-clock math.
5. Write the schedule: unit batches, branches, profiles, recon rows, isolated namespaces,
   idempotency, base branch, width, legacy-query cap, breaker threshold, serial floor, reviewer
   throughput, parallel-run tier, and projected cost. Batch small units at 4–5, complex at 1–2;
   XL units split decision-first; wave 1 is the pilot at width ≤ 5.
6. Have the `wave-plan` worker ticket write each `.migration/waves/wave-<N>.json` per its SKILL.
   Sibling pipelines (no shared write targets or source objects) get their
   `wave-<pipeline>-<N>.json` set pushed before the plan is presented (the planning barrier,
   `skills/wave-plan/SKILL.md`).
7. State the recon plan per unit: gates, live/snapshot source, threshold/sampling rule,
   determinism rule, projected legacy load, and cap check.
8. Map source grants/policies to approved target statements; unmatched semantics are GAP rows
   with an owner, never silently approximated.
9. Present `plan.yaml` for approval; on approval, dispatch the wave tickets to
   `migration-fanout` (one ticket per `Run wave N` step, naming the manifest).

## Where each D1–D10 crossing lands

- D1 (ordering): `depends_on` between step ids — never prose.
- D2 (shared objects): the wave-0 step (`scaffold-wave-0` / `run-wave-0`, `width: 1`).
- D3–D9: one plan decision each, shaped from `references/decisions.md`.
- D10 (access/lead time): blockers — `{kind: secret}` for named secrets, `{kind: repo}` for the
  connected repo, `{kind: mcp}` for connected services, no `check` for customer-side work.

## Always `important: true, selected: null`

Decisions the human must weigh before approval: cutover authorization, any tolerance change,
scope widening, anything that touches the legacy source (including every
`legacy_write_authorized` entry — mirrored by a human PR to `.migration/authorizations.json`),
a merge override and a gate waiver (mirrored the same way, as a `merge_override` / `gate_waived`
entry whose `objects` name the units; the wave halts before launch without it), and `auto_merge`.

## Cold-start ticket contents per step type

Every dispatched ticket carries: the connected repo and base branch, the plan step id it closes
(`plan_step` for wave manifests), the `.migration/` paths it may read or write, the plugin
skill to invoke (`workspace-setup`, `estate-inventory`, `pipeline-analysis`, `wave-plan`,
`migration-fanout`, `parallel-run`, `cutover`, or a dialect skill), and the evidence it must
attach (per that skill's own "Evidence the ticket must attach" section).

## What the manager judges as evidence

Worker prose is input; the manager opens the file. Gates tick only from:
the signed doctor record with `ready: true`; `.migration/waves/wave-<N>.result.json`
`closed: true` with every batch verify PASS; a recon `result.json` verdict/`merge_eligible`;
a PR merged on the protected branch.

## Evidence the manager attaches

- `plan.yaml` committed and passing `devin_common.migration_plan.validate` with no errors.
- The signed `wave-<N>.doctor.json` for the upcoming wave plus `capabilities.json` refreshed.
- Every `.migration/` contract committed: `allowed_targets.json`, `recon_tolerances.json`,
  `authorizations.json` (if any), `capabilities.json`.
- Every fired request is a `plan.yaml` blocker carrying the exact request text and its check; the
  reply or pending state is recorded on that blocker.
- The wave manifest set committed on the integration branch (`wave-<pipeline>-<N>.json` for
  siblings), each with its plan `plan_step` id.
- The cost estimate per unit and verifier depth, and the wave-0 scaffolding checklist.
