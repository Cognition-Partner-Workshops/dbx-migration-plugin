# Full worker procedure — one fan-out batch

Convert one batch of units, prove parity, and open one evidence-backed PR. The workflow embeds
`SKILL.md` in the child prompt; this reference is the unabridged procedure behind it.

## Before conversion

- Read the complete batch: units, profiles, dictionary, dependencies, write targets, branch,
  declared gates, tolerances, and the `.migration/` paths you own.
- Read the dialect skill's `SKILL.md` before the first unit.
- Run `factory-doctor --role child --reuse-record .migration/waves/wave-<TAG>.doctor.json` —
  the signed record committed beside the manifest — with `--expect-identity`,
  `--expect-host`, one `--unit` per unit in the batch, and the manifest's source flags
  (`source.family`, `source.secret`, `source.params`). A bare `--role child` run is the
  fallback only when no signed record exists. A `fail` row means BLOCKED naming the check id;
  a `warn` row goes in your summary and you continue.
- At the batch's `max_minutes`, stop and report BLOCKED with findings (what landed, what did
  not, what blocked it); never grind past it.
- Confirm every write target is declared; never edit `.migration/` outside
  `.migration/recon/<unit_id>/`, never edit `allowed_targets.json`, never touch the wave
  result or run log. Every changed path goes in `changed_paths`
  (`git diff --name-only <base>...<head>`).

## Convert and deploy

- Convert each unit with CORE + its workload profile + the dialect skill; record every rule you
  derive yourself as a `skill_feedback` line.
- Implement only the dependency mechanisms the plan decided for this batch.
- Deploy only to the batch's isolated namespace, idempotently, with Lakeflow Jobs owned by
  bundle/IaC and schedule PAUSED.
- A platform 5xx on `databricks bundle deploy`/`bundle run` follows the bounded retry rule in
  `skills/target-routing/SKILL.md` (twice: 30 s, then 120 s).
- Launch heavy backfills/recon as jobs, record run IDs, and never babysit polling in-session.

## Reconcile

- Fixture-first: develop against the fixture copy with declared endpoints (missing endpoints
  fail closed); read the real source once inside the legacy-query cap, one batched check
  window. In wave 0 the fixture's shape is proven first (`dbx-recon fixture-shape`): a `fail`
  is the wave-0 finding that keeps wave 1 from launching.
- Check counts, aggregates, keyed diffs, report output, declared source volumes, populations,
  and idempotency evidence.
- Idempotency is the rerun proof (`dbx-recon rerun-proof`, fixture
  `harness/fixtures/example_rerun/`): fresh target plus a target pre-created in the table's
  previous committed shape (`--prior-proof`, the last committed `rerun_proof.json`); pass
  `--rerun-proof` with `--rerun-source` to `run`. What the proof must show is the unit's
  `rerun_posture` in the committed `mapping_spec.json`, never a run flag you choose.
- On FAIL, capture evidence, fix converted code only, rerun, and stop after 3 full runs.
- Never change a tolerance or `03_recon_tolerances.json`.
- Read `result.json` as `parity` plus `merge_policy`: each `blockers` entry has a `reason` and a
  `class` (the classes also in `blocker_classes`; `merge_block_reasons` is the reasons alone).
  `data` means fix converted code; `structural` (a missing constraint, trigger, index, identity,
  or grant), `privilege_visibility`, `rerun_policy` and `evidence` (mode, provenance, rows not
  gradable yet such as `aggregates_ungraded_in_flight`) block merge with parity as measured and
  are reported by class. `unsupported` in `structural_checks` is unchecked, not clean.
- Keep the source principal read-only, use one warehouse window, and report connector/live
  budget and recon cost in the evidence.

## Merge verdict and merge authority

- Run the merge-evidence recon exactly once as specified — `live`, `snapshot`, or
  `transactional` (transactional for Lakebase/operational units) — with every unit's
  `.migration/recon/<unit>/result.json` saying `merge_eligible=true`. Fixture evidence is never
  PASS.
- On the third failing run report `status=FAIL` with a one-word `failure_class`
  (`timestamp_precision`, `decimal_rounding`, `sequence_behind_source`, `missing_rule`).
  A `failure_class` containing "identity" or "sequence" flags a parent-owned resync, not
  something you fix in the batch.
- The harness is the merge authority. If a unit is not eligible and the committed manifest's
  `merge_overrides` has exactly one entry covering your units, report
  `merge_authority {kind: human_override, decision_id: <that entry's decision>}`; never write
  or edit the manifest.

## Deliver

- Report every declared gate by id in `gates` as `passed` with its evidence path (a file under
  `.migration/recon/<unit>/` committed in your PR) or `failed`; an unreported gate fails the
  unit. A waived gate was decided in the plan and is not listed; never rename or re-kind one.
- Open exactly one PR per batch: first line PASS or FAIL, full recon JSON linked, three-part
  body, cost, write targets, and skill feedback. Do not merge it — review and merge happen at
  wave close.
- Structured report: `skill_feedback` one line per derived rule; `recon_cost` =
  `result.json['cost']` of the merge-evidence run; `one_line_summary` for a human skimming 20
  of these — what landed, or why not.

## Pointers

`data-reconciliation` owns tiers and finding codes; the workflow owns the collision checks,
merge-authority check, and wave result. `factory-doctor` `type_map_audit` rejects forbidden
target types before recon queries.
