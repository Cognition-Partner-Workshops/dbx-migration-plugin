# 0.5.0 — board rework ("Devin for Migrations")

The playbook/macro/stops model is replaced by the board flow: a human files a request, the
`migration-planning` manager writes `plan.yaml`, the human approves it in the Plan view, each
plan step becomes a ticket, and worker sessions execute the tickets. The manager ticks gates
from authoritative evidence, not from worker claims.

## Deleted

- `skills/install-dbx-factory/` (skill, `playbooks/`, `references/contract.md`) — the playbook
  chain is gone; its content is rewritten into the new skills below.
- `skills/migration-fanout/progress.py` and its tests (`test_progress.py`,
  `test_stop_b_condition.py`) — the generated progress ledger is gone; the board is the
  progress surface.

## Rewritten

- `skills/migration-fanout/workflow.py` — manifests now name `child_skill`/`verify_skill`
  (skills embedded verbatim in the child/verifier prompts) and a required `plan_step` slug;
  the run log records `{plan_step, manifest_sha, started}` per launch and refuses a rerun of
  the same manifest bytes; merge overrides come from the committed manifest's
  `merge_overrides` entries (`{decision, units}`; a batch is cleared only when exactly one
  entry covers all its units and the child claims that entry's decision); a waived gate stands
  on its manifest `decision_id`; `result.json` carries `brief` lines instead of a brief file;
  `ledger_tampered` is now `protected_files_tampered`; all decision ids are lowercase slugs.
- `skills/factory-doctor/doctor.py` — `REQUIRED_FILES` is now `allowed_targets.json` +
  `03_recon_tolerances.json`; new `authorizations_file` check (ok when absent; fails on
  malformed JSON, entries missing `id`/`kind`/`objects`/`by`, or a working copy that differs
  from the committed one).
- `skills/target-routing/pipeline_updates.py` — `serialized_pipelines` maps a pipeline to a
  plan decision slug and is shape-validated only (no runtime decision lookup); the
  `--decisions` flag is gone.
- `skills/data-reconciliation` (SKILL.md, harness `report.py`/`cli.py`/`cost.py`/`config.py`,
  tests), `skills/oracle-plsql`, `skills/target-routing`, `skills-extra/*`,
  `skills/_dialect-skill-template.md`, `hooks/dbx_guard.py` — process text updated to plan
  decisions, intake, wave manifests, and cutover.

## New skills

- `migration-planning` (+ `references/decisions.md`, `references/phase1-steps.md`,
  `references/plan.example.yaml`) — the manager ticket: writes `plan.yaml`, decides every
  D1–D10 crossing as plan decisions/blockers, fires lead-time requests, and cuts wave tickets.
- `intake` (+ `references/estate-types.md`) — manager-facing checklist: what the request and
  its attachments must contain and where each answer lands in `plan.yaml` (decision, blocker,
  or phase-skeleton change); it writes no `.migration/` file itself.
- `workspace-setup` — the phase-1 worker ticket that turns the approved intake decisions into
  the committed `.migration/` workspace (`allowed_targets.json`, `03_recon_tolerances.json`,
  `09_capabilities.json`) and proves every access path.
- `unit-migration` (+ `references/procedure.md`), `wave-verify`, `parallel-run`, `cutover`,
  `estate-inventory`, `pipeline-analysis`, `wave-plan` — the worker tickets.

## .migration fates

- `authorizations.json` — **new, machine-checkable**: the only file that authorizes legacy
  writes; enters only through a reviewed PR and the guard reads the committed copy.
- `allowed_targets.json`, `03_recon_tolerances.json`, `09_capabilities.json`,
  `units/<id>/{mapping_spec,dependencies}.json`, `waves/wave-<N>.{json,doctor.json,result.json,runs.jsonl}`,
  `recon/<unit_id>/` — kept.
- `00_context.md`, `01_conventions.md`, `02_glossary.md`, `03_recon_tolerances.md`,
  `04_dependency_register.md`, `05_progress.md`, `06_decisions.md`, `07_access_checklist.md`,
  `<Pipeline>_plan.md`, `wave-<N>.brief.md` — deleted; their content moves into `plan.yaml`
  (decisions, blockers, gates), the wave manifests, and the result's `brief` lines.
- `waves/current.json` — kept as the workflow pointer; its `plugin` key is now load-bearing
  (skills and `pipeline_updates.py` resolve from it).

## Manifest changes

- Added: `plan_step` (required), `child_skill`, `verify_skill`, `merge_overrides`
  (`{decision: <slug>, units: [...]}`), `resync`, `pipelines`/`serialized_pipelines` (slug
  values).
- Removed: `child_macro`, `verify_macro`, `stop_c`, `gates_sha`.
- `auto_merge` stays a boolean; the hard-mode coupling is gone.

## Guard change

`hooks/dbx_guard.py` resolves `DBX_DECISION=<id>` against the committed
`.migration/authorizations.json` (id → entry; `kind` must be `legacy_write_authorized`, `by` a
`user:...` author, and every written object a literal name in `objects`). The edit-tool rule
now blocks adding a `legacy_write_authorized` entry outright, and the block reason no longer
mentions a decision row.

## Doctor changes

`--source-attested <id>` records the plan decision id the human attested the source principal
with (`status ok`, the id in `data.decision`); it is recorded, not machine-verified.
`--role orchestrator` is kept as the CLI value for the wave ticket's worker.

## Calls made

1. `--source-attested` rows are `ok`, not `unverified` — `unverified` would block `ready` for
   the non-child roles the doctor actually signs for.
2. The run-log dedup key is the manifest sha: identical bytes launch once; changed bytes with a
   fresh signature launch a new run.
3. A waived gate needs its `decision_id` in the committed manifest; there is no runtime waiver
   lookup.
4. `merge_overrides` are manifest entries; a batch clears only when exactly one entry covers
   all of its units and the child claims that entry's decision.
5. `orchestrator` survives only as the doctor's `--role` CLI value; the banned-terms test
   exempts `skills/factory-doctor/` for that one word.
6. Teradata "macro"/"macros" mentions in `skills-extra/` (and the README dialect list) are the
   source database's own feature name; the banned-terms test exempts `skills-extra/` for that
   word.
7. `--role orchestrator` naming inside `doctor.py` was left as-is (CLI compatibility); prose
   says "the wave ticket's worker".
8. `test_no_legacy_process_terms.py` scans `*.md`/`*.py`/`*.json` except `CHANGES.md`,
   `__pycache__`, and harness fixtures, case-sensitively for identifiers and
   case-insensitively for `STOP [A-E]`, `playbook`, `macro`, `orchestrator`.
9. `unit-migration/SKILL.md` stays compact (~250 words) because the workflow embeds it verbatim
   in every child prompt, which must stay under the brief-plus-prompt word budget and must not
   pre-empt the ordered phrases the guard tests assert; the full procedure lives in
   `references/procedure.md` and the skill points to it.

## Unresolved

- None blocking. The new skills assume the platform's board/Plan-view plumbing
  (`plan.yaml` validation, ticket dispatch by plan step) already exists.
