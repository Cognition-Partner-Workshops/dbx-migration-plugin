# DBX Migration Factory: one-page overview

"Devin for Migrations": a repeatable way to move a legacy data estate (a SQL warehouse, an ETL
tool, a code estate, or an operational database) onto Databricks as a board of tickets, with a
manager session planning and worker sessions executing. Analytical workloads land in Delta under
Unity Catalog and run on Databricks SQL and Lakeflow; operational workloads land in Lakebase
(managed Postgres). One estate can span both tracks; they share one plan.

The target is constant, so target knowledge is built once (the official `databricks` plugin,
routed by `target-routing`). The source varies, so source specifics live in dialect skills.
Nothing merges on trust: the recon verdict rule is `skills/data-reconciliation/SKILL.md`.

## How a migration runs

1. A human files the migration request on the board.
2. The manager (`intake`) turns the request's answers into phase-1 decisions and blockers; a
   `workspace-setup` worker ticket then writes the committed `.migration/` workspace:
   `allowed_targets.json`, `03_recon_tolerances.json`, `09_capabilities.json`, and
   `authorizations.json` (by human PR) when legacy writes are needed.
3. `estate-inventory` and `pipeline-analysis` tickets census the estate and analyze the chosen
   pipeline into units and waves.
4. A **manager** session (`migration-planning`) writes `plan.yaml`: phases, steps with
   `depends_on`, decisions with options, gates, and blockers with machine checks.
5. The human approves `plan.yaml` in the Plan view. Devin never writes `approved`; approval is
   the only gate between plan and execution.
6. Each `Run wave N` step becomes a ticket. A `wave-plan` worker writes the wave manifest and
   has `factory-doctor` sign it; a `migration-fanout` worker runs `workflow.py`, which launches
   one `unit-migration` child per batch and one independent `wave-verify` verifier.
7. The manager ticks gates from authoritative evidence: verdict files, merged PRs, job runs —
   never from a worker's claim.
8. `parallel-run` operates coexistence after the waves merge; `cutover` assembles the evidence
   pack and executes the repoint under the human's explicit authorization.

## The wave loop

Units at the same lineage depth with no shared-object conflict form a wave; a wave is split
into unit batches, one child session each (default width 20, set in the manifest). Shared
objects go first in a `wave: 0`, `width: 1` manifest; a small first wave tunes the dialect
skill before full fan-out.

For each wave the worker: writes `.migration/waves/wave-N.json` → runs `factory-doctor`
(which signs `wave-N.doctor.json`) → writes the `waves/current.json` pointer → calls
`run_workflow` with `skills/migration-fanout/workflow.py`. The script holds no credentials: it
checks the signed doctor file, refuses write-target collisions and duplicate launches, launches
the children, trips the circuit breaker on 3 same-class failures, runs the independent
verifier, and writes `wave-N.result.json` plus the brief lines. Children develop against
fixtures, read the live source once inside the cap, cap themselves at 3 full recon re-runs,
and report BLOCKED rather than guess.

## What the code enforces

| Control | Where | What it does |
|---|---|---|
| Write-scope guard | `hooks/dbx_guard.py` (PreToolUse) | blocks writes outside `.migration/allowed_targets.json` — the allowlist in force is the copy committed on the protected branch — non-read statements against legacy sources, identity swaps, unreadable commands; policy table in `README.md` |
| Authorization file | `.migration/authorizations.json` | legacy writes need a committed `legacy_write_authorized` entry, entered only by reviewed PR; the guard reads the committed copy |
| Preflight doctor | `skills/factory-doctor/doctor.py` | CLI and identity, harness self-test, `.migration/` integrity, committed allowlist equal to the wave contract, source principal cannot write, hook nonce probe, authorizations file well-formed; signs the wave's doctor file |
| Fan-out workflow | `skills/migration-fanout/workflow.py` | doctor-file gate, collision check, duplicate-wave refusal via `wave-N.runs.jsonl`, circuit breaker, merge overrides and waived gates read from the committed manifest, single writer of wave results |
| Reconciliation harness | `skills/data-reconciliation/harness` (`dbx-recon`) | tiered parity with agreed tolerances, transactional mode for Lakebase, delete evidence, verdict line names fixture vs live; holds the verdict authority rule |
| Prose gates | `skills/test_no_estate_strings.py`, `skills/test_no_duplicated_rules.py`, `skills/test_no_legacy_process_terms.py` | no engagement-specific names; no `AGENTS.md` rule restated elsewhere; no stale process vocabulary |

The always-on safety rules are `AGENTS.md`. Databricks auth is
`skills/target-routing/SKILL.md`; the recon verdict authority is
`skills/data-reconciliation/SKILL.md`. Tool contracts are each skill's `SKILL.md`; the plan
schema is `skills/migration-planning/references/` (with `plan.example.yaml`). A rule lives in
exactly one of those; everything else points.
