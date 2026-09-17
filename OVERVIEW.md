# DBX Migration Factory: one-page overview

A repeatable way to move a legacy data estate (a SQL warehouse, an ETL tool, a code estate, or an operational database) onto Databricks with Devin sessions working in parallel and a human deciding at five stops. Analytical workloads land in Delta under Unity Catalog and run on Databricks SQL and Lakeflow; operational workloads land in Lakebase (managed Postgres). One estate can span both tracks; they share one plan, one dependency register and one set of stops.

The target is constant, so target knowledge is built once (the official `databricks` plugin, routed by `target-routing`). The source varies, so source specifics live in dialect skills. Nothing merges on trust: every converted unit must reconcile against the legacy system, and the reconciliation is re-run by a session that did not write the code. Independent units migrate many at a time.

## What the operator does

1. Install the plugin (`README.md`) and run the `install-dbx-factory` skill once per org: it imports the playbooks below and proposes the environment blueprint.
2. Start an engagement with one front door: `!dbx_migrate_etl`, `!dbx_migrate_warehouse`, `!dbx_migrate_code` or `!dbx_migrate_oltp`. The front door consumes the pre-kickoff intake (`playbooks/00_intake_template.md`).
3. Answer the stops when pinged. Everything else is automatic.

## The chain

| Step | Macro | Does | Human |
|---|---|---|---|
| 1 | `!dbx_migration_setup` | target profiles, `.migration/` workspace, tolerances, access checklist, allowlist | STOP A |
| 2 | `!dbx_estate_inventory` | asset census, lineage DAG, coverage proof, shared-object map | STOP B (skipped only when intake fixed pipeline and boundary; see `references/contract.md`) |
| 3 | `!dbx_pipeline_analysis` | unit inventory, lineage waves, type dictionary, dependency sweep, batches | |
| 4 | `!dbx_migration_plan` | plan, every dependency decided, access requests fired, wave manifests | STOP C |
| 5 | `!dbx_unit_migration` | one fan-out child per unit batch | |
| 6 | `!dbx_data_reconciliation` | independent verifier per wave | wave close (notification) |
| 7 | `!dbx_parallel_run` | scheduled recon job, event-driven fix sessions | |
| 8 | `!dbx_cutover_signoff` | end-to-end run, evidence, independent audit, cutover plan | STOP E (always blocking) |
| 9 | `!dbx_migrate_pipeline` | the orchestrator that drives 1-8 for one pipeline | |
| 10, 11, 12, 14 | `!dbx_migrate_etl`, `_warehouse`, `_code`, `_oltp` | front doors: pre-select skills and defaults, then call 9 | |
| 13 | `!dbx_dependency_resolution` | internal subroutine (register / decide / implement) called by 2-5; never operator-invoked | |

File names, titles and macros are in `skills/install-dbx-factory/playbooks/index.json`; the bodies are the `.md` files next to it. A whole estate is one orchestrator run per pipeline; pipelines with disjoint write targets run as sibling orchestrator sessions.

## The five stops

The questions, defaults, `stop_mode` (soft 60-second default vs hard) and what each stop decides are in `skills/install-dbx-factory/references/contract.md`; that file is the only home for them. In short: A confirms the target and tolerances, B picks the pipeline when the intake did not, C approves the plan and fan-out width, the wave close is a notification with the wave brief, E authorizes cutover and always blocks. Every stop writes one dated row to `.migration/06_decisions.md`; the chat is not the record.

## The wave loop

Units at the same lineage depth with no shared-object conflict form a wave; a wave is split into unit batches, one child session each (default width 20, set at STOP C). Shared objects (D2) go first in a `wave: 0`, `width: 1` manifest; a small first wave tunes the dialect skill before full fan-out.

For each wave the orchestrator: writes `.migration/waves/wave-N.json` → runs `factory-doctor` (`doctor.py --wave N`, which signs `wave-N.doctor.json`) → writes the `waves/current.json` pointer → calls `run_workflow` with `skills/migration-fanout/workflow.py`. The script holds no credentials: it checks the signed doctor file, refuses write-target collisions and duplicate launches, launches the children, trips the circuit breaker on 3 same-class failures, runs the independent verifier, and writes `wave-N.result.json` plus the ten-line wave brief. Children develop against fixtures, read the live source once inside the cap, self-grade with `dbx-recon`, cap themselves at 3 full recon re-runs, and report BLOCKED rather than guess. Only the verifier's live, snapshot or transactional PASS is merge-eligible.

## What the code enforces

| Control | Where | What it does |
|---|---|---|
| Write-scope guard | `hooks/dbx_guard.py` (PreToolUse) | blocks writes outside `.migration/allowed_targets.json` — the allowlist in force is the copy committed on the protected branch, so `.migration/` is writable and scope widens only by PR — non-read statements against legacy sources, identity swaps, unreadable commands; policy table in `README.md` |
| Preflight doctor | `skills/factory-doctor/doctor.py` | CLI and identity, harness self-test, `.migration/` integrity, committed allowlist equal to the wave contract, source principal cannot write, hook nonce probe, playbooks in sync with the org library; signs the wave's doctor file |
| Fan-out workflow | `skills/migration-fanout/workflow.py` | doctor-file gate, collision check, duplicate-wave refusal, circuit breaker, single writer of wave results |
| Reconciliation harness | `skills/data-reconciliation/harness` (`dbx-recon`) | tiered parity with agreed tolerances, transactional mode for Lakebase, delete evidence, verdict line names fixture vs live; the merge authority |
| Prose gates | `skills/test_no_estate_strings.py`, `skills/test_no_duplicated_rules.py` | no engagement-specific names; no `AGENTS.md` rule restated elsewhere |

The always-on rules are `AGENTS.md`. Process rules (stops, D1-D10, notifications, branch and merge) are `references/contract.md`. Tool contracts are each skill's `SKILL.md`. A rule lives in exactly one of those; everything else points.
