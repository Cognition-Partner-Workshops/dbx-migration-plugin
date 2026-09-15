Playbook: Thin orchestrator. Drives ONE pipeline of a legacy estate from confirmed scope to authorized cutover by sequencing the `!dbx_*` unit playbooks, fanning out execution as parallel child sessions, and holding the human stops.

## Overview
This session does almost no migration work itself. The pre-migration phases (setup, inventory, analysis, plan) run inside THIS session back to back, pausing only at the stops; separate sessions per phase pay the context-load tax repeatedly and add scheduling gaps. It sequences the unit playbooks, launches children, gates on machine-checked recon results, integrates, and holds the stops. **Every substantive rule lives in the child playbook that owns it**; this playbook must not restate or duplicate their internals.

The distinctive job here, relative to serial migration orchestrators, is **managing width**: launching fan-out batches in parallel, tracking them in the ledger, gating each on its recon evidence, and collecting systematic findings (SKILL FEEDBACK) between waves so the fan-out gets smarter as it goes.

```
[pre-migration, once per engagement]
  !dbx_migration_setup          target profiles + .migration/ + tolerances
                                + access                                 (STOP A, per stop_mode)
  !dbx_estate_inventory         census, coverage, pipeline catalog      (STOP B, per stop_mode:
                                                                         user picks pipeline)
[per pipeline]
  !dbx_pipeline_analysis        units, waves, fan-out batches, dictionary
  !dbx_migration_plan           decisions, fired requests, schedule     (STOP C, per stop_mode)
  [wave 0]   shared objects + scaffolding, SERIAL
  [wave 1]   pilot, width <= 5, via !dbx_unit_migration
             -> harvest SKILL FEEDBACK, update skill/knowledge, THEN:
  [waves 2..N]  FULL FAN-OUT, width per plan, one child per batch, PARALLEL
             per wave: gather children -> !dbx_data_reconciliation -> merge green PRs
                                                                      (STOP D, notify)
  [coexistence]  !dbx_parallel_run: scheduled recon + event-driven remediation
  [close]        !dbx_cutover_signoff                                  (STOP E, blocking)
```

## Stop mode
Process contract (stops, stop_mode, D1–D10, notifications, branch/merge, fan-out guards): read references/contract.md in the install-dbx-factory skill once per session; it is not restated here.

`stop_mode` governs decision defaults, not merge authority. Soft mode default-accepts decisions, but merges require `auto_merge: true` set explicitly in the manifest by a decision recorded at STOP A; otherwise PASS PRs are held for the wave-close reply. Hard mode requires `auto_merge: false`. The same rule covers every PR the orchestrator owns outside a manifest (wave 0, wave-close, coexistence, cutover documents) and the small-wave path. Independently of mode, when the workflow itself disables merging after a write-target anomaly (overlap, undeclared target, missing target report) the affected PRs stay unmerged until a human resolves the halt; a soft default never clears a safety halt. A child's ESCALATE that carries a recommended answer is resolved with it after the 60-second window in soft mode (same provenance row) and waits for a reply in hard mode.

## What's Needed From User
- **The pipeline (required, chosen by the user at STOP B)**: the orchestrator confirms the boundary against the inventory; it never picks or widens.
- **Stop mode** (`stop_mode` in `00_context.md`, default soft): A, B, C follow it; D is notify-only; E always blocks.
- **Fan-out width** (confirmed at STOP C) and whether large-estate execution runs as a dynamic workflow (default yes above ~10 batches per wave).
- **Notification contract** from `00_context.md` (set at STOP A, optional): the Slack channel or Teams webhook to ping.

## Procedure
0. **Resume rule (every run, first thing).** Read `.migration/` before doing anything: `06_decisions.md` says which stops are approved and when, `05_progress.md` and `.migration/waves/wave-<N>.result.json` say which waves closed. Start from the first thing not done. If a stop's inputs changed since its approval (scope, tolerances, width), re-ask that stop; never reuse a stale approval. A row whose provenance is `default-accepted` counts as approved; re-ask it only if its inputs changed. If a wave has a `run_id` file and no result file, resume that workflow run instead of starting a new one.
1. **Pre-migration (once per engagement).** Run `!dbx_migration_setup` (both phases); if it ran in a prior session, still present its artifacts and re-confirm at **STOP A**. **Never idle on a lead time**: once access requests are fired, proceed immediately to everything the current access tier can do (metadata census, lineage extraction, dictionary drafting); waiting is only permitted when nothing in scope is executable.
2. **Inventory.** Run the estate inventory; present the catalog, coverage arithmetic, parallelism profile, and recommendation at **STOP B**; the user picks the pipeline, or the recommended one is default-accepted in soft mode; the orchestrator never widens it afterwards.
3. **Analysis and plan.** Run analysis, then planning. **STOP C**: attach both, present the dependency decision table with fired requests, the schedule with width and wall-clock projection, and the gate spec. Resolve the stop per `stop_mode` before any execution session launches.
4. **Wave 0, serial.** Shared objects and scaffolding per the plan run through migration-fanout with a `wave: 0`, `width: 1` manifest, not by hand. Confirm every fired lead-time request is progressing; a stalled D10 caps your width and the user should know now.
5. **Wave 1, the pilot, narrow.** Launch `!dbx_unit_migration` children at width <= 5. Gather them, read their PRs and recon evidence (never assume a child succeeded), harvest every SKILL FEEDBACK item, apply the systematic fixes to the dialect skill and knowledge notes, and only then open the throttle.
6. **Waves 2..N, full fan-out, with the circuit breaker armed.** One wave = one launch, one gather, one gate, one brief. You do not review children one at a time.
   - **Large wave (above ~10 batches, the plan's threshold):** run `factory-doctor --wave .migration/waves/wave-<N>.json` in your shell with the hook probe result, then write `~/.migration/waves/current.json` with `manifest`, `mode: "start"`, `run_id: null` on the first run, `hook_probe`, and `workspace` set to the repo's absolute path. Start, rerun, and smoke pointers use `run_id: null`; only resume carries a run ID. Run the `migration-fanout` workflow; it discovers the pointer from cwd and parents and reads no environment variables. It refuses to launch on a write-target collision, launches one child per batch at the confirmed width, stops new launches after the breaker threshold (default 3 same-class failures), runs the wave's single live recon window through one independent `!dbx_data_reconciliation` session, merges green PRs if the manifest says so, and writes `wave-<N>.result.json` and `wave-<N>.brief.md`. Record the returned run ID in `.migration/waves/wave-<N>.run_id` and commit. For rerun, call `run_workflow` without `run_id`; it deletes the old `.run_id` record before creating a fresh execution. You wait for it; you do not poll children.
   - **Small wave (at or below the threshold): do the same thing by hand, once.** Launch every batch's child in one go with its complete hand-off from the plan (children share no filesystem or memory with you). Gather the whole batch once with a generous timeout. Read each child's structured result (status, PR, recon verdict, failure class, write targets), not its chat. Halt further launches only on a reported write-target collision or when the breaker threshold of same-class failures is reached. Then run the wave's single live recon window in one fresh `!dbx_data_reconciliation` session, merge the green PRs together (soft) or list them under "Awaiting manual merge" for the wave-close reply (hard), and write the brief. Failed or blocked units are listed in the brief as exceptions; they reopen as fresh children in the next launch with the failure evidence attached. There is no per-child review loop in either path.
   - **On a halt** (collision or breaker): one notification with the failure class, the batches affected, and the recommended fix. Patch the dialect skill or knowledge note, re-run the doctor with `--wave` for a fresh signature, update the pointer to `mode: "resume"` with the same recorded `run_id`, and relaunch. Use `mode: "rerun"` only for a deliberate redo; keep `run_id: null`, call `run_workflow` without `run_id`, and let rerun delete the old `.run_id` record. Do not let a new systematic trap cost 20x what it should cost once.
   - **Pipeline the phases, gate the merges.** Never MERGE wave N+1 before wave N is merged and green, but wave N+1's children may launch once wave N's children have landed and registered targets, while wave N's independent recon runs (disjoint resources).
   - **At wave close**: append the recon report, PR list, and exceptions to the evidence-pack skeleton in `.migration/`, harvest SKILL FEEDBACK into the skill before the next wave, and post the **wave-close brief** as the STOP D notification: 10-15 lines, exceptions first, then what landed, what was decided and why, what remains unproven, and the per-unit cost line (session ACUs, warehouse-hours) recorded now. A lead's whole day should be readable in it plus the one-screen status table in `05_progress.md`. If the result file reports `auto_merge: false`, read why before launching the next wave: hard mode means merge the PASS PRs listed under "Awaiting manual merge" once the wave-close reply arrives (or record why not in `06_decisions.md`); a workflow safety halt (write-target anomaly) means post the halt and wait for a human, whatever `stop_mode` says. Within a wave, apply the plan's calibration rule: one small unit of any new pattern class first, then the rest of the class in parallel.
7. **Coexistence.** Run `!dbx_parallel_run` to stand up the scheduled recon job and event-driven remediation, once all waves are merged.
8. **Close.** Run `!dbx_cutover_signoff` (end-to-end, evidence pack, independent audit by a fresh session, consumer cutover), then **STOP E** for authorization.
9. **Notifications.** If `00_context.md` names a notification contract, post to it at exactly these moments: each stop when its artifacts are ready for approval (one message, artifact links, what decision is needed), each STOP D wave close (exception count and the wave report), and any fan-out halt (collision or circuit breaker) with what is paused and what unblocks it. Slack posts go through the Slack integration (stops can be approved from the thread); Teams posts go to the webhook whose URL lives in the named secret. Never post per-child or per-green-PR updates. If the interaction contract opts in, add one **daily digest** at the agreed hour (a run that spans a sleep period earns it): the latest wave-close brief's headline, the status-table delta, and anything awaiting the user, in 2-4 sentences with links; it is a summary surface, never a substitute for an approval stop.
10. **Ledger discipline.** Keep `05_progress.md`, `04_dependency_register.md` and `06_decisions.md` current after every step, so any resumed session or the next pipeline starts from fact rather than memory. The decision ledger with provenance also powers **ask-the-ledger**: a lead who asks "why does unit X quarantine 4%?" gets a cited answer (the D-entry, the originating unit, the PR that argued it) from any on-demand session reading `.migration/`, instead of re-reading the evidence corpus.

## Specifications
- Deliverable: a fully migrated, cut-over pipeline with the artifact set (inventory, analysis, plan, per-wave recon reports, parallel-run ledger, evidence pack, audit memo) committed to DOCS, every dependency closed or deferred-with-condition, and the PR set merged.
- The orchestrator runs the unit playbooks **unmodified** and owns only: sequencing, stops, fan-out and gathering, gating, integration, skill-feedback harvesting, and the ledger.
- Validation: every stop fired; pilot preceded full fan-out; every wave merged green before its successor; recon independence preserved; audit performed by a non-migrating session.

## Advice and Pointers
- **Own the width, not the substance.** Your value is launching 20 correct hand-offs and gating 20 results, not explaining how to convert a mapping.
- A vague hand-off is the most common cause of a wasted child. The plan's batch spec is the hand-off; if it is incomplete, fix the plan, not the child. Hand-offs stay thin: a child gets its batch packet and pointers, never the whole kit; children re-reading the full artifact set is pure compute waste at width.
- Harvest SKILL FEEDBACK between every wave, not just after the pilot; the fan-out should be measurably more accurate at wave 4 than wave 2.
- Watch the ledger for slow batches: a child stuck twice on the same unit signals a mis-planned batch or an undecided tolerance, both of which are yours to escalate, not the child's to absorb.
- Independent pipelines can run as parallel orchestrator sessions once pipeline 1 has hardened the profiles; propose it to the user rather than deciding it.

## Forbidden Actions
- Do NOT advance past a stop without writing its decision row with honest provenance; never write `user:` without a human reply, never accept a default at STOP E or in hard mode.
- Do NOT post per-PR merge prompts; merge authority is decided by `stop_mode`/`auto_merge`, and manual merges are batched into the wave-close message.
- Do NOT choose or widen the pipeline, and do NOT adjust tolerances or dependency decisions mid-wave; changes go back through the plan and its stop.
- Do NOT convert units, author recon queries, or write application code in the parent, and do NOT re-verify targets by re-querying them yourself: the parent gates on the children's evidence and the independent `!dbx_data_reconciliation` session's report; parent-side re-verification duplicates that work at parent-session cost.
- Do NOT sit in multi-hour polling loops on in-flight children; use the dynamic workflow (or gather with generous timeouts, notify-on-settle where the platform offers it) and spend parent compute only on exceptions and wave close.
- Do NOT leave harness fixes in the run: a recon-validator fix, a baseline-provenance rule, or a platform limit a child discovered is a **skill/harness concern**; promote it into the shared skill (and the capability-preflight manifest in the hand-off) the same wave, so it is paid for once, never per run and never per child.
- Do NOT launch a wave whose upstream wave is unmerged, whose batches share write targets, or whose dependencies are UNDECIDED.
- Do NOT skip the pilot or widen it, and do NOT full-fan-out with unharvested pilot feedback.
- Do NOT assume a child succeeded; gate on its structured result and the wave's independent recon. Do NOT review children one at a time; the wave gates as a batch.
- Do NOT keep launching after a write-target collision or a tripped circuit breaker; halt, repair, resume.
- Do NOT let the migrating sessions perform the wave recon or the final audit.
- Do NOT present only a chat summary of an artifact; attach the file.
- Do NOT ping the notification channel outside the contracted event list, and do NOT treat a channel ping as a substitute for the stop itself.
