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
Every stop runs in one of two modes, recorded as `stop_mode` in `00_context.md` at intake:

- **soft (default)**: post the stop as usual (one decision, recommended default, the exact reply that changes it, artifact links), wait **60 seconds** for a reply, then accept the recommended default and continue. A reply inside the window is applied exactly as in hard mode; a reply that arrives after the window is treated as a change request at the next stop, never silently dropped.
- **hard**: post and block until a human replies. No timeout, no default.

Precedence: the user can set `stop_mode: hard` for the whole engagement at intake, or name individual stops ("make STOP C hard"), recorded in the same file. **STOP E is always hard**, whatever `stop_mode` says, as is any stop whose recommended default would change tolerances, widen scope, or touch the legacy source; those need a human reply.

Every stop, in either mode, writes one row to `06_decisions.md` with its provenance: `user:<message/event id>` when a human replied, or `default-accepted (soft, 60s, no reply)` when the window elapsed. Writing `user:` without a human reply is forbidden; accepting a default in hard mode is forbidden. The row is what the resume rule reads; the chat is not.

Why soft is the default: in the runs that shaped this kit, human turnaround between stops was the largest single wall-clock component (up to two hours per run), and the stops were almost always answered with the recommended default. Soft mode keeps every stop visible and overridable while letting an unattended run finish; the provenance column keeps the record honest about which decisions a human actually made. Engagements with real change control set `stop_mode: hard` at intake.

Soft mode also sets the run's other defaults: `auto_merge: true` for every PR the orchestrator owns (wave 0, wave-close, coexistence, cutover documents) as well as wave manifests, so no "reply merge" prompts are posted; and a child's ESCALATE that has a recommended answer is resolved with that answer after the same 60-second window, recorded with the same provenance. In hard mode, merges are batched into the wave-close message (one prompt per wave, never per PR) and escalations wait for a reply.

## What's Needed From User
- **The pipeline (required, chosen by the user at STOP B)**: the orchestrator confirms the boundary against the inventory; it never picks or widens.
- **Stop mode** (`stop_mode` in `00_context.md`, default soft): A, B, C follow it; D is notify-only; E always blocks.
- **Fan-out width** (confirmed at STOP C) and whether large-estate execution runs as a dynamic workflow (default yes above ~10 batches per wave).
- **Notification contract** from `00_context.md` (set at STOP A, optional): the Slack channel or Teams webhook to ping.

## Procedure
0. **Resume rule (every run, first thing).** Read `.migration/` before doing anything: `06_decisions.md` says which stops are approved and when, `05_progress.md` and `.migration/waves/wave-<N>.result.json` say which waves closed. Start from the first thing not done. If a stop's inputs changed since its approval (scope, tolerances, width), re-ask that stop; never reuse a stale approval. A row whose provenance is `default-accepted` counts as approved; re-ask it only if its inputs changed. If a wave has a `run_id` file and no result file, resume that workflow run instead of starting a new one.
1. **Pre-migration (once per engagement).** Run `!dbx_migration_setup` (both phases); if it ran in a prior session, still present its artifacts and re-confirm at **STOP A**. **Never idle on a lead time**: once access requests are fired, proceed immediately to everything the current access tier can do (metadata census, lineage extraction, dictionary drafting); waiting is only permitted when nothing in scope is executable.
2. **Inventory.** Run the estate inventory; present the catalog, coverage arithmetic, parallelism profile, and recommendation at **STOP B**; the user picks the pipeline.
3. **Analysis and plan.** Run analysis, then planning. **STOP C**: attach both, present the dependency decision table with fired requests, the schedule with width and wall-clock projection, and the gate spec. Resolve the stop per `stop_mode` before any execution session launches.
4. **Wave 0, serial.** Shared objects and scaffolding per the plan. Confirm every fired lead-time request is progressing; a stalled D10 caps your width and the user should know now.
5. **Wave 1, the pilot, narrow.** Launch `!dbx_unit_migration` children at width <= 5. Gather them, read their PRs and recon evidence (never assume a child succeeded), harvest every SKILL FEEDBACK item, apply the systematic fixes to the dialect skill and knowledge notes, and only then open the throttle.
6. **Waves 2..N, full fan-out, with the circuit breaker armed.** One wave = one launch, one gather, one gate, one brief. You do not review children one at a time.
   - **Large wave (above ~10 batches, the plan's threshold): run the `migration-fanout` workflow** with `WAVE_MANIFEST=.migration/waves/wave-<N>.json` and `WAVE_RUN_ID=<run_id>`. It refuses to launch on a write-target collision, launches one child per batch at the confirmed width, stops new launches after the breaker threshold (default 3 same-class failures), runs the wave's single live recon window through one independent `!dbx_data_reconciliation` session, merges green PRs if the manifest says so, and writes `wave-<N>.result.json` and `wave-<N>.brief.md`. Record the `run_id` in `.migration/waves/wave-<N>.run_id` and commit. You wait for it; you do not poll children.
   - **Small wave (at or below the threshold): do the same thing by hand, once.** Launch every batch's child in one go with its complete hand-off from the plan (children share no filesystem or memory with you). Gather the whole batch once with a generous timeout. Read each child's structured result (status, PR, recon verdict, failure class, write targets), not its chat. Halt further launches only on a reported write-target collision or when the breaker threshold of same-class failures is reached. Then run the wave's single live recon window in one fresh `!dbx_data_reconciliation` session, merge the green PRs together, and write the brief. Failed or blocked units are listed in the brief as exceptions; they reopen as fresh children in the next launch with the failure evidence attached. There is no per-child review loop in either path.
   - **On a halt** (collision or breaker): one notification with the failure class, the batches affected, and the recommended fix. Patch the dialect skill or knowledge note, then relaunch: same `run_id` plus `WAVE_RESUME=1` for the workflow, or the held-back batches by hand. Do not let a new systematic trap cost 20x what it should cost once.
   - **Pipeline the phases, gate the merges.** Never MERGE wave N+1 before wave N is merged and green, but wave N+1's children may launch once wave N's children have landed and registered targets, while wave N's independent recon runs (disjoint resources).
   - **At wave close**: append the recon report, PR list, and exceptions to the evidence-pack skeleton in `.migration/`, harvest SKILL FEEDBACK into the skill before the next wave, and post the **wave-close brief** as the STOP D notification: 10-15 lines, exceptions first, then what landed, what was decided and why, what remains unproven, and the per-unit cost line (session ACUs, warehouse-hours) recorded now. A lead's whole day should be readable in it plus the one-screen status table in `05_progress.md`. (hard mode only) If `auto_merge` is false, merge the PASS PRs listed under "Awaiting manual merge" in the brief (or record why not in `06_decisions.md`) before launching the next wave. Within a wave, apply the plan's calibration rule: one small unit of any new pattern class first, then the rest of the class in parallel.
7. **Coexistence.** Run `!dbx_parallel_run` to stand up the scheduled recon job and event-driven remediation, once all waves are merged.
8. **Close.** Run `!dbx_cutover_signoff` (end-to-end, evidence pack, independent audit by a fresh session, consumer cutover), then **STOP E** for authorization.
   9. **Notifications.** If `00_context.md` names a notification contract, post to it at exactly these moments: each stop when its artifacts are ready for approval (one message, artifact links, what decision is needed), each STOP D wave close (exception count and the wave report), and any fan-out halt (collision or circuit breaker) with what is paused and what unblocks it. Slack posts go through the Slack integration (stops can be approved from the thread); Teams posts go to the webhook whose URL lives in the named secret. Never post per-child or per-green-PR updates. If the interaction contract opts in, add one **daily digest** at the agreed hour (a run that spans a sleep period earns it): the latest wave-close brief's headline, the status-table delta, and anything awaiting the user, in 2-4 sentences with links; it is a summary surface, never a substitute for an approval stop.
10. **Ledger discipline.** Keep `05_progress.md`, `04_dependency_register.md` and `06_decisions.md` current after every step, so any resumed session or the next pipeline starts from fact rather than memory. The decision ledger with provenance also powers **ask-the-ledger**: a lead who asks "why does unit X quarantine 4%?" gets a cited answer (the D-entry, the originating unit, the PR that argued it) from any on-demand session reading `.migration/`, instead of re-reading the evidence corpus.

## Human stop points
- **STOP A** target state + tolerances + access, **STOP B** pipeline choice, **STOP C** plan, decisions, width: all per `stop_mode`. **STOP D** per-wave review (notify). **STOP E** cutover (always blocking).
- At every stop, attach the actual `.md` artifacts and state repo, path, branch. A chat summary is an addition, never a substitute.
- **Every stop is a one-decision presentation**: recommended default stated, the exact approval sentence needed, and nothing that opens a discussion. Human turnaround between stops is the chain's biggest hidden wall-clock floor; a stop answerable in one minute gets answered in one hour; soft mode exists because of it.
- Stops fire even when prior sessions produced the artifacts; a previous approval never carries over (in soft mode the re-fired stop still gets its 60-second window).

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
