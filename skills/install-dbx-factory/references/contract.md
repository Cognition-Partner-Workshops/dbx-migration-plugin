# Migration Factory Process Contract

The only home for the process rules below (stops, `stop_mode`, D1-D10, notifications, branch and merge, fan-out guards). `AGENTS.md` holds the always-on hard rules; each skill's `SKILL.md` holds its tool contract; `OVERVIEW.md` is the map. Other files point here and do not restate.

## Stops A–E and wave close

| Stop | Plain-language question | When | Default | What the user decides |
|---|---|---|---|---|
| **A** | "This is what 'migrated' will mean, these are the accuracy tolerances, and this is the access we need. Correct?" | after pre-migration | per `stop_mode` (default soft) | target profiles per workload (and which are N/A), recon tolerances, access checklist status, repo topology, and the first pipeline when the intake named one (then STOP B is skipped) |
| **B** | "Here is everything in your estate. Which pipeline do we migrate first?" | after estate inventory, only if the intake did not fix pipeline order | per `stop_mode` (default soft); skipped when STOP A fixed the first pipeline | **which pipeline to migrate** (default: the inventory's recommendation), its scope boundary and exclusions |
| **C** | "Here is the plan: order, dependencies, how many parallel sessions, cost. Approved?" | after plan | per `stop_mode` (default soft) | analysis, plan, every dependency decision, fan-out width, wave gates, data target |
| **Wave close** (was D) | "A batch is done, here is the evidence it matches the old system. Any concerns?" | after each wave | notify | review the wave's PRs and recon evidence in batch; optionally pause the fan-out |
| **E** | "The new system has matched the old one in production for weeks. Authorize cutover?" | before cutover | blocking | sign-off, evidence, independent audit, cutover authorization |

Stops fire even on resumed runs; approvals never carry over. Default `stop_mode` is soft (60-second window, then the recommended default, recorded as `default-accepted`); STOP E always blocks. At every stop, the full markdown artifacts are attached, not summarized. Wave close is a notification, never a stop: nothing waits on a reply.

## stop_mode

Every stop runs in one of two modes, recorded as `stop_mode` in `00_context.md` at intake:

- **soft (default)**: post the stop as usual (one decision, recommended default, the exact reply that changes it, artifact links), wait **60 seconds** for a reply, then accept the recommended default and continue. A reply inside the window is applied exactly as in hard mode; a reply that arrives after the window is treated as a change request at the next stop, never silently dropped.
- **hard**: post and block until a human replies. No timeout, no default.

Precedence: the user can set `stop_mode: hard` for the whole engagement at intake, or name individual stops ("make STOP C hard"), recorded in the same file. **STOP E is always hard**, whatever `stop_mode` says, as is any stop whose recommended default would change tolerances, widen scope, or touch the legacy source; those need a human reply.

Every stop, in either mode, writes one row to `06_decisions.md` with its provenance: `user:<message/event id>` when a human replied, or `default-accepted (soft, 60s, no reply)` when the window elapsed. Writing `user:` without a human reply is forbidden; accepting a default in hard mode is forbidden. The row is what the resume rule reads; the chat is not.

Soft is the default because human turnaround between stops dominates wall-clock time and stops are almost always answered with the recommended default; engagements with real change control set `stop_mode: hard` at intake.

## Dependency taxonomy

`!dbx_dependency_resolution` classifies every crossing into exactly one class:

| Class | What it is | Typical decision options |
|---|---|---|
| D1 | intra-pipeline lineage edge (ordering constraint, not a decision) | ordering constraint only; handled by wave order, no decision |
| D2 | shared object used by several pipelines (migrate once, first pipeline owns it) | migrate once in wave 0; owner pipeline per the shared-object map |
| D3 | upstream feed owned by a system not migrating (federation or ingestion contract) | federate for reads (default); managed ingestion via Lakeflow Connect (SQL Server CT/CDC gateway, Postgres/MySQL CDC, query-based Oracle/Teradata/SQL Server/PG/MySQL, foreign-catalog Snowflake/Redshift/Synapse/BigQuery) or Auto Loader for files; the connector choice, its source-side prerequisites, and cutover cadence are the contract |
| D4 | downstream consumer (BI dashboard, report, extract, API) reading the legacy output | re-point at cutover; dual-publish during coexistence; rebuild |
| D5 | scheduler / orchestration dependency (Control-M, Autosys, Airflow, cron) | replace with Lakeflow Jobs; keep external scheduler triggering Databricks; hybrid with completion signal |
| D6 | shared table written by both migrated and non-migrated writers | dual-write window; legacy remains writer + federated read; documented deferral |
| D7 | external hand-off (SFTP drop, message queue, partner feed) | preserve format contract exactly; re-platform the transport at cutover |
| D8 | security / governance contract (row-level security, PII masking, retention) | reproduce in UC (row filters, masks, grants) before any consumer re-points |
| D9 | ML model or scoring consumer of the data | prediction-parity gate (playbook 6, ML-SCORING step) per the profile before re-pointing |
| D10 | environment/access dependency (network path, service principal, sample data approval) | fire the request now; track to closure; gates fan-out width; the fired request carries the exact command(s) or grant statement(s) and the one-line reply that closes it |

Each entry records the full contract, then a decision (federate / re-point / dual-write during coexistence / documented deferral), the routing point that flips traffic, the cutover and decommission condition, and the fired lead-time request. D10 entries fire at STOP A, because access requests routinely outlast the code work.

## Notification contract

If `00_context.md` names a notification contract, post to it at exactly these moments: each stop when its artifacts are ready for approval (one message, artifact links, what decision is needed), each wave close (exception count and the wave report), and any fan-out halt (collision or circuit breaker) with what is paused and what unblocks it. Slack posts go through the Slack integration (stops can be approved from the thread); Teams posts go to the webhook whose URL lives in the named secret. Never post per-child or per-green-PR updates. If the interaction contract opts in, add one **daily digest** at the agreed hour (a run that spans a sleep period earns it): the latest wave-close brief's headline, the status-table delta, and anything awaiting the user, in 2-4 sentences with links; it is a summary surface, never a substitute for an approval stop.

Message style and the one-message-per-event rule are `AGENTS.md`.

## Branch, PR and merge

- Unit PRs and migration ledgers land on the engagement feature branch: `base_branch` is required; `main`/`master` require a recorded `trunk_base_decision`.
- `auto_merge` is false by default and may be true only under a decision recorded at STOP A; hard `stop_mode` requires false. In hard mode, the merge owner merges PASS PRs listed under "Awaiting manual merge" in the brief without a wave-close reply gate; rejected or paused PASS PRs need a `06_decisions.md` row, while a workflow safety halt waits for a human.
- The normal unit deliverable is one PR per unit batch.
- Only a live, snapshot, or transactional PASS from the independent verifier is merge-eligible.
- Detailed manifest and PR-diff enforcement remains in `skills/migration-fanout/SKILL.md`; plan/manifest construction remains in `playbooks/4-migration_plan.md`.

## Fan-out guards

Each row names the check that enforces it; the always-on rules (secrets, write scope, cutover, message volume, tolerances) are `AGENTS.md` and are not repeated here.

| If this happens | What catches it |
|---|---|
| A source probe runs before the write scope exists | Create `.migration/allowed_targets.json` with `catalogs` and `legacy_sources` before any source probe; authorized legacy writes carry `DBX_DECISION=D-<id>` with `legacy_write_authorized` and the object in `06_decisions.md`. |
| A stop is skipped or an old approval is reused | Every stop is a dated row in `06_decisions.md`; the orchestrator re-reads it on resume and re-asks if the inputs changed. |
| A child gets an incomplete brief | It reports BLOCKED, does nothing, and the brief says which item was missing. It never guesses. |
| Two children write the same table | `workflow.py` collision check refuses to launch the wave; found afterwards, merges are held and the brief says so. |
| The same wave is launched twice | `workflow.py` refuses if `wave-N.result.json` exists; resume uses the run_id, redo needs an explicit flag. |
| A wave launches on a stale or unsigned preflight | `workflow.py` requires the doctor-signed `wave-N.doctor.json` for that wave. |
| One mistake repeats across 20 children | `workflow.py` circuit breaker: 3 same-class failures and no new children launch; fix once, resume, held-back batches run. |
| A child keeps retrying a red recon | Hard cap of 3 full `dbx-recon` runs, then it reports FAIL with a one-word failure class. |
| Children hammer the live source | Fixture first; each child reads the real source once, inside the cap agreed at STOP A. |
| A child grades its own homework | The verifier session (wrote none of the code) re-runs the harness; only its PASS merges. |
| Fixture PASS gets mistaken for done | The `dbx-recon` verdict line names fixture vs live; only a live, snapshot or transactional PASS can merge. |
| The source moved during the check | Live comparisons are timestamped and re-run on the source side to separate drift from a real defect. |
