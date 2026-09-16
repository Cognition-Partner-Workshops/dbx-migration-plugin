Playbook: Sequence setup, inventory, analysis, planning, waves, reconciliation, coexistence, and cutover without doing child work.

## Contract
Read `references/contract.md` once per session for stops, `stop_mode`, D1–D10, notifications, branch/merge, and fan-out guards. The orchestrator owns sequencing, gathering, gates, integration, feedback, and the generated ledger; unit playbooks run unmodified.

## Procedure
0. **Resume.** Read `.migration/`, `06_decisions.md`, `05_progress.md`, and wave result files; re-ask a stop when its inputs changed, and resume a run with a `.run_id` rather than launching a duplicate.
1. **Setup.** Run migration setup and present its committed artifacts at STOP A; never wait on a lead time when executable metadata work remains.
2. **Inventory.** Run inventory; present coverage, catalog, recommendation, and boundary at STOP B only when intake did not fix pipeline order.
3. **Plan.** Run analysis and planning, resolve dependencies, and present width, cost, gates, manifests, and capability contract at STOP C before launching children.
4. **Wave 0.** Run shared objects and scaffolding serially through the workflow with `wave: 0`, `width: 1`; verify all D10 requests are progressing.
5. **Pilot.** Launch wave 1 at width <= 5, gather once, read structured results and recon, then harvest systematic SKILL FEEDBACK before widening.
6. **Full waves.** Launch, gather, gate, and brief each wave once; keep wave order for merges (never merge wave N+1 before wave N is merged and green; N+1 children may launch once N's have landed), and stop on a write-target collision or breaker threshold; run one unit of each new pattern class narrow before fanning it out.
   - For a large wave, run `devin_playbook_manage {"action":"list","first":200}` and paginate, `get` every record, take its `Macro:` header and body, and export every record whose macro is one of the repo's 14 to `.migration/live_playbooks.json` as `[{"macro","playbook_id","content"}]`; retain duplicates because the doctor rejects them by name, then run `factory-doctor --wave` with the hook probe.
   - Write `.migration/waves/current.json` with manifest, `mode: "start"`, `run_id`, hook probe, and absolute workspace; start/rerun use `run_id: null`, resume uses the recorded run ID.
   - Run the workflow, record `.migration/waves/wave-<N>.run_id`, and use its result/brief. It enforces declared targets, collision checks, breaker behavior, independent recon, and manifest-controlled merges; if the result says `auto_merge: false`, read why before the next wave: in hard mode, list the PASS PRs under "Awaiting manual merge" in the brief and proceed once the merge owner has merged them (the resume rule re-checks merge state); a PASS PR that is rejected or paused needs a `06_decisions.md` row; a workflow safety halt (write-target anomaly) waits for a human whatever `stop_mode` says.
   - For a small wave, gather every child once, run one fresh recon, merge or list manual merges, and write `.migration/waves/wave-<N>.result.json` beside the manifest `.migration/waves/wave-<N>.json` in the workflow shape (`wave`, `closed`, `auto_merge`, `breaker_tripped_on`, `batches[]` with `id`, `units`, `status`, `recon_verdict`, `pr_url`, `recon_cost`) because `progress.py` renders only result files.
   - **On a halt:** one notification gives the failure class, batches affected, and recommended fix; patch the dialect skill or knowledge note, re-run `factory-doctor --wave` for a fresh signature, set the pointer to `mode: "resume"` with the recorded `run_id`, and relaunch. `mode: "rerun"` is only for a deliberate redo (see `skills/migration-fanout/SKILL.md`).
7. **Coexistence.** After all waves merge, run parallel-run monitoring with its paused job, live window, evidence ledger, and remediation rules.
8. **Cutover.** Run cutover signoff and present the evidence pack for STOP E; the customer-held principal authorizes execution.
9. **Notifications.** See `references/contract.md`.
10. **Ledger.** Regenerate `05_progress.md` with `python3 skills/migration-fanout/progress.py .migration` at every wave close; keep dependency and decision ledgers current.

## Specifications
- Deliverable: committed artifacts, manifests, wave results/briefs, PR set, evidence pack, and current ledger.
- Validation: workflow safety and collision checks pass; wave order, target scope, and approvals are explicit.

## Pointers
`workflow.py` owns execution and result schema. `factory-doctor` owns capability and wave signatures.
