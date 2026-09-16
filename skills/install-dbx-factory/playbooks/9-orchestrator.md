Playbook: Sequence setup, inventory, analysis, planning, waves, reconciliation, coexistence, and cutover without doing child work.

## Contract
Read `references/contract.md` once per session for stops, `stop_mode`, D1–D10, notifications, branch/merge, and fan-out guards. The orchestrator owns sequencing, gathering, gates, integration, feedback, and the generated ledger; unit playbooks run unmodified.

## Procedure
0. **Resume.** Read `.migration/`, `06_decisions.md`, `05_progress.md`, and wave result files; re-ask a stop when its inputs changed, and resume a run with a `.run_id` rather than launching a duplicate.
1. **Setup.** Run migration setup and present its committed artifacts at STOP A; never wait on a lead time when executable metadata work remains.
2. **Inventory.** Run inventory when intake did not fix order; present coverage, catalog, recommendation, and boundary at STOP B (skipped if intake fixed pipeline order).
3. **Plan.** Run analysis and planning, resolve dependencies, and present width, cost, gates, manifests, and capability contract at STOP C before launching children.
4. **Wave 0.** Run shared objects and scaffolding serially through the workflow with `wave: 0`, `width: 1`; verify all D10 requests are progressing.
5. **Pilot.** Launch wave 1 at width <= 5, gather once, read structured results and recon, then harvest systematic SKILL FEEDBACK before widening.
6. **Full waves.** Launch, gather, gate, and brief each wave once; apply the calibration rule, keep wave order for merges, and stop on a write-target collision or breaker threshold.
   - For a large wave, run `devin_playbook_manage {"action":"list","first":200}` and paginate, `get` every record, take its `Macro:` header and body, and export every record whose macro is one of the repo's 14 to `.migration/live_playbooks.json` as `[{"macro","playbook_id","content"}]`; retain duplicates because the doctor rejects them by name, then run `factory-doctor --wave` with the hook probe.
   - Write `.migration/waves/current.json` with manifest, `mode: "start"`, `run_id`, hook probe, and absolute workspace; start/rerun use `run_id: null`, resume uses the recorded run ID.
   - Run the workflow, record `.migration/waves/wave-<N>.run_id`, and use its result/brief. It enforces declared targets, collision checks, breaker behavior, independent recon, and manifest-controlled merges.
   - For a small wave, gather every child once, run one fresh recon, merge or list manual merges, and write `waves/<manifest>.result.json` in the workflow shape (`wave`, `closed`, `auto_merge`, `breaker_tripped_on`, `batches[]` with `id`, `status`, `recon_verdict`, `pr_url`, `recon_cost`) because `progress.py` renders only result files.
7. **Coexistence.** After all waves merge, run parallel-run monitoring with its paused job, live window, evidence ledger, and remediation rules.
8. **Cutover.** Run cutover signoff and present the evidence pack for STOP E; the customer-held principal authorizes execution.
9. **Notifications.** See `references/contract.md`.
10. **Ledger.** Regenerate `05_progress.md` with `python3 skills/migration-fanout/progress.py .migration` at every wave close; keep dependency and decision ledgers current.

## Specifications
- Deliverable: committed artifacts, manifests, wave results/briefs, PR set, evidence pack, and current ledger.
- Validation: workflow safety and collision checks pass; wave order, target scope, and approvals are explicit.

## Pointers
`workflow.py` owns execution and result schema. `factory-doctor` owns capability and wave signatures.
