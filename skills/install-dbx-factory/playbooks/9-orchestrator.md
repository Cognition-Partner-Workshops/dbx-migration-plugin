Playbook: Sequence setup, inventory, analysis, planning, waves, reconciliation, coexistence, and cutover without doing child work.

## Contract
Read `references/contract.md` once per session for stops, `stop_mode`, D1–D10, notifications, branch/merge, and fan-out guards. The orchestrator owns sequencing, gathering, gates, integration, feedback, and the generated ledger; unit playbooks run unmodified.

## Procedure
0. **Prior result.** Read `.migration/`, `06_decisions.md`, `05_progress.md`, and wave result files; a wave whose `wave-<N>.result.json` exists does not relaunch. To redo it, delete the result and get a new STOP C row.
1. **Setup.** Run migration setup and present its committed artifacts at STOP A; never wait on a lead time when executable metadata work remains.
   - **Parallel pipelines.** Intake asks which pipelines share write targets or source objects. After STOP A, pipelines that share none each run in a sibling orchestrator session on the same repo, in parallel; a shared object is a D2 (wave 0, owner pipeline) and the sharing pipelines run in sequence. Each sibling names its manifests `wave-<pipeline>-<N>.json`, lists every pipeline with its wave count in each manifest's `pipelines` (`{<pipeline>: <count>}`, identical in every sibling; its own tag included and `<N>` within the count — a tagged manifest without it halts), and pushes its manifests to the integration branch before STOP C (the planning barrier); the workflow fetches origin and halts until every numbered manifest of every listed pipeline is there declaring the same `pipelines` and the manifests on disk equal origin's, so the collision check reads every pipeline's declared targets and the verifier branch `recon/wave-<pipeline>-<N>` is its own.
2. **Inventory.** Run inventory; present coverage, catalog, recommendation, and boundary at STOP B unless intake fixed both pipeline and boundary (condition in `references/contract.md`).
3. **Plan.** Run analysis and planning, resolve dependencies, and present width, cost, gates, manifests, and capability contract at STOP C before launching children.
4. **Wave 0.** Run shared objects and scaffolding serially through the workflow with `wave: 0`, `width: 1`; verify all D10 requests are progressing.
5. **Pilot.** It is a small wave: launch it through the workflow like every wave (doctor, pointer, `run_workflow`) at width <= 5; no wave launches without the launch guards. Gather it, read its PR and recon evidence, harvest every SKILL FEEDBACK item, apply systematic fixes to the dialect skill and knowledge notes, and only then open the throttle.
6. **Full waves.** Launch, gather, gate, and brief each wave once; keep wave order for merges (never merge wave N+1 before wave N is merged and green; N+1 children may launch once N's have landed), and stop on a write-target collision or breaker threshold; run one unit of each new pattern class narrow before fanning it out.
   - Run `factory-doctor --wave` with the hook probe before launching the wave.
   - Write `$HOME/.migration/waves/current.json` as `{"manifest": "wave-<N>.json", "hook_probe": "blocked:<nonce>|not-blocked|unknown", "workspace": "/abs/repo", "plugin": "<plugin>"}` (`plugin` is the absolute plugin root) and run `run_workflow(workflow_name="migration-wave-<N>", script_path="<plugin>/skills/migration-fanout/workflow.py")`. Use its result and brief; it enforces declared targets, collision checks, breaker behavior, independent recon, close proof, and acceptance gates. If `auto_merge` is false, follow the brief's manual-merge instructions.
   - **On a halt:** fix the cause, re-run the doctor, delete `wave-<N>.result.json`, record a new STOP C row, name it in `stop_c`, and relaunch. A wave result remains authoritative until this deliberate procedure is completed.
7. **Coexistence.** After all waves merge, run parallel-run monitoring with its paused job, live window, evidence ledger, and remediation rules.
8. **Cutover.** Run cutover signoff and present the evidence pack for STOP E (authorization: `AGENTS.md`).
9. **Notifications.** See `references/contract.md`.
10. **Ledger.** Never hand-edit `05_progress.md`; regenerate it with `python3 <plugin>/skills/migration-fanout/progress.py .migration --refresh-merged` at every wave close and after manual merges; keep dependency and decision ledgers current.

## Specifications
- Deliverable: committed artifacts, manifests, wave results/briefs, PR set, evidence pack, and current ledger.
- Validation: workflow safety and collision checks pass; wave order, target scope, gates, and approvals are explicit.

## Pointers
`workflow.py` owns execution, manifest-gate, and result schemas. `factory-doctor` owns capability and wave signatures.
