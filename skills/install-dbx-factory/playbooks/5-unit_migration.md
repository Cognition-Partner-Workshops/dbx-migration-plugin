Playbook: Convert one fan-out batch, prove parity, and open one evidence-backed PR.

## Child checklist

### Before conversion
- [ ] Read the complete hand-off: units, profiles, dictionary, dependencies, targets, branch, gate, tolerances, and `.migration/` path.
- [ ] Read the dialect skill's `SKILL.md` and `references/contract.md` fan-out guards before the first unit.
- [ ] Run `factory-doctor --role child` with the capability contract, every unit, source secret/parameters, and hook probe; report BLOCKED on any failed check.
- [ ] At the brief's `max_minutes` budget, stop and report BLOCKED with findings (what landed, what did not, what blocked it); never grind past it.
- [ ] Confirm every write target is declared; never edit `.migration/` outside `.migration/recon/<unit_id>/`, never edit `allowed_targets.json`, never write the ledger.
- [ ] Never edit `05_progress.md`; it is generated from wave results, and report every changed path in `result.json`.

### Convert and deploy
- [ ] Convert each unit with CORE + workload profile + dialect skill; record systematic gaps as SKILL FEEDBACK.
- [ ] Implement only the decided dependency mechanisms for this batch; never touch legacy sources.
- [ ] Deploy only to the isolated namespace, idempotently, with Lakeflow Jobs owned by bundle/IaC and schedule PAUSED.

### Reconcile
- [ ] Run `dbx-recon` fixture-first with declared endpoints and fail closed when endpoints are missing; use one batched check window.
- [ ] Fixture-first: develop against the fixture copy; read the real source once inside the legacy-query cap.
- [ ] Check counts, aggregates, keyed diffs, report output, declared source volumes, populations, and idempotency evidence.
- [ ] Launch heavy backfills/recon as jobs, record run IDs, and never babysit polling in-session.
- [ ] On FAIL, capture evidence, fix converted code only, rerun `dbx-recon`, and stop after three full runs.

### Review and deliver
- [ ] Self-review every gate, tolerance, unverified path, owner, severity, and closure gate; stop after three review rounds and escalate.
- [ ] Finish the Devin Review round on your PR before reporting done: zero open actionable findings at the head you report (`review_clean: true`); a wrong finding is waived only by a human's `review_waived` row in `06_decisions.md` naming your units and the PR head sha it clears, never worked around.
- [ ] Run the live or snapshot merge verdict exactly once as specified; fixture PASS is not merge eligibility.
- [ ] Treat a structural gap (missing constraint, trigger, index, identity, or grant; `structural_gap` in `merge_block_reasons`) as a merge blocker like a row-tier failure; `unsupported` in `structural_checks` is unchecked, not clean.
- [ ] Keep the source principal read-only, use one warehouse window, and report connector/live budget and recon cost in the evidence.
- [ ] Open one PR with changed paths, full recon JSON, short summary, three-part body, cost, write targets, and SKILL FEEDBACK.

## Specifications
- Deliverable: one PR per batch with recon evidence, or an explicit escalation.
- Validation: all units green or escalated; scope is limited to the batch; legacy is untouched; evidence is rerunnable.

## Pointers
The `data-reconciliation` skill owns tiers and finding codes; the workflow owns ledger and target enforcement. `factory-doctor` `type_map_audit` rejects forbidden target types before recon queries.
