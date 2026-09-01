Playbook: Independent reconciliation pass over a completed wave: re-run the parity evidence with fresh eyes, catch what the migrating children graded themselves too kindly on, and produce the wave's recon report for STOP D.

## Overview
Every unit PR already carries its own recon evidence, but the author grading their own work is not the whole story. This playbook runs in a session that did **not** convert any unit in the wave. It re-executes the gates, probes beyond them, and writes the wave-level report. It is the data-migration analogue of an independent test pass: cheap because the harness is machine-checkable, valuable because it is adversarial.

```
[wave complete: all batch PRs recon-green]  ->  re-run every gate independently
                                            ->  adversarial probes beyond the gate
                                            ->  cross-unit consistency checks
                                            ->  <Pipeline>_wave<N>_recon.md  ->  STOP D (notify)
```

## What's Needed From User (or from the orchestrator)
- The wave's PR list, the plan's recon spec, the tolerance record, and access to both engines (same coexistence mechanism the children used).
- Nothing else: independence means this session reads the artifacts, not the children's chat.

## Procedure
1. **Re-run every unit's recon gate verbatim** from the plan spec, not from the PR's pasted output. Both runs are timestamped; legacy data may have moved (drift), so a mismatch is triaged as drift vs conversion error before anyone panics: re-run the legacy side twice to see if it is moving. **In DEGRADED mode** (snapshot/sample per the tolerance record) the double-run trick does not apply: triage instead against the snapshot manifest (extraction time, row counts), verify the children compared against the same snapshot version, and mark every verdict with the mode; a DEGRADED PASS is a statement about the snapshot, never about production. Respect the legacy-query concurrency cap when parallelizing re-runs. During triage, re-run only the failed check on the failed unit; never re-run the wave's green checks on suspicion alone.
2. **Probe beyond the gate, adversarially**: null distributions per column, duplicate keys, boundary rows (min/max per key column), empty-input behavior, a sample of row-level spot checks on columns the gate only aggregates, and report-output byte comparison where the tolerance record demands exactness.
3. **Cross-unit consistency**: units in a wave share tables; check referential consistency across batch boundaries (the classic parallel failure: two batches each green alone, jointly inconsistent because both dual-wrote a shared dimension).
4. **ML-SCORING units**: re-run the prediction-parity harness on the held-out sample per the profile; verify the tolerance math, seeds, and sample provenance rather than trusting the child's summary.
5. **Write `<Pipeline>_wave<N>_recon.md`**: per-unit verdict table (PASS / FAIL / DRIFT-EXPLAINED), probe results, any finding with evidence, and the wave verdict. FAIL routes back to the orchestrator to reopen the unit; never fix converted code here, and never touch legacy.
6. Update the ledger and hand the report to the orchestrator for **STOP D** (notify: PRs, recon report, findings, in batch).

## Specifications
- Deliverable: the wave recon report, every unit independently re-verified.
- Validation: (1) every gate re-run from spec, not trusted from the PR; (2) probes executed and recorded even when all green; (3) any FAIL has evidence and a routed owner; (4) this session converted nothing in the wave.

## Advice and Pointers
- Independence is the point: do not read the children's diagnosis before re-running, or you will see what they saw.
- Drift vs defect is the first triage question in any live-legacy comparison; timestamp everything.
- The adversarial probes are where this pass earns its cost; the gate re-run mostly confirms, the probes occasionally catch the aggregate that hid a compensating error.
- Keep it fast: this pass gates wave N+1's merge, and the fan-out is waiting. Parallelize the re-runs themselves where the warehouse allows.

## Forbidden Actions
- Do NOT fix converted code, modify legacy source, or adjust tolerances here.
- Do NOT accept a PR's pasted evidence in place of re-execution.
- Do NOT run this pass in a session that performed any of the wave's migration.
- Do NOT mark DRIFT-EXPLAINED without the double-run evidence showing legacy movement.
