# Migration fan-out guards

These notes expand the one-line guard summary in the skill. The workflow remains the
only writer of wave results and workflow ledger records.

## Manifest check

The manifest must be a valid wave contract: naming, source fields, batches, units,
briefs, targets, gates, width, branch, and capabilities are checked before children.

## Signed doctor gate

The doctor record is bound to the exact manifest bytes and identity/host, has a recent
signature, records the hook probe, and must be ready with capabilities matching the plan.

## STOP C gates_sha approval

The named ledger row must identify this wave and exact gates hash; a changed declaration
or an approval under an incompatible stop mode halts the launch.

## One approval one run

An exclusive lock protects the append-only runs log. A STOP C identifier already present
in that log cannot authorize another execution.

## Duplicate wave

Any result path blocks launch. This applies to closed, halted, malformed, and unreadable
result files so an operator cannot accidentally replay a wave.

## Manifest name / pipelines barrier

The filename tag must match the wave number and declared sibling pipelines. Origin is
checked for every required sibling manifest before collision analysis.

## Collision check

Two batches cannot claim the same target. A child-reported overlap is recorded and holds
merge authority rather than being silently repaired.

## Declared targets match call graph

Mapping specifications and observed calls must stay within the batch's declared targets;
missing or extra target evidence is a halt or failed child result.

## Shared tables across waves

When waves share a table, every reader must carry a bounded predicate proving its slice.

## Width

The semaphore caps concurrent children at the manifest width, with wave 0 constrained
to serial execution.

## Time budget

Child prompts and runtime calls carry the manifest's batch or wave time budget.

## Circuit breaker

Repeated failures of the same class trip the breaker and prevent remaining batches from
launching; the result identifies the triggering batch.

## Single ledger writer

Children may write only their declared migration evidence. Result, brief, run log, and
workflow-owned ledger artifacts are written by the parent workflow.

## Child report

Each child report is schema-validated, must identify status and recon evidence, and must
list changed paths and write targets for later gates.

## Ledger gate

Git-observed changes to protected migration files are reclassified as ledger tampering,
even when a child claims a clean or harmless diff.

## Merge authority

PASS requires merge-eligible harness evidence for every unit, unless a valid human
override row names exactly the affected units.

## Acceptance gates

Every declared gate needs a valid passed outcome with evidence, or a ledger waiver
whose decision row names the gate and units.

## Resync

Only the manifest's declared resync command may run, against its listed units. A failed,
unexpected, or non-empty-change resync holds affected batches from merge.

## Independent verify

The verifier reruns reconciliation from the launch base and protected ledgers, not from
an untrusted child checkout, and supplies the verdict used for close.

## Verifier verdicts

Verdicts normalize unit keys to batches, require exactly one answer per batch, and reject
contradictory wave and batch outcomes.

## Wave close

Close accepts only git-proven merges whose landed change carries the gated PR head.
Unproven, moved-head, timeout, or crash reports remain unmerged and keep the wave open.
