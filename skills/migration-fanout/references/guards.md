# Migration fan-out guards

These notes expand the one-line guard summary in the skill. The workflow remains the
only writer of the wave result and the run log.

## Manifest check

The manifest must be a valid wave contract: naming, source fields, batches, units,
briefs, targets, gates, width, branch, and capabilities are checked before children.

## Signed doctor gate

The doctor record is bound to the exact manifest bytes and identity/host, has a recent
signature, records the hook probe, and must be ready with capabilities matching the plan.

## Plan step + run log

The manifest names the plan step id of the wave's "Run wave N" ticket. An exclusive lock
protects the append-only run log; each run records `{plan_step, manifest_sha, started}`
and a manifest whose sha is already present cannot launch again.

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

## Single result writer

Children may write only their declared migration evidence. Result and run log are
written by the parent workflow; the result's `brief` lines go on the ticket.

## Child report

Each child report is schema-validated, must identify status and recon evidence, and must
list changed paths and write targets for later gates.

## Protected files gate

Git-observed changes to protected `.migration/` files are reclassified as
`protected_files_tampered`, even when a child claims a clean or harmless diff.

## Merge authority

PASS requires merge-eligible harness evidence for every unit, unless a
`merge_overrides` entry in the committed manifest covers exactly the batch's units and
the child reports that entry's decision id.

## Acceptance gates

Every declared gate needs a valid passed outcome with evidence. A waived gate carries
the plan decision id in the committed manifest; there is no runtime waiver.

## Resync

Only the manifest's declared resync command may run, against its listed units. A failed,
unexpected, or non-empty-change resync holds affected batches from merge.

## Independent verify

The verifier reruns reconciliation from the launch base and the protected contract
files, not from an untrusted child checkout, and supplies the verdict used for close.

## Verifier verdicts

Verdicts normalize unit keys to batches, require exactly one answer per batch, and reject
contradictory wave and batch outcomes.

## Wave close

Close accepts only git-proven merges whose landed change carries the gated PR head.
Unproven, moved-head, timeout, or crash reports remain unmerged and keep the wave open.
