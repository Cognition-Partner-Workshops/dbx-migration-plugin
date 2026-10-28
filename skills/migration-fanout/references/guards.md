# Migration fan-out guards

These notes expand the one-line guard summary in the skill. The workflow remains the
only writer of the wave result, the card and the run log.

## Manifest check

The manifest must be a valid wave contract: naming, plan step, `child_skill` / `verify_skill`
(lowercase skill names whose `SKILL.md` exists under the pointer's plugin root), source fields,
batches, units, briefs, targets, gates, width, branch, and capabilities are checked before
children; a manifest carrying `merge_overrides` is refused (overrides are authorization entries).

## Signed doctor gate

The doctor record is bound to the exact manifest bytes and identity/host, has a recent
signature, records the hook probe, and must be ready with capabilities matching the plan.

## One manifest one run

A lock on `.wave-N.lock` is held for the whole run, so a second launch of a running wave
halts instead of launching its batches twice; a run that died holds nothing. An exclusive
lock protects the append-only run log, which records each launch as
`{plan_step, plan_sha, manifest_sha, started}` and, before any close step is dispatched (review-only
or merging), the PRs it was sent as `merged`. A manifest whose sha is already in the log cannot launch
again; changed bytes are a new manifest and need a doctor signature over them; a plan step that sent
PRs to a close cannot launch again whatever the close reported, proved, or merged without proof, and one
whose `plan_sha` differs from what it ran halts (a scope change is a new plan step). The `plan_sha`
in the result is the manifest less briefs, repo, secret names and estimates (of `source`, the
secret name is plumbing but the family and params are scope), so a reader can tell a plumbing
edit from a changed plan.

## Repo preflight

Before anything launches: `repo` is `host/owner/name` and matches a remote origin URL.

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

Children may write only their declared migration evidence. The result (with its `brief`
lines), the card and the run log are written by the parent workflow.

## Child report

Each child report is schema-validated, must identify status and recon evidence, and must
list changed paths and write targets for later gates.

## Protected files gate

Git-observed changes to protected `.migration/` files are reclassified as
`protected_files_tampered`, even when a child claims a clean or harmless diff.

## Grading

An object graded `unverified(by_design)` (a reject-only sink, a rollup) is named in three places or
nowhere: the unit spec (`grading: by_design_unverified`), the committed manifest batch
(`by_design_unverified`, part of `plan_sha`), and the harness result (`grading`). The launch halts
when the spec and the manifest disagree; a batch whose results grade a different set than the
manifest declares fails `grading_mismatch`, like `protected_files_tampered`, whatever the child
claims. The harness still blocks such an object on its own (`by_design_unverified`, class
`evidence`), so a declaration the manifest lacks never merges and no `authorizations.json` entry
is involved.

## Merge authority

PASS requires merge-eligible harness evidence for every unit (a result blocked only by
`by_design_unverified` for exactly the objects the batch declares counts as such), unless exactly one
`merge_override` entry of `.migration/authorizations.json` on origin/`base_branch` names this
wave (`wave: wave-<tag>`, the manifest file stem), covers every unit of the batch in `objects`,
and the child reports that entry's id. A `blocker_classes` scope forgives only the policy
classes it lists; an entry with no scope forgives every policy class; `data` is never forgiven
(an entry naming it is malformed; a unit with no recorded blocker classes fits no override).

## Plan decisions

A waived gate's `decision_id` names a plan decision the human selected; before launch it must
be an entry of `.migration/authorizations.json` on origin/`base_branch` with that `id`, `kind:
gate_waived`, `by: user:<id>` and `objects` naming every unit it covers. A merge override is the
same file's `merge_override` entry naming its `wave`, read at the same ref when the batch
reports. The working copy never counts, and a session cannot add those kinds (the guard blocks
the edit): the authorization arrives by reviewed PR.

## Acceptance gates

Every declared gate needs a valid passed outcome with evidence. A waived gate stands on its
committed `gate_waived` authorization; there is no other runtime waiver lookup.

## Resync

Only the manifest's declared resync command may run, against its listed units. A failed,
unexpected, or non-empty-change resync holds affected batches from merge.

## Independent verify

The verifier reruns reconciliation from the launch base and the protected contract files,
not from an untrusted child checkout, and supplies the verdict used for close.

## Verifier verdicts

Verdicts normalize unit keys to batches, require exactly one answer per batch, and reject
contradictory wave and batch outcomes.

## Wave close

Close accepts only git-proven merges whose landed change carries the gated PR head.
Unproven, moved-head, timeout, or crash reports remain unmerged and keep the wave open.
