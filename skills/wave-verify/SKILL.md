---
name: wave-verify
description: The independent verifier ticket for a completed wave — re-runs the recon harness itself, holds each unit's PASS to the merge-evidence bar, checks every gate's evidence, and writes the wave recon report. The migration-fanout workflow embeds this skill in the verifier prompt.
---

# wave-verify

You did not write any of this code. You re-run the evidence yourself and your verdict decides
what merges. Source and target are read-only to you.

## Ticket inputs (cold start)

- The passed batches: batch ids, units, PR urls, gated head shas, each batch's gates with the
  child's claimed evidence, and any `merge_authority` override records.
- The wave manifest's `verify_depth` per batch (default sampled; never lower it — raising is
  allowed and noted in findings) and its `degraded` flag.
- `03_recon_tolerances.json` and `allowed_targets.json` **from the base branch**, not the PR —
  a child that loosened a tolerance fails here.
- `skills/data-reconciliation/SKILL.md` — tiers, finding codes, DEGRADED wording.

## Procedure

1. For each PR run `git diff --name-only <base>...<head>`: any `.migration/` path outside
   `.migration/recon/<unit_id>/` is FAIL for that unit with finding `protected_files_tampered`.
2. Re-run the recon harness in one of the merge-evidence modes (`live`, `snapshot`,
   `transactional` — the same mode the child used; transactional for Lakebase/operational
   units) at the batch's listed depth: sampled = Tier 1+2 plus a stratified Tier 3 with a
   seed different from the child's; full = keyed full diff. Mark the unit PASS only when the
   run's `result.json` records the unit merge-eligible.
3. For a batch listed with `merge_authority` kind `human_override`: the committed manifest's
   `merge_overrides` entry cleared it — mark PASS on a PASS verdict even if `merge_eligible` is
   false, and cite the decision id in findings.
4. A batch declared DEGRADED (the manifest's `degraded: true`) verifies at the structural tier
   only: run the harness in structural mode (Tier 0: keys, constraints, indexes, triggers,
   identity columns, grants — read from both catalogs, no row tier) and mark the unit PASS only
   when that run's `result.json` says `verdict=PASS` and `merge_block_reasons` is exactly
   `["mode"]`: a `structural_gap` or `warnings` entry means unverified structure — FAIL with
   `structure_unverifiable`; `verdict=FAIL` is FAIL with `structural_drift`. Do not re-run
   Tier 1-3 and do not lower or raise a depth.
5. Open the evidence of every passed gate and FAIL the unit if it does not show what the gate's
   kind requires.
6. Probe adversarial boundaries at the listed depth: empty/all-null, duplicate keys, late
   arrivals, deletes, timezone/DST, decimal extremes, Unicode/collation, skew, retries, reruns.
   Compare cross-unit totals and consumer outputs; for ML score parity compare features,
   seed/model version, distributions, and agreed numeric tolerance.
7. Write the wave recon report to `.migration/recon/wave-<TAG>/report.md`, commit it on branch
   `recon/wave-<TAG>`, push, and give `<branch>:<path>` in `report_path`. Edit nothing else
   under `.migration/`; report your branch's `changed_paths` (`git diff --name-only
   <base>...<head>`). Source and target stay read-only.
8. Return `wave_verdict` PASS/FAIL and `unit_verdicts` keyed by batch id (a unit id key is
   normalised to its batch id; one verdict per batch whatever the batch size), plus `findings`
   and `recon_cost` (sum of `result.json['cost']` over your runs). Never merge anything.

## Evidence the ticket must attach

- `wave_verdict`, `unit_verdicts`, `findings` (with finding codes), `report_path`,
  `changed_paths`, `recon_cost`.
- The wave report naming the mode and depth per unit; a DEGRADED report names the mode in its
  header, carries sample-coverage statistics, and states that sample parity does not
  extrapolate to production distributions — never borrow live-run wording.
