---
name: plan
description: "The manager skill for a 0.7 run: assess the intake, launch analyze, write plan.yaml + policy.json + slices.json, land the policy PR (that merge is STOP 1), run the doctor, file the slice tickets and drive `fanout.py` to the three stops. Load it for any new migration engagement or a run's steering questions."
---

# Plan: assess → slices → plan.yaml → policy PR → doctor → tickets

You are the manager session. You never convert code, never write inside a slice's diff budget,
and you ask the human exactly the three stops — plan approval, the canary PR, each
`d-route-<slice>` decision.

## Steps

1. **Assess.** Read the intake (source family, estate size, target tracks, coexistence window,
   cutover owner, access prerequisites). Missing answers are plan `blockers`, never guesses; a
   customer prerequisite (UC connection privilege, connector DB user + CDC enablement, export
   delivery) is a blocker resolved at STOP 1, never a mid-run halt.
2. **Analyze.** Launch one `analyze` child; it returns a `slices.json` proposal (census,
   consumer map, dependency scans) that already passes `tools/slices.py --check`.
3. **Plan.yaml.** Write the Plan view entry: the slices and their order, `targets`,
   `tolerances`, the coexistence window, the cutover owner, `routing` per slice, `route_group`s,
   and the decisions. Every decision carries `selected` unless `important: true`
   (`d-route-<slice>` is always `important: true, selected: null`). `auto_merge: false` on the
   canary slice only.
4. **Policy PR.** Commit `.migration/policy.json`, `.migration/slices.json`, `.migration/base`
   and the per-slice recon `inputs/` to `base_branch` (`mig/<run>`) and open the PR. The human's
   merge of this PR **is** STOP 1 — the Plan entry links it. Until it merges, every deploy is
   guard-blocked by design and `fanout.py` refuses to launch (doctor `isolation`/`policy_committed`
   not `ok`).
5. **Doctor.** Run `tools/doctor.py`: `identity`, `isolation`, `policy_committed`,
   `source_read_only`, `hook_live`, `harness` — `ready` is every check not `fail`. Attach the
   report to the plan ticket. Branch protection on `base_branch` requiring `dbx-recon/verify` is
   configured by the repo admin at STOP 1; the doctor checks it.
6. **Tickets and fan-out.** File the slice tickets (slice-0 first: source access + reference
   tables). `tools/fanout.py` is re-run on every PR merge and every STOP reply: it launches one
   `migrate` child per slice whose `depends_on` are merged, one `verify` child on each PASS, and
   merges at the verifier's `verified_head` when `auto_merge` holds — then writes
   `.migration/recon/<slice>/fanout.json` on `base_branch`. The breaker (3 same-class failures),
   the collision refusal, and `protected_files_tampered` are its halts, each ending in a card
   whose last line is the exact reply that resumes it.
7. **Route.** At STOP 3 per `route_group`, `fanout.py` launches the route step of the slice's
   `migrate` child; the route packet (`route.md`) is the manager's own commit on `base_branch`,
   never in a slice PR, and the customer-held principal applies the repoint. Legacy stays as
   fallback through `rollback_window_h`.

## Example: a billing estate, replayed

| Step | Session | Human |
|---|---|---|
| analyze: Lakebridge analyze + census → `slices.json` (slice-0 reference, slice-1 plans (canary), slice-2 rating+invoicing merged because `sp_issue_invoice` writes `RATING_RESULTS`, slice-3 dunning) | manager + 1 child | — |
| plan.yaml + policy PR | manager | STOP 1 |
| doctor; slice-0 (source access + reference tables) | fanout → 1 child + verifier | — (auto-merge) |
| slice-1 canary PR with recon + bill | 1 child + verifier | STOP 2 |
| slice-2, slice-3 in parallel (`width 4`) | 2 children + 2 verifiers | — (auto-merge) |
| shadow 3 cycles per slice | jobs | — |
| route packets | manager | STOP 3 per slice |
