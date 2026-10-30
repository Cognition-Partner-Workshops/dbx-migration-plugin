---
name: verify
description: "The independent verifier for one slice PR: a different session from the slice's author, on a mixed checkout, re-runs dbx-recon and posts the `dbx-recon/verify` commit status that gates merge. Load it for the verify child `fanout.py` launches after a slice child reports PASS."
---

# Verify one slice PR

You are a different session from the slice's author, and that independence is the point: never
trust the child's own evidence. Checkout at the PR head with `policy.json`, `slices.json` and the
recon inputs taken from `base_branch`:

```
git checkout <pr_head>
git checkout <base> -- .migration/policy.json .migration/slices.json .migration/recon/<slice>/inputs/
```

## Run

1. Re-run `dbx-recon` at `depth sampled` with a fresh seed (`full` when the slice sets
   `verify_depth: full`), writing under `.migration/recon/<slice>/out/`.
2. Compare `changed_paths` (`git diff --name-only <base>...<pr_head>`) against the protected set:
   a slice PR may touch `.migration/` only under `.migration/recon/<slice>/out/` and
   `.migration/authorized/<slice>/`; anything else is `protected_files_tampered`.
3. Post the result as a GitHub commit status `dbx-recon/verify` (`success|failure`) with the
   report URL, using `DBX_VERIFY_TOKEN` — a secret only verifier sessions receive.
4. Return `{parity, findings, blocking: bool, verified_head}`.

## Verdict rules

- Merge rule: `parity == PASS`, `mode in {live, snapshot, transactional}`, and no finding with
  `blocking: true`. Findings of class `structural`, `privilege_visibility` or `evidence` are
  reported, never used to pass or fail a merge unless `tolerances.blocking_findings` names the
  class.
- `mode: fixture` is development evidence; refuse it for merge.
- Exit 3 (`NOT_RUN`, including a run that dies before `result.json`) is a child FAIL with
  `failure_class: evidence`, never a merge.
- Branch protection on `base_branch` requiring `dbx-recon/verify` is a precondition of the run
  (checked by the doctor, configured by the repo admin at STOP 1), not an option; `fanout.py`
  merges only when the PR head equals your `verified_head`.
- Lakebridge `reconcile` may run as a second opinion for `delta` slices; attach disagreement as an
  `evidence` finding. `dbx-recon` stays authoritative.
