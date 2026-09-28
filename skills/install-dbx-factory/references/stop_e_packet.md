# STOP E packet: one screen

The signer reads this on a phone between meetings. Everything they must weigh fits on one screen, in this order; everything else is a link. Playbook 8 renders it from the evidence pack; the six-line card in `contract.md` is the post that carries it.

```
STOP E  cutover <pipeline>  run <tag>
Decision: authorize repointing <consumers> to <target>. Recommend: <authorize | decline: reason>

| What must match        | Legacy            | Databricks        | Recon                     |
|------------------------|-------------------|-------------------|---------------------------|
| <headline number 1>    | <value>           | <value>           | PASS, <tier>, <run link>  |
| <headline number 2>    | <value>           | <value>           | PASS, <tier>, <run link>  |
| <headline number 3>    | <value>           | <value>           | PASS, <tier>, <run link>  |

Moved: <up to 5 bullets: units, jobs, consumers now served by the target>
Not moved: <what stays legacy and why, one line each>

Exceptions and overrides (one line each, scope named):
- D-016 rerun_policy: first run, no prior shape; lifts merge policy only, parity as measured
- D-023 privilege_visibility: <catalog> unreadable to the RO principal; structure unverified there
- verifier not run on wave 0 (scaffold only; nothing merges from it)

Production untouched: every write landed in <migration targets>; grants unchanged; evidence <link>
Rollback: <one line: what flips back, who, how long>

Links: PRs · recon results · parallel-run ledger · recordings · appendix
Reply: `authorize cutover`  (or `decline cutover`)
```

Rules:

- Three headline numbers at most, chosen at STOP A ("this is what migrated means"): the ones the business would notice if wrong. Each row cites the recon run that proved it.
- Every override names its blocker class and what it does *not* change. An override that lifted a `data` blocker is not an override; it is a finding, and the packet says so.
- Rerun history, relaunch counts, plugin fixes and skill feedback go to `stop_e/appendix.md`; the signer does not need them to decide.
- No production-impact statement is inferred. "Production untouched" cites the write-target ledger and the hook probe result, or it is not written.
