Playbook: Independently verify one completed wave and publish its evidence for wave close.

## Procedure
1. Use a fresh verifier session with the plan, manifest, child results, tolerances, source-volume declaration, and target allowlist; never grade your own conversion.
2. Choose LIVE when federation or an approved live path exists; use `--mode snapshot` for a customer export or in-perimeter dual-run when it does not. Record DEGRADED and the D10 reason.
3. Run Tier 1 row counts, Tier 2 per-column aggregates, and Tier 3 keyed row diffs at the manifest's required depth; check schema, nullability, precision, timezone, collation, and deletes.
4. Keep all federated queries, partition copies, and legacy extracts under the recorded legacy-query concurrency cap; wide pulls use size tiers, not ad hoc scans.
5. Probe adversarial boundaries: empty/all-null, duplicate keys, late arrivals, deletes, timezone/DST, decimal extremes, Unicode/collation, skew, retries, and reruns.
6. Compare cross-unit totals and consumer outputs; for ML score parity compare features, seed/model version, distributions, and agreed numeric tolerance.
7. Write a machine-readable report with mode, snapshot, populations, commands, counts, checksums, samples, failures, cap/cost, evidence paths, and finding codes. A result is PASS, FAIL, or DEGRADED with an explicit unverified-path register.
8. Hand the report, PR list, and exceptions to the orchestrator for wave close; the orchestrator owns notification and merge decisions.

## DEGRADED mode
DEGRADED recon rules apply here: every comparison baseline has a snapshot manifest with source, extraction time, and row counts.
Reports carry sample-coverage statistics and state that sample parity does not extrapolate to production distributions.
Every DEGRADED report names the mode in its header and never borrows LIVE wording; a customer-run in-perimeter recon with the delivered harness is an entry criterion for STOP E.

## Specifications
- Deliverable: independent wave report plus `dbx-recon` JSON and evidence ledger.
- Validation: source and target are read-only to the verifier, checks are rerunnable, and a fresh session can reproduce the verdict.

## Pointers
Tier definitions, source access, load posture, and D10 rules are in this skill and `references/contract.md`.
