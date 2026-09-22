Playbook: Register, decide, and implement one migration dependency with evidence.

1. **Register:** append an entry to `.migration/04_dependency_register.md` with D1–D10 class, source/target, owner, lead time, evidence, requested-by, and status; never rewrite history.
2. **Decide:** present the full contract, alternatives, blast radius, cost, deadline, and provenance at the owning stop; fire a lead-time request and record the human decision or default provenance.
3. **Implement:** use the approved mechanism, record request/result IDs and evidence, and leave a condition or rollback when unresolved; validate target scope, idempotency, and recon impact.
4. Route source access/load work through `data-reconciliation`; `target-routing` points to `databricks-lakeflow-connect` for connector paths.

## Pointers
`references/contract.md` owns D1–D10 taxonomy, stops, `stop_mode`, notifications, branch/merge, and fan-out guards. No dependency overrides `AGENTS.md`.
