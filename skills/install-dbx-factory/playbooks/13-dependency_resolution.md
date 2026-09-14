Playbook: Register, decide, and implement every point where a pipeline touches something that is not migrating with it. Called from the inventory, analysis, plan, and unit-migration playbooks; never run standalone.

## Overview
Dependencies, not conversion difficulty, are what make data migrations slow and risky: the upstream feed that keeps writing to the legacy warehouse, the dashboard nobody mentioned, the scheduler that expects a completion signal, the access request that takes four weeks. This playbook is the single method for handling them, in three modes:

- **register**: find crossings mechanically, classify them, specify their contracts, append to `.migration/04_dependency_register.md` as UNDECIDED.
- **decide**: at plan time, propose a decision for every UNDECIDED entry, present them all at STOP C (resolved per `stop_mode`), record the decision and the routing point, and fire the lead-time request.
- **implement**: during a wave, build the decided mechanism (federation view, dual-write, re-pointed connection, ingestion contract) and record the evidence.

## Taxonomy

Process contract (stops, stop_mode, D1–D10, notifications, branch/merge, fan-out guards): read references/contract.md in the install-dbx-factory skill once per session; it is not restated here.

## What's Needed From User (decide mode)
- A decision per entry, from the options the class admits, with the register's contract facts in front of them.
- For every deferral: the explicit condition under which it closes, and who owns it.

## Procedure
**register mode**
1. Sweep mechanically per the source-dialect skill: connection definitions, parameter files, scheduler exports, grant/consumer metadata, query history for readers of the pipeline's outputs, writers to its inputs.
2. Classify each crossing into exactly one class. Specify the full contract: direction, format/schema, frequency, SLA, transactional expectations, auth, owner. Mark unresolvable contract fields explicitly; an unresolved contract is a plan-stop blocker, not a footnote.
3. Append to the register as UNDECIDED with cites. Never decide here.

**decide mode**
4. Propose one decision per UNDECIDED entry (the safest option the class admits, usually the read-only or coexistence-preserving one) and present the whole table at STOP C. Record for each entry the decision, routing point (the single place traffic flips at cutover), cutover condition, decommission condition, owner, and provenance (`user:<id>` or `default-accepted`). A class with no safe proposal (anything that would write to the legacy source or change tolerances) has no default and waits for a human regardless of `stop_mode`.
5. **Fire every lead-time request immediately** (access, firewall, service principal, DBA/platform tickets, consumer-team notifications), and record what was fired, to whom, and the expected lead time. Requests fire at plan approval, not when the wave needs them.

**implement mode**
6. Build exactly the decided mechanism, cite the decision entry, capture evidence (the federated view resolving, the dual-write reconciling, the re-pointed dashboard rendering), and flip the register entry to IMPLEMENTED with the evidence link.

## Specifications
- Register entries: class, contract, status (UNDECIDED / DECIDED / IMPLEMENTED / DEFERRED-with-condition), decision, routing point, fired request, evidence. Append-only.
- Validation: no wave starts with an UNDECIDED dependency it touches; no cutover with an entry lacking evidence or an unclosed deferral condition; every D10 either closed or explicitly accepted by the user as a scope constraint.

## Advice and Pointers
- **D4 consumers are the ones nobody mentions.** Query history and BI-tool metadata find them; asking the room does not. Sweep mechanically first, then confirm.
- D3 via Lakeflow Connect has legacy-side prerequisites the migration principal must never perform: enabling change tracking / CDC on the source (`ALTER DATABASE ... SET CHANGE_TRACKING`, `sp_cdc_enable_table`, Postgres logical decoding), a dedicated minimum-privilege connector user, and the gateway's network path. Each is its own D10 entry owned by the customer DBA/platform team and fired at plan approval; until it closes, the D3 decision stays at federated read or a query-based connector (higher source load; count it against the legacy-query cap). Route the build to `target-routing` → `databricks-lakeflow-connect`; the factory's rule is that the connector lands in the migration catalog with a PAUSED schedule until STOP E.
- D6 shared tables are the coexistence trap: a table with legacy writers cannot simply move. Default to legacy-remains-writer + federated read, and make dual-write an explicit, tested exception.
- Lead times dominate the schedule. A four-week access request fired at STOP A instead of wave 3 is often the entire difference in engagement duration; check fired-request status at every stop.
- D10 gates parallelism directly: fan-out width is bounded by what the service principal and warehouse are approved to run concurrently. Confirm concurrency limits as part of the D10 contract.

## Forbidden Actions
- Do NOT decide anything in register mode, and do NOT register anything without its contract or an explicit unresolved flag.
- Do NOT let a wave start against an UNDECIDED entry, and do NOT let a deferral pass without a closure condition and owner.
- Do NOT implement a mechanism other than the decided one, and do NOT mark IMPLEMENTED without evidence.
- Do NOT delay firing a lead-time request past plan approval.
