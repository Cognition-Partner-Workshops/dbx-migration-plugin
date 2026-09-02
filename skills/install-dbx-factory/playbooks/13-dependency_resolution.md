Playbook: Register, decide, and implement every point where a pipeline touches something that is not migrating with it. Called from the inventory, analysis, plan, and unit-migration playbooks; never run standalone.

## Overview
Dependencies, not conversion difficulty, are what make data migrations slow and risky: the upstream feed that keeps writing to the legacy warehouse, the dashboard nobody mentioned, the scheduler that expects a completion signal, the access request that takes four weeks. This playbook is the single method for handling them, in three modes:

- **register**: find crossings mechanically, classify them, specify their contracts, append to `.migration/04_dependency_register.md` as UNDECIDED.
- **decide**: at plan time, propose a decision for every UNDECIDED entry, present them all at STOP C (resolved per `stop_mode`), record the decision and the routing point, and fire the lead-time request.
- **implement**: during a wave, build the decided mechanism (federation view, dual-write, re-pointed connection, ingestion contract) and record the evidence.

## Taxonomy

| Class | What it is | Typical decision options |
|---|---|---|
| D1 | intra-pipeline lineage edge | ordering constraint only; handled by wave order, no decision |
| D2 | shared object used by 2+ pipelines | migrate once in wave 0; owner pipeline per the shared-object map |
| D3 | upstream feed from a non-migrating system | federate for reads; ingestion contract (Auto Loader, CDC) at cutover |
| D4 | downstream consumer of legacy output | re-point at cutover; dual-publish during coexistence; rebuild |
| D5 | scheduler / orchestration dependency | replace with Workflows; keep external scheduler triggering Databricks; hybrid with completion signal |
| D6 | shared table with non-migrated writers | dual-write window; legacy remains writer + federated read; documented deferral |
| D7 | external hand-off (SFTP, queue, partner feed) | preserve format contract exactly; re-platform the transport at cutover |
| D8 | security / governance contract | reproduce in UC (row filters, masks, grants) before any consumer re-points |
| D9 | ML model / scoring consumer | prediction-parity gate per the ML-SCORING profile before re-pointing |
| D10 | environment / access dependency | fire the request now; track to closure; gates fan-out width |

## What's Needed From User (decide mode)
- A decision per entry, from the options the class admits, with the register's contract facts in front of them.
- For every deferral: the explicit condition under which it closes, and who owns it.

## Procedure
**register mode**
1. Sweep mechanically per the source-dialect skill: connection definitions, parameter files, scheduler exports, grant/consumer metadata, query history for readers of the pipeline's outputs, writers to its inputs.
2. Classify each crossing into exactly one class. Specify the full contract: direction, format/schema, frequency, SLA, transactional expectations, auth, owner. Mark unresolvable contract fields explicitly; an unresolved contract is a plan-stop blocker, not a footnote.
3. Append to the register as UNDECIDED with cites. Never decide here.

**decide mode**
4. Propose one decision per UNDECIDED entry (the safest option the class admits, usually the read-only or coexistence-preserving one) and present the whole table at STOP C. In hard mode walk each entry with the user; in soft mode the proposals are the stop's default and are default-accepted as a batch after the window unless a reply changes them. Record for each entry the decision, routing point (the single place traffic flips at cutover), cutover condition, decommission condition, owner, and provenance (`user:<id>` or `default-accepted`). A class with no safe proposal (anything that would write to the legacy source or change tolerances) has no default and waits for a human regardless of `stop_mode`.
5. **Fire every lead-time request immediately** (access, firewall, service principal, DBA/platform tickets, consumer-team notifications), and record what was fired, to whom, and the expected lead time. Requests fire at plan approval, not when the wave needs them.

**implement mode**
6. Build exactly the decided mechanism, cite the decision entry, capture evidence (the federated view resolving, the dual-write reconciling, the re-pointed dashboard rendering), and flip the register entry to IMPLEMENTED with the evidence link.

## Specifications
- Register entries: class, contract, status (UNDECIDED / DECIDED / IMPLEMENTED / DEFERRED-with-condition), decision, routing point, fired request, evidence. Append-only.
- Validation: no wave starts with an UNDECIDED dependency it touches; no cutover with an entry lacking evidence or an unclosed deferral condition; every D10 either closed or explicitly accepted by the user as a scope constraint.

## Advice and Pointers
- **D4 consumers are the ones nobody mentions.** Query history and BI-tool metadata find them; asking the room does not. Sweep mechanically first, then confirm.
- D6 shared tables are the coexistence trap: a table with legacy writers cannot simply move. Default to legacy-remains-writer + federated read, and make dual-write an explicit, tested exception.
- Lead times dominate the schedule. A four-week access request fired at STOP A instead of wave 3 is often the entire difference in engagement duration; check fired-request status at every stop.
- D10 gates parallelism directly: fan-out width is bounded by what the service principal and warehouse are approved to run concurrently. Confirm concurrency limits as part of the D10 contract.

## Forbidden Actions
- Do NOT decide anything in register mode, and do NOT register anything without its contract or an explicit unresolved flag.
- Do NOT let a wave start against an UNDECIDED entry, and do NOT let a deferral pass without a closure condition and owner.
- Do NOT implement a mechanism other than the decided one, and do NOT mark IMPLEMENTED without evidence.
- Do NOT delay firing a lead-time request past plan approval.
