# Estate types: question sets and profile routing

Four estate tracks. Each names what intake must capture and which profiles the workers use
(always CORE + DATA/DEPENDENCY plus the listed profile).

## ETL / batch pipeline

- Capture: source dialect, extractor/runtime, scheduler, mappings, rejects, restart,
  parameters, logging, output contracts.
- Hardening: preserve boundaries, retries, idempotency, quarantine, and observability before
  conversion.
- Scheduling: map D5 dependencies to Lakeflow Jobs or retain the scheduler with an explicit
  contract.
- Profiles: CORE + PIPELINE + ORCHESTRATION.

## Warehouse / reporting

- Capture: engine/version, catalogs/schemas, query history, views/procs/UDFs, BI consumers,
  schedules, SLAs, security.
- Access: prefer Lakehouse Federation for supported read-only discovery; record source-query
  cost and concurrency. If federation is denied or unsupported, use customer export or a
  connector path and record degraded mode + a blocker.
- Profiles: CORE + SQL + CONSUMER.

## Code / models / prediction consumers

- Capture: separate data movement, application code, model training, scoring, and consumers;
  give each an owner and target. Feature definitions, seeds, model version, numeric tolerance,
  prediction distributions, and a legacy bit-stability probe.
- Scope: repositories, jobs, packages, secrets by name, inputs/outputs, schedules, tests,
  runtime assumptions.
- Profiles: CORE + the matching PIPELINE, ML-SCORING, or CONSUMER profile.

## OLTP / operational database

- Track split: operational transactions and analytical/CDC consumers get separate units,
  owners, cutover gates, and parity checks.
- Target: Lakebase project/branch for operational state; Delta/SQL warehouse for analytical
  state; declare both in `allowed_targets.json`.
- CDC: customer-owned setup, source read-only principal, lag evidence, ordering, deletes,
  replay, and rollback.
- Transactions: preserve constraints, identity, isolation, retries, uniqueness, timestamps,
  and atomic boundaries; never invent a weaker contract.
- Routine EXECUTE: ask whether the read-only principal may hold EXECUTE on the packages and
  procedures under test; without it every writing routine stays `unproven`
  (`skills/data-reconciliation/SKILL.md`, "Routine parity") and the answer is a blocker.
- Profiles: CORE + LAKEBASE for the operational track; CORE + the matching SQL/PIPELINE/
  CONSUMER profile for the analytical track.

## Cross-cutting

- Ask which pipelines share write targets or source objects; disjoint ones run as sibling wave
  manifests after the plan is approved.
- Write `.migration/allowed_targets.json` (catalogs, legacy_sources) before any source probe;
  authorized legacy writes carry `DBX_DECISION=<id>` matching `.migration/authorizations.json`.
- Invoke an optional dialect skill only when installed; never infer skill existence from an
  adapter or Lakebridge flag.
- Bundle deploys and runs that hit a platform 5xx follow the bounded retry rule in
  `skills/target-routing/SKILL.md`.
