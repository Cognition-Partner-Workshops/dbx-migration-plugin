Playbook: Set up the engagement end to end: ingest the target state (what "migrated" means) as per-workload profiles, create the durable `.migration/` working memory, pin the reconciliation tolerances, and fire the access checklist, terminating in one blocking STOP A.

## Overview
This is the single engagement-start playbook. It runs once, in two phases, and ends at the one blocking confirmation (STOP A) that authorizes all downstream work. Phase 1 makes every later session (parent, child, or one of fifty parallel fan-out children) already know what "migrated" means without re-deriving it. Phase 2 creates the durable working memory, pins the parity contract, and fires the access requests that gate everything else. Keeping the two phases in one playbook keeps the target state and the tolerances from drifting apart: they are confirmed together, as one artifact set, at one stop.

## Phase 1: Ingest the target state

### Overview
This is a **knowledge ingestion** step, not an analysis step. Its job is to make every later session (parent, child, or one of fifty parallel fan-out children) already know what "migrated" means for this engagement, without re-deriving it and without inventing an architecture mid-wave.

**The target state is not one thing.** A SQL object, an ETL pipeline, an orchestration schedule, a BI consumer, and an ML scoring job land on different target shapes. The deliverable is a **common core plus one profile per workload surface**:

| Profile | Applies to | Typical contents |
|---|---|---|
| **CORE** | everything | Unity Catalog layout (catalog/schema naming, environments), Delta conventions, naming standards, code language policy (SQL vs PySpark, when each), repo layout and Asset Bundle usage, CI gates, test conventions, forbidden patterns, secret and service-principal conventions |
| **SQL** | views, procedures, report queries | dialect translation policy, function-equivalence rules, materialization policy (view vs table vs MV), performance conventions (liquid clustering, partitioning), report-output contracts |
| **PIPELINE** | ETL mappings / jobs | target runtime (Jobs, DLT, plain notebooks), medallion layering policy, incremental vs full-load patterns, error/reject-row handling, restart and idempotency semantics, parameterization, logging |
| **ORCHESTRATION** | schedules and triggers | Workflows vs external scheduler retained, dependency and completion signalling, calendar/SLA mapping, alerting and on-call routing, backfill procedure |
| **CONSUMER** | BI dashboards, extracts, APIs | re-point vs rebuild policy per consumer type, connection/auth conventions, SLA for consumer cutover, extract format contracts |
| **ML-SCORING** | model training / scoring jobs | target framework (PySpark ML, MLflow), prediction-parity tolerance policy (seeds, float tolerance, shuffle nondeterminism), feature pipeline conventions, model registry and lineage requirements |
| **DATA / DEPENDENCY** | all workloads | coexistence mechanism (Lakehouse Federation as default read bridge), dual-write policy, data target per legacy store type, PII/masking rules, sample-data fallback policy, decommission criteria |

Each profile can be sourced independently from a **reference implementation** (an already-migrated pipeline, the strongest evidence), an **architecture/standards document**, or a **skill** that encodes the conventions mechanically. It is normal for a customer to have a reference for pipelines, a document for UC layout, and nothing for ML scoring.

Downstream, each unit's workload type selects which profile every fan-out child must follow: CORE plus the matching surface profile plus DATA/DEPENDENCY.

Nothing downstream may guess. If this phase has not run, `!dbx_migration_plan` and `!dbx_unit_migration` stop and ask for it.

```
[reference impls / standards docs / skills]  ->  THIS PLAYBOOK  ->  <Estate>_target_state.md
                                                                     (CORE + per-workload profiles)
                                                                  +  knowledge note (auto-loaded)
                                                                  +  optional skill(s)
```

### What's Needed From User
- **What exists, per workload surface**: for SQL, PIPELINE, ORCHESTRATION, CONSUMER, ML-SCORING and DATA/DEPENDENCY, is there a reference implementation, a standards document, a skill, or nothing? Ask surface by surface; do not accept a single global answer.
- **Which surfaces are in scope**. An estate with no ML jobs does not need an ML-SCORING profile; record it as N/A rather than inventing one.
- **Workspace and environment topology**: workspaces per environment, existing warehouses and clusters to reuse, Unity Catalog metastore layout, where migrated code lives (repo roles: SOURCE for legacy exports, TARGET for Databricks code, DOCS). Detect and propose with evidence; never assume a single-repo shape.
- **Non-negotiables** fixed by the customer's platform team (UC naming, allowed compute, network policy, PII handling, deployment mechanism).
- **Defaults to apply where the customer has no opinion**, stated explicitly rather than smuggled in.

### Procedure
1. **Collect the sources per surface.** Read each standards document end to end; clone and read each reference implementation (a real migrated pipeline beats any document, so prefer it and cite real files); read each skill. Record every source with a link or `file:line` cite and note which surface it covers.
2. **Fill CORE first**, then each in-scope surface profile. Every field is either **FACT** (cited from a source) or **PROPOSED** (your default, flagged for confirmation). No field may be silently blank, and no surface silently skipped: mark N/A with a reason if out of scope.
3. **Reconcile across profiles.** Where two surfaces disagree on something that should be shared (naming, error handling, test conventions), decide whether the difference is deliberate or accidental drift, and say which. Push everything genuinely shared down into CORE so profiles stay small.
4. **Derive a proposal for every gap.** Where a surface has no reference at all, propose the closest analogue (usually CORE plus the nearest sibling profile) and mark it PROPOSED. Never leave a downstream child session to improvise: with 20-50 children running in parallel, an unstated convention becomes 20-50 divergent guesses.
5. **Capture the drift rules per profile**: the concrete list of what gets a PR rejected on that surface. This is what fan-out children actually need, and it is more useful than the positive conventions alone.
6. **Write `<Estate>_target_state.md`** to DOCS: the CORE section, one section per surface profile (including N/A ones with reasons), FACT/PROPOSED marking per field, source cites, cross-profile reconciliation notes, drift rules, and open questions.
7. **Persist it as knowledge.** Create or update a knowledge note holding the confirmed summary and the artifact path, scoped to the target repos, so later sessions load it automatically. Where conventions are mechanically checkable, also write a skill per surface in the target repo.
8. **Queue the PROPOSED fields for STOP A.** Do not block here; the target-state confirmation happens once, together with the tolerance and access confirmation, at the single STOP A that closes Phase 2. Update the document and the knowledge note with the outcome after STOP A.

### Specifications
- Deliverable: `<Estate>_target_state.md` in DOCS with CORE plus one section per surface profile, a knowledge note pointing at it, and optional skills. Every field FACT-cited or explicitly PROPOSED-and-confirmed; every out-of-scope surface marked N/A with a reason.
- The document must be sufficient for a child session that has never seen the customer's estate to write conformant code for its surface from CORE plus its profile alone. This is the load-bearing requirement for parallel fan-out.
- Validation: no blank fields; every FACT carries a cite; every PROPOSED field was explicitly confirmed; cross-profile conflicts resolved or recorded; the knowledge note resolves to the committed artifact.

### Advice and Pointers
- **A reference implementation outranks a reference document.** Documents describe intent; the merged pipeline shows what reviewers accept. Cite real files.
- **Ask per surface, not once.** The common failure is inheriting a SQL-shaped target state and discovering, mid-fan-out, that nobody decided the pipeline restart model or the reject-row convention.
- The **ORCHESTRATION and CONSUMER profiles are where most engagements are thinnest**. Scheduler completion signalling and dashboard re-pointing are decisions with long consequences; force them here rather than in a wave.
- For ML-SCORING, force the **tolerance conversation now**: seeds, float tolerance, Spark shuffle nondeterminism. A team that cannot state "the same prediction" is not ready to migrate scoring jobs, and it is better to learn that at STOP A than at wave 6.
- Keep profiles small and CORE fat. Anything duplicated across two profiles belongs in CORE.
- Record the workspace/repo topology once and thread it through everything; fan-out children share no filesystem with the parent.
- Re-run this playbook when the target state changes, and bump the document and knowledge note together so no session runs against a stale target.

### Forbidden Actions
- Do NOT invent, assume, or silently default any profile's runtime, UC layout, dialect policy, orchestration target, or recon tolerance.
- Do NOT apply a SQL-derived target state to a pipeline or ML workload, or the reverse.
- Do NOT collapse the profiles into a single undifferentiated description, and do NOT omit a surface silently; mark it N/A with a reason.
- Do NOT proceed past STOP A with PROPOSED fields unconfirmed.
- Do NOT hardcode workspace IDs, warehouse IDs, or a single-repo layout; record them as named facts in the document.
- Do NOT analyze the estate, plan a migration, or write pipeline code here.
- Do NOT leave the target state only in chat; it must be a committed artifact plus a knowledge note, or fifty parallel sessions will re-derive it fifty ways.


## Phase 2: Workspace, tolerances, and access

### Overview
Everything long-lived that is not the target state lives here: working context, conventions, glossary, the dependency register, the recon tolerance record, the progress ledger, and the decision log. Later playbooks append to these files, never rewrite them, so any resumed session or parallel child starts from fact.

This playbook also owns the **access checklist**: the environment prerequisites (workspace access, service principal, network path to the legacy system, sample or masked data approval) that routinely take longer than the code work. Each unmet prerequisite is registered as a D10 dependency and its request fired now.

```
THIS PLAYBOOK  ->  .migration/00_context.md          engagement facts, topology, contacts
                   .migration/01_conventions.md      working conventions, branch/PR naming
                   .migration/02_glossary.md         customer terms -> plain meaning
                   .migration/03_recon_tolerances.md THE parity contract (see below)
                   .migration/04_dependency_register.md   appended by later playbooks
                   .migration/05_progress.md         ledger: pipeline x wave x unit status
                   .migration/06_decisions.md        decision log with dates and owners
                   .migration/07_access_checklist.md D10 items, status, fired requests
```

### What's Needed From User
- **Recon tolerance decisions** (the single most important input): exact-match vs tolerance per data type (floats, timestamps, collation-sensitive strings), row-count tolerance (normally zero), aggregate tolerance, treatment of nondeterministic legacy behavior (unordered results, ties), and for ML-SCORING the prediction-parity tolerance. Propose strict defaults; the user relaxes explicitly. Also pin the **recon economics**: the row threshold above which row-level diffs switch to keyed stratified sampling plus full aggregates, and the **legacy-query concurrency cap** (how many concurrent recon queries the production legacy engine may carry, separate from session width). Both are business decisions about cost and load on a live system; they belong here, not improvised at recon time.
- **Coexistence mechanism confirmation**: Lakehouse Federation to the legacy system (default when a JDBC path exists), or exported snapshots (when access is denied), or dual-run in the legacy environment. This decides what "reconciliation against live legacy" physically means.
- **Access facts**: what Devin can reach today (legacy system, Databricks workspace, sample data), and who approves what is missing.
- Where artifacts live (default: `docs/migration/` in the DOCS repo, `.migration/` in the TARGET repo).
- **Notification contract (optional)**: a Slack channel (via the Slack integration) or a Teams incoming-webhook URL (stored as a secret, referenced by name) where the engagement wants pings. If none is named, all stops surface in-session only and behavior is unchanged.

### Procedure
0. **Discover before asking, default before eliciting.** If a filled `00_intake_template.md` exists (from the front door or attached), consume it: its FACT rows are answered, never re-asked. For everything else, probe first (list catalogs, test the legacy read, check warehouse reachability), then present the full remaining set as PROPOSED defaults in one artifact for a single batched confirmation at STOP A. Serial live questioning is the fallback for genuinely undiscoverable, undefaultable fields only.
1. Verify the Phase 1 target-state artifact and knowledge note exist and are current; cite them in `00_context.md`.
2. Write `00_context.md`, `01_conventions.md`, `02_glossary.md` from the target state, the front-door intake, and the user's answers. Record the **interaction contract** in `00_context.md` alongside the notification contract: where blocking stops are routed (Slack thread vs web session), and the question style (one question at a time, with concrete options where the answer set is small); settling this once here means no run renegotiates it live or loses questions to interrupting notifications. The contract also fixes the message style: 2-4 short sentences, lead with the one decision, state the recommended answer and the exact reply that approves it, link artifacts instead of summarizing them; one message per event, and the only events are blocking stops, wave closes, and emergency halts. Record the notification contract: the channel or webhook secret name, and the fixed event list it receives (blocking STOPs A/B/C/E when ready for approval, wave close at STOP D with the exception count, and any fan-out halt: write-target collision or tripped circuit breaker). Nothing else pings; a channel that receives every green PR gets muted and the halts get missed. Conventions must include branch naming for fan-out children (e.g. `migrate/<pipeline>/<wave>-<unit>`) and the PR-evidence contract (every unit PR carries its own recon output).
3. **Pin the tolerances in `03_recon_tolerances.md`**: one table, per data type and per workload surface, each row FACT (user-confirmed) or PROPOSED, plus the size-tier thresholds and the legacy-query concurrency cap. Every rate or threshold row also states the **population it is computed over** (e.g. a quarantine rate over the driver population only, a halt threshold over every population); a tolerance without its population is the most common source of silently-diluted checks. This file is the contract every recon run cites; a recon result is meaningless without it. The file also states the **recon mode**: LIVE (federation/dual-run) or DEGRADED (snapshot/sample), and includes the **amendment procedure**: a tolerance changes only by explicit user approval, recorded as a new dated version of the record with the old row preserved, plus a stated re-verification scope for already-merged waves under the old tolerance. Children never see an ambiguous tolerance; they see the current version and its date.
4. Initialize `04_dependency_register.md` (empty, with the D1-D10 taxonomy header) and `05_progress.md` (empty ledger). Both are **tables, not prose**: `05_progress.md` is one row per unit (unit / status / money parity / quarantine rate / unverified paths / PR / cost so far), readable in one screen because it doubles as the cutover-readiness and sign-off view; `06_decisions.md` rows carry **provenance and blast radius** (which unit forced the decision, which PR argued it, and which already-merged units' assumptions it invalidates), because a decision added at wave 3 that silently changes a wave-1 unit is otherwise invisible.
5. **Probe the environment and build `07_access_checklist.md`**: attempt a read query against the legacy system, a query on the Databricks warehouse, and a write to the target catalog. Record each as WORKS / BLOCKED with evidence. For every BLOCKED item, append a D10 entry to the dependency register and fire the concrete request (named approver, exact permission, suggested command or console path) via the user.
6. **Write the access model** (a one-page section of `07_access_checklist.md`) for the customer's security reviewer: the three principal tiers (assessment: metadata/read-only; migration: sandbox write plus legacy read-only, scoped to in-scope schemas, no DDL/DML on legacy, no grants on production catalogs; cutover: production repoint rights, customer-held, used once at STOP E), which secrets map to which tier, and how activity is attributable per session in the customer's own audit tables. This document is what turns the D10 lead time from a discovery process into an approval.
7. Write `.migration/allowed_targets.json` during setup with only the designated migration catalog, and commit it before STOP A.
8. Commit the workspace, then **STOP A (blocking, once for the whole playbook)**: attach the target-state document, the tolerance record, and the access checklist; walk the PROPOSED target-state fields surface by surface and the PROPOSED tolerances; get explicit confirmation on all of it. Do not proceed with BLOCKED items unacknowledged; the user may accept a metadata-only phase 0 explicitly.

### Specifications
- Deliverable: committed `.migration/` workspace with all eight files, every tolerance confirmed or explicitly PROPOSED-and-accepted, every access gap registered as D10 with a fired request.
- Validation: a fresh session reading only `.migration/` and the target state can state the engagement's scope, tolerances, coexistence mechanism, and current access posture without asking.

### Advice and Pointers
- **Tolerances are a business decision wearing a technical costume.** "AVG truncation differs between engines" is a classic first recon failure; deciding the rounding rule now saves a red wave later.
- Fire access requests at maximum breadth now: service principal, federation connection approval, sample-data sign-off. They gate the fan-out width more often than compute does.
- When production access is denied (a common enterprise pattern), set recon mode DEGRADED explicitly, with its own evidence standard: snapshot manifests (source, extraction time, row counts) for every comparison baseline, sample-coverage statistics (what fraction of rows/keys/distributions the sample exercises), the caveat that sample parity does not extrapolate to production distributions, and a mandatory in-perimeter recon run (executed by the customer with the delivered harness) as an entry criterion for STOP E. Every recon report in DEGRADED mode names the mode in its header; gate language never borrows LIVE-mode confidence.
- The ledger (`05_progress.md`) is what makes 50 parallel children auditable: one row per unit, status flowing NOT_STARTED -> IN_FLIGHT -> PR_OPEN -> RECON_GREEN -> MERGED.

### Forbidden Actions
- Do NOT start Phase 2 without a completed Phase 1 target-state artifact.
- Do NOT invent tolerances silently; every row is FACT or explicitly PROPOSED.
- Do NOT skip the live environment probe or claim access works without evidence.
- Do NOT write analysis, inventory, or migration code here.
- Do NOT store credentials in the workspace files; reference secret names only.
