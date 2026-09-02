Playbook: Front door for code and ML scoring estates (SAS, SPSS, R, Hadoop/Hive, on-prem Spark, legacy Python/notebooks, BigQuery ML). Thin intake that pins the languages and workload mix, loads the dialect skill(s), activates the ML-SCORING profile where models are present, and then runs `!dbx_migrate_pipeline` in the same session. No migration method lives here.

## Overview
The customer says "we have ten years of SAS" or "our scoring jobs run on the Hadoop cluster." This playbook turns that into a configured run of the standard chain, with one family-specific addition: where the estate contains **model training or scoring code**, the parity question changes from "same rows" to "same predictions," and that distinction must be pinned at intake, not discovered at recon time.

## What's Needed From User
- The language inventory (SAS, R, SPSS, HiveQL, Pig, Scala/PySpark versions, notebook platforms) and where the code lives (repo, server directories, scheduler-embedded).
- **Which parts are ML**: training jobs, scoring/inference jobs, feature preparation. For each scoring job: is there a business owner who can state an acceptable prediction-parity tolerance (exact match on scores, rank-order preservation, or a stated numeric tolerance)? A team that cannot state this is not ready to migrate that job; scope it out explicitly rather than absorbing the ambiguity.
- Runtime access for dual-runs: can the legacy code still be executed (SAS server, Hadoop cluster) to produce comparison outputs? If not, historical outputs become the recon baseline and the caveat is recorded.
- **Parity feasibility probe per scoring job (before any conversion effort)**: run the *legacy* scorer twice on the pinned comparison sample. If legacy itself is not bit-stable across two runs, an exact-match tolerance is structurally unachievable, and the tolerance conversation happens here, at intake, where it is cheap: propose rank-order preservation or a numeric tolerance calibrated to legacy's own run-to-run variance. Where legacy cannot be executed at all, say explicitly that exact-match cannot be promised and record the historical-baseline caveat.

## Procedure
1. **Consume the intake template first**: if a filled `00_intake_template.md` was attached or committed, load it, mark its rows FACT, probe the environment to fill what it left blank (DISCOVERED), and propose defaults for the rest (PROPOSED). Ask live only what neither the template nor a probe can answer.
2. Pin languages, versions, and code locations; probe repo/server access; register D10s.
3. Attach the matching dialect skill(s) (`sas-programs`, `hadoop-hive`, `sparkml-legacy`, ...). Multiple skills are normal in this family.
4. Partition the estate at intake into **data code** (ETL-like: standard chain, standard recon) and **model code** (training/scoring: ML-SCORING profile, prediction-parity gate, D9 consumers). This partition drives the inventory's workload typing.
5. Set family defaults: unit = program/script/notebook + its schedule entry; lineage extraction = parser + directory convention + scheduler export (weakest lineage of the three families, so INFERRED edges are expected and priced); dual-run mechanism per the runtime-access answer; prediction-parity tolerances routed into `03_recon_tolerances.md` at setup.
6. Record intake facts in `.migration/00_context.md` shape. Then run `!dbx_migrate_pipeline` **in this same session**, right away. Do not open a new session, do not ask the user to run it, do not stop here: the orchestrator reads the intake facts you just wrote and begins ingest and setup as normal. The user's next message from Devin is STOP A.

## Specifications
- Deliverable: configured hand-off: languages pinned, ML partition made, dialect skills attached, dual-run mechanism stated, parity posture per scoring job recorded or explicitly scoped out.
- Validation: `!dbx_migrate_pipeline` was invoked in this session and the orchestrator can start ingest without re-asking anything this intake covered; no scoring job enters scope without a stated parity tolerance.

## Advice and Pointers
- The ML partition is the credibility line: Devin migrates the engineering around models (pipelines, scoring jobs, lineage, upgrades) gated on prediction parity; it does not re-model. Scientific judgment (features, labels, model choice, release) stays with the customer's team, and saying so at intake wins trust.
- Prediction parity has known nondeterminism traps: seeds, float accumulation order, Spark shuffle. The tolerance conversation at intake is where these surface cheaply.
- SAS estates hide business logic in macro libraries and format catalogs; the dialect skill treats those as shared objects (D2), and the inventory should expect a fat wave 0.
- This family has the weakest mechanical lineage; budget more analysis time per pipeline and lean harder on last-run evidence for scope cutting.

## Forbidden Actions
- Do NOT begin inventory, analysis, or conversion here; hand off to the chain.
- Do NOT accept a scoring job into scope without a stated, user-owned parity tolerance.
- Do NOT accept an exact-match parity tolerance without the legacy bit-stability probe (or an explicit user acceptance that it was impossible to run).
- Do NOT position or attempt re-modeling, feature redesign, or model-quality improvement; that is out of kit scope by design.
- Do NOT treat notebook code as dead just because it lacks a schedule; PROPOSED-unused with evidence, per the inventory rules.
