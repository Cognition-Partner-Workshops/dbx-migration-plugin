---
name: prediction-parity
description: ML prediction-parity harness for migrated scoring jobs. Use when a migration unit is an ML scoring job (SAS, SPSS, R, SparkML, BigQuery ML) and its Databricks port must be proven equivalent to the legacy scorer.
---

# Prediction Parity Harness

Prove a migrated scoring job produces equivalent predictions to the legacy scorer, per the user-owned parity tolerance in `.migration/03_recon_tolerances.md`.

## Preconditions
- A parity tolerance exists and is user-confirmed: exact match, numeric tolerance on scores, or rank-order preservation. Never invent one.
- **Legacy bit-stability probe**: before any conversion effort, run the legacy scorer twice on the pinned sample. If legacy is not bit-stable with itself, an exact-match tolerance is structurally unachievable; report this and get the tolerance revised before proceeding.

## Method
1. Pin the comparison sample: a fixed, versioned input dataset with a manifest (source, extraction time, row count, checksum). All runs use it.
2. Pin determinism: seeds set on both sides; document Spark shuffle nondeterminism and floating-point accumulation-order effects where they apply.
3. Dual-score: legacy scorer and migrated scorer on the pinned sample.
4. Compare per tolerance: exact (bitwise/decimal equality), numeric (abs/relative diff per score), or rank-order (Spearman/Kendall on the ranking, top-k overlap where the consumer is a ranking).
5. Feature parity first when scores diverge: compare the intermediate feature values before blaming the model; most divergence is in the feature pipeline, not the scorer.

## Evidence format
Sample manifest, both run commands/environments, per-metric comparison table, verdict against the named tolerance version, and any divergence diagnosis. Scientific judgment (is this drift acceptable) belongs to the customer; the harness reports, it does not decide.

## Forbidden
- No re-modeling, retraining, or "improving" the model during migration.
- No relaxing the parity tolerance without explicit user approval.
