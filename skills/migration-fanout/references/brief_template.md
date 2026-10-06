# Batch brief template

A brief is what one child reads before it touches anything. It fits in `BRIEF_MAX_CHARS`
(4000) because everything an estate shares is already somewhere the child reads: the wave
manifest (repo, source family, secret names, capabilities, write targets, gates), the
capabilities file (`.migration/capabilities.json`: host, principal, catalogs, warehouse),
and the embedded `child_skill`. A brief that restates them goes stale the first time one of
them changes; a brief that points at them cannot.

```
Units: <unit_id>[, <unit_id>]            # the .migration/recon/<unit_id>/ this child alone writes
Track: analytical | oltp                 # Databricks SQL / Lakeflow, or Lakebase Postgres
Source: <schema.object>[, ...]           # legacy objects converted or read; legacy is read-only
Targets: <catalog.schema.table>[, ...]   # exactly the batch's write_targets in the manifest
Spec: .migration/units/<unit_id>/mapping_spec.json (tolerances: .migration/recon_tolerances.json)
Depends on: <unit_id> merged in wave <N> | none
Dialect skill: skills/<source>-<dialect>/SKILL.md (sections: <names>)
Rerun posture: required | first_run_baseline | not_applicable   # per unit, from the plan
Gates: <gate-id>: <one line on what its evidence path proves>   # one line per manifest gate
Known: <one line per estate-specific fact the plan learned that no file above records>
Done when: recon result.json committed at the PR head; PR against <base_branch>; nothing else
```

Twelve to thirty lines. Anything a child would have to be told twice belongs in the manifest,
the capabilities file, or a dialect skill, not in every brief of the wave.
