---
name: migrate
description: "One slice, five steps: access (slice-0 only), convert, prove, shadow, route. `fanout.py` embeds this skill verbatim in every slice child's prompt; load it for any slice work, including a slice-0 access ticket."
---

# Migrate one slice

Cold-start inputs: the slice id, `policy.json`, `slices.json`, the dialect skill named by
`source.family`, and the official databricks skills via `target-routing`. Work only inside the
slice's diff budget: `.migration/` writes go only under `.migration/recon/<slice>/out/` and
`.migration/authorized/<slice>/`. Report structured output
`{status: PASS|FAIL|BLOCKED, slice, pr_url, branch, parity, findings: [{class, code, detail}],
failure_class?, changed_paths: [...], recon_cost, one_line_summary}`.

## 1. Access (slice-0 only)

Slice-0 always exists, with zero tables when nothing is shared. Make `source.access` resolvable
using the official skill the estate calls for: a federation connection + foreign catalog, a
Lakeflow Connect pipeline into a landed schema, or a volume the export is copied into and loaded
in FK order. Prove it with a row count per table, and record
`{access, kind: federation|landed|loaded_from_export, pipeline_id?, export_manifest?}` in
`.migration/recon/slice-0/out/access.json`. Every later slice reads `source.access` and never
re-creates it. Access prerequisites that are the customer's (connection privilege, connector DB
user and CDC enablement, export delivery) are `blockers` on the plan resolved at STOP 1, never a
mid-run halt. For `loaded_from_export` the tables are a frozen snapshot: `mode: snapshot` is the
only merge-evidence mode and the route packet says so.

## 2. Convert

`databricks labs lakebridge transpile --source-dialect <family> --input-source <slice dir>
--output-folder <work dir> --error-file-path <work dir>/errors.log` for SQL/DDL/views; PL/SQL
bodies, triggers, schedulers and anything in `errors.log` are converted by hand against the
dialect skill's traps table. `delta` track → DBSQL scripting / SDP materialized views per
`databricks-dbsql` and `databricks-pipelines`; `lakebase` track → PL/pgSQL on branch
`targets.lakebase.branch`, reference data via `databricks postgres create-synced-table` from the
mirror. One DAB per slice named `mig_<run>_<slice>`, target `migration`, schedules `PAUSED`.

## 3. Prove

`dbx-recon run --unit <slice> --family <family> --mode live|transactional --depth threshold
--out .migration/recon/<slice>/out/`; retry cap 3; `data` findings are fixed in converted code;
`rerun-proof` once. Open the PR: parity on line 1, findings table, bill (`acus_consumed`, wall
time) as the last section.

## 4. Shadow

Deploy a Lakeflow Job `mig_<run>_<slice>_shadow` that runs the slice on the legacy cadence and
`dbx-recon run --mode continuous` after each cycle into `<catalog>.<schema_prefix>recon.drift`;
`tolerances.green_cycles` (default 3) consecutive PASS ticks gate `g-shadow-<slice>`. The job is
created paused and started by `fanout.py` after merge.

## 5. Route (after STOP 3 for the slice)

Produce the route packet (`.migration/recon/<slice>/route.md`: parity history, findings, shadow
drift, rollback steps) and the exact repoint change for the slice's `routing` mechanism
(`jdbc_url` → the config diff; `synonym` → the Oracle DDL for the customer DBA; `api` → the
client change). Devin opens the PR; the customer-held principal applies it. Legacy keeps running
for `tolerances.rollback_window` (default 24 h); shadow recon keeps ticking.
