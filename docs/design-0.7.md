# dbx-migration-factory 0.7: design spec

Status: draft for red-team. Decides the shape of 0.7; nothing here is implemented. Decisions taken
with the owner on 2026-10-30: safety lives in platform identity and isolation (F1), merge authority
is parity only (F2), three human stops (F3), the unit of work is a consumer slice (F4), the source is reached through one Unity Catalog reference the official
skills set up, never a hand-written loader (F5), Lakebridge writes the first conversion draft where it has a
dialect (F6). Whether 0.7 is a new tree or a trim of 0.6.2 is decided after this spec is red-teamed (F7).

Why: four unattended runs on 0.6.x halted on the plugin's own policy machinery and never on data;
the last run spent ~170 ACU and 8.5 h for zero merged units with six human stops, one of them a real
decision. Line budget for 0.7: ≤ 10,000 lines including tests (0.6.2: 48,764), with the recon
harness the majority.

## 1. Model

A migration is a set of **slices**. A slice is a consumer-facing capability (invoicing, rating,
dunning) and owns: source tables, routines, jobs, the consumers that read it, and its target track
(`delta` or `lakebase`). Shared reference tables that several slices read and none owns form
`slice-0`, always first. Each slice runs the same five steps and ends in one PR with one recon
verdict. Routing a slice's consumers to Databricks is a per-slice cutover; legacy stays as fallback
for the slice's rollback window. This is strangler fig: the estate is replaced one capability at a
time, with the two systems coexisting until the last slice routes.

```
assess ─▶ plan.yaml ──[STOP 1 approve]──▶ slice-0 (source access + reference tables) ─▶ slice-1 ──[STOP 2 canary]──▶ slice-2..N (auto-merge)
                                                                     each slice: convert ─▶ prove ─▶ shadow ─▶ ──[STOP 3 per slice]──▶ route
```

Three kinds of human stop, nowhere else (count per run: 1 + 1 + one route decision per `route_group`):

| Stop | What the human approves | Where | Pre-selected default |
|---|---|---|---|
| 1 Plan | slices and their order, targets, tolerances, coexistence window, cutover owner, `routing` mechanism per slice, access prerequisites (connection privilege, connector DB user, export delivery) | Plan view, `plan.yaml`; the human's merge of the policy PR is the approval, the Plan entry links it | yes: every decision has `selected` unless `important: true` |
| 2 Canary | the first non-zero slice's PR: conversion pattern, evidence shape, bill | PR review + Plan view gate `g-canary` | `auto_merge: false` for slice-1 only |
| 3 Route | repoint of one slice's consumers (`d-route-<slice>`), by the customer-held principal | Plan view decision | none; `important: true, selected: null` |

Everything else merges on evidence or halts with a card. A halt is: parity FAIL after the retry
cap, a guard block, a platform permission error on a write, breaker tripped (3 same-class
failures), source lag beyond `tolerances.source_lag_s`. A halt card ends with the one reply that
resumes it; a plugin defect is a `plugin finding:` entry on the step, never a question.

## 2. Repository layout

```
.devin-plugin/plugin.json      name, version, requiredPlugins: databricks/databricks-agent-skills (sha pinned)
AGENTS.md                      8 rules (section 3)
README.md                      install; policy.json; how a slice runs; the three stops; version matrix
CHANGES.md
hooks.json                     PreToolUse: exec + edit/write/MultiEdit → hooks/guard.py
hooks/guard.py                 ≤ 200 lines (section 4) + tests/test_guard.py (table-driven, ≤ 400 lines)
tools/doctor.py                ≤ 400 lines, 5 checks (section 5) + tests
tools/fanout.py                ≤ 500 lines (section 7) + tests
tools/slices.py                slices.json validation, dependency closure, collision check (section 6)
harness/                       dbx-recon, lifted from skills/data-reconciliation/harness with the verdict change (section 8)
skills/plan/SKILL.md           manager: assess → slices → plan.yaml → policy PR → doctor → tickets
skills/analyze/SKILL.md        one ticket: Lakebridge analyze + census + consumer map → slices.json proposal
skills/migrate/SKILL.md        per slice: convert, deploy, prove, shadow, route (section 9); embedded in every child prompt
skills/verify/SKILL.md         independent verifier → PR status check (section 10)
skills/dialects/<family>/      oracle-plsql, tsql-ssis, teradata-bteq, informatica-xml (traps table, canonicalization.json, examples)
skills/target-routing/SKILL.md step → official databricks skill table; auth rules (kept, halved)
.migration/policy.json         the one committed contract (section 6)
.migration/slices.json         slices, written by analyze, approved with the plan
.migration/recon/<slice>/inputs/   mapping, dependencies, canonicalization, grading, rerun_posture (manager-committed, protected)
.migration/recon/<slice>/out/      result.json, report.md, rerun_proof.json, access.json (slice-0); with .migration/authorized/<slice>/ the only paths a slice PR may touch under .migration/
.migration/recon/<slice>/fanout.json, route.md   manager-written on base_branch, never in a slice PR
```

Deleted from 0.6.2: `skills/{intake,migration-planning,workspace-setup,estate-inventory,pipeline-analysis,wave-plan,wave-verify,unit-migration,migration-fanout,parallel-run,cutover,factory-doctor}`, `hooks/dbx_guard.py`, `skills/test_no_*.py`, `skills/test_skill_manifests.py`, `OVERVIEW.md`, `references/{guards,checks,estate-types,decisions,phase1-steps}.md`, `.migration/{allowed_targets,recon_tolerances,capabilities,authorizations}.json`, `.migration/waves/`, `.migration/units/`, `~/.dbx-migration-factory/`, the doctor signature, the hook nonce, `plan_sha`/`manifest_sha`, `gate_waived`, `merge_override`, `source_principal_attested`, `by_design_unverified` manifest mirroring, `serialized_pipelines`, the pipelines barrier.

## 3. AGENTS.md (complete text)

```
1. The legacy source is read-only in every phase; a reconciliation failure is fixed in converted code.
2. Credentials are named secrets; never print, commit or paste a value.
3. Writes go only to the targets in the committed .migration/policy.json (the copy on the run branch's
   protected base); the migration principal holds no grant anywhere else. A guard block is a halt to
   report, never something to re-shape or retry.
4. Devin never writes to the legacy source and never holds a write-capable legacy credential. A legacy
   change the migration needs is a script in the slice PR under .migration/authorized/<slice>/, run by the
   customer's DBA; the PR records who ran it and when.
5. The dbx-recon parity verdict from the verifier session is the merge authority; structural, privilege
   and evidence findings are reported, never used to pass or fail a merge.
6. Devin never repoints a production consumer; routing runs under the customer-held principal after the
   human selects d-route-<slice>.
7. The human is asked exactly three times: plan approval, the canary PR, each route decision.
8. Cards: decision first, the exact reply that approves it last, artifacts linked, never summarized.
```

## 4. hooks/guard.py

The guard enforces only what the platform cannot: three irreversible classes. Everything else is
`warn` (approve with the reason attached) and appended to `.migration/guard.log` (gitignored) for the
card's audit line. There is no shell parser: each rule is a regex family over the raw command text
and the edit tool's `file_path` + content.

| # | Rule | Decision | Regex family (illustrative; the test table pins exact strings) |
|---|---|---|---|
| G1 | A legacy-only client, or a `legacy_sources` name, used for anything but an inline read | block | clients `sqlplus|sqlldr|bteq|mload|fastload|tbuild|tdload|snowsql|sqlcmd|psql|mysql`, or any `legacy_sources[]` token, on a command line that also has a write verb `\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|EXEC(UTE)?|CALL|BEGIN|COMMIT)\b` outside single quotes, **or** a script input (`-f|-i|@|<`), a pipe into the client, or an `ssh`/`docker exec` payload containing the client. No downgrade exists: there is no `DBX_DECISION`, no authorized-write entry and no write-capable legacy secret in 0.7 (§3 rule 4). Reads go through `source.access`, not legacy clients, so the rule costs nothing in practice. |
| G2 | A deploy to a production target | block | `databricks bundle (deploy|run|destroy)` or `dbt (run|build|seed)` with `-t|--target` matching `policy.forbidden_targets` (default `prod`, `production`); any `--target` not in `policy.bundle_targets`; `databricks postgres` resource path containing `/branches/production` with a write verb (`create-|delete-|update-`). |
| G3 | Editing a credential store | block | `file_path` matching `(^|/)\.databrickscfg$|\.databricks/|\.config/databricks/`; any command assigning `DATABRICKS_(HOST|TOKEN|CLIENT_ID|CLIENT_SECRET)=` or editing `/opt/.devin/envrc`. Edits to `policy.json`, `slices.json`, `.migration/base` and `.migration/recon/*/inputs/` pass the guard and are fenced at merge (CODEOWNERS lines naming exactly those paths, and `fanout.py`/`verify` failing a slice PR whose `git diff --name-only <base>...<head>` touches them). |
| G4 | Every other exec event | warn | one `guard.log` line per exec event: `{ts, rule: none|G1..G5, decision, cmd[:200]}`; nothing is silent. |
| G5 | Edits under the running plugin tree | block | `file_path` under `realpath(__file__)/../..` |

Arming: the guard is armed iff a `.migration/` directory exists in the working tree at or above the
command's cwd (or the edit's `file_path`); otherwise it exits 0 with no output, so it never fires in
another plugin's or an application's repo. `<base>` is the one line of `.migration/base` (written by
the plan step, committed with the policy, e.g. `mig/<run>`), read from `git show origin/HEAD:.migration/base`
else the working copy, else `origin/HEAD`. `policy.json` is `git show <base>:.migration/policy.json`,
never the working copy. Armed with no committed policy (the window between `git init` of `.migration/`
and the policy PR merging) → G1/G2/G3/G5 apply with `legacy_sources = []` and `bundle_targets = []`,
so every deploy blocks until the policy PR merges; reads are never blocked. Outside a run repo the
guard is silent by design: the only legacy credential any session holds is read-only (§3 rule 4), so
the fence there is the principal, not a hook; 0.6.2's org-wide hook firing in unrelated repos is the
behaviour this removes. Output contract is
unchanged (`{"decision": "block"|"warn", "reason": ...}` on stdout, exit 2 on block).

G2 is deliberately not a destination allowlist: the destination fence is the migration principal's grants, proven by the doctor's `isolation` check (§5), and `fanout.py` refuses to launch while `isolation` is not `ok`. `policy.json` and `slices.json` are further protected by a `CODEOWNERS` rule on `.migration/` requiring a human review on `base_branch`, and `fanout.py`/`verify` fail any slice PR that touches them (`protected_files_tampered`). The guard is a no-op in a checkout with no `.migration/policy.json` on its base; it never fires in another plugin's or project's repo.

Removed on purpose: SQL text classification, catalog resolution, Lakebase project/branch parsing,
curl/REST path parsing, Python literal inspection, git subcommand tables, `cd`/`ssh`/`docker`
re-rooting, identity-swap detection beyond G3, the hook nonce probe.

## 5. tools/doctor.py

Run by the manager after the policy PR merges and by `fanout.py` before launching children. Six
checks, each `ok|fail|warn` with `data`; `ready` is every check not `fail`. No signature, no
`capabilities.json`, no pointer file; the report is printed and attached to the ticket.

| Check | How | fail when |
|---|---|---|
| `identity` | `databricks current-user me`; `DATABRICKS_AUTH_TYPE` in `{oauth-m2m, env-oidc}`; principal equals `policy.migration_principal` | mismatch or PAT |
| `isolation` | enumerate, do not list: `SHOW CATALOGS` then `databricks grants get-effective` per catalog, plus external locations, storage credentials and connections visible to the principal; `databricks postgres list-projects` and roles per project. Expects write/ownership privilege on nothing but `policy.targets` and `SELECT`-class on `source.access` (and `forbidden_catalogs`, which exist only to make the intent reviewable) | any write or ownership privilege outside `policy.targets`; any role on a Lakebase project other than `targets.lakebase.project`; any role on its `production` branch |
| `policy_committed` | `git diff --quiet <base> -- .migration/policy.json .migration/slices.json` and schema validation (section 6) | working copy differs or invalid |
| `source_read_only` | effective privileges, not grant rows: Oracle `SESSION_PRIVS` + `ALL_TAB_PRIVS` + ownership of the slices' schemas; SQL Server `fn_my_permissions`; Postgres `has_table_privilege` + ownership; Databricks (`kind: existing`) `grants get-effective` on the source catalog; routine globs expanded first | any write/ALTER/EXECUTE-on-writer privilege or ownership |
| `hook_live` | runs `python - <<'' ` with a G5 probe (`file_path` under the plugin tree) through the hook entry point and asserts exit 2 | hook missing or not firing |
| `harness` | `dbx-recon selftest`; target reachable (`SELECT 1` on `policy.warehouse_http_path`, `SELECT 1` on the Lakebase endpoint when a slice is `lakebase`) | selftest fails or unreachable |

## 6. The committed contract: `.migration/policy.json` and `.migration/slices.json`

```json
{
  "version": 2,
  "repo": "github.com/<owner>/<name>",
  "base_branch": "mig/<run>",
  "migration_principal": "<application id or SP name>",
  "targets": {
    "catalog": "mig_cat",
    "schema_prefix": "mig_<run>_",
    "lakebase": {"project": "acme-billing", "branch": "mig-<run>", "endpoint_host": "ep-xxx.database.us-east-1.cloud.databricks.com"},
    "warehouse_http_path": "/sql/1.0/warehouses/<id>"
  },
  "forbidden_catalogs": ["prod_cat"],
  "forbidden_targets": ["prod", "production"],
  "bundle_targets": ["migration"],
  "legacy_sources": ["LEGACY_RO_DSN", "legacy-host.example"],
  "width": 3, "breaker": 3,
  "source": {"family": "oracle", "secret": "LEGACY_RO_DSN", "access": "mig_cat.legacy_fed"},
  "tolerances": {"version": "tol-<date>-v1", "exact": true, "per_type": {}, "source_lag_s": 900,
                  "full_diff_row_threshold": 1000000, "blocking_findings": []}
}
```

- `source.access` is the one Unity Catalog reference every slice and the harness read the source through: always a queryable `<catalog>` or `<catalog>.<schema>`: a foreign catalog (Lakehouse Federation) or a landed schema (Lakeflow Connect CDC or query-based pipeline, or, with no network path, tables slice-0 loads from an export volume with `COPY INTO`). A volume path is never the access value, `access` is required (`policy_committed` fails without it), and `kind: existing` covers a source that is already a Unity Catalog or `hive_metastore` catalog (Databricks→Databricks); for that kind the source catalog is listed in `forbidden_catalogs` because G1 has no lexical handle on it. The harness reads `source.access` with its Databricks adapter in every kind; the native family adapters remain only for `source_read_only` and for `mode: live` against the legacy engine itself. `tolerances.source_lag_s` is pipeline watermark against source high-water (not last-update recency), for `kind: landed` only. Which one, and how it is created, is slice-0's job through `target-routing` (`databricks-lakeflow-connect`, `databricks-dbsql` federation, `databricks-unity-catalog`); the plugin holds no connector or loader code. `tolerances.mirror_lag_s` applies only to a landed schema (read from the pipeline's last update) and is otherwise ignored.
- `tolerances.blocking_findings` lists structural classes the plan chose to make merge-blocking (`[]` by default). Any class not listed is a finding.
- `width` is the fan-out parallelism, `breaker` the same-class failure count that stops launching.

```json
{
  "version": 1,
  "slices": [
    {"id": "slice-0", "name": "reference", "track": "delta", "tables": ["BILLING.TENANTS", "BILLING.PLANS", "BILLING.CODES"],
     "routines": [], "jobs": [], "consumers": [], "depends_on": [], "routing": "none", "auto_merge": true},
    {"id": "slice-1", "name": "plans", "track": "lakebase", "tables": ["BILLING.SUBSCRIPTIONS", "BILLING.SUBSCRIPTIONS_HIST"],
     "routines": ["PKG_PLANS.*"], "jobs": [], "consumers": ["billing-api"], "depends_on": ["slice-0"], "routing": "jdbc_url", "route_group": "billing-api", "canary": true, "auto_merge": false, "verify_depth": "full", "rollback_window_h": 72, "green_cycles": 3},
    {"id": "slice-2", "name": "rating+invoicing", "track": "lakebase", "tables": ["..."], "routines": ["PKG_RATING.*", "PKG_INVOICING.*"],
     "jobs": ["NIGHTLY_RATING"], "consumers": ["billing-api", "finance-report"], "depends_on": ["slice-1"], "routing": "jdbc_url", "route_group": "billing-api",
     "shared_writes": {"RATING_RESULTS": "merged:slice-2"}, "excluded": [{"object": "BILLING.AUDIT_LOG", "decision": "d-exclude-audit"}]}
  ]
}
```

`tools/slices.py --check` fails when: a consumer appears in two slices whose `routing` is not independently switchable per capability (`jdbc_url`, `synonym`) unless those slices share a `route_group` (routed together at one STOP 3); a table appears in two slices without a `shared_writes`
disposition (`merged:<owner>` = the table is converted exactly once, in `<owner>`'s bundle, and the other slice treats it read-only; `dual_write:<owner>` = owner writes both target and, through the customer-run authorized script, legacy, until the other routes; recorded at STOP 1, never a runtime decision); a routine's transitive writes (from the dialect skill's dependency scan)
touch a table outside its slice and outside `slice-0`; a table of a routable slice is read by a routine or job of another slice that is not in the same `route_group` (readers come from the same dependency scan; an estate whose read closure is everything is one `route_group`, and the plan says so at STOP 1 rather than discovering it at route); a census object is assigned to no slice and not in an `excluded` row carrying a decision id; `depends_on` has a cycle; a `canary` slice is
not the first non-zero slice. Slice fields: `auto_merge` (default true; false for the canary), `verify_depth` (`sampled`|`full`), `rollback_window_h`, `green_cycles` (shadow cycles before the route card), `route_group`, `excluded`. This replaces `derive_waves.py`, the write-target manifests and the
exact-set rule: the check is a plan-time refusal the human sees before approval, never a launch halt.

## 7. tools/fanout.py

Input: `slices.json`, `policy.json`, the plan's `width` (default 4) and `breaker` (default 3). For
each slice whose `depends_on` are all merged (PR merged on `base_branch`): launch one `migrate`
child with a brief ≤ 4,000 chars pointing at the slice id; when the child reports `status: PASS`
with `pr_url`, launch one `verify` child; on verifier PASS and `auto_merge: true` (every slice except
the canary) merge the PR; write `.migration/recon/<slice>/fanout.json`. Breaker: stop launching after
`breaker` failures of the same `failure_class`. Collision: two running slices with a shared write
table and no `shared_writes` disposition is a launch refusal (cannot happen after `slices.py --check`,
kept as defence). No run log, no manifest sha, no lock file, no pointer; the manager re-runs `fanout.py` on every PR merge event and every STOP reply, and it skips slices whose PR is merged and relaunches the rest.

Child report schema (structured output): `{status: PASS|FAIL|BLOCKED, slice, pr_url, branch, parity:
PASS|FAIL, findings: [{class, code, detail}], failure_class?, changed_paths: [...], recon_cost,
one_line_summary}`. `changed_paths` is informational; `fanout.py` computes `git diff --name-only <base>...<pr_head>` itself and fails a child whose diff touches `.migration/` outside `.migration/recon/<slice>/out/` and `.migration/authorized/<slice>/` (`protected_files_tampered`); `fanout.json` and `route.md` are the manager's own commits on `base_branch`. It merges only when the PR head equals the `verified_head` in the verifier's report.

## 8. Harness: parity is the verdict

Recon inputs (`mapping`, `dependencies`, `canonicalization`, `grading`, `rerun_posture`) are plan artifacts the manager commits under `.migration/recon/<slice>/inputs/` before fan-out; a child may write only `.migration/recon/<slice>/out/`. Row differences are parity FAIL, not a finding; `findings[].class` is one of `structural|privilege_visibility|evidence|rerun_policy`, and `tolerances.blocking_findings` may name any of those classes (class-granular: the per-object `merge_override` of 0.6.2 has no equivalent, by decision). For `kind: loaded_from_export` the export manifest is the only source-truth anchor; slice-0's count compares the landed tables against it and the route packet says so.

`dbx-recon` keeps every tier and adapter. `result.json` changes:

| Field | 0.6.2 | 0.7 |
|---|---|---|
| `verdict` | every tier's pass/fail | **removed** (ambiguous) |
| `parity` | `PASS|FAIL|NOT_RUN` from row + routine tiers | unchanged; **this is the exit code** (0 PASS, 1 FAIL, 3 NOT_RUN) |
| `merge_eligible`, `merge_policy`, `merge_block_reasons`, `blocker_classes`, `merge_authority` | present | **removed** |
| `findings` | implicit in `blockers` | `[{class: structural|privilege_visibility|evidence|rerun_policy, code, object, detail, blocking: bool}]`; `blocking` is true only when `class` is in `tolerances.blocking_findings` |
| `mode` | fixture/live/snapshot/transactional/continuous/structural | unchanged; `fixture` parity is development evidence and the verifier refuses it |
| `grading` | `full|by_design_unverified` per object | unchanged, but an object graded `by_design_unverified` produces a finding `code: by_design_unverified`, never a block; no manifest mirroring |

`parity` is `NOT_RUN` (exit 3) unless every row tier ran for every table of the slice **and** every routine in `slices.json[].routines` that writes has a `routine_parity` row with a verdict; a missing dependency analysis or routine row never leaves `parity` at PASS (0.6.2 recorded that gap as an `evidence` blocker beside a PASS parity; 0.7 folds it into `parity`).

The verifier's merge rule: `parity == PASS` and `mode in {live, snapshot, transactional}` and no
finding with `blocking: true`. The `privilege_visibility` class covers the last run's `trigger_extra` (RO
principal cannot read `ALL_TRIGGERS`) and `grant_extra` on an inherited catalog grant; both are
reported in the PR and the route packet and never gate a merge unless the plan says so.

Lakebridge `reconcile` (`databricks labs lakebridge reconcile`, Oracle/SQL Server/Snowflake/Redshift
sources through a UC connection) may be run by the verifier as a second opinion for `delta` slices
and attached as a finding of class `evidence` when it disagrees; `dbx-recon` stays authoritative
because it covers routines, Lakebase transactional mode and rerun proof.

## 9. skills/migrate: one slice, five steps

Cold-start inputs: slice id, `policy.json`, `slices.json`, the dialect skill named by
`source.family`, the official databricks skills via `target-routing`.

1. **Access** (slice-0 only; slice-0 always exists, with zero tables when nothing is shared): make
   `source.access` resolvable using the official skill the estate calls for (federation connection +
   foreign catalog; a Lakeflow Connect pipeline into a landed schema; a volume the export is copied
   into and loaded in FK order), prove it with a row count per table, and record
   `{access, kind: federation|landed|loaded_from_export, pipeline_id?, export_manifest?}` in `.migration/recon/slice-0/out/access.json`.
   Every later slice reads `source.access` and never re-creates it. Access prerequisites that are the customer's (connection privilege, connector DB user and CDC enablement, export delivery) are `blockers` on the plan resolved at STOP 1, never a mid-run halt. For `loaded_from_export` the tables are a frozen snapshot: `mode: snapshot` is the only merge-evidence mode and the route packet says so.
2. **Convert**: `databricks labs lakebridge transpile --source-dialect <family> --input-source
   <slice dir> --output-folder <work dir> --error-file-path <work dir>/errors.log` for SQL/DDL/
   views; PL/SQL bodies, triggers, schedulers and anything in `errors.log` are converted by hand
   against the dialect skill's traps table. `delta` track → DBSQL scripting / SDP materialized views
   per `databricks-dbsql` and `databricks-pipelines`; `lakebase` track → PL/pgSQL on branch
   `targets.lakebase.branch`, reference data via `databricks postgres create-synced-table` from the
   mirror. One DAB per slice named `mig_<run>_<slice>`, target `migration`, schedules `PAUSED`.
3. **Prove**: `dbx-recon run --unit <slice> --family <family> --mode live|transactional --depth
   threshold --out .migration/recon/<slice>/out/`; retry cap 3; `data` findings are fixed in converted
   code; `rerun-proof` once. Open the PR: parity on line 1, findings table, bill (`acus_consumed`,
   wall time) as the last section.
4. **Shadow**: deploy a Lakeflow Job `mig_<run>_<slice>_shadow` that runs the slice on the legacy
   cadence and `dbx-recon run --mode continuous` after each cycle into
   `<catalog>.<schema_prefix>recon.drift`; `tolerances.green_cycles` (default 3) consecutive PASS
   ticks gate `g-shadow-<slice>`. The job is created paused and started by `fanout.py` after merge.
5. **Route** (after STOP 3 for the slice): produce the route packet (`.migration/recon/<slice>/route.md`:
   parity history, findings, shadow drift, rollback steps) and the exact repoint change for the
   slice's `routing` mechanism (`jdbc_url` → the config diff; `synonym` → the Oracle DDL for the
   customer DBA; `api` → the client change). Devin opens the PR; the customer-held principal applies it.
   Legacy keeps running for `tolerances.rollback_window` (default 24 h); shadow recon keeps ticking.

## 10. skills/verify

A different session from the slice's author, checkout at the PR head with `policy.json` and
`slices.json` taken from `base_branch`. Re-runs `dbx-recon` at `depth sampled` with a fresh seed
(`full` when the slice sets `verify_depth: full`), compares `changed_paths` against the protected set,
posts the result as a GitHub commit status `dbx-recon/verify` (`success|failure`) with the report
URL using `DBX_VERIFY_TOKEN`, a secret only verifier sessions receive, and returns `{parity, findings, blocking: bool, verified_head}`. Branch protection on `base_branch` requiring `dbx-recon/verify` is a precondition of the run (checked by the doctor, configured by the repo admin at STOP 1), not an option; `fanout.py` additionally merges only at `verified_head`. The mixed checkout is `git checkout <pr_head> && git checkout <base> -- .migration/policy.json .migration/slices.json .migration/recon/<slice>/inputs/`. Exit 3 (`NOT_RUN`, including a run that dies before `result.json`) is a child FAIL with `failure_class: evidence`, never a merge.

## 11. Generalisation seams

Two seams, not one. **Source engine** (what recon reads): (a) a harness source adapter with a live rehearsal, (b) one `source_read_only` query in the doctor. Core ships the engines whose adapters exist: `oracle`, `sqlserver`, `postgres`, `databricks`; a family without an adapter cannot produce merge evidence and lives in `skills-extra` until it has one. **Code dialect** (what the migrate step converts): `skills/dialects/<dialect>/SKILL.md` with its traps table and `canonicalization.json`, and the Lakebridge `--source-dialect` value when one exists; ETL tools (Informatica, SSIS) are dialects over an engine, never an engine.
Nothing else changes: no plan skeleton fork, no estate-type reference, no per-family guard rules.
OLTP vs analytical is the slice's `track`, not the plan's shape.

## 12. What a run looks like (a billing estate, replayed)

| Step | Session | Human |
|---|---|---|
| analyze: Lakebridge analyze + census → `slices.json` (slice-0 reference, slice-1 plans (canary), slice-2 rating+invoicing merged because `sp_issue_invoice` writes `RATING_RESULTS`, slice-3 dunning) | manager + 1 child | — |
| plan.yaml + policy PR | manager | STOP 1 |
| doctor; slice-0 (source access + reference tables) | fanout → 1 child + verifier | — (auto-merge) |
| slice-1 canary PR with recon + bill | 1 child + verifier | STOP 2 |
| slice-2, slice-3 in parallel (`width 4`) | 2 children + 2 verifiers | — (auto-merge) |
| shadow 3 cycles per slice | jobs | — |
| route packets | manager | STOP 3 per slice |

Expected: ~12 sessions instead of 17, three human replies instead of six, no authorization PRs, no
manifest or doctor re-sign PRs. Cost target: ≤ 60 ACU to the canary stop on SWE-2 High children.

## 13. Tests

`tests/test_guard.py` (one row per G1–G5 shape, positive and negative), `tools/tests/test_slices.py`
(collision, cycle, canary placement, shared_writes), `tools/tests/test_fanout.py` (dependency
scheduling, breaker, protected paths; children faked), `tools/tests/test_doctor.py` (five checks over
fakes), `harness/tests/` lifted with the `result.json` contract tests rewritten for section 8. No
prose-linting tests.

## 14. Migration from 0.6.2

`policy.json` is produced from `allowed_targets.json` + `recon_tolerances.json` by
`tools/policy_from_06.py`; a `.migration/waves/` directory makes the doctor's `policy_committed`
check warn "0.6 workspace: run policy_from_06.py". Runs in flight on 0.6.2 finish on 0.6.2; the
tag stays.

## 15. Open questions

1. Which `source.access` kind the demo estate uses first (federation is zero-copy and needs only a UC connection; a landed schema needs the Lakeflow Connect query-based Oracle connector in the workspace).
2. Whether Lakebridge (`databricks labs`) is acceptable as a hard dependency of `skills/migrate` or stays optional with hand conversion as the default.
3. Branch protection is a precondition (§10); the only open point is whether the doctor should configure it itself when the token has admin, or only check it.
4. Per-slice route (STOP 3 per `route_group`) or one route decision for the estate by default.
5. Rollback inside `rollback_window_h` for a routed `lakebase` slice: the route packet carries a replay procedure from the target's change feed (Lakebase logical decoding / Delta CDF) back to legacy, or declares rollback lossy past route time; which is the default.
6. Post-route: the route packet ends with a consumer probe (one query per consumer against the new store) and `green_cycles` clean shadow cycles; drift after that is triaged by the manager into a fix PR per slice. Whether that loop is in 0.7 or 0.8.
