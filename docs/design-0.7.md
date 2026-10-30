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

Three human stops, nowhere else:

| Stop | What the human approves | Where | Pre-selected default |
|---|---|---|---|
| 1 Plan | slices and their order, targets, tolerances, coexistence window, cutover owner, `routing` mechanism per slice | Plan view, `plan.yaml` | yes: every decision has `selected` unless `important: true` |
| 2 Canary | the first non-zero slice's PR: conversion pattern, evidence shape, bill | PR review + Plan view gate `g-canary` | `auto_merge: false` for slice-1 only |
| 3 Route | repoint of one slice's consumers (`d-route-<slice>`), by the customer-held principal | Plan view decision | none; `important: true, selected: null` |

Everything else merges on evidence or halts with a card. A halt is: parity FAIL after the retry
cap, a guard block, a platform permission error on a write, breaker tripped (3 same-class
failures), mirror lag beyond `tolerances.mirror_lag_s`. A halt card ends with the one reply that
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
.migration/recon/<slice>/      result.json, report.md, rerun_proof.json (the only paths a slice PR may touch under .migration/)
```

Deleted from 0.6.2: `skills/{intake,migration-planning,workspace-setup,estate-inventory,pipeline-analysis,wave-plan,wave-verify,unit-migration,migration-fanout,parallel-run,cutover,factory-doctor}`, `hooks/dbx_guard.py`, `skills/test_no_*.py`, `skills/test_skill_manifests.py`, `OVERVIEW.md`, `references/{guards,checks,estate-types,decisions,phase1-steps}.md`, `.migration/{allowed_targets,recon_tolerances,capabilities,authorizations}.json`, `.migration/waves/`, `.migration/units/`, `~/.dbx-migration-factory/`, the doctor signature, the hook nonce, `plan_sha`/`manifest_sha`, `gate_waived`, `merge_override`, `source_principal_attested`, `by_design_unverified` manifest mirroring, `serialized_pipelines`, the pipelines barrier.

## 3. AGENTS.md (complete text)

```
1. The legacy source is read-only in every phase; a reconciliation failure is fixed in converted code.
2. Credentials are named secrets; never print, commit or paste a value.
3. Writes go only to the targets in the committed .migration/policy.json (the copy on the run branch's
   protected base); the migration principal holds no grant anywhere else. A guard block is a halt to
   report, never something to re-shape or retry.
4. A legacy write happens only under a legacy_write_authorized entry of policy.json committed by a human
   PR, cited as DBX_DECISION=<id>; sessions never add such an entry.
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
| G1 | A legacy-only client or a `legacy_sources` name on the same command line as a write verb | block | clients `sqlplus|sqlldr|bteq|mload|fastload|tbuild|tdload|snowsql`; or any `legacy_sources[]` token; verb `\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|EXEC(UTE)?|CALL|BEGIN|COMMIT)\b` outside single quotes. The only downgrade is fail-closed and parses no SQL: the command is exactly `DBX_DECISION=<id> <client> <connection flags> -f|@ <file>` (one client, one script file, no inline SQL, no pipes or substitutions), the id matches a committed `legacy_write_authorized` entry, and `sha256(<file>)` equals that entry's `script_sha256`; the human who committed the entry reviewed that exact script. Anything else under a `DBX_DECISION` prefix blocks. |
| G2 | A deploy to a production target | block | `databricks bundle (deploy|run|destroy)` or `dbt (run|build|seed)` with `-t|--target` matching `policy.forbidden_targets` (default `prod`, `production`); any `--target` not in `policy.bundle_targets`; `databricks postgres` resource path containing `/branches/production` with a write verb (`create-|delete-|update-`). |
| G3 | Adding an authorization, or editing a credential store | block | edit/write with `file_path` ending `.migration/policy.json` whose new content adds or changes an `authorizations[]` entry (any other edit to `policy.json`/`slices.json` passes the guard and is fenced at merge: `CODEOWNERS` requires a human review for `.migration/` on `base_branch`, and `fanout.py`/`verify` fail a slice PR touching either file); `file_path` matching `(^|/)\.databrickscfg$|\.databricks/|\.config/databricks/`; any command assigning `DATABRICKS_(HOST|TOKEN|CLIENT_ID|CLIENT_SECRET)=` or editing `/opt/.devin/envrc`. |
| G4 | Anything the guard cannot read (script file it cannot open, `eval`, `$(…)` wrapping a G1 client) | warn | logged; the platform permission model is the control |
| G5 | Edits under the running plugin tree | block | `file_path` under `realpath(__file__)/../..` |

Policy source: `policy.json` read from `git show <base>:.migration/policy.json` where `<base>` is
`refs/remotes/origin/HEAD`, else `origin/main`, else `origin/master`; never the working copy. No
policy committed → G1/G2/G3/G5 still apply with `legacy_sources = []` and `bundle_targets = []`
(so every deploy blocks until the policy PR merges); reads are never blocked. Output contract is
unchanged (`{"decision": "block"|"warn", "reason": ...}` on stdout, exit 2 on block).

G2 is deliberately not a destination allowlist: the destination fence is the migration principal's grants, proven by the doctor's `isolation` check (§5), and `fanout.py` refuses to launch while `isolation` is not `ok`. `policy.json` and `slices.json` are further protected by a `CODEOWNERS` rule on `.migration/` requiring a human review on `base_branch`, and `fanout.py`/`verify` fail any slice PR that touches them (`protected_files_tampered`). The guard is a no-op in a checkout with no `.migration/policy.json` on its base; it never fires in another plugin's or project's repo.

Removed on purpose: SQL text classification, catalog resolution, Lakebase project/branch parsing,
curl/REST path parsing, Python literal inspection, git subcommand tables, `cd`/`ssh`/`docker`
re-rooting, identity-swap detection beyond G3, the hook nonce probe.

## 5. tools/doctor.py

Run by the manager after the policy PR merges and by `fanout.py` before launching children. Five
checks, each `ok|fail|warn` with `data`; `ready` is every check not `fail`. No signature, no
`capabilities.json`, no pointer file; the report is printed and attached to the ticket.

| Check | How | fail when |
|---|---|---|
| `identity` | `databricks current-user me`; `DATABRICKS_AUTH_TYPE` in `{oauth-m2m, env-oidc}`; principal equals `policy.migration_principal` | mismatch or PAT |
| `isolation` | `databricks grants get-effective` for the principal on every catalog in `policy.forbidden_catalogs` and `postgres get-branch` on `production`: expects **no** privilege beyond `USE_CATALOG`/`SELECT`/`BROWSE` and no role on `production` | any write or ownership privilege outside `policy.targets` |
| `policy_committed` | `git diff --quiet <base> -- .migration/policy.json .migration/slices.json` and schema validation (section 6) | working copy differs or invalid |
| `source_read_only` | one query per family (Oracle `USER_TAB_PRIVS`/`USER_ROLE_PRIVS`; SQL Server `fn_my_permissions`; Postgres `has_table_privilege`) over the slices' tables: no INSERT/UPDATE/DELETE/ALTER/EXECUTE on write-capable routines | any write privilege |
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
  "source": {"family": "oracle", "secret": "LEGACY_RO_DSN", "access": "mig_cat.legacy_fed"},
  "tolerances": {"version": "tol-<date>-v1", "exact": true, "per_type": {}, "mirror_lag_s": 900,
                  "full_diff_row_threshold": 1000000, "blocking_findings": []},
  "authorizations": []
}
```

- `source.access` is the one Unity Catalog reference every slice and the harness read the source through: always a queryable `<catalog>` or `<catalog>.<schema>`: a foreign catalog (Lakehouse Federation) or a landed schema (Lakeflow Connect CDC or query-based pipeline, or, with no network path, tables slice-0 loads from an export volume with `COPY INTO`). A volume path is never the access value; the harness reads `source.access` with its Databricks adapter (`--family databricks`) when the source is federated or landed, and with the native family adapter when `source.access` is absent and the source is reachable directly. Which one, and how it is created, is slice-0's job through `target-routing` (`databricks-lakeflow-connect`, `databricks-dbsql` federation, `databricks-unity-catalog`); the plugin holds no connector or loader code. `tolerances.mirror_lag_s` applies only to a landed schema (read from the pipeline's last update) and is otherwise ignored.
- `tolerances.blocking_findings` lists structural classes the plan chose to make merge-blocking (`[]` by default). Any class not listed is a finding.
- `authorizations` holds only `legacy_write_authorized` entries: `{"id", "by": "user:<id>", "objects": [...], "script_sha256": "<sha256 of the reviewed SQL script>"}`; `objects` is documentation for the reviewer, the guard checks only the hash.

```json
{
  "version": 1,
  "slices": [
    {"id": "slice-0", "name": "reference", "track": "delta", "tables": ["BILLING.TENANTS", "BILLING.PLANS", "BILLING.CODES"],
     "routines": [], "jobs": [], "consumers": [], "depends_on": [], "routing": "none"},
    {"id": "slice-1", "name": "plans", "track": "lakebase", "tables": ["BILLING.SUBSCRIPTIONS", "BILLING.SUBSCRIPTIONS_HIST"],
     "routines": ["PKG_PLANS.*"], "jobs": [], "consumers": ["billing-api"], "depends_on": ["slice-0"], "routing": "jdbc_url", "route_group": "billing-api", "canary": true},
    {"id": "slice-2", "name": "rating+invoicing", "track": "lakebase", "tables": ["..."], "routines": ["PKG_RATING.*", "PKG_INVOICING.*"],
     "jobs": ["NIGHTLY_RATING"], "consumers": ["billing-api", "finance-report"], "depends_on": ["slice-1"], "routing": "jdbc_url", "route_group": "billing-api",
     "shared_writes": {"RATING_RESULTS": "merged"}}
  ]
}
```

`tools/slices.py --check` fails when: a consumer appears in two slices whose `routing` is not independently switchable per capability (`jdbc_url`, `synonym`) unless those slices share a `route_group` (routed together at one STOP 3); a table appears in two slices without a `shared_writes`
disposition (`merged` = the slices were combined; `dual_write:<owner slice>` = owner writes both sides
until the other routes); a routine's transitive writes (from the dialect skill's dependency scan)
touch a table outside its slice and outside `slice-0`; `depends_on` has a cycle; a `canary` slice is
not the first non-zero slice. This replaces `derive_waves.py`, the write-target manifests and the
exact-set rule: the check is a plan-time refusal the human sees before approval, never a launch halt.

## 7. tools/fanout.py

Input: `slices.json`, `policy.json`, the plan's `width` (default 4) and `breaker` (default 3). For
each slice whose `depends_on` are all merged (PR merged on `base_branch`): launch one `migrate`
child with a brief ≤ 4,000 chars pointing at the slice id; when the child reports `status: PASS`
with `pr_url`, launch one `verify` child; on verifier PASS and `auto_merge: true` (every slice except
the canary) merge the PR; write `.migration/recon/<slice>/fanout.json`. Breaker: stop launching after
`breaker` failures of the same `failure_class`. Collision: two running slices with a shared write
table and no `shared_writes` disposition is a launch refusal (cannot happen after `slices.py --check`,
kept as defence). No run log, no manifest sha, no lock file, no pointer; re-running `fanout.py` skips
slices whose PR is merged and relaunches the rest.

Child report schema (structured output): `{status: PASS|FAIL|BLOCKED, slice, pr_url, branch, parity:
PASS|FAIL, findings: [{class, code, detail}], failure_class?, changed_paths: [...], recon_cost,
one_line_summary}`. `fanout.py` fails a child whose `changed_paths` touch `.migration/` outside
`.migration/recon/<slice>/` (`protected_files_tampered`).

## 8. Harness: parity is the verdict

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
   `{access, kind: federation|landed|loaded_from_export, pipeline_id?, export_manifest?}` in `.migration/recon/slice-0/access.json`.
   Every later slice reads `source.access` and never re-creates it. For `loaded_from_export` the tables are a frozen snapshot: `mode: snapshot` is the only merge-evidence mode and the route packet says so.
2. **Convert**: `databricks labs lakebridge transpile --source-dialect <family> --input-source
   <slice dir> --output-folder <work dir> --error-file-path <work dir>/errors.log` for SQL/DDL/
   views; PL/SQL bodies, triggers, schedulers and anything in `errors.log` are converted by hand
   against the dialect skill's traps table. `delta` track → DBSQL scripting / SDP materialized views
   per `databricks-dbsql` and `databricks-pipelines`; `lakebase` track → PL/pgSQL on branch
   `targets.lakebase.branch`, reference data via `databricks postgres create-synced-table` from the
   mirror. One DAB per slice named `mig_<run>_<slice>`, target `migration`, schedules `PAUSED`.
3. **Prove**: `dbx-recon run --unit <slice> --family <family> --mode live|transactional --depth
   threshold --out .migration/recon/<slice>/`; retry cap 3; `data` findings are fixed in converted
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
URL, and returns `{parity, findings, blocking: bool}`. Branch protection on `base_branch` requires
`dbx-recon/verify`; that, not a signature chain, is what stops a child merging on its own say-so.

## 11. Generalisation seams

New source family = (a) a harness source adapter with a live rehearsal, (b)
`skills/dialects/<family>/SKILL.md` with its traps table and `canonicalization.json`, (c) the
Lakebridge `--source-dialect` value when one exists, (d) one `source_read_only` query in the doctor.
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
3. Branch protection with a required status check needs admin on the run repo; for demo repos where that is unavailable, the fallback is `fanout.py` refusing to merge without the `dbx-recon/verify` status — weaker, and must be named in the README.
4. Per-slice route (STOP 3 per slice) or one route decision for the estate by default.
