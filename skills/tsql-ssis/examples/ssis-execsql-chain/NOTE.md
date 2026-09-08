# ssis-execsql-chain: `NightlyServicing.dtsx`

Source: `source.dtsx` — hand-written `.dtsx`-shaped description of the SQL Server / SQL Agent edition of the fixture's `batch/run_nightly_batch.sh` (same three procedures, expressed as Execute SQL Tasks). Converted: `converted.yml` (Lakeflow Jobs resource; task graph only — the procedure bodies are converted under `examples/proc-*`).

## Constructs exercised
- Five **Execute SQL Tasks** (`SqlStatementSource` literal, `?` parameter bindings, `ResultSetType_SingleRow` result binding) -> `sql_task.file` per task; `?` bindings -> `:name` named parameters (SKILL §2b "SSIS Execute SQL Task", §6 "SSIS control flow"; `[jobs:tasks]`, `[jobs:params]`).
- **Precedence constraints**: `Value=0` Success with `LogicalAnd` -> `depends_on` + `run_if: ALL_SUCCESS`; three `Value=1` Failure constraints with `LogicalAnd="False"` (OR) -> one task with three `depends_on` and `run_if: AT_LEAST_ONE_FAILED` (`[jobs]`, `[jobs:runif]`).
- **`ResultSet=SingleRow` -> variable + expression constraint** (`EvalOp=3` ExpressionAndConstraint, `@[User::EligibleCount] > 0`) -> a notebook task that publishes the count as a task value (`[jobs:taskvalues]`; SQL tasks cannot set task values) and an **If/else `condition_task`** (`GREATER_THAN`, `{{tasks.count_eligible.values.eligible_count}}` vs `0`) with accrual depending on `outcome: "true"` (`[jobs:ifelse]`, YAML shape from the bundles resources page). The branch must be graph-visible: folding the guard into accrual's SQL as `IF ... THEN CALL` makes accrual *succeed* on a zero count, so fees and EOD still run, which the SSIS package never does (SKILL §6 "SSIS precedence constraint: expression").
- **`FailPackageOnFailure`**, `MaxErrorCount=1` -> default Jobs semantics (any failed task fails the run); `max_concurrent_runs: 1` for SQL Agent's no-overlap behavior.
- **OnError event handler** writing `dbo.audit_trail` with `System::SourceName`/`System::ErrorDescription` -> folded into the `log_failure` task (`{{job.run_id}}` recorded instead; system variables have no SQL equivalent), which also depends on `count_eligible` because the handler is package-wide, plus `email_notifications.on_failure` (`[jobs:monitor]`).
- **Project parameter in a connection string** (`@[$Project::ServerName]`) and `EvaluateAsExpression` variable (`(DT_DBDATE)GETDATE()`) -> job parameters / `current_date()` (SKILL §6 "SSIS Variables / Project Parameters").
- `ProtectionLevel=EncryptSensitiveWithUserKey` -> §9 governance finding (package sensitive values unrecoverable outside the author's profile; recorded, never recovered).

## Lineage
FACT: reads `loan_servicing.dbo.loans`; calls `dbo.sp_nightly_accrual`, `dbo.sp_apply_late_fees`, `dbo.sp_end_of_day_reconciliation` (their writes come from the procedure units: `loans`, `payments`, `escrow_accounts`, `audit_trail`, ...); writes `dbo.audit_trail` directly (two tasks). Scheduler edge: this package would be a SQL Agent job step (`subsystem = SSIS`) -> the schedule row becomes `schedule.quartz_cron_expression` on the job (not shown here; see `examples/runner-nightly-batch`). INFERRED: the server behind `$Project::ServerName` (read from `Project.params` when available; otherwise the `interfaces`-style alias table).

## Recon tier that catches a wrong conversion
- **Task-graph parity** (pre-Tier check): every SSIS constraint maps to exactly one `depends_on` edge with the right `run_if`; a `LogicalOr` failure fan-in converted as `ALL_SUCCESS` would never log a failure, and a missing `AT_LEAST_ONE_FAILED` would make the logger run on success.
- **Tier 1** on each written table after a full run (`audit_trail` gains exactly the rows the three procedures write plus zero `BATCH_FAILED` rows on a clean run; `payments`/`loans` deltas equal the procedure units' own Tier 1 expectations).
- **Tier 2** on the guard: `count(*) FROM loans WHERE loan_status='AC' AND servicer_id = 7` equals the `EligibleCount` the package would have bound; if the converted guard mis-parameterises `servicer_id`, accrual runs (or is skipped) for the wrong population, visible as a `sum(accrued_interest)` delta.
- **Zero-count run parity**: replay with a servicer that has no active loans; the source leaves `payments`/`audit_trail` untouched, so Tier 1 deltas on every written table must be zero and `late_fee_balance` (Tier 2) unchanged. A conversion that guards only accrual (not the graph) fails this replay because `apply_late_fees`/`eod_reconciliation` still execute.
- Run-if semantics trap (`[jobs:runif]`): an Excluded `log_failure` is treated as success by downstream `run_if` evaluation; if the customer chains anything after the logger, model that explicitly.

## Lakebridge
`--source-dialect ssis` does not emit a task graph; the precedence-constraint mapping above is hand-derived (SKILL §10).
