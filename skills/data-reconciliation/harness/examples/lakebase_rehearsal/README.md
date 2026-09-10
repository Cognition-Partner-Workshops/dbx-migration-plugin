# Lakebase transactional rehearsal: loan servicing (SQL Server -> Postgres)

The run that validated `--mode transactional`. Source is the Sybase/SQL Server loan-servicing
estate (`ts-tsql-sybase-legacy-db` seed: 500 borrowers, 880 loans, 2988 payments, 1765 escrow
accounts, 597 modifications), read-only via an `ApplicationIntent=ReadOnly` DSN. Target is a
Postgres database standing in for a Lakebase migration branch (same wire protocol, same
`psycopg` adapter, same `REPEATABLE READ` window).

The fixture (mapping, target DDL, loader, canonicalization, four defect-injection scripts and
the repair script) lives with the estate it describes:
`Cognition-Partner-Workshops/ts-tsql-sybase-legacy-db` under `rehearsals/lakebase/`, with the
run commands. Only the tolerance record is kept here, as the OLTP example to copy:

- `tolerances.json`: zero numeric tolerance, `cdc_lag_max_s: 60`, `pk_set_ranges: 16`, and
  `accept_target_only_constraints: true` (the recorded decision that the target's added
  unique/FK/CHECK constraints are intended; see Observed).

## Observed

Clean target: `PASS`, `merge_eligible=true`; tiers 0/1/2/3/5/6/7 = 5/5/68/6730/5/5/10 checks,
zero findings, zero rows in flight. Cost 107 source statements / 6810 rows fetched, 56 target
statements / 6730 rows, under one second (ten of the source statements are the second
change-token read that brackets each of the five open and five close markers). Window isolation:
target `repeatable_read` (verified by updating a row from another connection mid-window: marker
unchanged inside, visible after close); source `none` because the fixture database has
`ALLOW_SNAPSHOT_ISOLATION OFF`, so the adapter reset to `READ COMMITTED`; its window strength is
`change_token` (the `sys.dm_db_index_usage_stats.user_updates` counter is readable with the
fixture login and sits in the marker as the third element, `880 -> 880` on `loans`). Tier 5
fingerprints (count, sum and modular sum of squares per key column and for the watermark)
matched on all 18 ranges of every table across engines (`DATEDIFF_BIG` microseconds vs
`EXTRACT(EPOCH)` microseconds; T-SQL `%` vs Postgres `MOD()`), so no keys streamed on the clean
run. `--target-catalog` was the connected database name; pointing it at another name is refused
before any query runs.

The first run with two-way constraint parity failed the clean target on tier 7 alone: `unique_extra
('loan_number',)`, `check_constraint_count_higher 0 -> 2` and `foreign_key_extra borrower_id ->
borrowers` on `loans`, `foreign_key_extra loan_id -> loans` on `payments`/`escrow_accounts`/
`loan_modifications`, `check_constraint_count_higher 0 -> 1` on `payments`. The `raw.*` source
schema declares none of them, so a legacy-valid orphan or duplicate loan number would be rejected
by the target. Keeping them is the intent of this rehearsal, so the decision is recorded as
`accept_target_only_constraints: true` in `tolerances.json`; the seven findings now sit under
`stats.accepted_target_only_constraints` and the run is `PASS` again. Without the record the
verdict is `FAIL`, `merge_eligible=false`.

Rehearsal A (`inject_target_defects.sql`): `FAIL`. Tier 1 count gaps on `payments`/`escrow_accounts`;
tier 5 `pk_missing_on_target [(10,), (11,)]` and `pk_extra_on_target [(999999,)]`; tier 6
`target_ahead_of_source` 86400s on `loan_modifications`; tier 7 `not_null_missing loan_status`
and `sequence_behind_source` (`payments` next 100 <= max 2988). Tiers 2-3 skipped (tier 1 failed).

Rehearsal B (`inject_target_drift.sql`): `FAIL`. Tier 6 `cdc_lag_exceeded` 300s on `borrowers`
with all 500 rows in flight and *no* tier 3 findings for them; tier 5 streams every `borrowers`
range (the watermark sums differ) and classifies all 500 as `in_flight_updates`, not ordering
findings; tier 3 `field_diff key=(5,) current_balance` on `loans`. Tiers 1/2/5/7 clean.

Rehearsal C (`inject_target_equal_count.sql`, run with `--depth sampled`): `FAIL`. Tier 1 and
the tier 6 lag check pass (every count and max watermark unchanged). Tier 5 `pk_missing_on_target
[(500,)]` and `pk_extra_on_target [(2989,)]` on `payments` from 2 mismatched ranges (376 keys
streamed of 2988); tier 6 `row_ahead_of_source [(7,)]` on `loans` from 1 mismatched range while
`lag_s = 0.0`.

Rehearsal D (`inject_target_applied_drift.sql`, `--depth sampled` with `sample_size` 50): `FAIL`.
Tier 0 reports 10 `loans` rows in flight; tier 2 aggregates the source bounded by the target's
applied watermark against the target minus those 10 keys (`applied_subset.loans = {in_flight: 10,
excluded_keys: 10}`) and fails `aggregate_sum`/`aggregate_max`/`aggregate_distinct_count` on
`term_months`, with no finding for the stale `current_balance` of the in-flight rows. Tier 3
sampled 125 of 880 rows, did not visit loan 5 and passed, so tier 2 is the only tier that names
the drift. Fields under `decimal_round` (`MONEY -> decimal(19,4)`) still have `sum` and
`distinct_count` deferred to tier 3, as in every mode.

Rehearsals A-D ran before the mapping declared `delete_evidence`, so every `pk_extra_on_target`
above was graded strictly. With evidence on (SQL Server CDC enabled by hand on the disposable
fixture, `cdc_checkpoint` written by the loader) rehearsal A's stray `escrow_accounts` row still
fails `pk_extra_on_target`, now worded "not deleted on the source after the target's applied
position", and `delete_evidence_statements = {source: 10, target: 5}` on the clean run (one
more source statement per object that has tombstones after the applied LSN: the keyed read that
checks whether the source still holds them).

Rehearsal E (`inject_source_deletes.sql` on the fixture SQL Server, run inside `cdc_lag_max_s`):
`PASS`, merge-eligible. Tier 5 `loan_modifications` reads 3 delete events after the applied LSN,
`in_flight_deletes: 3`, `extra_on_target: 0`; tier 1 counts the gap of 3 inside the in-flight
allowance; tier 6 records `target_max_from_in_flight_delete: true` because the deleted rows
carried the target's max `created_date` (4 ms newer than the surviving source max), so no
`target_ahead_of_source`. `delete_evidence_statements = {source: 10, target: 5}`. The same state
after 60 s: `FAIL` with `delete_lag_exceeded` ("3 source deletes still present on the target 141s
after commit"), `pk_extra_on_target` for the same keys and `root_count`. After `restore_source_deletes.sql`, still
inside `cdc_lag_max_s`: `PASS` with `reinserted: 3`, `in_flight_deletes: 0` and no tier 2
exclusion (`applied_subset` absent) — the 3 tombstoned keys the source holds again are graded as
ordinary rows on both sides.

Rehearsal F (`inject_target_checkpoint_gap.sql`): `FAIL` with `delete_evidence_retention_gap`
(applied LSN `...0001` older than the oldest retained change), strict `pk_extra_on_target` and
`root_count`; the delete query is not issued (`source: 9`).

## Not proven here

A real Lakebase branch (only the Postgres protocol and isolation level were exercised), a source
with snapshot isolation enabled (the fallback path ran instead), a live CDC feed (lag was
injected, not observed from Lakeflow Connect), a source write during the window (the source
is read-only in every rehearsal, so the change token detecting a below-max update or a balanced
insert+delete is proven by the harness tests, not here), and delete evidence from anything but
SQL Server CDC (Lakeflow Connect / Debezium change tables and audit tables have no adapter yet).
