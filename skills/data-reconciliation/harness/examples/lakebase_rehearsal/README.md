# Lakebase transactional rehearsal: loan servicing (SQL Server -> Postgres)

The run that validated `--mode transactional`. Source is the Sybase/SQL Server loan-servicing
estate (`ts-tsql-sybase-legacy-db` seed: 500 borrowers, 880 loans, 2988 payments, 1765 escrow
accounts, 597 modifications), read-only via an `ApplicationIntent=ReadOnly` DSN. Target is a
Postgres database standing in for a Lakebase migration branch (same wire protocol, same
`psycopg` adapter, same `REPEATABLE READ` window).

| File | Purpose |
|---|---|
| `target_ddl.sql` | Lakebase-shaped schema: `IDENTITY` for `INT IDENTITY`, `NUMERIC(19,4)` for `MONEY`, `TIMESTAMP(6)` for `DATETIME2`, `TEXT` for `VARCHAR(MAX)`, every PK/unique/FK/NOT NULL/CHECK/index carried over |
| `load_target.py` | One-shot SELECT -> `COPY` initial load in FK order; restarts identity sequences above the loaded max. Secrets by name only |
| `mapping.json` | Five objects with `key`, `watermark`, `identity`, field maps and type canonicalization |
| `tolerances.json` | OLTP record: zero numeric tolerance, `cdc_lag_max_s: 60`, `pk_set_ranges: 16` |
| `canonicalization.json` | `decimal_round` for MONEY, `datetime_utc_truncate_ms` for legacy DATETIME, `rstrip_spaces` for CHAR, `empty_string_is_null` for VARCHAR(MAX) |
| `inject_target_defects.sql` | Negative rehearsal A: missing keys, stray row, out-of-order apply, dropped NOT NULL, sequence behind |
| `inject_target_drift.sql` | Negative rehearsal B: CDC lag beyond tolerance plus one applied-row value drift |
| `inject_target_equal_count.sql` | Negative rehearsal C: a key swapped for a stray and one row applied ahead of its source, with every count and max watermark unchanged |
| `repair_target.sql` | Restores the schema-level defect; `load_target.py` restores the data |

## Run

```bash
export REHEARSAL_SOURCE_ODBC=...   # read-only ODBC DSN, value never leaves the shell
export LAKEBASE_MIGRATION_DSN=...  # Postgres DSN for the migration branch
psql "$LAKEBASE_MIGRATION_DSN" -f target_ddl.sql
python load_target.py --source-dsn-secret REHEARSAL_SOURCE_ODBC --target-secret LAKEBASE_MIGRATION_DSN

dbx-recon run --unit loan_servicing_oltp --family sqlserver \
  --mapping mapping.json --tolerances tolerances.json --canonicalization canonicalization.json \
  --mode transactional --source-dsn-secret REHEARSAL_SOURCE_ODBC \
  --target-kind lakebase --target-secret LAKEBASE_MIGRATION_DSN \
  --target-catalog <database> --target-schema loan_servicing \
  --allowed-targets-file .migration/allowed_targets.json --depth full --seed 7 --out run1
```

## Observed

Clean target: `PASS`, `merge_eligible=true`; tiers 0/1/2/3/5/6/7 = 5/5/68/6730/5/5/10 checks,
zero findings, zero rows in flight. Cost 97 source statements / 6810 rows fetched, 55 target
statements / 6730 rows, under one second. Window isolation: target `repeatable_read` (verified
by updating a row from another connection mid-window: marker unchanged inside, visible after
close); source `none` because the fixture database has `ALLOW_SNAPSHOT_ISOLATION OFF`, so the
adapter reset to `READ COMMITTED`; its window strength is `change_token` (the
`sys.dm_db_index_usage_stats.user_updates` counter is readable with the fixture login and sits
in the marker as the third element, `880 -> 880` on `loans`). Tier 5 fingerprints matched on
all 18 ranges of every table across engines (`DATEDIFF_BIG` microseconds vs `EXTRACT(EPOCH)`
microseconds), so no keys streamed on the clean run.

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

## Not proven here

A real Lakebase branch (only the Postgres protocol and isolation level were exercised), a source
with snapshot isolation enabled (the fallback path ran instead), a live CDC feed (lag was
injected, not observed from Lakeflow Connect), and a source write during the window (the source
is read-only in every rehearsal, so the change token detecting a below-max update or a balanced
insert+delete is proven by the harness tests, not here).
