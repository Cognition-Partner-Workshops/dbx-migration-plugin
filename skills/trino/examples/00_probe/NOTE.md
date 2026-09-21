# 00 — engine probe behind every **(probe)** row in `SKILL.md`

Not a conversion example. `probe_trino.sql` ran on Trino 483 (`trino --file`), `probe_dbx.sql` on a Databricks SQL
warehouse (DBSQL 2026.36, `databricks api ... statements`); `*.out` are the captured outputs, one `k`/`v` row per
behaviour. Four Trino rows (`lambda`, `truncate`, `map_keys_sorted`, `hash`) and two Databricks rows (`truncate`,
`ansi_mode`) errored on the probe expression itself (a cast of array/varbinary to varchar, an unsupported overload,
an unresolved routine), not on the behaviour under test; the affected `SKILL.md` rows say so.

Re-run both files against the engagement's versions before trusting a **(probe)** row on a different release.
