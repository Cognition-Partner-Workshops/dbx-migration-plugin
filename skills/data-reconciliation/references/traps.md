## Example inputs

`harness/examples/` has a mapping spec, a tolerance record, Redshift canonicalization rules,
and the `lakebase_rehearsal/` OLTP tolerance record. Copy and edit; do not start from a blank file.

## Known traps (append per engagement)
- AVG on integers truncates on some legacy engines and returns decimal on Databricks. Use
  `decimal_round` with the places from the tolerance record.
- Collation-sensitive strings (case, trailing spaces) differ. Enable `collation_casefold` or
  `rstrip_spaces` per the tolerance record only.
- CHAR columns pad with spaces on the source. `rstrip_spaces` is almost always needed.
