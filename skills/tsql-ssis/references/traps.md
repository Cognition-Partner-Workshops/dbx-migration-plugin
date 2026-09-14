## Traps with recon signature

| Trap | Recon signature | Fix / decision |
|---|---|---|
| **ASE `*=` with inner-side WHERE predicate** | Tier 1 count < source; Tier 2 null-rate on the outer columns = 0 | predicate into `ON`, not `WHERE` (`examples/view-outer-join`); Lakebridge mangles `*=` |
| **`DATETIME` 1/300 s grid** | Tier 3 on `DATEADD`/arithmetic columns recomputed on the target; Tier 2 `max()` drift <= 3 ms | loaded values compare exactly (`identity`); recomputed columns `datetime_grid_333` with the 10 ms grid in the tolerance record |
| **CI collation equality/grouping** (`_CI_AS`; **ASE** default is binary, check `sp_helpsort`) | Tier 2 distinct-count drift on string keys; Tier 1 on string joins | `COLLATE UTF8_LCASE`, never `lower()` in joins; `collation_casefold` only when the census reports CI |
| **Trailing-space padding** (`CHAR(n)`, `ANSI_PADDING`) | Tier 2 distinct-count on `loan_type`-style codes; joins fan out to 0 | `rtrim()` on load or `UTF8_BINARY_RTRIM`; `rstrip_spaces` |
| **`ISNULL` type coercion** | Tier 2 `max(length())` drift | reproduce the truncation or record the fix in the tolerance record |
| **`@@ROWCOUNT` / `SET ROWCOUNT` batching** | Tier 1 on the target after the run; `audit_trail.record_count` = rows updated | single `UPDATE` + `GET DIAGNOSTICS ROW_COUNT`; no pre-materialised target list |
| **Integer division / `AVG(INT)`** | Tier 2 drift on ratio columns, one unit on negatives with `floor` | `div`, `sum() div count()` |
| **`MONEY` arithmetic** | Tier 2 sums differ in the 5th+ decimal | `round(...,4)` at each source rounding step; `decimal_round` places=4 on those columns only |
| **`DATEDIFF` boundaries** | Tier 2 histogram drift on `days_past_due` buckets | rewrite per the map |
| **`CONVERT` style codes** (**ASE** default 100) | Tier 3 on formatted strings | pattern table in the map |
| **`@@identity` hijacked by triggers** | Tier 3 on FKs populated from it | business-key read-back; `identity` |
| **NULL concatenation** (**ASE** `NULL + 'x'` = `'x'`) | Tier 2 null-rate rises on `action_detail`-style columns | `coalesce(x,'')` per operand; `null_missing_equiv` is not the fix |
| **`SELECT @v = col` over N rows** | procedure raises, or Tier 3 diff on the derived value | deterministic `ORDER BY ... LIMIT 1` |
| **Parameter shadowing** (`@servicer_id` -> `servicer_id`) | Tier 1 count too high; wrong `batch_id` in Tier 3 | `p_` prefix, aliases on every column |
| **Multi-table transaction flattened to sequential DML**, or check racing its DML | clean run passes every tier; failure-injection and concurrent-writer replays fail | `BEGIN ATOMIC` on catalog-managed tables, else compare-and-set; per-row -> per-batch commit unit recorded per unit |
| **SSIS Lookup cache mode vs collation** | Tier 1 matched/no-match counts | `UTF8_BINARY` join for Full cache, `UTF8_LCASE` for no-cache |
| **Per-run SSIS parameter as a streaming table** | permanent false no-match rows after a parameter change | bounded batch (`examples/ssis-dataflow-lookup`) |
| **`UNIQUEIDENTIFIER` case** / **`DATETIME` zone** | Tier 3 100% key mismatch / Tier 2 `min()/max()` shifted | lower-case on load, `uuid_normalize` / land as `TIMESTAMP_NTZ`, tolerance record names the server zone |

