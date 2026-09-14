## Canonical Lakeflow / DBSQL shape

Procedure (T-SQL cursor + `GOTO` + `RAISERROR` + `@@identity`; full form in `examples/proc-cursor-payments/converted.sql`):

```sql
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.p(IN p_date TIMESTAMP_NTZ, IN p_servicer_id INT, OUT p_rc INT)
LANGUAGE SQL SQL SECURITY INVOKER
AS BEGIN
    DECLARE run_key STRING DEFAULT uuid();
    DECLARE failed CONDITION FOR SQLSTATE '45001';                 -- RAISERROR 50001
    DECLARE EXIT HANDLER FOR SQLEXCEPTION                          -- error_handler:
    BEGIN DROP TABLE IF EXISTS work; SET p_rc = 1;
          SIGNAL failed SET MESSAGE_TEXT = 'failed for run ' || run_key; END;
    SET p_rc = 0;
    CREATE TEMP TABLE work AS SELECT ... FROM t WHERE t.servicer_id = p_servicer_id;   -- SELECT INTO #t
    BEGIN ATOMIC                                                   -- BEGIN TRAN ... COMMIT across tables
        INSERT INTO ${catalog}.${schema}.child SELECT ... FROM work;
        IF EXISTS (SELECT 1 FROM ${catalog}.${schema}.parent t JOIN work w ON ... WHERE <trigger check>) THEN
            SIGNAL SQLSTATE '45050' SET MESSAGE_TEXT = '<trigger message>';   -- ROLLBACK TRIGGER
        END IF;
        MERGE INTO ${catalog}.${schema}.parent t USING work w ON t.k = w.k WHEN MATCHED THEN UPDATE SET ...;
    END;
    DROP TABLE IF EXISTS work;
END;
```

Package / Agent job (SSIS Data Flow with a per-run parameter; full form in `examples/ssis-dataflow-lookup/converted.yml`):

```yaml
resources:
  jobs:
    <package>_job:
      max_concurrent_runs: 1
      parameters:                                  # one pair per connection manager + package params
        - { name: src_catalog, default: "${var.src_catalog}" }
        - { name: tgt_schema,  default: "${var.tgt_schema}" }
        - { name: servicer_id, default: "7" }      # $Package::ServicerId, overridden per run
      tasks:
        - task_key: load                           # Data Flow Task -> sql_task file: TEMP TABLE window, MERGE per destination
          max_retries: 2
          sql_task:
            file: { path: ./load.sql, source: WORKSPACE }        # ships next to this yml in the bundle
            warehouse_id: ${var.warehouse_id}
            parameters: { src_catalog: "{{job.parameters.src_catalog}}", servicer_id: "{{job.parameters.servicer_id}}" }
        - task_key: log_onerror                    # OnError handler -> same etl_log row
          depends_on: [{ task_key: load }]
          run_if: AT_LEAST_ONE_FAILED
          sql_task: { file: { path: ./log_onerror.sql, source: WORKSPACE }, warehouse_id: ${var.warehouse_id} }
```

## Examples

| Example | Exercises | Recon tier that catches a wrong conversion |
|---|---|---|
| `examples/view-outer-join/` | ASE `*=` with inner-side WHERE predicate, comma FROM list, correlated `MAX()` -> window, UDF inlining, `CHAR` padding | Tier 1 row count; Tier 2 null-rate on outer columns |
| `examples/proc-cursor-payments/` | `@@sqlstatus` cursor -> set-based, `GOTO` -> `EXIT HANDLER`, `RAISERROR` -> `SIGNAL`, `@@identity` -> `run_key`, `#temp`, `MONEY`, folded triggers, `BEGIN ATOMIC` | Tier 1 payment/audit counts; Tier 2 `decimal_round` sums; failure-injection replay |
| `examples/ssis-dataflow-lookup/` | `.dtsx` Data Flow (Source `?`, Full-cache Lookup, Derived Column, two destinations, OnError) -> Jobs `sql_task` bounded batch with two namespace pairs | Tier 1 matched + no-match = window, rerun no-op, two-servicer run; one `etl_log` row on failure |
