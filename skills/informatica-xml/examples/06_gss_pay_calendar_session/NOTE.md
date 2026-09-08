# 06 - s_Pay_Calendar_Reset_Pay_Calendar (session-level semantics) / wf_Pay_Calendar

Fixture: `informatica/legacy_shared_services/XML/wf_GSS_PAY_CALENDAR.xml` (1,082 lines). `source.xml` is a
syntactically valid **excerpt**: the `m_Pay_Calendar_Reset_Pay_Calendar` mapping, the workflow header, the
`s_Pay_Calendar_Reset_Pay_Calendar` session with all its components/extensions, all workflow links and that session's
workflow variables; the three other mappings/sessions are elided and marked. `OWNER` redacted in this copy.

This is the one fixture export that carries the **session** layer (`SESSIONEXTENSION`, `CONNECTIONREFERENCE`,
`SESSIONCOMPONENT`, session `ATTRIBUTE`s), which the five Albion workflow exports abbreviate. It is the reference
for SKILL.md sections 1-2 (session/partition overrides) and 6 (procedural constructs).

## Constructs exercised

| Construct | Where in source | Handled by (SKILL.md) |
|---|---|---|
| Source Qualifier `Source Filter` with empty `Sql Query` -> WHERE clause of the generated SELECT | `SQ_PAY_PERIOD_RESET` | section 2 (SQ precedence), trap 1 |
| `Sql Query`, `User Defined Join`, `Pre SQL`, `Post SQL`, `Select Distinct`, `Number Of Sorted Ports` attributes present (empty) | `SQ_PAY_PERIOD_RESET` | section 2 (what to read, and that empty means generated) |
| Expression output port `NULL` with `DEFAULTVALUE="ERROR('transformation error')"` | `exp_Initial.o_CURR_PP_FLAG` | row 61 (`ERROR()`), section 6 "Row error handling" |
| Update Strategy `DD_UPDATE`, `Forward Rejected Rows=YES` | `upd_Reset_Current_PP` | row 90 |
| Session `Treat source rows as = Data driven` + writer `Insert/Update as Update/Delete` flags -> MERGE shape | session `ATTRIBUTE`, WRITER `SESSIONEXTENSION` | row 90, section 6 |
| `Target load type = Normal`, `Truncate target table option = NO` | WRITER `SESSIONEXTENSION` | section 2 (writes), section 6 |
| Reader and writer `CONNECTIONREFERENCE` both `INFO_TARGET` (Oracle) -> source == target, self-MERGE | READER/WRITER `SESSIONEXTENSION` | section 2 (lineage from session, not mapping), section 9 |
| `Commit Type=Target`, `Commit Interval=10000`, `Commit On End Of File=YES`, `Rollback Transactions on Errors=NO` | session `ATTRIBUTE` | section 6 (commit intervals), trap 25 |
| `Recovery Strategy = Fail task and continue workflow` on session and command components | session/`SESSIONCOMPONENT` | section 6 (recovery -> `max_retries`, `run_if`) |
| `Failure Email` component with `%s %b %c` placeholders | `on_failure_mail` | section 6 (Email task) |
| Pre-session / post-session success / post-session failure **variable assignment** components | `SESSIONCOMPONENT` x3 | section 6 (`$$` workflow variables, `SETVARIABLE`) |
| Predefined workflow variables `$s.Status`, `.TgtSuccessRows`, `.ErrorCode`, ... | `WORKFLOWVARIABLE` x13 | section 6 (link conditions -> `run_if`; counters -> OUT params / task values) |
| Link conditions `$s_X.Status = Succeeded` chaining four sessions to an Email task | `WORKFLOWLINK` x5 | section 6 (`depends_on` + `run_if: ALL_SUCCESS`) |
| `SCHEDULEINFO SCHEDULETYPE="ONDEMAND"` | `SCHEDULER` | section 2 (scheduler edges: external caller to be found) |
| `SESSTRANSFORMATIONINST` partition points (`PASS THROUGH`) and `Is Partitionable=NO` | session | section 2 (partition overrides), row 91 note on partition-dependent values |
| `PARTITION`-free session on a single-row update: commit-interval semantics vacuous | whole unit | trap 25 (when it does matter) |
| Reject file `$PMBadFileDir/reset_pay_period1.bad` | WRITER `SESSIONEXTENSION` | section 6 "Row error handling" (server variables `$PM*`) |

## Recon tier that catches a wrong conversion

- **Tier 1 count of `CURR_PP_FLAG = 'Y'` after the run** (expected 0) and of `CURR_PP_FLAG IS NULL` (expected
  previous NULLs + previous Ys): a conversion that inserts instead of updates (Update-else-Insert misread), or that
  writes `''` instead of `NULL` (Oracle folds `''` to NULL so the legacy target never distinguishes them - trap 5),
  fails the count. `empty_string_is_null` would mask the second case: this is a column where the STOP A policy must
  be **off** for the recon to be meaningful, and the note records that.
- **Tier 3 keyed on `PP_NUM, PP_END_YEAR`**: a wrong inferred key (e.g. `PP_NUM` alone) updates rows in other years
  -> extra diffs on `CURR_PP_FLAG` outside the current period. This is the check that validates the INFERRED key.
- **Tier 1 on the downstream chain**: if `run_if` is `ALL_DONE` instead of `ALL_SUCCESS`, `s_Set` runs after a
  failed reset and the table ends with two current periods; the count of `'Y'` rows (expected 1) catches it.
- Tier 2 is not informative on a one-row update; the unit is Tier 1 + Tier 3 by design.

## Findings recorded, not converted

- Source and target are the same table over the same connection: the mapping is an in-place UPDATE. Pre-cutover the
  legacy Oracle table stays the system of record; the converted MERGE runs against the migrated copy only.
- `ERROR('transformation error')` as the default value on a constant-NULL port can never fire; kept as documentation.
- The three elided sessions (`Set`, `Verify`, `Build_Message`) and the Email task body are separate units; the job
  YAML includes their task shapes so the DAG is complete, with their SQL files marked "not included".
- ONDEMAND scheduler with no wrapper in the fixture: who starts `wf_Pay_Calendar` is unknown (INFERRED manual).

## Not verified live

- `PAY_PERIOD` primary key (the `TARGET` element is not in the excerpt); the MERGE key is INFERRED.
- Whether the `INFO_TARGET` connection is really the same physical schema for reader and writer (same name, so
  assumed yes).
- Actual behaviour of `Rollback Transactions on Errors = NO` with `Commit Interval = 10000` on a multi-row run;
  irrelevant for this one-row unit, relevant for the sibling sessions.
- Live `$PMBadFileDir` contents (whether any `.bad` file has ever been non-empty).
- `%s`, `%b`, `%c` placeholder rendering in the failure mail; Lakeflow email content is not a like-for-like template.
