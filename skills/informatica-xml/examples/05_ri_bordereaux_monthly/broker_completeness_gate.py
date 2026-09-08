# Databricks notebook source
# Completeness gate for wf_REINSURANCE_BORDEREAUX_MONTHLY (task gate_all_brokers_arrived in converted.job.yml).
#
# Legacy: one Informatica file watcher per broker, Control-M WD3, and a manual-restart runbook for BRK0007 (habitually
# two days late). The Lakeflow `trigger.file_arrival` only says "something landed"; this task decides whether the
# MONTH is complete, so the publishing pipeline can never run on a partial broker set. It is read-only and therefore
# safe to run on every trigger (idempotent); publication happens only when the condition task evaluates true.
#
# Shapes: databricks-jobs references/task-types.md "Notebook Task" (base_parameters, dbutils.widgets.get) and
# "Generate Dynamic Inputs" (dbutils.jobs.taskValues.set); task-value reference and condition task per
# https://docs.databricks.com/aws/en/jobs/task-values and https://docs.databricks.com/aws/en/dev-tools/bundles/resources
# (`condition_task` op/left/right, `depends_on[].outcome`).
import re
from datetime import date, timedelta

LANDING = dbutils.widgets.get("bdx_landing_path")            # ${var.bdx_landing_path}; was /interface/inbound/bordereaux
EXPECTED_BROKERS_TABLE = dbutils.widgets.get("expected_brokers_table")   # <migration_catalog>.ri.expected_brokers
TARGET_MONTH = dbutils.widgets.get("target_month")            # '' on a triggered run; 'yyyyMM' on a manual rerun

# Bordereaux for month M land on working days 1-5 of month M+1 (cron `0 6 1-5 * *`), so the default target month is
# the previous calendar month. A rerun for an older month passes target_month explicitly (replaces the runbook restart).
if not TARGET_MONTH:
    TARGET_MONTH = (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y%m")

# Expected set: the broker list hard-coded in scripts/bdx_transfer (BRK0007 BRK0012 BRK0023 BRK0031) seeded into a
# reference table with effective-from/to months, because the MAPPING DESCRIPTION speaks of 14 broker sources and the
# set changes over time. Editing a ksh loop was the legacy "onboarding"; a row in this table is the converted one.
expected = {
    r.BROKER_ID
    for r in spark.sql(
        f"SELECT BROKER_ID FROM {EXPECTED_BROKERS_TABLE} "
        f"WHERE effective_from_month <= '{TARGET_MONTH}' AND coalesce(effective_to_month, '999912') >= '{TARGET_MONTH}'"
    ).collect()
}

# Arrived set: broker files named CLAIMS_BDX_<BROKER>_<yyyyMM>*.csv (bdx_transfer mget mask is CLAIMS_BDX_${BRK}_*.csv;
# the month token after the broker is INFERRED from the sample file names in the landing area, recorded in the
# unit brief). Reading names, not contents: cheap, and identical to what the per-broker file watcher tested.
name_re = re.compile(rf"^CLAIMS_BDX_(BRK\d{{4}})_{TARGET_MONTH}\w*\.csv$")
arrived = {m.group(1) for f in dbutils.fs.ls(LANDING) if (m := name_re.match(f.name))}

missing = sorted(expected - arrived)
unexpected = sorted(arrived - expected)      # a broker not in the expected table: a census finding, never silently loaded

dbutils.jobs.taskValues.set(key="target_month", value=TARGET_MONTH)
dbutils.jobs.taskValues.set(key="missing_brokers", value=",".join(missing))
dbutils.jobs.taskValues.set(key="unexpected_brokers", value=",".join(unexpected))
dbutils.jobs.taskValues.set(key="all_brokers_arrived", value="true" if not missing and expected else "false")

print(f"target_month={TARGET_MONTH} expected={sorted(expected)} arrived={sorted(arrived)} missing={missing} unexpected={unexpected}")
