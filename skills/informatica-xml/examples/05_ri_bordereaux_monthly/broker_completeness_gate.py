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

from pyspark.sql import functions as F

LANDING = dbutils.widgets.get("bdx_landing_path")            # ${var.bdx_landing_path}; was /interface/inbound/bordereaux
EXPECTED_BROKERS_TABLE = dbutils.widgets.get("expected_brokers_table")   # <migration_catalog>.ri.expected_brokers
TARGET_MONTH = dbutils.widgets.get("target_month").strip()    # '' on a triggered run; 'yyyyMM' on a manual rerun

# Both widgets are caller-controlled job parameters and run under the job's identity: neither is ever interpolated
# into SQL text. The table name must be a plain three-part UC identifier and the month a literal yyyyMM; anything
# else stops the run before any read.
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}", EXPECTED_BROKERS_TABLE):
    raise ValueError("expected_brokers_table must be catalog.schema.table (unquoted identifiers)")
if TARGET_MONTH and not re.fullmatch(r"\d{6}", TARGET_MONTH):
    raise ValueError("target_month must be yyyyMM or empty")

# Bordereaux for month M land on working days 1-5 of month M+1 (cron `0 6 1-5 * *`), so the default target month is
# the previous calendar month. A rerun for an older month passes target_month explicitly (replaces the runbook restart).
if not TARGET_MONTH:
    TARGET_MONTH = (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y%m")

# Expected set: the broker list hard-coded in scripts/bdx_transfer (BRK0007 BRK0012 BRK0023 BRK0031) seeded into a
# reference table with effective-from/to months, because the MAPPING DESCRIPTION speaks of 14 broker sources and the
# set changes over time. Editing a ksh loop was the legacy "onboarding"; a row in this table is the converted one.
# DataFrame API with Column parameters (no SQL string): the values above can only ever be compared, not executed.
expected = {
    r.BROKER_ID
    for r in spark.read.table(EXPECTED_BROKERS_TABLE)
        .filter((F.col("effective_from_month") <= F.lit(TARGET_MONTH))
                & (F.coalesce(F.col("effective_to_month"), F.lit("999912")) >= F.lit(TARGET_MONTH)))
        .select("BROKER_ID")
        .collect()
}

# Arrived set: broker files named CLAIMS_BDX_<BROKER>_<yyyyMM>*.csv (bdx_transfer mget mask is CLAIMS_BDX_${BRK}_*.csv;
# the month token after the broker is INFERRED from the sample file names in the landing area, recorded in the
# unit brief). Reading names, not contents: cheap, and identical to what the per-broker file watcher tested.
name_re = re.compile(rf"^CLAIMS_BDX_(BRK\d{{4}})_{re.escape(TARGET_MONTH)}\w*\.csv$")
arrived = {m.group(1) for f in dbutils.fs.ls(LANDING) if (m := name_re.match(f.name))}

missing = sorted(expected - arrived)
unexpected = sorted(arrived - expected)      # a broker not in the expected table: a census finding, never silently loaded

# The month is complete only when every expected broker has landed AND nothing unexpected has: a file from a broker
# that is not in the effective expected set is an undeclared source (a census finding), so the gate stays closed and
# the run FAILS visibly with the IDs, exactly like the legacy "unknown SOURCE definition" would have needed a
# developer. Nothing is quarantined here because the gate never reads file contents; the pipeline additionally
# routes any such rows to ri_claims_bdx_unexpected_broker (converted.py) so they can never reach the published view
# even if the pipeline is started by hand.
complete = bool(expected) and not missing and not unexpected

dbutils.jobs.taskValues.set(key="target_month", value=TARGET_MONTH)
dbutils.jobs.taskValues.set(key="missing_brokers", value=",".join(missing))
dbutils.jobs.taskValues.set(key="unexpected_brokers", value=",".join(unexpected))
dbutils.jobs.taskValues.set(key="all_brokers_arrived", value="true" if complete else "false")

print(f"target_month={TARGET_MONTH} expected={sorted(expected)} arrived={sorted(arrived)} missing={missing} unexpected={unexpected}")

if unexpected:
    raise RuntimeError(
        f"Unexpected broker files for {TARGET_MONTH}: {unexpected}. Add the broker to {EXPECTED_BROKERS_TABLE} "
        "(onboarding decision) or remove the files; publication is blocked until then."
    )
