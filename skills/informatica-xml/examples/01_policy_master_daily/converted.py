# Converted unit: INS_POLICY / m_POLICY_MASTER_DAILY  (fixture: source.xml, source.par)
# Target form: Lakeflow Spark Declarative Pipelines, modern API
#   (databricks-pipelines references/python-basics.md "Setup": `from pyspark import pipelines as dp`, never `import dlt`).
# Orchestration and the Event Wait / Email tasks are in converted.job.yml.
#
# Legacy pipeline (from CONNECTOR rows in source.xml):
#   SQ_PLCYMSTR_DAILY -> EXP_POLICY_DATES  -> STG_POLICY_MASTER.INCEPTION_DT
#                     -> EXP_POSTCODE_DQ   -> STG_POLICY_MASTER.POSTCODE_STD, POSTCODE_DQ_STATUS
#                     -> EXP_POLICY_FLAGS  -> STG_POLICY_MASTER.ACTIVE_POLICY_FLAG
#                     -> LKP_XREF_CLIENT_PARTY (connected, static cache, Use First Value) -> STG_POLICY_MASTER.PARTY_ID
# Port names are kept as column aliases so Tier 3 field mapping is 1:1 (SKILL.md section 5 row 70).

import re

from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F

# Parameter-file values (source.par) become pipeline configuration, not literals:
#   $InputFile_PLCYMSTR -> landing path under a UC volume; $DBConnection_LKP -> catalog.schema of the crosswalk.
#   $$RUNDATE was rewritten into the parameter file before every run by the wrapper (per-run value, 260115 is one
#   sample). Pipeline configuration is a deployment-time value, so it is NOT the daily source of the date: the date
#   is read from the arrived file's own name (PLCYMSTR_D<yymmdd>.dat, the Event Wait mask). The configuration key
#   is only an OPTIONAL override for an explicit rerun of an older day; empty (the deployed default) = newest file.
RUNDATE_OVERRIDE = spark.conf.get("informatica.RUNDATE", "").strip()
if RUNDATE_OVERRIDE and not re.fullmatch(r"\d{6}", RUNDATE_OVERRIDE):
    raise ValueError("informatica.RUNDATE must be yymmdd or empty")
LANDING = spark.conf.get("informatica.landing_path")                  # was /interface/inbound/plcymstr/
XREF_TABLE = spark.conf.get("informatica.xref_client_party_table")    # was REF_DB.XREF_CLIENT_PARTY via $DBConnection_LKP


@dp.table(name="plcymstr_raw", comment="PLCYMSTR_D<yymmdd>.dat lines as landed; one row per line, RUNDATE from the file name")
def plcymstr_raw():
    # Auto Loader ingests each landed file exactly once (databricks-pipelines references/auto-loader-python.md:
    # `spark.readStream.format("cloudFiles")`, `cloudFiles.format` text, generic `pathGlobFilter`; SKILL.md
    # "input_file_name() -> _metadata.file_path"). The legacy landing directory was swept by NDM; here every day's
    # file stays queryable, which is what makes an explicit-date rerun a filter instead of a file restore.
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "text")
                 .option("encoding", "windows-1252")                   # SOURCE codepage MS1252 (trap 18)
                 .option("pathGlobFilter", "PLCYMSTR_D*.dat")
                 .load(LANDING)
                 .withColumnRenamed("value", "line")
                 .withColumn("RUNDATE", F.regexp_extract(F.col("_metadata.file_path"), r"PLCYMSTR_D(\d{6})\.dat$", 1)))


@dp.temporary_view()
def sq_plcymstr_daily():
    # One run = one day's file, exactly as `pmcmd startworkflow` with the rewritten $$RUNDATE did. Without an
    # override the run processes the newest RUNDATE ingested (the file whose arrival fired the trigger); the target
    # load type is not in the export, so "replace with the newest day" is INFERRED and recorded in the unit brief.
    ingested = spark.read.table("plcymstr_raw")
    if RUNDATE_OVERRIDE:
        selected = ingested.filter(F.col("RUNDATE") == RUNDATE_OVERRIDE)
    else:
        newest = F.max("RUNDATE").over(Window.partitionBy())
        selected = ingested.withColumn("newest", newest).filter(F.col("RUNDATE") == F.col("newest")).drop("newest")
    # Fixed-width flat file: SOURCEFIELD OFFSET/LENGTH pairs from source.xml (LRECL 80, STRIPTRAILINGBLANKS=YES,
    # NULL_CHARACTER='*'). Read each line as one string and slice; SQL substr is 1-based, OFFSET is 0-based.
    raw = selected

    def field(offset, length):
        col = F.substring("line", offset + 1, length)
        col = F.rtrim(col)                                  # STRIPTRAILINGBLANKS=YES on the source definition
        return F.when(F.trim(col) == "*", None).otherwise(col)  # NULL_CHARACTER='*'

    return raw.select(
        F.col("RUNDATE"),
        field(0, 18).alias("PLCY_POLICY_NO"),
        field(18, 10).alias("PLCY_CLIENT_NO"),
        field(28, 4).alias("PLCY_PRODUCT_CD"),
        field(32, 5).alias("PLCY_INCEPT_DT_JUL"),
        field(37, 11).alias("PLCY_ANNL_PREM"),               # PICTURETEXT 9(09)V99: implied 2 decimals
        field(48, 2).alias("PLCY_STATUS"),
        field(50, 8).alias("PLCY_POSTCODE"),
        field(58, 20).alias("PLCY_SURNAME"),
    )


# The three Expression transformations are row-preserving (one output row per input row, no grain change). They are
# therefore evaluated as column derivations on the SAME source row below, never as separate views re-joined on
# PLCY_POLICY_NO: the file has no declared unique key, and a repeated policy number would multiply rows through such a
# join where the legacy pipeline kept exactly one output row per input row (SKILL.md section 5 rows 69-70; ex05 NOTE).


def exp_policy_dates(df):
    # EXP_POLICY_DATES: Julian YYDDD, Y2K window 00-49 => 20xx (pivot 49 is the mapping's rule; the actuarial SAS
    # macro uses 50: keep 49 here, record the disagreement as an estate finding, not a conversion choice).
    # TO_INTEGER(SUBSTR(...)) rounds in Informatica; on a 2-digit numeric text the result equals CAST, and a
    # non-numeric value would be 0 in Informatica (SKILL.md section 5 row 14) -> made explicit with coalesce.
    yy = F.coalesce(F.expr("try_cast(substr(PLCY_INCEPT_DT_JUL, 1, 2) AS INT)"), F.lit(0))
    ddd = F.coalesce(F.expr("try_cast(substr(PLCY_INCEPT_DT_JUL, 3, 3) AS INT)"), F.lit(0))
    v_year = F.when(yy <= 49, 2000 + yy).otherwise(1900 + yy)
    # ADD_TO_DATE(TO_DATE(TO_CHAR(v_YEAR)||'0101','YYYYMMDD'),'DD', ddd - 1)  (rows 9, 21)
    out_inception_dt = F.date_add(F.to_date(F.concat(v_year.cast("string"), F.lit("0101")), "yyyyMMdd"), ddd - 1)
    return df.withColumn("out_INCEPTION_DT", out_inception_dt)


def exp_postcode_dq(df):
    # EXP_POSTCODE_DQ (DQR-014 variant A): insert a space before the final 3 chars when absent and len >= 5,
    # UPPER, then REG_MATCH -> RLIKE (row 42). LENGTH/INSTR/LTRIM/RTRIM are identical (rows 31, 35, 36).
    # LTRIM(RTRIM(x)) trims spaces only, same as trim(); SUBSTR with a computed start/len keeps 1-based semantics.
    pc = F.trim(F.col("PLCY_POSTCODE"))
    v_pc_std = F.when(
        (F.locate(" ", pc) == 0) & (F.length(pc) >= 5),
        F.concat(pc.substr(F.lit(1), F.length(pc) - 3), F.lit(" "), pc.substr(F.length(pc) - 2, F.lit(3))),
    ).otherwise(F.upper(pc))
    out_postcode_std = F.upper(v_pc_std)
    out_postcode_dq_status = F.when(
        out_postcode_std.rlike(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"), "VALID"
    ).otherwise("INVALID")
    return (df.withColumn("out_POSTCODE_STD", out_postcode_std)
              .withColumn("out_POSTCODE_DQ_STATUS", out_postcode_dq_status))


def exp_policy_flags(df):
    # EXP_POLICY_FLAGS
    return df.withColumn("out_ACTIVE_POLICY_FLAG", F.when(F.col("PLCY_STATUS").isin("IF", "RN"), "Y").otherwise("N"))


@dp.temporary_view()
def lkp_xref_client_party():
    # LKP_XREF_CLIENT_PARTY: connected, static cache, 'Lookup policy on multiple match = Use First Value'.
    # Cache build order is not in the export: ORDER BY below is INFERRED and must be recorded in the unit brief
    # (SKILL.md section 5 row 74; trap 9). row_number() = 1 guarantees one lookup row per CLIENT_NO, so the join
    # below cannot multiply source rows.
    xref = spark.read.table(XREF_TABLE)
    w = Window.partitionBy("CLIENT_NO").orderBy(F.col("PARTY_ID"))  # INFERRED order
    return xref.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select("CLIENT_NO", "PARTY_ID")


@dp.materialized_view(name="stg_policy_master", comment="m_POLICY_MASTER_DAILY converted; one row per PLCYMSTR line")
@dp.expect_or_drop("policy_no_present", "POLICY_NO IS NOT NULL")          # NOTNULL flat-file field -> row error legacy
@dp.expect("postcode_valid", "POSTCODE_DQ_STATUS = 'VALID'")              # DQ status was a column, not a reject: warn only
def stg_policy_master():
    src = spark.read.table("sq_plcymstr_daily")
    xref = spark.read.table("lkp_xref_client_party")

    # SQ -> EXP_POLICY_DATES / EXP_POSTCODE_DQ / EXP_POLICY_FLAGS all fan out from the same SQ row and fan back into
    # one target row: one transformation chain over `src`, input cardinality preserved. The legacy target's key
    # handling (Teradata STG_POLICY_MASTER, load type not exported) decides whether duplicate policy numbers survive;
    # that is the target's behaviour, recorded in NOTE.md, not something the Expressions may change.
    derived = exp_policy_flags(exp_postcode_dq(exp_policy_dates(src)))

    # PICTURETEXT 9(09)V99 implied decimals: legacy carried the field as string(11); target ANNUAL_PREMIUM_GBP is
    # decimal(12,2). The export has no CONNECTOR for this column (see NOTE.md), so the scaling below is INFERRED.
    annual_premium_gbp = (F.expr("try_cast(PLCY_ANNL_PREM AS DECIMAL(11,0))") / 100).cast("decimal(12,2)")

    return (
        derived.join(xref, derived.PLCY_CLIENT_NO.cast("decimal(10,0)") == xref.CLIENT_NO, "left")  # decimal(10,0) lookup port
               .select(
                   F.col("PLCY_POLICY_NO").alias("POLICY_NO"),
                   F.col("PARTY_ID"),                                     # NULL when unmatched (~12%/day per DESCRIPTION)
                   F.col("PLCY_CLIENT_NO").alias("CLIENT_NO"),
                   F.col("PLCY_PRODUCT_CD").alias("PRODUCT_CD"),
                   F.col("out_INCEPTION_DT").alias("INCEPTION_DT"),
                   annual_premium_gbp.alias("ANNUAL_PREMIUM_GBP"),
                   F.col("PLCY_STATUS").alias("POLICY_STATUS"),
                   F.col("out_POSTCODE_STD").alias("POSTCODE_STD"),
                   F.col("out_POSTCODE_DQ_STATUS").alias("POSTCODE_DQ_STATUS"),
                   F.col("out_ACTIVE_POLICY_FLAG").alias("ACTIVE_POLICY_FLAG"),
                   F.col("RUNDATE"),                                      # the day this row was loaded from ($$RUNDATE)
                   F.current_timestamp().alias("LOAD_TS"),                # audit column: excluded from Tier 3
               )
    )
