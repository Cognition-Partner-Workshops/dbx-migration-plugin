# INS_POLICY / m_POLICY_MASTER_DAILY as a Lakeflow Spark Declarative Pipeline. Workflow tasks (Event Wait ->
# trigger.file_arrival, failure Email -> email_notifications.on_failure) live in the Lakeflow Job, not here.
# ruff: noqa: F821  (`spark` is pre-imported in pipeline files: databricks-pipelines references/python-basics.md)
import re

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

# $$RUNDATE (rewritten into the .par per run) is read from the arrived file name; the config key is an explicit-rerun override
RUNDATE_OVERRIDE = spark.conf.get("informatica.RUNDATE", "").strip()
if RUNDATE_OVERRIDE and not re.fullmatch(r"\d{6}", RUNDATE_OVERRIDE):
    raise ValueError("informatica.RUNDATE must be yymmdd or empty")
LANDING = spark.conf.get("informatica.landing_path")  # $InputFile_* dir -> volume path; $DBConnection_LKP -> catalog.schema
XREF_TABLE = spark.conf.get("informatica.xref_client_party_table")


@dp.table(name="plcymstr_raw", comment="PLCYMSTR_D<yymmdd>.dat lines as landed; RUNDATE from the file name")
def plcymstr_raw():
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "text").option("encoding", "windows-1252")  # FLATFILE CODEPAGE=MS1252
                 .option("pathGlobFilter", "PLCYMSTR_D*.dat")
                 .load(LANDING).withColumnRenamed("value", "line")
                 .withColumn("RUNDATE", F.regexp_extract(F.col("_metadata.file_path"), r"PLCYMSTR_D(\d{6})\.dat$", 1)))


@dp.temporary_view()
def sq_plcymstr_daily():
    ingested = spark.read.table("plcymstr_raw")
    if RUNDATE_OVERRIDE:
        selected = ingested.filter(F.col("RUNDATE") == RUNDATE_OVERRIDE)
    else:
        newest = F.max("RUNDATE").over(Window.partitionBy())
        selected = ingested.withColumn("newest", newest).filter(F.col("RUNDATE") == F.col("newest")).drop("newest")

    def field(offset, length):  # SOURCEFIELD OFFSET is 0-based, substring is 1-based
        col = F.rtrim(F.substring("line", offset + 1, length))  # STRIPTRAILINGBLANKS=YES
        return F.when(F.trim(col) == "*", None).otherwise(col)  # NULL_CHARACTER='*'

    return selected.select(
        F.col("RUNDATE"), field(0, 18).alias("PLCY_POLICY_NO"), field(18, 10).alias("PLCY_CLIENT_NO"),
        field(28, 4).alias("PLCY_PRODUCT_CD"), field(32, 5).alias("PLCY_INCEPT_DT_JUL"), field(37, 11).alias("PLCY_ANNL_PREM"),
        field(48, 2).alias("PLCY_STATUS"), field(50, 8).alias("PLCY_POSTCODE"), field(58, 20).alias("PLCY_SURNAME"))


@dp.temporary_view()
def lkp_xref_client_party():  # Use First Value: cache order is not exported, ORDER BY is INFERRED
    w = Window.partitionBy("CLIENT_NO").orderBy(F.col("PARTY_ID"))
    return spark.read.table(XREF_TABLE).withColumn("rn", F.row_number().over(w)).filter("rn = 1").select("CLIENT_NO", "PARTY_ID")


@dp.temporary_view()
def stg_policy_master_rows():  # every derived row, before the legacy reject condition splits target from quarantine
    src = spark.read.table("sq_plcymstr_daily")
    # EXP_POLICY_DATES: TO_INTEGER on bad text is 0 in Informatica; pivot 49 is the mapping's rule (SAS uses 50)
    yy = F.coalesce(F.expr("try_cast(substr(PLCY_INCEPT_DT_JUL, 1, 2) AS INT)"), F.lit(0))
    ddd = F.coalesce(F.expr("try_cast(substr(PLCY_INCEPT_DT_JUL, 3, 3) AS INT)"), F.lit(0))
    v_year = F.when(yy <= 49, 2000 + yy).otherwise(1900 + yy)
    inception_dt = F.date_add(F.to_date(F.concat(v_year.cast("string"), F.lit("0101")), "yyyyMMdd"), ddd - 1)
    # EXP_POSTCODE_DQ: INSTR -> locate, LENGTH/SUBSTR 1-based, REG_MATCH -> rlike
    pc = F.trim(F.col("PLCY_POSTCODE"))
    postcode_std = F.upper(F.when((F.locate(" ", pc) == 0) & (F.length(pc) >= 5),
                                  F.concat(pc.substr(F.lit(1), F.length(pc) - 3), F.lit(" "), pc.substr(F.length(pc) - 2, F.lit(3)))
                                  ).otherwise(F.upper(pc)))
    postcode_dq = F.when(postcode_std.rlike(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"), "VALID").otherwise("INVALID")
    # 9(09)V99 implied decimals; no CONNECTOR feeds ANNUAL_PREMIUM_GBP in the export, so the scaling is INFERRED
    annual_premium_gbp = (F.expr("try_cast(PLCY_ANNL_PREM AS DECIMAL(11,0))") / 100).cast("decimal(12,2)")
    xref = spark.read.table("lkp_xref_client_party")  # Expressions are row-preserving: derive on the same row
    return (src.join(xref, src.PLCY_CLIENT_NO.cast("decimal(10,0)") == xref.CLIENT_NO, "left")
               .select(F.col("PLCY_POLICY_NO").alias("POLICY_NO"), F.col("PARTY_ID"),
                       F.col("PLCY_CLIENT_NO").alias("CLIENT_NO"), F.col("PLCY_PRODUCT_CD").alias("PRODUCT_CD"),
                       inception_dt.alias("INCEPTION_DT"), annual_premium_gbp.alias("ANNUAL_PREMIUM_GBP"),
                       F.col("PLCY_STATUS").alias("POLICY_STATUS"), postcode_std.alias("POSTCODE_STD"),
                       postcode_dq.alias("POSTCODE_DQ_STATUS"),
                       F.when(F.col("PLCY_STATUS").isin("IF", "RN"), "Y").otherwise("N").alias("ACTIVE_POLICY_FLAG"),
                       F.col("RUNDATE"), F.current_timestamp().alias("LOAD_TS")))


@dp.materialized_view(name="stg_policy_master", comment="one row per PLCYMSTR line")
@dp.expect_or_drop("policy_no_present", "POLICY_NO IS NOT NULL")  # NOTNULL flat-file field: row error in legacy
@dp.expect("postcode_valid", "POSTCODE_DQ_STATUS = 'VALID'")  # a status column in legacy, not a reject
def stg_policy_master():
    return spark.read.table("stg_policy_master_rows")


@dp.materialized_view(name="stg_policy_master_rejects", comment="legacy .bad file: rows failing policy_no_present, kept for correction and replay")
def stg_policy_master_rejects():
    return spark.read.table("stg_policy_master_rows").filter("POLICY_NO IS NULL").withColumn("REJECT_REASON", F.lit("policy_no_present"))
