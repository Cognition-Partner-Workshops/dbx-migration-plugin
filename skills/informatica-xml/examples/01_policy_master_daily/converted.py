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

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Parameter-file values (source.par) become pipeline configuration, not literals:
#   $$RUNDATE, $InputFile_PLCYMSTR -> landing path under a UC volume; $DBConnection_LKP -> catalog.schema of the crosswalk
RUNDATE = spark.conf.get("informatica.RUNDATE")                       # was $$RUNDATE=260115
LANDING = spark.conf.get("informatica.landing_path")                  # was /interface/inbound/plcymstr/
XREF_TABLE = spark.conf.get("informatica.xref_client_party_table")    # was REF_DB.XREF_CLIENT_PARTY via $DBConnection_LKP


@dp.temporary_view()
def sq_plcymstr_daily():
    # Fixed-width flat file: SOURCEFIELD OFFSET/LENGTH pairs from source.xml (LRECL 80, STRIPTRAILINGBLANKS=YES,
    # NULL_CHARACTER='*'). Read each line as one string and slice; SQL substr is 1-based, OFFSET is 0-based.
    raw = spark.read.text(f"{LANDING}/PLCYMSTR_D{RUNDATE}.dat").withColumnRenamed("value", "line")

    def field(offset, length):
        col = F.substring("line", offset + 1, length)
        col = F.rtrim(col)                                  # STRIPTRAILINGBLANKS=YES on the source definition
        return F.when(F.trim(col) == "*", None).otherwise(col)  # NULL_CHARACTER='*'

    return raw.select(
        field(0, 18).alias("PLCY_POLICY_NO"),
        field(18, 10).alias("PLCY_CLIENT_NO"),
        field(28, 4).alias("PLCY_PRODUCT_CD"),
        field(32, 5).alias("PLCY_INCEPT_DT_JUL"),
        field(37, 11).alias("PLCY_ANNL_PREM"),               # PICTURETEXT 9(09)V99: implied 2 decimals
        field(48, 2).alias("PLCY_STATUS"),
        field(50, 8).alias("PLCY_POSTCODE"),
        field(58, 20).alias("PLCY_SURNAME"),
    )


@dp.temporary_view()
def exp_policy_dates():
    # EXP_POLICY_DATES: Julian YYDDD, Y2K window 00-49 => 20xx (pivot 49 is the mapping's rule; the actuarial SAS
    # macro uses 50: keep 49 here, record the disagreement as an estate finding, not a conversion choice).
    # TO_INTEGER(SUBSTR(...)) rounds in Informatica; on a 2-digit numeric text the result equals CAST, and a
    # non-numeric value would be 0 in Informatica (SKILL.md section 5 row 14) -> made explicit with coalesce.
    src = spark.read.table("sq_plcymstr_daily")
    yy = F.coalesce(F.expr("try_cast(substr(PLCY_INCEPT_DT_JUL, 1, 2) AS INT)"), F.lit(0))
    ddd = F.coalesce(F.expr("try_cast(substr(PLCY_INCEPT_DT_JUL, 3, 3) AS INT)"), F.lit(0))
    v_year = F.when(yy <= 49, 2000 + yy).otherwise(1900 + yy)
    # ADD_TO_DATE(TO_DATE(TO_CHAR(v_YEAR)||'0101','YYYYMMDD'),'DD', ddd - 1)  (rows 9, 21)
    out_inception_dt = F.date_add(F.to_date(F.concat(v_year.cast("string"), F.lit("0101")), "yyyyMMdd"), ddd - 1)
    return src.select("PLCY_POLICY_NO", out_inception_dt.alias("out_INCEPTION_DT"))


@dp.temporary_view()
def exp_postcode_dq():
    # EXP_POSTCODE_DQ (DQR-014 variant A): insert a space before the final 3 chars when absent and len >= 5,
    # UPPER, then REG_MATCH -> RLIKE (row 42). LENGTH/INSTR/LTRIM/RTRIM are identical (rows 31, 35, 36).
    src = spark.read.table("sq_plcymstr_daily").withColumn("pc", F.trim(F.col("PLCY_POSTCODE")))
    # LTRIM(RTRIM(x)) trims spaces only, same as trim(); SUBSTR with a computed start/len keeps 1-based semantics.
    v_pc_std = F.when(
        (F.locate(" ", F.col("pc")) == 0) & (F.length("pc") >= 5),
        F.concat(F.expr("substr(pc, 1, length(pc) - 3)"), F.lit(" "), F.expr("substr(pc, length(pc) - 2, 3)")),
    ).otherwise(F.upper(F.col("pc")))
    out_postcode_std = F.upper(v_pc_std)
    out_postcode_dq_status = F.when(
        out_postcode_std.rlike(r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"), "VALID"
    ).otherwise("INVALID")
    return src.select("PLCY_POLICY_NO", out_postcode_std.alias("out_POSTCODE_STD"),
                      out_postcode_dq_status.alias("out_POSTCODE_DQ_STATUS"))


@dp.temporary_view()
def lkp_xref_client_party():
    # LKP_XREF_CLIENT_PARTY: connected, static cache, 'Lookup policy on multiple match = Use First Value'.
    # Cache build order is not in the export: ORDER BY below is INFERRED and must be recorded in the unit brief
    # (SKILL.md section 5 row 74; trap 9).
    from pyspark.sql.window import Window
    xref = spark.read.table(XREF_TABLE)
    w = Window.partitionBy("CLIENT_NO").orderBy(F.col("PARTY_ID"))  # INFERRED order
    return xref.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select("CLIENT_NO", "PARTY_ID")


@dp.materialized_view(name="stg_policy_master", comment="m_POLICY_MASTER_DAILY converted; grain = POLICY_NO")
@dp.expect_or_drop("policy_no_present", "POLICY_NO IS NOT NULL")          # NOTNULL flat-file field -> row error legacy
@dp.expect("postcode_valid", "POSTCODE_DQ_STATUS = 'VALID'")              # DQ status was a column, not a reject: warn only
def stg_policy_master():
    src = spark.read.table("sq_plcymstr_daily")
    dates = spark.read.table("exp_policy_dates")
    pc = spark.read.table("exp_postcode_dq")
    xref = spark.read.table("lkp_xref_client_party")

    active_flag = F.when(F.col("PLCY_STATUS").isin("IF", "RN"), "Y").otherwise("N")   # EXP_POLICY_FLAGS
    # PICTURETEXT 9(09)V99 implied decimals: legacy carried the field as string(11); target ANNUAL_PREMIUM_GBP is
    # decimal(12,2). The export has no CONNECTOR for this column (see NOTE.md), so the scaling below is INFERRED.
    annual_premium_gbp = (F.expr("try_cast(PLCY_ANNL_PREM AS DECIMAL(11,0))") / 100).cast("decimal(12,2)")

    return (
        src.join(dates, "PLCY_POLICY_NO", "left")
           .join(pc, "PLCY_POLICY_NO", "left")
           .join(xref, src.PLCY_CLIENT_NO.cast("decimal(10,0)") == xref.CLIENT_NO, "left")  # decimal(10,0) lookup port
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
               active_flag.alias("ACTIVE_POLICY_FLAG"),
               F.current_timestamp().alias("LOAD_TS"),                # audit column: excluded from Tier 3
           )
    )
