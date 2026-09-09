# RI_CESSIONS / m_RI_BORDEREAUX_MONTHLY as a Lakeflow Spark Declarative Pipeline with Auto Loader ingestion
# (databricks-pipelines references/auto-loader-python.md, expectations-python.md). The SFTP pull (bdx_transfer.ksh) is an
# ingestion decision, not converted. Only EXP_REKEY_POLICY, EXP_AMT_CLEAN and LKP_SII_LOB are exported; the 14 per-broker
# SOURCE definitions, the ceded-claims computation and the Lloyd's outward bordereau are INFERRED from the DESCRIPTION.
# ruff: noqa: F821  (`spark` is pre-imported in pipeline files: databricks-pipelines references/python-basics.md)
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

LANDING = spark.conf.get("informatica.bdx_landing_path")  # was /interface/inbound/bordereaux
SII_LOB_MAP = spark.conf.get("informatica.sii_lob_map_table")  # REF_DB.SII_LOB_MAP
EXPECTED_BROKERS = spark.conf.get("informatica.expected_brokers_table")
BDX_LAYOUTS = spark.conf.get("informatica.bdx_layout_table")  # the 14 per-broker SOURCE definitions as (BROKER_ID, STD_COL, SRC_COL) rows
STD_COLS = ["CLAIM_REF", "POLICY_REF", "PRODUCT_CD", "AMT_TXT"]
# CODEPAGE Latin1: the pound sign is one byte 0xA3 = CHR(163); read as UTF-8 it becomes 'Â£' and only '£' is stripped (trap 18)
BROKER_ENCODING = spark.conf.get("informatica.bdx_encoding", "ISO-8859-1")


@dp.table(name="bdx_claims_raw", comment="Broker claims bordereaux CSVs as landed, every broker's own columns as STRING")
def bdx_claims_raw():
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "csv").option("header", "true").option("encoding", BROKER_ENCODING)
                 .option("cloudFiles.inferColumnTypes", "false")  # all-string; a new broker header adds columns, never shifts them
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns").option("pathGlobFilter", "CLAIMS_BDX_BRK*_*.csv")
                 .load(LANDING)
                 .withColumn("BROKER_ID", F.regexp_extract(F.col("_metadata.file_path"), r"CLAIMS_BDX_(BRK\d{4})_", 1))
                 .withColumn("FILE_MONTH", F.regexp_extract(F.col("_metadata.file_path"), r"CLAIMS_BDX_BRK\d{4}_(\d{6})", 1)))


@dp.temporary_view()
def bdx_claims_mapped():  # each broker's own header renamed to the common shape by BROKER_ID; a column its layout lacks is NULL
    raw = spark.read.table("bdx_claims_raw")
    cells = [c for c in raw.columns if c not in ("BROKER_ID", "FILE_MONTH")]  # schema only: no Spark action at planning time
    out = raw.withColumn("cells", F.map_from_arrays(F.array(*[F.lit(c) for c in cells]), F.array(*[F.col(c) for c in cells])))
    layouts = spark.read.table(BDX_LAYOUTS)  # one row per (BROKER_ID, STD_COL): the join must not multiply raw rows
    for c in STD_COLS:
        src = layouts.filter(F.col("STD_COL") == c).select("BROKER_ID", F.col("SRC_COL").alias(c + "_src"))
        out = out.join(src, "BROKER_ID", "left").withColumn(c, F.try_element_at("cells", F.col(c + "_src")))
    return out.select("BROKER_ID", "FILE_MONTH", *STD_COLS)


@dp.temporary_view()
def bdx_claims_expected():
    # An unknown broker's file could not load in legacy (no SOURCE for it); the expected-broker table (effective month
    # range) replaces that gate. left_semi / left_anti keep the raw row count intact.
    rows = spark.read.table("bdx_claims_mapped")
    allowed = (spark.read.table(EXPECTED_BROKERS)
                    .select(F.col("BROKER_ID").alias("exp_BROKER_ID"), "effective_from_month",
                            F.coalesce(F.col("effective_to_month"), F.lit("999912")).alias("effective_to_month"))
                    .dropDuplicates())
    match = ((rows.BROKER_ID == allowed.exp_BROKER_ID) & (rows.FILE_MONTH >= allowed.effective_from_month)
             & (rows.FILE_MONTH <= allowed.effective_to_month))
    return (rows.join(allowed, match, "left_semi").withColumn("broker_expected", F.lit(True))
                .unionByName(rows.join(allowed, match, "left_anti").withColumn("broker_expected", F.lit(False))))


@dp.materialized_view(name="ri_claims_bdx_unexpected_broker", comment="Undeclared-broker rows: census evidence, never published")
def ri_claims_bdx_unexpected_broker():
    return spark.read.table("bdx_claims_expected").filter(~F.col("broker_expected")).drop("broker_expected")


@dp.temporary_view()
def lkp_sii_lob():
    # LKP_SII_LOB (reusable, cached; key PRODUCT_CD INFERRED) reads the same drifted table (PET -> 'Other motor', trap 10).
    # A Lookup returns one row per input whatever the cache holds, so one row per PRODUCT_CD is enforced (tie-break INFERRED).
    w = Window.partitionBy("PRODUCT_CD").orderBy(F.col("SII_LOB").asc_nulls_last())
    return spark.read.table(SII_LOB_MAP).select("PRODUCT_CD", "SII_LOB").withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


@dp.materialized_view(name="ri_claims_bdx_std", comment="m_RI_BORDEREAUX_MONTHLY converted; grain = BROKER_ID, CLAIM_REF")
@dp.expect("layout_known", "CLAIM_REF IS NOT NULL AND POLICY_REF IS NOT NULL")  # a broker changed its header: fix the layout row
@dp.expect("amount_parsable", "NOT amt_unparsable")  # legacy silently produced 0: warn, do not drop
# Warn only: no Filter follows EXP_REKEY_POLICY, and case-insensitive REPLACESTR lands 'al/mot/1' as 'ALB-mot-1', hence (?i)
@dp.expect("policy_rekeyed", "POLICY_NO RLIKE '(?i)^ALB-[A-Z]{3}-[0-9]{7}$'")
def ri_claims_bdx_std():
    raw = spark.read.table("bdx_claims_expected").filter(F.col("broker_expected")).drop("broker_expected")
    # EXP_REKEY_POLICY: REPLACECHR(0, REPLACESTR(0, in_POLICY_REF, 'AL/', 'ALB-'), '/', '-'); caseFlag 0 -> (?i)
    policy_no = F.translate(F.regexp_replace(F.col("POLICY_REF"), r"(?i)AL/", "ALB-"), "/", "-")
    # EXP_AMT_CLEAN: TO_DECIMAL(REPLACECHR(0, REPLACECHR(0, x, CHR(163), ''), ',', '')): bad text is 0 in Informatica, NULL from try_cast
    stripped = F.translate(F.col("AMT_TXT"), "\u00a3,", "")
    parsed = F.expr("try_cast(translate(AMT_TXT, '\u00a3,', '') AS DECIMAL(12,2))")
    # Expressions are row-preserving: derive on the same raw row, never re-join on (BROKER_ID, CLAIM_REF), which files repeat
    rows = (raw.withColumn("POLICY_NO", policy_no)
               .withColumn("AMT", F.coalesce(parsed, F.lit(0).cast("decimal(12,2)")))
               .withColumn("amt_unparsable", F.col("AMT_TXT").isNotNull() & (F.trim(stripped) != "") & parsed.isNull()))
    return (rows.join(spark.read.table("lkp_sii_lob"), "PRODUCT_CD", "left")  # connected Lookup: unmatched -> NULL, row kept
                .select("BROKER_ID", "CLAIM_REF", "POLICY_NO", "AMT", "amt_unparsable", "SII_LOB", "PRODUCT_CD"))
