# RI_CESSIONS / m_RI_BORDEREAUX_MONTHLY as a Lakeflow Spark Declarative Pipeline with Auto Loader ingestion
# (databricks-pipelines references/auto-loader-python.md, references/expectations-python.md). The pre-step SFTP pull
# (bdx_transfer.ksh: mget CLAIMS_BDX_${BRK}_*.csv per broker, then mailx) is an ingestion decision, not converted here.
# Only EXP_REKEY_POLICY, EXP_AMT_CLEAN and LKP_SII_LOB are exported; the 14 per-broker SOURCE definitions, the
# ceded-claims computation and the Lloyd's outward bordereau are INFERRED from the MAPPING DESCRIPTION.
# ruff: noqa: F821  (`spark` is pre-imported in pipeline files: databricks-pipelines references/python-basics.md)
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

LANDING = spark.conf.get("informatica.bdx_landing_path")  # was /interface/inbound/bordereaux
SII_LOB_MAP = spark.conf.get("informatica.sii_lob_map_table")  # REF_DB.SII_LOB_MAP
EXPECTED_BROKERS = spark.conf.get("informatica.expected_brokers_table")
# Repository CODEPAGE is Latin1: the pound sign is one byte 0xA3 = CHR(163). Read as UTF-8 it becomes 'Â£' and the
# REPLACECHR(CHR(163)) equivalent strips only the second byte (trap 18).
BROKER_ENCODING = spark.conf.get("informatica.bdx_encoding", "ISO-8859-1")


@dp.table(name="bdx_claims_raw", comment="Broker claims bordereaux CSVs as landed")
def bdx_claims_raw():
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "csv").option("header", "true")
                 .option("encoding", BROKER_ENCODING)
                 .option("pathGlobFilter", "CLAIMS_BDX_BRK*_*.csv")
                 .load(LANDING)
                 .withColumn("BROKER_ID", F.regexp_extract(F.col("_metadata.file_path"), r"CLAIMS_BDX_(BRK\d{4})_", 1))
                 .withColumn("FILE_MONTH", F.regexp_extract(F.col("_metadata.file_path"), r"CLAIMS_BDX_BRK\d{4}_(\d{6})", 1)))


@dp.temporary_view()
def bdx_claims_expected():
    # Legacy had one hand-built SOURCE per broker, so an unknown broker's file could not load. The expected-broker
    # table (effective month range) replaces that; left_semi / left_anti keep the raw row count intact.
    raw = spark.read.table("bdx_claims_raw")
    allowed = (spark.read.table(EXPECTED_BROKERS)
                    .select(F.col("BROKER_ID").alias("exp_BROKER_ID"), "effective_from_month",
                            F.coalesce(F.col("effective_to_month"), F.lit("999912")).alias("effective_to_month"))
                    .dropDuplicates())
    match = ((raw.BROKER_ID == allowed.exp_BROKER_ID) & (raw.FILE_MONTH >= allowed.effective_from_month)
             & (raw.FILE_MONTH <= allowed.effective_to_month))
    return (raw.join(allowed, match, "left_semi").withColumn("broker_expected", F.lit(True))
               .unionByName(raw.join(allowed, match, "left_anti").withColumn("broker_expected", F.lit(False))))


@dp.materialized_view(name="ri_claims_bdx_unexpected_broker", comment="Undeclared-broker rows: census evidence, never published")
def ri_claims_bdx_unexpected_broker():
    return spark.read.table("bdx_claims_expected").filter(~F.col("broker_expected")).drop("broker_expected")


@dp.temporary_view()
def lkp_sii_lob():
    # LKP_SII_LOB (reusable, cached; keyed by PRODUCT_CD, INFERRED). Reads the same drifted table the session read
    # (PET -> 'Other motor'): the drift is reproduced and filed as a finding (trap 10). A Lookup returns exactly one
    # row per input whatever the cache holds, so one row per PRODUCT_CD is enforced; the tie-break is INFERRED.
    w = Window.partitionBy("PRODUCT_CD").orderBy(F.col("SII_LOB").asc_nulls_last())
    return (spark.read.table(SII_LOB_MAP).select("PRODUCT_CD", "SII_LOB")
                 .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))


@dp.materialized_view(name="ri_claims_bdx_std", comment="m_RI_BORDEREAUX_MONTHLY converted; grain = BROKER_ID, CLAIM_REF")
@dp.expect("amount_parsable", "NOT amt_unparsable")  # legacy silently produced 0: warn, do not drop
# Warn only: no Filter follows EXP_REKEY_POLICY in the legacy mapping, and the case-insensitive REPLACESTR means
# 'al/mot/0000001' legitimately lands as 'ALB-mot-0000001', so the check itself must be (?i).
@dp.expect("policy_rekeyed", "POLICY_NO RLIKE '(?i)^ALB-[A-Z]{3}-[0-9]{7}$'")
def ri_claims_bdx_std():
    raw = spark.read.table("bdx_claims_expected").filter(F.col("broker_expected")).drop("broker_expected")
    # EXP_REKEY_POLICY: REPLACECHR(0, REPLACESTR(0, in_POLICY_REF, 'AL/', 'ALB-'), '/', '-'); caseFlag 0 -> (?i)
    policy_no = F.translate(F.regexp_replace(F.col("POLICY_REF"), r"(?i)AL/", "ALB-"), "/", "-")
    # EXP_AMT_CLEAN: TO_DECIMAL(REPLACECHR(0, REPLACECHR(0, in_AMT_TXT, CHR(163), ''), ',', '')) -> decimal(12,2).
    # TO_DECIMAL of non-numeric/empty text is 0 in Informatica, NULL from try_cast: reproduce the 0 and count it.
    stripped = F.translate(F.col("AMT_TXT"), "\u00a3,", "")
    parsed = F.expr("try_cast(translate(AMT_TXT, '\u00a3,', '') AS DECIMAL(12,2))")
    # Expressions are row-preserving: derive on the same raw row, never in separate views re-joined on
    # (BROKER_ID, CLAIM_REF), which a broker file may repeat (n rows would become n*n).
    rows = (raw.withColumn("POLICY_NO", policy_no)
               .withColumn("AMT", F.coalesce(parsed, F.lit(0).cast("decimal(12,2)")))
               .withColumn("amt_unparsable", F.col("AMT_TXT").isNotNull() & (F.trim(stripped) != "") & parsed.isNull()))
    lob = spark.read.table("lkp_sii_lob")
    return (rows.join(lob, "PRODUCT_CD", "left")  # connected Lookup: unmatched -> NULL SII_LOB, row kept
                .select("BROKER_ID", "CLAIM_REF", "POLICY_NO", "AMT", "amt_unparsable", "SII_LOB", "PRODUCT_CD"))
