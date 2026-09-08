# Converted unit: RI_CESSIONS / m_RI_BORDEREAUX_MONTHLY  (fixture: source.xml, source.bdx_transfer.ksh)
# Target form: Lakeflow Spark Declarative Pipelines with Auto Loader ingestion of the broker CSVs
# (databricks-pipelines references/auto-loader-python.md `spark.readStream.format("cloudFiles")` inside `@dp.table`;
# SKILL.md "input_file_name() -> _metadata.file_path"; references/expectations-python.md for `@dp.expect*`). Ingestion of the files themselves (SFTP pull) is NOT converted here - see NOTE.md.
#
# Legacy: 14 hand-maintained broker-specific SOURCE definitions (not in the export) -> EXP_REKEY_POLICY,
# EXP_AMT_CLEAN, LKP_SII_LOB -> ceded-claims computation per treaty -> Lloyd's-format outward bordereau.
# Only the three transformations are exported; the rest is INFERRED from the MAPPING DESCRIPTION.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("informatica.bdx_landing_path")      # was /interface/inbound/bordereaux (bdx_transfer $LANDING)
SII_LOB_MAP = spark.conf.get("informatica.sii_lob_map_table")  # REF_DB.SII_LOB_MAP

# Broker files are Latin1/MS1252 (the pound sign is byte 0xA3 = CHR(163) in Latin1; in UTF-8 it is 0xC2 0xA3).
# Reading with the wrong encoding turns '£' into 'Â£' and REPLACECHR(CHR(163)) removes only the second byte
# (SKILL.md row 45, trap 18). The encoding is a per-broker fact recorded in the census, defaulting to the
# repository CODEPAGE (Latin1 in source.xml).
BROKER_ENCODING = spark.conf.get("informatica.bdx_encoding", "ISO-8859-1")


@dp.table(name="bdx_claims_raw", comment="Broker claims bordereaux CSVs as landed; one schema per broker is a decision")
def bdx_claims_raw():
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "csv")
                 .option("header", "true")
                 .option("encoding", BROKER_ENCODING)
                 .option("pathGlobFilter", "CLAIMS_BDX_BRK*_*.csv")
                 .load(LANDING)
                 .withColumn("BROKER_ID", F.regexp_extract(F.col("_metadata.file_path"), r"CLAIMS_BDX_(BRK\d{4})_", 1)))


@dp.temporary_view()
def exp_rekey_policy():
    # REPLACECHR(0, REPLACESTR(0, in_POLICY_REF, 'AL/', 'ALB-'), '/', '-')
    # caseFlag 0 = case-insensitive on both: 'al/mot/1' also becomes 'ALB-mot-1' in Informatica (row 41, trap 17),
    # so the REPLACESTR is (?i); REPLACECHR of a single '/' is case-free and becomes translate() (row 40).
    src = spark.read.table("bdx_claims_raw")
    rekeyed = F.translate(F.regexp_replace(F.col("POLICY_REF"), r"(?i)AL/", "ALB-"), "/", "-")
    return src.select("BROKER_ID", "CLAIM_REF", "POLICY_REF", rekeyed.alias("out_POLICY_NO"))


@dp.temporary_view()
def exp_amt_clean():
    # TO_DECIMAL(REPLACECHR(0, REPLACECHR(0, in_AMT_TXT, CHR(163), ''), ',', ''))  -> decimal(12,2) port
    # TO_DECIMAL of a non-numeric string is 0 in Informatica; try_cast gives NULL (row 13). The legacy behaviour is
    # reproduced (coalesce 0) AND the condition is surfaced as an expectation so the rows are countable. Empty string
    # -> 0 as well. A value like '(1,234.56)' (accounting negative) is 0 in both engines: recorded as a finding.
    src = spark.read.table("bdx_claims_raw")
    stripped = F.translate(F.col("AMT_TXT"), "\u00a3,", "")      # CHR(163) in Latin1 = U+00A3; two REPLACECHR -> one translate
    parsed = F.expr(f"try_cast(translate(AMT_TXT, '\u00a3,', '') AS DECIMAL(12,2))")
    return src.select(
        "BROKER_ID", "CLAIM_REF",                                  # brokers reuse CLAIM_REF: keep the full grain
        F.coalesce(parsed, F.lit(0).cast("decimal(12,2)")).alias("out_AMT"),
        (F.col("AMT_TXT").isNotNull() & (F.trim(stripped) != "") & parsed.isNull()).alias("amt_unparsable"),
    )


@dp.temporary_view()
def lkp_sii_lob():
    # LKP_SII_LOB: reusable, cached; REF_DB.SII_LOB_MAP keyed by PRODUCT_CD (INFERRED from the description).
    # The reference data is DRIFTED from the SAS copy (PET -> 'Other motor' here). The converted lookup reads the
    # SAME table the Informatica session read, so the drift is reproduced and filed as a finding (SKILL.md trap 10).
    return spark.read.table(SII_LOB_MAP).select("PRODUCT_CD", "SII_LOB")


@dp.materialized_view(name="ri_claims_bdx_std", comment="m_RI_BORDEREAUX_MONTHLY converted; grain = BROKER_ID, CLAIM_REF")
@dp.expect("amount_parsable", "NOT amt_unparsable")            # legacy silently produced 0: warn, do not drop
@dp.expect_or_drop("policy_rekeyed", "POLICY_NO RLIKE '^ALB-[A-Z]{3}-[0-9]{7}$'")   # INFERRED POLARIS key shape
def ri_claims_bdx_std():
    # Every Expression view is re-joined on the declared grain (BROKER_ID, CLAIM_REF), never on CLAIM_REF alone:
    # the same CLAIM_REF appears in more than one broker's file. If a broker file itself repeats a CLAIM_REF the
    # legacy row pipeline emitted both rows too; that duplication is caught by Tier 1 counts, not hidden here.
    grain = ["BROKER_ID", "CLAIM_REF"]
    raw = spark.read.table("bdx_claims_raw")
    key = spark.read.table("exp_rekey_policy")
    amt = spark.read.table("exp_amt_clean")
    lob = spark.read.table("lkp_sii_lob")
    return (raw.join(key, grain, "left")
               .join(amt, grain, "left")
               .join(lob, raw.PRODUCT_CD == lob.PRODUCT_CD, "left")
               .select(raw.BROKER_ID, raw.CLAIM_REF,
                       key.out_POLICY_NO.alias("POLICY_NO"),
                       amt.out_AMT.alias("AMT"),
                       amt.amt_unparsable,
                       lob.SII_LOB,                                # NULL when the product is not in the map
                       raw.PRODUCT_CD))
# Ceded-claims per quota-share treaty and the Lloyd's outward bordereau are described but not exported; they are
# separate units in the census (INFERRED) and not part of this example.
