# MDM_PARTY / m_PARTY_MDM_SYNC + shared mapplet mplt_DQ_PARTY_STANDARDISE as a Lakeflow Spark Declarative Pipeline
# (databricks-pipelines references/python-basics.md). The mapplet is used by two workflows, so it is converted once
# as the pure functions below (wave-0 library). Sources/targets are not in the export; .par: $DBConnection_SRC=
# TD_POLICY_ADMIN_PROD, $DBConnection_APF=TD_CORE_BANKING_PROD, $DBConnection_TGT=ORA_MDM_HUB_PROD.
# ruff: noqa: F821  (`spark` is pre-imported in pipeline files: databricks-pipelines references/python-basics.md)
from pyspark import pipelines as dp
from pyspark.sql import Column, Window
from pyspark.sql import functions as F

MATCH_CONFIDENCE_FLOOR = float(spark.conf.get("informatica.MATCH_CONFIDENCE_FLOOR"))  # $$MATCH_CONFIDENCE_FLOOR=0.82
PARTY_TABLE = spark.conf.get("informatica.party_table")
CUSTOMERS_TABLE = spark.conf.get("informatica.apf_customers_table")
REJECT = "NOT (birth_dt_present AND BIRTH_DT IS NULL)"  # TO_DATE row error -> reject file in legacy


def mplt_name_std(in_name: Column) -> Column:
    # INITCAP(REPLACESTR(0, LTRIM(RTRIM(in_NAME)), '  ', ' ')): single-pass replace like Spark; Informatica INITCAP
    # starts a word at ANY non-alphanumeric ("o'brien" -> "O'Brien"), Spark initcap only at whitespace.
    squeezed = F.lower(F.regexp_replace(F.trim(in_name), "  ", " "))
    tokens = F.split(squeezed, r"(?<=[^a-z0-9])(?=[a-z0-9])|(?<=[a-z0-9])(?=[^a-z0-9])")
    return F.array_join(F.transform(tokens, lambda t: F.concat(F.upper(F.substring(t, 1, 1)), F.substring(t, 2, 100))), "")


def mplt_phone_std(in_phone: Column) -> Column:
    # IIF(SUBSTR(REPLACECHR(0,in_PHONE,' ',''),1,3)='+44', '0'||SUBSTR(...,4), ...): REPLACECHR with '' deletes
    no_spaces = F.regexp_replace(in_phone, " ", "")
    return F.when(F.substring(no_spaces, 1, 3) == "+44", F.concat(F.lit("0"), F.substring(no_spaces, 4, 100))).otherwise(no_spaces)


def infa_soundex(col: Column) -> Column:
    # Informatica SOUNDEX is NULL when no English letter is present; Spark soundex() returns the input unchanged when
    # the first char is not a letter, so '-' and '???' would equal-match. Leading non-letters are dropped (INFERRED).
    return F.when(col.rlike(r"[A-Za-z]"), F.soundex(F.regexp_replace(F.upper(col), r"^[^A-Z]+", "")))


@dp.temporary_view()
def mdm_party_rows():  # every derived row, before the legacy reject condition splits target from quarantine
    party = spark.read.table(PARTY_TABLE)
    # EXP_NINO_MASK: NINO is Teradata CHAR(9), so a blank arrives as 9 spaces and LENGTH = 9 -> '  *****  ' in legacy;
    # do not trim before LENGTH (rstrip_spaces canon reconciles it). concat_ws skips NULLs exactly like ||.
    nino = F.col("NINO")
    nino_masked = F.when(nino.isNull() | (F.length(nino) < 9), None).otherwise(
        F.concat_ws("", F.substring(nino, 1, 2), F.lit("*****"), F.substring(nino, 8, 2)))
    # EXP_XMATCH_APF: TO_DATE(x,'DD/MM/YYYY') -> try_to_timestamp(x,'dd/MM/yyyy'); date/time (29,9) -> TIMESTAMP_NTZ
    birth_dt = F.expr("try_to_timestamp(BIRTH_DT_TXT, 'dd/MM/yyyy')").cast("timestamp_ntz")
    xm = party.select("PARTY_ID", "NINO", infa_soundex(F.col("LAST_NAME")).alias("SURNAME_SOUNDEX"), birth_dt.alias("BIRTH_DT"))

    cust = spark.read.table(CUSTOMERS_TABLE).select(
        F.col("CUSTOMER_ID").alias("LEGACY_CUSTOMER_ID"), F.col("NINO").alias("cust_nino"), F.col("EMAIL").alias("cust_email"),
        infa_soundex(F.col("LAST_NAME")).alias("cust_soundex"), F.col("DOB").alias("cust_dob"),
        F.col("LAST_UPDATED_TS").alias("cust_updated_ts"))  # EMAIL / LAST_UPDATED_TS column names INFERRED
    # Match order (NINO exact, then NAME_DOB fuzzy) and "most-recent-update wins" survivorship come from the MAPPING
    # DESCRIPTION only. A connected Lookup returns one row per input (Use First Value), so each match is reduced to
    # one row per PARTY_ID before the join back; LEGACY_CUSTOMER_ID breaks ties deterministically (trap 9).
    survivor = Window.partitionBy("PARTY_ID").orderBy(F.col("cust_updated_ts").desc_nulls_last(), F.col("LEGACY_CUSTOMER_ID"))

    def one_per_party(matches, prefix):
        return (matches.withColumn("rn", F.row_number().over(survivor)).filter("rn = 1")
                       .select("PARTY_ID", F.col("LEGACY_CUSTOMER_ID").alias(prefix + "_customer_id"), F.col("cust_email").alias(prefix + "_email")))

    nino_match = one_per_party(xm.join(cust, xm.NINO == cust.cust_nino), "nino")
    fuzzy = one_per_party(xm.join(cust, (xm.SURNAME_SOUNDEX == cust.cust_soundex) & (F.to_date(xm.BIRTH_DT) == cust.cust_dob)), "fuzzy")

    # EXP_EMAIL_DQ: LOWER(LTRIM(RTRIM(x))) -> lower(trim(x)); REG_MATCH -> rlike. The DESCRIPTION's survivorship
    # exception ("email: longest-string wins") is applied over the party email and the NINO-first / fuzzy-fallback
    # customer's email; equal lengths keep the party value (INFERRED). DQ runs on the surviving address.
    party_email = F.lower(F.trim(F.col("EMAIL")))
    cust_email = F.lower(F.trim(F.when(F.col("nino_customer_id").isNotNull(), F.col("nino_email")).otherwise(F.col("fuzzy_email"))))
    email_std = F.when(F.length(cust_email) > F.coalesce(F.length(party_email), F.lit(0)), cust_email).otherwise(party_email)
    email_dq = F.when(email_std.rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"), "VALID").otherwise("INVALID")
    return (party.join(xm.drop("NINO"), "PARTY_ID", "left").join(nino_match, "PARTY_ID", "left").join(fuzzy, "PARTY_ID", "left")
                 .select("PARTY_ID", mplt_name_std(F.col("FIRST_NAME")).alias("FIRST_NAME_STD"),
                         mplt_name_std(F.col("LAST_NAME")).alias("LAST_NAME_STD"), mplt_phone_std(F.col("PHONE")).alias("PHONE_STD"),
                         F.nullif(email_std, F.lit("")).alias("EMAIL_STD"),  # Oracle target stores '' as NULL (trap 5)
                         email_dq.alias("EMAIL_DQ_STATUS"), nino_masked.alias("NINO_MASKED"), "SURNAME_SOUNDEX", "BIRTH_DT",
                         F.coalesce(F.col("nino_customer_id"), F.col("fuzzy_customer_id")).alias("LEGACY_CUSTOMER_ID"),
                         F.col("BIRTH_DT_TXT").isNotNull().alias("birth_dt_present"), "BIRTH_DT_TXT"))


@dp.materialized_view(name="mdm_party_golden", comment="m_PARTY_MDM_SYNC converted; grain = PARTY_ID")
@dp.expect_or_drop("birth_dt_parses", REJECT)
def mdm_party_golden():
    return spark.read.table("mdm_party_rows").drop("BIRTH_DT_TXT")


@dp.materialized_view(name="mdm_party_rejects", comment="legacy session reject file: rows failing birth_dt_parses, kept for correction and replay")
def mdm_party_rejects():
    return spark.read.table("mdm_party_rows").filter(f"NOT ({REJECT})").withColumn("REJECT_REASON", F.lit("birth_dt_parses"))
