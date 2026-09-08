# Converted unit: MDM_PARTY / m_PARTY_MDM_SYNC + shared mapplet mplt_DQ_PARTY_STANDARDISE
# (fixture: source.xml, source.mapplet.xml, source.par)
# Target form: Lakeflow Spark Declarative Pipelines (databricks-pipelines references/python-basics.md: `from pyspark
# import pipelines as dp`, `@dp.temporary_view`, `@dp.materialized_view`, `spark.read.table` for siblings).
#
# The mapplet is a SHARED OBJECT (consumed by wf_PARTY_MDM_SYNC and wf_POLICY_MASTER_DAILY): it is converted ONCE as a
# pure function module and imported by both units (SKILL.md section 3 "shared objects"). The functions below are that
# module; in the repo they live in a wave-0 file the two pipelines both include as a library.
#
# Sources/targets are not in the export; the .par gives the connections: $DBConnection_SRC=TD_POLICY_ADMIN_PROD (PARTY),
# $DBConnection_APF=TD_CORE_BANKING_PROD (CUSTOMERS), $DBConnection_TGT=ORA_MDM_HUB_PROD (Oracle MDM hub).

from pyspark import pipelines as dp
from pyspark.sql import Column, Window, functions as F

MATCH_CONFIDENCE_FLOOR = float(spark.conf.get("informatica.MATCH_CONFIDENCE_FLOOR"))   # $$MATCH_CONFIDENCE_FLOOR=0.82
SUSPECT_QUEUE_CAP = int(spark.conf.get("informatica.SUSPECT_QUEUE_CAP"))               # $$SUSPECT_QUEUE_CAP=50000
PARTY_TABLE = spark.conf.get("informatica.party_table")
CUSTOMERS_TABLE = spark.conf.get("informatica.apf_customers_table")


# ---- mplt_DQ_PARTY_STANDARDISE (shared, wave 0) -------------------------------------------------------------------
def mplt_name_std(in_name: Column) -> Column:
    # EXP_NAME_STD: INITCAP(REPLACESTR(0, LTRIM(RTRIM(in_NAME)), '  ', ' '))
    # REPLACESTR caseFlag 0 = case-insensitive; irrelevant for a space pattern, so a plain replace is exact (row 41).
    # Informatica REPLACESTR replaces every occurrence, but '    ' (4 spaces) -> '  ' (2 spaces), NOT ' ': a single
    # pass, same as Spark replace(). INITCAP word boundaries differ (row 38): Informatica treats any non-alphanumeric as a
    # boundary ("o'brien" -> "O'Brien", "smith-jones" -> "Smith-Jones"); Spark initcap splits on whitespace only.
    # Splitting on alphanumeric/non-alphanumeric boundaries (lookarounds, Java regex) and upper-casing each token's first char
    # reproduces the legacy boundary set without a UDF.
    squeezed = F.lower(F.regexp_replace(F.trim(in_name), "  ", " "))
    tokens = F.split(squeezed, r"(?<=[^a-z0-9])(?=[a-z0-9])|(?<=[a-z0-9])(?=[^a-z0-9])")
    return F.array_join(F.transform(tokens, lambda t: F.concat(F.upper(F.substring(t, 1, 1)), F.substring(t, 2, 100))), "")


def mplt_phone_std(in_phone: Column) -> Column:
    # EXP_PHONE_STD: IIF(SUBSTR(REPLACECHR(0,in_PHONE,' ',''),1,3)='+44', '0'||SUBSTR(...,4), REPLACECHR(...))
    # REPLACECHR with an empty replacement deletes the characters (row 40); '||' would skip a NULL operand but the
    # left operand is the literal '0', so concat semantics are identical here (row 39).
    no_spaces = F.regexp_replace(in_phone, " ", "")
    return F.when(F.substring(no_spaces, 1, 3) == "+44",
                  F.concat(F.lit("0"), F.substring(no_spaces, 4, 100))).otherwise(no_spaces)
# ------------------------------------------------------------------------------------------------------------------


@dp.temporary_view()
def exp_email_dq():
    # LOWER(LTRIM(RTRIM(x))) -> lower(trim(x)); REG_MATCH -> rlike (rows 35, 37, 42). The pattern is Perl-style and
    # contains no POSIX classes, so it is byte-for-byte portable to Java regex.
    party = spark.read.table(PARTY_TABLE)
    email_std = F.lower(F.trim(F.col("EMAIL")))
    status = F.when(email_std.rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"), "VALID").otherwise("INVALID")
    return party.select("PARTY_ID", email_std.alias("out_EMAIL_STD"), status.alias("out_EMAIL_DQ_STATUS"))


@dp.temporary_view()
def exp_nino_mask():
    # IIF(ISNULL(in_NINO) OR LENGTH(in_NINO) < 9, NULL, SUBSTR(in_NINO,1,2) || '*****' || SUBSTR(in_NINO,8,2))
    # ISNULL -> IS NULL (row 4); LENGTH counts trailing spaces on a CHAR-origin port (row 36): PARTY.NINO is
    # Teradata CHAR(9) so a blank NINO arrives as 9 spaces, LENGTH = 9, and the legacy output is '  *****  '.
    # rstrip_spaces in canonicalization.json turns that into '' on both sides; the converted code must NOT trim
    # before LENGTH or the row becomes NULL instead (see NOTE.md).
    party = spark.read.table(PARTY_TABLE)
    nino = F.col("NINO")
    masked = F.when(nino.isNull() | (F.length(nino) < 9), None).otherwise(
        F.concat_ws("", F.substring(nino, 1, 2), F.lit("*****"), F.substring(nino, 8, 2)))  # concat_ws skips NULLs like ||
    return party.select("PARTY_ID", masked.alias("out_NINO_MASKED"))


@dp.temporary_view()
def exp_xmatch_apf():
    # SOUNDEX(UPPER(x)) -> soundex(upper(x)) (row 47); TO_DATE(x,'DD/MM/YYYY') -> to_timestamp(x,'dd/MM/yyyy') (row 9),
    # the date/time port (precision 29 scale 9) lands as TIMESTAMP_NTZ; unparsable text is a row error, so try_ +
    # expectation (row 61).
    party = spark.read.table(PARTY_TABLE)
    return party.select(
        "PARTY_ID",
        F.soundex(F.upper(F.col("LAST_NAME"))).alias("out_SURNAME_SOUNDEX"),
        F.expr("try_to_timestamp(BIRTH_DT_TXT, 'dd/MM/yyyy')").cast("timestamp_ntz").alias("out_BIRTH_DT"),
        F.col("BIRTH_DT_TXT").isNotNull().alias("birth_dt_present"),
    )


@dp.materialized_view(name="mdm_party_golden", comment="m_PARTY_MDM_SYNC converted; grain = PARTY_ID")
@dp.expect_or_drop("birth_dt_parses", "NOT (birth_dt_present AND BIRTH_DT IS NULL)")   # TO_DATE row error -> reject
def mdm_party_golden():
    party = spark.read.table(PARTY_TABLE)
    email = spark.read.table("exp_email_dq")
    nino = spark.read.table("exp_nino_mask")
    xm = spark.read.table("exp_xmatch_apf")
    cust = spark.read.table(CUSTOMERS_TABLE).select(
        F.col("CUSTOMER_ID").alias("LEGACY_CUSTOMER_ID"), "NINO",
        F.soundex(F.upper(F.col("LAST_NAME"))).alias("cust_soundex"), F.col("DOB").alias("cust_dob"),
        F.col("LAST_UPDATED_TS").alias("cust_updated_ts"))                       # INFERRED column name

    # Survivorship and the NINO-exact / NAME_DOB-fuzzy match order are described only in the MAPPING DESCRIPTION
    # (no Lookup/Joiner transformations are in the export): everything from here to the target is INFERRED.
    # Both matches can hit several APF customers per PARTY_ID (shared NINO, common surname + DOB). A connected
    # Lookup returns ONE row (row 74 'Use First Value' / 'Use Last Value'), so each match is reduced to one row per
    # PARTY_ID before it is joined back; "most-recent-update wins" is the documented survivorship and
    # LEGACY_CUSTOMER_ID is the deterministic tie-breaker (a cache-order dependency in the legacy engine, trap 9).
    survivor = Window.partitionBy("PARTY_ID").orderBy(F.col("cust_updated_ts").desc_nulls_last(),
                                                     F.col("LEGACY_CUSTOMER_ID").asc())

    def one_per_party(matches, out_col):
        return (matches.withColumn("rn", F.row_number().over(survivor))
                       .filter(F.col("rn") == 1)
                       .select("PARTY_ID", F.col("LEGACY_CUSTOMER_ID").alias(out_col)))

    nino_match = one_per_party(
        party.join(cust, party.NINO == cust.NINO, "inner").select("PARTY_ID", "LEGACY_CUSTOMER_ID", "cust_updated_ts"),
        "nino_customer_id")
    fuzzy = one_per_party(
        xm.join(cust, (xm.out_SURNAME_SOUNDEX == cust.cust_soundex) &
                      (F.to_date(xm.out_BIRTH_DT) == cust.cust_dob), "inner")
          .select("PARTY_ID", "LEGACY_CUSTOMER_ID", "cust_updated_ts"),
        "fuzzy_customer_id")

    return (
        party.join(email, "PARTY_ID", "left").join(nino, "PARTY_ID", "left").join(xm, "PARTY_ID", "left")
             .join(nino_match, "PARTY_ID", "left").join(fuzzy, "PARTY_ID", "left")
             .select(
                 "PARTY_ID",
                 mplt_name_std(F.col("FIRST_NAME")).alias("FIRST_NAME_STD"),      # mapplet, shared
                 mplt_name_std(F.col("LAST_NAME")).alias("LAST_NAME_STD"),
                 mplt_phone_std(F.col("PHONE")).alias("PHONE_STD"),                # mapplet, shared
                 # Oracle target: '' is stored as NULL. Reproduce at the write boundary so the converted Delta table
                 # matches the legacy Oracle hub (trap 5); empty_string_is_null stays SET_AT_STOP_A in the harness.
                 F.nullif(F.col("out_EMAIL_STD"), F.lit("")).alias("EMAIL_STD"),
                 F.col("out_EMAIL_DQ_STATUS").alias("EMAIL_DQ_STATUS"),
                 F.col("out_NINO_MASKED").alias("NINO_MASKED"),
                 F.col("out_SURNAME_SOUNDEX").alias("SURNAME_SOUNDEX"),
                 F.col("out_BIRTH_DT").alias("BIRTH_DT"),
                 # match-order precedence: NINO exact first, NAME_DOB fuzzy only when no NINO match
                 F.coalesce(F.col("nino_customer_id"), F.col("fuzzy_customer_id")).alias("LEGACY_CUSTOMER_ID"),
                 F.col("birth_dt_present"),
             )
    )
