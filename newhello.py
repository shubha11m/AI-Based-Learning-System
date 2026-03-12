# src/main/resources/Hello.py
import sys
import traceback
import boto3

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

BUCKET = "nontrauma-claim-prod"
PREFIX = "326/"
OUTPUT_PATH = "s3://nontrauma-analytics-prod"
BATCH_SIZE = 20000

sc = None
spark = None


def epoch_ms_to_ts(col_):
    return (
        F.when(
            col_.cast("string").rlike(r"^[0-9]{13}$"),
            F.to_timestamp(F.from_unixtime(col_.cast("double") / F.lit(1000.0))),
        )
        .otherwise(F.to_timestamp(col_))
        .cast("timestamp")
    )


def _lower_map(cols):
    return {c.lower(): c for c in cols}


def pick_col_case_insensitive(df, logical_name: str):
    """
    Returns a Column for the first matching top-level column by case-insensitive name.
    If not found, returns NULL.
    """
    m = _lower_map(df.columns)
    actual = m.get(logical_name.lower())
    return F.col(f"`{actual}`") if actual else F.lit(None)


def safe_col_expr(expr, data_type, alias_name):
    return expr.cast(data_type).alias(alias_name)


def safe_ts_expr(expr, alias_name):
    return epoch_ms_to_ts(expr).alias(alias_name)


def normalize_array_struct_fields_to_lower_keep_first(df, array_col_name: str):
    """
    Fixes ambiguity inside array<struct> when a struct contains fields that differ only by case
    (e.g., AdjustmentFlag vs adjustmentFlag).

    Glue/Spark 2.x/3.x compatibility note:
      - Some Glue runtimes don't expose `pyspark.sql.functions.struct_to_array`.

    Strategy (robust across Glue versions):
      - Convert each struct element to JSON (`to_json`) which collapses duplicate keys.
      - Parse it back using `from_json` with a schema whose field names are lowercase.
      - When both `X` and `x` exist, JSON parsing will keep one value (typically the last).

    This avoids Spark's ambiguous nested-field resolution and produces predictable lowercase
    nested fields used later (e.g., `line.adjustmentflag`).
    """
    if array_col_name not in df.columns:
        return df

    dt = df.schema[array_col_name].dataType
    if not isinstance(dt, T.ArrayType) or not isinstance(dt.elementType, T.StructType):
        return df

    # Build a lowercase schema for the struct element (dedupe by lowercase name)
    seen = set()
    lowered_fields = []
    for f in dt.elementType.fields:
        lo = f.name.lower()
        if lo in seen:
            continue
        seen.add(lo)
        lowered_fields.append(T.StructField(lo, f.dataType, True))

    lowered_schema = T.ArrayType(T.StructType(lowered_fields), True)

    return df.withColumn(
        array_col_name,
        F.from_json(
            F.to_json(F.col(array_col_name)),
            lowered_schema,
        ),
    )


def coalesce_parent_claimkey_into_array(df, array_col_name: str, parent_claimkey_col: str):
    """
    After normalization, nested key is `claimkey` in each element (lowercase).
    If missing/null, fill from parent claimkey.

    IMPORTANT:
      - Build nested-field expressions INSIDE the transform lambda.
      - Use x.getField("...") to reference fields of the lambda variable.
        (F.col("x.someField") is NOT valid here and gets resolved against the
        outer DataFrame, leading to errors like: Column 'x.adjustmentflag' does not exist.)
    """
    if array_col_name not in df.columns or parent_claimkey_col not in df.columns:
        return df

    dt = df.schema[array_col_name].dataType
    if not isinstance(dt, T.ArrayType) or not isinstance(dt.elementType, T.StructType):
        return df

    nested_fields = [f.name for f in dt.elementType.fields]
    if "claimkey" not in nested_fields:
        return df

    def rebuild_one(x):
        rebuilt = []
        for f in dt.elementType.fields:
            fname = f.name
            if fname == "claimkey":
                rebuilt.append(
                    F.coalesce(
                        x.getField("claimkey").cast("long"),
                        F.col(parent_claimkey_col).cast("long"),
                    ).alias("claimkey")
                )
            else:
                rebuilt.append(x.getField(fname).alias(fname))
        return F.struct(*rebuilt)

    return df.withColumn(
        array_col_name,
        F.transform(F.col(array_col_name), rebuild_one),
    )


def stable_claim_load_ts(df):
    if "claimLoadDateTime" not in df.columns:
        return (
            df.withColumn("claimLoadDateTime", F.lit(None).cast("timestamp"))
            .withColumn("loadyear", F.lit(None).cast("int"))
        )
    df2 = df.withColumn("claimLoadDateTime", epoch_ms_to_ts(F.col("claimLoadDateTime")))
    return df2.withColumn("loadyear", F.year(F.col("claimLoadDateTime")).cast("int"))


def with_canonical_top_level_keys(df):
    """
    Some files can contain duplicate root columns that differ only by case, e.g. `claimKey` and
    `ClaimKey`. Referencing `claimKey` then becomes ambiguous.

    This function:
      - picks one value case-insensitively
      - stores it into canonical columns: `claimkey_root`, `payerkey_root`
      - drops ALL case-variants of claimKey/payerKey from the root so ambiguity is gone
    """

    # Canonical values (case-insensitive pick)
    df = df.withColumn(
        "claimkey_root",
        pick_col_case_insensitive(df, "claimKey").cast("long"),
    )

    payer = pick_col_case_insensitive(df, "payerKey")
    df = df.withColumn(
        "payerkey_root",
        F.when(F.trim(payer.cast("string")) == "", None).otherwise(payer).cast("long"),
    )

    # Drop any root columns that are claimKey/payerKey in any casing
    lower_to_actuals = {}
    for c in df.columns:
        lower_to_actuals.setdefault(c.lower(), []).append(c)

    for c in lower_to_actuals.get("claimkey", []):
        df = df.drop(c)
    for c in lower_to_actuals.get("payerkey", []):
        df = df.drop(c)

    return df


try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
 
    # ---------------- PERFORMANCE SETTINGS ----------------
 
    spark.conf.set("spark.sql.caseSensitive", "false")
 
    spark.conf.set("spark.sql.shuffle.partitions", "800")
    spark.conf.set("spark.default.parallelism", "800")
 
    spark.conf.set("spark.sql.files.maxPartitionBytes", "512MB")
    spark.conf.set("spark.sql.files.openCostInBytes", "128MB")
 
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
 
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    # ---------------- FASTER S3 ----------------
 
    hconf = sc._jsc.hadoopConfiguration()
 
    hconf.set("fs.s3a.connection.maximum", "500")
    hconf.set("fs.s3a.threads.max", "500")
    hconf.set("fs.s3a.fast.upload", "true")
 
    s3 = boto3.client("s3")
 
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)

    file_batch = []

    def process_batch(batch_paths):
        # Read with case sensitivity ON so Spark preserves both ClaimKey and claimKey during parsing.
        prior_case = spark.conf.get("spark.sql.caseSensitive", "false")
        spark.conf.set("spark.sql.caseSensitive", "true")
        try:
            df0 = spark.read.json(batch_paths)
        finally:
            spark.conf.set("spark.sql.caseSensitive", prior_case)

        # Normalize nested arrays to eliminate ambiguous fields inside structs.
        df0 = normalize_array_struct_fields_to_lower_keep_first(df0, "claimLinesList")
        df0 = normalize_array_struct_fields_to_lower_keep_first(df0, "claimDiagnosisList")

        # Canonicalize top-level keys (AND drop ambiguous duplicates) + timestamps.
        df0 = with_canonical_top_level_keys(df0)
        df0 = stable_claim_load_ts(df0)

        # Use only canonical key columns downstream.
        df0 = df0.withColumnRenamed("claimkey_root", "claimKey")
        df0 = df0.withColumnRenamed("payerkey_root", "payerkey")

        # Backfill nested claimkey from parent claimKey when null.
        df0 = coalesce_parent_claimkey_into_array(df0, "claimLinesList", "claimKey")
        df0 = coalesce_parent_claimkey_into_array(df0, "claimDiagnosisList", "claimKey")

        # ---------------- CLAIMS ----------------
        claims_df = (
            df0.select(
                safe_col_expr(F.col("claimKey"), "long", "claimKey"),
                safe_col_expr(F.col("payerkey"), "long", "payerkey"),
                safe_col_expr(pick_col_case_insensitive(df0, "employerGroupKey"), "long", "employerGroupKey"),
                safe_col_expr(pick_col_case_insensitive(df0, "memberKey"), "long", "memberKey"),
                safe_col_expr(pick_col_case_insensitive(df0, "claimNumber"), "string", "claimNumber"),
                safe_col_expr(pick_col_case_insensitive(df0, "claimSource"), "string", "claimSource"),
                safe_col_expr(pick_col_case_insensitive(df0, "claimStatus"), "string", "claimStatus"),
                safe_col_expr(pick_col_case_insensitive(df0, "claimType"), "string", "claimType"),
                safe_col_expr(pick_col_case_insensitive(df0, "formType"), "string", "formType"),
                safe_col_expr(pick_col_case_insensitive(df0, "checkNumber"), "string", "checkNumber"),
                safe_col_expr(pick_col_case_insensitive(df0, "clientDataFeedCode"), "string", "clientDataFeedCode"),
                safe_col_expr(pick_col_case_insensitive(df0, "branchGroupID"), "string", "branchGroupID"),
                safe_col_expr(pick_col_case_insensitive(df0, "sourceSystemID"), "string", "sourceSystemID"),
                safe_col_expr(pick_col_case_insensitive(df0, "trackingInfo"), "string", "trackingInfo"),
                safe_col_expr(pick_col_case_insensitive(df0, "claimClientFields"), "string", "claimClientFields"),
                safe_col_expr(pick_col_case_insensitive(df0, "adjustmentIndicator"), "string", "adjustmentIndicator"),
                safe_col_expr(pick_col_case_insensitive(df0, "assignmentFlag"), "string", "assignmentFlag"),
                safe_col_expr(pick_col_case_insensitive(df0, "accidentFlag"), "string", "accidentFlag"),
                safe_col_expr(pick_col_case_insensitive(df0, "isCapitatedClaim"), "string", "isCapitatedClaim"),
                safe_col_expr(pick_col_case_insensitive(df0, "isMedicare"), "string", "isMedicare"),
                safe_col_expr(pick_col_case_insensitive(df0, "placeofService"), "string", "placeofService"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderTIN"), "string", "billingProviderTIN"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderNPI"), "string", "billingProviderNPI"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderID"), "string", "billingProviderID"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderName"), "string", "billingProviderName"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderAddress1"), "string", "billingProviderAddress1"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderAddress2"), "string", "billingProviderAddress2"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderCity"), "string", "billingProviderCity"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderState"), "string", "billingProviderState"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderZip"), "string", "billingProviderZip"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderContactName"), "string", "billingProviderContactName"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderContactPhone"), "string", "billingProviderContactPhone"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderEmail"), "string", "billingProviderEmail"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderFax"), "string", "billingProviderFax"),
                safe_col_expr(pick_col_case_insensitive(df0, "billingProviderPhone"), "string", "billingProviderPhone"),
                safe_col_expr(pick_col_case_insensitive(df0, "batchRunSequence"), "long", "batchRunSequence"),
                safe_col_expr(pick_col_case_insensitive(df0, "inboundBatchMasterKey"), "long", "inboundBatchMasterKey"),
                safe_col_expr(pick_col_case_insensitive(df0, "stageClaimKey"), "long", "stageClaimKey"),
                safe_col_expr(pick_col_case_insensitive(df0, "encounterClaimKey"), "long", "encounterClaimKey"),
                safe_col_expr(pick_col_case_insensitive(df0, "totalBilledAmount"), "decimal(18,2)", "totalBilledAmount"),
                safe_col_expr(pick_col_case_insensitive(df0, "totalClientPaidAmount"), "decimal(18,2)", "totalClientPaidAmount"),
                safe_col_expr(pick_col_case_insensitive(df0, "totalMemberPaidAmount"), "decimal(18,2)", "totalMemberPaidAmount"),
                safe_ts_expr(pick_col_case_insensitive(df0, "claimLoadDateTime"), "claimLoadDateTime"),
                safe_ts_expr(pick_col_case_insensitive(df0, "createdAt"), "createdAt"),
                safe_col_expr(pick_col_case_insensitive(df0, "createdBy"), "long", "createdBy"),
                safe_ts_expr(pick_col_case_insensitive(df0, "updatedAt"), "updatedAt"),
                safe_col_expr(pick_col_case_insensitive(df0, "updatedBy"), "long", "updatedBy"),
                safe_ts_expr(pick_col_case_insensitive(df0, "serviceBeginDate"), "serviceBeginDate"),
                safe_ts_expr(pick_col_case_insensitive(df0, "serviceThruDate"), "serviceThruDate"),
                safe_ts_expr(pick_col_case_insensitive(df0, "datePaid"), "datePaid"),
                safe_ts_expr(pick_col_case_insensitive(df0, "claimReceivedDate"), "claimReceivedDate"),
                safe_ts_expr(pick_col_case_insensitive(df0, "claimTransferredDateTime"), "claimTransferredDateTime"),
                safe_col_expr(F.col("loadyear"), "int", "loadyear"),
            )
            .filter(F.col("payerkey").isNotNull() & F.col("loadyear").isNotNull())
        )

        claims_df.repartition(300, "payerKey", "loadyear") \
            .write.mode("append") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", 500000) \
            .partitionBy("payerKey", "loadyear") \
            .parquet(f"{OUTPUT_PATH}/claims/")

        # ---------------- CLAIM LINES ----------------
        lines_src = df0.select(
            F.col("payerkey").cast("long").alias("payerkey"),
            F.col("claimLoadDateTime").cast("timestamp").alias("claimLoadDateTime"),
            F.col("loadyear").cast("int").alias("loadyear"),
            F.col("claimKey").cast("long").alias("parentClaimKey"),
            F.explode_outer("claimLinesList").alias("line"),
        )

        lines_flat = (
            lines_src.select(
                F.col("payerkey"),
                F.col("claimLoadDateTime"),
                F.col("loadyear"),
                F.col("line.claimlinekey").cast("long").alias("claimLineKey"),
                F.coalesce(F.col("line.claimkey").cast("long"), F.col("parentClaimKey")).alias("claimKey"),
                F.col("line.claimlinenumber").cast("string").alias("claimLineNumber"),
                F.col("line.claimdetailstatus").cast("string").alias("claimDetailStatus"),
                F.col("line.adjustmentflag").cast("string").alias("adjustmentFlag"),
                F.col("line.procedurecodetype").cast("string").alias("procedureCodeType"),
                F.col("line.procedurecode").cast("string").alias("procedureCode"),
                F.col("line.revenuecode").cast("string").alias("revenueCode"),
                F.col("line.isprimaryndc").cast("int").alias("isPrimaryNDC"),
                F.col("line.quantity").cast("string").alias("quantity"),
                F.col("line.modifiercode01").cast("string").alias("modifierCode01"),
                F.col("line.modifiercode02").cast("string").alias("modifierCode02"),
                F.col("line.placeofservice").cast("string").alias("placeofService"),
                F.col("line.servicetype").cast("string").alias("serviceType"),
                epoch_ms_to_ts(F.col("line.dateofservicefrom")).alias("dateofServiceFrom"),
                epoch_ms_to_ts(F.col("line.dateofservicethru")).alias("dateofServiceThru"),
                F.col("line.billedamount").cast("decimal(18,2)").alias("billedAmount"),
                F.col("line.coveredamount").cast("decimal(18,2)").alias("coveredAmount"),
                F.col("line.allowedamount").cast("decimal(18,2)").alias("allowedAmount"),
                F.col("line.discountamount").cast("decimal(18,2)").alias("discountAmount"),
                F.col("line.discountreason").cast("string").alias("discountReason"),
                F.col("line.withholdamount").cast("decimal(18,2)").alias("withholdAmount"),
                F.col("line.withholdreason").cast("string").alias("withholdReason"),
                F.col("line.excludedamount").cast("decimal(18,2)").alias("excludedAmount"),
                F.col("line.excludedreason").cast("string").alias("excludedReason"),
                F.col("line.memberpaid").cast("decimal(18,2)").alias("memberPaid"),
                F.col("line.originalclientpaidamount").cast("decimal(18,2)").alias("originalClientPaidAmount"),
                F.col("line.clientpaidamount").cast("decimal(18,2)").alias("clientPaidAmount"),
                F.col("line.previouspaidamount").cast("decimal(18,2)").alias("previousPaidAmount"),
                F.col("line.providerpaidamount").cast("decimal(18,2)").alias("providerPaidAmount"),
                F.col("line.housecode").cast("string").alias("houseCode"),
                F.col("line.housecodedescription").cast("string").alias("houseCodeDescription"),
                F.col("line.stageclaimlinekey").cast("long").alias("stageClaimLineKey"),
                F.col("line.clientdatafeedcode").cast("string").alias("clientDataFeedCode"),
                F.col("line.paymenttype").cast("string").alias("paymentType"),
                F.col("line.transactioncode").cast("string").alias("transactionCode"),
                F.col("line.transactiondescription").cast("string").alias("transactionDescription"),
                F.col("line.checknumber").cast("string").alias("checkNumber"),
                F.col("line.inboundbatchmasterkey").cast("long").alias("inboundBatchMasterKey"),
                F.col("line.batchrunsequence").cast("long").alias("batchRunSequence"),
                F.col("line.paymentcomments").cast("string").alias("paymentComments"),
                F.col("line.manipulationreason").cast("string").alias("manipulationReason"),
                F.col("line.paymenttypeid").cast("string").alias("paymentTypeID"),
                F.col("line.insuredtermdate").cast("string").alias("insuredTermDate"),
                F.col("line.claimlineclientfields").cast("string").alias("claimLineClientFields"),
                F.col("line.createdby").cast("long").alias("createdBy"),
                epoch_ms_to_ts(F.col("line.createdat")).alias("createdAt"),
                F.col("line.updatedby").cast("long").alias("updatedBy"),
                epoch_ms_to_ts(F.col("line.updatedat")).alias("updatedAt"),
            )
            .filter(F.col("payerkey").isNotNull() & F.col("loadyear").isNotNull())
            .filter(F.col("claimKey").isNotNull())
        )

        lines_src.repartition(300, "payerKey", "loadyear") \
            .write.mode("append") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", 500000) \
            .partitionBy("payerKey", "loadyear") \
            .parquet(f"{OUTPUT_PATH}/claimlines/")

        # ---------------- CLAIM DIAGNOSIS ----------------
        diag_src = df0.select(
            F.col("payerkey").cast("long").alias("payerkey"),
            F.col("claimLoadDateTime").cast("timestamp").alias("claimLoadDateTime"),
            F.col("loadyear").cast("int").alias("loadyear"),
            F.col("claimKey").cast("long").alias("parentClaimKey"),
            F.explode_outer("claimDiagnosisList").alias("diag"),
        )

        diag_flat = (
            diag_src.select(
                F.col("payerkey"),
                F.col("claimLoadDateTime"),
                F.col("loadyear"),
                F.col("diag.claimdiagnosiskey").cast("long").alias("claimDiagnosisKey"),
                F.coalesce(F.col("diag.claimkey").cast("long"), F.col("parentClaimKey")).alias("claimKey"),
                F.col("diag.versionindicator").cast("long").alias("versionIndicator"),
                F.col("diag.diagnosiscode").cast("string").alias("diagnosisCode"),
                F.col("diag.diagnosisorder").cast("long").alias("diagnosisOrder"),
                F.col("diag.isprimary").cast("string").alias("isPrimary"),
                F.col("diag.istrauma").cast("long").alias("isTrauma"),
                F.col("diag.clientdatafeedcode").cast("string").alias("clientDataFeedCode"),
                F.col("diag.inboundbatchmasterkey").cast("long").alias("inboundBatchMasterKey"),
                F.col("diag.batchrunsequence").cast("long").alias("batchRunSequence"),
                F.col("diag.issensitive").cast("long").alias("isSensitive"),
            )
            .filter(F.col("payerkey").isNotNull() & F.col("loadyear").isNotNull())
            .filter(F.col("claimKey").isNotNull())
        )
        diag_src.repartition(300, "payerKey", "loadyear") \
            .write.mode("append") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", 500000) \
            .partitionBy("payerKey", "loadyear") \
            .parquet(f"{OUTPUT_PATH}/claimdiagnosis/")

 
    for page in pages:
 
        for obj in page.get("Contents", []):
 
            key = obj["Key"]
 
            if key.endswith(".json"):
 
                file_batch.append(f"s3://{BUCKET}/{key}")
 
                if len(file_batch) >= BATCH_SIZE:
 
                    process_batch(file_batch)
 
                    file_batch = []
 
 
    if file_batch:
        process_batch(file_batch)

except Exception:
    traceback.print_exc()
    sys.exit(1)
finally:
    if spark:
        spark.stop()
