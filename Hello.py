import sys
import time
import traceback

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

INPUT_PATH = "s3://nontrauma-claim-prod/326"
OUTPUT_PATH = "s3://nontrauma-analytics-prod"

sc = None
spark = None


def _read_json_with_retry(spark_session, path: str, max_attempts: int = 8, base_sleep_seconds: int = 5):
    """Read JSON from S3 with exponential backoff for transient S3 SlowDown (503) errors.

    Notes:
    - Spark may fail during *file discovery* (listing/metadata collection) before any JSON parsing starts.
      We keep listing parallelism low via Spark/Hadoop configs set in the caller.
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return (
                spark_session.read
                # Avoid deeply recursive listing pressure when the prefix is huge.
                # Prefer passing a tighter glob like s3://bucket/prefix/* or partition paths.
                .option("recursiveFileLookup", "true")
                .option("mode", "PERMISSIVE")
                # Don’t infer/merge schema by sampling many files. Set schema explicitly if you can.
                .option("samplingRatio", "0.01")
                .json(path)
            )
        except Exception as e:
            last_err = e
            msg = str(e)
            if ("Slow Down" in msg) or ("503" in msg) or ("AmazonS3Exception" in msg):
                sleep_s = min(base_sleep_seconds * (2 ** (attempt - 1)), 120)
                print(
                    f"S3 throttled/transient error (attempt {attempt}/{max_attempts}). "
                    f"Sleeping {sleep_s}s then retrying..."
                )
                time.sleep(sleep_s)
                continue
            raise
    raise last_err


try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # --- Spark SQL / file source tuning
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.default.parallelism", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # IMPORTANT:
    # You can’t reliably change spark.driver.maxResultSize at runtime on Glue/EMR.
    # If you truly need a higher value, set it as a Job parameter (Spark/Glue config), not here.

    # Reduce driver pressure during file listing:
    # Spark’s InMemoryFileIndex uses parallel listing and then collects results to the driver.
    # If there are many files, the collected listing metadata can exceed driver maxResultSize.
    spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")
    spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")

    # --- Reduce S3 pressure (listing + retries)
    hconf = sc._jsc.hadoopConfiguration()

    # Listing threads: keep low to avoid 503 SlowDown during file discovery.
    hconf.set("mapreduce.input.fileinputformat.list-status.num-threads", "2")

    # S3A client tuning (some keys may be ignored depending on Glue runtime)
    hconf.set("fs.s3a.attempts.maximum", "20")
    hconf.set("fs.s3a.retry.limit", "20")
    hconf.set("fs.s3a.connection.maximum", "128")
    hconf.set("fs.s3a.threads.max", "32")

    # Faster parquet writes (S3A fast upload)
    hconf.set("fs.s3a.fast.upload", "true")
    hconf.set("fs.s3a.fast.upload.buffer", "disk")

    print("Spark started")

    # --- Read JSON
    # Reading `INPUT_PATH/*` helps avoid listing under higher prefixes.
    df = _read_json_with_retry(spark, f"{INPUT_PATH}/*")

    # Normalize columns once
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    def epoch_ms_to_ts(col):
        return F.when(
            col.cast("string").rlike("^[0-9]{13}$"),
            F.to_timestamp(F.from_unixtime((col.cast("double") / 1000.0)))
        ).otherwise(F.to_timestamp(col))

    df = (
        df.withColumn("claimloaddatetime", epoch_ms_to_ts(F.col("claimloaddatetime")))
          .withColumn("loadyear", F.year("claimloaddatetime"))
    )

    # Persist once so the 3 downstream datasets don’t re\-read and re\-parse JSON from S3.
    df = df.persist(StorageLevel.DISK_ONLY)
    df.count()  # materialize cache
    print("JSON loaded and cached")

    # Target output parallelism (tune to cluster size; keep consistent with shuffle partitions)
    OUT_PARTS = 200

    # \-\-\- CLAIMS TABLE
    claims_df = df.select(
        F.col("claimkey").cast("long"),
        F.col("payerkey").cast("long"),
        F.col("memberkey").cast("long"),
        F.col("employergroupkey").cast("long"),

        F.col("claimnumber"),
        F.col("claimsource"),
        F.col("claimstatus"),
        F.col("claimtype"),
        F.col("formtype"),

        F.col("checknumber"),
        F.col("clientdatafeedcode"),

        F.col("branchgroupid"),
        F.col("sourcesystemid"),
        F.col("trackinginfo"),
        F.col("claimclientfields"),

        F.col("adjustmentindicator"),
        F.col("assignmentflag"),
        F.col("accidentflag"),

        F.col("iscapitatedclaim"),
        F.col("ismedicare"),

        F.col("placeofservice"),

        F.col("billingprovidertin"),
        F.col("billingprovidernpi"),
        F.col("billingproviderid"),
        F.col("billingprovidername"),

        F.col("billingprovideraddress1"),
        F.col("billingprovideraddress2"),
        F.col("billingprovidercity"),
        F.col("billingproviderstate"),
        F.col("billingproviderzip"),

        F.col("billingprovidercontactname"),
        F.col("billingprovidercontactphone"),
        F.col("billingprovideremail"),
        F.col("billingproviderfax"),
        F.col("billingproviderphone"),

        F.col("batchrunsequence").cast("long"),
        F.col("inboundbatchmasterkey").cast("long"),
        F.col("stageclaimkey").cast("long"),
        F.col("encounterclaimkey").cast("long"),

        F.col("totalbilledamount").cast("decimal(18,2)"),
        F.col("totalclientpaidamount").cast("decimal(18,2)"),
        F.col("totalmemberpaidamount").cast("decimal(18,2)"),

        F.col("claimloaddatetime"),
        F.col("createdat"),
        F.col("createdby"),
        F.col("updatedat"),
        F.col("updatedby"),

        F.col("servicebegindate"),
        F.col("servicethrudate"),

        F.col("datepaid"),
        F.col("claimreceiveddate"),
        F.col("claimtransferreddatetime"),

        F.col("loadyear")
    ).repartition(OUT_PARTS, "payerkey", "loadyear")

    claims_df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .partitionBy("payerkey", "loadyear") \
        .parquet(f"{OUTPUT_PATH}/claims/")

    print("Claims written")

    # \-\-\- CLAIMLINES TABLE
    lines_df = (
        df.select(
            F.col("payerkey"),
            F.col("memberkey"),
            F.col("claimkey").alias("parentclaimkey"),
            F.col("claimloaddatetime"),
            F.col("loadyear"),
            F.explode_outer("claimlineslist").alias("line")
        )
        .select(
            "payerkey",
            "memberkey",
            "claimloaddatetime",
            "loadyear",

            F.col("line.claimlinekey").cast("long"),

            F.coalesce(
                F.col("line.claimkey"),
                F.col("parentclaimkey")
            ).alias("claimkey"),

            F.col("line.claimlinenumber"),
            F.col("line.claimdetailstatus"),
            F.col("line.adjustmentflag"),

            F.col("line.procedurecodetype"),
            F.col("line.procedurecode"),
            F.col("line.revenuecode"),

            F.col("line.isprimaryndc").cast("int"),
            F.col("line.quantity"),

            F.col("line.modifiercode01"),
            F.col("line.modifiercode02"),

            F.col("line.placeofservice"),
            F.col("line.servicetype"),

            F.col("line.dateofservicefrom"),
            F.col("line.dateofservicethru"),

            F.col("line.billedamount").cast("decimal(18,2)"),
            F.col("line.allowedamount").cast("decimal(18,2)"),
            F.col("line.coveredamount").cast("decimal(18,2)"),

            F.col("line.discountamount").cast("decimal(18,2)"),
            F.col("line.discountreason"),

            F.col("line.withholdamount").cast("decimal(18,2)"),
            F.col("line.withholdreason"),

            F.col("line.excludedamount").cast("decimal(18,2)"),
            F.col("line.excludedreason"),

            F.col("line.memberpaid").cast("decimal(18,2)"),
            F.col("line.clientpaidamount").cast("decimal(18,2)"),
            F.col("line.originalclientpaidamount").cast("decimal(18,2)"),
            F.col("line.previouspaidamount").cast("decimal(18,2)"),
            F.col("line.providerpaidamount").cast("decimal(18,2)"),

            F.col("line.housecode"),
            F.col("line.housecodedescription"),

            F.col("line.paymenttype"),
            F.col("line.paymenttypeid"),

            F.col("line.transactioncode"),
            F.col("line.transactiondescription"),

            F.col("line.checknumber"),

            F.col("line.batchrunsequence").cast("long"),
            F.col("line.inboundbatchmasterkey").cast("long"),
            F.col("line.stageclaimlinekey").cast("long"),

            F.col("line.paymentcomments"),
            F.col("line.manipulationreason"),

            F.col("line.insuredtermdate"),

            F.col("line.claimlineclientfields"),

            F.col("line.createdby").cast("long"),
            F.col("line.createdat"),
            F.col("line.updatedby").cast("long"),
            F.col("line.updatedat")
        )
        .repartition(OUT_PARTS, "payerkey", "loadyear")
    )

    lines_df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .partitionBy("payerkey", "loadyear") \
        .parquet(f"{OUTPUT_PATH}/claimlines/")

    print("ClaimLines written")

    # \-\-\- CLAIM DIAGNOSIS TABLE
    diag_df = (
        df.select(
            F.col("payerkey"),
            F.col("memberkey"),
            F.col("claimkey").alias("parentclaimkey"),
            F.col("claimloaddatetime"),
            F.col("loadyear"),
            F.explode_outer("claimdiagnosislist").alias("diag")
        )
        .select(
            "payerkey",
            "memberkey",
            "claimloaddatetime",
            "loadyear",

            F.col("diag.claimdiagnosiskey").cast("long").alias("claimdiagnosiskey"),

            F.coalesce(
                F.col("diag.claimkey"),
                F.col("parentclaimkey")
            ).alias("claimkey"),

            F.col("diag.diagnosiscode"),
            F.col("diag.diagnosisorder"),
            F.col("diag.versionindicator"),

            F.col("diag.isprimary"),
            F.col("diag.issensitive"),
            F.col("diag.istruama"),

            F.col("diag.clientdatafeedcode"),

            F.col("diag.batchrunsequence").cast("long"),
            F.col("diag.inboundbatchmasterkey").cast("long"),

            F.col("diag.createdby").cast("long"),
            F.col("diag.createdat"),
            F.col("diag.updatedby").cast("long"),
            F.col("diag.updatedat")
        )
        .repartition(OUT_PARTS, "payerkey", "loadyear")
    )

    diag_df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .partitionBy("payerkey", "loadyear") \
        .parquet(f"{OUTPUT_PATH}/claimdiagnosis/")

    print("ClaimDiagnosis written")

except Exception as e:
    print("JOB FAILED")
    print(str(e))
    traceback.print_exc()
    raise

finally:
    try:
        if spark:
            spark.stop()
    except Exception:
        pass
