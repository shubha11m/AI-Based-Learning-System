Here is the complete script with try/catch/finally added throughout:

```python
import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'param1',
    'param2',
])

# ═════════════════════════════════════════════════════════════════════════════
# ICEBERG CONFIG — must be set BEFORE Spark starts
# ═════════════════════════════════════════════════════════════════════════════
conf = SparkConf()
conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse",
    "s3://nontrauma-analytics-prod/iceberg/")
conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.defaultCatalog",
    "glue_catalog")
conf.set("spark.sql.iceberg.handle-timestamp-without-timezone",
    "true")
conf.set("spark.sql.sources.partitionOverwriteMode",
    "dynamic")

sc          = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

ENV = args['param1']
RUN = args['param2']

SOURCE_BUCKET    = "nontrauma-claims-prod"
TARGET_BUCKET    = "nontrauma-analytics-prod"
SOURCE_BUCKET_S3 = f"s3://{SOURCE_BUCKET}"
TARGET_BUCKET_S3 = f"s3://{TARGET_BUCKET}"
DATABASE         = "claims_db_dev"
WATERMARK_KEY    = "watermarks/claims_last_run.json"

print(f"[INFO] ENV={ENV} | RUN={RUN}")
print(f"[INFO] Source  → {SOURCE_BUCKET_S3}")
print(f"[INFO] Target  → {TARGET_BUCKET_S3}")
print(f"[INFO] DB      → {DATABASE}")
print("[INFO] Iceberg config loaded via SparkConf ✅")

# ── Tracking variables for finally block ─────────────────────────────────────
changed_files  = []
filtered_count = 0
is_full_load   = True
job_status     = "STARTED"
s3_client      = boto3.client("s3")

# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════
def normalize_ts(col_ref):
    return F.when(
        col_ref.cast("string").rlike("^\\d{13}$"),
        (col_ref.cast("long") / 1000).cast("timestamp")
    ).when(
        col_ref.cast("string").rlike("^\\d{4}-\\d{2}-\\d{2}"),
        F.to_timestamp(col_ref.cast("string"),
                       "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    ).otherwise(None)

def normalize_date(col_ref):
    return F.when(
        col_ref.cast("string").rlike("^\\d{13}$"),
        F.from_unixtime(col_ref.cast("long") / 1000).cast("date")
    ).when(
        col_ref.cast("string").rlike("^\\d{4}-\\d{2}-\\d{2}"),
        F.to_date(col_ref.cast("string"), "yyyy-MM-dd")
    ).otherwise(None)

def save_watermark(status, files_processed, records_merged, mode):
    """Always saves watermark regardless of success or failure"""
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=WATERMARK_KEY,
            Body=json.dumps({
                "last_run_epoch_ms": current_run_epoch_ms,
                "last_run_ts":       current_run_ts,
                "files_processed":   files_processed,
                "records_merged":    records_merged,
                "mode":              mode,
                "job_status":        status,
                "env":               ENV,
                "run":               RUN
            })
        )
        print(f"[WATERMARK] Saved → status={status} ✅")
    except Exception as wm_err:
        print(f"[WATERMARK] Failed to save watermark: {wm_err}")

# ═════════════════════════════════════════════════════════════════════════════
# MAIN JOB — wrapped in try/except/finally
# ═════════════════════════════════════════════════════════════════════════════
current_run_epoch_ms = int(datetime.utcnow().timestamp() * 1000)
current_run_ts       = datetime.utcnow().isoformat()

try:

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — CREATE DATABASE
    # ─────────────────────────────────────────────────────────────────────────
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DATABASE}")
        print(f"[STEP 1] Database {DATABASE} ready ✅")
    except Exception as e:
        print(f"[STEP 1] Warning — database creation: {e}")
        # Non fatal — database may already exist

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — READ WATERMARK
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 2] Reading watermark...")
    try:
        obj               = s3_client.get_object(Bucket=TARGET_BUCKET, Key=WATERMARK_KEY)
        watermark         = json.loads(obj["Body"].read())
        last_run_epoch_ms = int(watermark["last_run_epoch_ms"])
        last_run_ts       = watermark["last_run_ts"]
        is_full_load      = False
        print(f"[STEP 2] Last run : {last_run_ts}")
        print(f"[STEP 2] Mode     : INCREMENTAL")
    except s3_client.exceptions.NoSuchKey:
        print("[STEP 2] No watermark found → FULL LOAD (first run)")
        last_run_epoch_ms = 0
        last_run_ts       = "1970-01-01T00:00:00"
        is_full_load      = True
    except Exception as e:
        print(f"[STEP 2] Watermark read error: {e} → defaulting to FULL LOAD")
        last_run_epoch_ms = 0
        last_run_ts       = "1970-01-01T00:00:00"
        is_full_load      = True

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — FIND CHANGED FILES
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 3] Scanning S3 for changed files...")
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages     = paginator.paginate(Bucket=SOURCE_BUCKET, Prefix="payerkey=")

        for page in pages:
            for obj in page.get("Contents", []):
                key           = obj["Key"]
                last_modified = obj["LastModified"]
                file_epoch_ms = int(last_modified.timestamp() * 1000)
                if key.endswith(".json") and file_epoch_ms > last_run_epoch_ms:
                    changed_files.append(f"s3://{SOURCE_BUCKET}/{key}")

        print(f"[STEP 3] Changed files found: {len(changed_files)}")

        if len(changed_files) > 0:
            print("[STEP 3] Sample files:")
            for f in changed_files[:5]:
                print(f"         {f}")
            if len(changed_files) > 5:
                print(f"         ... and {len(changed_files) - 5} more")

    except Exception as e:
        raise Exception(f"[STEP 3] FAILED scanning S3: {e}")

    if len(changed_files) == 0:
        print("[STEP 3] No new or modified files — nothing to do!")
        save_watermark("SUCCESS - NO CHANGES", 0, 0,
                       "INCREMENTAL - NO CHANGES")
        job.commit()
        sys.exit(0)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — READ JSON FILES
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 4] Reading changed JSON files...")
    try:
        raw_df = spark.read \
            .option("multiline", "false") \
            .option("mode", "PERMISSIVE") \
            .json(changed_files)

        # POC FILTER — remove this line for full load
        raw_df    = raw_df.filter(F.col("payerKey") == 233)
        raw_count = raw_df.count()
        print(f"[STEP 4] Records loaded: {raw_count}")

        if raw_count == 0:
            print("[STEP 4] No records after payer filter — exiting")
            save_watermark("SUCCESS - NO RECORDS", len(changed_files), 0,
                           "INCREMENTAL - NO RECORDS FOR PAYER")
            job.commit()
            sys.exit(0)

    except Exception as e:
        raise Exception(f"[STEP 4] FAILED reading JSON files: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — FILTER BY updatedAt
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 5] Filtering records by updatedAt > last watermark...")
    try:
        raw_df = raw_df.withColumn(
            "updated_at_epoch_ms",
            F.when(
                F.col("updatedAt").cast("string").rlike("^\\d{13}$"),
                F.col("updatedAt").cast("long")
            ).when(
                F.col("updatedAt").cast("string").rlike("^\\d{4}-\\d{2}-\\d{2}"),
                (F.unix_timestamp(
                    F.to_timestamp(F.col("updatedAt").cast("string"),
                                   "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                ) * 1000).cast("long")
            ).otherwise(F.lit(0).cast("long"))
        )

        raw_df         = raw_df.filter(
            F.col("updated_at_epoch_ms") > F.lit(last_run_epoch_ms)
        )
        filtered_count = raw_df.count()
        print(f"[STEP 5] Records after filter: {filtered_count}")

        if filtered_count == 0:
            print("[STEP 5] All records already up to date")
            save_watermark("SUCCESS - ALREADY UP TO DATE",
                           len(changed_files), 0,
                           "INCREMENTAL - ALREADY UP TO DATE")
            job.commit()
            sys.exit(0)

    except Exception as e:
        raise Exception(f"[STEP 5] FAILED filtering records: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6 — EXTRACT PARTITION COLUMNS
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 6] Extracting load_year and load_month...")
    try:
        raw_df = raw_df \
            .withColumn("claim_load_ts",
                        normalize_ts(F.col("claimLoadDateTime"))) \
            .withColumn("load_year",
                        F.year(F.col("claim_load_ts")).cast("int")) \
            .withColumn("load_month",
                        F.month(F.col("claim_load_ts")).cast("int"))

        print("[STEP 6] Partition distribution:")
        raw_df.groupBy("payerKey", "load_year", "load_month") \
              .count() \
              .orderBy("load_year", "load_month") \
              .show(50, truncate=False)

    except Exception as e:
        raise Exception(f"[STEP 6] FAILED extracting partitions: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 7 — BUILD CLAIMS DATAFRAME
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 7] Building claims dataframe...")
    try:
        claims_df = raw_df.select(
            F.col("payerKey").cast("long").alias("payer_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.col("memberKey").cast("long").alias("member_key"),
            F.col("claimKey").cast("long").alias("claim_key"),
            F.col("employerGroupKey").cast("long").alias("employer_group_key"),
            F.col("inboundBatchMasterKey").cast("long").alias("inbound_batch_master_key"),
            F.col("batchRunSequence").cast("int").alias("batch_run_sequence"),
            F.col("stageClaimKey").cast("long").alias("stage_claim_key"),
            F.col("claimNumber").cast("string").alias("claim_number"),
            F.col("claimStatus").cast("string").alias("claim_status"),
            F.col("claimSource").cast("string").alias("claim_source"),
            F.col("claimType").cast("string").alias("claim_type"),
            F.col("claimMethod").cast("string").alias("claim_method"),
            F.col("formType").cast("string").alias("form_type"),
            F.col("typeofBill").cast("string").alias("type_of_bill"),
            F.col("clientDataFeedCode").cast("string").alias("client_data_feed_code"),
            F.col("sourceSystemID").cast("string").alias("source_system_id"),
            F.col("planType").cast("string").alias("plan_type"),
            F.col("unionType").cast("string").alias("union_type"),
            F.col("hospitalAccountNumber").cast("string").alias("hospital_account_number"),
            F.col("patientDischargeStatus").cast("string").alias("patient_discharge_status"),
            F.col("placeofService").cast("string").alias("place_of_service"),
            F.col("priorClaimReference").cast("string").alias("prior_claim_reference"),
            F.col("manipulationReason").cast("string").alias("manipulation_reason"),
            F.col("billingProviderName").cast("string").alias("billing_provider_name"),
            F.col("billingProviderTIN").cast("string").alias("billing_provider_tin"),
            F.col("billingProviderNPI").cast("string").alias("billing_provider_npi"),
            F.col("billingProviderID").cast("string").alias("billing_provider_id"),
            F.col("billingProviderAddress1").cast("string").alias("billing_provider_address1"),
            F.col("billingProviderAddress2").cast("string").alias("billing_provider_address2"),
            F.col("billingProviderCity").cast("string").alias("billing_provider_city"),
            F.col("billingProviderState").cast("string").alias("billing_provider_state"),
            F.col("billingProviderZip").cast("string").alias("billing_provider_zip"),
            F.col("billingProviderPhone").cast("string").alias("billing_provider_phone"),
            F.col("billingProviderEmail").cast("string").alias("billing_provider_email"),
            F.col("billingProviderContactName").cast("string").alias("billing_provider_contact_name"),
            F.col("billingProviderContactPhone").cast("string").alias("billing_provider_contact_phone"),
            F.col("treatingPhysicianName").cast("string").alias("treating_physician_name"),
            F.col("treatingProviderTIN").cast("string").alias("treating_provider_tin"),
            F.col("treatingProviderMedicare").cast("string").alias("treating_provider_medicare"),
            F.col("referringProviderTIN").cast("string").alias("referring_provider_tin"),
            F.col("admitProviderTIN").cast("string").alias("admit_provider_tin"),
            F.col("physicianProviderTIN").cast("string").alias("physician_provider_tin"),
            F.col("providerType").cast("string").alias("provider_type"),
            F.col("providerClass").cast("string").alias("provider_class"),
            F.col("reimbursementMethod").cast("string").alias("reimbursement_method"),
            F.col("isCapitatedClaim").cast("string").alias("is_capitated_claim"),
            F.col("isMedicare").cast("string").alias("is_medicare"),
            F.col("isSplitClaim").cast("string").alias("is_split_claim"),
            F.col("isWorkersComp").cast("string").alias("is_workers_comp"),
            F.col("isParticipatingProvider").cast("string").alias("is_participating_provider"),
            F.col("isEncounter").cast("string").alias("is_encounter"),
            F.col("adjustmentIndicator").cast("string").alias("adjustment_indicator"),
            F.col("assignmentFlag").cast("string").alias("assignment_flag"),
            F.col("accidentFlag").cast("string").alias("accident_flag"),
            F.col("includeEncounterAsPaid").cast("string").alias("include_encounter_as_paid"),
            F.col("totalBilledAmount").cast("decimal(18,2)").alias("total_billed_amount"),
            F.col("totalClientPaidAmount").cast("decimal(18,2)").alias("total_client_paid_amount"),
            F.col("totalMemberPaidAmount").cast("decimal(18,2)").alias("total_member_paid_amount"),
            F.col("checkNumber").cast("string").alias("check_number"),
            F.col("interestAllowed").cast("string").alias("interest_allowed"),
            F.col("interestClaimKey").cast("long").alias("interest_claim_key"),
            F.col("encounterClaimKey").cast("long").alias("encounter_claim_key"),
            F.col("encounterRelated").cast("string").alias("encounter_related"),
            F.col("encounterUnrelated").cast("string").alias("encounter_unrelated"),
            F.col("encounterClaimRequested").cast("string").alias("encounter_claim_requested"),
            normalize_date(F.col("serviceBeginDate")).alias("service_begin_date"),
            normalize_date(F.col("serviceThruDate")).alias("service_thru_date"),
            normalize_date(F.col("datePaid")).alias("date_paid"),
            normalize_date(F.col("claimReceivedDate")).alias("claim_received_date"),
            F.col("claim_load_ts").alias("claim_load_datetime"),
            normalize_ts(F.col("claimTransferredDateTime")).alias("claim_transferred_datetime"),
            normalize_ts(F.col("createdAt")).alias("created_at"),
            normalize_ts(F.col("updatedAt")).alias("updated_at"),
            F.col("updatedAt").cast("long").alias("updated_at_epoch"),
            F.col("legacySource").cast("string").alias("legacy_source"),
            F.col("legacySchema").cast("string").alias("legacy_schema"),
            F.col("legacyID").cast("string").alias("legacy_id"),
            F.col("trackingInfo").cast("string").alias("tracking_info"),
        ).dropDuplicates(["payer_key", "member_key", "claim_key"])

        print(f"[STEP 7] Claims rows: {claims_df.count()}")

    except Exception as e:
        raise Exception(f"[STEP 7] FAILED building claims dataframe: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 8 — BUILD CLAIM DIAGNOSIS DATAFRAME
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 8] Building claim_diagnosis dataframe...")
    try:
        diagnosis_df = raw_df.select(
            F.col("payerKey").cast("long").alias("payer_key"),
            F.col("memberKey").cast("long").alias("member_key"),
            F.col("claimKey").cast("long").alias("claim_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.explode("claimDiagnosisList").alias("dx")
        ).select(
            "payer_key", "member_key", "claim_key", "load_year", "load_month",
            F.coalesce(F.col("dx.diagnosisCode"),
                       F.col("dx.DiagnosisCode")).cast("string").alias("diagnosis_code"),
            F.coalesce(F.col("dx.diagnosisOrder"),
                       F.col("dx.DiagnosisOrder")).cast("int").alias("diagnosis_order"),
            F.coalesce(F.col("dx.isPrimary"),
                       F.col("dx.IsPrimary")).cast("string").alias("is_primary"),
            F.coalesce(F.col("dx.isSensitive"),
                       F.col("dx.IsSensitive")).cast("int").alias("is_sensitive"),
            F.coalesce(F.col("dx.isTrauma"),
                       F.col("dx.IsTrauma")).cast("int").alias("is_trauma"),
            F.coalesce(F.col("dx.versionIndicator"),
                       F.col("dx.VersionIndicator")).cast("int").alias("version_indicator"),
            F.coalesce(F.col("dx.clientDataFeedCode"),
                       F.col("dx.ClientDataFeedCode")).cast("string").alias("client_data_feed_code"),
            F.coalesce(F.col("dx.inboundBatchMasterKey"),
                       F.col("dx.InboundBatchMasterKey")).cast("long").alias("inbound_batch_master_key"),
            F.coalesce(F.col("dx.batchRunSequence"),
                       F.col("dx.BatchRunSequence")).cast("int").alias("batch_run_sequence"),
            F.coalesce(F.col("dx.claimDiagnosisKey"),
                       F.col("dx.ClaimDiagnosisKey")).cast("long").alias("claim_diagnosis_key"),
        )
        print(f"[STEP 8] Diagnosis rows: {diagnosis_df.count()}")

    except Exception as e:
        raise Exception(f"[STEP 8] FAILED building diagnosis dataframe: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 9 — BUILD CLAIM LINES DATAFRAME
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 9] Building claim_lines dataframe...")
    try:
        lines_df = raw_df.select(
            F.col("payerKey").cast("long").alias("payer_key"),
            F.col("memberKey").cast("long").alias("member_key"),
            F.col("claimKey").cast("long").alias("claim_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.explode("claimLinesList").alias("ln")
        ).select(
            "payer_key", "member_key", "claim_key", "load_year", "load_month",
            F.coalesce(F.col("ln.claimLineKey"),
                       F.col("ln.ClaimLineKey")).cast("long").alias("claim_line_key"),
            F.coalesce(F.col("ln.claimLineNumber"),
                       F.col("ln.ClaimLineNumber")).cast("string").alias("claim_line_number"),
            F.coalesce(F.col("ln.procedureCode"),
                       F.col("ln.ProcedureCode")).cast("string").alias("procedure_code"),
            F.coalesce(F.col("ln.procedureCodeType"),
                       F.col("ln.ProcedureCodeType")).cast("string").alias("procedure_code_type"),
            F.coalesce(F.col("ln.billedAmount"),
                       F.col("ln.BilledAmount")).cast("decimal(18,2)").alias("billed_amount"),
            F.coalesce(F.col("ln.clientPaidAmount"),
                       F.col("ln.ClientPaidAmount")).cast("decimal(18,2)").alias("client_paid_amount"),
            F.coalesce(F.col("ln.memberPaid"),
                       F.col("ln.MemberPaid")).cast("decimal(18,2)").alias("member_paid"),
            F.coalesce(F.col("ln.allowedAmount"),
                       F.col("ln.AllowedAmount")).cast("decimal(18,2)").alias("allowed_amount"),
            F.coalesce(F.col("ln.coveredAmount"),
                       F.col("ln.CoveredAmount")).cast("decimal(18,2)").alias("covered_amount"),
            F.coalesce(F.col("ln.discountAmount"),
                       F.col("ln.DiscountAmount")).cast("decimal(18,2)").alias("discount_amount"),
            F.coalesce(F.col("ln.discountReason"),
                       F.col("ln.DiscountReason")).cast("string").alias("discount_reason"),
            F.coalesce(F.col("ln.excludedAmount"),
                       F.col("ln.ExcludedAmount")).cast("decimal(18,2)").alias("excluded_amount"),
            F.coalesce(F.col("ln.excludedReason"),
                       F.col("ln.ExcludedReason")).cast("string").alias("excluded_reason"),
            F.coalesce(F.col("ln.withholdAmount"),
                       F.col("ln.WithholdAmount")).cast("decimal(18,2)").alias("withhold_amount"),
            F.coalesce(F.col("ln.withholdReason"),
                       F.col("ln.WithholdReason")).cast("string").alias("withhold_reason"),
            F.coalesce(F.col("ln.providerPaidAmount"),
                       F.col("ln.ProviderPaidAmount")).cast("decimal(18,2)").alias("provider_paid_amount"),
            F.coalesce(F.col("ln.originalClientPaidAmount"),
                       F.col("ln.OriginalClientPaidAmount")).cast("decimal(18,2)").alias("original_client_paid_amount"),
            F.coalesce(F.col("ln.previousPaidAmount"),
                       F.col("ln.PreviousPaidAmount")).cast("decimal(18,2)").alias("previous_paid_amount"),
            normalize_date(F.coalesce(F.col("ln.dateofServiceFrom"),
                           F.col("ln.DateofServiceFrom"))).alias("date_of_service_from"),
            normalize_date(F.coalesce(F.col("ln.dateofServiceThru"),
                           F.col("ln.DateofServiceThru"))).alias("date_of_service_thru"),
            F.coalesce(F.col("ln.modifierCode01"),
                       F.col("ln.ModifierCode01")).cast("string").alias("modifier_code_01"),
            F.coalesce(F.col("ln.modifierCode02"),
                       F.col("ln.ModifierCode02")).cast("string").alias("modifier_code_02"),
            F.coalesce(F.col("ln.placeofService"),
                       F.col("ln.PlaceofService")).cast("string").alias("place_of_service"),
            F.coalesce(F.col("ln.revenueCode"),
                       F.col("ln.RevenueCode")).cast("string").alias("revenue_code"),
            F.coalesce(F.col("ln.serviceType"),
                       F.col("ln.ServiceType")).cast("string").alias("service_type"),
            F.coalesce(F.col("ln.quantity"),
                       F.col("ln.Quantity")).cast("decimal(10,2)").alias("quantity"),
            F.coalesce(F.col("ln.houseCode"),
                       F.col("ln.HouseCode")).cast("string").alias("house_code"),
            F.coalesce(F.col("ln.houseCodeDescription"),
                       F.col("ln.HouseCodeDescription")).cast("string").alias("house_code_description"),
            F.coalesce(F.col("ln.paymentType"),
                       F.col("ln.PaymentType")).cast("string").alias("payment_type"),
            F.coalesce(F.col("ln.paymentTypeID"),
                       F.col("ln.PaymentTypeID")).cast("string").alias("payment_type_id"),
            F.coalesce(F.col("ln.paymentComments"),
                       F.col("ln.PaymentComments")).cast("string").alias("payment_comments"),
            F.coalesce(F.col("ln.checkNumber"),
                       F.col("ln.CheckNumber")).cast("string").alias("check_number"),
            F.coalesce(F.col("ln.transactionCode"),
                       F.col("ln.TransactionCode")).cast("string").alias("transaction_code"),
            F.coalesce(F.col("ln.transactionDescription"),
                       F.col("ln.TransactionDescription")).cast("string").alias("transaction_description"),
            F.coalesce(F.col("ln.adjustmentFlag"),
                       F.col("ln.AdjustmentFlag")).cast("string").alias("adjustment_flag"),
            F.coalesce(F.col("ln.isPrimaryNDC"),
                       F.col("ln.IsPrimaryNDC")).cast("string").alias("is_primary_ndc"),
            F.coalesce(F.col("ln.insuredTermDate"),
                       F.col("ln.InsuredTermDate")).cast("string").alias("insured_term_date"),
            F.coalesce(F.col("ln.manipulationReason"),
                       F.col("ln.ManipulationReason")).cast("string").alias("manipulation_reason"),
            F.coalesce(F.col("ln.claimDetailStatus"),
                       F.col("ln.ClaimDetailStatus")).cast("string").alias("claim_detail_status"),
            F.coalesce(F.col("ln.clientDataFeedCode"),
                       F.col("ln.ClientDataFeedCode")).cast("string").alias("client_data_feed_code"),
            F.coalesce(F.col("ln.inboundBatchMasterKey"),
                       F.col("ln.InboundBatchMasterKey")).cast("long").alias("inbound_batch_master_key"),
            F.coalesce(F.col("ln.batchRunSequence"),
                       F.col("ln.BatchRunSequence")).cast("int").alias("batch_run_sequence"),
            F.coalesce(F.col("ln.stageClaimLineKey"),
                       F.col("ln.StageClaimLineKey")).cast("long").alias("stage_claim_line_key"),
            normalize_ts(F.coalesce(F.col("ln.updatedAt"),
                         F.col("ln.UpdatedAt"))).alias("updated_at"),
            normalize_ts(F.coalesce(F.col("ln.createdAt"),
                         F.col("ln.CreatedAt"))).alias("created_at"),
        )
        print(f"[STEP 9] Claim lines rows: {lines_df.count()}")

    except Exception as e:
        raise Exception(f"[STEP 9] FAILED building lines dataframe: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 10 — MERGE INTO ICEBERG
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 10] Merging into Iceberg tables...")

    claims_df.createOrReplaceTempView("claims_stage")
    diagnosis_df.createOrReplaceTempView("diagnosis_stage")
    lines_df.createOrReplaceTempView("lines_stage")

    try:
        spark.sql(f"""
            MERGE INTO glue_catalog.{DATABASE}.claims AS t
            USING claims_stage AS s
            ON  t.payer_key   = s.payer_key
            AND t.load_year   = s.load_year
            AND t.load_month  = s.load_month
            AND t.member_key  = s.member_key
            AND t.claim_key   = s.claim_key
            WHEN MATCHED AND s.updated_at_epoch > t.updated_at_epoch
                THEN UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
        """)
        print("[STEP 10] Claims MERGE ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claims MERGE: {e}")

    try:
        spark.sql(f"""
            MERGE INTO glue_catalog.{DATABASE}.claim_diagnosis AS t
            USING diagnosis_stage AS s
            ON  t.payer_key       = s.payer_key
            AND t.load_year       = s.load_year
            AND t.load_month      = s.load_month
            AND t.claim_key       = s.claim_key
            AND t.diagnosis_order = s.diagnosis_order
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("[STEP 10] Diagnosis MERGE ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED diagnosis MERGE: {e}")

    try:
        spark.sql(f"""
            MERGE INTO glue_catalog.{DATABASE}.claim_lines AS t
            USING lines_stage AS s
            ON  t.payer_key         = s.payer_key
            AND t.load_year         = s.load_year
            AND t.load_month        = s.load_month
            AND t.claim_key         = s.claim_key
            AND t.claim_line_number = s.claim_line_number
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("[STEP 10] Claim Lines MERGE ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claim lines MERGE: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 11 — OPTIMIZE CURRENT MONTH
    # ─────────────────────────────────────────────────────────────────────────
    current_year  = datetime.utcnow().year
    current_month = datetime.utcnow().month
    print(f"[STEP 11] Optimizing {current_year}/{current_month}...")

    for tbl in ["claims", "claim_diagnosis", "claim_lines"]:
        try:
            spark.sql(f"""
                OPTIMIZE glue_catalog.{DATABASE}.{tbl}
                WHERE load_year  = {current_year}
                AND   load_month = {current_month}
            """)
            print(f"[STEP 11] OPTIMIZE done → {tbl} ✅")
        except Exception as e:
            # Non fatal — optimize failure should not fail the job
            print(f"[STEP 11] Warning — OPTIMIZE failed for {tbl}: {e}")

    job_status = "SUCCESS"
    print("[INFO] All steps completed successfully ✅")

except Exception as e:
    job_status = "FAILED"
    print(f"[ERROR] Job failed: {e}")
    raise

finally:
    # ─────────────────────────────────────────────────────────────────────────
    # FINALLY — always runs whether job succeeds OR fails
    # Saves watermark + commits job
    # ─────────────────────────────────────────────────────────────────────────
    print(f"[FINALLY] Job status: {job_status}")
    print(f"[FINALLY] Files processed : {len(changed_files)}")
    print(f"[FINALLY] Records merged  : {filtered_count}")

    save_watermark(
        status          = job_status,
        files_processed = len(changed_files),
        records_merged  = filtered_count,
        mode            = "FULL LOAD" if is_full_load else "INCREMENTAL"
    )

    print("=" * 60)
    print(f"[DONE] Job finished")
    print(f"       Status          : {job_status}")
    print(f"       Mode            : {'FULL LOAD' if is_full_load else 'INCREMENTAL'}")
    print(f"       Files processed : {len(changed_files)}")
    print(f"       Records merged  : {filtered_count}")
    print(f"       Payer (POC)     : 233")
    print(f"       Run timestamp   : {current_run_ts}")
    print("=" * 60)

    job.commit()
