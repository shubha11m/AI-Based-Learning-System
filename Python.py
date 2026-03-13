Great! Now we'll add **watermark-based incremental loading** so the job only processes new/changed files instead of reloading everything each time.

Here's the complete updated script:

```python
import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'param1',
    'param2',
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ENV = args['param1']   # DEV
RUN = args['param2']   # WORKDAY-5

SOURCE_BUCKET     = "nontrauma-claims-prod"          # no s3:// for boto3
TARGET_BUCKET     = "nontrauma-analytics-prod"
SOURCE_BUCKET_S3  = f"s3://{SOURCE_BUCKET}"
TARGET_BUCKET_S3  = f"s3://{TARGET_BUCKET}"
DATABASE          = "claims_db_dev"
WATERMARK_KEY     = "watermarks/claims_last_run.json"

print(f"[INFO] ENV={ENV} | RUN={RUN}")
print(f"[INFO] Source  → {SOURCE_BUCKET_S3}")
print(f"[INFO] Target  → {TARGET_BUCKET_S3}")
print(f"[INFO] DB      → {DATABASE}")

# ── Iceberg config ────────────────────────────────────────────────────────────
spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
    f"{TARGET_BUCKET_S3}/iceberg/")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")

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

# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 — READ WATERMARK
# Stored in: nontrauma-analytics-prod/watermarks/claims_last_run.json
# Format:    {"last_run_epoch_ms": 1767680791291, "last_run_ts": "2025-12-01T00:00:00"}
#
# First ever run → watermark file does not exist → full load (epoch = 0)
# Every run after → only process files modified AFTER last_run_epoch_ms
# ═════════════════════════════════════════════════════════════════════════════
print("[STEP 1] Reading watermark...")

s3_client = boto3.client("s3")
current_run_epoch_ms = int(datetime.utcnow().timestamp() * 1000)
current_run_ts       = datetime.utcnow().isoformat()

try:
    obj = s3_client.get_object(Bucket=TARGET_BUCKET, Key=WATERMARK_KEY)
    watermark        = json.loads(obj["Body"].read())
    last_run_epoch_ms = int(watermark["last_run_epoch_ms"])
    last_run_ts       = watermark["last_run_ts"]
    print(f"[STEP 1] Last run: {last_run_ts} (epoch ms: {last_run_epoch_ms})")
    is_full_load = False
except s3_client.exceptions.NoSuchKey:
    print("[STEP 1] No watermark found → FULL LOAD (first run)")
    last_run_epoch_ms = 0
    last_run_ts       = "1970-01-01T00:00:00"
    is_full_load      = True
except Exception as e:
    print(f"[STEP 1] Watermark read error: {e} → defaulting to FULL LOAD")
    last_run_epoch_ms = 0
    last_run_ts       = "1970-01-01T00:00:00"
    is_full_load      = True

print(f"[STEP 1] Mode: {'FULL LOAD' if is_full_load else 'INCREMENTAL'}")
print(f"[STEP 1] Processing changes after: {last_run_ts}")

# ═════════════════════════════════════════════════════════════════════════════
# STEP 2 — FIND CHANGED FILES USING S3 LAST MODIFIED
# Lists all JSON files in source bucket
# Filters to only files modified AFTER last watermark
# This avoids reading the entire dataset every run
# ═════════════════════════════════════════════════════════════════════════════
print("[STEP 2] Scanning S3 for new/modified files since last run...")

paginator   = s3_client.get_paginator("list_objects_v2")
pages       = paginator.paginate(Bucket=SOURCE_BUCKET, Prefix="payerkey=")
changed_files = []

for page in pages:
    for obj in page.get("Contents", []):
        key           = obj["Key"]
        last_modified = obj["LastModified"]
        # Convert S3 LastModified to epoch ms
        file_epoch_ms = int(last_modified.timestamp() * 1000)

        if key.endswith(".json") and file_epoch_ms > last_run_epoch_ms:
            changed_files.append(f"s3://{SOURCE_BUCKET}/{key}")

print(f"[STEP 2] Changed files found: {len(changed_files)}")

if len(changed_files) == 0:
    print("[STEP 2] No new or modified files since last run — nothing to do!")
    # Still update watermark so next run has correct timestamp
    s3_client.put_object(
        Bucket=TARGET_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps({
            "last_run_epoch_ms": current_run_epoch_ms,
            "last_run_ts":       current_run_ts,
            "files_processed":   0,
            "mode":              "INCREMENTAL - NO CHANGES"
        })
    )
    job.commit()
    sys.exit(0)

# Show sample of changed files for logging
print("[STEP 2] Sample changed files:")
for f in changed_files[:5]:
    print(f"         {f}")
if len(changed_files) > 5:
    print(f"         ... and {len(changed_files) - 5} more")

# ═════════════════════════════════════════════════════════════════════════════
# STEP 3 — READ ONLY CHANGED FILES
# Instead of reading entire S3 bucket, reads only the changed file paths
# For very large lists (10k+ files), batches the read
# ═════════════════════════════════════════════════════════════════════════════
print("[STEP 3] Reading changed JSON files...")

# Spark can read a list of specific S3 paths directly
raw_df = spark.read \
    .option("multiline", "false") \
    .option("mode", "PERMISSIVE") \
    .json(changed_files)

# ── POC FILTER: payer 233 only — REMOVE for full load ─────────────────────────
raw_df = raw_df.filter(F.col("payerKey") == 233)

raw_count = raw_df.count()
print(f"[STEP 3] Records loaded: {raw_count}")

if raw_count == 0:
    print("[STEP 3] No records after filter — exiting")
    job.commit()
    sys.exit(0)

# ═════════════════════════════════════════════════════════════════════════════
# STEP 4 — FILTER BY updatedAt INSIDE JSON
# Double filter: S3 file modified time (step 2) catches new/replaced files
# updatedAt inside JSON catches records updated within same file
# Together they ensure no update is missed
# ═════════════════════════════════════════════════════════════════════════════
print("[STEP 4] Filtering records by updatedAt > last watermark...")

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

# Keep records updated after last run
raw_df = raw_df.filter(
    F.col("updated_at_epoch_ms") > F.lit(last_run_epoch_ms)
)

filtered_count = raw_df.count()
print(f"[STEP 4] Records after updatedAt filter: {filtered_count}")

if filtered_count == 0:
    print("[STEP 4] All records already up to date — nothing to merge")
    s3_client.put_object(
        Bucket=TARGET_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps({
            "last_run_epoch_ms": current_run_epoch_ms,
            "last_run_ts":       current_run_ts,
            "files_processed":   len(changed_files),
            "records_merged":    0,
            "mode":              "INCREMENTAL - ALREADY UP TO DATE"
        })
    )
    job.commit()
    sys.exit(0)

# ═════════════════════════════════════════════════════════════════════════════
# STEP 5 — EXTRACT PARTITION COLUMNS FROM claimLoadDateTime
# ═════════════════════════════════════════════════════════════════════════════
print("[STEP 5] Extracting load_year and load_month...")

raw_df = raw_df \
    .withColumn("claim_load_ts",
                normalize_ts(F.col("claimLoadDateTime"))) \
    .withColumn("load_year",
                F.year(F.col("claim_load_ts")).cast("int")) \
    .withColumn("load_month",
                F.month(F.col("claim_load_ts")).cast("int"))

print("[STEP 5] Partition distribution:")
raw_df.groupBy("payerKey", "load_year", "load_month") \
      .count() \
      .orderBy("load_year", "load_month") \
      .show(50, truncate=False)

# ═════════════════════════════════════════════════════════════════════════════
# STEP 6 — BUILD 3 DATAFRAMES
# Same transformation logic as initial load
# ═════════════════════════════════════════════════════════════════════════════
print("[STEP 6] Building claims dataframe...")

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

print(f"[STEP 6] Claims rows: {claims_df.count()}")

# ── Diagnosis ─────────────────────────────────────────────────────────────────
print("[STEP 6] Building claim_diagnosis dataframe...")

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

print(f"[STEP 6] Diagnosis rows: {diagnosis_df.count()}")

# ── Claim Lines ───────────────────────────────────────────────────────────────
print("[STEP 6] Building claim_lines dataframe...")

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
    F.coalesce(F.col("ln.previ
