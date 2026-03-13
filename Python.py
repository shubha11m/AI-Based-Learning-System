Here is the complete full script:

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

# ── Let Glue handle ALL Iceberg config via --datalake-formats=iceberg ─────────
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

ENV = args['param1']
RUN = args['param2']

SOURCE_BUCKET    = "nontrauma-claim-prod"
TARGET_BUCKET    = "nontrauma-analytics-prod"
SOURCE_BUCKET_S3 = f"s3://{SOURCE_BUCKET}"
TARGET_BUCKET_S3 = f"s3://{TARGET_BUCKET}"
DATABASE         = "claims_db_dev"
PAYER_KEY        = "233"    # TODO: later replace with args['param3']
WATERMARK_KEY    = f"watermarks/claims_payer_{PAYER_KEY}_last_run.json"

print(f"[INFO] ENV={ENV} | RUN={RUN}")
print(f"[INFO] Source    → {SOURCE_BUCKET_S3}")
print(f"[INFO] Target    → {TARGET_BUCKET_S3}")
print(f"[INFO] DB        → {DATABASE}")
print(f"[INFO] PAYER_KEY → {PAYER_KEY}")

# ── Safe runtime configs only ─────────────────────────────────────────────────
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
print("[INFO] Spark runtime configs set ✅")

# ── Tracking variables for finally block ─────────────────────────────────────
changed_files  = []
filtered_count = 0
is_full_load   = True
job_status     = "STARTED"
s3_client      = boto3.client("s3")

current_run_epoch_ms = int(datetime.utcnow().timestamp() * 1000)
current_run_ts       = datetime.utcnow().isoformat()

# =============================================================================
# HELPERS
# =============================================================================
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
                "payer_key":         PAYER_KEY,
                "env":               ENV,
                "run":               RUN
            })
        )
        print(f"[WATERMARK] Saved → status={status} ✅")
    except Exception as wm_err:
        print(f"[WATERMARK] Failed to save watermark: {wm_err}")

# =============================================================================
# MAIN JOB
# =============================================================================
try:

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — CREATE DATABASE
    # ─────────────────────────────────────────────────────────────────────────
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        print(f"[STEP 1] Database {DATABASE} ready ✅")
    except Exception as e:
        print(f"[STEP 1] DB warning (may already exist): {e}")

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
    # Structure: s3://nontrauma-claim-prod/{payerkey}/{memberkey}/{claimkey}.json
    # ─────────────────────────────────────────────────────────────────────────
    print(f"[STEP 3] Scanning S3 for changed files — PAYER={PAYER_KEY}...")
    try:
        paginator      = s3_client.get_paginator("list_objects_v2")
        pages          = paginator.paginate(
            Bucket=SOURCE_BUCKET,
            Prefix=f"{PAYER_KEY}/",
            PaginationConfig={"PageSize": 1000}
        )

        changed_files  = []
        total_scanned  = 0
        members_found  = set()

        for page in pages:
            for obj in page.get("Contents", []):
                key           = obj["Key"]
                last_modified = obj["LastModified"]
                file_epoch_ms = int(last_modified.timestamp() * 1000)
                total_scanned += 1

                if key.endswith(".json") and file_epoch_ms > last_run_epoch_ms:
                    changed_files.append(f"s3://{SOURCE_BUCKET}/{key}")
                    parts = key.split("/")
                    if len(parts) >= 2:
                        members_found.add(parts[1])

        print(f"[STEP 3] Total files scanned    : {total_scanned}")
        print(f"[STEP 3] Changed files found    : {len(changed_files)}")
        print(f"[STEP 3] Unique members affected: {len(members_found)}")

        if len(changed_files) > 0:
            print("[STEP 3] Sample changed files:")
            for f in changed_files[:5]:
                print(f"         {f}")
            if len(changed_files) > 5:
                print(f"         ... and {len(changed_files) - 5} more")

    except Exception as e:
        raise Exception(f"[STEP 3] FAILED scanning S3: {e}")

    if len(changed_files) == 0:
        print("[STEP 3] No new or modified files — nothing to do!")
        save_watermark("SUCCESS - NO CHANGES", 0, 0, "INCREMENTAL - NO CHANGES")
        job.commit()
        sys.exit(0)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — READ JSON FILES
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 4] Reading changed JSON files...")
    try:
        raw_df    = spark.read \
            .option("multiline", "false") \
            .option("mode", "PERMISSIVE") \
            .json(changed_files)

        raw_count = raw_df.count()
        print(f"[STEP 4] Records loaded: {raw_count}")

        print("[STEP 4] Records per payer:")
        raw_df.groupBy("payerKey") \
              .agg(
                  F.count("*").alias("record_count"),
                  F.countDistinct("memberKey").alias("member_count"),
                  F.countDistinct("claimKey").alias("claim_count")
              ) \
              .orderBy("payerKey") \
              .show(50, truncate=False)

        if raw_count == 0:
            print("[STEP 4] No records loaded — exiting")
            save_watermark("SUCCESS - NO RECORDS", len(changed_files), 0,
                           "NO RECORDS")
            job.commit()
            sys.exit(0)

    except Exception as e:
        raise Exception(f"[STEP 4] FAILED reading JSON files: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — FILTER BY updatedAt > watermark
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
                    F.to_timestamp(
                        F.col("updatedAt").cast("string"),
                        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
                    )
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
        def safe_col(col_name, cast_type="string"):
            if col_name in raw_df.columns:
                return F.col(col_name).cast(cast_type)
            else:
                print(f"[WARN] Column missing: {col_name} → null")
                return F.lit(None).cast(cast_type)

        claims_df = raw_df.select(
            F.col("payerKey").cast("long").alias("payer_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.col("memberKey").cast("long").alias("member_key"),
            F.col("claimKey").cast("long").alias("claim_key"),
            safe_col("employerGroupKey", "long").alias("employer_group_key"),
            safe_col("inboundBatchMasterKey", "long").alias("inbound_batch_master_key"),
            safe_col("batchRunSequence", "int").alias("batch_run_sequence"),
            safe_col("stageClaimKey", "long").alias("stage_claim_key"),
            safe_col("claimNumber").alias("claim_number"),
            safe_col("claimStatus").alias("claim_status"),
            safe_col("claimSource").alias("claim_source"),
            safe_col("claimType").alias("claim_type"),
            safe_col("claimMethod").alias("claim_method"),
            safe_col("formType").alias("form_type"),
            safe_col("typeofBill").alias("type_of_bill"),
            safe_col("clientDataFeedCode").alias("client_data_feed_code"),
            safe_col("sourceSystemID").alias("source_system_id"),
            safe_col("planType").alias("plan_type"),
            safe_col("unionType").alias("union_type"),
            safe_col("hospitalAccountNumber").alias("hospital_account_number"),
            safe_col("patientDischargeStatus").alias("patient_discharge_status"),
            safe_col("placeofService").alias("place_of_service"),
            safe_col("priorClaimReference").alias("prior_claim_reference"),
            safe_col("manipulationReason").alias("manipulation_reason"),
            safe_col("billingProviderName").alias("billing_provider_name"),
            safe_col("billingProviderTIN").alias("billing_provider_tin"),
            safe_col("billingProviderNPI").alias("billing_provider_npi"),
            safe_col("billingProviderID").alias("billing_provider_id"),
            safe_col("billingProviderAddress1").alias("billing_provider_address1"),
            safe_col("billingProviderAddress2").alias("billing_provider_address2"),
            safe_col("billingProviderCity").alias("billing_provider_city"),
            safe_col("billingProviderState").alias("billing_provider_state"),
            safe_col("billingProviderZip").alias("billing_provider_zip"),
            safe_col("billingProviderPhone").alias("billing_provider_phone"),
            safe_col("billingProviderEmail").alias("billing_provider_email"),
            safe_col("billingProviderContactName").alias("billing_provider_contact_name"),
            safe_col("billingProviderContactPhone").alias("billing_provider_contact_phone"),
            safe_col("treatingPhysicianName").alias("treating_physician_name"),
            safe_col("treatingProviderTIN").alias("treating_provider_tin"),
            safe_col("treatingProviderMedicare").alias("treating_provider_medicare"),
            safe_col("referringProviderTIN").alias("referring_provider_tin"),
            safe_col("admitProviderTIN").alias("admit_provider_tin"),
            safe_col("physicianProviderTIN").alias("physician_provider_tin"),
            safe_col("providerType").alias("provider_type"),
            safe_col("providerClass").alias("provider_class"),
            safe_col("reimbursementMethod").alias("reimbursement_method"),
            safe_col("isCapitatedClaim").alias("is_capitated_claim"),
            safe_col("isMedicare").alias("is_medicare"),
            safe_col("isSplitClaim").alias("is_split_claim"),
            safe_col("isWorkersComp").alias("is_workers_comp"),
            safe_col("isParticipatingProvider").alias("is_participating_provider"),
            safe_col("isEncounter").alias("is_encounter"),
            safe_col("adjustmentIndicator").alias("adjustment_indicator"),
            safe_col("assignmentFlag").alias("assignment_flag"),
            safe_col("accidentFlag").alias("accident_flag"),
            safe_col("includeEncounterAsPaid").alias("include_encounter_as_paid"),
            safe_col("totalBilledAmount", "decimal(18,2)").alias("total_billed_amount"),
            safe_col("totalClientPaidAmount", "decimal(18,2)").alias("total_client_paid_amount"),
            safe_col("totalMemberPaidAmount", "decimal(18,2)").alias("total_member_paid_amount"),
            safe_col("checkNumber").alias("check_number"),
            safe_col("interestAllowed").alias("interest_allowed"),
            safe_col("interestClaimKey", "long").alias("interest_claim_key"),
            safe_col("encounterClaimKey", "long").alias("encounter_claim_key"),
            safe_col("encounterRelated").alias("encounter_related"),
            safe_col("encounterUnrelated").alias("encounter_unrelated"),
            safe_col("encounterClaimRequested").alias("encounter_claim_requested"),
            normalize_date(F.col("serviceBeginDate")).alias("service_begin_date"),
            normalize_date(F.col("serviceThruDate")).alias("service_thru_date"),
            normalize_date(F.col("datePaid")).alias("date_paid"),
            normalize_date(F.col("claimReceivedDate")).alias("claim_received_date"),
            F.col("claim_load_ts").alias("claim_load_datetime"),
            normalize_ts(safe_col("claimTransferredDateTime")).alias("claim_transferred_datetime"),
            normalize_ts(F.col("createdAt")).alias("created_at"),
            normalize_ts(F.col("updatedAt")).alias("updated_at"),
            F.col("updatedAt").cast("long").alias("updated_at_epoch"),
            safe_col("legacySource").alias("legacy_source"),
            safe_col("legacySchema").alias("legacy_schema"),
            safe_col("legacyID").alias("legacy_id"),
            safe_col("trackingInfo").alias("tracking_info"),
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
            F.explode(
                F.when(
                    F.col("claimDiagnosisList").isNotNull(),
                    F.col("claimDiagnosisList")
                ).otherwise(F.array())
            ).alias("dx")
        ).select(
            "payer_key", "member_key", "claim_key", "load_year", "load_month",
            F.coalesce(F.col("dx.diagnosisCode"),
                       F.col("dx.DiagnosisCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("diagnosis_code"),
            F.coalesce(F.col("dx.diagnosisOrder"),
                       F.col("dx.DiagnosisOrder"),
                       F.lit(None).cast("int")
                       ).cast("int").alias("diagnosis_order"),
            F.coalesce(F.col("dx.isPrimary"),
                       F.col("dx.IsPrimary"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("is_primary"),
            F.coalesce(F.col("dx.isSensitive"),
                       F.col("dx.IsSensitive"),
                       F.lit(None).cast("int")
                       ).cast("int").alias("is_sensitive"),
            F.coalesce(F.col("dx.isTrauma"),
                       F.col("dx.IsTrauma"),
                       F.lit(None).cast("int")
                       ).cast("int").alias("is_trauma"),
            F.coalesce(F.col("dx.versionIndicator"),
                       F.col("dx.VersionIndicator"),
                       F.lit(None).cast("int")
                       ).cast("int").alias("version_indicator"),
            F.coalesce(F.col("dx.clientDataFeedCode"),
                       F.col("dx.ClientDataFeedCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("client_data_feed_code"),
            F.coalesce(F.col("dx.inboundBatchMasterKey"),
                       F.col("dx.InboundBatchMasterKey"),
                       F.lit(None).cast("long")
                       ).cast("long").alias("inbound_batch_master_key"),
            F.coalesce(F.col("dx.batchRunSequence"),
                       F.col("dx.BatchRunSequence"),
                       F.lit(None).cast("int")
                       ).cast("int").alias("batch_run_sequence"),
            F.coalesce(F.col("dx.claimDiagnosisKey"),
                       F.col("dx.ClaimDiagnosisKey"),
                       F.lit(None).cast("long")
                       ).cast("long").alias("claim_diagnosis_key"),
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
            F.explode(
                F.when(
                    F.col("claimLinesList").isNotNull(),
                    F.col("claimLinesList")
                ).otherwise(F.array())
            ).alias("ln")
        ).select(
            "payer_key", "member_key", "claim_key", "load_year", "load_month",
            F.coalesce(F.col("ln.claimLineKey"),
                       F.col("ln.ClaimLineKey"),
                       F.lit(None).cast("long")
                       ).cast("long").alias("claim_line_key"),
            F.coalesce(F.col("ln.claimLineNumber"),
                       F.col("ln.ClaimLineNumber"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("claim_line_number"),
            F.coalesce(F.col("ln.procedureCode"),
                       F.col("ln.ProcedureCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("procedure_code"),
            F.coalesce(F.col("ln.procedureCodeType"),
                       F.col("ln.ProcedureCodeType"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("procedure_code_type"),
            F.coalesce(F.col("ln.billedAmount"),
                       F.col("ln.BilledAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("billed_amount"),
            F.coalesce(F.col("ln.clientPaidAmount"),
                       F.col("ln.ClientPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("client_paid_amount"),
            F.coalesce(F.col("ln.memberPaid"),
                       F.col("ln.MemberPaid"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("member_paid"),
            F.coalesce(F.col("ln.allowedAmount"),
                       F.col("ln.AllowedAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("allowed_amount"),
            F.coalesce(F.col("ln.coveredAmount"),
                       F.col("ln.CoveredAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("covered_amount"),
            F.coalesce(F.col("ln.discountAmount"),
                       F.col("ln.DiscountAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("discount_amount"),
            F.coalesce(F.col("ln.discountReason"),
                       F.col("ln.DiscountReason"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("discount_reason"),
            F.coalesce(F.col("ln.excludedAmount"),
                       F.col("ln.ExcludedAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("excluded_amount"),
            F.coalesce(F.col("ln.excludedReason"),
                       F.col("ln.ExcludedReason"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("excluded_reason"),
            F.coalesce(F.col("ln.withholdAmount"),
                       F.col("ln.WithholdAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("withhold_amount"),
            F.coalesce(F.col("ln.withholdReason"),
                       F.col("ln.WithholdReason"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("withhold_reason"),
            F.coalesce(F.col("ln.providerPaidAmount"),
                       F.col("ln.ProviderPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("provider_paid_amount"),
            F.coalesce(F.col("ln.originalClientPaidAmount"),
                       F.col("ln.OriginalClientPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("original_client_paid_amount"),
            F.coalesce(F.col("ln.previousPaidAmount"),
                       F.col("ln.PreviousPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")
                       ).cast("decimal(18,2)").alias("previous_paid_amount"),
            normalize_date(
                F.coalesce(F.col("ln.dateofServiceFrom"),
                           F.col("ln.DateofServiceFrom"),
                           F.lit(None))
            ).alias("date_of_service_from"),
            normalize_date(
                F.coalesce(F.col("ln.dateofServiceThru"),
                           F.col("ln.DateofServiceThru"),
                           F.lit(None))
            ).alias("date_of_service_thru"),
            F.coalesce(F.col("ln.modifierCode01"),
                       F.col("ln.ModifierCode01"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("modifier_code_01"),
            F.coalesce(F.col("ln.modifierCode02"),
                       F.col("ln.ModifierCode02"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("modifier_code_02"),
            F.coalesce(F.col("ln.placeofService"),
                       F.col("ln.PlaceofService"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("place_of_service"),
            F.coalesce(F.col("ln.revenueCode"),
                       F.col("ln.RevenueCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("revenue_code"),
            F.coalesce(F.col("ln.serviceType"),
                       F.col("ln.ServiceType"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("service_type"),
            F.coalesce(F.col("ln.quantity"),
                       F.col("ln.Quantity"),
                       F.lit(None).cast("decimal(10,2)")
                       ).cast("decimal(10,2)").alias("quantity"),
            F.coalesce(F.col("ln.houseCode"),
                       F.col("ln.HouseCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("house_code"),
            F.coalesce(F.col("ln.houseCodeDescription"),
                       F.col("ln.HouseCodeDescription"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("house_code_description"),
            F.coalesce(F.col("ln.paymentType"),
                       F.col("ln.PaymentType"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("payment_type"),
            F.coalesce(F.col("ln.paymentTypeID"),
                       F.col("ln.PaymentTypeID"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("payment_type_id"),
            F.coalesce(F.col("ln.paymentComments"),
                       F.col("ln.PaymentComments"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("payment_comments"),
            F.coalesce(F.col("ln.checkNumber"),
                       F.col("ln.CheckNumber"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("check_number"),
            F.coalesce(F.col("ln.transactionCode"),
                       F.col("ln.TransactionCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("transaction_code"),
            F.coalesce(F.col("ln.transactionDescription"),
                       F.col("ln.TransactionDescription"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("transaction_description"),
            F.coalesce(F.col("ln.adjustmentFlag"),
                       F.col("ln.AdjustmentFlag"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("adjustment_flag"),
            F.coalesce(F.col("ln.isPrimaryNDC"),
                       F.col("ln.IsPrimaryNDC"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("is_primary_ndc"),
            F.coalesce(F.col("ln.insuredTermDate"),
                       F.col("ln.InsuredTermDate"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("insured_term_date"),
            F.coalesce(F.col("ln.manipulationReason"),
                       F.col("ln.ManipulationReason"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("manipulation_reason"),
            F.coalesce(F.col("ln.claimDetailStatus"),
                       F.col("ln.ClaimDetailStatus"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("claim_detail_status"),
            F.coalesce(F.col("ln.clientDataFeedCode"),
                       F.col("ln.ClientDataFeedCode"),
                       F.lit(None).cast("string")
                       ).cast("string").alias("client_data_feed_code"),
            F.coalesce(F.col("ln.inboundBatchMasterKey"),
                       F.col("ln.InboundBatchMasterKey"),
                       F.lit(None).cast("long")
                       ).cast("long").alias("inbound_batch_master_key"),
            F.coalesce(F.col("ln.batchRunSequence"),
                       F.col("ln.BatchRunSequence"),
                       F.lit(None).cast("int")
                       ).cast("int").alias("batch_run_sequence"),
            F.coalesce(F.col("ln.stageClaimLineKey"),
                       F.col("ln.StageClaimLineKey"),
                       F.lit(None).cast("long")
                       ).cast("long").alias("stage_claim_line_key"),
            normalize_ts(
                F.coalesce(F.col("ln.updatedAt"),
                           F.col("ln.UpdatedAt"),
                           F.lit(None))
            ).alias("updated_at"),
            normalize_ts(
                F.coalesce(F.col("ln.createdAt"),
                           F.col("ln.CreatedAt"),
                           F.lit(None))
            ).alias("created_at"),
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
    # STEP 11 — OPTIMIZE CURRENT MONTH PARTITION
    # ─────────────────────────────────────────────────────────────────────────
    current_year  = datetime.utcnow().year
    current_month = datetime.utcnow().month
    print(f"[STEP 11] Optimizing partition {current_year}/{current_month}...")

    for tbl in ["claims", "claim_diagnosis", "claim_lines"]:
        try:
            spark.sql(f"""
                OPTIMIZE glue_catalog.{DATABASE}.{tbl}
                WHERE load_year  = {current_year}
                AND   load_month = {current_month}
            """)
            print(f"[STEP 11] OPTIMIZE done → {tbl} ✅")
        except Exception as e:
            print(f"[STEP 11] Warning — OPTIMIZE failed for {tbl}: {e}")

    job_status = "SUCCESS"
    print("[INFO] All steps completed successfully ✅")

except Exception as e:
    job_status = "FAILED"
    print(f"[ERROR] Job failed: {e}")
    raise

finally:
    print(f"[FINALLY] Job status       : {job_status}")
    print(f"[FINALLY] Files processed  : {len(changed_files)}")
    print(f"[FINALLY] Records merged   : {filtered_count}")

    save_watermark(
        status          = job_status,
        files_processed = len(changed_files),
        records_merged  = filtered_count,
        mode            = "FULL LOAD" if is_full_load else "INCREMENTAL"
    )

    print("=" * 60)
    print(f"[DONE] Status          : {job_status}")
    print(f"       Mode            : {'FULL LOAD' if is_full_load else 'INCREMENTAL'}")
    print(f"       Payer           : {PAYER_KEY}")
    print(f"       Files processed : {len(changed_files)}")
    print(f"       Records merged  : {filtered_count}")
    print(f"       Run timestamp   : {current_run_ts}")
    print("=" * 60)

    job.commit()
