import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'param1',
    'param2',
])

# ── Glue / Iceberg bootstrap ──────────────────────────────────────────────────
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
PAYER_KEY        = "233"    # TODO: replace with args['param3']
WATERMARK_KEY    = f"watermarks/claims_payer_{PAYER_KEY}_last_run.json"

print(f"[INFO] ENV={ENV} | RUN={RUN}")
print(f"[INFO] Source    → {SOURCE_BUCKET_S3}")
print(f"[INFO] Target    → {TARGET_BUCKET_S3}")
print(f"[INFO] DB        → {DATABASE}")
print(f"[INFO] PAYER_KEY → {PAYER_KEY}")

# ── Spark / Iceberg runtime config ────────────────────────────────────────────
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.catalog.glue_catalog",
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
               "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
               "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
               f"s3://{TARGET_BUCKET}/iceberg/")
print("[INFO] Spark runtime configs set ✅")

# ── Tracking variables ────────────────────────────────────────────────────────
changed_files        = []
filtered_count       = 0
is_full_load         = True
job_status           = "STARTED"
s3_client            = boto3.client("s3")
current_run_epoch_ms = int(datetime.utcnow().timestamp() * 1000)
current_run_ts       = datetime.utcnow().isoformat()

# =============================================================================
# HELPERS
# =============================================================================

def normalize_ts(col_ref):
    """Parse epoch-ms longs OR ISO strings into a Spark timestamp."""
    return (
        F.when(col_ref.cast("string").rlike("^\\d{13}$"),
               (col_ref.cast("long") / 1000).cast("timestamp"))
         .when(col_ref.cast("string").rlike("^\\d{4}-\\d{2}-\\d{2}"),
               F.to_timestamp(col_ref.cast("string"),
                              "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
         .otherwise(None)
    )

def normalize_date(col_ref):
    """Parse epoch-ms longs OR ISO strings into a Spark date."""
    return (
        F.when(col_ref.cast("string").rlike("^\\d{13}$"),
               F.from_unixtime(col_ref.cast("long") / 1000).cast("date"))
         .when(col_ref.cast("string").rlike("^\\d{4}-\\d{2}-\\d{2}"),
               F.to_date(col_ref.cast("string"), "yyyy-MM-dd"))
         .otherwise(None)
    )

def to_epoch_ms(col_ref):
    """Return a long epoch-ms from an epoch-ms long OR ISO string column; 0 if unknown."""
    return (
        F.when(col_ref.cast("string").rlike("^\\d{13}$"),
               col_ref.cast("long"))
         .when(col_ref.cast("string").rlike("^\\d{4}-\\d{2}-\\d{2}"),
               (F.unix_timestamp(
                   F.to_timestamp(col_ref.cast("string"),
                                  "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
               ) * 1000).cast("long"))
         .otherwise(F.lit(0).cast("long"))
    )

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
                "run":               RUN,
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
    #
    # S3 layout:  s3://nontrauma-claim-prod/{payerKey}/{memberKey}/{claimKey}.json
    #
    # Each file is ONE claim document — a single JSON object that contains:
    #   • top-level claim header fields  (claimKey, memberKey, payerKey, …)
    #   • "claimLinesList"    → array of claim-line objects
    #   • "claimDiagnosisList"→ array of diagnosis objects
    #
    # Change detection: compare S3 LastModified against the last watermark.
    # We also capture the S3 path so we can derive keys as a fallback if the
    # JSON fields are missing.
    # ─────────────────────────────────────────────────────────────────────────
    print(f"[STEP 3] Scanning S3 for changed files — PAYER={PAYER_KEY}...")
    try:
        paginator     = s3_client.get_paginator("list_objects_v2")
        pages         = paginator.paginate(
            Bucket=SOURCE_BUCKET,
            Prefix=f"{PAYER_KEY}/",
            PaginationConfig={"PageSize": 1000}
        )

        changed_files        = []   # list of s3:// URIs
        changed_file_meta    = []   # list of dicts {s3_path, path_payer_key, path_member_key, path_claim_key, file_epoch_ms}
        total_scanned        = 0
        members_found        = set()

        for page in pages:
            for obj in page.get("Contents", []):
                key           = obj["Key"]           # e.g. "233/456/789.json"
                last_modified = obj["LastModified"]
                file_epoch_ms = int(last_modified.timestamp() * 1000)
                total_scanned += 1

                if not key.endswith(".json"):
                    continue

                # Parse path components: {payerKey}/{memberKey}/{claimKey}.json
                parts = key.split("/")
                if len(parts) < 3:
                    continue

                path_payer_key  = parts[0]
                path_member_key = parts[1]
                path_claim_key  = parts[2].replace(".json", "")

                if file_epoch_ms > last_run_epoch_ms:
                    s3_path = f"s3://{SOURCE_BUCKET}/{key}"
                    changed_files.append(s3_path)
                    members_found.add(path_member_key)
                    changed_file_meta.append({
                        "s3_path":        s3_path,
                        "path_payer_key": path_payer_key,
                        "path_member_key":path_member_key,
                        "path_claim_key": path_claim_key,
                        "file_epoch_ms":  file_epoch_ms,
                    })

        print(f"[STEP 3] Total files scanned    : {total_scanned}")
        print(f"[STEP 3] Changed files found    : {len(changed_files)}")
        print(f"[STEP 3] Unique members affected: {len(members_found)}")
        if changed_files:
            for f in changed_files[:5]:
                print(f"         {f}")
            if len(changed_files) > 5:
                print(f"         ... and {len(changed_files) - 5} more")

    except Exception as e:
        raise Exception(f"[STEP 3] FAILED scanning S3: {e}")

    if not changed_files:
        print("[STEP 3] No new or modified files — nothing to do!")
        save_watermark("SUCCESS - NO CHANGES", 0, 0, "INCREMENTAL - NO CHANGES")
        job.commit()
        sys.exit(0)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — READ JSON FILES
    #
    # Each {claimKey}.json is a SINGLE JSON object (not newline-delimited),
    # so we MUST use multiline=true.  Spark will read each file as one row.
    #
    # We also build a path-metadata lookup so we can cross-check / backfill
    # payerKey / memberKey / claimKey from the S3 path when the JSON fields
    # are missing or null.
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 4] Reading changed JSON files (multiline=true)...")
    try:
        # Build a small DataFrame with path-derived keys + file modification time
        file_meta_df = spark.createDataFrame(changed_file_meta).select(
            F.col("s3_path"),
            F.col("path_payer_key").cast("long").alias("path_payer_key"),
            F.col("path_member_key").cast("long").alias("path_member_key"),
            F.col("path_claim_key").cast("long").alias("path_claim_key"),
            F.col("file_epoch_ms").cast("long").alias("file_epoch_ms"),
        )

        # Read all changed files as multiline JSON (one JSON object per file)
        raw_df = (
            spark.read
            .option("multiline", "true")   # ← critical: each file is ONE object
            .option("mode", "PERMISSIVE")
            .json(changed_files)
            # Attach the source file path so we can join with path metadata
            .withColumn("_input_file", F.input_file_name())
        )

        # Join path metadata onto the raw data
        raw_df = raw_df.join(
            file_meta_df.withColumnRenamed("s3_path", "_input_file"),
            on="_input_file",
            how="left"
        )

        raw_count = raw_df.count()
        print(f"[STEP 4] Records loaded: {raw_count}")

        print("[STEP 4] Records per payer (from JSON field):")
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
            save_watermark("SUCCESS - NO RECORDS", len(changed_files), 0, "NO RECORDS")
            job.commit()
            sys.exit(0)

    except Exception as e:
        raise Exception(f"[STEP 4] FAILED reading JSON files: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — RESOLVE KEYS & COMPUTE EPOCH; NO HARD FILTER
    #
    # S3 LastModified is already our change signal (used in STEP 3).
    # Here we:
    #   1. Resolve payerKey / memberKey / claimKey — prefer JSON field,
    #      fall back to path-derived value (handles malformed/missing fields).
    #   2. Compute updated_at_epoch_ms from the JSON updatedAt field.
    #      We do NOT hard-filter here — every file that passed STEP 3
    #      (LastModified > watermark) is a genuine change and must be processed,
    #      even if the JSON updatedAt hasn't been updated by the source system.
    #   3. Use file_epoch_ms as a fallback epoch when JSON updatedAt is absent.
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 5] Resolving keys and computing update epoch...")
    try:
        raw_df = raw_df \
            .withColumn(
                # Resolve claimKey: JSON first, S3 path as fallback
                "resolved_payer_key",
                F.coalesce(
                    F.col("payerKey").cast("long"),
                    F.col("path_payer_key")
                )
            ) \
            .withColumn(
                "resolved_member_key",
                F.coalesce(
                    F.col("memberKey").cast("long"),
                    F.col("path_member_key")
                )
            ) \
            .withColumn(
                "resolved_claim_key",
                F.coalesce(
                    F.col("claimKey").cast("long"),
                    F.col("path_claim_key")
                )
            ) \
            .withColumn(
                # Epoch ms from JSON updatedAt; fall back to S3 file timestamp
                # so that every record has a meaningful sort key for dedup.
                "updated_at_epoch_ms",
                F.coalesce(
                    to_epoch_ms(F.col("updatedAt")),
                    F.col("file_epoch_ms"),
                    F.lit(0).cast("long")
                )
            )

        # Drop rows where we still cannot resolve a claim key (truly malformed)
        before = raw_df.count()
        raw_df = raw_df.filter(F.col("resolved_claim_key").isNotNull())
        after  = raw_df.count()
        if before != after:
            print(f"[STEP 5] Dropped {before - after} rows with unresolvable claimKey")

        filtered_count = after
        print(f"[STEP 5] Records to process: {filtered_count}")

        if filtered_count == 0:
            print("[STEP 5] No processable records — exiting")
            save_watermark("SUCCESS - NO RECORDS", len(changed_files), 0, "NO RECORDS")
            job.commit()
            sys.exit(0)

    except Exception as e:
        raise Exception(f"[STEP 5] FAILED resolving keys: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6 — EXTRACT PARTITION COLUMNS
    #
    # Partitioning: (payer_key, load_year, load_month)
    # load_year/load_month are derived from claimLoadDateTime.
    # Fallback: use the S3 file modification date so that records with a
    # missing claimLoadDateTime are still assigned to a valid partition
    # (instead of NULL which would break overwritePartitions).
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 6] Extracting partition columns (load_year, load_month)...")
    try:
        raw_df = raw_df \
            .withColumn("claim_load_ts",
                        normalize_ts(F.col("claimLoadDateTime"))) \
            .withColumn(
                "load_year",
                F.coalesce(
                    F.year(F.col("claim_load_ts")).cast("int"),
                    # fallback: derive from S3 file modification epoch
                    F.year(
                        (F.col("file_epoch_ms") / 1000).cast("timestamp")
                    ).cast("int")
                )
            ) \
            .withColumn(
                "load_month",
                F.coalesce(
                    F.month(F.col("claim_load_ts")).cast("int"),
                    F.month(
                        (F.col("file_epoch_ms") / 1000).cast("timestamp")
                    ).cast("int")
                )
            )

        print("[STEP 6] Partition distribution:")
        raw_df.groupBy("resolved_payer_key", "load_year", "load_month") \
              .count() \
              .orderBy("load_year", "load_month") \
              .show(50, truncate=False)

    except Exception as e:
        raise Exception(f"[STEP 6] FAILED extracting partitions: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 7 — BUILD CLAIMS DATAFRAME
    #
    # One row per claim (top-level fields of the JSON document).
    # Uses resolved_payer_key / resolved_member_key / resolved_claim_key so
    # that rows with missing JSON key fields are still processed.
    # Within-batch dedup keeps the highest updated_at_epoch_ms per claim.
    # ───────────────────────���─────────────────────────────────────────────────
    print("[STEP 7] Building claims dataframe...")
    try:
        def safe_col(col_name, cast_type="string"):
            if col_name in raw_df.columns:
                return F.col(col_name).cast(cast_type)
            print(f"[WARN] Column missing: {col_name} → null")
            return F.lit(None).cast(cast_type)

        claims_df = raw_df.select(
            F.col("resolved_payer_key").alias("payer_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.col("resolved_member_key").alias("member_key"),
            F.col("resolved_claim_key").alias("claim_key"),
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
            normalize_date(safe_col("serviceBeginDate")).alias("service_begin_date"),
            normalize_date(safe_col("serviceThruDate")).alias("service_thru_date"),
            normalize_date(safe_col("datePaid")).alias("date_paid"),
            normalize_date(safe_col("claimReceivedDate")).alias("claim_received_date"),
            F.col("claim_load_ts").alias("claim_load_datetime"),
            normalize_ts(safe_col("claimTransferredDateTime")).alias("claim_transferred_datetime"),
            normalize_ts(safe_col("createdAt")).alias("created_at"),
            normalize_ts(safe_col("updatedAt")).alias("updated_at"),
            F.col("updated_at_epoch_ms").alias("updated_at_epoch"),   # bigint for Iceberg
            safe_col("legacySource").alias("legacy_source"),
            safe_col("legacySchema").alias("legacy_schema"),
            safe_col("legacyID").alias("legacy_id"),
            safe_col("trackingInfo").alias("tracking_info"),
            # helper for within-batch dedup — dropped below
            F.col("updated_at_epoch_ms"),
        )

        # Within-batch dedup: keep the latest version of each claim.
        # (The same claimKey.json might appear twice if re-delivered, or the
        #  payer sent two versions of the same claim in one batch.)
        _w = Window.partitionBy(
            "payer_key", "load_year", "load_month", "member_key", "claim_key"
        ).orderBy(
            F.coalesce(F.col("updated_at_epoch_ms"), F.lit(0)).desc()
        )
        claims_df = (
            claims_df
            .withColumn("_rn", F.row_number().over(_w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "updated_at_epoch_ms")
        )

        print(f"[STEP 7] Claims rows: {claims_df.count()}")

    except Exception as e:
        raise Exception(f"[STEP 7] FAILED building claims dataframe: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 8 — BUILD CLAIM DIAGNOSIS DATAFRAME
    #
    # Explode claimDiagnosisList from each claim document.
    # Each claim owns its diagnosis list — when a claim is re-delivered, the
    # entire list is the authoritative current state for that claim.
    # updated_at_epoch_ms falls back to the parent claim's epoch so every
    # diagnosis row has a valid sort key for cross-run upsert dedup.
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 8] Building claim_diagnosis dataframe...")
    try:
        diagnosis_df = raw_df.select(
            F.col("resolved_payer_key").alias("payer_key"),
            F.col("resolved_member_key").alias("member_key"),
            F.col("resolved_claim_key").alias("claim_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.col("updated_at_epoch_ms").alias("claim_updated_at_epoch_ms"),
            F.explode(
                F.when(
                    F.col("claimDiagnosisList").isNotNull(),
                    F.col("claimDiagnosisList")
                ).otherwise(F.array())
            ).alias("dx")
        ).select(
            "payer_key", "member_key", "claim_key", "load_year", "load_month",
            "claim_updated_at_epoch_ms",
            F.coalesce(F.col("dx.diagnosisCode"),
                       F.col("dx.DiagnosisCode"),
                       F.lit(None).cast("string")).cast("string").alias("diagnosis_code"),
            F.coalesce(F.col("dx.diagnosisOrder"),
                       F.col("dx.DiagnosisOrder"),
                       F.lit(None).cast("int")).cast("int").alias("diagnosis_order"),
            F.coalesce(F.col("dx.isPrimary"),
                       F.col("dx.IsPrimary"),
                       F.lit(None).cast("string")).cast("string").alias("is_primary"),
            F.coalesce(F.col("dx.isSensitive"),
                       F.col("dx.IsSensitive"),
                       F.lit(None).cast("int")).cast("int").alias("is_sensitive"),
            F.coalesce(F.col("dx.isTrauma"),
                       F.col("dx.IsTrauma"),
                       F.lit(None).cast("int")).cast("int").alias("is_trauma"),
            F.coalesce(F.col("dx.versionIndicator"),
                       F.col("dx.VersionIndicator"),
                       F.lit(None).cast("int")).cast("int").alias("version_indicator"),
            F.coalesce(F.col("dx.clientDataFeedCode"),
                       F.col("dx.ClientDataFeedCode"),
                       F.lit(None).cast("string")).cast("string").alias("client_data_feed_code"),
            F.coalesce(F.col("dx.inboundBatchMasterKey"),
                       F.col("dx.InboundBatchMasterKey"),
                       F.lit(None).cast("long")).cast("long").alias("inbound_batch_master_key"),
            F.coalesce(F.col("dx.batchRunSequence"),
                       F.col("dx.BatchRunSequence"),
                       F.lit(None).cast("int")).cast("int").alias("batch_run_sequence"),
            F.coalesce(F.col("dx.claimDiagnosisKey"),
                       F.col("dx.ClaimDiagnosisKey"),
                       F.lit(None).cast("long")).cast("long").alias("claim_diagnosis_key"),
            # Diagnosis rows carry no updatedAt of their own — use parent claim epoch
            F.lit(None).cast("timestamp").alias("updated_at"),
            F.coalesce(
                F.col("claim_updated_at_epoch_ms"),
                F.lit(0).cast("long")
            ).alias("updated_at_epoch_ms"),
        )

        # Within-batch dedup: keep latest per (payer, member, claim, dx order)
        _wdx = Window.partitionBy(
            "payer_key", "load_year", "load_month", "member_key", "claim_key", "diagnosis_order"
        ).orderBy(F.coalesce(F.col("updated_at_epoch_ms"), F.lit(0)).desc())
        diagnosis_df = (
            diagnosis_df
            .withColumn("_rn", F.row_number().over(_wdx))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "claim_updated_at_epoch_ms")
        )

        print(f"[STEP 8] Diagnosis rows: {diagnosis_df.count()}")

    except Exception as e:
        raise Exception(f"[STEP 8] FAILED building diagnosis dataframe: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 9 — BUILD CLAIM LINES DATAFRAME
    #
    # Explode claimLinesList from each claim document.
    # Same pattern as diagnosis: parent claim epoch used as fallback,
    # within-batch dedup by (payer, member, claim, line_number).
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 9] Building claim_lines dataframe...")
    try:
        lines_df = raw_df.select(
            F.col("resolved_payer_key").alias("payer_key"),
            F.col("resolved_member_key").alias("member_key"),
            F.col("resolved_claim_key").alias("claim_key"),
            F.col("load_year"),
            F.col("load_month"),
            F.col("updated_at_epoch_ms").alias("claim_updated_at_epoch_ms"),
            F.explode(
                F.when(
                    F.col("claimLinesList").isNotNull(),
                    F.col("claimLinesList")
                ).otherwise(F.array())
            ).alias("ln")
        ).select(
            "payer_key", "member_key", "claim_key", "load_year", "load_month",
            "claim_updated_at_epoch_ms",
            F.coalesce(F.col("ln.claimLineKey"),
                       F.col("ln.ClaimLineKey"),
                       F.lit(None).cast("long")).cast("long").alias("claim_line_key"),
            F.coalesce(F.col("ln.claimLineNumber"),
                       F.col("ln.ClaimLineNumber"),
                       F.lit(None).cast("string")).cast("string").alias("claim_line_number"),
            F.coalesce(F.col("ln.procedureCode"),
                       F.col("ln.ProcedureCode"),
                       F.lit(None).cast("string")).cast("string").alias("procedure_code"),
            F.coalesce(F.col("ln.procedureCodeType"),
                       F.col("ln.ProcedureCodeType"),
                       F.lit(None).cast("string")).cast("string").alias("procedure_code_type"),
            F.coalesce(F.col("ln.billedAmount"),
                       F.col("ln.BilledAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("billed_amount"),
            F.coalesce(F.col("ln.clientPaidAmount"),
                       F.col("ln.ClientPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("client_paid_amount"),
            F.coalesce(F.col("ln.memberPaid"),
                       F.col("ln.MemberPaid"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("member_paid"),
            F.coalesce(F.col("ln.allowedAmount"),
                       F.col("ln.AllowedAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("allowed_amount"),
            F.coalesce(F.col("ln.coveredAmount"),
                       F.col("ln.CoveredAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("covered_amount"),
            F.coalesce(F.col("ln.discountAmount"),
                       F.col("ln.DiscountAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("discount_amount"),
            F.coalesce(F.col("ln.discountReason"),
                       F.col("ln.DiscountReason"),
                       F.lit(None).cast("string")).cast("string").alias("discount_reason"),
            F.coalesce(F.col("ln.excludedAmount"),
                       F.col("ln.ExcludedAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("excluded_amount"),
            F.coalesce(F.col("ln.excludedReason"),
                       F.col("ln.ExcludedReason"),
                       F.lit(None).cast("string")).cast("string").alias("excluded_reason"),
            F.coalesce(F.col("ln.withholdAmount"),
                       F.col("ln.WithholdAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("withhold_amount"),
            F.coalesce(F.col("ln.withholdReason"),
                       F.col("ln.WithholdReason"),
                       F.lit(None).cast("string")).cast("string").alias("withhold_reason"),
            F.coalesce(F.col("ln.providerPaidAmount"),
                       F.col("ln.ProviderPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("provider_paid_amount"),
            F.coalesce(F.col("ln.originalClientPaidAmount"),
                       F.col("ln.OriginalClientPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("original_client_paid_amount"),
            F.coalesce(F.col("ln.previousPaidAmount"),
                       F.col("ln.PreviousPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("previous_paid_amount"),
            normalize_date(F.coalesce(F.col("ln.dateofServiceFrom"),
                                      F.col("ln.DateofServiceFrom"),
                                      F.lit(None))).alias("date_of_service_from"),
            normalize_date(F.coalesce(F.col("ln.dateofServiceThru"),
                                      F.col("ln.DateofServiceThru"),
                                      F.lit(None))).alias("date_of_service_thru"),
            F.coalesce(F.col("ln.modifierCode01"),
                       F.col("ln.ModifierCode01"),
                       F.lit(None).cast("string")).cast("string").alias("modifier_code_01"),
            F.coalesce(F.col("ln.modifierCode02"),
                       F.col("ln.ModifierCode02"),
                       F.lit(None).cast("string")).cast("string").alias("modifier_code_02"),
            F.coalesce(F.col("ln.placeofService"),
                       F.col("ln.PlaceofService"),
                       F.lit(None).cast("string")).cast("string").alias("place_of_service"),
            F.coalesce(F.col("ln.revenueCode"),
                       F.col("ln.RevenueCode"),
                       F.lit(None).cast("string")).cast("string").alias("revenue_code"),
            F.coalesce(F.col("ln.serviceType"),
                       F.col("ln.ServiceType"),
                       F.lit(None).cast("string")).cast("string").alias("service_type"),
            F.coalesce(F.col("ln.quantity"),
                       F.col("ln.Quantity"),
                       F.lit(None).cast("decimal(10,2)")).cast("decimal(10,2)").alias("quantity"),
            F.coalesce(F.col("ln.houseCode"),
                       F.col("ln.HouseCode"),
                       F.lit(None).cast("string")).cast("string").alias("house_code"),
            F.coalesce(F.col("ln.houseCodeDescription"),
                       F.col("ln.HouseCodeDescription"),
                       F.lit(None).cast("string")).cast("string").alias("house_code_description"),
            F.coalesce(F.col("ln.paymentType"),
                       F.col("ln.PaymentType"),
                       F.lit(None).cast("string")).cast("string").alias("payment_type"),
            F.coalesce(F.col("ln.paymentTypeID"),
                       F.col("ln.PaymentTypeID"),
                       F.lit(None).cast("string")).cast("string").alias("payment_type_id"),
            F.coalesce(F.col("ln.paymentComments"),
                       F.col("ln.PaymentComments"),
                       F.lit(None).cast("string")).cast("string").alias("payment_comments"),
            F.coalesce(F.col("ln.checkNumber"),
                       F.col("ln.CheckNumber"),
                       F.lit(None).cast("string")).cast("string").alias("check_number"),
            F.coalesce(F.col("ln.transactionCode"),
                       F.col("ln.TransactionCode"),
                       F.lit(None).cast("string")).cast("string").alias("transaction_code"),
            F.coalesce(F.col("ln.transactionDescription"),
                       F.col("ln.TransactionDescription"),
                       F.lit(None).cast("string")).cast("string").alias("transaction_description"),
            F.coalesce(F.col("ln.adjustmentFlag"),
                       F.col("ln.AdjustmentFlag"),
                       F.lit(None).cast("string")).cast("string").alias("adjustment_flag"),
            F.coalesce(F.col("ln.isPrimaryNDC"),
                       F.col("ln.IsPrimaryNDC"),
                       F.lit(None).cast("string")).cast("string").alias("is_primary_ndc"),
            F.coalesce(F.col("ln.insuredTermDate"),
                       F.col("ln.InsuredTermDate"),
                       F.lit(None).cast("string")).cast("string").alias("insured_term_date"),
            F.coalesce(F.col("ln.manipulationReason"),
                       F.col("ln.ManipulationReason"),
                       F.lit(None).cast("string")).cast("string").alias("manipulation_reason"),
            F.coalesce(F.col("ln.claimDetailStatus"),
                       F.col("ln.ClaimDetailStatus"),
                       F.lit(None).cast("string")).cast("string").alias("claim_detail_status"),
            F.coalesce(F.col("ln.clientDataFeedCode"),
                       F.col("ln.ClientDataFeedCode"),
                       F.lit(None).cast("string")).cast("string").alias("client_data_feed_code"),
            F.coalesce(F.col("ln.inboundBatchMasterKey"),
                       F.col("ln.InboundBatchMasterKey"),
                       F.lit(None).cast("long")).cast("long").alias("inbound_batch_master_key"),
            F.coalesce(F.col("ln.batchRunSequence"),
                       F.col("ln.BatchRunSequence"),
                       F.lit(None).cast("int")).cast("int").alias("batch_run_sequence"),
            F.coalesce(F.col("ln.stageClaimLineKey"),
                       F.col("ln.StageClaimLineKey"),
                       F.lit(None).cast("long")).cast("long").alias("stage_claim_line_key"),
            normalize_ts(
                F.coalesce(
                    F.col("ln").getField("updatedAt"),
                    F.col("ln").getField("UpdatedAt"),
                    F.lit(None)
                )
            ).alias("updated_at"),
            normalize_ts(
                F.coalesce(
                    F.col("ln").getField("createdAt"),
                    F.col("ln").getField("CreatedAt"),
                    F.lit(None)
                )
            ).alias("created_at"),
            # Row-level epoch; fall back to parent claim epoch
            F.coalesce(
                to_epoch_ms(F.col("ln").getField("updatedAt")),
                to_epoch_ms(F.col("ln").getField("UpdatedAt")),
                F.col("claim_updated_at_epoch_ms"),
                F.lit(0).cast("long")
            ).alias("updated_at_epoch_ms"),
        )

        # Within-batch dedup: keep latest per (payer, member, claim, line number)
        _wln = Window.partitionBy(
            "payer_key", "load_year", "load_month", "member_key", "claim_key", "claim_line_number"
        ).orderBy(F.coalesce(F.col("updated_at_epoch_ms"), F.lit(0)).desc())
        lines_df = (
            lines_df
            .withColumn("_rn", F.row_number().over(_wln))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "claim_updated_at_epoch_ms")
        )

        print(f"[STEP 9] Claim lines rows: {lines_df.count()}")

    except Exception as e:
        raise Exception(f"[STEP 9] FAILED building lines dataframe: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 10 — CREATE ICEBERG TABLES & MERGE
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 10] Merging into Iceberg tables...")

    claims_df.createOrReplaceTempView("claims_stage")
    diagnosis_df.createOrReplaceTempView("diagnosis_stage")
    lines_df.createOrReplaceTempView("lines_stage")
    print("[STEP 10A] Creating Iceberg tables if not exist...")

    def _table_exists(database, table_name):
        try:
            spark.sql(f"DESCRIBE TABLE glue_catalog.{database}.{table_name}")
            return True
        except Exception:
            return False

    def _safe_create_table(sql_ddl, table_label):
        try:
            spark.sql(sql_ddl)
            print(f"[STEP 10A] {table_label} table ready ✅")
        except Exception as e:
            err_str = str(e)
            if ("already exists" in err_str.lower()
                    or "nosuchkey" in err_str.lower()
                    or "table already exists" in err_str.lower()):
                print(f"[STEP 10A] {table_label} already exists — continuing ✅")
            else:
                print(f"[STEP 10A] {table_label} warning: {e}")

    # ── claims ────────────────────────────────────────────────────────────────
    if not _table_exists(DATABASE, "claims"):
        _safe_create_table(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claims (
                payer_key                    bigint,
                load_year                    int,
                load_month                   int,
                member_key                   bigint,
                claim_key                    bigint,
                employer_group_key           bigint,
                inbound_batch_master_key     bigint,
                batch_run_sequence           int,
                stage_claim_key              bigint,
                claim_number                 string,
                claim_status                 string,
                claim_source                 string,
                claim_type                   string,
                claim_method                 string,
                form_type                    string,
                type_of_bill                 string,
                client_data_feed_code        string,
                source_system_id             string,
                plan_type                    string,
                union_type                   string,
                hospital_account_number      string,
                patient_discharge_status     string,
                place_of_service             string,
                prior_claim_reference        string,
                manipulation_reason          string,
                billing_provider_name        string,
                billing_provider_tin         string,
                billing_provider_npi         string,
                billing_provider_id          string,
                billing_provider_address1    string,
                billing_provider_address2    string,
                billing_provider_city        string,
                billing_provider_state       string,
                billing_provider_zip         string,
                billing_provider_phone       string,
                billing_provider_email       string,
                billing_provider_contact_name   string,
                billing_provider_contact_phone  string,
                treating_physician_name      string,
                treating_provider_tin        string,
                treating_provider_medicare   string,
                referring_provider_tin       string,
                admit_provider_tin           string,
                physician_provider_tin       string,
                provider_type                string,
                provider_class               string,
                reimbursement_method         string,
                is_capitated_claim           string,
                is_medicare                  string,
                is_split_claim               string,
                is_workers_comp              string,
                is_participating_provider    string,
                is_encounter                 string,
                adjustment_indicator         string,
                assignment_flag              string,
                accident_flag                string,
                include_encounter_as_paid    string,
                total_billed_amount          decimal(18,2),
                total_client_paid_amount     decimal(18,2),
                total_member_paid_amount     decimal(18,2),
                check_number                 string,
                interest_allowed             string,
                interest_claim_key           bigint,
                encounter_claim_key          bigint,
                encounter_related            string,
                encounter_unrelated          string,
                encounter_claim_requested    string,
                service_begin_date           date,
                service_thru_date            date,
                date_paid                    date,
                claim_received_date          date,
                claim_load_datetime          timestamp,
                claim_transferred_datetime   timestamp,
                created_at                   timestamp,
                updated_at                   timestamp,
                updated_at_epoch             bigint,
                legacy_source                string,
                legacy_schema                string,
                legacy_id                    string,
                tracking_info                string
            )
            USING iceberg
            PARTITIONED BY (payer_key, load_year, load_month)
            LOCATION 's3://{TARGET_BUCKET}/iceberg/claims'
            TBLPROPERTIES (
                'format-version'              = '2',
                'write.object-storage.enabled'= 'false'
            )
        """, "claims")
    else:
        print("[STEP 10A] claims table already exists — skipping CREATE ✅")

    # ── claim_diagnosis ───────────────────────────────────────────────────────
    if not _table_exists(DATABASE, "claim_diagnosis"):
        _safe_create_table(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claim_diagnosis (
                payer_key                bigint,
                member_key               bigint,
                claim_key                bigint,
                load_year                int,
                load_month               int,
                diagnosis_code           string,
                diagnosis_order          int,
                is_primary               string,
                is_sensitive             int,
                is_trauma                int,
                version_indicator        int,
                client_data_feed_code    string,
                inbound_batch_master_key bigint,
                batch_run_sequence       int,
                claim_diagnosis_key      bigint,
                updated_at               timestamp,
                updated_at_epoch_ms      bigint
            )
            USING iceberg
            PARTITIONED BY (payer_key, load_year, load_month)
            LOCATION 's3://{TARGET_BUCKET}/iceberg/claim_diagnosis'
            TBLPROPERTIES (
                'format-version'              = '2',
                'write.object-storage.enabled'= 'false'
            )
        """, "claim_diagnosis")
    else:
        print("[STEP 10A] claim_diagnosis table already exists — skipping CREATE ✅")

    # ── claim_lines ───────────────────────────────────────────────────────────
    if not _table_exists(DATABASE, "claim_lines"):
        _safe_create_table(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claim_lines (
                payer_key                    bigint,
                member_key                   bigint,
                claim_key                    bigint,
                load_year                    int,
                load_month                   int,
                claim_line_key               bigint,
                claim_line_number            string,
                procedure_code               string,
                procedure_code_type          string,
                billed_amount                decimal(18,2),
                client_paid_amount           decimal(18,2),
                member_paid                  decimal(18,2),
                allowed_amount               decimal(18,2),
                covered_amount               decimal(18,2),
                discount_amount              decimal(18,2),
                discount_reason              string,
                excluded_amount              decimal(18,2),
                excluded_reason              string,
                withhold_amount              decimal(18,2),
                withhold_reason              string,
                provider_paid_amount         decimal(18,2),
                original_client_paid_amount  decimal(18,2),
                previous_paid_amount         decimal(18,2),
                date_of_service_from         date,
                date_of_service_thru         date,
                modifier_code_01             string,
                modifier_code_02             string,
                place_of_service             string,
                revenue_code                 string,
                service_type                 string,
                quantity                     decimal(10,2),
                house_code                   string,
                house_code_description       string,
                payment_type                 string,
                payment_type_id              string,
                payment_comments             string,
                check_number                 string,
                transaction_code             string,
                transaction_description      string,
                adjustment_flag              string,
                is_primary_ndc               string,
                insured_term_date            string,
                manipulation_reason          string,
                claim_detail_status          string,
                client_data_feed_code        string,
                inbound_batch_master_key     bigint,
                batch_run_sequence           int,
                stage_claim_line_key         bigint,
                updated_at                   timestamp,
                created_at                   timestamp,
                updated_at_epoch_ms          bigint
            )
            USING iceberg
            PARTITIONED BY (payer_key, load_year, load_month)
            LOCATION 's3://{TARGET_BUCKET}/iceberg/claim_lines'
            TBLPROPERTIES (
                'format-version'              = '2',
                'write.object-storage.enabled'= 'false'
            )
        """, "claim_lines")
    else:
        print("[STEP 10A] claim_lines table already exists — skipping CREATE ✅")

    # ── Iceberg write helpers ─────────────────────────────────────────────────

    # boto3 Glue client — direct catalog API, never touches S3
    glue_client = boto3.client("glue")

    # DDL map — used to re-register a table after purging its stale catalog entry
    _TABLE_DDL = {
        f"glue_catalog.{DATABASE}.claims": f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claims (
                payer_key bigint, load_year int, load_month int,
                member_key bigint, claim_key bigint,
                employer_group_key bigint, inbound_batch_master_key bigint,
                batch_run_sequence int, stage_claim_key bigint,
                claim_number string, claim_status string, claim_source string,
                claim_type string, claim_method string, form_type string,
                type_of_bill string, client_data_feed_code string,
                source_system_id string, plan_type string, union_type string,
                hospital_account_number string, patient_discharge_status string,
                place_of_service string, prior_claim_reference string,
                manipulation_reason string, billing_provider_name string,
                billing_provider_tin string, billing_provider_npi string,
                billing_provider_id string, billing_provider_address1 string,
                billing_provider_address2 string, billing_provider_city string,
                billing_provider_state string, billing_provider_zip string,
                billing_provider_phone string, billing_provider_email string,
                billing_provider_contact_name string,
                billing_provider_contact_phone string,
                treating_physician_name string, treating_provider_tin string,
                treating_provider_medicare string, referring_provider_tin string,
                admit_provider_tin string, physician_provider_tin string,
                provider_type string, provider_class string,
                reimbursement_method string, is_capitated_claim string,
                is_medicare string, is_split_claim string,
                is_workers_comp string, is_participating_provider string,
                is_encounter string, adjustment_indicator string,
                assignment_flag string, accident_flag string,
                include_encounter_as_paid string,
                total_billed_amount decimal(18,2),
                total_client_paid_amount decimal(18,2),
                total_member_paid_amount decimal(18,2),
                check_number string, interest_allowed string,
                interest_claim_key bigint, encounter_claim_key bigint,
                encounter_related string, encounter_unrelated string,
                encounter_claim_requested string,
                service_begin_date date, service_thru_date date,
                date_paid date, claim_received_date date,
                claim_load_datetime timestamp, claim_transferred_datetime timestamp,
                created_at timestamp, updated_at timestamp,
                updated_at_epoch bigint,
                legacy_source string, legacy_schema string,
                legacy_id string, tracking_info string
            )
            USING iceberg
            PARTITIONED BY (payer_key, load_year, load_month)
            LOCATION 's3://{TARGET_BUCKET}/iceberg/claims'
            TBLPROPERTIES ('format-version'='2','write.object-storage.enabled'='false')
        """,
        f"glue_catalog.{DATABASE}.claim_diagnosis": f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claim_diagnosis (
                payer_key bigint, member_key bigint, claim_key bigint,
                load_year int, load_month int,
                diagnosis_code string, diagnosis_order int,
                is_primary string, is_sensitive int, is_trauma int,
                version_indicator int, client_data_feed_code string,
                inbound_batch_master_key bigint, batch_run_sequence int,
                claim_diagnosis_key bigint,
                updated_at timestamp, updated_at_epoch_ms bigint
            )
            USING iceberg
            PARTITIONED BY (payer_key, load_year, load_month)
            LOCATION 's3://{TARGET_BUCKET}/iceberg/claim_diagnosis'
            TBLPROPERTIES ('format-version'='2','write.object-storage.enabled'='false')
        """,
        f"glue_catalog.{DATABASE}.claim_lines": f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claim_lines (
                payer_key bigint, member_key bigint, claim_key bigint,
                load_year int, load_month int,
                claim_line_key bigint, claim_line_number string,
                procedure_code string, procedure_code_type string,
                billed_amount decimal(18,2), client_paid_amount decimal(18,2),
                member_paid decimal(18,2), allowed_amount decimal(18,2),
                covered_amount decimal(18,2), discount_amount decimal(18,2),
                discount_reason string, excluded_amount decimal(18,2),
                excluded_reason string, withhold_amount decimal(18,2),
                withhold_reason string, provider_paid_amount decimal(18,2),
                original_client_paid_amount decimal(18,2),
                previous_paid_amount decimal(18,2),
                date_of_service_from date, date_of_service_thru date,
                modifier_code_01 string, modifier_code_02 string,
                place_of_service string, revenue_code string,
                service_type string, quantity decimal(10,2),
                house_code string, house_code_description string,
                payment_type string, payment_type_id string,
                payment_comments string, check_number string,
                transaction_code string, transaction_description string,
                adjustment_flag string, is_primary_ndc string,
                insured_term_date string, manipulation_reason string,
                claim_detail_status string, client_data_feed_code string,
                inbound_batch_master_key bigint, batch_run_sequence int,
                stage_claim_line_key bigint,
                updated_at timestamp, created_at timestamp,
                updated_at_epoch_ms bigint
            )
            USING iceberg
            PARTITIONED BY (payer_key, load_year, load_month)
            LOCATION 's3://{TARGET_BUCKET}/iceberg/claim_lines'
            TBLPROPERTIES ('format-version'='2','write.object-storage.enabled'='false')
        """,
    }

    def _glue_drop_and_recreate(table_fqn, df, label):
        """
        Purge a stale Glue catalog entry using boto3 (no S3 access),
        re-register the table with a fresh DDL, then write data with insertInto.

        Why boto3 instead of spark.sql DROP TABLE:
          spark.sql('DROP TABLE') also reads S3 metadata before dropping —
          it throws the same NoSuchKeyException we are trying to escape.
          boto3 glue_client.delete_table() only touches the Glue catalog API,
          never S3, so it always succeeds regardless of S3 state.
        """
        table_name = table_fqn.split(".")[-1]   # e.g. "claims"
        try:
            glue_client.delete_table(DatabaseName=DATABASE, Name=table_name)
            print(f"[WARN] {label}: stale Glue catalog entry deleted via boto3 ✅")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"[WARN] {label}: table not in Glue catalog — nothing to delete")
        except Exception as del_err:
            print(f"[WARN] {label}: boto3 delete_table warning: {del_err}")

        # Re-register the table in Glue with a fresh DDL (seeds new S3 metadata)
        ddl = _TABLE_DDL.get(table_fqn)
        if not ddl:
            raise Exception(f"{label}: no DDL found for {table_fqn} — cannot recreate")
        spark.sql(ddl)
        print(f"[WARN] {label}: table re-registered in Glue catalog ✅")

        # Write data — insertInto works on a freshly created empty table
        df.writeTo(table_fqn).append()
        print(f"[INFO] {label}: data written to fresh table ✅")

    def _safe_overwrite_or_append(df, table_fqn, label):
        """
        Write df into table_fqn.

        Escalation ladder:
          1. overwritePartitions() — normal path
          2. append()              — new table, metadata not yet seeded
          3. boto3 delete + DDL re-register + append()
                                   — stale Glue catalog (S3 metadata gone);
                                     boto3 never touches S3 so it always works
        """
        def _is_nosuchkey(e):
            return any(x in str(e).lower() for x in ["nosuchkey", "no such key", "404"])

        try:
            df.writeTo(table_fqn).overwritePartitions()
            return
        except Exception as e1:
            if not _is_nosuchkey(e1):
                raise
            print(f"[WARN] {label}: overwritePartitions → NoSuchKey, trying append()")

        try:
            df.writeTo(table_fqn).append()
            return
        except Exception as e2:
            if not _is_nosuchkey(e2):
                raise
            print(f"[WARN] {label}: append() → NoSuchKey, using boto3 catalog repair")

        _glue_drop_and_recreate(table_fqn, df, label)

    def _table_has_data(table_fqn):
        """
        Returns:
          True    — table is readable and has data
          False   — table does not exist or is genuinely empty
          "stale" — table exists in Glue catalog but S3 metadata is missing
                    (NoSuchKeyException); both reads and writes will fail until
                    the catalog entry is dropped and recreated
        """
        try:
            spark.table(table_fqn).limit(1).count()
            return True
        except Exception as te:
            err_str = str(te).lower()
            if "nosuchkey" in err_str or "no such key" in err_str or "404" in err_str:
                return "stale"   # catalog exists, S3 metadata is gone
            if ("table not found" in err_str
                    or "nosuchnamespaceexception" in err_str
                    or "table or view not found" in err_str):
                return False
            raise

    def iceberg_upsert(stage_df, table_fqn, pk_cols, updated_at_col):
        """
        Upsert for the CLAIMS table (header rows).
        Row-level merge: keep the row with the highest updated_at_col per pk.
        Claims from previous runs that are NOT in this batch are preserved.
        """
        table_label    = table_fqn.split(".")[-1]
        partition_cols = ["payer_key", "load_year", "load_month"]

        if is_full_load:
            print(f"[INFO] {table_label}: FULL LOAD — overwriting all affected partitions")
            _safe_overwrite_or_append(stage_df, table_fqn, table_label)
            return

        table_state = _table_has_data(table_fqn)
        if table_state in (False, "stale"):
            print(f"[INFO] {table_label}: {'stale Glue metadata' if table_state == 'stale' else 'no existing data'} — going straight to write")
            _safe_overwrite_or_append(stage_df, table_fqn, table_label)
            return

        incoming_partitions = stage_df.select(partition_cols).distinct()
        try:
            existing_df    = spark.table(table_fqn).join(incoming_partitions, on=partition_cols, how="inner")
            existing_count = existing_df.count()
            print(f"[INFO] {table_label}: existing rows in affected partitions = {existing_count}")
        except Exception as re:
            if any(x in str(re).lower() for x in ["nosuchkey", "no such key", "404"]):
                print(f"[WARN] {table_label}: S3 NoSuchKey — first write")
                _safe_overwrite_or_append(stage_df, table_fqn, table_label)
                return
            raise

        # Keep highest updated_at_col per PK across existing + incoming
        combined = existing_df.unionByName(stage_df)
        w = Window.partitionBy(pk_cols).orderBy(F.coalesce(F.col(updated_at_col), F.lit(0)).desc())
        merged = combined.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
        _safe_overwrite_or_append(merged, table_fqn, table_label)

    def iceberg_upsert_child(stage_df, table_fqn, updated_at_col):
        """
        Upsert for CLAIM_DIAGNOSIS and CLAIM_LINES (child rows).

        Because each {claimKey}.json contains the COMPLETE current list of
        lines/diagnoses for that claim, when a claim is re-delivered the
        incoming list is the authoritative truth — old child rows for that
        claim_key must be fully replaced, not merged row-by-row.

        Strategy (incremental):
          1. Read back existing rows for the affected partitions.
          2. EXCLUDE all existing rows whose claim_key appears in the
             incoming batch  →  those claims are being replaced wholesale.
          3. UNION the survivors with all incoming rows.
          4. Overwrite the partition.

        This correctly handles:
          • New claim   → inserted (no existing rows to exclude)
          • Updated claim → old lines/diagnoses dropped, new ones inserted
          • Removed line/diagnosis → not present in new file, so not re-added
          • Unrelated claims in same partition → preserved (not in incoming batch)
        """
        table_label    = table_fqn.split(".")[-1]
        partition_cols = ["payer_key", "load_year", "load_month"]

        if is_full_load:
            print(f"[INFO] {table_label}: FULL LOAD — overwriting all affected partitions")
            _safe_overwrite_or_append(stage_df, table_fqn, table_label)
            return

        table_state = _table_has_data(table_fqn)
        if table_state in (False, "stale"):
            print(f"[INFO] {table_label}: {'stale Glue metadata' if table_state == 'stale' else 'no existing data'} — going straight to write")
            _safe_overwrite_or_append(stage_df, table_fqn, table_label)
            return

        incoming_partitions = stage_df.select(partition_cols).distinct()

        # Distinct claim_keys being re-delivered in this batch
        incoming_claims = stage_df.select(
            "payer_key", "load_year", "load_month", "member_key", "claim_key"
        ).distinct()

        try:
            existing_df    = spark.table(table_fqn).join(incoming_partitions, on=partition_cols, how="inner")
            existing_count = existing_df.count()
            print(f"[INFO] {table_label}: existing rows in affected partitions = {existing_count}")
        except Exception as re:
            if any(x in str(re).lower() for x in ["nosuchkey", "no such key", "404"]):
                print(f"[WARN] {table_label}: S3 NoSuchKey — first write")
                _safe_overwrite_or_append(stage_df, table_fqn, table_label)
                return
            raise

        # Keep existing rows ONLY for claims NOT in the incoming batch
        # (anti-join on claim_key within the same partition)
        survivors = existing_df.join(
            incoming_claims,
            on=["payer_key", "load_year", "load_month", "member_key", "claim_key"],
            how="left_anti"   # exclude rows whose claim_key is being replaced
        )
        survivor_count = survivors.count()
        print(f"[INFO] {table_label}: survivor rows (unaffected claims) = {survivor_count}")
        print(f"[INFO] {table_label}: incoming rows (replacing)         = {stage_df.count()}")

        # Merge: unaffected existing rows + full incoming list
        merged = survivors.unionByName(stage_df)
        _safe_overwrite_or_append(merged, table_fqn, table_label)

    # ── UPSERT: claims ────────────────────────────────────────────────────────
    try:
        iceberg_upsert(
            stage_df       = claims_df,
            table_fqn      = f"glue_catalog.{DATABASE}.claims",
            pk_cols        = ["payer_key", "load_year", "load_month", "member_key", "claim_key"],
            updated_at_col = "updated_at_epoch"
        )
        print("[STEP 10] Claims UPSERT ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claims UPSERT: {e}")

    # ── UPSERT: claim_diagnosis ───────────────────────────────────────────────
    try:
        iceberg_upsert_child(
            stage_df       = diagnosis_df,
            table_fqn      = f"glue_catalog.{DATABASE}.claim_diagnosis",
            updated_at_col = "updated_at_epoch_ms"
        )
        print("[STEP 10] Diagnosis UPSERT ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED diagnosis UPSERT: {e}")

    # ── UPSERT: claim_lines ───────────────────────────────────────────────────
    try:
        iceberg_upsert_child(
            stage_df       = lines_df,
            table_fqn      = f"glue_catalog.{DATABASE}.claim_lines",
            updated_at_col = "updated_at_epoch_ms"
        )
        print("[STEP 10] Claim Lines UPSERT ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claim lines UPSERT: {e}")

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

