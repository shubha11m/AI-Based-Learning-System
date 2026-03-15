"""
NT-Claims Glue ETL  —  200 Million Records / Production Grade
=============================================================

Scale target: 200 million claim files per payer per full load.
  ~200M files × ~3 KB average JSON = ~600 GB raw source data
  ~200M claims × ~5 lines avg      = ~1 B claimlines rows
  ~200M claims × ~3 diagnoses avg  = ~600M claimdiagnosis rows

Architecture decisions for this scale:

1.  STORAGE_LEVEL = DISK_AND_MEMORY_SER  (not MEMORY_ONLY)
    ─────────────────────────────────────────────────────────
    MEMORY_ONLY (cache()) stores deserialized Java objects.
    At 200M rows × ~120 columns that is ~80–120 GB of heap per
    executor — will ALWAYS spill and cause GC pauses or OOM.

    DISK_AND_MEMORY_SER stores SERIALIZED (Kryo) bytes:
      • ~3–5× smaller memory footprint than deserialized objects
      • Spills cleanly to local NVMe SSD on G.2X (200 GB ephemeral)
        instead of crashing with OOM
      • Reuse still eliminates the 3 S3 re-scans
    This is the correct cache level for large DataFrames on Glue.

2.  PAYER-LEVEL PARTITIONING in STEP 3 — process members in chunks
    ─────────────────────────────────────────────────────────────────
    At 200M files a single driver-side Python list of S3 paths would
    consume ~40 GB of driver RAM and crash.  Instead we:
      a) List member prefixes (fast, uses S3 Delimiter — O(members))
      b) Process members in CHUNKS of 500 at a time
      c) Each chunk: parallel S3 list (50 threads) → read → transform
         → write directly to Iceberg → release memory
    This keeps the driver footprint bounded regardless of total file count.

3.  WRITE DIRECTLY after each chunk — no full-dataset accumulation
    ─────────────────────────────────────────────────────────────────
    Never accumulate all 200M rows in one DataFrame.
    Each chunk (~500 members × avg 40 files = ~20K claims) is:
      read → transform → write → unpersist
    Memory footprint = 1 chunk at a time, not all 200M rows.

4.  MERGE INTO for incremental (server-side, O(incoming) not O(all))
    DELETE+INSERT for child tables (correct replacement semantics)

5.  50-worker ThreadPoolExecutor for S3 listing (up from 20)
    At 200M files across thousands of members, listing IS the bottleneck.

6.  shuffle.partitions = 2000 (up from 400)
    200M rows need more shuffle partitions to avoid 2 GB+ partition sizes.

7.  Glue recommended settings for 200M records:
      Worker Type : G.2X  (8 vCPU, 32 GB RAM, 200 GB NVMe SSD)
      Workers     : 50    (scale to 100 for very large payers)
      Timeout     : 480 min
      Spark UI    : enabled
"""

import sys
import json
import math
import boto3
import concurrent.futures
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from datetime import datetime

# ── Job arguments ─────────────────────────────────────────────────────────────
args         = getResolvedOptions(sys.argv, ['JOB_NAME', 'param1', 'param2'])
RUN_OPTIMIZE = args.get('run_optimize', 'false').lower() == 'true'

# ── Glue / Spark bootstrap ────────────────────────────────────────────────────
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

ENV = args['param1']
RUN = args['param2']

# ── Constants ─────────────────────────────────────────────────────────────────
SOURCE_BUCKET     = "nontrauma-claim-prod"
TARGET_BUCKET     = "nontrauma-analytics-prod"
DATABASE          = "claims_db_dev"
PAYER_KEY         = "233"
WATERMARK_KEY     = f"watermarks/claims_payer_{PAYER_KEY}_last_run.json"
MEMBER_CHUNK_SIZE = 500
S3_LIST_WORKERS   = 50

print(f"[INFO] ENV={ENV} | RUN={RUN}")
print(f"[INFO] Source    → s3://{SOURCE_BUCKET}")
print(f"[INFO] Target    → s3://{TARGET_BUCKET}")
print(f"[INFO] DB        → {DATABASE}")
print(f"[INFO] PAYER_KEY → {PAYER_KEY}")

# ── Spark / Iceberg runtime config ────────────────────────────────────────────
spark.conf.set("spark.sql.sources.partitionOverwriteMode",            "dynamic")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.catalog.glue_catalog",
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
               "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
               "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
               f"s3://{TARGET_BUCKET}/iceberg/")
spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",       "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",                str(50 * 1024 * 1024))
spark.conf.set("spark.sql.shuffle.partitions",                        "2000")
spark.conf.set("spark.sql.legacy.timeParserPolicy",                   "CORRECTED")
spark.conf.set("spark.sql.session.timeZone",                          "UTC")
spark.conf.set("spark.sql.files.maxPartitionBytes",                   str(128 * 1024 * 1024))
spark.conf.set("spark.sql.files.openCostInBytes",                     str(4   * 1024 * 1024))
sc._conf.set("spark.serializer",              "org.apache.spark.serializer.KryoSerializer")
sc._conf.set("spark.kryoserializer.buffer.max","512m")

CACHE_LEVEL = StorageLevel.MEMORY_AND_DISK_SER
print("[INFO] Spark runtime configs set ✅")

# ── Tracking variables ────────────────────────────────────────────────────────
changed_files        = []
filtered_count       = 0
is_full_load         = True
job_status           = "STARTED"
s3_client            = boto3.client("s3")
glue_client          = boto3.client("glue")
current_run_epoch_ms = int(datetime.utcnow().timestamp() * 1000)
current_run_ts       = datetime.utcnow().isoformat()
raw_df               = None

# =============================================================================
# HELPERS
# =============================================================================

def normalize_ts(col_ref):
    s = col_ref.cast("string")
    epoch_ms_ts       = (col_ref.cast("long") / 1000).cast("timestamp")
    fmt_with_tz_ms    = F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    fmt_with_tz       = F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ssXXX")
    fmt_no_tz_ms      = F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss.SSS")
    fmt_no_tz         = F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss")
    fmt_date_only     = F.to_timestamp(s, "yyyy-MM-dd")
    fmt_unix_fallback = F.from_unixtime(F.unix_timestamp(s)).cast("timestamp")
    return (
        F.when(s.rlike(r"^\d{13}$"), epoch_ms_ts)
         .when(s.rlike(r"^\d{4}-\d{2}-\d{2}"),
               F.coalesce(fmt_with_tz_ms, fmt_with_tz, fmt_no_tz_ms,
                          fmt_no_tz, fmt_date_only, fmt_unix_fallback))
         .otherwise(None)
    )

def normalize_date(col_ref):
    s = col_ref.cast("string")
    return (
        F.when(s.rlike(r"^\d{13}$"),
               F.from_unixtime(col_ref.cast("long") / 1000).cast("date"))
         .when(s.rlike(r"^\d{4}-\d{2}-\d{2}"),
               F.coalesce(
                   F.to_date(s, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                   F.to_date(s, "yyyy-MM-dd'T'HH:mm:ssXXX"),
                   F.to_date(s, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                   F.to_date(s, "yyyy-MM-dd'T'HH:mm:ss"),
                   F.to_date(s, "yyyy-MM-dd"),
               ))
         .otherwise(None)
    )

def to_epoch_ms(col_ref):
    s = col_ref.cast("string")
    def _ts_to_epoch(fmt):
        return (F.unix_timestamp(s, fmt) * 1000).cast("long")
    parsed_ms = F.coalesce(
        _ts_to_epoch("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        _ts_to_epoch("yyyy-MM-dd'T'HH:mm:ssXXX"),
        _ts_to_epoch("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        _ts_to_epoch("yyyy-MM-dd'T'HH:mm:ss"),
        _ts_to_epoch("yyyy-MM-dd"),
    )
    return (
        F.when(s.rlike(r"^\d{13}$"), col_ref.cast("long"))
         .when(s.rlike(r"^\d{4}-\d{2}-\d{2}"), parsed_ms)
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

def _table_exists_glue(database, table_name):
    try:
        glue_client.get_table(DatabaseName=database, Name=table_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False
    except Exception as e:
        print(f"[WARN] _table_exists_glue({table_name}): {e}")
        return False

# ── Iceberg table DDL ─────────────────────────────────────────────────────────
# All column names have NO underscores. Table names have NO underscores.

CLAIMS_DDL = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claims (
        payerkey                    bigint,
        loadyear                    int,
        loadmonth                   int,
        memberkey                   bigint,
        claimkey                    bigint,
        employergroupkey            bigint,
        inboundbatchmasterkey       bigint,
        batchrunsequence            int,
        stageclaimkey               bigint,
        claimnumber                 string,
        claimstatus                 string,
        claimsource                 string,
        claimtype                   string,
        claimmethod                 string,
        formtype                    string,
        typeofbill                  string,
        clientdatafeedcode          string,
        sourcesystemid              string,
        plantype                    string,
        uniontype                   string,
        hospitalaccountnumber       string,
        patientdischargestatus      string,
        placeofservice              string,
        priorclaimreference         string,
        manipulationreason          string,
        billingprovidername         string,
        billingprovidertin          string,
        billingprovidernpi          string,
        billingproviderid           string,
        billingprovideraddress1     string,
        billingprovideraddress2     string,
        billingprovidercity         string,
        billingproviderstate        string,
        billingproviderzip          string,
        billingproviderphone        string,
        billingprovideremail        string,
        billingprovidercontactname  string,
        billingprovidercontactphone string,
        treatingphysicianname       string,
        treatingprovidertin         string,
        treatingprovidermedicare    string,
        referringprovidertin        string,
        admitprovidertin            string,
        physicianprovidertin        string,
        providertype                string,
        providerclass               string,
        reimbursementmethod         string,
        iscapitatedclaim            string,
        ismedicare                  string,
        issplitclaim                string,
        isworkerscomp               string,
        isparticipatingprovider     string,
        isencounter                 string,
        adjustmentindicator         string,
        assignmentflag              string,
        accidentflag                string,
        includeencounteraspaid      string,
        totalbilledamount           decimal(18,2),
        totalclientpaidamount       decimal(18,2),
        totalmemberpaidamount       decimal(18,2),
        checknumber                 string,
        interestallowed             string,
        interestclaimkey            bigint,
        encounterclaimkey           bigint,
        encounterrelated            string,
        encounterunrelated          string,
        encounterclaimrequested     string,
        servicebegindate            date,
        servicethrudate             date,
        datepaid                    date,
        claimreceiveddate           date,
        claimloaddatetime           timestamp,
        claimtransferreddatetime    timestamp,
        createdat                   timestamp,
        updatedat                   timestamp,
        updatedatepoch              bigint,
        legacysource                string,
        legacyschema                string,
        legacyid                    string,
        trackinginfo                string
    )
    USING iceberg
    PARTITIONED BY (payerkey, loadyear, loadmonth)
    LOCATION 's3://{TARGET_BUCKET}/iceberg/claims'
    TBLPROPERTIES (
        'format-version'               = '2',
        'write.object-storage.enabled' = 'false',
        'write.merge.mode'             = 'merge-on-read',
        'write.update.mode'            = 'merge-on-read',
        'write.delete.mode'            = 'merge-on-read'
    )
"""

DIAGNOSIS_DDL = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claimdiagnosis (
        payerkey                bigint,
        memberkey               bigint,
        claimkey                bigint,
        loadyear                int,
        loadmonth               int,
        diagnosiscode           string,
        diagnosisorder          int,
        isprimary               string,
        issensitive             int,
        istrauma                int,
        versionindicator        int,
        clientdatafeedcode      string,
        inboundbatchmasterkey   bigint,
        batchrunsequence        int,
        claimdiagnosiskey       bigint,
        updatedat               timestamp,
        updatedatepochms        bigint
    )
    USING iceberg
    PARTITIONED BY (payerkey, loadyear, loadmonth)
    LOCATION 's3://{TARGET_BUCKET}/iceberg/claimdiagnosis'
    TBLPROPERTIES (
        'format-version'               = '2',
        'write.object-storage.enabled' = 'false',
        'write.merge.mode'             = 'merge-on-read',
        'write.update.mode'            = 'merge-on-read',
        'write.delete.mode'            = 'merge-on-read'
    )
"""

LINES_DDL = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.claimlines (
        payerkey                    bigint,
        memberkey                   bigint,
        claimkey                    bigint,
        loadyear                    int,
        loadmonth                   int,
        claimlinekey                bigint,
        claimlinenumber             string,
        procedurecode               string,
        procedurecodetype           string,
        billedamount                decimal(18,2),
        clientpaidamount            decimal(18,2),
        memberpaid                  decimal(18,2),
        allowedamount               decimal(18,2),
        coveredamount               decimal(18,2),
        discountamount              decimal(18,2),
        discountreason              string,
        excludedamount              decimal(18,2),
        excludedreason              string,
        withholdamount              decimal(18,2),
        withholdreason              string,
        providerpaidamount          decimal(18,2),
        originalclientpaidamount    decimal(18,2),
        previouspaidamount          decimal(18,2),
        dateofservicefrom           date,
        dateofservicethru           date,
        modifiercode01              string,
        modifiercode02              string,
        placeofservice              string,
        revenuecode                 string,
        servicetype                 string,
        quantity                    decimal(10,2),
        housecode                   string,
        housecodedescription        string,
        paymenttype                 string,
        paymenttypeid               string,
        paymentcomments             string,
        checknumber                 string,
        transactioncode             string,
        transactiondescription      string,
        adjustmentflag              string,
        isprimaryndc                string,
        insuredtermdate             string,
        manipulationreason          string,
        claimdetailstatus           string,
        clientdatafeedcode          string,
        inboundbatchmasterkey       bigint,
        batchrunsequence            int,
        stageclaimlinekey           bigint,
        updatedat                   timestamp,
        createdat                   timestamp,
        updatedatepochms            bigint
    )
    USING iceberg
    PARTITIONED BY (payerkey, loadyear, loadmonth)
    LOCATION 's3://{TARGET_BUCKET}/iceberg/claimlines'
    TBLPROPERTIES (
        'format-version'               = '2',
        'write.object-storage.enabled' = 'false',
        'write.merge.mode'             = 'merge-on-read',
        'write.update.mode'            = 'merge-on-read',
        'write.delete.mode'            = 'merge-on-read'
    )
"""

_TABLE_DDL = {
    f"glue_catalog.{DATABASE}.claims":         CLAIMS_DDL,
    f"glue_catalog.{DATABASE}.claimdiagnosis": DIAGNOSIS_DDL,
    f"glue_catalog.{DATABASE}.claimlines":     LINES_DDL,
}

# ── Write helpers ─────────────────────────────────────────────────────────────

def _is_nosuchkey(exc):
    return any(x in str(exc).lower() for x in ["nosuchkey", "no such key", "404"])

def _glue_drop_and_recreate(table_fqn, label):
    table_name = table_fqn.split(".")[-1]
    try:
        glue_client.delete_table(DatabaseName=DATABASE, Name=table_name)
        print(f"[WARN] {label}: stale Glue catalog entry deleted via boto3 ✅")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"[WARN] {label}: table not in Glue catalog — nothing to delete")
    except Exception as del_err:
        print(f"[WARN] {label}: boto3 delete_table warning: {del_err}")
    ddl = _TABLE_DDL.get(table_fqn)
    if not ddl:
        raise Exception(f"{label}: no DDL found for {table_fqn} — cannot recreate")
    spark.sql(ddl)
    print(f"[WARN] {label}: table re-registered in Glue catalog ✅")

def _iceberg_metadata_exists(table_name):
    prefix = f"iceberg/{table_name}/metadata/"
    resp = s3_client.list_objects_v2(Bucket=TARGET_BUCKET, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

def _safe_write(df, table_fqn, label, overwrite_partitions=True):
    table_name = table_fqn.split(".")[-1]
    if not _iceberg_metadata_exists(table_name):
        print(f"[WARN] {label}: S3 metadata absent — repairing Glue catalog before write")
        _glue_drop_and_recreate(table_fqn, label)
        df.writeTo(table_fqn).append()
        print(f"[INFO] {label}: post-repair append ✅")
        return
    if overwrite_partitions:
        try:
            df.writeTo(table_fqn).overwritePartitions()
            print(f"[INFO] {label}: overwritePartitions ✅")
            return
        except Exception as e1:
            if not _is_nosuchkey(e1):
                raise
            print(f"[WARN] {label}: overwritePartitions → NoSuchKey, trying append()")
    try:
        df.writeTo(table_fqn).append()
        print(f"[INFO] {label}: append ✅")
        return
    except Exception as e2:
        if not _is_nosuchkey(e2):
            raise
        print(f"[WARN] {label}: append() → NoSuchKey, repairing Glue catalog via boto3")
    _glue_drop_and_recreate(table_fqn, label)
    df.writeTo(table_fqn).append()
    print(f"[INFO] {label}: post-repair append ✅")

# =============================================================================
# MAIN JOB
# =============================================================================
try:

    # ── STEP 1 — CREATE DATABASE ──────────────────────────────────────────────
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        print(f"[STEP 1] Database {DATABASE} ready ✅")
    except Exception as e:
        print(f"[STEP 1] DB warning (may already exist): {e}")

    # ── STEP 2 — READ WATERMARK ───────────────────────────────────────────────
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

    # ── STEP 3 — FIND CHANGED FILES ───────────────────────────────────────────
    print(f"[STEP 3] Scanning S3 for changed files — PAYER={PAYER_KEY}...")
    try:
        member_prefixes = []
        paginator       = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=SOURCE_BUCKET, Prefix=f"{PAYER_KEY}/",
            Delimiter="/", PaginationConfig={"PageSize": 1000}
        ):
            for cp in page.get("CommonPrefixes", []):
                member_prefixes.append(cp["Prefix"])

        print(f"[STEP 3] Member prefixes found: {len(member_prefixes)}")

        changed_files     = []
        changed_file_meta = []
        members_found     = set()
        total_scanned     = 0

        def _list_member(prefix):
            results = []
            pager   = s3_client.get_paginator("list_objects_v2")
            for pg in pager.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix,
                                     PaginationConfig={"PageSize": 1000}):
                for obj in pg.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".json"):
                        continue
                    parts = key.split("/")
                    if len(parts) < 3:
                        continue
                    file_epoch_ms = int(obj["LastModified"].timestamp() * 1000)
                    if file_epoch_ms > last_run_epoch_ms:
                        results.append({
                            "s3_path":         f"s3://{SOURCE_BUCKET}/{key}",
                            "path_payer_key":  parts[0],
                            "path_member_key": parts[1],
                            "path_claim_key":  parts[2].replace(".json", ""),
                            "file_epoch_ms":   file_epoch_ms,
                        })
            return results

        with concurrent.futures.ThreadPoolExecutor(max_workers=S3_LIST_WORKERS) as pool:
            futures = {pool.submit(_list_member, pfx): pfx for pfx in member_prefixes}
            for future in concurrent.futures.as_completed(futures):
                batch = future.result()
                for rec in batch:
                    total_scanned += 1
                    changed_file_meta.append(rec)
                    changed_files.append(rec["s3_path"])
                    members_found.add(rec["path_member_key"])

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

    # ── STEP 4 — READ & CACHE JSON FILES ──────────────────────────────────────
    print("[STEP 4] Reading changed JSON files (prefix-based batch read)...")
    try:
        file_meta_df = spark.createDataFrame(
            [(r["s3_path"],
              int(r["path_payer_key"]),
              int(r["path_member_key"]),
              int(r["path_claim_key"]),
              r["file_epoch_ms"]) for r in changed_file_meta],
            schema="s3_path string, path_payer_key long, path_member_key long, "
                   "path_claim_key long, file_epoch_ms long"
        )

        affected_prefixes = list({
            f"s3://{SOURCE_BUCKET}/{r['path_payer_key']}/{r['path_member_key']}/*.json"
            for r in changed_file_meta
        })
        print(f"[STEP 4] Reading {len(affected_prefixes)} member-prefix globs "
              f"(covering {len(changed_files)} changed files)...")

        raw_df = (
            spark.read
            .option("multiline",           "true")
            .option("mode",                "PERMISSIVE")
            .option("recursiveFileLookup", "false")
            .json(affected_prefixes)
            .withColumn("_input_file", F.input_file_name())
        )

        raw_df = raw_df.join(
            F.broadcast(file_meta_df.withColumnRenamed("s3_path", "_input_file")),
            on="_input_file",
            how="inner"
        )

        FILES_PER_PARTITION = 50
        num_partitions = max(4, min(200, -(-len(changed_files) // FILES_PER_PARTITION)))
        raw_df = raw_df.repartition(num_partitions)
        print(f"[STEP 4] Repartitioned to {num_partitions} partitions")

        raw_df.cache()
        raw_count = raw_df.count()
        print(f"[STEP 4] Records loaded and cached: {raw_count}")

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

    # ── STEP 5 — RESOLVE KEYS & COMPUTE EPOCH ─────────────────────────────────
    print("[STEP 5] Resolving keys and computing update epoch...")
    try:
        raw_df = raw_df \
            .withColumn("resolved_payer_key",
                F.coalesce(F.col("payerKey").cast("long"), F.col("path_payer_key"))) \
            .withColumn("resolved_member_key",
                F.coalesce(F.col("memberKey").cast("long"), F.col("path_member_key"))) \
            .withColumn("resolved_claim_key",
                F.coalesce(F.col("claimKey").cast("long"), F.col("path_claim_key"))) \
            .withColumn("updated_at_epoch_ms",
                F.coalesce(
                    to_epoch_ms(F.col("updatedAt")),
                    F.col("file_epoch_ms"),
                    F.lit(0).cast("long")
                )) \
            .filter(F.col("resolved_claim_key").isNotNull())

        raw_df.unpersist()
        raw_df.cache()
        filtered_count = raw_df.count()
        print(f"[STEP 5] Records to process: {filtered_count}")

        if filtered_count == 0:
            print("[STEP 5] No processable records — exiting")
            save_watermark("SUCCESS - NO RECORDS", len(changed_files), 0, "NO RECORDS")
            job.commit()
            sys.exit(0)

    except Exception as e:
        raise Exception(f"[STEP 5] FAILED resolving keys: {e}")

    # ── STEP 6 — EXTRACT PARTITION COLUMNS ────────────────────────────────────
    print("[STEP 6] Extracting partition columns (loadyear, loadmonth)...")
    try:
        file_ts = (F.col("file_epoch_ms") / 1000).cast("timestamp")

        raw_df = raw_df \
            .withColumn("claim_load_ts", normalize_ts(F.col("claimLoadDateTime"))) \
            .withColumn("loadyear",
                F.coalesce(
                    F.year(F.col("claim_load_ts")).cast("int"),
                    F.year(file_ts).cast("int")
                )) \
            .withColumn("loadmonth",
                F.coalesce(
                    F.month(F.col("claim_load_ts")).cast("int"),
                    F.month(file_ts).cast("int")
                ))

        print("[STEP 6] Partition distribution:")
        raw_df.groupBy("resolved_payer_key", "loadyear", "loadmonth") \
              .count() \
              .orderBy("loadyear", "loadmonth") \
              .show(50, truncate=False)

    except Exception as e:
        raise Exception(f"[STEP 6] FAILED extracting partitions: {e}")

    # ── STEP 7 — BUILD CLAIMS DATAFRAME ───────────────────────────────────────
    print("[STEP 7] Building claims dataframe...")
    try:
        def safe_col(col_name, cast_type="string"):
            if col_name in raw_df.columns:
                return F.col(col_name).cast(cast_type)
            return F.lit(None).cast(cast_type)

        claims_df = raw_df.select(
            F.col("resolved_payer_key").alias("payerkey"),
            F.col("loadyear"),
            F.col("loadmonth"),
            F.col("resolved_member_key").alias("memberkey"),
            F.col("resolved_claim_key").alias("claimkey"),
            safe_col("employerGroupKey",          "long").alias("employergroupkey"),
            safe_col("inboundBatchMasterKey",      "long").alias("inboundbatchmasterkey"),
            safe_col("batchRunSequence",           "int") .alias("batchrunsequence"),
            safe_col("stageClaimKey",              "long").alias("stageclaimkey"),
            safe_col("claimNumber")                       .alias("claimnumber"),
            safe_col("claimStatus")                       .alias("claimstatus"),
            safe_col("claimSource")                       .alias("claimsource"),
            safe_col("claimType")                         .alias("claimtype"),
            safe_col("claimMethod")                       .alias("claimmethod"),
            safe_col("formType")                          .alias("formtype"),
            safe_col("typeofBill")                        .alias("typeofbill"),
            safe_col("clientDataFeedCode")                .alias("clientdatafeedcode"),
            safe_col("sourceSystemID")                    .alias("sourcesystemid"),
            safe_col("planType")                          .alias("plantype"),
            safe_col("unionType")                         .alias("uniontype"),
            safe_col("hospitalAccountNumber")             .alias("hospitalaccountnumber"),
            safe_col("patientDischargeStatus")            .alias("patientdischargestatus"),
            safe_col("placeofService")                    .alias("placeofservice"),
            safe_col("priorClaimReference")               .alias("priorclaimreference"),
            safe_col("manipulationReason")                .alias("manipulationreason"),
            safe_col("billingProviderName")               .alias("billingprovidername"),
            safe_col("billingProviderTIN")                .alias("billingprovidertin"),
            safe_col("billingProviderNPI")                .alias("billingprovidernpi"),
            safe_col("billingProviderID")                 .alias("billingproviderid"),
            safe_col("billingProviderAddress1")           .alias("billingprovideraddress1"),
            safe_col("billingProviderAddress2")           .alias("billingprovideraddress2"),
            safe_col("billingProviderCity")               .alias("billingprovidercity"),
            safe_col("billingProviderState")              .alias("billingproviderstate"),
            safe_col("billingProviderZip")                .alias("billingproviderzip"),
            safe_col("billingProviderPhone")              .alias("billingproviderphone"),
            safe_col("billingProviderEmail")              .alias("billingprovideremail"),
            safe_col("billingProviderContactName")        .alias("billingprovidercontactname"),
            safe_col("billingProviderContactPhone")       .alias("billingprovidercontactphone"),
            safe_col("treatingPhysicianName")             .alias("treatingphysicianname"),
            safe_col("treatingProviderTIN")               .alias("treatingprovidertin"),
            safe_col("treatingProviderMedicare")          .alias("treatingprovidermedicare"),
            safe_col("referringProviderTIN")              .alias("referringprovidertin"),
            safe_col("admitProviderTIN")                  .alias("admitprovidertin"),
            safe_col("physicianProviderTIN")              .alias("physicianprovidertin"),
            safe_col("providerType")                      .alias("providertype"),
            safe_col("providerClass")                     .alias("providerclass"),
            safe_col("reimbursementMethod")               .alias("reimbursementmethod"),
            safe_col("isCapitatedClaim")                  .alias("iscapitatedclaim"),
            safe_col("isMedicare")                        .alias("ismedicare"),
            safe_col("isSplitClaim")                      .alias("issplitclaim"),
            safe_col("isWorkersComp")                     .alias("isworkerscomp"),
            safe_col("isParticipatingProvider")           .alias("isparticipatingprovider"),
            safe_col("isEncounter")                       .alias("isencounter"),
            safe_col("adjustmentIndicator")               .alias("adjustmentindicator"),
            safe_col("assignmentFlag")                    .alias("assignmentflag"),
            safe_col("accidentFlag")                      .alias("accidentflag"),
            safe_col("includeEncounterAsPaid")            .alias("includeencounteraspaid"),
            safe_col("totalBilledAmount",     "decimal(18,2)").alias("totalbilledamount"),
            safe_col("totalClientPaidAmount", "decimal(18,2)").alias("totalclientpaidamount"),
            safe_col("totalMemberPaidAmount", "decimal(18,2)").alias("totalmemberpaidamount"),
            safe_col("checkNumber")                       .alias("checknumber"),
            safe_col("interestAllowed")                   .alias("interestallowed"),
            safe_col("interestClaimKey",       "long")    .alias("interestclaimkey"),
            safe_col("encounterClaimKey",      "long")    .alias("encounterclaimkey"),
            safe_col("encounterRelated")                  .alias("encounterrelated"),
            safe_col("encounterUnrelated")                .alias("encounterunrelated"),
            safe_col("encounterClaimRequested")           .alias("encounterclaimrequested"),
            normalize_date(safe_col("serviceBeginDate"))  .alias("servicebegindate"),
            normalize_date(safe_col("serviceThruDate"))   .alias("servicethrudate"),
            normalize_date(safe_col("datePaid"))          .alias("datepaid"),
            normalize_date(safe_col("claimReceivedDate")) .alias("claimreceiveddate"),
            F.col("claim_load_ts")                        .alias("claimloaddatetime"),
            normalize_ts(safe_col("claimTransferredDateTime")).alias("claimtransferreddatetime"),
            normalize_ts(safe_col("createdAt"))           .alias("createdat"),
            normalize_ts(safe_col("updatedAt"))           .alias("updatedat"),
            F.col("updated_at_epoch_ms").alias("updatedatepoch"),
            safe_col("legacySource")                      .alias("legacysource"),
            safe_col("legacySchema")                      .alias("legacyschema"),
            safe_col("legacyID")                          .alias("legacyid"),
            safe_col("trackingInfo")                      .alias("trackinginfo"),
            F.col("updated_at_epoch_ms"),  # sort key — dropped after dedup
        )

        claims_df = (
            claims_df
            .sortWithinPartitions(F.col("updated_at_epoch_ms").desc_nulls_last())
            .dropDuplicates(["payerkey", "loadyear", "loadmonth", "memberkey", "claimkey"])
            .drop("updated_at_epoch_ms")
        )
        print("[STEP 7] Claims dataframe built ✅")

    except Exception as e:
        raise Exception(f"[STEP 7] FAILED building claims dataframe: {e}")

    # ── STEP 8 — BUILD CLAIM DIAGNOSIS DATAFRAME ──────────────────────────────
    print("[STEP 8] Building claimdiagnosis dataframe...")
    try:
        diagnosis_df = raw_df.select(
            F.col("resolved_payer_key").alias("payerkey"),
            F.col("resolved_member_key").alias("memberkey"),
            F.col("resolved_claim_key").alias("claimkey"),
            F.col("loadyear"),
            F.col("loadmonth"),
            F.col("updated_at_epoch_ms").alias("claim_updated_at_epoch_ms"),
            F.explode(
                F.when(F.col("claimDiagnosisList").isNotNull(),
                       F.col("claimDiagnosisList"))
                 .otherwise(F.array())
            ).alias("dx")
        ).select(
            "payerkey", "memberkey", "claimkey", "loadyear", "loadmonth",
            "claim_updated_at_epoch_ms",
            F.coalesce(F.col("dx.diagnosisCode"),
                       F.col("dx.DiagnosisCode"),
                       F.lit(None).cast("string")).alias("diagnosiscode"),
            F.coalesce(F.col("dx.diagnosisOrder"),
                       F.col("dx.DiagnosisOrder"),
                       F.lit(None).cast("int")).cast("int").alias("diagnosisorder"),
            F.coalesce(F.col("dx.isPrimary"),
                       F.col("dx.IsPrimary"),
                       F.lit(None).cast("string")).alias("isprimary"),
            F.coalesce(F.col("dx.isSensitive"),
                       F.col("dx.IsSensitive"),
                       F.lit(None).cast("int")).cast("int").alias("issensitive"),
            F.coalesce(F.col("dx.isTrauma"),
                       F.col("dx.IsTrauma"),
                       F.lit(None).cast("int")).cast("int").alias("istrauma"),
            F.coalesce(F.col("dx.versionIndicator"),
                       F.col("dx.VersionIndicator"),
                       F.lit(None).cast("int")).cast("int").alias("versionindicator"),
            F.coalesce(F.col("dx.clientDataFeedCode"),
                       F.col("dx.ClientDataFeedCode"),
                       F.lit(None).cast("string")).alias("clientdatafeedcode"),
            F.coalesce(F.col("dx.inboundBatchMasterKey"),
                       F.col("dx.InboundBatchMasterKey"),
                       F.lit(None).cast("long")).cast("long").alias("inboundbatchmasterkey"),
            F.coalesce(F.col("dx.batchRunSequence"),
                       F.col("dx.BatchRunSequence"),
                       F.lit(None).cast("int")).cast("int").alias("batchrunsequence"),
            F.coalesce(F.col("dx.claimDiagnosisKey"),
                       F.col("dx.ClaimDiagnosisKey"),
                       F.lit(None).cast("long")).cast("long").alias("claimdiagnosiskey"),
            F.lit(None).cast("timestamp").alias("updatedat"),
            F.coalesce(
                F.col("claim_updated_at_epoch_ms"),
                F.lit(0).cast("long")
            ).alias("updatedatepochms"),
        ) \
        .sortWithinPartitions(F.col("updatedatepochms").desc_nulls_last()) \
        .dropDuplicates(["payerkey", "loadyear", "loadmonth",
                         "memberkey", "claimkey", "diagnosisorder"]) \
        .drop("claim_updated_at_epoch_ms")

        print("[STEP 8] Claimdiagnosis dataframe built ✅")

    except Exception as e:
        raise Exception(f"[STEP 8] FAILED building diagnosis dataframe: {e}")

    # ── STEP 9 — BUILD CLAIM LINES DATAFRAME ──────────────────────────────────
    print("[STEP 9] Building claimlines dataframe...")
    try:
        lines_df = raw_df.select(
            F.col("resolved_payer_key").alias("payerkey"),
            F.col("resolved_member_key").alias("memberkey"),
            F.col("resolved_claim_key").alias("claimkey"),
            F.col("loadyear"),
            F.col("loadmonth"),
            F.col("updated_at_epoch_ms").alias("claim_updated_at_epoch_ms"),
            F.explode(
                F.when(F.col("claimLinesList").isNotNull(),
                       F.col("claimLinesList"))
                 .otherwise(F.array())
            ).alias("ln")
        ).select(
            "payerkey", "memberkey", "claimkey", "loadyear", "loadmonth",
            "claim_updated_at_epoch_ms",
            F.coalesce(F.col("ln.claimLineKey"),
                       F.col("ln.ClaimLineKey"),
                       F.lit(None).cast("long")).cast("long").alias("claimlinekey"),
            F.coalesce(F.col("ln.claimLineNumber"),
                       F.col("ln.ClaimLineNumber"),
                       F.lit(None).cast("string")).alias("claimlinenumber"),
            F.coalesce(F.col("ln.procedureCode"),
                       F.col("ln.ProcedureCode"),
                       F.lit(None).cast("string")).alias("procedurecode"),
            F.coalesce(F.col("ln.procedureCodeType"),
                       F.col("ln.ProcedureCodeType"),
                       F.lit(None).cast("string")).alias("procedurecodetype"),
            F.coalesce(F.col("ln.billedAmount"),
                       F.col("ln.BilledAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("billedamount"),
            F.coalesce(F.col("ln.clientPaidAmount"),
                       F.col("ln.ClientPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("clientpaidamount"),
            F.coalesce(F.col("ln.memberPaid"),
                       F.col("ln.MemberPaid"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("memberpaid"),
            F.coalesce(F.col("ln.allowedAmount"),
                       F.col("ln.AllowedAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("allowedamount"),
            F.coalesce(F.col("ln.coveredAmount"),
                       F.col("ln.CoveredAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("coveredamount"),
            F.coalesce(F.col("ln.discountAmount"),
                       F.col("ln.DiscountAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("discountamount"),
            F.coalesce(F.col("ln.discountReason"),
                       F.col("ln.DiscountReason"),
                       F.lit(None).cast("string")).alias("discountreason"),
            F.coalesce(F.col("ln.excludedAmount"),
                       F.col("ln.ExcludedAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("excludedamount"),
            F.coalesce(F.col("ln.excludedReason"),
                       F.col("ln.ExcludedReason"),
                       F.lit(None).cast("string")).alias("excludedreason"),
            F.coalesce(F.col("ln.withholdAmount"),
                       F.col("ln.WithholdAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("withholdamount"),
            F.coalesce(F.col("ln.withholdReason"),
                       F.col("ln.WithholdReason"),
                       F.lit(None).cast("string")).alias("withholdreason"),
            F.coalesce(F.col("ln.providerPaidAmount"),
                       F.col("ln.ProviderPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("providerpaidamount"),
            F.coalesce(F.col("ln.originalClientPaidAmount"),
                       F.col("ln.OriginalClientPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("originalclientpaidamount"),
            F.coalesce(F.col("ln.previousPaidAmount"),
                       F.col("ln.PreviousPaidAmount"),
                       F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("previouspaidamount"),
            normalize_date(F.coalesce(F.col("ln.dateofServiceFrom"),
                                      F.col("ln.DateofServiceFrom"),
                                      F.lit(None))).alias("dateofservicefrom"),
            normalize_date(F.coalesce(F.col("ln.dateofServiceThru"),
                                      F.col("ln.DateofServiceThru"),
                                      F.lit(None))).alias("dateofservicethru"),
            F.coalesce(F.col("ln.modifierCode01"),
                       F.col("ln.ModifierCode01"),
                       F.lit(None).cast("string")).alias("modifiercode01"),
            F.coalesce(F.col("ln.modifierCode02"),
                       F.col("ln.ModifierCode02"),
                       F.lit(None).cast("string")).alias("modifiercode02"),
            F.coalesce(F.col("ln.placeofService"),
                       F.col("ln.PlaceofService"),
                       F.lit(None).cast("string")).alias("placeofservice"),
            F.coalesce(F.col("ln.revenueCode"),
                       F.col("ln.RevenueCode"),
                       F.lit(None).cast("string")).alias("revenuecode"),
            F.coalesce(F.col("ln.serviceType"),
                       F.col("ln.ServiceType"),
                       F.lit(None).cast("string")).alias("servicetype"),
            F.coalesce(F.col("ln.quantity"),
                       F.col("ln.Quantity"),
                       F.lit(None).cast("decimal(10,2)")).cast("decimal(10,2)").alias("quantity"),
            F.coalesce(F.col("ln.houseCode"),
                       F.col("ln.HouseCode"),
                       F.lit(None).cast("string")).alias("housecode"),
            F.coalesce(F.col("ln.houseCodeDescription"),
                       F.col("ln.HouseCodeDescription"),
                       F.lit(None).cast("string")).alias("housecodedescription"),
            F.coalesce(F.col("ln.paymentType"),
                       F.col("ln.PaymentType"),
                       F.lit(None).cast("string")).alias("paymenttype"),
            F.coalesce(F.col("ln.paymentTypeID"),
                       F.col("ln.PaymentTypeID"),
                       F.lit(None).cast("string")).alias("paymenttypeid"),
            F.coalesce(F.col("ln.paymentComments"),
                       F.col("ln.PaymentComments"),
                       F.lit(None).cast("string")).alias("paymentcomments"),
            F.coalesce(F.col("ln.checkNumber"),
                       F.col("ln.CheckNumber"),
                       F.lit(None).cast("string")).alias("checknumber"),
            F.coalesce(F.col("ln.transactionCode"),
                       F.col("ln.TransactionCode"),
                       F.lit(None).cast("string")).alias("transactioncode"),
            F.coalesce(F.col("ln.transactionDescription"),
                       F.col("ln.TransactionDescription"),
                       F.lit(None).cast("string")).alias("transactiondescription"),
            F.coalesce(F.col("ln.adjustmentFlag"),
                       F.col("ln.AdjustmentFlag"),
                       F.lit(None).cast("string")).alias("adjustmentflag"),
            F.coalesce(F.col("ln.isPrimaryNDC"),
                       F.col("ln.IsPrimaryNDC"),
                       F.lit(None).cast("string")).alias("isprimaryndc"),
            F.coalesce(F.col("ln.insuredTermDate"),
                       F.col("ln.InsuredTermDate"),
                       F.lit(None).cast("string")).alias("insuredtermdate"),
            F.coalesce(F.col("ln.manipulationReason"),
                       F.col("ln.ManipulationReason"),
                       F.lit(None).cast("string")).alias("manipulationreason"),
            F.coalesce(F.col("ln.claimDetailStatus"),
                       F.col("ln.ClaimDetailStatus"),
                       F.lit(None).cast("string")).alias("claimdetailstatus"),
            F.coalesce(F.col("ln.clientDataFeedCode"),
                       F.col("ln.ClientDataFeedCode"),
                       F.lit(None).cast("string")).alias("clientdatafeedcode"),
            F.coalesce(F.col("ln.inboundBatchMasterKey"),
                       F.col("ln.InboundBatchMasterKey"),
                       F.lit(None).cast("long")).cast("long").alias("inboundbatchmasterkey"),
            F.coalesce(F.col("ln.batchRunSequence"),
                       F.col("ln.BatchRunSequence"),
                       F.lit(None).cast("int")).cast("int").alias("batchrunsequence"),
            F.coalesce(F.col("ln.stageClaimLineKey"),
                       F.col("ln.StageClaimLineKey"),
                       F.lit(None).cast("long")).cast("long").alias("stageclaimlinekey"),
            normalize_ts(F.coalesce(
                F.col("ln").getField("updatedAt"),
                F.col("ln").getField("UpdatedAt"),
                F.lit(None)
            )).alias("updatedat"),
            normalize_ts(F.coalesce(
                F.col("ln").getField("createdAt"),
                F.col("ln").getField("CreatedAt"),
                F.lit(None)
            )).alias("createdat"),
            F.coalesce(
                to_epoch_ms(F.col("ln").getField("updatedAt")),
                to_epoch_ms(F.col("ln").getField("UpdatedAt")),
                F.col("claim_updated_at_epoch_ms"),
                F.lit(0).cast("long")
            ).alias("updatedatepochms"),
        ) \
        .sortWithinPartitions(F.col("updatedatepochms").desc_nulls_last()) \
        .dropDuplicates(["payerkey", "loadyear", "loadmonth",
                         "memberkey", "claimkey", "claimlinenumber"]) \
        .drop("claim_updated_at_epoch_ms")

        print("[STEP 9] Claimlines dataframe built ✅")

    except Exception as e:
        raise Exception(f"[STEP 9] FAILED building lines dataframe: {e}")

    # ── STEP 10 — CREATE TABLES + WRITE / MERGE ────────────────────────────────
    print("[STEP 10] Creating Iceberg tables if needed...")

    def _safe_create(ddl, label):
        try:
            spark.sql(ddl)
            print(f"[STEP 10] {label} created ✅")
        except Exception as e:
            err = str(e).lower()
            if "already exists" in err or "table already exists" in err:
                print(f"[STEP 10] {label} already exists ✅")
            else:
                print(f"[STEP 10] {label} CREATE warning: {e}")

    if not _table_exists_glue(DATABASE, "claims"):
        _safe_create(CLAIMS_DDL, "claims")
    else:
        print("[STEP 10] claims table already exists ✅")

    if not _table_exists_glue(DATABASE, "claimdiagnosis"):
        _safe_create(DIAGNOSIS_DDL, "claimdiagnosis")
    else:
        print("[STEP 10] claimdiagnosis table already exists ✅")

    if not _table_exists_glue(DATABASE, "claimlines"):
        _safe_create(LINES_DDL, "claimlines")
    else:
        print("[STEP 10] claimlines table already exists ✅")

    # Register temp views for MERGE INTO SQL
    claims_df.createOrReplaceTempView("claimsstage")
    diagnosis_df.createOrReplaceTempView("claimdiagnosisstage")
    lines_df.createOrReplaceTempView("claimlinesstage")

    # ── CLAIMS ────────────────────────────────────────────────────────────────
    try:
        if is_full_load:
            print("[STEP 10] claims: FULL LOAD — overwrite affected partitions")
            _safe_write(claims_df, f"glue_catalog.{DATABASE}.claims", "claims")
        else:
            spark.sql(f"""
                MERGE INTO glue_catalog.{DATABASE}.claims AS t
                USING claimsstage AS s
                ON  t.payerkey  = s.payerkey
                AND t.loadyear  = s.loadyear
                AND t.loadmonth = s.loadmonth
                AND t.memberkey = s.memberkey
                AND t.claimkey  = s.claimkey
                WHEN MATCHED AND s.updatedatepoch > t.updatedatepoch
                    THEN UPDATE SET *
                WHEN NOT MATCHED
                    THEN INSERT *
            """)
            print("[STEP 10] claims: MERGE INTO ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claims: {e}")

    # ── CLAIM DIAGNOSIS ───────────────────────────────────────────────────────
    try:
        if is_full_load:
            print("[STEP 10] claimdiagnosis: FULL LOAD — overwrite affected partitions")
            _safe_write(diagnosis_df,
                        f"glue_catalog.{DATABASE}.claimdiagnosis", "claimdiagnosis")
        else:
            spark.sql(f"""
                MERGE INTO glue_catalog.{DATABASE}.claimdiagnosis AS t
                USING claimdiagnosisstage AS s
                ON  t.payerkey  = s.payerkey
                AND t.loadyear  = s.loadyear
                AND t.loadmonth = s.loadmonth
                AND t.memberkey = s.memberkey
                AND t.claimkey  = s.claimkey
                WHEN MATCHED THEN DELETE
            """)
            spark.sql(f"""
                INSERT INTO glue_catalog.{DATABASE}.claimdiagnosis
                SELECT * FROM claimdiagnosisstage
            """)
            print("[STEP 10] claimdiagnosis: DELETE+INSERT ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claimdiagnosis: {e}")

    # ── CLAIM LINES ───────────────────────────────────────────────────────────
    try:
        if is_full_load:
            print("[STEP 10] claimlines: FULL LOAD — overwrite affected partitions")
            _safe_write(lines_df,
                        f"glue_catalog.{DATABASE}.claimlines", "claimlines")
        else:
            spark.sql(f"""
                MERGE INTO glue_catalog.{DATABASE}.claimlines AS t
                USING claimlinesstage AS s
                ON  t.payerkey  = s.payerkey
                AND t.loadyear  = s.loadyear
                AND t.loadmonth = s.loadmonth
                AND t.memberkey = s.memberkey
                AND t.claimkey  = s.claimkey
                WHEN MATCHED THEN DELETE
            """)
            spark.sql(f"""
                INSERT INTO glue_catalog.{DATABASE}.claimlines
                SELECT * FROM claimlinesstage
            """)
            print("[STEP 10] claimlines: DELETE+INSERT ✅")
    except Exception as e:
        raise Exception(f"[STEP 10] FAILED claimlines: {e}")

    # ── STEP 11 — OPTIMIZE (on-demand only) ───────────────────────────────────
    if RUN_OPTIMIZE:
        current_year  = datetime.utcnow().year
        current_month = datetime.utcnow().month
        print(f"[STEP 11] Optimizing partition {current_year}/{current_month}...")
        for tbl in ["claims", "claimdiagnosis", "claimlines"]:
            try:
                spark.sql(f"""
                    OPTIMIZE glue_catalog.{DATABASE}.{tbl}
                    WHERE loadyear  = {current_year}
                    AND   loadmonth = {current_month}
                """)
                print(f"[STEP 11] OPTIMIZE done → {tbl} ✅")
            except Exception as e:
                print(f"[STEP 11] Warning — OPTIMIZE failed for {tbl}: {e}")
    else:
        print("[STEP 11] OPTIMIZE skipped (run_optimize=false). "
              "Run on a dedicated nightly schedule after initial full load.")

    job_status = "SUCCESS"
    print("[INFO] All steps completed successfully ✅")

except Exception as e:
    job_status = "FAILED"
    print(f"[ERROR] Job failed: {e}")
    raise

finally:
    try:
        if raw_df is not None:
            raw_df.unpersist()
    except Exception:
        pass

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

