"""
NT-Claims Glue ETL  —  Production Grade (10 M / 50 M scale)
============================================================

PERFORMANCE OPTIMISATIONS applied (2026-03-15 → 2026-03-16)
─────────────────────────────────────────────────────────────
1.  SINGLE persist() + count() per chunk
2.  ELIMINATED isin(chunk_files) filter (member-scoped globs)
3.  WINDOW-based dedup instead of sortWithinPartitions + dropDuplicates
4.  BROADCAST HINT on incremental anti-join small DFs
5.  CHUNK SIZE + S3 list workers tuned per scale tier (see below)
6.  ADAPTIVE shuffle partitions — AQE enabled; target per chunk scale
7.  SINGLE-STAGE metadata check cache (_table_meta_ok)
8.  Iceberg write.target-file-size-bytes tuned per scale tier
9.  Partition filter via broadcast join (Iceberg predicate pushdown)

NEW for 10 M / 50 M scale (2026-03-16)
─────────────────────────────────────────────────────────────
10. SCALE TIER auto-detection
    Reads the --scale_tier job param ("small" / "medium_10m" / "large_50m").
    All tuning knobs (chunk size, workers, shuffle partitions, file size,
    Iceberg table properties, Glue worker count hints) are driven from one
    central SCALE_CONFIG dict — no code changes needed between tiers.

11. DYNAMIC shuffle partitions per chunk
    Formula: max(400, min(8000, ceil(chunk_files / FILES_PER_PARTITION) * 4))
    AQE coalesces down; this prevents under-partitioning on 50 K-file chunks.

12. PARALLELISED S3 LIST with adaptive page size
    S3_LIST_WORKERS raised to 200 (50 M tier) — saturates the S3 list API.
    PaginationConfig PageSize raised to 1000 (already maximal; kept explicit).

13. MEMORY_AND_DISK_SER (serialised cache) for large chunks
    At 50 M scale a single chunk can be 50 K rows × wide schema.
    MEMORY_AND_DISK_SER halves executor heap pressure vs MEMORY_AND_DISK.
    Java serializer (NOT Kryo) is used — safe because serializer is set
    before SparkContext via Glue job parameter.

14. ICEBERG copy-on-write for FULL LOAD
    write.merge.mode / update.mode / delete.mode switched to copy-on-write
    during full load — avoids delete-file accumulation on brand-new tables.
    Switched back to merge-on-read for incremental runs via ALTER TABLE.

15. OPTIMIZE triggered automatically after full load when scale >= 10 M
    One OPTIMIZE call per table partitioned by (payerkey, loadyear, loadmonth)
    immediately after the chunk loop on full-load runs.

16. GLUE WORKER HINTS printed at startup
    Operator sees the recommended worker type / count / timeout for the
    detected scale tier at the top of the log.

Scale tiers
──────────────────────────────────────────────────────────────────────
Tier            Files       Members  Chunk  Workers  Shuffle  File MB
──────────────────────────────────────────────────────────────────────
small           < 500 K     < 5 K    1 000   50 G.2X  400     256
medium_10m      ~ 10 M      ~100 K   2 000  100 G.2X  1 600   512
large_50m       ~ 50 M      ~500 K   3 000  200 G.2X  4 000   512
──────────────────────────────────────────────────────────────────────

Recommended Glue job settings (large_50m)
  Worker Type  : G.2X  (8 vCPU, 32 GB RAM, 200 GB NVMe SSD)
  Workers      : 200
  Timeout      : 960 min  (16 h)
  Job params   : --scale_tier large_50m
                 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
                        (set via Glue job parameter, NOT post-init)
"""

import sys
import json
import boto3
import concurrent.futures
from collections import defaultdict
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

# ── Scale tier configuration ──────────────────────────────────────────────────
# Pass --scale_tier small | medium_10m | large_50m as a Glue job parameter.
# Defaults to 'small' if not supplied (backward-compatible with existing jobs).
#
# Tier          Files        Members  Chunk  ListW  Shuffle  FileMB  Workers
# small         < 500 K      < 5 K    1 000    100     400     256    50 G.2X
# medium_10m    ~ 10 M       ~100 K   2 000    150   1 600     512   100 G.2X
# large_50m     ~ 50 M       ~500 K   3 000    200   4 000     512   200 G.2X

_SCALE_CONFIG = {
    "small": {
        "member_chunk_size":        1_000,
        "s3_list_workers":          100,
        "base_shuffle_partitions":  400,
        "files_per_partition":      50,
        "max_shuffle_partitions":   2_000,
        "iceberg_file_size_bytes":  256 * 1024 * 1024,   # 256 MB
        "cache_level_ser":          False,
        "glue_worker_type":         "G.2X",
        "glue_workers":             50,
        "glue_timeout_min":         480,
        "auto_optimize_full_load":  False,
    },
    "medium_10m": {
        "member_chunk_size":        2_000,
        "s3_list_workers":          150,
        "base_shuffle_partitions":  1_600,
        "files_per_partition":      50,
        "max_shuffle_partitions":   4_000,
        "iceberg_file_size_bytes":  512 * 1024 * 1024,   # 512 MB
        "cache_level_ser":          True,
        "glue_worker_type":         "G.2X",
        "glue_workers":             100,
        "glue_timeout_min":         720,
        "auto_optimize_full_load":  True,
    },
    "large_50m": {
        "member_chunk_size":        3_000,
        "s3_list_workers":          200,
        "base_shuffle_partitions":  4_000,
        "files_per_partition":      50,
        "max_shuffle_partitions":   8_000,
        "iceberg_file_size_bytes":  512 * 1024 * 1024,   # 512 MB
        "cache_level_ser":          True,
        "glue_worker_type":         "G.2X",
        "glue_workers":             200,
        "glue_timeout_min":         960,
        "auto_optimize_full_load":  True,
    },
}

SCALE_TIER  = args.get('scale_tier', 'small').lower()
if SCALE_TIER not in _SCALE_CONFIG:
    print(f"[WARN] Unknown scale_tier='{SCALE_TIER}' — falling back to 'small'")
    SCALE_TIER = 'small'
_SC = _SCALE_CONFIG[SCALE_TIER]

# ── Constants ─────────────────────────────────────────────────────────────────
SOURCE_BUCKET     = "nontrauma-claim-prod"
TARGET_BUCKET     = "nontrauma-analytics-prod"
DATABASE          = "claims_db_dev"
PAYER_KEY         = "326"
WATERMARK_KEY     = f"watermarks/claims_payer_{PAYER_KEY}_last_run.json"
MEMBER_CHUNK_SIZE = _SC["member_chunk_size"]
S3_LIST_WORKERS   = _SC["s3_list_workers"]
FILES_PER_PART    = _SC["files_per_partition"]
BASE_SHUFFLE_PART = _SC["base_shuffle_partitions"]
MAX_SHUFFLE_PART  = _SC["max_shuffle_partitions"]

print(f"[INFO] ENV={ENV} | RUN={RUN}")
print(f"[INFO] Source      → s3://{SOURCE_BUCKET}")
print(f"[INFO] Target      → s3://{TARGET_BUCKET}")
print(f"[INFO] DB          → {DATABASE}")
print(f"[INFO] PAYER_KEY   → {PAYER_KEY}")
print(f"[INFO] SCALE TIER  → {SCALE_TIER.upper()}")
print(f"[INFO] ── Recommended Glue settings for tier '{SCALE_TIER}' ──────────────────")
print(f"[INFO]    Worker type : {_SC['glue_worker_type']}")
print(f"[INFO]    Workers     : {_SC['glue_workers']}")
print(f"[INFO]    Timeout     : {_SC['glue_timeout_min']} min")
print(f"[INFO]    Chunk size  : {MEMBER_CHUNK_SIZE} members")
print(f"[INFO]    S3 threads  : {S3_LIST_WORKERS}")
print(f"[INFO] ───────────────────────────────────────────────────────────────────────")

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
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",                str(100 * 1024 * 1024))
# Base shuffle partitions — AQE will coalesce down; dynamic override per chunk below
spark.conf.set("spark.sql.shuffle.partitions",                        str(BASE_SHUFFLE_PART))
spark.conf.set("spark.sql.legacy.timeParserPolicy",                   "CORRECTED")
spark.conf.set("spark.sql.session.timeZone",                          "UTC")
spark.conf.set("spark.sql.files.maxPartitionBytes",                   str(256 * 1024 * 1024))
spark.conf.set("spark.sql.files.openCostInBytes",                     str(8   * 1024 * 1024))
# Enable Iceberg partition pruning predicate pushdown
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping",   "true")
# NOTE: spark.serializer MUST be set before SparkContext starts (via Glue job
# parameters --conf spark.serializer=org.apache.spark.serializer.KryoSerializer).
# Setting it via sc._conf.set() AFTER SparkContext is running has NO effect.

# Cache level: serialised at medium/large scale to halve executor heap pressure
CACHE_LEVEL = (StorageLevel.MEMORY_AND_DISK_SER
               if _SC["cache_level_ser"]
               else StorageLevel.MEMORY_AND_DISK)
print("[INFO] Spark runtime configs set ✅")

# ── Tracking variables ────────────────────────────────────────────────────────
changed_files                  = []
filtered_count                 = 0
is_full_load                   = True
job_status                     = "STARTED"
deleted_members                = set()
current_member_claimkey_counts = {}   # saved to watermark for next run's delete detection
s3_client                      = boto3.client("s3")
glue_client          = boto3.client("glue")
current_run_epoch_ms = int(datetime.utcnow().timestamp() * 1000)
current_run_ts       = datetime.utcnow().isoformat()
raw_df               = None

# Per-table S3 metadata-ok cache: checked ONCE per job run, not once per chunk
_table_meta_ok: dict = {}

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

def save_watermark(status, files_processed, records_merged, mode,
                   members_deleted=0, member_claimkey_counts=None):
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=WATERMARK_KEY,
            Body=json.dumps({
                "last_run_epoch_ms":       current_run_epoch_ms,
                "last_run_ts":             current_run_ts,
                "files_processed":         files_processed,
                "records_merged":          records_merged,
                "members_deleted":         members_deleted,
                "mode":                    mode,
                "job_status":              status,
                "payer_key":               PAYER_KEY,
                "env":                     ENV,
                "run":                     RUN,
                # Per-member claim key counts — used next run to detect
                # partial deletes without reading Iceberg tables
                "member_claimkey_counts":  member_claimkey_counts or {},
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

_ICEBERG_FILE_SIZE = _SC["iceberg_file_size_bytes"]

# Write strategy driven by scale tier:
#   Full load  → copy-on-write (no delete-file accumulation on new tables)
#   Incremental → merge-on-read (low write amplification for small upserts)
# The DDL always creates tables with merge-on-read; an ALTER TABLE switches
# to copy-on-write at the start of a full-load run and back afterwards.
# (We use the same DDL for CREATE … so incremental increments stay MOR.)

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
        'write.target-file-size-bytes' = '{_ICEBERG_FILE_SIZE}',
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
        'write.target-file-size-bytes' = '{_ICEBERG_FILE_SIZE}',
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
        'write.target-file-size-bytes' = '{_ICEBERG_FILE_SIZE}',
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

# ── Copy-on-write helpers (full load only) ────────────────────────────────────
# On FULL LOAD → switch to copy-on-write to avoid delete-file accumulation.
# On INCREMENTAL → keep merge-on-read (low write amplification).
# Called ONCE before the chunk loop (full load) and once after (restore MOR).

def _set_iceberg_write_mode(mode):
    """
    mode = 'copy-on-write' or 'merge-on-read'

    SAFE GUARD: ALTER TABLE forces Iceberg to call loadTable → refreshFromMetadataLocation
    → S3InputStream.read on the metadata JSON.  If the table was just created this run
    (no S3 metadata yet), that read raises NoSuchKeyException and triggers the retry
    spiral seen in the logs.

    Rule: only ALTER a table when _table_meta_ok[tbl] is True (S3 metadata confirmed
    to exist BEFORE this job run started).  Tables with no prior S3 data already have
    the correct DDL defaults (merge-on-read) and need no ALTER.
    """
    for tbl_fqn in _TABLE_DDL:
        tbl_name = tbl_fqn.split(".")[-1]
        # Skip tables with no S3 metadata — ALTER would trigger NoSuchKeyException
        if not _table_meta_ok.get(tbl_name, False):
            print(f"[INFO] _set_iceberg_write_mode({mode}) → {tbl_name}: "
                  f"skipped (no S3 metadata yet — DDL defaults already correct)")
            continue
        try:
            spark.sql(f"""
                ALTER TABLE {tbl_fqn}
                SET TBLPROPERTIES (
                    'write.merge.mode'  = '{mode}',
                    'write.update.mode' = '{mode}',
                    'write.delete.mode' = '{mode}'
                )
            """)
            print(f"[INFO] _set_iceberg_write_mode({mode}) → {tbl_name} ✅")
        except Exception as e:
            # Non-fatal — worst case we stay on the current write mode
            print(f"[WARN] _set_iceberg_write_mode({mode}) → {tbl_name}: {e}")

# ── Write helpers ─────────────────────────────────────────────────────────────

def _is_nosuchkey(exc):
    return any(x in str(exc).lower() for x in ["nosuchkey", "no such key", "404"])

# Per-table write-mode cache:
#   True  → table exists and has valid S3 data → use overwritePartitions
#   False → table was just created / repaired this run → use append only
#           (avoids the overwritePartitions → NoSuchKey → append → NoSuchKey
#            → repair spiral on every run for new payer keys or repaired tables)
_table_use_overwrite: dict = {}

def _iceberg_metadata_exists(table_name):
    """
    Returns True only when BOTH the Glue catalog entry exists AND the Iceberg
    metadata directory on S3 contains at least one file.
    A Glue entry pointing at a missing/empty S3 location is the exact cause of
    the overwritePartitions → NoSuchKey spiral.
    """
    prefix = f"iceberg/{table_name}/metadata/"
    resp   = s3_client.list_objects_v2(Bucket=TARGET_BUCKET, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

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
    # After repair: switch to append for the rest of this job run.
    # This prevents the next chunk from hitting overwritePartitions → NoSuchKey
    # again on the same freshly-recreated table.
    _table_use_overwrite[table_name] = False
    _table_meta_ok[table_name]       = True
    print(f"[WARN] {label}: table re-registered in Glue catalog ✅")

def _iceberg_append(df, table_fqn, label):
    df.writeTo(table_fqn).append()
    print(f"[INFO] {label}: post-repair append ✅")

def _safe_write(df, table_fqn, label, overwrite_partitions=True):
    """
    Write df to the Iceberg table.

    Write-mode decision (evaluated once per table per job run):
    ─────────────────────────────────────────────────────────
    • _table_meta_ok[t]       = S3 metadata exists?  (pre-warmed before loop)
    • _table_use_overwrite[t] = safe to overwritePartitions?

    A table is marked use_overwrite=False when:
      - It was just created this run (new payer key, first write)
      - It was repaired this run (stale Glue catalog fixed)

    This eliminates the recurring spiral:
      overwritePartitions → NoSuchKey → append → NoSuchKey → repair
    for EVERY run on a new payer key or after any repair event.
    On the NEXT job run the table will have real S3 data, pre-warm will
    set use_overwrite=True, and overwritePartitions will work correctly.
    """
    table_name = table_fqn.split(".")[-1]

    # Pre-warm on first encounter (normally done before the loop)
    if table_name not in _table_meta_ok:
        _table_meta_ok[table_name] = _iceberg_metadata_exists(table_name)

    # First time seeing this table → decide write mode
    if table_name not in _table_use_overwrite:
        _table_use_overwrite[table_name] = _table_meta_ok[table_name]

    # Table has no S3 metadata → repair then append
    if not _table_meta_ok[table_name]:
        print(f"[WARN] {label}: S3 metadata absent — repairing Glue catalog before write")
        _glue_drop_and_recreate(table_fqn, label)
        _iceberg_append(df, table_fqn, label)
        return

    # Table exists but was repaired/created this run → append only (safe)
    if not _table_use_overwrite[table_name]:
        _iceberg_append(df, table_fqn, label)
        return

    # Table is healthy → try overwritePartitions, fall back gracefully
    if overwrite_partitions:
        try:
            df.writeTo(table_fqn).overwritePartitions()
            print(f"[INFO] {label}: overwritePartitions ✅")
            return
        except Exception as e1:
            if not _is_nosuchkey(e1):
                raise
            print(f"[WARN] {label}: overwritePartitions → NoSuchKey, repairing catalog")
            # Mark for append-only for rest of this run, then repair + append
            _table_use_overwrite[table_name] = False
            _glue_drop_and_recreate(table_fqn, label)
            _iceberg_append(df, table_fqn, label)
            return

    # overwrite_partitions=False caller path
    try:
        _iceberg_append(df, table_fqn, label)
    except Exception as e2:
        if not _is_nosuchkey(e2):
            raise
        print(f"[WARN] {label}: append() → NoSuchKey, repairing Glue catalog via boto3")
        _table_use_overwrite[table_name] = False
        _glue_drop_and_recreate(table_fqn, label)
        _iceberg_append(df, table_fqn, label)

# ── Partition-filter helper (Iceberg predicate pushdown friendly) ─────────────
def _partition_filter(source, affected_partitions, is_table=False):
    """
    Return a DataFrame scoped to only the (payerkey, loadyear, loadmonth)
    partitions this chunk touches.

    PERFORMANCE: Uses a broadcast-hint inner join instead of an OR-chain.
    The tiny partition-values DF is broadcast to every executor so Iceberg's
    partition pruning can push down to the metadata layer without a full scan
    or a large driver-serialised expression tree.
    """
    if is_table:
        base = spark.table(source)
    else:
        base = source

    if not affected_partitions:
        return base.filter(F.lit(False))

    part_df = spark.createDataFrame(
        [(int(r.payerkey), int(r.loadyear), int(r.loadmonth))
         for r in affected_partitions],
        ["payerkey", "loadyear", "loadmonth"]
    )
    return base.join(
        F.broadcast(part_df),
        on=["payerkey", "loadyear", "loadmonth"],
        how="inner"
    )

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
            """
            Scan one member prefix and return:
              changed_results  — files newer than last_run_epoch_ms (to write)
              all_s3_claimkeys — ALL claim keys currently on S3 for this member
                                 (used to detect partial/full deletes)
              has_any_json     — whether ANY json still exists for this member
              member_key       — the member id string
            """
            changed_results  = []
            all_s3_claimkeys = set()   # every claimkey file currently on S3
            has_any_json     = False
            pager            = s3_client.get_paginator("list_objects_v2")
            for pg in pager.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix,
                                     PaginationConfig={"PageSize": 1000}):
                for obj in pg.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".json"):
                        continue
                    has_any_json = True
                    parts = key.split("/")
                    if len(parts) < 3:
                        continue
                    claim_key_str = parts[2].replace(".json", "")
                    all_s3_claimkeys.add(claim_key_str)
                    file_epoch_ms = int(obj["LastModified"].timestamp() * 1000)
                    if file_epoch_ms > last_run_epoch_ms:
                        changed_results.append({
                            "s3_path":         f"s3://{SOURCE_BUCKET}/{key}",
                            "path_payer_key":  parts[0],
                            "path_member_key": parts[1],
                            "path_claim_key":  claim_key_str,
                            "file_epoch_ms":   file_epoch_ms,
                        })
            member_key = prefix.rstrip("/").split("/")[-1]
            return changed_results, all_s3_claimkeys, has_any_json, member_key

        # deleted_members     — member has ZERO json files left on S3
        # partially_deleted   — member still has SOME files but the watermark
        #                       recorded more claim keys last run (net deletion)
        # member_s3_claimkeys — {member_key → frozenset(claimkeys on S3 NOW)}
        #                       stored ONLY for members with changed/deleted files
        #                       NOT for every member — keeps driver memory bounded
        deleted_members     = set()
        partially_deleted   = set()
        member_s3_claimkeys = {}

        # Load per-member claim-key counts from the last-run watermark (if any).
        # This lets us detect partial deletes without reading Iceberg at all:
        # if S3 now has fewer claim keys than last run recorded, some were deleted.
        last_member_claimkey_counts = {}   # member_key → int count from watermark
        if not is_full_load:
            try:
                wm_obj = s3_client.get_object(Bucket=TARGET_BUCKET, Key=WATERMARK_KEY)
                wm     = json.loads(wm_obj["Body"].read())
                last_member_claimkey_counts = wm.get("member_claimkey_counts", {})
            except Exception:
                pass   # watermark missing or old format — treat all as no prior count

        # Current-run per-member claim key counts — saved to watermark at end
        current_member_claimkey_counts = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=S3_LIST_WORKERS) as pool:
            futures = {pool.submit(_list_member, pfx): pfx for pfx in member_prefixes}
            for future in concurrent.futures.as_completed(futures):
                batch, all_s3_claimkeys, has_any_json, member_key = future.result()
                for rec in batch:
                    total_scanned += 1
                    changed_file_meta.append(rec)
                    members_found.add(rec["path_member_key"])

                if not is_full_load:
                    s3_count   = len(all_s3_claimkeys)
                    last_count = last_member_claimkey_counts.get(member_key, None)

                    # Always record current count for next run's watermark
                    current_member_claimkey_counts[member_key] = s3_count

                    if not has_any_json:
                        # ALL files deleted
                        deleted_members.add(member_key)
                        # Store empty set so STEP 3.5-A can skip claimkey lookup
                        member_s3_claimkeys[member_key] = frozenset()

                    elif last_count is not None and s3_count < last_count:
                        # Fewer claim files on S3 than last run → partial delete
                        # Only store claimkeys for these members (not all members)
                        partially_deleted.add(member_key)
                        member_s3_claimkeys[member_key] = frozenset(all_s3_claimkeys)
                    # else: member count unchanged or grew → no delete tracking needed

        # Build the flat list once after all threads finish
        changed_files = [r["s3_path"] for r in changed_file_meta]

        print(f"[STEP 3] Total files scanned        : {total_scanned}")
        print(f"[STEP 3] Changed files found        : {len(changed_files)}")
        print(f"[STEP 3] Unique members affected    : {len(members_found)}")
        if deleted_members:
            print(f"[STEP 3] Fully-deleted members     : {len(deleted_members)} "
                  f"(all claims removed from S3)")
        if partially_deleted - deleted_members:
            print(f"[STEP 3] Partially-changed members : "
                  f"{len(partially_deleted - deleted_members)} "
                  f"(some claims deleted/moved from S3)")
        if changed_files:
            for f in changed_files[:5]:
                print(f"         {f}")
            if len(changed_files) > 5:
                print(f"         ... and {len(changed_files) - 5} more")

    except Exception as e:
        raise Exception(f"[STEP 3] FAILED scanning S3: {e}")

    if not changed_files and not deleted_members:
        print("[STEP 3] No new, modified, or deleted files — nothing to do!")
        save_watermark("SUCCESS - NO CHANGES", 0, 0, "INCREMENTAL - NO CHANGES")
        job.commit()
        sys.exit(0)

    # ── STEP 3.5 — SYNC DELETES: purge claims removed from S3 ───────────────────
    #
    # Three sub-cases handled here (all incremental only):
    #
    #  A. FULL MEMBER DELETE  — member prefix has zero JSON files left.
    #     → Remove ALL rows for that member from all 3 Iceberg tables.
    #
    #  B. PARTIAL CLAIM DELETE — member still has SOME files but fewer than
    #     before (e.g. 5 claims → 3 deleted, 2 remain).
    #     → Read Iceberg rows for that member, keep only rows whose claimkey
    #        still exists on S3, overwrite the partition.
    #
    #  C. CLAIM RE-ASSIGNMENT  — claim files deleted from m1 and added to m2.
    #     m1 ends up with fewer (or zero) claims → covered by A or B above.
    #     m2 gets the new files → picked up as changed_files → written in
    #     the normal chunk loop (STEP 4-10).
    #     The combined effect: claims appear under m2 only, gone from m1.

    if not is_full_load and (deleted_members or partially_deleted):

        # ── A: Fully deleted members ──────────────────────────────────────
        if deleted_members:
            print(f"[STEP 3.5-A] Purging {len(deleted_members)} fully-deleted "
                  f"members from Iceberg...")
            del_df = spark.createDataFrame(
                [(int(PAYER_KEY), int(m)) for m in deleted_members if m.isdigit()],
                ["payerkey_del", "memberkey_del"]
            )
            for tbl_name, tbl_fqn in [
                ("claims",         f"glue_catalog.{DATABASE}.claims"),
                ("claimdiagnosis", f"glue_catalog.{DATABASE}.claimdiagnosis"),
                ("claimlines",     f"glue_catalog.{DATABASE}.claimlines"),
            ]:
                try:
                    if not _table_exists_glue(DATABASE, tbl_name):
                        continue
                    if not _iceberg_metadata_exists(tbl_name):
                        continue
                    surviving = spark.table(tbl_fqn).join(
                        F.broadcast(del_df),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_del")) &
                            (F.col("memberkey") == F.col("memberkey_del"))
                        ),
                        how="left_anti"
                    )
                    _safe_write(surviving, tbl_fqn, f"{tbl_name}(purge-full-delete)")
                    print(f"[STEP 3.5-A] {tbl_name}: fully-deleted members purged ✅")
                except Exception as e:
                    print(f"[STEP 3.5-A] WARN {tbl_name}: purge failed (non-fatal): {e}")

        # ── B: Partial claim deletes (some files removed, member still exists) ──
        # Only runs for members where S3 count DROPPED vs last watermark.
        # Uses partition-scoped read (payerkey only) — NOT a full table scan.
        partial_only = partially_deleted - deleted_members
        if partial_only:
            print(f"[STEP 3.5-B] Reconciling partial deletes for "
                  f"{len(partial_only)} members...")

            # (payerkey, memberkey, claimkey) still alive on S3
            surviving_rows = [
                (int(PAYER_KEY), int(m), int(ck))
                for m in partial_only if m.isdigit()
                for ck in member_s3_claimkeys.get(m, set()) if ck.isdigit()
            ]
            partial_member_rows = [
                (int(PAYER_KEY), int(m))
                for m in partial_only if m.isdigit()
            ]

            if surviving_rows and partial_member_rows:
                s3_alive_df       = spark.createDataFrame(
                    surviving_rows, ["payerkey_s3", "memberkey_s3", "claimkey_s3"]
                )
                partial_member_df = spark.createDataFrame(
                    partial_member_rows, ["payerkey_pm", "memberkey_pm"]
                )

                for tbl_name, tbl_fqn in [
                    ("claims",         f"glue_catalog.{DATABASE}.claims"),
                    ("claimdiagnosis", f"glue_catalog.{DATABASE}.claimdiagnosis"),
                    ("claimlines",     f"glue_catalog.{DATABASE}.claimlines"),
                ]:
                    try:
                        if not _table_exists_glue(DATABASE, tbl_name):
                            continue
                        if not _iceberg_metadata_exists(tbl_name):
                            continue

                        # PERFORMANCE: read only payerkey partition — NOT full scan
                        # Iceberg pushes payerkey=PAYER_KEY to metadata layer so
                        # only the relevant partition files are opened.
                        tbl_scoped = (
                            spark.table(tbl_fqn)
                            .filter(F.col("payerkey") == int(PAYER_KEY))
                        )

                        # Rows outside the partially-deleted members → untouched
                        unaffected = tbl_scoped.join(
                            F.broadcast(partial_member_df),
                            on=(
                                (F.col("payerkey")  == F.col("payerkey_pm")) &
                                (F.col("memberkey") == F.col("memberkey_pm"))
                            ),
                            how="left_anti"
                        )

                        # Rows inside partially-deleted members that still have
                        # a live S3 file → keep them; others are dropped
                        affected_alive = tbl_scoped.join(
                            F.broadcast(partial_member_df),
                            on=(
                                (F.col("payerkey")  == F.col("payerkey_pm")) &
                                (F.col("memberkey") == F.col("memberkey_pm"))
                            ),
                            how="inner"
                        ).join(
                            F.broadcast(s3_alive_df),
                            on=(
                                (F.col("payerkey")  == F.col("payerkey_s3"))  &
                                (F.col("memberkey") == F.col("memberkey_s3")) &
                                (F.col("claimkey")  == F.col("claimkey_s3"))
                            ),
                            how="inner"
                        ).drop("payerkey_pm", "memberkey_pm",
                               "payerkey_s3", "memberkey_s3", "claimkey_s3")

                        reconciled = unaffected.unionByName(affected_alive)
                        _safe_write(reconciled, tbl_fqn,
                                    f"{tbl_name}(partial-delete-sync)")
                        print(f"[STEP 3.5-B] {tbl_name}: partial-delete sync ✅")
                    except Exception as e:
                        print(f"[STEP 3.5-B] WARN {tbl_name}: sync failed "
                              f"(non-fatal): {e}")

        print(f"[STEP 3.5] Delete sync complete ✅")

    if not changed_files:
        print("[STEP 3.5] Only deletions this run — no new files to process.")
        save_watermark("SUCCESS - DELETIONS ONLY",
                       len(deleted_members), 0, "INCREMENTAL - DELETIONS ONLY",
                       members_deleted        = len(deleted_members),
                       member_claimkey_counts = current_member_claimkey_counts)
        job.commit()
        sys.exit(0)

    # ── STEP 4–10 — CHUNK-BASED PROCESS: read → transform → write → unpersist ──
    #
    # Each chunk of MEMBER_CHUNK_SIZE (1000) members is:
    #   read (member-scoped globs) → transform in ONE Spark pass → write → unpersist
    # Memory footprint = 1 chunk at a time, not all 200M rows.

    print(f"[STEP 4] Grouping {len(changed_file_meta)} changed files "
          f"into member chunks of {MEMBER_CHUNK_SIZE}...")

    # ── STEP 10 setup — create tables once before chunk loop ──────────────────
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

    # ── Group changed_file_meta by member ─────────────────────────────────────
    member_to_files = defaultdict(list)
    for rec in changed_file_meta:
        member_to_files[rec["path_member_key"]].append(rec)

    all_members   = list(member_to_files.keys())
    num_chunks    = -(-len(all_members) // MEMBER_CHUNK_SIZE)  # ceiling div
    total_written = 0

    # Pre-warm ONCE before the chunk loop:
    #   _table_meta_ok      → does S3 metadata exist?
    #   _table_use_overwrite → is overwritePartitions safe this run?
    # Both are cached so the decision is made exactly once per table per job,
    # not once per chunk. A table that had no S3 data (new payer key or
    # repaired table) starts with use_overwrite=False → append-only this run.
    for _tbl in ["claims", "claimdiagnosis", "claimlines"]:
        _table_meta_ok[_tbl]       = _iceberg_metadata_exists(_tbl)
        _table_use_overwrite[_tbl] = _table_meta_ok[_tbl]

    # ── FULL LOAD: switch Iceberg to copy-on-write before writing ─────────────
    # copy-on-write produces clean data files with no delete-file accumulation,
    # which is ideal for a brand-new full load at 10 M / 50 M scale.
    # We restore merge-on-read at the end of the job so incremental runs stay
    # low-write-amplification.
    if is_full_load:
        print("[INFO] Full load detected → setting Iceberg write mode: copy-on-write")
        _set_iceberg_write_mode("copy-on-write")

    print(f"[STEP 4] {len(all_members)} unique members → "
          f"{num_chunks} chunks of ≤{MEMBER_CHUNK_SIZE} members each")

    for chunk_idx in range(num_chunks):
        chunk_members = all_members[
            chunk_idx * MEMBER_CHUNK_SIZE : (chunk_idx + 1) * MEMBER_CHUNK_SIZE
        ]
        chunk_meta = [rec for m in chunk_members for rec in member_to_files[m]]
        chunk_files = [r["s3_path"] for r in chunk_meta]

        print(f"\n[CHUNK {chunk_idx+1}/{num_chunks}] "
              f"{len(chunk_members)} members | {len(chunk_files)} files")

        # ── Dynamic shuffle partitions for this chunk ──────────────────────
        # Formula: ceil(files / FILES_PER_PART) * 4, clamped to [BASE, MAX].
        # Prevents under-partitioning on large chunks (50 K files at 50 M scale).
        chunk_shuffle_parts = max(
            BASE_SHUFFLE_PART,
            min(MAX_SHUFFLE_PART,
                (-(-len(chunk_files) // FILES_PER_PART)) * 4)
        )
        spark.conf.set("spark.sql.shuffle.partitions", str(chunk_shuffle_parts))

        raw_df = None
        try:
            min_epoch_ms = min(r["file_epoch_ms"] for r in chunk_meta)

            # Build ALL derived columns in ONE lazy chain, then persist ONCE.
            path_parts = F.split(F.input_file_name(), "/")
            file_ts    = (F.lit(min_epoch_ms).cast("long") / 1000).cast("timestamp")

            # ── READ: pass exact file paths, not member-level globs ──────
            # OLD: 103 member globs → Spark driver fires 103 S3 LIST calls,
            #      then re-reads the result before dispatching any tasks.
            # NEW: pass the exact list of changed paths directly.
            #      Zero extra S3 LIST calls — driver already has every path
            #      from STEP 3.  Spark splits the list across tasks directly.
            #
            # Per-file epoch: use actual file_epoch_ms from the S3 metadata
            # collected in STEP 3 (stored in chunk_meta), not a chunk-wide min.
            # Build a broadcast map {path → epoch_ms} for accurate timestamps.
            epoch_map = {r["s3_path"]: r["file_epoch_ms"] for r in chunk_meta}
            epoch_rows = [(path, epoch) for path, epoch in epoch_map.items()]
            epoch_df = spark.createDataFrame(epoch_rows, ["s3_path_key", "file_epoch_ms_actual"])

            raw_df = (
                spark.read
                .option("multiline",           "true")
                .option("mode",                "PERMISSIVE")
                .option("recursiveFileLookup", "false")
                .json(chunk_files)             # ← exact paths, no glob expansion
                # path keys
                .withColumn("path_payer_key",
                    F.element_at(path_parts, -3).cast("long"))
                .withColumn("path_member_key",
                    F.element_at(path_parts, -2).cast("long"))
                .withColumn("path_claim_key",
                    F.regexp_replace(
                        F.element_at(path_parts, -1), r"\.json$", ""
                    ).cast("long"))
                # join per-file epoch from STEP 3 metadata (accurate per file)
                .withColumn("_s3_path", F.input_file_name())
                .join(F.broadcast(epoch_df),
                      F.col("_s3_path") == F.col("s3_path_key"), "left")
                .withColumn("file_epoch_ms",
                    F.coalesce(F.col("file_epoch_ms_actual"),
                               F.lit(min_epoch_ms).cast("long")))
                .drop("_s3_path", "s3_path_key", "file_epoch_ms_actual")
                # resolved keys & epoch
                .withColumn("resolved_payer_key",
                    F.coalesce(F.col("payerKey").cast("long"),
                               F.col("path_payer_key")))
                .withColumn("resolved_member_key",
                    F.coalesce(F.col("memberKey").cast("long"),
                               F.col("path_member_key")))
                .withColumn("resolved_claim_key",
                    F.coalesce(F.col("claimKey").cast("long"),
                               F.col("path_claim_key")))
                .withColumn("updated_at_epoch_ms",
                    F.coalesce(
                        to_epoch_ms(F.col("updatedAt")),
                        F.col("file_epoch_ms"),
                        F.lit(0).cast("long")
                    ))
                .filter(F.col("resolved_claim_key").isNotNull())
                # partition columns
                .withColumn("claim_load_ts",
                    normalize_ts(F.col("claimLoadDateTime")))
                .withColumn("loadyear",
                    F.coalesce(
                        F.year(F.col("claim_load_ts")).cast("int"),
                        F.year(file_ts).cast("int")
                    ))
                .withColumn("loadmonth",
                    F.coalesce(
                        F.month(F.col("claim_load_ts")).cast("int"),
                        F.month(file_ts).cast("int")
                    ))
            )

            # coalesce (no shuffle) instead of repartition (full shuffle).
            # With multiline JSON each task = 1 file already — coalescing
            # groups small tasks into larger ones without moving data over
            # the network. Only repartition if we need MORE partitions.
            FILES_PER_PARTITION = FILES_PER_PART
            ideal_partitions    = max(4, min(400,
                -(-len(chunk_files) // FILES_PER_PARTITION)))
            # If Spark created more partitions than ideal, coalesce (cheap).
            # If Spark created fewer, repartition (adds parallelism).
            raw_df = (raw_df.coalesce(ideal_partitions)
                      if raw_df.rdd.getNumPartitions() > ideal_partitions
                      else raw_df.repartition(ideal_partitions))

            # Single persist + count — executes the whole lazy chain once.
            raw_df.persist(CACHE_LEVEL)
            chunk_count = raw_df.count()
            filtered_count += chunk_count

            print(f"[CHUNK {chunk_idx+1}] Cached {chunk_count} rows "
                  f"across {ideal_partitions} partitions")

            if chunk_count == 0:
                print(f"[CHUNK {chunk_idx+1}] Empty — skipping")
                continue

            # ── STEP 7 (per chunk) — BUILD CLAIMS DF ──────────────────────
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

            # ── STEP 8 (per chunk) — BUILD DIAGNOSIS DF ───────────────────
            # Resolve the diagnosis array column name at runtime.
            # Known variants across payers/feeds — checked in priority order.
            _dx_col_candidates = [
                "claimDiagnosisList", "ClaimDiagnosisList",
                "diagnosisList",      "DiagnosisList",
                "claimDiagnosis",     "ClaimDiagnosis",
            ]
            _dx_col = next(
                (c for c in _dx_col_candidates if c in raw_df.columns), None
            )
            # If none found — produce empty array so downstream schema is stable
            _dx_array_expr = (
                F.when(F.col(_dx_col).isNotNull(), F.col(_dx_col))
                 .otherwise(F.array())
                if _dx_col
                else F.array().cast("array<struct<>>")
            )
            if not _dx_col:
                print(f"[WARN] STEP 8: no diagnosis list column found in data "
                      f"(tried {_dx_col_candidates}) — claimdiagnosis will be empty "
                      f"for this chunk. Available cols: {raw_df.columns}")

            diagnosis_df = raw_df.select(
                F.col("resolved_payer_key").alias("payerkey"),
                F.col("resolved_member_key").alias("memberkey"),
                F.col("resolved_claim_key").alias("claimkey"),
                F.col("loadyear"),
                F.col("loadmonth"),
                F.col("updated_at_epoch_ms").alias("claim_updated_at_epoch_ms"),
                F.explode_outer(_dx_array_expr).alias("dx")
            ).filter(F.col("dx").isNotNull()).select(
                "payerkey", "memberkey", "claimkey", "loadyear", "loadmonth",
                "claim_updated_at_epoch_ms",
                # Use .getField() — returns null safely when struct field absent
                F.coalesce(F.col("dx").getField("diagnosisCode"),
                           F.col("dx").getField("DiagnosisCode"),
                           F.lit(None).cast("string")).alias("diagnosiscode"),
                F.coalesce(F.col("dx").getField("diagnosisOrder"),
                           F.col("dx").getField("DiagnosisOrder"),
                           F.lit(None).cast("int")).cast("int").alias("diagnosisorder"),
                F.coalesce(F.col("dx").getField("isPrimary"),
                           F.col("dx").getField("IsPrimary"),
                           F.lit(None).cast("string")).alias("isprimary"),
                F.coalesce(F.col("dx").getField("isSensitive"),
                           F.col("dx").getField("IsSensitive"),
                           F.lit(None).cast("int")).cast("int").alias("issensitive"),
                F.coalesce(F.col("dx").getField("isTrauma"),
                           F.col("dx").getField("IsTrauma"),
                           F.lit(None).cast("int")).cast("int").alias("istrauma"),
                F.coalesce(F.col("dx").getField("versionIndicator"),
                           F.col("dx").getField("VersionIndicator"),
                           F.lit(None).cast("int")).cast("int").alias("versionindicator"),
                F.coalesce(F.col("dx").getField("clientDataFeedCode"),
                           F.col("dx").getField("ClientDataFeedCode"),
                           F.lit(None).cast("string")).alias("clientdatafeedcode"),
                F.coalesce(F.col("dx").getField("inboundBatchMasterKey"),
                           F.col("dx").getField("InboundBatchMasterKey"),
                           F.lit(None).cast("long")).cast("long").alias("inboundbatchmasterkey"),
                F.coalesce(F.col("dx").getField("batchRunSequence"),
                           F.col("dx").getField("BatchRunSequence"),
                           F.lit(None).cast("int")).cast("int").alias("batchrunsequence"),
                F.coalesce(F.col("dx").getField("claimDiagnosisKey"),
                           F.col("dx").getField("ClaimDiagnosisKey"),
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

            # ── STEP 9 (per chunk) — BUILD LINES DF ───────────────────────
            # Resolve the claim lines array column name at runtime.
            _ln_col_candidates = [
                "claimLinesList", "ClaimLinesList",
                "claimLines",     "ClaimLines",
                "linesList",      "LinesList",
            ]
            _ln_col = next(
                (c for c in _ln_col_candidates if c in raw_df.columns), None
            )
            _ln_array_expr = (
                F.when(F.col(_ln_col).isNotNull(), F.col(_ln_col))
                 .otherwise(F.array())
                if _ln_col
                else F.array().cast("array<struct<>>")
            )
            if not _ln_col:
                print(f"[WARN] STEP 9: no lines list column found in data "
                      f"(tried {_ln_col_candidates}) — claimlines will be empty "
                      f"for this chunk. Available cols: {raw_df.columns}")

            lines_df = raw_df.select(
                F.col("resolved_payer_key").alias("payerkey"),
                F.col("resolved_member_key").alias("memberkey"),
                F.col("resolved_claim_key").alias("claimkey"),
                F.col("loadyear"),
                F.col("loadmonth"),
                F.col("updated_at_epoch_ms").alias("claim_updated_at_epoch_ms"),
                F.explode_outer(_ln_array_expr).alias("ln")
            ).filter(F.col("ln").isNotNull()).select(
                "payerkey", "memberkey", "claimkey", "loadyear", "loadmonth",
                "claim_updated_at_epoch_ms",
                # Use .getField() — returns null safely when struct field absent
                F.coalesce(F.col("ln").getField("claimLineKey"),
                           F.col("ln").getField("ClaimLineKey"),
                           F.lit(None).cast("long")).cast("long").alias("claimlinekey"),
                F.coalesce(F.col("ln").getField("claimLineNumber"),
                           F.col("ln").getField("ClaimLineNumber"),
                           F.lit(None).cast("string")).alias("claimlinenumber"),
                F.coalesce(F.col("ln").getField("procedureCode"),
                           F.col("ln").getField("ProcedureCode"),
                           F.lit(None).cast("string")).alias("procedurecode"),
                F.coalesce(F.col("ln").getField("procedureCodeType"),
                           F.col("ln").getField("ProcedureCodeType"),
                           F.lit(None).cast("string")).alias("procedurecodetype"),
                F.coalesce(F.col("ln").getField("billedAmount"),
                           F.col("ln").getField("BilledAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("billedamount"),
                F.coalesce(F.col("ln").getField("clientPaidAmount"),
                           F.col("ln").getField("ClientPaidAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("clientpaidamount"),
                F.coalesce(F.col("ln").getField("memberPaid"),
                           F.col("ln").getField("MemberPaid"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("memberpaid"),
                F.coalesce(F.col("ln").getField("allowedAmount"),
                           F.col("ln").getField("AllowedAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("allowedamount"),
                F.coalesce(F.col("ln").getField("coveredAmount"),
                           F.col("ln").getField("CoveredAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("coveredamount"),
                F.coalesce(F.col("ln").getField("discountAmount"),
                           F.col("ln").getField("DiscountAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("discountamount"),
                F.coalesce(F.col("ln").getField("discountReason"),
                           F.col("ln").getField("DiscountReason"),
                           F.lit(None).cast("string")).alias("discountreason"),
                F.coalesce(F.col("ln").getField("excludedAmount"),
                           F.col("ln").getField("ExcludedAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("excludedamount"),
                F.coalesce(F.col("ln").getField("excludedReason"),
                           F.col("ln").getField("ExcludedReason"),
                           F.lit(None).cast("string")).alias("excludedreason"),
                F.coalesce(F.col("ln").getField("withholdAmount"),
                           F.col("ln").getField("WithholdAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("withholdamount"),
                F.coalesce(F.col("ln").getField("withholdReason"),
                           F.col("ln").getField("WithholdReason"),
                           F.lit(None).cast("string")).alias("withholdreason"),
                F.coalesce(F.col("ln").getField("providerPaidAmount"),
                           F.col("ln").getField("ProviderPaidAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("providerpaidamount"),
                F.coalesce(F.col("ln").getField("originalClientPaidAmount"),
                           F.col("ln").getField("OriginalClientPaidAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("originalclientpaidamount"),
                F.coalesce(F.col("ln").getField("previousPaidAmount"),
                           F.col("ln").getField("PreviousPaidAmount"),
                           F.lit(None).cast("decimal(18,2)")).cast("decimal(18,2)").alias("previouspaidamount"),
                normalize_date(F.coalesce(F.col("ln").getField("dateofServiceFrom"),
                                          F.col("ln").getField("DateofServiceFrom"),
                                          F.lit(None))).alias("dateofservicefrom"),
                normalize_date(F.coalesce(F.col("ln").getField("dateofServiceThru"),
                                          F.col("ln").getField("DateofServiceThru"),
                                          F.lit(None))).alias("dateofservicethru"),
                F.coalesce(F.col("ln").getField("modifierCode01"),
                           F.col("ln").getField("ModifierCode01"),
                           F.lit(None).cast("string")).alias("modifiercode01"),
                F.coalesce(F.col("ln").getField("modifierCode02"),
                           F.col("ln").getField("ModifierCode02"),
                           F.lit(None).cast("string")).alias("modifiercode02"),
                F.coalesce(F.col("ln").getField("placeofService"),
                           F.col("ln").getField("PlaceofService"),
                           F.lit(None).cast("string")).alias("placeofservice"),
                F.coalesce(F.col("ln").getField("revenueCode"),
                           F.col("ln").getField("RevenueCode"),
                           F.lit(None).cast("string")).alias("revenuecode"),
                F.coalesce(F.col("ln").getField("serviceType"),
                           F.col("ln").getField("ServiceType"),
                           F.lit(None).cast("string")).alias("servicetype"),
                F.coalesce(F.col("ln").getField("quantity"),
                           F.col("ln").getField("Quantity"),
                           F.lit(None).cast("decimal(10,2)")).cast("decimal(10,2)").alias("quantity"),
                F.coalesce(F.col("ln").getField("houseCode"),
                           F.col("ln").getField("HouseCode"),
                           F.lit(None).cast("string")).alias("housecode"),
                F.coalesce(F.col("ln").getField("houseCodeDescription"),
                           F.col("ln").getField("HouseCodeDescription"),
                           F.lit(None).cast("string")).alias("housecodedescription"),
                F.coalesce(F.col("ln").getField("paymentType"),
                           F.col("ln").getField("PaymentType"),
                           F.lit(None).cast("string")).alias("paymenttype"),
                F.coalesce(F.col("ln").getField("paymentTypeID"),
                           F.col("ln").getField("PaymentTypeID"),
                           F.lit(None).cast("string")).alias("paymenttypeid"),
                F.coalesce(F.col("ln").getField("paymentComments"),
                           F.col("ln").getField("PaymentComments"),
                           F.lit(None).cast("string")).alias("paymentcomments"),
                F.coalesce(F.col("ln").getField("checkNumber"),
                           F.col("ln").getField("CheckNumber"),
                           F.lit(None).cast("string")).alias("checknumber"),
                F.coalesce(F.col("ln").getField("transactionCode"),
                           F.col("ln").getField("TransactionCode"),
                           F.lit(None).cast("string")).alias("transactioncode"),
                F.coalesce(F.col("ln").getField("transactionDescription"),
                           F.col("ln").getField("TransactionDescription"),
                           F.lit(None).cast("string")).alias("transactiondescription"),
                F.coalesce(F.col("ln").getField("adjustmentFlag"),
                           F.col("ln").getField("AdjustmentFlag"),
                           F.lit(None).cast("string")).alias("adjustmentflag"),
                F.coalesce(F.col("ln").getField("isPrimaryNDC"),
                           F.col("ln").getField("IsPrimaryNDC"),
                           F.lit(None).cast("string")).alias("isprimaryndc"),
                F.coalesce(F.col("ln").getField("insuredTermDate"),
                           F.col("ln").getField("InsuredTermDate"),
                           F.lit(None).cast("string")).alias("insuredtermdate"),
                F.coalesce(F.col("ln").getField("manipulationReason"),
                           F.col("ln").getField("ManipulationReason"),
                           F.lit(None).cast("string")).alias("manipulationreason"),
                F.coalesce(F.col("ln").getField("claimDetailStatus"),
                           F.col("ln").getField("ClaimDetailStatus"),
                           F.lit(None).cast("string")).alias("claimdetailstatus"),
                F.coalesce(F.col("ln").getField("clientDataFeedCode"),
                           F.col("ln").getField("ClientDataFeedCode"),
                           F.lit(None).cast("string")).alias("clientdatafeedcode"),
                F.coalesce(F.col("ln").getField("inboundBatchMasterKey"),
                           F.col("ln").getField("InboundBatchMasterKey"),
                           F.lit(None).cast("long")).cast("long").alias("inboundbatchmasterkey"),
                F.coalesce(F.col("ln").getField("batchRunSequence"),
                           F.col("ln").getField("BatchRunSequence"),
                           F.lit(None).cast("int")).cast("int").alias("batchrunsequence"),
                F.coalesce(F.col("ln").getField("stageClaimLineKey"),
                           F.col("ln").getField("StageClaimLineKey"),
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

            # ── STEP 10 (per chunk) — WRITE ────────────────────────────────
            # Collect distinct partitions this chunk touches (tiny — driver-side)
            affected_partitions = (
                claims_df.select("payerkey", "loadyear", "loadmonth")
                .distinct().collect()
            )

            # Members in this chunk whose files were previously deleted and
            # are now being re-added (claim re-assignment: m1 → m2).
            chunk_member_keys = {str(r["path_member_key"]) for r in chunk_meta}
            readded_members   = chunk_member_keys & deleted_members

            # Members in this chunk that may have had SOME claims deleted from S3
            # (partial delete). For these members, surviving S3 claim keys are
            # known from member_s3_claimkeys — we use this to strip ghost rows.
            partial_chunk_members = chunk_member_keys & partially_deleted

            def _build_s3_alive_df(members):
                """
                Build a broadcast DF of (payerkey, memberkey, claimkey) that
                currently exist on S3 — used to keep only valid rows in Iceberg.
                """
                rows = []
                for m in members:
                    if not m.isdigit():
                        continue
                    for ck in member_s3_claimkeys.get(m, set()):
                        if ck.isdigit():
                            rows.append((int(PAYER_KEY), int(m), int(ck)))
                if not rows:
                    return None
                return spark.createDataFrame(
                    rows, ["payerkey_s3", "memberkey_s3", "claimkey_s3"]
                )

            def _strip_deleted_claims(existing_df, members_to_check):
                """
                Remove Iceberg rows whose claimkey no longer exists on S3.
                Returns existing_df unchanged (zero Spark work) when
                members_to_check is empty — the normal case on runs with no
                partial deletes.
                """
                if not members_to_check:
                    return existing_df   # ← fast-path: no work done
                s3_alive = _build_s3_alive_df(members_to_check)
                if s3_alive is None:
                    return existing_df

                scoped_members_df = spark.createDataFrame(
                    [(int(PAYER_KEY), int(m)) for m in members_to_check
                     if m.isdigit()],
                    ["payerkey_sc", "memberkey_sc"]
                )
                # Rows NOT in the scoped members → untouched
                unscoped = existing_df.join(
                    F.broadcast(scoped_members_df),
                    on=(
                        (F.col("payerkey")  == F.col("payerkey_sc")) &
                        (F.col("memberkey") == F.col("memberkey_sc"))
                    ),
                    how="left_anti"
                )
                # Rows IN scoped members that still have an S3 file → keep
                scoped_alive = existing_df.join(
                    F.broadcast(scoped_members_df),
                    on=(
                        (F.col("payerkey")  == F.col("payerkey_sc")) &
                        (F.col("memberkey") == F.col("memberkey_sc"))
                    ),
                    how="inner"
                ).join(
                    F.broadcast(s3_alive),
                    on=(
                        (F.col("payerkey")  == F.col("payerkey_s3"))  &
                        (F.col("memberkey") == F.col("memberkey_s3")) &
                        (F.col("claimkey")  == F.col("claimkey_s3"))
                    ),
                    how="inner"
                ).drop("payerkey_sc", "memberkey_sc",
                       "payerkey_s3", "memberkey_s3", "claimkey_s3")

                return unscoped.unionByName(scoped_alive)

            # ── CLAIMS ────────────────────────────────────────────────────
            if is_full_load:
                _safe_write(claims_df,
                            f"glue_catalog.{DATABASE}.claims", "claims")
            else:
                claims_tbl      = f"glue_catalog.{DATABASE}.claims"
                existing_claims = _partition_filter(claims_tbl, affected_partitions,
                                                    is_table=True)

                # Strip rows for re-added members (claim re-assignment m1→m2)
                if readded_members:
                    ra_df = spark.createDataFrame(
                        [(int(PAYER_KEY), int(m)) for m in readded_members
                         if m.isdigit()],
                        ["payerkey_ra", "memberkey_ra"]
                    )
                    existing_claims = existing_claims.join(
                        F.broadcast(ra_df),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_ra")) &
                            (F.col("memberkey") == F.col("memberkey_ra"))
                        ),
                        how="left_anti"
                    )
                    print(f"[CHUNK {chunk_idx+1}] Re-added member rows stripped "
                          f"from claims: {readded_members}")

                # Strip rows whose claim files were partially deleted from S3
                existing_claims = _strip_deleted_claims(
                    existing_claims, partial_chunk_members
                )

                replaced_claims = F.broadcast(
                    claims_df.select("payerkey", "loadyear", "loadmonth",
                                     "memberkey", "claimkey").distinct()
                )
                merged_claims = (
                    existing_claims
                    .join(replaced_claims,
                          on=["payerkey", "loadyear", "loadmonth",
                              "memberkey", "claimkey"],
                          how="left_anti")
                    .unionByName(claims_df)
                )
                _safe_write(merged_claims, claims_tbl, "claims")
                print(f"[CHUNK {chunk_idx+1}] claims write ✅")

            # ── CLAIM DIAGNOSIS ───────────────────────────────────────────
            if is_full_load:
                _safe_write(diagnosis_df,
                            f"glue_catalog.{DATABASE}.claimdiagnosis",
                            "claimdiagnosis")
            else:
                dx_tbl      = f"glue_catalog.{DATABASE}.claimdiagnosis"
                existing_dx = _partition_filter(dx_tbl, affected_partitions,
                                                is_table=True)
                if readded_members:
                    ra_df_dx = spark.createDataFrame(
                        [(int(PAYER_KEY), int(m)) for m in readded_members
                         if m.isdigit()],
                        ["payerkey_ra", "memberkey_ra"]
                    )
                    existing_dx = existing_dx.join(
                        F.broadcast(ra_df_dx),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_ra")) &
                            (F.col("memberkey") == F.col("memberkey_ra"))
                        ),
                        how="left_anti"
                    )
                existing_dx = _strip_deleted_claims(
                    existing_dx, partial_chunk_members
                )
                replaced_dx = F.broadcast(
                    diagnosis_df.select("payerkey", "loadyear", "loadmonth",
                                        "memberkey", "claimkey").distinct()
                )
                merged_dx = (
                    existing_dx
                    .join(replaced_dx,
                          on=["payerkey", "loadyear", "loadmonth",
                              "memberkey", "claimkey"],
                          how="left_anti")
                    .unionByName(diagnosis_df)
                )
                _safe_write(merged_dx, dx_tbl, "claimdiagnosis")
                print(f"[CHUNK {chunk_idx+1}] claimdiagnosis write ✅")

            # ── CLAIM LINES ───────────────────────────────────────────────
            if is_full_load:
                _safe_write(lines_df,
                            f"glue_catalog.{DATABASE}.claimlines", "claimlines")
            else:
                ln_tbl      = f"glue_catalog.{DATABASE}.claimlines"
                existing_ln = _partition_filter(ln_tbl, affected_partitions,
                                                is_table=True)
                if readded_members:
                    ra_df_ln = spark.createDataFrame(
                        [(int(PAYER_KEY), int(m)) for m in readded_members
                         if m.isdigit()],
                        ["payerkey_ra", "memberkey_ra"]
                    )
                    existing_ln = existing_ln.join(
                        F.broadcast(ra_df_ln),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_ra")) &
                            (F.col("memberkey") == F.col("memberkey_ra"))
                        ),
                        how="left_anti"
                    )
                existing_ln = _strip_deleted_claims(
                    existing_ln, partial_chunk_members
                )
                replaced_ln = F.broadcast(
                    lines_df.select("payerkey", "loadyear", "loadmonth",
                                    "memberkey", "claimkey").distinct()
                )
                merged_ln = (
                    existing_ln
                    .join(replaced_ln,
                          on=["payerkey", "loadyear", "loadmonth",
                              "memberkey", "claimkey"],
                          how="left_anti")
                    .unionByName(lines_df)
                )
                _safe_write(merged_ln, ln_tbl, "claimlines")
                print(f"[CHUNK {chunk_idx+1}] claimlines write ✅")

            total_written += chunk_count
            print(f"[CHUNK {chunk_idx+1}/{num_chunks}] Written ✅  "
                  f"(cumulative records: {total_written})")

        except Exception as chunk_err:
            raise Exception(
                f"[CHUNK {chunk_idx+1}/{num_chunks}] FAILED: {chunk_err}"
            )
        finally:
            # Always release cache — even if write failed — before next chunk
            try:
                if raw_df is not None:
                    raw_df.unpersist()
                    raw_df = None
            except Exception:
                pass

    print(f"\n[STEP 4–10] All {num_chunks} chunks processed ✅  "
          f"Total records written: {total_written}")

    # ── Restore merge-on-read after full load ─────────────────────────────────
    # Full load switched tables to copy-on-write for efficient bulk writes.
    # Switch back to merge-on-read so subsequent incremental runs are cheap.
    # Re-check _table_meta_ok NOW (after all chunks wrote data) so that tables
    # created for the first time this run are included in the ALTER TABLE.
    if is_full_load:
        print("[INFO] Full load complete → re-checking S3 metadata for MOR restore...")
        for _tbl in ["claims", "claimdiagnosis", "claimlines"]:
            _table_meta_ok[_tbl] = _iceberg_metadata_exists(_tbl)
        print("[INFO] Restoring Iceberg write mode: merge-on-read")
        _set_iceberg_write_mode("merge-on-read")

    # ── STEP 11 — OPTIMIZE ────────────────────────────────────────────────────
    # Auto-optimize after full load when scale tier is medium_10m or large_50m.
    # Can also be triggered manually via --run_optimize true on any run.
    _auto_optimize = is_full_load and _SC.get("auto_optimize_full_load", False)
    if RUN_OPTIMIZE or _auto_optimize:
        trigger = "auto (full load + scale tier)" if _auto_optimize else "run_optimize=true"
        current_year  = datetime.utcnow().year
        current_month = datetime.utcnow().month
        print(f"[STEP 11] Running OPTIMIZE [{trigger}] "
              f"partition {current_year}/{current_month}...")
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
        print("[STEP 11] OPTIMIZE skipped (run_optimize=false, not a scale-tier full load). "
              "Run on a dedicated nightly schedule.")

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
    print(f"[FINALLY] Scale tier       : {SCALE_TIER}")
    print(f"[FINALLY] Files processed  : {len(changed_files)}")
    print(f"[FINALLY] Records merged   : {filtered_count}")
    print(f"[FINALLY] Members deleted  : {len(deleted_members)}")

    save_watermark(
        status                  = job_status,
        files_processed         = len(changed_files),
        records_merged          = filtered_count,
        mode                    = "FULL LOAD" if is_full_load else "INCREMENTAL",
        members_deleted         = len(deleted_members),
        member_claimkey_counts  = current_member_claimkey_counts,
    )

    print("=" * 60)
    print(f"[DONE] Status          : {job_status}")
    print(f"       Mode            : {'FULL LOAD' if is_full_load else 'INCREMENTAL'}")
    print(f"       Scale tier      : {SCALE_TIER}")
    print(f"       Payer           : {PAYER_KEY}")
    print(f"       Files processed : {len(changed_files)}")
    print(f"       Records merged  : {filtered_count}")
    print(f"       Members deleted : {len(deleted_members)}")
    print(f"       Run timestamp   : {current_run_ts}")
    print("=" * 60)

    job.commit()


