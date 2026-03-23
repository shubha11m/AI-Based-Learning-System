"""
NT-Claims EMR Full Load Script
===============================
Runs ONCE per payer for historical full load.
Pure PySpark — no Glue runtime dependency.
Shares same Iceberg tables + watermark with the Glue incremental job.

Key differences from Glue Hello.py:
  ✅ No Glue imports — pure PySpark + argparse
  ✅ KryoSerializer set via spark-submit BEFORE SparkContext starts
  ✅ Full load ONLY — watermark absent = process ALL files
  ✅ Writes member_claimkey_sets to watermark (for Glue delete detection)
  ✅ Chunk checkpoint resume — survives spot interruptions
  ✅ copy-on-write during full load then back to merge-on-read after
  ✅ OPTIMIZE runs automatically after full load
  ✅ spark.stop() at end — cluster auto-terminates after step completes

Usage (spark-submit on EMR):
  spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --executor-memory 90g \\
    --executor-cores 4 \\
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
    --conf spark.sql.caseSensitive=true \\
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \\
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \\
    --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\
    --conf spark.sql.catalog.glue_catalog.warehouse=s3://nontrauma-analytics-prod/iceberg/ \\
    --jars s3://nontrauma-analytics-prod/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \\
    s3://nontrauma-analytics-prod/scripts/emr_ntclaims_full_load.py \\
    --payer_key 233 \\
    --env PROD \\
    --source_bucket nontrauma-claim-prod \\
    --target_bucket nontrauma-analytics-prod \\
    --database claims_db_prod \\
    --scale_tier small

Scale tiers (same as Glue Hello.py):
  small         <  500K files / <  50K members → 40  G.1X workers
  medium_10m    ~   10M files / ~ 100K members → 100 G.2X workers
  medium_250k   ~   20M files / ~ 300K members → 100 G.2X workers
  payer326      payer 326 actual (19.8M files)  → 200 G.2X workers
  large_50m     ~   50M files / ~ 500K members → 200 G.2X workers
"""

import sys
import os
import json
import argparse
import concurrent.futures
from collections import defaultdict
from datetime import datetime

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, ArrayType,
                                StringType, IntegerType, LongType)
from pyspark.storagelevel import StorageLevel

# =============================================================================
# ARGUMENT PARSING
# =============================================================================
parser = argparse.ArgumentParser(description="NT-Claims EMR Full Load")
parser.add_argument("--payer_key",     required=True)
parser.add_argument("--env",           required=True)
parser.add_argument("--source_bucket", default="nontrauma-claim-prod")
parser.add_argument("--target_bucket", default="nontrauma-analytics-prod")
parser.add_argument("--database",      default="claims_db_prod")
parser.add_argument("--scale_tier",    default="small",
                    choices=["small", "medium_10m", "medium_250k",
                             "payer326", "large_50m"])
parser.add_argument("--run_optimize",  default="true")
parser.add_argument("--force_restart", default="false",
                    help="Delete checkpoints and restart from chunk 0")

args          = parser.parse_args()
PAYER_KEY     = args.payer_key.strip()
ENV           = args.env
SOURCE_BUCKET = args.source_bucket
TARGET_BUCKET = args.target_bucket
DATABASE      = args.database
SCALE_TIER    = args.scale_tier.lower()
RUN_OPTIMIZE  = args.run_optimize.lower() == "true"
FORCE_RESTART = args.force_restart.lower() == "true"

# =============================================================================
# SPARK SESSION — pure PySpark, no Glue runtime
# NOTE: KryoSerializer MUST be set via spark-submit --conf before SparkContext.
#       Setting it here after SparkSession starts has NO effect.
# =============================================================================
spark = SparkSession.builder \
    .appName(f"NT-Claims-FullLoad-Payer-{PAYER_KEY}") \
    .getOrCreate()
sc = spark.sparkContext

# =============================================================================
# SCALE TIER CONFIG — identical to Hello.py
# =============================================================================
_SCALE_CONFIG = {
    "small": {
        "member_chunk_size":       1_000,
        "s3_list_workers":         40,
        "base_shuffle_partitions": 160,
        "files_per_partition":     100,
        "max_shuffle_partitions":  800,
        "iceberg_file_size_bytes": 64  * 1024 * 1024,
        "cache_level_ser":         False,
        "parallel_table_writes":   False,
        "single_read_cache":       False,
    },
    "medium_10m": {
        "member_chunk_size":       2_000,
        "s3_list_workers":         150,
        "base_shuffle_partitions": 400,
        "files_per_partition":     200,
        "max_shuffle_partitions":  4_000,
        "iceberg_file_size_bytes": 256 * 1024 * 1024,
        "cache_level_ser":         False,
        "parallel_table_writes":   False,
        "single_read_cache":       False,
    },
    "medium_250k": {
        "member_chunk_size":       160,
        "s3_list_workers":         150,
        "base_shuffle_partitions": 400,
        "files_per_partition":     500,
        "max_shuffle_partitions":  2_000,
        "iceberg_file_size_bytes": 256 * 1024 * 1024,
        "cache_level_ser":         False,
        "parallel_table_writes":   False,
        "single_read_cache":       False,
    },
    "payer326": {
        "member_chunk_size":       2_000,
        "s3_list_workers":         150,
        "base_shuffle_partitions": 600,
        "files_per_partition":     200,
        "max_shuffle_partitions":  4_000,
        "iceberg_file_size_bytes": 512 * 1024 * 1024,
        "cache_level_ser":         True,
        "parallel_table_writes":   True,
        "single_read_cache":       True,
    },
    "large_50m": {
        "member_chunk_size":       3_000,
        "s3_list_workers":         200,
        "base_shuffle_partitions": 1_000,
        "files_per_partition":     500,
        "max_shuffle_partitions":  8_000,
        "iceberg_file_size_bytes": 512 * 1024 * 1024,
        "cache_level_ser":         True,
        "parallel_table_writes":   True,
        "single_read_cache":       True,
    },
}

_SC                   = _SCALE_CONFIG[SCALE_TIER]
MEMBER_CHUNK_SIZE     = _SC["member_chunk_size"]
S3_LIST_WORKERS       = _SC["s3_list_workers"]
FILES_PER_PART        = _SC["files_per_partition"]
BASE_SHUFFLE_PART     = _SC["base_shuffle_partitions"]
MAX_SHUFFLE_PART      = _SC["max_shuffle_partitions"]
PARALLEL_TABLE_WRITES = _SC.get("parallel_table_writes", False)
SINGLE_READ_CACHE     = _SC.get("single_read_cache",     False)
WATERMARK_KEY         = f"watermarks/claims_payer_{PAYER_KEY}_last_run.json"
_FL_CHECKPOINT_PREFIX = f"checkpoints/fullload/claims_payer_{PAYER_KEY}_chunk_"

print(f"[ARGS] payer_key     = {PAYER_KEY}")
print(f"[ARGS] env           = {ENV}")
print(f"[ARGS] source_bucket = {SOURCE_BUCKET}")
print(f"[ARGS] target_bucket = {TARGET_BUCKET}")
print(f"[ARGS] database      = {DATABASE}")
print(f"[ARGS] scale_tier    = {SCALE_TIER}")
print(f"[ARGS] run_optimize  = {RUN_OPTIMIZE}")
print(f"[ARGS] force_restart = {FORCE_RESTART}")
print(f"[INFO] Chunk size    = {MEMBER_CHUNK_SIZE} members")
print(f"[INFO] S3 workers    = {S3_LIST_WORKERS}")

# =============================================================================
# SPARK RUNTIME CONFIG
# =============================================================================
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
spark.conf.set("spark.sql.shuffle.partitions",                        str(BASE_SHUFFLE_PART))
spark.conf.set("spark.sql.legacy.timeParserPolicy",                   "CORRECTED")
spark.conf.set("spark.sql.caseSensitive",                             "true")
spark.conf.set("spark.sql.session.timeZone",                          "UTC")
spark.conf.set("spark.sql.files.maxPartitionBytes",                   str(256 * 1024 * 1024))
spark.conf.set("spark.sql.files.openCostInBytes",                     str(8   * 1024 * 1024))
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping",   "true")

_MEM_DISK_SER = StorageLevel(True, True, False, False, 1)
_MEM_DISK     = StorageLevel(True, True, False, True,  1)
CACHE_LEVEL   = _MEM_DISK_SER if _SC["cache_level_ser"] else _MEM_DISK
print("[INFO] Spark runtime configs set ✅")

# =============================================================================
# AWS CLIENTS + TRACKING VARIABLES
# =============================================================================
s3_client                      = boto3.client("s3")
glue_client                    = boto3.client("glue")
job_status                     = "STARTED"
filtered_count                 = 0
changed_files                  = []
current_run_epoch_ms           = int(datetime.utcnow().timestamp() * 1000)
current_run_ts                 = datetime.utcnow().isoformat()
current_member_claimkey_counts = {}
current_member_claimkey_sets   = {}
raw_df                         = None

# =============================================================================
# HELPERS
# =============================================================================
def normalize_ts(col_ref):
    s             = col_ref.cast("string")
    epoch_ms_ts   = (col_ref.cast("long") / 1000).cast("timestamp")
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
                   member_claimkey_counts=None,
                   member_claimkey_sets=None):
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=WATERMARK_KEY,
            Body=json.dumps({
                "last_run_epoch_ms":      current_run_epoch_ms,
                "last_run_ts":            current_run_ts,
                "files_processed":        files_processed,
                "records_merged":         records_merged,
                "mode":                   mode,
                "job_status":             status,
                "payer_key":              PAYER_KEY,
                "env":                    ENV,
                "member_claimkey_counts": member_claimkey_counts or {},
                "member_claimkey_sets":   member_claimkey_sets   or {},
            })
        )
        print(f"[WATERMARK] Saved → status={status} ✅")
    except Exception as e:
        print(f"[WATERMARK] Failed to save: {e}")

def _table_exists_glue(database, table_name):
    try:
        glue_client.get_table(DatabaseName=database, Name=table_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False
    except Exception as e:
        print(f"[WARN] _table_exists_glue({table_name}): {e}")
        return False

def _iceberg_metadata_exists(table_name):
    prefix = f"iceberg/{table_name}/metadata/"
    try:
        resp = s3_client.list_objects_v2(
            Bucket=TARGET_BUCKET, Prefix=prefix, MaxKeys=1
        )
        return resp.get("KeyCount", 0) > 0
    except Exception:
        return False

# =============================================================================
# CHECKPOINT HELPERS
# =============================================================================
def save_chunk_checkpoint(chunk_idx, chunk_count, cumulative):
    key = f"{_FL_CHECKPOINT_PREFIX}{chunk_idx:04d}.json"
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=key,
            Body=json.dumps({
                "payer_key":   PAYER_KEY,
                "chunk_idx":   chunk_idx,
                "chunk_count": chunk_count,
                "cumulative":  cumulative,
                "saved_at":    datetime.utcnow().isoformat(),
            })
        )
        print(f"[CHECKPOINT] Chunk {chunk_idx} saved ✅ (cumulative: {cumulative})")
    except Exception as e:
        print(f"[CHECKPOINT] WARN — chunk {chunk_idx} save failed: {e}")

def load_last_checkpoint():
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        completed = []
        for page in paginator.paginate(
            Bucket=TARGET_BUCKET,
            Prefix=_FL_CHECKPOINT_PREFIX,
            PaginationConfig={"PageSize": 1000}
        ):
            for obj in page.get("Contents", []):
                fname = obj["Key"].split("/")[-1]
                part  = fname.replace(".json", "").split("chunk_")
                if len(part) == 2 and part[1].isdigit():
                    completed.append(int(part[1]))
        if completed:
            last = max(completed)
            print(f"[CHECKPOINT] Found {len(completed)} completed chunks. "
                  f"Last: {last}. Resuming from chunk {last + 1}.")
            return last
        print("[CHECKPOINT] No checkpoints — starting from chunk 0.")
        return -1
    except Exception as e:
        print(f"[CHECKPOINT] WARN — scan failed: {e}. Starting from chunk 0.")
        return -1

def delete_checkpoints():
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(
            Bucket=TARGET_BUCKET,
            Prefix=_FL_CHECKPOINT_PREFIX,
            PaginationConfig={"PageSize": 1000}
        ):
            for obj in page.get("Contents", []):
                keys.append({"Key": obj["Key"]})
        if keys:
            for i in range(0, len(keys), 1000):
                s3_client.delete_objects(
                    Bucket=TARGET_BUCKET,
                    Delete={"Objects": keys[i:i+1000]}
                )
            print(f"[CHECKPOINT] Deleted {len(keys)} checkpoint(s) ✅")
    except Exception as e:
        print(f"[CHECKPOINT] WARN — cleanup failed (non-fatal): {e}")

# =============================================================================
# ICEBERG TABLE DDL (copy-on-write for full load)
# =============================================================================
_ICEBERG_FILE_SIZE = _SC["iceberg_file_size_bytes"]

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
        'write.merge.mode'             = 'copy-on-write',
        'write.update.mode'            = 'copy-on-write',
        'write.delete.mode'            = 'copy-on-write'
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
        'write.merge.mode'             = 'copy-on-write',
        'write.update.mode'            = 'copy-on-write',
        'write.delete.mode'            = 'copy-on-write'
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
        'write.merge.mode'             = 'copy-on-write',
        'write.update.mode'            = 'copy-on-write',
        'write.delete.mode'            = 'copy-on-write'
    )
"""

_TABLE_DDL = {
    f"glue_catalog.{DATABASE}.claims":         CLAIMS_DDL,
    f"glue_catalog.{DATABASE}.claimdiagnosis": DIAGNOSIS_DDL,
    f"glue_catalog.{DATABASE}.claimlines":     LINES_DDL,
}

# =============================================================================
# WRITE HELPERS
# =============================================================================
def _is_nosuchkey(exc):
    return any(x in str(exc).lower() for x in
               ["nosuchkey", "no such key", "404"])

def _glue_drop_and_recreate(table_fqn, label):
    table_name = table_fqn.split(".")[-1]
    try:
        glue_client.delete_table(DatabaseName=DATABASE, Name=table_name)
        print(f"[WARN] {label}: stale Glue entry deleted ✅")
    except glue_client.exceptions.EntityNotFoundException:
        pass
    except Exception as e:
        print(f"[WARN] {label}: boto3 delete warning: {e}")
    ddl = _TABLE_DDL.get(table_fqn)
    if not ddl:
        raise Exception(f"{label}: no DDL for {table_fqn}")
    spark.sql(ddl)
    print(f"[WARN] {label}: table re-registered ✅")

def _safe_write(df, table_fqn, label):
    table_name = table_fqn.split(".")[-1]
    if not _iceberg_metadata_exists(table_name):
        print(f"[WARN] {label}: no S3 metadata — repairing catalog")
        _glue_drop_and_recreate(table_fqn, label)
        df.writeTo(table_fqn).append()
        print(f"[INFO] {label}: post-repair append ✅")
        return
    try:
        df.writeTo(table_fqn).overwritePartitions()
        print(f"[INFO] {label}: overwritePartitions ✅")
        return
    except Exception as e1:
        if not _is_nosuchkey(e1):
            raise
        print(f"[WARN] {label}: overwritePartitions NoSuchKey → append()")
    try:
        df.writeTo(table_fqn).append()
        print(f"[INFO] {label}: append ✅")
        return
    except Exception as e2:
        if not _is_nosuchkey(e2):
            raise
        print(f"[WARN] {label}: append NoSuchKey → boto3 repair")
    _glue_drop_and_recreate(table_fqn, label)
    df.writeTo(table_fqn).append()
    print(f"[INFO] {label}: post-repair append ✅")

def _safe_create_table(ddl, label):
    try:
        spark.sql(ddl)
        print(f"[STEP 5] {label} created ✅")
    except Exception as e:
        err = str(e).lower()
        if "already exists" in err:
            print(f"[STEP 5] {label} already exists ✅")
        else:
            print(f"[STEP 5] {label} CREATE warning: {e}")

def _alter_write_mode(mode):
    for tbl in ["claims", "claimdiagnosis", "claimlines"]:
        try:
            spark.sql(f"""
                ALTER TABLE glue_catalog.{DATABASE}.{tbl}
                SET TBLPROPERTIES (
                    'write.merge.mode'  = '{mode}',
                    'write.update.mode' = '{mode}',
                    'write.delete.mode' = '{mode}'
                )
            """)
        except Exception as e:
            print(f"[WARN] ALTER TABLE {tbl}: {e}")
    print(f"[INFO] Iceberg write mode → {mode} ✅")

# =============================================================================
# HELPER: coalesce camelCase + PascalCase column (used inside chunk loop)
# =============================================================================
def _c2(df, camel, pascal, cast_type="string"):
    """Return coalesced camelCase/PascalCase column or null if both absent."""
    if camel in df.columns and pascal in df.columns:
        return F.coalesce(F.col(camel), F.col(pascal)).cast(cast_type)
    elif camel in df.columns:
        return F.col(camel).cast(cast_type)
    elif pascal in df.columns:
        return F.col(pascal).cast(cast_type)
    else:
        return F.lit(None).cast(cast_type)

def _struct_c2(row_col, camel, pascal):
    """Coalesce struct fields (for exploded dx/ln rows)."""
    return F.coalesce(row_col.getField(camel), row_col.getField(pascal))

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
        print(f"[STEP 1] DB warning: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — FULL LOAD ALWAYS (EMR script = full load only)
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 2] EMR Full Load — will process ALL files for payer")
    last_run_epoch_ms = 0   # 0 = every file qualifies regardless of LastModified
    is_full_load      = True

    if FORCE_RESTART:
        print("[STEP 2] FORCE RESTART — deleting existing checkpoints...")
        delete_checkpoints()

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — SCAN ALL S3 FILES FOR THIS PAYER
    # ─────────────────────────────────────────────────────────────────────────
    print(f"[STEP 3] Scanning ALL S3 files — PAYER={PAYER_KEY}...")
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

        changed_file_meta = []
        members_found     = set()
        total_scanned     = 0
        failed_prefixes   = []

        _S3_RETRIES    = 5
        _S3_BASE_DELAY = 1.0

        def _list_member(prefix):
            import time as _time
            import botocore.exceptions as _bce
            member_key = prefix.rstrip("/").split("/")[-1]
            for attempt in range(1, _S3_RETRIES + 1):
                try:
                    results          = []
                    all_s3_claimkeys = set()
                    pager = s3_client.get_paginator("list_objects_v2")
                    for pg in pager.paginate(
                        Bucket=SOURCE_BUCKET, Prefix=prefix,
                        PaginationConfig={"PageSize": 1000}
                    ):
                        for obj in pg.get("Contents", []):
                            key = obj["Key"]
                            if not key.endswith(".json"):
                                continue
                            parts = key.split("/")
                            if len(parts) < 3:
                                continue
                            ck            = parts[2].replace(".json", "")
                            file_epoch_ms = int(
                                obj["LastModified"].timestamp() * 1000
                            )
                            all_s3_claimkeys.add(ck)
                            # Full load — include EVERY file
                            results.append({
                                "s3_path":         f"s3://{SOURCE_BUCKET}/{key}",
                                "path_payer_key":  parts[0],
                                "path_member_key": parts[1],
                                "path_claim_key":  ck,
                                "file_epoch_ms":   file_epoch_ms,
                            })
                    return results, all_s3_claimkeys, member_key
                except _bce.ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code", "")
                    if code in ("SlowDown", "503", "Throttling",
                                "RequestLimitExceeded", "ServiceUnavailable"):
                        if attempt < _S3_RETRIES:
                            delay = _S3_BASE_DELAY * (2 ** (attempt - 1))
                            print(f"[WARN] S3 throttle {prefix} "
                                  f"(attempt {attempt}) — retry in {delay}s")
                            _time.sleep(delay)
                            continue
                    return None, set(), member_key
                except Exception:
                    return None, set(), member_key
            return None, set(), member_key

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=S3_LIST_WORKERS) as pool:
            fs = {pool.submit(_list_member, pfx): pfx for pfx in member_prefixes}
            for future in concurrent.futures.as_completed(fs):
                results, all_s3_claimkeys, member_key = future.result()
                if results is None:
                    failed_prefixes.append(fs[future])
                    continue
                total_scanned += len(results)
                for rec in results:
                    changed_file_meta.append(rec)
                    members_found.add(member_key)
                if all_s3_claimkeys:
                    current_member_claimkey_counts[member_key] = len(all_s3_claimkeys)
                    current_member_claimkey_sets[member_key]   = list(all_s3_claimkeys)

        if failed_prefixes:
            raise Exception(
                f"[STEP 3] {len(failed_prefixes)} members failed S3 LIST "
                f"after {_S3_RETRIES} retries: {failed_prefixes[:10]}"
            )

        changed_files = [r["s3_path"] for r in changed_file_meta]
        print(f"[STEP 3] Total files     : {total_scanned}")
        print(f"[STEP 3] Unique members  : {len(members_found)}")
        if changed_files[:5]:
            for f in changed_files[:5]:
                print(f"         {f}")
            if len(changed_files) > 5:
                print(f"         ... and {len(changed_files) - 5} more")

    except Exception as e:
        raise Exception(f"[STEP 3] FAILED: {e}")

    if not changed_files:
        print("[STEP 3] No files found — nothing to do!")
        save_watermark("SUCCESS - NO FILES", 0, 0, "FULL LOAD - NO FILES")
        sys.exit(0)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — GROUP FILES INTO MEMBER CHUNKS
    # ─────────────────────────────────────────────────────────────────────────
    print(f"[STEP 4] Grouping {len(changed_file_meta)} files "
          f"into chunks of {MEMBER_CHUNK_SIZE} members...")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — CREATE ICEBERG TABLES
    # ─────────────────────────────────────────────────────────────────────────
    print("[STEP 5] Creating Iceberg tables if needed...")
    if not _table_exists_glue(DATABASE, "claims"):
        _safe_create_table(CLAIMS_DDL, "claims")
    else:
        print("[STEP 5] claims already exists ✅")
    if not _table_exists_glue(DATABASE, "claimdiagnosis"):
        _safe_create_table(DIAGNOSIS_DDL, "claimdiagnosis")
    else:
        print("[STEP 5] claimdiagnosis already exists ✅")
    if not _table_exists_glue(DATABASE, "claimlines"):
        _safe_create_table(LINES_DDL, "claimlines")
    else:
        print("[STEP 5] claimlines already exists ✅")

    # Switch to copy-on-write for bulk full load
    _alter_write_mode("copy-on-write")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6 — CHECKPOINT RESUME
    # ─────────────────────────────────────────────────────────────────────────
    last_completed = -1 if FORCE_RESTART else load_last_checkpoint()

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 7 — CHUNK LOOP: READ → TRANSFORM → WRITE → CHECKPOINT
    # ─────────────────────────────────────────────────────────────────────────
    member_to_files = defaultdict(list)
    for rec in changed_file_meta:
        member_to_files[rec["path_member_key"]].append(rec)

    all_members   = list(member_to_files.keys())
    num_chunks    = -(-len(all_members) // MEMBER_CHUNK_SIZE)
    total_written = 0

    print(f"[STEP 7] {len(all_members)} members → "
          f"{num_chunks} chunks of ≤{MEMBER_CHUNK_SIZE}")

    for chunk_idx in range(num_chunks):

        # Skip already-completed chunks
        if chunk_idx <= last_completed:
            print(f"[CHUNK {chunk_idx+1}/{num_chunks}] Skip (checkpoint) ✅")
            continue

        chunk_members = all_members[
            chunk_idx * MEMBER_CHUNK_SIZE : (chunk_idx + 1) * MEMBER_CHUNK_SIZE
        ]
        chunk_meta  = [rec for m in chunk_members for rec in member_to_files[m]]
        chunk_files = [r["s3_path"] for r in chunk_meta]

        print(f"\n[CHUNK {chunk_idx+1}/{num_chunks}] "
              f"{len(chunk_members)} members | {len(chunk_files)} files")

        raw_df = None
        try:
            # ── READ ──────────────────────────────────────────────────────────
            chunk_prefixes = list({
                f"s3://{SOURCE_BUCKET}/"
                f"{r['path_payer_key']}/{r['path_member_key']}/*.json"
                for r in chunk_meta
            })

            _json_df = (
                spark.read
                .option("multiline",           "true")
                .option("mode",                "PERMISSIVE")
                .option("recursiveFileLookup", "false")
                .json(chunk_prefixes)
                .withColumn("_input_file", F.input_file_name())
            )

            # Derive path keys
            path_parts = F.split(F.col("_input_file"), "/")
            _json_df = _json_df \
                .withColumn("path_payer_key",
                    F.element_at(path_parts, -3).cast("long")) \
                .withColumn("path_member_key",
                    F.element_at(path_parts, -2).cast("long")) \
                .withColumn("path_claim_key",
                    F.regexp_replace(
                        F.element_at(path_parts, -1), r"\.json$", ""
                    ).cast("long")) \
                .withColumn("file_epoch_ms",
                    F.lit(int(min(r["file_epoch_ms"] for r in chunk_meta))).cast("long"))

            _json_df = _json_df.filter(F.col("_input_file").isin(chunk_files))

            n_parts = max(4, min(MAX_SHUFFLE_PART,
                                 -(-len(chunk_files) // FILES_PER_PART)))
            spark.conf.set("spark.sql.shuffle.partitions", str(n_parts * 4))
            _json_df = _json_df.repartition(n_parts)
            _json_df.persist(CACHE_LEVEL)
            raw_df = _json_df

            chunk_raw_count = raw_df.count()
            print(f"[CHUNK {chunk_idx+1}] Cached {chunk_raw_count} rows "
                  f"across {n_parts} partitions")

            if chunk_raw_count == 0:
                print(f"[CHUNK {chunk_idx+1}] Empty — skipping")
                save_chunk_checkpoint(chunk_idx, 0, total_written)
                continue

            # ── RESOLVE KEYS ──────────────────────────────────────────────────
            raw_df = raw_df \
                .withColumn("resolved_payer_key",
                    F.coalesce(
                        _c2(raw_df, "payerKey",  "PayerKey",  "long"),
                        F.col("path_payer_key")
                    )) \
                .withColumn("resolved_member_key",
                    F.coalesce(
                        _c2(raw_df, "memberKey", "MemberKey", "long"),
                        F.col("path_member_key")
                    )) \
                .withColumn("resolved_claim_key",
                    F.coalesce(
                        _c2(raw_df, "claimKey",  "ClaimKey",  "long"),
                        F.col("path_claim_key")
                    )) \
                .withColumn("updated_at_epoch_ms",
                    F.coalesce(
                        to_epoch_ms(_c2(raw_df, "updatedAt", "UpdatedAt")),
                        F.col("file_epoch_ms"),
                        F.lit(0).cast("long")
                    )) \
                .filter(F.col("resolved_claim_key").isNotNull())

            if SINGLE_READ_CACHE:
                raw_df.unpersist()
                raw_df.persist(CACHE_LEVEL)

            chunk_count = raw_df.count()
            filtered_count += chunk_count
            print(f"[CHUNK {chunk_idx+1}] {chunk_count} processable records")
            if chunk_count == 0:
                save_chunk_checkpoint(chunk_idx, 0, total_written)
                continue

            # ── PARTITION COLUMNS ─────────────────────────────────────────────
            file_ts = (F.col("file_epoch_ms") / 1000).cast("timestamp")
            raw_df = raw_df \
                .withColumn("claim_load_ts",
                    normalize_ts(
                        _c2(raw_df, "claimLoadDateTime", "ClaimLoadDateTime")
                    )) \
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

            # ── BUILD CLAIMS DF ───────────────────────────────────────────────
            def sc2(c, p, t="string"):
                return _c2(raw_df, c, p, t)

            claims_df = raw_df.select(
                F.col("resolved_payer_key").alias("payerkey"),
                F.col("loadyear"),
                F.col("loadmonth"),
                F.col("resolved_member_key").alias("memberkey"),
                F.col("resolved_claim_key").alias("claimkey"),
                sc2("employerGroupKey",        "EmployerGroupKey",        "long") .alias("employergroupkey"),
                sc2("inboundBatchMasterKey",   "InboundBatchMasterKey",   "long") .alias("inboundbatchmasterkey"),
                sc2("batchRunSequence",        "BatchRunSequence",        "int")  .alias("batchrunsequence"),
                sc2("stageClaimKey",           "StageClaimKey",           "long") .alias("stageclaimkey"),
                sc2("claimNumber",             "ClaimNumber")                     .alias("claimnumber"),
                sc2("claimStatus",             "ClaimStatus")                     .alias("claimstatus"),
                sc2("claimSource",             "ClaimSource")                     .alias("claimsource"),
                sc2("claimType",               "ClaimType")                       .alias("claimtype"),
                sc2("claimMethod",             "ClaimMethod")                     .alias("claimmethod"),
                sc2("formType",                "FormType")                        .alias("formtype"),
                sc2("typeofBill",              "TypeofBill")                      .alias("typeofbill"),
                sc2("clientDataFeedCode",      "ClientDataFeedCode")              .alias("clientdatafeedcode"),
                sc2("sourceSystemID",          "SourceSystemID")                  .alias("sourcesystemid"),
                sc2("planType",                "PlanType")                        .alias("plantype"),
                sc2("unionType",               "UnionType")                       .alias("uniontype"),
                sc2("hospitalAccountNumber",   "HospitalAccountNumber")           .alias("hospitalaccountnumber"),
                sc2("patientDischargeStatus",  "PatientDischargeStatus")          .alias("patientdischargestatus"),
                sc2("placeofService",          "PlaceofService")                  .alias("placeofservice"),
                sc2("priorClaimReference",     "PriorClaimReference")             .alias("priorclaimreference"),
                sc2("manipulationReason",      "ManipulationReason")              .alias("manipulationreason"),
                sc2("billingProviderName",     "BillingProviderName")             .alias("billingprovidername"),
                sc2("billingProviderTIN",      "BillingProviderTIN")              .alias("billingprovidertin"),
                sc2("billingProviderNPI",      "BillingProviderNPI")              .alias("billingprovidernpi"),
                sc2("billingProviderID",       "BillingProviderID")               .alias("billingproviderid"),
                sc2("billingProviderAddress1", "BillingProviderAddress1")         .alias("billingprovideraddress1"),
                sc2("billingProviderAddress2", "BillingProviderAddress2")         .alias("billingprovideraddress2"),
                sc2("billingProviderCity",     "BillingProviderCity")             .alias("billingprovidercity"),
                sc2("billingProviderState",    "BillingProviderState")            .alias("billingproviderstate"),
                sc2("billingProviderZip",      "BillingProviderZip")              .alias("billingproviderzip"),
                sc2("billingProviderPhone",    "BillingProviderPhone")            .alias("billingproviderphone"),
                sc2("billingProviderEmail",    "BillingProviderEmail")            .alias("billingprovideremail"),
                sc2("billingProviderContactName",  "BillingProviderContactName")  .alias("billingprovidercontactname"),
                sc2("billingProviderContactPhone", "BillingProviderContactPhone") .alias("billingprovidercontactphone"),
                sc2("treatingPhysicianName",   "TreatingPhysicianName")           .alias("treatingphysicianname"),
                sc2("treatingProviderTIN",     "TreatingProviderTIN")             .alias("treatingprovidertin"),
                sc2("treatingProviderMedicare","TreatingProviderMedicare")        .alias("treatingprovidermedicare"),
                sc2("referringProviderTIN",    "ReferringProviderTIN")            .alias("referringprovidertin"),
                sc2("admitProviderTIN",        "AdmitProviderTIN")                .alias("admitprovidertin"),
                sc2("physicianProviderTIN",    "PhysicianProviderTIN")            .alias("physicianprovidertin"),
                sc2("providerType",            "ProviderType")                    .alias("providertype"),
                sc2("providerClass",           "ProviderClass")                   .alias("providerclass"),
                sc2("reimbursementMethod",     "ReimbursementMethod")             .alias("reimbursementmethod"),
                sc2("isCapitatedClaim",        "IsCapitatedClaim")                .alias("iscapitatedclaim"),
                sc2("isMedicare",              "IsMedicare")                      .alias("ismedicare"),
                sc2("isSplitClaim",            "IsSplitClaim")                    .alias("issplitclaim"),
                sc2("isWorkersComp",           "IsWorkersComp")                   .alias("isworkerscomp"),
                sc2("isParticipatingProvider", "IsParticipatingProvider")         .alias("isparticipatingprovider"),
                sc2("isEncounter",             "IsEncounter")                     .alias("isencounter"),
                sc2("adjustmentIndicator",     "AdjustmentIndicator")             .alias("adjustmentindicator"),
                sc2("assignmentFlag",          "AssignmentFlag")                  .alias("assignmentflag"),
                sc2("accidentFlag",            "AccidentFlag")                    .alias("accidentflag"),
                sc2("includeEncounterAsPaid",  "IncludeEncounterAsPaid")          .alias("includeencounteraspaid"),
                sc2("totalBilledAmount",       "TotalBilledAmount",       "decimal(18,2)").alias("totalbilledamount"),
                sc2("totalClientPaidAmount",   "TotalClientPaidAmount",   "decimal(18,2)").alias("totalclientpaidamount"),
                sc2("totalMemberPaidAmount",   "TotalMemberPaidAmount",   "decimal(18,2)").alias("totalmemberpaidamount"),
                sc2("checkNumber",             "CheckNumber")                     .alias("checknumber"),
                sc2("interestAllowed",         "InterestAllowed")                 .alias("interestallowed"),
                sc2("interestClaimKey",        "InterestClaimKey",        "long") .alias("interestclaimkey"),
                sc2("encounterClaimKey",       "EncounterClaimKey",       "long") .alias("encounterclaimkey"),
                sc2("encounterRelated",        "EncounterRelated")                .alias("encounterrelated"),
                sc2("encounterUnrelated",      "EncounterUnrelated")              .alias("encounterunrelated"),
                sc2("encounterClaimRequested", "EncounterClaimRequested")         .alias("encounterclaimrequested"),
                normalize_date(sc2("serviceBeginDate",  "ServiceBeginDate"))      .alias("servicebegindate"),
                normalize_date(sc2("serviceThruDate",   "ServiceThruDate"))       .alias("servicethrudate"),
                normalize_date(sc2("datePaid",          "DatePaid"))              .alias("datepaid"),
                normalize_date(sc2("claimReceivedDate", "ClaimReceivedDate"))     .alias("claimreceiveddate"),
                F.col("claim_load_ts")                                             .alias("claimloaddatetime"),
                normalize_ts(sc2("claimTransferredDateTime", "ClaimTransferredDateTime")).alias("claimtransferreddatetime"),
                normalize_ts(sc2("createdAt", "CreatedAt"))                        .alias("createdat"),
                normalize_ts(sc2("updatedAt", "UpdatedAt"))                        .alias("updatedat"),
                F.col("updated_at_epoch_ms")                                        .alias("updatedatepoch"),
                sc2("legacySource", "LegacySource")                                .alias("legacysource"),
                sc2("legacySchema", "LegacySchema")                                .alias("legacyschema"),
                sc2("legacyID",     "LegacyID")                                    .alias("legacyid"),
                sc2("trackingInfo", "TrackingInfo")                                .alias("trackinginfo"),
                F.col("updated_at_epoch_ms"),
            )
            _wc = Window.partitionBy(
                "payerkey", "loadyear", "loadmonth", "memberkey", "claimkey"
            ).orderBy(F.coalesce(F.col("updated_at_epoch_ms"), F.lit(0)).desc())
            claims_df = (
                claims_df
                .withColumn("_rn", F.row_number().over(_wc))
                .filter(F.col("_rn") == 1)
                .drop("_rn", "updated_at_epoch_ms")
            )

            # ── BUILD DIAGNOSIS DF ────────────────────────────────────────────
            dx_col = (
                "claimDiagnosisList"  if "claimDiagnosisList"  in raw_df.columns else
                "ClaimDiagnosisList"  if "ClaimDiagnosisList"  in raw_df.columns else None
            )
            if dx_col:
                _dx_exp = raw_df.select(
                    F.col("resolved_payer_key").alias("payerkey"),
                    F.col("resolved_member_key").alias("memberkey"),
                    F.col("resolved_claim_key").alias("claimkey"),
                    F.col("loadyear"), F.col("loadmonth"),
                    F.col("updated_at_epoch_ms").alias("claim_epoch"),
                    F.explode(
                        F.when(F.col(dx_col).isNotNull(), F.col(dx_col))
                         .otherwise(F.array())
                    ).alias("_r")
                )
                def _dx(f, p):
                    return _struct_c2(_dx_exp["_r"], f, p)

                diagnosis_df = _dx_exp.select(
                    "payerkey", "memberkey", "claimkey",
                    "loadyear", "loadmonth", "claim_epoch",
                    _dx("diagnosisCode",      "DiagnosisCode")     .cast("string") .alias("diagnosiscode"),
                    _dx("diagnosisOrder",     "DiagnosisOrder")    .cast("int")    .alias("diagnosisorder"),
                    _dx("isPrimary",          "IsPrimary")         .cast("string") .alias("isprimary"),
                    _dx("isSensitive",        "IsSensitive")       .cast("int")    .alias("issensitive"),
                    _dx("isTrauma",           "IsTrauma")          .cast("int")    .alias("istrauma"),
                    _dx("versionIndicator",   "VersionIndicator")  .cast("int")    .alias("versionindicator"),
                    _dx("clientDataFeedCode", "ClientDataFeedCode").cast("string") .alias("clientdatafeedcode"),
                    _dx("inboundBatchMasterKey","InboundBatchMasterKey").cast("long").alias("inboundbatchmasterkey"),
                    _dx("batchRunSequence",   "BatchRunSequence")  .cast("int")    .alias("batchrunsequence"),
                    _dx("claimDiagnosisKey",  "ClaimDiagnosisKey") .cast("long")   .alias("claimdiagnosiskey"),
                    F.lit(None).cast("timestamp")                                   .alias("updatedat"),
                    F.coalesce(F.col("claim_epoch"), F.lit(0).cast("long"))         .alias("updatedatepochms"),
                )
                _wd = Window.partitionBy(
                    "payerkey", "loadyear", "loadmonth",
                    "memberkey", "claimkey", "diagnosisorder"
                ).orderBy(F.coalesce(F.col("updatedatepochms"), F.lit(0)).desc())
                diagnosis_df = (
                    diagnosis_df
                    .withColumn("_rn", F.row_number().over(_wd))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn", "claim_epoch")
                )
            else:
                print(f"[CHUNK {chunk_idx+1}] No diagnosis list column found — empty DF")
                diagnosis_df = spark.createDataFrame(
                    [], spark.table(f"glue_catalog.{DATABASE}.claimdiagnosis").schema
                )

            # ── BUILD LINES DF ────────────────────────────────────────────────
            ln_col = (
                "claimLinesList"  if "claimLinesList"  in raw_df.columns else
                "ClaimLinesList"  if "ClaimLinesList"  in raw_df.columns else None
            )
            if ln_col:
                _ln_exp = raw_df.select(
                    F.col("resolved_payer_key").alias("payerkey"),
                    F.col("resolved_member_key").alias("memberkey"),
                    F.col("resolved_claim_key").alias("claimkey"),
                    F.col("loadyear"), F.col("loadmonth"),
                    F.col("updated_at_epoch_ms").alias("claim_epoch"),
                    F.explode(
                        F.when(F.col(ln_col).isNotNull(), F.col(ln_col))
                         .otherwise(F.array())
                    ).alias("_r")
                )
                def _ln(f, p):
                    return _struct_c2(_ln_exp["_r"], f, p)
                def _lnd(f, p):
                    return _ln(f, p).cast("decimal(18,2)")

                lines_df = _ln_exp.select(
                    "payerkey", "memberkey", "claimkey",
                    "loadyear", "loadmonth", "claim_epoch",
                    _ln("claimLineKey",            "ClaimLineKey")           .cast("long")          .alias("claimlinekey"),
                    _ln("claimLineNumber",         "ClaimLineNumber")        .cast("string")        .alias("claimlinenumber"),
                    _ln("procedureCode",           "ProcedureCode")          .cast("string")        .alias("procedurecode"),
                    _ln("procedureCodeType",       "ProcedureCodeType")      .cast("string")        .alias("procedurecodetype"),
                    _lnd("billedAmount",           "BilledAmount")                                  .alias("billedamount"),
                    _lnd("clientPaidAmount",       "ClientPaidAmount")                              .alias("clientpaidamount"),
                    _lnd("memberPaid",             "MemberPaid")                                    .alias("memberpaid"),
                    _lnd("allowedAmount",          "AllowedAmount")                                 .alias("allowedamount"),
                    _lnd("coveredAmount",          "CoveredAmount")                                 .alias("coveredamount"),
                    _lnd("discountAmount",         "DiscountAmount")                                .alias("discountamount"),
                    _ln("discountReason",          "DiscountReason")         .cast("string")        .alias("discountreason"),
                    _lnd("excludedAmount",         "ExcludedAmount")                                .alias("excludedamount"),
                    _ln("excludedReason",          "ExcludedReason")         .cast("string")        .alias("excludedreason"),
                    _lnd("withholdAmount",         "WithholdAmount")                                .alias("withholdamount"),
                    _ln("withholdReason",          "WithholdReason")         .cast("string")        .alias("withholdreason"),
                    _lnd("providerPaidAmount",     "ProviderPaidAmount")                            .alias("providerpaidamount"),
                    _lnd("originalClientPaidAmount","OriginalClientPaidAmount")                     .alias("originalclientpaidamount"),
                    _lnd("previousPaidAmount",     "PreviousPaidAmount")                            .alias("previouspaidamount"),
                    normalize_date(_ln("dateofServiceFrom","DateofServiceFrom"))                    .alias("dateofservicefrom"),
                    normalize_date(_ln("dateofServiceThru","DateofServiceThru"))                    .alias("dateofservicethru"),
                    _ln("modifierCode01",          "ModifierCode01")         .cast("string")        .alias("modifiercode01"),
                    _ln("modifierCode02",          "ModifierCode02")         .cast("string")        .alias("modifiercode02"),
                    _ln("placeofService",          "PlaceofService")         .cast("string")        .alias("placeofservice"),
                    _ln("revenueCode",             "RevenueCode")            .cast("string")        .alias("revenuecode"),
                    _ln("serviceType",             "ServiceType")            .cast("string")        .alias("servicetype"),
                    _ln("quantity",                "Quantity")               .cast("decimal(10,2)") .alias("quantity"),
                    _ln("houseCode",               "HouseCode")              .cast("string")        .alias("housecode"),
                    _ln("houseCodeDescription",    "HouseCodeDescription")   .cast("string")        .alias("housecodedescription"),
                    _ln("paymentType",             "PaymentType")            .cast("string")        .alias("paymenttype"),
                    _ln("paymentTypeID",           "PaymentTypeID")          .cast("string")        .alias("paymenttypeid"),
                    _ln("paymentComments",         "PaymentComments")        .cast("string")        .alias("paymentcomments"),
                    _ln("checkNumber",             "CheckNumber")            .cast("string")        .alias("checknumber"),
                    _ln("transactionCode",         "TransactionCode")        .cast("string")        .alias("transactioncode"),
                    _ln("transactionDescription",  "TransactionDescription") .cast("string")        .alias("transactiondescription"),
                    _ln("adjustmentFlag",          "AdjustmentFlag")         .cast("string")        .alias("adjustmentflag"),
                    _ln("isPrimaryNDC",            "IsPrimaryNDC")           .cast("string")        .alias("isprimaryndc"),
                    _ln("insuredTermDate",         "InsuredTermDate")        .cast("string")        .alias("insuredtermdate"),
                    _ln("manipulationReason",      "ManipulationReason")     .cast("string")        .alias("manipulationreason"),
                    _ln("claimDetailStatus",       "ClaimDetailStatus")      .cast("string")        .alias("claimdetailstatus"),
                    _ln("clientDataFeedCode",      "ClientDataFeedCode")     .cast("string")        .alias("clientdatafeedcode"),
                    _ln("inboundBatchMasterKey",   "InboundBatchMasterKey")  .cast("long")          .alias("inboundbatchmasterkey"),
                    _ln("batchRunSequence",        "BatchRunSequence")       .cast("int")           .alias("batchrunsequence"),
                    _ln("stageClaimLineKey",       "StageClaimLineKey")      .cast("long")          .alias("stageclaimlinekey"),
                    normalize_ts(_ln("updatedAt",  "UpdatedAt"))                                     .alias("updatedat"),
                    normalize_ts(_ln("createdAt",  "CreatedAt"))                                     .alias("createdat"),
                    F.coalesce(
                        to_epoch_ms(_ln("updatedAt", "UpdatedAt")),
                        F.col("claim_epoch"),
                        F.lit(0).cast("long")
                    ).alias("updatedatepochms"),
                )
                _wl = Window.partitionBy(
                    "payerkey", "loadyear", "loadmonth",
                    "memberkey", "claimkey", "claimlinenumber"
                ).orderBy(F.coalesce(F.col("updatedatepochms"), F.lit(0)).desc())
                lines_df = (
                    lines_df
                    .withColumn("_rn", F.row_number().over(_wl))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn", "claim_epoch")
                )
            else:
                print(f"[CHUNK {chunk_idx+1}] No lines list column found — empty DF")
                lines_df = spark.createDataFrame(
                    [], spark.table(f"glue_catalog.{DATABASE}.claimlines").schema
                )

            # ── WRITE ─────────────────────────────────────────────────────────
            if PARALLEL_TABLE_WRITES:
                import concurrent.futures as _cf
                def _wc_():
                    _safe_write(claims_df,
                                f"glue_catalog.{DATABASE}.claims", "claims")
                def _wd_():
                    _safe_write(diagnosis_df,
                                f"glue_catalog.{DATABASE}.claimdiagnosis",
                                "claimdiagnosis")
                def _wl_():
                    _safe_write(lines_df,
                                f"glue_catalog.{DATABASE}.claimlines",
                                "claimlines")
                with _cf.ThreadPoolExecutor(max_workers=3) as ex:
                    fws = [ex.submit(_wc_), ex.submit(_wd_), ex.submit(_wl_)]
                    for fw in _cf.as_completed(fws):
                        fw.result()
            else:
                _safe_write(claims_df,
                            f"glue_catalog.{DATABASE}.claims", "claims")
                _safe_write(diagnosis_df,
                            f"glue_catalog.{DATABASE}.claimdiagnosis",
                            "claimdiagnosis")
                _safe_write(lines_df,
                            f"glue_catalog.{DATABASE}.claimlines",
                            "claimlines")

            total_written += chunk_count
            save_chunk_checkpoint(chunk_idx, chunk_count, total_written)
            print(f"[CHUNK {chunk_idx+1}/{num_chunks}] Done ✅ "
                  f"(cumulative: {total_written})")

        except Exception as ce:
            raise Exception(
                f"[CHUNK {chunk_idx+1}/{num_chunks}] FAILED: {ce}"
            )
        finally:
            try:
                if raw_df is not None:
                    raw_df.unpersist()
                    raw_df = None
            except Exception:
                pass

    print(f"\n[STEP 7] All {num_chunks} chunks done ✅  "
          f"Total records: {total_written}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 8 — RESTORE MERGE-ON-READ (Glue incremental needs this)
    # ─────────────────────────────────────────────────────────────────────────
    _alter_write_mode("merge-on-read")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 9 — OPTIMIZE (compact small files written during full load)
    # ─────────────────────────────────────────────────────────────────────────
    if RUN_OPTIMIZE:
        print("[STEP 9] Running OPTIMIZE on all partitions...")
        for tbl in ["claims", "claimdiagnosis", "claimlines"]:
            try:
                spark.sql(f"""
                    OPTIMIZE glue_catalog.{DATABASE}.{tbl}
                    WHERE payerkey = {int(PAYER_KEY)}
                """)
                print(f"[STEP 9] OPTIMIZE {tbl} ✅")
            except Exception as e:
                print(f"[STEP 9] OPTIMIZE {tbl} warning (non-fatal): {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 10 — CLEAN UP CHECKPOINTS + SAVE WATERMARK
    # ─────────────────────────────────────────────────────────────────────────
    delete_checkpoints()
    job_status = "SUCCESS"
    print("[INFO] EMR Full Load completed successfully ✅")

except Exception as e:
    job_status = "FAILED"
    print(f"[ERROR] Job failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    try:
        if raw_df is not None:
            raw_df.unpersist()
    except Exception:
        pass

    print(f"[FINALLY] Status  : {job_status}")
    print(f"[FINALLY] Files   : {len(changed_files)}")
    print(f"[FINALLY] Records : {filtered_count}")

    # Save watermark ONLY on SUCCESS
    # If failed → watermark absent → Glue will do full load (correct behaviour)
    if job_status == "SUCCESS":
        save_watermark(
            status                 = "SUCCESS",
            files_processed        = len(changed_files),
            records_merged         = filtered_count,
            mode                   = "FULL LOAD",
            member_claimkey_counts = current_member_claimkey_counts,
            member_claimkey_sets   = current_member_claimkey_sets,
        )
    else:
        print("[FINALLY] Watermark NOT saved (job failed). "
              "Re-run will resume from last checkpoint.")

    print("=" * 60)
    print(f"[DONE] Status  : {job_status}")
    print(f"       Payer   : {PAYER_KEY}")
    print(f"       Files   : {len(changed_files)}")
    print(f"       Records : {filtered_count}")
    print(f"       Time    : {current_run_ts}")
    print("=" * 60)

    spark.stop()   # allows EMR step to complete and cluster to auto-terminate
