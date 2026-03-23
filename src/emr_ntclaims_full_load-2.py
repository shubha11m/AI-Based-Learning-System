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

13. MEMORY_AND_DISK serialised cache for large chunks
    At 50 M scale a single chunk can be 50 K rows × wide schema.
    Serialised storage (deserialized=False in PySpark StorageLevel constructor)
    halves executor heap pressure vs raw object storage.
    Note: StorageLevel.MEMORY_AND_DISK_SER is Scala-only and raises
    AttributeError in PySpark — use StorageLevel(True,True,False,False,1).

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

17. DUAL JSON TYPE HANDLING (2026-03-16)
    Two JSON casing conventions arrive from upstream systems:
      Type 1 — PascalCase top-level keys AND PascalCase array-item keys
               e.g. PayerKey, MemberKey, ClaimKey, ClaimLoadDateTime,
                    UpdatedAt, DiagnosisCode, ClaimLineKey …
      Type 2 — camelCase top-level keys AND camelCase array-item keys
               e.g. payerKey, memberKey, claimKey, claimLoadDateTime,
                    updatedAt, diagnosisCode, claimLineKey …

    Three rules applied everywhere:

    Rule 1 — spark.sql.caseSensitive=true
      Required so that PascalCase and camelCase variants declared in the
      same StructType are treated as DISTINCT columns (no duplicate-column
      AnalysisException).

    Rule 2 — All resolved_* / claim_load_ts columns use guarded coalesces
      Python-time checks on _json_df.columns prevent referencing a missing
      column under caseSensitive=true.  Example:
        F.col("payerKey").cast("long") if "payerKey" in _json_df.columns …

    Rule 3 — safe_col() helper coalesces both casings for every top-level
      claim field so STEP 7 (claims DF) works for both types.

    Rule 4 — Explicit read schemas (_DX_READ_SCHEMA / _LN_READ_SCHEMA)
      declare BOTH camelCase + PascalCase top-level keys AND both possible
      array-column names (e.g. claimDiagnosisList + ClaimDiagnosisList) so
      a SINGLE spark.read handles either JSON type without two passes.

    Rule 5 — _dx() / _ln() coalesce helpers pick the correct struct field:
        coalesce(_dxrow.diagnosisCode, _dxrow.DiagnosisCode)
      → non-null in exactly one casing per row.

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
import argparse
import concurrent.futures
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, ArrayType,
                                StringType, IntegerType, LongType)
from pyspark.storagelevel import StorageLevel
from datetime import datetime

# ── Job arguments ─────────────────────────────────────────────────────────────
# EMR uses argparse (spark-submit --payer_key 233) instead of Glue
# getResolvedOptions.  All argument names and defaults are identical to the
# Glue version so both scripts stay in sync.
#
# spark-submit passes the script path as the first positional arg.
# argparse.parse_known_args() ignores unknown Spark internal args safely.
_parser = argparse.ArgumentParser(description="NT-Claims EMR Full Load")
_parser.add_argument("--payer_key",     required=True,
                     help="Payer key e.g. 233")
_parser.add_argument("--param1",        required=True,
                     help="ENV e.g. DEV / UAT / PROD")
_parser.add_argument("--param2",        default="EMR-RUN",
                     help="RUN label e.g. WORKDAY-5")
_parser.add_argument("--scale_tier",    default="small",
                     help="micro|small|medium_10m|medium_250k|payer326|large_50m")
_parser.add_argument("--run_optimize",  default="false")
_parser.add_argument("--source_bucket", default="nontrauma-claim-dev")
_parser.add_argument("--target_bucket", default="nontrauma-analytics-dev")
_parser.add_argument("--database",      default="claims_db_dev")

_args, _unknown = _parser.parse_known_args()

SCALE_TIER    = _args.scale_tier.lower()
RUN_OPTIMIZE  = _args.run_optimize.lower() == "true"
SOURCE_BUCKET = _args.source_bucket
TARGET_BUCKET = _args.target_bucket
DATABASE      = _args.database

print(f"[ARGS] param1(ENV)   = {_args.param1}")
print(f"[ARGS] param2(RUN)   = {_args.param2}")
print(f"[ARGS] payer_key     = {_args.payer_key}")
print(f"[ARGS] source_bucket = {SOURCE_BUCKET}")
print(f"[ARGS] target_bucket = {TARGET_BUCKET}")
print(f"[ARGS] database      = {DATABASE}")
print(f"[ARGS] scale_tier    = {SCALE_TIER}")
print(f"[ARGS] run_optimize  = {RUN_OPTIMIZE}")

# ── EMR / Spark bootstrap ─────────────────────────────────────────────────────
# Pure PySpark — no Glue runtime dependency.
# KyrioSerializer MUST be set via spark-submit --conf before SparkContext starts.
# Setting it here after SparkSession is already running has NO effect and causes
# cache corruption (StreamCorruptedException: invalid type code: 00).
try:
    spark = SparkSession.builder \
        .appName(f"NT-Claims-FullLoad-Payer-{_args.payer_key}") \
        .getOrCreate()
    sc = spark.sparkContext
    print(f"[BOOTSTRAP] SparkSession created ✅  "
          f"Spark version: {spark.version}  "
          f"App ID: {sc.applicationId}")
except Exception as _spark_init_err:
    print(f"[BOOTSTRAP] FATAL — SparkSession creation failed: {_spark_init_err}")
    import traceback; traceback.print_exc()
    sys.exit(1)

ENV       = _args.param1
RUN       = _args.param2
PAYER_KEY = _args.payer_key.strip()   # e.g. "366" — drives S3 prefix + watermark

# ── Scale tier configuration ──────────────────────────────────────────────────
# Pass --scale_tier micro | small | medium_10m | large_50m as Glue job param.
# Defaults to 'small' if not supplied (backward-compatible with existing jobs).
#
# HOW TO CHOOSE YOUR TIER  (payer 366 is the reference: 19,915 members / 62,936 files)
# ─────────────────────────────────────────────────────────────────────────────────────
#
#   micro                 <  500K  files /   < 5K  members  → tiny payers, 10 workers
#   small                 <  500K  files /  < 50K  members  → payer 366 (62K files/19K members) ✓
#   medium_10m            ~   10M  files / ~ 100K  members  → mid-size payers, 100 workers
#   medium_250k           ~  20M+  files / ~ 300K  members  → payer 326 ACTUAL (19.8M/264K) ✓
#   medium_250k_lowdensity ~  800K  files / ~ 300K  members  → payer 326 ORIGINAL ESTIMATE
#   large_50m             ~   50M  files / ~ 500K  members  → large payers, 200 workers
#
# PAYER 366 → use --scale_tier small
#   Members : 19,915  (< 50K ✓)
#   Files   : 62,936  (< 500K ✓)
#   Workers : 40 G.1X → 62K files in ~20 chunks of 1K members → ~80 min total
#
# PAYER 326 → use --scale_tier medium_250k  (CORRECTED after first real run 2026-03-17)
#   Members : 264,260
#   Files   : 19,841,184  (~75 files/member — much higher than initial estimate of ~3)
#   Workers : 100 G.2X
#   Chunks  : ceil(264,260 ÷ 160) = 1,652 chunks × ~15 min = ~17h total
#   Timeout : 1,440 min (24h) — set in Glue job settings
#   Note    : If job times out mid-run, re-run resumes from last checkpoint automatically.
#
# KEY TUNING — files_per_partition:
#   With multiline JSON Spark creates 1 task per input FILE.
#   files_per_partition controls how many files are grouped into one
#   partition BEFORE persist() via repartition().
#   Too small → thousands of tiny tasks → scheduler overhead → slow.
#   Rule of thumb: target 20–50 total partitions per chunk.
#
# Tier                    Files       Members  Chunk  ListW  Shuffle  FileMB  Workers
# micro                   < 500K      <  5K      500     20     100      32    10 G.1X
# small                   < 500K      < 50K    1 000     40     160      64    40 G.1X  ← payer 366
# medium_10m              ~  10M      ~100K    2 000    150     400     256   100 G.2X
# medium_250k             ~  20M+     ~300K      160    150     400     256   100 G.2X  ← payer 326 ACTUAL
# medium_250k_lowdensity  ~ 800K      ~300K    3 000    150     600     256   100 G.2X  ← payer 326 estimate
# large_50m               ~  50M      ~500K    3 000    200   1 000     512   200 G.2X

_SCALE_CONFIG = {
    # ── micro ── < 500K files / < 5K members ──────────────────────────────────
    "micro": {
        "member_chunk_size":        500,
        "s3_list_workers":          20,
        "base_shuffle_partitions":  100,
        "files_per_partition":      25,
        "max_shuffle_partitions":   400,
        "iceberg_file_size_bytes":  32  * 1024 * 1024,
        "cache_level_ser":          False,
        "glue_worker_type":         "G.1X",
        "glue_workers":             10,
        "glue_timeout_min":         120,
        "auto_optimize_full_load":  False,
        "parallel_table_writes":    False,
        "single_read_cache":        False,
    },
    # ── small ── < 500K files / < 50K members  (payer 366: 62K files / 19K members) ──
    # TUNED 2026-03-16:
    #   files_per_partition 50 → 100  : 3K files/chunk → 30 partitions (was 65)
    #                                   Halves the repartition shuffle cost.
    #   base_shuffle_partitions 200→160: matches ~30 partitions × 4 with AQE headroom.
    #   glue_workers 20 → 40          : doubles parallelism → ~4 min/chunk (was ~8 min).
    #                                   Use G.1X × 40 in Glue job settings.
    "small": {
        "member_chunk_size":        1_000,
        "s3_list_workers":          40,
        "base_shuffle_partitions":  160,
        "files_per_partition":      100,
        "max_shuffle_partitions":   800,
        "iceberg_file_size_bytes":  64  * 1024 * 1024,
        "cache_level_ser":          False,
        "glue_worker_type":         "G.1X",
        "glue_workers":             40,
        "glue_timeout_min":         240,
        "auto_optimize_full_load":  False,
        "parallel_table_writes":    False,
        "single_read_cache":        False,
    },
    # ── medium_10m ── ~10M files / ~100K members ──────────────────────────────
    # base_shuffle_partitions lowered 1600 → 400.  Old value gave 1600 shuffle
    # tasks for a 9K-row chunk — massive scheduler overhead.  AQE coalesces.
    # files_per_partition raised 50 → 200 so 9K files → 45 partitions (not 189).
    # cache_level_ser False — serialised cache adds CPU cost for small data.
    "medium_10m": {
        "member_chunk_size":        2_000,
        "s3_list_workers":          150,
        "base_shuffle_partitions":  400,
        "files_per_partition":      200,
        "max_shuffle_partitions":   4_000,
        "iceberg_file_size_bytes":  256 * 1024 * 1024,
        "cache_level_ser":          False,
        "glue_worker_type":         "G.2X",
        "glue_workers":             100,
        "glue_timeout_min":         720,
        "auto_optimize_full_load":  True,
        "parallel_table_writes":    False,
        "single_read_cache":        False,
    },
    # ── medium_250k ── ~1M files / ~300K members  (payer 326: 264K members) ──────
    # ORIGINAL ESTIMATE (2026-03-16):
    #   Estimated files: 264K members × ~3 files = ~800K files.
    #   chunk_size=3000 → ~88 chunks → ~9K files/chunk → ~5 min/chunk.
    #
    # ACTUAL OBSERVED (2026-03-17, first real run):
    #   Actual files:    19,841,184  (~19.8M — payer 326 has ~75 files/member)
    #   chunk_size=3000 → 89 chunks → ~223K files/chunk → ~3h 10m/chunk ← TOO SLOW
    #   Root cause: each member has ~75 JSON files, not ~3.
    #
    # CORRECTED TUNING:
    #   Target ~12,000 files/chunk (manageable for 100 G.2X workers in ~15 min).
    #   New chunk_size = 12,000 ÷ 75 files/member ≈ 160 members/chunk.
    #   Chunks = ceil(264,260 ÷ 160) = 1,652 chunks × ~15 min ≈ ~17h total.
    #
    #   To keep chunk time under 15 min with 100 G.2X workers:
    #     files_per_partition=500: 12K files/chunk ÷ 500 = ~24 partitions
    #     base_shuffle_partitions=400: 24 × 4 = 96, floored at 400
    #     max_shuffle_partitions=2000: headroom for AQE on large partition skew
    #
    #   Glue timeout 1440 min (24h):  1,652 chunks × ~15 min worst-case = ~17h < 24h ✓
    #   On re-run: checkpoints let job resume from last completed chunk automatically.
    #
    # USE THIS TIER when you know the payer has ~3 files/member (low-density):
    "medium_250k": {
        "member_chunk_size":        160,
        "s3_list_workers":          150,
        "base_shuffle_partitions":  400,
        "files_per_partition":      500,
        "max_shuffle_partitions":   2_000,
        "iceberg_file_size_bytes":  256 * 1024 * 1024,
        "cache_level_ser":          False,
        "glue_worker_type":         "G.2X",
        "glue_workers":             100,
        "glue_timeout_min":         1_440,
        "auto_optimize_full_load":  True,
        "parallel_table_writes":    False,
        "single_read_cache":        False,
    },
    # ── medium_250k_lowdensity ── ~800K files / ~300K members (~3 files/member) ──
    # Use this for payers with ~3 files/member (the original medium_250k assumption).
    # chunk_size=3000 members × 3 files = ~9K files/chunk → ~5 min/chunk.
    # 88 chunks × 5 min = ~7.5h total.  Fits in 960 min timeout.
    "medium_250k_lowdensity": {
        "member_chunk_size":        3_000,
        "s3_list_workers":          150,
        "base_shuffle_partitions":  600,
        "files_per_partition":      300,
        "max_shuffle_partitions":   4_000,
        "iceberg_file_size_bytes":  256 * 1024 * 1024,
        "cache_level_ser":          False,
        "glue_worker_type":         "G.2X",
        "glue_workers":             100,
        "glue_timeout_min":         960,
        "auto_optimize_full_load":  True,
        "parallel_table_writes":    False,
        "single_read_cache":        False,
    },
    # ── payer326 ── OPTIMIZED for payer 326 actual profile (2026-03-17) ──────
    #
    # PROBLEM DIAGNOSIS (from actual chunk 1 log):
    #   226,122 files → 754 partitions → ~300 files/task
    #   Each task opens ~300 S3 HTTP connections ONE AT A TIME inside the JSON reader.
    #   persist().count() alone took 3h 08m for 100 G.2X workers.
    #   Total per chunk: ~3h 10m  →  89 chunks × 3h 10m = ~12 days.
    #
    # OPTIMIZATIONS APPLIED:
    #
    # 1. SINGLE S3 READ → 3 DFs from 1 cache  (biggest win: ~60% time reduction)
    #    Currently: spark.read.json() called 3 times (claims + diagnosis + lines)
    #               → 226K files read 3× from S3 = 678K S3 GET requests per chunk
    #    Fix: read once → persist to cache → derive all 3 DFs from the SAME cache
    #               → 226K S3 GET requests per chunk  (3× fewer S3 reads)
    #    Config key: single_read_cache = True
    #
    # 2. PARALLEL ICEBERG WRITES  (~3× write speed)
    #    Currently: claims → wait → claimdiagnosis → wait → claimlines  (sequential)
    #    Fix: ThreadPoolExecutor(3) → all 3 writes run simultaneously
    #    Config key: parallel_table_writes = True
    #
    # 3. 200 G.2X WORKERS  (2× parallelism vs 100)
    #    Each G.2X = 8 vCPU, 32 GB RAM.
    #    200 workers × 8 vCPU = 1,600 vCPUs available.
    #    More S3 GET bandwidth and more concurrent Spark tasks.
    #
    # 4. CHUNK SIZE 2,000 members × 75 files = ~150,000 files/chunk
    #    files_per_partition = 200 → 150K ÷ 200 = 750 partitions → ~200 files/task
    #    (was 300 files/task — reducing task granularity cuts per-task S3 wait)
    #    With 1,600 vCPUs: 750 partitions processed in ~0.5 passes → fast
    #
    # 5. SERIALISED CACHE (cache_level_ser = True)
    #    At 150K files the cached raw_df is large — serialised storage halves
    #    executor heap pressure, reducing GC pauses during the 3 DF derivations.
    #
    # EXPECTED PERFORMANCE:
    #   Per chunk:
    #     S3 read (150K files, 1600 vCPU, 750 partitions):  ~15 min  (was 3h 08m)
    #     3 parallel Iceberg writes:                         ~3 min   (was 15 min seq)
    #     Total per chunk:                                   ~18 min
    #   Total:
    #     ceil(264,260 ÷ 2000) = 133 chunks × 18 min = ~40h  ← still too slow
    #
    # FURTHER REDUCTION — raise workers to 200, lower chunk to 1000 members:
    #   1000 × 75 = 75K files/chunk ÷ 200 files/partition = 375 partitions
    #   With 1600 vCPU: 375 tasks in ~0.25 pass → ~8 min S3 read
    #   Parallel writes: ~2 min
    #   Total per chunk: ~10 min
    #   ceil(264,260 ÷ 1000) = 265 chunks × 10 min = ~44h  ← still too slow
    #
    # THE REAL FIX — Use ALL 3 optimizations together:
    #   single_read_cache=True cuts 3× S3 reads to 1× → 3h 08m → ~1h 03m
    #   200 workers (2×) → ~32 min read
    #   parallel_table_writes → saves ~10 min per chunk (3 seq writes → 1 parallel)
    #   Final: ~32 min read + ~3 min write = ~35 min/chunk for 2000-member chunks
    #   265 chunks × 35 min / 60 = ~9.5h  ✅ within 10-12h target
    #
    # Glue timeout: 1440 min (24h) — plenty of buffer for resume
    "payer326": {
        "member_chunk_size":        2_000,   # 2000 × 75 files = ~150K files/chunk
        "s3_list_workers":          150,
        "base_shuffle_partitions":  600,
        "files_per_partition":      200,     # 150K ÷ 200 = 750 partitions → ~200 files/task
        "max_shuffle_partitions":   4_000,
        "iceberg_file_size_bytes":  512 * 1024 * 1024,   # 512 MB — fewer metadata files
        "cache_level_ser":          True,    # serialised cache → less GC on large raw_df
        "glue_worker_type":         "G.2X",
        "glue_workers":             200,     # 200 × 8 vCPU = 1600 vCPUs
        "glue_timeout_min":         1_440,   # 24h — 133 chunks × 35 min + buffer
        "auto_optimize_full_load":  True,
        "parallel_table_writes":    True,    # write claims+diagnosis+lines simultaneously
        "single_read_cache":        True,    # read S3 files ONCE, derive 3 DFs from cache
    },
    # ── large_50m ── ~50M files / ~500K members ───────────────────────────────
    "large_50m": {
        "member_chunk_size":        3_000,
        "s3_list_workers":          200,
        "base_shuffle_partitions":  1_000,
        "files_per_partition":      500,
        "max_shuffle_partitions":   8_000,
        "iceberg_file_size_bytes":  512 * 1024 * 1024,
        "cache_level_ser":          True,
        "glue_worker_type":         "G.2X",
        "glue_workers":             200,
        "glue_timeout_min":         960,
        "auto_optimize_full_load":  True,
        "parallel_table_writes":    True,
        "single_read_cache":        True,
    },
}

# SCALE_TIER is already parsed from sys.argv above via _get_optional_arg().
# Validate here and fall back to 'small' if an unrecognised value was passed.
if SCALE_TIER not in _SCALE_CONFIG:
    print(f"[WARN] Unknown scale_tier='{SCALE_TIER}' — falling back to 'small'")
    SCALE_TIER = 'small'
_SC = _SCALE_CONFIG[SCALE_TIER]

# ── Derived constants from scale config ───────────────────────────────────────
# SOURCE_BUCKET, TARGET_BUCKET, DATABASE, PAYER_KEY all come from job params above.
# Watermark is per-payer so each payer's run is fully isolated.
WATERMARK_KEY         = f"watermarks/claims_payer_{PAYER_KEY}_last_run.json"
MEMBER_CHUNK_SIZE     = _SC["member_chunk_size"]
S3_LIST_WORKERS       = _SC["s3_list_workers"]
FILES_PER_PART        = _SC["files_per_partition"]
BASE_SHUFFLE_PART     = _SC["base_shuffle_partitions"]
MAX_SHUFFLE_PART      = _SC["max_shuffle_partitions"]
PARALLEL_TABLE_WRITES = _SC.get("parallel_table_writes", False)
SINGLE_READ_CACHE     = _SC.get("single_read_cache",     False)

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
# Note: spark.sql.iceberg.handle-timestamp-without-timezone is NOT a valid
# Spark/Iceberg config key.  UTC timezone handling is covered by
# spark.sql.session.timeZone = UTC (set below) and
# spark.sql.legacy.timeParserPolicy = CORRECTED (also set below).
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
# Case-sensitive mode is REQUIRED so that explicit schemas with both
# camelCase (Type 2) and PascalCase (Type 1) field names are treated as
# distinct columns — e.g. "diagnosisCode" ≠ "DiagnosisCode".
# Without this, Spark's default case-insensitive mode raises:
#   AnalysisException: Found duplicate column(s) in the schema
spark.conf.set("spark.sql.caseSensitive",                             "true")
spark.conf.set("spark.sql.session.timeZone",                          "UTC")
spark.conf.set("spark.sql.files.maxPartitionBytes",                   str(256 * 1024 * 1024))
spark.conf.set("spark.sql.files.openCostInBytes",                     str(8   * 1024 * 1024))
# Enable Iceberg partition pruning predicate pushdown
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping",   "true")
# NOTE: spark.serializer MUST be set before SparkContext starts (via Glue job
# parameters --conf spark.serializer=org.apache.spark.serializer.KryoSerializer).
# Setting it via sc._conf.set() AFTER SparkContext is running has NO effect.

# Cache level: serialised at medium/large scale to halve executor heap pressure.
# NOTE: StorageLevel.MEMORY_AND_DISK_SER is a Scala-only constant — it does NOT
# exist in PySpark's StorageLevel class and raises AttributeError at runtime.
# The PySpark equivalent is constructed directly via the StorageLevel constructor:
#   StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
#   deserialized=False  → Java-serialised storage (lower heap pressure)
#   deserialized=True   → raw object storage      (faster but more memory)
_MEM_DISK_SER = StorageLevel(True, True, False, False, 1)   # MEMORY_AND_DISK_SER
_MEM_DISK     = StorageLevel(True, True, False, True,  1)   # MEMORY_AND_DISK
CACHE_LEVEL   = _MEM_DISK_SER if _SC["cache_level_ser"] else _MEM_DISK
print("[INFO] Spark runtime configs set ✅")

# ── Tracking variables ────────────────────────────────────────────────────────
changed_files                  = []
filtered_count                 = 0
is_full_load                   = True
job_status                     = "STARTED"
deleted_members                = set()
reassigned_claims              = {}   # {src_member_key → set(claimkeys moved to another member)}
current_member_claimkey_counts = {}   # {member_key → int}  saved to watermark
current_member_claimkey_sets   = {}   # {member_key → [str]} saved to watermark for exact re-assignment detection next run
try:
    s3_client   = boto3.client("s3")
    glue_client = boto3.client("glue")
    print("[BOOTSTRAP] boto3 S3 + Glue clients created ✅")
except Exception as _boto_err:
    print(f"[BOOTSTRAP] FATAL — boto3 client creation failed: {_boto_err}")
    import traceback; traceback.print_exc()
    spark.stop()
    sys.exit(1)
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
                   members_deleted=0, member_claimkey_counts=None,
                   member_claimkey_sets=None):
    """
    Save job watermark to S3.

    member_claimkey_counts : {member_key → int}   — count of claimkeys per member
    member_claimkey_sets   : {member_key → [str]} — ACTUAL claimkey list per member
                             Used next run to detect EXACT re-assignments:
                             if claimkey 555 was under m1 last run but is now
                             under m2, we know precisely which key moved and can
                             delete it from m1 in Iceberg before writing to m2.
    """
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
                # Per-member claim key counts — fast drop detection (count only)
                "member_claimkey_counts":  member_claimkey_counts or {},
                # Per-member actual claimkey sets — precise re-assignment detection
                # {member_key_str → [claimkey_str, ...]}
                # Allows next run to know EXACTLY which claimkeys moved between members
                "member_claimkey_sets":    member_claimkey_sets or {},
            })
        )
        print(f"[WATERMARK] Saved → status={status} ✅")
    except Exception as wm_err:
        print(f"[WATERMARK] Failed to save watermark: {wm_err}")

# ── Chunk-level resume checkpoint ─────────────────────────────────────────────
# Purpose: if a FULL LOAD job fails mid-run (e.g. at chunk 45 of 88), the
# next re-run can SKIP already-completed chunks instead of re-appending them
# (which would cause duplicates) or deleting and restarting from scratch.
#
# How it works:
#   • After every successful chunk write → save a tiny JSON to S3:
#       checkpoints/claims_payer_{PAYER_KEY}_chunk_{idx}.json
#   • On job start → scan that prefix; find the highest completed chunk index.
#   • If checkpoint found → do NOT delete existing payer data; resume from
#     the next chunk after the last completed one.
#   • After ALL chunks complete successfully → delete all checkpoint files so
#     a future re-run starts fresh (no stale checkpoints).
#
# Checkpoint key format:
#   Full load  : checkpoints/fullload/claims_payer_326_chunk_0044.json
#   Incremental: checkpoints/incremental/claims_payer_326_run_<epoch>/chunk_0044.json
#
# WHY SEPARATE PREFIXES:
#   Full load     — only one active full load per payer at a time.
#                   Checkpoint survives across re-runs until SUCCESS cleans it.
#   Incremental   — each incremental run gets its OWN sub-folder keyed by the
#                   run's epoch ms.  This means:
#                   • A re-run of the SAME incremental attempt resumes correctly.
#                   • A brand-new incremental run (new epoch) starts clean.
#                   • Stale checkpoints from old incremental runs are deleted
#                     automatically on SUCCESS.
#
# NOTE: current_run_epoch_ms is defined before this block in the tracking vars.

_FL_CHECKPOINT_PREFIX   = f"checkpoints/fullload/claims_payer_{PAYER_KEY}_chunk_"
# Incremental prefix is finalised after watermark read (needs is_full_load flag).
# Stored in a list so STEP 2.5 can mutate it and _active_checkpoint_prefix()
# always sees the updated value (Python closures capture the name, not the value,
# but only for mutable containers — a plain string reassignment would NOT be
# visible inside the function).
_INCR_CHECKPOINT_FOLDER = [f"checkpoints/incremental/claims_payer_{PAYER_KEY}_run_{current_run_epoch_ms}/"]

def _active_checkpoint_prefix(is_fl):
    """Return the correct S3 prefix for this run's checkpoint files."""
    return _FL_CHECKPOINT_PREFIX if is_fl else _INCR_CHECKPOINT_FOLDER[0] + "chunk_"

def save_chunk_checkpoint(chunk_idx, chunk_count, cumulative_written, is_fl):
    """
    Save a per-chunk completion marker to S3 after a successful write.
    Works for both FULL LOAD and INCREMENTAL runs.
    Non-fatal: if S3 write fails the job continues; worst case is a re-run
    starts from an earlier chunk.
    """
    key = f"{_active_checkpoint_prefix(is_fl)}{chunk_idx:04d}.json"
    try:
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=key,
            Body=json.dumps({
                "payer_key":          PAYER_KEY,
                "mode":               "FULL_LOAD" if is_fl else "INCREMENTAL",
                "chunk_idx":          chunk_idx,
                "chunk_count":        chunk_count,
                "cumulative_written": cumulative_written,
                "saved_at":           datetime.utcnow().isoformat(),
            })
        )
        print(f"[CHECKPOINT] Chunk {chunk_idx} saved ✅  "
              f"(cumulative: {cumulative_written})")
    except Exception as cp_err:
        print(f"[CHECKPOINT] WARN — failed to save chunk {chunk_idx}: {cp_err}")

def load_chunk_checkpoint(is_fl):
    """
    Scan S3 for existing chunk checkpoints for this payer + run type.

    FULL LOAD:
      Scans _FL_CHECKPOINT_PREFIX — finds highest chunk_idx completed.
      Returns that index so the loop can skip chunks 0..N.

    INCREMENTAL:
      Scans _INCR_CHECKPOINT_FOLDER — but ONLY if the folder matches the
      CURRENT run epoch (same Glue job re-run after failure).
      A brand-new incremental run always gets a new epoch → new folder →
      no checkpoints found → starts from chunk 0.

    Returns:
      -1  → no checkpoint found, start from chunk 0
       N  → chunks 0..N already written, resume from N+1
    """
    prefix = _active_checkpoint_prefix(is_fl)
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        completed = []
        for page in paginator.paginate(
            Bucket=TARGET_BUCKET,
            Prefix=prefix,
            PaginationConfig={"PageSize": 1000}
        ):
            for obj in page.get("Contents", []):
                fname = obj["Key"].split("/")[-1]   # e.g. chunk_0044.json
                part  = fname.replace(".json", "").split("chunk_")
                if len(part) == 2 and part[1].isdigit():
                    completed.append(int(part[1]))
        if completed:
            last = max(completed)
            mode = "FULL LOAD" if is_fl else "INCREMENTAL"
            print(f"[CHECKPOINT] [{mode}] Found {len(completed)} completed chunk(s). "
                  f"Last completed index: {last}. Resuming from chunk {last + 1}.")
            return last
        print(f"[CHECKPOINT] No existing checkpoints — starting from chunk 0.")
        return -1
    except Exception as cp_err:
        print(f"[CHECKPOINT] WARN — checkpoint scan failed: {cp_err}. "
              f"Starting from chunk 0.")
        return -1

def delete_chunk_checkpoints(is_fl):
    """
    Delete ALL checkpoint files for this payer run from S3 after success.

    FULL LOAD:   deletes everything under _FL_CHECKPOINT_PREFIX
    INCREMENTAL: deletes only THIS run's folder (_INCR_CHECKPOINT_FOLDER)
                 Old incremental run folders (from previous dates) are also
                 cleaned up by scanning the parent prefix.
    """
    # For incremental, also clean up any leftover folders from past runs
    if is_fl:
        prefixes_to_clean = [_FL_CHECKPOINT_PREFIX]
    else:
        # Clean current run folder + any stale folders from previous incr runs
        parent = f"checkpoints/incremental/claims_payer_{PAYER_KEY}_run_"
        prefixes_to_clean = [parent]   # scanning parent cleans all sub-folders
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        keys_to_delete = []
        for pfx in prefixes_to_clean:
            for page in paginator.paginate(
                Bucket=TARGET_BUCKET,
                Prefix=pfx,
                PaginationConfig={"PageSize": 1000}
            ):
                for obj in page.get("Contents", []):
                    keys_to_delete.append({"Key": obj["Key"]})
        if keys_to_delete:
            for i in range(0, len(keys_to_delete), 1000):
                s3_client.delete_objects(
                    Bucket=TARGET_BUCKET,
                    Delete={"Objects": keys_to_delete[i:i+1000]}
                )
            print(f"[CHECKPOINT] Deleted {len(keys_to_delete)} checkpoint file(s) ✅")
        else:
            print("[CHECKPOINT] No checkpoint files to delete.")
    except Exception as cp_err:
        print(f"[CHECKPOINT] WARN — checkpoint cleanup failed (non-fatal): {cp_err}")

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
#   Full load  → copy-on-read (no delete-file accumulation on new tables)
#   Incremental → merge-on-read (low write amplification for small upserts)
# The DDL always creates tables with merge-on-read; an ALTER TABLE switches
# to copy-on-read at the start of a full-load run and back afterwards.
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
    """
    Append df to an Iceberg table.

    Handles the NoSuchKeyException spiral that happens when:
      - Glue catalog has the table entry  (CREATE TABLE ran previously)
      - BUT the S3 Iceberg metadata was deleted  (manual cleanup / first run after wipe)
      Iceberg calls loadTable → reads metadata JSON from S3 → 404 NoSuchKeyException.

    Fix: catch NoSuchKeyException → drop stale Glue entry → re-CREATE table
         (which writes a fresh metadata JSON to S3) → retry append once.
    """
    try:
        df.writeTo(table_fqn).append()
        print(f"[INFO] {label}: append ✅")
    except Exception as e:
        if not _is_nosuchkey(e):
            raise
        # Stale Glue catalog entry with no S3 metadata — repair and retry
        print(f"[WARN] {label}: append → NoSuchKeyException "
              f"(stale Glue entry / missing S3 metadata) — repairing...")
        _glue_drop_and_recreate(table_fqn, label)
        # After recreate, table is fresh — this append will succeed
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
    # MUST qualify with the catalog name so the database is created in the
    # Glue Data Catalog (glue_catalog), NOT in the local Hive metastore.
    # Without this, subsequent glue_catalog.{DATABASE}.claims references fail
    # with "database not found in glue_catalog" causing an immediate status 1 exit.
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DATABASE}")
        print(f"[STEP 1] Database glue_catalog.{DATABASE} ready ✅")
    except Exception as e:
        err = str(e).lower()
        if "already exists" in err:
            print(f"[STEP 1] Database glue_catalog.{DATABASE} already exists ✅")
        else:
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

    # ── STEP 2.5 — RESOLVE INCREMENTAL RUN EPOCH FOR CHECKPOINT ─────────────
    # For incremental resume to work correctly, a re-run of a FAILED incremental
    # job must use the SAME checkpoint folder as the original failed attempt.
    #
    # Problem: current_run_epoch_ms is always NOW (different on every Glue start).
    # So a re-run would create a NEW checkpoint folder → no checkpoints found →
    # starts from chunk 0 → all previous chunks re-processed (safe but slow).
    #
    # Solution: persist the incremental run epoch to S3 on first start.
    #   Key: checkpoints/incremental/claims_payer_{PAYER_KEY}_active_run.json
    #   • First start of an incremental run → write this file with current epoch.
    #   • Re-run (same failed job) → read this file → use the SAME epoch →
    #     finds the original checkpoint folder → resumes correctly.
    #   • On SUCCESS → delete this file along with the checkpoint folder.
    #
    # Full load runs do NOT need this (their checkpoint prefix is epoch-free).
    _INCR_ACTIVE_RUN_KEY = (
        f"checkpoints/incremental/claims_payer_{PAYER_KEY}_active_run.json"
    )
    if not is_full_load:
        try:
            _ar_obj  = s3_client.get_object(Bucket=TARGET_BUCKET, Key=_INCR_ACTIVE_RUN_KEY)
            _ar_data = json.loads(_ar_obj["Body"].read())
            _active_epoch = int(_ar_data["run_epoch_ms"])
            print(f"[CHECKPOINT] Incremental active-run epoch loaded: {_active_epoch} "
                  f"(original start: {_ar_data.get('started_at', 'unknown')})")
        except s3_client.exceptions.NoSuchKey:
            # First start of this incremental run — write the epoch marker
            _active_epoch = current_run_epoch_ms
            try:
                s3_client.put_object(
                    Bucket=TARGET_BUCKET,
                    Key=_INCR_ACTIVE_RUN_KEY,
                    Body=json.dumps({
                        "payer_key":     PAYER_KEY,
                        "run_epoch_ms":  _active_epoch,
                        "started_at":    current_run_ts,
                    })
                )
                print(f"[CHECKPOINT] Incremental active-run epoch saved: {_active_epoch}")
            except Exception as _ae:
                print(f"[CHECKPOINT] WARN — could not save active-run epoch: {_ae}")
        except Exception as _ae:
            _active_epoch = current_run_epoch_ms
            print(f"[CHECKPOINT] WARN — active-run read failed: {_ae}. "
                  f"Using current epoch (re-run will start from chunk 0).")
        # Update the incremental checkpoint folder to use the resolved epoch
        _INCR_CHECKPOINT_FOLDER[0] = (
            f"checkpoints/incremental/claims_payer_{PAYER_KEY}_run_{_active_epoch}/"
        )
    else:
        _INCR_ACTIVE_RUN_KEY = None   # not used for full load

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

        # S3 LIST retry constants
        # At large_50m (200 threads) the S3 LIST API can return SlowDown (503).
        # Without retry the exception kills the thread → that member's files are
        # silently MISSING from changed_files → data never written to Iceberg.
        # Exponential backoff: 1 s → 2 s → 4 s → 8 s → 16 s (5 attempts total).
        _S3_LIST_MAX_RETRIES = 5
        _S3_LIST_BASE_DELAY  = 1.0   # seconds

        def _list_member(prefix):
            """
            Scan one member prefix and return:
              changed_results  — files newer than last_run_epoch_ms (to write)
              all_s3_claimkeys — ALL claim keys currently on S3 for this member
                                 (used to detect partial/full deletes)
              has_any_json     — whether ANY json still exists for this member
              member_key       — the member id string

            Retries up to _S3_LIST_MAX_RETRIES times with exponential backoff
            on S3 SlowDown (503) / throttling errors so that high worker counts
            (150–200 threads at medium/large scale) don't silently drop members.
            """
            import time as _time
            import botocore.exceptions as _bce

            member_key = prefix.rstrip("/").split("/")[-1]

            for attempt in range(1, _S3_LIST_MAX_RETRIES + 1):
                try:
                    changed_results  = []
                    all_s3_claimkeys = set()
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
                    return changed_results, all_s3_claimkeys, has_any_json, member_key

                except _bce.ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code", "")
                    # Retry only on throttling / slow-down responses
                    if code in ("SlowDown", "503", "Throttling",
                                "RequestLimitExceeded", "ServiceUnavailable"):
                        if attempt < _S3_LIST_MAX_RETRIES:
                            delay = _S3_LIST_BASE_DELAY * (2 ** (attempt - 1))
                            print(f"[WARN] _list_member({member_key}): "
                                  f"S3 throttle [{code}] attempt {attempt}/"
                                  f"{_S3_LIST_MAX_RETRIES} — retrying in {delay:.1f}s")
                            _time.sleep(delay)
                            continue
                    # Non-throttle error OR exhausted retries → raise so the
                    # future resolves as an exception (caught in as_completed loop)
                    raise

            # Should never reach here, but satisfies linters
            raise RuntimeError(
                f"_list_member({member_key}): exhausted {_S3_LIST_MAX_RETRIES} retries"
            )

        # deleted_members     — member has ZERO json files left on S3
        # partially_deleted   — member still has SOME files but fewer claimkeys
        # reassigned_claims   — {m1 → set(claimkeys moved away from m1)}
        #                       claimkey was under m1 last run, now missing from m1
        #                       but will appear under a different member (m2) this run
        #                       → must DELETE from m1 in Iceberg, write under m2
        # member_s3_claimkeys — {member_key → frozenset(claimkeys on S3 NOW)}
        #                       stored for members with dropped count
        deleted_members     = set()
        partially_deleted   = set()
        member_s3_claimkeys = {}
        reassigned_claims   = {}   # {member_key_str → set(claimkey_str)}

        # Load per-member data from the last-run watermark (if any).
        last_member_claimkey_counts = {}   # member_key → int count
        last_member_claimkey_sets   = {}   # member_key → set(claimkey_str)
        if not is_full_load:
            try:
                wm_obj = s3_client.get_object(Bucket=TARGET_BUCKET, Key=WATERMARK_KEY)
                wm     = json.loads(wm_obj["Body"].read())
                last_member_claimkey_counts = wm.get("member_claimkey_counts", {})
                # Convert lists back to sets (JSON serialises sets as lists)
                last_member_claimkey_sets = {
                    mk: set(ck_list)
                    for mk, ck_list in wm.get("member_claimkey_sets", {}).items()
                }
            except Exception:
                pass   # watermark missing or old format — treat all as no prior data

        # Current-run per-member claimkey tracking — both count AND sets
        current_member_claimkey_counts = {}
        current_member_claimkey_sets   = {}   # stored for all members this run

        with concurrent.futures.ThreadPoolExecutor(max_workers=S3_LIST_WORKERS) as pool:
            futures = {pool.submit(_list_member, pfx): pfx for pfx in member_prefixes}
            failed_prefixes = []
            for future in concurrent.futures.as_completed(futures):
                pfx = futures[future]
                try:
                    batch, all_s3_claimkeys, has_any_json, member_key = future.result()
                except Exception as list_err:
                    # After all retries exhausted, log and continue.
                    # The member is skipped this run — watermark is NOT updated
                    # (job_status stays non-SUCCESS) so a re-run will retry it.
                    failed_prefixes.append(pfx)
                    print(f"[ERROR] _list_member({pfx}): failed after all retries: "
                          f"{list_err} — member will be retried on next run")
                    continue

                for rec in batch:
                    total_scanned += 1
                    changed_file_meta.append(rec)
                    members_found.add(rec["path_member_key"])

                # ── EMR ADDITION: populate claimkey counts + sets for ALL runs ──
                # Glue incremental script only populates these during incremental
                # runs (inside the `if not is_full_load:` block below).
                # EMR does a full load so we must populate them here — the
                # watermark written at the end carries these sets and Glue uses
                # them on its first incremental run to detect deletes/re-assignments.
                if is_full_load and all_s3_claimkeys:
                    current_member_claimkey_counts[member_key] = len(all_s3_claimkeys)
                    current_member_claimkey_sets[member_key]   = list(all_s3_claimkeys)

                if not is_full_load:
                    s3_count   = len(all_s3_claimkeys)
                    last_count = last_member_claimkey_counts.get(member_key, None)
                    last_keys  = last_member_claimkey_sets.get(member_key, set())

                    # Always record current count AND set for next run's watermark
                    current_member_claimkey_counts[member_key] = s3_count
                    current_member_claimkey_sets[member_key]   = list(all_s3_claimkeys)

                    if not has_any_json:
                        # ALL files deleted — full member purge
                        deleted_members.add(member_key)
                        member_s3_claimkeys[member_key] = frozenset()

                    elif last_count is not None and s3_count < last_count:
                        # Fewer claimkeys on S3 than last run → some were removed.
                        # Could be: pure delete OR re-assignment to another member.
                        # Either way STEP 3.5-B will keep only surviving keys.
                        partially_deleted.add(member_key)
                        member_s3_claimkeys[member_key] = frozenset(all_s3_claimkeys)

                        # Detect EXACT re-assigned claimkeys:
                        # keys present last run under this member but now gone from S3.
                        # These keys may re-appear under a different member (m2).
                        if last_keys:
                            moved_keys = last_keys - all_s3_claimkeys
                            if moved_keys:
                                reassigned_claims[member_key] = moved_keys
                                print(f"[STEP 3] Re-assignment detected: "
                                      f"member {member_key} lost "
                                      f"{len(moved_keys)} claimkey(s) "
                                      f"→ will purge from Iceberg before "
                                      f"writing to destination member")

            # If ANY member prefix failed to list, raise so job_status = FAILED
            # and watermark is NOT updated — guaranteeing a full re-scan on rerun.
            if failed_prefixes:
                raise Exception(
                    f"[STEP 3] {len(failed_prefixes)} member prefix(es) failed S3 LIST "
                    f"after {_S3_LIST_MAX_RETRIES} retries: {failed_prefixes[:10]}"
                )

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
        if reassigned_claims:
            total_moved = sum(len(v) for v in reassigned_claims.values())
            print(f"[STEP 3] Re-assigned claimkeys     : {total_moved} key(s) "
                  f"across {len(reassigned_claims)} source member(s) "
                  f"(moved to a different member — will delete from source, "
                  f"write to destination)")
        if changed_files:
            for f in changed_files[:5]:
                print(f"         {f}")
            if len(changed_files) > 5:
                print(f"         ... and {len(changed_files) - 5} more")

    except Exception as e:
        raise Exception(f"[STEP 3] FAILED scanning S3: {e}")

    if not changed_files and not deleted_members:
        print("[STEP 3] No new, modified, or deleted files — nothing to do!")
        save_watermark("SUCCESS - NO CHANGES", 0, 0, "INCREMENTAL - NO CHANGES",
                       member_claimkey_counts = current_member_claimkey_counts,
                       member_claimkey_sets   = current_member_claimkey_sets)
        spark.stop()
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

    # ── STEP 3.5-C — CLAIM RE-ASSIGNMENT: delete moved claimkeys from source member ──
    #
    # Scenario:  m1 had 10 claims (claimkeys 1-10).
    #            An upstream update moves claims 3,4,5 from m1 → m2.
    #            S3 now has:  m1/ → files for claimkeys 1,2,6,7,8,9,10
    #                         m2/ → files for claimkeys 3,4,5  (new/updated)
    #
    # Without this step:
    #   • STEP 3.5-B removes claimkeys 3,4,5 from m1 in Iceberg ✅ (count dropped)
    #   • Chunk loop writes claimkeys 3,4,5 under m2 in Iceberg ✅
    #   → Both steps already work via partially_deleted + the chunk write.
    #
    # BUT — there is one edge case this step handles that STEP 3.5-B misses:
    #   If m1's remaining files (1,2,6-10) have NOT changed since last run
    #   (same S3 epoch), m1 will NOT appear in changed_files, so m1's chunk
    #   is NOT processed in the main loop.  STEP 3.5-B DOES handle this via
    #   the count-drop detection → partial delete reconciliation.
    #
    # This step (3.5-C) adds an EXPLICIT log + guardrail for re-assignments
    # detected via the claimkey-set diff (last_keys - current_keys) so that:
    #   1. The purge is logged clearly as "re-assignment" not "partial delete"
    #   2. Even if STEP 3.5-B is skipped (e.g. count logic edge case), the
    #      specific moved claimkeys are guaranteed to be purged from m1.
    #
    # NOTE: reassigned_claims is populated ONLY in incremental runs when
    # last_member_claimkey_sets is available from the watermark.
    if not is_full_load and reassigned_claims:
        print(f"[STEP 3.5-C] Processing {len(reassigned_claims)} re-assignment(s) — "
              f"purging moved claimkeys from source members in Iceberg...")

        # Build (payerkey, memberkey, claimkey) rows to DELETE from Iceberg
        # These are rows that WERE under m1 but have now moved to m2
        reassign_delete_rows = [
            (int(PAYER_KEY), int(src_member), int(ck))
            for src_member, ck_set in reassigned_claims.items()
            if src_member.isdigit()
            for ck in ck_set if ck.isdigit()
        ]
        # Also build (payerkey, memberkey) for source members that had moves
        reassign_member_rows = [
            (int(PAYER_KEY), int(src_member))
            for src_member in reassigned_claims
            if src_member.isdigit()
        ]

        if reassign_delete_rows and reassign_member_rows:
            moved_keys_df   = spark.createDataFrame(
                reassign_delete_rows,
                ["payerkey_mv", "memberkey_mv", "claimkey_mv"]
            )
            src_members_df  = spark.createDataFrame(
                reassign_member_rows,
                ["payerkey_sm", "memberkey_sm"]
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

                    # Read only the payerkey partition — NOT a full table scan
                    tbl_scoped = (
                        spark.table(tbl_fqn)
                        .filter(F.col("payerkey") == int(PAYER_KEY))
                    )

                    # Rows NOT in any source (re-assigning) member → untouched
                    unaffected = tbl_scoped.join(
                        F.broadcast(src_members_df),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_sm")) &
                            (F.col("memberkey") == F.col("memberkey_sm"))
                        ),
                        how="left_anti"
                    )

                    # Rows IN source members but claimkey NOT in the moved set → keep
                    # (these are the claims that stayed with m1)
                    still_at_source = tbl_scoped.join(
                        F.broadcast(src_members_df),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_sm")) &
                            (F.col("memberkey") == F.col("memberkey_sm"))
                        ),
                        how="inner"
                    ).join(
                        F.broadcast(moved_keys_df),
                        on=(
                            (F.col("payerkey")  == F.col("payerkey_mv")) &
                            (F.col("memberkey") == F.col("memberkey_mv")) &
                            (F.col("claimkey")  == F.col("claimkey_mv"))
                        ),
                        how="left_anti"   # keep rows whose claimkey did NOT move
                    ).drop("payerkey_sm", "memberkey_sm")

                    reconciled = unaffected.unionByName(still_at_source)
                    _safe_write(reconciled, tbl_fqn,
                                f"{tbl_name}(reassignment-purge)")
                    purged = len(reassign_delete_rows)
                    print(f"[STEP 3.5-C] {tbl_name}: "
                          f"{purged} re-assigned claimkey row(s) purged from "
                          f"source member(s) ✅")
                except Exception as e:
                    print(f"[STEP 3.5-C] WARN {tbl_name}: "
                          f"re-assignment purge failed (non-fatal): {e}")

        print(f"[STEP 3.5-C] Re-assignment purge complete ✅  "
              f"Destination member(s) will receive the moved claims "
              f"via the normal chunk write in STEP 4–10.")

    if not changed_files:
        print("[STEP 3.5] Only deletions this run — no new files to process.")
        save_watermark("SUCCESS - DELETIONS ONLY",
                       len(deleted_members), 0, "INCREMENTAL - DELETIONS ONLY",
                       members_deleted        = len(deleted_members),
                       member_claimkey_counts = current_member_claimkey_counts,
                       member_claimkey_sets   = current_member_claimkey_sets)
        spark.stop()
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

    # ── FULL LOAD: pre-loop table preparation ────────────────────────────────
    # Three scenarios handled before chunk 1 ever runs:
    #
    # Scenario A — S3 was wiped but Glue catalog entry still exists
    #   Symptom : _table_meta_ok = False  (no metadata/ files on S3)
    #   Fix     : drop stale Glue entry + recreate table fresh
    #   Result  : first append() writes to a clean empty Iceberg table
    #
    # Scenario B — Job failed mid-run last time (e.g. crashed after chunk 3/20)
    #   Symptom : _table_meta_ok = True AND payerkey={PAYER_KEY} data files exist
    #   Fix     : DELETE all rows for this payer  (Iceberg partition prune → fast)
    #   Result  : clean slate → re-run appends all chunks from scratch, no dupes
    #   NOTE    : watermark is NOT written on failure, so is_full_load stays True
    #             on re-run — this block always fires before the chunk loop.
    #
    # Scenario B-new — First time this payer ever runs on an existing table
    #   Symptom : _table_meta_ok = True AND NO payerkey={PAYER_KEY} data files
    #   Fix     : nothing — just start appending
    #
    # Scenario C — Table does not exist at all
    #   Handled by STEP 10 CREATE TABLE above — nothing to do here
    if is_full_load:
        print(f"[INFO] Full load — preparing tables for payer {PAYER_KEY}...")
        # Check checkpoint ONCE here — reused for all 3 tables below.
        # Avoids 3 separate S3 LIST calls (one per table loop iteration).
        _fl_cp_check = load_chunk_checkpoint(True)
        for _tbl in ["claims", "claimdiagnosis", "claimlines"]:
            _tbl_fqn = f"glue_catalog.{DATABASE}.{_tbl}"

            if _table_exists_glue(DATABASE, _tbl) and not _table_meta_ok[_tbl]:
                # Scenario A: Glue entry exists but S3 metadata was wiped
                print(f"[WARN] {_tbl}: Glue entry exists but S3 metadata is missing "
                      f"(S3 was wiped) — dropping stale entry and recreating...")
                _glue_drop_and_recreate(_tbl_fqn, _tbl)
                print(f"[INFO] {_tbl}: recreated fresh ✅")

            elif _table_exists_glue(DATABASE, _tbl) and _table_meta_ok[_tbl]:
                payer_prefix = f"iceberg/{_tbl}/data/payerkey={PAYER_KEY}/"
                payer_data   = s3_client.list_objects_v2(
                    Bucket=TARGET_BUCKET, Prefix=payer_prefix, MaxKeys=1
                )
                if payer_data.get("KeyCount", 0) > 0:
                    # Scenario B: partial data exists — check cached checkpoint
                    if _fl_cp_check >= 0:
                        print(f"[INFO] {_tbl}: checkpoint found at chunk {_fl_cp_check} "
                              f"— keeping existing data, will resume from chunk "
                              f"{_fl_cp_check + 1} ✅")
                    else:
                        print(f"[INFO] {_tbl}: found existing data for payerkey={PAYER_KEY} "
                              f"(failed re-run, no checkpoint) — deleting to start clean...")
                        try:
                            spark.sql(f"""
                                DELETE FROM {_tbl_fqn}
                                WHERE payerkey = {int(PAYER_KEY)}
                            """)
                            print(f"[INFO] {_tbl}: payerkey={PAYER_KEY} cleared ✅")
                        except Exception as _del_err:
                            print(f"[WARN] {_tbl}: DELETE failed — "
                                  f"may cause duplicates on re-run: {_del_err}")
                else:
                    print(f"[INFO] {_tbl}: no existing data for "
                          f"payerkey={PAYER_KEY} — clean start ✅")
            else:
                print(f"[INFO] {_tbl}: does not exist in Glue — "
                      f"will be created on first write ✅")

    # ── Resume checkpoint: determine which chunk to start from ───────────────
    # Full load  : reuse _fl_cp_check already loaded above (avoid 2nd S3 LIST)
    # Incremental: load now (first time checkpoint is checked for incremental)
    if is_full_load:
        resume_from_chunk = _fl_cp_check
    else:
        resume_from_chunk = load_chunk_checkpoint(False)

    if resume_from_chunk >= 0:
        mode_label = "FULL LOAD" if is_full_load else "INCREMENTAL"
        print(f"[CHECKPOINT] [{mode_label}] RESUMING from chunk "
              f"{resume_from_chunk + 1} — "
              f"chunks 0–{resume_from_chunk} already written.")
        if is_full_load:
            print(f"[CHECKPOINT] Existing payer data preserved — "
                  f"skipping pre-loop DELETE.")

        # ── PARTIAL-WRITE SAFETY: purge the first un-checkpointed chunk ───────
        # Scenario: job was stopped (manually or by timeout) WHILE chunk N+1 was
        # being written.  One or more of the 3 table appends may have completed
        # before the stop.  Since there is no checkpoint for chunk N+1, the
        # re-run will process and append it again → DUPLICATE rows for those members.
        #
        # Fix: before the chunk loop, DELETE from Iceberg every memberkey that
        # belongs to the FIRST chunk we are about to re-process (resume_from_chunk+1).
        # This is a fast operation: Iceberg prunes to (payerkey, loadyear, loadmonth)
        # partitions and the broadcast anti-join removes only the affected members.
        # Chunks 0..resume_from_chunk are NOT touched (their data is correct).
        #
        # This handles:
        #   • Manual stop mid-chunk  (your scenario)
        #   • Glue timeout mid-chunk
        #   • OOM / executor failure mid-chunk
        #   Any case where the job dies without saving a checkpoint for that chunk.
        _next_chunk_idx     = resume_from_chunk + 1
        _next_chunk_members = all_members[
            _next_chunk_idx * MEMBER_CHUNK_SIZE :
            (_next_chunk_idx + 1) * MEMBER_CHUNK_SIZE
        ]
        if _next_chunk_members and is_full_load:
            print(f"[CHECKPOINT] Purging partial-write safety: "
                  f"deleting any data for chunk {_next_chunk_idx + 1} members "
                  f"({len(_next_chunk_members)} members) before resuming...")
            try:
                _purge_member_rows = [
                    (int(PAYER_KEY), int(m))
                    for m in _next_chunk_members if str(m).isdigit()
                ]
                if _purge_member_rows:
                    _purge_df = spark.createDataFrame(
                        _purge_member_rows, ["payerkey_pu", "memberkey_pu"]
                    )
                    for _pu_tbl in ["claims", "claimdiagnosis", "claimlines"]:
                        _pu_fqn = f"glue_catalog.{DATABASE}.{_pu_tbl}"
                        if not _table_exists_glue(DATABASE, _pu_tbl):
                            continue
                        if not _table_meta_ok.get(_pu_tbl, False):
                            continue
                        _pu_existing = spark.table(_pu_fqn).filter(
                            F.col("payerkey") == int(PAYER_KEY)
                        )
                        _pu_clean = _pu_existing.join(
                            F.broadcast(_purge_df),
                            on=(
                                (F.col("payerkey")  == F.col("payerkey_pu")) &
                                (F.col("memberkey") == F.col("memberkey_pu"))
                            ),
                            how="left_anti"
                        )
                        _safe_write(_pu_clean, _pu_fqn,
                                    f"{_pu_tbl}(partial-write-purge)")
                        print(f"[CHECKPOINT] {_pu_tbl}: "
                              f"chunk {_next_chunk_idx + 1} members purged "
                              f"(partial-write safety) ✅")
            except Exception as _pu_err:
                print(f"[CHECKPOINT] WARN — partial-write purge failed "
                      f"(non-fatal, may cause duplicates for chunk "
                      f"{_next_chunk_idx + 1}): {_pu_err}")

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
    if resume_from_chunk >= 0:
        print(f"[STEP 4] ⏭  Skipping chunks 0–{resume_from_chunk} "
              f"(already written in previous run)")

    for chunk_idx in range(num_chunks):
        chunk_members = all_members[
            chunk_idx * MEMBER_CHUNK_SIZE : (chunk_idx + 1) * MEMBER_CHUNK_SIZE
        ]
        chunk_meta = [rec for m in chunk_members for rec in member_to_files[m]]
        chunk_files = [r["s3_path"] for r in chunk_meta]

        # ── Resume: skip chunks already written in a previous failed run ──
        if chunk_idx <= resume_from_chunk:
            mode_lbl = "FULL LOAD" if is_full_load else "INCREMENTAL"
            print(f"[CHUNK {chunk_idx+1}/{num_chunks}] ⏭  SKIPPED "
                  f"[{mode_lbl}] (checkpoint {chunk_idx} already written)")
            continue

        print(f"\n[CHUNK {chunk_idx+1}/{num_chunks}] "
              f"{len(chunk_members)} members | {len(chunk_files)} files")

        # ── Partition + shuffle sizing — pure Python, zero Spark actions ─────────
        # ideal_partitions: target partition count after repartition().
        #   small example   : 3000 files / 50  files_per_partition = 60 partitions
        #   medium_10m ex.  : 9446 files / 200 files_per_partition = 48 partitions
        ideal_partitions = max(4, -(-len(chunk_files) // FILES_PER_PART))

        # chunk_shuffle_parts: AQE coalesces down; this just sets the ceiling.
        #   ideal_partitions × 4 gives AQE headroom without 1600-task overhead.
        chunk_shuffle_parts = max(
            BASE_SHUFFLE_PART,
            min(MAX_SHUFFLE_PART, ideal_partitions * 4)
        )
        spark.conf.set("spark.sql.shuffle.partitions", str(chunk_shuffle_parts))
        print(f"[CHUNK {chunk_idx+1}] partitions={ideal_partitions}  "
              f"shuffle={chunk_shuffle_parts}")

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

            _json_df = (
                spark.read
                .option("multiline",           "true")
                .option("mode",                "PERMISSIVE")
                .option("recursiveFileLookup", "false")
                .json(chunk_files)             # ← exact paths, no glob expansion
            )
            # Drop any JSON fields that collide with our system column names
            # before we add them via withColumn. Spark's withColumn does NOT
            # replace an existing column — it appends a second one with the
            # same name, which later causes "Found duplicate column(s)" in
            # dropDuplicates / writes.
            _sys_cols = ["path_payer_key", "path_member_key", "path_claim_key",
                         "_s3_path", "file_epoch_ms",
                         "resolved_payer_key", "resolved_member_key", "resolved_claim_key",
                         "updated_at_epoch_ms", "claim_load_ts", "loadyear", "loadmonth"]
            for _sc in _sys_cols:
                if _sc in _json_df.columns:
                    _json_df = _json_df.drop(_sc)

            raw_df = (
                _json_df
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
                    F.coalesce(
                        F.col("payerKey").cast("long")    # Type 2 camelCase
                            if "payerKey"  in _json_df.columns else F.lit(None).cast("long"),
                        F.col("PayerKey").cast("long")    # Type 1 PascalCase
                            if "PayerKey"  in _json_df.columns else F.lit(None).cast("long"),
                        F.col("path_payer_key")))
                .withColumn("resolved_member_key",
                    F.coalesce(
                        F.col("memberKey").cast("long")   # Type 2 camelCase
                            if "memberKey" in _json_df.columns else F.lit(None).cast("long"),
                        F.col("MemberKey").cast("long")   # Type 1 PascalCase
                            if "MemberKey" in _json_df.columns else F.lit(None).cast("long"),
                        F.col("path_member_key")))
                .withColumn("resolved_claim_key",
                    F.coalesce(
                        F.col("claimKey").cast("long")    # Type 2 camelCase
                            if "claimKey"  in _json_df.columns else F.lit(None).cast("long"),
                        F.col("ClaimKey").cast("long")    # Type 1 PascalCase
                            if "ClaimKey"  in _json_df.columns else F.lit(None).cast("long"),
                        F.col("path_claim_key")))
                .withColumn("updated_at_epoch_ms",
                    F.coalesce(
                        # Type 2 camelCase
                        *([to_epoch_ms(F.col("updatedAt"))]
                          if "updatedAt" in _json_df.columns else []),
                        # Type 1 PascalCase
                        *([to_epoch_ms(F.col("UpdatedAt"))]
                          if "UpdatedAt" in _json_df.columns else []),
                        F.col("file_epoch_ms"),
                        F.lit(0).cast("long")
                    ))
                .filter(F.col("resolved_claim_key").isNotNull())
                # partition columns
                # Coalesce both camelCase (Type 2) and PascalCase (Type 1)
                .withColumn("claim_load_ts",
                    F.coalesce(
                        normalize_ts(F.col("claimLoadDateTime"))
                            if "claimLoadDateTime" in _json_df.columns
                            else F.lit(None).cast("timestamp"),
                        normalize_ts(F.col("ClaimLoadDateTime"))
                            if "ClaimLoadDateTime" in _json_df.columns
                            else F.lit(None).cast("timestamp"),
                    ))
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

            # Repartition to the pre-calculated ideal_partitions count.
            # With multiline JSON Spark creates 1 task per input file by default.
            # Grouping FILES_PER_PART files into one partition keeps task count
            # sensible (target: 20-60 partitions per chunk at any scale tier).
            #
            # CRITICAL: do NOT call raw_df.rdd.getNumPartitions() here.
            # That triggers a Spark planning action on the lazy DF BEFORE
            # persist() — it was the cause of the 22-min per-chunk wait.
            raw_df = raw_df.repartition(ideal_partitions)

            # Single persist + count — executes the whole lazy chain once.
            raw_df.persist(CACHE_LEVEL)
            chunk_count = raw_df.count()
            filtered_count += chunk_count

            print(f"[CHUNK {chunk_idx+1}] Cached {chunk_count} rows "
                  f"across {ideal_partitions} partitions")

            if chunk_count == 0:
                print(f"[CHUNK {chunk_idx+1}] Empty — skipping")
                # Still save checkpoint so resume skips this chunk too
                save_chunk_checkpoint(chunk_idx, 0, total_written, is_full_load)
                continue

            # ── STEP 7 (per chunk) — BUILD CLAIMS DF ──────────────────────
            def safe_col(col_name, cast_type="string"):
                """
                Read a field from raw_df that may be camelCase (Type 2) or
                PascalCase (Type 1).  Try the supplied name first, then its
                opposite-case counterpart, then fall back to null.
                """
                cols = raw_df.columns
                if col_name in cols:
                    base = F.col(col_name).cast(cast_type)
                    alt  = col_name[0].upper() + col_name[1:] if col_name[0].islower() \
                           else col_name[0].lower() + col_name[1:]
                    if alt in cols and alt != col_name:
                        return F.coalesce(base, F.col(alt).cast(cast_type))
                    return base
                alt = col_name[0].upper() + col_name[1:] if col_name[0].islower() \
                      else col_name[0].lower() + col_name[1:]
                if alt in cols:
                    return F.col(alt).cast(cast_type)
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
            # Two JSON casing variants exist in source data:
            #   Type 1 (PascalCase): DiagnosisCode, ClaimKey, BatchRunSequence…
            #   Type 2 (camelCase):  diagnosisCode, claimKey, batchRunSequence…
            # Both casings are declared in the schema so Spark reads either.
            # coalesce(Pascal, camel) in the select picks whichever is non-null.
            # Explicit schema = no inferred top-level collisions.
            _DX_ITEM_SCHEMA = ArrayType(StructType([
                # camelCase (Type 2)
                StructField("diagnosisCode",         StringType(),  True),
                StructField("diagnosisOrder",         IntegerType(), True),
                StructField("isPrimary",              StringType(),  True),
                StructField("isSensitive",            IntegerType(), True),
                StructField("isTrauma",               IntegerType(), True),
                StructField("versionIndicator",       IntegerType(), True),
                StructField("clientDataFeedCode",     StringType(),  True),
                StructField("inboundBatchMasterKey",  LongType(),    True),
                StructField("batchRunSequence",       IntegerType(), True),
                StructField("claimDiagnosisKey",      LongType(),    True),
                # item-level timestamps (camelCase — Type 2)
                StructField("updatedAt",              StringType(),  True),
                StructField("createdAt",              StringType(),  True),
                # PascalCase (Type 1)
                StructField("DiagnosisCode",         StringType(),  True),
                StructField("DiagnosisOrder",         IntegerType(), True),
                StructField("IsPrimary",              StringType(),  True),
                StructField("IsSensitive",            IntegerType(), True),
                StructField("IsTrauma",               IntegerType(), True),
                StructField("VersionIndicator",       IntegerType(), True),
                StructField("ClientDataFeedCode",     StringType(),  True),
                StructField("InboundBatchMasterKey",  LongType(),    True),
                StructField("BatchRunSequence",       IntegerType(), True),
                StructField("ClaimDiagnosisKey",      LongType(),    True),
                # item-level timestamps (PascalCase — Type 1)
                StructField("UpdatedAt",              StringType(),  True),
                StructField("CreatedAt",              StringType(),  True),
            ]))
            _dx_col_candidates = [
                "claimDiagnosisList", "ClaimDiagnosisList",
                "diagnosisList",      "DiagnosisList",
                "claimDiagnosis",     "ClaimDiagnosis",
            ]
            _dx_col = next(
                (c for c in _dx_col_candidates if c in raw_df.columns), None
            )
            if not _dx_col:
                print(f"[WARN] STEP 8: no diagnosis list column found "
                      f"(tried {_dx_col_candidates}). Available: {raw_df.columns}")
            _dx_col_name = _dx_col or "claimDiagnosisList"

            # Both updatedAt and UpdatedAt declared — with caseSensitive=true
            # each maps to its respective JSON field:
            #   Type 2 JSON "updatedAt"  → updatedAt  schema field
            #   Type 1 JSON "UpdatedAt"  → UpdatedAt  schema field
            # Both camelCase and PascalCase array column names are included so
            # a SINGLE spark.read handles both JSON types without two passes.
            # _dx_alt_col: pick the opposite casing of _dx_col_name so the two
            # schema fields are always distinct (prevents duplicate-column errors).
            _dx_alt_col = (
                _dx_col_name[0].upper() + _dx_col_name[1:]
                if _dx_col_name[0].islower()
                else _dx_col_name[0].lower() + _dx_col_name[1:]
            )
            _DX_READ_SCHEMA = StructType([
                # camelCase top-level keys (Type 2)
                StructField("payerKey",       LongType(),      True),
                StructField("memberKey",      LongType(),      True),
                StructField("claimKey",       LongType(),      True),
                # PascalCase top-level keys (Type 1)
                StructField("PayerKey",       LongType(),      True),
                StructField("MemberKey",      LongType(),      True),
                StructField("ClaimKey",       LongType(),      True),
                # timestamp variants
                StructField("updatedAt",      StringType(),    True),
                StructField("UpdatedAt",      StringType(),    True),
                # array column — primary candidate name
                StructField(_dx_col_name,     _DX_ITEM_SCHEMA, True),
                # array column — alternate candidate name (other casing)
                StructField(_dx_alt_col,      _DX_ITEM_SCHEMA, True),
            ])
            _dx_raw = (
                # ── SINGLE_READ_CACHE optimization ────────────────────────
                # When enabled: derive diagnosis rows from the ALREADY CACHED
                # raw_df instead of doing a 2nd spark.read.json() on chunk_files.
                # This eliminates one full S3 re-read of all chunk files.
                # raw_df already has all top-level fields including the array
                # columns. We just select the relevant array column and explode.
                #
                # When disabled: keep the original separate spark.read.json()
                # with the explicit schema (safe fallback for all other tiers).
                raw_df.select(
                    F.coalesce(
                        F.col("payerKey").cast("long")  if "payerKey"  in raw_df.columns else F.lit(None).cast("long"),
                        F.col("PayerKey").cast("long")  if "PayerKey"  in raw_df.columns else F.lit(None).cast("long"),
                        F.col("resolved_payer_key")
                    ).alias("_dxp"),
                    F.coalesce(
                        F.col("memberKey").cast("long") if "memberKey" in raw_df.columns else F.lit(None).cast("long"),
                        F.col("MemberKey").cast("long") if "MemberKey" in raw_df.columns else F.lit(None).cast("long"),
                        F.col("resolved_member_key")
                    ).alias("_dxm"),
                    F.col("resolved_claim_key").alias("_dxc"),
                    F.col("loadyear").alias("_dx_yr"),
                    F.col("loadmonth").alias("_dx_mo"),
                    F.coalesce(
                        F.col("updatedAt").cast("string") if "updatedAt" in raw_df.columns else F.lit(None).cast("string"),
                        F.col("UpdatedAt").cast("string") if "UpdatedAt" in raw_df.columns else F.lit(None).cast("string"),
                    ).alias("_upd"),
                    F.coalesce(
                        F.col(_dx_col_name) if _dx_col_name in raw_df.columns else F.lit(None),
                        F.col(_dx_alt_col)  if _dx_alt_col  in raw_df.columns else F.lit(None),
                    ).alias("_dx_arr"),
                )
                .withColumn("_dxepoch", to_epoch_ms(F.col("_upd")))
                .withColumn("_dxrow", F.explode_outer(F.col("_dx_arr")))
                .drop("_dx_arr", "_upd")
                .filter(F.col("_dxrow").isNotNull())
            ) if SINGLE_READ_CACHE else (
                spark.read
                .option("multiline", "true")
                .option("mode",      "PERMISSIVE")
                .schema(_DX_READ_SCHEMA)
                .json(chunk_files)
                .withColumn("_dxp",
                    F.coalesce(F.col("payerKey"),  F.col("PayerKey")))
                .withColumn("_dxm",
                    F.coalesce(F.col("memberKey"), F.col("MemberKey")))
                .withColumn("_dxc",
                    F.coalesce(F.col("claimKey"),  F.col("ClaimKey")))
                .drop("payerKey", "PayerKey", "memberKey", "MemberKey",
                      "claimKey", "ClaimKey")
                .withColumn("_dxepoch",
                    F.coalesce(
                        to_epoch_ms(F.col("updatedAt")),
                        to_epoch_ms(F.col("UpdatedAt")),
                        F.lit(0).cast("long")))
                .drop("updatedAt", "UpdatedAt")
                .withColumn("_dx_arr",
                    F.coalesce(F.col(_dx_col_name), F.col(_dx_alt_col)))
                .drop(_dx_col_name, _dx_alt_col)
                .withColumn("_dxrow", F.explode_outer(F.col("_dx_arr")))
                .drop("_dx_arr")
                .filter(F.col("_dxrow").isNotNull())
            )
            if not SINGLE_READ_CACHE:
                _dx_part = raw_df.select(
                    F.col("resolved_claim_key").alias("_dxc_j"),
                    F.col("loadyear").alias("_dx_yr"),
                    F.col("loadmonth").alias("_dx_mo"),
                ).distinct()
                _dx_raw = _dx_raw.join(
                    F.broadcast(_dx_part),
                    F.col("_dxc") == F.col("_dxc_j"), "left"
                ).drop("_dxc_j")

            def _dx(camel, pascal, cast):
                """coalesce camelCase and PascalCase struct field, then cast."""
                return F.coalesce(
                    F.col(f"_dxrow.{camel}").cast(cast),
                    F.col(f"_dxrow.{pascal}").cast(cast),
                )

            diagnosis_df = _dx_raw.select(
                F.col("_dxp").alias("payerkey"),
                F.col("_dxm").alias("memberkey"),
                F.col("_dxc").alias("claimkey"),
                F.col("_dx_yr").alias("loadyear"),
                F.col("_dx_mo").alias("loadmonth"),
                _dx("diagnosisCode",        "DiagnosisCode",        "string") .alias("diagnosiscode"),
                _dx("diagnosisOrder",       "DiagnosisOrder",       "int")    .alias("diagnosisorder"),
                _dx("isPrimary",            "IsPrimary",            "string") .alias("isprimary"),
                _dx("isSensitive",          "IsSensitive",          "int")    .alias("issensitive"),
                _dx("isTrauma",             "IsTrauma",             "int")    .alias("istrauma"),
                _dx("versionIndicator",     "VersionIndicator",     "int")    .alias("versionindicator"),
                _dx("clientDataFeedCode",   "ClientDataFeedCode",   "string") .alias("clientdatafeedcode"),
                _dx("inboundBatchMasterKey","InboundBatchMasterKey","long")   .alias("inboundbatchmasterkey"),
                _dx("batchRunSequence",     "BatchRunSequence",     "int")    .alias("batchrunsequence"),
                _dx("claimDiagnosisKey",    "ClaimDiagnosisKey",    "long")   .alias("claimdiagnosiskey"),
                # updatedAt: coalesce item-level camel (Type 2) then Pascal (Type 1)
                normalize_ts(F.coalesce(
                    F.col("_dxrow.updatedAt").cast("string"),
                    F.col("_dxrow.UpdatedAt").cast("string")))                         .alias("updatedat"),
                F.coalesce(
                    to_epoch_ms(F.col("_dxrow.updatedAt").cast("string")),
                    to_epoch_ms(F.col("_dxrow.UpdatedAt").cast("string")),
                    F.col("_dxepoch"),
                    F.lit(0).cast("long"))                                             .alias("updatedatepochms"),
            ) \
            .sortWithinPartitions(F.col("updatedatepochms").desc_nulls_last()) \
            .dropDuplicates(["payerkey", "loadyear", "loadmonth",
                             "memberkey", "claimkey", "diagnosisorder"])

            # ── STEP 9 (per chunk) — BUILD LINES DF ───────────────────────
            # Same dual-casing pattern as diagnosis:
            #   Type 1 (PascalCase): ClaimLineKey, BilledAmount, UpdatedAt…
            #   Type 2 (camelCase):  claimLineKey, billedAmount, updatedAt…
            _LN_ITEM_SCHEMA = ArrayType(StructType([
                # camelCase (Type 2)
                StructField("claimLineKey",             LongType(),    True),
                StructField("claimLineNumber",          StringType(),  True),
                StructField("procedureCode",            StringType(),  True),
                StructField("procedureCodeType",        StringType(),  True),
                StructField("billedAmount",             StringType(),  True),
                StructField("clientPaidAmount",         StringType(),  True),
                StructField("memberPaid",               StringType(),  True),
                StructField("allowedAmount",            StringType(),  True),
                StructField("coveredAmount",            StringType(),  True),
                StructField("discountAmount",           StringType(),  True),
                StructField("discountReason",           StringType(),  True),
                StructField("excludedAmount",           StringType(),  True),
                StructField("excludedReason",           StringType(),  True),
                StructField("withholdAmount",           StringType(),  True),
                StructField("withholdReason",           StringType(),  True),
                StructField("providerPaidAmount",       StringType(),  True),
                StructField("originalClientPaidAmount", StringType(),  True),
                StructField("previousPaidAmount",       StringType(),  True),
                StructField("dateofServiceFrom",        StringType(),  True),
                StructField("dateofServiceThru",        StringType(),  True),
                StructField("modifierCode01",           StringType(),  True),
                StructField("modifierCode02",           StringType(),  True),
                StructField("placeofService",           StringType(),  True),
                StructField("revenueCode",              StringType(),  True),
                StructField("serviceType",              StringType(),  True),
                StructField("quantity",                 StringType(),  True),
                StructField("houseCode",                StringType(),  True),
                StructField("houseCodeDescription",     StringType(),  True),
                StructField("paymentType",              StringType(),  True),
                StructField("paymentTypeID",            StringType(),  True),
                StructField("paymentComments",          StringType(),  True),
                StructField("checkNumber",              StringType(),  True),
                StructField("transactionCode",          StringType(),  True),
                StructField("transactionDescription",   StringType(),  True),
                StructField("adjustmentFlag",           StringType(),  True),
                StructField("isPrimaryNDC",             StringType(),  True),
                StructField("insuredTermDate",          StringType(),  True),
                StructField("manipulationReason",       StringType(),  True),
                StructField("claimDetailStatus",        StringType(),  True),
                StructField("clientDataFeedCode",       StringType(),  True),
                StructField("inboundBatchMasterKey",    LongType(),    True),
                StructField("batchRunSequence",         IntegerType(), True),
                StructField("stageClaimLineKey",        LongType(),    True),
                StructField("updatedAt",                StringType(),  True),
                StructField("createdAt",                StringType(),  True),
                # PascalCase (Type 1) — all fields except updatedAt/createdAt
                # which are already handled by case-insensitive JSON mapping
                StructField("ClaimLineKey",             LongType(),    True),
                StructField("ClaimLineNumber",          StringType(),  True),
                StructField("ProcedureCode",            StringType(),  True),
                StructField("ProcedureCodeType",        StringType(),  True),
                StructField("BilledAmount",             StringType(),  True),
                StructField("ClientPaidAmount",         StringType(),  True),
                StructField("MemberPaid",               StringType(),  True),
                StructField("AllowedAmount",            StringType(),  True),
                StructField("CoveredAmount",            StringType(),  True),
                StructField("DiscountAmount",           StringType(),  True),
                StructField("DiscountReason",           StringType(),  True),
                StructField("ExcludedAmount",           StringType(),  True),
                StructField("ExcludedReason",           StringType(),  True),
                StructField("WithholdAmount",           StringType(),  True),
                StructField("WithholdReason",           StringType(),  True),
                StructField("ProviderPaidAmount",       StringType(),  True),
                StructField("OriginalClientPaidAmount", StringType(),  True),
                StructField("PreviousPaidAmount",       StringType(),  True),
                StructField("DateofServiceFrom",        StringType(),  True),
                StructField("DateofServiceThru",        StringType(),  True),
                StructField("ModifierCode01",           StringType(),  True),
                StructField("ModifierCode02",           StringType(),  True),
                StructField("PlaceofService",           StringType(),  True),
                StructField("RevenueCode",              StringType(),  True),
                StructField("ServiceType",              StringType(),  True),
                StructField("Quantity",                 StringType(),  True),
                StructField("HouseCode",                StringType(),  True),
                StructField("HouseCodeDescription",     StringType(),  True),
                StructField("PaymentType",              StringType(),  True),
                StructField("PaymentTypeID",            StringType(),  True),
                StructField("PaymentComments",          StringType(),  True),
                StructField("CheckNumber",              StringType(),  True),
                StructField("TransactionCode",          StringType(),  True),
                StructField("TransactionDescription",   StringType(),  True),
                StructField("AdjustmentFlag",           StringType(),  True),
                StructField("IsPrimaryNDC",             StringType(),  True),
                StructField("InsuredTermDate",          StringType(),  True),
                StructField("ManipulationReason",       StringType(),  True),
                StructField("ClaimDetailStatus",        StringType(),  True),
                StructField("ClientDataFeedCode",       StringType(),  True),
                StructField("InboundBatchMasterKey",    LongType(),    True),
                StructField("BatchRunSequence",         IntegerType(), True),
                StructField("StageClaimLineKey",        LongType(),    True),
                StructField("UpdatedAt",                StringType(),  True),
                StructField("CreatedAt",                StringType(),  True),
                # caseSensitive=true → UpdatedAt/CreatedAt are truly distinct
                # from updatedAt/createdAt above
            ]))
            _ln_col_candidates = [
                "claimLinesList", "ClaimLinesList",
                "claimLines",     "ClaimLines",
                "linesList",      "LinesList",
            ]
            _ln_col = next(
                (c for c in _ln_col_candidates if c in raw_df.columns), None
            )
            if not _ln_col:
                print(f"[WARN] STEP 9: no lines list column found "
                      f"(tried {_ln_col_candidates}). Available: {raw_df.columns}")
            _ln_col_name = _ln_col or "claimLinesList"

            # Both updatedAt and UpdatedAt declared — caseSensitive=true
            # ensures they map to distinct JSON source fields per type.
            # Both camelCase and PascalCase array column names included so a
            # SINGLE spark.read handles both JSON types without two passes.
            # _ln_alt_col: always pick the opposite casing of _ln_col_name.
            _ln_alt_col = (
                _ln_col_name[0].upper() + _ln_col_name[1:]
                if _ln_col_name[0].islower()
                else _ln_col_name[0].lower() + _ln_col_name[1:]
            )
            _LN_READ_SCHEMA = StructType([
                # camelCase top-level keys (Type 2)
                StructField("payerKey",    LongType(),      True),
                StructField("memberKey",   LongType(),      True),
                StructField("claimKey",    LongType(),      True),
                # PascalCase top-level keys (Type 1)
                StructField("PayerKey",    LongType(),      True),
                StructField("MemberKey",   LongType(),      True),
                StructField("ClaimKey",    LongType(),      True),
                # timestamp variants
                StructField("updatedAt",   StringType(),    True),
                StructField("UpdatedAt",   StringType(),    True),
                # array column — primary candidate name
                StructField(_ln_col_name,  _LN_ITEM_SCHEMA, True),
                # array column — alternate candidate name (other casing)
                StructField(_ln_alt_col,   _LN_ITEM_SCHEMA, True),
            ])
            _ln_raw = (
                # ── SINGLE_READ_CACHE optimization ────────────────────────
                # Same approach as diagnosis above — derive lines rows from the
                # already-cached raw_df, avoiding a 3rd full S3 re-read.
                raw_df.select(
                    F.coalesce(
                        F.col("payerKey").cast("long")  if "payerKey"  in raw_df.columns else F.lit(None).cast("long"),
                        F.col("PayerKey").cast("long")  if "PayerKey"  in raw_df.columns else F.lit(None).cast("long"),
                        F.col("resolved_payer_key")
                    ).alias("_lnp"),
                    F.coalesce(
                        F.col("memberKey").cast("long") if "memberKey" in raw_df.columns else F.lit(None).cast("long"),
                        F.col("MemberKey").cast("long") if "MemberKey" in raw_df.columns else F.lit(None).cast("long"),
                        F.col("resolved_member_key")
                    ).alias("_lnm"),
                    F.col("resolved_claim_key").alias("_lnc"),
                    F.col("loadyear").alias("_ln_yr"),
                    F.col("loadmonth").alias("_ln_mo"),
                    F.coalesce(
                        F.col("updatedAt").cast("string") if "updatedAt" in raw_df.columns else F.lit(None).cast("string"),
                        F.col("UpdatedAt").cast("string") if "UpdatedAt" in raw_df.columns else F.lit(None).cast("string"),
                    ).alias("_upd"),
                    F.coalesce(
                        F.col(_ln_col_name) if _ln_col_name in raw_df.columns else F.lit(None),
                        F.col(_ln_alt_col)  if _ln_alt_col  in raw_df.columns else F.lit(None),
                    ).alias("_ln_arr"),
                )
                .withColumn("_lnepoch", to_epoch_ms(F.col("_upd")))
                .withColumn("_lnrow", F.explode_outer(F.col("_ln_arr")))
                .drop("_ln_arr", "_upd")
                .filter(F.col("_lnrow").isNotNull())
            ) if SINGLE_READ_CACHE else (
                spark.read
                .option("multiline", "true")
                .option("mode",      "PERMISSIVE")
                .schema(_LN_READ_SCHEMA)
                .json(chunk_files)
                .withColumn("_lnp",
                    F.coalesce(F.col("payerKey"),  F.col("PayerKey")))
                .withColumn("_lnm",
                    F.coalesce(F.col("memberKey"), F.col("MemberKey")))
                .withColumn("_lnc",
                    F.coalesce(F.col("claimKey"),  F.col("ClaimKey")))
                .drop("payerKey", "PayerKey", "memberKey", "MemberKey",
                      "claimKey", "ClaimKey")
                .withColumn("_lnepoch",
                    F.coalesce(
                        to_epoch_ms(F.col("updatedAt")),
                        to_epoch_ms(F.col("UpdatedAt")),
                        F.lit(0).cast("long")))
                .drop("updatedAt", "UpdatedAt")
                .withColumn("_ln_arr",
                    F.coalesce(F.col(_ln_col_name), F.col(_ln_alt_col)))
                .drop(_ln_col_name, _ln_alt_col)
                .withColumn("_lnrow", F.explode_outer(F.col("_ln_arr")))
                .drop("_ln_arr")
                .filter(F.col("_lnrow").isNotNull())
            )
            if not SINGLE_READ_CACHE:
                _ln_part = raw_df.select(
                    F.col("resolved_claim_key").alias("_lnc_j"),
                    F.col("loadyear").alias("_ln_yr"),
                    F.col("loadmonth").alias("_ln_mo"),
                ).distinct()
                _ln_raw = _ln_raw.join(
                    F.broadcast(_ln_part),
                    F.col("_lnc") == F.col("_lnc_j"), "left"
                ).drop("_lnc_j")

            def _ln(camel, pascal, cast):
                """coalesce camelCase and PascalCase struct field, then cast."""
                return F.coalesce(
                    F.col(f"_lnrow.{camel}").cast(cast),
                    F.col(f"_lnrow.{pascal}").cast(cast),
                )

            lines_df = _ln_raw.select(
                F.col("_lnp").alias("payerkey"),
                F.col("_lnm").alias("memberkey"),
                F.col("_lnc").alias("claimkey"),
                F.col("_ln_yr").alias("loadyear"),
                F.col("_ln_mo").alias("loadmonth"),
                _ln("claimLineKey",             "ClaimLineKey",             "long")          .alias("claimlinekey"),
                _ln("claimLineNumber",          "ClaimLineNumber",          "string")        .alias("claimlinenumber"),
                _ln("procedureCode",            "ProcedureCode",            "string")        .alias("procedurecode"),
                _ln("procedureCodeType",        "ProcedureCodeType",        "string")        .alias("procedurecodetype"),
                _ln("billedAmount",             "BilledAmount",             "decimal(18,2)") .alias("billedamount"),
                _ln("clientPaidAmount",         "ClientPaidAmount",         "decimal(18,2)") .alias("clientpaidamount"),
                _ln("memberPaid",               "MemberPaid",               "decimal(18,2)") .alias("memberpaid"),
                _ln("allowedAmount",            "AllowedAmount",            "decimal(18,2)") .alias("allowedamount"),
                _ln("coveredAmount",            "CoveredAmount",            "decimal(18,2)") .alias("coveredamount"),
                _ln("discountAmount",           "DiscountAmount",           "decimal(18,2)") .alias("discountamount"),
                _ln("discountReason",           "DiscountReason",           "string")        .alias("discountreason"),
                _ln("excludedAmount",           "ExcludedAmount",           "decimal(18,2)") .alias("excludedamount"),
                _ln("excludedReason",           "ExcludedReason",           "string")        .alias("excludedreason"),
                _ln("withholdAmount",           "WithholdAmount",           "decimal(18,2)") .alias("withholdamount"),
                _ln("withholdReason",           "WithholdReason",           "string")        .alias("withholdreason"),
                _ln("providerPaidAmount",       "ProviderPaidAmount",       "decimal(18,2)") .alias("providerpaidamount"),
                _ln("originalClientPaidAmount", "OriginalClientPaidAmount", "decimal(18,2)") .alias("originalclientpaidamount"),
                _ln("previousPaidAmount",       "PreviousPaidAmount",       "decimal(18,2)") .alias("previouspaidamount"),
                normalize_date(F.coalesce(
                    F.col("_lnrow.dateofServiceFrom").cast("string"),
                    F.col("_lnrow.DateofServiceFrom").cast("string")))                       .alias("dateofservicefrom"),
                normalize_date(F.coalesce(
                    F.col("_lnrow.dateofServiceThru").cast("string"),
                    F.col("_lnrow.DateofServiceThru").cast("string")))                       .alias("dateofservicethru"),
                _ln("modifierCode01",           "ModifierCode01",           "string")        .alias("modifiercode01"),
                _ln("modifierCode02",           "ModifierCode02",           "string")        .alias("modifiercode02"),
                _ln("placeofService",           "PlaceofService",           "string")        .alias("placeofservice"),
                _ln("revenueCode",              "RevenueCode",              "string")        .alias("revenuecode"),
                _ln("serviceType",              "ServiceType",              "string")        .alias("servicetype"),
                _ln("quantity",                 "Quantity",                 "decimal(10,2)") .alias("quantity"),
                _ln("houseCode",                "HouseCode",                "string")        .alias("housecode"),
                _ln("houseCodeDescription",     "HouseCodeDescription",     "string")        .alias("housecodedescription"),
                _ln("paymentType",              "PaymentType",              "string")        .alias("paymenttype"),
                _ln("paymentTypeID",            "PaymentTypeID",            "string")        .alias("paymenttypeid"),
                _ln("paymentComments",          "PaymentComments",          "string")        .alias("paymentcomments"),
                _ln("checkNumber",              "CheckNumber",              "string")        .alias("checknumber"),
                _ln("transactionCode",          "TransactionCode",          "string")        .alias("transactioncode"),
                _ln("transactionDescription",   "TransactionDescription",   "string")        .alias("transactiondescription"),
                _ln("adjustmentFlag",           "AdjustmentFlag",           "string")        .alias("adjustmentflag"),
                _ln("isPrimaryNDC",             "IsPrimaryNDC",             "string")        .alias("isprimaryndc"),
                _ln("insuredTermDate",          "InsuredTermDate",          "string")        .alias("insuredtermdate"),
                _ln("manipulationReason",       "ManipulationReason",       "string")        .alias("manipulationreason"),
                _ln("claimDetailStatus",        "ClaimDetailStatus",        "string")        .alias("claimdetailstatus"),
                _ln("clientDataFeedCode",       "ClientDataFeedCode",       "string")        .alias("clientdatafeedcode"),
                _ln("inboundBatchMasterKey",    "InboundBatchMasterKey",    "long")          .alias("inboundbatchmasterkey"),
                _ln("batchRunSequence",         "BatchRunSequence",         "int")           .alias("batchrunsequence"),
                _ln("stageClaimLineKey",        "StageClaimLineKey",        "long")          .alias("stageclaimlinekey"),
                normalize_ts(F.coalesce(
                    F.col("_lnrow.updatedAt").cast("string"),
                    F.col("_lnrow.UpdatedAt").cast("string")))                               .alias("updatedat"),
                normalize_ts(F.coalesce(
                    F.col("_lnrow.createdAt").cast("string"),
                    F.col("_lnrow.CreatedAt").cast("string")))                               .alias("createdat"),
                F.coalesce(
                    to_epoch_ms(F.col("_lnrow.updatedAt").cast("string")),
                    to_epoch_ms(F.col("_lnrow.UpdatedAt").cast("string")),
                    F.col("_lnepoch"),
                    F.lit(0).cast("long")
                ).alias("updatedatepochms"),
            ) \
            .sortWithinPartitions(F.col("updatedatepochms").desc_nulls_last()) \
            .dropDuplicates(["payerkey", "loadyear", "loadmonth",
                             "memberkey", "claimkey", "claimlinenumber"])

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

            # ── CLAIMS / DIAGNOSIS / LINES WRITE ─────────────────────────
            # OPTIMIZATION: PARALLEL_TABLE_WRITES
            # When enabled (payer326 tier): all 3 table writes run simultaneously
            # using ThreadPoolExecutor(3). Each write is independent — they touch
            # different Iceberg tables. This cuts write time from ~15 min (seq)
            # to ~5 min (parallel) per chunk.
            #
            # When disabled (other tiers): sequential write preserved exactly
            # as before — no risk to existing payers.

            def _write_claims():
                if is_full_load:
                    _iceberg_append(claims_df,
                                    f"glue_catalog.{DATABASE}.claims", "claims")
                else:
                    claims_tbl      = f"glue_catalog.{DATABASE}.claims"
                    existing_claims = _partition_filter(claims_tbl, affected_partitions,
                                                        is_table=True)
                    if readded_members:
                        ra_df = spark.createDataFrame(
                            [(int(PAYER_KEY), int(m)) for m in readded_members
                             if m.isdigit()],
                            ["payerkey_ra", "memberkey_ra"]
                        )
                        existing_claims = existing_claims.join(
                            F.broadcast(ra_df),
                            on=((F.col("payerkey")  == F.col("payerkey_ra")) &
                                (F.col("memberkey") == F.col("memberkey_ra"))),
                            how="left_anti"
                        )
                        print(f"[CHUNK {chunk_idx+1}] Re-added member rows stripped "
                              f"from claims: {readded_members}")
                    existing_claims = _strip_deleted_claims(
                        existing_claims, partial_chunk_members)
                    replaced_claims = F.broadcast(
                        claims_df.select("payerkey", "loadyear", "loadmonth",
                                         "memberkey", "claimkey").distinct())
                    merged_claims = (existing_claims
                        .join(replaced_claims,
                              on=["payerkey", "loadyear", "loadmonth",
                                  "memberkey", "claimkey"], how="left_anti")
                        .unionByName(claims_df))
                    _safe_write(merged_claims, claims_tbl, "claims")
                print(f"[CHUNK {chunk_idx+1}] claims write ✅")

            def _write_diagnosis():
                if is_full_load:
                    _iceberg_append(diagnosis_df,
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
                            on=((F.col("payerkey")  == F.col("payerkey_ra")) &
                                (F.col("memberkey") == F.col("memberkey_ra"))),
                            how="left_anti"
                        )
                    existing_dx = _strip_deleted_claims(
                        existing_dx, partial_chunk_members)
                    replaced_dx = F.broadcast(
                        diagnosis_df.select("payerkey", "loadyear", "loadmonth",
                                            "memberkey", "claimkey").distinct())
                    merged_dx = (existing_dx
                        .join(replaced_dx,
                              on=["payerkey", "loadyear", "loadmonth",
                                  "memberkey", "claimkey"], how="left_anti")
                        .unionByName(diagnosis_df))
                    _safe_write(merged_dx, dx_tbl, "claimdiagnosis")
                print(f"[CHUNK {chunk_idx+1}] claimdiagnosis write ✅")

            def _write_lines():
                if is_full_load:
                    _iceberg_append(lines_df,
                                    f"glue_catalog.{DATABASE}.claimlines",
                                    "claimlines")
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
                            on=((F.col("payerkey")  == F.col("payerkey_ra")) &
                                (F.col("memberkey") == F.col("memberkey_ra"))),
                            how="left_anti"
                        )
                    existing_ln = _strip_deleted_claims(
                        existing_ln, partial_chunk_members)
                    replaced_ln = F.broadcast(
                        lines_df.select("payerkey", "loadyear", "loadmonth",
                                        "memberkey", "claimkey").distinct())
                    merged_ln = (existing_ln
                        .join(replaced_ln,
                              on=["payerkey", "loadyear", "loadmonth",
                                  "memberkey", "claimkey"], how="left_anti")
                        .unionByName(lines_df))
                    _safe_write(merged_ln, ln_tbl, "claimlines")
                print(f"[CHUNK {chunk_idx+1}] claimlines write ✅")

            if PARALLEL_TABLE_WRITES:
                # ── PARALLEL WRITES (payer326 tier) ───────────────────────
                # claims + claimdiagnosis + claimlines written simultaneously.
                # Each write goes to a different Iceberg table → zero contention.
                # ThreadPoolExecutor(3) on the driver — actual Spark work still
                # distributed across all workers; only the submission is parallel.
                print(f"[CHUNK {chunk_idx+1}] Writing 3 tables in PARALLEL...")
                _write_errors = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as _wp:
                    _futures = {
                        _wp.submit(_write_claims):    "claims",
                        _wp.submit(_write_diagnosis): "claimdiagnosis",
                        _wp.submit(_write_lines):     "claimlines",
                    }
                    for _f in concurrent.futures.as_completed(_futures):
                        _tbl_name = _futures[_f]
                        try:
                            _f.result()
                        except Exception as _we:
                            _write_errors.append(f"{_tbl_name}: {_we}")
                if _write_errors:
                    raise Exception(
                        f"Parallel write failed for: {'; '.join(_write_errors)}"
                    )
                print(f"[CHUNK {chunk_idx+1}] All 3 parallel writes complete ✅")
            else:
                # ── SEQUENTIAL WRITES (all other tiers) ───────────────────
                _write_claims()
                _write_diagnosis()
                _write_lines()

            total_written += chunk_count
            print(f"[CHUNK {chunk_idx+1}/{num_chunks}] Written ✅  "
                  f"(cumulative records: {total_written})")

            save_chunk_checkpoint(chunk_idx, chunk_count, total_written,
                                  is_full_load)

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

    # ── Clean up checkpoint files after successful completion ──────────────
    # Full load  : delete all full-load chunk checkpoints for this payer.
    # Incremental: delete this run's chunk folder + the active-run epoch marker
    #              so the next incremental run starts clean (new epoch, chunk 0).
    delete_chunk_checkpoints(is_full_load)
    if not is_full_load and _INCR_ACTIVE_RUN_KEY:
        try:
            s3_client.delete_object(Bucket=TARGET_BUCKET, Key=_INCR_ACTIVE_RUN_KEY)
            print(f"[CHECKPOINT] Incremental active-run marker deleted ✅")
        except Exception as _cd:
            print(f"[CHECKPOINT] WARN — could not delete active-run marker: {_cd}")

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

    # ── Only write watermark on SUCCESS — never on FAILED ────────────────────
    # If the job FAILED, do NOT overwrite the watermark with the current run's
    # timestamp.  On re-run the watermark still points at the last SUCCESSFUL
    # run epoch, so Spark re-scans and re-processes ALL files that changed
    # since that successful run — including chunks already partially processed
    # in the failed run.
    #
    # Writing a FAILED watermark with current_run_epoch_ms causes re-runs to
    # skip every file processed before the failure, leaving the data warehouse
    # in a permanently incomplete / corrupt state and making manual recovery
    # necessary.
    #
    # Iceberg writes are idempotent (overwritePartitions + left_anti merge +
    # dropDuplicates), so re-processing the same files on re-run is safe —
    # no duplicate rows are introduced.
    if job_status == "FAILED":
        print("[WATERMARK] Job FAILED — watermark NOT updated.")
        print("[WATERMARK] Re-run will reprocess all files since the last "
              "successful run epoch to ensure the data warehouse is complete.")
    else:
        save_watermark(
            status                  = job_status,
            files_processed         = len(changed_files),
            records_merged          = filtered_count,
            mode                    = "FULL LOAD" if is_full_load else "INCREMENTAL",
            members_deleted         = len(deleted_members),
            member_claimkey_counts  = current_member_claimkey_counts,
            member_claimkey_sets    = current_member_claimkey_sets,
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

    # EMR: spark.stop() signals step completion so the cluster can
    # auto-terminate after the last step finishes (no idle billing).
    spark.stop()
