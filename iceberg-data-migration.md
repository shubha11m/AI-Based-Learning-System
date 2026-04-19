# Iceberg Data Migration Module — Complete Documentation

## Overview

Migrates **500M+ claim JSON files** from S3 into **3 Apache Iceberg tables** (claims, claimdiagnosis, claimlines) on AWS Glue Catalog, using Spark in local mode on ECS Fargate.

**Performance**: 3.5 lakh members (326K) / 14M+ files → **40 minutes** with 10 ECS tasks.

---

## Architecture

### High-Level System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                   AWS CLOUD                                      │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                      Application Load Balancer                             │  │
│  │                   (ntclaimdms.subropoint.{env})                            │  │
│  └──────────┬─────────────────────┬─────────────────────┬─────────────────────┘  │
│             │                     │                     │                         │
│    ┌────────▼─────────┐  ┌───────▼──────────┐  ┌───────▼──────────┐             │
│    │    ECS Task 1    │  │    ECS Task 2    │  │    ECS Task N    │  ... ×N     │
│    │   4vCPU / 30GB   │  │   4vCPU / 30GB   │  │   4vCPU / 30GB   │             │
│    │  ┌─────────────┐ │  │  ┌─────────────┐ │  │  ┌─────────────┐ │             │
│    │  │ Spring Boot │ │  │  │ Spring Boot │ │  │  │ Spring Boot │ │             │
│    │  │  ┌────────┐ │ │  │  │  ┌────────┐ │ │  │  │  ┌────────┐ │ │             │
│    │  │  │ Spark  │ │ │  │  │  │ Spark  │ │ │  │  │  │ Spark  │ │ │             │
│    │  │  │ Local  │ │ │  │  │  │ Local  │ │ │  │  │  │ Local  │ │ │             │
│    │  │  └────────┘ │ │  │  │  └────────┘ │ │  │  │  └────────┘ │ │             │
│    │  └─────────────┘ │  │  └─────────────┘ │  │  └─────────────┘ │             │
│    └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘             │
│             │                     │                     │                         │
│  ┌──────────▼─────────────────────▼─────────────────────▼──────────────────────┐ │
│  │                             Amazon S3                                       │ │
│  │                                                                             │ │
│  │  ┌──────────────────────────┐     ┌──────────────────────────┐             │ │
│  │  │     Source Bucket        │     │     Analytics Bucket      │             │ │
│  │  │  nontrauma-claim-{env}   │     │  nontrauma-analytics-{env}│             │ │
│  │  │                          │     │                           │             │ │
│  │  │  /{payer}/{member}/      │     │  /iceberg/                │             │ │
│  │  │    {claim}.json          │     │    claims/                │             │ │
│  │  │    (14KB, multiline)     │     │    claimdiagnosis/        │             │ │
│  │  │                          │     │    claimlines/            │             │ │
│  │  │  14M+ files              │     │                           │             │ │
│  │  └──────────────────────────┘     │  /tmp-jsonl/              │             │ │
│  │                                   │  /metadata/               │             │ │
│  │                                   └──────────────────────────┘             │ │
│  └─────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐   │
│  │    Aurora MySQL       │  │    AWS Glue Catalog   │  │      DynamoDB        │   │
│  │                       │  │                       │  │                      │   │
│  │  chunk_progress       │  │  claims_db_{env}      │  │  Iceberg Lock Table  │   │
│  │  table (~163 rows)    │  │    .claims            │  │  commit locks        │   │
│  │                       │  │    .claimdiagnosis     │  │  (per-table)         │   │
│  │                       │  │    .claimlines         │  │                      │   │
│  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘

External Callers:
  ┌──────────────┐            ┌──────────────┐            ┌──────────────┐
  │  Ops / Admin │──POST──>   │  Ingestion   │──POST──>   │   Reader     │──POST──>  ALB
  │              │<──202──    │  Service     │            │   Service    │
  └──────────────┘            └──────────────┘            └──────────────┘
```

### Data Flow — Full Load (Distributed)

```
┌─────────┐     ┌────────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────┐
│ Step 1   │────>│  Step 2    │────>│  Step 3      │────>│  Step 4    │────>│ Step 5   │
│ S3 Scan  │     │ Chunking   │     │ Process      │     │ OPTIMIZE   │     │ Watermark│
│          │     │            │     │ Chunks       │     │            │     │          │
│ LIST S3  │     │ 326K members│    │ 163 chunks   │     │ Compact    │     │ Save     │
│ Extract  │     │ ÷ 2000     │     │ × 10 workers │     │ Parquet    │     │ baseline │
│ members  │     │ = 163 chunks│    │ = ~16 each   │     │ files      │     │ Cleanup  │
└─────────┘     └────────────┘     └──────┬───────┘     └────────────┘     └──────────┘
                                           │
                          ┌────────────────┼────────────────┐
                          │                │                │
                    ┌─────▼─────┐   ┌──────▼─────┐  ┌──────▼──────┐
                    │ 3a.       │   │ 3b.        │  │ 3c.         │
                    │ Pre-      │   │ DualCase   │  │ Transform   │
                    │ Compact   │   │ Resolve    │  │ (×3 tables) │
                    │           │   │            │  │             │
                    │ 50 threads│   │ Normalize  │  │ Claims      │
                    │ read S3   │   │ field case │  │ Diagnosis   │
                    │ → JSONL   │   │ (once)     │  │ ClaimLines  │
                    └─────┬─────┘   └──────┬─────┘  └──────┬──────┘
                          │                │                │
                          └────────────────┼────────────────┘
                                           │
                                    ┌──────▼──────┐
                                    │ 3d.         │
                                    │ Iceberg     │
                                    │ Append      │
                                    │ (Sequential)│
                                    │ DynamoDB    │
                                    │ Lock        │
                                    └─────────────┘
```

### Chunk Processing Pipeline (per chunk, per worker)

```
┌────────────────────��─────────────────────────────────────────────────────────┐
│ CHUNK PROCESSING (FullLoadService.processChunk)                              │
│                                                                              │
│  ┌──────────────────���───────────────────────────────────────┐               │
│  │ PHASE 1: PRE-COMPACTION (3.5 min)              77% time  │               │
│  │                                                           │               │
│  │  S3 Source Files          JsonPreCompactionService        │    S3 Temp    │
│  │  ┌────┐┌────┐┌────┐     ┌─────────────────────┐         │   ┌────────┐  │
│  │  │.json││.json││.json│    │  50-thread pool     │    PUT  │   │  JSONL │  │
│  │  │14KB ││14KB ││14KB │───>│  S3 GET (3 retries) │────────>│   │  parts │  │
│  │  │×113K││    ││    │    │  Jackson compact    │         │   │  ×24   │  │
│  │  └────┘└────┘└────┘     └─────────────────────┘         │   └────────┘  │
│  └──────────────────────────────────────────────────────────┘               │
│                                          │                                   │
│  ┌───────────────────────────────────────▼──────────────────┐               │
│  │ PHASE 2: SPARK READ + TRANSFORM (0.5 min)      11% time  │               │
│  │                                                           │               │
│  │  spark.read.json(jsonlPath)  ──>  DualCaseResolver.resolve()             │
│  │  (multiline=false, FAST)          (normalize field casing)│               │
│  │                                          │                │               │
│  │           ┌──────────────────────────────┼────────────┐   │               │
│  │           │                              │            │   │               │
│  │    ┌──────▼───────┐  ┌──────────────┐  ┌▼──────────┐ │   │               │
│  │    │ClaimsTransform│  │DiagTransform │  │LinesTransf│ │   │               │
│  │    │  flat fields  │  │  explode()   │  │ explode() │ │   │               │
│  │    │  timestamps   │  │  diagList[]  │  │ linesList│ │   │               │
│  │    │  dedup        │  │  dedup       │  │  dedup    │ │   │               │
│  │    └──────┬────────┘  └──────┬───────┘  └─┬────────┘ │   │               │
│  │           │                  │             │          │   │               │
│  └───────────┼──────────────────┼─────────────┼──────────┘   │               │
│              │                  │             │               │               │
│  ┌───────────▼──────────────────▼─────────────▼──────────┐   │               │
│  │ PHASE 3: ICEBERG WRITE (0.5 min)           12% time    │   │               │
│  │                                                        │   │               │
│  │  appendAllTables() — SEQUENTIAL                        │   │               │
│  │                                                        │   │               │
│  │  ┌──────────┐    ┌──────────────┐    ┌─────────────┐  │   │               │
│  │  │ claims   │───>│claimdiagnosis│───>│ claimlines  │  │   │               │
│  │  │ append() │    │  append()    │    │  append()   │  │   │               │
│  │  │ +mergeSchema  │ +mergeSchema │    │+mergeSchema │  │   │               │
│  │  │ +DynamoDB lock│ +DynamoDB lock│   │+DynamoDB lock│ │   │               │
│  │  └──────────┘    └──────────────┘    └─────────────┘  │   │               │
│  └────────────────────────────────────────────────────────┘   │               │
│                                                               │               │
│  ┌────────────────────────────────────────────────────────┐   │               │
│  │ PHASE 4: CLEANUP                                       │   │               │
│  │  unpersist(blocking=true) → free Spark memory          │   │               │
│  │  preCompactionService.cleanup() → delete JSONL from S3 │   │               │
│  └────────────────────────────────────────────────────────┘   │               │
└──────────────────────────────────────────────────────────────────────────────┘
  Total: ~4.5 min per chunk
```

### Distributed Worker Coordination

```
                            ┌───────────────────────┐
                            │   COORDINATOR TASK     │
                            │   (1 of N ECS tasks)   │
                            │                        │
                            │  1. S3 scan            │
                            │  2. Create 163 chunks  │
                            │  3. Ensure tables      │
                            │  4. Fan-out via LB ────┼──────────────────────┐
                            │  5. Join as worker     │                      │
                            └───────────┬────────────┘                      │
                                        │                                   │
              ┌─────────────────────────┼────────────���────────┐            │
              │                         │                     │            │
     ┌────────▼─────────┐    ┌─────────▼────────┐  ┌────────▼─────────┐  │
     │   ECS Task 1     │    │   ECS Task 2     │  │   ECS Task N     │  │
     │                  │    │                  │  │                  │<─┘
     │  ┌─Semaphore(1)─┐│    │  ┌─Semaphore(1)─┐│  │  ┌─Semaphore(1)─┐│
     │  │ Worker A     ││    │  │ Worker C     ││  │  │ Worker E     ││
     │  │ (from coord) ││    │  │ (from LB)    ││  │  │ (from LB)    ││
     │  │              ││    │  │              ││  │  │              ││
     │  │ Worker B     ││    │  │ Worker D     ││  │  │ Worker F     ││
     │  │ (from LB)    ││    │  │ (from LB)    ││  │  │ (from LB)    ││
     │  │  (waits)     ││    │  │  (waits)     ││  │  │  (waits)     ││
     │  └──────────────┘│    │  └──────────────┘│  │  └──────────────┘│
     └──────────────────┘    └──────────────────┘  └──────────────────┘
              │                         │                     │
              └─────────────────────────┼─────────────────────┘
                                        │
                              ┌─────────▼─────────┐
                              │   Aurora MySQL     │
                              │                    │
                              │  chunk_progress    │
                              │  ┌──┬──┬──┬──┬──┐ │
                              │  │0 │✅│  │  │  │ │  ✅ = COMPLETED
                              │  │1 │✅│  │  │  │ │  🔄 = PROCESSING
                              │  │2 │🔄│W-A│  │  │ │  ⏳ = PENDING
                              │  │3 │🔄│W-C│  │  │ │
                              │  │4 │⏳│  │  │  │ │
                              │  │..│⏳│  │  │  │ │
                              │  ��162│⏳│ │  │  │ │
                              │  └──┴──┴──┴──┴──┘ │
                              │                    │
                              │  Work-stealing:    │
                              │  UPDATE SET status │
                              │  =PROCESSING WHERE │
                              │  status=PENDING    │
                              │  LIMIT 1           │
                              └────────────────────┘
```

---

## Package Structure

```
iceberg/
├── config/
│   └── IcebergMigrationConfig.java      # All configuration properties
├── controller/
│   └── IcebergMigrationController.java  # REST API endpoints
├── fullload/
│   ├── FullLoadService.java             # Full load orchestration (single + distributed)
│   ├── JsonPreCompactionService.java    # Multi-line JSON → single-line JSONL converter
│   ├── S3FileScanner.java              # S3 LIST → member discovery + chunking
│   ├── S3MetadataService.java          # S3-based chunk manifests (member key lists)
│   ├── ChunkProgressRepository.java    # MySQL chunk status tracking + work-stealing
│   ├── IcebergChunkProgress.java       # JPA entity for chunk progress
│   ├── IncrementalLoadService.java     # Incremental upsert/delete/reassign
│   ├── IncrementalScanResult.java      # S3 scan diff result holder
│   ├── WatermarkService.java          # S3-based watermark for incremental state
│   └── CheckpointService.java         # Legacy checkpoint compatibility
├── spark/
│   ├── SparkIcebergWriter.java        # All Iceberg write operations
│   ├── ClaimsTransformer.java         # JSON → claims table schema
│   ├── DiagnosisTransformer.java      # JSON → claimdiagnosis table schema
│   ├── ClaimLinesTransformer.java     # JSON → claimlines table schema
│   └── DualCaseResolver.java         # PascalCase/camelCase field normalization
├── maintenance/
│   └── IcebergMaintenanceService.java # OPTIMIZE, expire snapshots, orphan cleanup
└── sqs/
    └── (SQS integration for event-driven incremental)
```

---

## S3 Data Layout

### Source (JSON files)
```
s3://nontrauma-claim-{env}/{payerKey}/{memberKey}/{claimKey}.json
```
Each JSON file is **multi-line pretty-printed** (~14KB, ~289 lines).

### Iceberg Tables
```
s3://nontrauma-analytics-{env}/iceberg/claims/
s3://nontrauma-analytics-{env}/iceberg/claimdiagnosis/
s3://nontrauma-analytics-{env}/iceberg/claimlines/
```
Partitioned by: `payerkey`, `loadyear`, `loadmonth`

### Metadata (temporary, during full load)
```
s3://nontrauma-analytics-{env}/metadata/{payerKey}/members.json
s3://nontrauma-analytics-{env}/metadata/{payerKey}/chunks/{chunkIndex}.json
s3://nontrauma-analytics-{env}/tmp-jsonl/{payerKey}/{chunkIndex}/part-00000.jsonl
```

---

## Configuration (application.properties)

| Property | Default | Description |
|----------|---------|-------------|
| `iceberg.migration.env` | `dev` | Environment name (dev/qa2/prod) |
| `iceberg.migration.analytics-bucket` | `nontrauma-analytics-dev` | S3 bucket for Iceberg tables |
| `iceberg.migration.full-load-chunk-size` | `2000` | Members per chunk |
| `iceberg.migration.precompact-enabled` | `true` | Enable JSON→JSONL pre-compaction |
| `iceberg.migration.precompact-batch-size` | `100` | Members per JSONL flush batch |
| `iceberg.migration.stale-chunk-minutes` | `30` | Timeout before reclaiming stuck chunks |
| `iceberg.migration.max-chunk-retries` | `3` | Max retries for failed chunks |
| `spark.driver.memory.override` | `16g` | Spark driver heap size |
| `spark.shuffle.partitions.override` | `16` | Spark shuffle partition count |

---

## REST API Endpoints

### Full Load
| Method | Path | Description |
|--------|------|-------------|
| POST | `/migration/v1/fullload/{payerKey}` | Single-task full load (legacy) |
| POST | `/migration/v1/fullload/distributed/{payerKey}/{concurrency}` | Distributed full load (recommended) |
| POST | `/migration/v1/fullload/distributed/{payerKey}/worker` | Worker endpoint (internal, via LB) |
| GET | `/migration/v1/fullload/{payerKey}/status` | Single-task status |
| GET | `/migration/v1/fullload/distributed/{payerKey}/status` | Distributed status |
| DELETE | `/migration/v1/fullload/{payerKey}/reset` | Reset failed load |

### Incremental
| Method | Path | Description |
|--------|------|-------------|
| POST | `/migration/v1/catchup/{payerKey}` | Full incremental (upsert+delete+reassign) |
| POST | `/migration/v1/incremental/upsert/{payerKey}` | Upsert changed files only |
| POST | `/migration/v1/incremental/delete/{payerKey}` | Delete removed files only |
| POST | `/migration/v1/incremental/reassign/{payerKey}` | Handle moved claims only |
| GET | `/migration/v1/catchup/{payerKey}/status` | Incremental status |

### Maintenance
| Method | Path | Description |
|--------|------|-------------|
| POST | `/migration/v1/maintenance/optimize/{payerKey}` | Compact small files |
| POST | `/migration/v1/maintenance/expire-snapshots` | Remove old snapshots |
| POST | `/migration/v1/maintenance/delete-orphans` | Clean orphan files |

---

## Full Load Pipeline

### Step 1: S3 Scan → Member Discovery
**File**: `S3FileScanner.java`

- Uses S3 LIST with delimiter `/` to extract distinct member keys
- Stores member manifest on S3: `metadata/{payerKey}/members.json`
- No file paths stored — S3 paths are deterministic: `{payer}/{member}/*.json`

### Step 2: Chunk Creation
**File**: `S3FileScanner.java` + `S3MetadataService.java`

- Reads member manifest, partitions into chunks of 2000 members each
- Writes chunk manifests to S3: `metadata/{payerKey}/chunks/{i}.json`
- Inserts 1 row per chunk into MySQL `iceberg_chunk_progress` table

### Step 2.5: Table Setup
**File**: `SparkIcebergWriter.ensureTablesExist()`

- Reads 5-member sample with `multiline=true` (small, OK to be slow)
- Creates Glue database + 3 Iceberg tables if missing
- Schema inferred from sample, auto-evolved later via `mergeSchema=true`
- Never drops existing tables (data safety)

### Step 3: Process Each Chunk
**File**: `FullLoadService.processChunk()` + `JsonPreCompactionService.java`

#### 3a. Pre-Compaction (THE KEY OPTIMIZATION)

**Problem**: Spark's `multiline=true` uses BinaryFileRDD → reads each file as individual S3 GET → 113K sequential GETs = 30+ minutes per chunk.

**Solution**: AWS SDK reads all JSON files in parallel (50 threads), parses with Jackson (`readTree` → `writeValueAsString`), writes as single-line JSONL to temp S3 path. Spark then reads JSONL without `multiline=true` → TextInputFormat → fast parallel read.

```
BEFORE: Spark reads 113K files one-by-one → 30 min per chunk
AFTER:  50 threads read in parallel → write JSONL → Spark reads 24 files → 3.5 min per chunk
```

**Safety features**:
- 3 retries per file with exponential backoff (500ms, 1000ms, 1500ms)
- Failed files logged as ERROR with exact S3 key: `THIS CLAIM WILL BE MISSING`
- Temp JSONL files cleaned up after each chunk
- Memory-safe: flushes to S3 every 50MB

**Fallback**: Set `precompact-enabled=false` to use original `multiline=true` path.

#### 3b. DualCaseResolver
**File**: `DualCaseResolver.java`

JSON files have mixed casing (PascalCase and camelCase). Resolver normalizes all 73 known field mappings to lowercase. Called ONCE per chunk (optimized from 3× — one per transformer).

#### 3c. Transform
**Files**: `ClaimsTransformer.java`, `DiagnosisTransformer.java`, `ClaimLinesTransformer.java`

| Transformer | Source | Output Table | Key Logic |
|-------------|--------|-------------|-----------|
| ClaimsTransformer | JSON root fields | `claims` | Flat mapping, timestamp normalization |
| DiagnosisTransformer | `claimDiagnosisList[]` | `claimdiagnosis` | `explode()` nested array |
| ClaimLinesTransformer | `claimLinesList[]` | `claimlines` | `explode()` nested array, safe field access |

Common patterns:
- `safeCol()`: Handles missing/varying fields gracefully (no AnalysisException)
- `normalizeTimestamp()`: Epoch ms / ISO string → Spark TimestampType
- `normalizeDate()`: Epoch ms / ISO string → Spark DateType
- `dropDuplicates()`: Dedup on composite key (payerkey, memberkey, claimkey)
- `loadyear`/`loadmonth`: Derived from `claimLoadDateTime` for partitioning

#### 3d. Iceberg Write
**File**: `SparkIcebergWriter.appendAllTables()`

- Writes 3 tables **SEQUENTIALLY** (claims → claimdiagnosis → claimlines)
- Uses `mergeSchema=true` for automatic schema evolution
- DynamoDB lock ensures 1 commit at a time per table
- Iceberg built-in retry (10 attempts) handles `CommitFailedException`

#### 3e. Cleanup
- `unpersist(blocking=true)` — frees cached DataFrame memory immediately
- Delete temp JSONL files from S3
- No `System.gc()` — G1GC handles garbage collection automatically

### Step 4: OPTIMIZE
**File**: `IcebergMaintenanceService.java`

Compacts small Parquet files from many chunk appends into larger files for query performance.

### Step 5: Watermark + Cleanup
**File**: `WatermarkService.java`

- Saves watermark with member→claimkey baseline for incremental detection
- Deletes chunk progress from MySQL + metadata from S3

---

## Distributed Full Load

### Flow

```
Coordinator (1 task)                    Workers (N tasks via LB)
─────────────────────                   ────────────────────────
1. S3 scan → member manifest
2. Create chunks in DB
3. Ensure tables exist
4. Fan out N HTTP POSTs via LB ───────> Each receives POST
5. Join as worker                       Each runs work-stealing loop:
                                          - claimNextChunk() → atomic SQL
                                          - processChunk()
                                          - completeChunk()
                                          - repeat until no chunks left
                                        Last worker becomes FINISHER:
                                          - OPTIMIZE
                                          - Save watermark
                                          - Cleanup
```

### Work-Stealing Design
**File**: `ChunkProgressRepository.java`

| Method | SQL Pattern | Purpose |
|--------|------------|---------|
| `claimNextChunk()` | `UPDATE SET status=PROCESSING, worker_id=? WHERE status=PENDING LIMIT 1` | Atomic chunk claim |
| `completeChunk()` | `UPDATE SET status=COMPLETED WHERE chunk_index=? AND worker_id=?` | Mark done |
| `failChunk()` | `UPDATE SET status=FAILED, retry_count++ WHERE chunk_index=?` | Mark failed |
| `reclaimStaleChunks()` | `UPDATE SET status=PENDING WHERE status=PROCESSING AND updated_at < NOW()-30min` | Reclaim crashed |
| `claimFinisherRole()` | `INSERT IGNORE INTO finisher_lock` | Exactly 1 finisher |

### Concurrency Controls

```
┌─────────────────────────────────────────────────────────────────┐
│ LEVEL 1: Per-Task Spark Lock (Semaphore)                        │
│   Only 1 Spark job runs per ECS task at a time.                │
│   If LB sends 2 worker calls to same task, 2nd waits.          │
│                                                                  │
│ LEVEL 2: Per-Table DynamoDB Lock (Iceberg built-in)             │
│   Only 1 Iceberg commit per table across ALL tasks.             │
│   Other workers retry automatically (10 attempts).              │
│                                                                  │
│ LEVEL 3: Chunk Claiming (MySQL atomic UPDATE)                   │
│   No two workers can claim the same chunk.                      │
│   UPDATE WHERE status=PENDING LIMIT 1 is atomic.               │
│                                                                  │
│ LEVEL 4: Finisher Election (MySQL INSERT IGNORE)                │
│   Exactly 1 task runs OPTIMIZE + watermark + cleanup.           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Edge Cases Covered

### Full Load Edge Cases

| Case | How It's Handled |
|------|-----------------|
| **ECS task crashes mid-chunk** | `reclaimStaleChunks()` resets PROCESSING→PENDING after 30 min. Another worker picks it up. |
| **Chunk fails 3 times** | Status set to FAILED, skipped by other workers. Visible in status endpoint. Use `/reset` to retry. |
| **S3 file read fails** | 3 retries with backoff. If all fail, logged as ERROR with S3 key. Claim missing from table. |
| **Iceberg commit conflict** | Iceberg's built-in 10-attempt retry with DynamoDB lock. Transparent to our code. |
| **JSON has new columns** | `mergeSchema=true` auto-adds new columns to Iceberg table schema. |
| **JSON has mixed case fields** | `DualCaseResolver` normalizes 73 known mappings. Unknown fields pass through. |
| **JSON has missing fields** | `safeCol()` returns null literal instead of throwing AnalysisException. |
| **Resume after failure** | `findFirstIncompleteChunkIndex()` resumes from first non-COMPLETED chunk. |
| **Double coordinator call** | `isDistributedCoordinating` AtomicBoolean rejects duplicate. |
| **Full load already running** | `isAnyFullLoadRunning()` checks: isRunning flag + coordinating flag + DB chunks. |
| **Stale Glue table metadata** | Logs ERROR with manual fix instructions. Never auto-drops tables. |
| **Empty chunk (0 members)** | Skipped, marked COMPLETED. |
| **Temp JSONL not cleaned** | Cleaned in `finally` block. If JVM crashes, orphan files remain in `tmp-jsonl/` — harmless. |

### Incremental Edge Cases

| Case | How It's Handled |
|------|-----------------|
| **Claim moved from M1 to M2** | Detected by comparing member→claimkey sets. Purged from M1, upserted to M2. |
| **Member fully deleted** | All S3 files gone → all claims deleted from Iceberg for that member. |
| **Member partially deleted** | Some files gone → `reconcilePartialDeletes()` keeps surviving, removes deleted. |
| **Concurrent incremental on same payer** | Per-payer ConcurrentHashMap lock. Second call returns 409 CONFLICT. |
| **Incremental during full load** | Rejected. `isAnyFullLoadRunning()` guard on all incremental endpoints. |
| **overwritePartitions conflict** | 5-retry loop with exponential backoff + jitter. Re-reads fresh data each attempt. |
| **No watermark exists** | Incremental rejects with error. Must run full load first. |

---

## Who Calls What & When

### Ingestion Service (uploads new claims to S3)
```
1. Upload JSON files to s3://{bucket}/{payer}/{member}/{claim}.json
2. Call: POST /migration/v1/incremental/upsert/{payerKey}
   → IncrementalLoadService.executeIncrementalUpsert()
   → Scans S3 for new/changed files since watermark
   → Merge-upserts into Iceberg
```

### Reader Service (deletes claims from S3)
```
1. Delete JSON files from S3
2. Call: POST /migration/v1/incremental/delete/{payerKey}
   → IncrementalLoadService.executeIncrementalDelete()
   → Compares watermark baseline vs current S3
   → Deletes missing claims from Iceberg
```

### Reader Service (moves claims between members)
```
1. Move JSON files from s3://{payer}/M1/ to s3://{payer}/M2/
2. Call: POST /migration/v1/incremental/reassign/{payerKey}
   → IncrementalLoadService.executeIncrementalReassign()
   → Detects claimkeys that changed member
   → Purges from old member, upserts to new member
```

### Ops/Admin (first-time load or re-load)
```
1. Scale ECS service to 10 tasks
2. Call: POST /migration/v1/fullload/distributed/{payerKey}/10
   → FullLoadService.coordinateAndFanOut()
   → Coordinator: S3 scan → chunks → table setup → fan out 10 HTTP calls
   → 10 workers: work-stealing loop → process chunks → FINISHER
3. Monitor: GET /migration/v1/fullload/distributed/{payerKey}/status
4. If failed: DELETE /migration/v1/fullload/{payerKey}/reset → re-run step 2
```

### Ops/Admin (daily catchup)
```
Call: POST /migration/v1/catchup/{payerKey}
  → IncrementalLoadService.executeIncrementalLoad()
  → Does ALL 3: upsert + delete + reassign in one pass
```

### Ops/Admin (maintenance)
```
Weekly:  POST /migration/v1/maintenance/optimize/{payerKey}
Monthly: POST /migration/v1/maintenance/expire-snapshots
Monthly: POST /migration/v1/maintenance/delete-orphans
```

---

## Recommended Sizing

| Payer Size | Members | Files | Chunk Size | Tasks | Concurrency | Est. Time |
|-----------|---------|-------|-----------|-------|-------------|-----------|
| Small | <50K | <3M | 2000 | 3 | 3 | ~15 min |
| Medium | 50K-200K | 3M-12M | 2000 | 5 | 5 | ~40 min |
| Large | 200K-500K | 12M-30M | 2000 | 10 | 10 | ~2 hrs |
| X-Large | 500K-1M | 30M-60M | 2000 | 15 | 15 | ~3.5 hrs |

**Memory per task**: 30 GB (16g Spark + 4g off-heap + 10g OS/Spring)
**Cost**: ~$0.12/task/hour on Fargate (4 vCPU / 30 GB)

---

## Performance History

| Date | Version | Change | Members | Time | Speedup |
|------|---------|--------|---------|------|---------|
| Before 04/18 | v1 | multiline=true (BinaryFileRDD) | 300K | 8+ hrs (incomplete) | baseline |
| 04/18 | v2 | Pre-compaction (50 threads) | 326K | 40 min | **72x** |
| 04/18 | v2 | + 3 retries per S3 file | — | — | safety |
| 04/18 | v2 | + Sequential writes (not parallel) | — | — | less lock contention |
| 04/18 | v2 | + Remove System.gc() | — | — | no GC pauses |

