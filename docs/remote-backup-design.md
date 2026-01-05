# Remote Backup System Design for DuckPond

> **⚠️ SUPERSEDED**: This document describes the original tar+zstd bundle design.
> The implementation uses chunked parquet in Delta Lake instead.
> See [remote-backup-chunked-parquet-design.md](remote-backup-chunked-parquet-design.md) for the current implementation.

## Executive Summary

This document describes the design for a remote backup system that bundles and uploads pond files to S3-compatible object storage as a post-commit action. The system will:

1. Detect new files created in each commit via Delta Lake's transaction log
2. Bundle multiple files into a single archive to minimize remote I/O operations
3. Upload bundles to S3-compatible storage using the object_store crate
4. Track backup state to avoid duplicate uploads
5. Support incremental backups with efficient change detection

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pond Write Transaction                        │
│  - User writes files (CSV, Parquet, etc.)                       │
│  - Files stored in data filesystem (OpLog + Delta Lake)         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼ commit()
┌─────────────────────────────────────────────────────────────────┐
│               Delta Lake Commit (Version N)                      │
│  - Transaction log entry: _delta_log/00000N.json                │
│  - Add actions record new/modified Parquet files                │
│  - Remove actions record deleted files                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼ post-commit trigger
┌─────────────────────────────────────────────────────────────────┐
│          Remote Factory (PostCommitReader mode)                  │
│                                                                  │
│  Phase 1: Detect Changes                                        │
│    - Read Delta Lake transaction log for version N              │
│    - Extract Add actions (new files)                            │
│    - Extract Remove actions (deleted files)                     │
│    - Query backup state table for already-uploaded files        │
│    - Compute delta: files to upload, files to delete            │
│                                                                  │
│  Phase 2: Bundle Files                                          │
│    - Group files by strategy (size-based, time-based)           │
│    - Read file contents from pond via TinyFS                    │
│    - Create tar.zstd bundle with metadata                       │
│    - Include manifest: version, txn_seq, file list              │
│                                                                  │
│  Phase 3: Upload                                                │
│    - Upload bundle to S3: {prefix}/{version}/bundle-{N}.tar.zst │
│    - Upload manifest: {prefix}/{version}/manifest.json          │
│    - Record upload in backup state table                        │
│    - Delete remote files if Remove actions present              │
│                                                                  │
│  Phase 4: Cleanup                                               │
│    - Update backup state with new version                       │
│    - Log statistics (files uploaded, bytes transferred)         │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Change Detection Strategy

**Decision:** Use Delta Lake's transaction log to detect file changes

**Rationale:**
- Delta Lake already tracks Add/Remove actions for each commit
- Avoids scanning entire filesystem to find changes
- Provides accurate diff between versions
- Includes file metadata (size, modification time, statistics)

**Implementation:**
```rust
// Read transaction log for specific version
let commit_log = read_delta_commit_log(version).await?;

// Extract Add actions (new files created in this commit)
let new_files: Vec<Add> = commit_log
    .iter()
    .filter_map(|action| {
        if let Action::Add(add) = action {
            Some(add.clone())
        } else {
            None
        }
    })
    .collect();

// Extract Remove actions (files deleted in this commit)
let removed_files: Vec<Remove> = commit_log
    .iter()
    .filter_map(|action| {
        if let Action::Remove(remove) = action {
            Some(remove.clone())
        } else {
            None
        }
    })
    .collect();
```

### 2. File Bundling Strategy

**Decision:** Bundle all files from a single commit into one archive

**Rationale:**
- Minimizes S3 PUT operations (each costs $0.005 per 1000 requests)
- Reduces HTTP overhead (single upload vs many small uploads)
- Simplifies recovery (one archive per version)
- Maintains atomic backup boundary (all-or-nothing for a commit)

**Bundle Format:**
```
{bucket}/{prefix}/{version}/bundle-{txn_seq}.tar.zst
  ├── manifest.json           # Metadata about bundle
  ├── files/
  │   ├── {file_1.parquet}   # Original file content
  │   ├── {file_2.parquet}
  │   └── ...
  └── metadata/
      ├── add_actions.json   # Delta Lake Add actions
      └── file_mapping.json  # Pond path → Parquet path mapping
```

**Manifest Format:**
```json
{
  "version": 42,
  "txn_seq": 1001,
  "txn_id": "uuid-string",
  "bundle_timestamp": "2025-10-19T19:30:00Z",
  "total_files": 12,
  "total_bytes": 1048576,
  "compression": "zstd",
  "files": [
    {
      "pond_path": "/data/sensors/2025/10/readings.csv",
      "parquet_path": "part-00001-abc123.parquet",
      "size": 87456,
      "checksum": "sha256:abc123..."
    }
  ]
}
```

**Alternative Considered:** Individual file uploads
- Pros: Simpler implementation, easier partial recovery
- Cons: Expensive at scale (100s of files = 100s of PUT requests), more failure modes
- **Rejected:** Cost and complexity outweigh benefits

### 3. Backup State Tracking

**Decision:** Store backup state in pond's control filesystem

**Rationale:**
- Local-first: backup state lives with the data
- Crash recovery: state persists across restarts
- Query via SQL: can inspect backup history
- Consistent with existing control table pattern

**Schema:**
```sql
CREATE TABLE backup_state (
    version BIGINT NOT NULL,           -- Delta Lake version backed up
    txn_seq BIGINT NOT NULL,           -- Transaction sequence
    backup_timestamp TIMESTAMP,        -- When backup completed
    bundle_path STRING,                -- S3 path to bundle
    manifest_path STRING,              -- S3 path to manifest
    total_files INT,                   -- Files in this backup
    total_bytes BIGINT,                -- Bytes uploaded
    status STRING,                     -- "pending", "uploading", "completed", "failed"
    error_message STRING,              -- Error if failed
    PRIMARY KEY (version)
);

CREATE TABLE file_backup_history (
    pond_path STRING NOT NULL,         -- Logical path in pond
    version BIGINT NOT NULL,           -- Version where file was backed up
    parquet_path STRING,               -- Delta Lake file path
    bundle_path STRING,                -- Which bundle contains this file
    file_size BIGINT,
    checksum STRING,
    PRIMARY KEY (pond_path, version)
);
```

### 4. Incremental vs Full Backup

**Decision:** Always incremental, version-by-version

**Rationale:**
- Delta Lake is inherently incremental (commit-by-commit)
- Each version's bundle is self-contained and immutable
- Recovery can reconstruct any version by replaying bundles 1→N
- Avoids expensive full scans

**Backup Sequence Example:**
```
Version 1: Bundle contains files A, B, C
Version 2: Bundle contains files D, E (only new files)
Version 3: Bundle contains file F, removal of file B
Version 4: Bundle contains files G, H

To restore version 3:
  1. Download bundles 1, 2, 3
  2. Extract all files
  3. Apply removals (delete B)
  4. Result: A, C, D, E, F
```

### 5. Compression and Deduplication

**Decision:** Use zstd compression, no cross-version deduplication

**Rationale:**
- Zstd: Fast, excellent compression ratio, streaming-friendly
- No deduplication: Simplifies implementation, each bundle is independent
- Parquet files already compressed internally (diminishing returns)

**Compression Settings:**
```rust
use zstd::stream::Encoder;

// Level 3 = balanced speed/compression (7-9x faster than gzip, similar ratio)
let encoder = Encoder::new(output, 3)?;
```

**Alternative Considered:** Content-addressable storage (CAS)
- Pros: Deduplicates identical files across versions
- Cons: Complex (requires file hash index), slower uploads (hash before upload), harder recovery
- **Rejected:** Complexity not justified for typical pond usage (files rarely identical across versions)

### 6. S3 Path Layout

**Decision:** Hierarchical layout by version

**Structure:**
```
s3://{bucket}/{prefix}/
  ├── versions/
  │   ├── 00001/
  │   │   ├── manifest.json
  │   │   └── bundle-001.tar.zst
  │   ├── 00002/
  │   │   ├── manifest.json
  │   │   └── bundle-002.tar.zst
  │   └── ...
  ├── state/
  │   ├── latest_version.json       # Pointer to most recent version
  │   └── backup_log.jsonl          # Append-only log of backups
  └── tombstones/
      └── removed_files.jsonl       # Track deleted files
```

**Rationale:**
- Easy to list versions (S3 prefix listing)
- Supports parallel uploads (different versions = different paths)
- Simple versioning scheme (directory = version number)
- Cleanup-friendly (delete entire version directory)

## Implementation Plan

### Phase 1: Change Detection (Week 1)

**Goals:**
- Read Delta Lake transaction log for a specific version
- Parse Add/Remove actions
- Map Parquet paths back to pond logical paths

**Tasks:**
1. Add method to `State` or `OpLogPersistence` to read commit log
2. Parse JSON transaction log entries
3. Create mapping: Parquet path → Pond path (via node_id/part_id)
4. Filter for data filesystem changes (ignore control filesystem)

**Test:**
```rust
#[tokio::test]
async fn test_detect_changes_from_delta_log() {
    let pond = test_pond().await;
    
    // Write some files in transaction 1
    pond.transact(|tx, fs| async {
        fs.write_file("/data/file1.csv", b"data1").await?;
        fs.write_file("/data/file2.csv", b"data2").await?;
        Ok(())
    }).await?;
    
    // Get changes for version 1
    let changes = detect_changes(&pond, 1).await?;
    assert_eq!(changes.added.len(), 2);
    assert_eq!(changes.removed.len(), 0);
    
    // Delete a file in transaction 2
    pond.transact(|tx, fs| async {
        fs.remove_file("/data/file1.csv").await?;
        Ok(())
    }).await?;
    
    // Get changes for version 2
    let changes = detect_changes(&pond, 2).await?;
    assert_eq!(changes.added.len(), 0);
    assert_eq!(changes.removed.len(), 1);
}
```

### Phase 2: Bundle Creation (Week 1-2)

**Goals:**
- Read file contents from pond
- Create tar.zstd bundle with manifest
- Write to memory buffer (not yet S3)

**Tasks:**
1. Implement `BundleBuilder` struct
2. Add files to tar archive with proper paths
3. Compress with zstd
4. Generate manifest.json
5. Calculate checksums (SHA256)

**Test:**
```rust
#[tokio::test]
async fn test_create_bundle() {
    let pond = test_pond().await;
    
    // Write test files
    pond.transact(|tx, fs| async {
        fs.write_file("/data/a.csv", b"content a").await?;
        fs.write_file("/data/b.csv", b"content b").await?;
        Ok(())
    }).await?;
    
    // Create bundle
    let changes = detect_changes(&pond, 1).await?;
    let bundle = create_bundle(&pond, 1, &changes).await?;
    
    // Verify bundle
    assert!(bundle.len() > 0);
    assert!(bundle.len() < 1000); // Compressed
    
    // Extract and verify
    let extracted = extract_bundle(&bundle).await?;
    assert_eq!(extracted.files.len(), 2);
}
```

### Phase 3: S3 Upload (Week 2)

**Goals:**
- Connect to S3 using object_store crate
- Upload bundle and manifest
- Handle errors and retries

**Tasks:**
1. Initialize `AmazonS3Builder` from RemoteConfig
2. Implement upload_bundle function
3. Add error handling (network failures, auth errors)
4. Add progress logging

**Test:**
```rust
#[tokio::test]
async fn test_upload_to_s3() {
    // Requires localstack or minio
    let s3_config = RemoteConfig {
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        key: "minioadmin".to_string(),
        secret: "minioadmin".to_string(),
    };
    
    let store = create_s3_store(&s3_config)?;
    
    let bundle_data = vec![1, 2, 3, 4]; // Dummy data
    upload_bundle(&store, 1, &bundle_data).await?;
    
    // Verify uploaded
    let downloaded = download_bundle(&store, 1).await?;
    assert_eq!(downloaded, bundle_data);
}
```

### Phase 4: State Tracking (Week 2-3)

**Goals:**
- Create backup_state and file_backup_history tables
- Record backup progress
- Query backup history

**Tasks:**
1. Extend control table schema
2. Implement `ControlTable::record_backup_started()`
3. Implement `ControlTable::record_backup_completed()`
4. Implement `ControlTable::get_backup_status()`
5. Add recovery queries (find pending/failed backups)

**Test:**
```rust
#[tokio::test]
async fn test_backup_state_tracking() {
    let control = ControlTable::open(...).await?;
    
    // Start backup
    control.record_backup_started(1, 1001).await?;
    
    // Complete backup
    control.record_backup_completed(
        1,
        "s3://bucket/versions/00001/bundle.tar.zst",
        "s3://bucket/versions/00001/manifest.json",
        10,
        1048576,
    ).await?;
    
    // Query status
    let status = control.get_backup_status(1).await?;
    assert_eq!(status.status, "completed");
    assert_eq!(status.total_files, 10);
}
```

### Phase 5: Integration (Week 3)

**Goals:**
- Integrate with post-commit infrastructure
- Handle errors gracefully
- Add CLI commands

**Tasks:**
1. Update remote_factory.rs to implement full backup logic
2. Test end-to-end: write → commit → backup → verify
3. Add `pond backup` CLI commands:
   - `pond backup status` - Show backup state
   - `pond backup verify --version N` - Verify backup integrity
   - `pond backup restore --version N` - Restore from backup
4. Documentation

**Test:**
```rust
#[tokio::test]
async fn test_end_to_end_backup() {
    let pond = test_pond_with_remote_config().await;
    
    // Write data
    pond.transact(|tx, fs| async {
        fs.write_file("/data/sensor.csv", CSV_DATA).await?;
        Ok(())
    }).await?;
    
    // Post-commit should trigger backup
    // (happens automatically in commit())
    
    // Verify backup exists in S3
    let s3_config = pond.get_remote_config().await?;
    let store = create_s3_store(&s3_config)?;
    let manifest = download_manifest(&store, 1).await?;
    
    assert_eq!(manifest.total_files, 1);
    assert_eq!(manifest.version, 1);
}
```

## Parquet Path → Pond Path Mapping Challenge

### Problem Statement

Delta Lake tracks Parquet files (e.g., `part-00001-uuid.parquet`), but we need to map these back to logical pond paths (e.g., `/data/sensors/readings.csv`) for:

1. User-friendly backup manifests
2. Restore operations (know where to put files)
3. Selective backup (backup specific directories)

### Mapping Strategy

**Option 1: Query OpLog for node_id/part_id relationships**

```rust
// Given a Parquet path from Delta Lake Add action
let parquet_path = "part-00001-abc123.parquet";

// Extract part_id from Parquet filename (embedded during write)
// OR query OpLog for part_id that generated this file

// Query OpLog records for this part_id to get node_id
let records = oplog.query_by_part_id(part_id).await?;
let node_id = records[0].node_id;

// Traverse TinyFS to build path from node_id
let pond_path = fs.path_from_node_id(node_id).await?;
```

**Pros:**
- Accurate (source of truth is OpLog)
- Works for any file structure

**Cons:**
- Requires OpLog query per file (could be slow)
- Requires traversing filesystem to build paths

**Option 2: Embed pond path in Parquet metadata**

```rust
// During write, add custom metadata to Parquet file
let metadata = HashMap::from([
    ("pond_path", "/data/sensors/readings.csv"),
    ("node_id", "12345"),
    ("part_id", "67890"),
]);

writer.write_metadata(metadata)?;
```

**Pros:**
- Fast (no extra queries)
- Self-contained (Parquet file has all info)

**Cons:**
- Requires modifying Parquet write path
- Increases file size slightly
- Metadata might not survive all transformations

**Option 3: Maintain separate mapping table**

```sql
CREATE TABLE parquet_path_mapping (
    parquet_path STRING PRIMARY KEY,
    pond_path STRING,
    node_id BIGINT,
    part_id BIGINT,
    version BIGINT,
    created_at TIMESTAMP
);
```

**Pros:**
- Fast lookups (indexed)
- Doesn't modify Parquet files
- Can update mappings if pond structure changes

**Cons:**
- Additional table to maintain
- Could get out of sync with actual data

### Recommended Solution: Hybrid Approach

1. **Primary:** Query OpLog for accurate mappings (Option 1)
2. **Cache:** Store results in mapping table for performance (Option 3)
3. **Future:** Embed metadata in Parquet for self-contained backups (Option 2)

**Implementation:**
```rust
async fn map_parquet_to_pond_paths(
    state: &State,
    add_actions: &[Add],
) -> Result<HashMap<String, String>> {
    let mut mapping = HashMap::new();
    
    for add in add_actions {
        let parquet_path = &add.path;
        
        // Try cache first
        if let Some(pond_path) = check_mapping_cache(parquet_path).await? {
            mapping.insert(parquet_path.clone(), pond_path);
            continue;
        }
        
        // Query OpLog for part_id → node_id
        let part_id = extract_part_id_from_parquet_path(parquet_path)?;
        let records = state.query_records_by_part_id(part_id).await?;
        
        if let Some(record) = records.first() {
            let node_id = record.node_id;
            
            // Build path from node_id
            let pond_path = build_path_from_node_id(state, node_id).await?;
            
            // Cache for next time
            cache_mapping(parquet_path, &pond_path, node_id, part_id).await?;
            
            mapping.insert(parquet_path.clone(), pond_path);
        } else {
            log::warn!("No OpLog records found for Parquet file: {}", parquet_path);
        }
    }
    
    Ok(mapping)
}
```

## Error Handling and Recovery

### Failure Scenarios

1. **Network failure during upload**
   - Detection: S3 PUT returns error
   - Recovery: Retry with exponential backoff (3 attempts)
   - State: Mark backup as "failed" in backup_state table

2. **Partial upload (bundle succeeds, manifest fails)**
   - Detection: Bundle uploaded, but manifest upload fails
   - Recovery: On next run, check for orphaned bundles, re-upload manifest
   - State: Track upload_stage: "bundle" | "manifest" | "complete"

3. **Crash during bundle creation**
   - Detection: backup_state shows "pending" or "uploading" on startup
   - Recovery: Resume from last checkpoint or restart backup
   - State: Use txn_seq to identify incomplete backups

4. **S3 authentication failure**
   - Detection: S3 returns 403 Forbidden
   - Recovery: Log error, skip backup, alert user
   - State: Record auth failure in error_message

5. **Insufficient S3 storage**
   - Detection: S3 returns 507 Insufficient Storage
   - Recovery: Log error, skip backup, alert user
   - State: Record quota error in error_message

### Recovery Command

```bash
# Find incomplete backups
pond backup recover --list

# Resume incomplete backup for version N
pond backup recover --version N

# Verify backup integrity
pond backup verify --version N

# Re-upload failed backup
pond backup retry --version N
```

## Performance Considerations

### Bundling Performance

**Expected Scale:**
- 100 files/commit × 1MB/file = 100MB bundle (uncompressed)
- Zstd compression: ~50MB bundle (compressed)
- Upload time: ~10 seconds @ 50 Mbps

**Optimization:**
- Stream files directly to tar (avoid buffering all in memory)
- Compress on-the-fly (streaming zstd encoder)
- Read files in parallel (use tokio tasks)

```rust
// Pseudo-code for streaming bundle creation
let encoder = ZstdEncoder::new(s3_upload_stream);
let mut tar_builder = TarBuilder::new(encoder);

for file_path in files {
    let reader = fs.async_reader(&file_path).await?;
    tar_builder.append_async_reader(&file_path, reader).await?;
}

tar_builder.finish().await?;
```

### Large File Handling

**Challenge:** Single 10GB CSV file exceeds memory

**Solution:** Stream file in chunks
```rust
const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB chunks

let mut reader = fs.async_reader(&large_file).await?;
let mut buffer = vec![0u8; CHUNK_SIZE];

loop {
    let n = reader.read(&mut buffer).await?;
    if n == 0 { break; }
    
    tar_builder.write_all(&buffer[..n]).await?;
}
```

### Parallelization

**Opportunity:** Multiple bundles can upload concurrently

```rust
// If backing up multiple versions at once
let tasks: Vec<_> = versions
    .iter()
    .map(|v| tokio::spawn(backup_version(*v)))
    .collect();

// Wait for all uploads
for task in tasks {
    task.await??;
}
```

**Constraint:** S3 multipart upload limits (10,000 parts max)

## CLI Interface

```bash
# Configure remote backup
pond config remote \
  --bucket my-pond-backups \
  --region us-west-2 \
  --endpoint https://s3.amazonaws.com \
  --key AKIAIOSFODNN7EXAMPLE \
  --secret wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Show backup status
pond backup status
# Output:
#   Latest backup: Version 42 (2025-10-19 19:30:00 UTC)
#   Total versions backed up: 42
#   Total data uploaded: 5.2 GB
#   Last backup duration: 8.3 seconds

# List backup history
pond backup list --limit 10

# Verify backup integrity
pond backup verify --version 42

# Restore from backup (downloads + extracts)
pond backup restore --version 42 --to /tmp/restored-pond

# Show what would be backed up (dry run)
pond backup preview --version 42

# Manually trigger backup for current version
pond backup now
```

## Security Considerations

1. **Credentials:** Store securely (environment variables, not in config files)
2. **Encryption:** Enable S3 server-side encryption (SSE-S3 or SSE-KMS)
3. **Access Control:** Use IAM policies to restrict bucket access
4. **Audit:** Log all backup operations to control table

**Example IAM Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-pond-backups/*",
        "arn:aws:s3:::my-pond-backups"
      ]
    }
  ]
}
```

## Future Enhancements

1. **Incremental tar archives:** Use tar's append feature to add only new files
2. **Cross-region replication:** Backup to multiple S3 regions for disaster recovery
3. **Retention policies:** Auto-delete old backups after N days
4. **Compression levels:** Allow user to choose zstd level (speed vs size tradeoff)
5. **Encryption:** Add client-side encryption before upload
6. **Differential backups:** Only backup changed blocks within files (advanced)

## Open Questions

1. **How to handle very large commits (1000+ files)?**
   - Option A: Single large bundle (could be multi-GB)
   - Option B: Split into multiple bundles (adds complexity)
   - **Recommendation:** Start with single bundle, add splitting if needed

2. **Should we backup control filesystem?**
   - Pros: Complete recovery, includes control table history
   - Cons: Adds overhead, control data less critical
   - **Recommendation:** Yes, but separate bundle from data filesystem

3. **How to handle Parquet files that don't map to pond paths?**
   - Scenario: User manually adds Parquet file via Delta Lake API
   - **Recommendation:** Backup anyway, use Parquet path as pond_path

4. **What if S3 upload is slower than pond writes?**
   - Risk: Backups fall behind, queue builds up
   - **Recommendation:** Make post-commit async, queue backups, report lag in status

---

## Implementation Status (December 2025)

### DeltaLake Chunked Storage Implementation - COMPLETED ✅

**Decision Made:** Replaced tar.zstd bundle format with DeltaLake table for chunked file storage

**Rationale:**
- **Native querying**: Can query backup contents with SQL (via DataFusion)
- **Partitioning**: Files organized by bundle_id (transaction date + sequence)
- **Incremental operations**: Insert new chunks without reading existing data
- **Compression**: Built-in Parquet compression (typically better than zstd on structured data)
- **Change detection**: Can query what's in each bundle without downloading
- **Large file support**: Files chunked to 100MB (configurable), enabling parallel upload/download

**Schema Implementation:**
```rust
// Arrow schema in crates/remote/src/schema.rs
fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("bundle_id", DataType::Utf8, false),      // Partition key: FILE-META-{date}-{txn} or POND-FILE-{sha256}
        Field::new("pond_txn_id", DataType::Int64, false),   // Transaction sequence number
        Field::new("path", DataType::Utf8, false),           // File path in pond
        Field::new("chunk_id", DataType::Int32, false),      // Chunk sequence (0-based)
        Field::new("chunk_crc32", DataType::UInt32, false),  // CRC32 checksum for chunk
        Field::new("chunk_data", DataType::Binary, false),   // Raw chunk bytes
        Field::new("total_size", DataType::Int64, false),    // Total file size
        Field::new("total_sha256", DataType::Utf8, false),   // SHA256 of complete file
        Field::new("chunk_count", DataType::Int32, false),   // Total chunks for file
        Field::new("cli_args", DataType::Utf8, false),       // Command that created bundle
        Field::new("created_at", DataType::Utf8, false),     // ISO timestamp
    ]))
}
```

**Partitioning Strategy:**
- Transaction metadata files: `bundle_id = FILE-META-{YYYY-MM-DD}-{txn_seq}`
  - Example: `FILE-META-2025-12-23-7` (transaction 7 on Dec 23)
  - Groups all files from one transaction together
  - Enables efficient "restore transaction N" queries

- Large files (>100MB): `bundle_id = POND-FILE-{sha256}`
  - Example: `POND-FILE-e3b0c44298fc1c14...` (content-addressed)
  - Deduplicates identical large files across transactions
  - Chunks spread across multiple Parquet row groups

**Query Key:** Files uniquely identified by `(bundle_id, path, pond_txn_id)`
- Bundle ID: Partition key (date-based or content-addressed)
- Path: File path within transaction
- Pond Transaction ID: Transaction sequence number
- Prevents chunk mixing when multiple files share same bundle_id

**API Implementation:**
```rust
// Write file to remote (crates/remote/src/table.rs)
pub async fn write_file<R: AsyncRead + Unpin>(
    &mut self,
    pond_txn_id: i64,
    path: impl Into<String>,
    reader: R,
    cli_args: Vec<String>,
) -> Result<String>  // Returns bundle_id

// Read file from remote
pub async fn read_file<W: AsyncWrite + Unpin>(
    &self,
    bundle_id: &str,
    path: &str,
    pond_txn_id: i64,
    writer: W,
) -> Result<()>

// List files in remote
pub async fn list_files(&self, _pond_id: &str) -> Result<Vec<(String, String, i64, i64)>>
// Returns: (bundle_id, path, pond_txn_id, total_size)

// List files for specific transaction
pub async fn list_transaction_files(&self, txn_seq: i64) 
    -> Result<Vec<(String, String, String, i64, i64)>>
// Returns: (bundle_id, path, sha256, size, pond_txn_id)
```

**Factory Integration:**
- **Push mode (primary pond):** Transaction files backed up post-commit to bundle `FILE-META-{date}-{txn}`
- **Pull mode (replica pond):** Syncs bundles from remote, reconstructs transactions locally
- **Large file handling:** Files >100MB written to `POND-FILE-{sha256}` bundles, fetched on-demand

**Chunking Benefits:**
1. **Streaming**: No need to buffer entire file in memory
2. **Resumability**: Can resume interrupted transfers at chunk boundary
3. **Parallelization**: Future enhancement - upload/download chunks concurrently
4. **Integrity**: Per-chunk CRC32 + total file SHA256 verification
5. **Efficiency**: Only download chunks needed for partial reads (future)

**Testing Status:**
- ✅ All 316 tests passing
- ✅ Roundtrip write/read verified (small and large files)
- ✅ Empty file handling works
- ✅ Replica initialization from bundles working
- ✅ Large file on-demand fetch implemented
- ✅ Integration tested with setup_oteljson.sh (7 transactions, 3 large files)

**Performance Characteristics:**
- Chunk size: 100MB (configurable)
- Write throughput: Limited by Parquet encoding + object store bandwidth
- Read throughput: Limited by network + Parquet decode (single-threaded currently)
- Query performance: Fast (DeltaLake indexes + predicate pushdown)

**Key Improvements Over Original Tar Bundle Design:**

| Aspect | Tar Bundle | DeltaLake Implementation |
|--------|-----------|-------------------------|
| Querying | Must download + extract entire bundle | SQL queries without download |
| Chunking | All-or-nothing | 100MB chunks, resumable |
| Deduplication | None | Content-addressed large files |
| Change detection | Download manifest | Query Delta log |
| Large file support | Memory pressure | Streaming with controlled buffer |
| Partial restore | Must extract entire bundle | Selective file download |
| Compression | zstd (tar level) | Parquet (columnar + dictionary) |

**Known Limitations & Future Work:**
1. **Sequential chunk download**: Currently downloads chunks in order (could parallelize)
2. **Bundle restore gap**: If replica init fails partway, use `pond control sync --config=<base64>` to resume
3. **No bundle GC**: Old bundles accumulate (need retention policy)
4. **Single-threaded**: Read/write operations not parallelized yet
5. **Schema evolution**: Adding columns requires careful migration

**Design Lessons Learned:**
1. **Partition key matters**: Initial design used pond_id as partition, but bundle_id is better (groups related files)
2. **Unique keys critical**: Required 3-column key (bundle_id, path, pond_txn_id) to prevent chunk collisions
3. **Schema bloat**: Removed redundant columns (pond_id, version, file_type) that were constant within partition
4. **Factory modes**: Automatic "push" default prevents test noise, explicit modes still preferred for clarity

**Configuration:**
```yaml
# Factory node at /etc/system.d/1-backup
factory: remote
mode: push   # Primary ponds backup automatically (default)
# mode: pull # Replica ponds sync on demand

config:
  remote_url: "s3://my-bucket/ponds/prod"
  endpoint: "https://s3.amazonaws.com"
  region: "us-west-2"
```

**Next Steps:**
1. ✅ Schema optimization (removed 3 redundant columns)
2. ✅ Factory mode defaults (automatic "push" for new factories)
3. ⏳ Implement retention policy (delete old bundles after N days)
4. ⏳ Add parallel chunk upload/download
5. ⏳ Bundle verification command (`pond control verify`)
6. ⏳ Metrics/monitoring (upload success rate, throughput)
