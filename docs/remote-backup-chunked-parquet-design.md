# Remote Backup: Chunked Parquet Design

**Date:** December 19, 2025  
**Updated:** January 4, 2026  
**Status:** Implemented  
**Replaced:** tar+zstd bundle format (removed)  
**See Also:** [bao-tree-design.md](bao-tree-design.md) for detailed bao-tree validation strategies

## Overview

This document describes the new remote backup design that replaces tar bundles with chunked parquet files stored in a remote Delta Lake instance. This design provides better support for large files, atomic transactions, streaming I/O, and data integrity verification.

**Key Implementation Note:** Remote backups share the same chunked parquet format as local large file storage via `utilities::chunked_files`. Remote bundles always use **offset=0** for bao-tree hashing (they are standalone files, not versions within a FilePhysicalSeries).

## Design Goals

1. **Atomic Backups**: Each pond commit backs up atomically to remote Delta Lake
2. **Large File Support**: Handle files of any size via chunking
3. **Streaming I/O**: No need to load entire files into memory
4. **Data Integrity**: Per-chunk BLAKE3 hash with Merkle outboard + per-file root hash verification
5. **Verified Streaming**: Outboard data enables streaming validation without full file read
6. **Efficient Queries**: Use Delta Lake partitioning and SQL queries
7. **Deduplication**: Same file (by root hash) can be referenced by multiple commits
8. **No Special Formats**: Pure parquet + Delta Lake, no tar archives
9. **Future rsync Support**: Merkle tree structure enables future delta transfer optimizations

## Schema Design

```rust
Schema {
    // PARTITION COLUMN - identifies the file
    bundle_id: String,         // UUID for this backup bundle
    
    // File identity and transaction context
    pond_txn_id: Int64,        // Which pond commit this belongs to
    original_path: String,     // Original path in pond (e.g., "part_id=abc.../part-00001.parquet")
    file_type: String,         // "large_file" | "metadata"
    
    // Chunk information with BLAKE3 Merkle tree (SeriesOutboard format)
    chunk_id: Int64,           // 0, 1, 2, ... (ordering within file)
    chunk_hash: String,        // Cumulative BLAKE3 bao-root through this chunk (hex)
    chunk_outboard: Binary,    // SeriesOutboard.to_bytes() - cumulative state for verification
    chunk_data: Binary,        // The actual chunk (4-64MB, default 16MB)
    
    // File-level metadata (same for all chunks of a file)
    total_size: Int64,         // Total file size in bytes
    root_hash: String,         // BLAKE3 root hash (combined from chunk hashes)
}
```

### Field Details

| Field | Type | Purpose | Notes |
|-------|------|---------|-------|
| `bundle_id` | String | Partition key | UUID identifying this backup bundle |
| `pond_txn_id` | Int64 | Links files to pond commit | Allows querying all files in a backup |
| `original_path` | String | Source path in pond | Relative path including `part_id=` prefix |
| `file_type` | String | File classification | "large_file" or "metadata" |
| `chunk_id` | Int64 | Chunk ordering | 0-based, sequential within file |
| `chunk_hash` | String | Cumulative BLAKE3 | 64-char hex, bao-root through chunks 0..N |
| `chunk_outboard` | Binary | SeriesOutboard | Encodes cumulative_size, cumulative_blake3, frontier |
| `chunk_data` | Binary | Actual data | 4-64MB per chunk (default 16MB) |
| `total_size` | Int64 | File size | Same for all chunks of a file |
| `root_hash` | String | File root hash | Final chunk's cumulative_blake3 |

## Partition Strategy

Delta Lake uses `bundle_id` as the partition column, creating this directory structure:

```
/remote_pond/
  bundle_id=550e8400.../          # Backup bundle UUID
    part-00000.parquet            # All chunks for files in this bundle
```

### Benefits of this partitioning:

- **Fast lookup**: Query by `bundle_id` uses partition pruning
- **Self-contained**: Each bundle is independent
- **Deduplication-friendly**: Same file can be referenced by multiple `pond_txn_id`s
- **Simple structure**: One partition per backup operation

## Transaction Metadata File

Each pond commit backup includes a special metadata file that summarizes the transaction:

```rust
// Schema same as above, but chunk_data contains:
TransactionMetadata {
    file_count: usize,
    files: Vec<FileInfo>,
    cli_args: Vec<String>,
    created_at: i64,
}

FileInfo {
    path: String,           // Original path in pond
    root_hash: String,      // BLAKE3 root hash for the file
    size: i64,              // Total file size
    file_type: String,      // "large_file"
}
```

**Example row:**
```rust
{
    bundle_id: "550e8400-e29b-41d4-a716-446655440000",
    pond_txn_id: 123,
    original_path: "METADATA",
    file_type: "metadata",
    chunk_id: 0,
    chunk_hash: "a1b2c3d4...",  // BLAKE3 hash (hex)
    chunk_outboard: vec![...],   // bao-tree outboard bytes
    chunk_data: serde_json::to_vec(&TransactionMetadata { ... }),
    total_size: json_bytes.len(),
    root_hash: "e5f6g7h8...",   // BLAKE3 root hash (hex)
}
```

## Chunking Strategy

### Chunk Size Selection

**Implemented: 16MB default (power-of-2 sizes)**

| Constant | Value | Purpose |
|----------|-------|----------|
| `CHUNK_SIZE_MIN` | 4MB | Minimum chunk size |
| `CHUNK_SIZE_DEFAULT` | 16MB | Default chunk size (power-of-2) |
| `CHUNK_SIZE_MAX` | 64MB | Maximum chunk size |
| `BLAKE3_BLOCK_SIZE` | 16KB | Merkle tree block size (~0.39% overhead) |

### Parquet Writer Configuration

```rust
WriterProperties::builder()
    .set_compression(Compression::UNCOMPRESSED)  // Data may already be compressed
    .set_encoding(Encoding::PLAIN)               // Simple encoding for binary
    .set_data_page_size_limit(chunk_size + 1024) // ~One chunk per page
    .set_dictionary_enabled(false)               // No dictionary for random data
    .build()
```

### BLAKE3 Merkle Tree Integrity

The implementation uses `SeriesOutboard` format from `utilities::bao_outboard` (see [bao-tree-design.md](bao-tree-design.md)):

**Per-Chunk Level** (via `SeriesOutboard`):
- Each chunk stores cumulative bao-tree state through all chunks 0..N
- `chunk_hash` = cumulative bao-root (not per-chunk hash)
- `chunk_outboard` = `SeriesOutboard.to_bytes()` encoding:
  - `cumulative_size`: Total bytes through this chunk
  - `cumulative_blake3`: Bao-root through all content so far
  - `frontier`: Rightmost Merkle tree path for resumption
  - `version_size`: This chunk's size
- 16KB blocks (chunk_log=4), ~0.39% space overhead

**Remote Backup vs Local Large Files**:
- **Remote backups**: Always start at offset=0 (standalone files)
- **Local large files in FilePhysicalSeries**: May start at cumulative offset from prior versions

**Verification on Restore**:
- Chunk 0: Verify `SeriesOutboard::first_version(chunk_data).cumulative_blake3 == stored`
- Chunk N: Verify cumulative hash progression using stored `SeriesOutboard`
- Final chunk's `cumulative_blake3` must equal file's `root_hash`

## Example: Backing Up One Pond Commit

### Scenario

Pond commit 123 contains:
- 3 parquet files from pond partitions
- 2 large binary files

### Remote Delta Lake After Backup

```
bundle_id=550e8400-e29b-41d4.../
  part-00000.parquet              # All file chunks in one bundle
```

**Total: 1 partition with all chunks committed atomically in one Delta Lake transaction**

## Query Examples

### Get Transaction Metadata

```sql
SELECT chunk_data 
FROM remote_pond 
WHERE file_type = 'metadata' AND pond_txn_id = 123;
```

### List All Files in a Commit

```sql
SELECT DISTINCT 
    bundle_id,
    original_path,
    file_type,
    total_size,
    root_hash
FROM remote_pond 
WHERE pond_txn_id = 123 
  AND file_type != 'metadata'
ORDER BY original_path;
```

### Restore a Specific File

```sql
SELECT 
    chunk_id,
    chunk_hash,
    chunk_outboard,
    chunk_data
FROM remote_pond
WHERE original_path = 'path/to/file' AND pond_txn_id = 123
ORDER BY chunk_id;
```

### Find All Commits Containing a File (by root hash)

```sql
SELECT DISTINCT
    pond_txn_id,
    original_path
FROM remote_pond
WHERE root_hash = 'abc123def...'
ORDER BY pond_txn_id DESC;
```

### Get Total Backup Size for a Commit

```sql
SELECT 
    pond_txn_id,
    COUNT(DISTINCT original_path) as file_count,
    SUM(LENGTH(chunk_data)) as total_bytes
FROM remote_pond
WHERE pond_txn_id = 123
GROUP BY pond_txn_id;
```

## Implementation Outline

### 1. Write Path (Backup)

```rust
async fn backup_pond_commit(
    pond_txn_id: i64,
    pond_files: Vec<PondFile>,      // From delta log
    large_files: Vec<LargeFile>,    // From tlogfs
    cli_args: Vec<String>,
    remote_table: &mut DeltaTable,
) -> Result<()> {
    let mut delta_txn = remote_table.create_transaction(None);
    
    // 1. Write transaction metadata file
    let metadata = create_transaction_metadata(
        pond_txn_id, 
        &pond_files, 
        &large_files,
        cli_args,
    );
    write_metadata_file(&mut delta_txn, metadata).await?;
    
    // 2. Write each pond parquet file as chunked parquet
    for pond_file in pond_files {
        let file_id = compute_sha256(&pond_file)?;
        write_chunked_file(
            &mut delta_txn,
            file_id,
            pond_txn_id,
            &pond_file.path,
            "pond_parquet",
            pond_file.reader,
        ).await?;
    }
    
    // 3. Write each large file as chunked parquet
    for large_file in large_files {
        let file_id = large_file.sha256.clone();
        write_chunked_file(
            &mut delta_txn,
            file_id,
            pond_txn_id,
            &large_file.path,
            "large_file",
            large_file.reader,
        ).await?;
    }
    
    // 4. Commit atomically
    delta_txn.commit(None, None).await?;
    
    Ok(())
}
```

### 2. Write Chunked File

```rust
async fn write_chunked_file(
    delta_txn: &mut Transaction,
    file_id: String,
    pond_txn_id: i64,
    original_path: &str,
    file_type: &str,
    mut reader: impl AsyncRead + Unpin,
) -> Result<()> {
    const CHUNK_SIZE: usize = 50 * 1024 * 1024; // 50MB
    
    let schema = create_schema();
    let props = create_writer_properties();
    
    // Calculate file hash as we read
    let mut hasher = blake3::Hasher::new();
    let mut total_size = 0u64;
    let mut chunk_id = 0i64;
    let mut chunks = Vec::new();
    
    let mut buffer = vec![0u8; CHUNK_SIZE];
    
    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 { break; }
        
        let chunk = &buffer[..n];
        hasher.update(chunk);
        total_size += n as u64;
        
        let crc = crc32fast::hash(chunk);
        
        chunks.push((chunk_id, crc, chunk.to_vec()));
        chunk_id += 1;
    }
    
    let total_sha256 = hasher.finalize().to_hex().to_string();
    let chunk_count = chunks.len() as i64;
    
    // Write all chunks to parquet
    let mut writer = ArrowWriter::try_new(output, schema.clone(), Some(props))?;
    
    for (chunk_id, crc, data) in chunks {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![file_id.clone()])),
                Arc::new(Int64Array::from(vec![pond_txn_id])),
                Arc::new(StringArray::from(vec![original_path.to_string()])),
                Arc::new(StringArray::from(vec![file_type.to_string()])),
                Arc::new(Int64Array::from(vec![chunk_id])),
                Arc::new(UInt32Array::from(vec![crc])),
                Arc::new(BinaryArray::from(vec![&data[..]])),
                Arc::new(Int64Array::from(vec![total_size as i64])),
                Arc::new(StringArray::from(vec![total_sha256.clone()])),
                Arc::new(Int64Array::from(vec![chunk_count])),
                Arc::new(StringArray::from(vec![cli_args_json.clone()])),
                Arc::new(Int64Array::from(vec![created_at])),
            ],
        )?;
        
        writer.write(&batch)?;
    }
    
    writer.close()?;
    
    // Add to delta transaction
    let add = Add {
        path: format!("file_id={}/part-00000.parquet", file_id),
        tags: Some(HashMap::from([
            ("pond_txn_id".to_string(), pond_txn_id.to_string()),
            ("file_type".to_string(), file_type.to_string()),
            ("total_sha256".to_string(), total_sha256),
            ("chunk_count".to_string(), chunk_count.to_string()),
        ])),
        // ... other Add fields
    };
    
    delta_txn.add_action(add);
    
    Ok(())
}
```

### 3. Read Path (Restore)

```rust
async fn restore_file(
    remote_table: &DeltaTable,
    file_id: &str,
    output: impl AsyncWrite + Unpin,
) -> Result<()> {
    // Query for all chunks of this file
    let files = remote_table
        .get_files_by_partitions(&[("file_id", file_id)])
        .await?;
    
    for file_path in files {
        let file = open_parquet_async(&file_path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        let mut stream = builder.build()?;
        
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            
            let chunk_ids = batch.column(4).as_primitive::<Int64Type>();
            let crcs = batch.column(5).as_primitive::<UInt32Type>();
            let data = batch.column(6).as_binary::<i32>();
            
            for i in 0..batch.num_rows() {
                let chunk_id = chunk_ids.value(i);
                let expected_crc = crcs.value(i);
                let chunk = data.value(i);
                
                // Verify CRC
                let actual_crc = crc32fast::hash(chunk);
                if expected_crc != actual_crc {
                    return Err(Error::CorruptedChunk {
                        file_id: file_id.to_string(),
                        chunk_id,
                    });
                }
                
                // Write verified chunk
                output.write_all(chunk).await?;
            }
        }
    }
    
    output.flush().await?;
    Ok(())
}
```

## Comparison with Tar Bundle Format

| Feature | Tar Bundle (Old) | Chunked Parquet (New) |
|---------|-----------------|----------------------|
| **Format** | tar+zstd | parquet + Delta Lake |
| **Large Files** | ❌ Not supported | ✅ Fully supported via chunking |
| **Atomicity** | ❌ File-based | ✅ Delta transaction |
| **Streaming** | ❌ Must decompress entire tar | ✅ Read any file independently |
| **Checksums** | ❌ None | ✅ BLAKE3/chunk + Merkle outboard + root hash |
| **Verified Streaming** | ❌ Not possible | ✅ Validate chunks as they stream |
| **Query** | ❌ Must extract | ✅ SQL queries |
| **Deduplication** | ❌ Each commit duplicates files | ✅ Same root_hash shared across commits |
| **Resume** | ❌ All-or-nothing | ✅ Can resume chunk-by-chunk |
| **Metadata** | ✅ metadata.json in tar | ✅ Structured in schema |
| **Compression** | ✅ zstd on entire tar | ⚠️ Optional per-file (pond files already compressed) |
| **Object Store** | ❌ Custom integration | ✅ Native Delta Lake integration |

## Migration Plan

**No backwards compatibility required.** The tar bundle format has been removed.

1. ~~**Phase 1**: Implement chunked parquet writer~~ → **Done**
2. ~~**Phase 2**: Implement restore from both formats~~ → **Skipped** (no backwards compat)
3. ~~**Phase 3**: Optional migration tool~~ → **Skipped** (no backwards compat)
4. ~~**Phase 4**: Remove tar bundle code~~ → **Done** (bundle.rs removed, tokio-tar/zstd dependencies removed)

## Implementation Summary (January 2026)

The BLAKE3 migration was completed with the following changes:

### Architecture: Shared Chunked Parquet Format

Remote backups now share the same chunked parquet logic as local large file storage:

```
utilities::chunked_files  ← Shared chunking + SeriesOutboard logic
       ↑                ↑
       │                │
  tlogfs::large_files   remote::writer
  (local _large_files)  (Delta Lake backup)
```

**Key difference**: Local large files in `FilePhysicalSeries` may use non-zero starting offsets for cumulative checksumming. Remote backups always use offset=0 since each file is standalone.

### Schema Changes
| Old Field | New Field | Change |
|-----------|-----------|--------|
| `chunk_crc32` | `chunk_hash` | CRC32 → cumulative BLAKE3 bao-root |
| - | `chunk_outboard` | New: SeriesOutboard.to_bytes() |
| `total_sha256` | `root_hash` | SHA256 → final cumulative_blake3 |
| `cli_args` | - | Removed (redundant) |
| `created_at` | - | Removed (redundant) |
| `chunk_count` | - | Removed (derivable from row count) |
| `file_id` | `bundle_id` | Renamed for clarity |

### Crate Dependencies
- **Shared**: `utilities::chunked_files` - chunking, SeriesOutboard, Arrow schema
- **Shared**: `utilities::bao_outboard` - SeriesOutboard, IncrementalHashState
- **Added**: `blake3 = "1.8"` for hash computation
- **Removed**: `sha2`, `crc32fast`, `bao-tree` (now internal to utilities)

### Key Implementation Details
1. **SeriesOutboard format**: Each chunk stores cumulative bao-tree state (not per-chunk)
2. **Chunk hash semantics**: `chunk_hash` = cumulative bao-root through chunks 0..N
3. **Verification on read**: Parse SeriesOutboard, verify cumulative progression
4. **Empty file handling**: `SeriesOutboard::first_version(&[])` for zero-byte files
5. **root_hash**: Final chunk's cumulative_blake3 (bao-root of entire file)

### Files Implementing Chunked Parquet
- [crates/utilities/src/chunked_files.rs](../crates/utilities/src/chunked_files.rs) - Shared chunking logic
- [crates/utilities/src/bao_outboard.rs](../crates/utilities/src/bao_outboard.rs) - SeriesOutboard struct
- [crates/remote/src/writer.rs](../crates/remote/src/writer.rs) - Delta Lake wrapper
- [crates/tlogfs/src/large_files.rs](../crates/tlogfs/src/large_files.rs) - Local large file storage

---

## Future Enhancements

### 1. Compression Options

For large files that aren't already compressed:

```rust
WriterProperties::builder()
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    // ... other settings
```

### 2. Incremental Backups

Track which files have already been backed up:

```sql
-- Find files NOT yet in remote
SELECT DISTINCT sha256 
FROM pond_local.files
WHERE sha256 NOT IN (
    SELECT DISTINCT file_id 
    FROM remote_pond
    WHERE file_id != 'metadata_%'
);
```

Only backup files that don't exist remotely yet.

### 3. Garbage Collection

Expire old commits while keeping shared files:

```rust
async fn expire_old_commits(
    remote_table: &mut DeltaTable,
    before_txn_id: i64,
) -> Result<()> {
    // Get all file_ids still referenced by newer commits
    let active_files = query_active_files(remote_table, before_txn_id).await?;
    
    // Delete rows for old commits
    let delete_expr = format!("pond_txn_id < {}", before_txn_id);
    remote_table.delete(Some(delete_expr)).await?;
    
    // Vacuum to remove unreferenced files
    remote_table.vacuum().await?;
    
    Ok(())
}
```

## References

- [bao-tree-design.md](bao-tree-design.md) - DuckPond bao-tree validation strategies and SeriesOutboard format
- Delta Lake Transaction Protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- Parquet Format Specification: https://parquet.apache.org/docs/file-format/
- bao-tree crate (underlying Merkle implementation): https://crates.io/crates/bao-tree
- BLAKE3 specification: https://github.com/BLAKE3-team/BLAKE3-specs
- bao format (verified streaming): https://github.com/oconnor663/bao

## Open Questions

1. ~~**Chunk size**: 10MB, 50MB, or 100MB?~~ → **Resolved**: 16MB default (power-of-2)
2. **Compression**: Should we compress chunk_data? (Recommend: only for large files, not pond files)
3. ~~**BLAKE3**: Add now or later?~~ → **Resolved**: Implemented with SeriesOutboard format
4. ~~**Migration**: Mandatory or optional?~~ → **Resolved**: No backwards compatibility required, tar bundle code removed
5. ~~**Parquet files per partition**: One large file or split by row count?~~ → **Resolved**: One file per bundle_id

## Approval Status

- [x] Design review
- [x] Schema finalized (January 2026 - SeriesOutboard format)
- [x] Implementation plan approved
- [x] Shared chunked_files module implemented
- [ ] Migration strategy confirmed

