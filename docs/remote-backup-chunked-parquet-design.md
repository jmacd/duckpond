# Remote Backup: Chunked Parquet Design

**Date:** December 19, 2025  
**Updated:** January 1, 2026  
**Status:** Implemented  
**Replaces:** tar+zstd bundle format (bundle.rs)

## Overview

This document describes the new remote backup design that replaces tar bundles with chunked parquet files stored in a remote Delta Lake instance. This design provides better support for large files, atomic transactions, streaming I/O, and data integrity verification.

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
    
    // Chunk information with BLAKE3 Merkle tree
    chunk_id: Int64,           // 0, 1, 2, ... (ordering within file)
    chunk_hash: String,        // BLAKE3 hash of this chunk (hex)
    chunk_outboard: Binary,    // bao-tree Merkle outboard for verified streaming
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
| `chunk_hash` | String | Chunk BLAKE3 hash | 64-char hex string, root of chunk's Merkle tree |
| `chunk_outboard` | Binary | Merkle outboard data | bao-tree format for verified streaming (~0.4% overhead) |
| `chunk_data` | Binary | Actual data | 4-64MB per chunk (default 16MB) |
| `total_size` | Int64 | File size | Same for all chunks of a file |
| `root_hash` | String | File root hash | Combined from chunk hashes using BLAKE3 tree |

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

The implementation uses BLAKE3 hash trees at two levels:

**Per-Chunk Level** (via `bao-tree` crate):
- Each chunk gets its own Merkle tree with 16KB blocks
- Outboard data stored in `chunk_outboard` column
- Enables verified streaming: validate data as it's read
- ~0.39% space overhead: `(blocks - 1) * 64 bytes`

**Per-File Level** (via `blake3::hazmat`):
- Chunk hashes combined into file `root_hash`
- Uses `merge_subtrees_root` / `merge_subtrees_non_root` functions
- Validates entire file integrity from chunk hashes

```toml
[dependencies]
bao-tree = "0.16"  # Per-chunk Merkle outboards
blake3 = "1.8"     # Hash combining via hazmat API
```

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

1. **Phase 1**: Implement chunked parquet writer
   - Keep tar bundle writer for compatibility
   - New backups use chunked format
   - Old backups remain in tar format

2. **Phase 2**: Implement restore from both formats
   - Auto-detect format (tar vs chunked)
   - Restore from either format transparently

3. **Phase 3**: Optional migration tool
   - Convert old tar bundles to chunked format
   - Maintain same pond_txn_id references

4. **Phase 4**: Remove tar bundle code
   - After all old backups expired or migrated
   - Remove bundle.rs and dependencies

## Implementation Summary (January 2026)

The BLAKE3 migration was completed with the following changes:

### Schema Changes
| Old Field | New Field | Change |
|-----------|-----------|--------|
| `chunk_crc32` | `chunk_hash` | CRC32 → BLAKE3 hex string |
| - | `chunk_outboard` | New: bao-tree Merkle outboard |
| `total_sha256` | `root_hash` | SHA256 → BLAKE3 (combined from chunks) |
| `cli_args` | - | Removed (redundant) |
| `created_at` | - | Removed (redundant) |
| `chunk_count` | - | Removed (derivable from row count) |
| `file_id` | `bundle_id` | Renamed for clarity |

### Crate Dependencies
- **Added**: `bao-tree = "0.16"` for per-chunk outboard creation
- **Added**: `blake3 = "1.8"` for hash combining via `hazmat` API
- **Removed**: `sha2`, `crc32fast`

### Key Implementation Details
1. **Per-chunk hashing**: `bao_tree::PostOrderMemOutboard::create()` with 16KB blocks
2. **Hash combining**: `blake3::hazmat::{merge_subtrees_root, merge_subtrees_non_root}`
3. **Verification on read**: Recompute chunk hash + combine for root hash verification
4. **Empty file handling**: `blake3::hash(&[])` for zero-byte files

### Files Modified
- [crates/remote/src/schema.rs](../crates/remote/src/schema.rs) - Schema constants and Arrow schema
- [crates/remote/src/writer.rs](../crates/remote/src/writer.rs) - Chunk hashing and outboard creation
- [crates/remote/src/reader.rs](../crates/remote/src/reader.rs) - Verification on restore
- [crates/remote/src/table.rs](../crates/remote/src/table.rs) - SQL column names
- [crates/remote/src/lib.rs](../crates/remote/src/lib.rs) - Public exports

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

- Delta Lake Transaction Protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- Parquet Format Specification: https://parquet.apache.org/docs/file-format/
- bao-tree crate (Merkle outboards): https://crates.io/crates/bao-tree
- BLAKE3 specification: https://github.com/BLAKE3-team/BLAKE3-specs
- bao format (verified streaming): https://github.com/oconnor663/bao
- Current Bundle Implementation: crates/tlogfs/src/bundle.rs (to be replaced)

## Open Questions

1. ~~**Chunk size**: 10MB, 50MB, or 100MB?~~ → **Resolved**: 16MB default (power-of-2)
2. **Compression**: Should we compress chunk_data? (Recommend: only for large files, not pond files)
3. ~~**BLAKE3**: Add now or later?~~ → **Resolved**: Implemented with full Merkle tree support
4. **Migration**: Mandatory or optional? (Recommend: optional, keep tar for old backups)
5. **Parquet files per partition**: One large file or split by row count? (Recommend: one file per bundle_id)

## Approval Status

- [x] Design review
- [x] Schema finalized (January 2026 - BLAKE3 migration)
- [x] Implementation plan approved
- [ ] Migration strategy confirmed
