# Remote Backup: Chunked Parquet Design

**Date:** December 19, 2025  
**Status:** Design Proposal  
**Replaces:** tar+zstd bundle format (bundle.rs)

## Overview

This document describes the new remote backup design that replaces tar bundles with chunked parquet files stored in a remote Delta Lake instance. This design provides better support for large files, atomic transactions, streaming I/O, and data integrity verification.

## Design Goals

1. **Atomic Backups**: Each pond commit backs up atomically to remote Delta Lake
2. **Large File Support**: Handle files of any size via chunking
3. **Streaming I/O**: No need to load entire files into memory
4. **Data Integrity**: Per-chunk CRC32 + per-file SHA256 verification
5. **Efficient Queries**: Use Delta Lake partitioning and SQL queries
6. **Deduplication**: Same file (by SHA256) can be referenced by multiple commits
7. **No Special Formats**: Pure parquet + Delta Lake, no tar archives

## Schema Design

```rust
Schema {
    // PARTITION COLUMN - identifies the file
    file_id: String,           // sha256 of file OR "metadata_{pond_txn_id}"
    
    // File identity and transaction context
    pond_txn_id: Int64,        // Which pond commit this belongs to
    original_path: String,     // Original path in pond (e.g., "part_id=abc.../part-00001.parquet")
    file_type: String,         // "pond_parquet" | "large_file" | "metadata"
    
    // Chunk information
    chunk_id: Int64,           // 0, 1, 2, ... (ordering within file)
    chunk_crc32: UInt32,       // CRC32 of this chunk
    chunk_data: Binary,        // The actual chunk (10-100MB)
    
    // File-level metadata (same for all chunks of a file)
    total_size: Int64,         // Total file size in bytes
    total_sha256: String,      // Full file SHA256 hash
    chunk_count: Int64,        // Total chunks in this file
    
    // Transaction metadata (duplicated across all files, but queryable)
    cli_args: String,          // JSON array: ["mknod", "remote", "/path"]
    created_at: Int64,         // Unix milliseconds when backup created
}
```

### Field Details

| Field | Type | Purpose | Notes |
|-------|------|---------|-------|
| `file_id` | String | Partition key | sha256 for data files, `"metadata_{txn_id}"` for metadata |
| `pond_txn_id` | Int64 | Links files to pond commit | Allows querying all files in a backup |
| `original_path` | String | Source path in pond | Relative path including `part_id=` prefix |
| `file_type` | String | File classification | "pond_parquet", "large_file", or "metadata" |
| `chunk_id` | Int64 | Chunk ordering | 0-based, sequential within file |
| `chunk_crc32` | UInt32 | Chunk integrity | CRC32 of `chunk_data` bytes |
| `chunk_data` | Binary | Actual data | 10-100MB per chunk (configurable) |
| `total_size` | Int64 | File size | Same for all chunks of a file |
| `total_sha256` | String | File hash | Same for all chunks of a file |
| `chunk_count` | Int64 | Total chunks | Same for all chunks of a file |
| `cli_args` | String | Command that created backup | JSON array as string |
| `created_at` | Int64 | Backup timestamp | Unix milliseconds |

## Partition Strategy

Delta Lake uses `file_id` as the partition column, creating this directory structure:

```
/remote_pond/
  file_id=metadata_123/          # Transaction metadata
    part-00000.parquet
  file_id=abc123def.../           # Pond parquet file (sha256)
    part-00000.parquet
  file_id=456789abc.../           # Another pond file
    part-00000.parquet
  file_id=111222333.../           # Large binary file
    part-00000.parquet
    part-00001.parquet           # Multiple parquet files for many chunks
```

### Benefits of this partitioning:

- **Fast lookup**: Query by `file_id` uses partition pruning
- **Self-documenting**: Filename tells you the file's SHA256
- **Deduplication-friendly**: Same file can be referenced by multiple `pond_txn_id`s
- **Metadata isolation**: `file_id=metadata_*` partitions separate from data

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
    sha256: String,         // File hash (= file_id for this file)
    size: i64,              // Total file size
    file_type: String,      // "pond_parquet" | "large_file"
}
```

**Example row:**
```rust
{
    file_id: "metadata_123",
    pond_txn_id: 123,
    original_path: "METADATA",
    file_type: "metadata",
    chunk_id: 0,
    chunk_crc32: crc32(&json_bytes),
    chunk_data: serde_json::to_vec(&TransactionMetadata { ... }),
    total_size: json_bytes.len(),
    total_sha256: sha256(&json_bytes),
    chunk_count: 1,
    cli_args: "[\"mknod\",\"remote\",\"/path\"]",
    created_at: 1703001234567,
}
```

## Chunking Strategy

### Chunk Size Selection

**Recommended: 10-100MB per chunk**

| Chunk Size | Pros | Cons |
|------------|------|------|
| 10MB | More granular checksums, better resume | More overhead, more rows |
| 50MB | Good balance | - |
| 100MB | Fewer rows, less overhead | Larger in-memory buffers |

### Parquet Writer Configuration

```rust
WriterProperties::builder()
    .set_compression(Compression::UNCOMPRESSED)  // Data may already be compressed
    .set_encoding(Encoding::PLAIN)               // Simple encoding for binary
    .set_data_page_size_limit(chunk_size + 1024) // ~One chunk per page
    .set_dictionary_enabled(false)               // No dictionary for random data
    .build()
```

### CRC Feature

Enable the `crc` feature in parquet crate for automatic page-level CRC validation:

```toml
[dependencies]
parquet = { version = "...", features = ["crc"] }
```

This provides **two levels of integrity checking**:
1. Parquet page-level CRC32 (when feature enabled)
2. Our chunk-level CRC32 (in `chunk_crc32` column)
3. File-level SHA256 (in `total_sha256` column)

## Example: Backing Up One Pond Commit

### Scenario

Pond commit 123 contains:
- 3 parquet files from pond partitions
- 2 large binary files

### Remote Delta Lake After Backup

```
file_id=metadata_123/
  part-00000.parquet              # Transaction summary (1 row)

file_id=abc123.../                # Pond file 1 (sha256: abc123...)
  part-00000.parquet              # Chunks 0-9 (10 rows)

file_id=def456.../                # Pond file 2 (sha256: def456...)
  part-00000.parquet              # Chunks 0-5 (6 rows)

file_id=789abc.../                # Pond file 3 (sha256: 789abc...)
  part-00000.parquet              # Chunks 0-12 (13 rows)

file_id=111222.../                # Large file 1 (sha256: 111222...)
  part-00000.parquet              # Chunks 0-99 (100 rows)

file_id=333444.../                # Large file 2 (sha256: 333444...)
  part-00000.parquet              # Chunks 0-150 (151 rows)
```

**Total: 6 partitions, 281 rows, all committed atomically in one Delta Lake transaction**

## Query Examples

### Get Transaction Metadata

```sql
SELECT chunk_data 
FROM remote_pond 
WHERE file_id = 'metadata_123' AND chunk_id = 0;
```

### List All Files in a Commit

```sql
SELECT DISTINCT 
    file_id,
    original_path,
    file_type,
    total_size,
    total_sha256
FROM remote_pond 
WHERE pond_txn_id = 123 
  AND file_type != 'metadata'
ORDER BY original_path;
```

### Restore a Specific File

```sql
SELECT 
    chunk_id,
    chunk_crc32,
    chunk_data
FROM remote_pond
WHERE file_id = 'abc123def...' 
ORDER BY chunk_id;
```

### Find All Commits Containing a File (Deduplication)

```sql
SELECT 
    pond_txn_id,
    created_at,
    cli_args
FROM remote_pond
WHERE file_id = 'abc123def...'
GROUP BY pond_txn_id, created_at, cli_args
ORDER BY pond_txn_id DESC;
```

### Get Total Backup Size for a Commit

```sql
SELECT 
    pond_txn_id,
    COUNT(DISTINCT file_id) as file_count,
    SUM(LENGTH(chunk_data)) as total_bytes,
    MIN(created_at) as backup_time
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
| **Checksums** | ❌ None | ✅ CRC32/chunk + SHA256/file |
| **Query** | ❌ Must extract | ✅ SQL queries |
| **Deduplication** | ❌ Each commit duplicates files | ✅ Same file_id shared across commits |
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

## Future Enhancements

### 1. Add BLAKE3 Checksums

Add `chunk_blake3: FixedSizeBinary(32)` column for stronger integrity:

```rust
Schema {
    // ... existing fields
    chunk_blake3: FixedSizeBinary(32),  // BLAKE3 hash of chunk
}
```

**Benefits:**
- Stronger cryptographic verification
- Detect intentional tampering
- Only 32 bytes overhead per chunk

### 2. Compression Options

For large files that aren't already compressed:

```rust
WriterProperties::builder()
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    // ... other settings
```

### 3. Incremental Backups

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

### 4. Garbage Collection

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
- Parquet CRC Feature: arrow-rs/parquet README.md
- Current Bundle Implementation: crates/tlogfs/src/bundle.rs (to be replaced)

## Open Questions

1. **Chunk size**: 10MB, 50MB, or 100MB? (Recommend 50MB)
2. **Compression**: Should we compress chunk_data? (Recommend: only for large files, not pond files)
3. **BLAKE3**: Add now or later? (Recommend: add `chunk_blake3` column from start)
4. **Migration**: Mandatory or optional? (Recommend: optional, keep tar for old backups)
5. **Parquet files per partition**: One large file or split by row count? (Recommend: one file per file_id)

## Approval Required

- [ ] Design review
- [ ] Schema finalized
- [ ] Implementation plan approved
- [ ] Migration strategy confirmed
