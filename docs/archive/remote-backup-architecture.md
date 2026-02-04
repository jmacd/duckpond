# Remote Backup Architecture

## Overview

The remote backup system provides content-addressed storage for DuckPond data using Delta Lake and Apache Parquet. Files are split into chunks, stored with SHA256-based deduplication, and can be replicated across environments.

## Core Components

### 1. Remote Factory (`crates/remote/src/factory.rs`)

The remote factory is a provider plugin that handles backup/restore operations through subcommands:

- **`push`**: Backs up pond parquet files and large files to remote storage
- **`pull`**: Downloads files from remote that don't exist locally
- **`replicate`**: Generates a base64-encoded config for cloning ponds to new environments
- **`list-files`**: Shows what's stored in remote backup

The factory operates through the provider system's `ExecutionContext` and runs in `PreTxn` mode (before transactions commit).

### 2. Chunked Parquet Encoding

#### Delta Lake Schema (`crates/remote/src/schema.rs`)

The remote backup table has this Delta Lake schema:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **bundle_id** | String | No | **PARTITION COLUMN** - Content address for deduplication |
| pond_id | String | No | UUID of source pond |
| pond_txn_id | Int64 | No | Transaction sequence number from pond |
| path | String | No | Original file path (e.g., "part_id=abc/part-00001.parquet" or "_delta_log/00000000000000000001.json") |
| version | Int64 | No | Delta Lake version number when backed up |
| file_type | String | No | Type: "pond_parquet" (data files), "large_file" (spillover files), or "metadata" (Delta commit logs) |
| chunk_id | Int64 | No | Chunk sequence within file (0, 1, 2, ...) |
| chunk_crc32 | Int32 | No | CRC32 checksum of this chunk's data |
| chunk_data | Binary | No | Raw bytes (up to 50MB per chunk) |
| total_size | Int64 | No | Total file size in bytes |
| total_sha256 | String | No | SHA256 hash of complete file |
| chunk_count | Int64 | No | Total number of chunks for this file |
| cli_args | String | No | JSON array of CLI args that triggered backup |
| created_at | Timestamp | No | UTC timestamp (microsecond precision) |

**Partitioning Strategy:**
- Partitioned by `bundle_id` (first column)
- Each unique file gets its own partition directory
- Example: `/tmp/pond-backups/bundle_id=POND-FILE-c18f77.../part-00000.parquet`
- Enables efficient file-level queries and deduplication

**Key Insights:**
- Each row = one chunk of a file
- File metadata (total_size, total_sha256, chunk_count) is **duplicated across all chunk rows**
- Chunk reassembly: `SELECT chunk_data FROM table WHERE bundle_id = ? ORDER BY chunk_id`
- Deduplication: Same content → same total_sha256 → same bundle_id → reused partition

#### Chunk Size

- Default: 50MB (`CHUNK_SIZE_DEFAULT`)
- Range: 1MB - 100MB
- Files larger than 50MB are split into multiple chunks
- Each chunk is a separate parquet row

#### Bundle ID Strategy

| File Type | Bundle ID Format | Purpose |
|-----------|-----------------|---------|
| LargeFile | `POND-FILE-{sha256}` | Content deduplication - same content = same bundle_id |
| PondParquet | `POND-PARQUET-{txn}` | Transaction-specific - each txn gets unique bundle |
| Metadata | `POND-DELTALOG-{version}` | Delta commit log JSON for specific version |

### 3. Writer Pipeline (`crates/remote/src/writer.rs`)

**ChunkedWriter** streams files to Delta Lake:

1. **Read Phase**: Reads file in 50MB chunks, computing SHA256 and CRC32 incrementally
2. **Buffer Phase**: Accumulates chunks in memory (up to 10 chunks = ~500MB batch)
3. **Write Phase**: Creates Arrow RecordBatch and writes to Delta Lake in a single transaction

Key features:
- Bounded memory usage through batch writing
- Streaming SHA256 computation (no need to read file twice)
- Empty files get a single row with empty `chunk_data`

### 4. Reader Pipeline (`crates/remote/src/reader.rs`)

**ChunkedReader** reconstructs files from chunks:

1. **Query**: `SELECT chunk_data FROM remote_files WHERE bundle_id = ? ORDER BY chunk_id`
2. **Reassemble**: Concatenates chunks in order
3. **Verify**: Compares SHA256 of reassembled data with stored hash
4. **Stream**: Returns `AsyncRead` implementation for zero-copy streaming

### 5. Remote Table (`crates/remote/src/table.rs`)

**RemoteTable** wraps Delta Lake operations:

- Manages Delta Lake table at configured path (local filesystem or S3)
- Provides DataFusion `SessionContext` for SQL queries
- Registers Delta table as `remote_files` for querying
- Handles table refreshes after writes

Schema operations:
```rust
// Write a file
remote_table.write_file(pond_id, txn_id, path, version, file_type, reader, cli_args)

// Read a file  
remote_table.read_file(bundle_id, output_buffer)

// Query files
remote_table.list_files(pond_id)
```

## Interoperation with TLogFS and Steward

### TLogFS Integration

**Large Files** (`crates/tlogfs/src/large_files.rs`):

1. TLogFS writes files through `OpLogFileWriter`
2. `HybridWriter` checks file size threshold (default 1MB)
3. Small files: Written inline to Delta table
4. Large files: Spillover to `_large_files/sha256={hash}` on filesystem
5. OpLog records the SHA256, not the content

**Remote Backup** reads these SHA256-addressed files:
```rust
// get_large_files() scans _large_files directory
let large_files = get_large_files(&store_path)?;

// Backs up each to remote
for (path, size) in large_files {
    remote_table.write_file(..., FileType::LargeFile, ...);
}
```

The SHA256 in the filename is the content hash computed by `HybridWriter.poll_write()` - it updates the hasher as bytes are written, then renames the temp file to `sha256={hash}` on close.

### Steward Integration

**Session Context** (`crates/steward/src/session_context.rs`):

Steward provides the transaction boundary and metadata access:

```rust
pub struct SessionContext {
    pond_id: Uuid,
    birth_timestamp: i64,
    birth_hostname: String,
    birth_username: String,
    // ... Delta table and persistence handles
}
```

**Provider Execution** (`crates/provider/src/registry.rs`):

The remote factory runs via the provider system:

1. Steward opens a transaction via `StewardTransactionGuard`
2. Provider registry calls factory in `PreTxn` mode
3. Factory gets `FactoryContext` with pond metadata:
   ```rust
   pub struct FactoryContext {
       pub pond_metadata: Option<PondMetadata>,
       pub txn_seq: i64,
       pub store_path: String,
       // ...
   }
   ```
4. Factory queries the Delta table through `SessionContext.table()`
5. Backs up files to remote
6. Returns, steward commits transaction

**Single Transaction Rule**: The remote factory receives the already-opened transaction context. It must NOT call `OpLogPersistence::open()` or `begin()` - doing so would create a second transaction and panic. Instead, it uses the `SessionContext` provided in `FactoryContext`.

### Replication Flow

**Initial Pond** (source):
```bash
pond run /etc/system.d/1-backup replicate
# Outputs: pond init --config=<base64>
```

**Replica Pond** (destination):
```bash
pond init --config=<base64>
# Decodes ReplicationConfig with remote storage location and pond birth identity
# Creates new pond with same pond_id, birth_timestamp, hostname, username
# Pulls parquet files: OpLog and table data
# Pulls large files: Restores to _large_files/sha256={hash}
# Result: Identical pond in new location
```

The replication config contains:
```rust
pub struct ReplicationConfig {
    remote: RemoteConfig,      // S3/filesystem path
    pond_id: String,           // UUID to match
    birth_timestamp: i64,      // Original creation time
    birth_hostname: String,    // Original host
    birth_username: String,    // Original user
}
```

## File Type Flow

### Pond Parquet Files

```
TLogFS Delta Table
  └─> data parquet files (part-*.parquet)
      └─> Remote Factory: write_file(..., FileType::PondParquet, ...)
          └─> Delta Lake: POND-PARQUET-{txn}/chunk_0, chunk_1, ...
```

### Delta Commit Logs

```
TLogFS Delta Table
  └─> _delta_log/00000000000000000001.json (transaction metadata)
      └─> Remote Factory: write_file(..., FileType::Metadata, ...)
          └─> Delta Lake: POND-DELTALOG-{version}/chunk_0
```

### Large Files

```
Application → OpLogFileWriter → HybridWriter
                                    ├─> < 1MB: inline in Delta
                                    └─> ≥ 1MB: spillover to _large_files/sha256={hash}
                                                    └─> Remote Factory: write_file(..., FileType::LargeFile, ...)
                                                        └─> Delta Lake: POND-FILE-{sha256}/chunk_0, chunk_1, ...
```

### Data Redundancy

**Pond Parquet**: Stored both locally and remotely (backup copy)
**Delta Commit Logs**: Stored both locally (_delta_log/) and remotely (backup copy)  
**Large Files**: Stored both in `_large_files/` and remotely (backup copy)

All copies use SHA256 for integrity verification.

## Query Patterns

### Restore All Large Files for a Transaction

```sql
SELECT DISTINCT bundle_id, sha256_hash, path, total_size
FROM remote_files
WHERE pond_id = ?
  AND pond_txn_id = ?
  AND file_type = 'large_file'
```

### List All Ponds in Backup

```sql
SELECT DISTINCT pond_id, COUNT(*) as file_count
FROM remote_files
GROUP BY pond_id
```

### Get File Chunks for Restoration

```sql
SELECT chunk_id, chunk_data, crc32
FROM remote_files
WHERE bundle_id = ?
ORDER BY chunk_id
```

## Configuration

**Remote Storage** (`RemoteConfig`):
```rust
pub enum RemoteConfig {
    Local { path: String },
    S3 { bucket: String, prefix: String, region: String },
}
```

Currently only Local is implemented. S3 support requires adding AWS credentials and object_store S3 integration.

## Security Considerations

1. **Content Addressing**: SHA256 prevents tampering - restored files must match hash
2. **CRC32 per chunk**: Detects corruption during transfer or storage
3. **Immutable Delta Log**: Once written, Delta Lake commits cannot be changed
4. **Replication Identity**: Birth identity (pond_id, timestamp, hostname, user) prevents accidental cross-pond contamination

## Performance Characteristics

- **Write throughput**: Limited by Delta Lake transaction overhead (~1 transaction per file)
- **Read throughput**: Parallel chunk reads via DataFusion query engine
- **Memory usage**: Bounded by `CHUNKS_PER_BATCH` × `CHUNK_SIZE` ≈ 500MB
- **Deduplication**: O(1) bundle_id lookup for duplicate detection
- **Compression**: Parquet columnar format provides good compression on chunk_data column

## Future Enhancements

1. **Incremental Backup**: Only backup changed files since last push
2. **S3 Backend**: Implement S3 RemoteConfig variant
3. **Parallel Writes**: Batch multiple files per Delta transaction
4. **Compression**: Add zstd compression to chunk_data before writing
5. **Metadata Filtering**: Track which files are backed up to avoid rescanning _large_files
