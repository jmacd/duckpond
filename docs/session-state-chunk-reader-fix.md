# Session State: Chunk Reader Fix

**Date:** December 23, 2025

## The Original Problem

Transaction restore was failing with error:
```
Error: Failed to restore transaction 1: Invalid chunk sequence: expected chunk_id=1, got 0
```

### Root Cause Analysis

When reading bundle_id `FILE-META-2025-12-23-1`:
- Query returned 2 rows with `chunk_id=0`
- This means 2 different files were stored in the same bundle
- Reader expected chunks in sequence (0, 1, 2...) but got (0, 0, ...)

## The Schema

Full Delta Lake table schema:
```
bundle_id        String  (PARTITION KEY - physical directory)
pond_id          String
pond_txn_id      Int64
path             String  (original file path)
version          Int64
file_type        String  (pond_parquet | large_file | metadata)
chunk_id         Int64   (0, 1, 2... within each file)
chunk_crc32      Int32
chunk_data       Binary
total_size       Int64
total_sha256     String  (SHA256 of complete file)
chunk_count      Int64
cli_args         String
created_at       Timestamp
```

## Bundle ID Design

Two types of bundles:

### 1. Large Files
- Format: `POND-FILE-{sha256}`
- One file per bundle_id
- Content-addressed (same content = same bundle_id)

### 2. Transaction Files  
- Format: `FILE-META-{YYYY-MM-DD}-{txn_seq}`
- **Multiple files per bundle_id**
- All files in transaction N share: `FILE-META-2025-12-23-N`
- Files distinguished by `path` column (and `total_sha256`)

## The Problem with Current Reader

### Original Reader Query
```sql
SELECT chunk_id, chunk_crc32, chunk_data, total_size, total_sha256, chunk_count 
FROM remote_files 
WHERE bundle_id = '{bundle_id}' 
ORDER BY chunk_id
```

### Why It Failed
- For `FILE-META-2025-12-23-1` with 2 files:
  - File A: chunks with chunk_id (0, 1, 2...)
  - File B: chunks with chunk_id (0, 1, 2...)
- Query returned ALL chunks from BOTH files
- Ordered by chunk_id → (0, 0, 1, 1, 2, 2...)
- Reader expected (0, 1, 2...) → got 0 twice → ERROR

## Changes Made

### 1. Modified ChunkedReader Struct
Added `path` field:
```rust
pub struct ChunkedReader<'a> {
    table: &'a DeltaTable,
    bundle_id: String,
    path: String,  // NEW
}
```

### 2. Updated Reader Query
Changed to filter by BOTH bundle_id AND path:
```sql
SELECT chunk_id, chunk_crc32, chunk_data, total_size, total_sha256, chunk_count 
FROM remote_files 
WHERE bundle_id = '{bundle_id}' AND path = '{path}'
ORDER BY chunk_id
```

### 3. Updated RemoteTable.read_file() API
```rust
// Before
pub async fn read_file(&self, bundle_id: &str, writer: W) -> Result<()>

// After  
pub async fn read_file(&self, bundle_id: &str, path: &str, writer: W) -> Result<()>
```

### 4. Updated All Callers
- `factory.rs::apply_parquet_files_from_remote()` - pass `path` variable
- `factory.rs::execute_pull()` - pass `original_path`
- `factory.rs::execute_verify()` - pass `file_path` from query
- Tests in `reader.rs` - pass literal path strings

## Current Status

### Compilation: ✅ SUCCESS
```
Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.54s
```

### Tests: ⚠️ NOT YET RUN
User prevented running test to think through the design.

## Open Questions

### Question 1: Is `path` the Right Column?
User response: "I disagree. what is your query trying to do?"

Possible interpretations:
1. **Path is not unique?** Could two files in a bundle have same path?
2. **Should use `total_sha256` instead?** SHA256 is definitely unique
3. **Should use both path AND sha256?** Belt-and-suspenders approach
4. **Misunderstood the schema?** Are there other constraints?

### Question 2: What Makes a File Unique?
Within a `FILE-META-{date}-{txn}` bundle, what uniquely identifies a file?

Candidates:
- `path` - original file location (e.g., "part_id=abc/part-00001.parquet")
- `total_sha256` - content hash
- `pond_id` - always same within transaction?
- Combination of multiple columns?

### Question 3: Should Large Files Be Different?
For `POND-FILE-{sha256}` bundles:
- Only one file per bundle
- Path might be redundant
- Could special-case: if bundle starts with "POND-FILE-", skip path filter?

## What Needs to Happen Next

1. **Clarify the unique key** - What column(s) make a file unique within a bundle?
2. **Update the query** - Use the correct WHERE clause
3. **Test** - Run setup_oteljson.sh to verify fix works
4. **Update documentation** - Fix remote-backup-architecture.md with correct info

## Files Changed

Modified files (not yet tested):
```
crates/remote/src/reader.rs        - Added path field, updated query
crates/remote/src/table.rs         - Added path parameter to read_file()
crates/remote/src/factory.rs       - Updated all read_file() calls
crates/remote/src/schema.rs        - No changes (kept transaction_bundle_id())
```

## Git State

Currently have uncommitted changes. No git revert performed because:
- User said: "I will not let you revert because when you do that you also have literally no idea where we are reverting to"
- User said: "I refuse to let you run git commands unless you ask me to save a point"

Recommendation: Once design is confirmed, commit as checkpoint before testing.

## Key Insights from User

1. "Have you read the architecture document?" - Document may be outdated
2. "We need both naming schemes" - Can't use single approach for all files
3. "Think about how we are using partitions" - bundle_id is partition, not unique file ID
4. "Do you understand how our chunked parquet encoding works?" - Multiple files can share bundle_id, distinguished by OTHER columns
5. "What is your query trying to do?" - Need to be very clear about what uniquely identifies a file

## Next Step

Wait for user guidance on what columns should be in the WHERE clause to uniquely identify a file for reading.
