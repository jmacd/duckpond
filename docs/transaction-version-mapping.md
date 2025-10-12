# Transaction to Delta Lake Version Mapping

## Overview

DuckPond uses Delta Lake as its persistence layer, with each TLogFS transaction potentially creating a new Delta Lake version. Understanding the relationship between transactions and versions is critical for implementing commands like `pond show --mode detailed`.

## Architecture

### Transaction Flow

1. **Transaction Begin**: `OpLogPersistence::begin()` creates a new transaction guard with in-memory state
2. **Operations Accumulate**: File writes, directory modifications accumulate as `OplogEntry` records
3. **Transaction Commit**: `State::commit_impl()` writes accumulated records to Delta Lake

### Delta Lake Version Creation

From `crates/tlogfs/src/persistence.rs` (lines 1007-1041):

```rust
async fn commit_impl(
    &mut self,
    metadata: Option<HashMap<String, serde_json::Value>>,
    table: DeltaTable,
) -> Result<Option<()>, TLogFSError> {
    self.flush_directory_operations().await?;
    let records = std::mem::take(&mut self.records);

    if records.is_empty() {
        info!("Committing read-only transaction (no write operations)");
        return Ok(None);  // ← NO version created for read-only transactions
    }

    let count = records.len();
    info!("Committing {count} operations in {}", self.path);

    // Convert records to RecordBatch
    let batches = vec![
        serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &records)?,
    ];

    let mut write_op = DeltaOps(table).write(batches);

    // Add commit metadata (transaction ID, command args)
    if let Some(metadata_map) = metadata {
        let properties = CommitProperties::default()
            .with_metadata(metadata_map.into_iter());
        write_op = write_op.with_commit_properties(properties);
    }

    _ = write_op.await?;  // ← Creates new Delta Lake version here
    
    self.records.clear();
    self.operations.clear();
    Ok(Some(()))
}
```

**Key Points:**
1. **One transaction → Zero or One Delta version**
   - Read-only transactions create NO version
   - Write transactions create EXACTLY ONE version

2. **Transaction metadata stored in Delta commit**
   - `pond_txn` field contains: `{txn_id: "UUID", args: "[\"command\", \"args\"]"}`
   - Stored in `_delta_log/NNNNNNNNNNNNNNNNNNNN.json` commit metadata
   - NOT stored in the data rows themselves

3. **Multiple records per transaction**
   - One transaction can write many `OplogEntry` records
   - All records from one transaction go into ONE Delta version
   - Records are partitioned by `part_id` into separate parquet files

## The Problem with Current Detailed Mode

### What the Code Tries to Do

From `crates/cmd/src/commands/show.rs`:

```rust
async fn show_detailed_mode(
    commit_history: &[deltalake::kernel::CommitInfo],
    store_path: &str
) -> Result<String, steward::StewardError> {
    for commit_info in commit_history.iter() {
        // Problem 1: Tries to find version by timestamp matching
        match find_delta_log_version_by_timestamp(store_path, timestamp).await {
            Ok(Some(version)) => {
                // Problem 2: Multiple transactions can have same version
                // if read-only transactions are interleaved
            }
        }
    }
}
```

### Problems

#### Problem 1: Incorrect Version Lookup

The code uses `find_delta_log_version_by_timestamp()` to correlate transactions with versions. This is **fundamentally broken** because:

- Read-only transactions don't create versions
- Multiple write transactions at the same timestamp (batched operations) 
- Timestamp precision issues (microseconds vs milliseconds)

**Example:**
```
Transaction A (write)  @ t=1000 → creates version 1
Transaction B (read)   @ t=1001 → NO version created
Transaction C (write)  @ t=1002 → creates version 2
Transaction D (write)  @ t=1003 → creates version 3
```

When `show --mode detailed` runs:
- Looks up transaction A by timestamp → finds version 1 ✓
- Looks up transaction B by timestamp → finds version 1 (wrong!) ✗
- Looks up transaction C by timestamp → might find version 2 or 3 depending on timing ✗

#### Problem 2: Version Calculation from History

Delta Lake's `table.history(limit)` returns commits in **reverse chronological order**:

```rust
let history = table.history(None).await?;
// Returns: [version N, version N-1, ..., version 1, version 0]
```

The CORRECT way to calculate version numbers:

```rust
let current_version = table.version().unwrap_or(0);
for (index, commit_info) in history.iter().enumerate() {
    let version = current_version - (index as i64);
    // Now we know this commit created `version`
}
```

But this STILL doesn't solve the read-only transaction problem!

#### Problem 3: Missing Transactions

The output shows confusing results like:

```
=== Transaction 0199bc99-ca5c-7a98 (Command: ["set_extended_attributes", ...]) ===
  Delta Lake Version: 97

=== Transaction 0199bc99-ca0f-72a7 (Command: ["mknod", "dynamic-dir", "/templates"]) ===
  Delta Lake Version: 97

=== Transaction 0199bc99-c9bb-7f24 (Command: ["mknod", "dynamic-dir", "/reduced"]) ===
  Delta Lake Version: 97
```

**What actually happened:**
- These might be 3 separate transactions where only ONE was a write
- Or they might be showing the SAME parquet files multiple times
- The timestamp matching can't distinguish them

## The Correct Solution

### Option 1: Track Transaction-to-Version Mapping (Preferred)

Store the relationship in transaction metadata:

```rust
async fn commit_impl(...) -> Result<Option<i64>, TLogFSError> {
    // ... existing code ...
    
    if records.is_empty() {
        return Ok(None);  // Read-only, returns None
    }
    
    let table = write_op.await?;
    let new_version = table.version();
    
    Ok(Some(new_version))  // Return the version number
}
```

Then store this in Steward's transaction log or in the commit metadata itself.

### Option 2: Query by Delta Version Directly

Instead of showing "transactions", show "Delta versions":

```rust
async fn show_detailed_mode(store_path: &str) -> Result<String, steward::StewardError> {
    let table = deltalake::open_table(store_path).await?;
    let current_version = table.version().unwrap_or(0);
    
    // Iterate versions backwards
    for version in (0..=current_version).rev() {
        // Load the Delta log file for this version
        let log_path = format!("{}/_delta_log/{:020}.json", store_path, version);
        let commit_info = parse_delta_log(log_path)?;
        
        // Extract transaction metadata
        let tx_metadata = commit_info.get("pond_txn");
        
        // Read the parquet files added in this version
        let add_actions = extract_add_actions(&commit_info)?;
        let rows = read_parquet_files(&add_actions).await?;
        
        // Display this version
        format_version_output(version, tx_metadata, rows);
    }
}
```

### Option 3: SQL Query with Commit Metadata (Simplest)

Use DataFusion to query commit metadata alongside data:

```rust
async fn show_detailed_mode(store_path: &str) -> Result<String, steward::StewardError> {
    let mut persistence = tlogfs::OpLogPersistence::open(store_path).await?;
    let mut tx = persistence.begin().await?;
    let session_ctx = tx.session_context().await?;
    
    // Query all transactions with their metadata
    // This would require Delta Lake to expose commit metadata as queryable columns
    // Currently NOT supported by delta-rs
    
    let df = session_ctx.sql(
        "SELECT txn_id, timestamp, command, part_id, node_id, file_type 
         FROM delta_table_with_metadata 
         ORDER BY timestamp DESC"
    ).await?;
}
```

**Problem:** Delta Lake doesn't expose commit metadata as queryable columns.

## Recommendation

**Implement Option 2: Show Delta Versions Instead of Transactions**

Rationale:
1. Delta versions are the source of truth
2. Each version corresponds to AT MOST one write transaction
3. Read-only transactions are inherently invisible (they made no changes)
4. The current approach of reading specific parquet files is correct

Changes needed:
1. Remove `find_delta_log_version_by_timestamp()`
2. Calculate version from history position: `current_version - index`
3. Update output format to say "Delta Version X" instead of pretending to show every transaction
4. Accept that read-only transactions won't appear (they made no changes anyway)

## Example Fixed Output

```
╔══════════════════════════════════════════════════════════════╗
║                    DELTA LAKE HISTORY                        ║
╚══════════════════════════════════════════════════════════════╝

Version 97 (2025-10-07 02:56:59 UTC)
  Transaction: 0199bc99-ca5c-7a98
  Command: ["set_extended_attributes", "/hydrovu/devices/6582334615060480/SilverVulink1.series"]
  Changes:
    Partition 00000000 (/)
      └─ Updated: Dynamic Directory [d27b79d1] (1266 bytes)

Version 96 (2025-10-07 02:56:58 UTC)
  Transaction: 0199bc99-ca0f-72a7
  Command: ["mknod", "dynamic-dir", "/templates"]
  Changes:
    Partition 00000000 (/)
      └─ Created: Directory entry "templates" → [d27b79d1]

(Read-only transactions omitted - no versions created)
```

This accurately represents what Delta Lake tracks: versions and their changes.
