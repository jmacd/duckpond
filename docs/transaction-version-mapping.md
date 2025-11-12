# Transaction Sequence Numbers and Version Management# Transaction Sequence Numbers and Version Management



**Last Updated**: January 13, 2025  **Last Updated**: October 13, 2025  

**Status**: Implemented and tested**Status**: Implemented and tested



## Overview## Overview



DuckPond uses **transaction sequence numbers (txn_seq)** to provide deterministic, ordered transaction tracking across the system. This approach replaces the previous problematic method of correlating transactions with Delta Lake versions via timestamps.DuckPond now uses **transaction sequence numbers (txn_seq)** to provide deterministic, ordered transaction tracking across the system. This replaces the previous problematic approach of trying to correlate transactions with Delta Lake versions via timestamps.



## Architecture## Architecture



### Transaction Sequence Allocation### Transaction Sequence Allocation



**Steward is the source of truth for transaction sequencing.****Steward is the source of truth for transaction sequencing.**



From `crates/steward/src/ship.rs`:From `crates/steward/src/ship.rs`:



```rust```rust

pub struct Ship {pub struct Ship {

    data_persistence: OpLogPersistence,    data_persistence: OpLogPersistence,

    control_table: ControlTable,    control_table: ControlTable,

    last_write_seq: Arc<AtomicI64>,  // ← Atomic sequence counter    last_write_seq: Arc<AtomicI64>,  // ← Atomic sequence counter

    pond_path: String,    pond_path: String,

}}

``````



**Sequence Allocation:****Sequence Allocation:**

1. **Root initialization**: Uses `txn_seq=1` during pond creation1. **Root initialization**: Uses `txn_seq=1` during pond creation

2. **User transactions**: Atomic increment starting from 22. **User transactions**: Atomic increment starting from 2

   ```rust   ```rust

   let txn_seq = self.last_write_seq.fetch_add(1, Ordering::SeqCst) + 1;   let txn_seq = self.last_write_seq.fetch_add(1, Ordering::SeqCst) + 1;

   ```   ```



### Transaction Flow with Sequences### Transaction Flow with Sequences



1. **Transaction Begin**: 1. **Transaction Begin**: 

   - Steward allocates `txn_seq` (for writes) or reuses last sequence (for reads)   - Steward allocates `txn_seq` (for writes) or reuses last sequence (for reads)

   - Passes `txn_seq` to `OpLogPersistence::begin(txn_seq)`   - Passes `txn_seq` to `OpLogPersistence::begin(txn_seq)`

   - Records begin in control table   - Records begin in control table



2. **Operations Accumulate**: 2. **Operations Accumulate**: 

   - Each `OplogEntry` is tagged with the transaction's `txn_seq`   - Each `OplogEntry` is tagged with the transaction's `txn_seq`

   - File writes, directory modifications accumulate in memory   - File writes, directory modifications accumulate in memory



3. **Transaction Commit**:3. **Transaction Commit**:

   - All OpLog records written with same `txn_seq`   - All OpLog records written with same `txn_seq`

   - Creates one Delta Lake version (version numbers still increment independently)   - Creates one Delta Lake version (version numbers still increment independently)

   - Control table records completion with both `txn_seq` and `data_fs_version`   - Control table records completion with both `txn_seq` and `data_fs_version`



### Control Table Schema### Control Table Schema



The Steward control table tracks all transactions:The Steward control table tracks all transactions:



```rust```rust

Schema {Schema {

    txn_seq: Int64,              // Sequential: 1, 2, 3... (business transaction order)    txn_seq: Int64,              // Sequential: 1, 2, 3... (business transaction order)

    txn_id: Utf8,                // UUID for correlation    txn_id: Utf8,                // UUID for correlation

    based_on_seq: Int64?,        // For reads: references last write seq    based_on_seq: Int64?,        // For reads: references last write seq

    record_type: Utf8,           // "begin" | "data_committed" | "failed" | "completed"    record_type: Utf8,           // "begin" | "data_committed" | "failed" | "completed"

    timestamp: Timestamp(Nanosecond, None),    timestamp: Timestamp(Nanosecond, None),

    transaction_type: Utf8,      // "read" | "write"    transaction_type: Utf8,      // "read" | "write"

    cli_args: List<Utf8>,        // Command-line arguments    cli_args: List<Utf8>,        // Command-line arguments

    environment: Map<Utf8, Utf8>,    environment: Map<Utf8, Utf8>,

    data_fs_version: Int64?,     // DeltaLake version (links txn_seq to storage)    data_fs_version: Int64?,     // DeltaLake version (links txn_seq to storage)

}}

``````



**Key relationships:****Key relationships:**

- `txn_seq`: Monotonically increasing, never reused- `txn_seq`: Monotonically increasing, never reused

- `data_fs_version`: Delta Lake version created by this transaction (null for reads)- `data_fs_version`: Delta Lake version created by this transaction (null for reads)

- One `txn_seq` → Zero or One `data_fs_version`- One `txn_seq` → Zero or One `data_fs_version`



### OpLog Records with txn_seq### OpLog Records with txn_seq



Every OpLog record now includes the transaction sequence:Every OpLog record now includes the transaction sequence:



```rust```rust

pub struct OplogEntry {pub struct OplogEntry {

    part_id: String,    part_id: String,

    node_id: String,    node_id: String,

    file_type: EntryType,    file_type: EntryType,

    timestamp: i64,    timestamp: i64,

    version: i64,        // Per-node version counter (1, 2, 3...)    version: i64,        // Per-node version counter (1, 2, 3...)

    txn_seq: Option<i64>, // Transaction sequence number ← KEY FIELD    txn_seq: Option<i64>, // Transaction sequence number ← NEW

    content: Option<Vec<u8>>,    content: Option<Vec<u8>>,

    // ... other fields    // ... other fields

}}

```

```rust

**Important distinctions:**async fn commit_impl(

- `txn_seq`: Business transaction order (what users care about)    &mut self,

- `version`: Per-node versioning within each transaction (implementation detail)    metadata: Option<HashMap<String, serde_json::Value>>,

- `data_fs_version`: Delta Lake storage versions (backend detail)    table: DeltaTable,

) -> Result<Option<()>, TLogFSError> {

## Root Directory Initialization    self.flush_directory_operations().await?;

    let records = std::mem::take(&mut self.records);

The root directory is special-cased during pond creation:

    if records.is_empty() {

From `crates/steward/src/ship.rs` (`create_pond` function):        info!("Committing read-only transaction (no write operations)");

        return Ok(None);  // ← NO version created for read-only transactions

```rust    }

// Create root directory with txn_seq=1

let root_seq = ship.last_write_seq.fetch_add(1, Ordering::SeqCst) + 1; // Gets 1    let count = records.len();

tlogfs.create_dir(root_id.clone()).await?;    info!("Committing {count} operations in {}", self.path);



// Record in control table    // Convert records to RecordBatch

ship.control_table.record_begin(    let batches = vec![

    root_seq,        serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &records)?,

    None,  // No based_on_seq for initialization    ];

    txn_id.clone(),

    "write",    let mut write_op = DeltaOps(table).write(batches);

    cli_args,

    env    // Add commit metadata (transaction ID, command args)

).await?;    if let Some(metadata_map) = metadata {

        let properties = CommitProperties::default()

// ... commit TLogFS changes (creates DeltaLake version) ...            .with_metadata(metadata_map.into_iter());

        write_op = write_op.with_commit_properties(properties);

ship.control_table.record_data_committed(    }

    root_seq,

    txn_id,    _ = write_op.await?;  // ← Creates new Delta Lake version here

    0,  // data_fs_version=0 for root initialization    

    0   // total_bytes    self.records.clear();

).await?;    self.operations.clear();

```    Ok(Some(()))

}

**Result**: Root directory gets `txn_seq=1`, first user transaction gets `txn_seq=2````



## Show Command Implementation**Key Points:**

1. **One transaction → Zero or One Delta version**

The `pond show` command now queries transactions by `txn_seq`:   - Read-only transactions create NO version

   - Write transactions create EXACTLY ONE version

From `crates/cmd/src/commands/show.rs`:

2. **Transaction metadata stored in Delta commit**

```rust   - `pond_txn` field contains: `{txn_id: "UUID", args: "[\"command\", \"args\"]"}`

// Query all distinct transaction sequences in descending order   - Stored in `_delta_log/NNNNNNNNNNNNNNNNNNNN.json` commit metadata

let df = ctx.sql(   - NOT stored in the data rows themselves

    "SELECT DISTINCT txn_seq FROM oplog ORDER BY txn_seq DESC"

).await?;3. **Multiple records per transaction**

   - One transaction can write many `OplogEntry` records

// For each transaction sequence, get all operations   - All records from one transaction go into ONE Delta version

for txn_seq in sequences {   - Records are partitioned by `part_id` into separate parquet files

    let ops_df = ctx.sql(&format!(

        "SELECT node_id, part_id, version, file_type, timestamp ## The Problem with Current Detailed Mode

         FROM oplog 

         WHERE txn_seq = {} ### What the Code Tries to Do

         ORDER BY timestamp, node_id",

        txn_seqFrom `crates/cmd/src/commands/show.rs`:

    )).await?;

    ```rust

    // Query control table for command infoasync fn show_detailed_mode(

    let control_df = control_ctx.sql(&format!(    commit_history: &[deltalake::kernel::CommitInfo],

        "SELECT cli_args, timestamp, data_fs_version     store_path: &str

         FROM control_table ) -> Result<String, steward::StewardError> {

         WHERE txn_seq = {} AND record_type = 'data_committed'",    for commit_info in commit_history.iter() {

        txn_seq        // Problem 1: Tries to find version by timestamp matching

    )).await?;        match find_delta_log_version_by_timestamp(store_path, timestamp).await {

                Ok(Some(version)) => {

    // Display grouped by transaction                // Problem 2: Multiple transactions can have same version

    println!("╔══════════════════════════════════════════════╗");                // if read-only transactions are interleaved

    println!("║ Transaction Sequence: {:<23}║", txn_seq);            }

    println!("╚══════════════════════════════════════════════╝");        }

        }

    // Show command and operations}

    display_transaction_details(txn_seq, ops, control_info);```

}

```### Problems



**Key features:**#### Problem 1: Incorrect Version Lookup

- Queries OpLog directly by `txn_seq` (no timestamp matching)

- Groups all operations by transaction sequenceThe code uses `find_delta_log_version_by_timestamp()` to correlate transactions with versions. This is **fundamentally broken** because:

- Shows command arguments from control table

- Resolves paths using TLogFS path resolution- Read-only transactions don't create versions

- Orders transactions from newest to oldest- Multiple write transactions at the same timestamp (batched operations) 

- Timestamp precision issues (microseconds vs milliseconds)

## Read-Only Transactions

**Example:**

**Important**: Read-only transactions don't create OpLog records:```

Transaction A (write)  @ t=1000 → creates version 1

From `crates/tlogfs/src/persistence.rs`:Transaction B (read)   @ t=1001 → NO version created

Transaction C (write)  @ t=1002 → creates version 2

```rustTransaction D (write)  @ t=1003 → creates version 3

async fn commit_impl(&mut self) -> Result<Option<()>, TLogFSError> {```

    self.flush_directory_operations().await?;

    let records = std::mem::take(&mut self.records);When `show --mode detailed` runs:

- Looks up transaction A by timestamp → finds version 1 ✓

    if records.is_empty() {- Looks up transaction B by timestamp → finds version 1 (wrong!) ✗

        info!("Committing read-only transaction (no write operations)");- Looks up transaction C by timestamp → might find version 2 or 3 depending on timing ✗

        return Ok(None);  // ← NO OpLog records, NO Delta version

    }#### Problem 2: Version Calculation from History

    

    // ... write OpLog records ...Delta Lake's `table.history(limit)` returns commits in **reverse chronological order**:

}

``````rust

let history = table.history(None).await?;

**Result**: Read-only transactions:// Returns: [version N, version N-1, ..., version 1, version 0]

- Are recorded in control table (with `based_on_seq` referencing last write)```

- Do NOT appear in OpLog

- Do NOT appear in `pond show` output (nothing to show)The CORRECT way to calculate version numbers:

- Do NOT create Delta Lake versions

```rust

## Per-Node Version Managementlet current_version = table.version().unwrap_or(0);

for (index, commit_info) in history.iter().enumerate() {

Within each transaction, nodes track their own version sequence:    let version = current_version - (index as i64);

    // Now we know this commit created `version`

From `crates/tlogfs/src/persistence.rs`:}

```

```rust

async fn get_next_version_for_node(But this STILL doesn't solve the read-only transaction problem!

    &self,

    node_id: &str,#### Problem 3: Missing Transactions

    part_id: &str,

) -> Result<i64, TLogFSError> {The output shows confusing results like:

    let sql = format!(

        "SELECT MAX(version) as max_version FROM oplog ```

         WHERE node_id = '{}' AND part_id = '{}'",=== Transaction 0199bc99-ca5c-7a98 (Command: ["set_extended_attributes", ...]) ===

        node_id, part_id  Delta Lake Version: 97

    );

    === Transaction 0199bc99-ca0f-72a7 (Command: ["mknod", "dynamic-dir", "/templates"]) ===

    let max_version = execute_query(&sql).await?;  Delta Lake Version: 97

    Ok(max_version + 1)  // Increment for next version

}=== Transaction 0199bc99-c9bb-7f24 (Command: ["mknod", "dynamic-dir", "/reduced"]) ===

```  Delta Lake Version: 97

```

**Version allocation:**

- **New nodes**: Start at v1**What actually happened:**

- **Updated nodes**: Increment from last version (v2, v3, etc.)- These might be 3 separate transactions where only ONE was a write

- **Directory optimization**: Single version per node per transaction- Or they might be showing the SAME parquet files multiple times

  - Previous: v0 (empty) + v1 (with content) = wasteful- The timestamp matching can't distinguish them

  - Current: v1 (with content) = efficient

## The Correct Solution

**Example transaction creating `/a/b/c`:**

```### Option 1: Track Transaction-to-Version Mapping (Preferred)

txn_seq=5:

  node_id=00000000 (root)     part_id=00000000  version=2  (update)Store the relationship in transaction metadata:

  node_id=12345678 (/a)       part_id=12345678  version=1  (new)

  node_id=23456789 (/a/b)     part_id=23456789  version=1  (new)```rust

  node_id=34567890 (/a/b/c)   part_id=34567890  version=1  (new)async fn commit_impl(...) -> Result<Option<i64>, TLogFSError> {

```    // ... existing code ...

    

## Testing Infrastructure    if records.is_empty() {

        return Ok(None);  // Read-only, returns None

Public API for testing transaction sequences:    }

    

From `crates/steward/src/ship.rs`:    let table = write_op.await?;

    let new_version = table.version();

```rust    

pub async fn query_oplog_records(    Ok(Some(new_version))  // Return the version number

    &self,}

    txn_seq: i64,```

) -> Result<Vec<(String, String, i64)>, StewardError> {

    self.data_persistenceThen store this in Steward's transaction log or in the commit metadata itself.

        .query_oplog_by_txn_seq(txn_seq)

        .await### Option 2: Query by Delta Version Directly

        .map_err(|e| StewardError::Message(e.to_string()))

}Instead of showing "transactions", show "Delta versions":

```

```rust

From `crates/tlogfs/src/persistence.rs`:async fn show_detailed_mode(store_path: &str) -> Result<String, steward::StewardError> {

    let table = deltalake::open_table(store_path).await?;

```rust    let current_version = table.version().unwrap_or(0);

pub async fn query_oplog_by_txn_seq(    

    &self,    // Iterate versions backwards

    txn_seq: i64,    for version in (0..=current_version).rev() {

) -> Result<Vec<(String, String, i64)>, TLogFSError> {        // Load the Delta log file for this version

    let sql = format!(        let log_path = format!("{}/_delta_log/{:020}.json", store_path, version);

        "SELECT node_id, part_id, version FROM oplog         let commit_info = parse_delta_log(log_path)?;

         WHERE txn_seq = {}         

         ORDER BY node_id, version",        // Extract transaction metadata

        txn_seq        let tx_metadata = commit_info.get("pond_txn");

    );        

            // Read the parquet files added in this version

    // Execute query, handle DataFusion dictionary-encoded strings        let add_actions = extract_add_actions(&commit_info)?;

    // Returns: Vec<(node_id, part_id, version)>        let rows = read_parquet_files(&add_actions).await?;

}        

```        // Display this version

        format_version_output(version, tx_metadata, rows);

**Use case**: Tests verify version numbering per transaction:    }

}

```rust```

#[tokio::test]

async fn test_directory_tree_single_version_per_node() {### Option 3: SQL Query with Commit Metadata (Simplest)

    let ship = Ship::create_pond(...).await?;

    Use DataFusion to query commit metadata alongside data:

    // Create directory tree /a/b/c

    let txn_seq = ship.begin_write("mkdir").await?;```rust

    // ... operations ...async fn show_detailed_mode(store_path: &str) -> Result<String, steward::StewardError> {

    ship.commit().await?;    let mut persistence = tlogfs::OpLogPersistence::open(store_path).await?;

        let mut tx = persistence.begin().await?;

    // Query OpLog records for this transaction    let session_ctx = tx.session_context().await?;

    let records = ship.query_oplog_records(txn_seq).await?;    

        // Query all transactions with their metadata

    // Verify: one version per node    // This would require Delta Lake to expose commit metadata as queryable columns

    let mut node_versions: HashMap<String, Vec<i64>> = HashMap::new();    // Currently NOT supported by delta-rs

    for (node_id, _, version) in records {    

        node_versions.entry(node_id).or_default().push(version);    let df = session_ctx.sql(

    }        "SELECT txn_id, timestamp, command, part_id, node_id, file_type 

             FROM delta_table_with_metadata 

    for (node_id, versions) in node_versions {         ORDER BY timestamp DESC"

        assert_eq!(versions.len(), 1,     ).await?;

            "Node {} should have exactly 1 version, got: {:?}", }

            node_id, versions);```

    }

}**Problem:** Delta Lake doesn't expose commit metadata as queryable columns.

```

## Recommendation

## Example Output

**Implement Option 2: Show Delta Versions Instead of Transactions**

Actual output from `pond show`:

Rationale:

```1. Delta versions are the source of truth

╔══════════════════════════════════════════════╗2. Each version corresponds to AT MOST one write transaction

║ Transaction Sequence: 5                      ║3. Read-only transactions are inherently invisible (they made no changes)

╚══════════════════════════════════════════════╝4. The current approach of reading specific parquet files is correct



Command: pond mkdir -p /a/b/cChanges needed:

Time: 2025-01-13 15:23:45 UTC1. Remove `find_delta_log_version_by_timestamp()`

Delta Version: 42. Calculate version from history position: `current_version - index`

3. Update output format to say "Delta Version X" instead of pretending to show every transaction

Operations:4. Accept that read-only transactions won't appear (they made no changes anyway)

  00000000 (/) → v2

    └─ Updated directory with new entry "a"## Example Fixed Output

  

  12345678 (/a) → v1```

    └─ Created directory╔══════════════════════════════════════════════════════════════╗

  ║                    DELTA LAKE HISTORY                        ║

  23456789 (/a/b) → v1╚══════════════════════════════════════════════════════════════╝

    └─ Created directory

  Version 97 (2025-10-07 02:56:59 UTC)

  34567890 (/a/b/c) → v1  Transaction: 0199bc99-ca5c-7a98

    └─ Created directory  Command: ["set_extended_attributes", "/hydrovu/devices/6582334615060480/SilverVulink1.series"]

  Changes:

╔══════════════════════════════════════════════╗    Partition 00000000 (/)

║ Transaction Sequence: 4                      ║      └─ Updated: Dynamic Directory [d27b79d1] (1266 bytes)

╚══════════════════════════════════════════════╝

Version 96 (2025-10-07 02:56:58 UTC)

Command: pond touch /test.txt  Transaction: 0199bc99-ca0f-72a7

Time: 2025-01-13 15:20:12 UTC  Command: ["mknod", "dynamic-dir", "/templates"]

Delta Version: 3  Changes:

    Partition 00000000 (/)

Operations:      └─ Created: Directory entry "templates" → [d27b79d1]

  00000000 (/) → v1 (initial)

    └─ Updated directory with new entry "test.txt"(Read-only transactions omitted - no versions created)

  ```

  45678901 (/test.txt) → v1

    └─ Created file (0 bytes)This accurately represents what Delta Lake tracks: versions and their changes.

```

**Key observations:**
- Transactions ordered by `txn_seq` (descending)
- Each transaction shows one set of operations
- Read-only transactions don't appear (no OpLog records)
- Version numbers reflect per-node state
- Commands extracted from control table

## Benefits of Transaction Sequence Approach

1. **Deterministic ordering**: txn_seq provides absolute ordering independent of timestamps
2. **No timestamp ambiguity**: Multiple transactions in same millisecond are distinguishable  
3. **Direct queries**: `WHERE txn_seq = ?` is simpler than timestamp matching
4. **Control table integration**: Links business transactions to storage versions
5. **Testing support**: Easy to query specific transactions for verification
6. **Read-only handling**: Naturally excluded from OpLog (nothing to show)

## Future Enhancements

Potential improvements:

1. **Transaction metadata in OpLog**: Store command args directly in OpLog records (currently in control table only)
2. **Diff mode**: Show what changed between transactions using version deltas
3. **Filter by path**: `pond show /a/b` to see only operations affecting that subtree
4. **Time-based queries**: `pond show --since "1 hour ago"` using txn_seq ranges
5. **Transaction rollback**: Use txn_seq to identify and rollback specific transactions
6. **Audit trail**: Query control table for complete transaction history including reads
