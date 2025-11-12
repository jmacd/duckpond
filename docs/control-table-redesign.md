# Control Table Redesign for Complete Operational Tracking

## Executive Summary

Redesign the control table to track ALL pond operations: read transactions, write transactions, and post-commit task execution. This provides complete operational audit trail, debugging visibility, and foundation for recovery mechanisms.

## Current State

**Existing Schema (`TransactionRecord`):**
```rust
pub struct TransactionRecord {
    pub txn_seq: i64,              // Transaction sequence number
    pub txn_id: String,            // Unique transaction ID
    pub based_on_seq: Option<i64>, // Parent sequence for reads
    pub record_type: String,       // "begin" | "data_committed" | "failed" | "completed"
    pub timestamp: i64,            // Microseconds since epoch
    pub transaction_type: String,  // "read" | "write"
    pub cli_args: Vec<String>,     // Command that triggered transaction
    pub environment: HashMap<String, String>, // Environment variables
    pub data_fs_version: Option<i64>, // Delta Lake version after commit
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,
}
```

**Current Lifecycle Tracking:**

Write Transaction:
1. `record_begin(txn_seq, txn_id, "write")` - Transaction starts
2. `record_data_committed(txn_seq, txn_id, data_fs_version)` - Delta Lake commit succeeds
3. OR `record_failed(txn_seq, txn_id, error)` - Transaction failed

Read Transaction:
1. `record_begin(txn_seq, txn_id, "read")` - Transaction starts
2. `record_completed(txn_seq, txn_id)` - Transaction finished successfully
3. OR `record_failed(txn_seq, txn_id, error)` - Transaction failed

**What's Missing:**
- ❌ Post-commit task tracking (pending, started, completed, failed)
- ❌ Parent/child relationship for post-commit tasks
- ❌ Factory name and config path for post-commit executions
- ❌ Execution sequence for ordering post-commit tasks

## Design Goals

1. **Complete Operational Audit Trail** - Every operation recorded from start to finish
2. **Unified Event Log** - Single table tracks all activity (reads, writes, post-commit)
3. **Recovery-Friendly** - Can determine what needs retry from control log alone
4. **Query-Friendly** - Easy to query "current state" vs "full history"
5. **Minimal Schema Changes** - Extend existing fields, don't break compatibility

## Proposed Schema Extension

### Option 1: Add Operation Type + Post-Commit Fields (RECOMMENDED)

Extend `TransactionRecord` with new fields for post-commit tracking:

```rust
pub struct TransactionRecord {
    // Existing fields - NO CHANGES
    pub txn_seq: i64,
    pub txn_id: String,
    pub based_on_seq: Option<i64>,
    pub record_type: String,
    pub timestamp: i64,
    pub transaction_type: String,
    pub cli_args: Vec<String>,
    pub environment: HashMap<String, String>,
    pub data_fs_version: Option<i64>,
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,
    
    // NEW: Post-commit task tracking
    pub parent_txn_seq: Option<i64>,    // Parent write transaction (for post-commit tasks)
    pub execution_seq: Option<i32>,     // Order within post-commit sequence (1, 2, 3...)
    pub factory_name: Option<String>,   // Factory that executed (for post-commit)
    pub config_path: Option<String>,    // Path to config (e.g., /etc/system.d/10-validate)
}
```

**Arrow Schema Changes:**
```rust
Field::new("parent_txn_seq", DataType::Int64, true),    // NULL for regular transactions
Field::new("execution_seq", DataType::Int32, true),     // NULL for regular transactions
Field::new("factory_name", DataType::Utf8, true),       // NULL for regular transactions
Field::new("config_path", DataType::Utf8, true),        // NULL for regular transactions
```

### Record Types

**Existing (unchanged):**
- `"begin"` - Transaction started
- `"data_committed"` - Write transaction committed to Delta Lake
- `"completed"` - Read transaction finished successfully
- `"failed"` - Transaction failed

**NEW for post-commit:**
- `"post_commit_pending"` - Post-commit task discovered, needs execution
- `"post_commit_started"` - Post-commit execution began
- `"post_commit_completed"` - Post-commit execution succeeded
- `"post_commit_failed"` - Post-commit execution failed

### Transaction Types

**Existing (unchanged):**
- `"read"` - Read-only transaction
- `"write"` - Write transaction

**NEW:**
- `"post_commit"` - Post-commit task execution (read-only but special context)

## Complete Lifecycle Examples

### Example 1: Write Transaction with Post-Commit Tasks

```
┌─────────────────────────────────────────────────────────────────────┐
│ Record 1: Write Transaction Begin                                   │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 42                                                         │
│ txn_id: "pond-write-2025-10-19-12-34-56-abc123"                    │
│ record_type: "begin"                                                │
│ transaction_type: "write"                                           │
│ cli_args: ["pond", "run", "/etc/hydrovu/collector"]               │
│ parent_txn_seq: NULL                                                │
│ execution_seq: NULL                                                 │
│ factory_name: NULL                                                  │
│ config_path: NULL                                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 2: Data Committed                                            │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 42                                                         │
│ txn_id: "pond-write-2025-10-19-12-34-56-abc123"                    │
│ record_type: "data_committed"                                       │
│ data_fs_version: 42                                                 │
│ duration_ms: 1234                                                   │
│ parent_txn_seq: NULL                                                │
│ execution_seq: NULL                                                 │
│ factory_name: NULL                                                  │
│ config_path: NULL                                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 3: Post-Commit Task Pending                                  │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 43                                 (NEW txn_seq!)         │
│ txn_id: "post-commit-42-1-validate"                                │
│ record_type: "post_commit_pending"                                  │
│ transaction_type: "post_commit"                                     │
│ parent_txn_seq: 42                          (Links to write txn)   │
│ execution_seq: 1                            (First post-commit)    │
│ factory_name: "validator"                                           │
│ config_path: "/etc/system.d/10-validate"                           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 4: Post-Commit Task Pending                                  │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 44                                 (Another txn_seq)      │
│ txn_id: "post-commit-42-2-notify"                                  │
│ record_type: "post_commit_pending"                                  │
│ transaction_type: "post_commit"                                     │
│ parent_txn_seq: 42                                                  │
│ execution_seq: 2                            (Second post-commit)   │
│ factory_name: "notifier"                                            │
│ config_path: "/etc/system.d/20-notify"                             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 5: Post-Commit Execution Started                             │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 43                                 (Same as pending)      │
│ txn_id: "post-commit-42-1-validate"                                │
│ record_type: "post_commit_started"                                  │
│ transaction_type: "post_commit"                                     │
│ parent_txn_seq: 42                                                  │
│ execution_seq: 1                                                    │
│ factory_name: "validator"                                           │
│ config_path: "/etc/system.d/10-validate"                           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 6: Post-Commit Completed                                     │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 43                                                         │
│ txn_id: "post-commit-42-1-validate"                                │
│ record_type: "post_commit_completed"                                │
│ transaction_type: "post_commit"                                     │
│ parent_txn_seq: 42                                                  │
│ execution_seq: 1                                                    │
│ factory_name: "validator"                                           │
│ config_path: "/etc/system.d/10-validate"                           │
│ duration_ms: 456                                                    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 7: Post-Commit Execution Started                             │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 44                                                         │
│ txn_id: "post-commit-42-2-notify"                                  │
│ record_type: "post_commit_started"                                  │
│ transaction_type: "post_commit"                                     │
│ parent_txn_seq: 42                                                  │
│ execution_seq: 2                                                    │
│ factory_name: "notifier"                                            │
│ config_path: "/etc/system.d/20-notify"                             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 8: Post-Commit Failed                                        │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 44                                                         │
│ txn_id: "post-commit-42-2-notify"                                  │
│ record_type: "post_commit_failed"                                   │
│ transaction_type: "post_commit"                                     │
│ parent_txn_seq: 42                                                  │
│ execution_seq: 2                                                    │
│ factory_name: "notifier"                                            │
│ config_path: "/etc/system.d/20-notify"                             │
│ error_message: "Failed to send webhook: Connection refused"        │
│ duration_ms: 123                                                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Example 2: Read Transaction

```
┌─────────────────────────────────────────────────────────────────────┐
│ Record 1: Read Transaction Begin                                    │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 45                                                         │
│ txn_id: "pond-read-2025-10-19-12-35-00-xyz789"                     │
│ record_type: "begin"                                                │
│ transaction_type: "read"                                            │
│ cli_args: ["pond", "ls", "/hydrovu"]                               │
│ parent_txn_seq: NULL                                                │
│ execution_seq: NULL                                                 │
│ factory_name: NULL                                                  │
│ config_path: NULL                                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Record 2: Read Transaction Completed                                │
├─────────────────────────────────────────────────────────────────────┤
│ txn_seq: 45                                                         │
│ txn_id: "pond-read-2025-10-19-12-35-00-xyz789"                     │
│ record_type: "completed"                                            │
│ transaction_type: "read"                                            │
│ duration_ms: 89                                                     │
│ parent_txn_seq: NULL                                                │
│ execution_seq: NULL                                                 │
│ factory_name: NULL                                                  │
│ config_path: NULL                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Post-Commit Tasks Identified by (parent_txn_seq, execution_seq)

**Rationale:**
- Post-commit tasks don't change the pond transaction sequence
- They're operations triggered BY a pond transaction, not pond transactions themselves
- Unique identifier: `(parent_txn_seq, execution_seq)` where execution_seq is ordinal (1, 2, 3...)
- Control table's txn_seq is for control table versioning ONLY, not pond sequence tracking

**Benefits:**
- Clear separation: pond sequences vs control sequences
- Can recreate control table from backup without affecting pond sequences
- Post-commit task identity is stable: txn 42's first post-commit is always (42, 1)
- Easy to query: "show all post-commit tasks for pond transaction 42"

**Example:**
- Pond write transaction 42 commits
- Discovers 2 post-commit factories
- Post-commit tasks are identified as: (42, 1) and (42, 2)
- Control table records these with its own internal txn_seq for versioning
- But queries use parent_txn_seq=42 to find related post-commit work

### 2. parent_txn_seq Links Post-Commit to Parent Write

**Rationale:**
- Post-commit tasks are triggered BY pond write transactions
- parent_txn_seq is the POND sequence number (immutable, survives control table recreation)
- Control table may have its own internal versioning, but that's separate
- Query by pond sequence, not control table sequence

**Query Example:**
```sql
-- Find all post-commit tasks for pond write transaction 42
SELECT * FROM transactions 
WHERE parent_txn_seq = 42 
  AND record_type LIKE 'post_commit%'
ORDER BY execution_seq;

-- Find failed post-commit tasks that need retry
SELECT * FROM transactions
WHERE parent_txn_seq = 42
  AND record_type = 'post_commit_failed';
```

**Recovery Scenario:**
- Control table backed up at pond txn 40
- Pond continues to txn 50
- Restore control table from backup
- Control table's internal sequences are from backup
- But parent_txn_seq values (40, 41, 42...) still match pond sequences
- Can replay/reconcile transactions 41-50 from pond Delta Lake

### 3. execution_seq Orders Post-Commit Tasks

**Rationale:**
- Lexicographic sort of config paths gives natural ordering (10-validate, 20-notify, 30-export)
- execution_seq makes ordering explicit: 1, 2, 3...
- Combined with parent_txn_seq: (42, 1), (42, 2), (42, 3) uniquely identifies each task
- Ordinal is stable even if config files are renamed

**Identity Example:**
- Pond transaction 42 discovers 3 post-commit configs
- `/etc/system.d/10-validate` → (42, 1)
- `/etc/system.d/20-notify` → (42, 2)
- `/etc/system.d/30-export` → (42, 3)
- These identifiers are stable and queryable

### 4. Lifecycle: pending → started → (completed | failed)

**Rationale:**
- `pending` records created BEFORE execution starts (commit point)
- If steward crashes, we can see which tasks never started
- `started` records when execution begins
- Final state is either `completed` or `failed`
- All records share same (parent_txn_seq, execution_seq) identifier

**Recovery Logic:**
```sql
-- Tasks that never started (steward crashed before execution)
-- Identified by parent_txn_seq + execution_seq
SELECT parent_txn_seq, execution_seq, factory_name, config_path
FROM transactions
WHERE record_type = 'post_commit_pending'
  AND NOT EXISTS (
      SELECT 1 FROM transactions t2
      WHERE t2.parent_txn_seq = transactions.parent_txn_seq
        AND t2.execution_seq = transactions.execution_seq
        AND t2.record_type IN ('post_commit_started', 'post_commit_completed', 'post_commit_failed')
  );

-- Tasks that started but didn't finish (crashed during execution)
SELECT parent_txn_seq, execution_seq, factory_name, config_path
FROM transactions
WHERE record_type = 'post_commit_started'
  AND NOT EXISTS (
      SELECT 1 FROM transactions t2
      WHERE t2.parent_txn_seq = transactions.parent_txn_seq
        AND t2.execution_seq = transactions.execution_seq
        AND t2.record_type IN ('post_commit_completed', 'post_commit_failed')
  );
```

### 5. Control Table txn_seq is Separate from Pond txn_seq

**Rationale:**
- Control table has its own txn_seq for Delta Lake versioning (append-only log)
- Control table txn_seq is NOT the pond transaction sequence
- Pond transaction sequence comes from pond's Delta Lake version
- Control table can be recreated from backup without affecting pond sequences

**Separation of Concerns:**
```
Pond Transaction Sequence:
- Stored in pond Delta Lake table versions
- Immutable, permanent
- txn_seq = pond version number
- Example: Write transaction 42 → pond version 42

Control Table Transaction Sequence:
- Internal to control table Delta Lake
- Used for append-only log versioning
- NOT exposed to users
- Can be reset/recreated from backup
- Records reference pond txn_seq via parent_txn_seq field
```

**Why This Matters:**
- Control table is operational metadata, pond is source of truth
- Can backup/restore/recreate control table independently
- Post-commit task identity `(parent_txn_seq, execution_seq)` remains stable
- `parent_txn_seq` always refers to pond sequence, not control sequence

## Implementation Changes

### Control Table Schema - Clarified Design

The control table records ALL operations as an append-only event log. The `txn_seq` field in the control table is for its own Delta Lake versioning and is NOT the same as pond transaction sequences.

**Key Fields:**
```rust
pub struct TransactionRecord {
    // Control table's own sequence (for Delta Lake versioning)
    // This is internal, NOT exposed to users
    pub txn_seq: i64,
    
    // Pond transaction tracking
    pub txn_id: String,              // Unique transaction ID
    pub based_on_seq: Option<i64>,   // Pond sequence this read is based on
    pub data_fs_version: Option<i64>, // Pond version after write commits
    
    // Post-commit task tracking
    pub parent_txn_seq: Option<i64>,  // POND sequence that triggered this task
    pub execution_seq: Option<i32>,   // Ordinal in post-commit list (1, 2, 3...)
    
    // Rest of existing fields...
    pub record_type: String,
    pub transaction_type: String,
    // ...
}
```

**Important Distinction:**
- `txn_seq` = Control table's internal sequence (can be reset)
- `parent_txn_seq` = Pond transaction sequence (immutable, permanent)
- `execution_seq` = Ordinal in post-commit factory list
- Post-commit tasks identified by `(parent_txn_seq, execution_seq)`

### 1. ControlTable Methods

**NEW Methods:**

```rust
impl ControlTable {
    /// Record pending post-commit task (before execution)
    /// parent_txn_seq is the POND transaction sequence
    /// execution_seq is the ordinal in the factory list (1, 2, 3...)
    pub async fn record_post_commit_pending(
        &mut self,
        parent_txn_seq: i64,      // Pond sequence, not control sequence
        execution_seq: i32,        // Ordinal: 1, 2, 3...
        factory_name: String,
        config_path: String,
    ) -> Result<(), StewardError>;
    
    /// Record post-commit execution start
    /// Identified by (parent_txn_seq, execution_seq)
    pub async fn record_post_commit_started(
        &mut self,
        parent_txn_seq: i64,
        execution_seq: i32,
    ) -> Result<(), StewardError>;
    
    /// Record post-commit execution success
    pub async fn record_post_commit_completed(
        &mut self,
        parent_txn_seq: i64,
        execution_seq: i32,
        duration_ms: i64,
    ) -> Result<(), StewardError>;
    
    /// Record post-commit execution failure
    pub async fn record_post_commit_failed(
        &mut self,
        parent_txn_seq: i64,
        execution_seq: i32,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError>;
    
    /// Query post-commit tasks for a pond transaction
    pub async fn get_post_commit_tasks(
        &self,
        parent_txn_seq: i64,      // Pond sequence
    ) -> Result<Vec<PostCommitTask>, StewardError>;
    
    /// Find incomplete post-commit tasks for recovery
    pub async fn find_incomplete_post_commit_tasks(
        &self,
    ) -> Result<Vec<PostCommitTask>, StewardError>;
}

#[derive(Debug, Clone)]
pub struct PostCommitTask {
    pub parent_txn_seq: i64,      // Pond transaction sequence
    pub execution_seq: i32,        // Ordinal in factory list
    pub factory_name: String,
    pub config_path: String,
    pub state: String,             // "pending" | "started" | "completed" | "failed"
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,
}
```

### 2. StewardTransactionGuard Changes

**In `commit()` method:**

```rust
pub async fn commit(mut self) -> Result<Option<()>, StewardError> {
    // ... existing commit logic ...
    
    // After data_committed record, discover post-commit tasks
    let post_commit_tasks = self.discover_post_commit_factories().await?;
    
    // Record all tasks as pending (using pond txn_seq as parent)
    for (execution_seq, (factory_name, config_path, config_bytes)) in post_commit_tasks.iter().enumerate() {
        let execution_seq = (execution_seq + 1) as i32;  // 1-indexed ordinal
        
        // COMMIT POINT: Record pending task
        // parent_txn_seq = self.txn_seq (the POND sequence)
        self.control_table.record_post_commit_pending(
            self.txn_seq,              // Pond transaction sequence
            execution_seq,             // Ordinal: 1, 2, 3...
            factory_name.clone(),
            config_path.clone(),
        ).await?;
    }
    
    // Now execute each task
    for (execution_seq, (factory_name, config_path, config_bytes)) in post_commit_tasks.iter().enumerate() {
        let execution_seq = (execution_seq + 1) as i32;
        
        // Record started
        self.control_table.record_post_commit_started(
            self.txn_seq,              // Pond sequence
            execution_seq,
        ).await?;
        
        let start_time = Instant::now();
        
        // Execute factory
        let result = self.execute_post_commit_factory(...).await;
        
        let duration_ms = start_time.elapsed().as_millis() as i64;
        
        match result {
            Ok(()) => {
                self.control_table.record_post_commit_completed(
                    self.txn_seq,
                    execution_seq,
                    duration_ms,
                ).await?;
            }
            Err(e) => {
                self.control_table.record_post_commit_failed(
                    self.txn_seq,
                    execution_seq,
                    e.to_string(),
                    duration_ms,
                ).await?;
                // Continue to next task (don't halt on failure)
            }
        }
    }
    
    Ok(write_occurred)
}
```

**Track Read Transactions:**

```rust
pub async fn commit(mut self) -> Result<Option<()>, StewardError> {
    let duration = self.start_time.elapsed().as_millis() as i64;
    
    if let Some(()) = write_occurred {
        // ... write transaction commit logic ...
    } else {
        // Read transaction - record completion
        self.control_table.record_completed(
            self.txn_seq,
            self.txn_id,
            duration,
        ).await?;
    }
    
    Ok(write_occurred)
}
```

## CLI: pond control Command

### Basic Mode (Default)

Shows current transaction state summary:

```bash
$ pond control
DuckPond Control Table Summary

Last Write Transaction: 42
  Status: Committed
  Version: 42
  Command: pond run /etc/hydrovu/collector
  Duration: 1.234s
  
Post-Commit Tasks: 2 tasks
  ✓ 1: validator (/etc/system.d/10-validate) - completed in 456ms
  ✗ 2: notifier (/etc/system.d/20-notify) - failed: Connection refused

Recent Read Transactions: 3
  45: pond ls /hydrovu - completed in 89ms
  46: pond cat /etc/hydrovu/config - completed in 45ms
  47: pond sql "SELECT * FROM locations" - completed in 234ms

Incomplete Operations: 0
```

### Detailed Mode

Shows full operation log:

```bash
$ pond control --mode detailed
DuckPond Control Table - Full Operation Log

[2025-10-19 12:34:56.123] txn_seq=42 BEGIN write
  Command: pond run /etc/hydrovu/collector
  Transaction ID: pond-write-2025-10-19-12-34-56-abc123

[2025-10-19 12:34:57.357] txn_seq=42 DATA_COMMITTED
  Version: 42
  Duration: 1.234s

[2025-10-19 12:34:57.360] txn_seq=43 POST_COMMIT_PENDING
  Parent: 42, Execution: 1
  Factory: validator
  Config: /etc/system.d/10-validate

[2025-10-19 12:34:57.361] txn_seq=44 POST_COMMIT_PENDING
  Parent: 42, Execution: 2
  Factory: notifier
  Config: /etc/system.d/20-notify

[2025-10-19 12:34:57.362] txn_seq=43 POST_COMMIT_STARTED
  Factory: validator

[2025-10-19 12:34:57.818] txn_seq=43 POST_COMMIT_COMPLETED
  Duration: 456ms

[2025-10-19 12:34:57.819] txn_seq=44 POST_COMMIT_STARTED
  Factory: notifier

[2025-10-19 12:34:57.942] txn_seq=44 POST_COMMIT_FAILED
  Error: Failed to send webhook: Connection refused
  Duration: 123ms

[2025-10-19 12:35:00.123] txn_seq=45 BEGIN read
  Command: pond ls /hydrovu

[2025-10-19 12:35:00.212] txn_seq=45 COMPLETED
  Duration: 89ms

...
```

### Recovery Mode

Shows incomplete operations that need attention:

```bash
$ pond control --incomplete
DuckPond Incomplete Operations

Incomplete Write Transactions: 0

Incomplete Post-Commit Tasks: 1
  txn_seq=44 (parent=42) - notifier (/etc/system.d/20-notify)
    Status: FAILED
    Error: Failed to send webhook: Connection refused
    Action: Use 'pond control retry 44' to retry this task

Incomplete Read Transactions: 0
```

## Benefits

### 1. Complete Visibility

- Every operation recorded: reads, writes, post-commit tasks
- Can debug "what happened" from control table alone
- Audit trail for compliance, debugging, monitoring

### 2. Recovery-Friendly

- Clear state for each post-commit task
- Can identify tasks that never started vs crashed during execution
- Can retry failed tasks without re-running successful ones

### 3. Query-Friendly

- Simple SQL queries for common questions:
  - "What post-commit tasks ran for transaction N?"
  - "Which post-commit tasks failed?"
  - "What's the current pond state?"
  - "Show me all operations in the last hour"

### 4. Minimal Breaking Changes

- Extends existing schema with optional fields
- Existing code continues to work (new fields are NULL)
- Migration: existing records have NULL for new fields

## Migration Strategy

### Phase 1: Add New Fields to Schema

- Add 4 new fields to `TransactionRecord` struct
- Add 4 new fields to Arrow schema (all nullable)
- Existing records will have NULL for these fields
- ✅ Backward compatible

### Phase 2: Implement New Methods

- Add `record_post_commit_*` methods
- Add query methods for post-commit tasks
- Add recovery queries

### Phase 3: Update Transaction Lifecycle

- Update `commit()` to record post-commit pending/started/completed/failed
- Update `commit()` to record read transaction completion
- Test with existing code (should work transparently)

### Phase 4: Add CLI Command

- Implement `pond control` command
- Basic mode: summary
- Detailed mode: full log
- Recovery mode: incomplete operations

### Phase 5: Add Recovery Logic

- Implement `pond control retry <txn_seq>` command
- Re-execute failed post-commit tasks
- Update control table records

## Summary

**Core Changes:**
1. ✅ Extend schema with 4 new optional fields (backward compatible)
2. ✅ Add record_type values: `post_commit_pending`, `post_commit_started`, `post_commit_completed`, `post_commit_failed`
3. ✅ Add transaction_type value: `post_commit`
4. ✅ Each post-commit task gets unique txn_seq
5. ✅ parent_txn_seq links tasks to parent write transaction
6. ✅ execution_seq orders tasks within parent
7. ✅ Lifecycle: pending → started → (completed | failed)

**Benefits:**
- Complete operational audit trail
- Recovery-friendly design
- Query-friendly for debugging
- Minimal breaking changes
- Foundation for retry/recovery mechanisms

**Next Steps:**
1. Review this design document
2. Implement Phase 1 (schema extension)
3. Implement Phase 2 (new methods)
4. Update guard.rs for complete lifecycle tracking
5. Implement `pond control` command
6. Add comprehensive tests
