# Steward Control Table Migration Plan

**Date**: October 12, 2025  
**Status**: Planning Phase  
**Goal**: Replace control filesystem (TLogFS instance) with direct DeltaLake table for transaction tracking

## Executive Summary

Currently, steward uses a full TLogFS instance at `pond/control/` just to store transaction metadata JSON files. This is architectural overkill. We will replace it with a single DeltaLake table that:

1. Tracks transaction lifecycle (begin → commit/rollback)
2. Records both read and write transactions
3. Manages sequential transaction IDs (txn_seq)
4. Enables future replication coordination
5. Provides clear transaction ordering for "show" command

## Core Design Principles

### 1. No Code Duplication (#file:anti-duplication.md)
- **CRITICAL**: We will NOT create `begin_with_sequence()` as a separate method
- We will modify the existing `begin()` signature to accept `Option<i64>`
- Single code path for all transaction begins

### 2. Transaction Sequencing
- **Steward owns transaction sequence numbers** (txn_seq: 1, 2, 3...)
- **Read transactions**: Reuse last committed write sequence (txn_seq=13, based_on_seq=13)
- **Write transactions**: Allocate next sequence (txn_seq=14, based_on_seq=NULL)
- **Pass to TLogFS**: Write transactions get sequence for future OpLog integration

### 3. Abstraction Maintained
- Rest of system uses `StewardTransactionGuard` as before
- Steward intercepts begin/commit - implementation details hidden
- No visible changes to cmd crate or application code

## Architecture Changes

### Before (Current):
```
Ship {
    data_persistence: OpLogPersistence,     // TLogFS for data
    control_persistence: OpLogPersistence,  // TLogFS for metadata (overkill!)
}

OpLogPersistence::begin() -> TransactionGuard
```

### After (Target):
```
Ship {
    data_persistence: OpLogPersistence,     // TLogFS for data
    control_table: ControlTable,            // DeltaLake table for metadata
    last_write_seq: Arc<AtomicI64>,         // Current write sequence
}

OpLogPersistence::begin(Option<i64>) -> TransactionGuard
```

## DeltaLake Schema

### Control Table Location
- **Path**: `pond/control/` (direct DeltaLake table, no TLogFS wrapper)
- **Table name**: `transactions`

### Arrow Schema
```rust
Schema {
    // Primary transaction identifiers
    txn_seq: Int64,              // Sequential: 1, 2, 3... (business transaction order)
    txn_id: Utf8,                // UUID for correlation across systems
    based_on_seq: Int64?,        // For reads: references last write seq (e.g., 13)
    
    // Transaction lifecycle
    record_type: Utf8,           // "begin" | "data_committed" | "failed" | "completed"
    timestamp: Timestamp(Nanosecond, None),
    transaction_type: Utf8,      // "read" | "write"
    
    // Context capture
    cli_args: List<Utf8>,        // Command-line arguments
    environment: Map<Utf8, Utf8>, // Selected env vars (e.g., RUST_LOG)
    
    // Data filesystem linkage
    data_fs_version: Int64?,     // DeltaLake version (different from txn_seq!)
                                 // Links steward txn_seq to storage version
    
    // Error tracking
    error_message: Utf8?,
    duration_ms: Int64?,
    
    // Future: Replication coordination (all nullable, unused initially)
    replication_targets: List<Utf8>?,
    replication_status: Utf8?,           // "pending" | "in_progress" | "complete" | "failed"
    replication_started_at: Timestamp?,
    replication_completed_at: Timestamp?,
    replication_error: Utf8?,
    bytes_replicated: Int64?,
}
```

### Key Relationships
```
txn_seq (Steward) → OpLog records (TLogFS) → DeltaLake version
    14           → create /sensors/temp.csv →       42
    15           → create /data/export      →       43
   read(14)      → query operations         →     (none)
    16           → update /config.yaml      →       44
```

## Transaction Flow

### Write Transaction Lifecycle
```
1. Steward: Allocate txn_seq = last_write_seq + 1
2. Steward: Record "begin" in control table
3. TLogFS: Begin transaction with Some(txn_seq)
4. Application: Execute operations
5. TLogFS: Commit → returns DeltaLake version
6. Steward: Record "data_committed" with version
   [Future: Query commit log, bundle files, replicate]
7. Steward: Record "replication_complete" (future)
```

### Read Transaction Lifecycle
```
1. Steward: Use current_seq = last_write_seq
2. Steward: Record "begin" with based_on_seq=current_seq
3. TLogFS: Begin transaction with None (no sequence)
4. Application: Execute read operations
5. Steward: Record "completed"
```

## Implementation Tasks

### Phase 1: Core Infrastructure

#### Task 1: Create Control Table Module
**File**: `crates/steward/src/control_table.rs` (new)

**Components**:
- `ControlTable` struct
- Arrow schema definition
- DeltaLake table initialization
- `get_last_write_sequence()` - query max txn_seq for writes

**Key Methods**:
```rust
impl ControlTable {
    pub async fn new(path: &str) -> Result<Self, StewardError>;
    pub async fn get_last_write_sequence(&self) -> Result<i64, StewardError>;
}
```

**Deliverable**: Module compiles, can create/open DeltaLake table

---

#### Task 2: Implement Transaction Record Operations
**File**: `crates/steward/src/control_table.rs`

**Methods to add**:
```rust
impl ControlTable {
    pub async fn record_begin(
        &mut self,
        txn_seq: i64,
        based_on_seq: Option<i64>,
        txn_id: String,
        transaction_type: &str,  // "read" | "write"
        cli_args: Vec<String>,
        environment: HashMap<String, String>,
    ) -> Result<(), StewardError>;
    
    pub async fn record_data_committed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        data_fs_version: i64,
        duration_ms: i64,
    ) -> Result<(), StewardError>;
    
    pub async fn record_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError>;
    
    pub async fn record_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        duration_ms: i64,
    ) -> Result<(), StewardError>;
}
```

**Deliverable**: Can write transaction records to control table

---

### Phase 2: Ship Integration

#### Task 3: Replace Ship Control Persistence
**File**: `crates/steward/src/ship.rs`

**Changes**:
```rust
pub struct Ship {
    data_persistence: OpLogPersistence,
    control_table: ControlTable,           // NEW: replaces control_persistence
    last_write_seq: Arc<AtomicI64>,        // NEW: tracks write sequence
    pond_path: String,
}
```

**Methods to update**:
- `create_infrastructure()` - initialize control table instead of TLogFS
- `open_pond()` - load last_write_seq from control table
- `create_pond()` - initialize control table with seq=0

**Deliverable**: Ship compiles with control table instead of control persistence

---

#### Task 4: Update Transaction Begin Logic
**File**: `crates/steward/src/ship.rs`

**Method to update**: `begin_transaction()`

**New logic**:
```rust
pub async fn begin_transaction(
    &mut self,
    args: Vec<String>,
    variables: HashMap<String, String>,
    is_write: bool,  // NEW: determines sequence allocation
) -> Result<StewardTransactionGuard<'_>, StewardError>
```

**Sequence allocation**:
- Read: `current_seq = self.last_write_seq.load(Ordering::SeqCst)`
- Write: `txn_seq = self.last_write_seq.fetch_add(1, Ordering::SeqCst) + 1`

**Control table recording**:
- Call `control_table.record_begin()` before data FS transaction

**TLogFS invocation**:
- Write: `data_persistence.begin(Some(txn_seq))`
- Read: `data_persistence.begin(None)`

**Deliverable**: Transaction begin allocates sequences, records in control table

---

### Phase 3: Transaction Guard Updates

#### Task 5: Update StewardTransactionGuard
**File**: `crates/steward/src/guard.rs`

**Changes**:
```rust
pub struct StewardTransactionGuard<'a> {
    data_tx: Option<TransactionGuard<'a>>,
    txn_seq: i64,                          // NEW
    txn_id: String,
    transaction_type: String,              // NEW: "read" | "write"
    start_time: std::time::Instant,        // NEW: for duration tracking
    args: Vec<String>,
    control_table: &'a mut ControlTable,   // CHANGED: from control_persistence
}
```

**Methods to update**:
- `new()` - accept txn_seq, transaction_type
- `commit()` - record duration, call control_table methods

**Commit logic**:
```rust
pub async fn commit(mut self) -> Result<Option<()>, StewardError> {
    let duration_ms = self.start_time.elapsed().as_millis() as i64;
    
    // Commit data FS
    let data_tx = self.take_transaction()?;
    let result = data_tx.commit(metadata).await;
    
    match result {
        Ok(Some(version)) => {
            // Write committed
            self.control_table.record_data_committed(
                self.txn_seq, self.txn_id, version, duration_ms
            ).await?;
        }
        Ok(None) => {
            // Read-only
            self.control_table.record_completed(
                self.txn_seq, self.txn_id, duration_ms
            ).await?;
        }
        Err(e) => {
            // Failed
            self.control_table.record_failed(
                self.txn_seq, self.txn_id, e.to_string(), duration_ms
            ).await?;
            return Err(e.into());
        }
    }
    
    Ok(result.ok())
}
```

**Deliverable**: Guard tracks sequences, records lifecycle events

---

### Phase 4: Recovery Implementation

#### Task 6: Implement Control Table Recovery
**File**: `crates/steward/src/ship.rs`

**Methods to rewrite**:
- `check_recovery_needed()` - query control table
- `recover()` - query control table, detect incomplete transactions

**Recovery queries**:
```sql
-- Find incomplete write transactions
SELECT txn_seq, txn_id, data_fs_version 
FROM transactions
WHERE transaction_type = 'write'
  AND record_type = 'data_committed'
  AND txn_seq NOT IN (
    SELECT txn_seq FROM transactions 
    WHERE record_type IN ('replication_complete', 'failed')
  )
```

**Recovery actions**:
1. Report incomplete transactions with txn_seq and data_fs_version
2. For now: just report (don't start replication)
3. Future: will query data FS commit log, bundle files, replicate

**Deliverable**: Recovery detects incomplete transactions via control table

---

### Phase 5: Cleanup

#### Task 7: Remove Control Filesystem Code
**File**: `crates/steward/src/ship.rs`

**Methods to remove**:
- `initialize_control_filesystem()`
- `record_transaction_metadata_with_persistence()`
- `read_transaction_metadata()`

**Functions to remove from lib.rs**:
- `get_control_path()`

**Deliverable**: No TLogFS control filesystem references remain

---

#### Task 8: Update Error Types
**File**: `crates/steward/src/lib.rs`

**Changes**:
```rust
#[derive(Debug, Error)]
pub enum StewardError {
    #[error("Failed to initialize data filesystem: {0}")]
    DataInit(#[from] tlogfs::TLogFSError),
    
    #[error("Control table error: {0}")]
    ControlTable(String),  // NEW: replaces ControlInit
    
    #[error("Transaction sequence mismatch: expected {expected}, found {actual}")]
    TransactionSequenceMismatch { expected: i64, actual: i64 },
    
    #[error("Recovery needed: incomplete transaction {txn_seq}")]
    RecoveryNeeded { txn_seq: i64, txn_id: String },  // CHANGED: use txn_seq
    
    // ... other variants
}
```

**Deliverable**: Error types reflect control table architecture

---

### Phase 6: TLogFS Integration

#### Task 9: Update TLogFS begin() Signature
**Files**:
- `crates/tlogfs/src/persistence.rs`
- `crates/tlogfs/src/transaction_guard.rs`

**API change**:
```rust
// OLD signature (will break all call sites):
pub async fn begin(&mut self) -> Result<TransactionGuard, TLogFSError>

// NEW signature:
pub async fn begin(&mut self, txn_seq: Option<i64>) -> Result<TransactionGuard, TLogFSError>
```

**TransactionGuard changes**:
```rust
pub struct TransactionGuard<'a> {
    // ... existing fields
    txn_seq: Option<i64>,  // NEW: store for future OpLog integration
}

impl TransactionGuard {
    pub fn sequence(&self) -> Option<i64> {
        self.txn_seq
    }
}
```

**Implementation notes**:
- Store txn_seq in TransactionGuard
- DO NOT write to OpLog yet (future work)
- Just plumbing for now

**Call sites to update** (estimate ~100 locations):
- All tests in `crates/tlogfs/tests/*.rs`
- All tests in `crates/tlogfs/src/tests.rs`
- Test utilities in `crates/tlogfs/src/test_utils.rs`
- Documentation examples in `docs/*.md`

**Migration pattern**:
```rust
// Before:
let tx = persistence.begin().await?;

// After:
let tx = persistence.begin(None).await?;  // Most call sites use None
```

**Deliverable**: TLogFS accepts sequence, stores for future use

---

### Phase 7: Testing

#### Task 10: Update Steward Tests
**Files**: `crates/steward/tests/*.rs`, `crates/steward/src/*.rs` (test modules)

**Test scenarios**:
1. **Sequence allocation**:
   - First write gets seq=1
   - Second write gets seq=2
   - Read between writes reuses seq=2
   
2. **Control table records**:
   - Verify begin records created
   - Verify commit records txn_seq and data_fs_version
   - Verify read transactions marked correctly

3. **Recovery**:
   - Simulate crash after data commit, before control record
   - Verify recovery detects incomplete transaction
   - Verify recovery reports correct txn_seq

**Deliverable**: All steward tests pass with control table

---

#### Task 11: Update CMD Crate
**Files**: `crates/cmd/src/*.rs`

**Search for**:
- `get_control_path()` usage
- Filesystem choice flags referencing control FS
- Any direct control filesystem access

**Expected changes**:
- Remove control filesystem command options
- Update `pond init` to work with control table
- Update `pond recover` to report txn_seq
- Ensure no references to control as filesystem

**Deliverable**: CMD crate compiles and runs with new architecture

---

#### Task 12: Integration Testing
**File**: `crates/steward/tests/integration_test.rs` (new or existing)

**Test scenario**:
```rust
#[tokio::test]
async fn test_complete_lifecycle_with_sequences() {
    // 1. Initialize pond
    let ship = Ship::create_pond(temp_dir).await?;
    
    // 2. Write transaction (seq=1)
    ship.transact(vec!["write1"], true, |tx, fs| async {
        fs.root().await?.create_dir_path("/data").await?;
        Ok(())
    }).await?;
    
    // 3. Read transaction (seq=1, based_on=1)
    ship.transact(vec!["read1"], false, |tx, fs| async {
        fs.root().await?.list_dir_path("/").await?;
        Ok(())
    }).await?;
    
    // 4. Write transaction (seq=2)
    ship.transact(vec!["write2"], true, |tx, fs| async {
        fs.root().await?.create_file_path("/data/test.txt").await?;
        Ok(())
    }).await?;
    
    // 5. Verify control table
    let records = ship.control_table.query_all().await?;
    assert_eq!(records[0].txn_seq, 1);
    assert_eq!(records[0].transaction_type, "write");
    assert_eq!(records[1].txn_seq, 1);
    assert_eq!(records[1].transaction_type, "read");
    assert_eq!(records[1].based_on_seq, Some(1));
    assert_eq!(records[2].txn_seq, 2);
    assert_eq!(records[2].transaction_type, "write");
    
    // 6. Test recovery
    // (simulate crash scenario)
}
```

**Deliverable**: Full lifecycle works with sequences

---

## Migration Strategy

### Phase Ordering
1. Build control table infrastructure (Tasks 1-2)
2. Integrate with Ship (Tasks 3-4)
3. Update transaction guard (Task 5)
4. Implement recovery (Task 6)
5. Clean up old code (Tasks 7-8)
6. Update TLogFS (Task 9) - **Most invasive due to call sites**
7. Update all tests (Tasks 10-12)

### Risk Mitigation

**Biggest risk**: Task 9 (TLogFS signature change) affects ~100 call sites

**Mitigation**:
- Change signature first
- Fix compilation errors systematically
- Update tests file-by-file
- All changes are mechanical: `.begin()` → `.begin(None)`
- Steward is only caller using `.begin(Some(seq))`

### Backward Compatibility

**Breaking changes**:
- TLogFS `begin()` signature changes (compile-time break)
- Control filesystem no longer exists (runtime break)

**Non-breaking**:
- Application code using `StewardTransactionGuard` unchanged
- CMD commands work the same (internal implementation change)

### Testing Strategy

**Unit tests**: After each task, verify that task's component works

**Integration tests**: After Phase 7, verify full system works

**Manual testing**: Test pond init, transactions, recovery via CLI

## Success Criteria

### Functional Requirements
- [ ] Control table stores transaction records
- [ ] Sequential txn_seq allocation works correctly
- [ ] Read transactions reference last write sequence
- [ ] Write transactions pass sequence to TLogFS
- [ ] Recovery detects incomplete transactions
- [ ] All tests pass

### Non-Functional Requirements
- [ ] No code duplication (single begin() method)
- [ ] Abstraction maintained (applications unaware of changes)
- [ ] Clear transaction ordering via txn_seq
- [ ] Foundation for future replication

### Future Work (Not in this migration)
- [ ] Write txn_seq to OpLog records
- [ ] Implement "show" command with transaction ordering
- [ ] Query data FS commit log for replication
- [ ] Implement actual replication coordination
- [ ] Capture RUST_LOG output during transactions

## Open Questions

1. **Environment capture**: Which env vars to capture? Start with RUST_LOG only?
2. **Control table schema evolution**: How to version schema for future changes?
3. **Read transaction value**: Do we need to record all read transactions, or just writes?
4. **Error handling**: How to handle control table write failures during commit?

## References

- `docs/duckpond-overview.md` - System architecture
- `docs/duckpond-system-patterns.md` - Critical patterns
- `docs/anti-duplication.md` - Code duplication prevention
- `docs/fallback-antipattern-philosophy.md` - Fail-fast principles

## Change Log

- 2025-10-12: Initial planning document created
