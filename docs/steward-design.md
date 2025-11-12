# Steward Design Document

## Overview

The Steward crate provides transaction coordination and audit trail management for DuckPond. It ensures atomicity and consistency for data operations while maintaining a complete audit log of all transactions through a Delta Lake control table.

## Core Purpose

Steward coordinates transactions between the tlogfs persistence layer and application logic while maintaining comprehensive audit trails. This enables:

1. **Transaction sequence management** - Ensures write transactions increment sequences atomically, read transactions provide snapshot isolation
2. **Read/Write separation** - Distinguishes transactions that modify data from those that only query
3. **Complete audit trail** - Records every transaction (successful or failed) in the control table for monitoring and debugging
4. **Post-commit factory execution** - Runs background tasks after successful data commits
5. **Crash recovery** - Marks incomplete transactions during recovery for audit completeness

## Architecture Components

### Ship - The Transaction Coordinator

The `Ship` struct coordinates transactions and maintains sequence state:

```rust
pub struct Ship {
    /// Data persistence layer (tlogfs) - authoritative source for transaction sequences
    data_persistence: OpLogPersistence,
    /// Control table for audit trail and transaction lifecycle tracking
    control_table: ControlTable,
    /// Cached last write sequence (synchronized from data_persistence)
    last_write_seq: AtomicI64,
    /// Path to the pond root
    pond_path: String,
}
```

### Single Data Layer with Control Table

**Data Persistence (tlogfs/Delta Lake)**:
- Contains all user data backed by Delta Lake at `{pond}/data/`
- **Authoritative source** for transaction sequences via Delta Lake commit metadata
- Each write transaction creates a new Delta Lake version with metadata: `{"pond_txn": {txn_seq, txn_id, args, vars}}`
- Read transactions provide snapshot isolation without creating new versions

**Control Table (Delta Lake)**:
- Separate Delta Lake table at `{pond}/control/` for audit trail
- Records **every transaction** (read and write, successful and failed)
- Tracks transaction lifecycle: begin → outcome (data_committed | completed | failed)
- Enables monitoring, debugging, and compliance auditing
- Post-commit task tracking for background factory executions

### Transaction Coordination Protocol

#### Transaction Types

**Write Transactions**:
- Increment the transaction sequence: `txn_seq = last_write_seq + 1`
- Commit to Delta Lake with metadata containing txn_seq, txn_id, args, vars
- Create a new Delta Lake table version
- Recorded in control table with `data_fs_version`

**Read Transactions**:
- Reuse the last write sequence: `txn_seq = last_write_seq` (for snapshot isolation)
- Do NOT commit to Delta Lake or create new versions
- Provide read atomicity by snapshotting at a specific sequence
- Recorded in control table with `based_on_seq` to show which version was read

#### Write Transaction Sequence

1. **Begin Transaction**
   ```rust
   let options = TransactionOptions::write(args);
   let tx = ship.begin_transaction(options).await?;
   ```
   - Allocates next sequence: `txn_seq = last_write_seq + 1`
   - Creates UUID7 transaction identifier
   - Records "begin" in control table with transaction type
   - Calls `data_persistence.begin_write(txn_seq, metadata)`

2. **Perform Operations**
   ```rust
   let root = tx.root().await?;
   root.create_file_path("/example.txt", b"content").await?;
   ```
   - Execute filesystem operations within the transaction

3. **Commit**
   ```rust
   tx.commit().await?;
   ```
   - Commits to Delta Lake with metadata in commit
   - Control table records "data_committed" with new `data_fs_version`
   - Runs post-commit factories if configured

#### Read Transaction Sequence

1. **Begin Transaction**
   ```rust
   let options = TransactionOptions::read(args);
   let tx = ship.begin_transaction(options).await?;
   ```
   - Reuses current sequence: `txn_seq = last_write_seq`
   - Records "begin" in control table with `based_on_seq`
   - Calls `data_persistence.begin_read(txn_seq, metadata)`

2. **Perform Queries**
   ```rust
   let ctx = tx.session_context().await?;
   let df = ctx.sql("SELECT * FROM my_table").await?;
   ```
   - Execute read-only operations with snapshot isolation

3. **Complete**
   ```rust
   tx.commit().await?;  // Returns None
   ```
   - Does NOT commit to Delta Lake
   - Control table records "completed" (no version bump)
   - Provides consistent read view at `based_on_seq`

### Transaction Sequencing

**tlogfs is Authoritative**:
- On pond open, tlogfs loads `last_txn_seq` from Delta Lake commit metadata
- Ship synchronizes: `last_write_seq = data_persistence.last_txn_seq()`
- Single source of truth eliminates sequence drift

**Sequence Validation**:
- Write transactions: tlogfs validates `txn_seq == last + 1`
- Read transactions: tlogfs validates `txn_seq == last`
- Violations return errors before any operations execute

**Test Convenience**:
- Production code: `begin_write(txn_seq, metadata)` or `begin_read(txn_seq, metadata)`
- Tests: `begin_test()` auto-increments sequence (no parameters needed)

### Transaction Metadata

**PondTxnMetadata** (provided at begin):
```rust
pub struct PondTxnMetadata {
    pub txn_id: String,                  // UUID7 for recovery/debugging
    pub args: Vec<String>,               // CLI arguments
    pub vars: HashMap<String, String>,   // Key/value parameters
}
```

**DeltaCommitMetadata** (stored in Delta Lake commits):
```rust
pub struct DeltaCommitMetadata {
    pub txn_seq: i64,                    // Injected at commit time
    pub txn_id: String,
    pub args: Vec<String>,
    pub vars: HashMap<String, String>,
}
```

The `txn_seq` is added during commit, allowing tlogfs to load the authoritative sequence on pond open.

### Control Table Audit Trail

The control table records **every transaction** for complete audit visibility:

**TransactionRecord Schema**:
```rust
pub struct TransactionRecord {
    txn_seq: i64,                      // Transaction sequence
    txn_id: String,                    // UUID7 identifier
    based_on_seq: Option<i64>,         // For reads: which version
    record_type: String,               // "begin" | "data_committed" | "completed" | "failed"
    timestamp: i64,                    // Microseconds since epoch
    transaction_type: String,          // "read" | "write" | "post_commit"
    cli_args: Vec<String>,             // CLI arguments
    environment: HashMap<String, String>, // Variables
    data_fs_version: Option<i64>,      // Delta version (writes only)
    error_message: Option<String>,     // For failures
    duration_ms: Option<i64>,          // Execution time
    
    // Post-commit task tracking
    parent_txn_seq: Option<i64>,       // Triggering transaction
    execution_seq: Option<i32>,        // Factory execution order
    factory_name: Option<String>,      // Factory that executed
    config_path: Option<String>,       // Config file path
}
```

**Transaction Lifecycle**:

1. **record_begin()** - Called when transaction starts
   - Records txn_seq, txn_id, transaction_type ("read" or "write")
   - For reads: includes `based_on_seq` to show snapshot version
   - Captures CLI args and environment variables
   - Records start timestamp

2. **Outcome Recording** - One of three possibilities:

   **record_data_committed()** - Write transaction succeeded
   - Writes new `data_fs_version` (Delta Lake table version)
   - Records `duration_ms` for performance monitoring

   **record_completed()** - Read transaction succeeded (or idempotent write)
   - No version bump
   - Records duration for monitoring

   **record_failed()** - Transaction failed
   - Captures `error_message` with failure details
   - Records duration until failure

**Querying Transaction History**:

```sql
-- View recent transaction activity
SELECT txn_seq, txn_id, record_type, transaction_type, 
       data_fs_version, error_message, duration_ms
FROM control_table
ORDER BY timestamp DESC
LIMIT 20;

-- Check for failures
SELECT txn_seq, txn_id, error_message, duration_ms
FROM control_table
WHERE record_type = 'failed'
ORDER BY timestamp DESC;

-- Monitor read vs write activity
SELECT transaction_type, COUNT(*) as count
FROM control_table
WHERE record_type = 'begin'
GROUP BY transaction_type;

-- Track post-commit background tasks
SELECT parent_txn_seq, factory_name, record_type, error_message
FROM control_table
WHERE parent_txn_seq IS NOT NULL
ORDER BY parent_txn_seq, execution_seq;
```

### Crash Recovery System

Crash recovery identifies incomplete transactions in the control table and marks them as "completed" with a recovery flag.

#### Recovery Detection

```rust
let result = ship.recover().await?;
```

Scans the control table for transactions with "begin" records but no outcome record:
- Query for `record_type = 'begin'` without matching commit/completed/failed
- Indicates crash occurred between transaction start and outcome recording
- Returns list of incomplete transactions with their sequences and IDs

#### Recovery Process

For each incomplete transaction:
1. **Mark as recovered**: Call `record_completed(txn_seq, txn_id, 0)` with zero duration
2. **Distinguish from normal**: Recovery records can be identified by zero or very low duration
3. **Preserve audit trail**: Incomplete transactions remain visible in control table history
4. **Resume operations**: After recovery, pond is ready for normal transactions

Recovery is automatic on pond open and ensures the control table audit trail is complete.

## Post-Commit Factory System

Post-commit factories execute background tasks after successful write transactions.

### Factory Discovery and Execution

After a write transaction commits successfully, Steward:

1. **Discovers factories** from `/etc/system.d/*` configuration files
2. **Filters by mode**: Only executes factories with `mode: "push"` (skips "pull" mode)
3. **Executes in order**: Runs factories sequentially based on config file names (e.g., `10-validate`, `20-replicate`)
4. **Tracks in control table**: Records pending, started, completed, and failed states

### Factory Lifecycle Tracking

Each factory execution creates control table records:

```rust
// Before execution
record_post_commit_pending(parent_txn_seq, execution_seq, factory_name, config_path)

// When starting
record_post_commit_started(txn_seq, txn_id)

// On success
record_post_commit_completed(txn_seq, txn_id, duration_ms)

// On failure  
record_post_commit_failed(txn_seq, txn_id, error_message, duration_ms)
```

**Control Table Fields**:
- `parent_txn_seq`: The write transaction that triggered this factory
- `execution_seq`: Ordinal in factory list (1, 2, 3...)
- `factory_name`: Name from factory configuration
- `config_path`: Path to config file (e.g., `/etc/system.d/10-validate`)

### Factory Transaction Model

Each factory execution runs in its own **write transaction**:
- Gets next sequence: `txn_seq = last_write_seq + 1`
- Has its own `txn_id` (UUID7)
- Commits to Delta Lake if it modifies data
- Recorded with `transaction_type = "post_commit"`

This enables factories to:
- Make data modifications (e.g., validation tables, metrics)
- Chain additional factory executions
- Participate in the full audit trail

### Factory Configuration Example

```yaml
# /etc/system.d/10-validate
name: "validation"
mode: "push"  # Execute on post-commit
command: ["pond", "validate", "--all"]
```

Factories with `mode: "pull"` are skipped during post-commit execution.

## Integration with DuckPond

### CLI Integration

Commands use Steward's transaction API for all data operations:

```rust
// pond init - creates pond with root transaction
let ship = Ship::initialize_pond(&pond_path, args).await?;

// pond write operations - use write transactions
let mut ship = Ship::open_existing_pond(&pond_path).await?;
let options = TransactionOptions::write(args);
let tx = ship.begin_transaction(options).await?;
// ... perform write operations ...
tx.commit().await?;

// pond query operations - use read transactions
let options = TransactionOptions::read(args);
let tx = ship.begin_transaction(options).await?;
let ctx = tx.session_context().await?;
// ... execute queries ...
tx.commit().await?;  // Returns None, no Delta commit
```

### Convenience Methods

**transact()** - Scoped write transaction:
```rust
ship.transact(args, |tx, fs| {
    Box::pin(async move {
        let root = fs.root().await?;
        root.create_file_path("/data.txt", b"content").await?;
        Ok(())  // Auto-commits on success
    })
}).await?;
```

**transact_with_session()** - Scoped transaction with DataFusion SQL:
```rust
ship.transact_with_session(args, |tx, fs, ctx| {
    Box::pin(async move {
        ctx.sql("CREATE TABLE foo AS SELECT * FROM bar").await?;
        Ok(())
    })
}).await?;
```

### Factory System Integration

Dynamic filesystem objects work within transactions:
- **CSV directories**: Present CSV files as queryable Arrow tables
- **SQL-derived nodes**: Computed from other filesystem data
- **Hostmount directories**: Mirror host filesystem paths

All factory-generated objects participate in the transaction system and can trigger post-commit executions.

## Error Handling and Edge Cases

### Read Transaction Validation

If a transaction marked as "read" attempts to write data, it fails:
```rust
Error: ReadTransactionAttemptedWrite
```

The control table records this as a "failed" transaction with the error message.

### Idempotent Write Operations

Write transactions that make no changes (e.g., `mkdir -p` for existing directory) are allowed:
- Commit returns `Ok(None)` instead of `Ok(Some(()))`
- Control table records "completed" instead of "data_committed"
- No Delta Lake version created
- Prevents spurious version increments

### Concurrent Access

Steward uses an atomic counter (`AtomicI64`) for sequence allocation:
- Write transactions atomically increment: `fetch_add(1, Ordering::SeqCst)`
- Read transactions atomically read: `load(Ordering::SeqCst)`
- Delta Lake commits are serialized by the underlying storage layer

Future versions may add distributed locking for multi-host coordination.

### Storage Backend Failures

Delta Lake storage failures propagate as errors, allowing retry logic:
- `StewardError::DataInit` - tlogfs/Delta Lake errors
- `StewardError::ControlTable` - Control table recording errors

Transaction guards auto-rollback on drop if not committed.

## Performance Considerations

### Transaction Overhead

Each write transaction involves:
- **Sequence allocation**: Atomic increment (microseconds)
- **Control table begin record**: Single Delta Lake append (milliseconds)
- **Data operations**: Application-specific work
- **Delta Lake commit**: With metadata (milliseconds to seconds depending on data size)
- **Control table outcome record**: Single Delta Lake append (milliseconds)

Read transactions skip Delta commit but still record lifecycle in control table.

### Control Table Growth

The control table grows with every transaction:
- **Linear growth**: 2 records per write transaction (begin + outcome)
- **2 records per read**: Begin + completed
- **Compaction**: Future versions may implement retention policies for old records
- **Query performance**: Indexed by timestamp and txn_seq for efficient queries

### Recovery Performance

Recovery scans control table for incomplete transactions:
- **O(n) query**: Single SQL query for unmatched "begin" records
- **Efficient**: Control table is separate Delta Lake table, doesn't scan data
- **Typically fast**: Only incomplete transactions need marking

## Testing Strategy

### Unit Tests

- **Transaction lifecycle**: Begin, commit sequences for read and write
- **Read/write separation**: Validate read transactions cannot write
- **Sequence validation**: Ensure proper increment and reuse logic
- **Control table recording**: Verify all transaction states recorded
- **Idempotent operations**: Test no-op write behavior

### Integration Tests

- **Crash recovery**: Simulate incomplete transactions and verify recovery
- **Post-commit factories**: Test factory discovery and execution
- **SessionContext access**: Verify DataFusion SQL within transactions
- **Command integration**: Test with actual CLI commands

### Test Convenience Features

**Automatic sequencing**:
```rust
// Production code
let metadata = PondTxnMetadata::new(txn_id, args, vars);
let tx = persistence.begin_write(txn_seq, metadata).await?;

// Test code (auto-increments)
let tx = persistence.begin_test().await?;
tx.commit_test().await?;
```

Tests use `begin_test()` with no parameters for automatic sequence management.

## Monitoring and Observability

### Transaction Metrics

Query the control table for operational metrics:

**Success rate**:
```sql
SELECT 
    COUNT(CASE WHEN record_type IN ('data_committed', 'completed') THEN 1 END) as success,
    COUNT(CASE WHEN record_type = 'failed' THEN 1 END) as failed
FROM control_table
WHERE timestamp > current_timestamp - INTERVAL '1 hour';
```

**Average duration by type**:
```sql
SELECT transaction_type, AVG(duration_ms) as avg_duration_ms
FROM control_table
WHERE record_type IN ('data_committed', 'completed', 'failed')
GROUP BY transaction_type;
```

**Post-commit factory performance**:
```sql
SELECT factory_name, 
       COUNT(*) as executions,
       AVG(duration_ms) as avg_duration,
       COUNT(CASE WHEN record_type = 'post_commit_failed' THEN 1 END) as failures
FROM control_table
WHERE parent_txn_seq IS NOT NULL
GROUP BY factory_name
ORDER BY avg_duration DESC;
```

### Pond Identity

Each pond has immutable identity metadata stored in control table properties:

```rust
pub struct PondMetadata {
    pond_id: String,           // UUID v7
    birth_timestamp: i64,      // Creation time
    birth_hostname: String,    // Where created
    birth_username: String,    // Who created
}
```

This enables tracking pond lineage across replicas and environments.

## Future Enhancements

### Distributed Coordination

Support for multiple concurrent Steward instances:
- **Distributed sequence allocation**: Coordinate sequence numbers across hosts
- **Consensus protocols**: Ensure consistent ordering in multi-writer scenarios
- **Lock-free algorithms**: Optimize for high-concurrency environments

### Advanced Factory System

- **Factory dependencies**: Define execution order and prerequisites
- **Conditional execution**: Run factories based on transaction content (e.g., only if specific paths modified)
- **Factory rollback**: Undo factory effects on subsequent failures
- **Factory retries**: Configurable retry policies with exponential backoff
- **Factory chaining**: Factories that trigger other factories

### Control Table Enhancements

- **Retention policies**: Automatically archive or delete old transaction records
- **Compaction strategies**: Optimize control table performance as it grows
- **Replication tracking**: Record which replicas have received which transactions
- **Change capture**: Detailed diffs of what each transaction modified

### Performance Optimizations  

- **Batch operations**: Group multiple operations into single transaction
- **Parallel factories**: Execute independent post-commit tasks concurrently
- **Lazy control table writes**: Buffer control records for batch writes
- **Sequence number caching**: Reduce atomic operations in high-frequency scenarios

### Enhanced Monitoring

- **Real-time dashboards**: Live transaction activity visualization
- **Alerting**: Notifications for failed transactions or slow factories
- **Anomaly detection**: Identify unusual transaction patterns
- **Performance profiling**: Detailed timing breakdown of transaction phases

## Conclusion

The Steward architecture provides comprehensive transaction coordination with complete audit trails for DuckPond. Key design principles:

1. **tlogfs is authoritative** - Single source of truth for transaction sequences via Delta Lake metadata
2. **Read/write separation** - Clear semantics for data modification vs. queries with snapshot isolation
3. **Complete audit trail** - Control table records every transaction lifecycle for monitoring and compliance
4. **Post-commit factories** - Extensible background task execution after successful writes
5. **Crash recovery** - Automatic detection and marking of incomplete transactions

The architecture eliminates sequence drift by loading authoritative sequences from Delta Lake on pond open. The control table provides comprehensive visibility into transaction history, enabling monitoring, debugging, and compliance tracking without requiring external logging systems.

This design enables DuckPond to provide ACID guarantees with full observability while maintaining the flexibility needed for advanced features like replication, validation, and external system integration.
