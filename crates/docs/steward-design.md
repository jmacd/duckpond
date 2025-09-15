# Steward Design Document

## Overview

The Steward crate provides serialized commit coordination and post-commit action management for DuckPond's dual-filesystem architecture. It ensures atomicity and consistency across both data and control filesystems while enabling extensible post-commit hooks for operations like replication.

## Core Purpose

Steward exists to solve the problem of **coordinated transactional commits across two independent Delta Lake instances** while providing **serialized post-commit action execution**. This enables:

1. **Atomic dual-filesystem operations** - Ensures both data and control filesystems remain consistent
2. **Serialized commit sequencing** - Prevents race conditions in concurrent environments  
3. **Post-commit hook infrastructure** - Enables replication, notification, and other follow-up actions
4. **Crash recovery** - Maintains consistency even after system failures

## Architecture Components

### Ship - The Main Orchestrator

The `Ship` struct is the central coordinator managing:

```rust
pub struct Ship {
    /// Primary filesystem for user data
    data_fs: FS,
    /// Secondary filesystem for steward control and transaction metadata
    control_fs: FS,
    /// Direct access to data persistence layer for metadata operations
    data_persistence: OpLogPersistence,
    /// Path to the pond root
    pond_path: String,
    /// Current transaction descriptor (if any)
    current_tx_desc: Option<TxDesc>,
}
```

### Dual Filesystem Model

**Data Filesystem (`data_fs`)**:
- Contains user data and application objects
- Backed by Delta Lake at `{pond}/data/`
- Generates the primary transaction sequence via Delta Lake versions
- Each commit creates a new Delta Lake version (0, 1, 2, ...)

**Control Filesystem (`control_fs`)**:
- Contains transaction metadata and steward configuration
- Backed by Delta Lake at `{pond}/control/`
- Stores transaction descriptors at `/txn/{version}`
- Enables crash recovery and audit trails

### Transaction Coordination Protocol

#### Commit Sequence

1. **Begin Transaction**
   ```rust
   ship.begin_transaction_with_args(args).await?;
   ```
   - Stores transaction descriptor with command arguments
   - Begins transaction on data filesystem

2. **Perform Operations**
   ```rust
   let root = ship.data_fs().root().await?;
   root.create_file_path("/example.txt", b"content").await?;
   ```
   - Execute filesystem operations within the transaction

3. **Coordinated Commit**
   ```rust
   ship.commit_transaction().await?;
   ```
   - Commits data filesystem WITH recovery metadata
   - Reads committed Delta Lake version
   - Records transaction metadata in control filesystem
   - Future: Executes post-commit hooks

#### Version Mapping

The system uses Delta Lake versions directly to avoid off-by-one errors:

- **First commit**: Delta Lake version 0 → `/txn/0`
- **Second commit**: Delta Lake version 1 → `/txn/1`  
- **Third commit**: Delta Lake version 2 → `/txn/2`

This eliminates artificial sequence number mapping and reduces complexity.

### Crash Recovery System

#### Recovery Detection

```rust
ship.check_recovery_needed().await?;
```

Compares Delta Lake versions against control filesystem metadata:
- If Delta Lake is at version N but `/txn/N` is missing → recovery needed
- Detects partial commits where data committed but metadata recording failed

#### Recovery Process

```rust
let result = ship.recover().await?;
```

1. **Scan for gaps**: Check versions 0 through current for missing `/txn/{version}` files
2. **Extract metadata**: Retrieve steward metadata from Delta Lake commit info
3. **Reconstruct control records**: Create missing `/txn/{version}` files
4. **Verify consistency**: Ensure all commits have corresponding metadata

#### Metadata Embedding

During commit, steward embeds recovery metadata in Delta Lake commits:

```rust
let tx_metadata = HashMap::from([
    ("steward_tx_args", serde_json::Value::String(tx_desc.to_json()?)),
    ("steward_recovery_needed", serde_json::Value::Bool(true)),
    ("steward_timestamp", serde_json::Value::Number(timestamp)),
]);
```

This enables complete recovery of transaction context even after crashes.

## Transaction Descriptors

### TxDesc Structure

```rust
pub struct TxDesc {
    pub args: Vec<String>,
    // Additional metadata fields...
}
```

Contains:
- **Command arguments**: Original CLI args that triggered the transaction
- **Timestamps**: For audit and debugging
- **Recovery flags**: Crash recovery state tracking

### Metadata Storage

Transaction metadata is stored as JSON in control filesystem:

```
/control/_delta_log/           # Delta Lake metadata
/control/txn/0                 # Transaction 0 metadata (JSON)
/control/txn/1                 # Transaction 1 metadata (JSON)
/control/txn/2                 # Transaction 2 metadata (JSON)
```

Example `/txn/0` content:
```json
{
  "args": ["pond", "init"],
  "timestamp": 1691234567890,
  "command_name": "pond"
}
```

## Post-Commit Hook Architecture (Future)

### Design Intent

Post-commit hooks will execute AFTER successful dual-filesystem commit:

```rust
// Future implementation
impl Ship {
    async fn commit_transaction(&mut self) -> Result<(), StewardError> {
        // 1. Commit data filesystem with metadata
        self.commit_data_fs_with_metadata(tx_metadata).await?;
        
        // 2. Get committed version
        let version = self.get_committed_transaction_version().await?;
        
        // 3. Record control metadata
        self.record_transaction_metadata(version).await?;
        
        // 4. Execute post-commit hooks (FUTURE)
        self.execute_post_commit_hooks(version).await?;
        
        Ok(())
    }
}
```

### Hook Types

**Replication Hooks**:
- Trigger replication of data filesystem changes
- Ensure remote systems stay synchronized
- Handle replication failures and retries

**Notification Hooks**:  
- Send change notifications to external systems
- Update search indices or caches
- Trigger downstream processing pipelines

**Audit Hooks**:
- Log detailed change information
- Update compliance systems
- Generate change reports

### Hook Ordering and Failure Handling

Hooks will execute in dependency order with failure isolation:
- **Serialized execution**: One hook at a time to prevent conflicts
- **Failure isolation**: Hook failures don't affect other hooks
- **Retry mechanisms**: Failed hooks can be retried with exponential backoff
- **Hook metadata**: Track hook execution state in control filesystem

## Integration with DuckPond

### CLI Integration

Commands use steward for all filesystem modifications:

```rust
// pond init
let ship = Ship::initialize_new_pond(&pond_path, args).await?;

// pond copy file1 file2  
let mut ship = Ship::open_existing_pond(&pond_path).await?;
ship.begin_transaction_with_args(args).await?;
// ... perform copy operation ...
ship.commit_transaction().await?;
```

### Factory System Integration

Dynamic filesystem objects work within steward transactions:
- **CSV directories**: Present CSV files as queryable Arrow tables
- **SQL-derived nodes**: Computed from other filesystem data
- **Hostmount directories**: Mirror host filesystem paths

All factory-generated objects participate in the transaction system.

## Error Handling and Edge Cases

### Empty Transactions

Transactions with no filesystem operations are rejected:
```
Error: Cannot commit transaction with no filesystem operations
```

This prevents spurious version increments and maintains clean audit trails.

### Concurrent Access

While individual transactions are atomic, concurrent steward instances are not currently supported. Future versions may add distributed locking.

### Storage Backend Failures

Delta Lake storage failures are propagated as `StewardError::DeltaLake` errors, allowing callers to implement appropriate retry logic.

## Performance Considerations

### Transaction Overhead

Each transaction requires:
- **Data filesystem commit**: Delta Lake write with metadata
- **Version lookup**: Fresh Delta Lake table read
- **Control filesystem commit**: Metadata file write

This overhead is acceptable for command-level transactions but may need optimization for high-frequency operations.

### Recovery Performance

Recovery scanning is linear in the number of versions:
- **O(n) scan**: Check each version for missing metadata
- **Batching opportunities**: Future versions could batch recovery operations

## Testing Strategy

### Unit Tests

- **Transaction lifecycle**: Begin, operate, commit sequences
- **Version mapping**: Delta Lake version to metadata file mapping
- **Error conditions**: Empty transactions, missing metadata

### Integration Tests

- **Crash recovery**: Simulate failures at various points
- **Dual filesystem consistency**: Verify both filesystems stay synchronized
- **Command integration**: Test with actual CLI commands

### Delta Lake Tests

Dedicated tests verify Delta Lake version behavior:
- **Version numbering**: Confirm versions start at 0 and increment
- **Handle consistency**: Verify committed changes are immediately visible
- **Fresh handle behavior**: Confirm new handles see all commits

## Future Enhancements

### Distributed Coordination

Add support for multiple concurrent steward instances:
- **Distributed locking**: Prevent concurrent modifications
- **Consensus protocols**: Ensure consistent ordering across instances

### Advanced Hook System

- **Hook dependencies**: Define execution order requirements
- **Conditional hooks**: Execute based on transaction content
- **Hook rollback**: Undo hook effects on failure

### Performance Optimizations  

- **Batch operations**: Group multiple operations into single transactions
- **Lazy recovery**: Delay recovery until next write operation
- **Metadata caching**: Cache frequently accessed metadata

### Monitoring and Observability

- **Transaction metrics**: Commit rates, failure rates, recovery frequency
- **Hook performance**: Execution times and failure patterns
- **Storage utilization**: Delta Lake growth patterns

## Conclusion

The Steward architecture provides a robust foundation for coordinated transactional operations across dual Delta Lake instances. Its design prioritizes correctness and recoverability while laying groundwork for future extensibility through post-commit hooks.

The elimination of artificial sequence number mapping and direct use of Delta Lake versions reduces complexity and potential for off-by-one errors. The comprehensive crash recovery system ensures data integrity even in failure scenarios.

This architecture enables DuckPond to provide ACID guarantees across its complex filesystem operations while maintaining the flexibility needed for advanced features like replication and external system integration.
