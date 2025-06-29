# OpLog Crate Restructure Proposal

## Current Confusion
- `ContentTable` vs `OplogEntryTable` - both deal with "content" but at different abstraction levels
- Similar implementations but different purposes
- Names don't clearly indicate abstraction level
- Mixed responsibilities in single modules

## Proposed Structure

### New Module Organization
```
crates/oplog/src/
├── delta.rs                    # Core Delta Lake types (Record, ForArrow)
├── error.rs                    # Error handling
├── tinylogfs/                  # TinyFS integration (unchanged)
└── query/                      # NEW: DataFusion query interfaces
    ├── mod.rs                  # Public exports
    ├── ipc.rs                  # Generic Arrow IPC queries
    └── operations.rs           # Filesystem operations queries
```

### Renamed Components

#### 1. Generic IPC Queries (`query/ipc.rs`)
**Current**: `ContentTable` & `ContentExec`
**New**: `IpcTable` & `IpcExec`
```rust
/// Generic table for querying arbitrary Arrow IPC data stored in Delta Lake
pub struct IpcTable {
    schema: SchemaRef,
    table_path: String,
    delta_manager: DeltaTableManager,
}

/// Execution plan for IPC data queries
pub struct IpcExec { ... }
```

#### 2. Oplog-Specific Queries (`query/operations.rs`)
**Current**: `OplogEntryTable` & `OplogEntryExec` (removed)
**New**: `OperationsTable` & `OperationsExec`
```rust
/// Table for querying filesystem operations (OplogEntry records)
pub struct OperationsTable {
    table_path: String,
    schema: SchemaRef, // OplogEntry schema
}

/// Execution plan for filesystem operations queries
pub struct OperationsExec { ... }
```

### Clear Abstraction Layers

#### Layer 1: Storage (Delta Lake)
- Raw Record access
- Delta table management
- Transaction handling

#### Layer 2: IPC Deserialization  
- Generic Arrow IPC parsing
- Schema-flexible queries
- Low-level data access

#### Layer 3: Application Semantics
- OplogEntry-specific queries
- Filesystem operation understanding
- High-level business logic

### Usage Examples

```rust
// Generic IPC queries (any Arrow data)
let ipc_table = IpcTable::new(custom_schema, table_path, delta_manager);
ctx.register_table("raw_data", Arc::new(ipc_table))?;

// Filesystem operations queries
let ops_table = OperationsTable::new(table_path);
ctx.register_table("filesystem_ops", Arc::new(ops_table))?;

// SQL examples:
// SELECT * FROM raw_data WHERE field = 'value'          // Generic
// SELECT * FROM filesystem_ops WHERE file_type = 'file' // Oplog-specific
```

## Benefits

1. **Clear Purpose**: Names indicate what each component does
2. **Proper Layering**: Generic → Specific abstraction levels
3. **Maintainable**: Related functionality grouped together
4. **Extensible**: Easy to add new query types
5. **Self-Documenting**: Module structure explains architecture

## Migration Plan

1. Create `query/` module structure
2. Move and rename existing components
3. Update public exports
4. Update tests to use new names
5. Add documentation explaining layers

This makes it clear that:
- `IpcTable` = Generic Arrow data queries
- `OperationsTable` = Filesystem operations queries
- Both serve DataFusion but at different abstraction levels
