# DuckPond System Patterns: Critical Implementation Guidelines

## Overview

This document captures the critical system patterns and subtleties that are essential for successful development in the DuckPond codebase. These patterns emerged from real implementation challenges where architectural misunderstandings led to significant debugging sessions.

**Key Insight**: DuckPond's layered architecture creates subtle but critical constraints that must be understood to avoid common pitfalls. The system is designed around **single-instance patterns** and **low-level filesystem abstractions** that require precise handling.

## Critical Pattern #1: Transaction Guard Lifecycle

### The Single Transaction Guard Rule

**NEVER create multiple transaction guards concurrently.** The system is designed around a single active transaction at any point in time.

```rust
// ❌ WRONG - Multiple concurrent guards
let tx_guard1 = persistence.begin().await?;
let tx_guard2 = persistence.begin().await?; // BUG: Creates conflicts

// ✅ CORRECT - Single guard pattern
let tx_guard = persistence.begin().await?;
// ... do all work within single transaction scope
tx_guard.commit(None).await?;
```

### Transaction Guard Access Patterns

```rust
// Entry point: Always start with transaction guard
let tx_guard = persistence.begin().await?;

// Access state through guard (critical for context sharing)
let state = tx_guard.state()?;

// Access filesystem through guard
let root = tx_guard.root();

// Access DataFusion context through guard
let df_context = tx_guard.session_context().await?;
```

**Why This Matters**: Transaction guards manage Delta Lake versioning, DataFusion context initialization, and filesystem state consistency. Multiple guards create version conflicts and context isolation issues.

## Critical Pattern #2: State Object Management

### The Single State Context Rule

**State objects must be shared, not recreated.** Each transaction has exactly one State instance that coordinates all operations.

```rust
// ❌ WRONG - Creating new state instances
let state1 = State::new(...); // BUG: Isolates context
let state2 = State::new(...); // BUG: Creates conflicts

// ✅ CORRECT - Use transaction guard's state
let tx_guard = persistence.begin().await?;
let state = tx_guard.state()?; // Single shared state
```

### State Access Within TLogFS

```rust
// Inside TLogFS file implementations
impl QueryableFile for MyFile {
    fn as_table_provider(
        &self,
        node_id: tinyfs::NodeID,
        part_id: tinyfs::NodeID,
        state: &State  // This is THE state instance
    ) -> Result<Arc<dyn TableProvider>, anyhow::Error> {
        // Use this state for all operations
        // Never create new State instances
    }
}
```

**Key Insight**: State objects coordinate DataFusion contexts, filesystem metadata, and Delta Lake transactions. Creating multiple State instances breaks this coordination.

## Critical Pattern #3: DataFusion Session Context

### The Single Session Context Rule

**NEVER create new DataFusion SessionContext instances.** Always use the pre-initialized context from the State.

```rust
// ❌ WRONG - Creating new DataFusion contexts
let ctx = SessionContext::new(); // BUG: Loses table registrations

// ❌ WRONG - Even with configuration
let config = SessionConfig::new();
let ctx = SessionContext::new_with_config(config); // BUG: Still isolated

// ✅ CORRECT - Use existing context from State
let state = tx_guard.state()?;
let df_context = state.datafusion_context(); // Pre-initialized context
```

### Context Registration Patterns

```rust
// The State's DataFusion context has pre-registered tables
let df_context = state.datafusion_context();

// Register additional tables through the shared context
df_context.register_table("my_data", table_provider)?;

// Execute queries against the shared context
let df = df_context.sql("SELECT * FROM my_data").await?;
```

**Why This Matters**: The State's DataFusion context has pre-registered system tables, custom ObjectStore configurations, and table providers that are essential for filesystem operations. New contexts lose all this setup.

## Critical Pattern #4: NodeID/PartID Low-Level Access

### Understanding TLogFS File System Level

**Within TLogFS, you are operating at the low-level filesystem layer.** Files are accessed by NodeID/PartID, not by paths.

```rust
// At TLogFS level, files are identified by IDs
fn as_table_provider(
    &self,
    node_id: tinyfs::NodeID,    // Unique node identifier
    part_id: tinyfs::NodeID,    // Part within node
    state: &State               // Shared state context
) -> Result<Arc<dyn TableProvider>, anyhow::Error>
```

### Path vs NodeID Resolution Boundaries

```rust
// ❌ WRONG - Path resolution at TLogFS level
fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
    // Don't do pattern matching here!
    let matches = fs.root().collect_matches("/some/pattern").await?; // BUG
}

// ✅ CORRECT - Use provided NodeID/PartID
fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
    // Use the already-resolved file reference
    if let Some(file) = state.get_file(node_id, part_id)? {
        let table_provider = file.create_table_provider(node_id, part_id, state)?;
        return Ok(table_provider);
    }
}
```

**Critical Distinction**:
- **Factory Level**: Uses patterns to discover and create files
- **File Level**: Uses NodeID/PartID to access already-resolved files

### When Patterns vs When NodeIDs

```rust
// ✅ Pattern usage: During factory creation/discovery
impl TemplateDirectory {
    async fn discover_template_files(&self) -> TinyFSResult<Vec<(PathBuf, Vec<String>)>> {
        let matches = fs.root().collect_matches(&self.pattern).await?; // Correct here
        Ok(matches)
    }
}

// ✅ NodeID usage: During file operations
impl QueryableFile for TemporalReduceSqlFile {
    fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
        // node_id/part_id represent the SOURCE file - use them directly
        let source_file = state.get_file(node_id, part_id)?; // Correct here
    }
}
```

## Architecture Layer Boundaries

### High-Level: Applications & Steward
```rust
// Application level - works with paths and transactions
let tx_guard = steward.begin_transaction().await?;
let file_node = tx_guard.root().lookup("/sensors/data.parquet").await?;
```

### Mid-Level: TinyFS Navigation
```rust
// TinyFS level - type-safe filesystem operations
match file_node {
    Lookup::Found(node_path) => {
        let node_ref = node_path.borrow().await;
        // Convert to TLogFS level...
    }
}
```

### Low-Level: TLogFS Operations
```rust
// TLogFS level - NodeID/PartID and DataFusion integration
impl QueryableFile for MyFile {
    fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
        // Operating at filesystem metadata level
        // No path resolution - use provided IDs
    }
}
```

## Common Anti-Patterns and Fixes

### Anti-Pattern: Multiple Context Creation

```rust
// ❌ Creates isolated contexts
let tx_guard1 = persistence.begin().await?;
let tx_guard2 = persistence.begin().await?;
let ctx1 = SessionContext::new();
let ctx2 = SessionContext::new();

// ✅ Single shared context
let tx_guard = persistence.begin().await?;
let state = tx_guard.state()?;
let df_context = state.datafusion_context();
```

### Anti-Pattern: Path Resolution at Wrong Layer

```rust
// ❌ Pattern matching in file operations
impl QueryableFile for MyFile {
    fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
        let matches = fs.root().collect_matches("/pattern/*").await?; // WRONG LAYER
    }
}

// ✅ Use NodeID/PartID directly
impl QueryableFile for MyFile {
    fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
        let source_file = state.get_file(node_id, part_id)?; // CORRECT LAYER
    }
}
```

### Anti-Pattern: State Recreation

```rust
// ❌ Creating new state instances
let new_state = State::new(persistence.clone());

// ✅ Use transaction guard's state
let state = tx_guard.state()?;
```

## Transaction Lifecycle Best Practices

### Complete Transaction Pattern

```rust
async fn complete_operation() -> Result<(), anyhow::Error> {
    // 1. Single transaction guard
    let tx_guard = persistence.begin().await?;
    
    // 2. Get shared state
    let state = tx_guard.state()?;
    
    // 3. Get shared DataFusion context
    let df_context = state.datafusion_context();
    
    // 4. Do all operations within this context
    let file = state.get_file(node_id, part_id)?;
    let table_provider = file.create_table_provider(node_id, part_id, state)?;
    df_context.register_table("data", table_provider)?;
    
    // 5. Execute queries
    let results = df_context.sql("SELECT * FROM data").await?;
    
    // 6. Commit transaction
    tx_guard.commit(None).await?;
    
    Ok(())
}
```

### Error Recovery Pattern

```rust
async fn recoverable_operation() -> Result<(), anyhow::Error> {
    let tx_guard = persistence.begin().await?;
    
    // Transaction automatically rolls back on Drop if not committed
    match perform_operations(&tx_guard).await {
        Ok(result) => {
            tx_guard.commit(None).await?;
            Ok(result)
        }
        Err(e) => {
            // tx_guard drops here, auto-rollback
            Err(e)
        }
    }
}
```

## Memory and Concurrency Considerations

### Arc<Mutex<>> Patterns in TinyFS

```rust
// TinyFS uses Arc<Mutex<>> for thread safety
let file_handle = file_handle.0.lock().await; // Note: private field access

// Downcast to TLogFS types
if let Some(oplog_file) = file_guard.as_any().downcast_ref::<OpLogFile>() {
    // Now can access DataFusion capabilities
}
```

### Async Context Management

```rust
// Be careful with async boundaries and tokio runtime
let rt = tokio::runtime::Handle::current();
let result = rt.block_on(async {
    // Async operations within sync context
    let fs = FS::new(state.clone()).await?;
    fs.root().await?.collect_matches(pattern).await
})?;
```

## Testing Patterns

### Test Transaction Isolation

```rust
#[tokio::test]
async fn test_with_proper_isolation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let persistence = create_test_persistence(&temp_dir).await;
    
    // Single transaction per test
    let tx_guard = persistence.begin().await.unwrap();
    let state = tx_guard.state().unwrap();
    
    // All operations within single context
    // ...
    
    tx_guard.commit(None).await.unwrap();
}
```

## Debugging Strategies

### Context Validation

```rust
// Add debugging to verify context sharing
println!("DEBUG: State DataFusion context ID: {:p}", state.datafusion_context());
println!("DEBUG: Table registrations: {:?}", 
    state.datafusion_context().catalog("default").unwrap().names());
```

### NodeID/PartID Tracking

```rust
// Trace NodeID flow through system
println!("DEBUG: as_table_provider called with node_id={:?}, part_id={:?}", 
    node_id, part_id);
```

### Schema Validation

```rust
// Verify table provider schemas
let schema = table_provider.schema();
println!("DEBUG: Table provider schema: {:?}", schema);
```

## Conclusion

DuckPond's architecture requires understanding these critical patterns:

1. **Single Instance Rule**: One transaction guard, one state, one DataFusion context per operation
2. **Layer Boundaries**: Patterns at factory level, NodeID/PartID at file level
3. **Context Sharing**: Always use existing contexts, never create new ones
4. **Transaction Lifecycle**: Clear begin/commit boundaries with automatic rollback

**The Root Cause of Most Bugs**: Violating these single-instance patterns or operating at the wrong architectural layer. Understanding these constraints is essential for successful DuckPond development.

**For AI Agents**: These patterns are non-negotiable system constraints. When in doubt, always use existing contexts and operate at the correct architectural layer. The system is designed to fail fast when these patterns are violated, which is intentional and should guide debugging efforts.