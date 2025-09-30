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

## Critical Pattern #4: NodeID/PartID Relationship Rules

### The NodeID/PartID Architectural Constraint

**CRITICAL**: Physical files and directories have different NodeID/PartID relationships that directly impact DeltaLake partition pruning.

```rust
// For PHYSICAL FILES: part_id = parent_directory_node_id
// For DIRECTORIES: part_id = node_id (same as node_id)

// ❌ WRONG - Common mistake that breaks partition pruning
fn discover_source_columns(&self) -> TinyFSResult<Vec<String>> {
    let node_id = self.source_node.id().await;
    let part_id = node_id; // BUG: Files should use parent directory ID
}

// ✅ CORRECT - Proper parent directory resolution
fn discover_source_columns(&self) -> TinyFSResult<Vec<String>> {
    let node_id = self.source_node.id().await;
    
    // Get parent directory's node_id as part_id (standard TinyFS pattern)
    let fs = tinyfs::FS::new(self.context.state.clone()).await?;
    let tinyfs_root = fs.root().await?;
    let source_path_buf = std::path::PathBuf::from(&self.source_path);
    let parent_path = source_path_buf.parent()
        .ok_or_else(|| tinyfs::Error::Other("Source path has no parent directory".to_string()))?;
    
    let parent_node_path = tinyfs_root.resolve_path(parent_path).await?;
    let part_id = match parent_node_path.1 {
        tinyfs::Lookup::Found(parent_node) => parent_node.id().await,
        _ => tinyfs::NodeID::root() // Fallback for root files
    };
}
```

### Why This Matters: DeltaLake Partition Pruning

The NodeID/PartID relationship is fundamental to DeltaLake's partitioning scheme:

```rust
// DeltaLake table structure:
// /table/part_id=<parent-directory-id>/node_id=<file-id>/version=<version>

// Correct relationships enable partition pruning:
// - Files in /sensors/site1/ have part_id = node_id_of(/sensors/site1/)
// - Directory /sensors/site1/ has part_id = node_id_of(/sensors/site1/) (same ID)

// Wrong relationships cause full table scans instead of partition filtering
```

### The TinyFS resolve_path() Pattern

**This is the established pattern throughout the codebase for getting correct part_id:**

```rust
// Standard pattern seen in sql_derived.rs, query/sql_executor.rs, etc.
let parent_path = node_path.dirname();
let parent_node_path = tinyfs_root.resolve_path(&parent_path).await?;
match parent_node_path.1 {
    tinyfs::Lookup::Found(parent_node) => parent_node.id().await,
    _ => {
        log::warn!("Parent directory not found, using root");
        tinyfs::NodeID::root()
    }
}
```

### Understanding TLogFS File System Level

**Within TLogFS, you are operating at the low-level filesystem layer.** Files are accessed by NodeID/PartID, not by paths.

```rust
// At TLogFS level, files are identified by IDs
fn as_table_provider(
    &self,
    node_id: tinyfs::NodeID,    // File's unique identifier
    part_id: tinyfs::NodeID,    // Parent directory's identifier (for files)
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

### Anti-Pattern: Incorrect NodeID/PartID Relationships

```rust
// ❌ Using node_id as part_id for files (breaks partition pruning)
fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
    let wrong_part_id = node_id; // BUG: Files need parent directory ID
    let table_provider = create_table_provider(node_id, wrong_part_id, state)?; // Wrong!
}

// ✅ Use proper parent directory resolution
fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) {
    // part_id should already be correct from caller, but if you need to resolve:
    let parent_part_id = resolve_parent_directory_id(&self.file_path, state).await?;
    let table_provider = create_table_provider(node_id, parent_part_id, state)?; // Correct!
}
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

### Anti-Pattern: Unused Parameters (Indicates Architectural Issues)

```rust
// ❌ Unused parameters suggest incomplete DeltaLake integration
pub async fn create_table_provider(
    node_id: tinyfs::NodeID,
    _part_id: tinyfs::NodeID,  // TODO: Will be used for DeltaLake partition pruning
    state: &State,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // Using _part_id (unused) indicates partition pruning not implemented
}

// ✅ Use all parameters for proper partition pruning
pub async fn create_table_provider(
    node_id: tinyfs::NodeID,
    part_id: tinyfs::NodeID,  // Used for DeltaLake partition pruning
    state: &State,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    let temporal_overrides = get_temporal_overrides_for_node_id(state, &node_id, part_id).await?;
    let url_pattern = options.version_selection.to_url_pattern(&part_id, &node_id);
    // Both node_id AND part_id used for efficient partition access
}
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
// Trace NodeID/PartID relationships for partition pruning debugging
log::debug!("as_table_provider called with node_id={}, part_id={}", node_id, part_id);

// Verify correct relationships
if node_id == part_id {
    log::warn!("node_id equals part_id - this should only happen for directories!");
}

// Trace listing table URLs for partition pruning validation
log::debug!("Creating table provider with URL: tinyfs:///part/{}/node/{}/version/", 
    part_id, node_id);
```

### Schema Validation and Fail-Fast Patterns

```rust
// Verify table provider schemas and fail fast on empty schemas
let schema = table_provider.schema();
log::debug!("Table provider schema fields: {}", schema.fields().len());

if schema.fields().is_empty() {
    return Err(Error::Other(format!(
        "Schema discovery failed: no columns found. \
        This indicates a partition pruning issue. \
        Check part_id ({}) and node_id ({}) are correct.",
        part_id, node_id
    )));
}
```

### DeltaLake Partition Pruning Validation

```rust
// Use debug logging to trace partition pruning effectiveness
use log::debug;

// Before query
debug!("Querying with partition filter: part_id = {} AND node_id = {}", part_id, node_id);

// Check query execution plan for partition filtering
let explain_plan = datafusion_context
    .sql(&format!("EXPLAIN SELECT * FROM table WHERE part_id = '{}' AND node_id = '{}'", 
        part_id, node_id))
    .await?;

// Look for partition pruning in explain output
debug!("Query plan: {:?}", explain_plan);
```

## Critical Pattern #5: Fail-Fast Error Handling

### The Zero-Schema Detection Pattern

**Always fail fast when schema discovery returns empty results.** This indicates partition pruning or node resolution issues.

```rust
// ✅ Fail-fast pattern for schema validation
let schema = table_provider.schema();
let columns: Vec<String> = schema.fields()
    .iter()
    .map(|field| field.name().clone())
    .filter(|name| name != &timestamp_column)
    .collect();

// Fail fast with detailed error information
if columns.is_empty() {
    return Err(Error::Other(format!(
        "Schema discovery failed: no columns found in source file '{}'. \
        Table provider returned schema with {} total fields. \
        This indicates a problem with partition pruning or node resolution. \
        Check that part_id ({}) represents the parent directory and node_id ({}) represents the file. \
        Debug URLs: tinyfs:///part/{}/node/{}/version/",
        file_path, schema.fields().len(), part_id, node_id, part_id, node_id
    )));
}
```

### The Unused Parameter Detection Pattern

**Unused parameters prefixed with `_` often indicate incomplete architectural integration.**

```rust
// ❌ Warning sign - unused parameters suggest missing functionality
fn some_function(_part_id: NodeID) {  // TODO: Will be used later
    // If part_id is unused, partition pruning likely not implemented
}

// ✅ Address architectural gaps immediately
fn some_function(part_id: NodeID) {  // Actually use the parameter
    let url_pattern = create_url_pattern(&part_id, &node_id);
    let temporal_overrides = get_overrides(&node_id, part_id).await?;
}
```

## Conclusion

DuckPond's architecture requires understanding these critical patterns:

1. **Single Instance Rule**: One transaction guard, one state, one DataFusion context per operation
2. **NodeID/PartID Relationships**: Files use parent directory ID as part_id, directories use same ID for both
3. **Layer Boundaries**: Patterns at factory level, NodeID/PartID at file level  
4. **Context Sharing**: Always use existing contexts, never create new ones
5. **Fail-Fast Validation**: Empty schemas and unused parameters indicate architectural issues
6. **Transaction Lifecycle**: Clear begin/commit boundaries with automatic rollback

**The Root Cause of Most Bugs**: 
1. Violating NodeID/PartID relationships (breaks partition pruning)
2. Violating single-instance patterns 
3. Operating at the wrong architectural layer
4. Not failing fast on architectural constraint violations

**Critical Insight from Investigation**: The most subtle and dangerous bugs arise from incorrect NodeID/PartID relationships. These bugs manifest as:
- Empty schemas from table providers
- Full table scans instead of partition pruning  
- "0 columns discovered" errors
- Listing table providers finding no matching nodes

**For AI Agents**: These patterns are non-negotiable system constraints. Pay special attention to:
1. **Never use `node_id` as `part_id` for files** - always resolve parent directory
2. **Look for unused `_part_id` parameters** - they indicate incomplete DeltaLake integration
3. **Add fail-fast checks for empty schemas** - they reveal partition pruning issues
4. **Use the TinyFS `resolve_path()` pattern** - it's established throughout the codebase

The system is designed to fail fast when these patterns are violated, which is intentional and should guide debugging efforts.