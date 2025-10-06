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

### Fundamental Table Registration Pattern

**CRITICAL LESSON**: Fundamental system tables should be registered ONCE per transaction in the State constructor, not repeatedly by individual components.

```rust
// ✅ CORRECT - Register fundamental tables in State initialization
impl InnerState {
    async fn new(path: String, table: DeltaTable) -> Result<Self, TLogFSError> {
        let ctx = Arc::new(datafusion::execution::context::SessionContext::new());
        
        // Register fundamental system tables once per transaction
        let node_table = Arc::new(crate::query::NodeTable::new(table.clone()));
        ctx.register_table("nodes", node_table)?;
        
        let directory_func = Arc::new(crate::directory_table_function::DirectoryTableFunction::new(table.clone(), ctx.clone()));
        ctx.register_udtf("directory", directory_func);
        
        ctx.register_table("delta_table", Arc::new(table.clone()))?;
        
        Ok(Self { session_context: ctx, ... })
    }
}
```

```rust
// ❌ WRONG - Repeated table registration causes conflicts
impl NodeTable {
    pub async fn query_records_for_node(&self, ctx: &SessionContext, ...) -> Result<...> {
        // BUG: This fails on second call within same transaction
        ctx.register_table("nodes", Arc::new(self.clone()))?; // "table already exists" error
        
        let sql = "SELECT * FROM nodes WHERE ...";
        ctx.sql(&sql).await?
    }
}
```

## Critical Pattern #3: TableProvider Ownership Chain Management

### The Hierarchical Ownership Rule

**Every TableProvider must maintain a direct chain of unique ownership back to the FS root.** This ensures ephemeral files are handled with single instances and proper parent-child relationships.

```
✅ CORRECT: Hierarchical ownership tree
FS Root → State → Cache → TableProvider (single instance per logical data)
├── owns → Real File → owns → TableProvider
├── owns → Ephemeral SQL-derived File → owns → ViewTable
└── owns → Union Pattern → owns → Multi-URL TableProvider

❌ WRONG: Multiple orphaned paths  
FS Root → Multiple TableProvider instances for same logical data
(breaks ownership chain, creates resource leaks)
```

### TableProvider Creation Patterns

**Use centralized caching and avoid duplicate TableProvider creation:**

```rust
// ✅ CORRECT - Use centralized create_table_provider with caching
pub async fn create_table_provider(
    node_id: NodeID,
    part_id: NodeID,
    state: &State,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // Check cache first - maintains ownership chain
    let cache_key = TableProviderKey::new(node_id, part_id, options.version_selection.clone());
    if let Some(cached_provider) = state.get_table_provider_cache(&cache_key) {
        log::info!("📋 REUSED TableProvider from cache: node_id={}, part_id={}", node_id, part_id);
        return Ok(cached_provider);
    }
    
    // Create new provider with proper ownership chain
    let table_provider = Arc::new(TemporalFilteredListingTable::new(listing_table, min_time, max_time));
    log::info!("📋 CREATED TableProvider: node_id={}, part_id={}, temporal_bounds=({}, {})", 
               node_id, part_id, min_time, max_time);
    
    // Cache for reuse - maintains single ownership
    state.set_table_provider_cache(cache_key, table_provider.clone());
    Ok(table_provider)
}
```

```rust
// ❌ WRONG - Multiple TableProvider creation breaks ownership chain
impl SqlDerivedFile {
    async fn as_table_provider(&self, ...) -> Result<Arc<dyn TableProvider>, TLogFSError> {
        // BUG: Creates individual providers and unions them
        let mut individual_providers = Vec::new();
        for file in multiple_files {
            let provider = file.as_table_provider(node_id, part_id, state).await?; // Duplicate creation!
            individual_providers.push(provider);
        }
        
        // Creates orphaned temporary registrations
        for (i, provider) in individual_providers.into_iter().enumerate() {
            let temp_name = format!("temp_table_{}", i);
            ctx.register_table(&temp_name, provider)?; // Breaks ownership chain!
        }
        
        // Creates union without proper ownership
        let union_sql = "SELECT * FROM temp_table_0 UNION ALL SELECT * FROM temp_table_1";
        // ... creates ViewTable without caching
    }
}
```

### Multi-URL Pattern for Union Operations

**CRITICAL LESSON**: Instead of creating multiple TableProviders and unioning them, use DataFusion's native multi-URL capabilities:

```rust
// ✅ CORRECT - Use multi-URL ListingTable (maintains ownership chain)
impl SqlDerivedFile {
    async fn as_table_provider(&self, ...) -> Result<Arc<dyn TableProvider>, TLogFSError> {
        if queryable_files.len() > 1 {
            // Collect URLs instead of creating individual providers
            let mut urls = Vec::new();
            for (node_id, part_id, _) in queryable_files {
                let url_pattern = VersionSelection::AllVersions.to_url_pattern(&part_id, &node_id);
                urls.push(url_pattern);
            }
            
            // Create single TableProvider with multiple URLs
            let options = TableProviderOptions {
                additional_urls: urls,
                ..Default::default()
            };
            
            log::info!("📋 CREATING multi-URL TableProvider for pattern '{}': {} URLs", pattern_name, urls.len());
            return create_table_provider(dummy_node_id, dummy_part_id, state, options).await;
        }
        // ... single file case
    }
}
```

### Ephemeral File Ownership Management

**Ephemeral files must maintain references to their TableProviders with clear parent-child relationships:**

```rust
// ✅ CORRECT - Ephemeral file maintains ownership chain
impl TemporalReduceSqlFile {
    async fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, state: &State) -> Result<...> {
        log::info!("📋 DELEGATING TemporalReduceSqlFile to inner file: node_id={}, part_id={}", node_id, part_id);
        
        // Maintain ownership chain: TemporalReduceSqlFile → Inner File → TableProvider
        let inner = self.inner.lock().await;
        inner.as_table_provider(node_id, part_id, state).await // Proper delegation
    }
}
```

### TableProvider Lifecycle Monitoring

**Monitor all TableProvider creation with comprehensive logging:**

```rust
// All TableProvider creation points now logged for ownership chain tracking:
log::info!("📋 CREATED TableProvider: node_id={}, part_id={}, ...");        // Main creation
log::info!("📋 REUSED TableProvider from cache: node_id={}, part_id={}", ); // Cache hits  
log::info!("📋 CREATED ViewTable for SQL-derived file: patterns={}, ...");  // SQL views
log::info!("📋 CREATING multi-URL TableProvider: {} URLs", );               // Multi-file
log::info!("📋 DELEGATING to existing TableProvider");                      // Delegations
log::info!("📋 REGISTERING fundamental table '{}' in State constructor");   // System tables
```

### Anti-Pattern: Breaking Ownership Chains

```rust
// ❌ WRONG - Creates multiple providers for same data
for file in pattern_matches {
    // Each creates a separate TableProvider for same logical data
    let provider = file.as_table_provider(node_id, part_id, state).await?;
    providers.push(provider); // Breaks single ownership rule
}

// ❌ WRONG - Temporary table registration bypasses ownership  
ctx.register_table(&temp_name, provider)?; // Creates orphaned registration

// ❌ WRONG - No caching breaks reuse patterns
let provider = Arc::new(SomeTableProvider::new(...)); // Always creates new instance
return Ok(provider); // No ownership chain back to State cache
```

```rust
// ✅ CORRECT - Use pre-registered tables
impl NodeTable {
    pub async fn query_records_for_node(&self, ctx: &SessionContext, ...) -> Result<...> {
        // Table "nodes" already registered in State constructor
        let sql = "SELECT * FROM nodes WHERE ...";
        ctx.sql(&sql).await?
    }
}
```

**Key Insight**: Table registration conflicts ("table already exists" errors) indicate violation of the "ONE SessionContext per transaction" principle. These errors reveal that either:
1. Multiple SessionContexts are being created per transaction (architectural violation)
2. The same table is being registered repeatedly on the same context (implementation bug)

The fix is always to register fundamental tables ONCE in the State constructor.

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

### Anti-Pattern: Repeated Table Registration

```rust
// ❌ Registering the same table multiple times causes conflicts
impl NodeTable {
    pub async fn query_records(&self, ctx: &SessionContext) -> Result<...> {
        ctx.register_table("nodes", Arc::new(self.clone()))?; // First call: OK
        // ... some operations ...
        ctx.register_table("nodes", Arc::new(self.clone()))?; // Second call: FAILS!
        // Error: "Execution error: The table nodes already exists"
    }
}

// ✅ Register fundamental tables once in State constructor
impl InnerState {
    async fn new(path: String, table: DeltaTable) -> Result<Self, TLogFSError> {
        let ctx = Arc::new(SessionContext::new());
        
        // Register once per transaction
        ctx.register_table("nodes", Arc::new(NodeTable::new(table.clone())))?;
        ctx.register_table("delta_table", Arc::new(table.clone()))?;
        
        Ok(Self { session_context: ctx, ... })
    }
}

// Components just use pre-registered tables
impl NodeTable {
    pub async fn query_records(&self, ctx: &SessionContext) -> Result<...> {
        // Table already registered - just use it
        let sql = "SELECT * FROM nodes WHERE ...";
        ctx.sql(&sql).await?
    }
}
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

### The Zero-Tolerance Silent Fallback Rule

**NEVER return default values or continue execution when system errors occur.** The HydroVu modernization revealed multiple instances where silent fallbacks masked critical infrastructure issues.

```rust
// ❌ SILENT FALLBACK ANTI-PATTERN - Masks real problems
match root_wd.list_file_versions(&device_path).await {
    Ok(infos) => infos,
    Err(e) => {
        debug!("FileSeries metadata exists but version data inaccessible: {:?}", e);
        debug!("Starting from epoch due to inaccessible version data");
        return Ok(0); // DANGEROUS: Masks node storage corruption!
    }
}

// ✅ FAIL-FAST PATTERN - Surfaces real problems for diagnosis
let version_infos = root_wd
    .list_file_versions(&device_path)
    .await
    .map_err(|e| {
        steward::StewardError::Dyn(
            format!("Failed to query file versions for device {device_id}: {}", e).into(),
        )
    })?; // Forces addressing of underlying infrastructure issues
```

### Distinguish Business Cases from System Failures

**Critical Lesson from HydroVu**: Not all "missing data" cases are the same. Distinguish between legitimate business scenarios and system failures:

```rust
// ✅ LEGITIMATE BUSINESS CASE - Make visible with positive messaging
match max_timestamp {
    None => {
        info!("New device {device_id}: no existing temporal data found, starting fresh collection from epoch");
        Ok(0) // Expected behavior for new devices
    }
    // ... handle existing data case
}

// ✅ SYSTEM FAILURE - Fail fast with diagnostic context
Err(e) => {
    steward::StewardError::Dyn(
        format!("Infrastructure failure accessing device {device_id} storage: {}", e).into(),
    ) // Requires system-level investigation
}
```

### The External API Design Principle

**CRITICAL INSIGHT**: External crates should never access internal implementation details. The NodeTable removal demonstrated proper API boundaries:

```rust
// ❌ WRONG - External crate accessing internal implementation
// HydroVu directly using NodeTable with OplogEntry field access
let min_time = record.min_event_time; // Direct field access to internal structure

// ✅ CORRECT - External crate using structured API
// HydroVu using TinyFS list_file_versions with FileVersionInfo.extended_metadata
if let Some(metadata) = &version_info.extended_metadata {
    if let Some(min_str) = metadata.get("min_event_time") {
        let min_time = min_str.parse::<i64>()?; // Structured metadata access
    }
}
```

### Dead Code as Architecture Smell Detection

**Dead code often indicates incomplete architectural transitions:**

```rust
// ❌ WARNING SIGN - Placeholder functions indicate incomplete architecture
pub async fn get_temporal_overrides_for_node(
    _persistence: &OpLogPersistence,
    _node_id: &str,
) -> Result<Option<TemporalBounds>, TLogFSError> {
    // TODO: Implement temporal override retrieval once we have proper API access
    Ok(None) // Dead code indicating incomplete implementation
}

// ✅ COMPLETE IMPLEMENTATION - Proper fail-fast temporal override lookup
pub async fn get_temporal_overrides_for_node_id(
    &self,
    node_id: &tinyfs::NodeID,
    part_id: tinyfs::NodeID,
) -> Result<Option<(i64, i64)>, TLogFSError> {
    // Full implementation with proper error handling and context
    // Uses consistent query_records pattern for data access
}
```

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
5. **Fundamental Table Registration**: Register system tables once per transaction in State constructor
6. **TableProvider Ownership Chains**: Maintain hierarchical ownership from FS root to TableProvider
7. **Multi-URL Over Union**: Use DataFusion's native multi-URL capabilities instead of manual unions
8. **Fail-Fast Validation**: Empty schemas and unused parameters indicate architectural issues
9. **Transaction Lifecycle**: Clear begin/commit boundaries with automatic rollback
10. **Zero-Tolerance Silent Fallbacks**: Always fail fast on system errors, distinguish from business cases

## Critical Lessons from Real Implementation Challenges

### HydroVu Modernization: The Silent Fallback Investigation

**Problem**: After removing NodeTable, HydroVu was encountering "Node not found" errors that were being silently masked by fallback logic.

**Root Cause**: The code had multiple silent fallback patterns that made debugging impossible:
```rust
// Silent fallback masked infrastructure corruption
Err(e) => {
    debug!("Starting from epoch due to inaccessible version data");
    return Ok(0); // Dangerous: hides real problems!
}
```

**Solution**: Implemented fail-fast error handling with proper context:
```rust
// Fail-fast exposes real problems for diagnosis  
.map_err(|e| {
    steward::StewardError::Dyn(
        format!("Failed to query file versions for device {device_id}: {}", e).into(),
    )
})?
```

**Key Insight**: **Silent fallbacks are architecture cancer.** They make systems appear to work while masking critical infrastructure failures. Always fail fast and force proper diagnosis of underlying issues.

### External API Design: The NodeTable Boundary Violation

**Problem**: HydroVu was directly accessing NodeTable internals, breaking API boundaries and creating tight coupling.

**Root Cause**: External crates accessing internal implementation details:
```rust
// BAD: Direct access to internal OplogEntry fields
let temporal_range = node_table.temporal_range(); // Returns Option<(i64, i64)>
let min_time = record.min_event_time; // Direct field access
```

**Solution**: Clean external API with structured metadata:
```rust
// GOOD: Structured API through TinyFS
let version_infos = root_wd.list_file_versions(&device_path).await?;
if let Some(metadata) = &version_info.extended_metadata {
    if let Some(min_str) = metadata.get("min_event_time") {
        let min_time = min_str.parse::<i64>()?;
    }
}
```

**Key Insight**: **External crates should never access internal implementation details.** Clean API boundaries prevent coupling and enable internal refactoring without breaking external consumers.

### Business Logic vs System Failures: The Context Distinction

**Problem**: All "missing data" cases were being treated the same way, making it impossible to distinguish between legitimate business scenarios and system failures.

**Solution**: Explicit differentiation with appropriate logging levels:
```rust
// Business case: New device starting collection (expected)
None => {
    info!("New device {device_id}: no existing temporal data found, starting fresh collection from epoch");
    Ok(0)
}

// System failure: Infrastructure problem (unexpected)  
Err(e) => {
    steward::StewardError::Dyn(
        format!("Infrastructure failure accessing device {device_id} storage: {}", e).into(),
    )
}
```

**Key Insight**: **Context matters in error handling.** The same "missing data" symptom can indicate either normal business flow or critical system failure. Make the distinction explicit.

**The Root Cause of Most Bugs**: 
1. **Silent fallback anti-patterns** (masks real infrastructure problems)
2. **Violating external API boundaries** (creates tight coupling and breaks modularity)
3. **Treating business cases and system failures the same** (obscures root cause analysis)
4. **Violating TableProvider ownership chains** (creates duplicate providers, breaks resource management)
5. **Using UNION hacks instead of multi-URL ListingTable** (breaks ownership, creates temporary registrations)
6. Violating NodeID/PartID relationships (breaks partition pruning)
7. Violating single-instance patterns 
8. Operating at the wrong architectural layer
9. Repeated table registration within same transaction
10. Not failing fast on architectural constraint violations

**Critical Insight from Real Debugging Sessions**: The most dangerous bugs are those that appear to work correctly while masking underlying system corruption. **Silent fallbacks prevent proper root cause analysis and allow infrastructure problems to compound over time.**

**For AI Agents**: These patterns are non-negotiable system constraints. Pay special attention to:
1. **NEVER implement silent fallbacks** - always fail fast on system errors while properly handling business cases
2. **Maintain clean API boundaries** - external crates use structured APIs, never internal implementation details  
3. **Distinguish context in error handling** - business cases vs system failures require different responses
4. **Maintain TableProvider ownership chains** - every provider must trace back to FS root through State cache
5. **Use multi-URL ListingTable instead of UNION** - DataFusion handles multiple files natively
6. **Monitor TableProvider creation with logging** - track cache hits/misses and ownership compliance
7. **Never use `node_id` as `part_id` for files** - always resolve parent directory
8. **Register fundamental tables once in State constructor** - never in individual components
9. **Look for "table already exists" errors** - they indicate SessionContext architectural violations
10. **Look for unused `_part_id` parameters** - they indicate incomplete DeltaLake integration
11. **Add fail-fast checks for empty schemas** - they reveal partition pruning issues
12. **Use the TinyFS `resolve_path()` pattern** - it's established throughout the codebase
13. **Remove dead code immediately** - placeholder functions indicate incomplete architecture

The system is designed to fail fast when these patterns are violated, which is intentional and should guide debugging efforts. **Silent fallback patterns are the most dangerous because they make debugging impossible while allowing system corruption to continue undetected.**