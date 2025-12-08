# Provider Crate Migration Report

## Executive Summary

This document analyzes the feasibility and approach for migrating factory infrastructure from `tlogfs` to the `provider` crate. The goal is to enable in-memory testing of factories and create cleaner architectural boundaries.

**Key Finding**: Migration is feasible with State replaced by a trait abstraction, but requires careful handling of DataFusion SessionContext and dynamic node caching.

---

## Current State Analysis

### Existing Crates

1. **tinyfs**: Core filesystem abstractions (Node, FileID, EntryType, PersistenceLayer trait)
2. **tlogfs**: OpLog-based persistence implementation + all factories
3. **provider**: Minimal (URL-based file access, null_padding, scope_prefix)
4. **utilities**: Minimal (banner, perf_trace)

### Factory Inventory

**7 Production Factories:**
1. `dynamic_dir.rs` → Directory (composes other factories)
2. `template_factory.rs` → Directory (Tera template expansion)
3. `temporal_reduce.rs` → Directory (time-bucketed aggregations)
4. `sql_derived.rs` → 2 File factories (table mode + series mode)
5. `timeseries_join.rs` → File (multi-source SQL joins with temporal alignment)
6. `timeseries_pivot.rs` → File (time-series reshaping)
7. `remote_factory.rs` → Executable (S3 backup/restore)

**Test Fixtures:**
- `test_factory.rs`: 3 test factories (test-exec, test-file, test-dir)

---

## Dependency Analysis

### FactoryContext Dependencies

```rust
pub struct FactoryContext {
    pub state: State,              // ← PRIMARY BLOCKER
    pub file_id: FileID,           // ✅ Already in tinyfs
    pub pond_metadata: Option<PondMetadata>,  // Could move
}
```

### State Usage Patterns

**State methods used by factories:**

1. **session_context()** - Used by ALL DataFusion-based factories
   - Returns `Arc<SessionContext>` with registered TinyFS ObjectStore
   - Required for SQL execution, schema inference, table scanning
   - Used 16 times across sql_derived, timeseries_join, temporal_reduce, file_table

2. **table()** - Used by remote_factory only (2 usages)
   - Returns `DeltaTable` for direct Delta operations
   - Needed for bundle creation/restoration

2. ~~**get/set_dynamic_node_cache()**~~ - ✅ **REMOVED** 
   - Previously used by dynamic_dir for transaction-scoped caching
   - Now handled by CachingPersistence decorator (no State dependency)

4. **get/set_template_variables()** - Used by template_factory and export stages
   - CLI variable expansion for Tera templates
   - Mutable shared state for export data (updated between stages)
   - Controlled mutation via getter/setter pattern (not direct mutex access)

5. **table_provider_cache** - Used internally by file_table
   - Caches ListingTable instances to avoid schema re-inference
   - Performance optimization

### QueryableFile Trait

```rust
#[async_trait]
pub trait QueryableFile: File {
    async fn as_table_provider(
        &self,
        id: FileID,
        state: &crate::persistence::State,  // ← Uses State
    ) -> Result<Arc<dyn TableProvider>, TLogFSError>;
}
```

**Current implementations:**
- OpLogFile (physical files)
- SqlDerivedFile
- TimeseriesJoinFile
- TimeseriesPivotFile

---

## Proposed Migration Strategy

### Phase 1: Define Provider Context ✅ **COMPLETE**

~~Create concrete context struct in `provider` crate:~~

**Status**: ProviderContext implemented in tinyfs with internal trait-based injection:

```rust
// provider/src/context.rs

/// Internal trait for DataFusion session access
#[async_trait]
pub trait SessionProvider: Send + Sync {
    async fn session_context(&self) -> Result<Arc<SessionContext>>;
}

/// Internal trait for template variable management
pub trait TemplateVariableProvider: Send + Sync {
    fn get_template_variables(&self) -> Result<HashMap<String, Value>>;
    fn set_template_variables(&self, vars: HashMap<String, Value>) -> Result<()>;
}

/// Internal trait for TableProvider caching
pub trait TableProviderCache: Send + Sync {
    fn get(&self, key: &dyn Any) -> Option<Arc<dyn TableProvider>>;
    fn set(&self, key: Box<dyn Any + Send + Sync>, provider: Arc<dyn TableProvider>) -> Result<()>;
}

/// Provider context - concrete struct holding implementation details
/// Implementation injected via trait objects (no Arc<dyn ProviderContext>)
#[derive(Clone)]
pub struct ProviderContext {
    session: Arc<dyn SessionProvider>,
    template_vars: Arc<dyn TemplateVariableProvider>,
    table_cache: Arc<dyn TableProviderCache>,
}

/// Factory context for creating dynamic nodes
pub struct FactoryContext {
    pub context: ProviderContext,  // ← Direct ownership, not Arc<dyn>
    pub file_id: FileID,
    pub pond_metadata: Option<PondMetadata>,
}
```

**tlogfs implements the internal traits:**

```rust
// tlogfs/src/persistence.rs

#[async_trait]
impl SessionProvider for State {
    async fn session_context(&self) -> Result<Arc<SessionContext>> {
        // Existing implementation
        Ok(self.inner.lock().await.session_context.clone())
    }
}

impl TemplateVariableProvider for State {
    fn get_template_variables(&self) -> Result<HashMap<String, Value>> {
        Ok(self.template_variables.lock().unwrap().clone())
    }
    
    fn set_template_variables(&self, vars: HashMap<String, Value>) -> Result<()> {
        *self.template_variables.lock().unwrap() = vars;
        Ok(())
    }
}

impl TableProviderCache for State {
    fn get(&self, key: &dyn Any) -> Option<Arc<dyn TableProvider>> {
        // Downcast key and query cache
        let key = key.downcast_ref::<TableProviderKey>()?;
        self.table_provider_cache.lock().unwrap().get(key).cloned()
    }
    
    fn set(&self, key: Box<dyn Any + Send + Sync>, provider: Arc<dyn TableProvider>) -> Result<()> {
        // Downcast and insert
        let key = *key.downcast::<TableProviderKey>().map_err(|_| Error::InvalidCacheKey)?;
        self.table_provider_cache.lock().unwrap().insert(key, provider);
        Ok(())
    }
}

// State can now create ProviderContext:
impl State {
    pub fn as_provider_context(&self) -> ProviderContext {
        ProviderContext::new(
            Arc::new(self.clone()) as Arc<dyn SessionProvider>,
            Arc::new(self.clone()) as Arc<dyn TemplateVariableProvider>,
            Arc::new(self.clone()) as Arc<dyn TableProviderCache>,
        )
    }
}
```

### Phase 2: Move Core Infrastructure to Provider

**Move to provider:**
- `factory.rs` → `provider/src/registry.rs`
  - DynamicFactory struct
  - FactoryRegistry
  - register_dynamic_factory! macro
  - register_executable_factory! macro
- `FactoryContext` (with ProviderContext abstraction)
- `FactoryCommand`, `ExecutionMode`, `ExecutionContext`
- `PondMetadata`, `PondUserMetadata`

**Keep in tlogfs:**
- State implementation of ProviderContext
- OpLog-specific transaction handling
- TinyFS ObjectStore integration

### Phase 3: Migrate Factories by Category

#### Directories → `provider/src/directory/`
- `dynamic.rs` (from dynamic_dir.rs)
- `template.rs` (from template_factory.rs)  
- `temporal_reduce.rs` (unchanged name)

#### Files → `provider/src/file/`
- `sql_derived_table.rs` (extract from sql_derived.rs)
- `sql_derived_series.rs` (extract from sql_derived.rs)
- `timeseries_join.rs` (unchanged name)
- `timeseries_pivot.rs` (unchanged name)

#### Commands → `provider/src/command/`
- `remote.rs` (from remote_factory.rs)

#### Test Fixtures → `provider/src/testing/`
- `test_factories.rs` (from test_factory.rs)

### Phase 4: Move QueryableFile

```rust
// provider/src/queryable.rs

#[async_trait]
pub trait QueryableFile: File {
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &ProviderContext,  // ← Concrete struct, not trait object!
    ) -> Result<Arc<dyn TableProvider>>;
}
```

---

## Complications & Solutions

### 1. **SessionContext Lifecycle & Mutability**

**Problem**: SessionContext must be shared across all operations in a transaction to:
- Avoid ObjectStore registry conflicts
- Ensure consistent configuration
- Enable query optimization across factories
- Allow factories to register new table providers dynamically

**Solution**: 
- SessionProvider trait returns `Arc<SessionContext>` (cloneable reference)
- SessionContext is thread-safe by design - handles internal synchronization
- No external Mutex needed - factories can directly call `register_table()` etc.
- tlogfs State implements SessionProvider, provides ObjectStore registration
- ProviderContext holds Arc<dyn SessionProvider> internally (implementation detail)
- Factories see clean API: `context.session_context().await` (no trait bounds needed)

### 2. **Delta Table Access (remote_factory)**

**Problem**: remote_factory needs `DeltaTable` for bundle operations

**Solution**: 
- Option A: Add `delta_table()` method to ProviderContext (specific to tlogfs)
- Option B: Move bundle operations to steward layer (better separation)
- **Recommendation**: Option B - bundles are transaction-level concerns, not factory concerns

### 3. **Dynamic Node Caching**

**Status**: ✅ **RESOLVED** - No longer a migration obstacle

**Previous Problem**: DynamicDirDirectory needed State's dynamic_node_cache to avoid recreation

**Solution Implemented**: 
- Created `CachingPersistence` decorator in tinyfs that caches ALL nodes by FileID
- Removed `State.dynamic_node_cache` and `DynamicNodeKey` entirely
- Caching now happens transparently at the persistence layer
- No cache-related methods needed in ProviderContext trait

**Impact**: Simplifies migration - ProviderContext doesn't need to handle caching concerns

### 4. **Template Variables Mutability**

**Problem**: Template factory needs CLI variable expansion state that changes during pond sessions
- Export stages add data to variables
- Templates need to read current state
- Need controlled mutation (not direct Arc<Mutex> access in factories)

**Solution**: 
- TemplateVariableProvider trait provides `get_template_variables()` and `set_template_variables()`
- Internal locking managed by provider implementation (State uses Mutex)
- ProviderContext wraps Arc<dyn TemplateVariableProvider> internally
- Factories call `context.get_template_variables()` - no trait bounds needed
- Test implementations can use simple HashMap without actual locking

### 5. **Error Types**

**Problem**: Factories currently use TLogFSError

**Solution**: 
- Define ProviderError in provider crate
- Convert between tinyfs::Error, ProviderError, and TLogFSError at boundaries
- **Better**: Use tinyfs::Error everywhere since it's already generic

### 6. **DynamicNodeKey**

**Status**: ✅ **RESOLVED** - No longer needed

**Previous Problem**: DynamicNodeKey (compound key of PartID + entry_name) was defined in tlogfs

**Solution Implemented**: 
- Removed DynamicNodeKey entirely as part of caching refactor
- CachingPersistence uses FileID directly (simpler, more universal)
- No need to migrate this type to provider crate

**Impact**: One less type to move during migration

### 7. **TableProviderKey & Caching**

**Problem**: TableProvider cache is in State for performance

**Solution**: ✅ **IMPLEMENTED**
- TableProviderCache trait provides `get()` and `set()` methods
- Uses `&dyn Any` for cache keys (allows different key types)
- State implements TableProviderCache, downcasts keys to TableProviderKey
- ProviderContext wraps Arc<dyn TableProviderCache> internally
- Factories call `context.get_table_provider_cache()` - type-safe at usage site

---

## Dependency Graph

### After Migration:

```
tinyfs (core abstractions)
  ↓
provider (factories + ProviderContext trait)
  ↓
tlogfs (OpLog persistence + ProviderContext impl)
  ↓
steward (transaction orchestration)
```

### New Dependencies for Provider Crate:

```toml
[dependencies]
tinyfs = { path = "../tinyfs" }
datafusion = { workspace = true }
arrow = { workspace = true }
async-trait = { workspace = true }
linkme = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde_yaml = { workspace = true }
tera = { workspace = true }
tokio = { workspace = true }
futures = { workspace = true }
log = { workspace = true }
# ... other factory-specific deps
```

### Utilities Crate Extensions:

Could move to utilities (no tlogfs/tinyfs dependency):
- Pattern matching utilities
- CLI parsing helpers
- Common serialization helpers

---

## Testing Benefits

### In-Memory Factory Testing

With ProviderContext as a concrete struct, we can create mock trait implementations:

```rust
// provider/tests/mock_context.rs

struct MockSessionProvider {
    session: Arc<SessionContext>,
}

#[async_trait]
impl SessionProvider for MockSessionProvider {
    async fn session_context(&self) -> Result<Arc<SessionContext>> {
        Ok(self.session.clone())
    }
}

struct MockTemplateVars {
    vars: Arc<Mutex<HashMap<String, Value>>>,
}

impl TemplateVariableProvider for MockTemplateVars {
    fn get_template_variables(&self) -> Result<HashMap<String, Value>> {
        Ok(self.vars.lock().unwrap().clone())
    }
    
    fn set_template_variables(&self, vars: HashMap<String, Value>) -> Result<()> {
        *self.vars.lock().unwrap() = vars;
        Ok(())
    }
}

struct MockTableCache;

impl TableProviderCache for MockTableCache {
    fn get(&self, _key: &dyn Any) -> Option<Arc<dyn TableProvider>> { None }
    fn set(&self, _key: Box<dyn Any + Send + Sync>, _provider: Arc<dyn TableProvider>) -> Result<()> { Ok(()) }
}

// Create mock ProviderContext:
let context = ProviderContext::new(
    Arc::new(MockSessionProvider { session }),
    Arc::new(MockTemplateVars { vars }),
    Arc::new(MockTableCache),
);

// Note: Node caching is handled by CachingPersistence at FS layer
// Mock just needs to wrap MemoryPersistence with CachingPersistence
```

**Enables:**
- Fast unit tests for SQL-derived factories without OpLog
- Parallel test execution (no shared Delta table)
- Property-based testing of factory logic
- Easier debugging (no transaction complexity)

---

## Migration Risks

### High Risk
1. **SessionContext registration**: Must ensure ObjectStore is registered correctly in all contexts
2. **Transaction boundaries**: Factory operations must still respect transaction boundaries

### Medium Risk
1. **Error handling**: Converting between error types may lose information
2. **Test coverage**: Need comprehensive tests before/after migration
3. **Dependency cycles**: Careful ordering required to avoid circular deps

### Low Risk
1. **File moves**: Mechanical refactoring, easy to verify
2. **Naming**: Clear module structure proposed
3. **Documentation**: Well-defined interfaces

---

## Validation Plan

### Pre-Migration
1. ✅ All 94 tlogfs tests passing
2. ✅ Export script working (333 files)
3. ✅ EntryType bugs fixed

### During Migration
1. Create provider traits first, verify compilation
2. Implement MockProviderContext, write simple test
3. Migrate one directory factory (template), verify tests
4. Migrate one file factory (sql_derived_table), verify tests
5. Continue incrementally

### Post-Migration
1. All existing tests still pass
2. New in-memory tests for each factory
3. Integration tests using tlogfs implementation
4. Performance benchmarks unchanged

---

## Timeline Estimate

- **Phase 1** (ProviderContext): ✅ **COMPLETE** - Concrete struct with direct field access
- **Phase 2** (Infrastructure): ✅ **COMPLETE** - Registry, FactoryContext, error types
- **Phase 3** (Factory migration): ✅ **COMPLETE** - dynamic_dir and test_factory moved
- **Phase 4** (QueryableFile): ✅ **COMPLETE** - All 5 implementations migrated, old trait deleted
- **Phase 5** (Table infrastructure): ✅ **COMPLETE** - VersionSelection, TemporalFilteredListingTable moved
- **Phase 6** (sql_derived readiness): 🔄 **IN PROGRESS** - Need TableProviderOptions + create_table_provider abstraction
- **Testing & Polish**: Ongoing (423 tests passing)

**Progress**: ~85% complete - Core infrastructure done, most support moved, final sql_derived blockers remain

---

## Recommendation

✅ **PROCEED** with migration

**Rationale:**
1. ProviderContext is a concrete struct with clean API (no Arc<dyn> in public interface)
2. Implementation injection via internal trait objects (SessionProvider, TemplateVariableProvider, TableProviderCache)
3. QueryableFile naturally belongs with factories
4. Clear architectural improvement (better boundaries)
5. Enables in-memory testing (significant benefit)
6. Risks are manageable with incremental approach
7. No circular dependencies in proposed structure
8. ✅ **NEW**: Caching refactor already completed - one less migration concern
9. ✅ **NEW**: Concrete struct design eliminates trait object complexity for factory authors

**Key Success Factors:**
1. ProviderContext as concrete struct with internal traits (simpler for factories)
2. Use tinyfs::Error throughout (avoid error proliferation)
3. Keep remote_factory bundle operations in steward layer
4. Write comprehensive tests for mock trait implementations
5. Migrate incrementally with validation at each step
6. Ensure State trait implementations are efficient (Arc clones are cheap)

---

## Critical Design Issue: Temporal Bounds Architecture

### Current Problem

**Temporal bounds are implemented incorrectly in the State/persistence layer**, causing architectural confusion:

1. **Single-file special case**: When processing one file, we use its FileID to look up temporal bounds
2. **Multi-file hack**: When processing multiple files, we use `FileID::root()` as a dummy, which "accidentally works" because root has no FileSeries records
3. **Misleading semantics**: Temporal bounds are per-FileSeries metadata, not per-query metadata
4. **Wrong abstraction layer**: `create_table_provider()` queries State for temporal bounds, coupling table creation to tlogfs implementation

### Root Cause

Temporal bounds were made first-class in `OplogEntry` (separate columns `min_time`/`max_time`) to avoid parsing map-valued metadata columns for performance. This was a **tlogfs optimization** that leaked into the **tinyfs abstraction**.

**The confusion:**
- Temporal bounds ARE metadata (property of a FileSeries node)
- tlogfs stores them separately for performance (valid optimization)
- But the API treats them as a State-level concern (wrong layer)

### Correct Architecture

**Temporal bounds should be node-level metadata in tinyfs:**

```rust
// tinyfs/src/persistence.rs
pub trait PersistenceLayer {
    // Existing
    async fn get_metadata(&self, id: FileID) -> Result<NodeMetadata>;
    
    // NEW: Temporal bounds are first-class for FileSeries
    async fn get_temporal_bounds(&self, id: FileID) -> Result<Option<(i64, i64)>>;
}

// tinyfs/src/metadata.rs
pub struct NodeMetadata {
    pub version: u64,
    pub size: Option<u64>,
    pub sha256: Option<String>,
    pub entry_type: EntryType,
    pub timestamp: i64,
    // NOT here - handled separately via get_temporal_bounds()
}
```

**Why separate from NodeMetadata map:**
1. **Performance**: Commonly used for Parquet filtering (deserializing map is expensive)
2. **Type safety**: `Option<(i64, i64)>` vs parsing JSON
3. **Query optimization**: Can index temporal bounds for range queries

**Implementation strategy:**
- tinyfs trait defines the API
- tlogfs implements it efficiently (OplogEntry columns)
- MemoryPersistence implements it simply (HashMap<FileID, (i64, i64)>)

### Migration Impact

**file_table::create_table_provider() signature change:**

```rust
// BEFORE (tlogfs-coupled)
pub async fn create_table_provider(
    file_id: FileID,
    state: &crate::persistence::State,  // ← Concrete tlogfs type
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>>

// AFTER (tinyfs abstraction)
pub async fn create_table_provider(
    context: &ProviderContext,  // ← Has PersistenceLayer
    options: TableProviderOptions,  // ← Contains file_ids + temporal bounds
) -> Result<Arc<dyn TableProvider>>

// TableProviderOptions expanded:
pub struct TableProviderOptions {
    pub version_selection: VersionSelection,
    pub additional_urls: Vec<String>,
    
    // NEW: Explicit temporal bounds per file
    // - None = query from persistence for the file_id
    // - Some(bounds_vec) = use provided bounds (one per URL)
    pub temporal_bounds: Option<Vec<TemporalBounds>>,
}

pub struct TemporalBounds {
    pub file_id: FileID,  // Which file these bounds apply to
    pub min_time: i64,
    pub max_time: i64,
}
```

**Call sites update:**

```rust
// Single file (sql_derived.rs)
let temporal_bounds = context.persistence
    .get_temporal_bounds(file_id)
    .await?
    .map(|(min, max)| vec![TemporalBounds { file_id, min_time: min, max_time: max }]);

let options = TableProviderOptions {
    version_selection: VersionSelection::LatestVersion,
    additional_urls: vec![],
    temporal_bounds,
};

let provider = create_table_provider(context, options).await?;

// Multi-file (sql_derived.rs)
let mut temporal_bounds = Vec::new();
for (file_id, url) in file_urls {
    if let Some((min, max)) = context.persistence.get_temporal_bounds(file_id).await? {
        temporal_bounds.push(TemporalBounds { file_id, min_time: min, max_time: max });
    }
}

let options = TableProviderOptions {
    version_selection: VersionSelection::AllVersions,
    additional_urls: urls,
    temporal_bounds: Some(temporal_bounds),  // Explicit bounds for each file
};

let provider = create_table_provider(context, options).await?;
```

**No more dummy FileID, no more "accidentally works", proper semantics.**

### Benefits

1. ✅ **Correct abstraction**: Temporal bounds are node metadata, accessed via PersistenceLayer trait
2. ✅ **Testable**: MemoryPersistence can implement get_temporal_bounds() for in-memory testing
3. ✅ **No dummy FileID**: Multi-file queries explicitly pass bounds for each file
4. ✅ **Clear semantics**: TemporalBounds struct documents which file each range applies to
5. ✅ **Decoupled**: file_table can move to provider crate (no tlogfs State dependency)
6. ✅ **Performance**: tlogfs keeps efficient OplogEntry columns, just accessed via trait method
7. ✅ **Flexible**: Future persistence implementations can optimize as needed

### Migration Steps

1. **Add to tinyfs::PersistenceLayer trait**:
   ```rust
   async fn get_temporal_bounds(&self, id: FileID) -> Result<Option<(i64, i64)>>;
   ```

2. **Implement in tlogfs::State**:
   - Rename existing `get_temporal_overrides_for_node_id()` → impl for trait method
   - No logic changes, just API alignment

3. **Implement in tinyfs::MemoryPersistence**:
   ```rust
   temporal_bounds: Arc<RwLock<HashMap<FileID, (i64, i64)>>>,
   ```

4. **Update TableProviderOptions**:
   - Add `temporal_bounds: Option<Vec<TemporalBounds>>` field
   - Update create_table_provider() to use provided bounds or query persistence

5. **Update sql_derived.rs call sites**:
   - Single file: query temporal bounds, pass in options
   - Multi file: query bounds for each file, pass vector in options

6. **Move file_table to provider crate**:
   - No longer depends on tlogfs::State
   - Can now be tested with MemoryPersistence

## Open Questions

1. **Bundle operations**: Should remote_factory stay in tlogfs or move with abstraction?
   - **Recommendation**: Keep in tlogfs, it's transaction-level not factory-level

2. **PondMetadata**: Provider or tlogfs?
   - **Recommendation**: Provider - it's metadata about the pond, not persistence

3. **ConfigFile**: Where does it live?
   - **Recommendation**: Provider - it's a factory artifact

---

## Next Steps

1. Review this report with team
2. Create GitHub issue with phase breakdown
3. Start with Phase 1 (trait definition) as proof-of-concept
4. Get approval on ProviderContext API before proceeding
5. Begin incremental migration

---

## Recent Updates

### 2025-12-06: ProviderContext Design Finalized

**What Changed:**
- ProviderContext is now a concrete struct (not a trait)
- Three internal traits for implementation injection:
  - SessionProvider (DataFusion access)
  - TemplateVariableProvider (CLI variables)
  - TableProviderCache (performance optimization)
- FactoryContext holds `ProviderContext` directly (no Arc<dyn>)
- State implements the three internal traits

**Benefits:**
- ✅ No trait objects in public API (simpler for factory authors)
- ✅ Clean implementation injection (Arc<dyn> internal only)
- ✅ Cloneable ProviderContext (cheap Arc clones)
- ✅ Easy to mock for testing (implement internal traits)
- ✅ Type-safe API at call sites (no trait bounds on factory functions)

**Status**: Design complete, ready for implementation

---

### 2025-12-03: Caching Refactor Completed

**What Changed:**
- Implemented `CachingPersistence` decorator in tinyfs (wraps any PersistenceLayer)
- Removed `State.dynamic_node_cache` and `DynamicNodeKey` from tlogfs
- Simplified `dynamic_dir.rs` by removing manual cache management (~30 lines)
- All node caching now unified and transparent at persistence layer

**Migration Impact:**
- ✅ ProviderContext simpler (no node cache methods needed)
- ✅ No DynamicNodeKey type to migrate
- ✅ Reduced timeline estimate by ~2-3 days
- ✅ Lower risk (cache semantics already validated)
- ✅ Cleaner API surface for factory developers

**Status**: All tests passing, production-ready

---

### 2025-12-06: Phase 4 Complete - QueryableFile Migrated to Provider

**What Changed:**
- ✅ **Deleted old tlogfs QueryableFile trait** (`query/queryable_file.rs`)
- ✅ **Migrated all 5 implementations** to use `provider::QueryableFile`:
  - `OpLogFile` - Basic TLogFS file implementation
  - `SqlDerivedFile` - SQL-derived tables (factory-created)
  - `TimeseriesJoinFile` - Multi-source temporal joins (factory-created)
  - `TimeseriesPivotFile` - Time-series pivoting (factory-created)
  - `TemporalReduceSqlFile` - Temporal aggregations (directory factory)
- ✅ **Updated all call sites** (production + 11 test functions):
  - `sql_executor.rs` - added `state.as_provider_context()` calls
  - `export.rs` - added `state.as_provider_context()` call
  - `temporal_reduce.rs` - both production and test code updated
  - All test functions use `state.as_provider_context()` pattern
- ✅ **Enhanced error handling**:
  - Added `DataFusionError(#[from] datafusion::error::DataFusionError)` to provider::Error
  - Added `TLogFS(String)` variant for tlogfs-specific errors
  - Added `From<TLogFSError>` impl for automatic conversions
  - Eliminates most manual `.map_err()` calls

**Architecture - Generic Downcast Pattern:**
- `provider::QueryableFile` trait extends `File` (can use `as_any()`)
- Single helper function `try_as_queryable_file()` in `sql_derived.rs`
- Uses generic downcast: tries each concrete QueryableFile type
- Works for both factory-created and non-factory files
- No need for factory registry pattern (simpler, cleaner)
- Type-safe: compiler ensures all QueryableFile impls are also File

**Why Generic Downcast is Better:**
- ✅ Works for OpLogFile and TemporalReduceSqlFile (no factories)
- ✅ Works for factory-created files (SqlDerivedFile, etc.)
- ✅ Centralized in one function with clear documentation
- ✅ No complex factory registration needed
- ✅ Easy to add new QueryableFile types (just add downcast check)

**Testing:**
- ✅ **87 tests passing** (was 85, re-enabled 2 disabled tests)
- ✅ Re-enabled `test_create_dynamic_directory_path` (test-dir factory)
- ✅ Re-enabled `test_dynamic_node_entry_type_validation` (test-dir factory)
- ✅ Zero compilation warnings
- ✅ All QueryableFile call sites updated
- ✅ Old trait completely removed from codebase
- ✅ No disabled/ignored tests remaining

**Migration Impact:**
- Phase 4 **COMPLETE** - QueryableFile now lives in provider crate where it belongs
- Clean separation: provider has the trait, tlogfs has the implementations
- No backwards compatibility code - complete migration
- All legacy TODOs and comments removed
- Export script verified working with new architecture
- Ready for Phase 5 (remaining factory migration)

**Status**: Phase 4 complete, production-ready

---

---

### 2025-12-08: Temporal Bounds Architecture - Corrected

**Problem Identified:**
- Temporal bounds implementation mixed two distinct concepts:
  1. **High-level query boundaries** (timeseries-join range filters) - SQL WHERE clauses ✅ Correct
  2. **Low-level node metadata** (pond set-temporal-bounds) - Per-FileSeries data quality filtering
- sql_derived.rs was querying low-level metadata at wrong abstraction level
- file_table.rs had `temporal_bounds` field trying to handle per-file bounds explicitly
- Abstraction violation: factories shouldn't query node-level metadata directly

**Solution Implemented:**
- ✅ **Removed `get_temporal_bounds()` from PersistenceLayer trait** - Wrong abstraction!
- ✅ **Removed temporal bounds query from sql_derived.rs** - No longer calls `get_temporal_overrides_for_node_id()`
- ✅ **Removed `TemporalBounds` struct and `temporal_bounds` field** from TableProviderOptions
- ✅ **Kept TemporalFilteredListingTable** for single-file legacy path only
- ✅ **Single-file**: file_table queries `get_temporal_overrides_for_node_id()` directly (stays in tlogfs)
- ✅ **Multi-file**: No temporal filtering at this level (deferred to future Parquet reader implementation)

**Correct Architecture:**
- Low-level temporal bounds (`pond set-temporal-bounds`) should be enforced at **Parquet reader level**
- Per-file metadata filtering should be transparent to sql_derived and all factories
- High-level query boundaries (timeseries-join `range:`) already work correctly via SQL WHERE clauses
- TemporalFilteredListingTable is a tlogfs-specific optimization, not a provider abstraction

**Migration Impact:**
- ✅ sql_derived.rs no longer has temporal bounds dependency
- ✅ file_table remains in tlogfs (uses State directly for single-file optimization)
- ⚠️ file_table **cannot move to provider** - it's tlogfs-specific table creation logic
- ✅ SqlDerivedFile can move to provider (no file_table dependency in factory logic)
- ✅ Tests passing (87/87)

**Status**: ✅ **COMPLETE** - Abstraction violation fixed, ready for sql_derived migration

---

### 2025-12-08: Memory Persistence Storage API Complete

**What Completed:**
- ✅ **store_file_version()** and **store_file_version_with_metadata()** added to MemoryPersistence
- ✅ **list_file_versions()** already existed via PersistenceLayer trait
- ✅ **7/7 provider tests passing** (was 4/6)
- ✅ **423 total tests passing** project-wide

**Implementation:**
```rust
// Public API for tests
pub async fn store_file_version(&self, id: FileID, version: u64, content: Vec<u8>) -> Result<()>
pub async fn store_file_version_with_metadata(...) -> Result<()>

// Internal State storage
file_versions: HashMap<FileID, Vec<MemoryFileVersion>>
```

**Architecture:**
- Auto-timestamps with `chrono::Utc::now()`
- Stores version, timestamp, content, entry_type, extended_metadata
- MemoryFile::as_table_provider() uses ObjectStore pattern (same as tlogfs)
- TinyFsObjectStore<MemoryPersistence> enables ListingTable testing

**Testing:**
- ✅ test_temporal_filtered_listing_table_with_memory - Full storage API validation
- ✅ test_memory_file_queryable_interface - QueryableFile implementation
- ✅ test_tinyfs_object_store_with_memory - ObjectStore integration
- ✅ All memory persistence tests passing

**Benefits:**
- ✅ **sql_derived migration fully unblocked** - Can test with MemoryPersistence
- ✅ **No OpLog/DeltaLake required** for in-memory testing
- ✅ **Dramatically faster tests** - Pure memory operations
- ✅ **Complete QueryableFile interface** for both tlogfs and memory

**Status**: ✅ **COMPLETE** - Memory persistence ready for sql_derived in-memory testing

---

### 2025-12-08: sql_derived Migration Analysis

**Current Dependencies (sql_derived.rs in tlogfs):**

1. **✅ Already in provider:**
   - `SqlDerivedConfig` - moved to provider crate
   - `SqlDerivedMode` - moved to provider crate
   - `provider::FactoryContext` - infrastructure in place
   - `provider::QueryableFile` trait - Phase 4 complete

2. **❌ Still in tlogfs (blocking migration):**
   - `register_queryable_file_factory!` macro (line 35, 327, 335)
   - `try_as_queryable_file()` helper (line 51) - uses crate::file module
   - `file_table::create_table_provider()` (line 633, 647)
   - `file_table::VersionSelection` (line 431, 602)
   - `file_table::TableProviderOptions` (line 633)
   - State downcasting (line 423-426) - `context.persistence.downcast_ref::<State>()`
   - State methods: `session_context()`, `get/set_table_provider_cache()`
   - `OpLogPersistence` usage in tests (line 803, 2687, 3045)

3. **🔄 Coupling Analysis:**

   **Strong coupling to tlogfs:**
   - `file_table::create_table_provider()` is tlogfs-specific (uses State, TemporalFilteredListingTable)
   - State downcast pattern assumes tlogfs persistence
   - Factory registration macro in tlogfs

   **Weak coupling (can abstract):**
   - SessionContext access (via ProviderContext)
   - Table provider caching (via ProviderContext)
   - QueryableFile resolution (helper function)

**Migration Strategy - Three Options:**

**Option A: Full Migration (most ambitious)**
- Move sql_derived.rs entirely to provider crate
- Abstract away file_table dependency
- Create provider-level table creation API
- Pros: Clean separation, enables full in-memory testing
- Cons: Large refactor, need to abstract TemporalFilteredListingTable

**Option B: Partial Migration (pragmatic)**
- Move `SqlDerivedFile` struct to provider (core logic)
- Keep factory registration in tlogfs (uses tlogfs-specific infrastructure)
- Keep `try_as_queryable_file()` in tlogfs (knows about OpLogFile)
- Pros: Migrates business logic, smaller change
- Cons: Split across two crates, factory registration stays in tlogfs

**Option C: Minimal Migration (safest)**
- Keep sql_derived.rs in tlogfs entirely
- Already using provider::SqlDerivedConfig and provider::QueryableFile
- Document that sql_derived is tlogfs-specific implementation
- Pros: Zero risk, already clean enough
- Cons: Misses opportunity for in-memory testing

**Recommendation: Option B (Partial Migration)**

**Rationale:**
1. `SqlDerivedFile` business logic (pattern resolution, SQL generation) is provider-level
2. Factory registration naturally stays with persistence implementation
3. `try_as_queryable_file()` knows about OpLogFile, belongs in tlogfs
4. file_table remains in tlogfs (already decided - it's tlogfs-specific)
5. Enables testing SqlDerivedFile logic with MemoryPersistence
6. Incremental, lower risk than Option A

**Migration Steps (Option B):**

1. **Move SqlDerivedFile to provider:**
   - Create `provider/src/sql_derived_file.rs`
   - Move SqlDerivedFile struct and impl
   - Move as_table_provider() implementation
   - Use ProviderContext for session access
   - Abstract table provider creation (pass callback or use provider API)

2. **Keep in tlogfs:**
   - Factory registration (sql-derived-table, sql-derived-series)
   - `try_as_queryable_file()` helper
   - Test fixtures that use OpLogPersistence

3. **Update imports:**
   - tlogfs imports `provider::SqlDerivedFile`
   - Factories instantiate SqlDerivedFile, register with tlogfs infrastructure

**Blocking Issue: file_table::create_table_provider()**

Current signature:
```rust
pub async fn create_table_provider(
    file_id: FileID,
    state: &crate::persistence::State,  // ← tlogfs-specific
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>, TLogFSError>
```

**SqlDerivedFile.as_table_provider() calls this at line 647:**
```rust
let provider = create_table_provider(representative_file_id, state, options).await
```

**Problem:**
- `create_table_provider()` is in tlogfs, uses State directly
- Cannot move SqlDerivedFile to provider if it calls tlogfs functions
- Need abstraction for table provider creation

**Solutions:**

**Solution 1: Callback Pattern**
```rust
// In provider::SqlDerivedFile
pub async fn as_table_provider<F>(
    &self,
    id: FileID,
    context: &ProviderContext,
    create_table: F,  // ← Callback provided by caller
) -> Result<Arc<dyn TableProvider>>
where
    F: Fn(FileID, TableProviderOptions) -> BoxFuture<'static, Result<Arc<dyn TableProvider>>>,
{
    // SqlDerived logic...
    let provider = create_table(file_id, options).await?;
    // ...
}

// In tlogfs - factory registration provides the callback
let sql_file = SqlDerivedFile::new(config, context, mode)?;
sql_file.as_table_provider(id, context, |file_id, opts| {
    Box::pin(async move {
        create_table_provider(file_id, state, opts).await
    })
}).await?
```

**Solution 2: TableProviderFactory Trait**
```rust
// In provider crate
#[async_trait]
pub trait TableProviderFactory: Send + Sync {
    async fn create_table_provider(
        &self,
        file_id: FileID,
        options: TableProviderOptions,
    ) -> Result<Arc<dyn TableProvider>>;
}

// In tlogfs
struct TLogFSTableProviderFactory {
    state: Arc<State>,
}

#[async_trait]
impl TableProviderFactory for TLogFSTableProviderFactory {
    async fn create_table_provider(&self, file_id: FileID, options: TableProviderOptions) 
        -> Result<Arc<dyn TableProvider>> 
    {
        file_table::create_table_provider(file_id, &self.state, options).await
    }
}

// ProviderContext holds the factory
pub struct ProviderContext {
    table_factory: Arc<dyn TableProviderFactory>,
    // ...
}
```

**Solution 3: Keep SqlDerivedFile in tlogfs (Option C)**
- Simplest: no abstraction needed
- Already clean: uses provider::SqlDerivedConfig
- Testable: can test config/SQL generation separately
- Factory registration naturally stays with implementation

**Revised Recommendation: Option C (Keep in tlogfs)**

**Rationale After Deeper Analysis:**
1. SqlDerivedFile is tightly coupled to file_table infrastructure
2. file_table is staying in tlogfs (TemporalFilteredListingTable is tlogfs-specific)
3. Adding abstraction layers (callbacks/traits) adds complexity without clear benefit
4. Already achieved good separation: SqlDerivedConfig in provider ✅
5. Can test SQL generation logic separately (config → SQL transformation)
6. Factory registration belongs with persistence implementation
7. In-memory testing of full SqlDerivedFile needs file_table anyway

**What We've Already Achieved:**
- ✅ SqlDerivedConfig in provider (configuration abstraction)
- ✅ SqlDerivedMode in provider (series vs table distinction)
- ✅ QueryableFile trait in provider (interface abstraction)
- ✅ ProviderContext infrastructure (clean factory API)
- ✅ Tests passing (87/87)

**What Remains in tlogfs (correctly):**
- SqlDerivedFile implementation (uses tlogfs table infrastructure)
- Factory registration (tlogfs-specific)
- try_as_queryable_file() helper (knows about OpLogFile)
- Integration tests (require OpLogPersistence)

**Next Steps:**
1. ✅ Accept that sql_derived stays in tlogfs (correct architectural boundary)
2. 🔄 Move timeseries_join.rs to provider (next factory to migrate)
3. 🔄 Move timeseries_pivot.rs to provider (also depends on SqlDerivedFile)
4. 📝 Document tlogfs-specific vs provider-level factory distinction

**Status**: Analysis complete - sql_derived migration not needed, already well-factored

---

### 2025-12-08: Temporal Bounds Added to PersistenceLayer Trait

**Problem**: Low-level temporal bounds (from `pond set-temporal-bounds`) were tlogfs-specific, blocking migration of table creation logic.

**Solution Implemented:**
- ✅ Added `get_temporal_bounds(FileID) -> Result<Option<(i64, i64)>>` to tinyfs PersistenceLayer trait
- ✅ Implemented in tlogfs State: wraps existing `get_temporal_overrides_for_node_id()`
- ✅ Implemented in MemoryPersistence: HashMap storage with `set_temporal_bounds()` test helper
- ✅ Implemented in CachingPersistence: passthrough (temporal bounds immutable per version)
- ✅ Updated file_table.rs to use trait method instead of State-specific call

**Architecture Clarification:**
Two distinct temporal filtering layers:
1. **Low-level (per-file)**: Applied when ObjectStore URLs resolve to TableProviders
   - `file_table::create_table_provider()` wraps each file with TemporalFilteredListingTable
   - Filters garbage data based on `pond set-temporal-bounds` metadata
   - Now abstracted via PersistenceLayer::get_temporal_bounds() trait method

2. **High-level (per-query)**: Applied to final ViewTable from sql_derived
   - timeseries-join `range:` filters generate SQL WHERE clauses
   - User-specified time windows for query output
   - Already working correctly

**Benefits:**
- ✅ Temporal bounds abstraction now in tinyfs (not tlogfs-specific)
- ✅ MemoryPersistence can test temporal filtering without OpLog
- ✅ file_table.rs no longer directly depends on State implementation
- ✅ Ready for file_table migration to provider crate

**Testing:**
- ✅ All 87 tests passing
- ✅ tlogfs continues using OplogEntry.min_time/max_time columns (efficient)
- ✅ MemoryPersistence ready for in-memory testing

**Next Steps:**
- Move TemporalFilteredListingTable to provider crate
- Move VersionSelection to provider crate
- Move create_table_provider() to provider (uses ProviderContext + trait method)

**Status**: ✅ **COMPLETE** - Temporal bounds properly abstracted, ready for next phase

---

### 2025-12-08: TemporalFilteredListingTable Moved to Provider

**What Moved:**
- ✅ Created `provider/src/temporal_filter.rs` with full implementation
- ✅ Moved `TemporalFilteredListingTable` struct (200+ lines)
- ✅ Complete TableProvider implementation with temporal filtering logic
- ✅ Handles both normal queries and COUNT queries (empty projection case)
- ✅ Re-exported from provider::lib
- ✅ tlogfs now imports from provider (2-line re-export)

**Implementation Details:**
- Pure DataFusion wrapper - no tlogfs dependencies
- Applies timestamp-based filtering at ExecutionPlan level
- Converts milliseconds to seconds for HydroVu compatibility
- Handles unbounded case (i64::MIN/MAX) by skipping filter
- Special handling for COUNT queries (empty projection)
- Schema caching via OnceLock for performance

**Architecture:**
```
provider::TemporalFilteredListingTable (pure DataFusion)
  ↑
tlogfs::file_table (uses provider version)
  ↑
sql_derived, timeseries_join, etc.
```

**Benefits:**
- ✅ No code duplication
- ✅ Can be used by any crate depending on provider
- ✅ Testable independently of tlogfs
- ✅ Clear separation: filtering logic vs metadata lookup

**Testing:**
- ✅ All 87 tests passing
- ✅ Zero compilation warnings
- ✅ No behavioral changes

**Next Steps:**
- Move TableProviderOptions to provider
- Move create_table_provider() logic to provider
- sql_derived can then create tables without file_table dependency

**Status**: ✅ **COMPLETE** - TemporalFilteredListingTable successfully migrated

---

---

## Current Status: What Remains for sql_derived Migration

### ✅ What's Complete

**Infrastructure (100% done):**
- ✅ ProviderContext with MemoryPersistence support
- ✅ QueryableFile trait in provider (all implementations migrated)
- ✅ VersionSelection moved to provider
- ✅ TemporalFilteredListingTable moved to provider
- ✅ SqlDerivedConfig and SqlDerivedMode in provider
- ✅ Memory persistence storage API (store_file_version, list_file_versions)
- ✅ TinyFsObjectStore<MemoryPersistence> working
- ✅ 423 tests passing

### 🔄 What Remains (Blocking sql_derived)

**Critical Path: Abstract file_table dependencies**

**1. TableProviderOptions** (in tlogfs, needs to move to provider):
```rust
// Current: tlogfs/src/file_table.rs
pub struct TableProviderOptions {
    pub version_selection: VersionSelection,  // ✅ Already in provider
    pub additional_urls: Vec<String>,
}
```
- **Action**: Move to `provider/src/table_provider_options.rs`
- **Impact**: Used by sql_derived.rs (line 633)
- **Dependencies**: None (VersionSelection already in provider)

**2. create_table_provider()** (in tlogfs, needs abstraction):
```rust
// Current: tlogfs/src/file_table.rs line 35
pub async fn create_table_provider(
    file_id: FileID,
    state: &crate::persistence::State,  // ← tlogfs-specific
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>>
```

**Problem**: SqlDerivedFile.as_table_provider() calls this at line 647:
```rust
let provider = create_table_provider(representative_file_id, state, options).await
```

**State dependencies in create_table_provider():**
- `state.session_context()` - ✅ Available via ProviderContext
- `state.get_table_provider_cache()` - ✅ Available via ProviderContext
- `state.set_table_provider_cache()` - ✅ Available via ProviderContext  
- `state.get_temporal_bounds()` - ✅ Available via PersistenceLayer trait

**Solution Options:**

**Option A: Move create_table_provider to provider (Recommended)**
```rust
// provider/src/table_creation.rs
pub async fn create_table_provider(
    file_id: FileID,
    context: &ProviderContext,  // ← Generic, not tlogfs-specific
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>> {
    // Get SessionContext from ProviderContext
    let session = context.session_context().await?;
    
    // Check cache
    if let Some(cached) = context.get_table_provider_cache(&cache_key) {
        return Ok(cached);
    }
    
    // Query temporal bounds from persistence
    let bounds = context.persistence().get_temporal_bounds(file_id).await?;
    
    // Create ListingTable...
    // Wrap with TemporalFilteredListingTable...
    
    // Cache result
    context.set_table_provider_cache(cache_key, provider.clone());
    
    Ok(provider)
}
```

**Benefits:**
- ✅ SqlDerivedFile can move to provider (no tlogfs dependency)
- ✅ Testable with MemoryPersistence
- ✅ All State methods already abstracted
- ✅ Clean architectural boundary

**Migration Steps:**
1. Move TableProviderOptions to provider
2. Move TableProviderKey to provider (cache key struct)
3. Move create_table_provider logic to provider
4. tlogfs re-exports for backward compatibility
5. Update sql_derived.rs imports

**Option B: Keep in tlogfs, add ProviderContext overload**
```rust
// tlogfs keeps existing function
pub async fn create_table_provider(
    file_id: FileID,
    state: &State,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>>

// Add new overload in provider
pub async fn create_table_provider_generic(
    file_id: FileID,
    context: &ProviderContext,
    options: TableProviderOptions,
) -> Result<Arc<dyn TableProvider>>
```
- **Cons**: Code duplication, violates DRY principle
- **Not recommended**

### 📋 Remaining Work Estimate

**To move sql_derived functionality to provider:**

1. **Move TableProviderOptions** (30 minutes)
   - Create provider/src/table_provider_options.rs
   - Move struct definition
   - Update imports in tlogfs

2. **Move TableProviderKey** (15 minutes)
   - Cache key struct (FileID + VersionSelection)
   - Used for table provider caching

3. **Move create_table_provider** (2 hours)
   - Create provider/src/table_creation.rs
   - Port logic from tlogfs
   - Replace State calls with ProviderContext methods
   - Update temporal bounds query to use PersistenceLayer trait
   - Add comprehensive tests with MemoryPersistence

4. **Update sql_derived.rs** (30 minutes)
   - Change imports to use provider versions
   - Test with both tlogfs and MemoryPersistence

5. **Backward compatibility** (15 minutes)
   - tlogfs re-exports provider types
   - Verify all 87 tlogfs tests still pass

**Total estimate**: ~3.5 hours

### 🎯 Next Steps

**Immediate (unblocks sql_derived):**
1. Move TableProviderOptions to provider
2. Move create_table_provider to provider
3. Update sql_derived.rs to use provider versions

**Follow-up (optional improvements):**
- Move timeseries_join.rs to provider (similar dependencies)
- Move timeseries_pivot.rs to provider (similar dependencies)
- Move temporal_reduce.rs to provider (directory factory)

**Testing strategy:**
- Write create_table_provider tests with MemoryPersistence
- Verify sql_derived works with in-memory testing
- Ensure all 87 tlogfs tests still pass
- Add integration tests in provider crate

---

**Document Version**: 1.9  
**Date**: 2025-12-08 (Memory persistence complete, final sql_derived blockers identified)  
**Author**: Analysis based on codebase study
