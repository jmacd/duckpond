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

### Phase 1: Define Provider Context

Create concrete context struct in `provider` crate:

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
- **Phase 3** (Factory migration): 🔄 **IN PROGRESS** - dynamic_dir and test_factory moved
- **Phase 4** (QueryableFile): ✅ **COMPLETE** - All 5 implementations migrated, old trait deleted
- **Phase 5** (Remaining factories): Not started
- **Testing & Polish**: Ongoing (388 tests passing)

**Progress**: ~60% complete - Core infrastructure done, QueryableFile migrated, remaining factories to move

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

### 2025-12-08: Temporal Bounds Architecture Analysis

**Critical Finding:**
- Temporal bounds are implemented at the wrong abstraction layer
- Current design uses dummy `FileID::root()` for multi-file queries (accidental correctness)
- Temporal bounds are node-level metadata, should be in PersistenceLayer trait
- Current coupling to tlogfs::State blocks migration of file_table and sql_derived to provider

**Correct Design:**
- Add `get_temporal_bounds(FileID)` to tinyfs::PersistenceLayer trait
- tlogfs implements efficiently using OplogEntry columns (no change to storage)
- MemoryPersistence implements using HashMap (enables in-memory testing)
- TableProviderOptions gets `temporal_bounds: Option<Vec<TemporalBounds>>` field
- Call sites explicitly pass bounds (no dummy FileID, no "accidentally works")
- file_table can move to provider crate (no tlogfs dependency)

**Migration Priority:**
- Fix temporal bounds abstraction BEFORE moving remaining factories
- This unblocks sql_derived and file_table migration to provider
- Essential for in-memory testing of QueryableFile implementations

**Status**: Design complete, ready for implementation

---

**Document Version**: 1.5  
**Date**: 2025-12-08 (Temporal bounds architecture analysis - critical for Phase 5)  
**Author**: Analysis based on codebase study
