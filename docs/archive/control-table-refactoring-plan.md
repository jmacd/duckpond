# Control Table Refactoring Plan

## Problem Statement

The current control table implementation has critical bugs and design issues:

1. **Remote Factory Pond Isolation Bug**: `scan_remote_versions()` and `get_last_backed_up_version()` don't filter by pond_id, treating all ponds in a bucket as one namespace
2. **Poor Initialization Logic**: No clear separation between "create new" vs "open existing" 
3. **Repeated SessionContext Setup**: Every query function creates a new SessionContext and registers the table provider, wasting resources and creating opportunities for configuration drift
4. **Code Repetition**: Massive duplication in Arrow array handling
5. **Type Safety Issues**: Uses bare strings instead of enums for record_type, transaction_type, etc.
6. **Inconsistent Patterns**: Doesn't follow tlogfs patterns (compare `OpLogPersistence::open_or_create` vs `ControlTable::new`)
7. **Direct Arrow Usage**: Manual Arrow array construction instead of using serde_arrow like tlogfs does

## Critical Bug Analysis

### Remote Factory Pond Isolation Bugs (ACTUAL ROOT CAUSE)

**Files**: `crates/tlogfs/src/remote_factory.rs`

#### Bug 1: `scan_remote_versions()` (line 1259)
```rust
pub async fn scan_remote_versions(
    store: &Arc<dyn ObjectStore>,
) -> Result<Vec<i64>, TLogFSError> {
    // Lists ALL objects in bucket
    let list_stream = store.list(None);
    
    for meta in objects {
        if let Some((_pond_id, version)) = extract_bundle_info_from_path(&path_str) {
            versions.push(version);  // BUG: Includes ALL ponds!
        }
    }
}
```

**Impact**: 
- `verify` command found 31 versions from multiple old test ponds instead of 10 from current pond
- `list-bundles` shows bundles from all ponds mixed together
- No isolation between ponds sharing the same S3 bucket

**Fix Applied**: Added `pond_id: Option<&uuid7::Uuid>` parameter and filter bundles by UUID

#### Bug 2: `get_last_backed_up_version()` (line 739)
```rust
async fn get_last_backed_up_version(
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<i64>, TLogFSError> {
    // Finds maximum version across ALL ponds
    if let Some((_pond_id, version)) = extract_bundle_info_from_path(path_str) {
        max_version = Some(max_version.unwrap_or(0).max(version));  // BUG!
    }
}
```

**Impact**: 
- Pond with 11 transactions sees max version 30 from an old pond
- `push` thinks "all versions already backed up" and skips creating bundles
- Backup system silently fails for new ponds sharing a bucket

**Fix Applied**: Added `pond_id: &uuid7::Uuid` parameter and filter to current pond only

**Root Cause**: Sloppy assumptions that each pond has its own bucket. Multiple ponds CAN and DO share buckets in testing and production scenarios.

### ControlTable Initialization Design Issue

**File**: `crates/steward/src/control_table.rs`, lines 126-128

**Original Analysis Was Wrong**: The document claimed this code "always overwrites UUID on every open". That was a misdiagnosis.

**Actual Code Structure**:
```rust
pub async fn new(path: &str) -> Result<Self, StewardError> {
    match deltalake::open_table(path).await {
        Ok(table) => {
            // Table exists - just open it
            // NO pond_id modification! ✅
            Ok(Self { table, ... })
        }
        Err(_) => {
            // Table doesn't exist - create it
            let pond_metadata = PondMetadata::new();  // Only here!
            table.set_pond_metadata(&pond_metadata).await?;
            Ok(Self { table, ... })
        }
    }
}
```

**Actual Problem**: Not the UUID overwrite, but unclear API design:
- No explicit `create` vs `open` distinction  
- Caller can't control initialization behavior
- Inconsistent with tlogfs pattern

**Fix Applied**: Split into `ControlTable::create()` and `ControlTable::open()` methods

### SessionContext Registration Antipattern

**Problem**: Every query method creates its own SessionContext and registers table

Examples throughout `control_table.rs`:

```rust
pub async fn get_pond_metadata(&self) -> Result<PondMetadata, StewardError> {
    let ctx = SessionContext::new();  // Creates NEW context
    ctx.register_table("transactions", Arc::new(self.table.clone()))?;  // Registers AGAIN
    
    let df = ctx.sql("SELECT ...").await?;
    // ...
}

pub async fn find_incomplete_transactions(&self) -> Result<Vec<...>, StewardError> {
    let ctx = SessionContext::new();  // ANOTHER new context
    ctx.register_table("transactions", Arc::new(self.table.clone()))?;  // AGAIN
    
    let df = ctx.sql("SELECT ...").await?;
    // ...
}

// Repeated in:
// - get_last_transaction_seq()
// - get_all_transactions()
// - get_transaction_metadata()
// - find_post_commit_tasks()
// ... and many more
```

**Problems**:
1. **Wasteful**: Creates and tears down DataFusion query engine for each call
2. **Error-prone**: Each registration is an opportunity for typos or misconfigurations
3. **Inconsistent**: Some methods use `self.session_context`, others create new ones
4. **Drift risk**: No guarantee all registrations use same table name or configuration

**Correct Pattern**: Create SessionContext ONCE during initialization, reuse it:

```rust
pub struct ControlTable {
    table: DeltaTable,
    session_context: Arc<SessionContext>,  // Created once, shared
}

impl ControlTable {
    pub async fn create(path: &str, metadata: &PondMetadata) -> Result<Self, StewardError> {
        let table = /* create table */;
        
        // Register ONCE during construction
        let session_context = Arc::new(SessionContext::new());
        session_context.register_table("transactions", Arc::new(table.clone()))?;
        
        Ok(Self { table, session_context })
    }
    
    pub async fn get_pond_metadata(&self) -> Result<PondMetadata, StewardError> {
        // Reuse existing context - NO new registration!
        let df = self.session_context.sql("SELECT ...").await?;
        // ...
    }
}
```

**Benefits**:
- Setup happens once in constructor
- All queries use same configuration
- Less code, fewer opportunities for errors
- Better performance (SessionContext setup is not free)

**Current State**: Partially fixed! `ControlTable` struct now has `session_context: Arc<SessionContext>` field (line 122), but many query methods still create their own instead of using it.

## Comparison: TLogFS vs Control Table

### TLogFS Pattern (Good) ✅

```rust
pub async fn open_or_create(
    path: &str, 
    create_new: bool,
    txn_metadata: Option<PondUserMetadata>
) -> Result<Self, TLogFSError> {
    if create_new {
        // Initialize new table with metadata
        let mut table = deltalake::DeltaOps::try_from_uri(path).await?
            .create()
            .with_columns(schema)
            .with_save_mode(SaveMode::Ignore)  // Fail if exists!
            .await?;
            
        // Write initial transaction with metadata
        // ...
    } else {
        // Just open existing
        let table = deltalake::open_table(path).await?;
        // NO metadata modification!
    }
}
```

**Key Points**:
- Explicit `create_new` flag
- `SaveMode::Ignore` fails if table exists
- Only writes metadata when creating new
- Read-only when opening existing

### Control Table Pattern (Bad) ❌

```rust
pub async fn new(path: &str) -> Result<Self, StewardError> {
    let maybe_table = deltalake::open_table(path).await;
    
    let table = match maybe_table {
        Ok(table) => table,  // Exists - open it
        Err(_) => {
            // Doesn't exist - create it
            let mut builder = CreateBuilder::new()
                .with_columns(schema)
                .with_location(path);
            
            builder.await?
        }
    };
    
    // BUG: ALWAYS writes new UUID regardless!
    let pond_metadata = PondMetadata::new();
    table.set_pond_metadata(&pond_metadata).await?;
    
    Ok(Self { table })
}
```

**Problems**:
- No `create_new` flag
- Creates table even with bad config (no `SaveMode::Ignore`)
- **ALWAYS overwrites pond_id**, even when opening existing
- Can't tell if this is init or open

## Type Safety Issues

### Current: Bare Strings ❌

```rust
// Scattered throughout control_table.rs:
record_type: "begin".to_string()
record_type: "data_committed".to_string()
transaction_type: "write".to_string()
transaction_type: "post_commit".to_string()

// Query uses string literals:
WHERE record_type = 'begin'
WHERE transaction_type IN ('write', 'post_commit')
```

**Problems**:
- Typos not caught at compile time
- No exhaustiveness checking
- Unclear what values are valid
- Inconsistent naming (snake_case strings vs CamelCase in code)

### Proposed: Enums ✅

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordType {
    Begin,
    DataCommitted,
    Failed,
    Metadata,
    PostCommitPending,
    PostCommitStarted,
    PostCommitCompleted,
    PostCommitFailed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    Write,
    PostCommit,
    Metadata,
}

pub struct TransactionRecord {
    pub txn_seq: i64,
    pub record_type: RecordType,  // Type-safe!
    pub transaction_type: TransactionType,
    // ...
}
```

**Benefits**:
- Compile-time checking
- IDE autocomplete
- Exhaustive match checking
- Clear documentation of valid values
- serde handles string conversion

## Code Repetition Analysis

### Arrow Array Handling Duplication

**Problem**: Control table has ~500 lines of repetitive Arrow array construction code

Example from `write_record()` (lines 200-350):
```rust
let txn_seq_array = Int64Array::from(vec![record.txn_seq]);
let txn_id_array = StringArray::from(vec![record.txn_id.as_str()]);
let timestamp_array = TimestampMicrosecondArray::from(vec![record.timestamp])
    .with_timezone("+00:00");
// ... 15 more fields ...

let batch = RecordBatch::try_new(
    self.table.schema()?,
    vec![
        Arc::new(txn_seq_array),
        Arc::new(txn_id_array),
        // ... 15 more fields ...
    ],
)?;
```

**Compared to TLogFS** (uses serde_arrow):
```rust
// Define struct with derives
#[derive(Serialize)]
struct OpLogRecord {
    node_id: String,
    timestamp: i64,
    // ...
}

// Serialize to Arrow in one line!
let arrays = serde_arrow::to_arrow(&schema, &records)?;
let batch = RecordBatch::try_new(schema, arrays)?;
```

### Query Result Parsing Duplication

Lines 540-650 in `get_pond_metadata()`:
```rust
// Duplicate downcast logic for Utf8 vs Utf8View
let names: Vec<Option<String>> = if let Some(array) = 
    name_col.as_any().downcast_ref::<StringArray>() {
    (0..array.len())
        .map(|i| if array.is_null(i) { None } else { Some(array.value(i).to_string()) })
        .collect()
} else if let Some(array) = name_col
    .as_any()
    .downcast_ref::<StringViewArray>() {
    (0..array.len())
        .map(|i| if array.is_null(i) { None } else { Some(array.value(i).to_string()) })
        .collect()
} else {
    continue;
};

// Same logic repeated for `values` column
let values: Vec<Option<String>> = if let Some(array) = 
    value_col.as_any().downcast_ref::<StringArray>() {
    // ... identical code ...
} else if let Some(array) = value_col
    .as_any().downcast_ref::<StringViewArray>() {
    // ... identical code ...
} else {
    continue;
};
```

**With serde_arrow**:
```rust
#[derive(Deserialize)]
struct MetadataRow {
    factory_name: String,
    config_path: String,
}

let rows: Vec<MetadataRow> = serde_arrow::from_record_batch(&batch)?;
// That's it! No manual array downcasting.
```

## Refactoring Plan

### Phase 0: Fix Remote Factory Pond Isolation (COMPLETED ✅)

**Goal**: Filter remote operations by pond_id to isolate ponds sharing same bucket

**Changes Made**:
1. `scan_remote_versions()` - Added `pond_id: Option<&uuid7::Uuid>` parameter
2. `get_last_backed_up_version()` - Added `pond_id: &uuid7::Uuid` parameter  
3. Updated all callers to pass pond_id from context

**Results**:
- ✅ Verify finds exactly 10 bundles for current pond (not 31 from old ponds)
- ✅ Push correctly identifies last backed up version for current pond
- ✅ List-bundles shows only current pond's bundles

**Effort**: 2 hours  
**Risk**: Low - purely additive filtering

### Phase 1: Introduce Type-Safe Enums (Low Risk)

**Goal**: Replace bare strings with enums

**Files to modify**:
- `crates/steward/src/control_table.rs` - Add enums, update TransactionRecord
- All callers of `write_record()`, `record_begin()`, etc.

**Steps**:
1. Define `RecordType` and `TransactionType` enums
2. Update `TransactionRecord` struct to use enums
3. Update all write methods to use enums
4. Update all query methods to use enums in SQL (via ToString)
5. Test that all existing tests pass

**Risk**: Low - enum <-> string conversion via serde is straightforward

### Phase 2: Fix Initialization Logic (COMPLETED ✅)

**Goal**: Separate "create new" from "open existing" for clarity

**Pattern implemented**:
```rust
impl ControlTable {
    /// Open existing control table (read-only to pond_id)
    pub async fn open(path: &str) -> Result<Self, StewardError> {
        let table = deltalake::open_table(path).await?;
        let session_context = Arc::new(SessionContext::new());
        session_context.register_table("transactions", Arc::new(table.clone()))?;
        
        Ok(Self { table, session_context })
    }
    
    /// Create new control table with initial pond metadata
    pub async fn create(path: &str, pond_metadata: &PondMetadata) -> Result<Self, StewardError> {
        // Create empty table
        let table = CreateBuilder::new()
            .with_columns(Self::arrow_schema())
            .with_location(path)
            .await?;
        
        let session_context = Arc::new(SessionContext::new());
        session_context.register_table("transactions", Arc::new(table.clone()))?;
        
        // Write initial pond_id
        let mut control = Self { table, session_context };
        control.set_pond_metadata(pond_metadata).await?;
        
        Ok(control)
    }
}
```

**Updated callers**:
- `Ship::create_infrastructure()` - Uses `create()` when `create_new=true`, `open()` when false
- Tests updated to use explicit `create()` or `open()`

**Results**:
- ✅ API is explicit about intent
- ✅ Follows tlogfs pattern
- ✅ Tests confirm pond_id preserved across opens

**Effort**: 3 hours  
**Risk**: Low - mainly API reorganization

### Phase 2.5: Consolidate SessionContext Usage (TODO)

**Goal**: Eliminate repeated SessionContext creation and table registration

**Problem**: Many query methods still do this:
```rust
pub async fn some_query(&self) -> Result<T> {
    let ctx = SessionContext::new();  // Wasteful
    ctx.register_table("transactions", Arc::new(self.table.clone()))?;  // Error-prone
    // ... query ...
}
```

**Solution**: Use `self.session_context` consistently:
```rust
pub async fn some_query(&self) -> Result<T> {
    let df = self.session_context.sql("SELECT ...").await?;  // Reuse context
    // ... query ...
}
```

**Files to audit**:
- `crates/steward/src/control_table.rs` - Check every method that does SQL queries
- Look for pattern: `SessionContext::new()` followed by `register_table()`
- Replace with: `self.session_context.sql()`

**Benefits**:
- Single source of truth for table registration
- No duplicate setup code
- Harder to make registration mistakes
- Better performance

**Effort**: 2-3 hours  
**Risk**: Low - mechanical refactoring

### Phase 1: Introduce Type-Safe Enums (TODO)

**Goal**: Separate "create new" from "open existing", fix UUID bug

**Pattern to follow**:
```rust
impl ControlTable {
    /// Open existing control table (read-only to pond_id)
    pub async fn open(path: &str) -> Result<Self, StewardError> {
        let table = deltalake::open_table(path).await
            .map_err(|e| StewardError::ControlTable(format!("Table doesn't exist: {}", e)))?;
        
        // NO pond_id modification!
        Ok(Self { table })
    }
    
    /// Create new control table with initial pond metadata
    pub async fn create(path: &str, pond_metadata: &PondMetadata) -> Result<Self, StewardError> {
        let mut table = CreateBuilder::new()
            .with_columns(Self::schema())
            .with_location(path)
            .with_save_mode(SaveMode::ErrorIfExists)  // Fail if exists!
            .await?;
        
        // Write initial pond_id
        let mut control = Self { table };
        control.set_pond_metadata(pond_metadata).await?;
        
        Ok(control)
    }
    
    /// Open or create (for convenience)
    pub async fn open_or_create(
        path: &str,
        create_new: bool,
        pond_metadata: Option<&PondMetadata>
    ) -> Result<Self, StewardError> {
        if create_new {
            let metadata = pond_metadata.ok_or_else(|| 
                StewardError::ControlTable("pond_metadata required for create".to_string()))?;
            Self::create(path, metadata).await
        } else {
            Self::open(path).await
        }
    }
}
```

**Update callers**:
- `Ship::create_infrastructure()` - Pass `create_new` flag through
- `Ship::create_pond()` - Use `ControlTable::create()`
- `Ship::open_pond()` - Use `ControlTable::open()`

**Testing**:
1. Create pond, close it, reopen - pond_id should be SAME
2. Create replica, restore bundles - pond_id should be preserved
3. Try to create over existing - should fail with clear error

### Phase 3: Adopt serde_arrow (Reduce Duplication)

**Goal**: Replace manual Arrow array handling with serde_arrow

**Benefits**:
- Reduce code from ~500 lines to ~50 lines
- Automatic type conversions
- Better maintainability
- Consistent with tlogfs patterns

**Example**:
```rust
// Before: ~100 lines of manual Arrow construction
fn write_record(&mut self, record: TransactionRecord) -> Result<()> {
    let txn_seq_array = Int64Array::from(vec![record.txn_seq]);
    let txn_id_array = StringArray::from(vec![record.txn_id.as_str()]);
    // ... 13 more fields ...
    
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(txn_seq_array),
        Arc::new(txn_id_array),
        // ... 13 more arrays ...
    ])?;
    // ...
}

// After: ~10 lines with serde_arrow
fn write_record(&mut self, record: TransactionRecord) -> Result<()> {
    let schema = Self::arrow_schema();
    let arrays = serde_arrow::to_arrow(&schema, &[record])?;
    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
    // ... write batch ...
}
```

**Steps**:
1. Add serde derives to TransactionRecord
2. Create Arrow schema from serde_arrow traits
3. Replace `write_record()` implementation
4. Replace query result parsing with serde_arrow deserialization
5. Remove all manual array construction/downcasting code

### Phase 4: Improve Error Handling

**Current issues**:
- Generic `StewardError::ControlTable(String)` loses context
- No distinction between "not found" vs "corrupt" vs "permission denied"
- Hard to debug failures

**Proposed**:
```rust
#[derive(Debug, thiserror::Error)]
pub enum ControlTableError {
    #[error("Control table not found at {path}")]
    NotFound { path: String },
    
    #[error("Control table already exists at {path}")]
    AlreadyExists { path: String },
    
    #[error("Failed to write record: {0}")]
    WriteError(#[from] deltalake::DeltaTableError),
    
    #[error("Failed to query: {0}")]
    QueryError(#[from] datafusion::error::DataFusionError),
    
    #[error("Missing pond metadata: {field}")]
    MissingMetadata { field: String },
    
    #[error("Corrupted pond metadata: {reason}")]
    CorruptMetadata { reason: String },
}
```

## Testing Strategy

### Unit Tests
- Test enum serialization/deserialization
- Test `create()` fails if table exists
- Test `open()` fails if table doesn't exist
- Test pond_id preservation across open/close cycles

### Integration Tests
- Create pond, perform transactions, close, reopen - verify same pond_id
- Create replica from backup - verify pond_id matches source
- Simulate the noyo bug - open pond 10 times, verify pond_id doesn't change

### Backward Compatibility
- Existing control tables should still work (enum values match old strings)
- Migration path: existing tables use string columns, new code reads them as enums

## Implementation Order

1. **Phase 0 (Remote Factory Isolation)** - COMPLETED ✅
   - Fixed `scan_remote_versions()` and `get_last_backed_up_version()` to filter by pond_id
   - Risk: Critical bug causing backup failures
   - Effort: 2 hours
   - Priority: **COMPLETED**

2. **Phase 2 (API Clarity)** - COMPLETED ✅
   - Split `ControlTable::new()` into `create()` and `open()` 
   - Risk: Low, mostly API reorganization
   - Effort: 3 hours
   - Priority: **COMPLETED**

3. **Phase 2.5 (SessionContext Consolidation)** - TODO
   - Eliminate repeated SessionContext setup in query methods
   - Risk: Low, mechanical refactoring
   - Effort: 2-3 hours
   - Priority: High (reduces error-prone boilerplate)

4. **Phase 1 (Type Safety)** - TODO
   - Add enums for record_type, transaction_type
   - Risk: Low, mostly additive
   - Effort: 3-4 hours
   - Priority: High (prevents future string bugs)

5. **Phase 3 (Reduce Duplication)** - TODO
   - Adopt serde_arrow for Arrow array handling
   - Risk: Medium (changes serialization logic)
   - Effort: 5-6 hours
   - Priority: Medium (code quality improvement)

6. **Phase 4 (Error Handling)** - TODO
   - Better error types with more context
   - Risk: Low
   - Effort: 2-3 hours
   - Priority: Low (nice to have)

## Success Criteria

✅ Remote factory functions filter by pond_id (verify, push, pull, list-bundles)  
✅ Opening an existing pond never modifies its pond_id  
✅ `ControlTable::create()` and `open()` have clear, distinct semantics  
✅ Pond can be opened 100 times without pond_id changing  
⬜ SessionContext setup happens once per ControlTable instance  
⬜ All transaction record fields use type-safe enums  
⬜ Arrow array handling code reduced by 80%+  
⬜ Clear error messages for common failure modes  
✅ All existing tests pass  
✅ New tests verify pond_id preservation and bucket isolation

## Files to Modify

1. `crates/steward/src/control_table.rs` - Main refactoring
2. `crates/steward/src/ship.rs` - Update initialization calls
3. `crates/steward/src/guard.rs` - Update record_* calls
4. `crates/cmd/src/commands/init.rs` - Update create/open logic
5. `crates/cmd/src/common.rs` - Update Ship creation

## Related Documents

- `docs/duckpond-system-patterns.md` - Single transaction rule
- `docs/anti-duplication.md` - Code duplication anti-patterns
- TLogFS implementation - Reference for good Delta table patterns
