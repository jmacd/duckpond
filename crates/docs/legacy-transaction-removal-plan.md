# Legacy Transaction Removal Plan

**Objective**: Eliminate all unguarded transaction usage and remove legacy transaction APIs to enforce the transaction guard pattern system-wide.

**Status**: Planning Phase - Ready for execution
**Created**: August 11, 2025
**Priority**: High - Architectural cleanup to prevent fallback anti-patterns

## Executive Summary

DuckPond currently has **dual transaction systems**:
1. **Legacy unguarded**: `fs.begin_transaction() -> fs.commit()` (dangerous, allows misuse)
2. **New guarded**: `persistence.begin_transaction_with_guard() -> guard.commit()` (safe, prevents misuse)

This plan eliminates the legacy system entirely, forcing all code to use transaction guards and preventing the architectural problems that lead to fallback anti-patterns.

## Current Architecture Analysis

```
┌─────────────────────────────────────────────────────────────┐
│                    CURRENT STATE                            │
├─────────────────────────────────────────────────────────────┤
│  Application Code (Steward, Tests, etc.)                   │
│     ↓ uses                                                  │
│  tinyfs::FS (begin_transaction/commit/rollback)            │
│     ↓ delegates to                                          │
│  PersistenceLayer trait (begin_transaction/commit/rollback) │
│     ↓ implemented by                                        │
│  OpLogPersistence                                           │
│                                                             │
│  PLUS: TransactionGuard pattern (parallel system)          │
└─────────────────────────────────────────────────────────────┘
```

### Legacy Usage Found (Pre-execution Scan)

#### Production Code:
- **Steward (3 locations)**: `crates/steward/src/ship.rs`
  - `self.control_fs.begin_transaction().await` (line ~75)
  - `self.data_fs.begin_transaction().await` (line ~162) 
  - Using old commit patterns

#### Test Code:
- **40+ locations** across multiple test files:
  - `crates/tlogfs/src/file_writer/tests.rs`
  - `crates/tlogfs/src/test_phase4.rs` 
  - `crates/tlogfs/src/file_series_integration_tests.rs`
  - Various other test files

#### Core Infrastructure:
- **tinyfs::FS**: `crates/tinyfs/src/fs.rs` (3 methods)
- **PersistenceLayer trait**: `crates/tinyfs/src/persistence.rs` (trait definition)
- **OpLogPersistence**: `crates/tlogfs/src/persistence.rs` (trait implementation)

## Execution Plan

### Phase 1: Architecture Refactor (Foundation) 🏗️

**Goal**: Remove the legacy transaction API entirely, forcing compilation errors that guide migration.

#### Step 1.1: Remove PersistenceLayer Trait Methods
**File**: `crates/tinyfs/src/persistence.rs`
**Action**: Delete these trait methods:
```rust
// REMOVE THESE ENTIRELY:
async fn begin_transaction(&self) -> Result<()>;
async fn commit(&self) -> Result<()>;
async fn rollback(&self) -> Result<()>;
```

**Expected**: Compilation errors everywhere the old API is used.

#### Step 1.2: Remove FS Delegation Methods  
**File**: `crates/tinyfs/src/fs.rs`
**Action**: Delete these methods:
```rust
// REMOVE THESE:
pub async fn begin_transaction(&mut self) -> Result<()>
pub async fn commit(&mut self) -> Result<()>
pub async fn rollback(&mut self) -> Result<()>
```

**Validation**: 
- `cargo check` should show compilation errors pointing to all legacy usage
- Document the error locations for systematic fixing

### Phase 2: Steward Migration (Critical Path) ⚙️

**Goal**: Convert steward to use transaction guards exclusively.

#### Step 2.1: Update Ship Structure
**File**: `crates/steward/src/ship.rs`

**Current problematic fields**:
```rust
pub struct Ship {
    data_fs: FS,           // REMOVE - legacy wrapper
    control_fs: FS,        // REMOVE - legacy wrapper  
    data_persistence: OpLogPersistence,      // KEEP
    control_persistence: OpLogPersistence,   // KEEP
    // ... other fields
}
```

**New structure**:
```rust
pub struct Ship {
    // Direct persistence access only - no FS wrappers
    data_persistence: OpLogPersistence,
    control_persistence: OpLogPersistence,
    // ... other fields remain the same
}
```

#### Step 2.2: Convert Transaction Usage Patterns

**OLD PATTERN** (3 locations to fix):
```rust
// Control filesystem transaction
self.control_fs.begin_transaction().await?;
// ... operations ...
self.control_fs.commit().await?;

// Data filesystem transaction  
self.data_fs.begin_transaction().await?;
// ... operations ...  
self.data_fs.commit().await?;
```

**NEW PATTERN**:
```rust
// Control filesystem with guard
let tx = self.control_persistence.begin_transaction_with_guard().await?;
// ... operations via tx ...
tx.commit().await?;

// Data filesystem with guard
let tx = self.data_persistence.begin_transaction_with_guard().await?;
// ... operations via tx ...
tx.commit().await?;
```

#### Step 2.3: Update Root Directory Access
Replace `self.data_fs().root()` calls with persistence-based access:
```rust
// OLD: 
let root = self.data_fs().root().await?;

// NEW:
let tx = self.data_persistence.begin_transaction_with_guard().await?;
let root = tx.load_node(NodeID::root(), NodeID::root()).await?;
```

**Validation**:
- `cargo test -p steward` should pass
- All steward tests should use guarded transactions only

### Phase 3: Test Migration (Systematic) 🧪

**Goal**: Convert all test files to use transaction guards.

#### Step 3.1: Create Test Utility Functions
**File**: `crates/tlogfs/src/test_utils.rs` (new file)

```rust
//! Test utilities for transaction guard patterns

use crate::{OpLogPersistence, TransactionGuard};
use std::future::Future;
use std::pin::Pin;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Helper for tests that need a single transaction
pub async fn with_transaction<F, T>(
    persistence: &OpLogPersistence, 
    f: F
) -> Result<T, tlogfs::TLogFSError>
where 
    F: for<'a> FnOnce(TransactionGuard<'a>) -> BoxFuture<'a, Result<T, tlogfs::TLogFSError>>,
{
    let tx = persistence.begin_transaction_with_guard().await?;
    let result = f(tx).await?;
    Ok(result)
}

/// Helper for tests that need transaction + commit
pub async fn with_transaction_commit<F, T>(
    persistence: &OpLogPersistence, 
    f: F
) -> Result<T, tlogfs::TLogFSError>
where 
    F: for<'a> FnOnce(TransactionGuard<'a>) -> BoxFuture<'a, Result<T, tlogfs::TLogFSError>>,
{
    let tx = persistence.begin_transaction_with_guard().await?;
    let result = f(tx).await?;
    // Note: transaction auto-commits when guard drops successfully
    Ok(result)
}
```

#### Step 3.2: File-by-File Migration Plan

**Priority Order** (least disruptive first):

1. **`crates/tlogfs/src/file_writer/tests.rs`**
   - ~15 test functions using old patterns
   - Convert `fs.begin_transaction()` + `fs.commit()` to guards
   - Pattern: Simple transaction scopes

2. **`crates/tlogfs/src/test_phase4.rs`** 
   - Integration tests for FileWriter
   - Convert FS usage to direct persistence + guards

3. **`crates/tlogfs/src/file_series_integration_tests.rs`**
   - Complex tests with multiple transactions
   - May need custom transaction management

4. **Other test files** (as identified by compilation errors)
   - Systematic conversion using same patterns

**Migration Pattern for Each File**:
```rust
// OLD TEST PATTERN:
#[tokio::test]
async fn test_something() {
    let persistence = OpLogPersistence::new(path).await.unwrap();
    let fs = tinyfs::FS::new(persistence).await.unwrap();
    
    fs.begin_transaction().await.unwrap();
    // ... test operations ...
    fs.commit().await.unwrap();
}

// NEW TEST PATTERN:
#[tokio::test] 
async fn test_something() {
    let persistence = OpLogPersistence::new(path).await.unwrap();
    
    let tx = persistence.begin_transaction_with_guard().await.unwrap();
    // ... test operations via tx ...
    tx.commit().await.unwrap();
}
```

#### Step 3.3: Validation Strategy
After each test file conversion:
- `cargo test --test <test_file>` should pass
- `cargo test -p tlogfs` should continue passing
- No regression in test coverage

### Phase 4: Final Cleanup (Polish) ✨

**Goal**: Remove all remaining legacy implementation code.

#### Step 4.1: Remove OpLogPersistence Trait Implementation
**File**: `crates/tlogfs/src/persistence.rs`

Remove these trait implementation methods:
```rust
// REMOVE THESE IMPLEMENTATIONS:
async fn begin_transaction(&self) -> TinyFSResult<()> { 
    self.begin_transaction_impl().await
        .map_err(error_utils::to_tinyfs_error)
}

async fn commit(&self) -> TinyFSResult<()> {
    // ... remove entire implementation
}

async fn rollback(&self) -> TinyFSResult<()> {
    // ... remove entire implementation  
}
```

#### Step 4.2: Clean Up Factory Methods
**File**: `crates/tlogfs/src/persistence.rs`

Convert factory methods to use guards internally:
```rust
// Update create_oplog_fs() and create_oplog_fs_with_guards()
// to use consistent guard-based initialization
```

#### Step 4.3: Remove Unused Helper Methods
Look for any helper methods that were only used by the legacy API:
- `begin_transaction_impl()` (if not used elsewhere)
- Any transaction utility functions specific to old API

## Risk Mitigation & Rollback Strategy

### Compilation-Driven Migration
By removing trait methods first, the **compiler becomes our guide** - it will show exactly where legacy usage exists and needs to be updated.

### Incremental Validation
After each phase:
1. `cargo check` (compilation)  
2. `cargo test -p tlogfs` (core functionality)
3. `cargo test -p steward` (steward functionality)
4. `cargo test --all` (full system validation)

### Git Strategy
Each phase should be a separate commit:
- `git commit -m "Phase 1: Remove legacy transaction trait methods"`
- `git commit -m "Phase 2: Convert steward to transaction guards"`
- `git commit -m "Phase 3: Migrate test file X to transaction guards"`
- `git commit -m "Phase 4: Final cleanup of legacy implementations"`

### Rollback Plan
Each phase can be rolled back independently:
```bash
# Rollback just the last phase
git reset --hard HEAD~1

# Rollback to specific phase
git reset --hard <phase_commit_hash>
```

## Expected Benefits Post-Migration

### 1. **Architectural Integrity**
- ✅ Single transaction API (guards only)
- ✅ Impossible to forget commit/rollback  
- ✅ Clear resource ownership
- ✅ Prevention of fallback anti-patterns

### 2. **Developer Experience**
- ✅ Compiler-enforced correct usage
- ✅ RAII cleanup prevents resource leaks
- ✅ Consistent patterns across codebase
- ✅ Easier debugging (single code path)

### 3. **Maintainability**
- ✅ Reduced code surface area
- ✅ Fewer ways to do the same thing
- ✅ Less test complexity
- ✅ Clearer error handling

## Execution Checklist

### Pre-Execution
- [ ] Backup current codebase state
- [ ] Verify all tests pass in current state
- [ ] Create execution tracking branch

### Phase 1: Architecture Refactor
- [ ] Remove PersistenceLayer trait methods
- [ ] Remove FS delegation methods  
- [ ] Document compilation errors found
- [ ] Verify expected breakage locations

### Phase 2: Steward Migration
- [ ] Update Ship struct (remove FS fields)
- [ ] Convert control filesystem transactions
- [ ] Convert data filesystem transactions
- [ ] Update root directory access patterns
- [ ] Validate steward tests pass

### Phase 3: Test Migration
- [ ] Create test utility functions
- [ ] Migrate `file_writer/tests.rs`
- [ ] Migrate `test_phase4.rs`
- [ ] Migrate `file_series_integration_tests.rs`
- [ ] Migrate remaining test files (as discovered)
- [ ] Validate no test regression

### Phase 4: Final Cleanup
- [ ] Remove OpLogPersistence trait implementations
- [ ] Clean up factory methods
- [ ] Remove unused helper methods
- [ ] Final validation of all tests

### Post-Execution Validation
- [ ] `cargo test --all` passes
- [ ] No remaining legacy transaction usage
- [ ] Documentation updated
- [ ] Performance regression check

## Progress Tracking

### Completed:
- [x] **Analysis Phase**: Identified all legacy usage locations
- [x] **Planning Phase**: Created comprehensive migration plan
- [ ] **Execution Phase**: Ready to begin

### Current Session Focus:
**Ready for Phase 1 execution** - awaiting user confirmation to proceed with removing legacy trait methods.

### Session Boundaries:
This plan is designed to be executed across multiple sessions. Each phase can be completed independently and validated before moving to the next.

---

**Next Action**: Execute Phase 1 (Architecture Refactor) - Remove legacy transaction methods from PersistenceLayer trait to force compilation-guided migration.
