# DuckPond Test File Cleanup Plan

## Current Status (GOOD NEWS FIRST)

### ✅ MISSION ACCOMPLISHED: Dangerous &[u8] Interfaces Removed
The core goal has been achieved. All dangerous memory interfaces are gone from production code:

**Removed Dangerous Methods:**
- `WD::create_file_path(&[u8])` - could load large files into memory
- `WD::create_file_path_with_type(&[u8])` - could load large files into memory  
- `FS::create_file(&[u8])` - removed entirely
- `PersistenceLayer::store_file_content(&[u8])` - removed entirely
- `PersistenceLayer::create_file_node(&[u8])` - removed entirely

**Added Safe Streaming Replacements:**
- `WD::create_file_path_streaming()` - returns (NodePath, AsyncWrite)
- `WD::create_file_path_streaming_with_type()` - returns (NodePath, AsyncWrite)
- `WD::create_file_writer()` - convenience, returns just AsyncWrite
- `WD::create_file_writer_with_type()` - convenience, returns just AsyncWrite
- `PersistenceLayer::internal_store_accumulated_content()` - for writer completion

**Production Code Updated Successfully:**
- `crates/cmd/src/commands/copy.rs` - uses streaming pattern
- `crates/steward/src/ship.rs` - uses streaming pattern
- All production code now memory-safe for large files

### ❌ PROBLEM: Test Files in Chaos
Automated sed commands created a mess in test files:
- Duplicate imports: `use crate::async_helpers::test_helpers;` repeated multiple times
- Malformed calls: `roottest_helpers::create_file_path()` and `wdtest_helpers::create_file_path()`
- Mixed up variable names with function names
- Import errors and compilation failures

## Cleanup Plan

### Phase 1: Revert Test File Damage
```bash
# Revert all test file changes, keeping production code changes
git checkout HEAD -- crates/tinyfs/src/tests/
git checkout HEAD -- crates/tlogfs/src/tests.rs
git checkout HEAD -- crates/tlogfs/src/test_backend_query.rs
```

### Phase 2: Make Convenience Helpers Public
The `convenience` module in `async_helpers.rs` is already created and works well. Ensure it's public:

```rust
// In crates/tinyfs/src/async_helpers.rs
pub mod convenience {
    // ... existing implementation is correct
}
```

### Phase 3: Fix Key Test Files Manually (NO SCRIPTS)
Edit test files one by one to use convenience helpers:

**Pattern to Replace:**
```rust
// OLD (dangerous)
root.create_file_path("/path", b"content").await.unwrap();

// NEW (safe)
use crate::async_helpers::convenience;
convenience::create_file_path(&root, "/path", b"content").await.unwrap();
```

**Files to Fix (Priority Order):**
1. `crates/tinyfs/src/tests/memory.rs` - basic functionality tests
2. `crates/tlogfs/src/tests.rs` - core TLogFS tests
3. Other test files as needed for `cargo test --all` to pass

### Phase 4: Update Memory Bank Documentation
Document the completed interface cleanup in memory-bank files.

## Key Rules Going Forward

1. **NEVER use sed/awk/automated scripts for code changes**
2. **Always manually edit files one at a time**
3. **Test each file individually after editing**
4. **Use convenience helpers for test code only**
5. **Production code must use streaming interfaces**

## Test Pattern Templates

### For tinyfs tests:
```rust
use crate::async_helpers::convenience;

// Create file
convenience::create_file_path(&root, "/path", b"content").await?;

// Create file with type
convenience::create_file_path_with_type(&wd, "path", b"content", EntryType::FileTable).await?;
```

### For tlogfs tests:
```rust
// TLogFS doesn't have access to tinyfs convenience helpers
// Use streaming pattern directly:
let (_, mut writer) = working_dir.create_file_path_streaming("path").await?;
use tokio::io::AsyncWriteExt;
writer.write_all(b"content").await?;
writer.shutdown().await?;
```

## Success Criteria

1. ✅ All dangerous &[u8] interfaces removed (DONE)
2. ✅ Production code uses streaming (DONE) 
3. ✅ Convenience helpers available (DONE)
4. 🔄 Test files compile and pass
5. 🔄 `cargo test --all` succeeds
6. 🔄 No memory safety regressions

## Current Git State

The current diff shows:
- Production code successfully converted to streaming ✅
- Dangerous APIs successfully removed ✅
- Test files in broken state due to sed automation ❌

After cleanup, we should have:
- All production benefits retained ✅
- Test files working with convenience helpers ✅
- Clean, maintainable codebase ✅
