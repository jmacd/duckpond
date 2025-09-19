# Testing Best Practices for DuckPond

## Critical: Never Use Hardcoded Filesystem Paths in Tests

### The Problem

During development, we discovered that several tests were failing **only when run in parallel** (`cargo test --all`) but passing when run individually. The root cause was **hardcoded filesystem paths** that created shared state between parallel test executions.

### Examples of the Anti-Pattern

❌ **WRONG - Causes Test Interference:**
```rust
#[tokio::test]
async fn test_directory_table_creation() {
    // ANTI-PATTERN: Multiple tests using same hardcoded path
    let table_path = "/tmp/test_directory_table".to_string();
    let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
    // ... test continues
}
```

**What happens:** When multiple tests run in parallel, they all try to use the same `/tmp/test_directory_table` path, causing:
- File conflicts
- Data corruption between tests
- Intermittent failures that are hard to debug
- Tests that pass individually but fail in CI/parallel execution

### The Correct Pattern

✅ **CORRECT - Proper Test Isolation:**
```rust
#[tokio::test]
async fn test_directory_table_creation() {
    use tempfile::TempDir;
    
    // CORRECT: Each test gets its own isolated directory
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = DeltaOps::try_from_uri(table_path).await.unwrap().0;
    // ... test continues
    // temp_dir is automatically cleaned up when dropped
}
```

### Why This Works

1. **Isolation**: Each test gets a completely unique temporary directory
2. **Automatic Cleanup**: `TempDir` automatically removes the directory when dropped
3. **Parallel Safety**: No shared state between concurrent test executions
4. **Deterministic**: Tests behave identically whether run individually or in parallel

## Test Architecture Principles

### 1. Complete Environment Isolation

Each test should create its own isolated environment:

```rust
#[tokio::test]
async fn test_pond_operations() {
    use tempfile::TempDir;
    
    // Each test gets its own pond directory
    let temp_dir = TempDir::new().unwrap();
    let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
    
    // All operations happen in isolated environment
    // ...
}
```

### 2. One SessionContext Per OpLogPersistence

DuckPond's architecture ensures one DataFusion SessionContext per OpLogPersistence instance:

```rust
// CORRECT: This is automatically handled by our architecture
let persistence = OpLogPersistence::create(temp_path).await?;
// SessionContext is created and managed per OpLogPersistence instance
```

**Why this matters:**
- Each test gets its own isolated DataFusion state
- No table registration conflicts
- No ObjectStore sharing between tests

### 3. Test Naming Convention

Use descriptive test names that indicate what's being tested:

```rust
#[tokio::test]
async fn test_sql_derived_pattern_matching_with_multiple_files() {
    // Clear what this test validates
}

#[tokio::test] 
async fn test_directory_table_creation_with_temp_isolation() {
    // Indicates proper isolation is used
}
```

## Common Mistakes to Avoid

### 1. Hardcoded Paths (Fixed)
```rust
// ❌ DON'T DO THIS
let path = "/tmp/my_test_data";
let path = "/var/tmp/test.db";  
let path = "test_file.parquet"; // in current directory
```

### 2. Shared Global State
```rust
// ❌ DON'T DO THIS - Static/global variables
static mut TEST_COUNTER: i32 = 0;
lazy_static! {
    static ref GLOBAL_CONFIG: Config = Config::default();
}
```

### 3. Assuming Test Execution Order
```rust
// ❌ DON'T DO THIS - Tests might run in any order
#[tokio::test]
async fn test_step_1_create_data() { /* ... */ }

#[tokio::test] 
async fn test_step_2_process_data() { 
    // ❌ Assumes step_1 ran first!
}
```

## Audit Results (September 2025)

We audited all tests across the DuckPond codebase:

✅ **steward**: All tests properly using `tempdir()` - ✅ CLEAN
✅ **cmd**: All tests properly using `TempDir::new()` - ✅ CLEAN  
✅ **tlogfs**: Most tests using proper tempdir - ✅ 1 FIXED
❌ **tlogfs/query/operations.rs**: Fixed hardcoded `/tmp/test_directory_table` paths

### Fixed Issues

**Before (caused test interference):**
```rust
let table_path = "/tmp/test_directory_table".to_string();
```

**After (proper isolation):**
```rust
let temp_dir = TempDir::new().unwrap();
let table_path = temp_dir.path().to_str().unwrap();
```

**Result**: Tests that were failing in parallel execution now pass consistently.

## Testing Commands

### Run Tests Properly
```bash
# Run all tests (should work with proper isolation)
cargo test --all

# Run single crate tests
cargo test -p tlogfs

# Run tests sequentially (for debugging only)
cargo test --all -- --test-threads=1
```

### Debug Test Failures
```bash
# If tests fail in parallel but pass individually, suspect shared state
cargo test specific_test_name  # Should pass
cargo test --all              # Might fail if shared state exists
```

## Summary

**Key Takeaway**: The #1 cause of flaky/intermittent test failures in DuckPond was hardcoded filesystem paths causing shared state between parallel test executions.

**Solution**: Always use `tempfile::TempDir::new()` or `tempfile::tempdir()` to create isolated temporary directories for each test.

**Verification**: Tests that previously failed in `cargo test --all` but passed individually now pass consistently in all execution modes.

---

*This document was created after fixing test interference issues discovered in September 2025. The root cause was tests sharing hardcoded `/tmp` paths during parallel execution.*