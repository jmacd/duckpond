# Troubleshooting Guide for DuckPond Development

## 🔍 **Diagnostic Questions by Symptom**

### **Compilation Errors**

#### **"cannot find type X in crate root"**
**Root Cause**: Dependency violation - importing from wrong layer
```bash
# Quick check
grep -r "use steward" crates/tlogfs/  # Should be empty
grep -r "use tlogfs" crates/tinyfs/   # Should be empty
```
**Solution**: Remove forbidden import, thread parameter instead

#### **"cyclic package dependency"**  
**Root Cause**: Cargo.toml has circular dependency
```bash
# Find the cycle
cargo check --package [COMPONENT] 2>&1 | grep "Cycle:"
```
**Solution**: Remove dependency from lower-layer Cargo.toml

#### **"borrowed value does not live long enough"**
**Root Cause**: Async lifetime issue with Mutex guards
**Solution**: Extract values inside mutex scope, don't return references

#### **"method X is not a member of trait Y"**
**Root Cause**: Added implementation without updating trait definition
**Solution**: Either add to trait or remove implementation

### **Runtime Errors**

#### **"No active transaction - cannot write to file"**
**Root Cause**: Missing transaction context threading
**Diagnostic**: Check if State is being passed down correctly
```rust
// Find where State gets lost
let state = tx.state()?; // ✅ Extract from guard
operation(state).await?; // ✅ Pass explicitly
```

#### **"FileHandle is not an OpLogFile"**  
**Root Cause**: Wrong file type or failed downcast
**Diagnostic**: Check what file implementation is actually being used
```bash
# Look for other File implementations
grep -r "impl.*File for" crates/
```

#### **"Failed to register file versions"**
**Root Cause**: ObjectStore registration issue
**Diagnostic**: Check if node_id/part_id extraction worked
```rust
// Add debugging
println!("node_id: {:?}, part_id: {:?}", node_id, part_id);
```

### **Logic Errors**

#### **"0-byte files causing Parquet read failures"**
**Root Cause**: Custom file handling instead of DataFusion
**Solution**: Use ListingTable which automatically skips 0-byte files

#### **"Empty transactions being committed"**  
**Root Cause**: Missing operation count tracking
**Diagnostic**: Check if transaction guards are being used correctly

#### **"Schema mismatch errors"**
**Root Cause**: OpLog schema evolution not handled properly
**Solution**: Check OplogEntry::for_arrow() field definitions

## 🛠️ **Component-Specific Debugging**

### **TinyFS Issues**

#### **Path Resolution Failures**
```bash
# Check file paths
ls -la /path/to/delta/table/
# Look for _delta_log directory
```

#### **Entry Type Mismatches**
```rust
// Check metadata
let metadata = file_handle.metadata().await?;
println!("Entry type: {:?}", metadata.entry_type);
```

### **TLogFS Issues**

#### **Delta Lake Table Corruption**
```bash
# Inspect Delta Lake structure
ls -la /path/to/delta/table/_delta_log/
# Look for .json files
cat /path/to/delta/table/_delta_log/00000000000000000000.json
```

#### **Transaction State Problems**
```rust
// Check transaction state
println!("Transaction ID: {:?}", persistence.current_transaction_id().await?);
```

#### **DataFusion Integration Issues**
```rust
// Test DataFusion directly
let ctx = SessionContext::new();
let table = ctx.read_parquet("/path/to/file.parquet", ParquetReadOptions::default()).await?;
let batches = table.collect().await?;
```

### **Steward Issues**

#### **Dual Filesystem Coordination**
```bash
# Check both filesystems exist
ls -la /data/path/_delta_log/
ls -la /control/path/_delta_log/
```

#### **Transaction Metadata Mismatch**
```bash  
# Look for transaction metadata files
find /control/path -name "*.json" -exec cat {} \;
```

## 🔄 **Common Fix Patterns**

### **Fixing Circular Dependencies**
1. **Identify the cycle**: `cargo check --package X`
2. **Find the forbidden import**: `grep -r "use upper_layer" lower_layer/`
3. **Replace with parameter threading**:
   ```rust
   // Before: import upper layer type
   use steward::StewardTransactionGuard;
   fn operation(guard: &StewardTransactionGuard) { ... }
   
   // After: thread parameter from upper layer  
   fn operation(state: persistence::State) { ... }
   ```

### **Fixing Lifetime Issues**
```rust
// Before: trying to return reference from async function
pub async fn get_ref(&self) -> &SomeType {
    let guard = self.mutex.lock().await;
    guard.as_ref() // ❌ Guard dropped here
}

// After: extract value before returning
pub async fn get_value(&self) -> SomeType {
    let guard = self.mutex.lock().await;  
    guard.clone() // ✅ Value extracted
}
```

### **Fixing Duplication**
1. **Identify the duplicated functionality**
2. **Find the external library that already implements it**
3. **Replace custom implementation**:
   ```rust
   // Before: custom multi-file table
   struct GenericFileTable { ... }
   
   // After: use DataFusion's proven implementation
   let listing_table = ListingTable::try_new(config).await?;
   ```

## 📊 **Performance Debugging**

### **Slow Compilation**
```bash
# Find compilation bottlenecks
cargo build --timings
# Check dependency graph
cargo tree --package [COMPONENT]
```

### **Slow Runtime**  
```bash
# Profile with basic tools
time cargo run -- [COMMAND]
# Add timing logs
use std::time::Instant;
let start = Instant::now();
// ... operation ...
println!("Operation took: {:?}", start.elapsed());
```

## 🧪 **Testing Strategies**

### **Unit Testing Approach**
```rust
#[tokio::test]
async fn test_with_proper_context() -> Result<(), Box<dyn std::error::Error>> {
    // Create proper transaction context
    let persistence = OpLogPersistence::new(temp_path).await?;
    let tx = persistence.begin().await?;
    let state = tx.state()?;
    
    // Test with real transaction context
    let result = operation_under_test(state).await?;
    
    tx.commit().await?;
    assert_eq!(result, expected);
    Ok(())
}
```

### **Integration Testing**  
```rust
#[tokio::test] 
async fn test_full_stack() -> Result<(), Box<dyn std::error::Error>> {
    // Test through CLI layer to verify all component interactions
    let mut ship = Ship::new(data_path, control_path).await?;
    let tx = ship.begin_transaction(vec!["test".to_string()]).await?;
    
    // Perform operations through natural interfaces
    let result = cli_operation(&tx).await?;
    
    ship.commit_transaction(tx).await?;
    assert_eq!(result, expected);
    Ok(())
}
```

## 🚨 **Red Flags That Require Immediate Attention**

### **Architecture Violations**
- New imports between forbidden layers
- Custom implementations of existing external library features  
- Transaction context being recreated instead of threaded
- Multiple similar implementations of the same concept

### **Code Smells**
- Functions with `unwrap_or_default()` (hiding errors)
- Multiple `as_any()` calls in the same function (wrong abstraction level)
- Long parameter lists (missing context objects)
- Nested `Arc<Mutex<>>` patterns (complex state management)

### **Process Failures**
- Changes touching more than 3 components simultaneously
- Adding more lines than removing (in refactoring)  
- Compilation fixing one error but introducing others
- Test failures that require "updating expected results"

## 📝 **Debugging Log Template**

```
## Issue Description
- **Symptom**: [What's not working]
- **Component**: [Which layer the issue appears in]  
- **Error Message**: [Exact error text]
- **Reproduction**: [Minimal steps to reproduce]

## Investigation
- **Dependency Check**: [Any circular dependencies?]
- **Transaction Context**: [Is State being threaded correctly?]  
- **Architecture Alignment**: [Does this violate component responsibilities?]
- **Duplication Assessment**: [Is this reinventing existing functionality?]

## Root Cause  
- **Architectural**: [Boundary violation, missing context, etc.]
- **Implementation**: [Wrong pattern, missing error handling, etc.]

## Solution Applied
- **Approach**: [What pattern/principle guided the fix]
- **Files Changed**: [List of modified files]
- **Verification**: [How we confirmed the fix works]

## Prevention
- **What would have caught this earlier**: [Process improvement]
- **Related patterns to watch for**: [Similar issues to avoid]
```

---
*When debugging, always start with architectural questions before diving into implementation details.*
