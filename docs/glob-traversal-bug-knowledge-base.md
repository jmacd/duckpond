# Glob Traversal Bug Knowledge Base

## Bug Summary
The command `pond list '/**'` produces no output, despite all unit tests passing. This is a pattern matching bug in the TinyFS glob traversal system.

## Root Cause Analysis

### 1. Pattern Processing Flow
```
'/**' → strip_root() → '**' → parse_glob() → [DoubleWildcard{index: 0}]
```

### 2. Key Issue: Missing Root Directory Match
The current `DoubleWildcard` traversal logic only recurses into child directories but **does not match the current directory itself** when it's at the root level.

### 3. Shell Semantics vs Current Implementation
- **Shell behavior**: `/**` should match all files/directories recursively from root
- **Current behavior**: Only matches children of root, not root itself
- **Missing case**: When `/**` reduces to `**` at root, it should match the root directory contents

## Technical Details

### Current Traversal Logic (wd.rs:420-440)
```rust
WildcardComponent::DoubleWildcard { .. } => {
    // Match any single component and recurse with the same pattern
    use futures::StreamExt;
    let mut dir_stream = self.read_dir().await?;
    let mut children = Vec::new();
    while let Some(child) = dir_stream.next().await {
        children.push(child);
    }
    
    for child in children {
        captured.push(child.basename().clone());
        self.visit_match_with_visitor(
            child, true, pattern, visited, captured, stack, results, visitor,
        ).await?;
        captured.pop();
    }
}
```

### Problem Analysis
1. **Only processes children**: The logic iterates through `read_dir()` results
2. **No current directory match**: Never considers the current directory as a match
3. **Pattern length check**: The `pattern.len() == 1` check in `visit_match_with_visitor` determines when to emit results

### Expected Behavior for `/**`
When pattern is `[DoubleWildcard{index: 0}]` at root:
1. **Should match root directory contents directly** (files and subdirectories)
2. **Should also recurse into subdirectories** with the same pattern
3. **Should handle the terminal case** where `pattern.len() == 1`

## Test Coverage Gap

### Current Tests Pass But Miss the Bug
- Existing tests like `/**/*.txt` work because they have additional pattern components
- Pattern `/**` with single `DoubleWildcard` is not tested at CLI level
- Unit tests in `memory.rs` don't cover the bare `/**` case

### Missing Test Cases
1. `list '/**'` - should list all files recursively
2. `list '**'` - should list all files recursively (non-root)
3. Root-level single double-wildcard patterns

## Fix Strategy

### 1. Modify DoubleWildcard Handling
In `visit_recursive_with_visitor`, when handling `DoubleWildcard`:
- If `pattern.len() == 1`, emit matches for all children directly
- Still maintain recursive behavior for pattern continuation

### 2. Add Test Coverage
- Add CLI-level integration tests for `/**` patterns
- Add unit tests for single `DoubleWildcard` components
- Verify shell semantic compliance

### 3. Root Path Handling
- Ensure `strip_root()` correctly handles `/**` → `**`
- Verify `Lookup::Empty` cases are handled correctly
- Test edge cases with different path formats

## Code Locations

### Primary Files
- `/crates/tinyfs/src/wd.rs` - Main traversal logic (lines 420-440)
- `/crates/tinyfs/src/glob.rs` - Pattern parsing
- `/crates/tinyfs/src/path.rs` - Root stripping utility
- `/crates/cmd/src/commands/list.rs` - CLI command entry point

### Test Files
- `/crates/tinyfs/src/tests/memory.rs` - Current glob tests
- `/crates/tinyfs/src/tests/visit.rs` - Visitor pattern tests

## Related Context

### TinyFS Architecture
- **Backend trait system**: MemoryBackend for testing, OpLogBackend for persistence
- **Visitor pattern**: Used for glob traversal with custom logic
- **NodePath system**: Represents filesystem nodes with path context

### Recent Changes
- Structured logging migration completed (all print statements converted)
- Production-ready CLI with enhanced copy command
- Clean four-crate architecture established

## Next Steps

1. **Build comprehensive test suite** covering the bug case
2. **Implement fix** in `DoubleWildcard` handling logic
3. **Verify fix** with both unit tests and CLI integration tests
4. **Update documentation** with glob pattern semantics
