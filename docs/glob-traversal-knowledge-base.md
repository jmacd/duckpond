# DuckPond TinyFS Glob Traversal Knowledge Base

## Overview

This document describes the glob pattern matching and traversal system in DuckPond's TinyFS crate, including the bug that was discovered and fixed regarding `/**` patterns and trailing slash semantics.

## Background

The glob traversal system allows filesystem pattern matching similar to shell globbing. It supports:
- Literal path components (`file.txt`)
- Single wildcards (`*.txt`)
- Double wildcards (`**` for recursive descent)
- Capture groups for matched components

## Architecture

### Core Components

1. **WildcardComponent** (`glob.rs`): Parses and matches individual path components
   - `Normal(String)`: Literal component
   - `Wildcard { pattern, .. }`: Single wildcard with pattern
   - `DoubleWildcard { .. }`: Recursive wildcard

2. **WD::visit_with_visitor** (`wd.rs`): Entry point for glob traversal
   - Strips root prefix if needed
   - Parses glob pattern into components
   - Initiates recursive traversal

3. **WD::visit_recursive_with_visitor** (`wd.rs`): Recursive traversal engine
   - Handles each component type
   - Manages visited nodes to prevent cycles
   - Captures wildcard matches

4. **WD::visit_match_with_visitor** (`wd.rs`): Per-node matching logic
   - Resolves symlinks
   - Handles terminal vs non-terminal pattern positions
   - Manages recursive descent

### Key Data Structures

- **Visitor trait**: Callback interface for processing matched nodes
- **NodePath**: Represents a filesystem node with its path
- **Captured strings**: Wildcard matches collected during traversal
- **Visited sets**: Cycle detection per pattern level

## The `/**` Bug

### Problem Description

The `/**` pattern was not working correctly - it only matched files at the root level instead of recursively descending into subdirectories.

### Root Cause

The issue was in `visit_match_with_visitor` method:

```rust
// BUGGY CODE
if pattern.len() == 1 {
    let result = visitor.visit(child.clone(), captured).await?;
    results.push(result);
    return Ok(()); // ❌ Early return prevented recursion for DoubleWildcard
}
```

When a `**` pattern was the last (and only) component, the method would visit the node but then return early, preventing recursive descent into directories.

### Solution

The fix was to check if the pattern is a `DoubleWildcard` and continue with recursion logic even for terminal positions:

```rust
// FIXED CODE
let is_double_wildcard = matches!(pattern[0], WildcardComponent::DoubleWildcard { .. });
if pattern.len() == 1 {
    let result = visitor.visit(child.clone(), captured).await?;
    results.push(result);
    
    // For DoubleWildcard patterns, continue recursing even at terminal position
    if !is_double_wildcard {
        return Ok(());
    }
    // Continue to recursion logic below for DoubleWildcard
}
```

### Additional Fix for `/**/*.txt`

The `/**/*.txt` pattern also had issues. The problem was that `**` needs to handle two cases:
1. **Zero directories**: Match the current directory, then try the next pattern component
2. **One or more directories**: Recurse into subdirectories with the same pattern

The fix was in `visit_recursive_with_visitor`:

```rust
WildcardComponent::DoubleWildcard { .. } => {
    // Case 1: Match zero directories - try next pattern component in current directory
    if pattern.len() > 1 {
        self.visit_recursive_with_visitor(&pattern[1..], visited, captured, stack, results, visitor).await?;
    }
    
    // Case 2: Match one or more directories - recurse into children with same pattern
    // ... existing recursion logic
}
```

## Trailing Slash Semantics

### Shell Behavior

Real shell globbing treats trailing slashes as directory filters:
- `**` matches all files and directories
- `**/` matches only directories
- `*.txt` matches all .txt files
- `*.txt/` matches nothing (files aren't directories)

### Current Implementation

The current TinyFS implementation does NOT distinguish between patterns with and without trailing slashes. This is a semantic gap that should be addressed.

### Expected Behavior

Patterns ending with `/` should only match directories:
- `/**/` should match only directories recursively
- `/subdir/*/` should match only directories inside subdir
- `/**/*.txt/` should match nothing (no .txt directories)

## Test Coverage

### Existing Tests

1. **Basic glob tests** (`glob.rs`): Pattern parsing and component matching
2. **Visit tests** (`visit.rs`): Directory traversal and visitor patterns
3. **Memory tests** (`memory.rs`): File system operations with glob patterns
4. **Glob bug tests** (`glob_bug.rs`): Specific tests for the `/**` bug

### Test Results

- `/**` pattern: ✅ Fixed - now finds all files recursively
- `/**/*.txt` pattern: ✅ Fixed - now finds all .txt files including at root
- Order independence: ✅ Fixed - tests no longer depend on traversal order
- Trailing slash behavior: ⚠️ Not implemented - treats `/` and no `/` the same

## Implementation Details

### Pattern Processing

1. **Root stripping**: Patterns starting with `/` have the root removed when called from root
2. **Component parsing**: Patterns are split into `WildcardComponent` sequences
3. **Recursive descent**: Each component is processed recursively through the filesystem

### Visitor Pattern

The visitor pattern allows flexible processing of matched nodes:
- `CollectingVisitor`: Collects all matches into a vector
- `FileContentVisitor`: Reads file contents for matches
- `BasenameVisitor`: Extracts just filenames

### Cycle Detection

The system maintains `visited` sets indexed by pattern level to prevent infinite loops when encountering symlinks or dynamic directories.

## Performance Considerations

- **Async traversal**: All I/O is asynchronous for better performance
- **Cycle detection**: Prevents infinite loops but adds memory overhead
- **Pattern compilation**: Patterns are parsed once and reused
- **Lazy evaluation**: Only visits directories that match patterns

## Future Improvements

1. **Trailing slash support**: Implement directory-only filtering for patterns ending with `/`
2. **Pattern optimization**: Cache compiled patterns for reuse
3. **Parallel traversal**: Consider concurrent directory traversal for large filesystems
4. **Progress reporting**: Add callback mechanism for long-running traversals

## Summary

The TinyFS glob system provides shell-like pattern matching with proper recursive descent. The main bug with `/**` patterns has been fixed, and the system now correctly handles both simple and complex wildcard patterns. The next major enhancement would be implementing proper trailing slash semantics to match shell behavior more closely.
