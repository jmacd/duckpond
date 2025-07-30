# DuckPond Diagnostics Style Guide

**Version**: 1.0 (July 30, 2025)
**Status**: Current standard for all new code

## Quick Start

### Import Pattern

```rust
use diagnostics::*;
```

### Available Macros

- `info!()` - General information
- `debug!()` - Detailed debugging (only shows with DUCKPOND_LOG=debug)
- `warn!()` - Warnings and recoverable issues
- `error!()` - Error conditions

## Usage Examples

### ✅ **Example 1: Simple Variable Capture**

```rust
use diagnostics::*;

fn store_file(path: &str, size: u64) -> Result<(), Error> {
    debug!("Storing file {path} with size {size} bytes");

    // ... storage logic ...

    info!("Successfully stored {path}");
    Ok(())
}
```

### ✅ **Example 2: Complex Data with Formatting**

```rust
use diagnostics::*;

fn process_delta_transaction(version: u64, actions: &[Action]) -> Result<(), Error> {
    let action_count = actions.len();
    debug!("Processing Delta transaction {version} with {action_count} actions");

    match validate_actions(actions) {
        Ok(_) => {
            info!("Delta transaction {version} validated successfully");
        }
        Err(e) => {
            error!("Delta transaction {version} validation failed: {e}");
            return Err(e);
        }
    }

    Ok(())
}
```

### ✅ **Example 3: Structured Fields with Debug Formatting**

```rust
use diagnostics::*;

fn create_node(node_id: &NodeId, content: &Content) -> Result<Node, Error> {
    // Use Debug formatting for complex types
    let content_type = content.content_type();
    debug!("Creating node {node_id:?} with content type {content_type}");

    let node = Node::new(node_id.clone(), content.clone())?;

    info!("Created node {node_id:?} successfully");
    Ok(node)
}
```

## Key Principles

1. **Ergonomic Syntax**: Variables are automatically captured - no manual key-value pairs needed
2. **Wildcard Imports**: Always use `use diagnostics::*;`
3. **Short Names**: Use `debug!()`, not `diagnostics::log_debug!()`
4. **Appropriate Levels**:
   - `debug!()` for detailed information developers need
   - `info!()` for important state changes users might want to see
   - `warn!()` for recoverable issues
   - `error!()` for serious problems

## Environment Variable

Enable debug output: `DUCKPOND_LOG=debug`

---

**Note**: This style guide replaces all previous logging patterns. For historical context and migration instructions, see [ImproveLogging.prompt.md](./ImproveLogging.prompt.md).
