# Factory Subcommand Architecture

## Overview

Factories can now implement clap-based subcommand menus, enabling rich CLI interactions beyond simple post-commit execution.

## Design Pattern

### Two Execution Modes

**1. Post-Commit Mode** (Automatic)
- Triggered after successful write transactions
- Args come from Steward master configuration (`factory_mode` in control table)
- Example: `args[0] = "push"` or `args[0] = "pull"`

**2. Subcommand Mode** (Explicit)
- Invoked via `pond run /path/to/factory <subcommand> [args...]`
- Uses clap for argument parsing
- Enables factory-specific operations

### Mode Detection

```rust
async fn execute_factory(
    config: Value,
    context: FactoryContext,
    mode: ExecutionMode,
    args: Vec<String>,
) -> Result<(), TLogFSError> {
    // Subcommand mode: user explicitly ran `pond run /path/to/factory ...`
    if !args.is_empty() && mode == ExecutionMode::InTransactionWriter {
        return execute_subcommand(config, context, args).await;
    }
    
    // Post-commit mode: automatic execution after commit
    let operation_mode = args.get(0).unwrap_or("default");
    // ... dispatch based on operation_mode
}
```

## Remote Factory Example

### Subcommands

```bash
# Generate replication command with base64-encoded config
pond run /etc/system.d/10-remote replicate

# List available backup bundles
pond run /etc/system.d/10-remote list-bundles

# List with details
pond run /etc/system.d/10-remote list-bundles --verbose

# Verify backup integrity
pond run /etc/system.d/10-remote verify

# Verify specific version
pond run /etc/system.d/10-remote verify --version 5
```

### Implementation

```rust
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "remote", about = "Remote backup and replication operations")]
struct RemoteCommand {
    #[command(subcommand)]
    command: RemoteSubcommand,
}

#[derive(Debug, Subcommand)]
enum RemoteSubcommand {
    /// Generate replication command with base64-encoded config
    Replicate,
    
    /// List available backup bundles
    ListBundles {
        /// Show detailed information
        #[arg(long)]
        verbose: bool,
    },
    
    /// Verify backup integrity
    Verify {
        /// Specific version to verify
        #[arg(long)]
        version: Option<i64>,
    },
}

async fn execute_subcommand(
    config: RemoteConfig,
    context: FactoryContext,
    args: Vec<String>,
) -> Result<(), TLogFSError> {
    // Prepend factory name for clap
    let mut clap_args = vec!["remote".to_string()];
    clap_args.extend(args);
    
    // Parse with clap
    let cmd = RemoteCommand::try_parse_from(&clap_args)?;
    
    // Dispatch to handler
    match cmd.command {
        RemoteSubcommand::Replicate => {
            execute_replicate_subcommand(config, context).await
        }
        RemoteSubcommand::ListBundles { verbose } => {
            execute_list_bundles_subcommand(config, verbose).await
        }
        RemoteSubcommand::Verify { version } => {
            execute_verify_subcommand(config, version).await
        }
    }
}
```

## Migration: `pond control --mode=replicate` → `pond run ... replicate`

### Before (Hard-coded)
```bash
# ❌ Hard-coded path, inflexible
pond control --mode=replicate /etc/system.d/10-remote
```

**Problems:**
- Hard-coded path in control command
- Not extensible to other operations
- Doesn't fit factory execution model

### After (Subcommand)
```bash
# ✅ Uses factory subcommand architecture
pond run /etc/system.d/10-remote replicate
```

**Benefits:**
- Consistent with other factory operations
- Extensible (add more subcommands easily)
- Works for any factory location
- Uses existing `pond run` infrastructure

## Future Factory Subcommands

### HydroVu Factory
```bash
# Fetch data for specific date
pond run /etc/hydrovu fetch --date=2025-10-20

# List available locations
pond run /etc/hydrovu list-locations

# Show data statistics
pond run /etc/hydrovu stats --location=site1
```

### CSV Factory
```bash
# Preview CSV schema
pond run /etc/data/sensors preview

# Validate CSV format
pond run /etc/data/sensors validate

# Show column statistics
pond run /etc/data/sensors describe
```

### SQL Factory
```bash
# Show query plan
pond run /etc/views/monthly explain

# Refresh view
pond run /etc/views/monthly refresh

# Show dependencies
pond run /etc/views/monthly deps
```

## Implementation Checklist

- [x] Add clap dependency to tlogfs
- [x] Create `RemoteCommand` and `RemoteSubcommand` enums
- [x] Implement `execute_subcommand()` dispatcher
- [x] Implement `list-bundles` subcommand (working)
- [x] Implement `verify` subcommand (working)
- [x] Implement `replicate` subcommand (placeholder - needs control table access)
- [ ] Add control table access to FactoryContext for replicate
- [ ] Remove `pond control --mode=replicate` command (after replicate is complete)
- [ ] Update documentation
- [x] Test all subcommands (test script created)

## Current Status

### Working Subcommands ✅

1. **list-bundles** - Fully functional
   ```bash
   pond run /etc/system.d/10-remote list-bundles
   pond run /etc/system.d/10-remote list-bundles --verbose
   ```

2. **verify** - Fully functional
   ```bash
   pond run /etc/system.d/10-remote verify
   pond run /etc/system.d/10-remote verify --version 5
   ```

### In Progress 🚧

3. **replicate** - Placeholder implementation
   ```bash
   pond run /etc/system.d/10-remote replicate
   # Currently shows implementation note about needing control table access
   ```
   
   **Blocker**: Needs access to pond metadata (pond_id, birth_timestamp, etc.) which is stored in the control table. The factory context doesn't currently provide this.
   
   **Solutions being considered**:
   - Option A: Extend `FactoryContext` with control table access
   - Option B: Add State API to query control table metadata
   - Option C: Store pond metadata in a well-known filesystem location (e.g., `/etc/pond-metadata.json`)
   
   **Workaround**: Use `pond control --mode=replicate` until this is resolved.

## Testing

```bash
# 1. Create test pond with remote backup
pond init
echo "storage_type: local
path: /tmp/pond-backups
compression_level: 3" > remote-config.yaml
pond mknod remote /etc/system.d/10-remote --config-path remote-config.yaml

# 2. Write some data to create backups
pond run /etc/hydrovu  # Creates versions 1, 2, 3...

# 3. Test subcommands
pond run /etc/system.d/10-remote list-bundles
pond run /etc/system.d/10-remote list-bundles --verbose
pond run /etc/system.d/10-remote verify
pond run /etc/system.d/10-remote verify --version 1

# 4. Test replicate command
pond run /etc/system.d/10-remote replicate
# Should output: init --config=<BASE64>
```

## Architecture Benefits

1. **Extensibility** - Easy to add new operations without changing core commands
2. **Consistency** - All factory operations use same `pond run` interface
3. **Discoverability** - Clap provides automatic `--help` for each subcommand
4. **Type Safety** - Clap handles argument parsing and validation
5. **Factory Control** - Each factory defines its own command structure

## Next Steps

1. Complete `replicate` subcommand to fully replace `pond control --mode=replicate`
2. Remove old control command after migration
3. Add more useful subcommands based on user needs
4. Document subcommand conventions for factory developers
5. Consider adding subcommands to other factories (hydrovu, csv, etc.)
