# Run Configuration Factory Design Plan

## Executive Summary

This document outlines the design for a new factory type (`file:data:dynamic` with factory name `run-config`) that stores run configurations as static YAML content within dynamic file nodes. This will replace the current HydroVu `collect` command pattern by moving configuration into the pond itself, making it versionable, queryable, and executable through a unified `pond run` command.

## Motivation

**Current State:**
- HydroVu collector requires external YAML configuration files (e.g., `hydrovu-config.yaml`)
- Configuration is stored outside the pond, separate from the data it generates
- The `pond hydrovu collect <config-path>` command requires filesystem access to config files
- Configuration is not versioned with the pond's transaction history

**Desired State:**
- Run configurations stored as versioned nodes within the pond
- Configuration files accessible via pond paths (e.g., `/configs/hydrovu-main`)
- Single unified command: `pond run /configs/hydrovu-main`
- Factory system automatically locates and executes the appropriate runner
- Configuration history tracked in pond's transaction log

## Architecture Overview

### Component Structure

```
┌─────────────────────────────────────────────────────────────┐
│                      Pond Command Layer                      │
│  Commands: init, list, mknod, run                           │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ pond run /path/to/config
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                Run Command Handler                           │
│  1. Resolves path to node                                   │
│  2. Reads file content (YAML config)                        │
│  3. Identifies factory name from node metadata              │
│  4. Calls factory.execute() with config                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│               DynamicFactory Extension                       │
│  New field: execute_with_context                            │
│  Optional<fn(config, context, ship) -> Result<()>>          │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│            Concrete Factory Implementations                  │
│  - HydroVuRunConfigFactory                                  │
│  - Future: DataPipelineConfigFactory, etc.                  │
└─────────────────────────────────────────────────────────────┘
```

### Factory Pattern: file:data:dynamic with Static Content

**Key Insight:** This factory creates a `file:data:dynamic` node that appears dynamic (factory-based) but returns static YAML content. The dynamic aspect is that it supports execution via the `execute_with_context` entry point.

```
Node Structure:
- entry_type: file:data:dynamic
- factory_name: "hydrovu" (one factory per runner type)
- config_blob: Direct HydroVu YAML configuration (stored in node metadata)

When read:     Returns YAML config as file content (existing HydroVu format)
When executed: Factory's execute_with_context() is called
```

**Design Decision:** Each runner has its own factory named after the runner itself (e.g., "hydrovu", "data-pipeline"). The factory name identifies both the configuration schema and the execution behavior.

## Detailed Design

### 1. Factory System Extensions

#### 1.1 DynamicFactory Struct Extension

**File:** `crates/tlogfs/src/factory.rs`

```rust
pub struct DynamicFactory {
    pub name: &'static str,
    pub description: &'static str,
    
    // Existing entry points
    pub create_directory_with_context: Option<fn(config: Value, context: &FactoryContext) -> TinyFSResult<DirHandle>>,
    pub create_file_with_context: Option<fn(config: Value, context: &FactoryContext) -> TinyFSResult<FileHandle>>,
    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,
    pub try_as_queryable: Option<fn(&dyn tinyfs::File) -> Option<&dyn QueryableFile>>,
    
    // NEW: Execution entry point for run configurations
    pub execute_with_context: Option<fn(
        config: Value, 
        context: &FactoryContext,
        ship: &mut steward::Ship
    ) -> Pin<Box<dyn Future<Output = Result<(), TLogFSError>> + Send>>>,
}
```

**Rationale:**
- `execute_with_context` provides access to full Ship for running operations
- Returns a pinned future for async execution
- Access to FactoryContext for state and parent node information
- Distinct from `create_*` methods which only create nodes

#### 1.2 FactoryRegistry Extension

**File:** `crates/tlogfs/src/factory.rs`

```rust
impl FactoryRegistry {
    /// Execute a run configuration using the specified factory
    pub async fn execute_with_context(
        factory_name: &str,
        config: &[u8],
        context: &FactoryContext,
        ship: &mut steward::Ship
    ) -> Result<(), TLogFSError> {
        let factory = Self::get_factory(factory_name)
            .ok_or_else(|| TLogFSError::TinyFS(
                tinyfs::Error::Other(format!("Unknown factory: {}", factory_name))
            ))?;
        
        let config_value = (factory.validate_config)(config)?;
        
        if let Some(execute_fn) = factory.execute_with_context {
            execute_fn(config_value, context, ship).await
        } else {
            Err(TLogFSError::TinyFS(tinyfs::Error::Other(
                format!("Factory '{}' does not support execution", factory_name)
            )))
        }
    }
}
```

### 2. Run Configuration Factory Implementation

#### 2.1 HydroVu Run Config File

**File:** `crates/tlogfs/src/hydrovu_factory.rs` (NEW FILE)

```rust
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::pin::Pin;
use tinyfs::{AsyncReadSeek, File, FileHandle, NodeMetadata, EntryType};
use crate::factory::FactoryContext;

/// HydroVu configuration (direct format - same as existing hydrovu-config.yaml)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydroVuConfig {
    pub key: String,
    pub secret: String,
    pub hydrovu_path: String,
    pub devices: Vec<HydroVuDevice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydroVuDevice {
    pub id: i64,
    pub name: String,
    pub scope: String,
    pub comment: Option<String>,
}

/// A file that stores static HydroVu YAML configuration
/// but can be executed by the hydrovu factory
pub struct HydroVuConfigFile {
    config: HydroVuConfig,
    config_yaml: Vec<u8>,  // Cached YAML bytes for reading
}

impl HydroVuConfigFile {
    pub fn new(config: HydroVuConfig, config_yaml: Vec<u8>) -> Self {
        Self {
            config,
            config_yaml,
        }
    }
    
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Box::new(self))
    }
}

#[async_trait]
impl File for HydroVuConfigFile {
    async fn open(&self) -> tinyfs::Result<Box<dyn AsyncReadSeek>> {
        use std::io::Cursor;
        let cursor = Cursor::new(self.config_yaml.clone());
        Ok(Box::new(cursor))
    }

    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            entry_type: EntryType::FileDataDynamic,
            size: Some(self.config_yaml.len() as u64),
            modified: None,
            created: None,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

#### 2.2 HydroVu Factory Implementation

**File:** `crates/tlogfs/src/hydrovu_factory.rs` (continued)

```rust
use crate::register_dynamic_factory;
use futures::Future;
use log::info;

/// Validate and parse HydroVu configuration YAML
fn validate_hydrovu_config(config: &[u8]) -> tinyfs::Result<serde_json::Value> {
    let config_str = std::str::from_utf8(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;
    
    // Parse directly as HydroVu config (no wrapper)
    let hydrovu_config: HydroVuConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid HydroVu YAML: {}", e)))?;
    
    serde_json::to_value(hydrovu_config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

/// Create a HydroVu config file node
fn create_hydrovu_config_file(
    config: serde_json::Value,
    _context: &FactoryContext,
) -> tinyfs::Result<FileHandle> {
    let hydrovu_config: HydroVuConfig = serde_json::from_value(config.clone())
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;
    
    // Convert back to YAML bytes for storage
    let config_yaml = serde_yaml::to_string(&hydrovu_config)
        .map_err(|e| tinyfs::Error::Other(format!("YAML serialization error: {}", e)))?
        .into_bytes();
    
    let file = HydroVuConfigFile::new(hydrovu_config, config_yaml);
    Ok(file.create_handle())
}

/// Execute a HydroVu collection run
fn execute_hydrovu_run(
    config: serde_json::Value,
    context: &FactoryContext,
    ship: &mut steward::Ship,
) -> Pin<Box<dyn Future<Output = Result<(), crate::TLogFSError>> + Send>> {
    Box::pin(async move {
        let hydrovu_config: HydroVuConfig = serde_json::from_value(config)
            .map_err(|e| crate::TLogFSError::TinyFS(
                tinyfs::Error::Other(format!("Invalid config: {}", e))
            ))?;
        
        // State is available in context for transaction access
        info!("Executing HydroVu collection run from config node");
        
        // Create and run the HydroVu collector
        let mut collector = hydrovu::HydroVuCollector::new(hydrovu_config, ship.clone())
            .map_err(|e| crate::TLogFSError::TinyFS(
                tinyfs::Error::Other(format!("Failed to create collector: {}", e))
            ))?;
        
        // Async execution - can run long or short, not a problem
        collector.collect().await
            .map_err(|e| crate::TLogFSError::TinyFS(
                tinyfs::Error::Other(format!("Collection failed: {}", e))
            ))?;
        
        info!("HydroVu collection completed successfully");
        Ok(())
    })
}

// Register the factory - note factory name is simply "hydrovu"
register_dynamic_factory!(
    name: "hydrovu",
    description: "HydroVu data collection configuration and runner",
    file_with_context: create_hydrovu_config_file,
    validate: validate_hydrovu_config,
    execute: execute_hydrovu_run
);
```

**Note:** The `register_dynamic_factory!` macro will need a new variant to support the `execute` field.

### 3. CLI Command Implementation

#### 3.1 New "Run" Command

**File:** `crates/cmd/src/main.rs`

```rust
#[derive(Subcommand)]
enum Commands {
    // ... existing commands ...
    
    /// Run a configuration file from the pond
    Run {
        /// Path to the run configuration file
        path: String,
    },
}

// In main() match:
Commands::Run { path } => {
    commands::run_command(&ship_context, &path).await
}
```

#### 3.2 Run Command Implementation

**File:** `crates/cmd/src/commands/run.rs` (NEW FILE)

```rust
use anyhow::{Context, Result};
use log::info;
use crate::common::ShipContext;

/// Execute a run configuration stored in the pond
pub async fn run_command(
    ship_context: &ShipContext,
    config_path: &str,
) -> Result<()> {
    info!("Running configuration from: {}", config_path);
    
    let mut ship = ship_context.open_pond().await
        .with_context(|| "Failed to open pond")?;
    
    // Read the config file to determine factory and get content
    let (factory_name, config_content) = ship.transact_read(
        vec!["pond".to_string(), "run".to_string(), config_path.to_string()],
        move |tx, fs| {
            Box::pin(async move {
                let root = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Navigate to the config file
                let file_ref = root.navigate(config_path).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?
                    .ok_or_else(|| steward::StewardError::DataInit(
                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::NotFound)
                    ))?;
                
                // Get the node metadata to extract factory name
                let state = tx.state()?;
                let node_id = file_ref.node_id();
                let part_id = file_ref.part_id();
                
                let node_record = state.get_node_record(node_id).await
                    .map_err(|e| steward::StewardError::DataInit(e))?
                    .ok_or_else(|| steward::StewardError::DataInit(
                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::NotFound)
                    ))?;
                
                let factory_name = node_record.factory_name
                    .ok_or_else(|| steward::StewardError::DataInit(
                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                            "Config file is not a factory node".to_string()
                        ))
                    ))?;
                
                // Read the config content (the YAML file)
                let mut content = Vec::new();
                let file = file_ref.as_file()
                    .ok_or_else(|| steward::StewardError::DataInit(
                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                            "Path is not a file".to_string()
                        ))
                    ))?;
                
                use tokio::io::AsyncReadExt;
                let mut reader = file.open().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                reader.read_to_end(&mut content).await
                    .map_err(|e| steward::StewardError::DataInit(
                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string()))
                    ))?;
                
                Ok((factory_name, content))
            })
        }
    ).await?;
    
    info!("Executing config with factory: {}", factory_name);
    
    // Execute the configuration using the factory
    ship.transact(
        vec!["pond".to_string(), "run".to_string(), "execute".to_string()],
        move |tx, _fs| {
            let factory_name = factory_name.clone();
            let config_content = config_content.clone();
            Box::pin(async move {
                let state = tx.state()?;
                let parent_node_id = tinyfs::NodeID::root(); // Or appropriate parent
                let context = tlogfs::factory::FactoryContext::new(state, parent_node_id);
                
                tlogfs::factory::FactoryRegistry::execute_with_context(
                    &factory_name,
                    &config_content,
                    &context,
                    &mut ship,
                ).await
                    .map_err(|e| steward::StewardError::DataInit(e))?;
                
                Ok(())
            })
        }
    ).await?;
    
    println!("✓ Configuration executed successfully");
    Ok(())
}
```

### 4. Setup Script Integration

#### 4.1 Modified setup.sh

**File:** `noyo/setup.sh` (or similar)

```bash
#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/release/pond

export POND

cargo build --release

${EXE} init

# Create configs directory
${EXE} mkdir /configs

# Create HydroVu run configuration node
${EXE} mknod hydrovu /configs/hydrovu-main \
    --config-path ${NOYO}/hydrovu-config.yaml

# Traditional dynamic directory nodes
${EXE} mknod dynamic-dir /combined --config-path ${NOYO}/combine.yaml
${EXE} mknod dynamic-dir /singled --config-path ${NOYO}/single.yaml
${EXE} mknod dynamic-dir /reduced --config-path ${NOYO}/reduce.yaml
${EXE} mknod dynamic-dir /templates --config-path ${NOYO}/template.yaml

# To run HydroVu collection:
# ${EXE} run /configs/hydrovu-main
```

#### 4.2 HydroVu Config Example

**File:** `noyo/hydrovu-config.yaml` (existing format, no changes!)

```yaml
# Direct HydroVu configuration - same format as before
key: "your-client-id"
secret: "your-client-secret"
hydrovu_path: "/hydrovu"
devices:
  - id: 123456
    name: "Sensor A"
    scope: "main"
  - id: 789012
    name: "Sensor B"
    scope: "backup"
```

**Key Simplification:** We use the existing HydroVu configuration format directly. No wrapper needed. The factory name ("hydrovu") identifies both the configuration schema and the execution behavior.

### 5. Migration Path

#### 5.1 Deprecate HydroVu Collect Command

**Phase 1:** Both commands work
- `pond hydrovu collect <config-path>` (existing)
- `pond run /configs/hydrovu-main` (new)

**Phase 2:** Mark old command as deprecated
- Add deprecation warning to `hydrovu collect`
- Update documentation

**Phase 3:** Remove old command
- Delete `Commands::Hydrovu(HydroVuCommands::Collect)`
- Keep `Commands::Hydrovu(HydroVuCommands::Create)` for backward compatibility
- Keep `Commands::Hydrovu(HydroVuCommands::List)` for API queries

#### 5.2 Transition Checklist

- [ ] Implement `DynamicFactory.execute_with_context` field
- [ ] Extend `register_dynamic_factory!` macro for execution support
- [ ] Create `run_config_factory.rs` with generic and HydroVu implementations
- [ ] Add `Commands::Run` to CLI
- [ ] Implement `commands::run_command()`
- [ ] Add `FactoryRegistry::execute_with_context()`
- [ ] Update setup.sh scripts
- [ ] Create example run-config YAML files
- [ ] Test end-to-end: mknod → cat → run
- [ ] Document the pattern for future factories
- [ ] Mark `hydrovu collect` as deprecated
- [ ] Add tests for run command

## Benefits

### 1. **Unified Configuration Management**
- All configurations stored in pond, versioned with transaction log
- Can query/list configurations: `pond list /configs/**/*`
- Can inspect configuration: `pond cat /configs/hydrovu-main`

### 2. **Simplified CLI**
- Single command for all runners: `pond run <path>`
- No need to remember different command patterns
- Factory system handles routing automatically

### 3. **Versionable Configurations**
- Configuration changes tracked in transaction history
- Can use `--overwrite` to update configs
- Full audit trail of configuration evolution

### 4. **Extensible Pattern**
- Easy to add new run configurations (data pipelines, export jobs, etc.)
- Each factory implements its own execution logic
- Configuration format validated by factory

### 5. **Testability**
- Configurations are data, not code
- Can programmatically create/modify/test configs
- Integration tests can use the same mechanism

## Design Considerations

### Why file:data:dynamic instead of file:control?

**Decision:** Use `file:data:dynamic` with static content

**Rationale:**
1. Control files are for system files (pid, lock, etc.)
2. Run configs are user data that happens to be executable
3. Dynamic files support factory-based creation naturally
4. Users expect to `cat` configuration files (data behavior)
5. The "dynamic" aspect is execution, not content generation

### Why separate execute_with_context from create?

**Decision:** New field in `DynamicFactory` struct

**Rationale:**
1. Creation and execution are different concerns
2. Not all factories need execution support
3. Execution needs Ship access, creation only needs FactoryContext
4. Clear separation of concerns in the API
5. Allows factories to be read-only OR executable OR both

### One Factory Per Runner

**Decision:** Each runner type gets its own factory (e.g., "hydrovu", "data-pipeline")

**Rationale:**
1. Factory name identifies both config schema and execution behavior
2. Each factory validates its own configuration format
3. No need for runner routing logic
4. Direct configuration format - no wrappers
5. Simpler and more maintainable
6. Each factory can evolve independently

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_validate_hydrovu_config() {
        let config = r#"
key: "test-key"
secret: "test-secret"
hydrovu_path: "/hydrovu"
devices:
  - id: 12345
    name: "Test Sensor"
    scope: "test"
        "#;
        
        let result = validate_hydrovu_config(config.as_bytes());
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_create_hydrovu_config_file() {
        // Test file creation with direct config format
        let config = HydroVuConfig {
            key: "test-key".to_string(),
            secret: "test-secret".to_string(),
            hydrovu_path: "/hydrovu".to_string(),
            devices: vec![
                HydroVuDevice {
                    id: 12345,
                    name: "Test Sensor".to_string(),
                    scope: "test".to_string(),
                    comment: None,
                }
            ],
        };
        
        let config_yaml = serde_yaml::to_string(&config).unwrap().into_bytes();
        let file = HydroVuConfigFile::new(config, config_yaml);
        let metadata = file.metadata().await.unwrap();
        assert_eq!(metadata.entry_type, EntryType::FileDataDynamic);
    }
}
```

### Integration Tests

```bash
#!/bin/bash
# test-run-command.sh

set -e

POND=/tmp/test-pond-run
EXE=./target/debug/pond

rm -rf $POND
export POND

# Initialize pond
$EXE init

# Create configs directory
$EXE mkdir /configs

# Create test config (direct HydroVu format)
cat > /tmp/test-hydrovu-config.yaml << EOF
key: "test-key"
secret: "test-secret"
hydrovu_path: "/test-hydrovu"
devices:
  - id: 12345
    name: "Test Sensor"
    scope: "test"
EOF

# Create hydrovu config node
$EXE mknod hydrovu /configs/test --config-path /tmp/test-hydrovu-config.yaml

# Verify we can read it
echo "=== Reading config ==="
$EXE cat /configs/test

# Execute it (will fail without real API, but tests the command)
echo "=== Executing config ==="
$EXE run /configs/test || echo "Expected to fail without API credentials"

echo "✓ Run command test completed"
```

## Implementation Order

### Phase 1: Core Infrastructure (Week 1)
1. Add `execute_with_context` field to `DynamicFactory`
2. Extend `register_dynamic_factory!` macro
3. Add `FactoryRegistry::execute_with_context()`
4. Create unit tests for factory extensions

### Phase 2: HydroVu Factory (Week 1-2)
1. Create `crates/tlogfs/src/hydrovu_factory.rs`
2. Implement `HydroVuConfigFile` and `HydroVuConfig`
3. Implement `create_hydrovu_config_file` function
4. Implement `execute_hydrovu_run` function
5. Register the "hydrovu" factory
6. Add unit tests

### Phase 3: CLI Integration (Week 2)
1. Add `Commands::Run` to CLI
2. Create `crates/cmd/src/commands/run.rs`
3. Implement `run_command()` function
4. Update command routing in `main.rs`
5. Add integration tests

### Phase 4: Documentation & Migration (Week 2-3)
1. Update setup.sh examples
2. Create example configuration files
3. Write user-facing documentation
4. Add deprecation warnings to old commands
5. Update noyo/ scripts

### Phase 5: Cleanup (Week 3)
1. Remove deprecated commands
2. Refactor if needed
3. Performance testing
4. Final documentation pass

## Future Enhancements

### Additional Runner Types

Each new runner gets its own factory following the same pattern:

```rust
// Example: Data pipeline runner
register_dynamic_factory!(
    name: "data-pipeline",
    description: "Data pipeline configuration and runner",
    file_with_context: create_pipeline_config_file,
    validate: validate_pipeline_config,
    execute: execute_pipeline_run
);
```

Users can then:
```bash
pond mknod data-pipeline /configs/pipeline-main --config-path pipeline-config.yaml
pond run /configs/pipeline-main
```

### Schedule Support

Add scheduling metadata to run configs:

```yaml
runner: "hydrovu-collector"
schedule:
  cron: "0 * * * *"  # Every hour
  enabled: true
config:
  # ... actual config
```

Implement `pond scheduler` daemon that executes configs on schedule.

### Run History

Track execution history in pond metadata:

```sql
SELECT 
    config_path,
    execution_time,
    status,
    duration_ms
FROM run_history
WHERE config_path = '/configs/hydrovu-main'
ORDER BY execution_time DESC;
```

### Dry Run Mode

```bash
pond run /configs/hydrovu-main --dry-run
```

Validates configuration without executing.

## Conclusion

This design provides a clean, extensible pattern for storing and executing run configurations within the pond. It:

- ✅ Unifies configuration management
- ✅ Leverages existing factory system
- ✅ Maintains backward compatibility during migration
- ✅ Provides clear upgrade path
- ✅ Enables future enhancements (scheduling, history, etc.)
- ✅ Follows DuckPond system patterns (single transaction, factory-based)

The implementation is incremental and testable at each phase, minimizing risk while maximizing value.
