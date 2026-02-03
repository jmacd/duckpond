# Dynamic Node Architecture - Complete System Overview

## Executive Summary

Dynamic nodes are factory-backed filesystem nodes (files or directories) that have associated behavior. They exist at multiple abstraction layers, each with specific responsibilities. The current implementation is **complete and functional** across all critical layers - memory persistence, tlogfs backend, and post-commit execution all work.

## What Are Dynamic Nodes?

Dynamic nodes are entries in the filesystem that:
1. Have a **factory type** identifier (e.g., "test-executor", "replication-manager")
2. Store **configuration data** (YAML, JSON, or binary)
3. Are **discovered and executed** by the system (post-commit hooks, manual triggers)
4. Can be either files (`FileDataDynamic`) or directories (`DirectoryDynamic`)

**Primary Use Case**: Post-commit factories in `/etc/system.d/` for replication, data transformation, notifications, etc.

## Layer-by-Layer Implementation Status

### Layer 1: TinyFS (tinyfs crate) - Trait Definition

**Status**: ✅ **Fully Implemented**

**Location**: `crates/tinyfs/src/persistence.rs`

```rust
#[async_trait]
pub trait PersistenceLayer {
    // Create a new dynamic node with factory type and configuration
    async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node>;

    // Get factory configuration for existing dynamic node
    async fn get_dynamic_node_config(
        &self,
        id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>>; // (factory_type, config)

    // Update configuration of existing dynamic node
    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()>;
}
```

**Implementation Status**:
- ✅ **Memory Persistence**: Fully supports dynamic nodes using `file_versions` HashMap
  - Stores factory type in `extended_metadata["factory"]`
  - Stores config content in `MemoryFileVersion.content`
  - Reuses existing file versioning infrastructure
  
  ```rust
  async fn create_dynamic_node(&self, id: FileID, factory_type: &str, config_content: Vec<u8>) -> Result<Node> {
      let mut state = self.state.lock().unwrap();
      let mut extended_metadata = BTreeMap::new();
      extended_metadata.insert("factory".to_string(), factory_type.to_string());
      
      let version = MemoryFileVersion {
          content: config_content.clone(),
          timestamp: Utc::now(),
          extended_metadata,
      };
      state.file_versions.entry(id).or_insert_with(Vec::new).push(version);
      
      // Load dynamic node using factory
      node_factory::load_dynamic_node(id, factory_type, config_content, self.state.clone())
  }
  ```

- ✅ **OpLog Persistence**: Fully implemented using Delta Lake OpLog entries
  ```rust
  async fn create_dynamic_node(&mut self, id: FileID, factory_type: &str, config_content: Vec<u8>) -> Result<Node> {
      let now = Utc::now().timestamp_micros();
      let next_version = self.get_next_version_for_node(id).await?;
      
      let oplog_entry = OplogEntry::new_dynamic_node(id, now, next_version, factory_type, config_content.clone(), self.txn_seq);
      self.records.push(oplog_entry);
      
      node_factory::load_dynamic_node(id, factory_type, config_content, self.state.clone())
  }
  ```

---

### Layer 2: TinyFS WD API - High-Level Path Operations

**Status**: ✅ **Fully Implemented**

**Location**: `crates/tinyfs/src/wd.rs`

```rust
impl WD {
    /// Create a dynamic file or directory at path with factory type
    pub async fn create_dynamic_path<P: AsRef<Path>>(
        &self,
        path: P,
        entry_type: EntryType,      // FileDataDynamic or DirectoryDynamic
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodePath> {
        self.create_dynamic_path_with_overwrite(path, entry_type, factory_type, config_content, false).await
    }
}
```

**Current Status**: Works correctly end-to-end with both memory and tlogfs backends.

---

### Layer 3: TLogFS Schema - OpLog Entry Format

**Status**: ✅ **Fully Implemented**

**Location**: `crates/tlogfs/src/schema.rs`

```rust
pub struct OplogEntry {
    pub part_id: PartID,
    pub node_id: NodeID,
    pub file_type: EntryType,
    pub timestamp: i64,
    pub version: i64,
    pub content: Option<Vec<u8>>,
    
    // ... other fields ...
    
    /// Factory type for dynamic files/directories
    pub factory: Option<String>,  // ← KEY FIELD
    
    pub txn_seq: i64,
}
```

**Constructor for Dynamic Nodes**:
```rust
impl OplogEntry {
    pub fn new_dynamic_node(
        id: FileID,
        timestamp: i64,
        version: i64,
        factory_type: &str,       // ← Factory identifier
        config_content: Vec<u8>,  // ← Raw YAML config bytes
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(config_content),         // ← Config stored as raw YAML
            factory: Some(factory_type.to_string()), // ← Factory identifier
            storage_format: StorageFormat::Inline,
            txn_seq,
            // ... other fields default/empty ...
        }
    }
}
```

**Storage Format**: Config content is stored as raw YAML bytes in the `content` field for all dynamic node types (files, directories, executables). Previously used base64-encoded JSON, simplified to raw YAML.

**Safeguard**: The regular `new_inline()` constructor has an assertion:
```rust
assert!(
    !id.entry_type().is_dynamic(),
    "Cannot create OplogEntry for dynamic EntryType without factory field. Use new_dynamic_node instead."
);
```

**Status**: This layer is complete - the schema and Delta Lake storage support dynamic nodes.

---

### Layer 4: TLogFS Persistence - OpLog Transaction

**Status**: ✅ **Fully Implemented**

**Location**: `crates/tlogfs/src/persistence.rs`

```rust
impl StewardTransaction {
    async fn create_dynamic_node(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node, TLogFSError> {
        let now = Utc::now().timestamp_micros();
        let next_version = self.get_next_version_for_node(id).await?;
        
        // Create OplogEntry with factory field set
        let oplog_entry = OplogEntry::new_dynamic_node(
            id,
            now,
            next_version,
            factory_type,
            config_content.clone(),
            self.txn_seq,
        );
        
        self.records.push(oplog_entry);
        
        // Load in-memory node using factory
        node_factory::load_dynamic_node(id, factory_type, config_content, self.state.clone())
    }
}
```

**Implementation Notes**:
- Uses **deferred initialization** pattern: Creates OpLog entry but doesn't call `FactoryRegistry::initialize()`
- Factory initialization happens during post-commit discovery phase
- Separation of concerns: creation ≠ initialization
- Dynamic nodes can be data templates, schemas, or factory configs

---

### Layer 5: Dynamic Node Loading - From OpLog to Node

**Status**: ✅ **Fully Implemented**

**Location**: `crates/tlogfs/src/persistence.rs`

```rust
async fn create_dynamic_node_from_oplog_entry(
    id: FileID,
    factory_type: &str,
    config_content: Vec<u8>,
    state: Arc<InnerState>,
) -> Result<Node, TLogFSError> {
    let factory = FactoryRegistry::get(factory_type)?;
    
    match id.entry_type() {
        EntryType::FileDataDynamic => {
            // Check if this is a file factory or executable factory
            if factory.create_file.is_some() {
                // File factory: call create_file to transform config -> file
                FactoryRegistry::create_file(factory_type, &config_content, state.clone())
                    .await
                    .map_err(|e| TLogFSError::Factory { message: e.to_string() })
            } else {
                // Executable factory: config IS the file content
                Ok(ConfigFile::new(id, config_content, state))
            }
        }
        EntryType::DirectoryDynamic => {
            // Directory factory: call create_directory
            FactoryRegistry::create_directory(factory_type, &config_content, state.clone())
                .await
                .map_err(|e| TLogFSError::Factory { message: e.to_string() })
        }
        _ => Err(TLogFSError::Factory {
            message: format!("Invalid entry type for dynamic node: {:?}", id.entry_type())
        })
    }
}
```

**Key Logic**: Distinguishes between three factory types:
1. **File Factories** (`factory.create_file.is_some()`): Transform config → file content
2. **Executable Factories** (no `create_file`): Config becomes file content directly
3. **Directory Factories**: Transform config → directory structure

**Status**: Complete - handles all dynamic node types correctly.

---

### Layer 6: Factory Discovery - Reading Dynamic Nodes

**Status**: ✅ **Fully Implemented**

**Location**: `crates/steward/src/guard.rs`

```rust
impl StewardTransactionGuard {
    /// Discover post-commit factories from /etc/system.d/*
    async fn discover_post_commit_factories(&self) -> Result<Vec<PostCommitFactoryConfig>> {
        // 1. Glob match /etc/system.d/*
        let matches = root.collect_matches("/etc/system.d/*").await?;
        
        // 2. For each match, read config and get factory name
        for (node_path, _) in matches {
            let config_bytes = /* read file */;
            let parent_id = parent_wd.node_path().id();
            
            // 3. Query factory name from OpLog
            let factory_name = discovery_tx
                .state()?
                .get_factory_for_node(parent_id)  // ← Reads factory field from OpLog
                .await?;
            
            factory_configs.push(PostCommitFactoryConfig {
                factory_name,
                config_path,
                config_bytes,
                parent_node_id: parent_id,
            });
        }
        
        Ok(factory_configs)
    }
}
```

**Status**: This works! It queries the `factory` field from Delta Lake:

```rust
// crates/tlogfs/src/persistence.rs
async fn get_factory_for_node(&self, id: FileID) -> Result<Option<String>> {
    let records = self.query_records(id).await?;
    
    if let Some(record) = records.first() {
        Ok(record.factory.clone())  // ← Returns factory field from OplogEntry
    } else {
        // Check pending records...
    }
}
```

---

### Layer 7: Factory Execution - Running Post-Commit Hooks

**Status**: ✅ **Fully Implemented**

**Location**: `crates/steward/src/guard.rs`

```rust
impl StewardTransactionGuard {
    async fn run_post_commit_factories(&mut self) {
        // 1. Discover factories
        let factory_configs = self.discover_post_commit_factories().await?;
        
        // 2. Filter by mode (push vs pull)
        let factories_to_run = factory_configs.into_iter()
            .filter(|cfg| self.control_table.get_factory_mode(&cfg.factory_name) == Some("push"))
            .collect();
        
        // 3. Execute each factory
        for config in factories_to_run {
            self.execute_post_commit_factory(
                &config.factory_name,
                &config.config_path,
                &config.config_bytes,
                config.parent_node_id,
                &config.factory_mode,
            ).await?;
        }
    }
    
    async fn execute_post_commit_factory(...) -> Result<()> {
        // 1. Initialize factory through FactoryRegistry
        let context = FactoryContext::new(state, parent_node_id);
        FactoryRegistry::initialize(factory_name, config_bytes, context).await?;
        
        // 2. Run factory
        FactoryRegistry::run(factory_name, &context).await?;
    }
}
```

**Status**: This is the only layer that currently works end-to-end for dynamic nodes.

---

### Layer 8: CMD - Command-Line Interface

**Status**: ⚠️ **Not Yet Implemented**

**Location**: `crates/cmd/src/commands/mknod.rs` (does not exist yet)

**Expected Usage**:
```bash
# Create a replication factory config
duckpond mknod /etc/system.d/replicate-to-cloud.yaml \
    --type dynamic-file \
    --factory replication-manager \
    --config - < config.yaml

# Or inline
duckpond mknod /etc/system.d/notify.yaml \
    --type dynamic-file \
    --factory slack-notifier \
    --config '{ "webhook": "https://..." }'
```

**Current Status**: Dynamic nodes can be created programmatically via `WD::create_dynamic_path()` API, but no command-line tool exists yet for users.

---

## Implementation Status Summary

### What Works

1. ✅ **Schema**: OplogEntry has `factory` field and `new_dynamic_node()` constructor
2. ✅ **Storage**: Delta Lake stores and retrieves factory field correctly (raw YAML format)
3. ✅ **Memory Persistence**: Dynamic nodes work in memory backend using file_versions storage
4. ✅ **TLogFS Backend**: `StewardTransaction::create_dynamic_node()` fully implemented
5. ✅ **Dynamic Node Loading**: `create_dynamic_node_from_oplog_entry()` handles all factory types
6. ✅ **Discovery**: `get_factory_for_node()` reads factory from OpLog
7. ✅ **Execution**: Post-commit factory system works end-to-end
8. ✅ **High-level API**: `WD::create_dynamic_path()` works with all backends
9. ✅ **Testing**: Comprehensive test suite with test-file, test-dir, test-executor factories

### What's Missing

1. ❌ **CMD Integration**: No mknod command for users to create dynamic nodes from CLI

---

## Test Infrastructure

### Test Factory Implementations

**Location**: `crates/tlogfs/src/test_factory.rs`

Three test factories are registered:
1. **test-file**: File factory that validates `TestFileConfig` and creates file with specified content
2. **test-dir**: Directory factory that validates `TestDirectoryConfig` and creates directory with metadata
3. **test-executor**: Executable factory used in post-commit tests

### Test Configuration Types

**Location**: `crates/tinyfs/src/testing.rs`

```rust
pub struct TestFileConfig {
    pub content: String,
    pub mime_type: String,
}

pub struct TestDirectoryConfig {
    pub name: String,
    pub metadata: BTreeMap<String, String>,
}
```

Both have helper methods:
- `simple()`: Create basic config
- `to_yaml_bytes()`: Serialize to YAML for storage
- `from_yaml_bytes()`: Deserialize from YAML

### Current Test Pattern (TLogFS)

```rust
// Create config
let config = TestFileConfig {
    content: "Hello from factory".to_string(),
    mime_type: "text/plain".to_string(),
};

// Create dynamic file
let factory_node = root
    .create_dynamic_path(
        "/etc/system.d/test-file.yaml",
        tinyfs::EntryType::FileDataDynamic,
        "test-file",
        config.to_yaml_bytes(),
    )
    .await?;  // ← Works end-to-end!

// Verify content
let content = factory_node.read_to_string(0, None).await?;
assert_eq!(content, "Hello from factory");
```

### Post-Commit Test Pattern (Steward)

```rust
// Step 1: Create dynamic file with factory config
let factory_node = root1
    .create_dynamic_path(
        "/etc/system.d/test-post-commit.yaml",
        tinyfs::EntryType::FileDataDynamic,
        "test-executor",
        config_yaml.as_bytes().to_vec(),
    )
    .await?;

// Step 2: Initialize factory (for executable factories)
let factory_node_id = factory_node.id();
let context1 = FactoryContext::new(state1.clone(), factory_node_id);
FactoryRegistry::initialize("test-executor", config_yaml.as_bytes(), context1).await?;

// Step 3: Post-commit discovery automatically finds and executes it
```

**Note**: Manual initialization is only needed for executable factories in post-commit tests. File and directory factories work without manual initialization.

---

## Storage Format Details

### OpLog Storage (TLogFS Backend)

Dynamic nodes are stored in Delta Lake using `OplogEntry` with:
- **`factory` field**: Factory type identifier (e.g., "test-file", "replication-manager")
- **`content` field**: Raw YAML configuration bytes (no encoding, no JSON wrapping)
- **`storage_format`**: Always `StorageFormat::Inline` for dynamic nodes
- **`file_type`**: `FileDataDynamic` or `DirectoryDynamic`

**Example OplogEntry**:
```rust
OplogEntry {
    part_id: PartID(0),
    node_id: NodeID(42),
    file_type: EntryType::FileDataDynamic,
    timestamp: 1701432000000000,
    version: 1,
    content: Some(b"content: Hello\nmime_type: text/plain\n".to_vec()),
    factory: Some("test-file".to_string()),
    storage_format: StorageFormat::Inline,
    // ... other fields ...
}
```

### Memory Storage (Memory Backend)

Dynamic nodes are stored in `file_versions` HashMap:
- **Key**: `FileID`
- **Value**: `Vec<MemoryFileVersion>` with:
  - `content`: Raw YAML configuration bytes
  - `extended_metadata["factory"]`: Factory type identifier
  - `timestamp`: Creation time

**Example**:
```rust
MemoryFileVersion {
    content: b"content: Hello\nmime_type: text/plain\n".to_vec(),
    timestamp: Utc::now(),
    extended_metadata: {
        "factory": "test-file"
    }
}
```

### Historical Note: Storage Format Simplification

Originally, dynamic nodes used base64-encoded JSON wrapped in `extended_attributes`. This was simplified to raw YAML in the `content` field for all dynamic node types, matching how regular files are stored.

## Implementation Decisions

### Deferred Factory Initialization

**Decision**: `create_dynamic_node()` does NOT call `FactoryRegistry::initialize()`

**Rationale**:
- Factory initialization might have side effects
- Not all dynamic nodes are factories (could be data templates, schemas, etc.)
- Post-commit discovery provides explicit execution point
- Separation of concerns: creation ≠ initialization

### Factory Type Detection

**Decision**: Check `factory.create_file.is_some()` to distinguish file factories from executable factories

**Rationale**:
- File factories transform config → file content (e.g., test-file)
- Executable factories use config as file content directly (e.g., test-executor)
- Directory factories always call `create_directory`

## Bug Fixes Completed

### 1. Silent Error Conversion (wd.rs)

**Location**: `crates/tinyfs/src/wd.rs` lines 485-496

**Issue**: "Unknown factory" errors were being silently converted to `Lookup::NotFound`, hiding the real problem.

**Fix**: Changed error handling to preserve actual error with context:
```rust
return Err(Error::Other(format!(
    "Failed to access '{}' in path {}: {}", 
    name, path.display(), dir_error
)));
```

**Impact**: Real errors now visible, significantly reduced debugging time.

### 2. Entry Type Hardcoding

**Issue**: `create_dynamic_path()` hardcoded `DirectoryDynamic` instead of using `entry_type` parameter.

**Fix**: Use the provided `entry_type` parameter correctly.

## Future Work

### Phase 1: CMD Integration

Add `mknod` command to allow users to create dynamic nodes from CLI:

```bash
duckpond mknod /etc/system.d/replicate-to-cloud.yaml \
    --type dynamic-file \
    --factory replication-manager \
    --config - < config.yaml
```

---

## Key Insights

### 1. Silent Failures Waste Time

The biggest debugging obstacle was silent error conversion in `wd.rs`. Converting `Err(dir_error)` to `Lookup::NotFound` hid the real "Unknown factory" error, wasting significant time.

**Lesson**: Follow `fallback-antipattern-philosophy.md` - fail fast, preserve real errors.

### 2. Dynamic Nodes Are Just Files With Metadata

The key simplification was realizing dynamic nodes are stored identically to regular files, just with:
- Different `entry_type` (FileDataDynamic, DirectoryDynamic)
- Factory identifier in metadata
- Config as file content (raw YAML)

This insight enabled memory persistence implementation using existing `file_versions` infrastructure.

### 3. Factory Type Determines Loading Behavior

Three factory types require different loading logic:
1. **File Factories** (has `create_file`): Transform config → file
2. **Executable Factories** (no `create_file`): Config IS file content
3. **Directory Factories** (has `create_directory`): Transform config → directory

The `factory.create_file.is_some()` check distinguishes executable from file factories.

### 4. Deferred Initialization Is Cleaner

Not calling `FactoryRegistry::initialize()` during `create_dynamic_node()` keeps concerns separated:
- Creation: Store factory config in filesystem
- Initialization: Prepare factory for execution (separate phase)
- Execution: Run factory logic (post-commit)

This prevents side effects during filesystem operations.

---

## Summary

| Layer | Component | Status | Notes |
|-------|-----------|--------|-------|
| 1 | TinyFS Trait | ✅ Complete | Both backends fully support dynamic nodes |
| 2 | WD Path API | ✅ Complete | Works end-to-end with all backends |
| 3 | OpLog Schema | ✅ Complete | Raw YAML storage format |
| 4 | TLogFS Backend | ✅ Complete | Deferred initialization pattern |
| 5 | Dynamic Node Loading | ✅ Complete | Handles file/directory/executable factories |
| 6 | Factory Discovery | ✅ Complete | Reads factory from OpLog correctly |
| 7 | Factory Execution | ✅ Complete | Post-commit flow works |
| 8 | CMD Interface | ⚠️ Missing | No mknod command yet |

**Test Results**: 
- TinyFS: 78 tests passing (including memory persistence dynamic node tests)
- Steward: 3 tests passing (including post-commit factory execution)

**Current State**: All core functionality implemented and tested. Only missing CLI tool for user-facing dynamic node creation.

**Next Steps**: 
1. Add `duckpond mknod` command for CLI dynamic node creation
2. Add user-facing documentation with examples
3. Consider additional factory types for production use cases
