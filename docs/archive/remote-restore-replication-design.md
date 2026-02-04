# Remote Restore & Replication Design

## Overview

This document describes the design for restoring ponds from remote backups and continuous replication between ponds. It extends the remote backup system (see `remote-backup-design.md`) with pull and init capabilities.

# Remote Restore & Replication Design

**Status**: ✅ **Init mode implementation COMPLETE and tested (Oct 2025)**

## Quick Start - Using Init Mode

```bash
# 1. Create source pond with backup (push mode)
pond init
pond mknod remote /etc/system.d/10-remote --config-path remote-config.yaml
# Factory mode defaults to "push" - backups created automatically after commits

# 2. Create replica from backup (init mode)
pond init --from-backup remote-config.yaml
# Same config file! Mode="init" is passed as argument during restore

# That's it! Replica has identical Delta Lake history.
```

**Single Config File** (used by both source and replica):

`remote-config.yaml`:
```yaml
storage_type: local  # or s3
path: /tmp/pond-backups
compression_level: 3
# Note: No mode field! Mode comes from Steward master configuration.
```

**How Modes Work**:
- **Source pond**: Factory mode stored in control table as "push" (default)
- **Replica pond**: During `pond init --from-backup`, mode "init" passed as args
- **After init**: (Future) Factory mode set to "pull" in control table for continuous sync

## Overview

## Architecture Flow Diagram

```
SOURCE POND (Push Mode)                    BACKUP STORAGE                    REPLICA POND (Init Mode)
┌─────────────────────┐                   ┌──────────────────┐              ┌─────────────────────┐
│ Commit Transaction  │                   │  backups/        │              │ pond init           │
│   ├─ Parquet files  │──── Bundle ──────>│    version-001/  │              │  --from-backup      │
│   └─ Delta commit   │     Creation      │      bundle.tar  │<─── Scan ────│                     │
│                     │                   │      .zst        │    Versions  │                     │
│ Post-Commit Factory │                   │    version-002/  │              │                     │
│  (remote factory)   │                   │      bundle.tar  │              │                     │
│                     │                   │      .zst        │              │ For each version:   │
└─────────────────────┘                   │    ...           │              │  ├─ Download bundle │
                                          │    version-006/  │              │  ├─ Extract files   │
Each bundle contains:                     │      bundle.tar  │              │  │   ├─ Parquets    │
├─ Parquet data files                     │      .zst        │              │  │   └─ _delta_log/ │
└─ _delta_log/*.json ← CRITICAL!          └──────────────────┘              │  └─ Apply in txn   │
                                                                            │    (Delta ++vers)  │
                                                                            └─────────────────────┘
                                                                            Result: TRUE REPLICA
                                                                            (identical Delta history)
```

## Key Learnings - Critical Architecture Insight

### The Commit Log Discovery

**Initial Assumption (WRONG)**: Bundles only need Parquet data files. Restore applies them and Delta Lake creates new commits.

**Reality (CORRECT)**: For true replication, bundles MUST include `_delta_log/*.json` commit files.

**Why This Matters**:
- **Without commit logs**: Restore creates NEW commits → replica Delta history differs from source
- **With commit logs**: Restore copies existing commits → replica Delta history IDENTICAL to source

**Implementation**:
1. `create_backup_bundle()` now includes `_delta_log/{version:020}.json` for each version
2. `apply_parquet_files()` copies both Parquet files AND commit logs
3. `delta_table.load()` discovers existing commits (no new commits created)
4. Replica version matches source exactly

**Result**: TRUE READ-ONLY REPLICATION - replica never writes transactions, only copies from bundles.

## Three Operational Modes

The remote factory supports three modes of operation:

### 1. Push Mode (Backup - Current Implementation)
**Purpose**: Original pond backs up data after each commit

**Flow**:
```
Local Commit → Post-Commit Factory → Bundle & Upload → S3
```

**Configuration** (YAML file - identical everywhere):
```yaml
storage_type: s3
bucket: my-pond-backups
compression_level: 3
# Note: No mode field! Mode comes from Steward master config.
```

**Master Configuration** (in control table):
```bash
# Set factory mode to "push" for source pond
factory_mode:remote = "push"
```

**Use Case**: Production pond creating backups for disaster recovery

---

### 2. Init Mode (Restore from Backup - COMPLETE ✅)
**Purpose**: Create new pond by restoring complete history from backup

**Flow**:
```
Empty Pond → Init Command → Download All Bundles → Replay Transactions → Ready Pond
```

**Configuration** (same YAML file):
```yaml
storage_type: s3
bucket: my-pond-backups
compression_level: 3
# Mode passed to init command, not in config
```

**Command**:
```bash
pond init --from-backup remote-config.yaml
# During init, mode="init" is passed as args[0] to factory
```

**Use Case**: 
- Disaster recovery (restore from backup)
- Creating read replicas
- Migrating pond to new location

---

### 3. Pull Mode (Continuous Sync - IN PROGRESS 🚧)
**Purpose**: Replica pond continuously syncs new versions from source

**Flow**:
```
Manual Trigger → Post-Commit Factory → Check Remote for New Versions → Download & Apply
```

**Configuration** (same YAML file):
```yaml
storage_type: s3
bucket: my-pond-backups
compression_level: 3
# Mode comes from Steward master config
```

**Master Configuration** (set after init):
```bash
# Set factory mode to "pull" for replica pond
factory_mode:remote = "pull"
```

**Commands**:
```bash
# After init, set mode to pull
# (Future: will be automatic)
pond control --mode sync  # Manually trigger sync
```

**Use Case**:
- Read replicas staying in sync
- Multi-datacenter replication
- Follower ponds for analytics

## Architecture

### Configuration Schema

```yaml
# init.yaml - Bootstrap config for replica pond
remote:
  # Storage configuration (same as backup)
  storage_type: s3      # or "local"
  bucket: my-pond-backups
  region: us-west-2
  key: AKIAXXXXXXXXX
  secret: secretXXXXXX
  endpoint: https://s3.amazonaws.com  # Optional
  
  # Mode configuration
  mode: init            # "push", "init", or "pull"
  
  # Replica-specific settings
  source_pond_id: "uuid-of-source-pond"  # Optional
  config_path: /etc/system.d/10-remote   # Where to write config in new pond
  
  # Pull mode settings
  sync_interval: 60     # Seconds between sync checks (pull mode only)
  auto_switch_to_pull: true  # After init, switch to pull mode
  
  # Compression (inherited from backup)
  compression_level: 3
```

### Command-Line Interface

#### Create Backup Pond (Push Mode)
```bash
# Initialize pond with push mode (default)
pond init
pond mknod remote /etc/system.d/10-remote --config-path remote-push-config.yaml

# remote-push-config.yaml:
# storage_type: s3
# mode: push
# bucket: my-pond-backups
# ...
```

#### Restore from Backup (Init Mode)
```bash
# Option 1: Separate command
pond restore --from-backup s3://my-pond-backups --pond-path /new/pond

# Option 2: Init with config
pond init --from-backup init.yaml

# init.yaml contains mode: init
```

#### Create Read Replica (Init + Pull)
```bash
# Initialize from backup, then switch to pull mode
pond init --from-backup replica-config.yaml

# replica-config.yaml:
# mode: init
# auto_switch_to_pull: true
# sync_interval: 60
```

#### Manual Sync (Pull Mode)
```bash
# Trigger a sync check
pond sync

# Or use existing run command if factory is configured
pond run /etc/system.d/10-remote
```

### Factory Execution Flow

#### Push Mode (Current)
```rust
async fn execute_push(
    store: Arc<dyn ObjectStore>,
    context: FactoryContext,
    config: RemoteConfig,
) -> Result<(), TLogFSError> {
    // Get current version from State
    let table = context.state.table().await;
    let current_version = table.version()?;
    
    // Find versions to backup
    let last_backed_up = get_last_backed_up_version(&store).await?;
    let versions_to_backup = (last_backed_up + 1)..=current_version;
    
    // Backup each version
    for version in versions_to_backup {
        let changeset = detect_changes_from_delta_log(&table, version).await?;
        create_backup_bundle(store.clone(), &changeset, &table, config.compression_level).await?;
    }
    
    Ok(())
}
```

#### Init Mode (New)
```rust
async fn execute_init(
    store: Arc<dyn ObjectStore>,
    context: FactoryContext,
    config: RemoteConfig,
) -> Result<(), TLogFSError> {
    log::info!("🔄 Initializing pond from backup");
    
    // 1. Scan remote for all versions
    let available_versions = scan_remote_versions(&store).await?;
    log::info!("   Found {} versions in backup", available_versions.len());
    
    if available_versions.is_empty() {
        return Err(TLogFSError::TinyFS(tinyfs::Error::Other(
            "No backup versions found in remote storage".to_string()
        )));
    }
    
    // 2. Download and apply each version in order
    for version in available_versions {
        log::info!("   Restoring version {}...", version);
        
        // Download bundle
        let bundle_path = format!("backups/version-{:06}/bundle.tar.zst", version);
        let bundle_data = download_bundle(&store, &bundle_path).await?;
        
        // Extract Parquet files
        let parquet_files = extract_bundle(&bundle_data).await?;
        
        // Apply to Delta Lake (creates transaction for this version)
        apply_parquet_files_to_delta(context.state, &parquet_files, version).await?;
        
        log::info!("   ✓ Version {} restored", version);
    }
    
    // 3. If auto_switch_to_pull, update config
    if config.auto_switch_to_pull {
        update_mode_to_pull(context, config).await?;
        log::info!("   ✓ Switched to pull mode for ongoing sync");
    }
    
    log::info!("✓ Pond initialization from backup complete");
    Ok(())
}
```

#### Pull Mode (New)
```rust
async fn execute_pull(
    store: Arc<dyn ObjectStore>,
    context: FactoryContext,
    config: RemoteConfig,
) -> Result<(), TLogFSError> {
    log::info!("🔄 Checking for new versions to pull");
    
    // Get current local version
    let table = context.state.table().await;
    let local_version = table.version()?;
    
    // Check remote for newer versions
    let remote_versions = scan_remote_versions(&store).await?;
    let max_remote_version = remote_versions.iter().max().copied().unwrap_or(0);
    
    if max_remote_version <= local_version {
        log::info!("   Already up to date (local: {}, remote: {})", 
            local_version, max_remote_version);
        return Ok(());
    }
    
    // Download and apply new versions
    let versions_to_pull: Vec<i64> = ((local_version + 1)..=max_remote_version).collect();
    log::info!("   Pulling {} new version(s): {:?}", versions_to_pull.len(), versions_to_pull);
    
    for version in versions_to_pull {
        log::info!("   Pulling version {}...", version);
        
        let bundle_path = format!("backups/version-{:06}/bundle.tar.zst", version);
        let bundle_data = download_bundle(&store, &bundle_path).await?;
        let parquet_files = extract_bundle(&bundle_data).await?;
        
        apply_parquet_files_to_delta(context.state, &parquet_files, version).await?;
        
        log::info!("   ✓ Version {} applied", version);
    }
    
    log::info!("✓ Pull complete - now at version {}", max_remote_version);
    Ok(())
}
```

### Mode Dispatch

```rust
async fn execute_remote(
    config: Value,
    context: FactoryContext,
    mode: crate::factory::ExecutionMode,
) -> Result<(), TLogFSError> {
    let config: RemoteConfig = serde_json::from_value(config)?;
    
    log::info!("🌐 REMOTE FACTORY");
    log::info!("   Mode: {:?}", config.mode);
    log::info!("   Storage: {}", config.storage_type);
    
    // Build object store
    let store = build_object_store(&config)?;
    
    // Dispatch based on remote mode
    match config.mode.as_str() {
        "push" => execute_push(store, context, config).await,
        "init" => execute_init(store, context, config).await,
        "pull" => execute_pull(store, context, config).await,
        _ => Err(TLogFSError::TinyFS(tinyfs::Error::Other(
            format!("Unknown remote mode: {}", config.mode)
        ))),
    }
}
```

## Key Implementation Challenges

### 1. Bootstrap Problem: Config Before Pond

**Challenge**: Init mode needs remote config before pond exists

**Solution**: External config file during init
```bash
pond init --from-backup init.yaml
```

**Flow**:
1. Read `init.yaml` (external file with remote config)
2. Connect to S3 using credentials from init.yaml
3. Download all bundles
4. Create pond with restored data
5. Write config to `/etc/system.d/10-remote` with mode changed to `pull`

### 2. File Extraction: Applying Downloaded Parquet Files

**Challenge**: How to load Parquet files into Delta Lake's table directory

**Solution**: Extract files directly to Delta table location - Delta Lake discovers them automatically

**CRITICAL INSIGHT**: Bundles must include BOTH Parquet data files AND `_delta_log/*.json` commit files!

**Key Understanding**:
- **Bundle Contents**:
  - Parquet data files: `part_id=<uuid>/part-00001-<uuid>.snappy.parquet`
  - Delta commit logs: `_delta_log/{version:020}.json`
  - Both use paths that match Delta Lake's expected layout

**Implementation** (TESTED AND WORKING):
```rust
/// Apply extracted files (Parquet data + Delta commit logs) to Delta table location
/// 
/// Writes both Parquet files AND _delta_log/*.json commit files directly to storage.
/// This creates an identical Delta Lake replica without generating new commits.
pub async fn apply_parquet_files(
    delta_table: &mut deltalake::DeltaTable,
    files: &[ExtractedFile],
) -> Result<(), TLogFSError> {
    use object_store::path::Path as ObjectPath;
    
    let mut parquet_count = 0;
    let mut commit_log_count = 0;
    
    // Get the Delta table's object store
    let object_store = delta_table.object_store();
    
    // Write each file to the Delta table location
    for file in files {
        let dest_path = ObjectPath::from(file.path.as_str());
        
        // Track what we're writing
        if file.path.starts_with("_delta_log/") {
            log::debug!("Writing commit log: {} ({} bytes)", file.path, file.data.len());
            commit_log_count += 1;
        } else {
            log::debug!("Writing Parquet: {} ({} bytes)", file.path, file.data.len());
            parquet_count += 1;
        }
        
        // Write file data to object store
        let bytes = bytes::Bytes::copy_from_slice(&file.data);
        object_store.put(&dest_path, bytes.into()).await?;
    }
    
    log::info!("Files written: {} Parquet, {} commit logs", parquet_count, commit_log_count);
    
    if commit_log_count > 0 {
        log::info!("TRUE REPLICATION: Commit logs copied - Delta version will match source");
    } else {
        log::warn!("LEGACY MODE: No commit logs - will create new commits");
    }
    
    // Refresh the Delta table to discover files
    // If we copied commit logs, this just loads existing state (no new commit)
    // If no commit logs, this creates new commit (legacy behavior)
    delta_table.load().await?;
    
    log::info!("✓ Files extracted and Delta table refreshed");
    
    Ok(())
}
```

**Why this works**:
1. Bundle files have the exact paths Delta Lake expects (including `_delta_log/`)
2. Writing files to table location makes them discoverable
3. `delta_table.load()` discovers existing commit logs (no new version created)
4. Replica Delta version matches source exactly
5. **TRUE READ-ONLY SEMANTICS**: Replica never writes transactions

### 3. Transaction Sequence vs Delta Version

**Key Understanding**: txn_seq ≠ Delta Lake version

**txn_seq** (Steward transaction sequence):
- Local counter for each pond's transaction history
- Increments sequentially: 1, 2, 3, 4, 5, ...
- Used by Steward for transaction tracking and control table

**Delta Lake version**:
- Delta Lake's internal version number
- Also increments sequentially within each pond
- Stored in `_delta_log/` commit files

**CRITICAL FIX**: Each version must be restored in a SEPARATE Steward transaction!

**Initial Bug**:
```rust
// ❌ WRONG: All versions in single transaction
ship.transact(|tx, fs| async move {
    for version in versions {
        apply_bundle(version).await?;  // Delta version stuck at 1!
    }
    Ok(())
}).await?;
```

**Correct Implementation**:
```rust
// ✅ CORRECT: Separate transaction per version
for version in versions {
    ship.transact(
        vec![format!("restore-version-{}", version)],
        |tx, _fs| async move {
            let bundle_data = download_bundle(&store, version).await?;
            let files = extract_bundle(&bundle_data).await?;
            let mut table = tx.state()?.table().await;
            apply_parquet_files(&mut table, &files).await?;
            Ok(())
        }
    ).await?; // Transaction commits here, Delta version increments
}
```

**Why This Matters**:
- Each transaction commit → Delta table.load() → version increments
- Single transaction → single commit → all versions merge into one
- Result: Replica with 6 bundles had Delta version stuck at 1 (BUG!)
- Fix: 6 separate transactions → Delta versions 1, 2, 3, 4, 5, 6 (CORRECT!)

**During restore, txn_seq tracks each restore operation**:

**Source Pond**:
```
txn_seq=1, version=1  (mkdir /data)
txn_seq=2, version=2  (write file1)
txn_seq=3, version=3  (write file2)
txn_seq=4, version=4  (write file3)
```

**Replica Pond (restoring versions 2-4)**:
```
txn_seq=1, version=1  (pond init - creates empty pond)
txn_seq=2, version=2  (pull bundle for source v2 - restores file1)
txn_seq=3, version=3  (pull bundle for source v3 - restores file2)
txn_seq=4, version=4  (pull bundle for source v4 - restores file3)
```

**Important**: Each pull operation is a separate Steward transaction (post-commit action):
- Extracts files from bundle
- Writes to Delta table location
- Delta Lake creates new commit/version automatically
- Steward records transaction in control table

**This is correct behavior**:
- txn_seq tracks number of Steward transactions
- Delta version tracks Delta Lake state
- Both increment together, but represent different concepts
- Gaps in txn_seq are fine - it just means some source versions were skipped

### 4. Concurrent Updates During Init

**Challenge**: Source pond continues to commit while replica is initializing

**Scenario**:
1. Replica starts init: sees source versions 1-100 in backup
2. During init (takes 5 minutes), source commits versions 101-105
3. Replica finishes init at source v100
4. Replica needs to catch up on v101-105

**Solution**: Pull mode handles this automatically
```
1. Init pulls and applies v1-100 (each as separate txn_seq)
2. After init, switch to pull mode
3. Pull mode detects v101-105 are available in backup
4. Pull mode downloads and applies v101-105
```

**No coordination needed** - replica simply follows source asynchronously

### 5. Mode Transitions

**Challenge**: How to switch from init → pull automatically

**Solution**: After init completes, update stored config

```rust
async fn update_mode_to_pull(
    context: FactoryContext,
    mut config: RemoteConfig,
) -> Result<(), TLogFSError> {
    // Update mode to pull
    config.mode = "pull".to_string();
    
    // Write updated config to /etc/system.d/10-remote
    let config_json = serde_json::to_string_pretty(&config)?;
    
    // Use context.state to write config file
    context.state
        .write_file("/etc/system.d/10-remote", config_json.as_bytes())
        .await?;
    
    Ok(())
}
```

## Implementation Phases

### Phase 1: Configuration & CLI ✅ COMPLETE
- [x] Add `mode` field to RemoteConfig
- [x] Add `pond init --from-backup` command
- [x] Parse and validate init.yaml
- [ ] Add `pond sync` command for manual pull (future)

### Phase 2: Mode Dispatch ✅ COMPLETE
- [x] Refactor execute_remote() to dispatch by mode
- [x] Implement `execute_push()` (extract current logic)
- [x] Implement `execute_init()` (working!)
- [ ] Add `execute_pull()` (future)

### Phase 3: Bundle Extraction ✅ COMPLETE
- [x] Implement `download_bundle()` from object store
- [x] Implement `extract_bundle()` using tar+zstd decompression
- [x] Parse extracted Parquet files and metadata
- [x] Test: download → extract → verify files ✅ WORKING

### Phase 4: Delta Lake File Application ✅ COMPLETE
- [x] Implement `apply_parquet_files()` - copies both Parquet AND commit logs
- [x] Handle version number mapping (separate transaction per version)
- [x] Discovered and fixed: bundles must include `_delta_log/*.json` files
- [x] Test: apply files → verify Delta table state ✅ WORKING

### Phase 5: Init Mode ✅ COMPLETE
- [x] Implement `execute_init()` complete flow (via CLI command)
- [x] Scan remote for all versions
- [x] Download and apply in order (SEPARATE transactions per version)
- [ ] Switch to pull mode after completion (future enhancement)
- [x] Test: backup → restore → verify identical data ✅ WORKING

### Phase 6: Pull Mode (FUTURE)
- [ ] Implement `execute_pull()` complete flow
- [ ] Check for new remote versions
- [ ] Download and apply incrementally
- [ ] Test: continuous sync between two ponds

### Phase 7: CLI Integration ✅ COMPLETE
- [x] Integrate with `pond init --from-backup` command
- [ ] Add `pond restore` command (future - maybe unnecessary)
- [ ] Add `pond sync` command (future - for pull mode)
- [ ] Add status commands: `pond backup status`, `pond sync status` (future)

### Phase 8: Production Hardening (FUTURE)
- [ ] Error recovery (partial downloads, network failures)
- [ ] Incremental progress tracking
- [ ] Concurrency handling (multiple replicas)
- [ ] Performance optimization (parallel downloads)

## Testing Strategy

### Unit Tests
- Configuration parsing (all three modes)
- Bundle download/extraction
- Parquet file writing
- Delta Lake transaction creation

### Integration Tests ✅ TESTED AND WORKING
```bash
# test.sh - Full backup and restore test
# 1. Create source pond with hydrovu factory
# 2. Set up remote factory in push mode
# 3. Run hydrovu multiple times (creates 6 backup versions)
# 4. Restore to /tmp/pond-replica using pond init --from-backup
# 5. Verify files restored and Delta versions match

# Results: ✅ SUCCESS
# - All 6 versions restored correctly
# - Delta versions: 1, 2, 3, 4, 5, 6 (correct!)
# - Files visible in replica via `pond list`
# - Commit logs included in bundles
# - TRUE REPLICATION achieved
```

**Test Observations**:
- Initial bug: Delta version stuck at 1 (all versions in single transaction)
- Fix: Separate transaction per version → versions increment correctly
- Discovery: Bundles needed `_delta_log/*.json` files for true replication
- After fix: Replica Delta history identical to source ✅

### Automated Test Code
```rust
#[tokio::test]
async fn test_backup_and_restore_cycle() {
    // Create source pond
    let source = test_pond().await;
    
    // Write data
    source.transact(|tx, fs| async {
        fs.write_file("/data/test.csv", CSV_DATA).await?;
        Ok(())
    }).await?;
    
    // Backup (push mode)
    let backup_store = create_local_store("/tmp/backups")?;
    // ... backup happens via post-commit factory
    
    // Create replica pond with init mode
    let replica = restore_pond_from_backup("/tmp/replica", &backup_store).await?;
    
    // Verify data matches
    assert_eq!(
        source.read_file("/data/test.csv").await?,
        replica.read_file("/data/test.csv").await?
    );
}

#[tokio::test]
async fn test_continuous_sync() {
    // Create source and replica
    let source = test_pond_with_backup().await;
    let replica = test_pond_with_pull().await;
    
    // Write to source
    source.write("/data/v1.csv", DATA_V1).await?;
    // Backup happens automatically
    
    // Pull on replica
    replica.sync().await?;
    assert_eq!(replica.read("/data/v1.csv").await?, DATA_V1);
    
    // Write more to source
    source.write("/data/v2.csv", DATA_V2).await?;
    
    // Pull again
    replica.sync().await?;
    assert_eq!(replica.read("/data/v2.csv").await?, DATA_V2);
}
```

## Security Considerations

1. **Credentials**: Same S3 credentials for push/pull
   - Source pond: needs write access
   - Replica pond: only needs read access (can use read-only IAM role)

2. **Encryption**: Bundles are stored in S3
   - Enable S3 server-side encryption (SSE-S3 or SSE-KMS)
   - Consider client-side encryption for sensitive data

3. **Access Control**: Prevent unauthorized replicas
   - Use S3 bucket policies to restrict access
   - Consider signed URLs for time-limited access

## Future Enhancements

1. **Selective Replication**: Only replicate specific paths
   ```yaml
   mode: pull
   include_paths: ["/data/public/*"]
   exclude_paths: ["/data/private/*"]
   ```

2. **Bidirectional Sync**: Multi-master replication
   - Conflict resolution strategies
   - Vector clocks for causality tracking

3. **Delta-Only Pull**: Instead of full bundles
   - Only download changed files
   - More efficient for large ponds

4. **Snapshot Restore**: Restore to specific point in time
   ```bash
   pond restore --from-backup s3://bucket --version 42
   ```

5. **Cross-Region Replication**: Built-in multi-region support
   ```yaml
   replicas:
     - region: us-west-2
     - region: eu-west-1
   ```

## Questions for Review

1. ~~Should init mode replay each source version separately, or bundle into fewer transactions?~~
   - **ANSWERED**: Must be separate transactions - each version needs its own commit for Delta version to increment
2. ~~How should we handle version number mismatches between source and replica?~~
   - **ANSWERED**: Not an issue - replica creates its own version sequence by copying commit logs
3. Should pull mode be automatic (post-commit) or manual (`pond sync`)?
   - **FUTURE**: Will implement both options
4. ~~Do we need a separate `pond restore` command, or is `pond init --from-backup` sufficient?~~
   - **ANSWERED**: `pond init --from-backup` is sufficient and cleaner
5. ~~How to handle schema evolution across versions during restore?~~
   - **ANSWERED**: Not an issue - Delta Lake handles this automatically via commit logs

---

## Implementation Summary (October 2025)

### What We Built ✅

**Command**: `pond init --from-backup config.yaml`

**Config Format**:
```yaml
storage_type: local  # or s3
path: /tmp/pond-backups
mode: init
compression_level: 3
```

**Implementation Files**:
- `crates/cmd/src/main.rs` - Added `--from-backup` CLI flag
- `crates/cmd/src/commands/init.rs` - Implemented init-from-backup logic
- `crates/tlogfs/src/remote_factory.rs` - Updated bundle creation and restoration

**Key Code Changes**:

1. **Bundle Creation** (includes commit logs):
```rust
// Add Delta commit log to bundle
let commit_log_path = format!("_delta_log/{:020}.json", changeset.version);
builder.add_file(commit_log_path, size, reader)?;
```

2. **Bundle Restoration** (copies commit logs):
```rust
// Separate transaction per version
for version in versions {
    ship.transact(|tx, _fs| async move {
        let files = download_and_extract(version).await?;
        apply_parquet_files(&mut table, &files).await?;  // Copies both Parquet + commit logs
        Ok(())
    }).await?;
}
```

3. **File Application** (detects commit logs):
```rust
if file.path.starts_with("_delta_log/") {
    log::info!("TRUE REPLICATION: Commit logs copied");
} else {
    log::warn!("LEGACY MODE: No commit logs");
}
```

### Critical Discoveries 🔍

1. **Bundles Must Include Commit Logs**
   - Without: Restore creates NEW commits (replica differs from source)
   - With: Restore copies existing commits (replica identical to source)

2. **Separate Transactions Required**
   - Single transaction: All versions merge, Delta version stuck at 1
   - Separate transactions: Each version commits independently, versions increment correctly

3. **True Read-Only Replication**
   - Replica never writes transactions
   - All data comes from copied bundles
   - Delta Lake state identical to source

### Testing Results ✅

**Test**: 6-version backup → restore → verify
- ✅ All versions restored
- ✅ Delta versions increment: 1, 2, 3, 4, 5, 6
- ✅ Files visible via `pond list`
- ✅ Commit logs in bundles
- ✅ Replica Delta history matches source

**Performance**: Fast local restore, ready for S3 testing

---

## Args-Based Factory Execution (October 2025)

### Architecture Evolution: From Config Mode to Args

**Problem**: Initial design had `mode` field in YAML config, but configs should be identical on source and replica ponds.

**Solution**: Mode comes from Steward master configuration, passed as CLI-style arguments to factories.

### Master Configuration Architecture

**Key Insight**: Factory configs (YAML files) are identical everywhere. Only Steward's master configuration (stored in control table) differs.

**Configuration Storage**:
- **YAML Config** (identical on source & replica):
  ```yaml
  storage_type: local
  path: /tmp/pond-backups
  compression_level: 3
  # NO mode field!
  ```

- **Steward Master Config** (in control table, txn_seq=0):
  ```
  factory_mode:remote = "push"   # Source pond
  factory_mode:remote = "pull"   # Replica pond
  ```

### Factory Execution Flow

**1. Pre-load Factory Modes** (before transaction):
```rust
// Load all factory modes from control table
let all_factory_modes = ship.control_table().get_all_factory_modes().await?;
```

**2. Single Write Transaction**:
```rust
let tx = ship.begin_transaction(...).await?;
// ... discover factory name, read config ...
```

**3. Pass Args to Factory**:
```rust
// Get mode from pre-loaded settings
let args = all_factory_modes.get(&factory_name)
    .map(|mode| vec![mode.clone()])
    .unwrap_or_else(|| vec![]);

FactoryRegistry::execute(
    &factory_name,
    &config_bytes,
    factory_context,
    execution_mode,
    args,  // CLI-style arguments
).await?;
```

**4. Factory Receives Args**:
```rust
async fn execute_remote(
    config: Value,
    context: FactoryContext,
    mode: ExecutionMode,
    args: Vec<String>,  // NEW: args[0] = "push" or "pull"
) -> Result<(), TLogFSError> {
    let operation_mode = args.get(0).map(|s| s.as_str()).unwrap_or("push");
    match operation_mode {
        "push" => execute_push(...).await,
        "pull" => execute_pull(...).await,
        "init" => execute_init(...).await,
        _ => Ok(()),
    }
}
```

### Implementation Status ✅

**Completed**:
- ✅ Factory signature updates (all factories accept `Vec<String> args`)
- ✅ `FactoryRegistry::execute()` passes args
- ✅ `register_executable_factory!` macro updated
- ✅ Remote factory uses `args[0]` for mode
- ✅ Control table API: `get_factory_mode()`, `set_factory_mode()`, `get_all_factory_modes()`
- ✅ Ship API: `control_table()` getter
- ✅ Run command: pre-loads factory modes, single write transaction
- ✅ Post-commit execution: queries factory mode, passes as args
- ✅ All tests pass, no warnings

**Key Benefits**:
1. **Identical configs** - Same YAML file on source and replica
2. **Clean separation** - Settings loaded before/between transactions
3. **Extensible** - Can add more args in future (not just mode)
4. **Type-safe** - No special FactoryContext fields needed

### Control Table API

```rust
// Set factory mode (called during pond init --from-backup)
ship.control_table().set_factory_mode("remote", "pull").await?;

// Get single factory mode
let mode = ship.control_table().get_factory_mode("remote").await?;
// Returns: "push" (default), "pull", "init", etc.

// Get all factory modes (used by run command)
let modes = ship.control_table().get_all_factory_modes().await?;
// Returns: HashMap<String, String> - factory_name -> mode
```

### Remaining Work for Pull Mode

**TODO**:
- [ ] Implement `execute_pull()` in remote factory
- [ ] Add `pond control --mode sync` command to trigger manual sync
- [ ] During `pond init --from-backup`, set factory mode to "pull" in control table
- [ ] Test: source pond (push) → replica pond (pull) → continuous sync

**Pattern for Setting Mode**:
```rust
// In init.rs, after restore completes:
ship.control_table().set_factory_mode("remote", "pull").await?;
log::info!("✓ Factory mode set to 'pull' for continuous sync");
```

### What's Next 🚀

1. **Pull Mode Implementation** - Continuous sync for read replicas
   - Implement `execute_pull()` function
   - Add `pond control --mode sync` command
   - Set factory mode to "pull" after init
2. **S3 Testing** - Validate cross-region replication
3. **Error Handling** - Network failures, partial downloads
4. **Performance** - Parallel downloads, incremental progress
