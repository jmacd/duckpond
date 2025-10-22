# Remote Restore & Replication Design

## Overview

This document describes the design for restoring ponds from remote backups and continuous replication between ponds. It extends the remote backup system (see `remote-backup-design.md`) with pull and init capabilities.

## Three Operational Modes

The remote factory supports three modes of operation:

### 1. Push Mode (Backup - Current Implementation)
**Purpose**: Original pond backs up data after each commit

**Flow**:
```
Local Commit → Post-Commit Factory → Bundle & Upload → S3
```

**Configuration**:
```yaml
storage_type: s3
bucket: my-pond-backups
mode: push  # Default if not specified
```

**Use Case**: Production pond creating backups for disaster recovery

---

### 2. Init Mode (Restore from Backup - New)
**Purpose**: Create new pond by restoring complete history from backup

**Flow**:
```
Empty Pond → Pre-Init Factory → Download All Bundles → Replay Transactions → Ready Pond
```

**Configuration**:
```yaml
storage_type: s3
bucket: my-pond-backups
mode: init
source_pond_id: "original-pond-uuid"  # Optional: for tracking
```

**Use Case**: 
- Disaster recovery (restore from backup)
- Creating read replicas
- Migrating pond to new location

---

### 3. Pull Mode (Continuous Sync - New)
**Purpose**: Replica pond continuously syncs new versions from source

**Flow**:
```
Local Commit → Post-Commit Factory → Check Remote for New Versions → Download & Apply
```

**Configuration**:
```yaml
storage_type: s3
bucket: my-pond-backups
mode: pull
sync_interval: 60  # Optional: seconds between checks
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

**Key Insight**: Bundle files already have correct structure and naming:
- Partition directories: `part_id=<uuid>/`
- Parquet files: `part-00001-<uuid>.snappy.parquet`
- These match Delta Lake's expected layout

```rust
async fn apply_parquet_files(
    delta_table: &DeltaTable,
    files: &[ExtractedFile],
) -> Result<(), TLogFSError> {
    // Get Delta table location (e.g., /pond/data/_delta_log/../)
    let table_location = delta_table.table_uri();
    
    log::info!("Extracting {} files to {}", files.len(), table_location);
    
    // Extract each Parquet file to its correct location
    for file in files {
        let dest_path = format!("{}/{}", table_location, file.path);
        
        // Create parent directories if needed (for partitions)
        if let Some(parent) = std::path::Path::new(&dest_path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        // Write the Parquet file
        tokio::fs::write(&dest_path, &file.data).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to write {}: {}", dest_path, e)))?;
        
        log::debug!("  Wrote: {} ({} bytes)", file.path, file.size);
    }
    
    // Refresh Delta table to discover new files
    // Delta Lake will create a new commit with Add actions for these files
    delta_table.load().await
        .map_err(|e| TLogFSError::Delta(e))?;
    
    log::info!("✓ Files extracted and Delta table refreshed");
    
    Ok(())
}
```

**Why this works**:
1. Bundle Parquet files have the exact paths Delta Lake expects
2. Writing files to table location makes them discoverable
3. `delta_table.load()` refreshes metadata and creates new version
4. Delta Lake's version increments naturally (no manual version setting needed)

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

**During restore, txn_seq will have gaps**:

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

### Phase 1: Configuration & CLI (Week 1)
- [x] Add `mode` field to RemoteConfig
- [ ] Add `pond init --from-backup` command
- [ ] Parse and validate init.yaml
- [ ] Add `pond sync` command for manual pull

### Phase 2: Mode Dispatch (Week 1)
- [ ] Refactor execute_remote() to dispatch by mode
- [ ] Implement `execute_push()` (extract current logic)
- [ ] Add placeholder `execute_init()` and `execute_pull()`

### Phase 3: Bundle Extraction (Week 2)
- [ ] Implement `download_bundle()` from object store
- [ ] Implement `extract_bundle()` using tar+zstd decompression
- [ ] Parse extracted Parquet files and metadata
- [ ] Test: download → extract → verify files

### Phase 4: Delta Lake Transaction Replay (Week 2-3)
- [ ] Research Delta Lake's transaction replay API
- [ ] Implement `apply_parquet_files_to_delta()`
- [ ] Handle version number mapping
- [ ] Test: apply files → verify Delta table state

### Phase 5: Init Mode (Week 3)
- [ ] Implement `execute_init()` complete flow
- [ ] Scan remote for all versions
- [ ] Download and apply in order
- [ ] Switch to pull mode after completion
- [ ] Test: backup → restore → verify identical data

### Phase 6: Pull Mode (Week 4)
- [ ] Implement `execute_pull()` complete flow
- [ ] Check for new remote versions
- [ ] Download and apply incrementally
- [ ] Test: continuous sync between two ponds

### Phase 7: CLI Integration (Week 4)
- [ ] Integrate with `pond init` command
- [ ] Add `pond restore` command
- [ ] Add `pond sync` command
- [ ] Add status commands: `pond backup status`, `pond sync status`

### Phase 8: Production Hardening (Week 5)
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

### Integration Tests
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

1. Should init mode replay each source version separately, or bundle into fewer transactions?
2. How should we handle version number mismatches between source and replica?
3. Should pull mode be automatic (post-commit) or manual (`pond sync`)?
4. Do we need a separate `pond restore` command, or is `pond init --from-backup` sufficient?
5. How to handle schema evolution across versions during restore?
