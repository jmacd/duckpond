// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! CLI command for idempotent create-or-update of pond resources.
//!
//! `pond apply -f file1.yaml [file2.yaml ...]`
//!
//! Each YAML file contains a prelude (kind, path, version) separated
//! from the config body by a `---` document separator.
//!
//! Supported kinds:
//!
//! Factory nodes (any registered factory name):
//! ```yaml
//! kind: remote
//! path: /system/etc/10-water
//! version: v1
//! ---
//! region: "${env:S3_REGION}"
//! url: "${env:WATER_S3_URL}"
//! ```
//!
//! Directory creation:
//! ```yaml
//! kind: mkdir
//! path: /
//! version: v1
//! ---
//! paths:
//!   - /system/etc
//!   - /system/run
//!   - /sources
//! ```
//!
//! Host-to-pond copy:
//! ```yaml
//! kind: copy
//! path: /content
//! version: v1
//! ---
//! source: "host:///path/to/content"
//! overwrite: false
//! ```

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use provider::FactoryRegistry;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use utilities::env_substitution;

/// Prelude parsed from the first YAML document in an apply file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyPrelude {
    /// Kind: a factory type name, "mkdir", or "copy"
    kind: String,
    /// Path in the pond (factory mount path, copy destination, or "/" for mkdir)
    path: String,
    /// Format version (must be "v1")
    version: String,
}

/// Config body for `kind: mkdir`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MkdirConfig {
    /// Absolute paths to create (with implied -p semantics)
    paths: Vec<String>,
}

/// Config body for `kind: copy`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CopyConfig {
    /// Host source URL (e.g., "host:///path/to/content")
    source: String,
    /// If true, overwrite existing destination. If false (default), error if exists.
    #[serde(default)]
    overwrite: bool,
}

/// Parsed apply kind with its validated config.
#[derive(Debug)]
enum ApplyKind {
    /// Create directories (pond mkdir -p)
    Mkdir(MkdirConfig),
    /// Copy host content into pond (pond copy)
    Copy(CopyConfig),
    /// Create/update a factory node (pond mknod)
    Factory {
        factory_type: String,
        raw_config: String,
        expanded_config: String,
    },
}

/// A parsed apply spec ready for processing.
#[derive(Debug)]
struct ApplySpec {
    /// Source file path (for error messages)
    source_file: String,
    /// Pond path from prelude
    path: String,
    /// Parsed and validated kind
    kind: ApplyKind,
}

/// Result of applying a single spec.
#[derive(Debug, PartialEq, Eq)]
enum ApplyResult {
    Created,
    Updated,
    Unchanged,
}

/// Parse an apply file into prelude + config body.
///
/// The file must contain exactly two YAML documents separated by `---`:
/// 1. Prelude with kind, path, version
/// 2. Config body (passed to the factory)
fn parse_apply_file(content: &str, source_file: &str) -> Result<(ApplyPrelude, String)> {
    // Split on the YAML document separator.
    // The separator must be a line starting with "---" (standard YAML multi-doc).
    // We skip the first occurrence if the file starts with "---" (optional YAML preamble).
    let mut documents = Vec::new();
    let mut current = String::new();
    let mut first_line = true;

    for line in content.lines() {
        if !first_line && line == "---" {
            documents.push(std::mem::take(&mut current));
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
        first_line = false;
    }
    // Push the last document
    if !current.is_empty() {
        documents.push(current);
    }

    if documents.len() < 2 {
        return Err(anyhow!(
            "{}: expected prelude and config body separated by '---'",
            source_file
        ));
    }
    if documents.len() > 2 {
        return Err(anyhow!(
            "{}: expected exactly two YAML documents (prelude + config body), found {}",
            source_file,
            documents.len()
        ));
    }

    // Parse the prelude
    let prelude: ApplyPrelude = serde_yaml::from_str(&documents[0])
        .map_err(|e| anyhow!("{}: failed to parse prelude: {}", source_file, e))?;

    // Validate version
    if prelude.version != "v1" {
        return Err(anyhow!(
            "{}: unsupported version '{}', expected 'v1'",
            source_file,
            prelude.version
        ));
    }

    // Validate path starts with /
    if !prelude.path.starts_with('/') {
        return Err(anyhow!(
            "{}: path must be absolute (start with '/'), got '{}'",
            source_file,
            prelude.path
        ));
    }

    // The config body is everything after the separator.
    // Add a trailing newline to match how files are normally stored.
    let mut body = documents[1].clone();
    if !body.ends_with('\n') {
        body.push('\n');
    }

    Ok((prelude, body))
}

/// Apply one or more configuration files to the pond.
///
/// For each file:
/// 1. Parse prelude (kind, path, version) and config body
/// 2. Validate kind-specific configuration
/// 3. Create or update resources as needed
///
/// All changes are made in a single transaction, committed only if
/// at least one resource was created or updated.
pub async fn apply_command(ship_context: &ShipContext, files: &[String]) -> Result<()> {
    if files.is_empty() {
        return Err(anyhow!("no files specified; use -f <file.yaml>"));
    }

    // Phase 1: Parse and validate all files before opening a transaction.
    let mut specs = Vec::new();
    let mut claimed_paths: HashSet<String> = HashSet::new();

    for file_path in files {
        let content = fs::read_to_string(file_path)
            .map_err(|e| anyhow!("failed to read '{}': {}", file_path, e))?;

        let (prelude, body) = parse_apply_file(&content, file_path)?;

        let kind = parse_apply_kind(&prelude, &body, file_path)?;

        // Collect claimed paths for duplicate detection
        match &kind {
            ApplyKind::Mkdir(cfg) => {
                for p in &cfg.paths {
                    if !claimed_paths.insert(p.clone()) {
                        return Err(anyhow!(
                            "duplicate path '{}' in '{}' (already claimed by another spec)",
                            p,
                            file_path
                        ));
                    }
                }
            }
            ApplyKind::Copy(_) | ApplyKind::Factory { .. } => {
                if !claimed_paths.insert(prelude.path.clone()) {
                    return Err(anyhow!(
                        "duplicate path '{}' in '{}' (already claimed by another spec)",
                        prelude.path,
                        file_path
                    ));
                }
            }
        }

        specs.push(ApplySpec {
            source_file: file_path.clone(),
            path: prelude.path,
            kind,
        });
    }

    // Sort specs: mkdir first (shallowest), then factories/copies by depth
    specs.sort_by(|a, b| {
        let order_a = match &a.kind {
            ApplyKind::Mkdir(_) => 0,
            ApplyKind::Copy(_) => 1,
            ApplyKind::Factory { .. } => 2,
        };
        let order_b = match &b.kind {
            ApplyKind::Mkdir(_) => 0,
            ApplyKind::Copy(_) => 1,
            ApplyKind::Factory { .. } => 2,
        };
        order_a
            .cmp(&order_b)
            .then_with(|| {
                let depth_a = a.path.matches('/').count();
                let depth_b = b.path.matches('/').count();
                depth_a.cmp(&depth_b)
            })
            .then(a.path.cmp(&b.path))
    });

    // Phase 2: Open transaction and apply
    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_write(&steward::PondUserMetadata::new(vec![
            "apply".to_string(),
            format!("{} file(s)", specs.len()),
        ]))
        .await
        .map_err(|e| anyhow!("apply: failed to begin transaction: {}", e))?;

    // Open host steward for copy operations (read-only, no conflict with pond write).
    // Must be declared before the transaction so it outlives it.
    let needs_host = specs.iter().any(|s| matches!(s.kind, ApplyKind::Copy(_)));
    let mut host_ship = if needs_host {
        Some(ship_context.open_host()?)
    } else {
        None
    };
    let host_read = if let Some(ref mut hs) = host_ship {
        Some(
            hs.begin_read(&steward::PondUserMetadata::new(vec![
                "apply-copy-source".to_string(),
            ]))
            .await
            .map_err(|e| anyhow!("apply: failed to open host for copy: {}", e))?,
        )
    } else {
        None
    };

    let mut created = 0usize;
    let mut updated = 0usize;
    let mut unchanged = 0usize;

    for spec in &specs {
        let result = apply_one(&tx, &tx, spec, host_read.as_ref())
            .await
            .map_err(|e| anyhow!("{}: {}", spec.source_file, e))?;

        match result {
            ApplyResult::Created => {
                log::info!("  created  {}", spec.path);
                created += 1;
            }
            ApplyResult::Updated => {
                log::info!("  updated  {}", spec.path);
                updated += 1;
            }
            ApplyResult::Unchanged => {
                log::info!("  unchanged  {}", spec.path);
                unchanged += 1;
            }
        }
    }

    // Commit host read transaction if opened
    if let Some(host_tx) = host_read {
        _ = host_tx.commit().await.map_err(|e| {
            anyhow!("apply: failed to commit host read transaction: {}", e)
        })?;
    }

    // Only commit pond transaction if something changed
    if created > 0 || updated > 0 {
        _ = tx
            .commit()
            .await
            .map_err(|e| anyhow!("apply: failed to commit transaction: {}", e))?;
        log::info!(
            "apply: {} created, {} updated, {} unchanged",
            created,
            updated,
            unchanged
        );
    } else {
        log::info!(
            "apply: all {} config(s) unchanged, no transaction committed",
            unchanged
        );
    }

    Ok(())
}

/// Parse the kind field and body into a validated ApplyKind.
fn parse_apply_kind(prelude: &ApplyPrelude, body: &str, source_file: &str) -> Result<ApplyKind> {
    match prelude.kind.as_str() {
        "mkdir" => {
            let config: MkdirConfig = serde_yaml::from_str(body).map_err(|e| {
                anyhow!("{}: invalid mkdir config: {}", source_file, e)
            })?;
            // Validate all paths are absolute
            for p in &config.paths {
                if !p.starts_with('/') {
                    return Err(anyhow!(
                        "{}: mkdir path must be absolute: '{}'",
                        source_file,
                        p
                    ));
                }
            }
            Ok(ApplyKind::Mkdir(config))
        }
        "copy" => {
            let config: CopyConfig = serde_yaml::from_str(body).map_err(|e| {
                anyhow!("{}: invalid copy config: {}", source_file, e)
            })?;
            // Validate source is a host URL
            if !config.source.starts_with("host:///")
                && !config.source.starts_with("host+")
            {
                return Err(anyhow!(
                    "{}: copy source must be a host URL (host:///... or host+scheme:///...), got '{}'",
                    source_file,
                    config.source
                ));
            }
            Ok(ApplyKind::Copy(config))
        }
        factory_type => {
            // Expand env references for validation
            let expanded = env_substitution::substitute_env_vars(body).map_err(|e| {
                anyhow!(
                    "{}: failed to expand environment variables:\n  {}\n  \
                    Tip: Use ${{env:VAR}} to read environment variables, ${{env:VAR:-default}} for defaults",
                    source_file,
                    e
                )
            })?;

            // Validate the factory and configuration
            let _validated =
                FactoryRegistry::validate_config(factory_type, expanded.as_bytes()).map_err(
                    |e| {
                        anyhow!(
                            "{}: invalid configuration for factory '{}': {}",
                            source_file,
                            factory_type,
                            e
                        )
                    },
                )?;

            Ok(ApplyKind::Factory {
                factory_type: factory_type.to_string(),
                raw_config: body.to_string(),
                expanded_config: expanded,
            })
        }
    }
}

/// Apply a single spec within an open transaction.
async fn apply_one(
    tx: &steward::Transaction<'_>,
    fs: &tinyfs::FS,
    spec: &ApplySpec,
    host_read: Option<&steward::Transaction<'_>>,
) -> Result<ApplyResult> {
    match &spec.kind {
        ApplyKind::Mkdir(config) => apply_mkdir(fs, config).await,
        ApplyKind::Copy(config) => {
            let host_tx = host_read
                .ok_or_else(|| anyhow!("copy requires host access but host steward not available"))?;
            apply_copy(fs, &spec.path, config, host_tx).await
        }
        ApplyKind::Factory {
            factory_type,
            raw_config,
            expanded_config,
        } => {
            apply_factory(tx, fs, &spec.path, factory_type, raw_config, expanded_config).await
        }
    }
}

/// Apply a mkdir spec: create all listed directories with -p semantics.
async fn apply_mkdir(fs: &tinyfs::FS, config: &MkdirConfig) -> Result<ApplyResult> {
    let root = fs.root().await?;
    let mut any_created = false;

    for path in &config.paths {
        let components: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        let mut current_path = String::new();
        for component in components {
            current_path.push('/');
            current_path.push_str(component);

            if root.exists(&current_path).await {
                match root.open_dir_path(&current_path).await {
                    Ok(_) => continue,
                    Err(_) => {
                        return Err(anyhow!(
                            "path '{}' exists but is not a directory",
                            current_path
                        ));
                    }
                }
            } else {
                _ = root.create_dir_path(&current_path).await.map_err(|e| {
                    anyhow!("failed to create directory '{}': {}", current_path, e)
                })?;
                any_created = true;
            }
        }
    }

    if any_created {
        Ok(ApplyResult::Created)
    } else {
        Ok(ApplyResult::Unchanged)
    }
}

/// Apply a copy spec: copy host content into the pond.
async fn apply_copy(
    fs: &tinyfs::FS,
    dest_path: &str,
    config: &CopyConfig,
    host_tx: &steward::Transaction<'_>,
) -> Result<ApplyResult> {
    let pond_root = fs.root().await?;
    let host_root = host_tx.root().await?;

    // Parse the host source path from the URL
    let host_path = parse_host_source(&config.source)?;

    // Check if source is a directory or file on the host
    if host_root.open_dir_path(&host_path).await.is_ok() {
        // Directory copy
        if !config.overwrite && pond_root.exists(dest_path).await {
            return Err(anyhow!(
                "destination '{}' already exists; set overwrite: true to replace",
                dest_path
            ));
        }
        copy_directory_into_pond(&host_root, &pond_root, &host_path, dest_path).await?;
        Ok(ApplyResult::Created)
    } else {
        // Single file copy
        ensure_parent_dirs(&pond_root, dest_path).await?;
        if !config.overwrite && pond_root.exists(dest_path).await {
            return Err(anyhow!(
                "destination '{}' already exists; set overwrite: true to replace",
                dest_path
            ));
        }
        copy_single_file_into_pond(&host_root, &pond_root, &host_path, dest_path).await?;
        Ok(ApplyResult::Created)
    }
}

/// Parse host source URL to extract the filesystem path.
fn parse_host_source(source: &str) -> Result<String> {
    // Handle host:///path and host+scheme:///path
    if let Some(path) = source.strip_prefix("host:///") {
        Ok(format!("/{}", path))
    } else if source.starts_with("host+") {
        // host+file:///path, host+table:///path, etc.
        if let Some(pos) = source.find(":///") {
            Ok(format!("/{}", &source[pos + 4..]))
        } else {
            Err(anyhow!("invalid host URL: '{}'", source))
        }
    } else {
        Err(anyhow!(
            "copy source must be a host URL (host:///...), got '{}'",
            source
        ))
    }
}

/// Recursively copy a directory from host into the pond.
async fn copy_directory_into_pond(
    host_root: &tinyfs::WD,
    pond_root: &tinyfs::WD,
    host_path: &str,
    pond_path: &str,
) -> Result<()> {
    use futures::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Ensure destination directory exists
    ensure_parent_dirs(pond_root, &format!("{}/x", pond_path)).await?;
    if !pond_root.exists(pond_path).await {
        _ = pond_root
            .create_dir_path(pond_path)
            .await
            .map_err(|e| anyhow!("failed to create destination dir '{}': {}", pond_path, e))?;
    }

    // List host directory entries
    let host_dir = host_root
        .open_dir_path(host_path)
        .await
        .map_err(|e| anyhow!("failed to open host dir '{}': {}", host_path, e))?;

    let mut entries = host_dir.entries().await?;

    while let Some(entry) = entries.next().await {
        let entry =
            entry.map_err(|e| anyhow!("failed to read host dir entry in '{}': {}", host_path, e))?;
        let name = &entry.name;
        let src = format!("{}/{}", host_path, name);
        let dst = format!("{}/{}", pond_path, name);

        if entry.entry_type.is_directory() {
            Box::pin(copy_directory_into_pond(host_root, pond_root, &src, &dst)).await?;
        } else {
            let mut reader = host_root
                .async_reader_path(&src)
                .await
                .map_err(|e| anyhow!("failed to read host file '{}': {}", src, e))?;

            let mut content = Vec::new();
            _ = reader
                .read_to_end(&mut content)
                .await
                .map_err(|e| anyhow!("failed to read host file '{}': {}", src, e))?;

            let mut writer = pond_root
                .async_writer_path(&dst)
                .await
                .map_err(|e| anyhow!("failed to write pond file '{}': {}", dst, e))?;

            writer
                .write_all(&content)
                .await
                .map_err(|e| anyhow!("failed to write pond file '{}': {}", dst, e))?;
            writer
                .shutdown()
                .await
                .map_err(|e| anyhow!("failed to finalize pond file '{}': {}", dst, e))?;
        }
    }

    Ok(())
}

/// Copy a single file from host into the pond.
async fn copy_single_file_into_pond(
    host_root: &tinyfs::WD,
    pond_root: &tinyfs::WD,
    host_path: &str,
    pond_path: &str,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut reader = host_root
        .async_reader_path(host_path)
        .await
        .map_err(|e| anyhow!("failed to read host file '{}': {}", host_path, e))?;

    let mut content = Vec::new();
    _ = reader
        .read_to_end(&mut content)
        .await
        .map_err(|e| anyhow!("failed to read host file '{}': {}", host_path, e))?;

    let mut writer = pond_root
        .async_writer_path(pond_path)
        .await
        .map_err(|e| anyhow!("failed to write pond file '{}': {}", pond_path, e))?;

    writer
        .write_all(&content)
        .await
        .map_err(|e| anyhow!("failed to write pond file '{}': {}", pond_path, e))?;
    writer
        .shutdown()
        .await
        .map_err(|e| anyhow!("failed to finalize pond file '{}': {}", pond_path, e))?;

    Ok(())
}

/// Apply a factory spec: create or update a dynamic node.
async fn apply_factory(
    tx: &steward::Transaction<'_>,
    fs: &tinyfs::FS,
    path: &str,
    factory_type: &str,
    raw_config: &str,
    expanded_config: &str,
) -> Result<ApplyResult> {
    let root = fs.root().await?;

    // Ensure parent directories exist
    ensure_parent_dirs(&root, path).await?;

    // Check if the node already exists
    let (_, lookup) = root.resolve_path(path).await?;

    match lookup {
        tinyfs::Lookup::Found(existing_node) => {
            let node_id = existing_node.id();

            // Verify the factory type matches
            let existing_factory = tx
                .get_factory_for_node(node_id)
                .await
                .map_err(|e| anyhow!("failed to get factory for '{}': {}", path, e))?;

            if let Some(ref factory_name) = existing_factory
                && factory_name != factory_type
            {
                return Err(anyhow!(
                    "factory type mismatch at '{}': existing is '{}', apply specifies '{}'. \
                    Cannot change factory type in place; remove the node first.",
                    path,
                    factory_name,
                    factory_type
                ));
            }

            // Read existing config to compare
            let existing_config = fs
                .get_dynamic_node_config(node_id)
                .await
                .map_err(|e| anyhow!("failed to read existing config at '{}': {}", path, e))?;

            let new_config_bytes = raw_config.as_bytes();

            if let Some((_, ref existing_bytes)) = existing_config
                && existing_bytes == new_config_bytes
            {
                return Ok(ApplyResult::Unchanged);
            }

            // Config changed -- update
            let factory = FactoryRegistry::get_factory(factory_type)
                .ok_or_else(|| anyhow!("unknown factory type: {}", factory_type))?;

            let entry_type = determine_entry_type(factory, factory_type)?;

            if node_id.entry_type() != entry_type {
                return Err(anyhow!(
                    "entry type mismatch at '{}': existing node type {:?} does not match \
                    factory '{}' which creates {:?}",
                    path,
                    node_id.entry_type(),
                    factory_type,
                    entry_type
                ));
            }

            _ = root
                .create_dynamic_path_with_overwrite(
                    path,
                    entry_type,
                    factory_type,
                    new_config_bytes.to_vec(),
                    true,
                )
                .await
                .map_err(|e| anyhow!("failed to update config at '{}': {}", path, e))?;

            run_factory_init(tx, &root, path, factory_type, expanded_config).await?;

            Ok(ApplyResult::Updated)
        }
        tinyfs::Lookup::NotFound(_, _) => {
            let factory = FactoryRegistry::get_factory(factory_type)
                .ok_or_else(|| anyhow!("unknown factory type: {}", factory_type))?;
            let entry_type = determine_entry_type(factory, factory_type)?;

            _ = root
                .create_dynamic_path(
                    path,
                    entry_type,
                    factory_type,
                    raw_config.as_bytes().to_vec(),
                )
                .await
                .map_err(|e| anyhow!("failed to create node at '{}': {}", path, e))?;

            run_factory_init(tx, &root, path, factory_type, expanded_config).await?;

            Ok(ApplyResult::Created)
        }
        tinyfs::Lookup::Empty(_) => Err(anyhow!("empty path")),
    }
}

/// Determine the entry type for a factory (mirrors mknod logic).
fn determine_entry_type(
    factory: &provider::DynamicFactory,
    factory_name: &str,
) -> Result<tinyfs::EntryType> {
    if factory.create_directory.is_some() {
        Ok(tinyfs::EntryType::DirectoryDynamic)
    } else if factory.create_file.is_some()
        || factory.execute.is_some()
        || factory.apply_table_transform.is_some()
    {
        if factory.try_as_queryable.is_some() {
            Ok(tinyfs::EntryType::TableDynamic)
        } else {
            Ok(tinyfs::EntryType::FileDynamic)
        }
    } else {
        Err(anyhow!(
            "factory '{}' does not support creating directories or files",
            factory_name
        ))
    }
}

/// Ensure all parent directories exist for the given path (mkdir -p semantics).
async fn ensure_parent_dirs(root: &tinyfs::WD, path: &str) -> Result<()> {
    let parent = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new("/"));

    // Split into components and create each level
    let components: Vec<&str> = parent
        .to_str()
        .unwrap_or("/")
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    let mut current_path = String::new();
    for component in components {
        current_path.push('/');
        current_path.push_str(component);

        if root.exists(&current_path).await {
            // Verify it's a directory
            match root.open_dir_path(&current_path).await {
                Ok(_) => continue,
                Err(_) => {
                    return Err(anyhow!(
                        "path '{}' exists but is not a directory",
                        current_path
                    ));
                }
            }
        } else {
            _ = root.create_dir_path(&current_path).await.map_err(|e| {
                anyhow!(
                    "failed to create parent directory '{}': {}",
                    current_path,
                    e
                )
            })?;
        }
    }
    Ok(())
}

/// Run factory initialization after creating or updating a node.
async fn run_factory_init(
    tx: &steward::Transaction<'_>,
    root: &tinyfs::WD,
    path: &str,
    factory_type: &str,
    expanded_config: &str,
) -> Result<()> {
    let parent_path = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new("/"));
    let parent_node_path = root.resolve_path(parent_path).await?;
    let parent_node_id = match parent_node_path.1 {
        tinyfs::Lookup::Found(node) => node.id(),
        _ => {
            return Err(anyhow!(
                "parent directory not found: {}",
                parent_path.display()
            ));
        }
    };

    let provider_context = tx
        .provider_context()
        .map_err(|e| anyhow!("failed to get provider context: {}", e))?;
    let context = provider::FactoryContext::new(provider_context, parent_node_id);

    FactoryRegistry::initialize::<tlogfs::TLogFSError>(
        factory_type,
        expanded_config.as_bytes(),
        context,
    )
    .await
    .map_err(|e| anyhow!("factory initialization failed for '{}': {}", factory_type, e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use std::path::PathBuf;
    use tempfile::TempDir;

    struct TestSetup {
        temp_dir: TempDir,
        ship_context: ShipContext,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::pond_only(Some(&pond_path), init_args.clone());

            init_command(&ship_context, None, None).await?;

            Ok(Self {
                temp_dir,
                ship_context,
            })
        }

        /// Write an apply-format config file and return its path
        fn write_apply_config(
            &self,
            filename: &str,
            kind: &str,
            path: &str,
            body: &str,
        ) -> PathBuf {
            let file_path = self.temp_dir.path().join(filename);
            let content = format!("kind: {kind}\npath: {path}\nversion: v1\n---\n{body}");
            fs::write(&file_path, content).expect("Failed to write config file");
            file_path
        }

        /// Verify that a node exists at the given path
        async fn node_exists(&self, pond_path: &str) -> bool {
            let mut ship = self.ship_context.open_pond().await.unwrap();
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "test_verify".to_string(),
                ]))
                .await
                .unwrap();

            let result = {
                let fs_ref: &tinyfs::FS = &*tx;
                let root = fs_ref.root().await.unwrap();
                root.exists(pond_path).await
            };

            _ = tx.commit().await.unwrap();
            result
        }

        /// Read config bytes from a dynamic node in the pond
        async fn read_node_config(&self, pond_path: &str) -> Vec<u8> {
            let mut ship = self.ship_context.open_pond().await.unwrap();
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "test_read".to_string(),
                ]))
                .await
                .unwrap();

            let result = {
                let fs_ref: &tinyfs::FS = &*tx;
                let root = fs_ref.root().await.unwrap();
                let (_, lookup) = root.resolve_path(pond_path).await.unwrap();
                match lookup {
                    tinyfs::Lookup::Found(node) => {
                        let config = fs_ref.get_dynamic_node_config(node.id()).await.unwrap();
                        config.map(|(_, bytes)| bytes).unwrap_or_default()
                    }
                    _ => panic!("Node not found: {}", pond_path),
                }
            };

            _ = tx.commit().await.unwrap();
            result
        }
    }

    #[test]
    fn test_parse_apply_file_basic() {
        let content = "kind: remote\npath: /etc/water\nversion: v1\n---\nregion: us-east-1\n";
        let (prelude, body) = parse_apply_file(content, "test.yaml").unwrap();
        assert_eq!(prelude.kind, "remote");
        assert_eq!(prelude.path, "/etc/water");
        assert_eq!(prelude.version, "v1");
        assert_eq!(body, "region: us-east-1\n");
    }

    #[test]
    fn test_parse_apply_file_missing_separator() {
        let content = "kind: remote\npath: /etc/water\nversion: v1\n";
        let result = parse_apply_file(content, "test.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("---"));
    }

    #[test]
    fn test_parse_apply_file_bad_version() {
        let content = "kind: remote\npath: /etc/water\nversion: v2\n---\nfoo: bar\n";
        let result = parse_apply_file(content, "test.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("v1"));
    }

    #[test]
    fn test_parse_apply_file_relative_path() {
        let content = "kind: remote\npath: etc/water\nversion: v1\n---\nfoo: bar\n";
        let result = parse_apply_file(content, "test.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute"));
    }

    #[test]
    fn test_parse_apply_file_unknown_prelude_field() {
        let content = "kind: remote\npath: /etc/water\nversion: v1\nextra: bad\n---\nfoo: bar\n";
        let result = parse_apply_file(content, "test.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown field"));
    }

    #[test]
    fn test_parse_apply_file_multiline_body() {
        let content = "kind: remote\npath: /etc/water\nversion: v1\n---\nregion: us-east-1\nurl: s3://bucket\nendpoint: http://localhost:9000\n";
        let (_, body) = parse_apply_file(content, "test.yaml").unwrap();
        assert!(body.contains("region: us-east-1"));
        assert!(body.contains("url: s3://bucket"));
        assert!(body.contains("endpoint: http://localhost:9000"));
    }

    #[tokio::test]
    async fn test_apply_create_new_node() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config_path = setup.write_apply_config(
            "derived.yaml",
            "sql-derived-table",
            "/data/derived",
            "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT * FROM source\"\n",
        );

        apply_command(
            &setup.ship_context,
            &[config_path.to_string_lossy().to_string()],
        )
        .await?;

        assert!(setup.node_exists("/data/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_idempotent() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config_path = setup.write_apply_config(
            "derived.yaml",
            "sql-derived-table",
            "/data/derived",
            "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT * FROM source\"\n",
        );

        let file_arg = config_path.to_string_lossy().to_string();

        // First apply — creates
        apply_command(&setup.ship_context, &[file_arg.clone()]).await?;
        assert!(setup.node_exists("/data/derived").await);

        // Second apply — unchanged
        apply_command(&setup.ship_context, &[file_arg]).await?;
        assert!(setup.node_exists("/data/derived").await);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_update_changed_config() -> Result<()> {
        let setup = TestSetup::new().await?;

        // First apply
        let config_path = setup.write_apply_config(
            "derived.yaml",
            "sql-derived-table",
            "/data/derived",
            "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT * FROM source\"\n",
        );
        apply_command(
            &setup.ship_context,
            &[config_path.to_string_lossy().to_string()],
        )
        .await?;

        let original_config = setup.read_node_config("/data/derived").await;

        // Update the config file
        let new_body = "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT * FROM source LIMIT 10\"\n";
        let config_path2 = setup.write_apply_config(
            "derived2.yaml",
            "sql-derived-table",
            "/data/derived",
            new_body,
        );
        apply_command(
            &setup.ship_context,
            &[config_path2.to_string_lossy().to_string()],
        )
        .await?;

        let updated_config = setup.read_node_config("/data/derived").await;
        assert_ne!(original_config, updated_config);
        assert_eq!(updated_config, new_body.as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_duplicate_path_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config1 = setup.write_apply_config(
            "a.yaml",
            "sql-derived-table",
            "/data/derived",
            "patterns:\n  source: \"table:///a/*.table\"\nquery: \"SELECT 1\"\n",
        );
        let config2 = setup.write_apply_config(
            "b.yaml",
            "sql-derived-table",
            "/data/derived",
            "patterns:\n  source: \"table:///b/*.table\"\nquery: \"SELECT 2\"\n",
        );

        let result = apply_command(
            &setup.ship_context,
            &[
                config1.to_string_lossy().to_string(),
                config2.to_string_lossy().to_string(),
            ],
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate path"));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_missing_file_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        let result = apply_command(
            &setup.ship_context,
            &["/nonexistent/config.yaml".to_string()],
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to read"));

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_no_files_error() -> Result<()> {
        let setup = TestSetup::new().await?;

        let result = apply_command(&setup.ship_context, &[]).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no files specified")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_auto_creates_parent_dirs() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Apply to a deeply nested path — parents should be auto-created
        let config_path = setup.write_apply_config(
            "deep.yaml",
            "sql-derived-table",
            "/system/etc/deep/nested/derived",
            "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT * FROM source\"\n",
        );

        apply_command(
            &setup.ship_context,
            &[config_path.to_string_lossy().to_string()],
        )
        .await?;

        assert!(setup.node_exists("/system/etc/deep/nested/derived").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_multiple_files() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config1 = setup.write_apply_config(
            "a.yaml",
            "sql-derived-table",
            "/data/derived-a",
            "patterns:\n  source: \"table:///a/*.table\"\nquery: \"SELECT 1\"\n",
        );
        let config2 = setup.write_apply_config(
            "b.yaml",
            "sql-derived-table",
            "/data/derived-b",
            "patterns:\n  source: \"table:///b/*.table\"\nquery: \"SELECT 2\"\n",
        );

        apply_command(
            &setup.ship_context,
            &[
                config1.to_string_lossy().to_string(),
                config2.to_string_lossy().to_string(),
            ],
        )
        .await?;

        assert!(setup.node_exists("/data/derived-a").await);
        assert!(setup.node_exists("/data/derived-b").await);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_invalid_factory() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config_path =
            setup.write_apply_config("bad.yaml", "nonexistent-factory", "/data/bad", "foo: bar\n");

        let result = apply_command(
            &setup.ship_context,
            &[config_path.to_string_lossy().to_string()],
        )
        .await;

        assert!(result.is_err());
        // Node should not exist
        assert!(!setup.node_exists("/data/bad").await);

        Ok(())
    }

    // ---- mkdir kind tests ----

    #[test]
    fn test_parse_mkdir_config() {
        let content = "kind: mkdir\npath: /\nversion: v1\n---\npaths:\n  - /system/etc\n  - /sources\n";
        let (prelude, body) = parse_apply_file(content, "test.yaml").unwrap();
        assert_eq!(prelude.kind, "mkdir");
        let kind = parse_apply_kind(&prelude, &body, "test.yaml").unwrap();
        assert!(matches!(kind, ApplyKind::Mkdir(_)));
    }

    #[test]
    fn test_parse_mkdir_relative_path_error() {
        let content = "kind: mkdir\npath: /\nversion: v1\n---\npaths:\n  - relative/path\n";
        let (prelude, body) = parse_apply_file(content, "test.yaml").unwrap();
        let result = parse_apply_kind(&prelude, &body, "test.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute"));
    }

    #[tokio::test]
    async fn test_apply_mkdir() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config_path = setup.write_apply_config(
            "dirs.yaml",
            "mkdir",
            "/",
            "paths:\n  - /system/etc\n  - /system/run\n  - /sources\n",
        );

        apply_command(
            &setup.ship_context,
            &[config_path.to_string_lossy().to_string()],
        )
        .await?;

        assert!(setup.node_exists("/system/etc").await);
        assert!(setup.node_exists("/system/run").await);
        assert!(setup.node_exists("/sources").await);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_mkdir_idempotent() -> Result<()> {
        let setup = TestSetup::new().await?;

        let config_path = setup.write_apply_config(
            "dirs.yaml",
            "mkdir",
            "/",
            "paths:\n  - /mydir\n",
        );
        let file_arg = config_path.to_string_lossy().to_string();

        // First apply creates
        apply_command(&setup.ship_context, &[file_arg.clone()]).await?;
        assert!(setup.node_exists("/mydir").await);

        // Second apply is unchanged
        apply_command(&setup.ship_context, &[file_arg]).await?;
        assert!(setup.node_exists("/mydir").await);

        Ok(())
    }

    // ---- copy kind tests ----

    #[test]
    fn test_parse_copy_config() {
        let content =
            "kind: copy\npath: /content\nversion: v1\n---\nsource: \"host:///tmp/content\"\n";
        let (prelude, body) = parse_apply_file(content, "test.yaml").unwrap();
        assert_eq!(prelude.kind, "copy");
        let kind = parse_apply_kind(&prelude, &body, "test.yaml").unwrap();
        assert!(matches!(kind, ApplyKind::Copy(_)));
    }

    #[test]
    fn test_parse_copy_invalid_source() {
        let content =
            "kind: copy\npath: /content\nversion: v1\n---\nsource: \"/local/path\"\n";
        let (prelude, body) = parse_apply_file(content, "test.yaml").unwrap();
        let result = parse_apply_kind(&prelude, &body, "test.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("host URL"));
    }

    // ---- mixed kind tests ----

    #[tokio::test]
    async fn test_apply_mkdir_then_factory() -> Result<()> {
        let setup = TestSetup::new().await?;

        let mkdir_config = setup.write_apply_config(
            "dirs.yaml",
            "mkdir",
            "/",
            "paths:\n  - /data\n",
        );
        let factory_config = setup.write_apply_config(
            "derived.yaml",
            "sql-derived-table",
            "/data/derived",
            "patterns:\n  source: \"table:///data/*.table\"\nquery: \"SELECT * FROM source\"\n",
        );

        apply_command(
            &setup.ship_context,
            &[
                mkdir_config.to_string_lossy().to_string(),
                factory_config.to_string_lossy().to_string(),
            ],
        )
        .await?;

        assert!(setup.node_exists("/data").await);
        assert!(setup.node_exists("/data/derived").await);
        Ok(())
    }
}
