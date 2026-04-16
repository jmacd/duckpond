// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Git-ingest factory registration and execution.

use crate::git;
use crate::sync;
use log::info;
use provider::{ExecutionContext, FactoryContext, register_executable_factory};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::{EntryType, Result as TinyFSResult};

use clap::{Parser, Subcommand};

/// Git-ingest factory subcommands
#[derive(Debug, Parser)]
struct GitIngestCommand {
    #[command(subcommand)]
    command: Option<GitIngestSubcommand>,
}

#[derive(Debug, Subcommand)]
enum GitIngestSubcommand {
    /// Pull changes from git into the pond
    Pull,

    /// Push mode (no-op for git-ingest -- git is read-only source)
    Push,

    /// Show current sync status
    Status,
}

/// Configuration for the git-ingest factory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIngestConfig {
    /// Git remote URL (HTTPS or SSH)
    pub url: String,

    /// Git ref to track (branch name, tag, or SHA)
    #[serde(rename = "ref")]
    pub git_ref: String,

    /// Destination path within the pond (relative to pond root)
    pub pond_path: String,
}

impl GitIngestConfig {
    /// Validate the configuration
    pub fn validate(&self) -> TinyFSResult<()> {
        if self.url.is_empty() {
            return Err(tinyfs::Error::Other("url cannot be empty".to_string()));
        }
        if self.git_ref.is_empty() {
            return Err(tinyfs::Error::Other("ref cannot be empty".to_string()));
        }
        if self.pond_path.is_empty() {
            return Err(tinyfs::Error::Other(
                "pond_path cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// Parse command-line arguments
fn parse_command(ctx: ExecutionContext) -> Result<GitIngestCommand, tinyfs::Error> {
    let args_with_prog_name: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();

    GitIngestCommand::try_parse_from(args_with_prog_name)
        .map_err(|e| tinyfs::Error::Other(format!("Command parse error: {}", e)))
}

/// Initialize factory (called once per dynamic node creation)
async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    Ok(())
}

/// Pond path for the manifest file, stored inside the synced directory
fn manifest_pond_path(config: &GitIngestConfig) -> String {
    format!("{}/.git-manifest", config.pond_path)
}

/// Read manifest from the pond filesystem, or return empty if not found
async fn read_manifest(
    context: &FactoryContext,
    config: &GitIngestConfig,
) -> Result<git::GitManifest, tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;
    let path = manifest_pond_path(config);

    match root.read_file_path_to_vec(&path).await {
        Ok(data) => git::GitManifest::from_bytes(&data),
        Err(tinyfs::Error::NotFound(_)) => Ok(git::GitManifest::empty()),
        Err(e) => Err(e),
    }
}

/// Write manifest to the pond filesystem (atomic with other pond changes)
async fn write_manifest(
    context: &FactoryContext,
    config: &GitIngestConfig,
    manifest: &git::GitManifest,
) -> Result<(), tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;
    let path = manifest_pond_path(config);
    let data = manifest.to_bytes()?;

    // Ensure the destination directory exists
    let _ = root.create_dir_all(&config.pond_path).await?;

    // Use async_writer_path_with_type: creates if missing, appends new version if exists
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
        .await?;
    writer
        .write_all(&data)
        .await
        .map_err(|e| tinyfs::Error::Other(format!("Failed to write manifest: {}", e)))?;
    writer
        .shutdown()
        .await
        .map_err(|e| tinyfs::Error::Other(format!("Failed to finalize manifest: {}", e)))?;

    Ok(())
}

/// Execute the git-ingest factory
pub async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: GitIngestConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    let cmd = parse_command(ctx)?;

    // Get the pond path on disk from the provider context
    let pond_path = context
        .context
        .pond_path()
        .ok_or_else(|| {
            tinyfs::Error::Other(
                "git-ingest requires a real pond (pond_path not available)".to_string(),
            )
        })?
        .to_path_buf();

    let node_id = context.file_id.node_id().to_string();

    match cmd.command {
        Some(GitIngestSubcommand::Push) => {
            info!("git-ingest: 'push' mode is a no-op (git is a read-only source)");
            Ok(())
        }
        Some(GitIngestSubcommand::Status) => execute_status(&context, &config).await,
        Some(GitIngestSubcommand::Pull) | None => {
            execute_pull(&context, &pond_path, &node_id, &config).await
        }
    }
}

/// Show sync status
async fn execute_status(
    context: &FactoryContext,
    config: &GitIngestConfig,
) -> Result<(), tinyfs::Error> {
    let manifest = read_manifest(context, config).await?;

    if manifest.commit_sha.is_empty() {
        log::info!("git-ingest: not yet synced");
    } else {
        log::info!("git-ingest: synced at {}", manifest.commit_sha);
        log::info!("  entries: {}", manifest.entries.len());
    }
    log::info!("  url: {}", config.url);
    log::info!("  ref: {}", config.git_ref);
    log::info!("  pond_path: {}", config.pond_path);
    Ok(())
}

/// Pull changes from git into the pond
async fn execute_pull(
    context: &FactoryContext,
    pond_path: &std::path::Path,
    node_id: &str,
    config: &GitIngestConfig,
) -> Result<(), tinyfs::Error> {
    info!(
        "git-ingest pull: {} (ref: {}) -> {}",
        config.url, config.git_ref, config.pond_path
    );

    // Ensure the git cache directory exists
    let git_dir = pond_path.join("git");
    std::fs::create_dir_all(&git_dir)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to create git dir: {}", e)))?;

    // Fetch and resolve the ref to a commit SHA
    let repo_path = git::bare_repo_path(pond_path, node_id);
    let commit_sha = git::fetch_and_resolve(&repo_path, &config.url, &config.git_ref)?;
    info!("Resolved {} -> {}", config.git_ref, &commit_sha[..12]);

    // Load the existing manifest from the pond (transactional)
    let old_manifest = read_manifest(context, config).await?;

    // Check if anything changed
    if old_manifest.commit_sha == commit_sha {
        info!("Already at commit {}, nothing to do", &commit_sha[..12]);
        return Ok(());
    }

    // Walk the new tree
    let new_manifest = git::walk_tree(pond_path, node_id, &commit_sha)?;
    info!(
        "Tree has {} entries (was {})",
        new_manifest.entries.len(),
        old_manifest.entries.len()
    );

    // Compute diff
    let changes = sync::diff_manifests(&old_manifest, &new_manifest);
    if changes.is_empty() {
        info!("No file changes detected (tree structure unchanged)");
        write_manifest(context, config, &new_manifest).await?;
        return Ok(());
    }

    info!("Applying {} changes to pond", changes.len());

    // Apply changes
    let stats =
        sync::apply_changes(context, pond_path, node_id, &config.pond_path, &changes).await?;

    info!("Sync complete: {}", stats);

    // Save updated manifest to pond (commits atomically with file changes)
    write_manifest(context, config, &new_manifest).await?;

    Ok(())
}

/// Validate configuration YAML
fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let config: GitIngestConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config YAML: {}", e)))?;

    config.validate()?;

    serde_json::to_value(&config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to serialize config: {}", e)))
}

// Register the factory
register_executable_factory!(
    name: "git-ingest",
    description: "Pull files from a git repository branch into the pond",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let config = GitIngestConfig {
            url: "https://github.com/user/blog.git".to_string(),
            git_ref: "main".to_string(),
            pond_path: "site/content".to_string(),
        };
        config.validate().expect("valid config should pass");
    }

    #[test]
    fn test_validate_config_empty_url() {
        let config = GitIngestConfig {
            url: String::new(),
            git_ref: "main".to_string(),
            pond_path: "site/content".to_string(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_empty_ref() {
        let config = GitIngestConfig {
            url: "https://github.com/user/blog.git".to_string(),
            git_ref: String::new(),
            pond_path: "site/content".to_string(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_yaml() {
        let yaml = b"url: https://github.com/user/blog.git\nref: main\npond_path: site/content\n";
        let result = validate_config(yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_config_yaml_unknown_field() {
        let yaml = b"url: https://github.com/user/blog.git\nref: main\npond_path: site/content\nextra: bad\n";
        let result = validate_config(yaml);
        assert!(result.is_err());
    }
}
