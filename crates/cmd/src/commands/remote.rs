// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::print_stdout)]

//! `pond remote` subcommands: `add`, `remove`, `list`.
//!
//! Remote attachments live as small YAML files under `/sys/remotes/<name>`.
//! Per-remote runtime state (`last_pushed_seq:<url>`, `last_pulled_seq:<url>`,
//! `remote_mode:<name>`) lives in the control table's raw_config map; the
//! YAML on disk is intentionally portable (no per-pond watermarks).
//!
//! YAML schema -- see [`RemoteAttachment`]:
//! ```yaml
//! url: file:///path/to/remote   # required
//! region: us-east-1             # optional (S3 only)
//! access_key_id: ...            # optional (S3 only)
//! secret_access_key: ...        # optional (S3 only)
//! endpoint: http://minio:9000   # optional (S3 only, for non-AWS endpoints)
//! allow_http: true              # optional (S3 only, for non-TLS endpoints)
//! ```

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use steward::{REMOTE_MODE_PREFIX, SYS_DIR, SYS_REMOTES_DIR};
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

/// Portable, on-disk YAML config for one remote attachment.  Stored at
/// `/sys/remotes/<name>` and serialized as YAML.  Pushing this config to a
/// backup is safe -- it carries no per-pond watermarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteAttachment {
    /// Canonical remote URL: `file:///path` or `s3://bucket/prefix`.
    pub url: String,

    /// AWS region (S3 only; ignored for `file://`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub region: String,

    /// S3 access key id (S3 only).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access_key_id: String,

    /// S3 secret access key (S3 only).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_access_key: String,

    /// Custom S3 endpoint (e.g., MinIO, R2).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,

    /// Allow plain HTTP (required for local MinIO).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,
}

impl RemoteAttachment {
    /// Build the storage options map passed to `Remote::open_at_url`.
    /// Returns an empty map for `file://` URLs.
    #[must_use]
    pub fn to_storage_options(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        if self.url.starts_with("s3://") {
            if !self.region.is_empty() {
                let _ = out.insert("region".to_string(), self.region.clone());
            }
            if !self.access_key_id.is_empty() {
                let _ = out.insert("access_key_id".to_string(), self.access_key_id.clone());
            }
            if !self.secret_access_key.is_empty() {
                let _ = out.insert(
                    "secret_access_key".to_string(),
                    self.secret_access_key.clone(),
                );
            }
            if !self.endpoint.is_empty() {
                let _ = out.insert("endpoint".to_string(), self.endpoint.clone());
                let _ = out.insert(
                    "virtual_hosted_style_request".to_string(),
                    "false".to_string(),
                );
            }
            if self.allow_http {
                let _ = out.insert("allow_http".to_string(), "true".to_string());
            }
        }
        out
    }
}

/// Operating mode for a remote attachment.  Stored in the control table's
/// raw_config map under key `remote_mode:<name>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteMode {
    /// Local writes are pushed to the remote (default for `add`).
    Push,
    /// Local pulls from the remote; no writes are pushed back.
    Pull,
    /// Both directions enabled.
    Both,
}

impl RemoteMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RemoteMode::Push => "push",
            RemoteMode::Pull => "pull",
            RemoteMode::Both => "both",
        }
    }

    /// Parse from the persisted string form.  Returns an error on
    /// unrecognized values so the operator notices typos.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "push" => Ok(RemoteMode::Push),
            "pull" => Ok(RemoteMode::Pull),
            "both" => Ok(RemoteMode::Both),
            other => Err(anyhow!(
                "invalid remote mode `{}` (expected `push`, `pull`, or `both`)",
                other
            )),
        }
    }
}

/// Validate a remote name: single path segment, non-empty, no slashes,
/// no leading dot, ASCII printable.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("remote name must not be empty"));
    }
    if name.contains('/') {
        return Err(anyhow!("remote name `{}` must not contain `/`", name));
    }
    if name.starts_with('.') {
        return Err(anyhow!("remote name `{}` must not start with `.`", name));
    }
    if !name.chars().all(|c| c.is_ascii_graphic()) {
        return Err(anyhow!(
            "remote name `{}` must be ASCII printable (no whitespace or control chars)",
            name
        ));
    }
    Ok(())
}

/// Build the in-pond path for a remote attachment config.
#[must_use]
pub fn remote_config_path(name: &str) -> String {
    format!("{SYS_REMOTES_DIR}/{name}")
}

/// `pond remote add <name> <url> [--mode=push|pull|both] [--region=...] ...`
#[allow(clippy::too_many_arguments)]
pub async fn add_remote_command(
    ship_context: &ShipContext,
    name: &str,
    url: &str,
    mode: RemoteMode,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
    overwrite: bool,
) -> Result<()> {
    validate_name(name)?;
    let _parsed =
        url::Url::parse(url).map_err(|e| anyhow!("invalid remote URL `{}`: {}", url, e))?;

    let attachment = RemoteAttachment {
        url: url.to_string(),
        region: region.unwrap_or_default(),
        access_key_id: access_key_id.unwrap_or_default(),
        secret_access_key: secret_access_key.unwrap_or_default(),
        endpoint: endpoint.unwrap_or_default(),
        allow_http,
    };
    let yaml = serde_yaml::to_string(&attachment)
        .map_err(|e| anyhow!("failed to serialize remote attachment YAML: {}", e))?;

    let config_path = remote_config_path(name);
    let name_owned = name.to_string();
    let mode_str = mode.as_str();

    let mut ship = ship_context.open_pond().await?;

    ship.write_transaction(
        &steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "add".to_string(),
            name_owned.clone(),
            url.to_string(),
        ]),
        async |fs| {
            let root = fs.root().await?;

            // Idempotent parent setup: ensure /sys and /sys/remotes exist.
            let _ = root.create_dir_all(SYS_DIR).await?;
            let _ = root.create_dir_all(SYS_REMOTES_DIR).await?;

            if root.exists(&config_path).await {
                if !overwrite {
                    return Err(steward::StewardError::Aborted(format!(
                        "remote `{}` already exists at {} (use --overwrite to replace)",
                        name_owned, config_path
                    )));
                }
                let remotes_dir = root.open_dir_path(SYS_REMOTES_DIR).await?;
                remotes_dir.remove_entry(&name_owned).await.map_err(|e| {
                    steward::StewardError::Aborted(format!(
                        "failed to remove existing remote config {}: {}",
                        config_path, e
                    ))
                })?;
            }

            let mut writer = root
                .async_writer_path_with_type(&config_path, EntryType::FilePhysicalVersion)
                .await?;
            writer
                .write_all(yaml.as_bytes())
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("write yaml: {}", e)))?;
            writer
                .shutdown()
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("close yaml: {}", e)))?;

            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("Failed to add remote: {}", e))?;

    // Mode is per-pond runtime state; store in raw_config so it never ships
    // with the YAML.  The write happens in its own short-lived transaction
    // managed entirely by the control table (no data txn needed).
    let mode_key = format!("{REMOTE_MODE_PREFIX}{name}");
    ship.control_table_mut()
        .raw_config_set(&mode_key, mode_str)
        .await
        .map_err(|e| anyhow!("Failed to set remote mode `{}`: {}", mode_str, e))?;

    log::info!(
        "[OK] added remote {} -> {} (mode={})",
        name,
        url,
        mode.as_str()
    );
    Ok(())
}

/// `pond remote remove <name>` -- delete the attachment config and clear
/// associated raw_config keys.  Does NOT yet refuse on pending PostPush*
/// records (a future enhancement once D4.4 lands).
pub async fn remove_remote_command(ship_context: &ShipContext, name: &str) -> Result<()> {
    validate_name(name)?;
    let config_path = remote_config_path(name);
    let name_owned = name.to_string();

    let mut ship = ship_context.open_pond().await?;

    // Look up the URL before deleting so we can clear its watermark keys.
    let url_to_clear: Option<String> = {
        match load_remote_attachment(&mut ship, &name_owned).await {
            Ok(attachment) => Some(attachment.url),
            Err(_) => None,
        }
    };

    ship.write_transaction(
        &steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "remove".to_string(),
            name_owned.clone(),
        ]),
        async |fs| {
            let root = fs.root().await?;
            if !root.exists(&config_path).await {
                return Err(steward::StewardError::Aborted(format!(
                    "no remote named `{}` (looked at {})",
                    name_owned, config_path
                )));
            }
            let remotes_dir = root.open_dir_path(SYS_REMOTES_DIR).await?;
            remotes_dir.remove_entry(&name_owned).await.map_err(|e| {
                steward::StewardError::Aborted(format!(
                    "failed to delete remote config {}: {}",
                    config_path, e
                ))
            })?;
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("Failed to remove remote: {}", e))?;

    // Clear runtime state.  All failures here are best-effort warnings;
    // the YAML on disk is already gone.
    let mode_key = format!("{REMOTE_MODE_PREFIX}{name}");
    if let Err(e) = ship.control_table_mut().raw_config_set(&mode_key, "").await {
        log::warn!("[WARN] failed to clear {}: {}", mode_key, e);
    }
    if let Some(url) = url_to_clear {
        for key in [
            format!("last_pushed_seq:{url}"),
            format!("last_pulled_seq:{url}"),
        ] {
            if let Err(e) = ship.control_table_mut().raw_config_set(&key, "").await {
                log::warn!("[WARN] failed to clear {}: {}", key, e);
            }
        }
    }

    log::info!("[OK] removed remote {}", name);
    Ok(())
}

/// `pond remote list` -- print each remote with URL, mode, and latest
/// watermarks (`last_pushed_seq`, `last_pulled_seq`).
pub async fn list_remotes_command(ship_context: &ShipContext) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    let entries = list_remote_names(&mut ship).await?;

    if entries.is_empty() {
        println!("(no remotes; use `pond remote add <name> <url>` to attach one)");
        return Ok(());
    }

    println!(
        "{:<20} {:<60} {:<6} {:>16} {:>16}",
        "NAME", "URL", "MODE", "LAST_PUSHED_SEQ", "LAST_PULLED_SEQ"
    );
    for name in entries {
        let attachment = match load_remote_attachment(&mut ship, &name).await {
            Ok(a) => a,
            Err(e) => {
                log::warn!("[WARN] could not read /sys/remotes/{}: {}", name, e);
                continue;
            }
        };
        let mode = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
            .await
            .unwrap_or_default()
            .unwrap_or_else(|| "push".to_string());
        let last_pushed = ship
            .control_table()
            .raw_config_get(&format!("last_pushed_seq:{}", attachment.url))
            .await
            .unwrap_or_default()
            .unwrap_or_else(|| "-".to_string());
        let last_pulled = ship
            .control_table()
            .raw_config_get(&format!("last_pulled_seq:{}", attachment.url))
            .await
            .unwrap_or_default()
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<20} {:<60} {:<6} {:>16} {:>16}",
            name, attachment.url, mode, last_pushed, last_pulled
        );
    }
    Ok(())
}

/// Read and parse the YAML for `<name>` from `/sys/remotes/<name>`.
///
/// Returns an error if the file does not exist or fails to parse.
pub async fn load_remote_attachment(
    ship: &mut steward::Steward,
    name: &str,
) -> Result<RemoteAttachment> {
    validate_name(name)?;
    let config_path = remote_config_path(name);
    let name_owned = name.to_string();

    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "load".to_string(),
            name_owned.clone(),
        ]))
        .await
        .map_err(|e| anyhow!("failed to begin read transaction: {}", e))?;

    let yaml_bytes = {
        let fs = &*tx;
        let root = fs
            .root()
            .await
            .map_err(|e| anyhow!("failed to open root: {}", e))?;
        if !root.exists(&config_path).await {
            let _ = tx
                .commit()
                .await
                .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;
            return Err(anyhow!("no remote named `{}` ({})", name, config_path));
        }
        root.read_file_path_to_vec(&config_path)
            .await
            .map_err(|e| anyhow!("failed to read {}: {}", config_path, e))?
    };
    let _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;

    let attachment: RemoteAttachment = serde_yaml::from_slice(&yaml_bytes)
        .map_err(|e| anyhow!("failed to parse {}: {}", config_path, e))?;
    Ok(attachment)
}

/// List all remote names under `/sys/remotes/`.  Returns an empty Vec if
/// `/sys/remotes/` does not exist.
pub async fn list_remote_names(ship: &mut steward::Steward) -> Result<Vec<String>> {
    use crate::common::FileInfoVisitor;
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "remote".to_string(),
            "list".to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("failed to begin read transaction: {}", e))?;

    let names: Vec<String> = {
        let fs = &*tx;
        let root = fs
            .root()
            .await
            .map_err(|e| anyhow!("failed to open root: {}", e))?;
        if !root.exists(SYS_REMOTES_DIR).await {
            let _ = tx
                .commit()
                .await
                .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;
            return Ok(Vec::new());
        }
        let pattern = format!("{SYS_REMOTES_DIR}/*");
        let mut visitor = FileInfoVisitor::new(true);
        let infos = root
            .visit_with_visitor(&pattern, &mut visitor)
            .await
            .map_err(|e| anyhow!("failed to list {}: {}", pattern, e))?;
        infos
            .into_iter()
            .filter_map(|info| {
                std::path::Path::new(&info.path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(str::to_string)
            })
            .collect()
    };
    let _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("failed to commit read tx: {}", e))?;
    Ok(names)
}

/// Lookup the operating mode for `<name>`.  Defaults to `Push` if unset.
pub async fn remote_mode_for(ship: &steward::Steward, name: &str) -> Result<RemoteMode> {
    let raw = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("failed to read remote mode for `{}`: {}", name, e))?;
    match raw {
        Some(s) if !s.is_empty() => RemoteMode::parse(&s),
        _ => Ok(RemoteMode::Push),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_accepts_simple() {
        validate_name("origin").unwrap();
        validate_name("backup-01").unwrap();
        validate_name("s3_minio").unwrap();
    }

    #[test]
    fn test_validate_name_rejects_bad() {
        assert!(validate_name("").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name(".hidden").is_err());
        assert!(validate_name("has space").is_err());
        assert!(validate_name("tab\there").is_err());
    }

    #[test]
    fn test_remote_config_path_format() {
        assert_eq!(remote_config_path("origin"), "/sys/remotes/origin");
    }

    #[test]
    fn test_mode_roundtrip() {
        for m in [RemoteMode::Push, RemoteMode::Pull, RemoteMode::Both] {
            assert_eq!(RemoteMode::parse(m.as_str()).unwrap(), m);
        }
        assert!(RemoteMode::parse("bogus").is_err());
    }

    #[test]
    fn test_yaml_roundtrip_file_url() {
        let a = RemoteAttachment {
            url: "file:///tmp/x".to_string(),
            region: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            endpoint: String::new(),
            allow_http: false,
        };
        let y = serde_yaml::to_string(&a).unwrap();
        let back: RemoteAttachment = serde_yaml::from_str(&y).unwrap();
        assert_eq!(back.url, a.url);
        assert_eq!(back.allow_http, false);
        // skip_serializing_if should keep empty fields out of the YAML
        assert!(!y.contains("region:"));
        assert!(!y.contains("access_key_id:"));
    }

    #[test]
    fn test_storage_options_empty_for_file() {
        let a = RemoteAttachment {
            url: "file:///tmp/x".to_string(),
            region: "us-west-2".to_string(), // ignored for file://
            access_key_id: String::new(),
            secret_access_key: String::new(),
            endpoint: String::new(),
            allow_http: false,
        };
        assert!(a.to_storage_options().is_empty());
    }

    #[test]
    fn test_storage_options_populated_for_s3() {
        let a = RemoteAttachment {
            url: "s3://bucket/prefix".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "AKIA".to_string(),
            secret_access_key: "SECRET".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            allow_http: true,
        };
        let opts = a.to_storage_options();
        assert_eq!(opts.get("region").unwrap(), "us-east-1");
        assert_eq!(opts.get("access_key_id").unwrap(), "AKIA");
        assert_eq!(opts.get("secret_access_key").unwrap(), "SECRET");
        assert_eq!(opts.get("endpoint").unwrap(), "http://localhost:9000");
        assert_eq!(opts.get("allow_http").unwrap(), "true");
        assert_eq!(opts.get("virtual_hosted_style_request").unwrap(), "false");
    }
}
