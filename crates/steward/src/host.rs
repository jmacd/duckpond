// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Host filesystem steward -- lightweight, no transactions or control table
//!
//! `HostSteward` wraps a host directory via `tinyfs::hostmount`, providing
//! a `HostTransaction` that gives `FS` + `ProviderContext` access.
//! There are no real transactions -- the host filesystem is always
//! immediately consistent.
//!
//! When mount specs are provided (via `--hostmount`), the steward creates
//! an `OverlayPersistence` that merges factory-mounted nodes with the host
//! filesystem, enabling dynamic-dir and other factories without a pond.

use crate::StewardError;
use datafusion::execution::context::SessionContext;
use provider::FactoryRegistry;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tinyfs::hostmount::MountSpec;
use tinyfs::{FS, PersistenceLayer, ProviderContext};
use tlogfs::PondUserMetadata;

/// Lightweight steward for the host filesystem.
///
/// No control table, no audit log, no Delta Lake transactions.
/// Each "transaction" simply creates an `FS` from the hostmount
/// persistence layer and a standalone DataFusion session.
///
/// When mount specs are present, the host filesystem is wrapped with
/// an `OverlayPersistence` that injects factory nodes at mount points.
pub struct HostSteward {
    /// Root directory for the hostmount
    root_path: PathBuf,
    /// Overlay mount specifications (from --hostmount flags)
    mount_specs: Vec<MountSpec>,
}

impl HostSteward {
    /// Create a new host steward rooted at the given directory.
    ///
    /// The directory must exist and be accessible.
    #[must_use]
    pub fn new(root_path: PathBuf, mount_specs: Vec<MountSpec>) -> Self {
        Self {
            root_path,
            mount_specs,
        }
    }

    /// Get the root path of this host steward.
    #[must_use]
    pub fn root_path(&self) -> &std::path::Path {
        &self.root_path
    }

    /// Begin a host "transaction" (creates FS + session).
    ///
    /// There is no distinction between read and write transactions
    /// on the host filesystem -- both return the same `HostTransaction`.
    /// The `meta` is carried for debugging/logging, not for audit.
    ///
    /// If mount specs are present, the persistence layer is wrapped with
    /// `OverlayPersistence` to inject factory nodes at mount points.
    pub async fn begin(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<HostTransaction, StewardError> {
        let hostmount =
            tinyfs::hostmount::HostmountPersistence::new(self.root_path.clone())?;

        let persistence_arc: Arc<dyn PersistenceLayer> = if self.mount_specs.is_empty() {
            Arc::new(hostmount)
        } else {
            // Process mount specs into overlay entries
            let root_overlays =
                Self::process_mount_specs(&hostmount, &self.mount_specs).await?;

            log::debug!(
                "[HOST] Created {} overlay mounts",
                root_overlays.len()
            );

            Arc::new(tinyfs::hostmount::OverlayPersistence::new(
                hostmount,
                root_overlays,
            ))
        };

        let fs = FS::from_arc(persistence_arc.clone());
        let session = Arc::new(SessionContext::new());

        log::debug!(
            "[HOST] Begin transaction: root={:?}, mounts={}, meta={:?}",
            self.root_path,
            self.mount_specs.len(),
            meta.args
        );

        Ok(HostTransaction {
            fs,
            session,
            persistence: persistence_arc,
            meta: meta.clone(),
        })
    }

    /// Process mount specs by reading config files and creating factory nodes.
    ///
    /// Currently supports only depth-1 mounts (e.g., `/reduced`).
    /// Returns a map of root-level overlay entries: name -> Node.
    async fn process_mount_specs(
        hostmount: &tinyfs::hostmount::HostmountPersistence,
        specs: &[MountSpec],
    ) -> Result<HashMap<String, tinyfs::Node>, StewardError> {
        let mut root_overlays: HashMap<String, tinyfs::Node> = HashMap::new();

        // We need a temporary FS + ProviderContext to read config files
        // and create factory nodes. The factory nodes will then be injected
        // into the overlay, which creates the real FS.
        let temp_persistence: Arc<dyn PersistenceLayer> = Arc::new(
            tinyfs::hostmount::HostmountPersistence::new(hostmount.root_path().to_path_buf())?,
        );
        let temp_fs = FS::from_arc(temp_persistence.clone());
        let temp_session = Arc::new(SessionContext::new());
        let provider_context = ProviderContext::new(temp_session, temp_persistence);

        for spec in specs {
            if spec.remaining_path().is_some() {
                return Err(StewardError::ControlTable(format!(
                    "Nested mount paths not yet supported: '{}'. Only depth-1 mounts (e.g., /reduced) are currently implemented.",
                    spec.mount_path
                )));
            }

            let mount_name = spec.root_component().to_string();

            log::debug!(
                "[HOST] Processing mount: {} -> factory={}, config={}",
                spec.mount_path,
                spec.factory_name,
                spec.config_path
            );

            // Read config file from host filesystem
            let config_path = format!("/{}", spec.config_path);
            let config_bytes = {
                let root = temp_fs.root().await.map_err(|e| {
                    StewardError::ControlTable(format!(
                        "Failed to open hostmount root: {}",
                        e
                    ))
                })?;
                let mut reader = root
                    .async_reader_path(&config_path)
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!(
                            "Failed to open config file '{}': {}",
                            spec.config_path, e
                        ))
                    })?;

                let mut buffer = Vec::new();
                use tokio::io::AsyncReadExt;
                _ = reader.read_to_end(&mut buffer).await.map_err(|e| {
                    StewardError::ControlTable(format!(
                        "Failed to read config file '{}': {}",
                        spec.config_path, e
                    ))
                })?;
                buffer
            };

            log::debug!(
                "[HOST] Read {} bytes from config '{}'",
                config_bytes.len(),
                spec.config_path
            );

            // Resolve factory name aliases
            let factory_name = resolve_factory_alias(&spec.factory_name);

            // Compute deterministic FileID for the mount point
            let mount_id_bytes = format!("hostmount:{}:{}", mount_name, factory_name);
            let parent_part_id =
                tinyfs::PartID::from_node_id(tinyfs::FileID::root().node_id());

            // Determine if the factory creates directories or files
            let creates_directory =
                FactoryRegistry::factory_creates_directory(&factory_name).map_err(|e| {
                    StewardError::ControlTable(format!(
                        "Factory '{}' not found: {}",
                        factory_name, e
                    ))
                })?;

            if creates_directory {
                let entry_type = tinyfs::EntryType::DirectoryDynamic;
                let child_file_id = tinyfs::FileID::from_content(
                    parent_part_id,
                    entry_type,
                    mount_id_bytes.as_bytes(),
                );
                let factory_context =
                    tinyfs::FactoryContext::new(provider_context.clone(), child_file_id);
                let dir_handle = FactoryRegistry::create_directory(
                    &factory_name,
                    &config_bytes,
                    factory_context,
                )
                .map_err(|e| {
                    StewardError::ControlTable(format!(
                        "Failed to create factory directory for mount '{}': {}",
                        spec.mount_path, e
                    ))
                })?;

                let node = tinyfs::Node::new(
                    child_file_id,
                    tinyfs::NodeType::Directory(dir_handle),
                );
                _ = root_overlays.insert(mount_name, node);
            } else {
                let entry_type = tinyfs::EntryType::TableDynamic;
                let child_file_id = tinyfs::FileID::from_content(
                    parent_part_id,
                    entry_type,
                    mount_id_bytes.as_bytes(),
                );
                let factory_context =
                    tinyfs::FactoryContext::new(provider_context.clone(), child_file_id);
                let file_handle = FactoryRegistry::create_file(
                    &factory_name,
                    &config_bytes,
                    factory_context,
                )
                .await
                .map_err(|e| {
                    StewardError::ControlTable(format!(
                        "Failed to create factory file for mount '{}': {}",
                        spec.mount_path, e
                    ))
                })?;

                let node = tinyfs::Node::new(
                    child_file_id,
                    tinyfs::NodeType::File(file_handle),
                );
                _ = root_overlays.insert(mount_name, node);
            }

            log::info!(
                "[HOST] Mounted factory '{}' at '{}'",
                factory_name,
                spec.mount_path
            );
        }

        Ok(root_overlays)
    }
}

/// Resolve factory name aliases.
///
/// `dyndir` is a short alias for `dynamic-dir`.
fn resolve_factory_alias(name: &str) -> String {
    match name {
        "dyndir" => "dynamic-dir".to_string(),
        other => other.to_string(),
    }
}

/// A host filesystem "transaction".
///
/// Not a real transaction -- the host filesystem has no commit/rollback.
/// Provides `FS` access (via `Deref`) and `ProviderContext` /
/// `SessionContext` for DataFusion queries against host files.
pub struct HostTransaction {
    /// The hostmount-backed filesystem
    fs: FS,
    /// Standalone DataFusion session
    session: Arc<SessionContext>,
    /// Persistence layer (shared with fs, used for ProviderContext)
    persistence: Arc<dyn PersistenceLayer>,
    /// Command metadata for debugging (not persisted -- host has no control table)
    meta: PondUserMetadata,
}

impl HostTransaction {
    /// Get a `ProviderContext` for this host transaction.
    #[must_use]
    pub fn provider_context(&self) -> ProviderContext {
        ProviderContext::new(self.session.clone(), self.persistence.clone())
    }

    /// Get the DataFusion `SessionContext`.
    #[must_use]
    pub fn session_context(&self) -> Arc<SessionContext> {
        self.session.clone()
    }

    /// Look up the factory name for a filesystem node.
    ///
    /// On the host filesystem, factory names will eventually come from
    /// the URL scheme (e.g., `host+sitegen://site.yaml`). For now,
    /// returns `None` -- factory support is wired in Phase 5.
    pub async fn get_factory_for_node(
        &self,
        _id: tinyfs::FileID,
    ) -> Result<Option<String>, tinyfs::Error> {
        Ok(None)
    }

    /// Get the debug metadata for this transaction.
    #[must_use]
    pub fn meta(&self) -> &PondUserMetadata {
        &self.meta
    }
}

impl Deref for HostTransaction {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        &self.fs
    }
}
