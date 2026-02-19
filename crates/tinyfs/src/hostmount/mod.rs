// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Hostmount -- host filesystem as a TinyFS persistence layer
//!
//! This module maps a host directory tree onto TinyFS, implementing
//! `PersistenceLayer` backed by `std::fs` / `tokio::fs`. It is a peer
//! of the `memory` module (in-memory persistence) and `tlogfs` (Delta Lake).
//!
//! The hostmount root `/` maps to a configurable host directory. Files are
//! presented as `FilePhysicalVersion` (data) by default -- URL scheme
//! layering handles type interpretation at command time.
//!
//! FileIDs are deterministic using `FileID::from_content()`, derived from
//! the relative path within the hostmount root. This gives stable,
//! reproducible IDs across runs.

mod directory;
mod file;
mod persistence;

#[cfg(test)]
mod tests;

pub use directory::HostDirectory;
pub use file::HostFile;
pub use persistence::HostmountPersistence;

/// Create a new hostmount-based filesystem rooted at the given host directory
///
/// The directory must exist. All TinyFS paths will resolve relative to this root.
///
/// # Errors
///
/// Returns an error if the root directory does not exist or is not a directory.
pub async fn new_fs(root: std::path::PathBuf) -> crate::error::Result<crate::FS> {
    let persistence = HostmountPersistence::new(root)?;
    crate::FS::new(persistence).await
}
