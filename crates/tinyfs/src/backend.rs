//! Filesystem backend trait.

use crate::dir;
use crate::file;
use crate::symlink;
use crate::error::Result;
use std::path::Path;

/// Trait for filesystem backend.
pub trait FilesystemBackend {
    fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> Result<file::Handle>;
    fn create_directory(&self) -> Result<dir::Handle>;
    fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> Result<symlink::Handle>;
}
