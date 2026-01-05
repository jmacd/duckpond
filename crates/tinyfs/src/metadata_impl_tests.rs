// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod metadata_tests {
    use crate::EntryType;
    use crate::memory::{MemoryDirectory, MemoryPersistence, MemorySymlink};
    use crate::node::{FileID, PartID};
    use crate::persistence::PersistenceLayer;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_memory_file_metadata() {
        let persistence = MemoryPersistence::default();
        let id = FileID::new_in_partition(PartID::root(), EntryType::FilePhysicalVersion);

        // Create file node in persistence (proper way to create nodes)
        let node = persistence.create_file_node(id).await.unwrap();
        persistence.store_node(&node).await.unwrap();

        // Get file handle from the node
        let file_handle = match &node.node_type {
            crate::node::NodeType::File(handle) => handle.clone(),
            _ => panic!("Expected file node"),
        };

        // Store content as a version so metadata can read it
        persistence
            .store_file_version(id, 1, b"test content".to_vec())
            .await
            .unwrap();

        let metadata = file_handle.metadata().await.unwrap();

        assert_eq!(metadata.entry_type, EntryType::FilePhysicalVersion);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, Some(12)); // "test content" is 12 bytes
        assert!(metadata.blake3.is_some());
    }

    #[tokio::test]
    async fn test_memory_directory_metadata() {
        let dir_handle = MemoryDirectory::new_handle();
        let metadata = dir_handle.metadata().await.unwrap();

        assert_eq!(metadata.entry_type, EntryType::DirectoryPhysical);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, None); // Directories don't have sizes
        assert_eq!(metadata.blake3, None); // Directories don't have checksums
    }

    #[tokio::test]
    async fn test_memory_symlink_metadata() {
        let target = PathBuf::from("/target/path");
        let symlink_handle = MemorySymlink::new_handle(target);
        let metadata = symlink_handle.metadata().await.unwrap();

        assert_eq!(metadata.entry_type, EntryType::Symlink);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, None); // Symlinks don't have sizes
        assert_eq!(metadata.blake3, None); // Symlinks don't have checksums
    }
}
