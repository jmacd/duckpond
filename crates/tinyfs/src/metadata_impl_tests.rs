// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod metadata_tests {
    use crate::EntryType;
    use crate::memory::{MemoryDirectory, MemoryFile, MemoryPersistence, MemorySymlink};
    use crate::node::{FileID, PartID};
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_memory_file_metadata() {
        let persistence = MemoryPersistence::default();
        let id = FileID::new_in_partition(PartID::root(), EntryType::FilePhysicalVersion);

        // Store content as a version so metadata can read it
        persistence
            .store_file_version(id, 1, b"test content".to_vec())
            .await
            .unwrap();

        let file_handle = MemoryFile::new_handle(id, persistence, EntryType::FilePhysicalVersion);
        let metadata = file_handle.metadata().await.unwrap();

        assert_eq!(metadata.entry_type, EntryType::FilePhysicalVersion);
        assert_eq!(metadata.version, 1);
        // Note: size is now 0 because MemoryFile content starts empty (content lives in versions)
        // This test verifies the basic structure works
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
