#[cfg(test)]
mod metadata_tests {
    use super::*;
    use crate::{NodeMetadata, EntryType, Metadata};
    use crate::memory::{MemoryFile, MemoryDirectory, MemorySymlink};
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_memory_file_metadata() {
        let file_handle = MemoryFile::new_handle(b"test content".to_vec());
        let metadata = file_handle.metadata().await.unwrap();
        
        assert_eq!(metadata.entry_type, EntryType::FileData);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, Some(12)); // "test content" is 12 bytes
        assert!(metadata.sha256.is_some());
    }

    #[tokio::test]
    async fn test_memory_directory_metadata() {
        let dir_handle = MemoryDirectory::new_handle();
        let metadata = dir_handle.metadata().await.unwrap();
        
        assert_eq!(metadata.entry_type, EntryType::Directory);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, None); // Directories don't have sizes
        assert_eq!(metadata.sha256, None); // Directories don't have checksums
    }

    #[tokio::test]
    async fn test_memory_symlink_metadata() {
        let target = PathBuf::from("/target/path");
        let symlink_handle = MemorySymlink::new_handle(target);
        let metadata = symlink_handle.metadata().await.unwrap();
        
        assert_eq!(metadata.entry_type, EntryType::Symlink);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, None); // Symlinks don't have sizes
        assert_eq!(metadata.sha256, None); // Symlinks don't have checksums
    }

    #[tokio::test]
    async fn test_metadata_u64_compatibility() {
        let file_handle = MemoryFile::new_handle(b"test content".to_vec());
        
        // Test that metadata_u64 uses the new metadata() method
        let version = file_handle.metadata_u64("version").await.unwrap();
        assert_eq!(version, Some(1));
        
        let size = file_handle.metadata_u64("size").await.unwrap();
        assert_eq!(size, Some(12));
        
        // Test unknown field
        let unknown = file_handle.metadata_u64("unknown").await.unwrap();
        assert_eq!(unknown, None);
    }
}
