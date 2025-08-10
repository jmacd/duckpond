#[cfg(test)]
mod metadata_consolidation_tests {
    use crate::schema::{OplogEntry, compute_sha256};
    use tinyfs::EntryType;
    use chrono::Utc;

    #[test]
    fn test_oplog_entry_metadata_small_file() {
        // Test that small files have correct metadata
        let content = b"test file content";
        let entry = OplogEntry::new_small_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileTable, // Parquet file
            Utc::now().timestamp_micros(),
            1,
            content.to_vec(),
        );

        let metadata = entry.metadata();
        
        assert_eq!(metadata.entry_type, EntryType::FileTable);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, Some(content.len() as u64));
        assert!(metadata.sha256.is_some());
        assert_eq!(metadata.sha256.unwrap(), compute_sha256(content));
        assert!(!entry.is_large_file());
    }

    #[test]
    fn test_oplog_entry_metadata_large_file() {
        // Test that large files have correct metadata
        let expected_size = 5000000u64; // 5MB
        let expected_sha256 = "abc123def456";
        
        let entry = OplogEntry::new_large_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileSeries, // Time series file
            Utc::now().timestamp_micros(),
            1,
            expected_sha256.to_string(),
            expected_size as i64, // Cast to i64 to match Delta Lake protocol
        );

        let metadata = entry.metadata();
        
        assert_eq!(metadata.entry_type, EntryType::FileSeries);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, Some(expected_size));
        assert_eq!(metadata.sha256, Some(expected_sha256.to_string()));
        assert!(entry.is_large_file());
    }

    #[test]
    fn test_oplog_entry_metadata_directory() {
        // Test that directories have correct metadata
        let entry = OplogEntry::new_inline(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::Directory,
            Utc::now().timestamp_micros(),
            1,
            vec![1, 2, 3], // Some directory content
        );

        let metadata = entry.metadata();
        
        assert_eq!(metadata.entry_type, EntryType::Directory);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, None); // Directories don't have sizes
        assert_eq!(metadata.sha256, None); // Directories don't have checksums
        assert!(!entry.is_large_file());
    }

    #[test]
    fn test_oplog_entry_metadata_symlink() {
        // Test that symlinks have correct metadata
        let entry = OplogEntry::new_inline(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::Symlink,
            Utc::now().timestamp_micros(),
            1,
            b"/target/path".to_vec(),
        );

        let metadata = entry.metadata();
        
        assert_eq!(metadata.entry_type, EntryType::Symlink);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.size, None); // Symlinks don't have sizes
        assert_eq!(metadata.sha256, None); // Symlinks don't have checksums
        assert!(!entry.is_large_file());
    }

    #[test]
    fn test_file_type_preservation() {
        // Test that different file types are preserved correctly
        let test_cases = vec![
            EntryType::FileData,
            EntryType::FileTable,
            EntryType::FileSeries,
        ];

        for entry_type in test_cases {
            let entry = OplogEntry::new_small_file(
                "part123".to_string(),
                "node456".to_string(),
                entry_type,
                Utc::now().timestamp_micros(),
                1,
                b"test content".to_vec(),
            );

            let metadata = entry.metadata();
            assert_eq!(metadata.entry_type, entry_type, 
                      "Entry type should be preserved for {:?}", entry_type);
        }
    }

    #[test]
    fn test_sha256_computation() {
        // Test that SHA256 is computed correctly for small files
        let content = b"Hello, World!";
        let expected_sha256 = compute_sha256(content);
        
        let entry = OplogEntry::new_small_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileData,
            Utc::now().timestamp_micros(),
            1,
            content.to_vec(),
        );

        let metadata = entry.metadata();
        assert_eq!(metadata.sha256, Some(expected_sha256));
    }

    #[test]
    fn test_large_file_detection() {
        // Test the is_large_file logic
        
        // Small file - has content, no matter what sha256 says
        let small_file = OplogEntry::new_small_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileData,
            Utc::now().timestamp_micros(),
            1,
            b"small content".to_vec(),
        );
        assert!(!small_file.is_large_file());

        // Large file - no content, is a file type
        let large_file = OplogEntry::new_large_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileTable,
            Utc::now().timestamp_micros(),
            1,
            "sha256hash".to_string(),
            1000000,
        );
        assert!(large_file.is_large_file());

        // Directory - no content, but not a file type
        let directory = OplogEntry::new_inline(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::Directory,
            Utc::now().timestamp_micros(),
            1,
            vec![],
        );
        assert!(!directory.is_large_file());
    }

    #[test]
    fn test_file_size_method() {
        // Test the file_size convenience method
        
        let small_file = OplogEntry::new_small_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileData,
            Utc::now().timestamp_micros(),
            1,
            b"content with 20 chars".to_vec(),
        );
        assert_eq!(small_file.file_size(), Some(21)); // "content with 20 chars" is 21 chars

        let large_file = OplogEntry::new_large_file(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::FileTable,
            Utc::now().timestamp_micros(),
            1,
            "sha256hash".to_string(),
            5000000,
        );
        assert_eq!(large_file.file_size(), Some(5000000));

        let directory = OplogEntry::new_inline(
            "part123".to_string(),
            "node456".to_string(),
            EntryType::Directory,
            Utc::now().timestamp_micros(),
            1,
            vec![],
        );
        assert_eq!(directory.file_size(), None);
    }
}
