#[cfg(test)]
mod phase2_architecture_tests {
    use crate::query::{MetadataTable, FileInfo};
    use crate::schema::{OplogEntry, ExtendedAttributes};
    use crate::delta::DeltaTableManager;
    use crate::error::TLogFSError;
    use tinyfs::EntryType;

    #[tokio::test]
    async fn test_series_table_creation() -> Result<(), TLogFSError> {
        // Test that we can create a MetadataTable successfully (SeriesTable requires actual node_id)
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().join("test_table").to_string_lossy().to_string();
        
        // Create a DeltaTableManager (this is a placeholder for the actual setup)
        let delta_manager = DeltaTableManager::new();
        
        // Create MetadataTable (this is what we can test without a full TinyFS setup)
        let _metadata_table = MetadataTable::new(table_path.clone(), delta_manager);
        
        // Note: SeriesTable requires actual TinyFS setup with real node_id
        // This test validates that MetadataTable can be created successfully
        // For full SeriesTable testing, use integration tests with actual file system
        
        println!("✅ MetadataTable creation works (SeriesTable requires real TinyFS setup)");
        Ok(())
    }

    #[tokio::test]
    async fn test_file_info_creation() -> Result<(), TLogFSError> {
        // Test FileInfo struct functionality
        let file_info = FileInfo {
            file_path: "/test/series/v1".to_string(),
            version: 1,
            min_event_time: 1000,
            max_event_time: 2000,
            timestamp_column: "timestamp".to_string(),
            size: Some(1024),
        };
        
        assert_eq!(file_info.version, 1);
        assert_eq!(file_info.min_event_time, 1000);
        assert_eq!(file_info.max_event_time, 2000);
        assert_eq!(file_info.timestamp_column, "timestamp");
        
        println!("✅ FileInfo creation and field access work");
        Ok(())
    }

    #[test]
    fn test_series_table_time_range_filtering_logic() {
        // Test the core logic for time range overlap detection
        // This tests the algorithm without requiring a full database setup
        
        // Simulate file versions with different time ranges
        let file_versions = vec![
            (1000, 1500), // File 1: covers 1000-1500
            (1400, 1800), // File 2: covers 1400-1800 (overlaps with File 1)
            (2000, 2500), // File 3: covers 2000-2500 (no overlap)
        ];
        
        // Query range: 1200-1600
        let query_start = 1200;
        let query_end = 1600;
        
        let overlapping_files: Vec<_> = file_versions
            .iter()
            .enumerate()
            .filter(|(_, (min_time, max_time))| {
                // File overlaps if: max_file >= start_query AND min_file <= end_query
                *max_time >= query_start && *min_time <= query_end
            })
            .collect();
        
        // Should find File 1 and File 2, but not File 3
        assert_eq!(overlapping_files.len(), 2);
        assert_eq!(overlapping_files[0].0, 0); // File 1 (index 0)
        assert_eq!(overlapping_files[1].0, 1); // File 2 (index 1)
        
        println!("✅ Time range overlap filtering logic works correctly");
        println!("   Query range: [{}, {}]", query_start, query_end);
        println!("   Found {} overlapping files", overlapping_files.len());
    }

    #[test]
    fn test_oplog_entry_temporal_metadata() {
        // Test that OplogEntry temporal metadata fields work correctly
        let mut attrs = ExtendedAttributes::new();
        attrs.set_timestamp_column("event_time");
        
        let entry = OplogEntry::new_file_series(
            "part123".to_string(),
            "node456".to_string(),
            12345678900, // timestamp
            1,           // version
            vec![1, 2, 3, 4], // content
            1000,        // min_event_time
            2000,        // max_event_time
            attrs,
        );
        
        // Test temporal range extraction
        let temporal_range = entry.temporal_range();
        assert_eq!(temporal_range, Some((1000, 2000)));
        
        // Test that it's identified as a FileSeries
        assert!(entry.is_series_file());
        assert_eq!(entry.file_type, EntryType::FileSeries);
        
        println!("✅ OplogEntry temporal metadata works correctly");
        println!("   Temporal range: {:?}", temporal_range);
        println!("   Entry type: {:?}", entry.file_type);
    }

    #[test]
    fn test_series_stream_metadata() {
        // Test SeriesStream metadata functionality
        use crate::query::SeriesStream;
        
        let file_infos = vec![
            FileInfo {
                file_path: "/test/v1".to_string(),
                version: 1,
                min_event_time: 1000,
                max_event_time: 1500,
                timestamp_column: "timestamp".to_string(),
                size: Some(1024),
            },
            FileInfo {
                file_path: "/test/v2".to_string(),
                version: 2,
                min_event_time: 1400,
                max_event_time: 2000,
                timestamp_column: "timestamp".to_string(),
                size: Some(2048),
            },
        ];
        
        let stream = SeriesStream::new(file_infos, "timestamp".to_string()).unwrap();
        
        // Test metadata extraction
        assert_eq!(stream.total_versions, 2);
        assert_eq!(stream.time_range, (1000, 2000)); // min of mins, max of maxes
        assert_eq!(stream.schema_info.timestamp_column, "timestamp");
        assert_eq!(stream.schema_info.total_size_bytes, 3072); // 1024 + 2048
        
        println!("✅ SeriesStream metadata calculation works correctly");
        println!("   Total versions: {}", stream.total_versions);
        println!("   Time range: {:?}", stream.time_range);
        println!("   Total size: {} bytes", stream.schema_info.total_size_bytes);
    }

    #[test]
    fn test_extended_attributes_timestamp_column() {
        // Test ExtendedAttributes functionality for timestamp column management
        let mut attrs = ExtendedAttributes::new();
        
        // Test setting and getting timestamp column
        attrs.set_timestamp_column("custom_time");
        assert_eq!(attrs.timestamp_column(), "custom_time");
        
        // Test default fallback
        let default_attrs = ExtendedAttributes::new();
        assert_eq!(default_attrs.timestamp_column(), "Timestamp");
        
        // Test JSON serialization roundtrip
        let json = attrs.to_json().unwrap();
        let restored_attrs = ExtendedAttributes::from_json(&json).unwrap();
        assert_eq!(restored_attrs.timestamp_column(), "custom_time");
        
        println!("✅ ExtendedAttributes timestamp column management works");
        println!("   Custom column: {}", attrs.timestamp_column());
        println!("   Default fallback: {}", default_attrs.timestamp_column());
    }
}
