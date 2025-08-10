/// Tests for FileSeries functionality with temporal metadata
/// 
/// This module tests the Phase 0 schema foundation for file:series implementation,
/// including OplogEntry extensions, temporal metadata extraction, and extended attributes.

use super::schema::{
    OplogEntry, ExtendedAttributes, ForArrow, 
    extract_temporal_range_from_batch, detect_timestamp_column, duckpond
};
use arrow::record_batch::RecordBatch;
use arrow::array::{Int64Array, TimestampMillisecondArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;
use chrono::Utc;

#[test]
fn test_extended_attributes_creation() {
    let mut attrs = ExtendedAttributes::new();
    
    // Test setting timestamp column
    attrs.set_timestamp_column("event_time");
    assert_eq!(attrs.timestamp_column(), "event_time");
    
    // Test raw attributes
    attrs.set_raw("sensor.type", "temperature");
    attrs.set_raw("sensor.location", "building_a");
    
    assert_eq!(attrs.get_raw("sensor.type"), Some("temperature"));
    assert_eq!(attrs.get_raw("sensor.location"), Some("building_a"));
    assert_eq!(attrs.get_raw("nonexistent"), None);
}

#[test]
fn test_extended_attributes_json_serialization() {
    let mut attrs = ExtendedAttributes::new();
    attrs.set_timestamp_column("custom_time");
    attrs.set_raw("project.name", "duckpond");
    attrs.set_raw("version", "1.0");
    
    // Test serialization
    let json = attrs.to_json().expect("Failed to serialize to JSON");
    assert!(json.contains("duckpond.timestamp_column"));
    assert!(json.contains("custom_time"));
    
    // Test deserialization
    let deserialized = ExtendedAttributes::from_json(&json).expect("Failed to deserialize from JSON");
    assert_eq!(deserialized.timestamp_column(), "custom_time");
    assert_eq!(deserialized.get_raw("project.name"), Some("duckpond"));
    assert_eq!(deserialized.get_raw("version"), Some("1.0"));
}

#[test]
fn test_extended_attributes_default_timestamp_column() {
    let attrs = ExtendedAttributes::new();
    // Should return default "Timestamp" when not set
    assert_eq!(attrs.timestamp_column(), "Timestamp");
}

#[test]
fn test_oplog_entry_file_series_constructor() {
    let mut attrs = ExtendedAttributes::new();
    attrs.set_timestamp_column("event_time");
    attrs.set_raw("sensor.type", "pressure");
    
    let content = b"test parquet data".to_vec();
    let entry = OplogEntry::new_file_series(
        "partition123".to_string(),
        "node456".to_string(),
        Utc::now().timestamp_micros(),
        1,
        content.clone(),
        1640995200000i64, // 2022-01-01 00:00:00 UTC in milliseconds
        1640995260000i64, // 2022-01-01 00:01:00 UTC in milliseconds
        attrs,
    );
    
    assert_eq!(entry.file_type, tinyfs::EntryType::FileSeries);
    assert_eq!(entry.min_event_time, Some(1640995200000i64));
    assert_eq!(entry.max_event_time, Some(1640995260000i64));
    assert!(entry.extended_attributes.is_some());
    assert!(entry.is_series_file());
    
    // Test temporal range extraction
    let range = entry.temporal_range();
    assert_eq!(range, Some((1640995200000i64, 1640995260000i64)));
    
    // Test extended attributes retrieval
    let retrieved_attrs = entry.get_extended_attributes().expect("Failed to get extended attributes");
    assert_eq!(retrieved_attrs.timestamp_column(), "event_time");
    assert_eq!(retrieved_attrs.get_raw("sensor.type"), Some("pressure"));
}

#[test]
fn test_oplog_entry_large_file_series_constructor() {
    let mut attrs = ExtendedAttributes::new();
    attrs.set_timestamp_column("timestamp");
    
    let entry = OplogEntry::new_large_file_series(
        "partition789".to_string(),
        "node101112".to_string(),
        Utc::now().timestamp_micros(),
        2,
        "sha256:abcdef123456".to_string(),
        1024000i64, // 1MB file (i64 to match Delta Lake protocol)
        1640995200000i64,
        1640995800000i64, // 10 minute range
        attrs,
    );
    
    assert_eq!(entry.file_type, tinyfs::EntryType::FileSeries);
    assert!(entry.content.is_none()); // Large file
    assert_eq!(entry.sha256, Some("sha256:abcdef123456".to_string()));
    assert_eq!(entry.size, Some(1024000i64)); // i64 to match Delta Lake protocol
    assert_eq!(entry.min_event_time, Some(1640995200000i64));
    assert_eq!(entry.max_event_time, Some(1640995800000i64));
    assert!(entry.is_large_file());
    assert!(entry.is_series_file());
}

#[test]
fn test_temporal_range_extraction_from_batch() {
    // Create test data with timestamp column
    let timestamps = TimestampMillisecondArray::from(vec![
        1640995200000i64, // 2022-01-01 00:00:00 UTC
        1640995260000i64, // 2022-01-01 00:01:00 UTC  
        1640995320000i64, // 2022-01-01 00:02:00 UTC
        1640995140000i64, // 2021-12-31 23:59:00 UTC (earlier)
        1640995380000i64, // 2022-01-01 00:03:00 UTC (later)
    ]);
    
    let values = StringArray::from(vec!["reading1", "reading2", "reading3", "reading4", "reading5"]);
    
    let schema = Schema::new(vec![
        Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Utf8, false),
    ]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(timestamps),
            Arc::new(values),
        ]
    ).expect("Failed to create RecordBatch");
    
    // Test temporal range extraction
    let (min_time, max_time) = extract_temporal_range_from_batch(&batch, "event_time")
        .expect("Failed to extract temporal range");
    
    assert_eq!(min_time, 1640995140000i64); // Earliest timestamp
    assert_eq!(max_time, 1640995380000i64); // Latest timestamp
}

#[test]
fn test_temporal_range_extraction_int64_timestamps() {
    // Test with raw int64 timestamps
    let timestamps = Int64Array::from(vec![
        1640995200i64,
        1640995260i64,
        1640995320i64,
        1640995140i64, // Min
        1640995380i64, // Max
    ]);
    
    let values = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    
    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(timestamps),
            Arc::new(values),
        ]
    ).expect("Failed to create RecordBatch");
    
    let (min_time, max_time) = extract_temporal_range_from_batch(&batch, "timestamp")
        .expect("Failed to extract temporal range");
    
    assert_eq!(min_time, 1640995140i64);
    assert_eq!(max_time, 1640995380i64);
}

#[test]
fn test_timestamp_column_detection() {
    // Test schema with standard timestamp column
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]);
    
    let detected = detect_timestamp_column(&schema1).expect("Failed to detect timestamp column");
    assert_eq!(detected, "timestamp");
    
    // Test schema with alternative timestamp column name
    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_time", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]);
    
    let detected2 = detect_timestamp_column(&schema2).expect("Failed to detect timestamp column");
    assert_eq!(detected2, "event_time");
    
    // Test schema with int64 timestamp
    let schema3 = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]);
    
    let detected3 = detect_timestamp_column(&schema3).expect("Failed to detect timestamp column");
    assert_eq!(detected3, "ts");
}

#[test]
fn test_timestamp_column_detection_failure() {
    // Test schema with no timestamp columns
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    
    let result = detect_timestamp_column(&schema);
    assert!(result.is_err());
}

#[test]
fn test_oplog_entry_schema_includes_new_fields() {
    let arrow_fields = OplogEntry::for_arrow();
    
    // Check that new temporal fields are present
    let field_names: Vec<&str> = arrow_fields.iter()
        .map(|field| field.name().as_str())
        .collect();
    
    assert!(field_names.contains(&"min_event_time"));
    assert!(field_names.contains(&"max_event_time"));
    assert!(field_names.contains(&"extended_attributes"));
    
    // Verify field types
    let min_event_time_field = arrow_fields.iter()
        .find(|field| field.name() == "min_event_time")
        .expect("min_event_time field not found");
    assert_eq!(min_event_time_field.data_type(), &DataType::Int64);
    assert!(min_event_time_field.is_nullable()); // Should be nullable
    
    let extended_attrs_field = arrow_fields.iter()
        .find(|field| field.name() == "extended_attributes")
        .expect("extended_attributes field not found");
    assert_eq!(extended_attrs_field.data_type(), &DataType::Utf8);
    assert!(extended_attrs_field.is_nullable()); // Should be nullable
}

#[test]
fn test_regular_file_entries_have_no_temporal_metadata() {
    // Test that regular file entries (non-series) have None for temporal fields
    let regular_file = OplogEntry::new_small_file(
        "partition".to_string(),
        "node".to_string(),
        tinyfs::EntryType::FileData,
        Utc::now().timestamp_micros(),
        1,
        b"regular file content".to_vec(),
    );
    
    assert_eq!(regular_file.file_type, tinyfs::EntryType::FileData);
    assert_eq!(regular_file.min_event_time, None);
    assert_eq!(regular_file.max_event_time, None);
    assert_eq!(regular_file.extended_attributes, None);
    assert!(!regular_file.is_series_file());
    assert_eq!(regular_file.temporal_range(), None);
}

#[test]
fn test_directory_entries_have_no_temporal_metadata() {
    // Test that directory entries have None for temporal fields
    let directory = OplogEntry::new_inline(
        "partition".to_string(),
        "node".to_string(),
        tinyfs::EntryType::Directory,
        Utc::now().timestamp_micros(),
        1,
        b"directory content".to_vec(),
    );
    
    assert_eq!(directory.file_type, tinyfs::EntryType::Directory);
    assert_eq!(directory.min_event_time, None);
    assert_eq!(directory.max_event_time, None);
    assert_eq!(directory.extended_attributes, None);
    assert!(!directory.is_series_file());
}

#[test] 
fn test_duckpond_constants() {
    // Test that the constant is properly defined
    assert_eq!(duckpond::TIMESTAMP_COLUMN, "duckpond.timestamp_column");
}

#[test]
fn test_extract_temporal_range_missing_column() {
    // Test error handling when timestamp column is missing
    let values = StringArray::from(vec!["a", "b", "c"]);
    let schema = Schema::new(vec![
        Field::new("data", DataType::Utf8, false),
    ]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(values)]
    ).expect("Failed to create RecordBatch");
    
    let result = extract_temporal_range_from_batch(&batch, "nonexistent_column");
    assert!(result.is_err());
}

#[test]
fn test_extract_temporal_range_unsupported_type() {
    // Test error handling with unsupported column type
    let values = StringArray::from(vec!["not", "a", "timestamp"]);
    let schema = Schema::new(vec![
        Field::new("text_column", DataType::Utf8, false),
    ]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(values)]
    ).expect("Failed to create RecordBatch");
    
    let result = extract_temporal_range_from_batch(&batch, "text_column");
    assert!(result.is_err());
}
