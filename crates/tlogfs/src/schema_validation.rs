// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Schema validation utilities for TLogFS
//!
//! This module provides common schema validation functions used across
//! the codebase to ensure data quality and fail-fast behavior.

use arrow::datatypes::Schema;
use std::sync::Arc;

/// Validate that a FileSeries schema has a non-nullable timestamp column
///
/// FileSeries files MUST have non-nullable timestamp columns for temporal partitioning.
/// Nullable timestamps would create invalid year=0/month=0 partitions in DataFusion's
/// temporal partitioning system.
///
/// # Arguments
/// * `schema` - The Arrow schema to validate
/// * `context` - Context string for error messages (e.g., file path)
///
/// # Returns
/// * `Ok(())` if schema is valid
/// * `Err(String)` with detailed error message if validation fails
pub fn validate_fileseries_timestamp(schema: &Arc<Schema>, context: &str) -> Result<(), String> {
    log::debug!(
        "[SEARCH] SCHEMA VALIDATION for '{}': Schema has {} fields",
        context,
        schema.fields().len()
    );

    // Log all field information for debugging
    for (i, field) in schema.fields().iter().enumerate() {
        log::debug!(
            "[SEARCH]   Field {}: name='{}', data_type={:?}, nullable={}",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    // Check for timestamp field
    match schema.field_with_name("timestamp") {
        Ok(timestamp_field) => {
            log::debug!(
                "[SEARCH] TIMESTAMP FIELD: name='{}', data_type={:?}, nullable={}",
                timestamp_field.name(),
                timestamp_field.data_type(),
                timestamp_field.is_nullable()
            );

            if timestamp_field.is_nullable() {
                log::error!("[ERR] NULLABLE TIMESTAMP DETECTED in '{}'", context);
                return Err(format!(
                    "FileSeries schema violation in '{}': timestamp column is nullable. \
                    FileSeries must have non-nullable timestamp columns for temporal partitioning. \
                    Nullable timestamps would create invalid year=0/month=0 partitions. \
                    This indicates a data pipeline bug in SQL queries (check for NATURAL FULL OUTER JOIN usage, \
                    use explicit COALESCE for timestamp columns in joins).",
                    context
                ));
            }

            log::debug!("[OK] Timestamp column is non-nullable - schema validation passed");
            Ok(())
        }
        Err(_) => {
            log::error!("[ERR] NO TIMESTAMP COLUMN found in '{}'", context);
            Err(format!(
                "FileSeries schema error in '{}': no timestamp column found",
                context
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};

    #[test]
    fn test_valid_non_nullable_timestamp() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("temperature", DataType::Float64, true),
            Field::new("humidity", DataType::Float64, true),
        ]));

        let result = validate_fileseries_timestamp(&schema, "/test/sensor.series");
        assert!(
            result.is_ok(),
            "Non-nullable timestamp should pass validation"
        );
    }

    #[test]
    fn test_invalid_nullable_timestamp() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ), // nullable=true
            Field::new("temperature", DataType::Float64, true),
        ]));

        let result = validate_fileseries_timestamp(&schema, "/test/sensor.series");
        assert!(result.is_err(), "Nullable timestamp should fail validation");

        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("nullable"),
            "Error should mention nullable"
        );
        assert!(
            error_msg.contains("year=0"),
            "Error should mention year=0 partitions"
        );
    }

    #[test]
    fn test_missing_timestamp_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Timestamp(TimeUnit::Second, None), false), // wrong name
            Field::new("temperature", DataType::Float64, true),
        ]));

        let result = validate_fileseries_timestamp(&schema, "/test/sensor.series");
        assert!(
            result.is_err(),
            "Missing timestamp column should fail validation"
        );

        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("no timestamp column found"),
            "Error should mention missing timestamp"
        );
    }

    #[test]
    fn test_timestamp_with_different_time_units() {
        // Test various timestamp types - all should work if non-nullable
        let time_units = vec![
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];

        for unit in time_units {
            let schema = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(unit, None), false),
                Field::new("value", DataType::Float64, true),
            ]));

            let result = validate_fileseries_timestamp(&schema, "/test/sensor.series");
            assert!(
                result.is_ok(),
                "Timestamp with {:?} should pass validation",
                unit
            );
        }
    }

    #[test]
    fn test_timestamp_with_timezone() {
        // Timestamp with timezone should also work if non-nullable
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));

        let result = validate_fileseries_timestamp(&schema, "/test/sensor.series");
        assert!(
            result.is_ok(),
            "Timestamp with timezone should pass validation"
        );
    }
}
