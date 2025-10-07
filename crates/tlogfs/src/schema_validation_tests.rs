//! Tests for temporal SQL patterns that ensure non-nullable time        let df = ctx.sql(sql).await.unwrap();
        let schema = df.schema();
        
        let timestamp_field = schema.field_with_name(None, \"timestamp\").unwrap();
        println!(\"✅ COALESCE FULL OUTER JOIN: timestamp nullable={}\", timestamp_field.is_nullable());s
//!
//! This module tests various SQL patterns for combining time series data
//! to verify which approaches produce non-nullable timestamp columns.

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::*;
    use std::sync::Arc;

    /// Helper to create a test session context with sample data
    async fn create_test_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        
        // Create two sample tables with overlapping timestamps
        use arrow::array::{Float64Array, TimestampSecondArray};
        use arrow::record_batch::RecordBatch;
        
        // Table 1: timestamps 1, 2, 3
        let batch1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("temp", DataType::Float64, true),
            ])),
            vec![
                Arc::new(TimestampSecondArray::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![Some(20.0), Some(21.0), Some(22.0)])),
            ],
        )?;
        
        ctx.register_batch("table1", batch1)?;
        
        // Table 2: timestamps 3, 4, 5
        let batch2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("humidity", DataType::Float64, true),
            ])),
            vec![
                Arc::new(TimestampSecondArray::from(vec![3, 4, 5])),
                Arc::new(Float64Array::from(vec![Some(60.0), Some(65.0), Some(70.0)])),
            ],
        )?;
        
        ctx.register_batch("table2", batch2)?;
        
        Ok(ctx)
    }

    #[tokio::test]
    async fn test_coalesce_full_outer_join_nullability() {
        let ctx = create_test_context().await.unwrap();
        
        // Test COALESCE approach (our current fix)
        let sql = r#"
            SELECT 
                COALESCE(table1.timestamp, table2.timestamp) AS timestamp,
                table1.temp,
                table2.humidity
            FROM table1 
            FULL OUTER JOIN table2 ON table1.timestamp = table2.timestamp
            ORDER BY timestamp
        "#;
        
        let df = ctx.sql(sql).await.unwrap();
        let schema = df.schema();
        
        let timestamp_field = schema.field_with_name("timestamp").unwrap();
        println!("✅ COALESCE FULL OUTER JOIN: timestamp nullable={}", timestamp_field.is_nullable());
        
        // COALESCE should produce non-nullable timestamp
        assert!(!timestamp_field.is_nullable(), 
                "COALESCE should produce non-nullable timestamp");
    }

    #[tokio::test]
    async fn test_select_distinct_then_join_nullability() {
        let ctx = create_test_context().await.unwrap();
        
        // Test SELECT DISTINCT approach (user's suggestion)
        let sql = r#"
            WITH all_timestamps AS (
                SELECT DISTINCT timestamp FROM table1
                UNION
                SELECT DISTINCT timestamp FROM table2
            )
            SELECT 
                t.timestamp,
                table1.temp,
                table2.humidity
            FROM all_timestamps t
            LEFT JOIN table1 ON t.timestamp = table1.timestamp
            LEFT JOIN table2 ON t.timestamp = table2.timestamp
            ORDER BY t.timestamp
        "#;
        
        let df = ctx.sql(sql).await.unwrap();
        let schema = df.schema();
        
        let timestamp_field = schema.field_with_name("timestamp").unwrap();
        println!("🔍 SELECT DISTINCT + UNION + LEFT JOIN: timestamp nullable={}", timestamp_field.is_nullable());
        
        // This approach should also produce non-nullable timestamp
        // because the CTE selects from non-nullable columns and UNION preserves non-nullability
        assert!(!timestamp_field.is_nullable(), 
                "SELECT DISTINCT with UNION should produce non-nullable timestamp");
    }

    #[tokio::test]
    async fn test_natural_full_outer_join_nullability() {
        let ctx = create_test_context().await.unwrap();
        
        // Test NATURAL FULL OUTER JOIN (the problematic original approach)
        let sql = r#"
            SELECT * FROM table1 
            NATURAL FULL OUTER JOIN table2
            ORDER BY timestamp
        "#;
        
        let df = ctx.sql(sql).await.unwrap();
        let schema = df.schema();
        
        let timestamp_field = schema.field_with_name("timestamp").unwrap();
        println!("❌ NATURAL FULL OUTER JOIN: timestamp nullable={}", timestamp_field.is_nullable());
        
        // NATURAL FULL OUTER JOIN may produce nullable timestamp (this was our bug)
        // We expect this to fail validation
        if timestamp_field.is_nullable() {
            println!("✅ Confirmed: NATURAL FULL OUTER JOIN produces nullable timestamp");
        }
    }

    #[tokio::test]
    async fn test_date_trunc_with_coalesce_nullability() {
        let ctx = create_test_context().await.unwrap();
        
        // Test DATE_TRUNC with COALESCE (temporal-reduce pattern)
        let sql = r#"
            WITH time_buckets AS (
                SELECT 
                    DATE_TRUNC('second', timestamp) AS time_bucket,
                    AVG(temp) AS avg_temp
                FROM table1
                WHERE timestamp IS NOT NULL
                GROUP BY DATE_TRUNC('second', timestamp)
            )
            SELECT 
                COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS timestamp,
                avg_temp
            FROM time_buckets
            ORDER BY time_bucket
        "#;
        
        let df = ctx.sql(sql).await.unwrap();
        let schema = df.schema();
        
        let timestamp_field = schema.field_with_name("timestamp").unwrap();
        println!("🔍 DATE_TRUNC + COALESCE: timestamp nullable={}", timestamp_field.is_nullable());
        
        // COALESCE should force non-nullable
        assert!(!timestamp_field.is_nullable(), 
                "DATE_TRUNC with COALESCE should produce non-nullable timestamp");
    }

    #[tokio::test]
    async fn test_date_trunc_without_coalesce_nullability() {
        let ctx = create_test_context().await.unwrap();
        
        // Test DATE_TRUNC without COALESCE (problematic pattern)
        let sql = r#"
            WITH time_buckets AS (
                SELECT 
                    DATE_TRUNC('second', timestamp) AS time_bucket,
                    AVG(temp) AS avg_temp
                FROM table1
                WHERE timestamp IS NOT NULL
                GROUP BY DATE_TRUNC('second', timestamp)
            )
            SELECT 
                CAST(time_bucket AS TIMESTAMP) AS timestamp,
                avg_temp
            FROM time_buckets
            ORDER BY time_bucket
        "#;
        
        let df = ctx.sql(sql).await.unwrap();
        let schema = df.schema();
        
        let timestamp_field = schema.field_with_name("timestamp").unwrap();
        println!("❌ DATE_TRUNC + CAST (no COALESCE): timestamp nullable={}", timestamp_field.is_nullable());
        
        // Without COALESCE, the timestamp may be nullable
        if timestamp_field.is_nullable() {
            println!("✅ Confirmed: DATE_TRUNC + CAST without COALESCE produces nullable timestamp");
        }
    }
}
