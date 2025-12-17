//! Scope Prefix Table Provider
//!
//! Factory function to create a TableProvider that adds a scope prefix to all column names
//! except the time column. This enables dynamic column renaming without schema introspection
//! at configuration time.
//!
//! Example: With scope "Vulink" and time_column "timestamp", transforms:
//!   - `timestamp` → `timestamp` (unchanged)
//!   - `WaterTemp` → `Vulink.WaterTemp`
//!   - `DO.mg/L` → `Vulink.DO.mg/L`
//!
//! This is implemented using ColumnRenameTableProvider with a scope-specific rename function.

use crate::transform::column_rename::ColumnRenameTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::error::Result as DataFusionResult;
use std::sync::Arc;

/// Create a TableProvider that prefixes column names with a scope
///
/// # Arguments
/// * `inner` - The underlying table provider to wrap
/// * `scope` - The scope prefix to add (e.g., "Vulink", "AT500_Surface")
/// * `time_column` - The time column name to exclude from prefixing
///
/// # Returns
/// A ColumnRenameTableProvider configured to prefix all columns except the time column
pub fn scope_prefix_table_provider(
    inner: Arc<dyn TableProvider>,
    scope: String,
    time_column: String,
) -> DataFusionResult<ColumnRenameTableProvider> {
    // Create rename function that prefixes all columns except time_column
    let rename_fn = Arc::new(move |col_name: &str| {
        if col_name == time_column.as_str() {
            col_name.to_string()
        } else {
            format!("{}.{}", scope, col_name)
        }
    });

    ColumnRenameTableProvider::new(inner, rename_fn, std::collections::HashMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use arrow_array::record_batch;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_schema_transformation() -> DataFusionResult<()> {
        let batch = record_batch!(
            ("timestamp", Int64, [1, 2, 3]),
            ("temperature", Float64, [20.0, 21.0, 22.0]),
            ("humidity", Float64, [50.0, 55.0, 60.0])
        )?;

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let scoped = scope_prefix_table_provider(
            Arc::new(table),
            "Vulink".to_string(),
            "timestamp".to_string(),
        )?;

        let schema = scoped.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "timestamp"); // Unchanged
        assert_eq!(schema.field(1).name(), "Vulink.temperature"); // Prefixed
        assert_eq!(schema.field(2).name(), "Vulink.humidity"); // Prefixed

        Ok(())
    }

    #[tokio::test]
    async fn test_data_integrity() -> DataFusionResult<()> {
        let batch = record_batch!(
            ("timestamp", Int64, [1, 2, 3, 4, 5]),
            ("temp", Float64, [10.0, 20.0, 30.0, 40.0, 50.0])
        )?;

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let scoped = Arc::new(scope_prefix_table_provider(
            Arc::new(table),
            "Site1".to_string(),
            "timestamp".to_string(),
        )?);

        let ctx = SessionContext::new();
        _ = ctx.register_table("data", scoped)?;

        let df = ctx.sql("SELECT * FROM data").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        assert_eq!(results[0].schema().field(0).name(), "timestamp");
        assert_eq!(results[0].schema().field(1).name(), "Site1.temp");

        // Verify data values
        let timestamp_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamp_col.value(0), 1);
        assert_eq!(timestamp_col.value(4), 5);

        let temp_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(temp_col.value(0), 10.0);
        assert_eq!(temp_col.value(4), 50.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_with_where_clause() -> DataFusionResult<()> {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::prelude::SessionConfig;
        use std::fs::File;
        use tempfile::TempDir;

        let batch = record_batch!(
            ("timestamp", Int64, [1, 2, 3, 4]),
            ("temp", Float64, [20.5, 21.0, 22.5, 23.0])
        )?;

        // Create a temporary directory and parquet file
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("test.parquet");

        // Write to parquet file
        let file = File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();

        // Register parquet file as table with filter pushdown enabled
        let mut cfg = SessionConfig::default();
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        cfg.options_mut().execution.parquet.reorder_filters = true;
        let ctx = SessionContext::new_with_config(cfg);
        ctx.register_parquet(
            "inner_table",
            parquet_path.to_str().unwrap(),
            Default::default(),
        )
        .await?;

        // Get the parquet table provider
        let parquet_provider = ctx.table_provider("inner_table").await?;

        let scoped = Arc::new(scope_prefix_table_provider(
            parquet_provider,
            "Station".to_string(),
            "timestamp".to_string(),
        )?);

        _ = ctx.register_table("station_data", scoped)?;

        let df = ctx
            .sql("SELECT timestamp, \"Station.temp\" FROM station_data WHERE timestamp > 1")
            .await?;
        let results = df.collect().await?;

        assert_eq!(results[0].num_rows(), 3); // Rows where timestamp > 1
        assert_eq!(results[0].schema().field(1).name(), "Station.temp");

        Ok(())
    }

    #[tokio::test]
    async fn test_dots_in_column_names() -> DataFusionResult<()> {
        let batch = record_batch!(
            ("timestamp", Int64, [1, 2]),
            ("DO.mg/L", Float64, [8.5, 9.0]),
            ("Temperature.C", Float64, [20.0, 21.0])
        )?;

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let scoped = scope_prefix_table_provider(
            Arc::new(table),
            "Sensor".to_string(),
            "timestamp".to_string(),
        )?;

        let schema = scoped.schema();
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "Sensor.DO.mg/L");
        assert_eq!(schema.field(2).name(), "Sensor.Temperature.C");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_batches() -> DataFusionResult<()> {
        let batch1 = record_batch!(
            ("timestamp", Int64, [1, 2]),
            ("metric", Float64, [10.0, 20.0])
        )?;

        let batch2 = record_batch!(
            ("timestamp", Int64, [3, 4]),
            ("metric", Float64, [30.0, 40.0])
        )?;

        let schema = batch1.schema();
        let table = MemTable::try_new(schema, vec![vec![batch1, batch2]])?;
        let scoped = Arc::new(scope_prefix_table_provider(
            Arc::new(table),
            "Multi".to_string(),
            "timestamp".to_string(),
        )?);

        let ctx = SessionContext::new();
        _ = ctx.register_table("multi_batch", scoped)?;

        let df = ctx.sql("SELECT * FROM multi_batch").await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);

        for batch in &results {
            assert_eq!(batch.schema().field(0).name(), "timestamp");
            assert_eq!(batch.schema().field(1).name(), "Multi.metric");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_integer_types() -> DataFusionResult<()> {
        let batch = record_batch!(
            ("timestamp", Int64, [1, 2, 3]),
            ("count", Int32, [10, 20, 30]),
            ("value", Float64, [1.5, 2.5, 3.5])
        )?;

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let scoped = Arc::new(scope_prefix_table_provider(
            Arc::new(table),
            "Mixed".to_string(),
            "timestamp".to_string(),
        )?);

        let ctx = SessionContext::new();
        _ = ctx.register_table("data", scoped)?;

        let df = ctx.sql("SELECT * FROM data").await?;
        let results = df.collect().await?;

        assert_eq!(results[0].schema().field(0).name(), "timestamp");
        assert_eq!(results[0].schema().field(1).name(), "Mixed.count");
        assert_eq!(results[0].schema().field(2).name(), "Mixed.value");

        // Verify Int32 data
        let count_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(count_col.value(1), 20);

        Ok(())
    }
}
