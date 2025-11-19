//! Scope Prefix Table Provider
//!
//! Wraps a TableProvider to add a scope prefix to all column names except the time column.
//! This enables dynamic column renaming without schema introspection at configuration time.
//!
//! Example: With scope "Vulink" and time_column "timestamp", transforms:
//!   - `timestamp` → `timestamp` (unchanged)
//!   - `WaterTemp` → `Vulink.WaterTemp`
//!   - `DO.mg/L` → `Vulink.DO.mg/L`

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType};
use datafusion::arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

/// Wraps a TableProvider to prefix column names with a scope
#[derive(Debug)]
pub struct ScopePrefixTableProvider {
    /// The underlying table provider
    inner: Arc<dyn TableProvider>,
    /// Scope prefix to prepend (e.g., "Vulink")
    scope: String,
    /// Time column to exclude from prefixing
    time_column: String,
    /// Cached prefixed schema
    schema: SchemaRef,
}

impl ScopePrefixTableProvider {
    /// Create a new scope prefix wrapper
    ///
    /// # Arguments
    /// * `inner` - The underlying table provider to wrap
    /// * `scope` - The scope prefix to add (e.g., "Vulink", "AT500_Surface")
    /// * `time_column` - The time column name to exclude from prefixing
    pub fn new(
        inner: Arc<dyn TableProvider>,
        scope: String,
        time_column: String,
    ) -> DataFusionResult<Self> {
        let original_schema = inner.schema();
        let prefixed_schema = Self::create_prefixed_schema(&original_schema, &scope, &time_column)?;
        
        Ok(Self {
            inner,
            scope,
            time_column,
            schema: Arc::new(prefixed_schema),
        })
    }

    /// Create a new schema with prefixed column names
    fn create_prefixed_schema(
        original: &Schema,
        scope: &str,
        time_column: &str,
    ) -> DataFusionResult<Schema> {
        let prefixed_fields: Vec<Field> = original
            .fields()
            .iter()
            .map(|field| {
                if field.name() == time_column {
                    // Keep time column unchanged
                    field.as_ref().clone()
                } else {
                    // Prefix other columns with scope
                    let new_name = format!("{}.{}", scope, field.name());
                    Field::new(
                        new_name,
                        field.data_type().clone(),
                        field.is_nullable(),
                    )
                }
            })
            .collect();

        Ok(Schema::new(prefixed_fields))
    }

    /// Get the underlying table provider
    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[async_trait]
impl TableProvider for ScopePrefixTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Rewrite filter expressions to use original column names
        let rewritten_filters: Vec<Expr> = filters
            .iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        
        // Pass projection and rewritten filters through to inner
        let plan = self.inner.scan(state, projection, &rewritten_filters, limit).await?;
        
        // Calculate output schema based on projection
        let output_schema = if let Some(proj) = projection {
            let projected_fields: Vec<_> = proj
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(projected_fields))
        } else {
            self.schema.clone()
        };
        
        // Wrap the execution plan to rename columns in output batches
        Ok(Arc::new(ScopePrefixExec::new(
            plan,
            output_schema,
            self.scope.clone(),
            self.time_column.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Rewrite filters to use original column names, then ask inner provider
        let rewritten: Vec<Expr> = filters.iter().map(|f| self.rewrite_expr(f)).collect();
        let rewritten_refs: Vec<&Expr> = rewritten.iter().collect();
        
        // Delegate to inner provider - it knows best whether it can handle these filters
        self.inner.supports_filters_pushdown(&rewritten_refs)
    }
}

impl ScopePrefixTableProvider {
    /// Rewrite an expression to use original column names instead of prefixed names
    fn rewrite_expr(&self, expr: &Expr) -> Expr {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        use datafusion::common::Column;
        
        let prefix = format!("{}.", self.scope);
        
        expr.clone()
            .transform(|e| {
                Ok(if let Expr::Column(col) = &e {
                    // Strip scope prefix from column names
                    if let Some(original_name) = col.name.strip_prefix(&prefix) {
                        Transformed::yes(Expr::Column(Column::new(
                            col.relation.clone(),
                            original_name.to_string(),
                        )))
                    } else {
                        Transformed::no(e)
                    }
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .expect("Column name rewriting is infallible")
    }
}

/// Execution plan that renames columns in output batches
struct ScopePrefixExec {
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    scope: String,
    time_column: String,
    properties: datafusion::physical_plan::PlanProperties,
}

impl ScopePrefixExec {
    fn new(
        inner: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        scope: String,
        time_column: String,
    ) -> Self {
        let properties = Self::compute_properties(&output_schema, &inner);
        
        Self {
            inner,
            output_schema,
            scope,
            time_column,
            properties,
        }
    }
    
    fn compute_properties(
        schema: &SchemaRef,
        inner: &Arc<dyn ExecutionPlan>,
    ) -> datafusion::physical_plan::PlanProperties {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::PlanProperties;
        
        let inner_props = inner.properties();
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            inner_props.output_partitioning().clone(),
            inner_props.emission_type,
            inner_props.boundedness,
        )
    }
}

impl std::fmt::Debug for ScopePrefixExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScopePrefixExec(scope={})", self.scope)
    }
}

impl std::fmt::Display for ScopePrefixExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScopePrefixExec: scope={}", self.scope)
    }
}

impl DisplayAs for ScopePrefixExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScopePrefixExec(scope={})", self.scope)
    }
}

impl ExecutionPlan for ScopePrefixExec {
    fn name(&self) -> &str {
        "ScopePrefixExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ScopePrefixExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(ScopePrefixExec::new(
            children[0].clone(),
            self.output_schema.clone(),
            self.scope.clone(),
            self.time_column.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<datafusion::physical_plan::SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        
        Ok(Box::pin(ScopePrefixStream::new(
            inner_stream,
            self.output_schema.clone(),
        )))
    }
}

/// Stream that renames columns in record batches
struct ScopePrefixStream {
    inner: datafusion::physical_plan::SendableRecordBatchStream,
    output_schema: SchemaRef,
}

impl ScopePrefixStream {
    fn new(
        inner: datafusion::physical_plan::SendableRecordBatchStream,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            inner,
            output_schema,
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for ScopePrefixStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl futures::Stream for ScopePrefixStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt;
        
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                // Just rename columns - projection already handled by inner
                match RecordBatch::try_new(
                    self.output_schema.clone(),
                    batch.columns().to_vec(),
                ) {
                    Ok(new_batch) => std::task::Poll::Ready(Some(Ok(new_batch))),
                    Err(e) => std::task::Poll::Ready(Some(Err(DataFusionError::ArrowError(
                        Box::new(e),
                        None,
                    )))),
                }
            }
            std::task::Poll::Ready(Some(Err(e))) => std::task::Poll::Ready(Some(Err(e))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
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
        let scoped = ScopePrefixTableProvider::new(
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
        let scoped = Arc::new(ScopePrefixTableProvider::new(
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
        let timestamp_col = results[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(timestamp_col.value(0), 1);
        assert_eq!(timestamp_col.value(4), 5);

        let temp_col = results[0].column(1).as_any().downcast_ref::<Float64Array>().unwrap();
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
        ctx.register_parquet("inner_table", parquet_path.to_str().unwrap(), Default::default())
            .await?;
        
        // Get the parquet table provider
        let parquet_provider = ctx.table_provider("inner_table").await?;

        let scoped = Arc::new(ScopePrefixTableProvider::new(
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
        let scoped = ScopePrefixTableProvider::new(
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
        let scoped = Arc::new(ScopePrefixTableProvider::new(
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
        let scoped = Arc::new(ScopePrefixTableProvider::new(
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
        let count_col = results[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(count_col.value(1), 20);

        Ok(())
    }
}
