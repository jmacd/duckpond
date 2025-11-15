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
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Get full plan from inner (no projection, no filters since column names don't match)
        let plan = self.inner.scan(state, None, &[], limit).await?;
        
        // Wrap the execution plan to rename columns in output batches
        // Pass projection info so ScopePrefixExec can apply it after renaming
        Ok(Arc::new(ScopePrefixExec::new(
            plan,
            self.schema.clone(),
            self.scope.clone(),
            self.time_column.clone(),
            self.inner.schema(),
            projection.cloned(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Disable filter pushdown since expressions reference renamed columns
        Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
    }
}

/// Execution plan that renames columns in output batches
struct ScopePrefixExec {
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    scope: String,
    time_column: String,
    original_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: datafusion::physical_plan::PlanProperties,
}

impl ScopePrefixExec {
    fn new(
        inner: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        scope: String,
        time_column: String,
        original_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        // Calculate the projected schema if projection is specified
        let final_schema = if let Some(ref proj_indices) = projection {
            let projected_fields: Vec<_> = proj_indices
                .iter()
                .map(|&i| output_schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(projected_fields))
        } else {
            output_schema.clone()
        };
        
        // Create PlanProperties with our transformed schema
        let properties = Self::compute_properties(&final_schema, &inner);
        
        Self {
            inner,
            output_schema: final_schema,
            scope,
            time_column,
            original_schema,
            projection,
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
            self.original_schema.clone(),
            self.projection.clone(),
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
            self.projection.clone(),
        )))
    }
}

/// Stream that renames columns in record batches and applies projection
struct ScopePrefixStream {
    inner: datafusion::physical_plan::SendableRecordBatchStream,
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl ScopePrefixStream {
    fn new(
        inner: datafusion::physical_plan::SendableRecordBatchStream,
        output_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            inner,
            output_schema,
            projection,
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
                // Apply projection if specified
                let columns = if let Some(ref proj_indices) = self.projection {
                    proj_indices
                        .iter()
                        .map(|&i| batch.column(i).clone())
                        .collect()
                } else {
                    batch.columns().to_vec()
                };
                
                // Create a new batch with the renamed (and optionally projected) schema
                match RecordBatch::try_new(
                    self.output_schema.clone(),
                    columns,
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
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_scope_prefix_schema() {
        // Create a simple schema
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("temperature", DataType::Float64, true),
            Field::new("humidity", DataType::Float64, true),
        ]));

        // Create test data
        let batch = RecordBatch::try_new(
            original_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![20.0, 21.0, 22.0])),
                Arc::new(Float64Array::from(vec![50.0, 55.0, 60.0])),
            ],
        )
        .unwrap();

        // Create mem table
        let mem_table = MemTable::try_new(original_schema.clone(), vec![vec![batch]]).unwrap();

        // Wrap with scope prefix
        let scoped = ScopePrefixTableProvider::new(
            Arc::new(mem_table),
            "Vulink".to_string(),
            "timestamp".to_string(),
        )
        .unwrap();

        // Check schema transformation
        let schema = scoped.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "timestamp"); // Unchanged
        assert_eq!(schema.field(1).name(), "Vulink.temperature"); // Prefixed
        assert_eq!(schema.field(2).name(), "Vulink.humidity"); // Prefixed
    }

    #[tokio::test]
    async fn test_scope_prefix_execution() {
        // Create test data
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("temp", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            original_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();

        let mem_table = MemTable::try_new(original_schema.clone(), vec![vec![batch]]).unwrap();

        // Wrap with scope
        let scoped = Arc::new(
            ScopePrefixTableProvider::new(
                Arc::new(mem_table),
                "Site1".to_string(),
                "timestamp".to_string(),
            )
            .unwrap(),
        );

        // Execute scan
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = scoped.scan(&state, None, &[], None).await.unwrap();

        // Verify output schema
        let output_schema = plan.schema();
        assert_eq!(output_schema.field(0).name(), "timestamp");
        assert_eq!(output_schema.field(1).name(), "Site1.temp");
    }

    #[tokio::test]
    async fn test_scope_prefix_with_sql_query() {
        // This test mimics what SqlDerivedFile does: register a wrapped table and query it with SQL
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("temp", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            original_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();

        let mem_table = MemTable::try_new(original_schema.clone(), vec![vec![batch]]).unwrap();

        // Wrap with scope
        let scoped = Arc::new(
            ScopePrefixTableProvider::new(
                Arc::new(mem_table),
                "Site1".to_string(),
                "timestamp".to_string(),
            )
            .unwrap(),
        );

        // Register the wrapped table and query it with SQL
        let ctx = SessionContext::new();
        _ = ctx.register_table("input0", scoped);

        // Try a simple SELECT *
        let df = ctx.sql("SELECT * FROM input0").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "Site1.temp");
    }
}
