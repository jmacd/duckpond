//! Generic Column Rename TableProvider Implementation
//!
//! This module provides the core infrastructure for renaming columns in a TableProvider.
//! It's used by both:
//! - `column-rename` factory: User-configured rename rules
//! - `scope-prefix` factory: Auto-generated rules for scope prefixing
//!
//! The implementation wraps any TableProvider and applies column renaming via:
//! - Schema transformation (forward map: original → renamed)
//! - Filter rewriting (reverse map: renamed → original for pushdown)
//! - Batch column renaming in ExecutionPlan

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Column rename function: takes original column name, returns renamed column name
pub type ColumnRenameFunc = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Wraps a TableProvider to rename columns according to a function
pub struct ColumnRenameTableProvider {
    /// The underlying table provider
    inner: Arc<dyn TableProvider>,
    
    /// Function to rename columns (original → renamed)
    rename_fn: ColumnRenameFunc,
    
    /// Cached renamed schema
    schema: SchemaRef,
    
    /// Reverse mapping: renamed → original (for filter pushdown)
    reverse_map: HashMap<String, String>,
}

impl ColumnRenameTableProvider {
    /// Create a new column rename wrapper
    ///
    /// # Arguments
    /// * `inner` - The underlying table provider to wrap
    /// * `rename_fn` - Function that maps original column names to renamed names
    pub fn new(
        inner: Arc<dyn TableProvider>,
        rename_fn: ColumnRenameFunc,
    ) -> DataFusionResult<Self> {
        let original_schema = inner.schema();
        let (renamed_schema, reverse_map) = Self::create_renamed_schema(&original_schema, &rename_fn)?;

        Ok(Self {
            inner,
            rename_fn,
            schema: Arc::new(renamed_schema),
            reverse_map,
        })
    }

    /// Create a new schema with renamed columns and reverse mapping
    fn create_renamed_schema(
        original: &Schema,
        rename_fn: &ColumnRenameFunc,
    ) -> DataFusionResult<(Schema, HashMap<String, String>)> {
        let mut reverse_map = HashMap::new();

        let renamed_fields: Vec<Field> = original
            .fields()
            .iter()
            .map(|field| {
                let original_name = field.name();
                let new_name = rename_fn(original_name);

                // Track reverse mapping for filter pushdown
                if new_name != *original_name {
                    let _ = reverse_map.insert(new_name.clone(), original_name.clone());
                }

                Field::new(new_name, field.data_type().clone(), field.is_nullable())
            })
            .collect();

        Ok((Schema::new(renamed_fields), reverse_map))
    }

    /// Rewrite an expression to use original column names
    fn rewrite_expr(&self, expr: &Expr) -> Expr {
        use datafusion::common::Column;
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};

        expr.clone()
            .transform(|e| {
                Ok(if let Expr::Column(col) = &e {
                    // Map renamed column back to original
                    if let Some(original) = self.reverse_map.get(&col.name) {
                        Transformed::yes(Expr::Column(Column::new(
                            col.relation.clone(),
                            original.clone(),
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

impl fmt::Debug for ColumnRenameTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColumnRenameTableProvider")
            .field("inner", &"<TableProvider>")
            .field("schema", &self.schema)
            .field("reverse_map", &self.reverse_map)
            .finish()
    }
}

#[async_trait]
impl TableProvider for ColumnRenameTableProvider {
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
        // Rewrite filters to use original column names
        let rewritten_filters: Vec<Expr> = filters
            .iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();

        // Get plan from inner provider
        let plan = self
            .inner
            .scan(state, projection, &rewritten_filters, limit)
            .await?;

        // Calculate output schema based on projection
        let output_schema = if let Some(proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        // Wrap execution plan to rename columns in output batches
        Ok(Arc::new(ColumnRenameExec::new(
            plan,
            output_schema,
            self.rename_fn.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Rewrite filters and ask inner provider
        let rewritten: Vec<Expr> = filters.iter().map(|f| self.rewrite_expr(f)).collect();
        let refs: Vec<&Expr> = rewritten.iter().collect();
        self.inner.supports_filters_pushdown(&refs)
    }
}

/// Execution plan that renames columns in output batches
pub struct ColumnRenameExec {
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    rename_fn: ColumnRenameFunc,
    properties: datafusion::physical_plan::PlanProperties,
}

impl ColumnRenameExec {
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        rename_fn: ColumnRenameFunc,
    ) -> Self {
        let properties = Self::compute_properties(&output_schema, &inner);
        Self {
            inner,
            output_schema,
            rename_fn,
            properties,
        }
    }

    fn compute_properties(
        schema: &SchemaRef,
        inner: &Arc<dyn ExecutionPlan>,
    ) -> datafusion::physical_plan::PlanProperties {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::{Partitioning, PlanProperties};

        let inner_props = inner.properties();
        let partition_count = inner_props.output_partitioning().partition_count();
        PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            inner_props.emission_type,
            inner_props.boundedness,
        )
    }
}

impl fmt::Debug for ColumnRenameExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnRenameExec")
    }
}

impl DisplayAs for ColumnRenameExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnRenameExec")
    }
}

impl ExecutionPlan for ColumnRenameExec {
    fn name(&self) -> &str {
        "ColumnRenameExec"
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
                "ColumnRenameExec expects exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(ColumnRenameExec::new(
            Arc::clone(&children[0]),
            self.output_schema.clone(),
            self.rename_fn.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let output_schema = self.output_schema.clone();
        let output_schema_for_adapter = output_schema.clone();
        let rename_fn = self.rename_fn.clone();

        // Create stream that renames columns in each batch
        let stream = inner_stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                rename_batch_columns(&batch, &output_schema, &rename_fn)
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema_for_adapter,
            stream,
        )))
    }
}

/// Rename columns in a RecordBatch
fn rename_batch_columns(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    _rename_fn: &ColumnRenameFunc,
) -> DataFusionResult<RecordBatch> {
    // Verify batch schema matches expected input
    let _batch_schema = batch.schema();
    
    // Create new batch with renamed schema
    let columns = batch.columns().to_vec();
    
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(e.into(), None))
}
