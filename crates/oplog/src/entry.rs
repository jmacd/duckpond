use crate::delta::ForArrow;
use arrow::datatypes::{SchemaRef};
use std::sync::Arc;

// DataFusion imports for table providers
use arrow_array::{Array, RecordBatch};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::Boundedness, execution_plan::EmissionType, stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;
use std::any::Any;
use super::tinylogfs::OplogEntry;

/// Table provider for OplogEntry records
/// This enables SQL queries over filesystem operations stored in Delta Lake
#[derive(Debug, Clone)]
pub struct OplogEntryTable {
    schema: SchemaRef,
    table_path: String,
}

impl OplogEntryTable {
    pub fn new(table_path: String) -> Self {
        // Use OplogEntry schema since that's what we want to expose via SQL
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        Self { schema, table_path }
    }
}

#[async_trait]
impl TableProvider for OplogEntryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Apply projection to schema if provided
        let projected_schema = match projection {
            Some(indices) => {
                let projected_fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        };

        // Use a custom OplogEntry execution plan with projected schema
        Ok(Arc::new(OplogEntryExec::new(
            self.table_path.clone(),
            projected_schema,
            projection.cloned(),
        )))
    }
}

/// Execution plan that reads OplogEntry records from Delta Lake Record.content field
#[derive(Debug)]
pub struct OplogEntryExec {
    table_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl DisplayAs for OplogEntryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "OplogEntryExec: {}", self.table_path)
            }
        }
    }
}

impl OplogEntryExec {
    pub fn new(table_path: String, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Self {
            table_path,
            schema,
            projection,
            properties,
        }
    }

    /// Extract OplogEntry records from Record batch content field
    fn extract_oplog_entries(batch: RecordBatch) -> Result<Vec<Result<RecordBatch>>> {
        batch
            .column_by_name("content")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>())
            .map(Self::process_content_array)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Process content binary array to extract OplogEntry records
    fn process_content_array(
        binary_array: &arrow_array::BinaryArray,
    ) -> Result<Vec<Result<RecordBatch>>> {
        Ok((0..binary_array.len())
            .filter_map(|i| binary_array.value(i).get(0..))
            .map(Self::deserialize_oplog_entry_bytes)
            .collect())
    }

    /// Deserialize OplogEntry IPC bytes to record batches
    fn deserialize_oplog_entry_bytes(bytes: &[u8]) -> Result<RecordBatch> {
        use arrow::ipc::reader::StreamReader;

        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))?;

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "No OplogEntry batches found in IPC stream".to_string(),
                )
            })
    }
}

impl ExecutionPlan for OplogEntryExec {
    fn name(&self) -> &'static str {
        "OplogEntryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table_path = self.table_path.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();

        let stream = async_stream::stream! {
            // Load Delta Lake records and extract OplogEntry data
            match Self::load_delta_stream(&table_path).await {
                Ok(mut delta_stream) => {
                    while let Some(batch_result) = delta_stream.next().await {
                        let results = batch_result
                            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
                            .and_then(Self::extract_oplog_entries);

                        match results {
                            Ok(oplog_batches) => {
                                for batch in oplog_batches {
                                    match batch {
                                        Ok(record_batch) => {
                                            // Apply projection if specified
                                            if let Some(ref proj_indices) = projection {
                                                match Self::apply_projection(record_batch, proj_indices) {
                                                    Ok(projected_batch) => yield Ok(projected_batch),
                                                    Err(e) => yield Err(e),
                                                }
                                            } else {
                                                yield Ok(record_batch);
                                            }
                                        }
                                        Err(e) => yield Err(e),
                                    }
                                }
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                }
                Err(e) => yield Err(e),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl OplogEntryExec {
    async fn load_delta_stream(table_path: &str) -> Result<SendableRecordBatchStream> {
        use deltalake::DeltaOps;

        let delta_ops = DeltaOps::try_from_uri(table_path)
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let (_table, stream) = delta_ops
            .load()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        Ok(stream)
    }

    fn apply_projection(batch: RecordBatch, projection: &[usize]) -> Result<RecordBatch> {
        let projected_columns: Vec<_> = projection
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        
        let projected_fields: Vec<_> = projection
            .iter()
            .map(|&i| batch.schema().field(i).clone())
            .collect();
        
        let projected_schema = Arc::new(arrow::datatypes::Schema::new(projected_fields));
        
        RecordBatch::try_new(projected_schema, projected_columns)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(e, None))
    }
}

