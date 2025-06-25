use arrow::datatypes::{SchemaRef};
use arrow_array::{Array, RecordBatch};
use std::any::Any;

use datafusion::catalog::{Session, TableProvider};

use deltalake::DeltaOps;
use crate::tinylogfs::delta_manager::DeltaTableManager;

use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::Boundedness, execution_plan::EmissionType, stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;

/// A custom table that reads byte arrays from Delta Lake content field
/// and converts them to RecordBatches using Arrow IPC
#[derive(Debug, Clone)]
pub struct ContentTable {
    schema: SchemaRef,
    table_path: String,
    delta_manager: DeltaTableManager,
}

impl ContentTable {
    pub fn new(schema: SchemaRef, table_path: String, delta_manager: DeltaTableManager) -> Self {
        Self { 
            schema, 
            table_path,
            delta_manager,
        }
    }
}

#[async_trait]
impl TableProvider for ContentTable {
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ContentExec::new(self.clone())))
    }
}

/// Execution plan that reads from byte stream and converts to RecordBatches
pub struct ContentExec {
    table: ContentTable,
    properties: PlanProperties,
}

impl ContentExec {
    pub fn new(table: ContentTable) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(table.schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Self { table, properties }
    }
}

impl std::fmt::Debug for ContentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentExec")
            .field("schema", &self.table.schema)
            .finish()
    }
}

impl DisplayAs for ContentExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "ContentExec")
            }
        }
    }
}

impl ExecutionPlan for ContentExec {
    fn name(&self) -> &'static str {
        "ContentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
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
        let table_path = self.table.table_path.clone();
        let schema = self.table.schema.clone();
        let delta_manager = self.table.delta_manager.clone();

        let stream = async_stream::stream! {
            let batches = Self::load_delta_stream(&table_path, &delta_manager)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)));

            match batches {
                Ok(mut delta_stream) => {
                    while let Some(batch_result) = delta_stream.next().await {
                        let results = batch_result
                            .map_err(|e| DataFusionError::External(Box::new(e)))
                            .and_then(Self::extract_content_batches);

                        match results {
                            Ok(inner_batches) => {
                                for batch in inner_batches {
                                    yield batch;
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

impl ContentExec {
    /// Load Delta stream in a functional style using cached Delta table manager
    async fn load_delta_stream(
        table_path: &str,
        delta_manager: &DeltaTableManager,
    ) -> Result<SendableRecordBatchStream, deltalake::DeltaTableError> {
        let table = delta_manager.get_table_for_read(table_path).await?;
        let delta_ops = DeltaOps::from(table);
        let (_table, stream) = delta_ops.load().await?;
        Ok(stream)
    }

    /// Extract and process content batches from a Delta Lake record batch
    fn extract_content_batches(batch: RecordBatch) -> Result<Vec<Result<RecordBatch>>> {
        batch
            .column_by_name("content")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>())
            .map(Self::process_binary_array)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Process binary array using functional style
    fn process_binary_array(
        binary_array: &arrow_array::BinaryArray,
    ) -> Result<Vec<Result<RecordBatch>>> {
        Ok((0..binary_array.len())
            .filter_map(|i| binary_array.value(i).get(0..))
            .map(Self::deserialize_ipc_bytes)
            .collect())
    }

    /// Deserialize IPC bytes to record batches
    fn deserialize_ipc_bytes(bytes: &[u8]) -> Result<RecordBatch> {
        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::ArrowError(e, None))?
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("No batches found in IPC stream".to_string()))
    }
}
