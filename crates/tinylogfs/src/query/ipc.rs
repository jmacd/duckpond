use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use arrow_array::{Array, RecordBatch, Int64Array};
use std::any::Any;

use datafusion::catalog::{Session, TableProvider};

use deltalake::DeltaOps;
use crate::delta_manager::DeltaTableManager;

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

/// Generic table for querying arbitrary Arrow IPC data stored in Delta Lake
/// 
/// This table provides a DataFusion interface to query any Arrow IPC-encoded data
/// stored in the Delta Lake "content" field. The schema is provided at construction
/// time, making this a flexible low-level interface for data access.
/// 
/// When include_txn_seq is true, an additional 'txn_seq' column is added that contains:
/// - For committed records: the Delta Lake version (transaction sequence)
/// - For pending records: -1 (synthetic transaction sequence)
#[derive(Debug, Clone)]
pub struct IpcTable {
    schema: SchemaRef,
    table_path: String,
    delta_manager: DeltaTableManager,
    include_txn_seq: bool,
}

impl IpcTable {
    pub fn new(schema: SchemaRef, table_path: String, delta_manager: DeltaTableManager) -> Self {
        Self { 
            schema, 
            table_path,
            delta_manager,
            include_txn_seq: false,
        }
    }
    
    /// Create a new IpcTable with transaction sequence projection
    pub fn with_txn_seq(schema: SchemaRef, table_path: String, delta_manager: DeltaTableManager) -> Self {
        // Extend the schema to include txn_seq column
        let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("txn_seq", DataType::Int64, false)));
        let enhanced_schema = Arc::new(Schema::new(fields));
        
        Self { 
            schema: enhanced_schema, 
            table_path,
            delta_manager,
            include_txn_seq: true,
        }
    }
}

#[async_trait]
impl TableProvider for IpcTable {
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
        Ok(Arc::new(IpcExec::new(self.clone())))
    }
}

/// Execution plan for generic Arrow IPC data queries
/// 
/// Reads from Delta Lake records, extracts the "content" field, and deserializes
/// the Arrow IPC data to provide as query results.
pub struct IpcExec {
    table: IpcTable,
    properties: PlanProperties,
}

impl IpcExec {
    pub fn new(table: IpcTable) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(table.schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Self { table, properties }
    }
}

impl std::fmt::Debug for IpcExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpcExec")
            .field("schema", &self.table.schema)
            .finish()
    }
}

impl DisplayAs for IpcExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "IpcExec")
            }
        }
    }
}

impl ExecutionPlan for IpcExec {
    fn name(&self) -> &'static str {
        "IpcExec"
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
        let include_txn_seq = self.table.include_txn_seq;

        let stream = async_stream::stream! {
            if include_txn_seq {
                // Use enhanced version that includes Delta Lake version
                let batches = Self::load_delta_stream_with_version(&table_path, &delta_manager)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)));

                match batches {
                    Ok((version, mut delta_stream)) => {
                        while let Some(batch_result) = delta_stream.next().await {
                            let results = batch_result
                                .map_err(|e| DataFusionError::External(Box::new(e)))
                                .and_then(|batch| Self::extract_ipc_batches_with_txn_seq(batch, version));

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
            } else {
                // Use original version without transaction sequence
                let batches = Self::load_delta_stream(&table_path, &delta_manager)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)));

                match batches {
                    Ok(mut delta_stream) => {
                        while let Some(batch_result) = delta_stream.next().await {
                            let results = batch_result
                                .map_err(|e| DataFusionError::External(Box::new(e)))
                                .and_then(Self::extract_ipc_batches);

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
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl IpcExec {
    /// Load Delta stream using cached Delta table manager
    async fn load_delta_stream(
        table_path: &str,
        delta_manager: &DeltaTableManager,
    ) -> Result<SendableRecordBatchStream, deltalake::DeltaTableError> {
        let table = delta_manager.get_table_for_read(table_path).await?;
        let delta_ops = DeltaOps::from(table);
        let (_table, stream) = delta_ops.load().await?;
        Ok(stream)
    }

    /// Load Delta stream with version information for transaction sequence projection
    async fn load_delta_stream_with_version(
        table_path: &str,
        delta_manager: &DeltaTableManager,
    ) -> Result<(i64, SendableRecordBatchStream), deltalake::DeltaTableError> {
        let table = delta_manager.get_table_for_read(table_path).await?;
        let version = table.version(); // Get the Delta Lake version
        let delta_ops = DeltaOps::from(table);
        let (_table, stream) = delta_ops.load().await?;
        Ok((version, stream))
    }

    /// Extract and process IPC batches from a Delta Lake record batch
    fn extract_ipc_batches(batch: RecordBatch) -> Result<Vec<Result<RecordBatch>>> {
        batch
            .column_by_name("content")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>())
            .map(Self::process_binary_array)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Extract and process IPC batches with transaction sequence projection
    fn extract_ipc_batches_with_txn_seq(batch: RecordBatch, _delta_version: i64) -> Result<Vec<Result<RecordBatch>>> {
        let ipc_batches = batch
            .column_by_name("content")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>())
            .map(Self::process_binary_array)
            .unwrap_or_else(|| Ok(Vec::new()))?;

        // Extract version column from the batch (this is the transaction sequence)
        let version_column = batch
            .column_by_name("version")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::Int64Array>())
            .ok_or_else(|| DataFusionError::Internal("version column not found or not Int64Array".to_string()))?;

        // Add txn_seq column to each batch using the version from the record
        let enhanced_batches = ipc_batches
            .into_iter()
            .enumerate()
            .map(|(record_idx, batch_result)| {
                batch_result.and_then(|batch| {
                    let num_rows = batch.num_rows();
                    
                    // Use the version from the corresponding record for all rows in this batch
                    let record_version = if record_idx < version_column.len() {
                        version_column.value(record_idx)
                    } else {
                        -1 // Fallback for pending records
                    };
                    
                    let txn_seq_values = vec![record_version; num_rows];
                    let txn_seq_array = Arc::new(Int64Array::from(txn_seq_values));
                    
                    let mut columns = batch.columns().to_vec();
                    columns.push(txn_seq_array);
                    
                    // Get the enhanced schema (original + txn_seq)
                    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
                    fields.push(Arc::new(Field::new("txn_seq", DataType::Int64, false)));
                    let enhanced_schema = Arc::new(Schema::new(fields));
                    
                    RecordBatch::try_new(enhanced_schema, columns)
                        .map_err(|e| DataFusionError::ArrowError(e, None))
                })
            })
            .collect();
            
        Ok(enhanced_batches)
    }

    /// Process binary array to extract Arrow IPC data
    fn process_binary_array(
        binary_array: &arrow_array::BinaryArray,
    ) -> Result<Vec<Result<RecordBatch>>> {
        Ok((0..binary_array.len())
            .filter_map(|i| binary_array.value(i).get(0..))
            .map(Self::deserialize_ipc_bytes)
            .collect())
    }

    /// Deserialize Arrow IPC bytes to record batches
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
