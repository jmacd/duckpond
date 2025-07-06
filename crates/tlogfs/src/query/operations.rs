use oplog::delta::ForArrow;
use oplog::query::{IpcTable, DeltaTableManager};
use crate::OplogEntry;
use arrow::datatypes::{SchemaRef};
use std::sync::Arc;

// DataFusion imports for table providers
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;

/// Table for querying filesystem operations (OplogEntry records)
/// 
/// This table provides a high-level DataFusion interface to query filesystem operations
/// stored in the oplog. It wraps the generic IpcTable with OplogEntry-specific schema
/// and provides SQL access to filesystem metadata like file_type, node_id, part_id, etc.
/// 
/// Example queries:
/// - SELECT * FROM filesystem_ops WHERE file_type = 'file'
/// - SELECT node_id, file_type FROM filesystem_ops WHERE part_id = '0000000000000000'
#[derive(Debug, Clone)]
pub struct OperationsTable {
    inner: IpcTable,
}

impl OperationsTable {
    /// Create a new OperationsTable for querying OplogEntry records
    pub fn new(table_path: String, delta_manager: DeltaTableManager) -> Self {
        // Use OplogEntry schema since that's what we want to expose via SQL
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        let inner = IpcTable::new(schema, table_path, delta_manager);
        Self { inner }
    }

    /// Create a new OperationsTable with transaction sequence projection
    pub fn with_txn_seq(table_path: String, delta_manager: DeltaTableManager) -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        let inner = IpcTable::with_txn_seq(schema, table_path, delta_manager);
        Self { inner }
    }
}

#[async_trait]
impl TableProvider for OperationsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate everything to the inner IpcTable
        self.inner.scan(state, projection, filters, limit).await
    }
}
