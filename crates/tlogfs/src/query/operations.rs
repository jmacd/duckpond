use crate::schema::ForArrow;
use crate::query::{IpcTable};
use crate::delta::DeltaTableManager;
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

/// Table for querying directory data using IPC encoding
/// 
/// This table provides a DataFusion interface specifically for directory entries
/// that use Arrow IPC encoding in their content field. It wraps the generic IpcTable
/// with directory-specific schema and provides SQL access to directory metadata.
/// 
/// Example queries:
/// - SELECT * FROM directories WHERE file_type = 'Directory'
/// - SELECT node_id, part_id FROM directories WHERE path LIKE '/some/dir%'
#[derive(Debug, Clone)]
pub struct DirectoryTable {
    inner: IpcTable,
}

impl DirectoryTable {
    /// Create a new DirectoryTable for querying directory entries with IPC content
    pub fn new(table_path: String, delta_manager: DeltaTableManager) -> Self {
        // Use OplogEntry schema since that's what we want to expose via SQL
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        let inner = IpcTable::new(schema, table_path, delta_manager);
        Self { inner }
    }

    /// Create a new DirectoryTable with transaction sequence projection
    pub fn with_txn_seq(table_path: String, delta_manager: DeltaTableManager) -> Self {
        let schema = Arc::new(arrow::datatypes::Schema::new(OplogEntry::for_arrow()));
        let inner = IpcTable::with_txn_seq(schema, table_path, delta_manager);
        Self { inner }
    }
}

#[async_trait]
impl TableProvider for DirectoryTable {
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
