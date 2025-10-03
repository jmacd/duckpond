//! Directory Table Function for DataFusion
//! 
//! Provides a `directory(node_id)` table function that returns directory entries
//! for a specific directory node_id with proper partition pruning.
//!
//! Usage: `SELECT * FROM directory('node_id_here')`

use crate::query::DirectoryTable;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

/// Table Function that provides access to directory entries by node_id
/// 
/// Usage: `SELECT * FROM directory('node_id_string')`
/// 
/// This automatically creates a DirectoryTable scoped to the specific node_id,
/// ensuring proper partition pruning and avoiding full table scans.
pub struct DirectoryTableFunction {
    session_context: Arc<datafusion::execution::context::SessionContext>,
}

impl std::fmt::Debug for DirectoryTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryTableFunction")
            .field("session_context", &"<SessionContext>")
            .finish()
    }
}

impl DirectoryTableFunction {
    /// Create a new DirectoryTableFunction 
    pub fn new(session_context: Arc<datafusion::execution::context::SessionContext>) -> Self {
        Self { session_context }
    }
}

impl TableFunctionImpl for DirectoryTableFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        // Expect exactly one argument: the directory node_id as a string
        let Some(Expr::Literal(ScalarValue::Utf8(Some(node_id_str)))) = exprs.first() else {
            return plan_err!("directory() requires exactly one string argument: directory('node_id')");
        };
        
        if exprs.len() != 1 {
            return plan_err!("directory() requires exactly one argument: directory('node_id')");
        }

        // Create a DirectoryTable scoped to this specific node_id
        let directory_table = DirectoryTable::for_directory(
            node_id_str.clone(),
            self.session_context.clone()
        );
        
        log::info!("📋 CREATED DirectoryTable for node_id: {}", node_id_str);
        
        Ok(Arc::new(directory_table))
    }
}

#[cfg(test)]
mod tests {
    use tempfile;
    use crate::OpLogPersistence;

    #[tokio::test]
    async fn test_directory_table_function() {
        // Create temporary directory for test pond
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path();
        
        // Create a proper pond with TLogFS persistence
        let mut persistence = OpLogPersistence::create(pond_path.to_str().unwrap()).await
            .expect("Failed to create OpLogPersistence");
        
        // Begin a transaction to get the session context
        let mut tx = persistence.begin().await.expect("Failed to begin transaction");
        let ctx = tx.session_context().await.expect("Failed to get session context");
        
        // Test that the directory function is registered
        // Try to call it with a sample node_id (this will return empty results but should not error)
        let test_node_id = "019945f3-031b-7e54-863d-895392f16dac"; // Sample UUID7
        let sql = format!("SELECT * FROM directory('{}')", test_node_id);
        
        let df = ctx.sql(&sql).await.expect("Failed to execute directory function");
        let results = df.collect().await.expect("Failed to collect results");
        
        // Should return empty results but not error
        assert_eq!(results.len(), 0, "Expected empty result for non-existent directory");
        
        println!("✅ Directory table function test passed!");
    }
}
