//! QueryableFile trait for files that can be queried as DataFusion tables
//!
//! This trait follows anti-duplication principles by providing a single interface
//! for all file types that can be converted to DataFusion TableProviders.
//!
//! Instead of type checking and branching logic, we use trait dispatch.

use crate::error::TLogFSError;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use std::sync::Arc;
use tinyfs::{File, NodeID};

/// Single trait for all files that can be queried as DataFusion tables
///
/// Follows anti-duplication principle: one interface instead of multiple similar methods.
/// Each file type implements its own table provider creation logic without duplication.
#[async_trait]
pub trait QueryableFile: File {
    /// Convert this file to a DataFusion TableProvider
    ///
    /// Each implementation handles its own table provider creation:
    /// - OpLogFile: delegates to existing create_listing_table_provider (no duplication)
    /// - SqlDerivedFile: creates simple table provider that executes SQL
    /// - Other file types: implement as needed
    async fn as_table_provider(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        state: &crate::persistence::State,
    ) -> Result<Arc<dyn TableProvider>, TLogFSError>;
}
