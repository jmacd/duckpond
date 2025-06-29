//! DataFusion query interfaces for oplog data
//! 
//! This module provides structured access to oplog data through DataFusion tables:
//! 
//! ## Abstraction Layers
//! 
//! ### Layer 1: Generic IPC Queries (`ipc`)
//! - **Purpose**: Query arbitrary Arrow IPC data stored in Delta Lake
//! - **Schema**: Flexible - provided at construction time
//! - **Use Case**: Low-level data access, custom schemas, debugging
//! 
//! ### Layer 2: Filesystem Operations (`operations`) 
//! - **Purpose**: Query filesystem operations (OplogEntry records)
//! - **Schema**: Fixed OplogEntry schema (part_id, node_id, file_type, content)
//! - **Use Case**: High-level filesystem queries, operation analysis
//! 
//! ## Usage Examples
//! 
//! ```rust,no_run
//! use std::sync::Arc;
//! use datafusion::prelude::*;
//! use tinylogfs::query::{IpcTable, OperationsTable};
//! use tinylogfs::DeltaTableManager;
//! use arrow::datatypes::{DataType, Field, Schema};
//! 
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//! 
//! // Generic IPC queries (any Arrow data)
//! let custom_schema = Arc::new(Schema::new(vec![Field::new("field", DataType::Utf8, false)]));
//! let table_path = "path/to/table".to_string();
//! let delta_manager = DeltaTableManager::new();
//! let ipc_table = IpcTable::new(custom_schema, table_path.clone(), delta_manager);
//! ctx.register_table("raw_data", Arc::new(ipc_table))?;
//! 
//! // Filesystem operations queries  
//! let ops_table = OperationsTable::new(table_path);
//! ctx.register_table("filesystem_ops", Arc::new(ops_table))?;
//! 
//! // SQL examples:
//! let df1 = ctx.sql("SELECT * FROM raw_data WHERE field = 'value'").await?;
//! let df2 = ctx.sql("SELECT * FROM filesystem_ops WHERE file_type = 'file'").await?;
//! # Ok(())
//! # }
//! ```

pub mod ipc;
pub mod operations;

// Re-export main types for convenience
pub use ipc::{IpcTable, IpcExec};
pub use operations::{OperationsTable, OperationsExec};
