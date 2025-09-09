pub mod nodes;
pub mod node_version_table;
pub mod operations;
pub mod series;
pub mod series_ext;
pub mod sql_executor;
pub mod table;

// pub use nodes::NodeTable;  // Replaced with SQL view: CREATE VIEW nodes AS SELECT ... FROM oplog_entries
// But still needed by legacy SeriesTable and TableTable implementations
pub use nodes::NodeTable;
pub use operations::DirectoryTable;

// Legacy exports for backward compatibility
pub use series::{SeriesTable, FileInfo};
pub use series_ext::{SeriesExt, SeriesStream, SeriesSchemaInfo, SeriesSummary, series_utils};
pub use table::{TableTable, TableFileInfo};
pub use node_version_table::NodeVersionTable;

// FileTable architecture exports
pub use crate::file_table::{FileTable, FileTableProvider, create_table_provider, create_table_provider_from_path};

// SQL execution interface
pub use sql_executor::execute_sql_on_file;
