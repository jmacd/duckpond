pub mod ipc;
pub mod metadata;
pub mod operations;
pub mod series;
pub mod series_ext;
pub mod sql_executor;
pub mod table;

// Test modules
// NOTE: order_by_schema_test.rs preserved for reference but not compiled
// It tests UnifiedTableProvider ORDER BY schema harmonization bug that was fixed
// May be useful for reimplementing similar tests with FileTable architecture
#[cfg(test)]
mod disabled_tests {
    // order_by_schema_test.rs - preserved as reference, used UnifiedTableProvider
}

pub use ipc::IpcTable;
pub use metadata::MetadataTable;
pub use operations::DirectoryTable;

// Legacy exports for backward compatibility
pub use series::{SeriesTable, FileInfo};
pub use series_ext::{SeriesExt, SeriesStream, SeriesSchemaInfo, SeriesSummary, series_utils};
pub use table::{TableTable, TableFileInfo};

// FileTable architecture exports
pub use crate::file_table::{FileTable, FileTableProvider, create_table_provider, create_table_provider_from_path};

// SQL execution interface
pub use sql_executor::{execute_sql_on_file, format_query_results};