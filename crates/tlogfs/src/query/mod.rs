pub mod ipc;
pub mod metadata;
pub mod operations;
pub mod series;
pub mod series_ext;
pub mod table;

// NEW: Unified architecture modules
pub mod unified;

pub use ipc::IpcTable;
pub use metadata::MetadataTable;
pub use operations::DirectoryTable;

// Legacy exports for backward compatibility
pub use series::{SeriesTable, FileInfo};
pub use series_ext::{SeriesExt, SeriesStream, SeriesSchemaInfo, SeriesSummary, series_utils};
pub use table::{TableTable, TableFileInfo};

// NEW: Unified architecture exports
pub use unified::{UnifiedTableProvider, FileHandle, FileMetadata, ProviderType};