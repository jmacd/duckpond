// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Delta Lake table management for remote backups

use crate::error::RemoteError;
use crate::schema::ChunkedFileRecord;
use crate::{ChunkedReader, ChunkedWriter, Result};
use arrow_array::Array;
use datafusion::prelude::SessionContext;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable};
use log::debug;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

/// Remote backup table using Delta Lake
///
/// Manages a Delta Lake table storing chunked files with the schema:
/// - bundle_id (partition): SHA256 hash or "metadata_{txn_id}"
/// - pond_txn_id, original_path, file_type
/// - chunk_id, chunk_crc32, chunk_data
/// - total_size, total_sha256, chunk_count
/// - cli_args, created_at
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use remote::RemoteTable;
///
/// // Create new remote table
/// let mut table = RemoteTable::create("/path/to/remote").await?;
///
/// // Or open existing table
/// let mut table = RemoteTable::open("/path/to/remote").await?;
/// # Ok(())
/// # }
/// ```
pub struct RemoteTable {
    /// Path to the Delta Lake table
    path: String,
    /// The Delta Lake table instance
    table: DeltaTable,
    /// Shared SessionContext for queries
    session_context: Arc<SessionContext>,
}

impl RemoteTable {
    /// Create a new remote backup table
    ///
    /// Creates a Delta Lake table with the chunked file schema.
    /// Returns an error if the table already exists.
    ///
    /// # Arguments
    /// * `path` - Directory path for the Delta Lake table
    ///
    /// # Errors
    /// Returns error if:
    /// - Table already exists
    /// - Cannot create directory
    /// - Delta Lake initialization fails
    pub async fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!("Creating new remote backup table at {}", path_str);

        // Ensure parent directory exists
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RemoteError::TableOperation(format!("Failed to create directory: {}", e))
            })?;
        }

        // Configure Delta Lake table to skip stats on binary column
        // This avoids warnings about large binary data in statistics
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingStatsColumns".to_string(),
            Some(
                "bundle_id,pond_txn_id,original_path,file_type,chunk_id,chunk_crc32,\
                 total_size,total_sha256,chunk_count,cli_args,created_at"
                    .to_string(),
            ),
        )]
        .into_iter()
        .collect();

        // Create Delta Lake table
        let table = DeltaOps::try_from_uri(&path_str)
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to initialize table URI: {}", e))
            })?
            .create()
            .with_columns(ChunkedFileRecord::delta_schema())
            .with_partition_columns(vec!["bundle_id"]) // Partition by file hash
            .with_configuration(config)
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!(
                    "Failed to create table (already exists?): {}",
                    e
                ))
            })?;

        debug!("Created remote backup table at {}", path_str);

        // Create SessionContext and register table
        let session_context = Arc::new(SessionContext::new());
        session_context
            .register_table("remote_files", Arc::new(table.clone()))
            .map_err(|e| RemoteError::TableOperation(format!("Failed to register table: {}", e)))?;

        Ok(Self {
            path: path_str,
            table,
            session_context,
        })
    }

    /// Open an existing remote backup table
    ///
    /// Opens an existing Delta Lake table for reading and writing.
    /// Returns an error if the table doesn't exist.
    ///
    /// # Arguments
    /// * `path` - Directory path to the Delta Lake table
    ///
    /// # Errors
    /// Returns error if:
    /// - Table doesn't exist
    /// - Cannot open table
    /// - Schema is invalid
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!("Opening existing remote backup table at {}", path_str);

        let table = deltalake::open_table(&path_str).await.map_err(|e| {
            RemoteError::TableOperation(format!(
                "Failed to open remote table at {}: {}",
                path_str, e
            ))
        })?;

        // Create SessionContext and register table
        let session_context = Arc::new(SessionContext::new());
        session_context
            .register_table("remote_files", Arc::new(table.clone()))
            .map_err(|e| RemoteError::TableOperation(format!("Failed to register table: {}", e)))?;

        debug!("Opened remote backup table at {}", path_str);

        Ok(Self {
            path: path_str,
            table,
            session_context,
        })
    }

    /// Create or open a remote backup table
    ///
    /// Convenience method that creates a new table if it doesn't exist,
    /// or opens an existing table.
    ///
    /// # Arguments
    /// * `path` - Directory path for the Delta Lake table
    /// * `create_new` - If true, require table creation (error if exists)
    ///                  If false, open existing table (error if doesn't exist)
    pub async fn open_or_create<P: AsRef<Path>>(path: P, create_new: bool) -> Result<Self> {
        if create_new {
            Self::create(path).await
        } else {
            Self::open(path).await
        }
    }

    /// Get access to the underlying Delta table
    #[must_use]
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Get the SessionContext for queries
    #[must_use]
    pub fn session_context(&self) -> &Arc<SessionContext> {
        &self.session_context
    }

    /// Get the table path
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Write a file to remote storage in chunks
    ///
    /// Streams the file content, chunking it into manageable pieces,
    /// computing CRC32 for each chunk and SHA256 for the entire file,
    /// and writing to Delta Lake in a single transaction.
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number from pond
    /// * `original_path` - Original path in pond
    /// * `file_type` - Type of file being backed up
    /// * `reader` - Async reader providing file content
    /// * `cli_args` - CLI arguments that triggered this backup
    ///
    /// # Returns
    /// The bundle_id (SHA256 hash) of the written file
    ///
    /// # Errors
    /// Returns error if:
    /// - Cannot read from input
    /// - Cannot compute checksums
    /// - Cannot write to Delta Lake
    pub async fn write_file<R: AsyncRead + Unpin>(
        &mut self,
        pond_txn_id: i64,
        original_path: impl Into<String>,
        file_type: crate::FileType,
        reader: R,
        cli_args: Vec<String>,
    ) -> Result<String> {
        let writer = ChunkedWriter::new(
            pond_txn_id,
            original_path.into(),
            file_type,
            reader,
            cli_args,
        );

        let bundle_id = writer.write_to_table(&mut self.table).await?;

        // Re-register the updated table with the session context
        // The table reference in session_context needs to point to the updated table state
        self.session_context.deregister_table("remote_files")?;
        self.session_context
            .register_table("remote_files", Arc::new(self.table.clone()))
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to re-register table: {}", e))
            })?;

        Ok(bundle_id)
    }

    /// Read a file from remote storage
    ///
    /// Streams chunks from Delta Lake, verifies CRC32 for each chunk
    /// and SHA256 for the entire file, writing the reconstructed file
    /// to the output writer.
    ///
    /// # Arguments
    /// * `bundle_id` - SHA256 hash identifying the file
    /// * `writer` - Async writer to receive the file content
    ///
    /// # Errors
    /// Returns error if:
    /// - File not found
    /// - CRC32 checksum mismatch
    /// - SHA256 hash mismatch
    /// - Cannot read from Delta Lake
    /// - Cannot write to output
    pub async fn read_file<W: AsyncWrite + Unpin>(&self, bundle_id: &str, writer: W) -> Result<()> {
        let reader = ChunkedReader::new(&self.table, bundle_id);
        reader.read_to_writer(writer).await
    }

    /// Write transaction metadata
    ///
    /// Stores a summary of a pond transaction backup, including the list
    /// of files and their hashes. This is stored in a special partition
    /// with bundle_id="metadata_{pond_txn_id}".
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number
    /// * `metadata` - Transaction metadata to store
    ///
    /// # Errors
    /// Returns error if cannot serialize or write metadata
    pub async fn write_metadata(
        &mut self,
        pond_txn_id: i64,
        metadata: &crate::schema::TransactionMetadata,
    ) -> Result<()> {
        let metadata_json = serde_json::to_vec(metadata)?;
        let metadata_reader = std::io::Cursor::new(metadata_json);

        let _bundle_id = ChunkedFileRecord::metadata_bundle_id(pond_txn_id);

        self.write_file(
            pond_txn_id,
            "METADATA",
            crate::FileType::Metadata,
            metadata_reader,
            metadata.cli_args.clone(),
        )
        .await?;

        Ok(())
    }

    /// Read transaction metadata
    ///
    /// Retrieves the summary of a pond transaction backup.
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number
    ///
    /// # Returns
    /// The transaction metadata
    ///
    /// # Errors
    /// Returns error if metadata not found or cannot be deserialized
    pub async fn read_metadata(
        &self,
        pond_txn_id: i64,
    ) -> Result<crate::schema::TransactionMetadata> {
        let bundle_id = ChunkedFileRecord::metadata_bundle_id(pond_txn_id);
        let mut buffer = Vec::new();
        self.read_file(&bundle_id, &mut buffer).await?;

        let metadata = serde_json::from_slice(&buffer)?;
        Ok(metadata)
    }

    /// List all transactions in the remote backup
    ///
    /// Returns a list of all pond transaction IDs that have been backed up.
    ///
    /// # Errors
    /// Returns error if query fails
    pub async fn list_transactions(&self) -> Result<Vec<i64>> {
        let df = self
            .session_context
            .sql(
                "SELECT DISTINCT pond_txn_id 
                 FROM remote_files 
                 ORDER BY pond_txn_id",
            )
            .await?;

        let batches = df.collect().await?;

        let mut txn_ids = Vec::new();
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for pond_txn_id".to_string())
                })?;

            for i in 0..col.len() {
                if !col.is_null(i) {
                    txn_ids.push(col.value(i));
                }
            }
        }

        Ok(txn_ids)
    }

    /// List all files in a transaction
    ///
    /// Returns information about all files backed up in a specific transaction.
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number
    ///
    /// # Returns
    /// Vec of (bundle_id, original_path, file_type, total_size)
    ///
    /// # Errors
    /// Returns error if query fails
    pub async fn list_files(&self, pond_txn_id: i64) -> Result<Vec<(String, String, String, i64)>> {
        let df = self
            .session_context
            .sql(&format!(
                "SELECT DISTINCT bundle_id, original_path, file_type, total_size 
                 FROM remote_files 
                 WHERE pond_txn_id = {} 
                   AND file_type != 'metadata'
                 ORDER BY original_path",
                pond_txn_id
            ))
            .await?;

        let batches = df.collect().await?;

        let mut files = Vec::new();
        for batch in batches {
            // Extract string values - may be dictionary encoded by DataFusion
            let bundle_id_col = batch.column(0);
            let bundle_ids = arrow_cast::cast(bundle_id_col, &arrow_schema::DataType::Utf8)?;
            let bundle_ids = bundle_ids
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for bundle_id".to_string())
                })?;

            let path_col = batch.column(1);
            let paths = arrow_cast::cast(path_col, &arrow_schema::DataType::Utf8)?;
            let paths = paths
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for original_path".to_string())
                })?;

            let type_col = batch.column(2);
            let types = arrow_cast::cast(type_col, &arrow_schema::DataType::Utf8)?;
            let types = types
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for file_type".to_string())
                })?;

            let sizes = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for total_size".to_string())
                })?;

            for i in 0..batch.num_rows() {
                files.push((
                    bundle_ids.value(i).to_string(),
                    paths.value(i).to_string(),
                    types.value(i).to_string(),
                    sizes.value(i),
                ));
            }
        }

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_table() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote_backup");

        let table = RemoteTable::create(&table_path).await.unwrap();
        assert_eq!(table.path(), table_path.to_string_lossy());

        // Should fail to create again
        let result = RemoteTable::create(&table_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_table() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote_backup");

        // Create table
        let _table = RemoteTable::create(&table_path).await.unwrap();

        // Open table
        let table = RemoteTable::open(&table_path).await.unwrap();
        assert_eq!(table.path(), table_path.to_string_lossy());
    }

    #[tokio::test]
    async fn test_open_nonexistent_table() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("nonexistent");

        let result = RemoteTable::open(&table_path).await;
        assert!(result.is_err());
    }
}
