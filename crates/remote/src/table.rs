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
#[derive(Clone)]
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
    /// Open an existing table or create it if it doesn't exist
    /// 
    /// Despite the parameter name, this always tries to open first,
    /// then creates only if the table doesn't exist.
    pub async fn open_or_create<P: AsRef<Path>>(path: P, _create_new: bool) -> Result<Self> {
        match Self::open(&path).await {
            Ok(table) => Ok(table),
            Err(_) => Self::create(path).await,
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

    /// Write a file to remote storage with custom bundle_id
    ///
    /// Same as write_file but uses a provided bundle_id instead of computing SHA256.
    /// Used for transaction-based partitioning where all files in a transaction
    /// share the same bundle_id (e.g., "FILE-META-{txn_seq}").
    ///
    /// # Arguments
    /// * `bundle_id` - Custom bundle_id to use
    /// * Other arguments same as write_file
    ///
    /// # Returns
    /// The bundle_id that was used
    ///
    /// # Errors
    /// Same as write_file
    pub async fn write_file_with_bundle_id<R: AsyncRead + Unpin>(
        &mut self,
        bundle_id: &str,
        pond_txn_id: i64,
        path: impl Into<String>,
        reader: R,
        cli_args: Vec<String>,
    ) -> Result<()> {
        let writer = ChunkedWriter::new(
            pond_txn_id,
            path.into(),
            reader,
            cli_args,
        )
        .with_bundle_id(bundle_id.to_string());

        writer.write_to_table(&mut self.table).await?;

        // Re-register the updated table with the session context
        self.session_context.deregister_table("remote_files")?;
        self.session_context
            .register_table("remote_files", Arc::new(self.table.clone()))
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to re-register table: {}", e))
            })?;

        Ok(())
    }

    /// Write a file to remote storage in chunks
    ///
    /// Streams the file content, chunking it into manageable pieces,
    /// computing CRC32 for each chunk and SHA256 for the entire file,
    /// and writing to Delta Lake in a single transaction.
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number from pond
    /// * `path` - Original path in pond
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
        path: impl Into<String>,
        reader: R,
        cli_args: Vec<String>,
    ) -> Result<String> {
        let writer = ChunkedWriter::new(
            pond_txn_id,
            path.into(),
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
    /// * `bundle_id` - Partition identifier (FILE-META-{date}-{txn} or POND-FILE-{sha256})
    /// * `path` - Original file path to uniquely identify file within partition
    /// * `pond_txn_id` - Transaction sequence number
    /// * `writer` - Async writer to receive the file content
    ///
    /// # Errors
    /// Returns error if:
    /// - File not found
    /// - CRC32 checksum mismatch
    /// - SHA256 hash mismatch
    /// - Cannot read from Delta Lake
    /// - Cannot write to output
    pub async fn read_file<W: AsyncWrite + Unpin>(&self, bundle_id: &str, path: &str, pond_txn_id: i64, writer: W) -> Result<()> {
        let reader = ChunkedReader::new(&self.table, bundle_id, path, pond_txn_id);
        reader.read_to_writer(writer).await
    }

    /// Write transaction metadata
    ///
    /// Stores a summary of a pond transaction backup. All metadata for a pond
    /// uses the same bundle_id="POND:META:{pond_id}" partition, with individual
    /// transactions distinguished by the pond_txn_id column.
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

        // Use pond-specific metadata partition
        let bundle_id = ChunkedFileRecord::metadata_bundle_id(&metadata.pond_id);

        let writer = ChunkedWriter::new(
            pond_txn_id,
            "METADATA".to_string(),
            metadata_reader,
            metadata.cli_args.clone(),
        )
        .with_bundle_id(bundle_id);

        writer.write_to_table(&mut self.table).await?;

        // Re-register the updated table
        self.session_context.deregister_table("remote_files")?;
        self.session_context
            .register_table("remote_files", Arc::new(self.table.clone()))
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to re-register table: {}", e))
            })?;

        Ok(())
    }

    /// Read transaction metadata
    ///
    /// Retrieves the summary of a pond transaction backup by querying
    /// the metadata partition for the specific pond_txn_id.
    ///
    /// # Arguments
    /// * `pond_id` - Pond UUID
    /// * `pond_txn_id` - Transaction sequence number
    ///
    /// # Returns
    /// The transaction metadata
    ///
    /// # Errors
    /// Returns error if metadata not found or cannot be deserialized
    pub async fn read_metadata(
        &self,
        pond_id: &str,
        pond_txn_id: i64,
    ) -> Result<crate::schema::TransactionMetadata> {
        let bundle_id = ChunkedFileRecord::metadata_bundle_id(pond_id);
        
        // Query for the specific transaction's metadata
        let df = self
            .session_context
            .sql(&format!(
                "SELECT chunk_data FROM remote_files \
                 WHERE bundle_id = '{}' AND pond_txn_id = {} \
                 ORDER BY chunk_id",
                bundle_id, pond_txn_id
            ))
            .await?;

        let batches = df.collect().await?;
        
        if batches.is_empty() {
            return Err(RemoteError::FileNotFound(format!(
                "Metadata not found for pond {} txn {}",
                pond_id, pond_txn_id
            )));
        }

        // Reconstruct metadata from chunks
        let mut buffer = Vec::new();
        for batch in batches {
            let chunk_data_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid chunk_data column type".to_string())
                })?;

            for i in 0..chunk_data_col.len() {
                if !chunk_data_col.is_null(i) {
                    buffer.extend_from_slice(chunk_data_col.value(i));
                }
            }
        }

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

    /// List all files in a pond
    ///
    /// Returns information about all files backed up for the given pond.
    ///
    /// # Arguments
    /// * `pond_id` - UUID of the pond (empty string = all ponds)
    ///
    /// # Returns
    /// Vec of (bundle_id, path, file_type, total_size)
    ///
    /// # Errors
    /// Returns error if query fails
    pub async fn list_files(&self, _pond_id: &str) -> Result<Vec<(String, String, i64, i64)>> {
        let df = self
            .session_context
            .sql(
                "SELECT DISTINCT bundle_id, path, pond_txn_id, total_size 
                 FROM remote_files 
                 ORDER BY path"
            )
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
                    RemoteError::TableOperation("Invalid column type for path".to_string())
                })?;

            let txn_ids = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for pond_txn_id".to_string())
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
                    txn_ids.value(i),
                    sizes.value(i),
                ));
            }
        }

        Ok(files)
    }

    /// Find maximum transaction sequence number using efficient object_store listing
    ///
    /// Lists FILE-META-* partition directories without opening any parquet files.
    /// This is much cheaper than querying the Delta table, especially on S3.
    ///
    /// # Arguments
    /// * `pond_id` - Optional pond_id to filter by (currently not used, all txns returned)
    ///
    /// # Returns
    /// Maximum txn_seq found, or None if no transactions exist
    ///
    /// # Errors
    /// Returns error if cannot list object store
    pub async fn find_max_transaction(&self, _pond_id: Option<&str>) -> Result<Option<i64>> {
        use object_store::path::Path;
        
        let store = self.table.object_store();
        
        // List with delimiter to get only directories (partitions)
        let prefix = Some(Path::from("bundle_id=FILE-META-"));
        let list_result = store
            .list_with_delimiter(prefix.as_ref())
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to list partitions: {}", e))
            })?;

        // Parse directory names to extract transaction numbers
        let max_txn = list_result
            .common_prefixes
            .iter()
            .filter_map(|path| {
                // Extract txn_seq from "bundle_id=FILE-META-42/"
                let path_str = path.as_ref();
                path_str
                    .strip_prefix("bundle_id=FILE-META-")
                    .and_then(|s| s.trim_end_matches('/').parse::<i64>().ok())
            })
            .max();

        Ok(max_txn)
    }

    /// List all transaction numbers available in FILE-META partitions
    ///
    /// Uses object_store listing to enumerate all FILE-META-{txn_seq} directories.
    /// This is the efficient way to discover what transactions exist for restoration.
    ///
    /// # Returns
    /// Sorted vector of transaction sequence numbers
    ///
    /// # Errors
    /// Returns error if listing fails
    pub async fn list_available_transactions(&self) -> Result<Vec<i64>> {
        use object_store::path::Path;
        
        let store = self.table.object_store();
        
        // List with delimiter to get only directories (partitions)
        let prefix = Some(Path::from("bundle_id=FILE-META-"));
        let list_result = store
            .list_with_delimiter(prefix.as_ref())
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to list partitions: {}", e))
            })?;

        // Parse directory names to extract transaction numbers
        let mut txns: Vec<i64> = list_result
            .common_prefixes
            .iter()
            .filter_map(|path| {
                // Extract txn_seq from "bundle_id=FILE-META-42/"
                let path_str = path.as_ref();
                path_str
                    .strip_prefix("bundle_id=FILE-META-")
                    .and_then(|s| s.trim_end_matches('/').parse::<i64>().ok())
            })
            .collect();

        txns.sort_unstable();
        Ok(txns)
    }

    /// List all transaction numbers that have FILE-META partitions
    ///
    /// Uses efficient object_store listing to get actual transaction numbers.
    /// Returns sorted list of transaction sequence numbers.
    ///
    /// # Arguments
    /// * `pond_id` - Optional pond_id (currently unused)
    ///
    /// # Returns
    /// Sorted vec of transaction numbers (e.g., [3, 4, 5, 6, 7])
    ///
    /// # Errors
    /// Returns error if cannot list object store
    pub async fn list_transaction_numbers(&self, _pond_id: Option<&str>) -> Result<Vec<i64>> {
        // Query Delta table's file list directly from metadata
        // This is more reliable than object_store.list() which may not reflect latest commits
        let snapshot = self.table.snapshot()
            .map_err(|e| RemoteError::TableOperation(format!("Failed to get Delta snapshot: {}", e)))?;
        
        log::info!("Querying Delta files (table version: {:?})", self.table.version());
        
        let mut paths = Vec::new();
        for file in snapshot.file_paths_iter() {
            let path_str = file.as_ref();
            // Only include paths with FILE-META partition
            if path_str.starts_with("bundle_id=FILE-META-") {
                log::info!("Found Delta file: {}", path_str);
                paths.push(path_str.to_string());
            }
        }
        
        log::info!("Found {} FILE-META files in Delta", paths.len());
        
        // Extract unique partition directories from file paths
        let mut partition_dirs = std::collections::HashSet::new();
        for path_str in &paths {
            // Paths look like "bundle_id=FILE-META-2025-12-22-3/file.parquet"
            // Extract the partition directory part
            if let Some(dir_end) = path_str.find('/') {
                let dir = &path_str[..dir_end];
                if dir.starts_with("bundle_id=FILE-META-") {
                    log::debug!("Found partition dir: {}", dir);
                    partition_dirs.insert(dir.to_string());
                }
            }
        }
        
        log::debug!("Found {} FILE-META partitions", partition_dirs.len());

        // Parse directory names to extract transaction numbers
        // Format: "bundle_id=FILE-META-2025-12-22-42" -> 42
        let mut txn_numbers: Vec<i64> = partition_dirs
            .iter()
            .filter_map(|dir| {
                // Strip "bundle_id=FILE-META-" and extract last part after final dash
                let rest = dir.strip_prefix("bundle_id=FILE-META-")?;
                // rest is like "2025-12-22-42", extract the last number
                let txn = rest.rsplit('-').next()?.parse::<i64>().ok()?;
                log::debug!("Parsed txn {} from {}", txn, dir);
                Some(txn)
            })
            .collect();

        txn_numbers.sort_unstable();
        log::info!("Found {} transaction numbers: {:?}", txn_numbers.len(), txn_numbers);
        Ok(txn_numbers)
    }

    /// List transactions from metadata partition (old approach for backward compatibility)
    ///
    /// Queries the POND-META-{pond_id} partition for all pond_txn_id values.
    /// Used as fallback when FILE-META-* partitions don't exist.
    ///
    /// # Arguments
    /// * `pond_id` - Pond UUID to query
    ///
    /// # Returns
    /// Vec of transaction sequence numbers found in metadata
    ///
    /// # Errors
    /// Returns error if query fails
    pub async fn list_transactions_from_metadata(&self, pond_id: &str) -> Result<Vec<i64>> {
        let bundle_id = crate::schema::ChunkedFileRecord::metadata_bundle_id(pond_id);

        let df = self
            .session_context
            .sql(&format!(
                "SELECT DISTINCT pond_txn_id \
                 FROM remote_files \
                 WHERE bundle_id = '{}' \
                 ORDER BY pond_txn_id",
                bundle_id
            ))
            .await?;

        let batches = df.collect().await?;

        let mut transactions = Vec::new();
        for batch in batches {
            let txn_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid pond_txn_id column".to_string())
                })?;

            for i in 0..batch.num_rows() {
                transactions.push(txn_ids.value(i));
            }
        }

        Ok(transactions)
    }

    /// Query for files in a specific transaction
    ///
    /// Returns all files (parquet + delta logs) for the given transaction.
    /// Uses exact partition match on bundle_id for efficiency.
    ///
    /// # Arguments
    /// * `txn_seq` - Transaction sequence number
    ///
    /// # Returns
    /// Vec of (path, total_sha256, total_size, file_type)
    ///
    /// # Errors
    /// Returns error if query fails
    pub async fn list_transaction_files(
        &self,
        txn_seq: i64,
    ) -> Result<Vec<(String, String, String, i64, i64)>> {
        let bundle_id = crate::schema::ChunkedFileRecord::transaction_bundle_id(txn_seq);

        let df = self
            .session_context
            .sql(&format!(
                "SELECT DISTINCT bundle_id, path, total_sha256, total_size, pond_txn_id \
                 FROM remote_files \
                 WHERE bundle_id = '{}' \
                 ORDER BY path",
                bundle_id
            ))
            .await?;

        let batches = df.collect().await?;

        let mut files = Vec::new();
        for batch in batches {
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
                    RemoteError::TableOperation("Invalid column type for path".to_string())
                })?;

            let sha_col = batch.column(2);
            let shas = arrow_cast::cast(sha_col, &arrow_schema::DataType::Utf8)?;
            let shas = shas
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for total_sha256".to_string())
                })?;

            let sizes = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for total_size".to_string())
                })?;

            let txn_ids = batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::TableOperation("Invalid column type for pond_txn_id".to_string())
                })?;

            for i in 0..batch.num_rows() {
                files.push((
                    bundle_ids.value(i).to_string(),
                    paths.value(i).to_string(),
                    shas.value(i).to_string(),
                    sizes.value(i),
                    txn_ids.value(i),
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
