use super::error::TLogFSError;
use super::schema::{OplogEntry, VersionedDirectoryEntry, OperationType, ForArrow};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use super::transaction_guard::TransactionGuard;
use crate::factory::{FactoryRegistry, FactoryContext};
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{EntryType, FS, NodeID, NodeType, Result as TinyFSResult, NodeMetadata, FileVersionInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::pin::Pin;
use async_trait::async_trait;
use uuid7;
use chrono::Utc;
use diagnostics::*;
use tokio::sync::Mutex;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::CommitInfo;

pub struct OpLogPersistence {
    pub(crate) path: String,
    pub(crate) table: DeltaTable,
    pub(crate) fs: Option<FS>,
    pub(crate) state: Option<State>,
}

pub struct InnerState {
    path: String,
    table: Option<deltalake::DeltaTable>, // If present on open
    records: Vec<OplogEntry>, // @@@ LINEAR SEARCH
    operations: HashMap<NodeID, HashMap<String, DirectoryOperation>>,
}

#[derive(Clone)]
pub struct State(Arc<Mutex<InnerState>>);

impl OpLogPersistence {
    /// Get the Delta table for query operations
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }
    
    /// Creates a new OpLogPersistence instance with a new table
    ///
    /// This constructor creates a new Delta Lake table, failing if it already exists,
    /// and initializes the filesystem with a root directory.
    pub async fn create(path: &str) -> Result<Self, TLogFSError> {
        debug!("create called with path: {path}");
        
	Self::open_or_create(path, true).await
    }
    
    /// Opens an existing OpLogPersistence instance
    ///
    /// This constructor opens an existing Delta Lake table for append operations.
    pub async fn open(path: &str) -> Result<Self, TLogFSError> {
        debug!("open called with path: {path}");

	Self::open_or_create(path, false).await
    }

    pub async fn open_or_create(path: &str, create_new: bool) -> Result<Self, TLogFSError> {
	// Enable RUST_LOG logging configuration for tests@@@
	let _ = env_logger::try_init();

	let mode = if create_new { SaveMode::ErrorIfExists } else { SaveMode::Append };

        // Check what's in the directory
	//show_dir(path);
        
        // First try to open existing table
        let table = match deltalake::open_table(path).await {
            Ok(existing_table) => {
                debug!("Found existing table at {path}");
                existing_table
            }
            Err(open_err) => {
                debug!("no existing table at {path}, will create: {open_err}");
                // Table doesn't exist, create it
                let create_result = DeltaOps::try_from_uri(path).await?
                    .create()
                    .with_columns(OplogEntry::for_delta())
                    .with_partition_columns(["part_id"])
                    .with_save_mode(mode)
                    .await;
                
                match create_result {
                    Ok(table) => {
                        //debug!("created table at {path}");
			//show_dir(path);
                        table
                    }
                    Err(create_err) => {
                        debug!("failed to create table at {path}: {create_err}", path, create_err);
                        return Err(create_err.into());
                    }
                }
            }
        };

        let mut persistence = Self {
	    table: table,
	    path: path.into(),
	    fs: None,
	    state: None,
        };

	if mode == SaveMode::ErrorIfExists {
	    let tx = persistence.begin().await?;
	    tx.state()?.initialize_root_directory().await?;
	    tx.commit(None).await?;
	}

        Ok(persistence)
    }
    
    pub(crate) fn state(&self) -> Result<State, TLogFSError> {
	self.state.as_ref().map(|x| x.clone())
	    .ok_or(TLogFSError::Missing {})
    }

    /// Get commit history from Delta table via State
    pub async fn get_commit_history(&self, limit: Option<usize>) -> Result<Vec<CommitInfo>, TLogFSError> {
        self.state()?.get_commit_history(limit).await
    }

    /// Begin a transaction and return a transaction guard
    ///
    /// This is the new transaction guard API that provides RAII-style transaction management
    pub async fn begin(&mut self) -> Result<TransactionGuard<'_>, TLogFSError> {
	let state = State(Arc::new(Mutex::new(InnerState::new(self.path.clone(), self.table.clone()))));
        state.begin_impl().await?;

	self.fs = Some(FS::new(state.clone()).await?);
	self.state = Some(state);

        Ok(TransactionGuard::new(self))
    }

    /// Commit a transaction with metadata and return the committed version
    pub(crate) async fn commit(
        &mut self,
        metadata: Option<std::collections::HashMap<String, serde_json::Value>>
    ) -> Result<Option<()>, TLogFSError> {
	self.fs = None;
        let did = self.state.take().unwrap().commit_impl(metadata).await?;
	self.table.update().await?;
	Ok(did)
    }
    
    /// Get the store path for this persistence layer
    pub fn store_path(&self) -> &str {
        &self.path
    }
    
}

impl State {
    /// Get the Delta table for query operations
    pub async fn table(&self) -> Result<Option<deltalake::DeltaTable>, TLogFSError> {
        Ok(self.0.lock().await.table.clone())
    }

    /// Get commit metadata for a specific version
    pub async fn get_commit_metadata(&self, ts: i64) -> Result<Option<std::collections::HashMap<String, serde_json::Value>>, TLogFSError> {
        self.0.lock().await.get_commit_metadata(ts).await
    }

    /// Get commit metadata for a specific version
    pub async fn get_last_commit_metadata(&self) -> Result<Option<std::collections::HashMap<String, serde_json::Value>>, TLogFSError> {
        self.0.lock().await.get_last_commit_metadata().await
    }

    async fn initialize_root_directory(&self) -> Result<(), TLogFSError> {
	self.0.lock().await.initialize_root_directory().await
    }

    async fn begin_impl(&self) -> Result<(), TLogFSError> {
	self.0.lock().await.begin_impl().await
    }

    async fn commit_impl(
        &mut self,
        metadata: Option<HashMap<String, serde_json::Value>>
    ) -> Result<Option<()>, TLogFSError> {
	self.0.lock().await.commit_impl(metadata).await
    }    

    pub(crate) async fn store_file_content_ref(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        content_ref: crate::file_writer::ContentRef,
        file_type: tinyfs::EntryType,
        metadata: crate::file_writer::FileMetadata,
    ) -> Result<(), TLogFSError> {
	self.0.lock().await.store_file_content_ref(node_id, part_id, content_ref, file_type, metadata).await
    }

    /// Create an async reader for a file without loading entire content into memory
    pub(crate) async fn async_file_reader(
        &self,
        node_id: NodeID,
        part_id: NodeID
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.0.lock().await.async_file_reader(node_id, part_id).await
    }

    /// Get commit history from Delta table
    pub async fn get_commit_history(&self, limit: Option<usize>) -> Result<Vec<CommitInfo>, TLogFSError> {
        self.0.lock().await.get_commit_history(limit).await
    }

    /// Add an arbitrary OplogEntry record to pending transaction state
    /// This is used for metadata-only operations like temporal bounds setting
    pub async fn add_oplog_entry(&self, entry: OplogEntry) -> Result<(), TLogFSError> {
        self.0.lock().await.records.push(entry);
        Ok(())
    }
}

#[async_trait]
impl PersistenceLayer for State {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<NodeType> {
	self.0.lock().await.load_node(node_id, part_id, self.clone()).await
    }

    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> TinyFSResult<()> {
	self.0.lock().await.store_node(node_id, part_id, node_type).await
    }
    
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<bool> {
	self.0.lock().await.exists_node(node_id, part_id).await
    }

    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<std::path::PathBuf> {
	self.0.lock().await.load_symlink_target(node_id, part_id).await
    }

    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<()> {
	self.0.lock().await.store_symlink_target(node_id, part_id, target).await
    }

    async fn create_file_node(&self, node_id: NodeID, part_id: NodeID, entry_type: EntryType) -> TinyFSResult<NodeType> {
	self.0.lock().await.create_file_node(node_id, part_id, entry_type, self.clone()).await
    }

    async fn create_directory_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<NodeType> {
	self.0.lock().await.create_directory_node(node_id, part_id, self.clone()).await
    }

    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<NodeType> {
	self.0.lock().await.create_symlink_node(node_id, part_id, target, self.clone()).await
    }
    
    async fn create_dynamic_directory_node(&self, part_id: NodeID, name: String, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
	self.0.lock().await.create_dynamic_directory_node(part_id, name, factory_type, config_content).await
    }

    async fn create_dynamic_file_node(&self, part_id: NodeID, name: String, file_type: EntryType, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
	self.0.lock().await.create_dynamic_file_node(part_id, name, file_type, factory_type, config_content).await
    }

    async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Option<(String, Vec<u8>)>> {
	self.0.lock().await.get_dynamic_node_config(node_id, part_id).await
	    .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn load_directory_entries(&self, part_id: NodeID) -> TinyFSResult<HashMap<String, (NodeID, EntryType)>> {
	self.0.lock().await.load_directory_entries(part_id).await
    }

    async fn query_directory_entry(&self, part_id: NodeID, entry_name: &str) -> TinyFSResult<Option<(NodeID, EntryType)>> {
	self.0.lock().await.query_directory_entry(part_id, entry_name).await
    }

    async fn update_directory_entry(&self, part_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> TinyFSResult<()> {
	self.0.lock().await.update_directory_entry(part_id, entry_name, operation).await
    }

    async fn metadata(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<NodeMetadata> {
	self.0.lock().await.metadata(node_id, part_id).await
    }

    async fn metadata_u64(&self, node_id: NodeID, part_id: NodeID, name: &str) -> TinyFSResult<Option<u64>> {
	self.0.lock().await.metadata_u64(node_id, part_id, name).await
    }

    async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Vec<FileVersionInfo>> {
	self.0.lock().await.list_file_versions(node_id, part_id).await
    }

    async fn read_file_version(&self, node_id: NodeID, part_id: NodeID, version: Option<u64>) -> TinyFSResult<Vec<u8>> {
	self.0.lock().await.read_file_version(node_id, part_id, version).await
    }

    async fn set_extended_attributes(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        attributes: std::collections::HashMap<String, String>
    ) -> TinyFSResult<()> {
        self.0.lock().await.set_extended_attributes(node_id, part_id, attributes).await
    }
}

impl InnerState {
    fn new(path: String, table: DeltaTable) -> Self {
	Self {
	    path,
	    table: Some(table),
            records: Vec::new(),
            operations: HashMap::new(),
	}
    }

    async fn initialize_root_directory(&mut self) -> Result<(), TLogFSError> {
        let root_node_id = NodeID::root();

        // Create root directory using direct TLogFS commit (no transaction guard needed for bootstrap)
        let now = Utc::now().timestamp_micros();
        let empty_entries: Vec<VersionedDirectoryEntry> = Vec::new();
        let content = self.serialize_directory_entries(&empty_entries)?;

        let root_entry = OplogEntry::new_inline(
            root_node_id,
            root_node_id,
            tinyfs::EntryType::Directory,
            now,
            1, // First version of root directory node
            content,
        );

        self.records.push(root_entry);

        Ok(())
    }

    /// Begin a new transaction
    async fn begin_impl(&mut self) -> Result<(), TLogFSError> {
        // Clear any stale state (should be clean already, but just in case)
        self.records.clear();
        self.operations.clear();

        debug!("Started transaction");
        Ok(())
    }

    /// Create a hybrid writer for streaming file content
    async fn create_hybrid_writer(&self) -> crate::large_files::HybridWriter {
        crate::large_files::HybridWriter::new(self.path.clone())
    }

    /// Store file content from hybrid writer result
    async fn store_file_from_hybrid_writer(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        result: crate::large_files::HybridWriterResult,
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
	let entry = if result.size < crate::large_files::LARGE_FILE_THRESHOLD {
            // Small file: store content directly in Delta Lake
            let now = Utc::now().timestamp_micros();
            OplogEntry::new_small_file(
                part_id,
                node_id,
                entry_type,  // Use provided entry type instead of hardcoded FileData
                now,
                0, // Placeholder - actual version assigned by Delta Lake transaction log
                result.content,
            )
        } else {
            // Large file: content already stored, just create OplogEntry with SHA256
            let size = result.size;
            debug!("Storing large file: {size} bytes");
            let now = Utc::now().timestamp_micros();

            match entry_type {
                tinyfs::EntryType::FileSeries => {
                    // For FileSeries, extract temporal metadata from Parquet content
                    use super::schema::{extract_temporal_range_from_batch, detect_timestamp_column, ExtendedAttributes};
                    use tokio_util::bytes::Bytes;

                    // Read the Parquet data to extract temporal metadata
                    let bytes = Bytes::from(result.content.clone());
                    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet reader: {}", e)))?
                        .build()
                        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet reader: {}", e)))?;

                    let mut all_batches = Vec::new();
                    for batch_result in reader {
                        let batch = batch_result
                            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read batch: {}", e)))?;
                        all_batches.push(batch);
                    }

                    if all_batches.is_empty() {
                        return Err(TLogFSError::ArrowMessage("No data in Parquet file".to_string()));
                    }

                    // For temporal extraction, we'll process all batches to get global min/max
                    let schema = all_batches[0].schema();

                    // Determine timestamp column
                    let time_col = detect_timestamp_column(&schema)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to detect timestamp column: {}", e)))?;

                    // Extract temporal range from all batches
                    let mut global_min = i64::MAX;
                    let mut global_max = i64::MIN;

                    for batch in &all_batches {
                        let (batch_min, batch_max) = extract_temporal_range_from_batch(batch, &time_col)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to extract temporal range: {}", e)))?;
                        global_min = global_min.min(batch_min);
                        global_max = global_max.max(batch_max);
                    }

                    // Create extended attributes with timestamp column info
                    let mut extended_attrs = ExtendedAttributes::new();
                    extended_attrs.set_timestamp_column(&time_col);

                    // Create large FileSeries entry with temporal metadata and size
                    OplogEntry::new_large_file_series(
                        part_id,
                        node_id,
                        now,
                        0, // Placeholder - actual version assigned by Delta Lake transaction log
                        result.sha256,
                        result.size as i64, // Cast to i64 to match Delta Lake protocol
                        global_min,
                        global_max,
                        extended_attrs,
                    )
                },
                _ => {
                    // For other entry types, use generic large file constructor
                    OplogEntry::new_large_file(
                        part_id,
                        node_id,
                        entry_type,  // Use provided entry type instead of hardcoded FileData
                        now,
                        0, // Placeholder - actual version assigned by Delta Lake transaction log
                        result.sha256,
                        result.size as i64, // Cast to i64 to match Delta Lake protocol
                    )
                }
            }
	};
        self.records.push(entry);
	Ok(())
    }

    /// Store file content with automatic size-based strategy (small inline vs large external)
    pub async fn store_file_content_with_type(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
        use crate::large_files::should_store_as_large_file;

        if should_store_as_large_file(content) {
            // Use hybrid writer for large files
            let mut writer = self.create_hybrid_writer().await;
            use tokio::io::AsyncWriteExt;
            writer.write_all(content).await?;
            writer.shutdown().await?;
            let result = writer.finalize().await?;
            self.store_file_from_hybrid_writer(node_id, part_id, result, entry_type).await
        } else {
            // For small files, always use direct storage regardless of type
            // This avoids Parquet parsing issues for small FileSeries test files
            self.store_small_file_with_type(node_id, part_id, content, entry_type).await
        }
    }

    /// Store small file directly in Delta Lake with specific entry type
    async fn store_small_file_with_type(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
        let now = Utc::now().timestamp_micros();
        let entry = OplogEntry::new_small_file(
            part_id,
            node_id,
            entry_type, // Use the provided entry type instead of hardcoded FileData
            now,
            0, // @@@ !!! Placeholder - actual version assigned by Delta Lake transaction log
            content.to_vec(),
        );

        self.records.push(entry);
        Ok(())
    }

    /// Get the next version number for a specific node (current max + 1)
    async fn get_next_version_for_node(&self, node_id: NodeID, part_id: NodeID) -> Result<i64, TLogFSError> {
        debug!("get_next_version_for_node called for node_id={node_id}, part_id={part_id}");

        // Query all records for this node and find the maximum version
        match self.query_records(part_id, Some(node_id)).await {
            Ok(records) => {
                let record_count = records.len();
                debug!("get_next_version_for_node found {record_count} existing records", record_count: record_count);

                let next_version = if records.is_empty() {
                    // This is a new node - start with version 1
                    debug!("get_next_version_for_node: new node, starting with version 1");
                    1
                } else {
                    // This is an existing node - find max version and increment
                    let max_version = records.iter()
                        .map(|r| r.version)
                        .max()
                        .expect("records is non-empty, so max() should succeed");
                    let next_version = max_version + 1;
                    debug!("get_next_version_for_node: existing node with max_version={max_version}, returning next_version={next_version}", max_version: max_version, next_version: next_version);
                    next_version
                };

                Ok(next_version)
            }
            Err(e) => {
                let error_str = format!("{:?}", e);
                debug!("get_next_version_for_node query failed: {error}", error: error_str);
                // Critical error: cannot determine proper version sequence
                Err(TLogFSError::ArrowMessage(format!("Cannot determine next version for node {node_id}: query failed: {e}")))
            }
        }
    }

    /// Find existing large file entry in pending records (to avoid duplicates during store_node)
    async fn find_existing_large_file_entry(&self, node_id_str: &str, part_id_str: &str) -> Option<OplogEntry> {
        self.records.iter().find(|entry| {
            entry.node_id == node_id_str &&
		entry.part_id == part_id_str &&
		entry.content.is_none() && // Large files have no inline content @@@
		entry.sha256.is_some() // Large files have SHA256 @@@
        }).cloned()
    }

    /// Store FileSeries with temporal metadata extraction from Parquet data
    /// This method extracts min/max timestamps from the specified time column
    pub async fn store_file_series_from_parquet(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64), TLogFSError> {
        use super::schema::{extract_temporal_range_from_batch, detect_timestamp_column, ExtendedAttributes};
        use tokio_util::bytes::Bytes;

        // Get the next version number for this node
        let next_version = self.get_next_version_for_node(node_id, part_id).await?;        // First, read the Parquet data to extract temporal metadata
        let bytes = Bytes::from(content.to_vec());
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet reader: {}", e)))?
            .build()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet reader: {}", e)))?;

        let mut all_batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read batch: {}", e)))?;
            all_batches.push(batch);
        }

        if all_batches.is_empty() {
            return Err(TLogFSError::ArrowMessage("No data in Parquet file".to_string()));
        }

        // For temporal extraction, we'll process all batches to get global min/max
        let schema = all_batches[0].schema();

        // Determine timestamp column
        let time_col = match timestamp_column {
            Some(col) => col.to_string(),
            None => detect_timestamp_column(&schema)?,
        };

        // Extract temporal range from all batches
        let mut global_min = i64::MAX;
        let mut global_max = i64::MIN;

        for batch in &all_batches {
            let (batch_min, batch_max) = extract_temporal_range_from_batch(batch, &time_col)?;
            global_min = global_min.min(batch_min);
            global_max = global_max.max(batch_max);
        }

        // Create extended attributes with timestamp column info
        let mut extended_attrs = ExtendedAttributes::new();
        extended_attrs.set_timestamp_column(&time_col);

        // Store the FileSeries using the unified hybrid writer pattern
        use crate::large_files::should_store_as_large_file;

        let content_len = content.len();
        let is_large_file = should_store_as_large_file(content);
        debug!("store_file_series_from_parquet decision: content_len={content_len}, is_large_file={is_large_file}");

        if is_large_file {
            // Use hybrid writer for large files (same pattern as store_file_content_with_type)
            let mut writer = self.create_hybrid_writer().await;
            use tokio::io::AsyncWriteExt;
            writer.write_all(content).await?;
            writer.shutdown().await?;
            let result = writer.finalize().await?;

            // Extract hybrid writer result data
            let sha256 = result.sha256.clone();
            let size = result.size as i64;
            let now = Utc::now().timestamp_micros();

            debug!("Stored large FileSeries via HybridWriter: {size} bytes, SHA256: {sha256}");

            let entry = OplogEntry::new_large_file_series(
                part_id,
                node_id,
                now,
                next_version, // Use proper version counter
                sha256,
                size,
                global_min,
                global_max,
                extended_attrs,
            );

            // Store metadata in pending records (content is external)
            self.records.push(entry);
        } else {
            // Store as small FileSeries
            let now = Utc::now().timestamp_micros();
            let content_size = content.len();

            debug!("store_file_series_from_parquet - storing as small FileSeries with {content_size} bytes content", content_size: content_size);

            let entry = OplogEntry::new_file_series(
                part_id,
                node_id,
                now,
                next_version, // Use proper version counter
                content.to_vec(),
                global_min,
                global_max,
                extended_attrs,
            );

            let entry_content_size = entry.content.as_ref().map(|c| c.len()).unwrap_or(0);
            debug!("store_file_series_from_parquet - created OplogEntry with content size: {entry_content_size}", entry_content_size: entry_content_size);

            self.records.push(entry);
        }

        Ok((global_min, global_max))
    }

    /// Create an async reader for a file without loading entire content into memory
    pub async fn async_file_reader(
        &self,
        node_id: NodeID,
        part_id: NodeID
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        let records = self.query_records(part_id, Some(node_id)).await?;

        if let Some(record) = records.first() {
            if record.is_large_file() {
                // Large file: create async file reader
                let sha256 = record.sha256.as_ref().ok_or_else(|| TLogFSError::ArrowMessage(
                    "Large file entry missing SHA256".to_string()
                ))?;

                // Find the file in either flat or hierarchical structure
                let large_file_path = crate::large_files::find_large_file_path(&self.path, sha256).await
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Error searching for large file: {}", e)))?
                    .ok_or_else(|| TLogFSError::LargeFileNotFound {
                        sha256: sha256.clone(),
                        path: format!("_large_files/sha256={}", sha256),
                        source: std::io::Error::new(std::io::ErrorKind::NotFound, "Large file not found in any location"),
                    })?;

                // Open file for async reading
                let file = tokio::fs::File::open(&large_file_path).await
                    .map_err(|e| TLogFSError::LargeFileNotFound {
                        sha256: sha256.clone(),
                        path: large_file_path.display().to_string(),
                        source: e,
                    })?;

                Ok(Box::pin(file))
            } else {
                // Small file: create cursor from inline content
                let content = record.content.clone().ok_or_else(|| TLogFSError::ArrowMessage(
                    "Small file entry missing content".to_string()
                ))?;
                
                Ok(Box::pin(std::io::Cursor::new(content)))
            }
        } else {
            Err(TLogFSError::NodeNotFound {
                path: std::path::PathBuf::from(format!("File {node_id} not found"))
            })
        }
    }

    /// Commit pending records to Delta Lake
    async fn commit_impl(
        &mut self,
        metadata: Option<HashMap<String, serde_json::Value>>
    ) -> Result<Option<()>, TLogFSError> {
        self.flush_directory_operations().await?;

        let records = std::mem::take(&mut self.records);

        if records.is_empty() {
            info!("Committing read-only transaction (no write operations)");

	    // This is for development: we do not expect read-only transactions.
	    //panic!("Committing read-only transaction (no write operations)");
            return Ok(None);
        }

        let count = records.len();
        info!("Committing {count} operations in {path}", path: self.path);

        // Convert records to RecordBatch
        let batches = vec![
	    serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &records)?,
	];

	let mut write_op = DeltaOps(self.table.as_ref().unwrap().clone())
 	    .write(batches);

        // Add commit metadata
        if let Some(metadata_map) = metadata {
            let properties = CommitProperties::default()
                .with_metadata(metadata_map.into_iter());
            write_op = write_op.with_commit_properties(properties);
        }

        _ = write_op.await?;

	self.table = None;
	self.records.clear();
	self.operations.clear();
	
        Ok(Some(()))
    }

    /// Serialize VersionedDirectoryEntry records as Arrow IPC bytes
    fn serialize_directory_entries(&self, entries: &[VersionedDirectoryEntry]) -> Result<Vec<u8>, TLogFSError> {
        serialization::serialize_to_arrow_ipc(entries)
    }

    /// Deserialize VersionedDirectoryEntry records from Arrow IPC bytes
    fn deserialize_directory_entries(&self, content: &[u8]) -> Result<Vec<VersionedDirectoryEntry>, TLogFSError> {
        serialization::deserialize_from_arrow_ipc(content)
    }

    /// Commit with metadata for crash recovery
    /// Get commit history from Delta table
    pub async fn get_commit_history(&self, limit: Option<usize>) -> Result<Vec<CommitInfo>, TLogFSError> {
	if self.table.is_none() {
	    return Err(TLogFSError::Missing {})
	}
	
        self.table.as_ref().unwrap().history(limit).await
	    .map_err(|e| TLogFSError::Delta(e))
    }

    /// Get commit metadata for a specific version
    pub async fn get_commit_metadata(&self, ts: i64) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        let history = self.get_commit_history(Some(1)).await?;
	for hist in history.iter() {
	    if hist.timestamp == Some(ts) {
		return Ok(Some(hist.info.clone()))
	    }
	}
        Ok(None)
    }

    /// Get commit metadata for a specific version
    pub async fn get_last_commit_metadata(&self) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        let history = self.get_commit_history(Some(1)).await?;
	return Ok(history.iter().next().as_ref().map(|x| x.info.clone()))
    }

    /// Store file content reference with transaction context (used by transaction guard FileWriter)
    pub async fn store_file_content_ref(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        content_ref: crate::file_writer::ContentRef,
        file_type: tinyfs::EntryType,
        metadata: crate::file_writer::FileMetadata,
    ) -> Result<(), TLogFSError> {
        debug!("store_file_content_ref_transactional called for node_id={node_id}, part_id={part_id}");

        // Create OplogEntry from content reference
        let now = chrono::Utc::now().timestamp_micros();

        // Get proper version number for this node
        // Check if there's already an entry for this node in this transaction
        let version = {
            let existing_entry = self.records.iter()
                .find(|e| e.node_id == node_id.to_hex_string() && e.part_id == part_id.to_hex_string());

            if let Some(existing) = existing_entry {
                // Check if this is a placeholder entry (version 0) vs real content
                if existing.version == 0 {
                    // This is the first time content is being added - use version 1
                    // The version 0 placeholder will be replaced, not duplicated
                    1
                } else {
                    // Replacing existing content - preserve the same version
                    existing.version
                }
            } else {
                // New entry - calculate next version
                self.get_next_version_for_node(node_id, part_id).await?
            }
        };

        let entry = match content_ref {
            crate::file_writer::ContentRef::Small(content) => {
                // Small file: store content inline
                match file_type {
                    tinyfs::EntryType::FileSeries => {
                        // FileSeries needs temporal metadata
                        match metadata {
                            crate::file_writer::FileMetadata::Series { min_timestamp, max_timestamp, timestamp_column } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::new();
                                extended_attrs.set_timestamp_column(&timestamp_column);

                                super::schema::OplogEntry::new_file_series(
                                    part_id,
                                    node_id,
                                    now,
                                    version, // Use proper version counter
                                    content,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                )
                            }
                            _ => {
                                return Err(TLogFSError::Transaction {
                                    message: "FileSeries requires Series metadata".to_string()
                                });
                            }
                        }
                    }
                    _ => {
                        // Regular small file
                        super::schema::OplogEntry::new_small_file(
                            part_id,
                            node_id,
                            file_type,
                            now,
                            version, // Use proper version counter
                            content,
                        )
                    }
                }
            }
            crate::file_writer::ContentRef::Large(sha256, size) => {
                // Large file: store reference
                match file_type {
                    tinyfs::EntryType::FileSeries => {
                        // Large FileSeries needs temporal metadata
                        match metadata {
                            crate::file_writer::FileMetadata::Series { min_timestamp, max_timestamp, timestamp_column } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::new();
                                extended_attrs.set_timestamp_column(&timestamp_column);

                                super::schema::OplogEntry::new_large_file_series(
                                    part_id,
                                    node_id,
                                    now,
                                    version, // Use proper version counter
                                    sha256,
                                    size as i64,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                )
                            }
                            _ => {
                                return Err(TLogFSError::Transaction {
                                    message: "Large FileSeries requires Series metadata".to_string()
                                });
                            }
                        }
                    }
                    _ => {
                        // Regular large file
                        super::schema::OplogEntry::new_large_file(
                            part_id,
                            node_id,
                            file_type,
                            now,
                            version, // Use proper version counter
                            sha256,
                            size as i64,
                        )
                    }
                }
            }
        };

        // Find existing entry for this node/part combination
        let existing_index = self.records.iter().position(|existing_entry| {
            existing_entry.part_id == part_id.to_hex_string() && existing_entry.node_id == node_id.to_hex_string()
        });

        if let Some(index) = existing_index {
            // Replace existing entry (content changes, version stays the same within transaction)
            self.records[index] = entry;
        } else {
            // No existing entry - add new entry with version 1 (??)
            debug!("Adding new pending entry for node {node_id} with version {entry_version}", entry_version: entry.version);
            self.records.push(entry);
        }

        debug!("Stored file content reference for node {node_id}");
        Ok(())
    }

    /// Query directory entries for a parent node
    async fn query_directory_entries(&self, part_id: NodeID) -> Result<Vec<VersionedDirectoryEntry>, TLogFSError> {
        let records = self.query_records(part_id, None).await?;

        let mut all_entries = Vec::new();
        for record in records {
            // record is already an OplogEntry - no need to deserialize
            if record.file_type == tinyfs::EntryType::Directory {
                if let Some(content) = &record.content {
                    if let Ok(dir_entries) = self.deserialize_directory_entries(content) {
                        all_entries.extend(dir_entries);
                    }
                }
            }
        }

        // Deduplicate entries by name, keeping only the latest operation
        // Since records are ordered by timestamp DESC, newer entries come first
        let mut seen_names = std::collections::HashSet::new();
        let mut deduplicated_entries = Vec::new();

        // Process in forward order so later entries (newer transactions) take precedence
        for entry in all_entries.into_iter() {
            if !seen_names.contains(&entry.name) {
                seen_names.insert(entry.name.clone());
                if matches!(entry.operation_type, OperationType::Insert | OperationType::Update) {
                    deduplicated_entries.push(entry);
                }
            }
        }

        deduplicated_entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(deduplicated_entries)
    }

    /// Query for a single directory entry by name
    async fn query_single_directory_entry(&self, part_id: NodeID, entry_name: &str) -> Result<Option<VersionedDirectoryEntry>, TLogFSError> {
        // Check pending directory operations first
            if let Some(operations) = self.operations.get(&part_id) {
                if let Some(operation) = operations.get(entry_name) {
                    match operation {
                        DirectoryOperation::InsertWithType(node_id, node_type) => {
                            return Ok(Some(VersionedDirectoryEntry::new(
                                entry_name.to_string(),
                                Some(node_id.clone()),
                                OperationType::Insert,
                                node_type.clone(),
                            )));
                        }
                        DirectoryOperation::DeleteWithType(_node_type) => {
                            return Ok(None);
                        }
                        DirectoryOperation::RenameWithType(new_name, node_id, node_type) => {
                            return Ok(Some(VersionedDirectoryEntry::new(
                                new_name.clone(),
                                Some(node_id.clone()),
                                OperationType::Insert,
                                node_type.clone(),
                            )));
                        }
                    }
                }
            }

        // Query committed records
        let records = self.query_records(part_id, None).await?;

        // Process records in order (latest first) to get the most recent operation
        // query_records already returns records sorted by timestamp DESC
        for record in records.iter() {
            // record is already an OplogEntry - no need to deserialize
            if record.file_type == tinyfs::EntryType::Directory {
                if let Some(content) = &record.content {
                    if let Ok(directory_entries) = self.deserialize_directory_entries(content) {
                        // Process entries in reverse order within each record (latest first)
                        for entry in directory_entries.iter().rev() {
                            if entry.name == entry_name {
                                match entry.operation_type {
                                    OperationType::Insert | OperationType::Update => return Ok(Some(entry.clone())),
                                    OperationType::Delete => return Ok(None),
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Process all accumulated directory operations in a batch
    async fn flush_directory_operations(&mut self) -> Result<(), TLogFSError> {
        let pending_dirs =
	    std::mem::take(&mut self.operations);

        if pending_dirs.is_empty() {
            return Ok(());
        }

        for (part_id, operations) in pending_dirs {
            let mut versioned_entries = Vec::new();

            for (entry_name, operation) in operations {
                match operation {
                    DirectoryOperation::InsertWithType(child_node_id, node_type) => {
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            entry_name,
                            Some(child_node_id),
                            OperationType::Insert,
                            node_type,
                        ));
                    }
                    DirectoryOperation::DeleteWithType(node_type) => {
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            entry_name,
                            None,
                            OperationType::Delete,
                            node_type,
                        ));
                    }
                    DirectoryOperation::RenameWithType(new_name, child_node_id, node_type) => {
                        // Delete the old entry
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            entry_name,
                            None,
                            OperationType::Delete,
                            node_type.clone(),
                        ));
                        // Insert with new name
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            new_name,
                            Some(child_node_id),
                            OperationType::Insert,
                            node_type,
                        ));
                    }
                }
            }

            // Create directory record for parent directory contents
            let content_bytes = self.serialize_directory_entries(&versioned_entries)?;

            let now = Utc::now().timestamp_micros();
            let record = OplogEntry::new_inline(
                part_id,
                part_id,
                tinyfs::EntryType::Directory,
                now,
                0,
                content_bytes,
            );

            self.records.push(record);
        }

        Ok(())
    }

    /// Create a dynamic directory node with factory configuration
    /// This is the primary method for implementing the `mknod` command functionality
    async fn create_dynamic_directory(
        &mut self,
        part_id: NodeID,
	name: String,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID, TLogFSError> {
        let node_id = NodeID::generate();
        let now = Utc::now().timestamp_micros();

        // Create dynamic directory OplogEntry
        let entry = OplogEntry::new_dynamic_directory(
            part_id,	    
            node_id,
            now,
	    1,  // @@@ MaDE THIS UP
            factory_type,
            config_content,
        );

        // Add to pending records
        self.records.push(entry);

        // Add directory operation for parent
        let directory_op = DirectoryOperation::InsertWithType(node_id, tinyfs::EntryType::Directory);
        self.update_directory_entry(part_id, &name, directory_op).await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        Ok(node_id)
    }

    /// Create a dynamic file node with factory configuration
    pub async fn create_dynamic_file(
        &mut self,
        part_id: NodeID,
        name: String,
        file_type: tinyfs::EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID, TLogFSError> {
        let node_id = NodeID::generate();
        let now = Utc::now().timestamp_micros();

        // Create dynamic file OplogEntry
        let entry = OplogEntry::new_dynamic_file(
            part_id,
            node_id,
            file_type,
            now,
            1, // Made UP @@@
            factory_type,
            config_content,
        );

        // Add to pending records
        self.records.push(entry);

        // Add directory operation for parent
        let directory_op = DirectoryOperation::InsertWithType(node_id, file_type);
        self.update_directory_entry(part_id, &name, directory_op).await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        Ok(node_id)
    }

    /// Get dynamic node configuration if the node is dynamic
    /// Uses the same query pattern as the rest of the persistence layer
    pub async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> Result<Option<(String, Vec<u8>)>, TLogFSError> {
        // First check pending records (for nodes created in current transaction)
        for record in &self.records {
            if record.node_id == node_id.to_hex_string() {
                if let Some(factory_type) = &record.factory {
                    if factory_type != "tlogfs" {
                        if let Some(config_content) = &record.content {
                            return Ok(Some((factory_type.clone(), config_content.clone())));
                        }
                    }
                }
            }
        }

        // Then check committed records (for existing nodes)
        let records = self.query_records(part_id, Some(node_id)).await?;

        if let Some(record) = records.first() {
            if let Some(factory_type) = &record.factory {
                if factory_type != "tlogfs" {
                    if let Some(config_content) = &record.content {
                        return Ok(Some((factory_type.clone(), config_content.clone())));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Query records from both committed (Delta Lake) and pending (in-memory) data
    /// This ensures TinyFS operations can see pending data before commit
    /// @@@ WHY NOT REAL TYPE ARGS?
    async fn query_records(&self, part_id: NodeID, node_id: Option<NodeID>) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Step 1: Get committed records from Delta Lake
	let committed_records = match self.table.clone() {
            Some(table) => {
                let sql = if node_id.is_some() {
                    "SELECT * FROM {table} WHERE part_id = '{0}' AND node_id = '{1}' ORDER BY timestamp DESC"
                } else {
                    "SELECT * FROM {table} WHERE part_id = '{0}' ORDER BY timestamp DESC"
                };
                let params = if let Some(node_id) = node_id {
                    vec![part_id.to_hex_string(), node_id.to_hex_string()]
                } else {
                    vec![part_id.to_hex_string()]
                };

                match query_utils::execute_sql_query(table, sql, &params).await {
                    Ok(records) => records,
                    Err(_e) => Vec::new(),
                }
            }
            None => Vec::new(),
        };

        // Step 2: Get pending records from memory
        let records = {
            self.records.iter()
                .filter(|record| {
                    record.part_id == part_id.to_hex_string() &&
                    (node_id.is_none() || Some(record.node_id.clone()) == node_id.map(|x| x.to_hex_string()))
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        // Step 3: Combine and sort by timestamp
        let mut all_records = committed_records;
        all_records.extend(records);
        all_records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Ok(all_records)
    }

    async fn load_node(&self, node_id: NodeID, part_id: NodeID, state: State) -> TinyFSResult<NodeType> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        debug!("load_node {node_id_str} part_id={part_id_str}");

        // Query Delta Lake for the most recent record for this node
        let records = match self.query_records(part_id, Some(node_id)).await {
            Ok(records) => {
                let record_count = records.len();
                debug!("query_records returned {record_count} records");
                records
            }
            Err(e) => {
                let error_msg = e.to_string();
                debug!("query_records failed with error: {error_msg}");
                return Err(error_utils::to_tinyfs_error(e));
            }
        };

        if let Some(record) = records.first() {
            // record is already an OplogEntry - no need to deserialize again
            debug!("storage has existing record");

            // Use node factory to create the appropriate node type
            node_factory::create_node_from_oplog_entry(
                record,  // Use the OplogEntry directly
                node_id,
                part_id,
                state,
            )
        } else {
            // Node doesn't exist in committed data, check pending transactions
            let pending_record =
                self.records.iter().find(|entry| {
                    entry.node_id == node_id_str && entry.part_id == part_id_str
                }).cloned();

            if let Some(record) = pending_record {
                // Found in pending records, create node from it
                debug!("Found node in pending records");

                node_factory::create_node_from_oplog_entry(
                    &record,
                    node_id,
                    part_id,
                    state,
                )
            } else {
                // Node doesn't exist in database or pending transactions
                Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("Node {} not found", node_id_str))))
            }
        }
    }

    async fn store_node(&mut self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> TinyFSResult<()> {
        let node_hex = node_id.to_hex_string();
        let part_hex = part_id.to_hex_string();
        debug!("TRANSACTION: OpLogPersistence::store_node() - node: {node_hex}, part: {part_hex}");

        // Create OplogEntry based on node type
        let (file_type, content) = match node_type {
            tinyfs::NodeType::File(file_handle) => {
                let file_content = tinyfs::buffer_helpers::read_file_to_vec(file_handle).await
                    .map_err(|e| tinyfs::Error::Other(format!("File content error: {}", e)))?;
                let content_len = file_content.len();
                debug!("TRANSACTION: store_node() - file has {content_len} bytes of content");

                // Query the file handle's metadata to get the entry type
                let metadata = file_handle.metadata().await
                    .map_err(|e| tinyfs::Error::Other(format!("Metadata query error: {}", e)))?;

                // Check if this file was written as a large file by looking for existing large file storage
                // This handles the case where TinyFS async writer used HybridWriter but the memory content is empty
                if file_content.is_empty() {
                    // Check if there's an existing large file stored for this node
                    let node_hex = node_id.to_hex_string();
                    if let Some(_existing_entry) = self.find_existing_large_file_entry(&node_hex, &part_id.to_hex_string()).await {
                        debug!("TRANSACTION: store_node() - found existing large file entry for {node_hex}, skipping duplicate");
                        return Ok(()); // Don't create duplicate entry
                    }
                    
                    // Also skip empty files that will be written to later - they'll be handled by the file writer
                    debug!("TRANSACTION: store_node() - empty file content for {node_hex}, skipping to avoid duplicate with file writer");
                    return Ok(());
                }

                (metadata.entry_type, file_content)
            }
            tinyfs::NodeType::Directory(_) => {
                let empty_entries: Vec<VersionedDirectoryEntry> = Vec::new();
                let content = self.serialize_directory_entries(&empty_entries)
                    .map_err(error_utils::to_tinyfs_error)?;
                (tinyfs::EntryType::Directory, content)
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle.readlink().await
                    .map_err(|e| tinyfs::Error::Other(format!("Symlink readlink error: {}", e)))?;
                let target_bytes = target.to_string_lossy().as_bytes().to_vec();
                (tinyfs::EntryType::Symlink, target_bytes)
            }
        };

        let now = Utc::now().timestamp_micros();

        // Get proper version for this node
        let next_version = self.get_next_version_for_node(node_id, part_id).await
            .map_err(error_utils::to_tinyfs_error)?;

        let oplog_entry = OplogEntry::new_inline(
            part_id,
            node_id,
            file_type,
            now, // Node modification time
            next_version, // Use proper version counter
            content,
        );

        // Add to pending records - no double-nesting, store OplogEntry directly
        self.records.push(oplog_entry);
        Ok(())
    }

    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<bool> {
        let records = self.query_records(part_id, Some(node_id)).await
            .map_err(error_utils::to_tinyfs_error)?;

        Ok(!records.is_empty())
    }

    async fn load_directory_entries(&self, part_id: NodeID) -> TinyFSResult<HashMap<String, (NodeID, EntryType)>> {
        let all_entries = self.query_directory_entries(part_id).await
            .map_err(error_utils::to_tinyfs_error)?;

        let mut current_state = HashMap::new();
        for entry in all_entries {
            match entry.operation_type {
                OperationType::Insert | OperationType::Update => {
                    if let Ok(child_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        current_state.insert(entry.name, (child_id, entry.node_type));
                    }
                }
                OperationType::Delete => {
                    current_state.remove(&entry.name);
                }
            }
        }

        Ok(current_state)
    }

    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<std::path::PathBuf> {
        let records = self.query_records(part_id, Some(node_id)).await
            .map_err(error_utils::to_tinyfs_error)?;

        if let Some(record) = records.first() {
            if record.file_type == tinyfs::EntryType::Symlink {
                let content = record.content.clone().ok_or_else(||
                    tinyfs::Error::Other("Symlink content is missing".to_string()))?;
                let target_str = String::from_utf8(content)
                    .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8 in symlink target: {}", e)))?;
                Ok(std::path::PathBuf::from(target_str))
            } else {
                Err(tinyfs::Error::Other("Expected symlink node type".to_string()))
            }
        } else {
            Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("Symlink {node_id} not found"))))
        }
    }

    async fn store_symlink_target(&mut self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<()> {
        let symlink_handle = tinyfs::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = tinyfs::NodeType::Symlink(symlink_handle);
        self.store_node(node_id, part_id, &node_type).await
    }

    async fn create_file_node(&mut self, node_id: NodeID, part_id: NodeID, entry_type: tinyfs::EntryType, state: State) -> TinyFSResult<NodeType> {
        // Create file node in memory only - no immediate persistence
        self.store_file_content_with_type(node_id, part_id, &[], entry_type).await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

        node_factory::create_file_node(node_id, part_id, state)
    }

    async fn create_directory_node(&self, node_id: NodeID, part_id: NodeID, state: State) -> TinyFSResult<NodeType> {
        node_factory::create_directory_node(node_id, part_id, state)
    }

    async fn create_symlink_node(&mut self, node_id: NodeID, part_id: NodeID, target: &std::path::Path, state: State) -> TinyFSResult<NodeType> {
        // Store the target immediately
        self.store_symlink_target(node_id, part_id, target).await?;

        // Create and return the symlink node
        node_factory::create_symlink_node(node_id, part_id, state, target)
    }

    async fn metadata(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<tinyfs::NodeMetadata> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        debug!("metadata: querying node_id={node_id_str}, part_id={part_id_str}");

        // Query Delta Lake for the most recent record for this node using the correct partition
        let records = self.query_records(part_id, Some(node_id)).await
            .map_err(error_utils::to_tinyfs_error)?;

        let record_count = records.len();
        debug!("metadata: found {record_count} records");

        // Debug: log all records to understand the issue
        for (i, record) in records.iter().enumerate() {
            let file_type_str = format!("{:?}", record.file_type);
            let version = record.version;
            let timestamp = record.timestamp;
            debug!("metadata: record[{i}] - file_type={file_type_str}, version={version}, timestamp={timestamp}");
        }

        if let Some(record) = records.first() {
            // Use the record directly - it's already an OplogEntry with metadata() method
            let file_type_str = format!("{:?}", record.file_type);
            debug!("metadata: returning consolidated metadata from OplogEntry - using file_type={file_type_str}");
            Ok(record.metadata())
        } else {
            debug!("metadata: no records found");
            // Node doesn't exist
            Err(tinyfs::Error::not_found(&format!("Node {}", node_id_str)))
        }
    }

    async fn metadata_u64(&self, node_id: NodeID, part_id: NodeID, name: &str) -> TinyFSResult<Option<u64>> {
        debug!("metadata_u64: querying node_id={node_id}, part_id={part_id}, name={name}");

        // Query Delta Lake for the most recent record for this node using the correct partition
        let records = self.query_records(part_id, Some(node_id)).await
            .map_err(error_utils::to_tinyfs_error)?;

        let record_count = records.len();
        debug!("metadata_u64: found {record_count} records");

        if let Some(record) = records.first() {
            // Use the record directly - it's already an OplogEntry
            let timestamp = record.timestamp;
            let version = record.version;
            debug!("metadata_u64: record.timestamp={timestamp}, record.version={version}");

            // Return the requested metadata field
            match name {
                "timestamp" => Ok(Some(record.timestamp as u64)),
                "version" => Ok(Some(record.version as u64)),
                _ => Ok(None), // Unknown metadata field
            }
        } else {
            debug!("metadata_u64: no records found");
            // Node doesn't exist
            Ok(None)
        }
    }

    async fn query_directory_entry(&self, part_id: NodeID, entry_name: &str) -> TinyFSResult<Option<(NodeID, EntryType)>> {
        match self.query_single_directory_entry(part_id, entry_name).await {
            Ok(Some(entry)) => {
                if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
                    match entry.operation_type {
                        OperationType::Delete => Ok(None),
                        _ => Ok(Some((child_node_id, entry.node_type))),
                    }
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(error_utils::to_tinyfs_error(e)),
        }
    }

    async fn update_directory_entry(
        &mut self,
        part_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> TinyFSResult<()> {
        // Enhanced directory coalescing - accumulate operations with node types for batch processing
        let dir_ops = self.operations.entry(part_id).or_insert_with(HashMap::new);

        // All operations must now include node type - no legacy conversion
        dir_ops.insert(entry_name.to_string(), operation);
        Ok(())
    }

    // Versioning operations implementation
    async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Vec<tinyfs::FileVersionInfo>> {
        debug!("list_file_versions called for node_id={node_id}, part_id={part_id}");
        let mut records = self.query_records(part_id, Some(node_id)).await
            .map_err(error_utils::to_tinyfs_error)?;
        
        let record_count = records.len();
        debug!("list_file_versions found {record_count} records for node {node_id}");

        // Sort records by timestamp ASC (oldest first) to assign logical file versions
        records.sort_by_key(|record| record.timestamp);

        let version_infos = records.into_iter().enumerate().map(|(index, record)| {
            let logical_version = (index + 1) as u64; // Assign logical file versions 1, 2, 3, etc.

            let size = if record.is_large_file() {
                record.size.unwrap_or(0)
            } else {
                record.content.as_ref().map(|c| c.len() as i64).unwrap_or(0)
            };

            // Extract extended metadata for file:series
            let extended_metadata = if record.file_type == tinyfs::EntryType::FileSeries {
                let mut metadata = std::collections::HashMap::new();
                if let (Some(min_time), Some(max_time)) = (record.min_event_time, record.max_event_time) {
                    metadata.insert("min_event_time".to_string(), min_time.to_string());
                    metadata.insert("max_event_time".to_string(), max_time.to_string());
                }
                if let Some(attrs) = &record.extended_attributes {
                    metadata.insert("extended_attributes".to_string(), attrs.clone());
                }
                Some(metadata)
            } else {
                None
            };

            tinyfs::FileVersionInfo {
                version: logical_version,
                timestamp: record.timestamp,
                size: size as u64, // Cast back to u64 for tinyfs interface
                sha256: record.sha256.clone(),
                entry_type: record.file_type.clone(),
                extended_metadata,
            }
        }).collect();

        Ok(version_infos)
    }

    async fn read_file_version(&self, node_id: NodeID, part_id: NodeID, version: Option<u64>) -> TinyFSResult<Vec<u8>> {
        let mut records = self.query_records(part_id, Some(node_id)).await
            .map_err(error_utils::to_tinyfs_error)?;

        // Sort records by timestamp ASC (oldest first) to create logical file versions
        records.sort_by_key(|record| record.timestamp);

        let target_record = match version {
            Some(v) => {
                // Find specific version by the actual version field, not array index
                records.into_iter().find(|record| record.version == v as i64)
                    .ok_or_else(|| tinyfs::Error::NotFound(
                        std::path::PathBuf::from(format!("Version {} of file {} not found", v, node_id))
                    ))?
            }
            None => {
                // Return latest version (last record after sorting by timestamp ASC)
                records.into_iter().last()
                    .ok_or_else(|| tinyfs::Error::NotFound(
                        std::path::PathBuf::from(format!("No versions of file {} found", node_id))
                    ))?
            }
        };

        // Load content based on file type
        if target_record.is_large_file() {
            // Large file: read from external storage
            let sha256 = target_record.sha256.as_ref()
                .ok_or_else(|| tinyfs::Error::Other("Large file entry missing SHA256".to_string()))?;

            let large_file_path = crate::large_files::find_large_file_path(&self.path, sha256).await
                .map_err(|e| tinyfs::Error::Other(format!("Error searching for large file: {}", e)))?
                .ok_or_else(|| tinyfs::Error::NotFound(
                    std::path::PathBuf::from(format!("Large file with SHA256 {} not found", sha256))
                ))?;

            tokio::fs::read(&large_file_path).await
                .map_err(|e| tinyfs::Error::Other(format!("Failed to read large file: {}", e)))
        } else {
            // Small file: content stored inline
            target_record.content
                .ok_or_else(|| tinyfs::Error::Other("Small file entry missing content".to_string()))
        }
    }

    async fn set_extended_attributes(
        &mut self, 
        node_id: NodeID, 
        part_id: NodeID, 
        attributes: std::collections::HashMap<String, String>
    ) -> TinyFSResult<()> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        debug!("set_extended_attributes searching for node_id={node_id_str}, part_id={part_id_str}");
        let records_count = self.records.len();
        debug!("set_extended_attributes current pending records count: {records_count}");

        // Find the pending record with max version for this node/part in current transaction
        let mut max_version = -1;
        let mut target_index = None;

        for (index, record) in self.records.iter().enumerate() {
            let record_node = &record.node_id;
            let record_part = &record.part_id;
            let record_version = record.version;
            debug!("set_extended_attributes checking record[{index}]: node_id={record_node}, part_id={record_part}, version={record_version}");
            
            if record.node_id == node_id_str && record.part_id == part_id_str {
                debug!("set_extended_attributes found matching record at index {index} with version {record_version}");
                if record.version > max_version {
                    max_version = record.version;
                    target_index = Some(index);
                }
            }
        }

        let index = target_index.ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "No pending version found for node {} - extended attributes can only be set on files created in the current transaction", 
                node_id
            ))
        })?;

        // Check for special temporal override attributes and handle them separately
        let mut remaining_attributes = attributes;
        let mut min_override = None;
        let mut max_override = None;

        let attrs_count = remaining_attributes.len();
        diagnostics::log_info!("set_extended_attributes processing attributes for node {node_id_str} at index {index}", attrs_count: attrs_count);

        // Extract temporal overrides if present
        if let Some(min_val) = remaining_attributes.remove(crate::schema::duckpond::MIN_TEMPORAL_OVERRIDE) {
            diagnostics::log_info!("set_extended_attributes found min_temporal_override: {min_val}");
            match min_val.parse::<i64>() {
                Ok(timestamp) => {
                    min_override = Some(timestamp);
                    diagnostics::log_info!("set_extended_attributes parsed min_temporal_override timestamp: {timestamp}");
                },
                Err(e) => return Err(tinyfs::Error::Other(format!(
                    "Invalid min_temporal_override value '{}': {}", min_val, e
                ))),
            }
        }

        if let Some(max_val) = remaining_attributes.remove(crate::schema::duckpond::MAX_TEMPORAL_OVERRIDE) {
            diagnostics::log_info!("set_extended_attributes found max_temporal_override: {max_val}");
            match max_val.parse::<i64>() {
                Ok(timestamp) => {
                    max_override = Some(timestamp);
                    diagnostics::log_info!("set_extended_attributes parsed max_temporal_override timestamp: {timestamp}");
                },
                Err(e) => return Err(tinyfs::Error::Other(format!(
                    "Invalid max_temporal_override value '{}': {}", max_val, e
                ))),
            }
        }

        // Set the temporal override fields directly in the OplogEntry
        if let Some(min_ts) = min_override {
            diagnostics::log_info!("set_extended_attributes setting min_override to {min_ts} for node {node_id_str}");
            self.records[index].min_override = Some(min_ts);
        }
        if let Some(max_ts) = max_override {
            diagnostics::log_info!("set_extended_attributes setting max_override to {max_ts} for node {node_id_str}");
            self.records[index].max_override = Some(max_ts);
        }

        if min_override.is_some() || max_override.is_some() {
            diagnostics::log_info!("set_extended_attributes final record state for node {node_id_str} - temporal overrides set");
        }

        // Store remaining attributes as JSON (if any)
        if !remaining_attributes.is_empty() {
            let attributes_json = serde_json::to_string(&remaining_attributes)
                .map_err(|e| tinyfs::Error::Other(format!("Failed to serialize extended attributes: {}", e)))?;
            self.records[index].extended_attributes = Some(attributes_json);
        }
        
        Ok(())
    }

    // Dynamic node factory methods
    async fn create_dynamic_directory_node(&mut self, part_id: NodeID, name: String, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
        self.create_dynamic_directory(part_id, name, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn create_dynamic_file_node(&mut self, part_id: NodeID, name: String, file_type: tinyfs::EntryType, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
        self.create_dynamic_file(part_id, name, file_type, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }
}

/// Serialization utilities for Arrow IPC format
mod serialization {
    use super::*;
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use arrow::ipc::reader::StreamReader;

    /// Generic serialization function for Arrow IPC format
    pub fn serialize_to_arrow_ipc<T>(items: &[T]) -> Result<Vec<u8>, TLogFSError>
    where
        T: Clone + crate::schema::ForArrow + serde::Serialize,
    {
        let batch = serde_arrow::to_record_batch(&T::for_arrow(), &items.to_vec())?;

        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default();
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        writer.write(&batch)
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        writer.finish()
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;

        Ok(buffer)
    }

    /// Generic deserialization function for Arrow IPC format
    pub fn deserialize_from_arrow_ipc<T>(content: &[u8]) -> Result<Vec<T>, TLogFSError>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        let mut reader = StreamReader::try_new(std::io::Cursor::new(content), None)
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;

        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
            let entries: Vec<T> = serde_arrow::from_record_batch(&batch)?;
            Ok(entries)
        } else {
            Ok(Vec::new())
        }
    }

    // This function is no longer needed after Phase 2 abstraction consolidation
    // It was part of the old Record-based approach that caused double-nesting issues
}

/// Error handling utilities to reduce boilerplate
mod error_utils {
    use super::*;

    /// Convert Arrow error to TLogFSError
    pub fn arrow_error(e: impl std::fmt::Display) -> TLogFSError {
        TLogFSError::ArrowMessage(e.to_string())
    }

    /// Convert TLogFSError to TinyFSResult
    pub fn to_tinyfs_error(e: TLogFSError) -> tinyfs::Error {
	// @@@ No
        tinyfs::Error::Other(e.to_string())
    }
}

/// Query execution utilities to reduce DataFusion boilerplate
mod query_utils {
    use super::*;
    use datafusion::prelude::SessionContext;
    use uuid7;

    /// Execute a SQL query against a Delta table and return records
    pub async fn execute_sql_query(
        table: DeltaTable,
        sql_template: &str,
        params: &[String],
    ) -> Result<Vec<OplogEntry>, TLogFSError> {
        let ctx = SessionContext::new();
        let table_name = format!("query_table_{}", uuid7::uuid7().to_string().replace("-", ""));

        ctx.register_table(&table_name, Arc::new(table))
            .map_err(error_utils::arrow_error)?;

        // Format SQL with parameters
        let sql = if params.is_empty() {
            sql_template.replace("{table}", &table_name)
        } else {
            let mut formatted_sql = sql_template.replace("{table}", &table_name);
            for (i, param) in params.iter().enumerate() {
                formatted_sql = formatted_sql.replace(&format!("{{{}}}", i), param);
            }
            formatted_sql
        };

        let df = ctx.sql(&sql).await
            .map_err(error_utils::arrow_error)?;

        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                // Handle the "Empty batch" error gracefully - this is expected for new tables
                let error_msg = e.to_string();
                if error_msg.contains("Empty batch") {
                    Vec::new()
                } else {
                    return Err(error_utils::arrow_error(e));
                }
            }
        };

        let mut records = Vec::new();
        for batch in batches {
            let batch_records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)?;
            records.extend(batch_records);
        }

        Ok(records)
    }
}

/// Node creation utilities to reduce duplication
mod node_factory {
    use super::*;

    /// Create a file node
    pub fn create_file_node(
        node_id: NodeID,
        part_id: NodeID,
        state: State,
    ) -> Result<NodeType, tinyfs::Error> {
        let oplog_file = crate::file::OpLogFile::new(node_id, part_id, state);
        let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
        Ok(NodeType::File(file_handle))
    }

    /// Create a directory node
    pub fn create_directory_node(
        node_id: NodeID,
	part_id: NodeID,
        state: State,
    ) -> Result<NodeType, tinyfs::Error> {
	debug!("create directory {node_id}");
        let oplog_dir = super::super::directory::OpLogDirectory::new(node_id, part_id, state);
        let dir_handle = super::super::directory::OpLogDirectory::create_handle(oplog_dir);
        Ok(NodeType::Directory(dir_handle))
    }

    /// Create a symlink node with the given target
    pub fn create_symlink_node(
        node_id: NodeID,
        part_id: NodeID,
        state: State,
        _target: &std::path::Path, // @@@ WHY UNUSED?
    ) -> Result<NodeType, tinyfs::Error> {
        let oplog_symlink = super::super::symlink::OpLogSymlink::new(node_id, part_id, state);
        let symlink_handle = super::super::symlink::OpLogSymlink::create_handle(oplog_symlink);
        Ok(NodeType::Symlink(symlink_handle))
    }

    /// Create a node from an OplogEntry
    pub fn create_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        node_id: NodeID,
        part_id: NodeID,
        state: State,
    ) -> Result<NodeType, tinyfs::Error> {
        // Check if this is a dynamic node (has factory type)
        if let Some(factory_type) = &oplog_entry.factory {
            return create_dynamic_node_from_oplog_entry(oplog_entry, node_id, part_id, state, factory_type);
        }

        // Handle static nodes (traditional TLogFS nodes)
        match oplog_entry.file_type {
            tinyfs::EntryType::FileData | tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                let oplog_file = crate::file::OpLogFile::new(node_id, part_id, state);
                let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
                Ok(NodeType::File(file_handle))
            }
            tinyfs::EntryType::Directory => {
                let oplog_dir = super::super::directory::OpLogDirectory::new(
                    node_id,
		    part_id,
                    state,
                );
                let dir_handle = super::super::directory::OpLogDirectory::create_handle(oplog_dir);
                Ok(NodeType::Directory(dir_handle))
            }
            tinyfs::EntryType::Symlink => {
                let oplog_symlink = super::super::symlink::OpLogSymlink::new(node_id, part_id, state);
                let symlink_handle = super::super::symlink::OpLogSymlink::create_handle(oplog_symlink);
                Ok(NodeType::Symlink(symlink_handle))
            }
        }
    }

    /// Create a dynamic node from an OplogEntry with factory type
    fn create_dynamic_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        _node_id: NodeID,
        _part_id: NodeID,
        state: State,
        factory_type: &str,
    ) -> Result<NodeType, tinyfs::Error> {
        // Get configuration from the oplog entry
        let config_content = oplog_entry.content.as_ref()
            .ok_or_else(|| tinyfs::Error::Other(format!("Dynamic node missing configuration for factory '{}'", factory_type)))?;

        // All factories now require context - get OpLogPersistence
        let context = FactoryContext {
            state: state.clone(),
        };

        // Use context-aware factory registry to create the appropriate node type
        match oplog_entry.file_type {
            tinyfs::EntryType::Directory => {
                let dir_handle = FactoryRegistry::create_directory_with_context(factory_type, config_content, &context)?;
                Ok(NodeType::Directory(dir_handle))
            }
            _ => {
                let file_handle = FactoryRegistry::create_file_with_context(factory_type, config_content, &context)?;
                Ok(NodeType::File(file_handle))
            }
        }
    }
}

// fn show_dir(path: &str) {
//     // emit-rs is awful!!! :( TODO
//     let ents: Vec<_> = if let Ok(entries) = std::fs::read_dir(path) {
// 	entries.into_iter().map(|x| x.unwrap().file_name().to_string_lossy().to_string()).collect()
//     } else {
// 	vec![]
//     };
//     let ents = format!("{:?}", ents);
//     debug!("Directory {path} contents: {ents}");
// }    
