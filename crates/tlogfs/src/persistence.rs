use super::error::TLogFSError;
use super::schema::{OplogEntry, VersionedDirectoryEntry, OperationType, ForArrow};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use super::transaction_guard::TransactionGuard;
use crate::factory::{FactoryRegistry, FactoryContext};
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{EntryType, FS, NodeID, NodeType, Result as TinyFSResult, NodeMetadata, FileVersionInfo};
use std::collections::HashMap;
use std::sync::Arc;
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
    pub(crate) table: Option<deltalake::DeltaTable>,
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
    /// Creates a new OpLogPersistence instance
    ///
    /// This constructor initializes the Delta Lake table (creating if needed),
    /// sets up the DataFusion session context, and prepares all internal state
    /// for filesystem operations.
    pub async fn create(path: &str) -> Result<Self, TLogFSError> {
	Self::open_or_create(path, SaveMode::ErrorIfExists).await
    }
    
    pub async fn open(path: &str) -> Result<Self, TLogFSError> {
	Self::open_or_create(path, SaveMode::Append).await
    }

    async fn open_or_create(path: &str, mode: SaveMode) -> Result<Self, TLogFSError> {
        let delta = deltalake::open_table(path).await;

	let table = match (delta, mode) {
	    (Ok(table), SaveMode::Append) => Some(table),
	    (Ok(_), SaveMode::ErrorIfExists) => return Err(TLogFSError::PathExists { path: path.into() }),
	    (Err(_), SaveMode::Append) => {
		return Err(TLogFSError::Missing {})
	    },
	    (Err(e), SaveMode::ErrorIfExists) => {
		debug!("Delta error: {e}");
		None
	    },
	    _ => None,
	};	    

        let mut persistence = Self {
	    table,
	    path: path.into(),
	    fs: None,
	    state: None,
        };

	if mode == SaveMode::ErrorIfExists {
	    let tx = persistence.begin().await?;
	    tx.state().initialize_root_directory().await?;
	    tx.commit(None).await?;
	}

        Ok(persistence)
    }
    
    pub(crate) fn state(&self) -> State {
	self.state.clone().unwrap()
    }

    /// Begin a transaction and return a transaction guard
    ///
    /// This is the new transaction guard API that provides RAII-style transaction management
    pub async fn begin(&mut self) -> Result<TransactionGuard<'_>, TLogFSError> {
	let state = State(Arc::new(Mutex::new(InnerState::new(self.path.clone(), self.table.clone()))));
        state.begin_impl().await?;

	self.fs = Some(FS::new(state.clone()).await?);
	self.state = Some(state);

        debug!("Creating transaction guard for transaction");
        Ok(TransactionGuard::new(self))
    }

    /// Commit a transaction with metadata and return the committed version
    pub(crate) async fn commit(
        &mut self,
        metadata: Option<std::collections::HashMap<String, serde_json::Value>>
    ) -> Result<Option<u64>, TLogFSError> {
        self.state.as_mut().unwrap().commit_impl(metadata).await
    }
    
    /// Get the store path for this persistence layer
    pub fn store_path(&self) -> &str {
        &self.path
    }
}

impl State {
    async fn initialize_root_directory(&self) -> Result<(), TLogFSError> {
	self.0.lock().await.initialize_root_directory().await
    }

    async fn begin_impl(&self) -> Result<(), TLogFSError> {
	self.0.lock().await.begin_impl().await
    }

    async fn commit_impl(
        &mut self,
        metadata: Option<HashMap<String, serde_json::Value>>
    ) -> Result<Option<u64>, TLogFSError> {
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

    /// Thisis reading whole content @@@
    pub(crate) async fn load_file_content(
        &self,
        node_id: NodeID,
        part_id: NodeID
    ) -> Result<Vec<u8>, TLogFSError> {
	self.0.lock().await.load_file_content(node_id, part_id).await
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

    async fn create_directory_node(&self, node_id: NodeID, parent_node_id: NodeID) -> TinyFSResult<NodeType> {
	self.0.lock().await.create_directory_node(node_id, parent_node_id, self.clone()).await
    }

    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<NodeType> {
	self.0.lock().await.create_symlink_node(node_id, part_id, target, self.clone()).await
    }
    
    async fn create_dynamic_directory_node(&self, parent_node_id: NodeID, name: String, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
	self.0.lock().await.create_dynamic_directory_node(parent_node_id, name, factory_type, config_content).await
    }

    async fn create_dynamic_file_node(&self, parent_node_id: NodeID, name: String, file_type: EntryType, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
	self.0.lock().await.create_dynamic_file_node(parent_node_id, name, file_type, factory_type, config_content).await
    }

    async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Option<(String, Vec<u8>)>> {
	self.0.lock().await.get_dynamic_node_config(node_id, part_id).await
	    .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, (NodeID, EntryType)>> {
	self.0.lock().await.load_directory_entries(parent_node_id).await
    }

    async fn query_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<(NodeID, EntryType)>> {
	self.0.lock().await.query_directory_entry(parent_node_id, entry_name).await
    }

    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> TinyFSResult<()> {
	self.0.lock().await.update_directory_entry(parent_node_id, entry_name, operation).await
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
}

impl InnerState {
    fn new(path: String, table: Option<deltalake::DeltaTable>) -> Self {
	Self {
	    path,
	    table,
            records: Vec::new(),
            operations: HashMap::new(),
	}
    }

    /// Initialize the root directory without committing
    /// This should be called after creating a new persistence layer and starting a transaction,
    /// but before any calls to .root() that would trigger on-demand creation.
    async fn initialize_root_directory(&mut self) -> Result<(), TLogFSError> {
        let root_node_id = NodeID::root();
        let root_node_id_str = root_node_id.to_hex_string();

        diagnostics::debug!("Root initialization");

        // Check if root already exists in the committed data
        let records = match self.query_records(&root_node_id_str, Some(&root_node_id_str)).await {
            Ok(records) => {
                let count = records.len();
                diagnostics::debug!("initialize_root_directory: Found {count} existing records for root", count: count);
                records
            },
            Err(TLogFSError::NodeNotFound { .. }) | Err(TLogFSError::Missing) => {
                // Root doesn't exist yet - this is expected for first-time initialization
                diagnostics::debug!("initialize_root_directory: No existing root directory found, will create new one");
                Vec::new()
            },
            Err(e) => {
                // Real system error - Delta Lake corruption, IO failure, etc.
                // Don't mask these as they indicate serious problems that need investigation
                diagnostics::error!("CRITICAL: Failed to query root directory existence due to system error: {error}", error: e);
                return Err(e);
            }
        };

        if !records.is_empty() {
            // Root directory already exists in committed data, nothing to do
            diagnostics::debug!("initialize_root_directory: Root directory already exists in committed data, skipping");
            return Ok(());
        }

        // Check if root already exists in pending transactions
            let root_exists_in_pending = self.records.iter().any(|entry| {
                entry.node_id == root_node_id_str && entry.part_id == root_node_id_str
            });

            if root_exists_in_pending {
                // Root directory already exists in pending transaction, nothing to do
                diagnostics::debug!("initialize_root_directory: Root directory already exists in pending records, skipping");
                return Ok(());
            }

        diagnostics::debug!("initialize_root_directory: Creating new root directory with direct commit");

        // Create root directory using direct TLogFS commit (no transaction guard needed for bootstrap)
        let now = Utc::now().timestamp_micros();
        let empty_entries: Vec<VersionedDirectoryEntry> = Vec::new();
        let content = self.serialize_directory_entries(&empty_entries)?;

        let root_entry = OplogEntry::new_inline(
            root_node_id_str.clone(), // Root directory is its own partition
            root_node_id_str.clone(),
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

        info!("Started transaction");
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
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                entry_type,  // Use provided entry type instead of hardcoded FileData
                now,
                0, // Placeholder - actual version assigned by Delta Lake transaction log
                result.content,
            )
        } else {
            // Large file: content already stored, just create OplogEntry with SHA256
            let size = result.size;
            info!("Storing large file: {size} bytes");
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
                        part_id.to_hex_string(),
                        node_id.to_hex_string(),
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
                        part_id.to_hex_string(),
                        node_id.to_hex_string(),
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
            part_id.to_hex_string(),
            node_id.to_hex_string(),
            entry_type, // Use the provided entry type instead of hardcoded FileData
            now,
            0, // Placeholder - actual version assigned by Delta Lake transaction log
            content.to_vec(),
        );

        self.records.push(entry);
        Ok(())
    }

    /// Get the next version number for a specific node (current max + 1)
    async fn get_next_version_for_node(&self, node_id: NodeID, part_id: NodeID) -> Result<i64, TLogFSError> {
        let part_id_str = part_id.to_hex_string();
        let node_id_str = node_id.to_hex_string();

        // Debug logging
        diagnostics::debug!("get_next_version_for_node called for node_id={node_id_str}, part_id={part_id_str}", node_id_str: node_id_str, part_id_str: part_id_str);

        // Query all records for this node and find the maximum version
        match self.query_records(&part_id_str, Some(&node_id_str)).await {
            Ok(records) => {
                let record_count = records.len();
                diagnostics::debug!("get_next_version_for_node found {record_count} existing records", record_count: record_count);

                let max_version = records.iter()
                    .map(|r| r.version)
                    .max()
                    .unwrap_or(0);
                let next_version = max_version + 1;

                diagnostics::debug!("get_next_version_for_node: max_version={max_version}, returning next_version={next_version}", max_version: max_version, next_version: next_version);
                Ok(next_version)
            }
            Err(e) => {
                let error_str = format!("{:?}", e);
                diagnostics::debug!("get_next_version_for_node query failed: {error}", error: error_str);
                // Critical error: cannot determine proper version sequence
                Err(TLogFSError::ArrowMessage(format!("Cannot determine next version for node {}: query failed: {}", node_id_str, e)))
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

        // Store the FileSeries using the appropriate size strategy
        use crate::large_files::should_store_as_large_file;

        if should_store_as_large_file(content) {
            // Store as large FileSeries with external file storage
            let sha256 = super::schema::compute_sha256(content);
            let size = content.len() as u64;
            let now = Utc::now().timestamp_micros();

            // Write content to external storage
            let large_file_path = crate::large_files::large_file_path(&self.path, &sha256).await
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to determine large file path: {}", e)))?;

            // Ensure parent directory exists
            if let Some(parent) = large_file_path.parent() {
                tokio::fs::create_dir_all(parent).await
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create large file directory: {}", e)))?;
            }

            // Write and sync the file
            use tokio::fs::OpenOptions;
            use tokio::io::AsyncWriteExt;

            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&large_file_path).await
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create large file: {}", e)))?;

            file.write_all(content).await
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to write large file content: {}", e)))?;

            file.sync_all().await
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to sync large file: {}", e)))?;

            let large_file_path_str = large_file_path.to_string_lossy().to_string();
            diagnostics::info!("Stored large FileSeries: {size} bytes at {large_file_path}", size: size, large_file_path: large_file_path_str);

            let entry = OplogEntry::new_large_file_series(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                now,
                next_version, // Use proper version counter
                sha256,
                size as i64, // Cast to i64 to match Delta Lake protocol
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

            diagnostics::debug!("store_file_series_from_parquet - storing as small FileSeries with {content_size} bytes content", content_size: content_size);

            let entry = OplogEntry::new_file_series(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                now,
                next_version, // Use proper version counter
                content.to_vec(),
                global_min,
                global_max,
                extended_attrs,
            );

            let entry_content_size = entry.content.as_ref().map(|c| c.len()).unwrap_or(0);
            diagnostics::debug!("store_file_series_from_parquet - created OplogEntry with content size: {entry_content_size}", entry_content_size: entry_content_size);

            self.records.push(entry);
        }

        Ok((global_min, global_max))
    }

    /// Load file content using size-based strategy
    /// Thisis reading whole content @@@
    pub async fn load_file_content(
        &self,
        node_id: NodeID,
        part_id: NodeID
    ) -> Result<Vec<u8>, TLogFSError> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        let records = self.query_records(&part_id_str, Some(&node_id_str)).await?;

        if let Some(record) = records.first() {
            if record.is_large_file() {
                // Large file: read from separate storage
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

                let content = tokio::fs::read(&large_file_path).await
                    .map_err(|e| TLogFSError::LargeFileNotFound {
                        sha256: sha256.clone(),
                        path: large_file_path.display().to_string(),
                        source: e,
                    })?;

                Ok(content)
            } else {
                // Small file: content stored inline
                record.content.clone().ok_or_else(|| TLogFSError::ArrowMessage(
                    "Small file entry missing content".to_string()
                ))
            }
        } else {
            Err(TLogFSError::NodeNotFound {
                path: std::path::PathBuf::from(format!("File {} not found", node_id_str))
            })
        }
    }

    /// Commit pending records to Delta Lake
    async fn commit_impl(
        &mut self,
        metadata: Option<HashMap<String, serde_json::Value>>
    ) -> Result<Option<u64>, TLogFSError> {
        self.flush_directory_operations().await?;

        let records = std::mem::take(&mut self.records);

        if records.is_empty() {
            // No write operations to commit - this is a read-only transaction
            info!("Committing read-only transaction (no write operations)");
            return Ok(None);
        }

        let count = records.len();
        info!("Committing {count} operations");

        // Convert records to RecordBatch
        let batches = vec![
	    serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &records)?,
	];

	let mut write_op = if self.table.is_none() {
            let ops = DeltaOps::try_from_uri(&	self.path).await?;
            ops.write(batches).with_save_mode(SaveMode::ErrorIfExists)
	} else {
            let ops = DeltaOps(self.table.as_ref().unwrap().clone());
            ops.write(batches).with_save_mode(SaveMode::Append)
	};

        // Add commit metadata
        if let Some(metadata_map) = metadata {
            let properties = CommitProperties::default()
                .with_metadata(metadata_map.into_iter());
            write_op = write_op.with_commit_properties(properties);
        }

        let table = write_op.await?;

        let version = table.version() as u64;
        info!("Transaction committed to version {version}");

	self.table = None;
	self.records.clear();
	self.operations.clear();
	
        Ok(Some(version))
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
    pub async fn get_commit_metadata(&self, _version: u64) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        // Get Delta Lake history
        let history = self.get_commit_history(None).await?;  // Get full history

        // Look through all commits to find one that contains steward metadata
        // for the target version or close to it
        for commit in history.iter() {
            // Check if this commit has steward metadata
            if commit.info.contains_key("steward_tx_args") {
                return Ok(Some(commit.info.clone()));
            }
        }

        // If no steward metadata found, return None - recovery must fail for missing metadata
        Ok(None)
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
        let node_id_debug = node_id.to_hex_string();
        let part_id_debug = part_id.to_hex_string();
        diagnostics::debug!("store_file_content_ref_transactional called for node_id={node_id}, part_id={part_id}", node_id: node_id_debug, part_id: part_id_debug);

        // Create OplogEntry from content reference
        let now = chrono::Utc::now().timestamp_micros();
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        // Get proper version number for this node
        // Check if there's already an entry for this node in this transaction
        let version = {
	    // THIS pattern REPEATS??
            let existing_entry = self.records.iter()
		.find(|e| e.node_id == node_id_str && e.part_id == part_id_str);

            if let Some(existing) = existing_entry {
                // Check if this is a placeholder entry (version 0) vs real content
                if existing.version == 0 {
                    // This is the first time content is being added - bump to version 1
		    // ??? always 1 right?
                    self.get_next_version_for_node(node_id, part_id).await?
                } else {
                    // Replacing existing content - preserve the same version
                    existing.version
                }
            } else {
                // New entry - calculate next version
                self.get_next_version_for_node(node_id, part_id).await?
            }
        };

        // Create filter copies before strings get moved
        let node_id_filter = node_id_str.clone();
        let part_id_filter = part_id_str.clone();

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
                                    part_id_str,
                                    node_id_str,
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
                            part_id_str,
                            node_id_str,
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
                                    part_id_str,
                                    node_id_str,
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
                            part_id_str,
                            node_id_str,
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
            existing_entry.part_id == part_id_filter && existing_entry.node_id == node_id_filter
        });

        if let Some(index) = existing_index {
            // Replace existing entry (content changes, version stays the same within transaction)
            self.records.insert(index, entry);
        } else {
            // No existing entry - add new entry with version 1 (??)
            debug!("Adding new pending entry for node {node_id_filter} with version {entry_version}", entry_version: entry.version);
            self.records.push(entry);
        }

        debug!("Stored file content reference for node {node_id_filter}");
        Ok(())
    }

    /// Query directory entries for a parent node
    async fn query_directory_entries(&self, parent_node_id: NodeID) -> Result<Vec<VersionedDirectoryEntry>, TLogFSError> {
        let part_id_str = parent_node_id.to_hex_string();
        let records = self.query_records(&part_id_str, None).await?;

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
    async fn query_single_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<VersionedDirectoryEntry>, TLogFSError> {
        // Check pending directory operations first
            if let Some(operations) = self.operations.get(&parent_node_id) {
                if let Some(operation) = operations.get(entry_name) {
                    match operation {
                        DirectoryOperation::InsertWithType(node_id, node_type) => {
                            return Ok(Some(VersionedDirectoryEntry::new(
                                entry_name.to_string(),
                                node_id.to_hex_string(),
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
                                node_id.to_hex_string(),
                                OperationType::Insert,
                                node_type.clone(),
                            )));
                        }
                    }
                }
            }

        // Query committed records
        let part_id_str = parent_node_id.to_hex_string();
        let records = self.query_records(&part_id_str, None).await?;

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

        for (parent_node_id, operations) in pending_dirs {
            let mut versioned_entries = Vec::new();

            for (entry_name, operation) in operations {
                match operation {
                    DirectoryOperation::InsertWithType(child_node_id, node_type) => {
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            entry_name,
                            child_node_id.to_hex_string(),
                            OperationType::Insert,
                            node_type,
                        ));
                    }
                    DirectoryOperation::DeleteWithType(node_type) => {
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            entry_name,
                            "".to_string(),
                            OperationType::Delete,
                            node_type,
                        ));
                    }
                    DirectoryOperation::RenameWithType(new_name, child_node_id, node_type) => {
                        // Delete the old entry
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            entry_name,
                            "".to_string(),
                            OperationType::Delete,
                            node_type.clone(),
                        ));
                        // Insert with new name
                        versioned_entries.push(VersionedDirectoryEntry::new(
                            new_name,
                            child_node_id.to_hex_string(),
                            OperationType::Insert,
                            node_type,
                        ));
                    }
                }
            }

            // Create directory record for parent directory contents
            let content_bytes = self.serialize_directory_entries(&versioned_entries)?;
            let part_id_str = parent_node_id.to_hex_string();
            let directory_node_id_str = parent_node_id.to_hex_string();

            let now = Utc::now().timestamp_micros();
            let record = OplogEntry::new_inline(
                part_id_str,
                directory_node_id_str,
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
        parent_id: NodeID,
	name: String,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID, TLogFSError> {
        let node_id = NodeID::generate();
        let part_id = parent_id.to_hex_string();
        let now = Utc::now().timestamp_micros();

        // Create dynamic directory OplogEntry
        let entry = OplogEntry::new_dynamic_directory(
            part_id,
	    
            node_id.to_hex_string(),
            now,
	    1,  // @@@ MaDE THIS UP
            factory_type,
            config_content,
        );

        // Add to pending records
        self.records.push(entry);

        // Add directory operation for parent
        let directory_op = DirectoryOperation::InsertWithType(node_id, tinyfs::EntryType::Directory);
        self.update_directory_entry(parent_id, &name, directory_op).await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        Ok(node_id)
    }

    /// Create a dynamic file node with factory configuration
    pub async fn create_dynamic_file(
        &mut self,
        parent_id: NodeID,
        name: String,
        file_type: tinyfs::EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID, TLogFSError> {
        let node_id = NodeID::generate();
        let part_id = parent_id.to_hex_string();
        let now = Utc::now().timestamp_micros();

        // Create dynamic file OplogEntry
        let entry = OplogEntry::new_dynamic_file(
            part_id,
            node_id.to_hex_string(),
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
        self.update_directory_entry(parent_id, &name, directory_op).await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        Ok(node_id)
    }

    /// Get dynamic node configuration if the node is dynamic
    /// Uses the same query pattern as the rest of the persistence layer
    pub async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> Result<Option<(String, Vec<u8>)>, TLogFSError> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        // First check pending records (for nodes created in current transaction)
        for record in &self.records {
            if record.node_id == node_id_str {
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
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await?;

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
    async fn query_records(&self, part_id: &str, node_id: Option<&str>) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Step 1: Get committed records from Delta Lake
	let committed_records = match self.table.clone() {
            Some(table) => {
                let sql = if node_id.is_some() {
                    "SELECT * FROM {table} WHERE part_id = '{0}' AND node_id = '{1}' ORDER BY timestamp DESC"
                } else {
                    "SELECT * FROM {table} WHERE part_id = '{0}' ORDER BY timestamp DESC"
                };
                let params = if let Some(node_id_filter) = node_id {
                    vec![part_id, node_id_filter]
                } else {
                    vec![part_id]
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
                    record.part_id == part_id &&
                    (node_id.is_none() || Some(record.node_id.as_str()) == node_id)
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

        debug!("LOAD_NODE: load_node called with node_id={node_id_str}, part_id={part_id_str}");

        // Query Delta Lake for the most recent record for this node
        let records = match self.query_records(&part_id_str, Some(&node_id_str)).await {
            Ok(records) => {
                let record_count = records.len();
                debug!("LOAD_NODE: query_records returned {record_count} records");
                records
            }
            Err(e) => {
                let error_msg = e.to_string();
                debug!("LOAD_NODE: query_records failed with error: {error_msg}");
                return Err(error_utils::to_tinyfs_error(e));
            }
        };

        if let Some(record) = records.first() {
            // record is already an OplogEntry - no need to deserialize again
            debug!("LOAD_NODE: Using record directly as OplogEntry");

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
                debug!("LOAD_NODE: Found node in pending records");

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
                    debug!("TRANSACTION: store_node() - empty file content for {node_hex}, no existing large file entry found");
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
            part_id.to_hex_string(),
            node_id.to_hex_string(),
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
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(error_utils::to_tinyfs_error)?;

        Ok(!records.is_empty())
    }

    async fn load_directory_entries(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, (NodeID, EntryType)>> {
        let all_entries = self.query_directory_entries(parent_node_id).await
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
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
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
            Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("Symlink {} not found", node_id_str))))
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

    async fn create_directory_node(&self, node_id: NodeID, parent_node_id: NodeID, state: State) -> TinyFSResult<NodeType> {
        node_factory::create_directory_node(node_id, parent_node_id, state)
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
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
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
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        debug!("metadata_u64: querying node_id={node_id_str}, part_id={part_id_str}, name={name}");

        // Query Delta Lake for the most recent record for this node using the correct partition
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
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

    async fn query_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<(NodeID, EntryType)>> {
        match self.query_single_directory_entry(parent_node_id, entry_name).await {
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
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> TinyFSResult<()> {
        // Enhanced directory coalescing - accumulate operations with node types for batch processing
        let dir_ops = self.operations.entry(parent_node_id).or_insert_with(HashMap::new);

        // All operations must now include node type - no legacy conversion
        dir_ops.insert(entry_name.to_string(), operation);
        Ok(())
    }

    // Versioning operations implementation
    async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Vec<tinyfs::FileVersionInfo>> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        let mut records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(error_utils::to_tinyfs_error)?;

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
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();

        let mut records = self.query_records(&part_id_str, Some(&node_id_str)).await
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

    // Dynamic node factory methods
    async fn create_dynamic_directory_node(&mut self, parent_node_id: NodeID, name: String, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
        self.create_dynamic_directory(parent_node_id, name, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn create_dynamic_file_node(&mut self, parent_node_id: NodeID, name: String, file_type: tinyfs::EntryType, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
        self.create_dynamic_file(parent_node_id, name, file_type, factory_type, config_content)
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
        params: &[&str],
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
        parent_node_id: NodeID,
        state: State,
    ) -> Result<NodeType, tinyfs::Error> {
        let node_id_str = node_id.to_hex_string();
        let parent_node_id_str = parent_node_id.to_hex_string();
        let oplog_dir = super::super::directory::OpLogDirectory::new(node_id_str, parent_node_id_str, state);
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
                    oplog_entry.node_id.clone(),
                    part_id.to_hex_string(),
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

