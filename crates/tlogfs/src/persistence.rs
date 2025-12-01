use super::directory::OpLogDirectory;
use super::error::TLogFSError;
use super::schema::{DirectoryEntry, ForArrow, OplogEntry};
use super::symlink::OpLogSymlink;
use super::transaction_guard::TransactionGuard;
use crate::factory::{FactoryContext, FactoryRegistry};
use crate::txn_metadata::{PondTxnMetadata, PondUserMetadata};
use arrow::array::DictionaryArray;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::UInt16Type;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::execution::context::{SessionConfig, SessionContext};
use deltalake::kernel::CommitInfo;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable};
use log::{debug, info, warn};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{
    EntryType, FS, FileID, FileVersionInfo, Node, NodeID, NodeMetadata, NodeType, PartID,
    Result as TinyFSResult, persistence::PersistenceLayer,
};
use tokio::sync::Mutex;

pub struct OpLogPersistence {
    pub(crate) path: PathBuf,
    pub(crate) table: DeltaTable,
    pub(crate) fs: Option<FS>,
    pub(crate) state: Option<State>,
    pub(crate) last_txn_seq: i64, // Track last committed transaction sequence for validation
}

#[derive(Clone)]
pub enum DirectoryOperation {
    /// Insert operation that includes node type (only supported operation)
    InsertWithType(NodeID, EntryType),
    /// Delete operation with node type for consistency
    DeleteWithType(EntryType),
    /// Rename operation with node type
    RenameWithType(String, NodeID, EntryType), // old_name, new_node_id, node_type
}

pub struct InnerState {
    path: PathBuf,
    table: DeltaTable,        // The Delta table for this transaction
    records: Vec<OplogEntry>, // @@@ LINEAR SEARCH
    operations: HashMap<FileID, HashMap<String, DirectoryOperation>>, // @@@ SUS
    created_directories: HashSet<FileID>, // Track mkdir operations separately
    session_context: Arc<SessionContext>,
    txn_seq: i64,
}

#[derive(Clone)]
pub struct State {
    inner: Arc<Mutex<InnerState>>,
    /// TinyFS ObjectStore instance - shared with SessionContext
    object_store: Arc<tokio::sync::OnceCell<Arc<crate::tinyfs_object_store::TinyFsObjectStore>>>,
    /// Transaction-scoped cache for dynamic nodes
    dynamic_node_cache: Arc<std::sync::Mutex<HashMap<DynamicNodeKey, Node>>>,
    /// Template variables for CLI variable expansion - mutable shared state
    template_variables: Arc<std::sync::Mutex<HashMap<String, serde_json::Value>>>,
    /// Cache for TableProvider instances to avoid repeated ListingTable creation and schema inference
    /// Key: (node_id, part_id, version_selection) -> TableProvider with temporal filtering
    table_provider_cache: Arc<
        std::sync::Mutex<HashMap<TableProviderKey, Arc<dyn datafusion::catalog::TableProvider>>>,
    >,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DynamicNodeKey {
    pub parent_id: PartID,
    pub entry_name: String,
}

impl DynamicNodeKey {
    #[must_use]
    pub fn new(parent_id: PartID, entry_name: String) -> Self {
        Self {
            parent_id,
            entry_name,
        }
    }
}

/// Cache key for TableProvider instances in TLogFS queries
/// Combines node_id, part_id, and version selection to uniquely identify table configurations
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TableProviderKey {
    pub id: FileID,
    pub version_selection: crate::file_table::VersionSelection,
}

impl TableProviderKey {
    #[must_use]
    pub fn new(
        id: FileID,
        version_selection: crate::file_table::VersionSelection,
    ) -> Self {
        Self {
            id,
            version_selection,
        }
    }
}

impl OpLogPersistence {
    /// Get the Delta table for query operations
    #[must_use]
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Get the last committed transaction sequence number
    ///
    /// This is the authoritative source for the current transaction sequence.
    /// Use this to determine the next sequence number: `last_txn_seq() + 1`
    #[must_use]
    pub fn last_txn_seq(&self) -> i64 {
        self.last_txn_seq
    }

    /// Creates a new OpLogPersistence instance with a new table and initializes root
    pub async fn create<P: AsRef<Path>>(
        path: P,
        metadata: PondUserMetadata,
    ) -> Result<Self, TLogFSError> {
        debug!("create called with path: {:?}", path.as_ref());

        Self::open_or_create(path, true, Some(metadata)).await
    }

    /// Test-only helper: Create a new pond with synthetic metadata.
    #[cfg(test)]
    pub async fn create_test(path: &str) -> Result<Self, TLogFSError> {
        Self::create(
            path,
            PondUserMetadata::new(vec!["test".to_string(), "create".to_string()]),
        )
        .await
    }

    /// Test-only helper: Begin a transaction with automatic sequence numbering.
    #[cfg(test)]
    pub async fn begin_test(&mut self) -> Result<TransactionGuard<'_>, TLogFSError> {
        let next_seq = self.last_txn_seq + 1;
        let metadata =
            PondTxnMetadata::new(next_seq, PondUserMetadata::new(vec!["test".to_string()]));
        self.begin_write(&metadata).await // Tests are write transactions
    }

    /// Opens an existing OpLogPersistence instance
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, TLogFSError> {
        debug!("open called with path: {:?}", path.as_ref());

        Self::open_or_create(path, false, None).await
    }

    /// Create an empty Delta table structure for restoration
    /// This creates the table schema but does NOT initialize the root directory.
    /// Used when restoring from bundles - the first bundle will write transaction #1
    /// which initializes the root directory.
    pub async fn create_empty<P: AsRef<Path>>(path: P) -> Result<Self, TLogFSError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!(
            "Creating empty table structure for restoration at: {}",
            path_str
        );

        // Create the Delta table structure
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingStatsColumns".to_string(),
            Some("part_id,name,parent_id,entry_type,file_type,timestamp,version,sha256,size,min_event_time,max_event_time,min_override,max_override,extended_attributes,factory,txn_seq".to_string())
        )]
        .into_iter()
        .collect();

        let table = DeltaOps::try_from_uri(path_str.clone())
            .await?
            .create()
            .with_columns(OplogEntry::for_delta())
            .with_partition_columns(["part_id"])
            .with_configuration(config)
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;

        debug!("Created empty Delta table at {}", path_str);

        Ok(Self {
            table,
            path: path.as_ref().to_path_buf(),
            fs: None,
            state: None,
            last_txn_seq: 0, // No transactions yet - bundles will provide them
        })
    }

    /// @@@ UNCLEAR this should not be public
    pub async fn open_or_create<P: AsRef<Path>>(
        path: P,
        create_new: bool,
        root_metadata: Option<PondUserMetadata>,
    ) -> Result<Self, TLogFSError> {
        // Enable RUST_LOG logging configuration for tests
        let _ = env_logger::try_init();

        let mode = if create_new {
            SaveMode::ErrorIfExists
        } else {
            SaveMode::Append
        };

        // First try to open existing table
        let path_str = path.as_ref().to_string_lossy().to_string();
        let table = match deltalake::open_table(path_str.clone()).await {
            Ok(existing_table) => {
                debug!("Found existing table at {}", &path_str);
                existing_table
            }
            Err(open_err) => {
                debug!(
                    "no existing table at {}, will create: {open_err}",
                    &path_str
                );
                // Table doesn't exist, create it
                // Configure stats collection to skip the binary 'content' column to avoid warnings
                let config: HashMap<String, Option<String>> = vec![(
                    "delta.dataSkippingStatsColumns".to_string(),
		    // @@@ Awful
                    Some("part_id,name,parent_id,entry_type,file_type,timestamp,version,sha256,size,min_event_time,max_event_time,min_override,max_override,extended_attributes,factory,txn_seq".to_string())
                )]
                .into_iter()
                .collect();

                let create_result = DeltaOps::try_from_uri(path_str.clone())
                    .await?
                    .create()
                    .with_columns(OplogEntry::for_delta())
                    .with_partition_columns(["part_id"])
                    .with_configuration(config)
                    .with_save_mode(mode)
                    .await;

                match create_result {
                    Ok(table) => table,
                    Err(create_err) => {
                        debug!("failed to create table at {}: {create_err}", &path_str);
                        return Err(create_err.into());
                    }
                }
            }
        };

        let mut persistence = Self {
            table: table.clone(),
            path: path.as_ref().to_path_buf(),
            fs: None,
            state: None,
            last_txn_seq: 0, // Will be updated below
        };

        // Initialize root directory ONLY when creating a new pond
        if create_new {
            debug!("Initializing root directory for new pond at {}", &path_str);

            let metadata = PondTxnMetadata::new(1, root_metadata.expect("metadata when new"));

            let mut tx = persistence.begin_write(&metadata).await?;

            // Actually create the root directory entry
            // IMPORTANT: initialize_root_directory adds entry to records AFTER begin_impl cleared them
            tx.state()
                .map_err(|e| {
                    TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to get state: {}", e)))
                })?
                .initialize_root_directory()
                .await?;

            _ = tx.commit().await.map_err(TLogFSError::TinyFS)?;
        } else {
            // Opening existing table - load last_txn_seq from Delta commit metadata
            // This is the authoritative source (not the control table, which is Steward's)
            let hist = table.history(Some(1)).await?;
            if let Some(last_commit) = hist.first() {
                if let Some(txn_seq) = PondTxnMetadata::extract_txn_seq(&last_commit.info) {
                    persistence.last_txn_seq = txn_seq;
                    debug!(
                        "Loaded last_txn_seq={} from Delta metadata at {}",
                        txn_seq, &path_str,
                    );
                } else {
                    // No txn_seq in metadata - this might be a pond created for restoration
                    // or an old pond without metadata. Start at 0.
                    persistence.last_txn_seq = 0;
                    debug!(
                        "No txn_seq found in Delta metadata at {}, starting at 0",
                        &path_str,
                    );
                }
            } else {
                // No history yet - brand new table
                persistence.last_txn_seq = 0;
                debug!("No Delta history at {}, starting at 0", &path_str);
            }
        }

        Ok(persistence)
    }

    pub(crate) fn state(&self) -> Result<State, TLogFSError> {
        self.state.clone().ok_or(TLogFSError::Missing)
    }

    /// Get commit history from Delta table via State
    pub async fn get_commit_history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<CommitInfo>, TLogFSError> {
        self.table.history(limit).await.map_err(TLogFSError::Delta)
    }

    /// Get commit metadata for a specific version
    pub async fn get_commit_metadata(
        &self,
        ts: i64,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        let history = self.get_commit_history(Some(1)).await?;
        for hist in history.iter() {
            if hist.timestamp == Some(ts) {
                return Ok(Some(hist.info.clone()));
            }
        }
        Ok(None)
    }

    /// Get commit metadata for a specific version
    pub async fn get_last_commit_metadata(
        &self,
    ) -> Result<Option<HashMap<String, serde_json::Value>>, TLogFSError> {
        let history = self.get_commit_history(Some(1)).await?;
        Ok(history.first().as_ref().map(|x| x.info.clone()))
    }

    /// Begin a write transaction - allocates next sequence number
    ///
    /// Write transactions must use sequence number last_txn_seq + 1 and will
    /// commit changes to Delta Lake when committed.
    ///
    /// # Arguments
    /// * `txn_seq` - Transaction sequence number (must be last_txn_seq + 1)
    /// * `metadata` - Transaction metadata (txn_id, CLI args, key/value params)
    ///
    /// # Returns
    /// TransactionGuard that must be explicitly committed or will auto-rollback on drop
    pub async fn begin_write(
        &mut self,
        metadata: &PondTxnMetadata,
    ) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Write transactions must be strictly increasing
        if metadata.txn_seq != self.last_txn_seq + 1 {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "Write transaction sequence must be exactly +1: attempted txn_seq={} but last_txn_seq={} (expected {})",
                    metadata.txn_seq,
                    self.last_txn_seq,
                    self.last_txn_seq + 1
                ),
            });
        }

        self.begin_impl(metadata, true).await
    }

    /// Begin a read transaction - reuses last write sequence for read atomicity
    ///
    /// Read transactions use the last committed write sequence to get a consistent
    /// snapshot. They don't modify data or increment sequences.
    ///
    /// # Arguments
    /// * `txn_seq` - Transaction sequence number (must equal last_txn_seq)
    /// * `metadata` - Transaction metadata (txn_id, CLI args, key/value params)
    ///
    /// # Returns
    /// TransactionGuard that should NOT be committed (drop it to rollback)
    pub async fn begin_read(
        &mut self,
        txn_meta: &PondTxnMetadata,
    ) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Read transactions reuse the last write sequence
        if txn_meta.txn_seq != self.last_txn_seq {
            return Err(TLogFSError::Transaction {
                message: format!(
                    "Read transaction must use last write sequence: attempted txn_seq={} but last_txn_seq={}",
                    txn_meta.txn_seq, self.last_txn_seq
                ),
            });
        }

        self.begin_impl(txn_meta, false).await
    }

    /// Internal implementation for begin - shared by begin_write and begin_read
    async fn begin_impl(
        &mut self,
        metadata: &PondTxnMetadata,
        is_write: bool,
    ) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Prevent multiple concurrent transactions on the same
        // OpLogPersistence This is a programming error that indicates
        // improper transaction guard lifecycle management.
        if self.state.is_some() || self.fs.is_some() {
            panic!("🚨 TRANSACTION GUARD VIOLATION");
        }

        let state = State {
            inner: Arc::new(Mutex::new(
                InnerState::new(self.path.clone(), self.table.clone(), metadata.txn_seq).await?,
            )),
            object_store: Arc::new(tokio::sync::OnceCell::new()),
            dynamic_node_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            template_variables: Arc::new(std::sync::Mutex::new(HashMap::new())),
            table_provider_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
        };

        // Complete SessionContext setup with ObjectStore registration
        {
            let inner = state.inner.lock().await;
            inner.complete_session_setup(&state).await?;
        }

        state.begin_impl().await?;

        self.fs = Some(FS::new(state.clone()).await?);
        self.state = Some(state);

        Ok(TransactionGuard::new(self, metadata, is_write))
    }

    /// Commit a transaction with metadata and return the committed version
    pub(crate) async fn commit(
        &mut self,
        metadata: PondTxnMetadata,
    ) -> Result<Option<()>, TLogFSError> {
        let new_seq = metadata.txn_seq;
        self.fs = None;
        let res = self
            .state
            .take()
            .ok_or(TLogFSError::Missing)?
            .commit_impl(metadata, self.table.clone())
            .await?;
        
        // Reload the table from disk to pick up the committed changes
        // This ensures subsequent transactions see the new data
        self.table = deltalake::open_table(self.path.to_string_lossy().to_string()).await?;
        self.last_txn_seq = new_seq;

        Ok(res)
    }

    /// Get the store path for this persistence layer
    #[must_use]
    pub fn store_path(&self) -> &PathBuf {
        &self.path
    }

    /// Query OpLog records by transaction sequence for testing
    ///
    /// Returns tuples of (node_id_hex, part_id_hex, version) for verification purposes.
    /// This is a simplified testing API that doesn't return full OplogEntry structs.
    ///
    /// # Arguments
    /// * `txn_seq` - Optional transaction sequence number to filter by
    ///
    /// # Returns
    /// Vector of tuples containing (node_id, part_id, version) as hex strings and i64
    pub async fn query_oplog_by_txn_seq(
        &self,
        txn_seq: Option<i64>,
    ) -> Result<Vec<(String, String, i64)>, TLogFSError> {
        // Create SessionContext and register the oplog table
        let ctx = SessionContext::new();
        _ = ctx
            .register_table("oplog", Arc::new(self.table.clone()))
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to register oplog table: {}", e))
            })?;

        // Build query - select node_id, part_id, version
        let sql = if let Some(seq) = txn_seq {
            format!(
                "SELECT node_id, part_id, version FROM oplog WHERE txn_seq = {} ORDER BY node_id, version",
                seq
            )
        } else {
            "SELECT node_id, part_id, version FROM oplog ORDER BY node_id, version".to_string()
        };

        // Execute query
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to execute query: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect results: {}", e)))?;

        let mut records = Vec::new();

        for batch in batches.iter() {
            // Helper to extract string from either StringArray or DictionaryArray
            let get_string_value = |col_idx: usize,
                                    row_idx: usize|
             -> Result<String, TLogFSError> {
                let column = batch.column(col_idx);

                // Try dictionary-encoded first (most common in DataFusion)
                if let Some(dict_array) = column
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt16Type>>()
                {
                    let values = dict_array
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            TLogFSError::ArrowMessage("Failed to get dictionary values".to_string())
                        })?;
                    let key = dict_array.keys().value(row_idx);
                    return Ok(values.value(key as usize).to_string());
                }

                // Fall back to plain string array
                if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    return Ok(string_array.value(row_idx).to_string());
                }

                Err(TLogFSError::ArrowMessage(format!(
                    "Unsupported column type: {:?}",
                    column.data_type()
                )))
            };

            let version_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    TLogFSError::ArrowMessage(format!(
                        "Failed to downcast version, actual type: {:?}",
                        batch.column(2).data_type()
                    ))
                })?;

            for row_idx in 0..batch.num_rows() {
                let node_id = get_string_value(0, row_idx)?;
                let part_id = get_string_value(1, row_idx)?;
                let version = version_array.value(row_idx);

                records.push((node_id, part_id, version));
            }
        }

        Ok(records)
    }
}

impl State {
    /// Set template variables for CLI variable expansion
    pub fn set_template_variables(
        &self,
        variables: HashMap<String, serde_json::Value>,
    ) -> Result<(), TLogFSError> {
        match self.template_variables.lock() {
            Ok(mut guard) => {
                *guard = variables;
                Ok(())
            }
            Err(_) => Err(TLogFSError::Transaction {
                message: "Template variables mutex poisoned".to_string(),
            }),
        }
    }

    /// Get the Delta table for this transaction
    /// This allows factories to access the table for operations like reading Parquet files
    pub async fn table(&self) -> DeltaTable {
        self.inner.lock().await.table.clone()
    }

    /// Get template variables for CLI variable expansion
    #[must_use]
    pub fn get_template_variables(&self) -> Arc<HashMap<String, serde_json::Value>> {
        Arc::new(
            self.template_variables
                .lock()
                .expect("Failed to acquire template variables lock")
                .clone(),
        )
    }

    /// Add export data to template variables
    pub fn add_export_data(&self, export_data: serde_json::Value) -> Result<(), TLogFSError> {
        let mut variables = self.template_variables.lock().map_err(|e| {
            TLogFSError::ArrowMessage(format!("Failed to acquire template variables lock: {}", e))
        })?;
        debug!(
            "📝 STATE: Before add_export_data: keys = {:?}",
            variables.keys().collect::<Vec<_>>()
        );
        _ = variables.insert("export".to_string(), export_data.clone());
        debug!(
            "📝 STATE: After add_export_data: keys = {:?}",
            variables.keys().collect::<Vec<_>>()
        );
        debug!("📝 STATE: Added export data: {:?}", export_data);
        Ok(())
    }

    /// Initialize root directory - delegates to inner StateImpl
    ///
    /// Should only be called during pond bootstrap from steward or tests
    pub async fn initialize_root_directory(&self) -> Result<(), TLogFSError> {
        self.inner.lock().await.initialize_root_directory().await
    }

    async fn begin_impl(&self) -> Result<(), TLogFSError> {
        self.inner.lock().await.begin_impl().await
    }

    async fn commit_impl(
        &mut self,
        metadata: PondTxnMetadata,
        table: DeltaTable,
    ) -> Result<Option<()>, TLogFSError> {
        self.inner.lock().await.commit_impl(metadata, table).await
    }

    pub(crate) async fn store_file_content_ref(
        &mut self,
        id: FileID,
        content_ref: crate::file_writer::ContentRef,
        metadata: crate::file_writer::FileMetadata,
    ) -> Result<(), TLogFSError> {
        self.inner
            .lock()
            .await
            .store_file_content_ref(id, content_ref, metadata)
            .await
    }

    /// Create an async reader for a file without loading entire content into memory
    pub(crate) async fn async_file_reader(
        &self,
        id: FileID,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        self.inner
            .lock()
            .await
            .async_file_reader(id)
            .await
    }

    /// Add an arbitrary OplogEntry record to pending transaction state
    /// This is used for metadata-only operations like temporal bounds setting
    pub async fn add_oplog_entry(&self, entry: OplogEntry) -> Result<(), TLogFSError> {
        self.inner.lock().await.records.push(entry);
        Ok(())
    }

    /// Get the factory name for a specific node from the oplog
    /// Returns None if the node has no associated factory (static files/directories)
    pub async fn get_factory_for_node(
        &self,
        id: FileID,
    ) -> Result<Option<String>, TLogFSError> {
        self.inner
            .lock()
            .await
            .get_factory_for_node(id)
            .await
    }
}

#[async_trait]
impl PersistenceLayer for State {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn load_node(&self, id: FileID) -> TinyFSResult<Node> {
        self.inner.lock().await.load_node(id, self.clone()).await
    }

    async fn store_node(&self, node: &Node) -> TinyFSResult<()> {
        self.inner.lock().await.store_node(node).await
    }

    async fn create_file_node(&self, id: FileID) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_file_node(id, self.clone())
            .await
    }

    async fn create_directory_node(&self, id: FileID) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_directory_node(id, self.clone())
            .await
    }

    async fn create_symlink_node(&self, id: FileID, target: &Path) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_symlink_node(id, target, self.clone())
            .await
    }

    async fn create_dynamic_node(
        &self,
        id: FileID,
        // name: String, // SUS @@@
        // entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_dynamic_node(id, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> TinyFSResult<Option<(String, Vec<u8>)>> {
        self.inner
            .lock()
            .await
            .get_dynamic_node_config(id)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> TinyFSResult<()> {
        self.inner
            .lock()
            .await
            .update_dynamic_node_config(id, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }

    /// Batch load multiple nodes grouped by partition for efficiency.
    /// Issues one SQL query per partition instead of one query per node.
    async fn batch_load_nodes(
        &self,
        parent_id: FileID,
        requests: Vec<DirectoryEntry>,
    ) -> TinyFSResult<HashMap<String, Node>> {
        let map = self.inner
            .lock()
            .await
            .load_nodes_batched(parent_id, requests, self.clone())
            .await?;
        Ok(map.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    async fn metadata(&self, id: FileID) -> TinyFSResult<NodeMetadata> {
        self.inner.lock().await.metadata(id).await
    }

    async fn list_file_versions(&self, id: FileID) -> TinyFSResult<Vec<FileVersionInfo>> {
        self.inner.lock().await.list_file_versions(id).await
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> TinyFSResult<Vec<u8>> {
        self.inner.lock().await.read_file_version(id, version).await
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> TinyFSResult<()> {
        self.inner
            .lock()
            .await
            .set_extended_attributes(id, attributes)
            .await
    }
}

impl State {
    /// Load symlink target path
    pub async fn load_symlink_target(&self, id: FileID) -> TinyFSResult<PathBuf> {
        self.inner.lock().await.load_symlink_target(id).await
    }

    /// Track a directory as created in this transaction (for deferred storage decision)
    pub async fn track_created_directory(&self, id: FileID) {
        _ = self.inner.lock().await.created_directories.insert(id);
    }

    /// Query oplog records for a node (used by directory operations)
    pub async fn query_records(&self, id: FileID) -> Result<Vec<OplogEntry>, TLogFSError> {
        self.inner.lock().await.query_records(id).await
    }

    /// Update directory content (used by OpLogDirectory::insert)
    pub async fn update_directory_content(
        &self,
        id: FileID,
        content: Vec<u8>,
    ) -> Result<(), TLogFSError> {
        self.inner
            .lock()
            .await
            .update_directory_content(id, content)
            .await
    }

    /// Get the shared DataFusion SessionContext
    ///
    /// This method ensures a single SessionContext across all operations using this State,
    /// preventing ObjectStore registry conflicts and ensuring consistent configuration.
    /// This is the method SqlDerived should use instead of creating its own SessionContext.
    pub async fn session_context(&self) -> Result<Arc<SessionContext>, TLogFSError> {
        let inner = self.inner.lock().await;
        Ok(inner.session_context.clone())
    }

    /// Get the TinyFS ObjectStore instance if it has been created
    /// This provides direct access to the same ObjectStore that DataFusion uses
    #[must_use]
    pub fn object_store(&self) -> Option<Arc<crate::tinyfs_object_store::TinyFsObjectStore>> {
        self.object_store.get().cloned()
    }

    /// Get cached dynamic node by key (for dynamic directory factory)
    #[must_use]
    pub fn get_dynamic_node_cache(&self, key: &DynamicNodeKey) -> Option<Node> {
        self.dynamic_node_cache
            .lock()
            .expect("Failed to acquire dynamic node cache lock")
            .get(key)
            .cloned()
    }

    /// Set cached dynamic node by key (for dynamic directory factory)
    pub fn set_dynamic_node_cache(&self, key: DynamicNodeKey, value: Node) {
        _ = self.dynamic_node_cache
            .lock()
            .expect("Failed to acquire dynamic node cache lock")
            .insert(key, value);
    }

    /// Get cached TableProvider by key (for TLogFS query optimization)
    #[must_use]
    pub fn get_table_provider_cache(
        &self,
        key: &TableProviderKey,
    ) -> Option<Arc<dyn datafusion::catalog::TableProvider>> {
        self.table_provider_cache
            .lock()
            .expect("Failed to acquire table provider cache lock")
            .get(key)
            .cloned()
    }

    /// Set cached TableProvider by key (for TLogFS query optimization)
    pub fn set_table_provider_cache(
        &self,
        key: TableProviderKey,
        value: Arc<dyn datafusion::catalog::TableProvider>,
    ) {
        _ = self
            .table_provider_cache
            .lock()
            .expect("Failed to acquire table provider cache lock")
            .insert(key, value);
    }

    /// FAIL-FAST: Get temporal overrides for a FileSeries node
    /// This method replaces the fallback-riddled direct SQL approach with proper error handling
    /// and consistent data access through the persistence layer.
    pub async fn get_temporal_overrides_for_node_id(
        &self,
        id: FileID,
    ) -> Result<Option<(i64, i64)>, TLogFSError> {
        debug!("🔍 TEMPORAL: Looking up temporal overrides for node_id: {id}");

        // FAIL-FAST: Use consistent data access by duplicating query_records logic
        // This ensures we see the same data that persistence operations work with
        let inner = self.inner.lock().await;

        // Query for committed records from Delta Lake
        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' ORDER BY timestamp DESC",
            id.part_id(), id.node_id(),
        );

        let committed_records = match inner.session_context.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let mut records = Vec::new();
                    for batch in batches {
                        match serde_arrow::from_record_batch(&batch) {
                            Ok(batch_records) => {
                                let batch_records: Vec<OplogEntry> = batch_records;
                                records.extend(batch_records);
                            }
                            Err(e) => {
                                debug!(
                                    "❌ FAIL-FAST: Failed to deserialize temporal override records: {e}"
                                );
                                return Err(TLogFSError::Transaction {
                                    message: format!(
                                        "Temporal override deserialization failed for {id}: {e}"
                                    ),
                                });
                            }
                        }
                    }
                    records
                }
                Err(e) => {
                    debug!("❌ FAIL-FAST: Failed to collect temporal override records: {e}");
                    return Err(TLogFSError::Transaction {
                        message: format!("Temporal override collection failed for {id}: {e}"),
                    });
                }
            },
            Err(e) => {
                debug!("❌ FAIL-FAST: Failed to query temporal overrides SQL: {e}");
                return Err(TLogFSError::Transaction {
                    message: format!("Temporal override SQL query failed for {id}: {e}"),
                });
            }
        };

        // Add pending records (same logic as query_records)
        let mut all_records = committed_records;
        for record in &inner.records {
            if record.part_id == id.part_id() && record.node_id == id.node_id() {
                all_records.push(record.clone());
            }
        }

        // Release the lock before processing
        drop(inner);

        // FAIL-FAST: Filter for FileSeries explicitly and validate file_type
        let file_series_records: Vec<_> = all_records
            .into_iter()
            .filter(|record| record.file_type.is_series_file())
            .collect();

        debug!(
            "🔍 TEMPORAL: Found {} FileSeries records for node_id {id}",
            file_series_records.len()
        );

        if file_series_records.is_empty() {
            debug!(
                "⚠️ TEMPORAL: No FileSeries records found for node_id {id} - temporal overrides not available"
            );
            return Ok(None);
        }

        // FAIL-FAST: Find latest version or fail explicitly
        let latest_version = file_series_records.iter()
            .max_by_key(|r| r.version)
            .ok_or_else(|| {
                debug!("❌ FAIL-FAST: No records found to determine latest version for node_id {id}");
                TLogFSError::Transaction { message: format!("Cannot find latest version for temporal overrides: node_id {id}") }
            })?;

        let version = latest_version.version;
        let temporal_overrides = latest_version.temporal_overrides();
        let has_overrides = temporal_overrides.is_some();
        debug!("🔍 TEMPORAL: Latest version {version} has temporal overrides: {has_overrides}");

        if let Some((min_time, max_time)) = temporal_overrides {
            debug!(
                "✅ TEMPORAL: Found temporal overrides in latest version {version}: {min_time} to {max_time}"
            );
            Ok(Some((min_time, max_time)))
        } else {
            debug!(
                "⚠️ TEMPORAL: Latest version {version} has no temporal overrides - this may be expected"
            );
            Ok(None)
        }
    }
}

impl InnerState {
    async fn new<P: AsRef<Path>>(
        path: P,
        table: DeltaTable,
        txn_seq: i64,
    ) -> Result<Self, TLogFSError> {
        // Create the SessionContext with caching enabled (64MiB limit)
        use datafusion::execution::{
            cache::{
                cache_manager::CacheManagerConfig,
                cache_unit::{DefaultFileStatisticsCache, DefaultListFilesCache},
            },
            runtime_env::RuntimeEnvBuilder,
        };

        // Enable DataFusion file statistics and list files caching (64MiB total)
        let file_stats_cache = Arc::new(DefaultFileStatisticsCache::default());
        let list_files_cache = Arc::new(DefaultListFilesCache::default());

        let cache_config = CacheManagerConfig::default()
            .with_files_statistics_cache(Some(file_stats_cache))
            .with_list_files_cache(Some(list_files_cache));

        let runtime_env = RuntimeEnvBuilder::new()
            .with_cache_manager(cache_config)
            .with_memory_limit(512 * 1024 * 1024, 1.0) // 512 MiB memory limit for query execution
            .build_arc()
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create runtime environment: {}", e))
            })?;

        let session_config = SessionConfig::default().with_target_partitions(2); // Limit parallelism to reduce memory pressure
        let ctx = Arc::new(SessionContext::new_with_config_rt(
            session_config,
            runtime_env,
        ));

        debug!(
            "📋 ENABLED DataFusion caching: file statistics + list files caches with 512 MiB memory limit, parallelism=2"
        );

        // Register the fundamental delta_table for direct DeltaTable queries
        debug!("📋 REGISTERING fundamental table 'delta_table' in State constructor");
        _ = ctx
            .register_table("delta_table", Arc::new(table.clone()))
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to register delta_table: {}", e))
            })?;

        // Note: TinyFS ObjectStore registration will be done later when State is available

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            table,
            records: Vec::new(),
            operations: HashMap::new(),
            created_directories: HashSet::new(),
            session_context: ctx,
            txn_seq,
        })
    }

    /// Complete SessionContext setup after State is available
    /// This registers the TinyFS ObjectStore which requires a State reference
    async fn complete_session_setup(&self, state: &State) -> Result<(), TLogFSError> {
        // Register the TinyFS ObjectStore with the context
        let _object_store =
            crate::file_table::register_tinyfs_object_store(&self.session_context, state.clone())
                .await?;
        debug!("✅ Completed SessionContext setup with TinyFS ObjectStore");
        Ok(())
    }

    /// Initialize root directory - should only be called during pond bootstrap
    ///
    /// This is a special operation that creates the root directory entry.
    /// Should only be called from:
    /// - Steward layer during pond initialization (with proper metadata)
    /// - Tests (with test metadata)
    pub async fn initialize_root_directory(&mut self) -> Result<(), TLogFSError> {
         let root_id = FileID::root();

        // Create root directory using direct TLogFS commit (no transaction guard needed for bootstrap)
        let now = Utc::now().timestamp_micros();
        let empty_entries: Vec<DirectoryEntry> = Vec::new();
        let content = self.serialize_directory_entries(&empty_entries)?;

        let root_entry = OplogEntry::new_inline(
            root_id,
            now,
            1, // First version of root directory node
            content,
            self.txn_seq, // Bootstrap operations don't have a sequence from Steward
        );

        self.records.push(root_entry);

        Ok(())
    }

    /// Check if a directory has pending operations in this transaction
    fn has_pending_operations(&self, id: FileID) -> bool {
        // Check if there are any pending OpLog records for this directory
        // This includes directory content updates from insert() operations
        self.records.iter().any(|r| r.node_id == id.node_id() && r.part_id == id.part_id() && r.file_type.is_directory())
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
        id: FileID,
        result: crate::large_files::HybridWriterResult,
    ) -> Result<(), TLogFSError> {
        let entry = if result.size < crate::large_files::LARGE_FILE_THRESHOLD {
            // Small file: store content directly in Delta Lake
            let now = Utc::now().timestamp_micros();
            OplogEntry::new_small_file(
                id,
                now,
                0, // Placeholder - actual version assigned by Delta Lake transaction log
                result.content,
                self.txn_seq,
            )
        } else {
            // Large file: content already stored, just create OplogEntry with SHA256
            let size = result.size;
            debug!("Storing large file: {size} bytes");
            let now = Utc::now().timestamp_micros();

            match id.entry_type() {
                EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => {
                    // For FileSeries, extract temporal metadata from Parquet content
                    use super::schema::{
                        ExtendedAttributes, detect_timestamp_column,
                        extract_temporal_range_from_batch,
                    };
                    use tokio_util::bytes::Bytes;

                    // Read the Parquet data to extract temporal metadata
                    let bytes = Bytes::from(result.content.clone());
                    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                        .map_err(|e| {
                            TLogFSError::ArrowMessage(format!(
                                "Failed to create Parquet reader: {}",
                                e
                            ))
                        })?
                        .build()
                        .map_err(|e| {
                            TLogFSError::ArrowMessage(format!(
                                "Failed to build Parquet reader: {}",
                                e
                            ))
                        })?;

                    let mut all_batches = Vec::new();
                    for batch_result in reader {
                        let batch = batch_result.map_err(|e| {
                            TLogFSError::ArrowMessage(format!("Failed to read batch: {}", e))
                        })?;
                        all_batches.push(batch);
                    }

                    if all_batches.is_empty() {
                        return Err(TLogFSError::ArrowMessage(
                            "No data in Parquet file".to_string(),
                        ));
                    }

                    // For temporal extraction, we'll process all batches to get global min/max
                    let schema = all_batches[0].schema();

                    // Determine timestamp column
                    let time_col = detect_timestamp_column(&schema).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to detect timestamp column: {}", e),
                        )
                    })?;

                    // Extract temporal range from all batches
                    let mut global_min = i64::MAX;
                    let mut global_max = i64::MIN;

                    for batch in &all_batches {
                        let (batch_min, batch_max) =
                            extract_temporal_range_from_batch(batch, &time_col).map_err(|e| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Failed to extract temporal range: {}", e),
                                )
                            })?;
                        global_min = global_min.min(batch_min);
                        global_max = global_max.max(batch_max);
                    }

                    // Create extended attributes with timestamp column info
                    let mut extended_attrs = ExtendedAttributes::default();
                    _ = extended_attrs.set_timestamp_column(&time_col);

                    // Create large FileSeries entry with temporal metadata and size
                    OplogEntry::new_large_file_series(
                        id,
                        now,
                        // @@@ GARBAGE BELOW
                        0, // Placeholder - actual version assigned by Delta Lake transaction log
                        result.sha256,
                        result.size as i64, // Cast to i64 to match Delta Lake protocol
                        global_min,
                        global_max,
                        extended_attrs,
                        self.txn_seq,
                    )
                }
                _ => {
                    // For other entry types, use generic large file constructor
                    OplogEntry::new_large_file(
                        id,
                        now,
                        0, // Placeholder - actual version assigned by Delta Lake transaction log
                        result.sha256,
                        result.size as i64, // Cast to i64 to match Delta Lake protocol
                        self.txn_seq,
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
        id: FileID,
        content: &[u8],
    ) -> Result<(), TLogFSError> {
        use crate::large_files::should_store_as_large_file;

        if should_store_as_large_file(content) {
            // Use hybrid writer for large files
            let mut writer = self.create_hybrid_writer().await;
            use tokio::io::AsyncWriteExt;
            writer.write_all(content).await?;
            writer.shutdown().await?;
            let result = writer.finalize().await?;
            self.store_file_from_hybrid_writer(id, result).await
        } else {
            // For small files, always use direct storage regardless of type
            // This avoids Parquet parsing issues for small FileSeries test files
            self.store_small_file_with_type(id, content).await
        }
    }

    /// Store small file directly in Delta Lake with specific entry type
    async fn store_small_file_with_type(
        &mut self,
        id: FileID,
        content: &[u8],
    ) -> Result<(), TLogFSError> {
        let now = Utc::now().timestamp_micros();
        let entry = OplogEntry::new_small_file(
            id,
            now,
            0, // @@@ !!! Placeholder - actual version assigned by Delta Lake transaction log
            content.to_vec(),
            self.txn_seq,
        );

        self.records.push(entry);
        Ok(())
    }

    /// Get the next version number for a specific node (current max + 1)
    async fn get_next_version_for_node(&self, id: FileID) -> Result<i64, TLogFSError> {
        debug!("get_next_version_for_node called for node_id={id}");

        // Query all records for this node and find the maximum version
        match self.query_records(id).await {
            Ok(records) => {
                let record_count = records.len();
                debug!("get_next_version_for_node found {record_count} existing records");

                let next_version = if records.is_empty() {
                    // This is a new node - start with version 1
                    debug!("get_next_version_for_node: new node, starting with version 1");
                    1
                } else {
                    // This is an existing node - find max version and increment
                    let max_version = records
                        .iter()
                        .map(|r| r.version)
                        .max()
                        .expect("records is non-empty, so max() should succeed");
                    let next_version = max_version + 1;
                    debug!(
                        "get_next_version_for_node: existing node with max_version={max_version}, returning next_version={next_version}"
                    );
                    next_version
                };

                Ok(next_version)
            }
            Err(e) => {
                let error_str = format!("{:?}", e);
                debug!("get_next_version_for_node query failed: {error_str}");
                // Critical error: cannot determine proper version sequence
                Err(TLogFSError::ArrowMessage(format!(
                    "Cannot determine next version for node {id}: query failed: {e}"
                )))
            }
        }
    }

    /// Find existing large file entry in pending records (to avoid duplicates during store_node)
    async fn find_existing_large_file_entry(
        &self,
        id: FileID,
    ) -> Option<OplogEntry> {
        self.records
            .iter()
            .find(|entry| {
                entry.node_id == id.node_id() &&
		entry.part_id == id.part_id() &&
		entry.content.is_none() && // Large files have no inline content @@@
		entry.sha256.is_some() // Large files have SHA256 @@@
            })
            .cloned()
    }

    /// Store FileSeries with temporal metadata extraction from Parquet data
    /// This method extracts min/max timestamps from the specified time column
    pub async fn store_file_series_from_parquet(
        &mut self,
        id: FileID,
        content: &[u8],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64), TLogFSError> {
        use super::schema::{
            ExtendedAttributes, detect_timestamp_column, extract_temporal_range_from_batch,
        };
        use tokio_util::bytes::Bytes;

        // Get the next version number for this node
        let next_version = self.get_next_version_for_node(id).await?; // First, read the Parquet data to extract temporal metadata
        let bytes = Bytes::from(content.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create Parquet reader: {}", e))
            })?
            .build()
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to build Parquet reader: {}", e))
            })?;

        let mut all_batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read batch: {}", e)))?;
            all_batches.push(batch);
        }

        if all_batches.is_empty() {
            return Err(TLogFSError::ArrowMessage(
                "No data in Parquet file".to_string(),
            ));
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
        let mut extended_attrs = ExtendedAttributes::default();
        _ = extended_attrs.set_timestamp_column(&time_col);

        // Store the FileSeries using the unified hybrid writer pattern
        use crate::large_files::should_store_as_large_file;

        let content_len = content.len();
        let is_large_file = should_store_as_large_file(content);
        debug!(
            "store_file_series_from_parquet decision: content_len={content_len}, is_large_file={is_large_file}"
        );

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
                id,
                now,
                next_version, // Use proper version counter
                sha256,
                size,
                global_min,
                global_max,
                extended_attrs,
                self.txn_seq,
            );

            // Store metadata in pending records (content is external)
            self.records.push(entry);
        } else {
            // Store as small FileSeries
            let now = Utc::now().timestamp_micros();
            let content_size = content.len();

            debug!(
                "store_file_series_from_parquet - storing as small FileSeries with {content_size} bytes content"
            );

            let entry = OplogEntry::new_file_series(
                id,
                now,
                next_version, // Use proper version counter
                content.to_vec(),
                global_min,
                global_max,
                extended_attrs,
                self.txn_seq,
            );

            let entry_content_size = entry.content.as_ref().map(|c| c.len()).unwrap_or(0);
            debug!(
                "store_file_series_from_parquet - created OplogEntry with content size: {entry_content_size}"
            );

            self.records.push(entry);
        }

        Ok((global_min, global_max))
    }

    /// Create an async reader for a file without loading entire content into memory
    pub async fn async_file_reader(
        &self,
        id: FileID,
    ) -> Result<Pin<Box<dyn tinyfs::AsyncReadSeek>>, TLogFSError> {
        let records = self.query_records(id).await?;

        // Find the latest record with actual content (skip empty temporal override versions)
        let record = records
            .iter()
            .find(|r| r.size.unwrap_or(0) > 0) // Skip 0-byte versions
            .ok_or_else(|| {
                TLogFSError::ArrowMessage(format!("No non-empty versions found for file {id}"))
            })?;

        if true {
            // Changed condition to always enter this block
            if record.is_large_file() {
                // Large file: create async file reader
                let sha256 = record.sha256.as_ref().ok_or_else(|| {
                    TLogFSError::ArrowMessage("Large file entry missing SHA256".to_string())
                })?;

                // Find the file in either flat or hierarchical structure
                let large_file_path = crate::large_files::find_large_file_path(&self.path, sha256)
                    .await
                    .map_err(|e| {
                        TLogFSError::ArrowMessage(format!("Error searching for large file: {}", e))
                    })?
                    .ok_or_else(|| TLogFSError::LargeFileNotFound {
                        sha256: sha256.clone(),
                        path: format!("_large_files/sha256={}", sha256),
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Large file not found in any location",
                        ),
                    })?;

                // Open file for async reading
                let file = tokio::fs::File::open(&large_file_path).await.map_err(|e| {
                    TLogFSError::LargeFileNotFound {
                        sha256: sha256.clone(),
                        path: large_file_path.display().to_string(),
                        source: e,
                    }
                })?;

                Ok(Box::pin(file))
            } else {
                // Small file: create cursor from inline content
                let content = record.content.clone().ok_or_else(|| {
                    TLogFSError::ArrowMessage("Small file entry missing content".to_string())
                })?;

                Ok(Box::pin(std::io::Cursor::new(content)))
            }
        } else {
            Err(TLogFSError::NodeNotFound {
                path: PathBuf::from(format!("File {id} not found")),
            })
        }
    }

    /// Commit pending records to Delta Lake
    async fn commit_impl(
        &mut self,
        metadata: PondTxnMetadata,
        table: DeltaTable,
    ) -> Result<Option<()>, TLogFSError> {
        self.flush_directory_operations().await?;

        let records = std::mem::take(&mut self.records);

        if records.is_empty() {
            debug!("Committing read-only transaction");
            return Ok(None);
        }

        let count = records.len();
        info!("Committing {count} operations in {:?}", self.path);

        // Convert records to RecordBatch
        let batches = vec![serde_arrow::to_record_batch(
            &OplogEntry::for_arrow(),
            &records,
        )?];

        let mut write_op = DeltaOps(table).write(batches);

        // Add commit metadata
        let properties =
            CommitProperties::default().with_metadata(metadata.to_delta_metadata().into_iter());
        write_op = write_op.with_commit_properties(properties);

        _ = write_op.await?;

        self.records.clear();
        self.operations.clear();
        self.created_directories.clear();

        Ok(Some(()))
    }

    /// Serialize DirectoryEntry records as Arrow IPC bytes
    fn serialize_directory_entries(
        &self,
        entries: &[DirectoryEntry],
    ) -> Result<Vec<u8>, TLogFSError> {
        // For empty directories, use truly empty content (0 bytes) instead of Arrow IPC with schema
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        serialization::serialize_to_arrow_ipc(entries)
    }

    /// Deserialize DirectoryEntry records from Arrow IPC bytes into HashMap by name
    fn deserialize_directory_entries(
        &self,
        content: &[u8],
    ) -> Result<HashMap<String, DirectoryEntry>, TLogFSError> {
        // Handle empty directories (0 bytes of content)
        if content.is_empty() {
            return Ok(HashMap::new());
        }
        debug!(
            "deserialize_directory_entries: processing {} bytes for directory entries",
            content.len()
        );
        let entries: Vec<DirectoryEntry> = serialization::deserialize_from_arrow_ipc(content)?;
        let map: HashMap<String, DirectoryEntry> =
            entries.into_iter().map(|e| (e.name.clone(), e)).collect();
        debug!(
            "deserialize_directory_entries: successfully deserialized {} directory entries",
            map.len()
        );
        Ok(map)
    }

    /// Store file content reference with transaction context (used by transaction guard FileWriter)
    pub async fn store_file_content_ref(
        &mut self,
        id: FileID,
        content_ref: crate::file_writer::ContentRef,
        metadata: crate::file_writer::FileMetadata,
    ) -> Result<(), TLogFSError> {
        debug!(
            "store_file_content_ref_transactional called for node_id={id}"
        );

        // Create OplogEntry from content reference
        let now = Utc::now().timestamp_micros();

        // Get proper version number for this node
        // Check if there's already an entry for this node in this transaction
        let version = {
            let existing_entry = self
                .records
                .iter()
                .find(|e| e.node_id == id.node_id() && e.part_id == id.part_id());

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
                self.get_next_version_for_node(id).await?
            }
        };

        let txn_seq = self.txn_seq; // Capture for use in match arms

        let entry = match content_ref {
            crate::file_writer::ContentRef::Small(content) => {
                // Small file: store content inline
                match id.entry_type() {
                    EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => {
                        // FileSeries needs temporal metadata
                        match metadata {
                            crate::file_writer::FileMetadata::Series {
                                min_timestamp,
                                max_timestamp,
                                timestamp_column,
                            } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::default();
                                _ = extended_attrs.set_timestamp_column(&timestamp_column);

                                OplogEntry::new_file_series(
				    id,
                                    now,
                                    version, // Use proper version counter
                                    content,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                    txn_seq,
                                )
                            }
                            _ => {
                                return Err(TLogFSError::Transaction {
                                    message: "FileSeries requires Series metadata".to_string(),
                                });
                            }
                        }
                    }
                    _ => {
                        // Regular small file
                        OplogEntry::new_small_file(
                            id, now,
                            version, // Use proper version counter
                            content, txn_seq,
                        )
                    }
                }
            }
            crate::file_writer::ContentRef::Large(sha256, size) => {
                // Large file: store reference
                match id.entry_type() {
                    EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => {
                        // Large FileSeries needs temporal metadata
                        match metadata {
                            crate::file_writer::FileMetadata::Series {
                                min_timestamp,
                                max_timestamp,
                                timestamp_column,
                            } => {
                                use crate::schema::ExtendedAttributes;
                                let mut extended_attrs = ExtendedAttributes::default();
                                _ = extended_attrs.set_timestamp_column(&timestamp_column);

                                OplogEntry::new_large_file_series(
                                    id,
                                    now,
                                    version, // Use proper version counter
                                    sha256,
                                    size as i64,
                                    min_timestamp,
                                    max_timestamp,
                                    extended_attrs,
                                    txn_seq,
                                )
                            }
                            _ => {
                                return Err(TLogFSError::Transaction {
                                    message: "Large FileSeries requires Series metadata"
                                        .to_string(),
                                });
                            }
                        }
                    }
                    _ => {
                        // Regular large file
                        OplogEntry::new_large_file(
                            id,
                            now,
                            version, // Use proper version counter
                            sha256,
                            size as i64,
                            txn_seq,
                        )
                    }
                }
            }
        };

        // Find existing entry for this node/part combination
        let existing_index = self.records.iter().position(|existing_entry| {
            existing_entry.part_id == id.part_id() && existing_entry.node_id == id.node_id()
        });

        if let Some(index) = existing_index {
            // Replace existing entry (content changes, version stays the same within transaction)
            self.records[index] = entry;
        } else {
            // No existing entry - add new entry with version 1 (??)
            debug!(
                "Adding new pending entry for node {id} with version {}",
                entry.version
            );
            self.records.push(entry);
        }

        debug!("Stored file content reference for node {id}");
        Ok(())
    }

    /// Query directory entries for a parent node (OPTIMIZED: LATEST VERSION ONLY)
    ///
    /// 🚀 MAJOR PERFORMANCE IMPROVEMENT: This function now reads ONLY the latest version
    /// instead of iterating through all historical versions. This is O(1) instead of O(N)
    /// where N = number of transactions that modified the directory.
    ///
    /// Uses SQL "ORDER BY version DESC LIMIT 1" to fetch exactly one record from Delta Lake.
    async fn query_directory_entries(
        &self,
        id: FileID,
    ) -> Result<Vec<DirectoryEntry>, TLogFSError> {
        // 🚀 CRITICAL: Use specialized query that fetches ONLY latest record via SQL LIMIT 1
        let latest_record = self
            .query_latest_directory_record(id)
            .await?
            .ok_or_else(|| TLogFSError::NodeNotFound {
                path: PathBuf::from(format!("Directory {}", id)),
            })?;

        debug!(
            "🚀 query_directory_entries: read ONLY version {} for part_id={} (SQL LIMIT 1)",
            latest_record.version, id
        );

        // Validate format field (future-proofing)
        use super::schema::StorageFormat;
        if latest_record.format != StorageFormat::FullDir {
            warn!(
                "Directory {} has unexpected format {:?}, expected FullDir",
                id, latest_record.format
            );
            // Don't fail - allow Inline format for backward compatibility during transition
        }

        // ✅ Single record fetched via SQL LIMIT 1
        // ✅ Single deserialize - complete directory state as HashMap
        // ✅ No iteration through history
        // ✅ No deduplication across versions
        // ✅ Constant time regardless of transaction count
        if let Some(content) = &latest_record.content {
            debug!(
                "query_directory_entries: deserializing {} bytes for directory {}",
                content.len(),
                id
            );
            let entries_map = self.deserialize_directory_entries(content).map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to deserialize directory content for part_id={}, version={}: {}",
                    id, latest_record.version, e
                ))
            })?;
            debug!(
                "✅ query_directory_entries: loaded {} entries from snapshot (version {})",
                entries_map.len(),
                latest_record.version
            );
            Ok(entries_map.into_values().collect())
        } else {
            // Empty directory
            Ok(Vec::new())
        }
    }

    /// Query for a single directory entry by name
    async fn query_single_directory_entry(
        &self,
        id: FileID,
        entry_name: &str,
    ) -> Result<Option<DirectoryEntry>, TLogFSError> {
        // Performance tracing - enable with perf analysis
        let mut trace = utilities::perf_trace::PerfTrace::start("query_single_directory_entry");
        let caller = utilities::perf_trace::extract_caller(
            "tlogfs::persistence::InnerState::",
            "query_single_directory_entry",
        );
        trace.param("caller", &caller);
        trace.param("id", id);
        trace.param("entry_name", entry_name);

        // Check pending directory operations first
        if let Some(operations) = self.operations.get(&id)
            && let Some(operation) = operations.get(entry_name)
        {
            match operation {
                DirectoryOperation::InsertWithType(node_id, node_type) => {
                    return Ok(Some(DirectoryEntry::new(
                        entry_name.to_string(),
                        *node_id,
                        *node_type,
                        0, // Version will be set when flushed
                    )));
                }
                DirectoryOperation::DeleteWithType(_node_type) => {
                    return Ok(None);
                }
                DirectoryOperation::RenameWithType(new_name, node_id, node_type) => {
                    return Ok(Some(DirectoryEntry::new(
                        new_name.clone(),
                        *node_id,
                        *node_type,
                        0, // Version will be set when flushed
                    )));
                }
            }
        }

        // 🚀 OPTIMIZATION: Read latest directory snapshot and do O(1) HashMap lookup
        let query_start = std::time::Instant::now();
        let latest_record = self.query_latest_directory_record(id).await?;
        trace.metric("query_ms", query_start.elapsed().as_millis() as u64);

        if let Some(record) = latest_record {
            if let Some(content) = &record.content {
                let entries_map = self.deserialize_directory_entries(content).map_err(|e| {
                    TLogFSError::ArrowMessage(format!(
                        "Failed to deserialize directory content for part_id={}: {}",
                        id, e
                    ))
                })?;
                trace.metric("entry_count", entries_map.len() as u64);

                // O(1) HashMap lookup by name
                let result = entries_map.get(entry_name).cloned();
                trace.metric("found", if result.is_some() { 1 } else { 0 });
                Ok(result)
            } else {
                // Empty directory
                trace.metric("entry_count", 0);
                trace.metric("found", 0);
                Ok(None)
            }
        } else {
            // Directory not found
            trace.metric("entry_count", 0);
            trace.metric("found", 0);
            Ok(None)
        }
    }

    /// Process all accumulated directory operations in a batch (FULL SNAPSHOT VERSION)
    ///
    /// This rewritten function implements full directory snapshots instead of incremental changes.
    /// Key changes:
    /// 1. Load current directory state (if exists)
    /// 2. Apply pending operations to build new complete state
    /// 3. Write full snapshot using new_directory_full_snapshot constructor
    async fn flush_directory_operations(&mut self) -> Result<(), TLogFSError> {
        debug!(
            "flush_directory_operations: starting FULL SNAPSHOT mode, txn_seq={}, operations.len()={}, created_directories.len()={}",
            self.txn_seq,
            self.operations.len(),
            self.created_directories.len()
        );

        let pending_dirs = std::mem::take(&mut self.operations);

        // Track which directories have operations (will be written with content)
        let populated_directories: HashSet<_> = pending_dirs.keys().copied().collect();

        if !pending_dirs.is_empty() {
            debug!(
                "flush_directory_operations: processing {} directories with operations",
                pending_dirs.len()
            );

            for (id, operations) in pending_dirs {
                debug!(
                    "flush_directory_operations: writing directory {} with {} operations",
                    id,
                    operations.len()
                );

                // 🚀 STEP 1: Load current directory state (or empty map for new directories)
                let mut current_state: HashMap<String, DirectoryEntry> =
                    match self.query_directory_entries(id).await {
                        Ok(entries) => {
                            debug!(
                                "Loaded {} existing entries for directory {}",
                                entries.len(),
                                id
                            );
                            entries.into_iter().map(|e| (e.name.clone(), e)).collect()
                        }
                        Err(_) => {
                            debug!(
                                "No existing entries for directory {} (new directory)",
                                id
                            );
                            HashMap::new()
                        }
                    };

                // 🚀 STEP 2: Apply pending operations to build new complete state
                let next_version = self.get_next_version_for_node(id).await?;

                for (entry_name, operation) in operations {
                    match operation {
                        DirectoryOperation::InsertWithType(child_node_id, entry_type) => {
                            _ = current_state.insert(
                                entry_name.clone(),
                                DirectoryEntry::new(
                                    entry_name,
                                    child_node_id,
                                    entry_type,
                                    next_version, // Mark with current version
                                ),
                            );
                        }
                        DirectoryOperation::DeleteWithType(_) => {
                            _ = current_state.remove(&entry_name);
                        }
                        DirectoryOperation::RenameWithType(new_name, child_node_id, entry_type) => {
                            _ = current_state.remove(&entry_name);
                            _ = current_state.insert(
                                new_name.clone(),
                                DirectoryEntry::new(
                                    new_name,
                                    child_node_id,
                                    entry_type,
                                    next_version, // Mark with current version
                                ),
                            );
                        }
                    }
                }

                // 🚀 STEP 3: Serialize complete state as full snapshot
                let all_entries: Vec<DirectoryEntry> = current_state.into_values().collect();
                debug!(
                    "Serializing {} total entries for directory {}",
                    all_entries.len(),
                    id
                );
                let content_bytes = self.serialize_directory_entries(&all_entries)?;

                // 🚀 STEP 4: Create full snapshot OplogEntry
                let now = Utc::now().timestamp_micros();
                let record = OplogEntry::new_directory_full_snapshot(
                    id,
                    now,
                    next_version,
                    content_bytes,
                    self.txn_seq,
                );

                self.records.push(record);
            }
        } else {
            debug!("flush_directory_operations: no pending_dirs, checking for empty directories");
        }

        // Handle truly empty directories: directories created but never populated
        let empty_directories: Vec<FileID> = self
            .created_directories
            .iter()
            .filter(|id| !populated_directories.contains(id))
            .copied()
            .collect();

        for id in empty_directories {
            debug!(
                "flush_directory_operations: creating empty directory snapshot for {}",
                id
            );

            // Get proper version number for this directory
            let version = self.get_next_version_for_node(id).await?;

            let now = Utc::now().timestamp_micros();
            // Empty directories still use full snapshot format
            let record = OplogEntry::new_directory_full_snapshot(
                id,
                now,
                version,
                Vec::new(), // Empty content for empty directory
                self.txn_seq,
            );
            self.records.push(record);
        }

        Ok(())
    }

    /// Create a dynamic directory node with factory configuration
    /// Create a dynamic node with factory configuration (used by `mknod` command)
    /// 
    /// NOTE: Most dynamic nodes (created by DynamicDirDirectory, TemplateDirectory, etc.)
    /// do NOT need this method - they are created on-the-fly from configuration when
    /// their parent directory is accessed. This method is only for explicitly creating
    /// persistent dynamic nodes via `mknod`.
    ///
    /// TODO: Complete implementation - for now, callers should use FactoryRegistry directly
    async fn create_dynamic_node(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node, TLogFSError> {
        // TODO: Implement proper dynamic node creation with oplog entry
        let _ = (id, factory_type, config_content, self.txn_seq);
        Err(TLogFSError::Transaction {
            message: "create_dynamic_node not yet implemented - use FactoryRegistry directly for now".to_string(),
        })
    }

    // /// Create a dynamic file node with factory configuration
    // pub async fn create_dynamic_file(
    //     &mut self,
    //     part_id: NodeID,
    //     name: String,
    //     file_type: EntryType,
    //     factory_type: &str,
    //     config_content: Vec<u8>,
    // ) -> Result<NodeID, TLogFSError> {
    //     debug!(
    //         "🔧 create_dynamic_file called: part_id={}, name='{}', file_type={:?}, factory_type='{}', txn_seq={}",
    //         part_id, name, file_type, factory_type, self.txn_seq
    //     );

    //     let node_id = NodeID::generate();
    //     let now = Utc::now().timestamp_micros();

    //     // Create dynamic file OplogEntry
    //     let entry = OplogEntry::new_dynamic_file(
    //         part_id,
    //         node_id,
    //         file_type,
    //         now,
    //         1, // Made UP @@@
    //         factory_type,
    //         config_content,
    //         self.txn_seq,
    //     );

    //     // Add to pending records
    //     self.records.push(entry);
    //     debug!(
    //         "🔧 create_dynamic_file: pushed OplogEntry to records, now calling update_directory_entry"
    //     );

    //     // Add directory operation for parent
    //     let directory_op = DirectoryOperation::InsertWithType(node_id, file_type);
    //     self.update_directory_entry(part_id, &name, directory_op)
    //         .await
    //         .map_err(TLogFSError::TinyFS)?;

    //     debug!(
    //         "🔧 create_dynamic_file: update_directory_entry completed successfully, returning node_id={}",
    //         node_id
    //     );

    //     Ok(node_id)
    // }

    /// Get dynamic node configuration if the node is dynamic
    /// Uses the same query pattern as the rest of the persistence layer
    pub async fn get_dynamic_node_config(
        &self,
        id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>, TLogFSError> {
        // First check pending records (for nodes created in current transaction)
        for record in &self.records {
            if record.node_id == id.node_id()
                && record.part_id == id.part_id()
                && let Some(factory_type) = &record.factory
                && let Some(config_content) = &record.content
            {
                return Ok(Some((factory_type.clone(), config_content.clone())));
            }
        }

        // Then check committed records (for existing nodes)
        let records = self.query_records(id).await?;

        if let Some(record) = records.first()
            && let Some(factory_type) = &record.factory
            && let Some(config_content) = &record.content
        {
            Ok(Some((factory_type.clone(), config_content.clone())))
        } else {
            Ok(None)
        }
    }

    /// Update the configuration of an existing dynamic node
    /// This creates a new OplogEntry with the updated configuration
    pub async fn update_dynamic_node_config(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<(), TLogFSError> {
        let now = Utc::now().timestamp_micros();

        // First, verify the node exists and is a dynamic node
        let existing_config = self.get_dynamic_node_config(id).await?;
        if existing_config.is_none() {
            return Err(TLogFSError::NodeNotFound {
                path: format!("id:{}", id).into(),
                // @@@ LAME
            });
        }

        // Get the current version from existing records to increment it
        let records = self.query_records(id).await?;
        let current_version = records.first().map(|r| r.version).unwrap_or(0);
        let new_version = current_version + 1;

        let entry = OplogEntry::new_dynamic_node(
            id,
            now,
            new_version,
            factory_type,
            config_content,
            self.txn_seq,
        );

        // Add to pending records
        self.records.push(entry);

        Ok(())
    }

    /// Query for ONLY the latest record for any node type (O(1) performance)
    async fn query_latest_record(&self, id: FileID) -> Result<OplogEntry, TLogFSError> {
        // Step 1: Check pending records in memory FIRST
        let pending_record = self
            .records
            .iter()
            .filter(|r| {
                // @@@ Linear!
                r.part_id == id.part_id() && r.node_id == id.node_id()
            })
            .max_by_key(|r| r.version)
            .cloned();

        // If a record is found pending, it must be later than persistence storage.
        if let Some(pending) = pending_record {
            return Ok(pending);
        }

        // Step 2: Query Delta Lake for the single latest committed record
        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' ORDER BY version DESC LIMIT 1",
            id.part_id(),
            id.node_id()
        );

        debug!("query_latest_record SQL: {}", sql);
        match self.session_context.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    if batches.is_empty() || batches[0].num_rows() == 0 {
                        return Err(TLogFSError::Missing);
                    } else {
                        let records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batches[0])?;
                        Ok(records.into_iter().next().expect("one"))
                    }
                }
                Err(e) => return Err(TLogFSError::DataFusion(e)),
            },
            Err(e) => return Err(TLogFSError::DataFusion(e)),
        }
    }

    /// Query for ONLY the latest directory record (O(1) performance)
    ///
    /// This function uses SQL LIMIT 1 to fetch only the most recent directory version,
    /// avoiding the O(N) cost of reading all historical versions.
    ///
    /// IMPORTANT: This function checks BOTH committed (Delta Lake) and pending (self.records) state.
    /// During a transaction, pending directory records in self.records take precedence over committed ones.
    async fn query_latest_directory_record(
        &self,
        id: FileID,
    ) -> Result<Option<OplogEntry>, TLogFSError> {
        // Step 1: Check pending records in memory FIRST
        // During flush_directory_operations, newly created directory snapshots are in self.records
        let pending_record = self
            .records
            .iter()
            .filter(|r| r.part_id == id.part_id() && r.node_id == id.node_id())
            .max_by_key(|r| r.version)
            .cloned();

        // Step 2: Query Delta Lake for the single latest committed directory record
        // Note: file_type values in database are serialized as 'dir:physical' and 'dir:dynamic'
        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' AND file_type IN (') ORDER BY version DESC LIMIT 1",
            id.part_id(), id.part_id()
        );

        let committed_record = match self.session_context.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    if batches.is_empty() || batches[0].num_rows() == 0 {
                        None
                    } else {
                        let records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batches[0])?;
                        records.into_iter().next()
                    }
                }
                Err(e) => return Err(TLogFSError::DataFusion(e)),
            },
            Err(e) => return Err(TLogFSError::DataFusion(e)),
        };

        // Step 3: Return the latest between committed and pending (pending wins if both exist)
        match (committed_record, pending_record) {
            (Some(c), Some(p)) => Ok(Some(if p.version > c.version { p } else { c })),
            (Some(c), None) => Ok(Some(c)),
            (None, Some(p)) => Ok(Some(p)),
            (None, None) => Ok(None),
        }
    }

    /// Query records from both committed (Delta Lake) and pending (in-memory) data
    /// This ensures TinyFS operations can see pending data before commit
    ///
    /// SECURITY: Always requires node_id to enforce proper data isolation between nodes
    async fn query_records(&self, id: FileID) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Performance tracing - enable with RUST_LOG=trace and redirect stderr
        let mut trace = utilities::perf_trace::PerfTrace::start("query_records");
        let caller = utilities::perf_trace::extract_caller(
            "tlogfs::persistence::InnerState::",
            "query_records",
        );
        trace.param("caller", &caller);
        trace.param("id", id);

        // Step 1: Get committed records from Delta Lake using node-scoped SQL
        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' ORDER BY timestamp DESC",
            id.part_id(),
            id.node_id()
        );

        let query_start = std::time::Instant::now();
        let committed_records = match self.session_context.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let mut records = Vec::new();
                    for batch in batches {
                        let batch_records: Vec<OplogEntry> =
                            serde_arrow::from_record_batch(&batch)?;
                        records.extend(batch_records);
                    }
                    records
                }
                Err(e) => {
                    return Err(TLogFSError::DataFusion(e));
                }
            },
            Err(e) => {
                return Err(TLogFSError::DataFusion(e));
            }
        };
        trace.metric("query_ms", query_start.elapsed().as_millis() as u64);
        trace.metric("committed_count", committed_records.len() as u64);

        // Step 2: Get pending records from memory (node-scoped)
        let pending_start = std::time::Instant::now();
        let records = {
            self.records
                .iter()
                .filter(|record| record.part_id == id.part_id() && record.node_id == id.node_id())
                .cloned()
                .collect::<Vec<_>>()
        };
        trace.metric("memory_scan_us", pending_start.elapsed().as_micros() as u64);
        trace.metric("pending_count", records.len() as u64);

        // Step 3: Combine and sort by timestamp
        let mut all_records = committed_records;
        all_records.extend(records);
        all_records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        trace.metric("total_count", all_records.len() as u64);

        Ok(all_records)
    }

    /// Batch load multiple nodes in a single partition.
    /// Checks in-memory structures first (pending operations, created directories), then queries Delta Lake.
    async fn load_nodes_batched(
        &self,
	parent_id: FileID,
        requests: Vec<DirectoryEntry>,
        state: State,
    ) -> TinyFSResult<HashMap<NodeID, Node>> {
        if requests.is_empty() {
            debug!("load_nodes_batched: EMPTY REQUEST for parent {}", parent_id);
            return Ok(HashMap::new());
        }

        debug!(
            "🔍 BATCH_LOAD: {} nodes from parent {} (partition={})",
            requests.len(),
            parent_id,
            parent_id.part_id()
        );

        let mut results = HashMap::new();
        let mut nodes_to_query = Vec::new();

        // Step 1: Check in-memory structures first (transaction-local data)
        for entry in &requests {
            let child_id = FileID::new_from_ids(parent_id.part_id(), entry.child_node_id);
            debug!("  📂 Checking entry '{}' -> node_id={}", entry.name, entry.child_node_id);
            
            // Check if this is a newly created directory in this transaction
            if self.created_directories.contains(&child_id) {
                debug!("  ✅ Found node {} in created_directories", child_id);
                match node_factory::create_directory_node(child_id, state.clone()) {
                    Ok(node) => {
                        _ = results.insert(entry.child_node_id, node);
                        continue;
                    }
                    Err(e) => {
                        debug!("Failed to create directory node {}: {}", child_id, e);
                    }
                }
            }

            // Check if there are pending operations for this node in self.operations
            if self.has_pending_operations(child_id) {
                debug!("Node {} has pending operations, will need to merge with committed data", child_id);
                // Still need to query for this node to get base state
            }

            // Check if there's a pending record in self.records
            let has_pending_record = self
                .records
                .iter()
                .any(|r| r.node_id == entry.child_node_id && r.part_id == parent_id.part_id());
            
            if has_pending_record {
                debug!("Node {} has pending records, loading via load_node", child_id);
                // Use load_node which properly merges pending and committed data
                match self.load_node(child_id, state.clone()).await {
                    Ok(node) => {
                        _ = results.insert(entry.child_node_id, node);
                        continue;
                    }
                    Err(e) => {
                        debug!("Failed to load node with pending records {}: {}", child_id, e);
                    }
                }
            }

            // No in-memory data for this node - add to batch query list
            nodes_to_query.push(entry.child_node_id);
        }

        // Step 2: Batch query Delta Lake for remaining nodes
        if !nodes_to_query.is_empty() {
            debug!(
                "  💾 Batch querying {} nodes from Delta Lake for partition {}",
                nodes_to_query.len(),
                parent_id.part_id()
            );

            // Build SQL with IN clause for all node_ids in this partition
            let node_list = nodes_to_query
                .iter()
                .map(|id| format!("'{}'", id))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY version DESC) as rn
                    FROM delta_table
                    WHERE part_id = '{}' AND node_id IN ({})
                ) WHERE rn = 1",
                parent_id.part_id(),
                node_list
            );

            match self.session_context.sql(&sql).await {
                Ok(df) => match df.collect().await {
                    Ok(batches) => {
                        for batch in batches {
                            let records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)
                                .map_err(|e| {
                                    TLogFSError::ArrowMessage(format!(
                                        "Failed to parse batch: {}",
                                        e
                                    ))
                                })
                                .map_err(error_utils::to_tinyfs_error)?;
                            
                            for record in records {
                                let id = FileID::new_from_ids(parent_id.part_id(), record.node_id);

                                match node_factory::create_node_from_oplog_entry(
                                    &record,
                                    id,
                                    state.clone(),
                                )
                                .await
                                {
                                    Ok(node) => {
                                        _ = results.insert(record.node_id, node);
                                    }
                                    Err(e) => {
                                        debug!("Failed to create node from oplog entry {}: {}", id, e);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Failed to collect batches for partition {}: {}",
                            parent_id.part_id(),
                            e
                        );
                    }
                },
                Err(e) => {
                    debug!("SQL query failed for partition {}: {}", parent_id.part_id(), e);
                }
            }
        }

        debug!("  ✨ BATCH_LOAD COMPLETE: returning {} nodes", results.len());
        Ok(results)
    }

    async fn load_node(&self, id: FileID, state: State) -> TinyFSResult<Node> {
        debug!("load_node {id:?}");

        // 🚀 OPTIMIZATION: Query only the latest record (O(1) instead of O(N))
        match self.query_latest_record(id).await {
            Ok(record) => {
                debug!("load_node: found latest record version {}", record.version);
                // Use node factory to create the appropriate node type
                node_factory::create_node_from_oplog_entry(&record, id, state).await
            }
            Err(e) => {
                let error_msg = e.to_string();
                debug!("query_latest_record failed with error: {error_msg}");
                Err(error_utils::to_tinyfs_error(e))
            }
        }
    }

    async fn get_factory_for_node(
        &self,
        id: FileID,
    ) -> Result<Option<String>, TLogFSError> {
        debug!("get_factory_for_node {id}");

        // Query Delta Lake for the most recent record for this node
        let records = self
            .query_records(id)
            .await
            .map_err(|e| TLogFSError::TinyFS(error_utils::to_tinyfs_error(e)))?;

        if let Some(record) = records.first() {
            // Return the factory from the most recent oplog record
            Ok(record.factory.clone())
        } else {
            // Check pending records
            let pending_record = self
                .records
                .iter()
                .find(|entry| entry.node_id == id.node_id() && entry.part_id == id.part_id())
                .cloned();

            if let Some(record) = pending_record {
                Ok(record.factory.clone())
            } else {
                // Node doesn't exist
                Err(TLogFSError::TinyFS(tinyfs::Error::NotFound(PathBuf::from(
                    format!("Node {} not found", id),
                ))))
            }
        }
    }

    async fn store_node(&mut self, node: &Node) -> TinyFSResult<()> {
        debug!(
            "TRANSACTION: OpLogPersistence::store_node() - node: {}",
            node.id
        );

        let id = node.id();

        // Create OplogEntry based on node type
        let content = match &node.node_type {
            NodeType::File(_file_handle) => {
                // NOT SURE WHAT IS HAPPENING HERE @@@
                //

                // let file_content = tinyfs::buffer_helpers::read_file_to_vec(file_handle)
                //     .await
                //     .map_err(|e| tinyfs::Error::Other(format!("File content error: {}", e)))?;
                // let content_len = file_content.len();
                // debug!("TRANSACTION: store_node() - file has {content_len} bytes of content");

                // // Query the file handle's metadata to get the entry type
                // let metadata = file_handle
                //     .metadata()
                //     .await
                //     .map_err(|e| tinyfs::Error::Other(format!("Metadata query error: {}", e)))?;

                // // Check if this file was written as a large file by looking for existing large file storage
                // // This handles the case where TinyFS async writer used HybridWriter but the memory content is empty
                // if file_content.is_empty() {
                //     // Check if there's an existing large file stored for this node
                //     if let Some(_existing_entry) = self
                //         .find_existing_large_file_entry(node_id, part_id)
                //         .await
                //     {
                //         debug!(
                //             "TRANSACTION: store_node() - found existing large file entry for {node_id}, skipping duplicate"
                //         );
                //         return Ok(()); // Don't create duplicate entry
                //     }

                //     // Also skip empty files that will be written to later - they'll be handled by the file writer
                //     debug!(
                //         "TRANSACTION: store_node() - empty file content for {node_hex}, skipping to avoid duplicate with file writer"
                //     );
                //     return Ok(());
                // }

                // (metadata.entry_type, file_content)

                // @@@ NOT SURE????!!!
                vec![]
            }
            NodeType::Directory(_) => {
                // Check if this directory has pending operations in this transaction
                // If so, skip creating an empty entry - flush_directory_operations will create v0 with content
                // This matches the file pattern where empty content is skipped
                let has_pending = self.has_pending_operations(id);
                debug!(
                    "TRANSACTION: store_node() - directory {} has_pending_operations={}",
                    id, has_pending
                );
                if has_pending {
                    debug!(
                        "TRANSACTION: store_node() - directory {} has pending operations, skipping empty entry",
                        id
                    );
                    return Ok(());
                }

                // No pending operations - this is a truly empty leaf directory
                // Create 0-byte entry (not Arrow IPC serialization)
                debug!(
                    "TRANSACTION: store_node() - directory {} is empty leaf, creating 0-byte entry",
                    id
                );
                vec![]
            }
            NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle
                    .readlink()
                    .await
                    .map_err(|e| tinyfs::Error::Other(format!("Symlink readlink error: {}", e)))?;
                let target_bytes = target.to_string_lossy().as_bytes().to_vec();
                target_bytes
            }
        };

        let now = Utc::now().timestamp_micros();

        let next_version = self
            .get_next_version_for_node(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        let oplog_entry =
            OplogEntry::new_inline(id, now, next_version, content, self.txn_seq);

        self.records.push(oplog_entry);
        Ok(())
    }

    // async fn exists_node(&self, id: NodeID) -> TinyFSResult<bool> {
    //     // Check pending records first (uncommitted writes in this transaction)
    //     let in_pending = self
    //         .records
    //         .iter()
    //         .any(|r| r.node_id == node_id && r.part_id == part_id);
    //     if in_pending {
    //         return Ok(true);
    //     }

    //     // Check created directories (tracked but not yet written to records)
    //     if self.created_directories.contains(&node_id) {
    //         return Ok(true);
    //     }

    //     // Check committed records in database
    //     let records = self
    //         .query_records(part_id, node_id)
    //         .await
    //         .map_err(error_utils::to_tinyfs_error)?;

    //     Ok(!records.is_empty())
    // }

    async fn load_directory_entries(
        &self,
        id: FileID,
    ) -> TinyFSResult<HashMap<String, DirectoryEntry>> {
        let all_entries = self
            .query_directory_entries(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        // With full snapshots, all entries in the snapshot exist (no delete operations)
        // Return full DirectoryEntry for each entry
        let mut current_state = HashMap::new();
        for entry in all_entries {
            _ = current_state.insert(entry.name.clone(), entry);
        }

        Ok(current_state)
    }

    async fn load_symlink_target(&self, id: FileID) -> TinyFSResult<PathBuf> {
        let records = self
            .query_records(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        if let Some(record) = records.first() {
            if record.file_type == EntryType::Symlink {
                let content = record.content.clone().ok_or_else(|| {
                    tinyfs::Error::Other("Symlink content is missing".to_string())
                })?;
                let target_str = String::from_utf8(content).map_err(|e| {
                    tinyfs::Error::Other(format!("Invalid UTF-8 in symlink target: {}", e))
                })?;
                Ok(PathBuf::from(target_str))
            } else {
                Err(tinyfs::Error::Other(
                    "Expected symlink node type".to_string(),
                ))
            }
        } else {
            Err(tinyfs::Error::NotFound(PathBuf::from(format!(
                "Symlink {id} not found"
            ))))
        }
    }

    /// Update directory content with new encoded entries
    /// Called by OpLogDirectory::insert to persist directory changes
    async fn update_directory_content(
        &mut self,
        id: FileID,
        content: Vec<u8>,
    ) -> Result<(), TLogFSError> {
        let now = Utc::now().timestamp_micros();
        
        let next_version = self
            .get_next_version_for_node(id)
            .await?;
        
        let oplog_entry = OplogEntry::new_inline(
            id,
            now,
            next_version,
            content,
            self.txn_seq,
        );
        
        self.records.push(oplog_entry);
        Ok(())
    }

    async fn create_file_node(&mut self, id: FileID, state: State) -> TinyFSResult<Node> {
        // Create file node in memory only - no immediate persistence
        self.store_file_content_with_type(id, &[])
            .await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

        node_factory::create_file_node(id, state)
    }

    async fn create_directory_node(&self, id: FileID, state: State) -> TinyFSResult<Node> {
        node_factory::create_directory_node(id, state)
    }

    async fn create_symlink_node(
        &mut self,
        id: FileID,
        target: &Path,
        state: State,
    ) -> TinyFSResult<Node> {
        // Create symlink node in memory only - store_node will persist it
        node_factory::create_symlink_node(id, target, state)
    }

    async fn metadata(&self, id: FileID) -> TinyFSResult<NodeMetadata> {
        debug!("metadata: querying id={id}");

        // Query Delta Lake for the most recent record for this node using the correct partition
        let records = self
            .query_records(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        let record_count = records.len();
        debug!("metadata: found {record_count} records");

        // Debug: log all records to understand the issue
        for (i, record) in records.iter().enumerate() {
            let file_type_str = format!("{:?}", record.file_type);
            let version = record.version;
            let timestamp = record.timestamp;
            debug!(
                "metadata: record[{i}] - file_type={file_type_str}, version={version}, timestamp={timestamp}"
            );
        }

        if let Some(record) = records.first() {
            // Use the record directly - it's already an OplogEntry with metadata() method
            let file_type_str = format!("{:?}", record.file_type);
            debug!(
                "metadata: returning consolidated metadata from OplogEntry - using file_type={file_type_str}"
            );
            Ok(record.metadata())
        } else {
            debug!("metadata: no records found");
            // Node doesn't exist
            Err(tinyfs::Error::not_found(format!("Node {id}")))
        }
    }

    // async fn query_directory_entry(
    //     &self,
    //     id: FileID,
    //     entry_name: &str,
    // ) -> TinyFSResult<Option<(NodeID, EntryType)>> {
    //     match self.query_single_directory_entry(part_id, entry_name).await {
    //         Ok(Some(entry)) => {
    //             // With full snapshots, if entry exists in result, it's a valid entry
    //             let child_node_id = entry.child_node_id;
    //             Ok(Some((child_node_id, entry.entry_type)))
    //         }
    //         Ok(None) => Ok(None),
    //         Err(e) => Err(error_utils::to_tinyfs_error(e)),
    //     }
    // }

    // async fn update_directory_entry(
    //     &mut self,
    //     part_id: NodeID,
    //     entry_name: &str,
    //     operation: DirectoryOperation,
    // ) -> TinyFSResult<()> {
    //     // Enhanced directory coalescing - accumulate operations with node types for batch processing
    //     let dir_ops = self.operations.entry(part_id).or_default();

    //     debug!(
    //         "update_directory_entry: part_id={}, entry_name='{}', txn_seq={}",
    //         part_id, entry_name, self.txn_seq
    //     );

    //     // All operations must now include node type - no legacy conversion
    //     _ = dir_ops.insert(entry_name.to_string(), operation);
    //     Ok(())
    // }

    // Versioning operations implementation

    async fn list_file_versions(&self, id: FileID) -> TinyFSResult<Vec<FileVersionInfo>> {
        debug!("list_file_versions called for id={id}");

        let mut records = self
            .query_records(id)
            .await
            .map_err(error_utils::to_tinyfs_error)?;

        let record_count = records.len();
        debug!("list_file_versions found {record_count} records for node {id}");

        // Sort records by version number (which should match timestamp order anyway)
        records.sort_by_key(|record| record.version);

        let version_infos = records
            .into_iter()
            .map(|record| {
                // Use the actual database version number, not a re-enumerated logical version
                let version = record.version as u64;

                let size = if record.is_large_file() {
                    record.size.unwrap_or(0)
                } else {
                    record.content.as_ref().map(|c| c.len() as i64).unwrap_or(0)
                };

                // Extract extended metadata for file:series
                let extended_metadata = if record.file_type.is_series_file() {
                    let mut metadata = HashMap::new();
                    if let (Some(min_time), Some(max_time)) =
                        (record.min_event_time, record.max_event_time)
                    {
                        _ = metadata.insert("min_event_time".to_string(), min_time.to_string());
                        _ = metadata.insert("max_event_time".to_string(), max_time.to_string());
                    }
                    if let Some(attrs) = &record.extended_attributes {
                        _ = metadata.insert("extended_attributes".to_string(), attrs.clone());
                    }
                    Some(metadata)
                } else {
                    None
                };

                FileVersionInfo {
                    version,
                    timestamp: record.timestamp,
                    size: size as u64, // Cast back to u64 for tinyfs interface
                    sha256: record.sha256.clone(),
                    entry_type: record.file_type,
                    extended_metadata,
                }
            })
            .collect();

        Ok(version_infos)
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> TinyFSResult<Vec<u8>> {
        // OPTIMIZATION: Query for specific version instead of fetching all versions
        // Query for specific version only
        let sql = format!(
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' AND version = {} LIMIT 1",
            id.part_id(),
            id.node_id(),
            version
        );

        let records = match self.session_context.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let mut records = Vec::new();
                    for batch in batches {
                        let batch_records: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)
                            .map_err(|e| {
                                tinyfs::Error::Other(format!("Failed to deserialize record: {}", e))
                            })?;
                        records.extend(batch_records);
                    }
                    records
                }
                Err(e) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Query execution failed: {}",
                        e
                    )));
                }
            },
            Err(e) => {
                return Err(tinyfs::Error::Other(format!("SQL parse failed: {}", e)));
            }
        };

        // Check pending records too (for uncommitted writes)
        let pending_record = self
            .records
            .iter()
            .find(|r| r.part_id == id.part_id() && r.node_id == id.node_id() && r.version == version as i64)
            .cloned();

        let target_record = {
            pending_record
                .or_else(|| records.into_iter().next())
                .ok_or_else(|| {
                    tinyfs::Error::NotFound(PathBuf::from(format!(
                        "Version {version} of file {id} not found",
                    )))
                })?
        };

        // Load content based on file type
        if target_record.is_large_file() {
            // Large file: read from external storage
            let sha256 = target_record.sha256.as_ref().ok_or_else(|| {
                tinyfs::Error::Other("Large file entry missing SHA256".to_string())
            })?;

            let large_file_path = crate::large_files::find_large_file_path(&self.path, sha256)
                .await
                .map_err(|e| {
                    tinyfs::Error::Other(format!("Error searching for large file: {}", e))
                })?
                .ok_or_else(|| {
                    tinyfs::Error::NotFound(PathBuf::from(format!(
                        "Large file with SHA256 {} not found",
                        sha256
                    )))
                })?;

            tokio::fs::read(&large_file_path)
                .await
                .map_err(|e| tinyfs::Error::Other(format!("Failed to read large file: {}", e)))
        } else {
            // Small file: content stored inline
            target_record
                .content
                .ok_or_else(|| tinyfs::Error::Other("Small file entry missing content".to_string()))
        }
    }

    async fn set_extended_attributes(
        &mut self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> TinyFSResult<()> {
        debug!(
            "set_extended_attributes searching for node_id={id}"
        );
        let records_count = self.records.len();
        debug!("set_extended_attributes current pending records count: {records_count}");

        // Find the pending record with max version for this node/part in current transaction
        let mut max_version = -1;
        let mut target_index = None;

        for (index, record) in self.records.iter().enumerate() {
            let record_node = &record.node_id;
            let record_part = &record.part_id;
            let record_version = record.version;
            debug!(
                "set_extended_attributes checking record[{index}]: node_id={record_node}, part_id={record_part}, version={record_version}"
            );

            if record.node_id == id.node_id() && record.part_id == id.part_id() {
                debug!(
                    "set_extended_attributes found matching record at index {index} with version {record_version}"
                );
                if record.version > max_version {
                    max_version = record.version;
                    target_index = Some(index);
                }
            }
        }

        let index = target_index.ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "No pending version found for node {} - extended attributes can only be set on files created in the current transaction",
                id
            ))
        })?;

        // Check for special temporal override attributes and handle them separately
        let mut remaining_attributes = attributes;
        let mut min_override = None;
        let mut max_override = None;

        let attrs_count = remaining_attributes.len();
        info!(
            "set_extended_attributes processing attributes for node {id} at index {index}, attrs_count: {attrs_count}"
        );

        // Extract temporal overrides if present
        if let Some(min_val) =
            remaining_attributes.remove(crate::schema::duckpond::MIN_TEMPORAL_OVERRIDE)
        {
            info!("set_extended_attributes found min_temporal_override: {min_val}");
            match min_val.parse::<i64>() {
                Ok(timestamp) => {
                    min_override = Some(timestamp);
                    info!(
                        "set_extended_attributes parsed min_temporal_override timestamp: {timestamp}"
                    );
                }
                Err(e) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Invalid min_temporal_override value '{}': {}",
                        min_val, e
                    )));
                }
            }
        }

        if let Some(max_val) =
            remaining_attributes.remove(crate::schema::duckpond::MAX_TEMPORAL_OVERRIDE)
        {
            info!("set_extended_attributes found max_temporal_override: {max_val}");
            match max_val.parse::<i64>() {
                Ok(timestamp) => {
                    max_override = Some(timestamp);
                    info!(
                        "set_extended_attributes parsed max_temporal_override timestamp: {timestamp}"
                    );
                }
                Err(e) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Invalid max_temporal_override value '{}': {}",
                        max_val, e
                    )));
                }
            }
        }

        // Set the temporal override fields directly in the OplogEntry
        if let Some(min_ts) = min_override {
            info!(
                "set_extended_attributes setting min_override to {min_ts} for node {id}"
            );
            self.records[index].min_override = Some(min_ts);
        }
        if let Some(max_ts) = max_override {
            info!(
                "set_extended_attributes setting max_override to {max_ts} for node {id}"
            );
            self.records[index].max_override = Some(max_ts);
        }

        if min_override.is_some() || max_override.is_some() {
            info!(
                "set_extended_attributes final record state for node {id} - temporal overrides set"
            );
        }

        // Store remaining attributes as JSON (if any)
        if !remaining_attributes.is_empty() {
            let attributes_json = serde_json::to_string(&remaining_attributes).map_err(|e| {
                tinyfs::Error::Other(format!("Failed to serialize extended attributes: {}", e))
            })?;
            self.records[index].extended_attributes = Some(attributes_json);
        }

        Ok(())
    }

    // // Dynamic node factory methods
    // async fn create_dynamic_node(
    //     &mut self,
    //     id: FileID,
    //     entry_type: EntryType,
    //     factory_type: &str,
    //     config_content: Vec<u8>,
    // ) -> TinyFSResult<NodeID> {
    //     self.create_dynamic_node(id, entry_type, factory_type, config_content)
    //         .await
    //         .map_err(error_utils::to_tinyfs_error)
    // }
}

/// Serialization utilities for Arrow IPC format
mod serialization {
    use super::*;
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    /// Generic serialization function for Arrow IPC format
    pub fn serialize_to_arrow_ipc<T>(items: &[T]) -> Result<Vec<u8>, TLogFSError>
    where
        T: Clone + ForArrow + serde::Serialize,
    {
        let batch = serde_arrow::to_record_batch(&T::for_arrow(), &items.to_vec())?;

        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default();
        let mut writer =
            StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
                .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;

        Ok(buffer)
    }

    /// Generic deserialization function for Arrow IPC format
    pub fn deserialize_from_arrow_ipc<T>(content: &[u8]) -> Result<Vec<T>, TLogFSError>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        // Debug logging with size validation - catch corrupted IPC streams early
        let content_size = content.len();
        debug!(
            "deserialize_from_arrow_ipc: processing {} bytes of IPC data",
            content_size
        );

        // Fail fast on unreasonably large content for directory entries
        const MAX_REASONABLE_DIRECTORY_SIZE: usize = 10 * 1024 * 1024; // 10MB should be plenty for directory metadata
        if content_size > MAX_REASONABLE_DIRECTORY_SIZE {
            return Err(TLogFSError::ArrowMessage(format!(
                "🚨 CORRUPTED IPC DATA: Content size {} bytes exceeds reasonable limit of {} bytes. \
                This indicates corrupted Arrow IPC stream data. \
                Directory metadata should never be this large.",
                content_size, MAX_REASONABLE_DIRECTORY_SIZE
            )));
        }

        // Log first few bytes for corruption diagnosis
        if content_size >= 8 {
            let header_bytes = &content[0..8];
            debug!(
                "deserialize_from_arrow_ipc: IPC header bytes: {:02x?}",
                header_bytes
            );
        } else {
            warn!(
                "deserialize_from_arrow_ipc: Content too small ({} bytes) for valid IPC stream",
                content_size
            );
        }

        let mut reader =
            StreamReader::try_new(std::io::Cursor::new(content), None).map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to create IPC StreamReader for {} bytes: {}. \
                This may indicate corrupted Arrow IPC stream data.",
                    content_size, e
                ))
            })?;

        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to read IPC batch from {} bytes: {}. \
                This may indicate corrupted Arrow IPC stream data.",
                    content_size, e
                ))
            })?;

            debug!(
                "deserialize_from_arrow_ipc: Successfully read batch with {} rows, {} columns",
                batch.num_rows(),
                batch.num_columns()
            );

            let entries: Vec<T> = serde_arrow::from_record_batch(&batch)?;
            Ok(entries)
        } else {
            debug!("deserialize_from_arrow_ipc: No batches found in IPC stream");
            Ok(Vec::new())
        }
    }

    // This function is no longer needed after Phase 2 abstraction consolidation
    // It was part of the old Record-based approach that caused double-nesting issues
}

/// Error handling utilities to reduce boilerplate
mod error_utils {
    use super::*;

    /// Convert TLogFSError to TinyFSResult
    pub fn to_tinyfs_error(e: TLogFSError) -> tinyfs::Error {
        // @@@ No
        tinyfs::Error::Other(e.to_string())
    }
}

/// Node creation utilities to reduce duplication
mod node_factory {
    use super::*;

    /// Create a file node
    pub fn create_file_node(id: FileID, state: State) -> Result<Node, tinyfs::Error> {
        let oplog_file = crate::file::OpLogFile::new(id, state);
        let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
        Ok(Node::new(id, NodeType::File(file_handle)))
    }

    /// Create a directory node
    pub fn create_directory_node(id: FileID, state: State) -> Result<Node, tinyfs::Error> {
        debug!("create directory {id}");
        let oplog_dir = OpLogDirectory::new(id, state);
        let dir_handle = OpLogDirectory::create_handle(oplog_dir);
        Ok(Node::new(id, NodeType::Directory(dir_handle)))
    }

    /// Create a symlink node with the given target
    pub fn create_symlink_node(
        id: FileID,
        target: &Path,
        state: State,
    ) -> Result<Node, tinyfs::Error> {
        let oplog_symlink = OpLogSymlink::new(id, state);
        let symlink_handle = OpLogSymlink::create_handle(oplog_symlink);
        Ok(Node::new(id, NodeType::Symlink(symlink_handle)))
    }

    /// Create a node from an OplogEntry
    pub async fn create_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        id: FileID,
        state: State,
    ) -> Result<Node, tinyfs::Error> {
        // Handle static nodes (traditional TLogFS nodes)
        match oplog_entry.file_type {
            EntryType::DirectoryDynamic
            | EntryType::FileDataDynamic
            | EntryType::FileTableDynamic
            | EntryType::FileSeriesDynamic => {
                assert!(id.entry_type().is_dynamic());
                assert!(oplog_entry.factory.is_some());

                let factory_type = oplog_entry.factory.as_ref().expect("factory");
                return create_dynamic_node_from_oplog_entry(oplog_entry, id, state, factory_type)
                    .await;
            }
            EntryType::FileDataPhysical
            | EntryType::FileTablePhysical
            | EntryType::FileSeriesPhysical => {
                let oplog_file = crate::file::OpLogFile::new(id, state);
                let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
                Ok(Node::new(id, NodeType::File(file_handle)))
            }
            EntryType::DirectoryPhysical => {
                let oplog_dir = OpLogDirectory::new(id, state);
                let dir_handle = OpLogDirectory::create_handle(oplog_dir);
                Ok(Node::new(id, NodeType::Directory(dir_handle)))
            }
            EntryType::Symlink => {
                let oplog_symlink = OpLogSymlink::new(id, state);
                let symlink_handle = OpLogSymlink::create_handle(oplog_symlink);
                Ok(Node::new(id, NodeType::Symlink(symlink_handle)))
            }
        }
    }

    /// Create a dynamic node from an OplogEntry with factory type
    async fn create_dynamic_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        // @@@ Unclear about this, used to pass a name, need the parent ??
        // @@@ OH parent_id has parent identity, maybe, below node_id.to_string()
        id: FileID,
        state: State,
        factory_type: &str,
    ) -> Result<Node, tinyfs::Error> {
        let cache_key = DynamicNodeKey::new(
            id.part_id(),
            // @@@ BOGUS
            id.node_id().to_string(),
        );
        {
            let cache = state
                .dynamic_node_cache
                .lock()
                .expect("Failed to acquire dynamic node cache lock");
            if let Some(existing) = cache.get(&cache_key) {
                return Ok(existing.clone());
            }
        }

        // Get configuration from the oplog entry
        let config_content = oplog_entry.content.as_ref().ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "Dynamic node missing configuration for factory '{}'",
                factory_type
            ))
        })?;

        // Create context with all template variables (vars, export, and any other keys)
        let context = FactoryContext::new(state.clone(), id);

        // Use context-aware factory registry to create the appropriate node type
        let node_type = match oplog_entry.file_type {
            EntryType::DirectoryDynamic => {
                let dir_handle = FactoryRegistry::create_directory(
                    factory_type,
                    config_content,
                    context.clone(),
                )?;
                NodeType::Directory(dir_handle)
            }
            _ => {
                // For file factories, the config_content IS the file content
                // (For executable factories, this is their configuration; for template factories, this is their input)
                // We don't call create_file here - that's for programmatic file creation, not reading back stored content
                let config_file = crate::factory::ConfigFile::new(config_content.clone());
                let file_handle = config_file.create_handle();
                NodeType::File(file_handle)
            }
        };

	let node = Node::new(id, node_type);

        // Insert into cache
        {
            let mut cache = state
                .dynamic_node_cache
                .lock()
                .expect("Failed to acquire dynamic node cache lock");
            _ = cache.insert(cache_key, node.clone());
        }

        Ok(node)
    }
}
