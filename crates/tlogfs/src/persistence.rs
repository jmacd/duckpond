use super::directory::OpLogDirectory;
use super::error::TLogFSError;
use super::schema::{DirectoryEntry, ForArrow, OplogEntry};
use super::symlink::OpLogSymlink;
use super::transaction_guard::TransactionGuard;
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
use provider::{FactoryContext, FactoryRegistry};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{
    EntryType, FS, FileID, FileVersionInfo, Node, NodeMetadata, NodeType, Result as TinyFSResult,
    persistence::PersistenceLayer, transaction_guard::TransactionState as TinyFsTransactionState,
};
use tokio::sync::Mutex;

pub struct OpLogPersistence {
    pub(crate) path: PathBuf,
    pub(crate) table: DeltaTable,
    pub(crate) fs: Option<FS>,
    pub(crate) state: Option<State>,
    pub(crate) last_txn_seq: i64, // Track last committed transaction sequence for validation
    /// Transaction state for enforcing single-writer pattern (shared with tinyfs)
    pub(crate) txn_state: Arc<TinyFsTransactionState>,
}

/// In-memory directory state during a transaction
/// Tracks loaded directory content and whether it has been modified
pub struct DirectoryState {
    /// Whether this directory has been modified during this transaction
    pub modified: bool,
    /// Map of child name -> DirectoryEntry for O(1) lookups and duplicate detection
    pub mapping: HashMap<String, tinyfs::DirectoryEntry>,
}

impl DirectoryState {
    /// Create a new unmodified directory state from entries
    fn new_unmodified(entries: Vec<tinyfs::DirectoryEntry>) -> Self {
        let mapping = entries
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        Self {
            modified: false,
            mapping,
        }
    }

    /// Create a new empty unmodified directory state
    fn new_empty() -> Self {
        Self {
            modified: false,
            mapping: HashMap::new(),
        }
    }
}

pub struct InnerState {
    path: PathBuf,
    table: DeltaTable,        // The Delta table for this transaction
    records: Vec<OplogEntry>, // @@@ LINEAR SEARCH
    directories: HashMap<FileID, DirectoryState>,
    session_context: Arc<SessionContext>,
    txn_seq: i64,
}

#[derive(Clone)]
pub struct State {
    inner: Arc<Mutex<InnerState>>,
    /// TinyFS ObjectStore instance - shared with SessionContext
    object_store: Arc<tokio::sync::OnceCell<Arc<crate::TinyFsObjectStore<Self>>>>,
    /// The DataFusion SessionContext - stored outside the lock to avoid deadlocks
    /// This is the same instance stored in inner.session_context
    session_context: Arc<SessionContext>,
    /// Template variables for CLI variable expansion - mutable shared state
    template_variables: Arc<std::sync::Mutex<HashMap<String, serde_json::Value>>>,
    /// Cache for TableProvider instances to avoid repeated ListingTable creation and schema inference
    /// Key: (node_id, part_id, version_selection) -> TableProvider with temporal filtering
    table_provider_cache: Arc<
        std::sync::Mutex<HashMap<TableProviderKey, Arc<dyn datafusion::catalog::TableProvider>>>,
    >,
    /// Transaction state for enforcing single-writer pattern (shared with tinyfs)
    txn_state: Arc<TinyFsTransactionState>,
}

// Re-export TableProviderKey from provider for backward compatibility
pub use provider::TableProviderKey;

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
            txn_state: Arc::new(TinyFsTransactionState::new()),
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
            txn_state: Arc::new(TinyFsTransactionState::new()),
        };

        // Initialize root directory ONLY when creating a new pond
        if create_new {
            debug!("Initializing root directory for new pond at {}", &path_str);

            let metadata = PondTxnMetadata::new(1, root_metadata.expect("metadata when new"));

            let tx = persistence.begin_write(&metadata).await?;

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
        if self.state.is_none() {
            panic!(
                "❌ CRITICAL BUG: state() called but self.state is None! This should never happen during an active transaction."
            );
        }
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
        // Sanity check - state/fs should be None between transactions
        if self.state.is_some() || self.fs.is_some() {
            panic!("🚨 INTERNAL ERROR: state/fs is Some at begin_impl start");
        }

        let inner_state =
            InnerState::new(self.path.clone(), self.table.clone(), metadata.txn_seq).await?;
        let session_context = inner_state.session_context.clone();

        let state = State {
            inner: Arc::new(Mutex::new(inner_state)),
            object_store: Arc::new(tokio::sync::OnceCell::new()),
            session_context,
            template_variables: Arc::new(std::sync::Mutex::new(HashMap::new())),
            table_provider_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            txn_state: self.txn_state.clone(),
        };

        // Complete SessionContext setup with ObjectStore registration
        {
            let inner = state.inner.lock().await;
            inner.complete_session_setup(&state).await?;
        }

        state.begin_impl().await?;

        let fs = FS::new(state.clone()).await?;
        self.fs = Some(fs.clone());
        self.state = Some(state);

        // Create the tinyfs transaction guard (this marks the transaction as active)
        let tinyfs_guard = self
            .txn_state
            .begin(fs, Some(metadata.txn_seq))
            .map_err(|e| TLogFSError::Transaction {
                message: format!("Failed to begin tinyfs transaction: {}", e),
            })?;

        Ok(TransactionGuard::new(
            tinyfs_guard,
            self,
            metadata,
            is_write,
        ))
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
        debug!(
            "🔄 Reloaded table after commit, new version: {:?}",
            self.table.version()
        );
        self.last_txn_seq = new_seq;

        // Note: txn_state clearing is handled by tinyfs::TransactionGuard drop

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

    /// Get the store path for this transaction
    /// This is used for large file external storage
    pub async fn store_path(&self) -> PathBuf {
        self.inner.lock().await.path.clone()
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
        self.inner.lock().await.async_file_reader(id).await
    }

    /// Add an arbitrary OplogEntry record to pending transaction state
    /// This is used for metadata-only operations like temporal bounds setting
    pub async fn add_oplog_entry(&self, entry: OplogEntry) -> Result<(), TLogFSError> {
        self.inner.lock().await.records.push(entry);
        Ok(())
    }

    /// Get the factory name for a specific node from the oplog
    /// Returns None if the node has no associated factory (static files/directories)
    pub async fn get_factory_for_node(&self, id: FileID) -> Result<Option<String>, TLogFSError> {
        self.inner.lock().await.get_factory_for_node(id).await
    }

    /// Create a ProviderContext from this State
    ///
    /// This allows State to be used with factories that expect a ProviderContext.
    /// Create a ProviderContext from this State with concrete values (no trait objects!)
    /// This is synchronous and lock-free - it accesses session_context directly
    pub fn as_provider_context(&self) -> provider::ProviderContext {
        // Get current template variables
        let template_vars = self
            .template_variables
            .lock()
            .expect("Failed to lock template variables")
            .clone();

        // Create provider context with State as the persistence layer
        provider::ProviderContext::new(
            self.session_context.clone(),
            template_vars,
            Arc::new(self.clone()) as Arc<dyn PersistenceLayer>,
        )
    }
}

#[async_trait]
impl PersistenceLayer for State {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn transaction_state(&self) -> Arc<TinyFsTransactionState> {
        self.txn_state.clone()
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
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> TinyFSResult<Node> {
        self.inner
            .lock()
            .await
            .create_dynamic_node(id, factory_type, config_content, self.clone())
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

    async fn get_temporal_bounds(&self, id: FileID) -> TinyFSResult<Option<(i64, i64)>> {
        self.get_temporal_overrides_for_node_id(id)
            .await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))
    }
}

impl State {
    /// Load symlink target path
    pub async fn load_symlink_target(&self, id: FileID) -> TinyFSResult<PathBuf> {
        self.inner.lock().await.load_symlink_target(id).await
    }

    /// Query oplog records for a node (used by directory operations)
    pub async fn query_records(&self, id: FileID) -> Result<Vec<OplogEntry>, TLogFSError> {
        self.inner.lock().await.query_records(id).await
    }

    /// Update directory content (used by OpLogDirectory::insert)
    /// DEPRECATED: This writes immediately. Use ensure_directory_loaded + insert_directory_entry instead.
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

    /// Ensure directory is loaded into in-memory state
    /// If not present, loads from OpLog and caches with modified: false
    pub async fn ensure_directory_loaded(&self, id: FileID) -> Result<(), TLogFSError> {
        self.inner.lock().await.ensure_directory_loaded(id).await
    }

    /// Get a directory entry from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    pub async fn get_directory_entry(
        &self,
        dir_id: FileID,
        entry_name: &str,
    ) -> Result<Option<tinyfs::DirectoryEntry>, TLogFSError> {
        Ok(self
            .inner
            .lock()
            .await
            .get_directory_entry(dir_id, entry_name))
    }

    /// Get all directory entries for a directory
    /// Directory must be loaded first via ensure_directory_loaded
    pub async fn get_all_directory_entries(
        &self,
        dir_id: FileID,
    ) -> Result<Vec<tinyfs::DirectoryEntry>, TLogFSError> {
        Ok(self.inner.lock().await.get_all_directory_entries(dir_id))
    }

    /// Insert a directory entry into in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    /// Checks for duplicates and marks directory as modified
    pub async fn insert_directory_entry(
        &self,
        dir_id: FileID,
        entry: tinyfs::DirectoryEntry,
    ) -> Result<(), TLogFSError> {
        self.inner
            .lock()
            .await
            .insert_directory_entry(dir_id, entry)
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
    pub fn object_store(&self) -> Option<Arc<crate::TinyFsObjectStore<Self>>> {
        self.object_store.get().cloned()
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
            id.part_id(),
            id.node_id(),
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
        let latest_version = file_series_records
            .iter()
            .max_by_key(|r| r.version)
            .ok_or_else(|| {
                debug!(
                    "❌ FAIL-FAST: No records found to determine latest version for node_id {id}"
                );
                TLogFSError::Transaction {
                    message: format!(
                        "Cannot find latest version for temporal overrides: node_id {id}"
                    ),
                }
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
        debug!(
            "📋 REGISTERING fundamental table 'delta_table' in State constructor, Delta table version={:?}",
            table.version()
        );
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
            directories: HashMap::new(),
            session_context: ctx,
            txn_seq,
        })
    }

    /// Complete SessionContext setup after State is available
    /// This registers the TinyFS ObjectStore which requires a State reference
    async fn complete_session_setup(&self, state: &State) -> Result<(), TLogFSError> {
        // Register the TinyFS ObjectStore with the context
        let _object_store =
            provider::register_tinyfs_object_store(&self.session_context, state.clone()).map_err(
                |e| TLogFSError::ArrowMessage(format!("Failed to register object store: {}", e)),
            )?;
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

        // Initialize root directory in memory as empty (will be written to OpLog at commit)
        _ = self
            .directories
            .insert(root_id, DirectoryState::new_empty());
        // Mark as modified so flush writes it
        if let Some(dir_state) = self.directories.get_mut(&root_id) {
            dir_state.modified = true;
        }

        debug!(
            "Initialized root directory {} in memory, marked as modified",
            root_id
        );

        Ok(())
    }

    /// Ensure directory is loaded into in-memory state
    /// If not present, loads from OpLog and caches with modified: false
    async fn ensure_directory_loaded(&mut self, id: FileID) -> Result<(), TLogFSError> {
        // Check if already loaded
        if self.directories.contains_key(&id) {
            return Ok(());
        }

        // Load from OpLog
        let entries = self.query_directory_entries(id).await?;

        // Cache in memory with modified: false
        _ = self
            .directories
            .insert(id, DirectoryState::new_unmodified(entries));
        Ok(())
    }

    /// Get a directory entry from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    fn get_directory_entry(
        &self,
        dir_id: FileID,
        entry_name: &str,
    ) -> Option<tinyfs::DirectoryEntry> {
        self.directories
            .get(&dir_id)
            .and_then(|dir_state| dir_state.mapping.get(entry_name).cloned())
    }

    /// Get all directory entries from in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    fn get_all_directory_entries(&self, dir_id: FileID) -> Vec<tinyfs::DirectoryEntry> {
        self.directories
            .get(&dir_id)
            .map(|dir_state| dir_state.mapping.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Insert a directory entry into in-memory state
    /// Directory must be loaded first via ensure_directory_loaded
    /// Checks for duplicates and marks directory as modified
    fn insert_directory_entry(
        &mut self,
        dir_id: FileID,
        entry: tinyfs::DirectoryEntry,
    ) -> Result<(), TLogFSError> {
        let dir_state = self
            .directories
            .get_mut(&dir_id)
            .ok_or_else(|| TLogFSError::Internal(format!("Directory {} not loaded", dir_id)))?;

        // Check for duplicate entry (O(1) lookup)
        if dir_state.mapping.contains_key(&entry.name) {
            return Err(TLogFSError::Internal(format!(
                "Directory entry '{}' already exists in directory {}",
                entry.name, dir_id
            )));
        }

        // Insert new entry and mark as modified
        _ = dir_state.mapping.insert(entry.name.clone(), entry);
        dir_state.modified = true;

        debug!(
            "Inserted entry into directory {}, marked as modified",
            dir_id
        );
        Ok(())
    }

    /// Begin a new transaction
    async fn begin_impl(&mut self) -> Result<(), TLogFSError> {
        // Clear any stale state (should be clean already, but just in case)
        self.records.clear();
        self.directories.clear();

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

        // Debug: log what we're about to commit
        for (i, record) in records.iter().enumerate() {
            debug!(
                "  Record {}: part_id={}, node_id={}, file_type={:?}, version={}, content_len={}, factory={:?}",
                i,
                record.part_id,
                record.node_id,
                record.file_type,
                record.version,
                record.content.as_ref().map(|c| c.len()).unwrap_or(0),
                record.factory
            );
        }

        // Convert records to RecordBatch
        debug!(
            "🔄 About to serialize {} records with serde_arrow",
            records.len()
        );
        let batches = vec![serde_arrow::to_record_batch(
            &OplogEntry::for_arrow(),
            &records,
        )?];

        // Drop records immediately after Arrow conversion to free ~100MB
        drop(records);

        debug!(
            "📊 RecordBatch created with {} batches, batch[0] has {} rows",
            batches.len(),
            batches[0].num_rows()
        );

        // DEBUG: Print first few values from part_id and node_id columns
        if batches[0].num_rows() > 0 {
            use arrow::array::StringArray;
            let part_id_col = batches[0].column(0);
            let node_id_col = batches[0].column(1);
            debug!("🔍 First 3 rows of RecordBatch:");
            for i in 0..batches[0].num_rows().min(3) {
                debug!(
                    "   Row {}: part_id={:?}, node_id={:?}",
                    i,
                    part_id_col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| a.value(i)),
                    node_id_col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| a.value(i))
                );
            }
        }

        let mut write_op = DeltaOps(table).write(batches);

        // Add commit metadata
        let properties =
            CommitProperties::default().with_metadata(metadata.to_delta_metadata().into_iter());
        write_op = write_op.with_commit_properties(properties);

        let version = write_op.await?;

        debug!(
            "✅ Delta write completed successfully, new table version: {:?}",
            version.version()
        );

        self.records.clear();
        self.directories.clear();

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

    /// Store file content reference with transaction context (used by transaction guard NewFileWriter)
    pub async fn store_file_content_ref(
        &mut self,
        id: FileID,
        content_ref: crate::file_writer::ContentRef,
        metadata: crate::file_writer::FileMetadata,
    ) -> Result<(), TLogFSError> {
        debug!("store_file_content_ref_transactional called for node_id={id}");

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
                            id, now, version, // Use proper version counter
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

    /// Flush all pending directory operations to the oplog
    ///
    /// New architecture: In-memory directory state with commit-time flush.
    /// Only writes directories that have been modified during this transaction.
    async fn flush_directory_operations(&mut self) -> Result<(), TLogFSError> {
        let modified_count = self.directories.values().filter(|ds| ds.modified).count();
        debug!(
            "flush_directory_operations: starting, txn_seq={}, directories.len()={}, modified={}",
            self.txn_seq,
            self.directories.len(),
            modified_count
        );

        let pending_dirs = std::mem::take(&mut self.directories);

        // Iterate all directories and write only those marked as modified
        for (dir_id, dir_state) in pending_dirs {
            if !dir_state.modified {
                continue; // Skip unmodified directories
            }

            debug!(
                "flush_directory_operations: writing modified directory {} with {} entries",
                dir_id,
                dir_state.mapping.len()
            );

            // Get next version for this directory
            let next_version = self.get_next_version_for_node(dir_id).await?;

            // Convert HashMap to Vec for serialization, updating version_last_modified
            let all_entries: Vec<DirectoryEntry> = dir_state
                .mapping
                .values()
                .map(|entry| {
                    // Update version_last_modified to current version
                    DirectoryEntry::new(
                        entry.name.clone(),
                        entry.child_node_id,
                        entry.entry_type,
                        next_version,
                    )
                })
                .collect();

            debug!(
                "Serializing {} entries for directory {} at version {}",
                all_entries.len(),
                dir_id,
                next_version
            );

            // Serialize full snapshot
            let content_bytes = self.serialize_directory_entries(&all_entries)?;

            // Create full snapshot OplogEntry
            let now = Utc::now().timestamp_micros();
            let record = OplogEntry::new_directory_full_snapshot(
                dir_id,
                now,
                next_version,
                content_bytes,
                self.txn_seq,
            );

            debug!(
                "📝 FLUSH CREATING RECORD: part_id={}, node_id={}, file_type={:?}, version={}, content_len={}, format={:?}",
                record.part_id,
                record.node_id,
                record.file_type,
                record.version,
                record.content.as_ref().map(|c| c.len()).unwrap_or(0),
                record.format
            );

            self.records.push(record);
        }

        debug!(
            "flush_directory_operations: complete, wrote {} modified directories",
            modified_count
        );

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
        state: State,
    ) -> Result<Node, TLogFSError> {
        debug!(
            "create_dynamic_node: id={}, entry_type={:?}, factory_type='{}', txn_seq={}",
            id,
            id.entry_type(),
            factory_type,
            self.txn_seq
        );

        let now = Utc::now().timestamp_micros();
        let next_version = self.get_next_version_for_node(id).await?;

        // Create dynamic node OplogEntry with factory field
        let oplog_entry = OplogEntry::new_dynamic_node(
            id,
            now,
            next_version,
            factory_type,
            config_content.clone(),
            self.txn_seq,
        );

        debug!(
            "📝 CREATING DYNAMIC NODE OPLOG ENTRY: part_id={}, node_id={}, file_type={:?}, version={}, factory={:?}, content_len={:?}, extended_attrs={:?}, format={:?}, txn_seq={}",
            oplog_entry.part_id,
            oplog_entry.node_id,
            oplog_entry.file_type,
            oplog_entry.version,
            oplog_entry.factory,
            oplog_entry.content.as_ref().map(|c| c.len()),
            oplog_entry
                .extended_attributes
                .as_ref()
                .map(|s| &s[0..s.len().min(100)]),
            oplog_entry.format,
            oplog_entry.txn_seq
        );

        // Add to pending records
        self.records.push(oplog_entry);

        // Create in-memory node - dynamic nodes are treated as regular files/directories
        // with factory metadata attached. Don't initialize factory here - that's done
        // by post-commit discovery or explicit FactoryRegistry calls.
        //
        // Note: OpLog entry created above contains:
        // - FileDataDynamic: config in content field
        // - DirectoryDynamic: config in extended_attributes, empty content for directory entries
        let node = match id.entry_type() {
            EntryType::FileDataDynamic => {
                // Create as regular file node - factory metadata in OpLog
                node_factory::create_file_node(id, state)
            }
            EntryType::DirectoryDynamic => {
                // For dynamic directories, the OpLog entry was created with empty content
                // (which represents an empty directory). We still need to track it in
                // the directories HashMap so subsequent operations (adding children) work.
                debug!(
                    "create_dynamic_node: tracking empty dynamic directory {} in memory",
                    id
                );
                let dir_state = DirectoryState::new_empty();
                // Don't mark as modified - OpLog entry already created above with empty content
                _ = self.directories.insert(id, dir_state);

                // Create directory node
                node_factory::create_directory_node(id, state)
            }
            _ => {
                return Err(TLogFSError::Transaction {
                    message: format!(
                        "create_dynamic_node called with non-dynamic EntryType: {:?}",
                        id.entry_type()
                    ),
                });
            }
        };

        node.map_err(|e| TLogFSError::TinyFS(e))
    }

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
            {
                // For directories, config is in extended_attributes
                // For files, config is in content
                let config_content = if record.file_type == EntryType::DirectoryDynamic {
                    // Extract from extended_attributes
                    if let Some(attrs_json) = &record.extended_attributes {
                        let attrs: serde_json::Value =
                            serde_json::from_str(attrs_json).map_err(|e| {
                                TLogFSError::ArrowMessage(format!(
                                    "Invalid JSON in extended_attributes: {}",
                                    e
                                ))
                            })?;
                        if let Some(config_b64) =
                            attrs.get("factory_config").and_then(|v| v.as_str())
                        {
                            base64::Engine::decode(
                                &base64::engine::general_purpose::STANDARD,
                                config_b64,
                            )
                            .map_err(|e| {
                                TLogFSError::ArrowMessage(format!(
                                    "Invalid base64 in factory_config: {}",
                                    e
                                ))
                            })?
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    // For files, use content field
                    record.content.clone().unwrap_or_default()
                };

                return Ok(Some((factory_type.clone(), config_content)));
            }
        }

        // Then check committed records (for existing nodes)
        let records = self.query_records(id).await?;

        if let Some(record) = records.first()
            && let Some(factory_type) = &record.factory
        {
            // For directories, config is in extended_attributes
            // For files, config is in content
            let config_content = if record.file_type == EntryType::DirectoryDynamic {
                // Extract from extended_attributes
                if let Some(attrs_json) = &record.extended_attributes {
                    let attrs: serde_json::Value =
                        serde_json::from_str(attrs_json).map_err(|e| {
                            TLogFSError::ArrowMessage(format!(
                                "Invalid JSON in extended_attributes: {}",
                                e
                            ))
                        })?;
                    if let Some(config_b64) = attrs.get("factory_config").and_then(|v| v.as_str()) {
                        base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            config_b64,
                        )
                        .map_err(|e| {
                            TLogFSError::ArrowMessage(format!(
                                "Invalid base64 in factory_config: {}",
                                e
                            ))
                        })?
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            } else {
                // For files, use content field
                record.content.clone().unwrap_or_default()
            };

            Ok(Some((factory_type.clone(), config_content)))
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

        // Verify the node exists and is a dynamic node by checking if it has a factory field
        // We don't try to parse the existing config since it might be malformed (that's why we're overwriting)
        let has_factory = self
            .records
            .iter()
            .any(|r| r.node_id == id.node_id() && r.part_id == id.part_id() && r.factory.is_some());

        if !has_factory {
            // Check committed records
            let records = self.query_records(id).await?;
            if !records
                .first()
                .map(|r| r.factory.is_some())
                .unwrap_or(false)
            {
                return Err(TLogFSError::NodeNotFound {
                    path: format!("id:{}", id).into(),
                });
            }
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
                        let batch = &batches[0];
                        debug!("🔍 Query returned {} rows", batch.num_rows());
                        debug!("🔍 Schema: {:?}", batch.schema());

                        // Print first row details
                        if batch.num_rows() > 0 {
                            for (i, field) in batch.schema().fields().iter().enumerate() {
                                let col = batch.column(i);
                                debug!("  Column {}: {} = {:?}", i, field.name(), col);
                            }
                        }

                        let records: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)?;
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
            "SELECT * FROM delta_table WHERE part_id = '{}' AND node_id = '{}' AND file_type IN ('dir:physical', 'dir:dynamic') ORDER BY version DESC LIMIT 1",
            id.part_id(),
            id.node_id()
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

    async fn load_node(&self, id: FileID, state: State) -> TinyFSResult<Node> {
        debug!("load_node {id:?}");

        // 🔍 CRITICAL FIX: Check if this is a directory in self.directories first
        // During a transaction, directories created by store_node() exist in self.directories
        // but don't have OpLog records in self.records until flush_directory_operations() is called at commit
        if self.directories.contains_key(&id) {
            debug!(
                "load_node: found directory {} in self.directories (not yet flushed to OpLog)",
                id
            );
            // Create directory node directly - it exists in memory
            return node_factory::create_directory_node(id, state);
        }

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

    async fn get_factory_for_node(&self, id: FileID) -> Result<Option<String>, TLogFSError> {
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
                // Files are created without content - actual content is written later via write_file()
                // This creates the initial file node record; subsequent writes update it
                vec![]
            }
            NodeType::Directory(_) => {
                // Check if directory already loaded in memory
                // If present, skip - will be written by flush_directory_operations if modified
                if self.directories.contains_key(&id) {
                    debug!(
                        "TRANSACTION: store_node() - directory {} already tracked in memory, skipping",
                        id
                    );
                    return Ok(());
                }

                // Directory not yet tracked - initialize empty directory in memory and mark as modified
                // This ensures flush_directory_operations will write the directory to OpLog
                debug!(
                    "TRANSACTION: store_node() - initializing empty directory {} in memory, marked as modified",
                    id
                );
                let mut dir_state = DirectoryState::new_empty();
                dir_state.modified = true; // Mark as modified so it gets written at commit
                _ = self.directories.insert(id, dir_state);
                return Ok(());
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

        let oplog_entry = OplogEntry::new_inline(id, now, next_version, content, self.txn_seq);

        self.records.push(oplog_entry);
        Ok(())
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

        let next_version = self.get_next_version_for_node(id).await?;

        let oplog_entry = OplogEntry::new_inline(id, now, next_version, content, self.txn_seq);

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
            .find(|r| {
                r.part_id == id.part_id()
                    && r.node_id == id.node_id()
                    && r.version == version as i64
            })
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
        debug!("set_extended_attributes searching for node_id={id}");
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
            info!("set_extended_attributes setting min_override to {min_ts} for node {id}");
            self.records[index].min_override = Some(min_ts);
        }
        if let Some(max_ts) = max_override {
            info!("set_extended_attributes setting max_override to {max_ts} for node {id}");
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
        let oplog_symlink = OpLogSymlink::new(id, target.to_path_buf(), state);
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
                let oplog_symlink = OpLogSymlink::new_from_persistence(id, state);
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
        // Note: Node caching is now handled by CachingPersistence decorator in tinyfs
        // No need for manual caching here

        // Get configuration from the oplog entry
        // For ALL dynamic nodes, config is stored as-is in content field (original YAML bytes)
        let config_content = oplog_entry.content.as_ref().ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "Dynamic node missing configuration for factory '{}'",
                factory_type
            ))
        })?;

        // Create context with all template variables (vars, export, and any other keys)
        let context = FactoryContext {
            context: state.as_provider_context(),
            file_id: id,
            pond_metadata: None,
        };

        debug!(
            "🔍 create_dynamic_node_from_oplog_entry: factory='{}', entry_type={:?}, config_len={}",
            factory_type,
            oplog_entry.file_type,
            config_content.len()
        );

        // Use context-aware factory registry to create the appropriate node type
        let node_type = match oplog_entry.file_type {
            EntryType::DirectoryDynamic => {
                debug!(
                    "🔍 Calling FactoryRegistry::create_directory for '{}'",
                    factory_type
                );
                let dir_handle = FactoryRegistry::create_directory(
                    factory_type,
                    config_content,
                    context.clone(),
                )
                .map_err(|e| {
                    debug!("❌ FactoryRegistry::create_directory failed: {}", e);
                    e
                })?;
                debug!("✅ FactoryRegistry::create_directory succeeded");
                NodeType::Directory(dir_handle)
            }
            EntryType::FileDataDynamic => {
                // Check if this is an executable factory
                if let Some(factory) = FactoryRegistry::get_factory(factory_type) {
                    if factory.create_file.is_some() {
                        // File factory - call create_file
                        debug!(
                            "🔍 Calling FactoryRegistry::create_file for '{}'",
                            factory_type
                        );
                        let file_handle = FactoryRegistry::create_file(
                            factory_type,
                            config_content,
                            context.clone(),
                        )
                        .await
                        .map_err(|e| {
                            debug!("❌ FactoryRegistry::create_file failed: {}", e);
                            e
                        })?;
                        debug!("✅ FactoryRegistry::create_file succeeded");
                        NodeType::File(file_handle)
                    } else {
                        // Executable factory - config IS the file content
                        debug!(
                            "🔍 Executable factory '{}' - using config as file content",
                            factory_type
                        );
                        let config_file = provider::ConfigFile::new(config_content.clone());
                        NodeType::File(config_file.create_handle())
                    }
                } else {
                    return Err(tinyfs::Error::Other(format!(
                        "Unknown factory: {}",
                        factory_type
                    )));
                }
            }
            _ => {
                // Unknown entry type - shouldn't happen
                let config_file = provider::ConfigFile::new(config_content.clone());
                let file_handle = config_file.create_handle();
                NodeType::File(file_handle)
            }
        };

        let node = Node::new(id, node_type);

        // Note: Node caching is now handled by CachingPersistence decorator in tinyfs
        // The node will be automatically cached when returned

        Ok(node)
    }
}
