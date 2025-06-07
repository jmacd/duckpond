// Core TinyLogFS hybrid filesystem implementation
use super::{TinyLogFSError, TransactionState, FilesystemOperation, OplogEntry, DirectoryEntry};
use tinyfs::{FS, WD, NodePath, Error as TinyFSError};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, Duration};
use deltalake::{DeltaOps, protocol::SaveMode};
use datafusion::prelude::SessionContext;
use datafusion::logical_expr::col;
use uuid::Uuid;

/// Core TinyLogFS structure with single-threaded design
#[derive(Debug)]
pub struct TinyLogFS {
    // Fast in-memory filesystem for hot operations (single-threaded)
    memory_fs: FS,
    
    // Persistent storage configuration
    oplog_store_path: String,
    
    // Transaction state using Arrow Array builders
    transaction_state: RefCell<TransactionState>,
    
    // Node metadata tracking
    node_metadata: RefCell<HashMap<String, NodeMetadata>>,
    
    // Last sync timestamp
    last_sync_timestamp: RefCell<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct NodeMetadata {
    pub node_id: String,
    pub file_type: FileType,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub parent_id: Option<String>,
    pub is_dirty: bool,
}

#[derive(Debug, Clone)]
pub enum FileType {
    File,
    Directory,
    Symlink { target: PathBuf },
}

#[derive(Debug)]
pub struct CommitResult {
    pub operations_committed: usize,
    pub transaction_id: String,
    pub commit_duration: Duration,
    pub bytes_written: u64,
    pub record_batch_size: usize,
}

#[derive(Debug)]
pub struct RestoreResult {
    pub nodes_restored: usize,
    pub restore_duration: Duration,
    pub target_timestamp: SystemTime,
}

#[derive(Debug)]
pub struct TinyLogFSStatus {
    pub pending_operations: usize,
    pub total_nodes: usize,
    pub last_commit: Option<SystemTime>,
    pub memory_usage: usize,
    pub transaction_id: String,
}

impl TinyLogFS {
    /// Create new TinyLogFS instance, loading existing state from OpLog
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError> {
        let memory_fs = FS::new();
        
        let instance = Self {
            memory_fs,
            oplog_store_path: store_path.to_string(),
            transaction_state: RefCell::new(TransactionState::new()),
            node_metadata: RefCell::new(HashMap::new()),
            last_sync_timestamp: RefCell::new(SystemTime::now()),
        };
        
        // Load existing state from OpLog if it exists
        if std::path::Path::new(store_path).exists() {
            // TODO: Implement restore_from_oplog
            // instance.restore_from_oplog().await?;
        }
        
        Ok(instance)
    }
    
    /// Initialize empty TinyLogFS with root directory
    pub async fn init_empty(store_path: &str) -> Result<Self, TinyLogFSError> {
        // Create the store directory if it doesn't exist
        std::fs::create_dir_all(store_path)?;
        
        // Initialize the oplog with root directory
        super::super::tinylogfs::create_oplog_table(store_path).await
            .map_err(|e| TinyLogFSError::OpLog(e))?;
        
        // Create TinyLogFS instance
        let memory_fs = FS::new();
        let instance = Self {
            memory_fs,
            oplog_store_path: store_path.to_string(),
            transaction_state: RefCell::new(TransactionState::new()),
            node_metadata: RefCell::new(HashMap::new()),
            last_sync_timestamp: RefCell::new(SystemTime::now()),
        };
        
        // Create root directory in memory
        let root_wd = instance.memory_fs.working_dir();
        let root_node_id = generate_node_id();
        
        // Add root directory metadata
        {
            let mut metadata = instance.node_metadata.borrow_mut();
            metadata.insert(root_node_id.clone(), NodeMetadata {
                node_id: root_node_id.clone(),
                file_type: FileType::Directory,
                created_at: SystemTime::now(),
                modified_at: SystemTime::now(),
                parent_id: None,
                is_dirty: false, // Already persisted by create_oplog_table
            });
        }
        
        Ok(instance)
    }
    
    // === Core Filesystem Operations ===
    
    /// Create a new file with content, returns NodePath for navigation
    pub async fn create_file(&self, path: &Path, content: &[u8]) -> Result<NodePath, TinyLogFSError> {
        let node_id = generate_node_id();
        let parent_id = self.get_parent_node_id(path)?;
        
        // Create file in memory filesystem using WD context
        let root_wd = self.memory_fs.root();
        let file_node = root_wd.create_file_path(path, content)
            .map_err(TinyLogFSError::TinyFS)?;
        
        // Add to transaction
        let operation = FilesystemOperation::CreateFile {
            path: path.to_path_buf(),
            content: content.to_vec(),
            node_id: node_id.clone(),
            parent_id: parent_id.clone(),
        };
        self.transaction_state.borrow_mut().add_operation(&operation)?;
        
        // Update metadata
        {
            let mut metadata = self.node_metadata.borrow_mut();
            metadata.insert(node_id.clone(), NodeMetadata {
                node_id: node_id.clone(),
                file_type: FileType::File,
                created_at: SystemTime::now(),
                modified_at: SystemTime::now(),
                parent_id: Some(parent_id),
                is_dirty: true,
            });
        }
        
        // Create NodePath for the new file
        let working_dir = self.memory_fs.working_dir();
        let node_path = working_dir.create_node_path(file_node)
            .map_err(TinyLogFSError::TinyFS)?;
        
        Ok(node_path)
    }
    
    /// Create a new directory, returns WorkingDirectory for navigation  
    pub async fn create_directory(&self, path: &Path) -> Result<WD, TinyLogFSError> {
        let node_id = generate_node_id();
        let parent_id = self.get_parent_node_id(path).ok();
        
        // Create directory in memory filesystem
        let dir_node = self.memory_fs.create_directory()
            .map_err(TinyLogFSError::TinyFS)?;
        
        // Add to transaction
        let operation = FilesystemOperation::CreateDirectory {
            path: path.to_path_buf(),
            node_id: node_id.clone(),
            parent_id: parent_id.clone(),
        };
        self.transaction_state.borrow_mut().add_operation(&operation)?;
        
        // Update metadata
        {
            let mut metadata = self.node_metadata.borrow_mut();
            metadata.insert(node_id.clone(), NodeMetadata {
                node_id: node_id.clone(),
                file_type: FileType::Directory,
                created_at: SystemTime::now(),
                modified_at: SystemTime::now(),
                parent_id,
                is_dirty: true,
            });
        }
        
        // Create working directory for the new directory
        let working_dir = self.memory_fs.working_dir_from_node(dir_node)
            .map_err(TinyLogFSError::TinyFS)?;
        
        Ok(working_dir)
    }
    
    /// Read file content by path
    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>, TinyLogFSError> {
        // Try to read from memory first
        let working_dir = self.memory_fs.working_dir();
        match working_dir.open_file(path) {
            Ok(node_path) => {
                let content = node_path.read_to_vec()
                    .map_err(TinyLogFSError::TinyFS)?;
                Ok(content)
            },
            Err(TinyFSError::NotFound(_)) => {
                // If not in memory, try to restore from OpLog
                // TODO: Implement selective restore for specific nodes
                Err(TinyLogFSError::NodeNotFound { path: path.to_path_buf() })
            },
            Err(e) => Err(TinyLogFSError::TinyFS(e)),
        }
    }
    
    /// Update file content
    pub async fn update_file(&self, path: &Path, content: &[u8]) -> Result<(), TinyLogFSError> {
        let node_id = self.get_node_id_for_path(path)?;
        
        // Update in memory
        let working_dir = self.memory_fs.working_dir();
        let mut node_path = working_dir.open_file(path)
            .map_err(TinyLogFSError::TinyFS)?;
        node_path.write_all(content)
            .map_err(TinyLogFSError::TinyFS)?;
        
        // Add to transaction
        let operation = FilesystemOperation::UpdateFile {
            node_id: node_id.clone(),
            content: content.to_vec(),
        };
        self.transaction_state.borrow_mut().add_operation(&operation)?;
        
        // Update metadata
        {
            let mut metadata = self.node_metadata.borrow_mut();
            if let Some(meta) = metadata.get_mut(&node_id) {
                meta.modified_at = SystemTime::now();
                meta.is_dirty = true;
            }
        }
        
        Ok(())
    }
    
    // === State Management ===
    
    /// Get current working directory interface
    pub fn working_directory(&self) -> Result<WD, TinyLogFSError> {
        Ok(self.memory_fs.working_dir())
    }
    
    /// Get reference to underlying TinyFS for advanced operations
    pub fn memory_fs(&self) -> &FS {
        &self.memory_fs
    }
    
    /// Check if node exists (fast, memory-only check)
    pub fn exists(&self, path: &Path) -> bool {
        let working_dir = self.memory_fs.working_dir();
        working_dir.exists(path)
    }
    
    // === Persistence Operations ===
    
    /// Flush pending transactions to OpLog by snapshotting builders to RecordBatch
    pub async fn commit(&self) -> Result<CommitResult, TinyLogFSError> {
        let start_time = SystemTime::now();
        
        // Snapshot transaction state to record batch
        let record_batch = {
            let mut transaction = self.transaction_state.borrow_mut();
            if !transaction.has_pending_operations() {
                return Ok(CommitResult {
                    operations_committed: 0,
                    transaction_id: transaction.get_status().transaction_id,
                    commit_duration: Duration::from_millis(0),
                    bytes_written: 0,
                    record_batch_size: 0,
                });
            }
            transaction.snapshot_to_record_batch()?
        };
        
        let operations_count = record_batch.num_rows();
        let bytes_written = record_batch.get_array_memory_size() as u64;
        
        // Write to Delta Lake
        let delta_ops = DeltaOps::try_from_uri(&self.oplog_store_path).await
            .map_err(|e| TinyLogFSError::Commit { message: e.to_string() })?;
        
        let _table = delta_ops
            .write(vec![record_batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Commit { message: e.to_string() })?;
        
        // Update sync timestamp and clear dirty flags
        {
            *self.last_sync_timestamp.borrow_mut() = SystemTime::now();
            let mut metadata = self.node_metadata.borrow_mut();
            for meta in metadata.values_mut() {
                meta.is_dirty = false;
            }
        }
        
        let transaction_id = self.transaction_state.borrow().get_status().transaction_id;
        
        Ok(CommitResult {
            operations_committed: operations_count,
            transaction_id,
            commit_duration: start_time.elapsed().unwrap_or(Duration::from_millis(0)),
            bytes_written,
            record_batch_size: record_batch.num_rows(),
        })
    }
    
    /// Get current transaction status and pending operation count
    pub fn get_status(&self) -> TinyLogFSStatus {
        let transaction_status = self.transaction_state.borrow().get_status();
        let metadata = self.node_metadata.borrow();
        
        TinyLogFSStatus {
            pending_operations: transaction_status.operation_count,
            total_nodes: metadata.len(),
            last_commit: Some(*self.last_sync_timestamp.borrow()),
            memory_usage: std::mem::size_of::<Self>(), // Simplified calculation
            transaction_id: transaction_status.transaction_id,
        }
    }
    
    // === Query Operations ===
    
    /// Query filesystem history using SQL
    pub async fn query_history(&self, sql: &str) -> Result<Vec<arrow_array::RecordBatch>, TinyLogFSError> {
        let ctx = SessionContext::new();
        
        // Register the OplogEntry table for filesystem operations
        let oplog_table = crate::entry::OplogEntryTable::new(self.oplog_store_path.clone());
        ctx.register_table("filesystem_ops", std::sync::Arc::new(oplog_table))
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        // Execute query
        let df = ctx.sql(sql).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let batches = df.collect().await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(batches)
    }
    
    // === Transaction Integration ===
    
    /// Add pending operation to transaction builders
    pub fn add_pending_operation(&self, operation: &FilesystemOperation) -> Result<(), TinyLogFSError> {
        self.transaction_state.borrow_mut().add_operation(operation)
    }
    
    /// Create OpLogDirectory factory for TinyFS integration
    pub fn create_oplog_directory(&self, node_id: String) -> Result<super::OpLogDirectory, TinyLogFSError> {
        use std::rc::{Rc, Weak};
        
        // Create a weak reference - in practice this would need proper Rc management
        let weak_ref: Weak<RefCell<TinyLogFS>> = Weak::new();
        super::OpLogDirectory::new(node_id, weak_ref)
    }
    
    // === Helper Methods ===
    
    /// Get parent node ID for a given path
    fn get_parent_node_id(&self, path: &Path) -> Result<String, TinyLogFSError> {
        let parent_path = path.parent()
            .ok_or_else(|| TinyLogFSError::NodeNotFound { path: path.to_path_buf() })?;
        
        // For now, return root node ID if parent is root
        if parent_path == Path::new("") || parent_path == Path::new("/") {
            Ok(format!("{:016x}", 0)) // Root node ID
        } else {
            // TODO: Implement proper parent node ID lookup
            Ok(format!("{:016x}", 0)) // Fallback to root for now
        }
    }
    
    /// Get node ID for a given path
    fn get_node_id_for_path(&self, path: &Path) -> Result<String, TinyLogFSError> {
        // TODO: Implement path -> node_id mapping
        // For now, generate a temporary ID
        Ok(generate_node_id())
    }
}

/// Generate a new unique node ID
fn generate_node_id() -> String {
    format!("{:016x}", Uuid::new_v4().as_u128() & 0xFFFFFFFFFFFFFFFF)
}
