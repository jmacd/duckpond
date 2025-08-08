//! # OpLog Persistence Layer - Delta Lake-Based Filesystem Storage
//!
//! This module implements a high-performance, ACID-compliant persistence layer for TinyFS
//! using Delta Lake as the storage backend. It provides versioned filesystem operations
//! with comprehensive transaction support and directory operation coalescing.
//!
//! ## Architecture Overview
//! 
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                           OpLogPersistence                                      │
//! │  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐      │
//! │  │   Transaction       │  │   Serialization     │  │   Query Engine      │      │
//! │  │   Management        │  │   (Arrow IPC)       │  │   (DataFusion)      │      │
//! │  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘      │
//! │  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐      │
//! │  │   Node Operations   │  │   Directory         │  │   I/O Metrics       │      │
//! │  │   (Files/Dirs/Links)│  │   Coalescing        │  │   Tracking          │      │
//! │  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘      │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//!                                         │
//!                                         ▼
//!                                  Delta Lake Storage
//!                              (Versioned, ACID-compliant)
//! ```
//!
//! ## Key Features
//! - **Delta Lake Integration**: Leverages Delta Lake's versioning and ACID guarantees
//! - **Arrow IPC Serialization**: Efficient binary serialization for all data structures
//! - **Directory Operation Coalescing**: Batches directory changes for performance
//! - **Transaction Sequencing**: Uses Delta Lake versions for consistent transaction ordering
//! - **Comprehensive Metrics**: Tracks I/O operations and performance characteristics
//!
//! ## Transaction Model
//! - **Single Command = Single Transaction**: Each CLI command is one atomic transaction
//! - **No Cross-Command State**: Transaction state is reset between command invocations
//! - **Commit or Rollback**: Each transaction must end with either commit or rollback
//! - **Delta Lake Ordering**: Transaction sequences follow Delta Lake version ordering
//! - **O(1) Sequence Lookup**: `table.version() + 1` gives next transaction sequence
//!
//! ## Transaction Lifecycle
//! 1. Command starts: `create_oplog_fs()` creates fresh persistence layer
//! 2. Command calls: `fs.begin_transaction()` clears any stale state
//! 3. Get sequence: `table.version() + 1` provides next transaction sequence
//! 4. Operations: All operations use the same transaction sequence
//! 5. Command ends: `fs.commit()` or `fs.rollback()` finalizes the transaction
//! 6. Process exits: All transaction state is cleared

use super::error::TLogFSError;
use super::schema::{OplogEntry, VersionedDirectoryEntry, OperationType, ForArrow};
use super::transaction_guard::TransactionGuard;
use crate::delta::DeltaTableManager;
use crate::factory::{FactoryRegistry, FactoryContext};
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{NodeID, NodeType, Result as TinyFSResult};
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use uuid7;
use chrono::Utc;
use diagnostics::*;

#[derive(Clone)]
pub struct OpLogPersistence {
    store_path: String,
    // session_ctx: SessionContext,  // Reserved for future DataFusion queries
    pending_records: Arc<tokio::sync::Mutex<Vec<OplogEntry>>>,
    // table_name: String,  // Reserved for future table operations
    /// The current active transaction sequence (derived from Delta Lake version)
    /// None means no active transaction
    current_transaction_version: Arc<tokio::sync::Mutex<Option<i64>>>,
    // Directory update coalescing - accumulate directory changes during transaction
    pending_directory_operations: Arc<tokio::sync::Mutex<HashMap<NodeID, HashMap<String, DirectoryOperation>>>>,
    delta_manager: DeltaTableManager,
    // Comprehensive I/O metrics for performance analysis
    // io_metrics: Arc<tokio::sync::Mutex<IOMetrics>>,  // Reserved for future performance tracking
}

/// Comprehensive I/O operation counters for performance analysis (reserved for future use)
#[derive(Debug, Clone, Default)]
pub struct IOMetrics {
    // High-level operation counts
    pub directory_queries: u64,
    pub file_reads: u64,
    pub file_writes: u64,
    
    // Delta Lake operation counts
    pub delta_table_opens: u64,
    pub delta_queries_executed: u64,
    pub delta_batches_processed: u64,
    pub delta_records_read: u64,
    pub delta_commits: u64,
    
    // Deserialization counts
    pub oplog_entries_deserialized: u64,
    pub directory_entries_deserialized: u64,
    pub arrow_batches_deserialized: u64,
    
    // Object store operations (future expansion)
    pub object_store_gets: u64,
    pub object_store_puts: u64,
    pub object_store_lists: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl OpLogPersistence {
    /// Creates a new OpLogPersistence instance
    /// 
    /// This constructor initializes the Delta Lake table (creating if needed),
    /// sets up the DataFusion session context, and prepares all internal state
    /// for filesystem operations.
    pub async fn new(store_path: &str) -> Result<Self, TLogFSError> {
        // Initialize diagnostics on first use
        init();
        
        let delta_manager = DeltaTableManager::new();
        
        // Check if table exists, but DON'T create it during initialization
        // Let the first transaction create it to ensure steward controls version 0
        let table_exists = match delta_manager.get_table(store_path).await {
            Ok(_) => {
                debug!("Existing Delta table found at: {store_path}");
                true
            }
            Err(_) => {
                debug!("No Delta table found at: {store_path} - will create during first commit");
                false
            }
        };
        
        let _session_ctx = SessionContext::new();
        let _table_name = format!("oplog_store_{}", uuid7::uuid7().to_string().replace("-", ""));
        
        let persistence = Self {
            store_path: store_path.to_string(),
            pending_records: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            current_transaction_version: Arc::new(tokio::sync::Mutex::new(None)),
            pending_directory_operations: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            delta_manager,
        };
        
        // Initialize NodeID counter based on existing data
        if table_exists {
            persistence.initialize_node_id_counter().await?;
        }
        
        Ok(persistence)
    }
    
    /// Get the store path for this persistence layer
    pub fn store_path(&self) -> &str {
        &self.store_path
    }
    
    /// Initialize the root directory without committing
    /// This should be called after creating a new persistence layer and starting a transaction,
    /// but before any calls to .root() that would trigger on-demand creation.
    pub async fn initialize_root_directory(&self) -> Result<(), TLogFSError> {
        let root_node_id = NodeID::root();
        let root_node_id_str = root_node_id.to_hex_string();
        
        // Check if root already exists in the committed data
        let records = match self.query_records(&root_node_id_str, Some(&root_node_id_str)).await {
            Ok(records) => records,
            Err(TLogFSError::NodeNotFound { .. }) | Err(TLogFSError::Missing) => {
                // Root doesn't exist yet - this is expected for first-time initialization
                Vec::new()
            },
            Err(e) => {
                // Real system error - Delta Lake corruption, IO failure, etc.
                // Don't mask these as they indicate serious problems that need investigation
                diagnostics::log_error!("CRITICAL: Failed to query root directory existence due to system error: {error}", error: e);
                return Err(e);
            }
        };
        
        if !records.is_empty() {
            // Root directory already exists in committed data, nothing to do
            return Ok(());
        }
        
        // Check if root already exists in pending transactions
        {
            let pending = self.pending_records.lock().await;
            let root_exists_in_pending = pending.iter().any(|entry| {
                entry.node_id == root_node_id_str && entry.part_id == root_node_id_str
            });
            
            if root_exists_in_pending {
                // Root directory already exists in pending transaction, nothing to do
                return Ok(());
            }
        }
        
        // Create root directory entry in the current transaction (without committing)
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
        
        // Add to pending records (will be committed when transaction commits)
        let mut pending = self.pending_records.lock().await;
        pending.push(root_entry);
        
        Ok(())
    }
    
    /// Initialize the NodeID counter based on the maximum node_id in the oplog
    async fn initialize_node_id_counter(&self) -> Result<(), TLogFSError> {
        // This method would scan the oplog to find the maximum node_id
        // For now, we'll implement a simple version that doesn't do anything
        // since NodeID generation is handled elsewhere
        Ok(())
    }
    
    /// Get the next transaction sequence number using Delta Lake version
    /// If we're already in a transaction, reuse the same sequence number
    /// If starting a new transaction, get the current Delta Lake version + 1
    async fn next_transaction_sequence(&self) -> Result<i64, TLogFSError> {
        transaction_utils::get_or_create_transaction_sequence(
            &self.current_transaction_version,
            &self.delta_manager,
            &self.store_path,
        ).await
    }
    
    // Large file storage support methods
    
    /// Create a hybrid writer for streaming file content
    pub fn create_hybrid_writer(&self) -> crate::large_files::HybridWriter {
        crate::large_files::HybridWriter::new(self.store_path.clone())
    }
    
    /// Store file content from hybrid writer result
    pub async fn store_file_from_hybrid_writer(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        result: crate::large_files::HybridWriterResult
    ) -> Result<(), TLogFSError> {
        use crate::large_files::LARGE_FILE_THRESHOLD;
        
        if result.size >= LARGE_FILE_THRESHOLD {
            // Large file: content already stored, just create OplogEntry with SHA256
            let size = result.size;
            info!("Storing large file: {size} bytes");
            let now = Utc::now().timestamp_micros();
            let entry = OplogEntry::new_large_file(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                tinyfs::EntryType::FileData,
                now,
                0, // Placeholder - actual version assigned by Delta Lake transaction log
                result.sha256,
                result.size as u64, // NEW: Include size parameter (cast to u64)
            );
            
            self.pending_records.lock().await.push(entry);
            Ok(())
        } else {
            // Small file: store content directly in Delta Lake
            let now = Utc::now().timestamp_micros();
            let entry = OplogEntry::new_small_file(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                tinyfs::EntryType::FileData,
                now,
                0, // Placeholder - actual version assigned by Delta Lake transaction log
                result.content,
            );
            
            self.pending_records.lock().await.push(entry);
            Ok(())
        }
    }
    
    /// Store file content using size-based strategy (convenience method for FileData)
    pub async fn store_file_content(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8]
    ) -> Result<(), TLogFSError> {
        self.store_file_content_with_type(node_id, part_id, content, tinyfs::EntryType::FileData).await
    }

    /// Store file content with entry type using size-based strategy
    pub async fn store_file_content_with_type(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8],
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
        use crate::large_files::should_store_as_large_file;
        
        if should_store_as_large_file(content) {
            // Use hybrid writer for large files
            let mut writer = self.create_hybrid_writer();
            use tokio::io::AsyncWriteExt;
            writer.write_all(content).await?;
            writer.shutdown().await?;
            let result = writer.finalize().await?;
            self.store_file_from_hybrid_writer(node_id, part_id, result).await
        } else {
            self.store_small_file_with_type(node_id, part_id, content, entry_type).await
        }
    }
    
    /// Update existing file content within the same transaction
    /// This replaces any existing entries for the same file in the current transaction
    async fn update_file_content_with_type_impl(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8],
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
        use crate::large_files::should_store_as_large_file;
        
        if should_store_as_large_file(content) {
            // TODO: Store entry type in metadata when large file support is complete
            self.update_large_file(node_id, part_id, content).await
        } else {
            self.update_small_file_with_type(node_id, part_id, content, entry_type).await
        }
    }
    
    /// Store small file directly in Delta Lake with specific entry type
    async fn store_small_file_with_type(
        &self, 
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
        
        self.pending_records.lock().await.push(entry);
        Ok(())
    }
    
    /// Update small file by replacing any existing pending entry for the same file
    async fn update_small_file_with_type(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8],
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
        // Get the next version number for this node
        let next_version = self.get_next_version_for_node(node_id, part_id).await?;
        
        let now = Utc::now().timestamp_micros();
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Handle FileSeries with temporal metadata extraction
        let entry = if entry_type == tinyfs::EntryType::FileSeries && !content.is_empty() {
            // Extract temporal metadata from Parquet content
            self.create_file_series_entry_with_temporal_extraction(
                part_id_str.clone(),
                node_id_str.clone(),
                now,
                next_version,
                content,
            ).await?
        } else {
            // Regular file entry
            OplogEntry::new_small_file(
                part_id_str.clone(),
                node_id_str.clone(),
                entry_type, // Use the provided entry type
                now,
                next_version, // Use proper version counter
                content.to_vec(),
            )
        };
        
        // Smart deduplication: replace empty files with actual content within same transaction
        // This handles the common pattern where file creation + content write happens in sequence
        let mut pending = self.pending_records.lock().await;
        
        // Check if we're replacing an empty file with actual content
        let replacing_empty_file = pending.iter().any(|existing_entry| {
            existing_entry.part_id == part_id_str 
                && existing_entry.node_id == node_id_str
                && existing_entry.content.as_ref().map_or(true, |c| c.is_empty()) // Previous entry was empty
                && !content.is_empty() // Current entry has content
        });
        
        if replacing_empty_file {
            // Remove the empty file entry - this is likely file creation followed by content write
            pending.retain(|existing_entry| {
                !(existing_entry.part_id == part_id_str && existing_entry.node_id == node_id_str)
            });
        }
        
        // Add the new entry (either as replacement or new version)
        pending.push(entry);
        Ok(())
    }
    
    /// Get the next version number for a specific node (current max + 1)
    async fn get_next_version_for_node(&self, node_id: NodeID, part_id: NodeID) -> Result<i64, TLogFSError> {
        let part_id_str = part_id.to_hex_string();
        let node_id_str = node_id.to_hex_string();
        
        // Query all records for this node and find the maximum version
        match self.query_records(&part_id_str, Some(&node_id_str)).await {
            Ok(records) => {
                let max_version = records.iter()
                    .map(|r| r.version)
                    .max()
                    .unwrap_or(0);
                Ok(max_version + 1)
            }
            Err(_e) => {
                // If query fails, start with version 1
                Ok(1)
            }
        }
    }

    /// Create a FileSeries entry with temporal metadata extraction from Parquet content
    async fn create_file_series_entry_with_temporal_extraction(
        &self,
        part_id_str: String,
        node_id_str: String,
        timestamp: i64,
        version: i64,
        content: &[u8],
    ) -> Result<OplogEntry, TLogFSError> {
        use super::schema::{extract_temporal_range_from_batch, detect_timestamp_column, ExtendedAttributes};
        use tokio_util::bytes::Bytes;
        
        // Read the Parquet data to extract temporal metadata
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
        let time_col = detect_timestamp_column(&schema)?;
        
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
        
        // Create FileSeries entry
        let entry = OplogEntry::new_file_series(
            part_id_str,
            node_id_str,
            timestamp,
            version,
            content.to_vec(),
            global_min,
            global_max,
            extended_attrs,
        );
        
        Ok(entry)
    }

    /// Update large file by replacing any existing pending entry for the same file
    async fn update_large_file(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8]
    ) -> Result<(), TLogFSError> {
        // Use hybrid writer for large files
        let mut writer = self.create_hybrid_writer();
        use tokio::io::AsyncWriteExt;
        writer.write_all(content).await?;
        writer.shutdown().await?;
        let result = writer.finalize().await?;
        self.store_file_from_hybrid_writer(node_id, part_id, result).await
    }
    
    /// Store FileSeries with temporal metadata extraction from Parquet data
    /// This method extracts min/max timestamps from the specified time column
    pub async fn store_file_series_from_parquet(
        &self,
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
            // Store as large FileSeries
            let sha256 = super::schema::compute_sha256(content);
            let size = content.len() as u64;
            let now = Utc::now().timestamp_micros();
            
            let entry = OplogEntry::new_large_file_series(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                now,
                next_version, // Use proper version counter
                sha256,
                size,
                global_min,
                global_max,
                extended_attrs,
            );
            
            // Store content externally via object store (implementation depends on configuration)
            // For now, we'll store in pending records and handle external storage during commit
            self.pending_records.lock().await.push(entry);
        } else {
            // Store as small FileSeries
            let now = Utc::now().timestamp_micros();
            let content_size = content.len();
            
            diagnostics::log_debug!("store_file_series_from_parquet - storing as small FileSeries with {content_size} bytes content", content_size: content_size);
            
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
            diagnostics::log_debug!("store_file_series_from_parquet - created OplogEntry with content size: {entry_content_size}", entry_content_size: entry_content_size);
            
            self.pending_records.lock().await.push(entry);
        }
        
        Ok((global_min, global_max))
    }
    
    /// Store FileSeries with pre-computed temporal metadata
    /// Use this when you already know the min/max event times
    /// Store FileSeries with pre-extracted temporal metadata
    /// 
    /// # Internal/Legacy Use
    /// This function is primarily for test utilities and legacy compatibility.
    /// The recommended approach is to use streaming writes with `EntryType::FileSeries`
    /// and let the OpLogFileWriter extract temporal metadata automatically during shutdown.
    #[doc(hidden)]
    pub async fn store_file_series_with_metadata(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        min_event_time: i64,
        max_event_time: i64,
        timestamp_column: &str,
    ) -> Result<(), TLogFSError> {
        use super::schema::ExtendedAttributes;
        
        // Get the next version number for this node
        let next_version = self.get_next_version_for_node(node_id, part_id).await?;
        
        // Create extended attributes with timestamp column info
        let mut extended_attrs = ExtendedAttributes::new();
        extended_attrs.set_timestamp_column(timestamp_column);
        
        // Store the FileSeries using the appropriate size strategy
        use crate::large_files::should_store_as_large_file;
        
        if should_store_as_large_file(content) {
            // Store as large FileSeries
            let sha256 = super::schema::compute_sha256(content);
            let size = content.len() as u64;
            let now = Utc::now().timestamp_micros();
            
            let entry = OplogEntry::new_large_file_series(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                now,
                next_version, // Use proper version counter
                sha256,
                size,
                min_event_time,
                max_event_time,
                extended_attrs,
            );
            
            self.pending_records.lock().await.push(entry);
        } else {
            // Store as small FileSeries
            let now = Utc::now().timestamp_micros();
            
            let entry = OplogEntry::new_file_series(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                now,
                next_version, // Use proper version counter
                content.to_vec(),
                min_event_time,
                max_event_time,
                extended_attrs,
            );
            
            self.pending_records.lock().await.push(entry);
        }
        
        Ok(())
    }
    
    /// Load file content using size-based strategy
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
                let large_file_path = crate::large_files::find_large_file_path(&self.store_path, sha256).await
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
    
    /// Begin a new transaction - fails if a transaction is already active
    async fn begin_transaction_internal(&self) -> Result<(), TLogFSError> {
        // Check if transaction is already active
        let current_transaction = self.current_transaction_version.lock().await;
        if current_transaction.is_some() {
            let tx_id = current_transaction.unwrap();
            drop(current_transaction);
            return Err(TLogFSError::Transaction { 
                message: format!("Transaction {} is already active - commit or rollback first", tx_id) 
            });
        }
        drop(current_transaction);
        
        // Clear any stale state (should be clean already, but just in case)
        self.pending_records.lock().await.clear();
        self.pending_directory_operations.lock().await.clear();
        
        // Create new transaction ID immediately
        // Get fresh table state for transaction sequencing
        self.delta_manager.invalidate_table(&self.store_path).await;
        
        let new_sequence = match deltalake::operations::DeltaOps::try_from_uri(&self.store_path).await {
            Ok(ops) => {
                let table: deltalake::DeltaTable = ops.into();
                if table.version() >= 0 {
                    // Table exists and has commits
                    table.version() + 1
                } else {
                    // Table doesn't exist or has no commits yet - this will be the first transaction
                    1
                }
            }
            Err(_) => {
                // Fallback: assume this is the first transaction
                1
            }
        };
        *self.current_transaction_version.lock().await = Some(new_sequence);
        
        info!("Started transaction {new_sequence}");
        Ok(())
    }
    
    /// Commit pending records to Delta Lake
    async fn commit_internal(&self) -> Result<(), TLogFSError> {
        self.commit_internal_with_metadata(None).await?;
        Ok(())
    }
    
    /// Commit pending records to Delta Lake with optional metadata
    async fn commit_internal_with_metadata(
        &self, 
        metadata: Option<HashMap<String, serde_json::Value>>
    ) -> Result<u64, TLogFSError> {
        let records = {
            let mut pending = self.pending_records.lock().await;
            let records = pending.drain(..).collect::<Vec<_>>();
            records
        };
        
        let count = records.len();
        if count > 0 {
            info!("Committing {count} operations");
        }
        
        if records.is_empty() {
            // No write operations to commit - this is a read-only transaction
            // Read-only transactions should be allowed to commit successfully
            info!("Committing read-only transaction (no write operations)");
            return Ok(0); // Return dummy version number for read-only transactions
        }

        // Convert records to RecordBatch
        let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &records)?;

        // Use the delta manager to write, which will create the table if needed
        let table = self.delta_manager.write_to_table_with_metadata(
            &self.store_path, 
            vec![batch], 
            deltalake::protocol::SaveMode::Append,
            metadata
        ).await.map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        
        let actual_version = table.version() as u64;
        info!("Transaction committed to version {actual_version}");
        
        Ok(actual_version)
    }
    
    /// Serialize VersionedDirectoryEntry records as Arrow IPC bytes
    fn serialize_directory_entries(&self, entries: &[VersionedDirectoryEntry]) -> Result<Vec<u8>, TLogFSError> {
        serialization::serialize_to_arrow_ipc(entries)
    }
    
    /// Serialize OplogEntry as Arrow IPC bytes
    // These functions are no longer needed after Phase 2 abstraction consolidation
    // They were part of the old Record-based approach that caused double-nesting issues
    
    /// Deserialize VersionedDirectoryEntry records from Arrow IPC bytes  
    fn deserialize_directory_entries(&self, content: &[u8]) -> Result<Vec<VersionedDirectoryEntry>, TLogFSError> {
        serialization::deserialize_from_arrow_ipc(content)
    }
    
    /// Commit with metadata for crash recovery
    pub async fn commit_with_metadata(
        &self, 
        metadata: Option<HashMap<String, serde_json::Value>>
    ) -> Result<u64, TLogFSError> {
        // First, flush any accumulated directory operations to pending records
        self.flush_directory_operations().await?;
        
        // Commit all pending records to Delta Lake with metadata
        let committed_version = self.commit_internal_with_metadata(metadata).await?;
        
        // Reset transaction state after successful commit
        transaction_utils::clear_transaction_state(
            &self.pending_records,
            &self.pending_directory_operations,
            &self.current_transaction_version,
        ).await;
        
        Ok(committed_version)
    }
    
    /// Get commit history from Delta table
    pub async fn get_commit_history(&self, limit: Option<usize>) -> Result<Vec<deltalake::kernel::CommitInfo>, TLogFSError> {
        let table = self.delta_manager.get_table(&self.store_path).await
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        
        let history = table.history(limit).await
            .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
        
        Ok(history)
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
    
    // =====================================================
    // Transaction Guard Methods
    // =====================================================
    
    /// Begin a transaction and return a transaction guard
    /// 
    /// This is the new transaction guard API that provides RAII-style transaction management
    pub async fn begin_transaction_with_guard(&self) -> Result<TransactionGuard<'_>, TLogFSError> {
        // Begin the internal transaction
        self.begin_transaction_internal().await?;
        
        // Get the transaction ID
        let tx_id = {
            let current_tx = self.current_transaction_version.lock().await;
            current_tx.expect("Transaction should be active after begin_transaction_internal")
        };
        
        debug!("Creating transaction guard for transaction {tx_id}");
        Ok(TransactionGuard::new(self, tx_id))
    }
    
    /// Load a node with transaction context (used by transaction guard)
    pub async fn load_node_transactional(&self, node_id: NodeID, part_id: NodeID, tx_id: i64) -> TinyFSResult<NodeType> {
        // Verify transaction context
        {
            let current_tx = self.current_transaction_version.lock().await;
            match *current_tx {
                Some(current_tx_id) if current_tx_id == tx_id => {
                    // Transaction context is valid
                }
                Some(other_tx_id) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Transaction context mismatch: expected {}, got {}", other_tx_id, tx_id
                    )));
                }
                None => {
                    return Err(tinyfs::Error::Other("No active transaction".to_string()));
                }
            }
        }
        
        // Delegate to existing load_node method
        self.load_node(node_id, part_id).await
    }
    
    /// Store a node with transaction context (used by transaction guard)
    pub async fn store_node_transactional(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType, tx_id: i64) -> TinyFSResult<()> {
        // Verify transaction context
        {
            let current_tx = self.current_transaction_version.lock().await;
            match *current_tx {
                Some(current_tx_id) if current_tx_id == tx_id => {
                    // Transaction context is valid
                }
                Some(other_tx_id) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Transaction context mismatch: expected {}, got {}", other_tx_id, tx_id
                    )));
                }
                None => {
                    return Err(tinyfs::Error::Other("No active transaction".to_string()));
                }
            }
        }
        
        // Delegate to existing store_node method
        self.store_node(node_id, part_id, node_type).await
    }
    
    /// Initialize root directory with transaction context (used by transaction guard)
    pub async fn initialize_root_directory_transactional(&self, tx_id: i64) -> TinyFSResult<()> {
        // Verify transaction context
        {
            let current_tx = self.current_transaction_version.lock().await;
            match *current_tx {
                Some(current_tx_id) if current_tx_id == tx_id => {
                    // Transaction context is valid
                }
                Some(other_tx_id) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Transaction context mismatch: expected {}, got {}", other_tx_id, tx_id
                    )));
                }
                None => {
                    return Err(tinyfs::Error::Other("No active transaction".to_string()));
                }
            }
        }
        
        // Delegate to existing initialize_root_directory method
        self.initialize_root_directory().await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    /// Commit a transaction with transaction context (used by transaction guard)
    pub async fn commit_transactional(&self, tx_id: i64) -> TinyFSResult<()> {
        // Verify transaction context
        {
            let current_tx = self.current_transaction_version.lock().await;
            match *current_tx {
                Some(current_tx_id) if current_tx_id == tx_id => {
                    // Transaction context is valid
                }
                Some(other_tx_id) => {
                    return Err(tinyfs::Error::Other(format!(
                        "Transaction context mismatch: expected {}, got {}", other_tx_id, tx_id
                    )));
                }
                None => {
                    return Err(tinyfs::Error::Other("No active transaction".to_string()));
                }
            }
        }
        
        // First, flush any accumulated directory operations to pending records
        self.flush_directory_operations().await
            .map_err(error_utils::to_tinyfs_error)?;
        
        // Commit all pending records to Delta Lake
        self.commit_internal().await
            .map_err(error_utils::to_tinyfs_error)?;
        
        // Reset transaction state after successful commit
        transaction_utils::clear_transaction_state(
            &self.pending_records,
            &self.pending_directory_operations,
            &self.current_transaction_version,
        ).await;
        
        Ok(())
    }
    
    /// Rollback a transaction with transaction context (used by transaction guard)
    pub async fn rollback_transactional(&self, tx_id: i64) -> TinyFSResult<()> {
        // Verify transaction context (allow rollback even if transaction ID doesn't match)
        {
            let current_tx = self.current_transaction_version.lock().await;
            if current_tx.is_none() {
                debug!("Rollback called on transaction {tx_id} but no active transaction - ignoring");
                return Ok(());
            }
        }
        
        debug!("Rolling back transaction {tx_id}");
        
        // Clear transaction state
        transaction_utils::clear_transaction_state(
            &self.pending_records,
            &self.pending_directory_operations,
            &self.current_transaction_version,
        ).await;
        
        Ok(())
    }
    
    // =====================================================
    // End Transaction Guard Methods  
    // =====================================================
    
    /// Query records from both committed (Delta Lake) and pending (in-memory) data
    /// This ensures TinyFS operations can see pending data before commit
    async fn query_records(&self, part_id: &str, node_id: Option<&str>) -> Result<Vec<OplogEntry>, TLogFSError> {
        // Step 1: Get committed records from Delta Lake
        let committed_records = match self.delta_manager.get_table_for_read(&self.store_path).await {
            Ok(_table) => {
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
                
                match query_utils::execute_sql_query(&self.delta_manager, &self.store_path, sql, &params).await {
                    Ok(records) => records,
                    Err(_e) => Vec::new(),
                }
            }
            Err(_e) => {
                Vec::new()
            }
        };
        
        // Step 2: Get pending records from memory
        let pending_records = {
            let pending = self.pending_records.lock().await;
            pending.iter()
                .filter(|record| {
                    record.part_id == part_id && 
                    (node_id.is_none() || Some(record.node_id.as_str()) == node_id)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        
        // Step 3: Combine and sort by timestamp
        let mut all_records = committed_records;
        all_records.extend(pending_records);
        all_records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        
        Ok(all_records)
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
    pub async fn query_single_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<VersionedDirectoryEntry>, TLogFSError> {
        // Check pending directory operations first
        {
            let pending_dirs = self.pending_directory_operations.lock().await;
            if let Some(operations) = pending_dirs.get(&parent_node_id) {
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
        }
        
        // Fall back to querying committed records
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
    async fn flush_directory_operations(&self) -> Result<(), TLogFSError> {
        let pending_dirs = {
            let mut pending = self.pending_directory_operations.lock().await;
            std::mem::take(&mut *pending)
        };
        
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
            
            self.pending_records.lock().await.push(record);
        }
        
        Ok(())
    }
    
    /// Create a dynamic directory node with factory configuration
    /// This is the primary method for implementing the `mknod` command functionality
    pub async fn create_dynamic_directory(
        &self,
        parent_id: NodeID,
        name: String,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID, TLogFSError> {
        let node_id = NodeID::generate();
        let part_id = parent_id.to_hex_string();
        let now = Utc::now().timestamp_micros();
        
        // Get transaction sequence
        let transaction_sequence = transaction_utils::get_or_create_transaction_sequence(
            &self.current_transaction_version,
            &self.delta_manager,
            &self.store_path,
        ).await?;
        
        // Create dynamic directory OplogEntry
        let entry = OplogEntry::new_dynamic_directory(
            part_id,
            node_id.to_hex_string(),
            now,
            transaction_sequence,
            factory_type,
            config_content,
        );
        
        // Add to pending records
        self.pending_records.lock().await.push(entry);
        
        // Add directory operation for parent
        let directory_op = DirectoryOperation::InsertWithType(node_id, tinyfs::EntryType::Directory);
        self.update_directory_entry_with_type(parent_id, &name, directory_op, &tinyfs::EntryType::Directory).await
            .map_err(|e| TLogFSError::TinyFS(e))?;
        
        Ok(node_id)
    }
    
    /// Create a dynamic file node with factory configuration
    pub async fn create_dynamic_file(
        &self,
        parent_id: NodeID,
        name: String,
        file_type: tinyfs::EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID, TLogFSError> {
        let node_id = NodeID::generate();
        let part_id = parent_id.to_hex_string();
        let now = Utc::now().timestamp_micros();
        
        // Get transaction sequence
        let transaction_sequence = transaction_utils::get_or_create_transaction_sequence(
            &self.current_transaction_version,
            &self.delta_manager,
            &self.store_path,
        ).await?;
        
        // Create dynamic file OplogEntry
        let entry = OplogEntry::new_dynamic_file(
            part_id,
            node_id.to_hex_string(),
            file_type,
            now,
            transaction_sequence,
            factory_type,
            config_content,
        );
        
        // Add to pending records
        self.pending_records.lock().await.push(entry);
        
        // Add directory operation for parent
        let directory_op = DirectoryOperation::InsertWithType(node_id, file_type);
        self.update_directory_entry_with_type(parent_id, &name, directory_op, &file_type).await
            .map_err(|e| TLogFSError::TinyFS(e))?;
        
        Ok(node_id)
    }
    
    /// Get dynamic node configuration if the node is dynamic
    /// Uses the same query pattern as the rest of the persistence layer
    pub async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> Result<Option<(String, Vec<u8>)>, TLogFSError> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // First check pending records (for nodes created in current transaction)
        let pending_records = self.pending_records.lock().await;
        for record in pending_records.iter() {
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
        drop(pending_records);
        
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
        delta_manager: &DeltaTableManager,
        store_path: &str,
        sql_template: &str,
        params: &[&str],
    ) -> Result<Vec<OplogEntry>, TLogFSError> {
        let table = delta_manager.get_table_for_read(store_path).await
            .map_err(error_utils::arrow_error)?;
        
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

    /// Create a file node with the given content and entry type
    pub fn create_file_node(
        node_id: NodeID,
        part_id: NodeID,
        persistence: Arc<dyn tinyfs::persistence::PersistenceLayer>,
        _content: &[u8],
        _entry_type: tinyfs::EntryType,
    ) -> Result<NodeType, tinyfs::Error> {
        let oplog_file = crate::file::OpLogFile::new(node_id, part_id, persistence);
        let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
        Ok(NodeType::File(file_handle))
    }

    /// Create a directory node
    pub fn create_directory_node(
        node_id: NodeID,
        parent_node_id: NodeID,
        persistence: Arc<dyn tinyfs::persistence::PersistenceLayer>,
    ) -> Result<NodeType, tinyfs::Error> {
        let node_id_str = node_id.to_hex_string();
        let parent_node_id_str = parent_node_id.to_hex_string();
        let oplog_dir = super::super::directory::OpLogDirectory::new(node_id_str, parent_node_id_str, persistence);
        let dir_handle = super::super::directory::OpLogDirectory::create_handle(oplog_dir);
        Ok(NodeType::Directory(dir_handle))
    }

    /// Create a symlink node with the given target
    pub fn create_symlink_node(
        node_id: NodeID,
        part_id: NodeID,
        persistence: Arc<dyn tinyfs::persistence::PersistenceLayer>,
        _target: &std::path::Path,
    ) -> Result<NodeType, tinyfs::Error> {
        let oplog_symlink = super::super::symlink::OpLogSymlink::new(node_id, part_id, persistence);
        let symlink_handle = super::super::symlink::OpLogSymlink::create_handle(oplog_symlink);
        Ok(NodeType::Symlink(symlink_handle))
    }

    /// Create a node from an OplogEntry
    pub fn create_node_from_oplog_entry(
        oplog_entry: &OplogEntry,
        node_id: NodeID,
        part_id: NodeID,
        persistence: Arc<dyn tinyfs::persistence::PersistenceLayer>,
    ) -> Result<NodeType, tinyfs::Error> {
        // Check if this is a dynamic node (has factory type)
        if let Some(factory_type) = &oplog_entry.factory {
            return create_dynamic_node_from_oplog_entry(oplog_entry, node_id, part_id, persistence, factory_type);
        }
        
        // Handle static nodes (traditional TLogFS nodes)
        match oplog_entry.file_type {
            tinyfs::EntryType::FileData | tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                let oplog_file = crate::file::OpLogFile::new(node_id, part_id, persistence);
                let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
                Ok(NodeType::File(file_handle))
            }
            tinyfs::EntryType::Directory => {
                let oplog_dir = super::super::directory::OpLogDirectory::new(
                    oplog_entry.node_id.clone(),
                    part_id.to_hex_string(),
                    persistence,
                );
                let dir_handle = super::super::directory::OpLogDirectory::create_handle(oplog_dir);
                Ok(NodeType::Directory(dir_handle))
            }
            tinyfs::EntryType::Symlink => {
                let oplog_symlink = super::super::symlink::OpLogSymlink::new(node_id, part_id, persistence);
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
        persistence: Arc<dyn tinyfs::persistence::PersistenceLayer>,
        factory_type: &str,
    ) -> Result<NodeType, tinyfs::Error> {
        // Get configuration from the oplog entry
        let config_content = oplog_entry.content.as_ref()
            .ok_or_else(|| tinyfs::Error::Other(format!("Dynamic node missing configuration for factory '{}'", factory_type)))?;
        
        // All factories now require context - get OpLogPersistence
        let oplog_persistence = persistence.as_any().downcast_ref::<OpLogPersistence>()
            .ok_or_else(|| tinyfs::Error::Other("Dynamic nodes require OpLogPersistence context".to_string()))?;
            
        let context = FactoryContext {
            persistence: Arc::new(oplog_persistence.clone()),
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

/// Transaction state management utilities
mod transaction_utils {
    use super::*;

    /// Clear all transaction state
    pub async fn clear_transaction_state(
        pending_records: &Arc<tokio::sync::Mutex<Vec<OplogEntry>>>,
        pending_directory_operations: &Arc<tokio::sync::Mutex<HashMap<NodeID, HashMap<String, DirectoryOperation>>>>,
        current_transaction_version: &Arc<tokio::sync::Mutex<Option<i64>>>,
    ) {
        pending_records.lock().await.clear();
        pending_directory_operations.lock().await.clear();
        *current_transaction_version.lock().await = None;
    }

    /// Get or create transaction sequence number
    pub async fn get_or_create_transaction_sequence(
        current_transaction_version: &Arc<tokio::sync::Mutex<Option<i64>>>,
        delta_manager: &DeltaTableManager,
        store_path: &str,
    ) -> Result<i64, TLogFSError> {
        let mut current_transaction = current_transaction_version.lock().await;
        if let Some(transaction_sequence) = *current_transaction {
            Ok(transaction_sequence)
        } else {
            let table = delta_manager.get_table(store_path).await
                .map_err(|e| TLogFSError::ArrowMessage(e.to_string()))?;
            let current_version = table.version();
            let new_sequence = current_version + 1;
            *current_transaction = Some(new_sequence);
            Ok(new_sequence)
        }
    }
}

#[async_trait]
impl PersistenceLayer for OpLogPersistence {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<NodeType> {
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
                Arc::new(self.clone()),
            )
        } else {
            // Node doesn't exist in committed data, check pending transactions
            let pending_record = {
                let pending = self.pending_records.lock().await;
                pending.iter().find(|entry| {
                    entry.node_id == node_id_str && entry.part_id == part_id_str
                }).cloned()
            };
            
            if let Some(record) = pending_record {
                // Found in pending records, create node from it
                debug!("LOAD_NODE: Found node in pending records");
                
                node_factory::create_node_from_oplog_entry(
                    &record,
                    node_id,
                    part_id,
                    Arc::new(self.clone()),
                )
            } else {
                // Node doesn't exist in database or pending transactions
                Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("Node {} not found", node_id_str))))
            }
        }
    }
    
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> TinyFSResult<()> {
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
        
        let _version = self.next_transaction_sequence().await
            .map_err(error_utils::to_tinyfs_error)?;
        
        // Add to pending records - no double-nesting, store OplogEntry directly
        self.pending_records.lock().await.push(oplog_entry);
        Ok(())
    }
    
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<bool> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(error_utils::to_tinyfs_error)?;
        
        Ok(!records.is_empty())
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, NodeID>> {
        let all_entries = self.query_directory_entries(parent_node_id).await
            .map_err(error_utils::to_tinyfs_error)?;
        
        let mut current_state = HashMap::new();
        for entry in all_entries {
            match entry.operation_type {
                OperationType::Insert | OperationType::Update => {
                    if let Ok(child_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        current_state.insert(entry.name, child_id);
                    }
                }
                OperationType::Delete => {
                    current_state.remove(&entry.name);
                }
            }
        }
        
        Ok(current_state)
    }
    
    async fn load_file_content(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Vec<u8>> {
        // Use the large file handling logic instead of bypassing it
        self.load_file_content(node_id, part_id).await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn store_file_content(&self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> TinyFSResult<()> {
        // Use the large file handling logic instead of bypassing it
        self.store_file_content(node_id, part_id, content).await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn store_file_content_with_type(&self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: tinyfs::EntryType) -> TinyFSResult<()> {
        // Use the large file handling logic with entry type
        self.store_file_content_with_type(node_id, part_id, content, entry_type).await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn update_file_content_with_type(&self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: tinyfs::EntryType) -> TinyFSResult<()> {
        // For TLogFS, we implement "update" by replacing the entry in the current transaction
        // This prevents duplicate entries for the same file within one transaction
        self.update_file_content_with_type_impl(node_id, part_id, content, entry_type).await
            .map_err(error_utils::to_tinyfs_error)
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
    
    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<()> {
        let symlink_handle = tinyfs::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = tinyfs::NodeType::Symlink(symlink_handle);
        self.store_node(node_id, part_id, &node_type).await
    }
    
    async fn create_file_node(&self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: tinyfs::EntryType) -> TinyFSResult<NodeType> {
        // Store the content immediately with entry type information
        self.store_file_content_with_type(node_id, part_id, content, entry_type).await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Create and return the file node
        node_factory::create_file_node(node_id, part_id, Arc::new(self.clone()), content, entry_type)
    }
    
    async fn create_file_node_memory_only(&self, node_id: NodeID, part_id: NodeID, entry_type: tinyfs::EntryType) -> TinyFSResult<NodeType> {
        // Create file node in memory only - no immediate persistence
        // This allows streaming operations to write content before persisting
        // However, we need to store the entry type metadata so store_node() knows what type to use
        
        // Store empty content with the correct entry type immediately
        // This ensures that when store_node() is called, it can read the empty content and get the right type
        self.store_file_content_with_type(node_id, part_id, &[], entry_type).await
            .map_err(error_utils::to_tinyfs_error)?;
        
        node_factory::create_file_node(node_id, part_id, Arc::new(self.clone()), &[], entry_type)
    }
    
    async fn create_directory_node(&self, node_id: NodeID, parent_node_id: NodeID) -> TinyFSResult<NodeType> {
        node_factory::create_directory_node(node_id, parent_node_id, Arc::new(self.clone()))
    }
    
    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<NodeType> {
        // Store the target immediately
        self.store_symlink_target(node_id, part_id, target).await?;
        
        // Create and return the symlink node
        node_factory::create_symlink_node(node_id, part_id, Arc::new(self.clone()), target)
    }
    
    async fn begin_transaction(&self) -> TinyFSResult<()> {
        self.begin_transaction_internal().await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn commit(&self) -> TinyFSResult<()> {
        // First, flush any accumulated directory operations to pending records
        self.flush_directory_operations().await
            .map_err(error_utils::to_tinyfs_error)?;
        
        // Commit all pending records to Delta Lake
        self.commit_internal().await
            .map_err(error_utils::to_tinyfs_error)?;
        
        // Reset transaction state after successful commit
        transaction_utils::clear_transaction_state(
            &self.pending_records,
            &self.pending_directory_operations,
            &self.current_transaction_version,
        ).await;
        
        Ok(())
    }
    
    async fn rollback(&self) -> TinyFSResult<()> {
        transaction_utils::clear_transaction_state(
            &self.pending_records,
            &self.pending_directory_operations,
            &self.current_transaction_version,
        ).await;
        
        Ok(())
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
    
    async fn has_pending_operations(&self) -> TinyFSResult<bool> {
        let pending_records = self.pending_records.lock().await;
        let pending_dirs = self.pending_directory_operations.lock().await;
        Ok(!pending_records.is_empty() || !pending_dirs.is_empty())
    }
    
    async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<NodeID>> {
        match self.query_single_directory_entry(parent_node_id, entry_name).await {
            Ok(Some(entry)) => {
                if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
                    match entry.operation_type {
                        OperationType::Delete => Ok(None),
                        _ => Ok(Some(child_node_id)),
                    }
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(error_utils::to_tinyfs_error(e)),
        }
    }
    
    async fn query_directory_entry_with_type_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<(NodeID, tinyfs::EntryType)>> {
        match self.query_single_directory_entry(parent_node_id, entry_name).await {
            Ok(Some(entry)) => {
                if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
                    match entry.operation_type {
                        OperationType::Delete => Ok(None),
                        _ => {
                            let entry_type = entry.entry_type();
                            Ok(Some((child_node_id, entry_type)))
                        }
                    }
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(error_utils::to_tinyfs_error(e)),
        }
    }
    
    async fn load_directory_entries_with_types(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, (NodeID, tinyfs::EntryType)>> {
        let all_entries = self.query_directory_entries(parent_node_id).await
            .map_err(error_utils::to_tinyfs_error)?;
        
        let mut entries_with_types = HashMap::new();
        for entry in all_entries {
            match entry.operation_type {
                OperationType::Insert | OperationType::Update => {
                    if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        let entry_type = entry.entry_type();
                        entries_with_types.insert(entry.name, (child_node_id, entry_type));
                    }
                }
                OperationType::Delete => {
                    entries_with_types.remove(&entry.name);
                }
            }
        }
        
        Ok(entries_with_types)
    }
    
    async fn current_transaction_id(&self) -> TinyFSResult<Option<i64>> {
        let current_transaction = self.current_transaction_version.lock().await;
        Ok(*current_transaction)
    }
    
    async fn update_directory_entry_with_type(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
        _node_type: &tinyfs::EntryType, // node_type is now embedded in the operation
    ) -> TinyFSResult<()> {
        // Enhanced directory coalescing - accumulate operations with node types for batch processing
        let mut pending_dirs = self.pending_directory_operations.lock().await;
        let dir_ops = pending_dirs.entry(parent_node_id).or_insert_with(HashMap::new);
        
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
                record.content.as_ref().map(|c| c.len() as u64).unwrap_or(0)
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
                size,
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
            
            let large_file_path = crate::large_files::find_large_file_path(&self.store_path, sha256).await
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
    
    async fn is_versioned_file(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<bool> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(error_utils::to_tinyfs_error)?;
        
        // A file is considered versioned if it has more than one version
        Ok(records.len() > 1)
    }

    async fn store_file_series_with_metadata(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        min_event_time: i64,
        max_event_time: i64,
        timestamp_column: &str,
    ) -> TinyFSResult<()> {
        // Use the existing OpLogPersistence implementation
        OpLogPersistence::store_file_series_with_metadata(self, node_id, part_id, content, min_event_time, max_event_time, timestamp_column)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    // Dynamic node factory methods
    async fn create_dynamic_directory_node(&self, parent_node_id: NodeID, name: String, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
        OpLogPersistence::create_dynamic_directory(self, parent_node_id, name, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn create_dynamic_file_node(&self, parent_node_id: NodeID, name: String, file_type: tinyfs::EntryType, factory_type: &str, config_content: Vec<u8>) -> TinyFSResult<NodeID> {
        OpLogPersistence::create_dynamic_file(self, parent_node_id, name, file_type, factory_type, config_content)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }
    
    async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Option<(String, Vec<u8>)>> {
        OpLogPersistence::get_dynamic_node_config(self, node_id, part_id)
            .await
            .map_err(error_utils::to_tinyfs_error)
    }
}

/// Factory function to create an FS with OpLogPersistence
pub async fn create_oplog_fs(store_path: &str) -> Result<tinyfs::FS, TLogFSError> {
    let persistence = OpLogPersistence::new(store_path).await?;
    let fs = tinyfs::FS::with_persistence_layer(persistence.clone()).await
        .map_err(|e| TLogFSError::TinyFS(e))?;
    
    // Always begin a transaction first since ALL operations require transactions
    fs.begin_transaction().await.map_err(|e| TLogFSError::TinyFS(e))?;
    
    // Check if root directory already exists
    let root_node_id = tinyfs::NodeID::root();
    match persistence.load_node(root_node_id, root_node_id).await {
        Ok(_) => {
            // Root already exists, commit the transaction (load_node counts as an operation)
            debug!("Root directory already exists, skipping initialization");
            fs.commit().await.map_err(|e| TLogFSError::TinyFS(e))?;
        }
        Err(tinyfs::Error::NotFound(_)) => {
            // Root doesn't exist, initialize it within this transaction
            debug!("Root directory not found, initializing");
            persistence.initialize_root_directory().await?;
            fs.commit().await.map_err(|e| TLogFSError::TinyFS(e))?;
        }
        Err(e) => {
            // Some other error, rollback and propagate
            let _ = fs.rollback().await; // Best effort cleanup
            return Err(TLogFSError::TinyFS(e));
        }
    }
    
    Ok(fs)
}

/// Factory function to create an FS with OpLogPersistence using transaction guards
/// 
/// This is the new guard-based factory function that eliminates empty transactions
pub async fn create_oplog_fs_with_guards(store_path: &str) -> Result<tinyfs::FS, TLogFSError> {
    let persistence = OpLogPersistence::new(store_path).await?;
    
    // Use transaction guard for initialization
    {
        let tx = persistence.begin_transaction_with_guard().await?;
        
        // Check if root directory already exists
        let root_node_id = tinyfs::NodeID::root();
        match tx.load_node(root_node_id, root_node_id).await {
            Ok(_) => {
                // Root already exists, commit the transaction (load_node counts as an operation)
                debug!("Root directory already exists, skipping initialization");
                tx.commit().await.map_err(|e| TLogFSError::TinyFS(e))?;
            }
            Err(tinyfs::Error::NotFound(_)) => {
                // Root doesn't exist, initialize it within this transaction
                debug!("Root directory not found, initializing");
                tx.initialize_root_directory().await.map_err(|e| TLogFSError::TinyFS(e))?;
                tx.commit().await.map_err(|e| TLogFSError::TinyFS(e))?;
            }
            Err(e) => {
                // Some other error, rollback and propagate
                let _ = tx.rollback().await; // Best effort cleanup
                return Err(TLogFSError::TinyFS(e));
            }
        }
    } // Transaction guard automatically cleaned up here
    
    // Create FS layer on top of persistence
    let fs = tinyfs::FS::with_persistence_layer(persistence).await
        .map_err(|e| TLogFSError::TinyFS(e))?;
    
    Ok(fs)
}
