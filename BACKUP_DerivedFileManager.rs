// BACKUP: DerivedFileManager implementation (removed for simplicity)
// This code was removed to reduce complexity but preserved for future reference
// Date: June 22, 2025
// 
// The DerivedFileManager was designed to cache computed/derived file content,
// but was adding unnecessary complexity to the filesystem tests and implementation.
// The original design was for on-demand computation, which is simpler and sufficient.

use crate::error::{Error, Result};
use crate::fs::FS;
use crate::node::{NodeID, NodeRef, NodeType};
use crate::dir::Directory; // Add Directory trait import for insert method
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tokio::sync::Mutex;

/// Manager for derived/computed file content using memory backend
/// This implements Phase 3 of the TinyFS refactoring plan
pub struct DerivedFileManager {
    source_fs: Arc<FS>,
    memory_fs: Arc<FS>,
    computation_cache: Arc<Mutex<HashMap<String, NodeRef>>>,
}

impl DerivedFileManager {
    /// Create a new derived file manager
    pub async fn new(source_fs: Arc<FS>) -> Result<Self> {
        let memory_persistence = crate::memory_persistence::MemoryPersistence::new();
        let memory_fs = Arc::new(FS::with_persistence_layer(memory_persistence).await?);
        
        Ok(Self { 
            source_fs, 
            memory_fs,
            computation_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    
    /// Get downsampled timeseries (example of derived computation)
    pub async fn get_downsampled_timeseries(
        &self,
        source_node_id: NodeID,
        resolution: Duration,
    ) -> Result<NodeRef> {
        let computation_key = format!("downsample_{}_{}", source_node_id.to_hex_string(), resolution.as_secs());
        
        // Check if already computed
        {
            let cache = self.computation_cache.lock().await;
            if let Some(cached_node) = cache.get(&computation_key) {
                return Ok(cached_node.clone());
            }
        }
        
        // Compute derived result
        let source_node = self.source_fs.get_node(source_node_id, crate::node::ROOT_ID).await?;
        let downsampled_data = self.expensive_downsample_operation(&source_node).await?;
        
        // Store in memory filesystem
        let derived_node = self.memory_fs.create_node(
            crate::node::ROOT_ID, // Root as parent for derived files
            NodeType::File(downsampled_data)
        ).await?;
        
        // Cache the result
        {
            let mut cache = self.computation_cache.lock().await;
            cache.insert(computation_key, derived_node.clone());
        }
        
        Ok(derived_node)
    }
    
    /// Create a visit directory with computed results
    pub async fn create_visit_directory(
        &self,
        pattern: &str,
    ) -> Result<NodeRef> {
        let computation_key = format!("visit_directory_{}", pattern);
        
        // Check if already computed
        {
            let cache = self.computation_cache.lock().await;
            if let Some(cached_node) = cache.get(&computation_key) {
                return Ok(cached_node.clone());
            }
        }
        
        // Compute visit directory contents
        let visit_dir_handle = self.compute_visit_directory(pattern).await?;
        let derived_node = self.memory_fs.create_node(
            crate::node::ROOT_ID,
            NodeType::Directory(visit_dir_handle)
        ).await?;
        
        // Cache the result
        {
            let mut cache = self.computation_cache.lock().await;
            cache.insert(computation_key, derived_node.clone());
        }
        
        Ok(derived_node)
    }
    
    /// Get the memory filesystem for direct access
    pub fn memory_fs(&self) -> Arc<FS> {
        self.memory_fs.clone()
    }
    
    /// Get the source filesystem for direct access  
    pub fn source_fs(&self) -> Arc<FS> {
        self.source_fs.clone()
    }
    
    /// Clear computation cache
    pub async fn clear_cache(&self) {
        let mut cache = self.computation_cache.lock().await;
        cache.clear();
    }
    
    // Private methods
    
    async fn expensive_downsample_operation(&self, _source_node: &NodeRef) -> Result<crate::file::Handle> {
        // Placeholder for actual downsampling computation
        // This would implement the real downsampling logic
        let sample_data = b"downsampled data".to_vec();
        Ok(crate::memory::MemoryFile::new_handle(&sample_data))
    }
    
    async fn compute_visit_directory(&self, pattern: &str) -> Result<crate::dir::Handle> {
        // Create a memory directory to hold the computed results
        let mut memory_dir = crate::memory::MemoryDirectory::new();
        
        // Use visitor pattern to collect matching files from source filesystem
        let root = self.source_fs.root().await?;
        let mut visitor = VisitDirectoryCollector::new();
        
        // Collect all matching files
        if let Err(e) = root.visit_with_visitor(pattern, &mut visitor).await {
            // If visit fails, return empty directory rather than error
            // This handles cases where pattern doesn't match anything
            eprintln!("Visit pattern '{}' failed: {:?}, returning empty directory", pattern, e);
        }
        
        // Add collected items to memory directory
        for (name, node_ref) in visitor.items {
            if let Err(e) = memory_dir.insert(name.clone(), node_ref).await {
                eprintln!("Failed to insert '{}' into visit directory: {:?}", name, e);
            }
        }
        
        Ok(memory_dir.to_handle())
    }
}

/// Visitor for collecting files into a derived directory
struct VisitDirectoryCollector {
    items: Vec<(String, NodeRef)>,
}

impl VisitDirectoryCollector {
    fn new() -> Self {
        Self { items: Vec::new() }
    }
}

#[async_trait::async_trait]
impl crate::wd::Visitor<(String, NodeRef)> for VisitDirectoryCollector {
    async fn visit(&mut self, node: crate::node::NodePath, captured: &[String]) -> Result<(String, NodeRef)> {
        let filename = if captured.is_empty() {
            node.basename()
        } else {
            captured.join("_")
        };
        
        let node_ref = node.node.clone(); // NodePath has a public node field
        let result = (filename.clone(), node_ref.clone());
        self.items.push(result.clone());
        Ok(result)
    }
}
