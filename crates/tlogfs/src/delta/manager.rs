use deltalake::{DeltaTable, DeltaOps, DeltaTableError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Configuration for the Delta table manager
#[derive(Clone, Debug)]
pub struct DeltaManagerConfig {
    /// Time-to-live for cached tables
    pub cache_ttl: Duration,
    /// Maximum number of tables to cache
    pub max_cache_size: usize,
}

impl Default for DeltaManagerConfig {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(5),
            max_cache_size: 100,
        }
    }
}

/// Cached Delta table with metadata
#[derive(Clone, Debug)]
struct CachedTable {
    table: DeltaTable,
    last_accessed: Instant,
}

/// Manages Delta table instances with caching and object_store compatibility
#[derive(Clone, Debug)]
pub struct DeltaTableManager {
    /// Cache of opened tables by URI
    table_cache: Arc<RwLock<HashMap<String, CachedTable>>>,
    /// Configuration settings
    config: DeltaManagerConfig,
}

impl DeltaTableManager {
    /// Create a new Delta table manager with default configuration
    pub fn new() -> Self {
        Self::with_config(DeltaManagerConfig::default())
    }

    /// Create a new Delta table manager with custom configuration
    pub fn with_config(config: DeltaManagerConfig) -> Self {
        Self {
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Get or open a Delta table with caching
    pub async fn get_table(&self, uri: &str) -> Result<DeltaTable, DeltaTableError> {
        // First, try to get from cache
        {
            let cache = self.table_cache.read().await;
            if let Some(cached) = cache.get(uri) {
                // Check if we should refresh based on TTL
                if !self.should_refresh_cache(cached) {
                    return Ok(cached.table.clone());
                }
            }
        }

        // Need to open/refresh the table
        let table = deltalake::open_table(uri).await?;

        // Update cache
        self.update_cache(uri, table.clone()).await;

        Ok(table)
    }

    /// Get Delta operations for a table, optimized for write operations
    /// This invalidates the cache to ensure we have the freshest state for writes
    pub async fn get_ops(&self, uri: &str) -> Result<DeltaOps, DeltaTableError> {
        // For write operations, we want the freshest possible state
        self.invalidate_table(uri).await;
        let table = self.get_table(uri).await?;
        Ok(DeltaOps::from(table))
    }

    /// Get a table for read operations with caching
    pub async fn get_table_for_read(&self, uri: &str) -> Result<DeltaTable, DeltaTableError> {
        self.get_table(uri).await
    }

    /// Invalidate cache for a specific table (call after writes)
    pub async fn invalidate_table(&self, uri: &str) {
        let mut cache = self.table_cache.write().await;
        cache.remove(uri);
    }

    /// Clear all cached tables
    pub async fn clear_cache(&self) {
        let mut cache = self.table_cache.write().await;
        cache.clear();
    }

    /// Get cache statistics for monitoring
    pub async fn cache_stats(&self) -> CacheStats {
        let cache = self.table_cache.read().await;
        let total_entries = cache.len();
        let expired_entries = cache
            .values()
            .filter(|cached| self.should_refresh_cache(cached))
            .count();

        CacheStats {
            total_entries,
            expired_entries,
            active_entries: total_entries - expired_entries,
        }
    }

    /// Check if a cached table should be refreshed
    fn should_refresh_cache(&self, cached: &CachedTable) -> bool {
        cached.last_accessed.elapsed() > self.config.cache_ttl
    }

    /// Update the cache with a new table instance
    async fn update_cache(&self, uri: &str, table: DeltaTable) {
        let mut cache = self.table_cache.write().await;

        // Implement LRU eviction if cache is full
        if cache.len() >= self.config.max_cache_size {
            self.evict_lru_entry(&mut cache);
        }

        cache.insert(
            uri.to_string(),
            CachedTable {
                table,
                last_accessed: Instant::now(),
            },
        );
    }

    /// Evict the least recently used entry from cache
    fn evict_lru_entry(&self, cache: &mut HashMap<String, CachedTable>) {
        if let Some((lru_key, _)) = cache
            .iter()
            .min_by_key(|(_, cached)| cached.last_accessed)
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            cache.remove(&lru_key);
        }
    }

    /// Create a new Delta table and add it to cache
    /// This ensures the manager is aware of tables it creates
    pub async fn create_table(
        &self,
        uri: &str,
        columns: Vec<deltalake::kernel::StructField>,
        partition_columns: Option<Vec<String>>,
    ) -> Result<DeltaTable, DeltaTableError> {
        // Create the table
        let ops = DeltaOps::try_from_uri(uri).await?;
        let mut create_op = ops.create().with_columns(columns);

        if let Some(partitions) = partition_columns {
            create_op = create_op.with_partition_columns(partitions);
        }

        let table = create_op.await?;

        // Add to cache
        self.update_cache(uri, table.clone()).await;

        Ok(table)
    }

    /// Write data to a table and invalidate cache
    pub async fn write_to_table(
        &self,
        uri: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
        save_mode: deltalake::protocol::SaveMode,
    ) -> Result<DeltaTable, DeltaTableError> {
        // Get the table (this will use cached version or open fresh)
        let table = self.get_table(uri).await?;

        // Write the data
        let ops = DeltaOps::from(table);
        let table = ops.write(batches).with_save_mode(save_mode).await?;

        // Update cache with new table state
        self.update_cache(uri, table.clone()).await;

        Ok(table)
    }
}

impl Default for DeltaTableManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_entries: usize,
    pub expired_entries: usize,
    pub active_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_delta_manager_creation() {
        let manager = DeltaTableManager::new();
        let stats = manager.cache_stats().await;
        assert_eq!(stats.total_entries, 0);
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = DeltaManagerConfig {
            cache_ttl: Duration::from_secs(10),
            max_cache_size: 50,
        };
        let manager = DeltaTableManager::with_config(config.clone());
        assert_eq!(manager.config.cache_ttl, Duration::from_secs(10));
        assert_eq!(manager.config.max_cache_size, 50);
    }

    #[tokio::test]
    async fn test_get_table_nonexistent() {
        let manager = DeltaTableManager::new();
        // Test with a path that definitely doesn't exist
        let result = manager.get_table("/tmp/nonexistent_table_12345").await;
        assert!(result.is_err(), "Non-existent table should return an error");
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let manager = DeltaTableManager::new();
        manager.invalidate_table("test://table").await;
        let stats = manager.cache_stats().await;
        assert_eq!(stats.total_entries, 0);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let manager = DeltaTableManager::new();
        manager.clear_cache().await;
        let stats = manager.cache_stats().await;
        assert_eq!(stats.total_entries, 0);
    }
}
