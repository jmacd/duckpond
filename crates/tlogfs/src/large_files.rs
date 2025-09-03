use diagnostics::*;
use sha2::{Sha256, Digest};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::AsyncWrite;

/// Threshold for storing files separately: 64 KiB (configurable)
pub const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

/// Threshold for creating subdirectories: when more than this many files exist
pub const DIRECTORY_SPLIT_THRESHOLD: usize = 100;

/// Number of bits (4 hex digits) for directory prefix
pub const PREFIX_BITS: usize = 16;

/// Get large file path with hierarchical directory structure
/// Returns the path where the file should be stored, handling directory migration automatically
pub async fn large_file_path(pond_path: &str, sha256: &str) -> std::io::Result<PathBuf> {
    let large_files_dir = PathBuf::from(pond_path).join("_large_files");
    
    // Check if we need hierarchical structure
    if should_use_hierarchical_structure(&large_files_dir).await? {
        // Ensure migration is complete
        migrate_to_hierarchical_structure(&large_files_dir).await?;
        
        // Use hierarchical path
        let prefix = &sha256[0..4]; // First 4 hex digits (16 bits)
        Ok(large_files_dir
            .join(format!("sha256_{}={}", PREFIX_BITS, prefix))
            .join(format!("sha256={}", sha256)))
    } else {
        // Use flat structure
        Ok(large_files_dir.join(format!("sha256={}", sha256)))
    }
}

/// Check if we should use hierarchical directory structure
async fn should_use_hierarchical_structure(large_files_dir: &PathBuf) -> std::io::Result<bool> {
    // If hierarchical directories already exist, use hierarchical structure
    if has_hierarchical_directories(large_files_dir).await? {
        return Ok(true);
    }
    
    // Count flat files to see if we need to migrate
    let flat_file_count = count_flat_files(large_files_dir).await?;
    Ok(flat_file_count > DIRECTORY_SPLIT_THRESHOLD)
}

/// Check if hierarchical directories already exist
async fn has_hierarchical_directories(large_files_dir: &PathBuf) -> std::io::Result<bool> {
    if !large_files_dir.exists() {
        return Ok(false);
    }
    
    let mut entries = tokio::fs::read_dir(large_files_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with(&format!("sha256_{}=", PREFIX_BITS)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Count files in flat structure (sha256=* files directly in _large_files/)
async fn count_flat_files(large_files_dir: &PathBuf) -> std::io::Result<usize> {
    if !large_files_dir.exists() {
        return Ok(0);
    }
    
    let mut count = 0;
    let mut entries = tokio::fs::read_dir(large_files_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with("sha256=") {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Migrate flat structure to hierarchical structure (idempotent)
async fn migrate_to_hierarchical_structure(large_files_dir: &PathBuf) -> std::io::Result<()> {
    if !large_files_dir.exists() {
        return Ok(());
    }
    
    // Collect all flat files that need migration
    let mut flat_files = Vec::new();
    let mut entries = tokio::fs::read_dir(large_files_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with("sha256=") {
                // Extract SHA256 from filename: "sha256=<sha256>" 
                if let Some(sha256) = name.strip_prefix("sha256=") {
                    flat_files.push((entry.path(), sha256.to_string()));
                }
            }
        }
    }
    
    // Migrate each file to hierarchical structure
    for (old_path, sha256) in flat_files {
        let prefix = &sha256[0..4]; // First 4 hex digits
        let subdir = large_files_dir.join(format!("sha256_{}={}", PREFIX_BITS, prefix));
        let new_path = subdir.join(format!("sha256={}", sha256));
        
        // Create subdirectory if it doesn't exist
        tokio::fs::create_dir_all(&subdir).await?;
        
        // Move file to new location (idempotent - only if source exists and target doesn't)
        if old_path.exists() && !new_path.exists() {
            tokio::fs::rename(&old_path, &new_path).await?;
        }
    }
    
    Ok(())
}

/// Find large file path (for reading) - searches both flat and hierarchical locations
pub async fn find_large_file_path(pond_path: &str, sha256: &str) -> std::io::Result<Option<PathBuf>> {
    let large_files_dir = PathBuf::from(pond_path).join("_large_files");
    
    // Try hierarchical path first
    let prefix = &sha256[0..4];
    let hierarchical_path = large_files_dir
        .join(format!("sha256_{}={}", PREFIX_BITS, prefix))
        .join(format!("sha256={}", sha256));
    
    if hierarchical_path.exists() {
        return Ok(Some(hierarchical_path));
    }
    
    // Try flat path
    let flat_path = large_files_dir.join(format!("sha256={}", sha256));
    if flat_path.exists() {
        return Ok(Some(flat_path));
    }
    
    Ok(None)
}

/// Check if content should be stored as large file
pub fn should_store_as_large_file(content: &[u8]) -> bool {
    let content_len = content.len();
    let is_large = content_len >= LARGE_FILE_THRESHOLD;
    debug!("should_store_as_large_file: content_len={content_len}, threshold={LARGE_FILE_THRESHOLD}, is_large={is_large}");
    is_large
}

/// Result of hybrid writer finalization
#[derive(Clone)]
pub struct HybridWriterResult {
    pub content: Vec<u8>,
    pub sha256: String,
    pub size: usize,
}

/// Hybrid writer that implements AsyncWrite with incremental hashing and spillover
#[derive(Default)]
pub struct HybridWriter {
    /// Memory buffer for small files
    memory_buffer: Option<Vec<u8>>,
    /// Temporary file for large files
    temp_file: Option<File>,
    /// Path to temporary file
    temp_path: Option<PathBuf>,
    /// Incremental SHA256 hasher
    hasher: Sha256,
    /// Total bytes written
    total_written: usize,
    /// Target pond directory for final file
    pond_path: String,
}

impl HybridWriter {
    pub fn new(pond_path: String) -> Self {
        Self {
            memory_buffer: Some(Vec::new()),
            temp_file: None,
            temp_path: None,
            hasher: Sha256::new(),
            total_written: 0,
            pond_path,
        }
    }
    
    /// Finalize the writer and return content strategy decision
    pub async fn finalize(self) -> std::io::Result<HybridWriterResult> {
        // Finalize hash computation
        let sha256 = format!("{:x}", self.hasher.finalize());
        
        let total_written = self.total_written;
        debug!("HybridWriter finalize: {total_written} bytes, threshold={LARGE_FILE_THRESHOLD}, sha256={sha256}");
        
        let content = if self.total_written >= LARGE_FILE_THRESHOLD {
            debug!("Large file path - total_written >= threshold");
            // Large file: ensure it's written to external storage
            if let Some(buffer) = self.memory_buffer {
                debug!("Writing large file from memory buffer to external storage");
                // Still in memory but qualifies as large file - write to external storage
                let large_files_dir = PathBuf::from(&self.pond_path).join("_large_files");
                tokio::fs::create_dir_all(&large_files_dir).await?;
                
                let final_path = large_file_path(&self.pond_path, &sha256).await?;
                let final_path_str = format!("{final_path:?}");
                debug!("Large file final path: {final_path_str}");
                
                // Write and sync to ensure durability before Delta commit
                {
                    use tokio::fs::OpenOptions;
                    use tokio::io::AsyncWriteExt;
                    
                    let mut file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&final_path)
                        .await?;
                    
                    file.write_all(&buffer).await?;
                    file.sync_all().await?; // Ensure data and metadata are synced to disk
                }
                
                info!("Successfully wrote large file to {final_path_str}");
                // Return empty Vec to indicate large file (content stored externally)
                Vec::new()
            } else if let Some(temp_path) = self.temp_path {
                // Already in temp file - move to final location
                let large_files_dir = PathBuf::from(&self.pond_path).join("_large_files");
                tokio::fs::create_dir_all(&large_files_dir).await?;
                
                let final_path = large_file_path(&self.pond_path, &sha256).await?;
                tokio::fs::rename(&temp_path, &final_path).await?;
                
                // Sync the file after move to ensure it's durable
                {
                    use tokio::fs::OpenOptions;
                    let file = OpenOptions::new()
                        .write(true)
                        .open(&final_path)
                        .await?;
                    file.sync_all().await?; // Ensure moved file is synced to disk
                }
                
                // Return empty Vec to indicate large file (content stored externally)
                Vec::new()
            } else {
                Vec::new()
            }
        } else if let Some(buffer) = self.memory_buffer {
            // Small file: return memory buffer
            buffer
        } else {
            // This shouldn't happen for small files, but handle gracefully
            Vec::new()
        };
        
        Ok(HybridWriterResult {
            content,
            sha256,
            size: self.total_written,
        })
    }
}

impl AsyncWrite for HybridWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = &mut *self;
        
        // Update incremental hash
        this.hasher.update(buf);
        this.total_written += buf.len();
        
        // For simplicity, we'll only spill during finalize() phase
        // This avoids the complex async-in-sync polling issue
        if let Some(ref mut buffer) = this.memory_buffer {
            // Still in memory mode - just accumulate
            buffer.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        } else if let Some(ref mut temp_file) = this.temp_file {
            // In temp file mode
            Pin::new(temp_file).poll_write(cx, buf)
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer in invalid state"
            )))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
