use sha2::{Sha256, Digest};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::AsyncWrite;

/// Threshold for storing files separately: 64 KiB (configurable)
pub const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

/// Get large file path in pond directory
pub fn large_file_path(pond_path: &str, sha256: &str) -> PathBuf {
    PathBuf::from(pond_path)
        .join("_large_files")
        .join(format!("{}.data", sha256))
}

/// Check if content should be stored as large file
pub fn should_store_as_large_file(content: &[u8]) -> bool {
    content.len() > LARGE_FILE_THRESHOLD
}

/// Result of hybrid writer finalization
#[derive(Clone)]
pub struct HybridWriterResult {
    pub content: Vec<u8>,
    pub sha256: String,
    pub size: usize,
}

/// Hybrid writer that implements AsyncWrite with incremental hashing and spillover
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
        
        let content = if self.total_written > LARGE_FILE_THRESHOLD {
            // Large file: ensure it's written to external storage
            if let Some(buffer) = self.memory_buffer {
                // Still in memory but qualifies as large file - write to external storage
                let large_files_dir = PathBuf::from(&self.pond_path).join("_large_files");
                tokio::fs::create_dir_all(&large_files_dir).await?;
                
                let final_path = large_file_path(&self.pond_path, &sha256);
                
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
                
                // Return empty Vec to indicate large file (content stored externally)
                Vec::new()
            } else if let Some(temp_path) = self.temp_path {
                // Already in temp file - move to final location
                let large_files_dir = PathBuf::from(&self.pond_path).join("_large_files");
                tokio::fs::create_dir_all(&large_files_dir).await?;
                
                let final_path = large_file_path(&self.pond_path, &sha256);
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
