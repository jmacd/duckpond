use log::{debug, info};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
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
pub async fn large_file_path<P: AsRef<Path>>(
    pond_path: P,
    sha256: &str,
) -> std::io::Result<PathBuf> {
    let large_files_dir = pond_path.as_ref().to_path_buf().join("_large_files");

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
pub async fn find_large_file_path<P: AsRef<Path>>(
    pond_path: P,
    sha256: &str,
) -> std::io::Result<Option<PathBuf>> {
    let large_files_dir = pond_path.as_ref().join("_large_files");

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
#[must_use]
pub fn should_store_as_large_file(content: &[u8]) -> bool {
    let content_len = content.len();
    let is_large = content_len >= LARGE_FILE_THRESHOLD;
    debug!(
        "should_store_as_large_file: content_len={content_len}, threshold={LARGE_FILE_THRESHOLD}, is_large={is_large}"
    );
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
pub struct HybridWriter {
    /// Temporary file for streaming writes (created lazily)
    temp_file: Option<File>,
    /// Path to temporary file
    temp_path: Option<PathBuf>,
    /// Incremental SHA256 hasher
    hasher: Sha256,
    /// Total bytes written
    total_written: usize,
    /// Target pond directory for final file
    pond_path: PathBuf,
    /// Future for creating temp file
    create_future: Option<Pin<Box<dyn Future<Output = std::io::Result<(File, PathBuf)>> + Send>>>,
}

impl HybridWriter {
    pub fn new<P: AsRef<Path>>(pond_path: P) -> Self {
        Self {
            temp_file: None,
            temp_path: None,
            hasher: Sha256::new(),
            total_written: 0,
            pond_path: pond_path.as_ref().into(),
            create_future: None,
        }
    }

    pub fn total_written(&self) -> usize {
        self.total_written
    }

    /// Finalize the writer and return content strategy decision
    pub async fn finalize(self) -> std::io::Result<HybridWriterResult> {
        // Flush and sync temp file if it exists
        if let Some(mut temp_file) = self.temp_file {
            use tokio::io::AsyncWriteExt;
            temp_file.flush().await?;
            temp_file.sync_all().await?;
        }

        // Finalize hash computation
        let sha256 = format!("{:x}", self.hasher.finalize());

        let total_written = self.total_written;
        debug!(
            "HybridWriter finalize: {total_written} bytes, threshold={LARGE_FILE_THRESHOLD}, sha256={sha256}"
        );

        let content = if self.total_written >= LARGE_FILE_THRESHOLD {
            debug!("Large file: moving temp file to external storage");
            // Large file: move temp file to final location
            let temp_path = self.temp_path.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No temp file created for large file",
                )
            })?;

            let large_files_dir = self.pond_path.join("_large_files");
            tokio::fs::create_dir_all(&large_files_dir).await?;

            let final_path = large_file_path(&self.pond_path, &sha256).await?;
            tokio::fs::rename(&temp_path, &final_path).await?;

            // Sync the file after move
            let file = File::open(&final_path).await?;
            file.sync_all().await?;

            info!("Successfully wrote large file to {:?}", final_path);
            Vec::new() // Empty vec indicates external storage
        } else if self.temp_path.is_some() {
            debug!("Small file: reading temp file into memory");
            // Small file: read temp file into memory and delete it
            let temp_path = self.temp_path.unwrap();
            let content = tokio::fs::read(&temp_path).await?;
            tokio::fs::remove_file(&temp_path).await?;
            content
        } else {
            // No data written
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

        // Lazy initialization: create temp file on first write
        if this.temp_file.is_none() && this.create_future.is_none() {
            let pond_path = this.pond_path.clone();
            this.create_future = Some(Box::pin(async move {
                let temp_dir = pond_path.join("_large_files");
                tokio::fs::create_dir_all(&temp_dir).await?;

                // Create unique temp file using process ID and timestamp
                let temp_path = temp_dir.join(format!(
                    "tmp_{}_{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                let temp_file = File::create(&temp_path).await?;

                Ok((temp_file, temp_path))
            }));
        }

        // Poll the creation future if it exists
        if let Some(future) = this.create_future.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Ready(Ok((file, path))) => {
                    this.temp_file = Some(file);
                    this.temp_path = Some(path);
                    this.create_future = None;
                    debug!("Created streaming temp file: {:?}", this.temp_path);
                }
                Poll::Ready(Err(e)) => {
                    this.create_future = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Now write to the temp file
        if let Some(ref mut temp_file) = this.temp_file {
            this.hasher.update(buf);

            let result = Pin::new(temp_file).poll_write(cx, buf);

            if let Poll::Ready(Ok(n)) = result {
                this.total_written += n;
            }

            result
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer in invalid state",
            )))
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
