use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Helper functions for cases that truly need buffer operations
/// Most code should use async_reader()/async_writer() directly
pub mod buffer_helpers {
    use super::*;

    /// Read all content from an async reader into a Vec<u8>
    /// WARNING: This loads the entire file into memory. Only use for small files
    /// or in tests. Most code should stream via async_reader() instead.
    pub async fn read_all_to_vec(
        mut reader: Pin<Box<dyn AsyncRead + Send>>,
    ) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = Vec::new();
        _ = reader.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

}

/// Convenience functions for tests and small files
/// WARNING: These functions load entire content into memory.
/// Production code should use streaming interfaces directly.
pub mod convenience {
    use crate::{EntryType, NodePath, error::Result, wd::WD};
    use std::path::Path;

    /// Creates a file at the specified path (convenience for tests/small files)  
    /// WARNING: Loads entire content into memory. For large files use create_file_path_streaming() instead.
    pub async fn create_file_path<P: AsRef<Path>>(
        wd: &WD,
        path: P,
        content: &[u8],
    ) -> Result<NodePath> {
        let (node_path, mut writer) = wd.create_file_path_streaming(path).await?;

        // Write content via streaming
        use tokio::io::AsyncWriteExt;
        writer.write_all(content).await.map_err(|e| {
            crate::error::Error::Other(format!("Failed to write file content: {}", e))
        })?;
        writer.flush().await.map_err(|e| {
            crate::error::Error::Other(format!("Failed to flush file content: {}", e))
        })?;
        writer.shutdown().await.map_err(|e| {
            crate::error::Error::Other(format!("Failed to complete file write: {}", e))
        })?;

        // Add a small delay to ensure the async writer background task completes
        tokio::task::yield_now().await;

        Ok(node_path)
    }

    /// Creates a file at the specified path with content and specific entry type. This is a convenience function for test usage.
    pub async fn create_file_path_with_type<P: AsRef<Path>>(
        working_dir: &WD,
        path: P,
        content: &[u8],
        entry_type: EntryType,
    ) -> Result<NodePath> {
        let (node_path, mut writer) = working_dir
            .create_file_path_streaming_with_type(path, entry_type)
            .await?;

        // Write content via streaming
        use tokio::io::AsyncWriteExt;
        writer.write_all(content).await.map_err(|e| {
            crate::error::Error::Other(format!("Failed to write file content: {}", e))
        })?;
        writer.flush().await.map_err(|e| {
            crate::error::Error::Other(format!("Failed to flush file content: {}", e))
        })?;
        writer.shutdown().await.map_err(|e| {
            crate::error::Error::Other(format!("Failed to complete file write: {}", e))
        })?;

        // Add a small delay to ensure the async writer background task completes
        tokio::task::yield_now().await;

        Ok(node_path)
    }
}
