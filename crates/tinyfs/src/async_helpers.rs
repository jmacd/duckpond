use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Helper functions for cases that truly need buffer operations
/// Most code should use async_reader()/async_writer() directly
pub mod buffer_helpers {
    use super::*;
    use crate::error;

    /// Read all content from an async reader into a Vec<u8>
    /// WARNING: This loads the entire file into memory. Only use for small files
    /// or in tests. Most code should stream via async_reader() instead.
    pub async fn read_all_to_vec(
        mut reader: Pin<Box<dyn AsyncRead + Send>>,
    ) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    /// Write all content from a slice to an async writer
    /// WARNING: This assumes the entire content fits in memory.
    /// Most code should stream via async_writer() instead.
    pub async fn write_all_from_slice(
        mut writer: Pin<Box<dyn AsyncWrite + Send>>,
        content: &[u8],
    ) -> Result<(), std::io::Error> {
        writer.write_all(content).await?;
        writer.shutdown().await?;
        Ok(())
    }

    /// Read entire file content via Handle (convenience for tests/special cases)
    /// WARNING: Loads entire file into memory
    pub async fn read_file_to_vec(handle: &crate::file::Handle) -> error::Result<Vec<u8>> {
        let reader = handle.async_reader().await?;
        // Convert AsyncReadSeek to AsyncRead + Send
        let async_read: Pin<Box<dyn AsyncRead + Send>> = Box::pin(reader);
        read_all_to_vec(async_read)
            .await
            .map_err(|e| error::Error::Other(format!("Failed to read file content: {}", e)))
    }

    /// Write entire buffer to file via Handle (convenience for tests/special cases)
    /// WARNING: Assumes entire content fits in memory
    pub async fn write_file_from_slice(
        handle: &crate::file::Handle,
        content: &[u8],
    ) -> error::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut writer = handle.async_writer().await?;
        writer
            .write_all(content)
            .await
            .map_err(|e| error::Error::Other(format!("Failed to write file content: {}", e)))?;
        writer
            .shutdown()
            .await
            .map_err(|e| error::Error::Other(format!("Failed to shutdown writer: {}", e)))?;
        Ok(())
    }
}

/// Helper functions for common async read/write patterns (kept for internal use)
pub mod helpers {
    use super::*;

    /// Read all content from an AsyncRead into a Vec<u8>
    pub async fn read_all_to_vec(
        reader: Pin<Box<dyn AsyncRead + Send>>,
    ) -> Result<Vec<u8>, std::io::Error> {
        buffer_helpers::read_all_to_vec(reader).await
    }

    /// Write all content from a slice to an AsyncWrite
    pub async fn write_all_from_slice(
        writer: Pin<Box<dyn AsyncWrite + Send>>,
        content: &[u8],
    ) -> Result<(), std::io::Error> {
        buffer_helpers::write_all_from_slice(writer, content).await
    }
}

/// Convenience functions for tests and small files
/// WARNING: These functions load entire content into memory.
/// Production code should use streaming interfaces directly.
pub mod convenience {
    use super::*;
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

/// A simple buffering async writer that executes a closure on completion
pub struct SimpleBufferedWriter<F>
where
    F: FnOnce(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>> + Send,
{
    buffer: Vec<u8>,
    completion_fn: Option<F>,
    completion_rx: Option<tokio::sync::oneshot::Receiver<Result<(), std::io::Error>>>,
}

impl<F> SimpleBufferedWriter<F>
where
    F: FnOnce(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>> + Send,
{
    pub fn new(completion_fn: F) -> Self {
        Self {
            buffer: Vec::new(),
            completion_fn: Some(completion_fn),
            completion_rx: None,
        }
    }
}

impl<F> AsyncWrite for SimpleBufferedWriter<F>
where
    F: FnOnce(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>
        + Send
        + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // @@@ WHAT UNSAFE
        let this = unsafe { self.get_unchecked_mut() };
        this.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        // @@@ WHAT UNSAFE
        let this = unsafe { self.get_unchecked_mut() };

        // Start the completion task if not already started
        if this.completion_rx.is_none() {
            if let Some(completion_fn) = this.completion_fn.take() {
                let buffer = std::mem::take(&mut this.buffer);
                let (tx, rx) = tokio::sync::oneshot::channel();
                this.completion_rx = Some(rx);

                // Spawn the completion task
                tokio::spawn(async move {
                    let result = completion_fn(buffer).await;
                    let _ = tx.send(result);
                });
            } else {
                // Empty buffer or already completed
                return Poll::Ready(Ok(()));
            }
        }

        // Poll for completion
        if let Some(ref mut rx) = this.completion_rx {
            match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    this.completion_rx = None;
                    Poll::Ready(result)
                }
                Poll::Ready(Err(e)) => {
                    this.completion_rx = None;
                    Poll::Ready(Err(std::io::Error::other(e)))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
