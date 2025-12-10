//! Layer 1: Compression Utilities
//!
//! Standalone decompression utilities usable by any code (including TinyFS)
//! for decompressing files of any type (binary, text, config, etc.)
//!
//! Format-agnostic - works with any AsyncRead stream.

use crate::{Error, Result};
use std::pin::Pin;
use tokio::io::AsyncRead;

/// Wrap an AsyncRead with decompression based on compression type.
///
/// This is a standalone utility usable by any code for decompressing
/// files of any type (binary, text, config, parquet, etc.)
///
/// Supported compressions: "zstd", "gzip", "bzip2"
/// Returns the same reader if compression is "none" or None
///
/// # Examples
///
/// ```ignore
/// use provider::decompress;
///
/// // Decompress any file type
/// let compressed = fs.open("/config/app.json.zst").await?;
/// let decompressed = decompress(compressed, Some("zstd"))?;
///
/// // Read the decompressed content
/// let mut content = String::new();
/// decompressed.read_to_string(&mut content).await?;
/// ```
pub fn decompress(
    reader: Pin<Box<dyn AsyncRead + Send>>,
    compression: Option<&str>,
) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
    match compression {
        None | Some("none") => Ok(reader),
        Some("zstd") => {
            use async_compression::tokio::bufread::ZstdDecoder;
            let buf_reader = tokio::io::BufReader::new(reader);
            Ok(Box::pin(ZstdDecoder::new(buf_reader)))
        }
        Some("gzip") => {
            use async_compression::tokio::bufread::GzipDecoder;
            let buf_reader = tokio::io::BufReader::new(reader);
            Ok(Box::pin(GzipDecoder::new(buf_reader)))
        }
        Some("bzip2") => {
            use async_compression::tokio::bufread::BzDecoder;
            let buf_reader = tokio::io::BufReader::new(reader);
            Ok(Box::pin(BzDecoder::new(buf_reader)))
        }
        Some(other) => Err(Error::DecompressionError(format!(
            "Unsupported compression: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_decompress_none() {
        let data = b"hello world";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&data[..]);

        let mut decompressed = decompress(reader, None).unwrap();

        let mut result = String::new();
        let _ = decompressed.read_to_string(&mut result).await.unwrap();
        assert_eq!(result, "hello world");
    }

    #[tokio::test]
    async fn test_decompress_invalid() {
        let data = b"hello world";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&data[..]);

        let result = decompress(reader, Some("invalid"));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Unsupported compression"));
        }
    }
}
