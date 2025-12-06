//! Provider: URL-Based file access and factory infrastructure

mod context;
mod error;
mod null_padding;
mod scope_prefix;

pub use context::{
    FactoryContext, PondMetadata, ProviderContext,
    SessionProvider, TableProviderCache, TemplateVariableProvider,
};
pub use error::{Error, Result};
pub use null_padding::null_padding_table;
pub use scope_prefix::ScopePrefixTableProvider;

use std::pin::Pin;
use tokio::io::AsyncRead;

/// URL for accessing files with optional compression
#[derive(Debug, Clone)]
pub struct Url {
    inner: url::Url,
}

impl Url {
    /// Parse URL from string
    pub fn parse(url_str: &str) -> Result<Self> {
        let inner = url::Url::parse(url_str)?;
        
        if inner.fragment().is_some() {
            return Err(Error::InvalidUrl("fragment not allowed".into()));
        }
        if inner.port().is_some() {
            return Err(Error::InvalidUrl("port not allowed".into()));
        }
        if !inner.username().is_empty() {
            return Err(Error::InvalidUrl("username not allowed".into()));
        }
        if inner.password().is_some() {
            return Err(Error::InvalidUrl("password not allowed".into()));
        }
        
        Ok(Self { inner })
    }
    
    /// Get URL scheme (e.g., "file")
    pub fn scheme(&self) -> &str {
        self.inner.scheme()
    }
    
    /// Get optional compression from host (e.g., "zstd", "gzip")
    pub fn compression(&self) -> Option<&str> {
        self.inner.host_str()
    }
    
    /// Get path component
    pub fn path(&self) -> &str {
        self.inner.path()
    }
}

/// Trait for URL access to Tinyfs.
#[async_trait::async_trait]
pub trait FileProvider {
    /// Open a URL
    async fn open_url(&self, url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>>;
}

#[async_trait::async_trait]
impl FileProvider for tinyfs::FS {
    /// Open a file for reading with optional decompression
    async fn open_url(&self, url: &Url) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
	// Get reader from TinyFS
	let reader = self.root().await?.async_reader_path(url.path()).await?;
	
	// Wrap with decompression if needed
	decompress_reader(reader, url.compression())
    }
}

/// Wrap AsyncRead with decompression
fn decompress_reader(
    reader: Pin<Box<dyn tinyfs::AsyncReadSeek>>,
    compression: Option<&str>,
) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
    match compression {
        None | Some("none") => {
            // No compression - cast AsyncReadSeek to AsyncRead
            Ok(reader as Pin<Box<dyn AsyncRead + Send>>)
        }
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
        Some(other) => Err(Error::DecompressionError(
            format!("Unsupported compression: {}", other),
        )),
    }
}

