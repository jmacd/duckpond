//! Parquet support for TinyFS using Arrow integration
//!
//! This module provides both low-level Arrow RecordBatch operations
//! and high-level ForArrow collection serialization.

use arrow_array::RecordBatch;
use arrow::datatypes::Schema;
use std::path::Path;
use std::sync::Arc;
use async_trait::async_trait;
use crate::{WD, Result, EntryType};
use super::schema::ForArrow;
use serde_arrow::{to_arrow, from_arrow};
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::file::properties::WriterProperties;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures::StreamExt;

/// Default batch size for iterating over collections
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Extension trait for Arrow RecordBatch operations on WD
/// This provides convenience methods as an arrow module extension
#[async_trait]
pub trait ParquetExt {
    /// Create a Table file from a RecordBatch
    async fn create_table_from_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
        batch: &RecordBatch,
    ) -> Result<()>;
    
    /// Create a Table file from a collection implementing ForArrow
    async fn create_table_from_items<P: AsRef<Path> + Send, T, I>(
        &self,
        path: P,
        items: I,
    ) -> Result<()>
    where
        T: ForArrow + serde::Serialize + Send,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send;
    
    /// Read a Table file as a single RecordBatch
    async fn read_table_as_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<RecordBatch>;
    
    /// Read a Table file as a collection of items implementing ForArrow
    async fn read_table_as_items<P: AsRef<Path> + Send, T>(
        &self,
        path: P,
    ) -> Result<Vec<T>>
    where
        T: ForArrow + serde::de::DeserializeOwned + Send;
}

#[async_trait]
impl ParquetExt for WD {
    async fn create_table_from_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
        batch: &RecordBatch,
    ) -> Result<()> {
        // 1. Create streaming AsyncWrite from TinyFS with FileTable entry type
        let writer = self.async_writer_path_with_type(&path, EntryType::FileTable).await?;
        
        // 2. Use AsyncArrowWriter to serialize RecordBatch to Parquet
        let compat_writer = writer.compat_write();
        let mut arrow_writer = AsyncArrowWriter::try_new(compat_writer, batch.schema(), None)
            .map_err(|e| crate::Error::Other(format!("Failed to create Arrow writer: {}", e)))?;
        
        arrow_writer.write(batch).await
            .map_err(|e| crate::Error::Other(format!("Failed to write batch: {}", e)))?;
        
        arrow_writer.close().await
            .map_err(|e| crate::Error::Other(format!("Failed to close Arrow writer: {}", e)))?;
        
        Ok(())
    }
    
    async fn create_table_from_items<P: AsRef<Path> + Send, T, I>(
        &self,
        path: P,
        items: I,
    ) -> Result<()>
    where
        T: ForArrow + serde::Serialize + Send,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send,
    {
        // 1. Convert iterator to Vec for batching
        let items_vec: Vec<T> = items.into_iter().collect();
        
        // 2. Create Arrow schema from ForArrow trait
        let fields = T::for_arrow();
        let arrow_schema = Arc::new(Schema::new(fields));
        
        // 3. Process items in batches of DEFAULT_BATCH_SIZE (1000)
        let mut batches = Vec::new();
        for chunk in items_vec.chunks(DEFAULT_BATCH_SIZE) {
            // Use serde_arrow to convert chunk to RecordBatch
            let arrays = to_arrow(&T::for_arrow(), chunk)
                .map_err(|e| crate::Error::Other(format!("Failed to convert to Arrow: {}", e)))?;
            let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
                .map_err(|e| crate::Error::Other(format!("Failed to create RecordBatch: {}", e)))?;
            batches.push(batch);
        }
        
        // 4. Create streaming AsyncWrite from TinyFS with FileTable entry type
        let writer = self.async_writer_path_with_type(&path, EntryType::FileTable).await?;
        let compat_writer = writer.compat_write();
        let mut arrow_writer = AsyncArrowWriter::try_new(compat_writer, arrow_schema, None)
            .map_err(|e| crate::Error::Other(format!("Failed to create Arrow writer: {}", e)))?;
        
        // 5. Write all batches
        for batch in batches {
            arrow_writer.write(&batch).await
                .map_err(|e| crate::Error::Other(format!("Failed to write batch: {}", e)))?;
        }
        arrow_writer.close().await
            .map_err(|e| crate::Error::Other(format!("Failed to close Arrow writer: {}", e)))?;
        
        Ok(())
    }
    
    async fn read_table_as_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<RecordBatch> {
        // 1. Create streaming AsyncRead from TinyFS
        let reader = self.async_reader_path(&path).await?;
        
        // 2. Use ParquetRecordBatchStreamBuilder for async reading
        let compat_reader = reader.compat();
        let builder = ParquetRecordBatchStreamBuilder::new(compat_reader).await
            .map_err(|e| crate::Error::Other(format!("Failed to create Parquet reader: {}", e)))?;
        let mut stream = builder.build()
            .map_err(|e| crate::Error::Other(format!("Failed to build Parquet stream: {}", e)))?;
        
        // 3. Collect all batches and concatenate them
        let mut all_batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| crate::Error::Other(format!("Failed to read batch: {}", e)))?;
            all_batches.push(batch);
        }
        
        if all_batches.is_empty() {
            return Err(crate::Error::Other("Empty table file".to_string()));
        }
        
        // 4. Concatenate all batches into single RecordBatch
        if all_batches.len() == 1 {
            Ok(all_batches.into_iter().next().unwrap())
        } else {
            let schema = all_batches[0].schema();
            let batch_refs: Vec<&RecordBatch> = all_batches.iter().collect();
            arrow::compute::concat_batches(&schema, &batch_refs)
                .map_err(|e| crate::Error::Other(format!("Failed to concatenate batches: {}", e)))
        }
    }
    
    async fn read_table_as_items<P: AsRef<Path> + Send, T>(
        &self,
        path: P,
    ) -> Result<Vec<T>>
    where
        T: ForArrow + serde::de::DeserializeOwned + Send,
    {
        // 1. Read as RecordBatch first
        let batch = self.read_table_as_batch(path).await?;
        
        // 2. Use serde_arrow to convert from RecordBatch to Vec<T>
        let items: Vec<T> = from_arrow(&T::for_arrow(), &batch)
            .map_err(|e| crate::Error::Other(format!("Failed to convert from Arrow: {}", e)))?;
        
        Ok(items)
    }
}
