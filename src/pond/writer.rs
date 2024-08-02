use super::dir::DirEntry;

use std::sync::Arc;
use std::fs::File;
use std::path::Path;
use crate::pond::ForArrow;

use arrow::array::{StringBuilder,BinaryBuilder,Int32Builder,UInt64Builder,UInt8Builder,FixedSizeBinaryBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_array::array::ArrayRef;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use anyhow::{Result,Context};

#[derive(Debug)]
pub struct Writer {
    prefix: StringBuilder,
    number: Int32Builder,
    size: UInt64Builder,
    ftype: UInt8Builder,
    sha256: FixedSizeBinaryBuilder,
    content: BinaryBuilder,
}

impl Writer {
    pub fn new() -> Self {
	Writer{
	    prefix: StringBuilder::new(),
	    number: Int32Builder::new(),
	    size: UInt64Builder::new(),
	    ftype: UInt8Builder::new(),
	    sha256: FixedSizeBinaryBuilder::new(32),
	    content: BinaryBuilder::new(),
	}
    }

    pub fn record(&mut self, update: &DirEntry) -> Result<()> {
	self.prefix.append_value(update.prefix.clone());
	self.number.append_value(update.number);
	self.size.append_value(update.size);
	self.ftype.append_value(update.ftype.clone() as u8);
	self.sha256.append_value(update.sha256.clone())?;
	self.content.append_option(update.content.clone());
	Ok(())
    }

    pub fn commit_to_local_file<P: AsRef<Path>>(&mut self, filename: P) -> Result<()> {
	self.commit(filename)
    }

    fn commit<P: AsRef<Path>>(&mut self, filename: P) -> Result<()> {
	let fields = DirEntry::for_arrow();
	let schema = Schema::new(fields);
	let builders: Vec<ArrayRef> = vec![
	    Arc::new(self.prefix.finish_cloned()),
	    Arc::new(self.number.finish_cloned()),
	    Arc::new(self.size.finish_cloned()),
	    Arc::new(self.ftype.finish_cloned()),
	    Arc::new(self.sha256.finish_cloned()),
	    Arc::new(self.content.finish_cloned()),
	];

	let batch = RecordBatch::try_new(Arc::new(schema), builders)?;

	let props = WriterProperties::builder()
            .set_compression(
		Compression::ZSTD(ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?))
            .build();

	let file = File::create_new(filename.as_ref())
	    .with_context(|| format!("create new parquet file {:?}", filename.as_ref()))?;

	let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .with_context(|| "new arrow writer failed")?;
	
	writer
            .write(&batch)
            .with_context(|| "write parquet data failed")?;
	writer
            .close()
            .with_context(|| "close parquet file failed")?;
	
	Ok(())
    }
}

#[derive(Debug)]
pub struct MultiWriter {
    writers: Vec<Writer>,
}

impl MultiWriter {
    pub fn new() -> Self {
	MultiWriter{
	    writers: Vec::new(),
	}
    }

    pub fn record(&mut self, update: &DirEntry) -> Result<()> {
	for wr in &mut self.writers {
	    wr.record(update)?;
	}
	Ok(())
    }

    pub fn add_writer(&mut self) -> usize {
	self.writers.push(Writer::new());
	self.writers.len()-1
    }

    pub fn writer_mut(&mut self, id: usize) -> Option<&mut Writer> {
	self.writers.get_mut(id)
    }
}
