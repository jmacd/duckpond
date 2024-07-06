use super::dir::DirEntry;

use arrow::array::{StringBuilder,BinaryBuilder,Int32Builder,UInt64Builder,UInt8Builder,FixedSizeBinaryBuilder};

use anyhow::Result;

#[derive(Debug)]
pub struct Writer {
    prefix: StringBuilder,
    number: Int32Builder,
    size: UInt64Builder,
    ftype: UInt8Builder,
    uuid: FixedSizeBinaryBuilder,
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
	    uuid: FixedSizeBinaryBuilder::new(16),
	    sha256: FixedSizeBinaryBuilder::new(32),
	    content: BinaryBuilder::new(),
	}
    }

    pub fn record(&mut self, update: DirEntry) -> Result<()> {
	self.prefix.append_value(update.prefix.clone());
	self.number.append_value(update.number);
	self.uuid.append_value(update.uuid.as_bytes())?;
	self.size.append_value(update.size);
	self.ftype.append_value(update.ftype.clone() as u8);
	self.sha256.append_value(update.sha256.clone())?;
	self.content.append_option(update.content.clone());
	Ok(())
    }
}
