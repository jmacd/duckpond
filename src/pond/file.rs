use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::path::Path;
use arrow::datatypes::Field;
use anyhow::{Context, Result};
use std::fs::File;
use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::io::Write;
use std::io::Read;

use sha2::{Sha256, Digest};

pub fn read_file<T: for<'a> Deserialize<'a>, P: AsRef<Path>>(name: P) -> Result<Vec<T>> {
    let p = name.as_ref();
    let file = File::open(p)
	.with_context(|| format!("open {:?} failed", p.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
	.with_context(|| format!("open {:?} failed", p.display()))?;
    let mut reader = builder.build()
	.with_context(|| "initialize reader failed")?;
    let input = reader.next();
    match input {
	None => Ok(vec![]),
	Some(value) => 
	    Ok(serde_arrow::from_record_batch(&value.with_context(|| "deserialize record batch failed")?)
	       .with_context(|| "parse record batch failed")?),
    }
}

pub fn write_file<T: Serialize, P: AsRef<Path>>(
    name: P,
    records: &Vec<T>,
    fields: &[Arc<Field>],
) -> Result<()> {
    let batch = serde_arrow::to_record_batch(fields, &records)
        .with_context(|| "serialize arrow data failed")?;

    let file = File::create_new(&name)
	.with_context(|| format!("create new parquet file {:?}", name.as_ref().display()))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
        ))
        .build();

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

pub fn sha256_file<P: AsRef<Path>>(path: P) -> Result<(sha2::Sha256, u64, Option<Vec<u8>>)> {
    let mut buffer = [0; 1<<16];
    let mut count: u64 = 0;
    let mut hasher = Sha256::new();
    let mut file = File::open(path)?;

    loop {
	let n = file.read(&mut buffer[..])?;
	hasher.write(&buffer[0..n])?;
	count += n as u64;
	if n == 0 {
	    if count <= buffer.len() as u64 {
		return Ok((hasher, count as u64, Some(Vec::from(&buffer[..]))))
	    } else {
		return Ok((hasher, count, None))
	    }
	}
    }
}
