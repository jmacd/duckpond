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

pub fn open_file<T: for<'a> Deserialize<'a>>(name: &Path) -> Result<Vec<T>> {
    let file = File::open(name)
	.with_context(|| format!("open {:?} failed", name))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
	.with_context(|| format!("open {:?} failed", name))?;
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
    records: Vec<T>,
    fields: &[Arc<Field>],
) -> Result<()> {
    let batch = serde_arrow::to_record_batch(fields, &records)
        .with_context(|| "serialize arrow data failed")?;

    let file = File::create(&name).with_context(|| format!("open parquet file {:?}", name.as_ref().display()))?;

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
