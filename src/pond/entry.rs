use arrow::array::{StringBuilder,BinaryBuilder,Int32Builder,UInt64Builder,UInt8Builder,FixedSizeBinaryBuilder};
use arrow::datatypes::{DataType, Field};
use arrow_array::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use std::fs::File;
use std::path::PathBuf;
use crate::pond::dir::DirEntry;
use arrow::datatypes::Schema;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use std::sync::Arc;

use anyhow::{Context, Result};

pub fn write_dir(full: &PathBuf, v: &[DirEntry]) -> Result<()> {
    let mut d_prefix = StringBuilder::new();
    let mut d_number = Int32Builder::new();
    let mut d_size = UInt64Builder::new();
    let mut d_ftype = UInt8Builder::new();
    let mut d_sha256 = FixedSizeBinaryBuilder::new(32);
    let mut d_content = BinaryBuilder::new();

    for ent in v {
	d_prefix.append_value(ent.prefix.clone());
	d_number.append_value(ent.number);
	d_size.append_value(ent.size);
	d_ftype.append_value(ent.ftype.clone() as u8);
	d_sha256.append_value(ent.sha256.clone())?;
	d_content.append_option(ent.content.clone());
    }

    let fields =
        vec![
	    Arc::new(Field::new("prefix", DataType::Utf8, false)),
	    Arc::new(Field::new("number", DataType::Int32, false)),
	    Arc::new(Field::new("size", DataType::UInt64, false)),
	    Arc::new(Field::new("filetype", DataType::UInt8, false)),
	    Arc::new(Field::new("sha256", DataType::FixedSizeBinary(32), false)),
	    Arc::new(Field::new("contents", DataType::Binary, true)),
	];
    
    let schema = Schema::new(fields);
    let builders: Vec<ArrayRef> = vec![
	Arc::new(d_prefix.finish()),
	Arc::new(d_number.finish()),
	Arc::new(d_size.finish()),
	Arc::new(d_ftype.finish()),
	Arc::new(d_sha256.finish()),
	Arc::new(d_content.finish()),
    ];

    let batch = RecordBatch::try_new(Arc::new(schema), builders)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)
					   .with_context(|| "invalid zstd level 6")?,
        ))
        .build();

    let file = File::create_new(full)
	.with_context(|| format!("create new parquet file {:?}", full.display()))?;

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .with_context(|| "new arrow writer failed")?;
	
    writer
        .write(&batch)
        .with_context(|| "write parquet data failed")?;
    writer
        .close()
        .with_context(|| "close parquet file failed")?;
    eprintln!("wellp");
	
    Ok(())
}

//     let fields = vec![
//         Field::new("prefix", DataType::Utf8, false),
//         Field::new("number", DataType::Int32, false),
//         Field::new("size", DataType::UInt64, false),
//         Field::new("filetype", DataType::UInt8, false),
//         Field::new("sha256", DataType::FixedSizeBinary(32), false),
//         Field::new("contents", DataType::Binary, true),
//     ];

//     let boxes = vec![
// 	Utf8Array::<i32>::from_slice(d_prefix).boxed(),
// 	Int32Array::from_slice(d_number).boxed(),
// 	UInt64Array::from_slice(d_size).boxed(),
// 	UInt8Array::from_slice(d_ftype).boxed(),
// 	FixedSizeBinaryArray::from_slice(d_sha256).boxed(),
// 	BinaryArray::<i32>::from(d_content).boxed(),
//     ];

//     let schema = Schema::from(fields);

//     let chunk = Chunk::new(boxes);

//     write_chunk(full.clone(), schema, chunk)
// }

// fn write_chunk(path: PathBuf, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
//     let options = WriteOptions {
//         write_statistics: true,
//         compression: CompressionOptions::Uncompressed,
//         version: Version::V2,
// 	data_pagesize_limit: None,
//     };

//     let iter = vec![Ok(chunk)];

//     let encodings = schema
//         .fields
//         .iter()
//         .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
//         .collect();

//     let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

//     // Create a new empty file
//     let file = File::create(path)?;

//     let mut writer = FileWriter::try_new(file, schema, options)?;

//     for group in row_groups {
//         writer.write(group?)?;
//     }
//     let _size = writer.end(None)?;
//     Ok(())
// }
