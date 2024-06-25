// use arrow2::array::*;
// use arrow2::datatypes::*;

use arrow2::{
    array::{Array, Int32Array, Utf8Array, UInt64Array, UInt8Array, FixedSizeBinaryArray, BinaryArray},
    chunk::Chunk,
    datatypes::{Field, Schema, DataType},
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};

use std::fs::File;
use std::path::PathBuf;
use crate::pond::dir::DirEntry;

use anyhow::Result;

pub fn write_dir(full: &PathBuf, vents: &[DirEntry]) -> Result<()> {
    let mut d_prefix: Vec<String> = Vec::new();
    let mut d_number: Vec<i32> = Vec::new();
    let mut d_size: Vec<u64> = Vec::new();
    let mut d_ftype: Vec<u8> = Vec::new();
    let mut d_sha256: Vec<[u8; 32]> = Vec::new();
    let mut d_content: Vec<Option<Vec<u8>>> = Vec::new();

    for ent in vents {
	d_prefix.push(ent.prefix.clone());
	d_number.push(ent.number);
	d_size.push(ent.size);
	d_ftype.push(ent.ftype.clone() as u8);
	d_sha256.push(ent.sha256.clone());
	d_content.push(ent.content.clone());
    }

    let fields = vec![
        Field::new("prefix", DataType::Utf8, false),
        Field::new("number", DataType::Int32, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("filetype", DataType::UInt8, false),
        Field::new("sha256", DataType::FixedSizeBinary(32), false),
        Field::new("contents", DataType::Binary, true),
    ];

    let boxes = vec![
	Utf8Array::<i32>::from_slice(d_prefix).boxed(),
	Int32Array::from_slice(d_number).boxed(),
	UInt64Array::from_slice(d_size).boxed(),
	UInt8Array::from_slice(d_ftype).boxed(),
	FixedSizeBinaryArray::from_slice(d_sha256).boxed(),
	BinaryArray::<i32>::from(d_content).boxed(),
    ];

    let schema = Schema::from(fields);

    let chunk = Chunk::new(boxes);

    write_chunk(full.clone(), schema, chunk)
}

fn write_chunk(path: PathBuf, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
	data_pagesize_limit: None,
    };

    let iter = vec![Ok(chunk)];

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}
