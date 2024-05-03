use anyhow::Context;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::File;
use super::error::Error;
use super::model::Location;
use super::model::Mapping;
use super::model::Vu;

pub fn open_units() -> Result<BTreeMap<String, String>, Error> {
    return open_mapping("units.parquet");
}

pub fn open_parameters() -> Result<BTreeMap<String, String>, Error> {
    return open_mapping("params.parquet");
}

pub fn open_locations() -> Result<Vec<Location>, Error> {
    return open_file("locations.parquet");
}

fn open_file<T: for<'a> Deserialize<'a>>(name: &str) -> Result<Vec<T>, Error> {
    let file = File::open(name)
	.with_context(|| format!("open {} failed", name))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
	.with_context(|| format!("open {} failed", name))?;
    let mut reader = builder.build()
	.with_context(|| "initialize reader failed")?;
    Ok(serde_arrow::from_record_batch(&reader.next().unwrap()
				      .with_context(|| "next record batch failed")?)
       .with_context(|| "deserialize record batch failed")?)
}

fn open_mapping(name: &str) -> Result<BTreeMap<String, String>, Error> {
    let items: Vec<Mapping> = open_file(name)?;
    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect())
}

pub fn load() -> Result<Vu, Error> {
    Ok(Vu {
        units: open_units()?,
        params: open_parameters()?,
        locations: open_locations()?,
    })
}
