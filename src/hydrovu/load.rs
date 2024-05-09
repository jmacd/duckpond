use std::collections::BTreeMap;
use super::model::Location;
use super::model::Mapping;
use super::model::Vu;
use crate::pond::file;
use anyhow::{Result, Error};

pub fn open_units() -> Result<BTreeMap<i16, String>> {
    return open_mapping("units.parquet");
}

pub fn open_parameters() -> Result<BTreeMap<i16, String>, Error> {
    return open_mapping("params.parquet");
}

pub fn open_locations() -> Result<Vec<Location>, Error> {
    return file::open_file("locations.parquet");
}

fn open_mapping(name: &str) -> Result<BTreeMap<i16, String>, Error> {
    let items: Vec<Mapping> = file::open_file(name)?;
    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect())
}

pub fn load() -> Result<Vu, Error> {
    Ok(Vu {
        units: open_units()?,
        params: open_parameters()?,
        locations: open_locations()?,
    })
}
