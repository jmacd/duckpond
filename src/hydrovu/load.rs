use std::collections::BTreeMap;
use super::model::Location;
use super::model::Mapping;
use std::path::Path;
use super::model::Vu;
use crate::pond;
use anyhow::Result;

pub fn open_units(pond: &pond::Pond) -> Result<BTreeMap<i16, String>> {
    return open_mapping(pond, Path::new("units.parquet"));
}

pub fn open_parameters(pond: &pond::Pond) -> Result<BTreeMap<i16, String>> {
    return open_mapping(pond, Path::new("params.parquet"));
}

pub fn open_locations(pond: &pond::Pond) -> Result<Vec<Location>> {
    return pond.open_file(Path::new("locations.parquet"));
}

fn open_mapping<P: AsRef<Path>>(pond: &pond::Pond, name: P) -> Result<BTreeMap<i16, String>> {
    let items: Vec<Mapping> = pond.open_file(name)?;
    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect())
}

pub fn load() -> Result<Vu> {
    let pond = pond::open()?;
    Ok(Vu {
        units: open_units(&pond)?,
        params: open_parameters(&pond)?,
        locations: open_locations(&pond)?,
    })
}
