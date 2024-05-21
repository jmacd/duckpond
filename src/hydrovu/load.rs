use std::collections::BTreeMap;
use super::model::Location;
use super::model::Mapping;
use std::path::Path;
use super::model::Vu;
use crate::pond;
use anyhow::Result;

pub fn open_units(pond: &mut pond::Pond) -> Result<BTreeMap<i16, String>> {
    return open_mapping(pond, Path::new("units"));
}

pub fn open_parameters(pond: &mut pond::Pond) -> Result<BTreeMap<i16, String>> {
    return open_mapping(pond, Path::new("params"));
}

pub fn open_locations(pond: &mut pond::Pond) -> Result<Vec<Location>> {
    return pond.read_file(Path::new("locations"));
}

fn open_mapping<P: AsRef<Path>>(pond: &mut pond::Pond, name: P) -> Result<BTreeMap<i16, String>> {
    let items: Vec<Mapping> = pond.read_file(name)?;
    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect())
}

pub fn load() -> Result<Vu> {
    let mut pond = pond::open()?;
    Ok(Vu {
        units: open_units(&mut pond)?,
        params: open_parameters(&mut pond)?,
        locations: open_locations(&mut pond)?,
    })
}
