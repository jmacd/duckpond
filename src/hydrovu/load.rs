use super::model::Mapping;
use super::model::ScopedLocation;
use super::model::Vu;

use crate::pond::wd::WD;

use anyhow::Result;
use std::collections::BTreeMap;

pub fn open_units(d: &mut WD) -> Result<BTreeMap<String, String>> {
    open_mapping(d, "units")
}

pub fn open_parameters(d: &mut WD) -> Result<BTreeMap<String, String>> {
    open_mapping(d, "params")
}

pub fn open_locations(d: &mut WD) -> Result<Vec<ScopedLocation>> {
    d.read_file("locations")
}

fn open_mapping(d: &mut WD, name: &str) -> Result<BTreeMap<String, String>> {
    let items: Vec<Mapping> = d.read_file(name)?;

    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect());
}

pub fn load(d: &mut WD) -> Result<Vu> {
    Ok(Vu {
        units: open_units(d)?,
        params: open_parameters(d)?,
        locations: open_locations(d)?,
    })
}
