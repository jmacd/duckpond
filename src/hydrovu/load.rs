use super::model::Location;
use super::model::Mapping;
use super::model::Vu;
use crate::pond::dir;
use anyhow::Result;
use std::collections::BTreeMap;

pub fn open_units(d: &mut dir::Directory) -> Result<BTreeMap<i16, String>> {
    open_mapping(d, "units")
}

pub fn open_parameters(d: &mut dir::Directory) -> Result<BTreeMap<i16, String>> {
    open_mapping(d, "params")
}

pub fn open_locations(d: &mut dir::Directory) -> Result<Vec<Location>> {
    d.read_file("locations")
}

fn open_mapping(
    d: &mut dir::Directory,
    name: &str,
) -> Result<BTreeMap<i16, String>> {
    let items: Vec<Mapping> = d.read_file(name)?;

    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect());
}

pub fn load(d: &mut dir::Directory) -> Result<Vu> {
    Ok(Vu {
        units: open_units(d)?,
        params: open_parameters(d)?,
        locations: open_locations(d)?,
    })
}
