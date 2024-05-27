use super::model::Location;
use super::model::Mapping;
use super::model::Vu;
use crate::pond;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;

pub fn open_units(pond: &mut pond::Pond) -> Result<BTreeMap<i16, String>> {
    open_mapping(pond, "units")
}

pub fn open_parameters(pond: &mut pond::Pond) -> Result<BTreeMap<i16, String>> {
    open_mapping(pond, "params")
}

pub fn open_locations(pond: &mut pond::Pond) -> Result<Vec<Location>> {
    // @@@ "" -> "Hydrovu" need to create dir once, have static resource?
    pond.root.in_path(Path::new(""), |dir: &dir::Directory| dir.read_file("locations"))
}

fn open_mapping(
    pond: &mut pond::Pond,
    name: &str,
) -> Result<BTreeMap<i16, String>> {
    let items: Vec<Mapping> = pond.in_dir(Path::new(""), |dir| dir.read_file(name))?;

    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect());
}

pub fn load() -> Result<Vu> {
    let mut pond = pond::open()?;
    Ok(Vu {
        units: open_units(&mut pond)?,
        params: open_parameters(&mut pond)?,
        locations: open_locations(&mut pond)?,
    })
}
