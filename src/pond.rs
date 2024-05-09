pub mod crd;
pub mod file;

use std::fs::create_dir;
use std::path::Path;
use std::sync::Arc;

use crd::CRD;
use anyhow::{Context,Result,anyhow};

use arrow::datatypes::{DataType, Field, FieldRef};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PondResource {
    kind: String,
    api_version: String,
    name: String,
}

fn pond_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("kind", DataType::Utf8, false)),
        Arc::new(Field::new("apiVersion", DataType::Utf8, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
    ]
}

// pub fn locate() -> Result<Option<PathBuf>> {
//     match std::env::current_dir() {
// 	None => qkq
//     }
// }

pub fn init() -> Result<()> {

    create_dir(".pond")
	.with_context(|| "pond already exists")?;

    let empty: Vec<PondResource> = vec![];
    file::write_file(".pond/pond.parquet", empty, pond_fields().as_slice())?;
    Ok(())
}

pub fn open() -> Result<Vec<PondResource>> {
    file::open_file(".pond/pond.parquet")
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HydrovuSpec {
    key: String,
    secret: String,
}

pub fn apply(file_name: &Path) -> Result<()> {
    let mut ress = open()?;

    let add: CRD<HydrovuSpec> = crd::open(file_name)?;

    if let None = add.metadata {
	return Err(anyhow!("missing metadata"))
    }
    let md = add.metadata.as_ref().unwrap();
    let name = md.get("name");
    if let None = name {
	return Err(anyhow!("missing name"))
    }

    for item in &ress {
	if item.name == *name.unwrap() {
	    eprintln!("exists! {:?}", &add);
	    return Ok(());
	}
    }
    eprintln!("add {:?}", add);

    ress.push(PondResource{
	kind: add.kind,
	api_version: add.api_version,
	name: name.unwrap().clone(),
    });

    file::write_file(".pond/pond.parquet", ress, pond_fields().as_slice())?;

    Ok(())
}
