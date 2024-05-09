pub mod crd;
pub mod file;

use std::fs::create_dir;
use std::path::Path;
use std::path::PathBuf;
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

fn resource_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("kind", DataType::Utf8, false)),
        Arc::new(Field::new("apiVersion", DataType::Utf8, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
    ]
}

pub fn find_pond() -> Result<Option<PathBuf>> {
    let path = std::env::current_dir().with_context(|| "could not get working directory")?;
	
    find_recursive(path.as_path())
}

fn find_recursive<P: AsRef<Path>>(path: P) -> Result<Option<PathBuf>> {
    let mut ppath = path.as_ref().to_path_buf();
    ppath.push(".pond");
    let path = ppath.as_path();
    if path.is_dir() {
	return Ok(Some(path.to_path_buf()));
    }
    ppath.pop();
    if let Some(parent) = ppath.parent() {
	return find_recursive(parent);
    }
    Ok(None)
}

pub fn init() -> Result<()> {
    let has = find_pond()?;
    if let Some(path) = has {
	return Err(anyhow!("pond exists! {:?}", path));
    }

    create_dir(".pond")
	.with_context(|| "pond already exists")?;

    let empty: Vec<PondResource> = vec![];
    file::write_file("pond.parquet", empty, resource_fields().as_slice())?;
    Ok(())
}

#[derive(Debug)]
pub struct Pond {
    pub root: PathBuf,
    pub resources: Vec<PondResource>,
}

pub fn open() -> Result<Pond> {
    let loc = find_pond()?;
    if let None = loc {
	return Err(anyhow!("pond does not exist"))
    }
    let root = loc.unwrap();
    let mut path = root.clone();
    path.push("pond.parquet");
    Ok(Pond{
	root: root,
	resources: file::open_file(&path)?,
    })
}

impl Pond {
    pub fn open_file<T: for<'a> Deserialize<'a>, P: AsRef<Path>>(&self, name: P) -> Result<Vec<T>> {
	file::open_file(self.path_of(name).as_path())
    }

    pub fn write_file<T: Serialize, P: AsRef<Path>>(
	&self,
	name: P,
	records: Vec<T>,
	fields: &[Arc<Field>],
    ) -> Result<()> {
	file::write_file(self.path_of(name).as_path(), records, fields)
    }

    pub fn path_of<P: AsRef<Path>>(&self, name: P) -> PathBuf {
	let mut p = self.root.clone();
	p.push(name);
	p
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HydrovuSpec {
    key: String,
    secret: String,
}

pub fn apply<P: AsRef<Path>>(file_name: P) -> Result<()> {
    let mut pond = open()?;

    let add: CRD<HydrovuSpec> = crd::open(file_name)?;

    if let None = add.metadata {
	return Err(anyhow!("missing metadata"))
    }
    let md = add.metadata.as_ref().unwrap();
    let name = md.get("name");
    if let None = name {
	return Err(anyhow!("missing name"))
    }

    for item in pond.resources.iter() {
	if item.name == *name.unwrap() {
	    eprintln!("exists! {:?}", &add);
	    return Ok(());
	}
    }
    eprintln!("add {:?}", add);

    pond.resources.push(PondResource{
	kind: add.kind,
	api_version: add.api_version,
	name: name.unwrap().clone(),
    });

    file::write_file(Path::new("pond.parquet"), pond.resources, resource_fields().as_slice())?;

    Ok(())
}
