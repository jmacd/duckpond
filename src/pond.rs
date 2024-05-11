pub mod crd;
pub mod file;
pub mod dir;

use uuid::Uuid;

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crd::CRDSpec;
use anyhow::{Context,Result,anyhow};

use arrow::datatypes::{DataType, Field, Fields, FieldRef};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PondResource {
    kind: String,
    name: String,
    api_version: String,
    uuid: Uuid,
    metadata: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UniqueSpec<T> {
    uuid: Uuid,

    #[serde(flatten)]
    spec: T,
}

fn resource_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("kind", DataType::Utf8, false)),
        Arc::new(Field::new("apiVersion", DataType::Utf8, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("uuid", DataType::Utf8, false)),
        Arc::new(Field::new("metadata",
			    DataType::Map(
				Arc::new(
				    Field::new("entries",
					       DataType::Struct(Fields::from(vec![
						   Field::new("key", DataType::Utf8, false),
						   Field::new("value", DataType::Utf8, false),
					       ])),
					       false,
				    )),
				false),
			    true)),
    ]
}

fn hydrovu_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("uuid", DataType::Utf8, false)),
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("secret", DataType::Utf8, false)),
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

    let directory = dir::create_dir(".pond")?;

    let empty: Vec<PondResource> = vec![];
    directory.write_file("pond", &empty, resource_fields().as_slice())?;

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

pub fn apply<P: AsRef<Path>>(file_name: P) -> Result<()> {
    let pond = open()?;

    let add: CRDSpec = crd::open(file_name)?;

    match add {
	CRDSpec::HydroVu(spec) => pond.apply_spec("HydroVu", spec.api_version, spec.name, spec.metadata, spec.spec),
    }
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
	file::write_file(self.path_of(name).as_path(), &records, fields)
    }

    pub fn path_of<P: AsRef<Path>>(&self, name: P) -> PathBuf {
	let mut p = self.root.clone();
	p.push(name);
	p
    }

    fn apply_spec<T>(&self, kind: &str, api_version: String, name: String, metadata: Option<BTreeMap<String, String>>, spec: T) -> Result<()>
    where
	T: for<'a> Deserialize<'a> + Serialize,
{
	for item in self.resources.iter() {
	    if item.name == name {
		eprintln!("{} exists! {:?} {:?}", name, &api_version, &metadata);
		return Ok(());
	    }
	}

	let id = Uuid::new_v4();
	let mut res = self.resources.clone();
	let pres = PondResource{
	    kind: kind.to_string(),
	    api_version: api_version.clone(),
	    name: name,
	    uuid: id,
	    metadata: metadata,
	};
	eprintln!("add {:?}", pres);
	res.push(pres);

	file::write_file(self.path_of("pond.parquet"), &res, resource_fields().as_slice())?;

	let path = self.path_of(format!("{}.parquet", kind));
	let mut exist: Vec<UniqueSpec<T>> = Vec::new();
	//@@@ TODOfile::open_file(path.as_path())?;
	exist.push(UniqueSpec::<T>{
	    uuid: id,
	    spec: spec,
	});

	file::write_file(path.as_path(), &exist, hydrovu_fields().as_slice())?;
	
	Ok(())
    }
}
