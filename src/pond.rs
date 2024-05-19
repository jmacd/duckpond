pub mod crd;
pub mod file;
pub mod dir;

use uuid::Uuid;

use std::collections::BTreeMap;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crd::CRDSpec;
use anyhow::{Context,Result,anyhow};

use arrow::datatypes::{DataType, Field, Fields, FieldRef};
use serde::{Serialize, Deserialize};

//use std::ffi::OsStr;

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

    let mut directory = dir::create_dir(".pond")?;

    let empty: Vec<PondResource> = vec![];
    directory.write_file("pond", &empty, resource_fields().as_slice())?;

    directory.close_dir()?;

    Ok(())
}

#[derive(Debug)]
pub struct Pond {
    pub root: dir::Directory,
    pub resources: Vec<PondResource>,
}

pub fn open() -> Result<Pond> {
    let loc = find_pond()?;
    if let None = loc {
	return Err(anyhow!("pond does not exist"))
    }
    let path = loc.unwrap().clone();
    let root = dir::open_dir(&path)?;
    let pond_path = root.current_path_of("pond")?;
    
    Ok(Pond{
	root: root,
	resources: file::open_file(pond_path)?,
    })
}

pub fn apply<P: AsRef<Path>>(file_name: P) -> Result<()> {
    let mut pond = open()?;

    let add: CRDSpec = crd::open(file_name)?;

    match add {
	CRDSpec::HydroVu(spec) => pond.apply_spec("HydroVu", spec.api_version, spec.name, spec.metadata, spec.spec),
    }
}

pub fn get(_name: Option<String>) -> Result<()> {
    let pond = open()?;

    //file::write_file(self.path_of("pond.parquet"), &res, resource_fields().as_slice())?;
    for res in pond.resources {
	// match &name {
	//     None => {},
	//     Some(name) => if res.name == *name {
	// 	continue;
	//     },
	// }
	eprintln!("{:?}", res);
    }
    Ok(())
}

fn check_path<P: AsRef<Path>>(name: P) -> Result<()> {
    let pref = name.as_ref();
    for p in pref.components() {
	match p {
	    Component::Normal(_) => {},
	    _ => { return Err(anyhow!("invalid path {}", name.as_ref().display())) }
	}
    }
    Ok(())
}

impl Pond {
    pub fn open_file<T: for<'a> Deserialize<'a>, P: AsRef<Path>>(&self, name: P) -> Result<Vec<T>> {
	let (parent, base) = self.base_in_dir_path(name)?;
	file::open_file(parent.current_path_of(base)?.as_path())
    }

    pub fn write_file<T: Serialize, P: AsRef<Path>>(
	&self,
	name: P,
	records: Vec<T>,
	fields: &[Arc<Field>],
    ) -> Result<()> {
	let (parent, base) = self.base_in_dir_path(name)?;
	file::write_file(parent.next_path_of(base)?.as_path(), &records, fields)
    }

    pub fn base_in_dir_path<P: AsRef<Path>>(
	&self,
	name: P,
    ) -> Result<(dir::Directory, &str)> {
	let parts = name.as_ref().components().clone();
	let basecomp = parts.last().ok_or(anyhow!("empty path"))?;

	check_path(parts.clone())?;

	let dp = self.root.real_path_of(parts.as_path());
	let parent = dir::open_dir(&dp)?;

	if let Component::Normal(base) = basecomp {
	    let ustr = base.to_str().ok_or(anyhow!("invalid utf8"))?;
	    Ok((parent, ustr))
	} else {
	    Err(anyhow!("invalid path"))
	}
    }
    
    pub fn current_path_of(&self, name: &str) -> Result<PathBuf> {
	self.root.current_path_of(name)
    }

    fn apply_spec<T>(&mut self, kind: &str, api_version: String, name: String, metadata: Option<BTreeMap<String, String>>, spec: T) -> Result<()>
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

    // let mut directory = dir::create_dir(".pond")?;
    // let empty: Vec<PondResource> = vec![];
    // directory.write_file("pond".to_string(), &empty, resource_fields().as_slice())?;
    // directory.close_dir()?;

	self.root.write_file("pond", &res, resource_fields().as_slice())?;

	let path = self.current_path_of(&format!("{}.parquet", kind))?;
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

