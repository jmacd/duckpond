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

#[derive(Debug)]
pub struct Pond {
    pub dirs: BTreeMap<PathBuf, dir::Directory>,
    pub resources: Vec<PondResource>,
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

pub fn open() -> Result<Pond> {
    let loc = find_pond()?;
    if let None = loc {
	return Err(anyhow!("pond does not exist"))
    }
    let path = loc.unwrap().clone();
    let root = dir::open_dir(&path)?;
    let pond_path = root.current_path_of("pond")?;
    let mut dirs: BTreeMap<PathBuf, dir::Directory> = BTreeMap::new();
    dirs.insert(PathBuf::new(), root);
    
    Ok(Pond{
	dirs: dirs,
	resources: file::read_file(pond_path)?,
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
    pub fn get_mut_dir<P: AsRef<Path>>(&mut self, path: P) -> Option<&mut dir::Directory> {
	self.dirs.get_mut(path.as_ref())
    }

    pub fn real_path_of<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf> {
	let dir = self.dirs.get(path.as_ref()).ok_or(anyhow!("internal error"))?;
	Ok(dir.real_path_of(path))
    }

    pub fn current_path_of(&self, name: &str) -> Result<PathBuf> {
	let path = PathBuf::new();
	let dir = self.dirs.get(&path).ok_or(anyhow!("internal error"))?;
	dir.current_path_of(name)
    }

    pub fn next_path_of(&self, name: &str) -> Result<PathBuf> {
	let path = PathBuf::new();
	let dir = self.dirs.get(&path).ok_or(anyhow!("internal error"))?;
	Ok(dir.next_path_of(name))
    }

    pub fn read_file<T: for<'a> Deserialize<'a>, P: AsRef<Path>>(&mut self, name: P) -> Result<Vec<T>> {
	let (parent, base) = self.base_in_dir_path(&name)?;
	file::read_file(parent.current_path_of(&base)?.as_path())
    }

    pub fn write_file<T: Serialize, P: AsRef<Path>>(
	&mut self,
	name: P,
	records: Vec<T>,
	fields: &[Arc<Field>],
    ) -> Result<()> {
	let (parent, base) = self.base_in_dir_path(&name)?;
	file::write_file(parent.next_path_of(&base), &records, fields)
    }

    pub fn base_in_dir_path<P: AsRef<Path>>(
	&mut self,
	name: &P,
    ) -> Result<(&mut dir::Directory, String)> {
	let mut parts = name.as_ref().components();

	check_path(&parts)?;

	let basecomp = parts.next_back().ok_or(anyhow!("empty path"))?;
	let path = parts.as_path();
	let dir = self.get_mut_dir(&path);

	if let None = dir {
	    let d = dir::create_dir(self.real_path_of(&path)?)?;
	    self.dirs.insert(path.to_path_buf(), d);
	}

	let d = self.dirs.get_mut(path).unwrap();

	if let Component::Normal(base) = basecomp {
	    let ustr = base.to_str().ok_or(anyhow!("invalid utf8"))?;
	    Ok((d, ustr.to_string()))
	} else {
	    Err(anyhow!("invalid path"))
	}
    }
    
    fn apply_spec<T>(&mut self, kind: &str, api_version: String, name: String, metadata: Option<BTreeMap<String, String>>, spec: T) -> Result<()>
    where
	T: for<'a> Deserialize<'a> + Serialize
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

	let ppath = PathBuf::new().join("pond");
	let (dir, ustr): (&mut dir::Directory, String) = self.base_in_dir_path(&ppath)?;
	
	dir.write_file(&ustr, &res, resource_fields().as_slice())?;

	let mut exist: Vec<UniqueSpec<T>>;

	if let Some(_) = dir.last_path_of(kind) {
	    exist = self.read_file(kind)?;
	} else {
	    exist = Vec::new();
	}

	exist.push(UniqueSpec::<T>{
	    uuid: id,
	    spec: spec,
	});

	dir.write_file(kind, &exist, hydrovu_fields().as_slice())?;

	self.close()
    }

    pub fn close(&self) -> Result<()> {
	// @@@
	for (_, d) in self.dirs {
	    d.close_dir()?
	}
	Ok(())
    }
}

