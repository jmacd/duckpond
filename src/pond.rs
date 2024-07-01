use futures::executor;

pub mod crd;
pub mod dir;
pub mod file;
pub mod wd;
pub mod writer;
pub mod entry;
pub mod backup;

use wd::WD;
use writer::Writer;
use uuid::Uuid;

use std::collections::BTreeMap;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crd::CRDSpec;
use anyhow::{Context,Result,anyhow};
use std::env;

use crate::hydrovu;

use arrow::datatypes::{DataType, Field, Fields, FieldRef};
use serde::{Serialize, Deserialize};

use datafusion::{
    prelude::SessionContext,
    datasource::{file_format::parquet::ParquetFormat,listing::ListingOptions},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PondResource {
    kind: String,
    name: String,
    desc: String,
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
    pub root: dir::Directory,
    pub resources: Vec<PondResource>,
    pub writer: Writer,
}

fn resource_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("kind", DataType::Utf8, false)),
        Arc::new(Field::new("apiVersion", DataType::Utf8, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("desc", DataType::Utf8, false)),
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
    match env::var("POND") {
	Ok(val) => Ok(Some(PathBuf::new().join(val))),
	_ => {
	    let path = std::env::current_dir().with_context(|| "could not get working directory")?;
	
	    find_recursive(path.as_path())
	},
    }
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

    let mut p = Pond{
	resources: vec![],
	root: dir::create_dir(".pond")?,
	writer: Writer::new(),
    };
    let newres = p.resources.clone();
    p.in_path(Path::new(""),
	      |d| d.write_whole_file("pond", &newres, resource_fields().as_slice()))?;

    p.close()
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
	resources: file::read_file(pond_path)?,
	writer: Writer::new(),
    })
}

pub fn apply<P: AsRef<Path>>(file_name: P) -> Result<()> {
    let mut pond = open()?;

    let add: CRDSpec = crd::open(file_name)?;

    match add {
	CRDSpec::HydroVu(spec) => pond.apply_spec("HydroVu", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, hydrovu::init_func),
	CRDSpec::S3Backup(spec) => pond.apply_spec("S3Backup", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, backup::init_func),	
    }
}

// What is this: Use _name @@@
pub fn get(_name: Option<String>) -> Result<()> {
    let pond = open()?;

    // create local execution context
    let ctx = SessionContext::new();
    let file_format = ParquetFormat::default().with_enable_pruning(true);

    let listing_options = ListingOptions {
        file_extension: ".parquet".to_owned(),
        format: Arc::new(file_format),
        table_partition_cols: vec![],
	file_sort_order: vec![],
        collect_stat: true,
        target_partitions: 1,
    };

    executor::block_on(ctx.register_listing_table(
        "resources",
        &format!("file://{}", pond.root.current_path_of("pond")?.display()),
        listing_options,
	None,
	None,
    )).with_context(|| "df could not load pond")?;
    
    // execute the query
    let df =
	executor::block_on(ctx.sql("SELECT * FROM resources"))
	.with_context(|| "could not select")?;

    executor::block_on(df.show())
	.with_context(|| "show failed")?;
    Ok(())
}

fn check_path<P: AsRef<Path>>(name: P) -> Result<()> {
    let pref = name.as_ref();
    let mut comp = pref.components();
    if let Some(Component::RootDir) = comp.next() {
	// pass
    } else {
	return Err(anyhow!("use an absolute path"))
    }
    for p in comp {
	match p {
	    Component::Normal(_) => {},
	    _ => { return Err(anyhow!("invalid path {}", name.as_ref().display())) }
	}
    }
    Ok(())
}

impl Pond {
    fn apply_spec<T, F>(&mut self, kind: &str, api_version: String, name: String, desc: String, metadata: Option<BTreeMap<String, String>>, spec: T, init_func: F) -> Result<()>
    where
	T: for<'a> Deserialize<'a> + Serialize,
        F: FnOnce(&mut WD) -> Result<()>
    {
	for item in self.resources.iter() {
	    if item.name == name {
		return Ok(());
	    }
	}

	// Add a new resource
	let id = Uuid::new_v4();
	let mut res = self.resources.clone();
	let pres = PondResource{
	    kind: kind.to_string(),
	    api_version: api_version.clone(),
	    name: name.clone(),
	    desc: desc.clone(),
	    uuid: id,
	    metadata: metadata,
	};
	res.push(pres);

	let (dirname, basename) = split_path(Path::new("/pond"))?;

	self.in_path(dirname, |d: &mut WD| -> Result<()> {
	    // Write the updated resources.
	    d.write_whole_file(&basename, &res, resource_fields().as_slice())?;

	    d.in_path(kind, |d: &mut WD| -> Result<()> {
	    
		let mut exist: Vec<UniqueSpec<T>>;
		
		if let Some(_) = d.last_path_of(kind) {
		    exist = d.read_file(kind)?;
		} else {
		    exist = Vec::new();
		}
	    
		// Write the new unique spec.
		exist.push(UniqueSpec::<T>{
		    uuid: id,
		    spec: spec,
		});
	    
		d.write_whole_file(kind, &exist, hydrovu_fields().as_slice())?;

		// Kind-specific initialization.
		let uuidstr = id.to_string();
		d.in_path(uuidstr, init_func)
	    })
	})?;

	self.close()
    }

    pub fn in_path<P: AsRef<Path>, F, T>(&mut self, path: P, f: F) -> Result<T>
    where F: FnOnce(&mut WD) -> Result<T> {
	let mut wd = WD{
	    w: &mut self.writer,
	    d: &mut self.root,
	};
	wd.in_path(path, f)
    }

    pub fn close(&mut self) -> Result<()> {
	self.root.close(&mut self.writer).map(|_| ())
    }
}

pub fn run() -> Result<()> {
    let mut pond = open()?;

    let ress = pond.resources.clone();
    for res in ress {
	match res.kind.as_str() {
	    "HydroVu" => hydrovu::run(&mut pond, Path::new("HydroVu").join(res.uuid.to_string()))?,
	    _ => Err(anyhow!("unknown resource"))?,
	}
    }
    
    Ok(())
}

fn split_path<P: AsRef<Path>>(path: P) -> Result<(PathBuf, String)> {
    let mut parts = path.as_ref().components();

    check_path(&parts)?;
    parts.next();

    let base = parts.next_back().ok_or(anyhow!("empty path"))?;

    if let Component::Normal(base) = base {
        let ustr = base.to_str().ok_or(anyhow!("invalid utf8"))?;
        let prefix = parts.as_path();
        Ok((prefix.to_path_buf(), ustr.to_string()))
    } else {
        Err(anyhow!("non-utf8 path {:?}", base))
    }
}

pub fn export_data(name: String) -> Result<()> {
    let mut pond = open()?;

    let dname = Path::new(&name);

    pond.in_path(dname, hydrovu::export_data)
}

pub fn check() ->Result<()> {
    let mut pond = open()?;
    pond.in_path(Path::new(""), |d| d.check())
}
