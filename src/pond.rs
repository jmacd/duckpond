use futures::executor;

pub mod crd;
pub mod dir;
pub mod file;
pub mod wd;
pub mod writer;
pub mod entry;
pub mod backup;
pub mod scribble;
pub mod copy;
pub mod inbox;

use wd::WD;
use writer::MultiWriter;
use uuid::Uuid;
use dir::FileType;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Write;
use std::iter::Iterator;

use sha2::{Sha256, Digest};

use crd::CRDSpec;
use anyhow::{Context,Result,anyhow};
use std::env;
use arrow::array::as_string_array;

use crate::hydrovu;

use arrow::datatypes::{DataType, Field, Fields, FieldRef};
use serde::{Serialize, Deserialize};

use datafusion::{
    prelude::SessionContext,
    datasource::{file_format::parquet::ParquetFormat,listing::ListingOptions},
};

pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;
}

pub trait ForPond {
    fn spec_kind() -> &'static str;
}

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

impl ForArrow for PondResource {
    fn for_arrow() -> Vec<FieldRef> {
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UniqueSpec<T: ForArrow> {
    uuid: Uuid,

    #[serde(flatten)]
    spec: T,
}

impl<T: ForPond+ForArrow> UniqueSpec<T> {
    fn dirpath(&self) -> PathBuf {
	Path::new(T::spec_kind()).join(self.uuid.to_string())
    }
}

impl<T: ForArrow> ForArrow for UniqueSpec<T> {
    fn for_arrow() -> Vec<FieldRef> {
	let mut fields = T::for_arrow();
	fields.push(Arc::new(Field::new("uuid", DataType::Utf8, false)));
	fields
    }    
}

#[derive(Debug)]
pub struct Pond {
    pub root: dir::Directory,
    pub resources: Vec<PondResource>,
    pub writer: MultiWriter,
}

pub type InitContinuation = Box<dyn FnOnce(&mut Pond) -> Result<()>>;

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

    if let Some(path) = has.clone() {
	if let Ok(_) = std::fs::metadata(&path) {
	    return Err(anyhow!("pond exists! {:?}", path));
	}
    }

    let mut p = Pond{
	resources: vec![],
	// @@@ use of new_v4 indicates not copying Uuid from the remote.
	// Copy module can pass it in with an Option<.> using the value
	// from the backup.
	root: dir::create_dir(has.unwrap(), PathBuf::new())?,
	writer: MultiWriter::new(),
    };
    let newres = p.resources.clone();
    p.in_path(Path::new(""),
	      |d| d.write_whole_file("pond", &newres))?;

    p.sync()
}

pub fn open() -> Result<Pond> {
    let loc = find_pond()?;
    if let None = loc {
	return Err(anyhow!("pond does not exist"))
    }
    let path = loc.unwrap().clone();
    let relp = PathBuf::new();
    let root = dir::open_dir(&path, &relp)?;
    let pond_path = root.current_path_of("pond")?;
    
    Ok(Pond{
	root: root,
	resources: file::read_file(pond_path)?,
	writer: MultiWriter::new(),
    })
}

pub fn apply<P: AsRef<Path>>(file_name: P) -> Result<()> {
    let mut pond = open()?;

    let add: CRDSpec = crd::open(file_name)?;

    match add {
	CRDSpec::HydroVu(spec) => pond.apply_spec("HydroVu", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, hydrovu::init_func),
	CRDSpec::S3Backup(spec) => pond.apply_spec("S3Backup", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, backup::init_func),
	CRDSpec::S3Copy(spec) => pond.apply_spec("S3Copy", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, copy::init_func),
	CRDSpec::Scribble(spec) => pond.apply_spec("Scribble", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, scribble::init_func),
	CRDSpec::Inbox(spec) => pond.apply_spec("Inbox", spec.api_version, spec.name, spec.desc, spec.metadata, spec.spec, inbox::init_func),
    }
}

pub fn get(name_opt: Option<String>) -> Result<()> {
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
    
    match name_opt {
	None => {
	    let df =
		executor::block_on(ctx.sql("SELECT * FROM resources"))
		.with_context(|| "could not select")?;

	    executor::block_on(df.show())
		.with_context(|| "show failed")
	},
	Some(name) => {
	    let parts: Vec<&str> = name.split('/').collect();

	    match parts.len() {
		2 => {
		    let query = format!("SELECT {:?} FROM resources WHERE name = '{}'", parts[1], parts[0]);
		    let df =
			executor::block_on(ctx.sql(query.as_str()))
			.with_context(|| "could not select")?;
		    
		    let res = executor::block_on(df.collect())
			.with_context(|| "collect failed")?;

		    for batch in res {
			for value in as_string_array(batch.column(0)) {
			    std::io::stdout().write_all(format!("{}\n", value.unwrap()).as_bytes()).with_context(|| format!("could not write to stdout"))?;
			}
		    }
		    Ok(())
		},
		_ => {
		    Err(anyhow!("unknown get syntax"))
		},
	    }
	},
    }
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
	T: for<'a> Deserialize<'a> + Serialize + Clone + std::fmt::Debug + ForArrow,
        F: FnOnce(&mut WD, &mut UniqueSpec<T>) -> Result<Option<InitContinuation>>
    {
	for item in self.resources.iter() {
	    if item.name == name {
		// @@@ update logic?
		return Err(anyhow!("resource exists"));
	    }
	}

	// Add a new resource
	let id = Uuid::new_v4();
	let uuidstr = id.to_string();
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

	let cont = self.in_path(dirname, |d: &mut WD| -> Result<Option<InitContinuation>> {
	    // Write the updated resources.
	    d.write_whole_file(&basename, &res)?;

	    d.in_path(kind, |d: &mut WD| -> Result<Option<InitContinuation>> {
	    
		let mut exist: Vec<UniqueSpec<T>>;
		
		if let Some(_) = d.last_path_of(kind) {
		    exist = d.read_file(kind)?;
		} else {
		    exist = Vec::new();
		}
	    
		// Write the new unique spec.
		let mut uspec = UniqueSpec::<T>{
		    uuid: id,
		    spec: spec.clone(),
		};

		// Kind-specific initialization.
		let cont = d.in_path(id.to_string(), |wd| init_func(wd, &mut uspec))?;

		exist.push(uspec);

		d.write_whole_file(kind, &exist)?;

		Ok(cont)
	    })
	})?;

	if let Some(f) = cont {
	    f(self)?;
	}

	self.sync()?;

	std::io::stdout().write_all(format!("{}\n", uuidstr).as_bytes()).with_context(|| format!("could not write to stdout"))
    }

    pub fn in_path<P: AsRef<Path>, F, T>(&mut self, path: P, f: F) -> Result<T>
    where F: FnOnce(&mut WD) -> Result<T> {
	let mut wd = WD{
	    w: &mut self.writer,
	    d: &mut self.root,
	};
	wd.in_path(path, f)
    }

    pub fn sync(&mut self) -> Result<()> {
	self.root.sync(&mut self.writer).map(|_| ())
    }
}

impl Pond {
    fn call_in_wd<T, F>(&mut self, ft: F) -> Result<()>
    where T: ForPond + ForArrow + for<'b> Deserialize<'b>,
	  F: Fn(&mut WD, &UniqueSpec<T>) -> Result<()>
    {
	let kind = T::spec_kind();
	
	self.in_path(kind, |d: &mut WD| -> Result<()> {

	    if let None = d.last_path_of(kind) {
		return Ok(())
	    }
	    let uniq: Vec<UniqueSpec<T>> = d.read_file(kind)?;

	    for res in &uniq {
		d.in_path(res.uuid.to_string(),
			  |d: &mut WD| -> Result<()> {
			      ft(d, res)
			  })?;
	    }

	    Ok(())
	})?;
	    
	Ok(())
    }

    fn call_in_pond<T, F, R>(&mut self, ft: F) -> Result<Vec<R>>
    where T: ForPond + ForArrow + for<'b> Deserialize<'b>,
	  F: Fn(&mut Pond, &UniqueSpec<T>) -> Result<R>
    {
	let kind = T::spec_kind();
	let mut uniq: Vec<UniqueSpec<T>> = Vec::new();
	
	self.in_path(kind, |d: &mut WD| -> Result<()> {
	    if let None = d.last_path_of(kind) {
		return Ok(())
	    }
	    uniq.extend(d.read_file(kind)?);

	    Ok(())
	})?;

	uniq.iter().map(|x| ft(self, x)).collect()
    }
}

pub fn run() -> Result<()> {
    let mut pond = open()?;

    // I tried various ways to make a resource trait that would generalize this
    // pattern, but got stuck and now am not sure how to do this in Rust.

    let mut finish1: Vec<
	    Box<dyn FnOnce(&mut Pond) ->
		Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>
		>> = Vec::new();

    finish1.extend(pond.call_in_pond(backup::start)?);
    finish1.extend(pond.call_in_pond(copy::start)?);
    finish1.extend(pond.call_in_pond(scribble::start)?);
    finish1.extend(pond.call_in_pond(hydrovu::start)?);
    finish1.extend(pond.call_in_pond(inbox::start)?);

    pond.call_in_wd(backup::run)?;
    pond.call_in_wd(copy::run)?;
    pond.call_in_wd(scribble::run)?;
    pond.call_in_wd(hydrovu::run)?;
    pond.call_in_wd(inbox::run)?;

    let mut finish2: Vec<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> = Vec::new();

    for bf in finish1 {
	finish2.push(bf(&mut pond)?);
    }

    pond.sync()?;

    for bf in finish2 {
	bf(&mut pond.writer)?;
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

pub fn check() -> Result<()> {
    let mut pond = open()?;

    let ress: BTreeSet<String> = pond.in_path("", |wd| Ok(dirnames(wd)))?;

    for kind in ress.iter() {
	pond.in_path(kind.clone(), |wd| check_reskind(wd, kind.to_string()))?;
    }
    Ok(())
}

fn dirnames(wd: &mut WD) -> BTreeSet<String> {
    wd.d.ents.iter().filter(|x| x.ftype == FileType::Tree).map(|x| x.prefix.clone()).collect()
}

fn filenames(wd: &mut WD) -> BTreeSet<String> {
    wd.d.ents.iter().filter(|x| x.ftype != FileType::Tree).map(|x| x.prefix.clone()).collect()
}

fn check_reskind(wd: &mut WD, kind: String) -> Result<()> {
    for id in dirnames(wd).iter() {
	let path = PathBuf::from(kind.clone()).join(id.as_str());

	let digest = wd.in_path(id.clone(), |wd| check_instance(wd))?.finalize();
	
	eprintln!("{} => {:#x}", path.display(), digest);
    }

    Ok(())
}

fn check_instance(wd: &mut WD) -> Result<Sha256> {
    let mut hasher = Sha256::new();

    // Note that we ignore DirEntry number and only use current contents
    // to calculate. This makes version number irrelevant and allows the
    // backup process to coallesce writes.
    for name in filenames(wd) {
	let ent = wd.last_path_of(name.as_str()).unwrap();

	hasher.update(ent.prefix.as_bytes());
	hasher.update(ent.size.to_be_bytes());
	hasher.update(vec![ent.ftype as u8]);
	hasher.update(ent.sha256);
    }

    for name in dirnames(wd) {
	hasher.update(wd.in_path(name, |wd| check_instance(wd))?.finalize());
    }	

    Ok(hasher)
}
