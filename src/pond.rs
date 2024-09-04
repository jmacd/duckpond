use futures::executor;

pub mod backup;
pub mod copy;
pub mod crd;
pub mod derive;
pub mod dir;
pub mod file;
pub mod inbox;
pub mod scribble;
pub mod wd;
pub mod writer;

// use std::collections::HashMap;
use crate::hydrovu;
use anyhow::{anyhow, Context, Result};
use arrow::array::as_string_array;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use crd::CRDSpec;
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    prelude::SessionContext,
};
use dir::DirEntry;
use dir::FileType;
use dir::TreeLike;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;
use std::fs::File;
use std::io::Write;
use std::iter::Iterator;
use std::ops::Deref;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;
use wax::{CandidatePath, Glob, Pattern};
use wd::WD;
use writer::MultiWriter;

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
            Arc::new(Field::new(
                "metadata",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, false),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            )),
        ]
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UniqueSpec<T: ForArrow> {
    uuid: Uuid,

    #[serde(flatten)]
    spec: T,
}

impl<T: ForPond + ForArrow> UniqueSpec<T> {
    pub fn dirpath(&self) -> PathBuf {
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

pub trait Deriver: std::fmt::Debug {
    fn open_derived(&self, path: &PathBuf, entry: &DirEntry) -> Result<Rc<RefCell<dyn TreeLike>>>;
}

#[derive(Debug)]
pub struct Pond {
    root: Rc<RefCell<dyn TreeLike>>,
    ders: BTreeMap<PathBuf, Box<dyn Deriver>>,

    pub resources: Vec<PondResource>,
    pub writer: MultiWriter,
}

pub type InitContinuation = Box<dyn FnOnce(&mut Pond) -> Result<()>>;

pub fn find_pond() -> Result<Option<PathBuf>> {
    match env::var("POND") {
        Ok(val) => Ok(Some(PathBuf::new().join(val))),
        _ => {
            let path =
                std::env::current_dir().with_context(|| "could not get working directory")?;

            find_recursive(path.as_path())
        }
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

    let mut p = Pond {
        resources: vec![],
        ders: BTreeMap::new(),
        root: Rc::new(RefCell::new(dir::create_dir(has.unwrap(), PathBuf::new())?)),
        writer: MultiWriter::new(),
    };
    let newres = p.resources.clone();
    p.in_path(Path::new(""), |d| {
        d.write_whole_file("Pond", FileType::Table, &newres)
    })?;

    p.sync()
}

pub fn open() -> Result<Pond> {
    let loc = find_pond()?;
    if let None = loc {
        return Err(anyhow!("pond does not exist"));
    }
    let path = loc.unwrap().clone();
    let relp = PathBuf::new();
    let root = dir::open_dir(&path, &relp)?;
    let pond_path = root.realpath_current("Pond")?;

    Ok(Pond {
        root: Rc::new(RefCell::new(root)),
        ders: BTreeMap::new(),
        resources: file::read_file(pond_path)?,
        writer: MultiWriter::new(),
    })
}

pub fn apply<P: AsRef<Path>>(file_name: P, vars: &Vec<(String, String)>) -> Result<()> {
    let mut pond = open()?;

    let add: CRDSpec = crd::open(file_name, vars)?;

    match add {
        CRDSpec::HydroVu(spec) => pond.apply_spec(
            "HydroVu",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            hydrovu::init_func,
        ),
        CRDSpec::Backup(spec) => pond.apply_spec(
            "Backup",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            backup::init_func,
        ),
        CRDSpec::Copy(spec) => pond.apply_spec(
            "Copy",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            copy::init_func,
        ),
        CRDSpec::Scribble(spec) => pond.apply_spec(
            "Scribble",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            scribble::init_func,
        ),
        CRDSpec::Inbox(spec) => pond.apply_spec(
            "Inbox",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            inbox::init_func,
        ),
        CRDSpec::Derive(spec) => pond.apply_spec(
            "Derive",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            derive::init_func,
        ),
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
        &format!("file://{}", pond.root.deref().borrow().realpath_current("Pond")?.display()),
        listing_options,
        None,
        None,
    ))
    .with_context(|| "df could not load pond")?;

    match name_opt {
        None => {
            let df = executor::block_on(ctx.sql("SELECT * FROM resources"))
                .with_context(|| "could not select")?;

            executor::block_on(df.show()).with_context(|| "show failed")
        }
        Some(name) => {
            let parts: Vec<&str> = name.split('/').collect();

            match parts.len() {
                2 => {
                    let query = format!(
                        "SELECT {:?} FROM resources WHERE name = '{}'",
                        parts[1], parts[0]
                    );
                    let df = executor::block_on(ctx.sql(query.as_str()))
                        .with_context(|| "could not select")?;

                    let res = executor::block_on(df.collect()).with_context(|| "collect failed")?;

                    for batch in res {
                        for value in as_string_array(batch.column(0)) {
                            std::io::stdout()
                                .write_all(format!("{}\n", value.unwrap()).as_bytes())
                                .with_context(|| format!("could not write to stdout"))?;
                        }
                    }
                    Ok(())
                }
                _ => Err(anyhow!("unknown get syntax")),
            }
        }
    }
}

fn check_path<P: AsRef<Path>>(name: P) -> Result<()> {
    let pref = name.as_ref();
    let mut comp = pref.components();
    if let Some(Component::RootDir) = comp.next() {
        // pass
    } else {
        return Err(anyhow!("use an absolute path"));
    }
    for p in comp {
        match p {
            Component::Normal(_) => {}
            _ => return Err(anyhow!("invalid path {}", name.as_ref().display())),
        }
    }
    Ok(())
}

impl Pond {
    fn apply_spec<T, F>(
        &mut self,
        kind: &str,
        api_version: String,
        name: String,
        desc: String,
        metadata: Option<BTreeMap<String, String>>,
        spec: T,
        init_func: F,
    ) -> Result<()>
    where
        T: for<'a> Deserialize<'a> + Serialize + Clone + std::fmt::Debug + ForArrow,
        F: FnOnce(&mut WD, &UniqueSpec<T>) -> Result<Option<InitContinuation>>,
    {
        for item in self.resources.iter() {
            if item.name == name {
                // TODO: update logic.
                return Err(anyhow!("resource exists"));
            }
        }

        let ff = self.start_resources()?;

        // Add a new resource
        let id = Uuid::new_v4();
        let uuidstr = id.to_string();
        let mut res = self.resources.clone();
        let pres = PondResource {
            kind: kind.to_string(),
            api_version: api_version.clone(),
            name: name.clone(),
            desc: desc.clone(),
            uuid: id,
            metadata: metadata,
        };
        res.push(pres);

        eprintln!("create {kind} uuid {uuidstr}");

        let (dirname, basename) = split_path(Path::new("/Pond"))?;

        let cont = self.in_path(dirname, |d: &mut WD| -> Result<Option<InitContinuation>> {
            // Write the updated resources.
            d.write_whole_file(&basename, FileType::Table, &res)?;

            d.in_path(kind, |d: &mut WD| -> Result<Option<InitContinuation>> {
                let mut exist: Vec<UniqueSpec<T>>;

                if let Some(_) = d.lookup(kind) {
                    exist = d.read_file(kind)?;
                } else {
                    exist = Vec::new();
                }

                // Write the new unique spec.
                let uspec = UniqueSpec::<T> {
                    uuid: id,
                    spec: spec.clone(),
                };

                // Kind-specific initialization.
                let cont = d.in_path(id.to_string(), |wd| init_func(wd, &uspec))?;

                exist.push(uspec);

                d.write_whole_file(kind, FileType::Table, &exist)?;

                Ok(cont)
            })
        })?;

        if let Some(f) = cont {
            f(self)?;
        }

        self.close_resources(ff)?;

        std::io::stdout()
            .write_all(format!("{}\n", uuidstr).as_bytes())
            .with_context(|| format!("could not write to stdout"))
    }

    pub fn in_path<P: AsRef<Path>, F, T>(&mut self, path: P, f: F) -> Result<T>
    where
        F: FnOnce(&mut WD) -> Result<T>,
    {
        self.wd().in_path(path, f)
    }

    pub fn wd(&mut self) -> WD {
        let node = self.root.clone();
        WD::new(self, node)
    }

    pub fn sync(&mut self) -> Result<()> {
        let d = self.root.clone();
        let r = d.deref().borrow_mut().sync(self);
        r.map(|_| ())
    }

    pub fn open_derived(
        &mut self,
        relp: &PathBuf,
        ent: &DirEntry,
    ) -> Result<Rc<RefCell<dyn TreeLike>>> {
        let mut it = relp.components();
        let top = it.next().unwrap();
        let uuid = it.next().unwrap();
        let p2 = PathBuf::new().join(top).join(uuid);

        match self.ders.get(&p2) {
            None => Err(anyhow!("deriver not found: {}", p2.display())),
            Some(dv) => dv.open_derived(relp, ent),
        }
    }

    pub fn register_deriver(&mut self, path: PathBuf, der: Box<dyn Deriver>) {
        self.ders.insert(path, der);
    }
}

impl Pond {
    fn call_in_pond<T, F, R>(&mut self, ft: F) -> Result<Vec<R>>
    where
        T: ForPond + ForArrow + for<'b> Deserialize<'b>,
        F: Fn(&mut Pond, &UniqueSpec<T>) -> Result<R>,
    {
        let kind = T::spec_kind();
        let mut uniq: Vec<UniqueSpec<T>> = Vec::new();

        if let None = self.root.deref().borrow().lookup(kind) {
            return Ok(vec![]);
        }

        self.in_path(kind, |d: &mut WD| -> Result<()> {
            if let None = d.lookup(kind) {
                return Ok(());
            }
            uniq.extend(d.read_file(kind)?);

            Ok(())
        })?;

        uniq.iter().map(|x| ft(self, x)).collect()
    }
}

type FinishFunc =
    Box<dyn FnOnce(&mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>;

type AfterFunc = Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>;

impl Pond {
    fn start_resources(&mut self) -> Result<Vec<FinishFunc>> {
        let mut after: Vec<FinishFunc> = Vec::new();

        after.extend(self.call_in_pond(backup::start)?);
        after.extend(self.call_in_pond(copy::start)?);
        after.extend(self.call_in_pond(scribble::start)?);
        after.extend(self.call_in_pond(hydrovu::start)?);
        after.extend(self.call_in_pond(inbox::start)?);
        after.extend(self.call_in_pond(derive::start)?);

        Ok(after)
    }

    fn close_resources(&mut self, ff: Vec<FinishFunc>) -> Result<()> {
        let mut finalize: Vec<AfterFunc> = Vec::new();

        for bf in ff {
            finalize.push(bf(self)?);
        }

        self.sync()?;

        for bf in finalize {
            bf(&mut self.writer)?;
        }

        Ok(())
    }
}

pub fn run() -> Result<()> {
    let mut pond = open()?;

    let ff = pond.start_resources()?;

    pond.call_in_pond(backup::run)?;
    pond.call_in_pond(copy::run)?;
    pond.call_in_pond(scribble::run)?;
    pond.call_in_pond(hydrovu::run)?;
    pond.call_in_pond(inbox::run)?;

    pond.close_resources(ff)
}

pub fn list(expr: String) -> Result<()> {
    let (path, glob) = Glob::new(&expr)?.partition();
    if glob.has_semantic_literals() {
        return Err(anyhow!("glob not supported {}", &expr));
    }

    let mut pond = open()?;

    let ff = pond.start_resources()?;

    pond.in_path(&path, |wd| {
        visit(wd, &glob, &mut |wd: &mut WD, ent: &DirEntry| {
            eprintln!("{}", wd.pondpath(&ent.prefix).display());
            Ok(())
        })
    })?;

    pond.close_resources(ff)
}

fn visit(
    wd: &mut WD,
    glob: &Glob,
    f: &mut impl FnMut(&mut WD, &DirEntry) -> Result<()>,
) -> Result<()> {
    for entry in wd.unique() {
        let full = wd.pondpath(&entry.prefix);
        let cp = CandidatePath::from(full.as_path());

        if glob.is_match(cp) {
            f(wd, &entry)?;
        }

        match entry.ftype {
            FileType::Tree | FileType::SynTree => {
                let mut sd = wd.subdir(&entry.prefix)?;
                visit(&mut sd, glob, f)?;
            }
            _ => {}
        };
    }

    Ok(())
}

pub fn cat(path: String) -> Result<()> {
    let mut pond = open()?;

    let (dp, bn) = split_path(path)?;

    pond.in_path(dp, |wd| {
        let p = wd.realpath_current(&bn)?;
        let mut f = File::open(p)?;
        let mut o = std::io::stdout();
        let _ = std::io::copy(&mut f, &mut o)?;
        Ok(())
    })
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
    wd.d()
        .deref()
        .borrow()
        .entries()
        .iter()
        .filter(|x| x.ftype == FileType::Tree)
        .map(|x| x.prefix.clone())
        .collect()
}

fn filenames(wd: &mut WD) -> BTreeSet<String> {
    wd.d()
        .deref()
        .borrow()
        .entries()
        .iter()
        .filter(|x| x.ftype != FileType::Tree)
        .map(|x| x.prefix.clone())
        .collect()
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
        let ent = wd.lookup(name.as_str()).unwrap();

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

// TODO: use type aliases to simplify this decl (in several places)
pub fn start_noop<T: ForArrow>(
    _pond: &mut Pond,
    _uspec: &UniqueSpec<T>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    Ok(Box::new(
        |_pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
            Ok(Box::new(|_| -> Result<()> { Ok(()) }))
        },
    ))
}
