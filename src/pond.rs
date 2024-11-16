pub mod backup;
pub mod combine;
pub mod copy;
pub mod crd;
pub mod derive;
pub mod dir;
pub mod file;
pub mod inbox;
pub mod scribble;
pub mod wd;
pub mod writer;
pub mod observable;

use crate::hydrovu;

use std::env::temp_dir;
use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use crd::CRDSpec;
use dir::DirEntry;
use dir::FileType;
use dir::TreeLike;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use rand::prelude::thread_rng;
use rand::Rng;
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
use std::sync::LazyLock;
use std::sync::Mutex;
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

/// ForTera indicates a type that can enumerate its string fields
/// as mutable references, so that the tera template engine can
/// run over individual fields.
pub trait ForTera {
    /// for_tera may exclude fields that are used as templates
    /// in application code.
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String>;
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

static CONNECTION: LazyLock<Mutex<Connection>> =
    LazyLock::new(|| Mutex::new(duckdb::Connection::open_in_memory().unwrap()));


pub fn new_connection() -> Result<Connection> {
    let guard = CONNECTION.deref().lock();
    let conn = guard.unwrap().try_clone()?;
    Ok(conn)
}

static TMPDIR: LazyLock<Mutex<PathBuf>> =
    LazyLock::new(|| Mutex::new(temp_dir()));

pub fn tmpfile(ext: &str) -> PathBuf {
    let guard = TMPDIR.deref().lock();
    let tmpdir = guard.unwrap();

    let mut rng = thread_rng();
    let mut tmp = tmpdir.clone();
    tmp.push(format!("{}.{}", rng.gen::<u64>(), ext));
    tmp
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

impl<T: ForArrow> UniqueSpec<T> {
    pub fn inner(&self) -> &T {
        &self.spec
    }
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
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize>;
}

#[derive(Debug)]

pub struct Pond {
    nodes: Vec<Rc<RefCell<dyn TreeLike>>>,
    ders: BTreeMap<PathBuf, Rc<RefCell<dyn Deriver>>>,

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

    let mut p = Pond::new();
    p.insert(Rc::new(RefCell::new(dir::create_dir(
        has.unwrap(),
        PathBuf::new(),
    )?)));
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
    let relp = Path::new("/").to_path_buf();
    let mut root = dir::open_dir(&path, &relp)?;

    let mut p = Pond::new();
    let pond_path = root
        .realpath_current(&mut p, "Pond")?
        .expect("real path here");
    p.resources = file::read_file(pond_path)?;
    p.insert(Rc::new(RefCell::new(root)));
    Ok(p)
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
        CRDSpec::Combine(spec) => pond.apply_spec(
            "Combine",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            combine::init_func,
        ),
	CRDSpec::Observable(spec) => pond.apply_spec(
	    "Observable",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            observable::init_func,
	),
    }
}

pub fn get(_name_opt: Option<String>) -> Result<()> {
    Err(anyhow!("not implemented"))
}

fn check_path<P: AsRef<Path>>(name: P) -> Result<PathBuf> {
    let pref = name.as_ref();
    let mut comp = pref.components();
    if let Some(Component::RootDir) = comp.next() {
        // pass
    } else {
        comp = pref.components()
    }
    for p in comp.clone() {
        match p {
            Component::Normal(_) => {}
            _ => return Err(anyhow!("invalid path {}", name.as_ref().display())),
        }
    }
    Ok(comp.as_path().to_path_buf())
}

type FinishFunc =
    Box<dyn FnOnce(&mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>;

type AfterFunc = Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>;

impl Pond {
    fn new() -> Pond {
        Pond {
            nodes: Vec::new(),
            ders: BTreeMap::new(),
            resources: Vec::new(),
            writer: MultiWriter::new(),
        }
    }

    fn insert(&mut self, node: Rc<RefCell<dyn TreeLike>>) -> usize {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    fn get(&self, id: usize) -> Rc<RefCell<dyn TreeLike>> {
        self.nodes.get(id).unwrap().clone()
    }

    fn root(&self) -> Rc<RefCell<dyn TreeLike>> {
        self.get(0)
    }

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
        T: for<'b> Deserialize<'b> + Serialize + Clone + std::fmt::Debug + ForArrow,
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
        // TODO: Strip leading / RootDir
        self.wd().in_path(path, f)
    }

    pub fn lookup(&mut self, path: &str) -> Option<DirEntry> {
        let (dp, bn) = split_path(path).ok()?;
        self.in_path(dp, |wd| wd.lookup(&bn).ok_or(anyhow!("missing")))
            .ok()
    }

    pub fn wd(&mut self) -> WD {
        WD::new(self, 0)
    }

    pub fn realpath_of(&mut self) -> PathBuf {
        self.root().clone().deref().borrow().realpath_of()
    }

    pub fn sync(&mut self) -> Result<()> {
        let d = self.root().clone();
        let r = d.deref().borrow_mut().sync(self);
        r.map(|_| ())
    }

    pub fn open_derived<'a: 'b, 'b>(
        &'a mut self,
        real: &PathBuf,
        relp: &PathBuf,
        ent: &DirEntry,
    ) -> Result<usize> {
        let mut it = relp.components();
        it.next(); // skip root /
        let top = it.next().unwrap();
        let uuid = it.next().unwrap();
        let p2 = PathBuf::new().join(top).join(uuid);

        match self.ders.get(&p2) {
            None => Err(anyhow!("deriver not found: {}", p2.display())),
            Some(dv) => dv
                .clone()
                .deref()
                .borrow_mut()
                .open_derived(self, real, relp, ent),
        }
    }

    pub fn register_deriver(&mut self, path: PathBuf, der: Rc<RefCell<dyn Deriver>>) {
        self.ders.insert(path, der);
    }

    fn call_in_pond<T, F, R>(&mut self, ft: F) -> Result<Vec<R>>
    where
        T: ForPond + ForArrow + for<'b> Deserialize<'b>,
        F: Fn(&mut Pond, &UniqueSpec<T>) -> Result<R>,
    {
        let kind = T::spec_kind();
        let mut uniq: Vec<UniqueSpec<T>> = Vec::new();

        if let None = self.root().deref().borrow_mut().lookup(self, kind) {
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

    fn start_resources(&mut self) -> Result<Vec<FinishFunc>> {
        let mut after: Vec<FinishFunc> = Vec::new();

        after.extend(self.call_in_pond(backup::start)?);
        after.extend(self.call_in_pond(copy::start)?);
        after.extend(self.call_in_pond(scribble::start)?);
        after.extend(self.call_in_pond(hydrovu::start)?);
        after.extend(self.call_in_pond(inbox::start)?);
        after.extend(self.call_in_pond(derive::start)?);
        after.extend(self.call_in_pond(combine::start)?);
        after.extend(self.call_in_pond(observable::start)?);

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

    pub fn visit_path<P: AsRef<Path>>(
        &mut self,
        path: P,
        glob: &Glob,
        f: &mut impl FnMut(&mut WD, &DirEntry, &Vec<String>) -> Result<()>,
    ) -> Result<()> {
        let (dp, bn) = split_path(&path)?;
        self.in_path(&dp, |wd| {
            if bn == "" {
                return wd.in_path(bn, |wd| visit(wd, glob, Path::new(""), f));
            }
            let ent = wd.lookup(&bn);
            if let None = ent {
                return Ok(());
            }
            let ent = ent.unwrap();
            match ent.ftype {
                FileType::Tree | FileType::SynTree => {
                    // Prefix is a dir
                    wd.in_path(bn, |wd| visit(wd, glob, Path::new(""), f))
                }
                _ => {
                    // Prefix is a full path
                    if glob.is_match(CandidatePath::from("")) {
                        f(wd, &ent, &vec![])
                    } else {
                        Ok(())
                    }
                }
            }
        })
    }
}

fn visit(
    wd: &mut WD,
    glob: &Glob,
    relp: &Path,
    f: &mut impl FnMut(&mut WD, &DirEntry, &Vec<String>) -> Result<()>,
) -> Result<()> {
    let u = wd.unique();
    let cap_cnt = glob.captures().count();
    for entry in &u {
        let np = relp.to_path_buf().join(&entry.prefix);
        let cp = CandidatePath::from(np.as_path());

        if let Some(matched) = glob.matched(&cp) {

	    let captures = (1..=cap_cnt).
		map(|x| matched.get(x).unwrap().to_string()).
		collect();
	    
            f(wd, &entry, &captures)?;
        }

        match entry.ftype {
            FileType::Tree | FileType::SynTree => {
                let mut sd = wd.subdir(&entry.prefix)?;
                let np = PathBuf::new().join(relp).join(&entry.prefix);
                visit(&mut sd, glob, np.as_path(), f)
            }
            _ => Ok(()),
        }?;
    }

    Ok(())
}

pub fn run() -> Result<()> {
    let mut pond = open()?;

    let ff = pond.start_resources()?;

    pond.call_in_pond(backup::run)?;
    pond.call_in_pond(copy::run)?;
    pond.call_in_pond(scribble::run)?;
    pond.call_in_pond(hydrovu::run)?;
    pond.call_in_pond(inbox::run)?;
    pond.call_in_pond(derive::run)?;
    pond.call_in_pond(combine::run)?;
    pond.call_in_pond(observable::run)?;

    pond.close_resources(ff)
}

pub fn list(pattern: &str) -> Result<()> {
    foreach(pattern, &mut |wd: &mut WD, ent: &DirEntry, _: &Vec<String>| {
        let p = wd.pondpath(&ent.prefix);
        // TOOD: Nope; need ASCII boxing
        // p.add_extension(ent.ftype.ext());
        let ps = format!("{}\n", p.display());
        std::io::stdout().write_all(ps.as_bytes())?;
        Ok(())
    })
}

pub fn export(pattern: String, dir: &Path) -> Result<()> {
    std::fs::metadata(dir)?
        .is_dir()
        .then_some(())
        .ok_or(anyhow!("not a dir"))?;

    let glob = Glob::new(&pattern)?;

    foreach(&pattern, &mut |wd: &mut WD, ent: &DirEntry, _: &Vec<String>| {

	let pp = wd.pondpath(&ent.prefix);
	let mp = CandidatePath::from(pp.as_path());
	let matched = glob.matched(&mp).expect("this already matched");
	let cap_cnt = glob.captures().count();
	let name = (1..=cap_cnt).
	    map(|x| matched.get(x).unwrap().to_string()).
	    fold("combined".to_string(), |a, b| format!("{}-{}", a, b.replace("/", ":")));

	let output = PathBuf::from(dir).join(format!("{}.parquet", name));
	wd.copy_to(ent, &mut File::create(&output).with_context(|| format!("create {}", output.display()))?)
    })
}

pub fn foreach<F>(pattern: &str, f: &mut F) -> Result<()>
where
    F: FnMut(&mut WD, &DirEntry, &Vec<String>) -> Result<()>,
{
    let (path, glob) = Glob::new(pattern)?.partition();
    if glob.has_semantic_literals() {
        return Err(anyhow!("glob not supported {}", pattern));
    }

    let mut pond = open()?;

    let ff = pond.start_resources()?;

    pond.visit_path(&path, &glob, f)?;

    pond.close_resources(ff)
}

pub fn cat(path: String) -> Result<()> {
    let mut pond = open()?;

    let ff = pond.start_resources()?;

    let (dp, bn) = split_path(path)?;

    pond.in_path(dp, |wd| {
        let ent = wd.lookup(&bn).ok_or(anyhow!(
            "file not found {} in {}",
            &bn,
            wd.pondpath("").display()
        ))?;
        let mut o = std::io::stdout();
        let _ = wd.copy_to(&ent, &mut o)?;
        Ok(())
    })?;

    pond.close_resources(ff)
}

fn split_path<P: AsRef<Path>>(path: P) -> Result<(PathBuf, String)> {
    let path = check_path(path)?;

    let mut parts = path.components();
    if parts.clone().count() == 0 {
        return Ok((PathBuf::new(), "".to_string()));
    }

    let base = parts.next_back().ok_or(anyhow!("empty path"))?;

    if let Component::Normal(base) = base {
        let ustr = base.to_str().ok_or(anyhow!("invalid utf8"))?;
        let prefix = parts.as_path();
        Ok((prefix.to_path_buf(), ustr.to_string()))
    } else {
        Err(anyhow!("non-utf8 path {:?}", base))
    }
}

pub fn check() -> Result<()> {
    let mut pond = open()?;

    let ress: BTreeSet<String> = pond.in_path("", |wd| Ok(dirnames(wd)))?;

    for kind in ress.iter() {
        pond.in_path(kind.clone(), |wd| wd.check())?;
    }
    for kind in ress.iter() {
        pond.in_path(kind.clone(), |wd| check_reskind(wd, kind.to_string()))?;
    }
    Ok(())
}

fn dirnames(wd: &mut WD) -> BTreeSet<String> {
    wd.entries()
        .iter()
        .filter(|x| x.ftype == FileType::Tree)
        .map(|x| x.prefix.clone())
        .collect()
}

fn filenames(wd: &mut WD) -> BTreeSet<String> {
    wd.entries()
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
