pub mod backup;
pub mod combine;
pub mod copy;
pub mod crd;
pub mod derive;
pub mod dir;
pub mod file;
pub mod inbox;
pub mod reduce;
pub mod scribble;
pub mod template;
pub mod wd;
pub mod writer;

use crate::hydrovu;

use anyhow::{Context, Result, anyhow};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use chrono::TimeZone;
use crd::CRDSpec;
use dir::DirEntry;
use dir::FileType;
use dir::TreeLike;
use duckdb::Connection;
use rand::Rng;
use rand::prelude::thread_rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::env::temp_dir;
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

static TMPDIR: LazyLock<Mutex<PathBuf>> = LazyLock::new(|| Mutex::new(temp_dir()));

pub fn tmpfile(ext: &str) -> PathBuf {
    let guard = TMPDIR.deref().lock();
    let tmpdir = guard.unwrap();

    let mut rng = thread_rng();
    let mut tmp = tmpdir.clone();
    tmp.push(format!("{}.{}", rng.r#gen::<u64>(), ext));
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
    pub fn kind(&self) -> &'static str {
        T::spec_kind()
    }
}

impl<T: ForArrow> ForArrow for UniqueSpec<T> {
    fn for_arrow() -> Vec<FieldRef> {
        let mut fields = T::for_arrow();
        fields.push(Arc::new(Field::new("uuid", DataType::Utf8, false)));
        fields
    }
}

pub trait Deriver {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize>;
}

pub struct Pond {
    /// nodes are working-directory handles corresponding with tree-like objects
    /// that have been initialized.
    nodes: Vec<Rc<RefCell<dyn TreeLike>>>,

    ders: BTreeMap<String, Rc<RefCell<dyn Deriver>>>,

    pub resources: Vec<PondResource>,
    pub writer: MultiWriter,

    pub expctx: tera::Context,
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
        CRDSpec::Template(spec) => pond.apply_spec(
            "Template",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            template::init_func,
        ),
        CRDSpec::Reduce(spec) => pond.apply_spec(
            "Reduce",
            spec.api_version,
            spec.name,
            spec.desc,
            spec.metadata,
            spec.spec,
            reduce::init_func,
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
            expctx: tera::Context::new(),
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
        F: FnOnce(
            &mut WD,
            &UniqueSpec<T>,
            Option<UniqueSpec<T>>,
        ) -> Result<Option<InitContinuation>>,
    {
        let existing = self.resources.iter().find(|x| x.name == name).cloned();

        let is_new = existing.is_none();

        let ff = self.start_resources()?;

        let mut res = self.resources.clone();

        let id = if let Some(pres) = existing {
            // Update existing resource
            pres.uuid
        } else {
            // Add a new resource
            let id = Uuid::new_v4();
            let pres = PondResource {
                kind: kind.to_string(),
                api_version: api_version.clone(),
                name: name.clone(),
                desc: desc.clone(),
                uuid: id,
                metadata: metadata,
            };
            res.push(pres);
            id
        };

        let uuidstr = id.to_string();

        eprintln!(
            "{} {kind} uuid {uuidstr} name {name}",
            if is_new { "create" } else { "update" }
        );

        let (dirname, basename) = split_path(Path::new("/Pond"))?;

        let cont = self.in_path(dirname, |d: &mut WD| -> Result<Option<InitContinuation>> {
            if is_new {
                // Write the updated resources.
                // TODO: Assumes we haven't changed desc, metadata, etc, i.e.,
                // not changing the definition, only the spec.
                d.write_whole_file(&basename, FileType::Table, &res)?;
            }

            d.in_path(kind, |d: &mut WD| -> Result<Option<InitContinuation>> {
                let mut exist: Vec<UniqueSpec<T>>;

                if let Some(_) = d.lookup(kind).entry {
                    exist = d.read_file(kind)?;
                } else {
                    exist = Vec::new();
                }

                // Form a unique spec.
                let uspec = UniqueSpec::<T> {
                    uuid: id,
                    spec: spec.clone(),
                };

                let mut former: Option<UniqueSpec<T>> = None;
                if is_new {
                    exist.push(uspec.clone());

                    // Enter a new symlink.
                    d.create_symlink(&name, &id.to_string())?;
                } else {
                    // Modify `exist` with the new spec
                    let old = exist.iter_mut().find(|x| x.uuid == id);
                    if old.is_none() {
                        return Err(anyhow!(
                            "expected to find existing spec {}/{}",
                            kind,
                            uuidstr
                        ));
                    }
                    let replace = old.unwrap();
                    former = Some(replace.clone());
                    *replace = uspec.clone();
                }

                // Kind-specific initialization.
                let cont = d.in_path(id.to_string(), |wd| init_func(wd, &uspec, former))?;

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
        self.in_path(dp, |wd| wd.lookup(&bn).entry.ok_or(anyhow!("missing")))
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

    pub fn register_deriver(&mut self, kind: &'static str, der: Rc<RefCell<dyn Deriver>>) {
        self.ders
            .entry(kind.to_string())
            .or_insert_with(|| der.clone());
    }

    pub fn open_derived<'a: 'b, 'b>(
        &'a mut self,
        real: &PathBuf,
        relp: &PathBuf,
        ent: &DirEntry,
    ) -> Result<usize> {
        let mut it = relp.components();
        it.next(); // skip root /
        let top = match it.next() {
            Some(Component::Normal(memb)) => Ok(memb.to_string_lossy()),
            _ => Err(anyhow!("unexpected path structure")),
        }?
        .into_owned();

        match self.ders.get(&top) {
            None => Err(anyhow!("deriver not found: {}: {}", top, relp.display())),
            Some(dv) => dv
                .clone()
                .deref()
                .borrow_mut()
                .open_derived(self, real, relp, ent),
        }
    }

    fn call_in_pond<T, F, R>(&mut self, ft: F) -> Result<Vec<R>>
    where
        T: ForPond + ForArrow + for<'b> Deserialize<'b>,
        F: Fn(&mut Pond, &UniqueSpec<T>) -> Result<R>,
    {
        let kind = T::spec_kind();
        let mut uniq: Vec<UniqueSpec<T>> = Vec::new();

        if let None = self.root().deref().borrow_mut().lookup(self, kind).entry {
            return Ok(vec![]);
        }

        self.in_path(kind, |d: &mut WD| -> Result<()> {
            if let None = d.lookup(kind).entry {
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
        after.extend(self.call_in_pond(template::start)?);
        after.extend(self.call_in_pond(reduce::start)?);

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

    pub fn visit_path<I, T: Default + Extend<I> + IntoIterator<Item = I>, P: AsRef<Path>>(
        &mut self,
        path: P,
        glob: &Glob,
        mut f: &mut impl FnMut(&mut WD, &DirEntry, &Vec<String>) -> Result<T>,
    ) -> Result<T> {
        // @@@ TODO there is an easy way to go recursive here and not return.
        // There was a typo, where "out_pattern" ("reduce-$0") was passed as
        // the in_pattern. Since it has no slash, it was scanning the entire tree,
        // recursing indefintely.
        let (dp, bn) = split_path(&path)?;
        self.in_path(&dp, &mut |wd: &mut WD| -> Result<T> {
            if bn == "" {
                return wd.in_path(&bn, |wd| visit(wd, glob, Path::new(""), &mut f));
            }
            let look = wd.lookup(&bn);
            if look.entry.is_none() {
                return Ok(T::default());
            }
            let ent = look.entry.unwrap();
            match ent.ftype {
                FileType::Tree | FileType::SynTree => {
                    // Prefix is a dir
                    wd.in_path(&bn, |wd| visit(wd, glob, Path::new(""), &mut f))
                }
                _ => {
                    // Prefix is a full path
                    if glob.is_match(CandidatePath::from("")) {
                        f(wd, &ent, &vec![])
                    } else {
                        Ok(T::default())
                    }
                }
            }
        })
    }
}

fn visit<I, T: Default + Extend<I> + IntoIterator<Item = I>>(
    wd: &mut WD,
    glob: &Glob,
    relp: &Path,
    f: &mut impl FnMut(&mut WD, &DirEntry, &Vec<String>) -> Result<T>,
) -> Result<T> {
    let u = wd.unique();
    let cap_cnt = glob.captures().count();
    let mut r = T::default();
    for entry in &u {
        let np = relp.to_path_buf().join(&entry.prefix);
        let cp = CandidatePath::from(np.as_path());

        if let Some(matched) = glob.matched(&cp) {
            let captures = (1..=cap_cnt)
                .map(|x| matched.get(x).unwrap().to_string())
                .collect();

            r.extend(f(wd, &entry, &captures)?.into_iter());
        }

        match entry.ftype {
            FileType::Tree | FileType::SynTree => {
                // This use of lookup is super inefficient. Finish the refactoring!
                let lu = wd.lookup(&entry.prefix);
                let mut sd = wd.subdir(&lu)?;
                let np = PathBuf::new().join(relp).join(&entry.prefix);
                r.extend(visit(&mut sd, glob, np.as_path(), f)?.into_iter());
            }
            _ => {}
        };
    }
    Ok(r)
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
    pond.call_in_pond(template::run)?;
    pond.call_in_pond(reduce::run)?;

    pond.close_resources(ff)
}

pub fn list(pattern: &str) -> Result<()> {
    foreach(pattern, &mut |wd: &mut WD,
                           ent: &DirEntry,
                           _: &Vec<String>| {
        let p = wd.pondpath(&ent.prefix);
        // TOOD: Nope; need ASCII boxing
        // p.add_extension(ent.ftype.ext());
        let ps = format!("{}\n", p.display());
        std::io::stdout().write_all(ps.as_bytes())?;
        Ok(vec![])
    })?;
    Ok(())
}

/// ExportOutput returns the exported file name(s) with relevan metadata
/// passed to/from stages of the export command.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ExportOutput {
    file: PathBuf,
    start_time: Option<i64>,
    end_time: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ExportSet {
    Empty(),
    Files(Vec<ExportOutput>),
    Map(HashMap<String, Box<ExportSet>>),
}

pub fn export(patterns: Vec<String>, dir: &Path, temporal: &String) -> Result<()> {
    let nametemp = if temporal == "" {
        vec![]
    } else {
        let temps: HashSet<&str> = temporal.split(",").collect();
        let widths = vec!["year", "month", "day", "hour", "minute", "second"];
        widths.into_iter().filter(|&x| temps.contains(x)).collect()
    };

    std::fs::metadata(dir)?
        .is_dir()
        .then_some(())
        .ok_or(anyhow!("not a dir {}", dir.display()))?;

    let mut pond = open()?;

    let ff = pond.start_resources()?;

    for pattern in patterns {
        let (path, glob) = Glob::new(&pattern)?.partition();
        if glob.has_semantic_literals() {
            return Err(anyhow!("glob not supported {}", pattern));
        }

        let fullglob = Glob::new(&pattern)?;
        let mut matches = 0;

        eprintln!("export {} ...", &pattern);

        let mut eouts = pond.visit_path(&path, &glob, &mut |wd: &mut WD,
                                                             ent: &DirEntry,
                                                             _: &Vec<String>|
         -> Result<
            Vec<(Vec<String>, ExportOutput)>,
        > {
            matches += 1;
            let pp = wd.pondpath(&ent.prefix);
            let mp = CandidatePath::from(pp.as_path());
            // TODO: This matched().expect() will fail when the pattern uses
            // a symlink. Bogus!
            let matched = fullglob.matched(&mp).expect("this already matched");
            let cap_cnt = fullglob.captures().count();
            let caps: Vec<String> = (1..=cap_cnt)
                .map(|x| matched.get(x).unwrap().to_string())
                .collect();

            let name = caps
                .iter()
                .fold(".".to_string(), |a, b| format!("{}/{}", a, b));

            // first, the case with no temporal subdivisions

            if nametemp.len() == 0 || !ent.ftype.is_relation() {
                let mut output = PathBuf::from(dir).join(&name);
                match ent.ftype {
                    FileType::Series | FileType::Table => output.add_extension("parquet"),
                    _ => true,
                };
                wd.copy_to(
                    ent,
                    &mut File::create(&output)
                        .with_context(|| format!("create {}", output.display()))?,
                )?;

                return Ok(vec![(
                    caps.clone(),
                    ExportOutput {
                        file: output,

                        // @@@ Set these to complete data set if table/series
                        start_time: None,
                        end_time: None,
                    },
                )]);
            }

            let output = PathBuf::from(dir).join(&name);
            std::fs::create_dir_all(&output)?;

            // Note: Extract into a parittioned hive-style database
            let qs = wd.sql_for(ent)?;

            let mut hs = "COPY (SELECT *".to_string();

            for part in &nametemp {
                hs = format!("{}, {}(Timestamp) AS {}", hs, part, part);
            }

            hs = format!(
                "{} FROM ({})) TO '{}' (FORMAT PARQUET, PARTITION_BY ({}), OVERWRITE)",
                hs,
                qs,
                output.display(),
                nametemp.join(",")
            );

            let conn = new_connection()?;
            conn.execute(&hs, [])
                .with_context(|| format!("can't prepare statement {}", &qs))?;

            let mut files = list_recursive(&output)?;
            files.sort();

            // Give the files a timestamp
            Ok(files
                .into_iter()
                .map(|fname| {
                    let subp = fname.strip_prefix(dir).unwrap();
                    let mut comps = subp.components();
                    // Skip the captured parameters.
                    for _ in &caps {
                        comps.next();
                    }

                    let mut mm = HashMap::from([
                        ("year", 0),
                        ("month", 1),
                        ("day", 1),
                        ("hour", 0),
                        ("minute", 0),
                        ("second", 0),
                    ]);

                    for &tname in nametemp.iter() {
                        let part = comps.next().unwrap().as_os_str().to_string_lossy();
                        let mut aeqb = part.split("=");
                        let a = aeqb.next().unwrap();
                        let b = aeqb.next().unwrap().parse::<i32>().unwrap();
                        assert_eq!(a, tname);
                        mm.insert(tname, b);
                    }

                    let start_time = build_utc(&mm);
                    *mm.get_mut(nametemp.last().unwrap()).unwrap() += 1;
                    let end_time = build_utc(&mm);

                    (
                        caps.clone(),
                        ExportOutput {
                            file: subp.into(),
                            start_time: Some(start_time),
                            end_time: Some(end_time),
                        },
                    )
                })
                .collect())
        })?;

        eprintln!("  matched {} files", matches);

        eouts.sort_by(|a, b| a.1.start_time.cmp(&b.1.start_time));

        if eouts.len() == 0 {
            continue;
        }

        // simple case w/ no wildcard, just reduce the vector
        let eset = ExportSet::construct(eouts);

        // Note: only one record will be available
        pond.expctx.insert("export", &eset);
    }

    pond.close_resources(ff)
}

pub fn foreach<F>(pattern: &str, f: &mut F) -> Result<()>
where
    F: FnMut(&mut WD, &DirEntry, &Vec<String>) -> Result<Vec<()>>,
{
    let (path, glob) = Glob::new(pattern)?.partition();
    if glob.has_semantic_literals() {
        return Err(anyhow!("glob not supported {}", pattern));
    }

    let mut pond = open()?;

    let ff = pond.start_resources()?;

    let _unused = pond.visit_path(&path, &glob, f)?;

    pond.close_resources(ff)
}

pub fn cat(path: String) -> Result<()> {
    let mut pond = open()?;

    let ff = pond.start_resources()?;

    let (dp, bn) = split_path(path)?;

    pond.in_path(dp, |wd| {
        let ent = wd.lookup(&bn).entry.ok_or(anyhow!(
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
        let ent = wd.lookup(name.as_str()).entry.unwrap();

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

pub fn sub_main_cmd<F, T: ForPond + ForArrow + for<'de> Deserialize<'de>>(
    pond: &mut Pond,
    uuidstr: &str,
    f: F,
) -> Result<()>
where
    F: Fn(&mut Pond, &mut UniqueSpec<T>) -> Result<()>,
{
    let kind = T::spec_kind();
    let specs: Vec<UniqueSpec<T>> = pond.in_path(&kind, |wd| wd.read_file(kind))?;
    let mut onespec: Vec<_> = specs
        .into_iter()
        .filter(|x| x.uuid.to_string() == *uuidstr)
        .collect();

    if onespec.len() == 0 {
        return Err(anyhow!("uuid not found {}", uuidstr));
    }
    let mut spec = onespec.remove(0);

    f(pond, &mut spec)
}

fn list_recursive(p: &PathBuf) -> Result<Vec<PathBuf>> {
    if !p.is_dir() {
        return Ok(vec![p.clone()]);
    }
    let mut r = vec![];
    for entry in std::fs::read_dir(p)? {
        let entry = entry?;
        let path = entry.path();
        r.extend(list_recursive(&path)?);
    }
    Ok(r)
}

fn build_utc(mm: &HashMap<&str, i32>) -> i64 {
    let mut year = mm["year"];
    let mut month = mm["month"] as u32;
    let day = mm["day"] as u32;
    let hour = mm["hour"] as u32;
    let minute = mm["minute"] as u32;
    let second = mm["second"] as u32;

    // Yuck special case @@@
    if month == 13 {
        year += 1;
        month = 1;
    }

    chrono::FixedOffset::west_opt(0)
        .unwrap()
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .unwrap()
        .timestamp()
}

impl ExportSet {
    fn construct(ins: Vec<(Vec<String>, ExportOutput)>) -> Self {
        let mut eset = ExportSet::Empty();

        for (caps, output) in ins {
            eset.insert(&caps, output);
        }

        eset
    }

    fn insert(&mut self, caps: &[String], output: ExportOutput) {
        if caps.len() == 0 {
            if let ExportSet::Empty() = self {
                *self = ExportSet::Files(vec![]);
            }
            if let ExportSet::Files(files) = self {
                files.push(output);
            }
            return;
        }

        if let ExportSet::Empty() = self {
            *self = ExportSet::Map(HashMap::new());
        }
        if let ExportSet::Map(map) = self {
            map.entry(caps[0].clone())
                .and_modify(|e| {
                    e.insert(&caps[1..], output.clone());
                })
                .or_insert_with(|| {
                    let mut x = ExportSet::Empty();
                    x.insert(&caps[1..], output.clone());
                    Box::new(x)
                });
        }
    }
}
