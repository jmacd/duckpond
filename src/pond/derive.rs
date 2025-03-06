use crate::pond::crd::DeriveCollection;
use crate::pond::crd::DeriveSpec;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::dir::Lookup;
use crate::pond::file::read_file;
use crate::pond::new_connection;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::Deriver;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::TreeLike;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Context, Result};
use duckdb::arrow::array::StructArray;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Statement;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use wax::Glob;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Target {
    pub glob: Glob<'static>,
    pub path: PathBuf,
    pub orig: String,
}

#[derive(Debug)]
pub struct Collection {
    target: Rc<RefCell<Target>>,
    query: String,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

struct DuckArrow<'conn> {
    stmt: Statement<'conn>,
}

pub fn init_func(
    wd: &mut WD,
    uspec: &UniqueSpec<DeriveSpec>,
    _former: Option<UniqueSpec<DeriveSpec>>,
) -> Result<Option<InitContinuation>> {
    for coll in &uspec.spec.collections {
        _ = parse_glob(&coll.pattern)?;

        let cv = vec![coll.clone()];
        wd.write_whole_file(&coll.name, FileType::SynTree, &cv)?;
    }
    Ok(None)
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<DeriveSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    let instance = Rc::new(RefCell::new(Module {}));
    pond.register_deriver(spec.kind(), instance);
    start_noop(pond, spec)
}

pub fn parse_glob<'a>(pattern: &str) -> Result<Target> {
    let (path, glob) = Glob::new(pattern)
        .with_context(|| format!("parsing {}", &pattern))?
        .into_owned()
        .partition();
    if glob.has_semantic_literals() {
        return Err(anyhow!("glob not supported {}", pattern));
    }
    Ok(Target {
        path,
        glob: glob.into_owned(),
        orig: pattern.to_string(),
    })
}

impl Target {
    pub fn reconstruct(&self, values: &Vec<String>) -> String {
        let mut r = String::new();
        let mut l = 0;
        // Note: the +1 below is because a / gets stripped in the
        // partition function.  This function is ugly!
        let off = self.path.as_os_str().len() + 1;
        for (cap, val) in self.glob.captures().zip(values.iter()) {
            let (start, end) = cap.span();
            r.push_str(&self.orig[l..start + off]);
            r.push_str(&val);
            l = off + start + end;
        }
        r.push_str(&self.orig[l..]);
        r
    }
}

impl Deriver for Module {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
        let mut colls: Vec<DeriveCollection> = read_file(real)?;
        let spec = colls.remove(0);
        let target = parse_glob(&spec.pattern)?;

        Ok(pond.insert(Rc::new(RefCell::new(Collection {
            query: spec.query,
            target: Rc::new(RefCell::new(target)),
            real: real.clone(),
            relp: relp.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl<'conn> Iterator for DuckArrow<'conn> {
    type Item = StructArray;

    fn next(&mut self) -> Option<StructArray> {
        Some(StructArray::from(self.stmt.step()?))
    }
}

impl TreeLike for Collection {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _lookup: &Lookup) -> Result<WD<'a>> {
        Err(anyhow!("no subdirs"))
    }

    fn pondpath(&self, prefix: &str) -> PathBuf {
        if prefix.is_empty() {
            self.relp.clone()
        } else {
            self.relp.clone().join(prefix)
        }
    }

    fn realpath_of(&self) -> PathBuf {
        self.real.clone() // @@@ Hmmm
    }

    fn realpath_version(
        &mut self,
        _pond: &mut Pond,
        _prefix: &str,
        _numf: i32,
        _ext: &str,
    ) -> Option<PathBuf> {
        None
    }

    fn entries_syn(
        &mut self,
        pond: &mut Pond,
    ) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |_wd: &mut WD, ent: &DirEntry, _: &Vec<String>| {
                let mut res = BTreeMap::new();
                res.insert(
                    DirEntry {
                        prefix: ent.prefix.clone(),
                        size: 0,
                        number: 1,
                        ftype: FileType::Series,

                        // @@@ Should describe source parent directory?
                        // Otherwise note intermediate directories could have
                        // matched, e.g., subdirs of the inbox.
                        sha256: [0; 32],
                        content: None,
                    },
                    None,
                );
                Ok(res)
            },
        )
        .expect("otherwise nope")
    }

    fn sql_for_version(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
    ) -> Result<String> {
        pond.in_path(&self.target.deref().borrow().path, |wd| -> Result<String> {
            Ok(self.query.replace(
                "$1",
                &wd.realpath_current(prefix)?
                    .expect("real file")
                    .to_string_lossy(),
            ))
        })
    }

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        numf: i32,
        ext: &str,
        to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
        let qs = self.sql_for_version(pond, prefix, numf, ext)?;

        copy_parquet_to(qs, to)
    }

    fn sync(&mut self, _pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Ok((
            self.real.clone(),
            self.entry.number,
            self.entry.size as usize, // Q@@@: ((why u64 vs usize happening?))
            false,
        ))
    }

    fn update(
        &mut self,
        _pond: &mut Pond,
        _prefix: &str,
        _newfile: &PathBuf,
        _seq: i32,
        _ftype: FileType,
        _row_cnt: Option<usize>,
    ) -> Result<()> {
        Err(anyhow!("no update for synthetic trees"))
    }
}

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<DeriveSpec>) -> Result<()> {
    Ok(())
}

pub fn copy_parquet_to<'a>(qs: String, to: Box<dyn Write + Send + 'a>) -> Result<()> {
    let conn = new_connection()?;
    let mut arrow = DuckArrow {
        stmt: conn
            .prepare(&qs)
            .with_context(|| format!("can't prepare statement {}", &qs))?,
    };
    arrow.stmt.execute([])?;

    match arrow.next() {
        Some(batch) => {
            let rb0: RecordBatch = batch.into();
            let mut writer = ArrowWriter::try_new(to, rb0.schema(), None)?;
            writer.write(&rb0)?;
            for batch in arrow {
                let rb_n: RecordBatch = batch.into();
                writer.write(&rb_n)?;
            }

            writer.close()?;
            Ok(())
        }
        None => Err(anyhow!("empty derived file lacks schema")),
    }
}
