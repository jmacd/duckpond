use crate::pond::crd::DeriveCollection;
use crate::pond::crd::DeriveSpec;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
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
use arrow::record_batch::RecordBatch;
use duckdb::Statement;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::cell::RefCell;
use std::collections::BTreeSet;
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

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<DeriveSpec>) -> Result<Option<InitContinuation>> {
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
    pond.register_deriver(spec.dirpath(), instance);
    start_noop(pond, spec)
}

pub fn parse_glob<'a>(pattern: &str) -> Result<Target> {
    let (path, glob) = Glob::new(pattern)?.into_owned().partition();
    if glob.has_semantic_literals() {
        return Err(anyhow!("glob not supported {}", pattern));
    }
    Ok(Target {
        path,
        glob: glob.into_owned(),
    })
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
    type Item = RecordBatch;

    fn next(&mut self) -> Option<RecordBatch> {
        Some(RecordBatch::from(self.stmt.step()?))
    }
}

impl TreeLike for Collection {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _prefix: &str) -> Result<WD<'a>> {
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

    fn entries(&mut self, pond: &mut Pond) -> BTreeSet<DirEntry> {
        // TODO visit_path should return ::<T> ?
        let mut res = BTreeSet::new();
        pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |_wd: &mut WD, ent: &DirEntry| {
                res.insert(DirEntry {
                    prefix: ent.prefix.clone(),
                    size: 0,
                    number: 1,
                    ftype: FileType::Series,

                    // @@@ Should describe source parent directory?
                    // Otherwise note intermediate directories could have
                    // matched, e.g., subdirs of the inbox.
                    sha256: [0; 32],
                    content: None,
                });
                Ok(())
            },
        )
        .expect("otherwise nope");
        res
    }

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
        to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
        pond.in_path(&self.target.deref().borrow().path, |wd| -> Result<()> {
            let qs = self.query.replace(
                "$1",
                &wd.realpath_current(prefix)?
                    .expect("real file")
                    .to_string_lossy(),
            );

            let conn = new_connection()?;
            let mut arrow = DuckArrow {
                stmt: conn
                    .prepare(&qs)
                    .with_context(|| "can't prepare statement")?,
            };
            arrow.stmt.execute([])?;

            match arrow.next() {
                Some(batch) => {
                    let mut writer = ArrowWriter::try_new(to, batch.schema(), None)?;
                    writer.write(&batch)?;
                    for batch in arrow {
                        writer.write(&batch)?;
                    }

                    writer.close()?;
                    Ok(())
                }
                None => Err(anyhow!("empty derived file lacks schema")),
            }
        })
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

// e.g.

// WITH INPUT as
//  (SELECT
//   Timestamp as T,
//   "Series Name" as SN,
//   Location as L,
//   Parameter as P,
//   Value as V,
//   "Offset" as O
//   FROM read_csv('$1')
//  )

// SELECT

// I1.T as "Timestamp",
// I1.V as "Surface pH",
// I2.V as "Surface Temp",
// I3.V as "Surface Chl-a",
// I4.V as "Surface DO",
// I5.V as "Surface Salinity",
// (I6.V - I6.O) as "Tide"

// FROM INPUT as I1
// INNER JOIN INPUT as I2 on I1.T = I2.T
// INNER JOIN INPUT as I3 on I1.T = I3.T
// INNER JOIN INPUT as I4 on I1.T = I4.T
// INNER JOIN INPUT as I5 on I1.T = I5.T
// INNER JOIN INPUT as I6 on I1.T = I6.T

// WHERE

// I1.SN = 'Surface pH' AND
// I2.SN = 'Surface Temp' AND
// I3.SN = 'Surface Chl-a' AND
// I4.SN = 'Surface DO' AND
// I5.SN = 'Surface Salinity' AND
// I6.SN = 'Depth'

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<DeriveSpec>) -> Result<()> {
    Ok(())
}
