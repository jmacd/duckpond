use crate::pond::crd::DeriveCollection;
use crate::pond::crd::DeriveSpec;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::file::read_file;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::Deriver;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::TreeLike;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use wax::Glob;

#[derive(Debug)]
pub struct Module {
    conn: Rc<RefCell<Connection>>,
}

#[derive(Debug)]
struct Target {
    glob: Glob<'static>,
    path: PathBuf,
}

#[derive(Debug)]
pub struct Collection {
    target: Rc<RefCell<Target>>,
    conn: Rc<RefCell<Connection>>,
    query: String,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<DeriveSpec>) -> Result<Option<InitContinuation>> {
    for coll in &uspec.spec.collections {
        _ = parse_glob(coll.pattern.clone())?;

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
    let instance = Rc::new(RefCell::new(Module {
        conn: Rc::new(RefCell::new(Connection::open_in_memory()?)),
    }));
    pond.register_deriver(spec.dirpath(), instance);
    start_noop(pond, spec)
}

fn parse_glob<'a>(pattern: String) -> Result<Target> {
    let (path, glob) = Glob::new(&pattern)?.into_owned().partition();
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
        let target = parse_glob(spec.pattern.clone())?;

        Ok(pond.insert(Rc::new(RefCell::new(Collection {
            conn: self.conn.clone(),
            query: spec.query,
            target: Rc::new(RefCell::new(target)),
            real: real.clone(),
            relp: relp.clone(),
            entry: entry.clone(),
        }))))
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
    ) -> PathBuf {
        self.real.clone() // @@@ Hmmm
    }

    fn entries(&mut self, pond: &mut Pond) -> BTreeSet<DirEntry> {
        // TODO visit_path should return ::<T> ?
        let mut res = BTreeSet::new();
        //eprintln!("START DERIVE");
        pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |_wd: &mut WD, ent: &DirEntry| {
                //eprintln!("set match {}", &ent.prefix);
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

        //eprintln!("END DERIVE");
        res
    }

    fn open_version(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
    ) -> Result<Box<dyn Read>> {
        pond.in_path(
            &self.target.deref().borrow().path,
            |wd| -> Result<Box<dyn Read>> {
                let qs = self
                    .query
                    .replace("$1", &wd.realpath_current(prefix)?.to_string_lossy());
                let ps = self.conn.deref().borrow_mut().prepare(&qs)?;
                let ar = ps.query_arrow([])?;
                // So, like before:
                // move ps, ar into Xfer, which is a Read
                // Arrow batch write into a VecDeque
                // Read will read it.
                // The CAT program will learn to pretty-print series.
                // New trait method for Series to learn their time interval.
                // Code to remove overlaps.
                // Code to export!

                Err(anyhow!("nope"))
            },
        )
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
