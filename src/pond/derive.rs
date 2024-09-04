use crate::pond::crd::DeriveCollection;
use crate::pond::crd::DeriveSet;
use crate::pond::crd::DeriveSpec;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::Deriver;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::TreeLike;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Result};

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<DeriveSpec>) -> Result<Option<InitContinuation>> {
    for coll in &uspec.spec.collections {
        eprintln!("Derive {} {}", coll.name, coll.pattern);

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
    let instance = Box::new(Module { spec: spec.clone() });
    pond.register_deriver(spec.dirpath(), instance);
    start_noop(pond, spec)
}

impl Deriver for Module {
    fn open_derived(&self, path: &PathBuf, entry: &DirEntry) -> Result<Rc<RefCell<dyn TreeLike>>> {
        for c in &self.spec.spec.collections {
            if c.name == entry.prefix {
                return Ok(Rc::new(RefCell::new(Synthetic::Collection(c.clone()))));
            }
        }
        Err(anyhow!("collection not found: {}", path.display()))
    }
}

#[derive(Debug)]
pub struct Module {
    spec: UniqueSpec<DeriveSpec>,
}

#[derive(Debug)]
pub enum Synthetic {
    Collection(DeriveCollection),
    Set(DeriveSet),
}

impl TreeLike for Synthetic {
    fn subdir<'a, 'b, 'c: 'a>(
        &'a mut self,
        _pond: &mut Pond,
        prefix: &'b str,
    ) -> Result<Rc<RefCell<dyn TreeLike>>> {
        eprintln!("subdir call {}", prefix);
        Err(anyhow!("subdir not implemented"))
    }

    fn pondpath(&self, _prefix: &str) -> PathBuf {
        PathBuf::new()
    }

    fn realpath_of(&self) -> PathBuf {
        PathBuf::new()
    }

    fn realpath_version(&self, _prefix: &str, _numf: i32, _ext: &str) -> PathBuf {
        PathBuf::new()
    }

    fn entries(&self) -> BTreeSet<DirEntry> {
        BTreeSet::new()
    }

    fn sync(&mut self, _writer: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Err(anyhow!("sync not implemented"))
    }

    fn lookup(&self, _prefix: &str) -> Option<DirEntry> {
        None
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
        Ok(())
    }
}
