use crate::pond::crd::DeriveSpec;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::TreeLike;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Result};

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;

pub fn init_func(
    _wd: &mut WD,
    _uspec: &UniqueSpec<DeriveSpec>,
) -> Result<Option<InitContinuation>> {
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
    start_noop(pond, spec)
}

#[derive(Debug)]
pub struct Derived {
    ents: BTreeSet<DirEntry>,
}

impl TreeLike for Derived {
    fn pondpath(&self, _prefix: &str) -> PathBuf {
        PathBuf::new()
    }

    fn realpath_of(&self) -> PathBuf {
        PathBuf::new()
    }

    fn realpath_version(&self, _prefix: &str, _numf: i32, _ext: &str) -> PathBuf {
        PathBuf::new()
    }

    fn entries(&self) -> &BTreeSet<DirEntry> {
        &self.ents
    }

    fn sync(&mut self, _writer: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Err(anyhow!("not implemented"))
    }

    fn subdir<'a, 'b, 'c: 'a>(&'a mut self, _prefix: &'b str) -> Result<Rc<RefCell<dyn TreeLike>>> {
        Err(anyhow!("not implemented"))
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
