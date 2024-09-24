use crate::pond::crd::OverlayDirectory;
use crate::pond::crd::OverlaySpec;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::dir::TreeLike;
use crate::pond::file::read_file;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::Deriver;
use crate::pond::InitContinuation;
use crate::pond::MultiWriter;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Result};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Overlay {
    spec: OverlayDirectory,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<OverlaySpec>) -> Result<Option<InitContinuation>> {
    for over in &uspec.spec.overlay {
        let cv = vec![over.clone()];
        wd.write_whole_file(&over.name, FileType::SynTree, &cv)?;
    }
    Ok(None)
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<OverlaySpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    let instance = Rc::new(RefCell::new(Module {}));
    pond.register_deriver(spec.dirpath(), instance);
    start_noop(pond, spec)
}

impl Deriver for Module {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
        let mut overs: Vec<OverlayDirectory> = read_file(real)?;
        let spec = overs.remove(0);
        Ok(pond.insert(Rc::new(RefCell::new(Overlay {
            spec,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl TreeLike for Overlay {
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
        let mut res = BTreeSet::new();
        for s in &self.spec.series {
            let ent = pond.lookup(&s.path).unwrap();
            res.insert(DirEntry {
                prefix: ent.prefix,
                ftype: ent.ftype,
                content: None,
                size: 0,
                number: 0,
                sha256: [0; 32],
            });
        }
        res
    }

    fn copy_version_to<'a>(
        &mut self,
        _pond: &mut Pond,
        _prefix: &str,
        _numf: i32,
        _ext: &str,
        _to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
        Err(anyhow!("empty derived file lacks schema"))
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
