use crate::pond::crd::DeriveCollection;
use crate::pond::crd::DeriveSet;
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

use std::cell::RefCell;
use std::collections::BTreeMap;
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
    let instance = Box::new(Module {});
    pond.register_deriver(spec.dirpath(), instance);
    start_noop(pond, spec)
}

impl Deriver for Module {
    fn open_derived(
        &self,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<Rc<RefCell<dyn TreeLike>>> {
        let mut colls: Vec<DeriveCollection> = read_file(real)?;
        Ok(Rc::new(RefCell::new(Collection {
            spec: colls.remove(0),
            real: real.clone(),
            relp: relp.clone(),
            entry: entry.clone(),
            subs: BTreeMap::new(),
        })))
    }
}

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Collection {
    spec: DeriveCollection,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
    subs: BTreeMap<String, Rc<RefCell<dyn TreeLike>>>,
}

// Set {
//         spec: DeriveSet,
//     },
// }

impl TreeLike for Collection {
    fn subdir<'a, 'b, 'c: 'a>(
        &'a mut self,
        _pond: &mut Pond,
        prefix: &'b str,
    ) -> Result<Rc<RefCell<dyn TreeLike>>> {
        eprintln!("subdir call {}", prefix);
        Err(anyhow!("subdir not implemented"))
    }

    fn pondpath(&self, prefix: &str) -> PathBuf {
        if prefix.len() == 0 {
            self.relp.clone()
        } else {
            self.relp.clone().join(prefix)
        }
    }

    fn realpath_of(&self) -> PathBuf {
        self.real.clone() // @@@ Hmmm
    }

    fn realpath_version(&self, _prefix: &str, _numf: i32, _ext: &str) -> PathBuf {
        self.real.clone() // @@@ Hmmm
    }

    fn entries(&self) -> BTreeSet<DirEntry> {
        self.spec.sets.iter().map(|x| s2d(x)).collect()
    }

    fn sync(&mut self, _pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Ok((
            self.real.clone(),
            self.entry.number,
            self.entry.size as usize, // Q@@@: ((why u64 vs usize happening?))
            false,
        ))
    }

    fn lookup(&self, prefix: &str) -> Option<DirEntry> {
        for set in &self.spec.sets {
            if set.name == prefix {
                return Some(s2d(&set));
            }
        }
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
        Err(anyhow!("no update for synthetic trees"))
    }
}

fn s2d(x: &DeriveSet) -> DirEntry {
    DirEntry {
        prefix: x.name.clone(),
        size: 0,   // @@@
        number: 1, // @@@
        ftype: FileType::SynTree,
        sha256: [0; 32], // @@@
        content: None,
    }
}
