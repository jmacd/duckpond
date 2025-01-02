use crate::pond::UniqueSpec;
use crate::pond::wd::WD;
use crate::pond::Pond;
use crate::pond::crd::ReduceSpec;
use crate::pond::crd::ReduceDataset;
use crate::pond::InitContinuation;
use crate::pond::dir::FileType;
use crate::pond::dir::DirEntry;
use crate::pond::dir::TreeLike;
use crate::pond::template::check_inout;
use crate::pond::derive::copy_parquet_to;
use crate::pond::start_noop;
use crate::pond::MultiWriter;
use crate::pond::Deriver;
use crate::pond::file::read_file;

use std::io::Write;
use std::rc::Rc;
use std::path::PathBuf;
use std::cell::RefCell;
use anyhow::{anyhow,Result};
use std::collections::BTreeSet;

#[derive(Debug)]
pub struct ModuleByRes {}

#[derive(Debug)]
pub struct ModuleByQuery {}

#[derive(Debug)]
pub struct ReduceByRes {
    dataset: ReduceDataset,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

#[derive(Debug)]
pub struct ReduceByQuery {
    dataset: ReduceDataset,
    resolution: String,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

// static PARTITIONS: [&'static str; 5_] = ["year",
// 			 "quarter",
// 			 "month",
// 			 "week",
// 			 "day"
// 			 ];

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<ReduceSpec>, _former: Option<UniqueSpec<ReduceSpec>>) -> Result<Option<InitContinuation>> {
    for ds in &uspec.spec.datasets {
	check_inout(&ds.in_pattern, &ds.out_pattern)?;

	// @@@ Check resolutions, queries

        let dv = vec![ds.clone()];
        wd.write_whole_file(&ds.name, FileType::SynTree, &dv)?;
    }

    Ok(None)
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<ReduceSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    pond.register_deriver(spec.kind(), 3, Rc::new(RefCell::new(ModuleByRes {})));
    pond.register_deriver(spec.kind(), 4, Rc::new(RefCell::new(ModuleByQuery {})));
    start_noop(pond, spec)
}

impl Deriver for ModuleByRes {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
        let ds: ReduceDataset = read_file(real)?.remove(0);
        Ok(pond.insert(Rc::new(RefCell::new(ReduceByRes {
            dataset: ds,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl TreeLike for ReduceByRes {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _prefix: &str, _parent_node: usize) -> Result<WD<'a>> {
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
        self.real.clone()
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

    fn entries(&mut self, _pond: &mut Pond) -> BTreeSet<DirEntry> {
	self.dataset.resolutions.iter().map(|x| DirEntry {
            prefix: format!("res={}", x),
            number: 0,
            ftype: FileType::SynTree,
            sha256: [0; 32],
            size: 0,
            content: None,
        }).collect()
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

impl Deriver for ModuleByQuery {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
        let ds: ReduceDataset = self.ds.clone();

        Ok(pond.insert(Rc::new(RefCell::new(ReduceByQuery {
            dataset: ds,
	    resolution: 0, // @@@ Need to get the parent dir's
	    // ReduceByRes object and extract res= from the path. Hmm.
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl TreeLike for ReduceByQuery {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _prefix: &str, _parent_node: usize) -> Result<WD<'a>> {
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
        self.real.clone()
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

    fn entries(&mut self, _pond: &mut Pond) -> BTreeSet<DirEntry> {
	vec![DirEntry {
            prefix: "reduce".to_string(),
            number: 0,
            ftype: FileType::SynTree,
            sha256: [0; 32],
            size: 0,
            content: None,
        }].into_iter().collect()
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


pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<ReduceSpec>) -> Result<()> {
    Ok(())
}
