use crate::pond::crd::OverlaySeries;
use crate::pond::crd::OverlaySpec;
use crate::pond::derive::parse_glob;
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
use rand::prelude::thread_rng;
use rand::Rng;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::env::temp_dir;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Overlay {
    series: Vec<OverlaySeries>,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
    tmp: PathBuf,
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<OverlaySpec>) -> Result<Option<InitContinuation>> {
    for scope in &uspec.spec.scopes {
        for ser in &scope.series {
            parse_glob(&ser.pattern)?;
        }
        wd.write_whole_file(&scope.name, FileType::SynTree, &scope.series)?;
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
        Ok(pond.insert(Rc::new(RefCell::new(Overlay {
            series: read_file(real)?,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
            tmp: temp_dir(),
        }))))
    }
}

impl Overlay {
    fn tmpfile(&self) -> PathBuf {
        let mut rng = thread_rng();
        let mut tmp = self.tmp.clone();
        tmp.push(format!("{}.parquet", rng.gen::<u64>()));
        tmp
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
    ) -> Option<PathBuf> {
        None
    }

    fn entries(&mut self, _pond: &mut Pond) -> BTreeSet<DirEntry> {
        vec![DirEntry {
            prefix: "overlay".to_string(),
            number: 0,
            ftype: FileType::Series,
            sha256: [0; 32],
            size: 0,
            content: None,
        }]
        .into_iter()
        .collect()
    }

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        _prefix: &str,
        _numf: i32,
        _ext: &str,
        _to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
        // TODO: Use a two pass algorithm.
        // 1. get schemas, join them; get time ranges, eliminate gaps
        // 2. read combined, non-overlapping
        // With duckdb?
        let mut fs: Vec<PathBuf> = vec![];
        for s in &self.series {
            let tgt = parse_glob(&s.pattern).unwrap();
            pond.visit_path(&tgt.path, &tgt.glob, &mut |wd: &mut WD, ent: &DirEntry| {
                match wd.realpath(ent) {
                    None => {
                        let tfn = self.tmpfile();
                        let mut file = File::open(&tfn)?;
                        wd.copy_to(ent, &mut file)?;
                        fs.push(tfn);
                    }
                    Some(path) => {
                        fs.push(path);
                    }
                }

                eprintln!("VISIT {} for {}", wd.pondpath("").display(), ent.prefix);
                Ok(())
            })
            .unwrap();
        }
        eprintln!("See inputs {:?}", fs);
        Ok(())
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

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<OverlaySpec>) -> Result<()> {
    Ok(())
}
