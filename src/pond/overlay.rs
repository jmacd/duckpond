use crate::pond::crd::OverlaySeries;
use crate::pond::crd::OverlaySpec;
use crate::pond::derive::parse_glob;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::dir::TreeLike;
use crate::pond::file::read_file;
use crate::pond::new_connection;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::Deriver;
use crate::pond::InitContinuation;
use crate::pond::MultiWriter;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Context, Result};
use chrono::Local;
use chrono::TimeZone;
use rand::prelude::thread_rng;
use rand::Rng;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::env::temp_dir;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::rc::Rc;
use unbounded_interval_tree::interval_tree::IntervalTree;

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
        let mut fs: Vec<PathBuf> = vec![];
        for s in &self.series {
            let tgt = parse_glob(&s.pattern).unwrap();
            pond.visit_path(&tgt.path, &tgt.glob, &mut |wd: &mut WD, ent: &DirEntry| {
                match wd.realpath(ent) {
                    None => {
                        let tfn = self.tmpfile();
                        let mut file = File::create(&tfn)
                            .with_context(|| format!("open {}", tfn.display()))?;
                        wd.copy_to(ent, &mut file)?;
                        fs.push(tfn);
                    }
                    Some(path) => {
                        fs.push(path);
                    }
                }
                Ok(())
            })
            .unwrap();
        }
        // TODO: Could time ranges be stored as metadata on the nodes? then
        // no need to calculate.
        eprintln!("See inputs {:?}", fs);

        let mut tree = IntervalTree::default();
        let mut allmax: i64 = 0;

        let conn = new_connection()?;
        for input in fs {
            let (mint, maxt): (i64, i64) = conn.query_row(
                format!(
                    "SELECT MIN(Timestamp), MAX(Timestamp) FROM read_parquet('{}')",
                    input.display()
                )
                .as_str(),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            allmax = std::cmp::max(allmax, maxt);
            tree.insert((Included(mint), Included(maxt)));
            eprintln!(
                "res for {} is {}-{}",
                input.display(),
                Local.timestamp_micros(mint).unwrap(),
                Local.timestamp_micros(maxt).unwrap(),
            );
        }

        let overs = tree.get_interval_overlaps(&(0i64..allmax));
        eprintln!("overlaps {:?}", overs);

        for ov in overs {
            eprintln!("interval {:?}-{:?}", ov.0, ov.1);
        }

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
