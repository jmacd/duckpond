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
use csv;
use wax::Glob;

use std::cell::RefCell;
use std::cmp::min;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
struct Target {
    glob: Glob<'static>,
    path: PathBuf,
}

#[derive(Debug)]
pub struct Collection {
    target: Rc<RefCell<Target>>,
    spec: DeriveCollection,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
    subs: BTreeMap<String, usize>,
}

#[derive(Debug)]
pub struct Set {
    target: Rc<RefCell<Target>>,
    spec: DeriveSet,
    relp: PathBuf,
}

#[derive(Debug)]
pub struct Xfer {
    dat: csv::Reader<File>,
    deq: VecDeque<u8>,
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
    let instance = Rc::new(RefCell::new(Module {}));
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
            spec,
            target: Rc::new(RefCell::new(target)),
            real: real.clone(),
            relp: relp.clone(),
            entry: entry.clone(),
            subs: BTreeMap::new(),
        }))))
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

impl TreeLike for Collection {
    fn subdir<'a>(&mut self, pond: &'a mut Pond, prefix: &str) -> Result<WD<'a>> {
        match self.spec.sets.iter().find(|x| x.name == prefix) {
            None => Err(anyhow!(
                "subdir not found: {}/{}",
                self.relp.display(),
                prefix,
            )),
            Some(set) => {
                let newid = *self.subs.entry(prefix.to_string()).or_insert_with(|| {
                    pond.insert(Rc::new(RefCell::new(Set {
                        target: self.target.clone(),
                        spec: set.clone(),
                        relp: self.relp.join(prefix),
                    })))
                });
                Ok(WD::new(pond, newid))
            }
        }
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

    fn entries(&mut self, _pond: &mut Pond) -> BTreeSet<DirEntry> {
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

impl TreeLike for Set {
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
        panic!("not realistic")
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
                let file = File::open(wd.realpath_current(prefix)?)?;
                let mut xfer = Xfer {
                    dat: csv::Reader::from_reader(file),
                    deq: VecDeque::new(),
                };
                {
                    let hdrs = xfer.dat.headers()?;
                    eprintln!("headers look {:?}", &hdrs);
                }
                Ok(Box::new(xfer))
            },
        )
    }

    fn realpath_version(
        &mut self,
        _pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
    ) -> PathBuf {
        panic!("not realistic {}", prefix)
    }

    fn sync(&mut self, _pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Err(anyhow!("no sync for subsynth"))
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
        Err(anyhow!("no update for synthetics"))
    }
}

impl Read for Xfer {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let mut copied = 0;
        while !self.deq.is_empty() && !buf.is_empty() {
            let c = self.deq.read(buf)?;
            buf = &mut buf[c..];
            copied += c;
        }
        let mut wtr = csv::Writer::from_writer(vec![]);
        while wtr.get_ref().len() < buf.len() {
            match self.dat.records().next() {
                Some(_d) => {
                    //eprintln!("I SEE {:?}", d?);

                    wtr.write_record(&["a", "b", "c"])?;
                }
                None => {
                    break;
                }
            }
        }
        let wb = wtr.into_inner().unwrap(); // @@@ ? error conversion from csv::Error to std::error::Error

        let tk = min(wb.len(), buf.len());
        buf[0..tk].clone_from_slice(&wb[0..tk]);
        copied += tk;
        self.deq.extend(wb[tk..].iter());
        Ok(copied)
    }
}
