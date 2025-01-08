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
use crate::pond::derive::Target;
use crate::pond::derive::parse_glob;
use crate::pond::template::glob_placeholder;
use crate::pond::start_noop;
use crate::pond::MultiWriter;
use crate::pond::Deriver;
use crate::pond::file::read_file;
use crate::pond::tmpfile;
use crate::pond::dir::Lookup;

use parse_duration::parse;
use std::io::Write;
use std::rc::Rc;
use std::fs::File;
use std::path::PathBuf;
use std::cell::RefCell;
use anyhow::{anyhow,Result,Context};
use std::collections::BTreeMap;
use std::time::Duration;
use std::ops::Deref;

#[derive(Debug)]
pub struct ModuleByRes {}

#[derive(Debug)]
pub struct ModuleByQuery {
    resolution: Duration,
    target: Rc<RefCell<Target>>,
    dataset: ReduceDataset,
}

#[derive(Debug)]
pub struct ReduceByRes {
    dataset: ReduceDataset,
    // parse and store resolutions, queries in infallable form
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

#[derive(Debug)]
pub struct ReduceByQuery {
    dataset: ReduceDataset,
    target: Rc<RefCell<Target>>,
    resolution: Duration,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<ReduceSpec>, _former: Option<UniqueSpec<ReduceSpec>>) -> Result<Option<InitContinuation>> {
    for ds in &uspec.spec.datasets {
	check_inout(&ds.in_pattern, &ds.out_pattern)?;

	// @@@ Check queries

	for res in &ds.resolutions {
	    parse(res)?;
	}

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
    pond.register_deriver(spec.kind(), Rc::new(RefCell::new(ModuleByRes {})));
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
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _lookup: &Lookup) -> Result<WD<'a>> {
        Err(anyhow!("no subdirs (A)"))
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

    fn entries_syn(&mut self, _pond: &mut Pond) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
	let target = Rc::new(RefCell::new(parse_glob(&self.dataset.out_pattern).unwrap()));

	self.dataset.resolutions.iter().map(|res| {
	    let dbox: Box<dyn Deriver + 'static> = Box::new(ModuleByQuery{
		resolution: parse(res).unwrap(),
		target: target.clone(),
		dataset: self.dataset.clone(),
	    });
	    let dder: std::option::Option<Rc<RefCell<Box<dyn Deriver>>>> =
		Some(Rc::new(RefCell::new(dbox)));

	    (DirEntry {
		prefix: format!("res={}", *res),
		number: 0,
		ftype: FileType::SynTree,
		sha256: [0; 32],
		size: 0,
		content: None,
            }, dder)
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
        Ok(pond.insert(Rc::new(RefCell::new(ReduceByQuery {
            dataset: self.dataset.clone(),
	    target: self.target.clone(),
	    resolution: self.resolution,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl TreeLike for ReduceByQuery {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _lookup: &Lookup) -> Result<WD<'a>> {
        Err(anyhow!("no subdirs (B)"))
	//Ok(lookup.deri.unwrap().deref().borrow_mut().open_derived(pond))
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

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        numf: i32,
        ext: &str,
        to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
	copy_parquet_to(self.sql_for_version(pond, prefix, numf, ext)?, to)
    }

    fn sql_for_version(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
    ) -> Result<String> {
	// This is gross! Repeat the visit call and find the match, b/c no
	// Deriver construct is passed through.
        let paths = pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |wd: &mut WD, ent: &DirEntry, captures: &Vec<String>| {

		let name = glob_placeholder(captures, &self.dataset.out_pattern)?;
		if name != prefix {
		    Ok(Vec::new())
		} else {
		    materialize_one_input(wd, ent, captures)
		}
	    })?;
	assert_eq!(0, paths.len());
	let path = paths.get(0).unwrap();

	Ok(format!("HEY read_parquet('{}') with DUR {:?}", path.display(), self.resolution))
	
	// select time_bucket('2 hours', epoch_ms(CAST("Timestamp"*1000 as BIGINT))) as twohours, avg("AT500_Bottom.DO.mg/L") from read_parquet('./tmp/combined-FieldStation.parquet') group by twohours order by twohours;
	
	//Ok(format!("select time_bucket('{}', epoch_ms(CAST(\"Timestamp\")*1000 as BIGINT)) as T
    }

    fn entries_syn(&mut self, pond: &mut Pond) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |_wd: &mut WD, _ent: &DirEntry, captures: &Vec<String>| {

		let name = glob_placeholder(captures, &self.dataset.out_pattern)?;
	
		let mut res = BTreeMap::new();
                res.insert(DirEntry {
                    prefix: name,
                    size: 0,
                    number: 1,
                    ftype: FileType::Data,
                    sha256: [0; 32],
                    content: None,
                }, None);
                Ok(res)
            },
        )
            .expect("otherwise nope")
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

fn materialize_one_input(wd: &mut WD, ent: &DirEntry, _: &Vec<String>) -> Result<Vec<PathBuf>> {
    // @@@ Copied from combine::materialize_all_inputs
    let mut fs = Vec::new();
    if let Some(item) = wd.lookup(&ent.prefix).entry {
        match wd.realpath(&item) {
	    None => {
                // Materialize the output.
                let tfn = tmpfile("parquet");
                let mut file = File::create(&tfn)
		    .with_context(|| format!("open {}", tfn.display()))?;
                wd.copy_to(&item, &mut file)?;
                fs.push(tfn);
	    }
	    Some(path) => {
                // Real file.
                fs.push(path);
	    }
        }
    };
    Ok(fs)
}    
