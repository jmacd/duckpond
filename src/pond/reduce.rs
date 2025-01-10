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
use crate::pond::combine::DuckFunc;

use anyhow::{anyhow,Result,Context};
use parse_duration::parse;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sea_query::{
    Alias, BinOper, ColumnRef, Expr, Func, Order,
    Query, SeaRc, SimpleExpr, SqliteQueryBuilder,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

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
    fn subdir<'a>(&mut self, pond: &'a mut Pond, lookup: &Lookup) -> Result<WD<'a>> {
	let relp = self.pondpath(&lookup.prefix);
	let real = PathBuf::new();
	let entry = lookup.entry.clone().unwrap();
	let node = lookup.deri.as_ref().unwrap().deref().borrow_mut()
		   .open_derived(pond,
				 &real,
				 &relp,
				 &entry)?;
        Ok(WD::new(pond, node))
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
	let target = Rc::new(RefCell::new(parse_glob(&self.dataset.in_pattern).unwrap()));

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

fn res_to_sql_interval(dur: Duration) -> String {
    if dur >= Duration::from_days(365) {
	return "1 year".to_string();
    } else if dur >= Duration::from_days(90) {
	return "1 quarter".to_string();
    } else if dur >= Duration::from_days(30) {
	return "1 month".to_string();
    } else if dur >= Duration::from_days(7) {
	return "1 week".to_string();
    } else if dur >= Duration::from_days(1) {
	return "1 day".to_string();
    }

    let hrs = dur.as_secs() / 3600;
    if hrs >= 1 {
	return format!("{} hours", hrs);
    }
    let mins = dur.as_secs() / 60;
    if mins >= 1 {
	return format!("{} minutes", mins);
    }
    return format!("{} seconds", dur.as_secs())
}

impl TreeLike for ReduceByQuery {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _lookup: &Lookup) -> Result<WD<'a>> {
        Err(anyhow!("no subdirs (B)"))
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
	assert_eq!(1, paths.len());
	let path = paths.get(0).unwrap();

	let fh = File::open(&path)?;
	let pf = ParquetRecordBatchReaderBuilder::try_new(fh)?;

	let mut qs = Query::select()
	    .expr_as(
                Func::cust(DuckFunc::TimeBucket)
                    .arg(Expr::val(res_to_sql_interval(self.resolution)))
                    .arg(Func::cust(DuckFunc::EpochMs)
			 .arg(SimpleExpr::FunctionCall(
			     Func::cast_as(
				 SimpleExpr::Column(
				     ColumnRef::Column(
					 SeaRc::new(Alias::new("Timestamp")))),
				 Alias::new("BIGINT")))
			      .binary(BinOper::Mul, Expr::val(1000)))),
		Alias::new("T"))
	    .to_owned();

        for f in pf.schema().fields() {
            if f.name().to_lowercase() == "timestamp" {
		continue;
	    }

	    // TODO
	    // if let Some(ops) = self.dataset.queries.get(f.name()) {}
	    qs = qs.expr_as(
		Func::cust(DuckFunc::Avg)
		    .arg(
			SimpleExpr::Column(
			    ColumnRef::Column(
				SeaRc::new(Alias::new(f.name()))))),
		Alias::new(format!("{}_avg", f.name())),
	    ).to_owned();
	}

	qs = qs
            .from_function(
                Func::cust(DuckFunc::ReadParquet)
                    .arg(Expr::val(format!("{}", path.display()))),
                Alias::new("IN".to_string()),
	    )
                   .group_by_col(Alias::new("T"))
            .order_by(Alias::new("T"), Order::Asc)
            .to_owned();

	Ok(qs.to_string(SqliteQueryBuilder))
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
