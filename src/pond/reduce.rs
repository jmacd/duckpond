use crate::pond::combine::DuckFunc;
use crate::pond::crd::ReduceDataset;
use crate::pond::crd::ReduceSpec;
use crate::pond::derive::copy_parquet_to;
use crate::pond::derive::parse_glob;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::dir::Lookup;
use crate::pond::dir::TreeLike;
use crate::pond::file::read_file;
use crate::pond::split_path;
use crate::pond::start_noop;
use crate::pond::template::check_inout;
use crate::pond::template::glob_placeholder;
use crate::pond::tmpfile;
use crate::pond::wd::WD;
use crate::pond::Deriver;
use crate::pond::InitContinuation;
use crate::pond::MultiWriter;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Context, Result};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parse_duration::parse;
use sea_query::{
    Alias, BinOper, ColumnRef, Expr, Func, Order, Query, SeaRc, SimpleExpr, SqliteQueryBuilder,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

/// ModuleLevel1 synthesizes each top-level data set.  This SynTree
/// contains an object for each in_pattern match.
pub struct ModuleLevel1 {}

/// ModuleLevel2 synthesizes a folder for every resolution.
pub struct ModuleLevel2 {
    boxed: Rc<RefCell<LazyMaterialize>>,
    dataset: ReduceDataset,
}

/// LazyMaterialize defers materializing the input data until it is
/// read.
struct LazyMaterialize {
    materialize: Option<Box<dyn FnOnce(&mut Pond) -> PathBuf>>,
    path: Option<PathBuf>,
}

/// ReduceLevel1 corresponds with a single dataset.  Listing returns
/// one entry per in_pattern match.
pub struct ReduceLevel1 {
    dataset: ReduceDataset,
    ents: BTreeMap<String, Rc<RefCell<LazyMaterialize>>>,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

/// ReduceLevel1 corresponds with a single match.  Listing returns one
/// entry per resolution.
pub struct ReduceLevel2 {
    dataset: ReduceDataset,
    lazy: Rc<RefCell<LazyMaterialize>>,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

pub fn init_func(
    wd: &mut WD,
    uspec: &UniqueSpec<ReduceSpec>,
    _former: Option<UniqueSpec<ReduceSpec>>,
) -> Result<Option<InitContinuation>> {
    for ds in &uspec.spec.datasets {
        check_inout(&ds.in_pattern, &ds.out_pattern)?;

        for (_name, ops) in &ds.queries {
            for op in ops {
                match op.as_str() {
                    "avg" | "min" | "max" => {}
                    _ => {
                        return Err(anyhow!("unrecognized operator {}", op));
                    }
                }
            }
        }

        for res in &ds.resolutions {
            parse(res)?;
        }

        let dv = vec![ds.clone()];
        wd.write_whole_file(&ds.name, FileType::SynTree, &dv)?;
    }

    Ok(None)
}

/// start registers a top-level deriver for Reduce resources.
pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<ReduceSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    pond.register_deriver(spec.kind(), Rc::new(RefCell::new(ModuleLevel1 {})));
    start_noop(pond, spec)
}

impl Deriver for ModuleLevel1 {
    /// At the top level,
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
        let ds: ReduceDataset = read_file(real)?.remove(0);
        let target = parse_glob(&ds.in_pattern)?;

        // Here evaluate the input pattern and form the matching output
        // names.  Remember the full path (can't keep a `pond` reference)
        // of the input, box a function to materialize that path lazily.
        let ents = pond
            .visit_path(
                &target.path,
                &target.glob,
                &mut |wd: &mut WD, ent: &DirEntry, captures: &Vec<String>| {
                    let name = glob_placeholder(captures, &ds.out_pattern)?;
                    let fullp = wd.pondpath(&ent.prefix);

                    let pbox: Box<dyn FnOnce(&mut Pond) -> PathBuf> =
                        Box::new(|pond: &mut Pond| materialize_one_input(pond, fullp).unwrap());

                    let mut res = BTreeMap::new();

                    res.insert(name, Rc::new(RefCell::new(LazyMaterialize::new(pbox))));
                    Ok(res)
                },
            )
            .expect("otherwise nope");

        Ok(pond.insert(Rc::new(RefCell::new(ReduceLevel1 {
            dataset: ds,
            ents: ents,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl TreeLike for ReduceLevel1 {
    fn subdir<'a>(&mut self, pond: &'a mut Pond, lookup: &Lookup) -> Result<WD<'a>> {
        // Second-level lookup using the deriver returned from
        // first-level entries.
        let relp = self.pondpath(&lookup.prefix);
        let real = PathBuf::new();
        let entry = lookup.entry.clone().unwrap();
        let node = lookup
            .deri
            .as_ref()
            .unwrap()
            .deref()
            .borrow_mut()
            .open_derived(pond, &real, &relp, &entry)?;
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

    fn entries_syn(
        &mut self,
        _pond: &mut Pond,
    ) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        // For each entry, construct a deriver with a reference to the
        // the lazy materialize function.
        let mut res = BTreeMap::new();
        for (name, boxed) in &self.ents {
            let dbox: Box<dyn Deriver + 'static> = Box::new(ModuleLevel2 {
                boxed: boxed.clone(),
                dataset: self.dataset.clone(),
            });
            let dder: Rc<RefCell<Box<dyn Deriver>>> = Rc::new(RefCell::new(dbox));

            res.insert(
                DirEntry {
                    prefix: name.clone(),
                    size: 0,
                    number: 1,
                    ftype: FileType::SynTree,
                    sha256: [0; 32],
                    content: None,
                },
                Some(dder),
            );
        }
        res
    }

    fn sync(&mut self, _pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Ok((
            self.real.clone(),
            self.entry.number,
            self.entry.size as usize,
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

impl Deriver for ModuleLevel2 {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
        Ok(pond.insert(Rc::new(RefCell::new(ReduceLevel2 {
            dataset: self.dataset.clone(),
            lazy: self.boxed.clone(),
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
    return format!("{} seconds", dur.as_secs());
}

impl TreeLike for ReduceLevel2 {
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
        // Note that we materialize in order to get the schema.
        let fh = File::open(self.lazy.deref().borrow_mut().get(pond))?;
        let pf = ParquetRecordBatchReaderBuilder::try_new(fh)?;

        // Parse the prefix to recover res={} -- we avoid use of
        // a deriver here but note it's a redundant parse operation.
        let resolution = parse(&prefix[4..])?;

        let mut qs = Query::select()
            .expr_as(
                Func::cust(DuckFunc::TimeBucket)
                    .arg(Expr::custom_keyword(Alias::new(format!(
                        "INTERVAL {}",
                        res_to_sql_interval(resolution)
                    ))))
                    // Note! Some of the commented sections may instead
                    // work for the Combine table, but the Reduce table
                    // has a Timestamp type for its Timestamp column.
                    // .arg(SimpleExpr::Column(
                    // 	ColumnRef::Column(
                    // 	    SeaRc::new(Alias::new("Timestamp"))))),
                    // .arg(Func::cust(DuckFunc::EpochMs)
                    // 	 .arg(
                    // 		 SimpleExpr::Column(
                    // 		     ColumnRef::Column(
                    // 			 SeaRc::new(Alias::new("Timestamp"))))
                    // 	      .binary(BinOper::Mul, Expr::val(1000)))),
                    .arg(
                        Func::cust(DuckFunc::EpochMs).arg(
                            SimpleExpr::FunctionCall(Func::cast_as(
                                SimpleExpr::Column(ColumnRef::Column(SeaRc::new(Alias::new(
                                    "Timestamp",
                                )))),
                                Alias::new("BIGINT"),
                            ))
                            .binary(BinOper::Mul, Expr::val(1000)),
                        ),
                    ),
                Alias::new("RTimestamp"),
            )
            .to_owned();

        for f in pf.schema().fields() {
            if f.name().to_lowercase() == "timestamp" {
                continue;
            }

            let ops = self
                .dataset
                .queries
                .get(f.name())
                .cloned()
                .or_else(|| Some(vec!["avg".to_string()]))
                .unwrap();

            for op in &ops {
                let opexpr = match op.as_str() {
                    "avg" => Func::cust(DuckFunc::Avg),
                    "min" => Func::cust(DuckFunc::Min),
                    "max" => Func::cust(DuckFunc::Max),
                    _ => panic!("unchecked function"),
                };
                qs = qs
                    .expr_as(
                        opexpr.arg(SimpleExpr::Column(ColumnRef::Column(SeaRc::new(
                            Alias::new(f.name()),
                        )))),
                        Alias::new(format!("{}.{}", f.name(), op)),
                    )
                    .to_owned();
            }
        }

        qs = qs
            .from_function(
                Func::cust(DuckFunc::ReadParquet).arg(Expr::val(format!(
                    "{}",
                    self.lazy.deref().borrow_mut().get(pond).display()
                ))),
                Alias::new("IN".to_string()),
            )
            .group_by_col(Alias::new("RTimestamp"))
            .order_by(Alias::new("RTimestamp"), Order::Asc)
            .to_owned();

        Ok(qs.to_string(SqliteQueryBuilder))
    }

    fn entries_syn(
        &mut self,
        _pond: &mut Pond,
    ) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        self.dataset
            .resolutions
            .iter()
            .map(|res| {
                (
                    DirEntry {
                        prefix: format!("res={}", *res),
                        number: 0,
                        ftype: FileType::Series,
                        sha256: [0; 32],
                        size: 0,
                        content: None,
                    },
                    None,
                )
            })
            .collect()
    }

    fn sync(&mut self, _pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        Ok((
            self.real.clone(),
            self.entry.number,
            self.entry.size as usize,
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

fn materialize_one_input(pond: &mut Pond, fullp: PathBuf) -> Result<PathBuf> {
    // Note: Derived from combine::materialize_all_inputs
    let (dn, bn) = split_path(&fullp)?;

    pond.in_path(dn, |wd| {
        if let Some(item) = wd.lookup(&bn).entry {
            match wd.realpath(&item) {
                None => {
                    // Materialize the output.
                    let tfn = tmpfile("parquet");
                    let mut file =
                        File::create(&tfn).with_context(|| format!("open {}", tfn.display()))?;
                    wd.copy_to(&item, &mut file)?;
                    Ok(tfn)
                }
                Some(path) => {
                    // Real file.
                    Ok(path)
                }
            }
        } else {
            Err(anyhow!(
                "did not match materialize path {}",
                fullp.display()
            ))
        }
    })
}

impl LazyMaterialize {
    fn new(mf: Box<dyn FnOnce(&mut Pond) -> PathBuf>) -> Self {
        LazyMaterialize {
            materialize: Some(mf),
            path: None,
        }
    }

    fn get(&mut self, pond: &mut Pond) -> PathBuf {
        if self.path.is_some() {
            return self.path.clone().unwrap();
        }

        self.path = Some((self.materialize.take().unwrap())(pond));

        self.path.as_ref().unwrap().clone()
    }
}
