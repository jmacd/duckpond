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

use anyhow::{anyhow, Context, Result}; use
parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder; use
parse_duration::parse; use sea_query::{ Asterisk, Alias, BinOper,
ColumnRef, Expr, Func, Order, Query, SeaRc, SimpleExpr,
SqliteQueryBuilder, WithClause, CommonTableExpression, UnionType, };
use std::cell::RefCell; use std::collections::BTreeMap; use
std::collections::BTreeSet; use std::fs::File; use std::io::Write; use
std::ops::Deref; use std::path::PathBuf; use std::rc::Rc; use
std::time::Duration;

/// ModuleLevel1 synthesizes each top-level data set.  This SynTree
/// contains an object for each in_pattern match.
pub struct ModuleLevel1 {}

/// ModuleLevel2 synthesizes a folder for every resolution.
pub struct ModuleLevel2 {
    boxed: Vec<Rc<RefCell<LazyMaterialize>>>,
    dataset: ReduceDataset,
}

/// LazyMaterialize defers materializing the input data until it is
/// read.
struct LazyMaterialize {
    captures: Vec<String>,
    materialize: Option<Box<dyn FnOnce(&mut Pond) -> PathBuf>>,
    path: Option<PathBuf>,
}

/// ReduceLevel1 corresponds with a single dataset.  Listing returns
/// one entry per in_pattern match.
pub struct ReduceLevel1 {
    dataset: ReduceDataset,
    ents: Vec<(String, Rc<RefCell<LazyMaterialize>>)>,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

/// ReduceLevel1 corresponds with a single match.  Listing returns one
/// entry per resolution.
pub struct ReduceLevel2 {
    dataset: ReduceDataset,
    lazy: Vec<Rc<RefCell<LazyMaterialize>>>,
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

        for res in &ds.resolutions {
            parse(res)?;
        }

        let dv = vec![ds.clone()];
        wd.write_whole_file(&ds.name, FileType::SynTree, &dv)?;
    }

    // TODO: Need a way to remove files to support the update
    // here. Remove entries in _former that are not in uspec.

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

                    Ok(vec![(name, Rc::new(RefCell::new(LazyMaterialize::new(captures.clone(), pbox))))])
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
	let folded: BTreeMap<String, Vec<_>> =
	    self.ents.iter().fold(
		BTreeMap::new(),
		|mut m, x| {
		    m.entry(x.0.clone()).and_modify(|v| {
			v.push(x.1.clone());
		    }).or_insert(vec![x.1.clone()]);
		    m
		});

	let mut res = BTreeMap::new();
        for (name, boxed) in folded {
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
        // Parse the prefix to recover res={} -- we avoid use of
        // a deriver here but note it's a redundant parse operation.
        let resolution = parse(&prefix[4..])?;

        let ops = vec![
            "avg".to_string(),
            "min".to_string(),
            "max".to_string(),
	];

	let mut wc = WithClause::new();
	let columns: BTreeSet<_> = self.dataset.columns.clone().map_or(
	    BTreeSet::new(),
	    |x| x.into_iter().collect());

	let mut allnames = Vec::new();

        for (idx, lazy) in self.lazy.iter().enumerate() {
	    let mut material = lazy.deref().borrow_mut();
	    let path = material.get(pond);
            let fh = File::open(&path)?;
            let pf = ParquetRecordBatchReaderBuilder::try_new(fh)?;
	    let schema = pf.schema().clone();
	    let prefix = material.captures.join(".");
	    
            let mut qs = Query::select()
		.expr_as(Expr::col((table(idx), Alias::new("Timestamp"))), Alias::new("RTimestamp"))
                .from_function(
                    Func::cust(DuckFunc::ReadParquet)
                        .arg(Expr::val(format!("{}", path.display()))),
                    table(idx),
                )
		.to_owned();

            for f in schema.fields() {
		let name = f.name();
                if name.to_lowercase() == "timestamp" {
                    continue;
                }
		if columns.len() > 0 && !columns.contains(name) {
		    continue;
		}

		qs = qs.expr_as(Expr::col(Alias::new(name)),
				Alias::new(format!("{}.{}", prefix, name)))
		    .to_owned();

		allnames.push(format!("{}.{}", prefix, name));
            }

            wc.cte(
                CommonTableExpression::from_select(qs)
                    .table_name(table(idx))
                    .to_owned(),
            );
        }

	// Build an ALL table.  Will replace "UNION" with "UNION BY NAME".
	let mut all = None;
	for i in 0..self.lazy.len() {
	    let part = Query::select().column(Asterisk).from(table(i)).to_owned();
	    if i == 0 {
		all = Some(part);
	    } else {
		all = Some(all.unwrap().union(UnionType::Distinct, part).to_owned())
	    }
	}
	wc.cte(
	    CommonTableExpression::from_select(all.unwrap())
		.table_name(Alias::new("ALL"))
		.to_owned(),
	);
	
        let mut query = Query::select()
            .expr_as(
                Func::cust(DuckFunc::TimeBucket)
                    .arg(Expr::custom_keyword(Alias::new(format!(
                        "INTERVAL {}",
                        res_to_sql_interval(resolution)
                    ))))
                    .arg(
                        Func::cust(DuckFunc::EpochMs).arg(
                            SimpleExpr::FunctionCall(Func::cast_as(
                                SimpleExpr::Column(ColumnRef::Column(SeaRc::new(Alias::new(
                                    "RTimestamp",
                                )))),
                                Alias::new("BIGINT"),
                            ))
                            .binary(BinOper::Mul, Expr::val(1000)),
                        ),
                    ),
                Alias::new("Timestamp"),
            )
            .to_owned();

	for name in &allnames {
            for op in &ops {
                let opexpr = match op.as_str() {
                    "avg" => Func::cust(DuckFunc::Avg),
                    "min" => Func::cust(DuckFunc::Min),
                    "max" => Func::cust(DuckFunc::Max),
                    _ => panic!("unchecked function"),
                };
                query = query
                    .expr_as(
                        opexpr.arg(SimpleExpr::Column(ColumnRef::Column(SeaRc::new(
                            Alias::new(name),
                        )))),
                        Alias::new(format!("{}.{}", name, op)),
                    )
                    .to_owned();
            }
	}

	let query = query
            .from(Alias::new("ALL"))
            .group_by_col(Alias::new("Timestamp"))
            .order_by(Alias::new("Timestamp"), Order::Asc)
            .to_owned()
            .with(wc)
	    .to_owned();

	let qstr = query.to_string(SqliteQueryBuilder);
	let qstr = qstr.replace("UNION", "UNION BY NAME");
        Ok(qstr)
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
    fn new(captures: Vec<String>, mf: Box<dyn FnOnce(&mut Pond) -> PathBuf>) -> Self {
        LazyMaterialize {
	    captures,
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

fn table(x: usize) -> Alias {
    Alias::new(format!("T{}", x))
}
