use crate::pond::crd::CombineScope;
use crate::pond::crd::CombineSeries;
use crate::pond::crd::CombineSpec;
use crate::pond::derive::copy_parquet_to;
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
use crate::pond::tmpfile;
use crate::pond::dir::Lookup;

use anyhow::{anyhow, Context, Result};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sea_query::{
    all, Alias, Asterisk, ColumnRef, CommonTableExpression, Expr, Func, Iden, Order, Query, SeaRc,
    SelectStatement, SqliteQueryBuilder, UnionType, WithClause,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Combine {
    series: Vec<CombineSeries>,
    columns: Option<Vec<String>>,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

#[derive(Iden)]
pub enum DuckFunc {
    ReadParquet,
    TimeBucket,
    EpochMs,
    Avg,
    Min,
    Max,
}

fn table(x: usize) -> Alias {
    Alias::new(format!("T{}", x))
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<CombineSpec>, _former: Option<UniqueSpec<CombineSpec>>) -> Result<Option<InitContinuation>> {
    for scope in &uspec.spec.scopes {
        for ser in &scope.series {
            parse_glob(&ser.pattern)?;
        }
        let scope1 = vec![scope.clone()];
        wd.write_whole_file(&scope.name, FileType::SynTree, &scope1)?;
    }
    Ok(None)
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<CombineSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    let instance = Rc::new(RefCell::new(Module {}));
    pond.register_deriver(spec.kind(), instance);
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
        let scope: CombineScope = read_file(real)?.remove(0);
        Ok(pond.insert(Rc::new(RefCell::new(Combine {
            series: scope.series,
            columns: scope.columns,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
        }))))
    }
}

impl TreeLike for Combine {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _lookup: &Lookup) -> Result<WD<'a>> {
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

    fn entries_syn(&mut self, _pond: &mut Pond) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        vec![DirEntry {
            prefix: "combine".to_string(),
            number: 0,
            ftype: FileType::Series,
            sha256: [0; 32],
            size: 0,
            content: None,
        }]
            .into_iter()
            .map(|x| (x, None))
            .collect()
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
        _prefix: &str,
        _numf: i32,
        _ext: &str,
    ) -> Result<String> {
    
        let conn = new_connection()?;
        let mut cnum: usize = 0;
        let mut tnum: usize = 0;

        let mut wc = WithClause::new();
        // Compute unique columns
	let mut fields_from: BTreeMap<String, BTreeSet<Alias>> = BTreeMap::new();
	
        for s in &self.series {
            // First, for each series in the scope, match the glob.
            let tgt = parse_glob(&s.pattern).unwrap();

            let fs = pond.visit_path(&tgt.path, &tgt.glob, &mut materialize_all_inputs)?;
	    
            let mut tree = BTreeMap::new();
            let mut allmax: i64 = 0;
 
            // In case there are no matches.
            if fs.len() == 0 {
                eprintln!("pattern '{}' matched no files -- skipping", &s.pattern);
                continue;
            }

            tnum += 1;
	    
            // For each file that matched, determine a min/max timestamp.
            for input in fs {
		// Compute the schema each time.
		let fh = File::open(&input)?;
		let pf = ParquetRecordBatchReaderBuilder::try_new(fh)?;

                for f in pf.schema().fields() {
                    if f.name().to_lowercase() == "timestamp" {
			continue;
		    }
		    fields_from.entry(f.name().to_string())
			.and_modify(|x| {
			    x.insert(table(tnum));
			})
			.or_insert(vec![table(tnum)].into_iter().collect());
		}
		
		drop(pf);

                let (mint, maxt): (i64, i64) = conn.query_row(
                    // TODO: Use sea-query here?
                    format!(
                        "SELECT MIN(Timestamp), MAX(Timestamp) FROM read_parquet('{}')",
                        input.display()
                    )
                    .as_str(),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                allmax = std::cmp::max(allmax, maxt);
                tree.insert((mint, maxt), input.clone());
            }

            // Build a select statement to join matching files with
            // a non-overlapping timeline.
            let mut qs: Option<SelectStatement> = None;
            let mut start: i64 = 0;

            for (ov, inp) in tree.iter() {
                if start >= ov.1 {
                    continue;
                }
                cnum += 1;
                let from = std::cmp::max(start, ov.0);

                let subq = Query::select()
                    .column(Asterisk)
                    .from_function(
                        Func::cust(DuckFunc::ReadParquet)
                            .arg(Expr::val(format!("{}", inp.display()))),
                        Alias::new(format!("IN{}", cnum)),
                    )
                    .cond_where(all![
                        Expr::col(Alias::new("Timestamp")).gte(Expr::val(from)),
                        Expr::col(Alias::new("Timestamp")).lt(Expr::val(ov.1)),
                    ])
                    .to_owned();

                match qs {
                    None => qs = Some(subq),
                    Some(q2) => {
                        qs = Some(q2.to_owned().union(UnionType::Distinct, subq).to_owned())
                    }
                }
                // eprintln!(
                //     "interval {:?}-{:?} => {}",
                //     Local.timestamp_opt(from, 0).unwrap(),
                //     Local.timestamp_opt(ov.1, 0).unwrap(),
                //     from,
                //     ov.1,
                //     inp.display(),
                // );
                start = ov.1;
            }

            wc.cte(
                CommonTableExpression::from_select(qs.expect("a query"))
                    .table_name(table(tnum))
                    .to_owned(),
            );
        }

        let mut select = SelectStatement::new();

        let select = if self.columns.is_some() {
            // User has named the columns
	    panic!("dead code path");

            // let mut cols: Vec<ColumnRef> = vec![ColumnRef::TableColumn(
            //     SeaRc::new(table(1)),
            //     SeaRc::new(Alias::new("Timestamp")),
            // )];
            // for cn in self.columns.as_ref().unwrap() {
            //     cols.push(ColumnRef::Column(SeaRc::new(Alias::new(cn))));
            // }
            // select.columns(cols)
        } else {
            select.column(ColumnRef::TableColumn(
                SeaRc::new(table(1)),
                SeaRc::new(Alias::new("Timestamp")),
            ));
	    // Note that this Coalesce option does not happen with the
	    // LT dataset because (I think) no instruments overlap in 
	    
            for (cn, als) in fields_from {
                if als.len() == 1 {
                    select.column(ColumnRef::Column(SeaRc::new(Alias::new(cn))));
                } else {
		    panic!("can't happen because UNION BY NAME")
                    // select.expr_as(
                    //     Func::cust(DuckFunc::Coalesce).args(als.iter().map(|x| {
                    //         SimpleExpr::Column(ColumnRef::TableColumn(
                    //             SeaRc::new(x.clone()),
                    //             SeaRc::new(Alias::new(cn.clone())),
                    //         ))
                    //     })),
                    //     Alias::new(cn),
                    // );
                }
            }
            &mut select
        };
        let mut select = select.from(table(1)).to_owned();

        for i in 2..=self.series.len() {
            let tl = table(i - 1);
            let tr = table(i);
            select = select
                .left_join(
                    tr.clone(),
                    Expr::col((tl, Alias::new("Timestamp")))
                        .equals((tr.clone(), Alias::new("Timestamp"))),
                )
                .to_owned();
        }

        let query = select
            .order_by((table(1), Alias::new("Timestamp")), Order::Asc)
            .to_owned()
            .with(wc)
            .to_string(SqliteQueryBuilder);

	// DuckDB's UNION BY NAME
	let query = query.replace("UNION", "UNION BY NAME");

        Ok(query)
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

fn materialize_all_inputs(wd: &mut WD, ent: &DirEntry, _: &Vec<String>) -> Result<Vec<PathBuf>> {
    // @@@ TODO: Would be nice to push sql_for_version() through so that materialize
    // is not needed, to use in-line MIN/MAX select statements for the time range,
    // and so on?
    let mut fs = Vec::new();
    for item in wd.lookup_all(&ent.prefix) {
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

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<CombineSpec>) -> Result<()> {
    Ok(())
}
