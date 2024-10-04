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

use anyhow::{anyhow, Context, Result};
use chrono::Local;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use rand::prelude::thread_rng;
use rand::Rng;
use sea_query::{
    all, Alias, Asterisk, CommonTableExpression, Expr, Func, Iden, Order, Query, SelectStatement,
    SqliteQueryBuilder, UnionType, WithClause,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env::temp_dir;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Combine {
    series: Vec<CombineSeries>,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
    tmp: PathBuf,
}

#[derive(Iden)]
enum DuckFunc {
    ReadParquet,
}

#[derive(Iden)]
enum PondColumn {
    Timestamp,
}

#[derive(Iden, Clone, Copy)]
enum TableNum {
    // I'm doing something wrong.  Why is the Iden type so difficult to make?
    T1 = 1,
    T2,
    T3,
    T4,
    T5,
    T6,
    T7,
    T8,
    T9,
    T10,
}

impl TableNum {
    fn get(x: usize) -> Option<TableNum> {
        match x {
            1 => Some(TableNum::T1),
            2 => Some(TableNum::T2),
            3 => Some(TableNum::T3),
            4 => Some(TableNum::T4),
            5 => Some(TableNum::T5),
            6 => Some(TableNum::T6),
            7 => Some(TableNum::T7),
            8 => Some(TableNum::T8),
            9 => Some(TableNum::T9),
            10 => Some(TableNum::T10),
            _ => None,
        }
    }
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<CombineSpec>) -> Result<Option<InitContinuation>> {
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
    spec: &UniqueSpec<CombineSpec>,
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
        Ok(pond.insert(Rc::new(RefCell::new(Combine {
            series: read_file(real)?,
            relp: relp.clone(),
            real: real.clone(),
            entry: entry.clone(),
            tmp: temp_dir(),
        }))))
    }
}

impl Combine {
    fn tmpfile(&self) -> PathBuf {
        let mut rng = thread_rng();
        let mut tmp = self.tmp.clone();
        tmp.push(format!("{}.parquet", rng.gen::<u64>()));
        tmp
    }
}

impl TreeLike for Combine {
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
            prefix: "combine".to_string(),
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
        to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
        let conn = new_connection()?;
        let mut cnum: usize = 0;
        let mut tnum: usize = 0;

        let mut wc = WithClause::new();

        for s in &self.series {
            let mut fs: Vec<PathBuf> = vec![];
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

            let mut tree = BTreeMap::new();
            let mut allmax: i64 = 0;

            for input in fs {
                let (mint, maxt): (i64, i64) = conn.query_row(
                    // TODO: Use sea-query here
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

            let mut qs: Option<SelectStatement> = None;
            let mut start: i64 = 0;

            for (ov, inp) in tree.iter() {
                if start >= ov.1 {
                    continue;
                }
                cnum += 1;
                let from = std::cmp::max(start, ov.0);
                let start_tm: NaiveDateTime = Local.timestamp_micros(from).unwrap().naive_utc();
                let finish_tm: NaiveDateTime = Local.timestamp_micros(ov.1).unwrap().naive_utc();
                let subq = Query::select()
                    .column(Asterisk)
                    .from_function(
                        Func::cust(DuckFunc::ReadParquet)
                            .arg(Expr::val(format!("{}", inp.display()))),
                        Alias::new(format!("IN{}", cnum)),
                    )
                    .cond_where(all![
                        Expr::col(PondColumn::Timestamp).gte(start_tm),
                        Expr::col(PondColumn::Timestamp).lt(finish_tm),
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
                //     Local.timestamp_micros(from).unwrap(),
                //     Local.timestamp_micros(ov.1).unwrap(),
                //     inp.display()
                // );
                start = ov.1;
            }

            tnum += 1;
            let table = TableNum::get(tnum).unwrap();
            wc.cte(
                CommonTableExpression::from_select(qs.expect("a query"))
                    .table_name(table)
                    .to_owned(),
            );
        }
        let mut select = SelectStatement::new()
            .column(Asterisk)
            .from(TableNum::T1)
            .to_owned();

        for i in 2..=self.series.len() {
            let tl = TableNum::get(i - 1).unwrap();
            let tr = TableNum::get(i).unwrap();
            select = select
                .left_join(
                    tr,
                    Expr::col((tl, PondColumn::Timestamp)).equals((tr, PondColumn::Timestamp)),
                )
                .to_owned();
        }

        let query = select
            .order_by((TableNum::T1, PondColumn::Timestamp), Order::Asc)
            .to_owned()
            .with(wc)
            .to_string(SqliteQueryBuilder);

        eprintln!("q {:?}", &query);

        copy_parquet_to(query, to)
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

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<CombineSpec>) -> Result<()> {
    Ok(())
}
