use crate::pond::crd::ObservableCollection;
use crate::pond::crd::ObservableSpec;
use crate::pond::derive::parse_glob;
use crate::pond::derive::Target;
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
use crate::pond::split_path;
use crate::pond::tmpfile;

use anyhow::{anyhow, Context, Result};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::Write;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::fs::File;
use tera::Tera;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Collection {
    target: Rc<RefCell<Target>>,
    name: String,
    tera: Tera,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

#[derive(Debug,Serialize,Deserialize)]
pub struct Schema {
    fields: Vec<Field>,
}

#[derive(Debug,Serialize,Deserialize)]
pub struct Field {
    name: String,
    instrument: String,
    unit: String,
}

pub fn init_func(
    wd: &mut WD,
    uspec: &UniqueSpec<ObservableSpec>,
    _former: Option<UniqueSpec<ObservableSpec>>,
) -> Result<Option<InitContinuation>> {
    for coll in &uspec.spec.collections {
        let target = parse_glob(&coll.pattern)?;

	let cap_cnt = target.glob.captures().count();
	if cap_cnt != 1 {
	    return Err(anyhow!("pattern should have one wildcard"));
	}

	let mut tera = Tera::default();
	tera.add_raw_template(&coll.name, &coll.template)?;

	// Creates a file with the collection's name and saves its
	// spec as the contents of a SynTree file.
        let cv = vec![coll.clone()];
        wd.write_whole_file(&coll.name, FileType::SynTree, &cv)?;
    }
    Ok(None)
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<ObservableSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
    > {
    // Register this module for derived content.
    let instance = Rc::new(RefCell::new(Module {}));
    pond.register_deriver(spec.dirpath(), instance);
    start_noop(pond, spec)
}

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<ObservableSpec>) -> Result<()> {
    Ok(())
}

impl Deriver for Module {
    fn open_derived(
        &self,
        pond: &mut Pond,
        real: &PathBuf,
        relp: &PathBuf,
        entry: &DirEntry,
    ) -> Result<usize> {
	// Open the SynTree file of one collection, return the
	// object w/ parsed glob and prepared template.
        let mut colls: Vec<ObservableCollection> = read_file(real)?;
        let spec = colls.remove(0);
        let target = parse_glob(&spec.pattern)?;

	let mut tera = Tera::default();
	tera.add_raw_template(&spec.name, &spec.template)?;
	
        Ok(pond.insert(Rc::new(RefCell::new(Collection {
	    tera: tera,
	    name: spec.name.clone(),
	    target: Rc::new(RefCell::new(target)),
	    real: real.clone(),
	    relp: relp.clone(),
	    entry: entry.clone(),
        }))))
    }

}

impl TreeLike for Collection {
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

    fn entries(&mut self, pond: &mut Pond) -> BTreeSet<DirEntry> {
        let mut res = BTreeSet::new();
        pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |_wd: &mut WD, _ent: &DirEntry, captures: &Vec<String>| {

		let name = captures.get(0).unwrap().clone();

		assert_eq!(1, captures.len());  // len is checked in init_func
                res.insert(DirEntry {
                    prefix: format!("{}.md", name),
                    size: 0,
                    number: 1,
                    ftype: FileType::Data,
                    sha256: [0; 32],
                    content: None,
                });
                Ok(())
            },
        )
            .expect("otherwise nope");
        res
    }

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
       mut to:  Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
	let base = &prefix[..prefix.len()-3];

	// TODO This reconstruct logic is awful.  Would be nicer to pass a
	// more explicit reference or parameter through the DirEnt, maybe?
	let vals = vec![base.to_string()];
	let rec = self.target.deref().borrow().reconstruct(&vals);

	let (dp, bn) = split_path(&rec)?;

	// TODO: Note we're materializing files for which we could've stored
	// the schema.  This is not efficient!
	let mpath = pond.in_path(dp, |d| {
	    let item = d.lookup(&bn).ok_or(anyhow!("reconstructed path not found {}", &rec))?;
            match d.realpath(&item) {
		None => {
                    // Materialize the output.
                    let tfn = tmpfile("parquet");
                    let mut file = File::create(&tfn)
			.with_context(|| format!("open {}", tfn.display()))?;
                    d.copy_to(&item, &mut file)?;
                    Ok(tfn)
		}
		Some(path) => {
                    // Real file.
                    Ok(path)
		}
            }
	})?;

	let file = File::open(&mpath)?;
        let pf = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = pf.schema().clone();
        drop(pf);

	let mut sch = Schema{
	    fields: Vec::new(),
	};
        for f in schema.fields() {
            if f.name().to_lowercase() == "timestamp" {
                continue;
            }
	    let parts: Vec<&str> = f.name().split(".").collect();
	    if parts.len() != 3 {
		return Err(anyhow!("field name: unknown format: {}", f.name()));
	    }
	    sch.fields.push(Field{
		instrument: parts.get(0).unwrap().to_string(),
		name: parts.get(1).unwrap().to_string(),
		unit: parts.get(2).unwrap().to_string(),
	    });
        }

	let mut ctx = tera::Context::new();
        ctx.insert("schema", &sch);

	let rendered = self.tera.render(&self.name, &ctx).unwrap();

	to.write(rendered.as_bytes())?;
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
