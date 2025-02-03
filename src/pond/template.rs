use crate::pond::crd::TemplateCollection;
use crate::pond::crd::TemplateSpec;
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
use crate::pond::dir::Lookup;

use anyhow::{anyhow, Context, Result};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::Write;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::fs::File;
use tera::{Tera,Value,Error,from_value};
use std::sync::LazyLock;
use std::sync::Mutex;
use regex::Regex;
use serde_json::json;

static PLACEHOLDER: LazyLock<Mutex<Regex>> =
    LazyLock::new(|| Mutex::new(Regex::new(r#"\$(\d+)"#).unwrap()));

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Collection {
    target: Rc<RefCell<Target>>,
    name: String,
    out_pattern: String,
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
    uspec: &UniqueSpec<TemplateSpec>,
    _former: Option<UniqueSpec<TemplateSpec>>,
) -> Result<Option<InitContinuation>> {
    for coll in &uspec.spec.collections {
	check_patterns(coll)?;

	// Creates a file with the collection's name and saves its
	// spec as the contents of a SynTree file.
        let cv = vec![coll.clone()];
        wd.write_whole_file(&coll.name, FileType::SynTree, &cv)?;
    }
    Ok(None)
}

pub fn placeholder_regex() -> Result<Regex> {
    let guard = PLACEHOLDER.deref().lock();
    Ok(guard.unwrap().clone())
}

pub fn check_inout(in_pattern: &str, out_pattern: &str) -> Result<()> {
    // Parse in_pattern (a glob)
    let target = parse_glob(in_pattern)?;
    let tgt_cnt = target.glob.captures().count();

    // Parse out_pattern, check that each placeholder is viable.
    for cap in placeholder_regex()?.captures_iter(out_pattern) {
        let grp = cap.get(1).expect("regex group 1");
        let num: usize = grp.as_str().parse().expect("regexp placeholder");
	if num >= tgt_cnt {
	    return Err(anyhow!("pattern should have more wildcards: {}", num));
	}
    }

    Ok(())
}

fn check_patterns(coll: &TemplateCollection) -> Result<()> {
    check_inout(&coll.in_pattern, &coll.out_pattern)?;

    // Check that the template input is well formed.
    let mut tera = Tera::default();
    tera.add_raw_template(&coll.name, &coll.template)?;

    Ok(())
}

pub fn start(
    pond: &mut Pond,
    spec: &UniqueSpec<TemplateSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
    > {
    // Register this module for derived content.
    let instance = Rc::new(RefCell::new(Module {}));
    pond.register_deriver(spec.kind(), instance);
    start_noop(pond, spec)
}

pub fn run(_pond: &mut Pond, _uspec: &UniqueSpec<TemplateSpec>) -> Result<()> {
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
        let mut colls: Vec<TemplateCollection> = read_file(real)?;
        let spec = colls.remove(0);
        let target = parse_glob(&spec.in_pattern)?;

	let mut tera = Tera::default();
	tera.add_raw_template(&spec.name, &spec.template)?;
	tera.register_function("group", group);

        Ok(pond.insert(Rc::new(RefCell::new(Collection {
	    tera,
	    name: spec.name.clone(),
	    out_pattern: spec.out_pattern.clone(),
	    target: Rc::new(RefCell::new(target)),
	    real: real.clone(),
	    relp: relp.clone(),
	    entry: entry.clone(),
        }))))
    }

}

pub fn glob_placeholder(captures: &Vec<String>, pattern: &str) -> Result<String> {
    // compute name from out_pattern placeholders and captured text.
    let mut name = String::new();
    let mut start = 0;

    for placecap in placeholder_regex()?.captures_iter(pattern) {
	let g0 = placecap.get(0).ok_or(anyhow!("regex group 0"))?;
	let g1 = placecap.get(1).ok_or(anyhow!("regex group 1"))?;
	let num: usize = g1.as_str().parse()?;
	if num >= captures.len() {
	    return Err(anyhow!("too many placeholders"));
	}

	name.push_str(&pattern[start..g0.start()]);
	name.push_str(captures.get(num).ok_or(anyhow!("missing capture"))?);

	start = g0.end();
    }

    name.push_str(&pattern[start..pattern.len()]);
    return Ok(name)
}

impl TreeLike for Collection {
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

    fn entries_syn(&mut self, pond: &mut Pond) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        pond.visit_path(
            &self.target.deref().borrow().path,
            &self.target.deref().borrow().glob,
            &mut |_wd: &mut WD, _ent: &DirEntry, captures: &Vec<String>| {

		let name = glob_placeholder(captures, &self.out_pattern)?;
	
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
	    let item = d.lookup(&bn).entry.ok_or(anyhow!("reconstructed path not found {}", &rec))?;
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

	let mut ctx = pond.expctx.clone();
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

pub fn group(args: &HashMap<String, Value>) -> Result<Value, tera::Error> {
    let key = args.get("by").map(|x| from_value::<String>(x.clone())).ok_or_else(|| Error::msg("missing group-by key"))??;

    let obj = args.get("in").ok_or_else(|| Error::msg("expected a input value"))?;

    match obj {
	Value::Array(items) => {
	    let mut mapped = serde_json::Map::new();
	    // For each input item in an array
	    for item in items.into_iter() {
		match item.clone() {
		    // Expect the item is an object.
		    Value::Object(fields) => {
			
			// Expect the value is a string
			let idk = fields.get(&key).ok_or_else(|| Error::msg("expected a string-valued group"))?;
			let value = from_value::<String>(idk.clone())?;

			// Check mapped.get(value) 
			mapped.entry(&value)
			    .and_modify(|e| e.as_array_mut().expect("is an array").push(item.clone()))
			    .or_insert_with(|| json!(vec![item.clone()]));
		    },
		    _ => {
			return Err(Error::msg("cannot group non-object"));
		    },
		}
	    }
	    Ok(Value::Object(mapped))
	},
	_ => Err(Error::msg("cannot group non-array"))
    }
}

