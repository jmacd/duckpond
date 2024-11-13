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

use anyhow::{anyhow, Result};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use tera::Tera;

#[derive(Debug)]
pub struct Module {}

#[derive(Debug)]
pub struct Collection {
    target: Rc<RefCell<Target>>,
    tera: Tera,
    real: PathBuf,
    relp: PathBuf,
    entry: DirEntry,
}

#[derive(Debug)]
pub struct DataSet {
    relp: PathBuf,
    parent: usize,
}

pub fn init_func(
    wd: &mut WD,
    uspec: &UniqueSpec<ObservableSpec>,
) -> Result<Option<InitContinuation>> {
    for coll in &uspec.spec.collections {
        let target = parse_glob(&coll.pattern)?;

	let cap_cnt = target.glob.captures().count();
	if cap_cnt != 1 {
	    return Err(anyhow!("pattern should have one wildcard"));
	}

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
	let fname = format!("{}.html", &spec.name);
	tera.add_raw_template(&fname, &spec.template)?;
	
        Ok(pond.insert(Rc::new(RefCell::new(Collection {
	    tera: tera,
	    target: Rc::new(RefCell::new(target)),
	    real: real.clone(),
	    relp: relp.clone(),
	    entry: entry.clone(),
        }))))
    }

}

impl TreeLike for Collection {
    fn subdir<'a>(&mut self, pond: &'a mut Pond, prefix: &str, parent_node: usize) -> Result<WD<'a>> {
	let node = pond.insert(Rc::new(RefCell::new(DataSet {
	    relp: self.relp.join(prefix),
	    parent: parent_node,
	})));
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
        _pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
        _to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
	eprintln!("asked to read {}", prefix);
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

impl TreeLike for DataSet {
    fn subdir<'a>(&mut self, _pond: &'a mut Pond, _prefix: &str, _parent_node: usize) -> Result<WD<'a>> {
        Err(anyhow!("datasets have no subdirs"))
    }

    fn pondpath(&self, prefix: &str) -> PathBuf {
        if prefix.is_empty() {
            self.relp.clone()
        } else {
            self.relp.clone().join(prefix)
        }
    }

    fn realpath_of(&self) -> PathBuf {
        panic!("impossible");
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
	panic!("impossible");
    }

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        _numf: i32,
        _ext: &str,
        _to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
	let base = &prefix[..prefix.len()-3];
	let par = pond.get(self.parent);
	let par_ref = par.deref().borrow();
	let par_any = &*par_ref as &dyn std::any::Any;
	eprintln!("GRR asked to read {} !!", base);
	match par_any.downcast_ref::<Collection>() {
	    Some(coll) => {
		let vals = vec![base.to_string()];
		let rec = coll.target.deref().borrow().reconstruct(&vals);
		eprintln!("yes yes {}", rec);
	    },
	    None => {
		eprintln!("nope");
	    },
	}
	eprintln!("byee");
	Ok(())
    }

    fn sync(&mut self, _pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
	panic!("not possible");
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
