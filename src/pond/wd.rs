use crate::pond::ForArrow;
use crate::pond::writer::MultiWriter;
use crate::pond::dir::Directory;
use crate::pond::dir::FileType;
use crate::pond::dir::DirEntry;
use crate::pond::dir;
use crate::pond::file;

use serde::{Serialize,Deserialize};

use std::fs::File;
use std::path::{Component,Path,PathBuf};

use anyhow::{Context, Result, anyhow};

use std::collections::{BTreeSet,BTreeMap};

use sha2::Digest;

#[derive(Debug)]
pub struct WD<'a> {
    pub w: &'a mut MultiWriter,
    pub d: &'a mut Directory,
}

impl <'a> WD <'a> {
    pub fn fullname(&self, entry: &DirEntry) -> PathBuf {
	self.d.relp.join(&entry.prefix)
    }

    pub fn unique(&mut self) -> BTreeSet<dir::DirEntry> {
	let mut sorted: BTreeMap<String, DirEntry> = BTreeMap::new();
	for ent in &self.d.ents {
	    if let Some(has) = sorted.get(&ent.prefix) {
		if has.number > ent.number {
		    continue
		}
	    }
	    sorted.insert(ent.prefix.clone(), ent.clone());
	}
	sorted.iter().map(|(_x, y)| y.clone()).collect()
    }

    pub fn in_path<P: AsRef<Path>, F, T>(&mut self, path: P, f: F) -> Result<T>
    where F: FnOnce(&mut WD) -> Result<T> {
	let mut comp = path.as_ref().components();
	let first = comp.next();

	match first {

	    None => {
		f(self)
	    },

	    Some(part) => {
		let one: String;
		if let Component::Normal(oss) = part {
		    one = oss.to_str().ok_or(anyhow!("invalid utf-8"))?.to_string();
		} else {
		    return Err(anyhow!("invalid path {:?}", part));
		}

		let od = self.d.subdirs.get_mut(&one);

		if let Some(d) = od {
		    let mut wd = WD{
			d: d,
			w: self.w,
		    };
		    return wd.in_path(comp.as_path(), f);
		}

		let mut wd = self.subdir(&one)?;

		wd.in_path(comp.as_path(), f)
	    }
	}
    }

    pub fn subdir(&mut self, prefix: &str) -> Result<WD> {
	let newpath = self.d.path.join(prefix);
	let newrelp = self.d.relp.join(prefix);

	match self.d.lookup(prefix) {
	    None => self.d.subdirs.insert(prefix.to_string(), dir::create_dir(newpath, newrelp)?),
	    Some(exists) => {
		if exists.ftype != FileType::Tree {
		    return Err(anyhow!("not a directory: {}", newrelp.display()));
		}		
		self.d.subdirs.insert(prefix.to_string(), dir::open_dir(newpath, newrelp)?)
	    },
	};
	
	let od = self.d.subdirs.get_mut(prefix);
	Ok(WD{
	    d: od.unwrap(),
	    w: self.w,
	})
    }
    
    pub fn read_file<T: for<'b> Deserialize<'b>>(&self, prefix: &str) -> Result<Vec<T>> {
	file::read_file(self.d.current_path_of(prefix)?)
    }

    pub fn current_path_of(&self, prefix: &str) -> Result<PathBuf> {
	self.d.current_path_of(prefix)
    }

    pub fn all_paths_of(&self, prefix: &str) -> Vec<PathBuf> {
	self.d.all_paths_of(prefix)
    }

    pub fn lookup(&self, prefix: &str) -> Option<DirEntry> {
	self.d.lookup(prefix)
    }

    pub fn prefix_num_path(&self, prefix: &str, num: i32, ext: &str) -> PathBuf {
	self.d.prefix_num_path(prefix, num, ext)
    }

    pub fn check(&mut self) -> Result<()> {
	let entries = std::fs::read_dir(&self.d.path)
	    .with_context(|| format!("could not read directory {}", self.d.path.display()))?;

	let mut prefix_idxs: BTreeMap<String, BTreeSet<i32>> = BTreeMap::new();

	for entry_r in entries {
            let entry = entry_r?;
	    let osname = entry.file_name();
	    let name = osname.into_string();
	    if let Err(_) = name {
		return Err(anyhow!("difficult to display an OS string! sorry!!"))
	    }
	    let name = name.unwrap();

	    if entry.file_type()?.is_dir() {
		self.in_path(name, |sub| sub.check())?;
		continue;
	    }

	    // TODO need to prohibit '.' from name prefix
	    let v: Vec<&str> = name.split('.').collect();

	    if v.len() != 3 {
		return Err(anyhow!("wrong number of parts: {}", name));
	    }
	    if *v[2] != *"parquet" {
		return Err(anyhow!("not a parquet file: {}", name));
	    }
	    let num = v[1].parse::<i32>()?;

	    match prefix_idxs.get_mut(v[0]) {
		Some(exist) => {
		    exist.insert(num);
		},
		None => {
		    let mut t: BTreeSet<i32> = BTreeSet::new();
		    t.insert(num);
		    prefix_idxs.insert(v[0].to_string(), t);
		},
	    }
	}

	for ent in &self.d.ents {
	    if let FileType::Tree = ent.ftype {
		continue;
	    }
	    // Build the set of existing verions by prefix
	    match prefix_idxs.get_mut(ent.prefix.as_str()) {
		Some(exist) => {
		    if let Some(_found) = exist.get(&ent.number) {
			exist.remove(&ent.number);
		    } else {
			return Err(anyhow!("prefix {} number {} is missing", ent.prefix, ent.number));
		    }
		},
		None => {
		    return Err(anyhow!("unknown prefix {} number {}", ent.prefix, ent.number));
		},
	    }
	    // Verify sha256 and size
	    let (hasher, size, _content) = file::sha256_file(self.d.prefix_num_path(ent.prefix.as_str(), ent.number, ent.ftype.ext()))?;

	    if size != ent.size {
		return Err(anyhow!("size mismatch {} (v{}): {} != {}", ent.prefix, ent.number, size, ent.size));
	    }
	    let sha: [u8; 32] = hasher.finalize().into();
	    if sha != ent.sha256 {
		return Err(anyhow!("sha256 mismatch {} (v{}): {} != {}", ent.prefix, ent.number, hex::encode(sha), hex::encode(ent.sha256)));
	    }
	}

	for leftover in &prefix_idxs {
	    if *leftover.0 == "dir".to_string() {
		// TODO: @@@ this is not finished.
		continue;
	    }
	    if leftover.1.len() != 0 {
		for idx in leftover.1.iter() {
		    eprintln!("unexpected file {}.{}.parquet", self.d.path.join(leftover.0).display(), idx);
		}
	    }
	}

	Ok(())
    }

    /// create_any_file is for ad-hoc structures
    pub fn create_any_file<F>(&mut self, prefix: &str, ftype: FileType, f: F) -> Result<()>
    where F: FnOnce(&File) -> Result<()> {
	let seq: i32;
	if let Some(cur) = self.d.lookup(prefix) {
	    seq = cur.number+1;
	} else {
	    seq = 1;
	}
	let newpath = self.d.prefix_num_path(prefix, seq, ftype.ext());
	let file = File::create_new(&newpath)
	    .with_context(|| format!("could not open {}", newpath.display()))?;
	f(&file)?;

	self.d.update(self.w, prefix, &newpath, seq, ftype, None)?;

	Ok(())
    }

    /// write_whole_file is for Serializable slices
    pub fn write_whole_file<T: Serialize + ForArrow>(
	&mut self,
	prefix: &str,
	ftype: FileType,
	records: &Vec<T>,
    ) -> Result<()> {
	let seq: i32;
	// Note: This uses a directory lookup to
	// determine if a file is present or not
	if let Some(cur) = self.lookup(prefix) {
	    seq = cur.number+1;
	} else {
	    seq = 1;
	}
	let newfile = self.d.prefix_num_path(prefix, seq, ftype.ext());
	let rlen = records.len();

	file::write_file(&newfile, records, T::for_arrow().as_slice())?;

	self.d.update(self.w, prefix, &newfile, seq, FileType::Table, Some(rlen))?;

	Ok(())
    }
}
