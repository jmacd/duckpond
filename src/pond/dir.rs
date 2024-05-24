use serde::{Serialize, Deserialize};
use hex;

use sha2::{Sha256, Digest};
use std::{io, fs};

use std::fs::File;
use std::path::{Path,PathBuf};
use crate::pond::file;
use anyhow::{Context, Result, anyhow};
use arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;
use std::collections::BTreeSet;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirEntry {
    prefix: String,
    number: i32,
    size: u64,
    is_dir: bool,

    //sha256: [u8; 32],
    sha256: String,
}

#[derive(Debug)]
pub struct Directory {
    path: PathBuf,
    ents: BTreeSet<DirEntry>,
    dirfnum: i32,
}

fn directory_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("prefix", DataType::Utf8, false)),
        Arc::new(Field::new("number", DataType::Int32, false)),
        Arc::new(Field::new("size", DataType::UInt64, false)),
        Arc::new(Field::new("is_dir", DataType::Boolean, false)),

	// "Error: Only primitive data types can be converted to T"
        //Arc::new(Field::new("sha256", DataType::FixedSizeBinary(32), false)),

	Arc::new(Field::new("sha256", DataType::Utf8, false)),
    ]
}

pub fn create_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {
    let path = path.as_ref();

    std::fs::create_dir(path)
	.with_context(|| "pond directory already exists")?;

    Ok(Directory{
	path: path.into(),
	ents: BTreeSet::new(),
	dirfnum: 0,
    })
}

pub fn open_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {

    let path = path.as_ref();

    let mut dirfnum: i32 = 0; // @@@

    let entries = std::fs::read_dir(path)
	.with_context(|| format!("could not read directory {}", path.display()))?;

    for entry_r in entries {
        let entry = entry_r?;
	let osname = entry.file_name();

	// @@@ not sure why not
	//let name = osname.into_string()?;
	//    .with_context(|| format!("file name is invalid utf8: {}", osname.to_utf8_lossy()))?;

	let name = osname.into_string();
	if let Err(_) = name {
	    return Err(anyhow!("difficult to display an OS string! sorry!!"))
	}
	let name = name.unwrap();

	if !name.starts_with("dir.") {
	    continue;
	}

	let numstr = name.trim_start_matches("dir.").trim_end_matches(".parquet");
	let num = numstr.parse::<i32>()?;
	dirfnum = std::cmp::max(dirfnum, num);
    }

    let mut d = Directory{
	ents: BTreeSet::new(),
	path: path.to_path_buf(),
	dirfnum: dirfnum,
    };

    let ents: Vec<DirEntry> = file::read_file(d.real_path_of(format!("dir.{}.parquet", dirfnum)))?;
    
    // @@@ not sure how to construct btreeset from iterator, &DirEntry vs DirEntry
    for ent in ents {
	d.ents.insert(ent);
    }

    Ok(d)
}

impl Directory {
    pub fn real_path_of<P: AsRef<Path>>(&self, base: P) -> PathBuf {
	self.path.clone().join(base)
    }

    pub fn current_path_of(&self, prefix: &str) -> Result<PathBuf> {
	if let Some(cur) = self.last_path_of(prefix) {
	    Ok(self.prefix_num_path(prefix, cur.number))
	} else {
	    Err(anyhow!("no current path"))
	}
    }

    pub fn next_path_of(&self, prefix: &str) -> PathBuf {
	if let Some(cur) = self.last_path_of(prefix) {
	    self.prefix_num_path(prefix, cur.number+1)
	} else {
	    self.prefix_num_path(prefix, 1)
	}
    }

    fn prefix_num_path(&self, prefix: &str, num: i32) -> PathBuf {
	self.real_path_of(format!("{}.{}.parquet", prefix, num))
    }

    pub fn last_path_of(&self, prefix: &str) -> Option<DirEntry> {
	self.ents 
	    .iter()
	    .filter(|x| x.prefix == prefix)
	    .reduce(|a, b| if a.number > b.number { a } else { b })
	    .cloned()
    }
    
    pub fn write_file<T: Serialize>(
	&mut self,
	prefix: &str,
	records: &Vec<T>,
	fields: &[Arc<Field>],
    ) -> Result<()> {
	let seq: i32;
	// Note: This uses a directory lookup to
	// determine if a file is present or not

	if let Some(cur) = self.last_path_of(prefix) {
	    seq = cur.number+1;
	} else {
	    seq = 1;
	}
	let newfile = self.prefix_num_path(prefix, seq);

	file::write_file(&newfile, records, fields)?;

	self.update(prefix, &newfile, seq)?;

	Ok(())
    }

    pub fn update<P: AsRef<Path>>(&mut self, prefix: &str, newfile: P, seq: i32) -> Result<()> {
	let mut hasher = Sha256::new();
	let mut file = fs::File::open(newfile)?;

	let bytes_written = io::copy(&mut file, &mut hasher)?;

	let digest = hasher.finalize();

	self.ents.insert(DirEntry{
	    prefix: prefix.to_string(),
	    number: seq,
	    size: bytes_written,
	    is_dir: false,
	    sha256: hex::encode(&digest),
	});
	Ok(())
    }

    pub fn read_file<T: for<'a> Deserialize<'a>>(&self, prefix: &str) -> Result<Vec<T>> {
	file::read_file(self.current_path_of(prefix)?)
    }
    
    pub fn close_dir(&mut self) -> Result<()> {
	let vents: Vec<DirEntry> = self.ents.iter().cloned().collect();

	self.dirfnum += 1;

	file::write_file(self.real_path_of(format!("dir.{}.parquet", self.dirfnum)), &vents, directory_fields().as_slice())
    }

    pub fn create_file<F>(&mut self, prefix: &str, f: F) -> Result<()>
    where F: FnOnce(&File) -> Result<()> {
	let seq: i32;
	if let Some(cur) = self.last_path_of(prefix) {
	    seq = cur.number+1
	} else {
	    seq = 1
	}
	let newpath = self.prefix_num_path(prefix, seq);
	let file = File::create_new(&newpath)
	    .with_context(|| format!("could not open {}", newpath.display()))?;
	f(&file)?;

	self.update(prefix, &newpath, seq)?;

	Ok(())
    }
}
