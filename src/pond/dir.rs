use crate::pond::file;
use crate::pond::writer::Writer;

use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};

use hex;
use sha2::{Sha256, Digest};
use std::fs;
use std::fs::File;
use std::io;

use std::path::{Path,PathBuf};
use anyhow::{Context, Result, anyhow};
use arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;
use std::collections::BTreeSet;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirEntry {
    pub prefix: String,
    pub number: i32,
    pub size: u64,
    pub ftype: FileType,

    // Note: sha256 should be fixed-size bytes, but serde_arrow does not
    // support.  Consider not using serde_arrow.
    //
    // "Error: Only primitive data types can be converted to T"
    //
    // Arc::new(Field::new("sha256", DataType::FixedSizeBinary(32), false)),
    // sha256: [u8; 32],

    pub sha256: String,

    //@@@
    pub content: Option<Vec<u8>>,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum FileType {
    Tree = 1,   // Directory structure
    Table = 2,  // One-shot table
    Series = 3, // Multi-part table
}


#[derive(Debug)]
pub struct Directory {
    pub path: PathBuf,
    pub ents: BTreeSet<DirEntry>,
    pub subdirs: BTreeMap<String, Directory>,
    pub dirfnum: i32,
}

fn directory_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("prefix", DataType::Utf8, false)),
        Arc::new(Field::new("number", DataType::Int32, false)),
        Arc::new(Field::new("size", DataType::UInt64, false)),
        Arc::new(Field::new("ftype", DataType::Int32, false)),
	Arc::new(Field::new("sha256", DataType::Utf8, false)),
	Arc::new(Field::new("contents", DataType::LargeBinary, true)),
    ]
}

pub fn create_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {
    let path = path.as_ref();

    fs::create_dir(path)
	.with_context(|| "pond directory already exists")?;

    Ok(Directory{
	path: path.into(),
	ents: BTreeSet::new(),
	subdirs: BTreeMap::new(),
	dirfnum: 0,
    })
}

pub fn open_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {

    let path = path.as_ref();

    let mut dirfnum: i32 = 0; // @@@

    let entries = fs::read_dir(path)
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
	subdirs: BTreeMap::new(),
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
    pub fn self_path(&self) -> PathBuf {
	self.path.to_path_buf()
    }

    pub fn real_path_of<P: AsRef<Path>>(&self, base: P) -> PathBuf {
	self.path.clone().join(base)
    }

    pub fn current_path_of(&self, prefix: &str) -> Result<PathBuf> {
	if let Some(cur) = self.last_path_of(prefix) {
	    Ok(self.prefix_num_path(prefix, cur.number))
	} else {
	    Err(anyhow!("no current path: {} in {}", prefix, self.path.display()))
	}
    }

    pub fn next_path_of(&self, prefix: &str) -> PathBuf {
	if let Some(cur) = self.last_path_of(prefix) {
	    self.prefix_num_path(prefix, cur.number+1)
	} else {
	    self.prefix_num_path(prefix, 1)
	}
    }

    pub fn prefix_num_path(&self, prefix: &str, num: i32) -> PathBuf {
	self.real_path_of(format!("{}.{}.parquet", prefix, num))
    }

    pub fn last_path_of(&self, prefix: &str) -> Option<DirEntry> {
	self.ents 
	    .iter()
	    .filter(|x| x.prefix == prefix)
	    .reduce(|a, b| if a.number > b.number { a } else { b })
	    .cloned()
    }

    pub fn all_paths_of(&self, prefix: &str) -> Vec<PathBuf> {
	self.ents 
	    .iter()
	    .filter(|x| x.prefix == prefix)
	    .map(|x| self.real_path_of(format!("{}.{}.parquet", x.prefix, x.number)))
	    .collect()
    }

    pub fn update<P: AsRef<Path>>(&mut self, writer: &mut Writer, prefix: &str, newfile: P, seq: i32, ftype: FileType) -> Result<()> {
	let mut hasher = Sha256::new();
	let mut file = File::open(newfile)?;

	let bytes_written = io::copy(&mut file, &mut hasher)?;

	let digest = hasher.finalize();

	let de = DirEntry{
	    prefix: prefix.to_string(),
	    number: seq,
	    size: bytes_written,
	    ftype: ftype,
	    sha256: hex::encode(&digest),
	    content: None,
	};

	let cde = de.clone();

	self.ents.insert(de);

	// @@@ cde.XXX

	writer.record(&cde);

	Ok(())
    }

    pub fn close(&mut self, writer: &mut Writer) -> Result<(PathBuf, i32)> {
	let mut drecs: Vec<(String, PathBuf, i32)> = Vec::new();

	for (base, ref mut sd) in self.subdirs.iter_mut() {
	    let (dfn, num) = sd.close(writer)?;
	    drecs.push((base.to_string(), dfn, num));
	}

	for dr in drecs {
	    self.update(writer, &dr.0, dr.1, dr.2, FileType::Tree)?;
	}
	
	let vents: Vec<DirEntry> = self.ents.iter().cloned().collect();

	self.dirfnum += 1;

	let full = self.real_path_of(format!("dir.{}.parquet", self.dirfnum));

	file::write_file(&full, &vents, directory_fields().as_slice())?;

	return Ok((full, self.dirfnum))
    }
}
