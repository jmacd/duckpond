use serde::{Serialize, Deserialize};
use hex;

use sha2::{Sha256, Digest};
use std::{io, fs};

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
    deleted: bool,

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
        Arc::new(Field::new("deleted", DataType::Boolean, false)),

	// "Error: Only primitive data types can be converted to T"
        //Arc::new(Field::new("sha256", DataType::FixedSizeBinary(32), false)),

	Arc::new(Field::new("sha256", DataType::Utf8, false)),
    ]
}

pub fn create_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {
    let path = path.as_ref();

    std::fs::create_dir(path)
	.with_context(|| "pond directory already exists")?;

    //let empty: Vec<DirEntry> = vec![];
    //file::write_file(path.to_path_buf().join("dir.0.parquet"), &empty, directory_fields().as_slice())?;

    Ok(Directory{
	path: path.into(),
	ents: BTreeSet::new(),
	dirfnum: 0,
    })
}

pub fn open_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {
    let path = path.as_ref();

    let mut dirfnum: i32 = 0;

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

    let ents: Vec<DirEntry> = file::open_file(d.real_path_of(format!("dir.{}.parquet", dirfnum)))?;
    
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
	self.dir_path_of(prefix, 0)
    }


    pub fn next_path_of(&self, prefix: &str) -> Result<PathBuf> {
	self.dir_path_of(prefix, 1)
    }
    

    pub fn dir_path_of(&self, prefix: &str, add: i32) -> Result<PathBuf> {
	// @@@ O(N) fix
	let cur = self.ents
	    .iter()
	    .filter(|x| x.prefix == prefix)
	    .reduce(|a, b| if a.number > b.number { a } else { b })
	    .with_context(|| format!("no values by that prefix {}", prefix))?;
	
	Ok(self.real_path_of(format!("{}.{}.parquet", prefix, cur.number + add)))
    }
    
    pub fn write_file<T: Serialize>(
	&mut self,
	name: &str,
	records: &Vec<T>,
	fields: &[Arc<Field>],
    ) -> Result<()> {
	let seq: i32 = 1 + self.ents.iter().filter(|x| x.prefix == name).fold(0, |b, x| std::cmp::max(b, x.number));

	let newfile = self.path.join(format!("{}.{}.parquet", name, seq));

	file::write_file(&newfile, records, fields)?;

	let mut hasher = Sha256::new();
	let mut file = fs::File::open(newfile)?;

	let bytes_written = io::copy(&mut file, &mut hasher)?;
	let digest = hasher.finalize();

	self.ents.insert(DirEntry{
	    prefix: name.to_string(),
	    number: seq,
	    size: bytes_written,
	    deleted: false,
	    sha256: hex::encode(&digest),
	});


	Ok(())
    }

    pub fn close_dir(&mut self) -> Result<()> {
	// BTreeSet is difficult to use.  @@@?
	let mut vents: Vec<DirEntry> = Vec::new();
	for ent in &self.ents {
	    vents.push(ent.clone());
	}

	file::write_file(self.path.to_path_buf().join(format!("dir.{}.parquet", self.dirfnum)), &vents, directory_fields().as_slice())
    }
}
