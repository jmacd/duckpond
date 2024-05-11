use serde::{Serialize, Deserialize};
use hex;

use sha2::{Sha256, Digest};
use std::{io, fs};

use std::path::{Path,PathBuf};
use crate::pond::file;
use anyhow::{Context, Result};
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

pub struct Directory {
    path: PathBuf,
    ents: BTreeSet<DirEntry>,
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

    let empty: Vec<DirEntry> = vec![];
    file::write_file(path.to_path_buf().join("dir.0.parquet"), &empty, directory_fields().as_slice())?;

    Ok(Directory{
	path: path.into(),
	ents: BTreeSet::new(),
    })
}

pub fn open_dir<P: AsRef<Path>>(path: P) -> Result<Directory> {
    let path = path.as_ref();
    let ents: Vec<DirEntry> = file::open_file(path)?;

    let mut d = Directory{
	ents: BTreeSet::new(),
	path: path.to_path_buf(),
    };

    // @@@ not sure how to construct btreeset from iterator, &DirEntry vs DirEntry
    for ent in ents {
	d.ents.insert(ent);
    }

    Ok(d)
}

impl Directory {
    pub fn write_file<T: Serialize>(
	&mut self,
	name: String,
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
	    prefix: name,
	    number: seq,
	    size: bytes_written,
	    deleted: false,
	    sha256: hex::encode(&digest),
	});

	// BTreeSet is difficult to use.  @@@?
	let mut vents: Vec<DirEntry> = Vec::new();
	for ent in &self.ents {
	    vents.push(ent.clone());
	}

	file::write_file(self.path.to_path_buf().join("dir.1.parquet"), &vents, directory_fields().as_slice())?;

	Ok(())
    }
}
