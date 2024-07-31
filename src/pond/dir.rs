use crate::pond::writer::MultiWriter;
use crate::pond::entry;
use crate::pond::ForArrow;

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_to_parquet_schema;

use arrow::array::as_string_array;
use arrow::array::as_primitive_array;
use arrow::array::AsArray;
use arrow::datatypes::{Int32Type,UInt64Type,UInt8Type};

use arrow::datatypes::{DataType, Field, FieldRef};

use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};

use sha2::{Sha256, Digest};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use std::path::{Path,PathBuf};
use anyhow::{Context, Result, anyhow};

use std::collections::BTreeSet;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirEntry {
    pub prefix: String,
    pub number: i32,
    pub size: u64,
    pub ftype: FileType,

    pub sha256: [u8; 32],
    pub content: Option<Vec<u8>>,
}

impl ForArrow for DirEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
	    Arc::new(Field::new("prefix", DataType::Utf8, false)),
	    Arc::new(Field::new("number", DataType::Int32, false)),
	    Arc::new(Field::new("size", DataType::UInt64, false)),
	    Arc::new(Field::new("filetype", DataType::UInt8, false)),
	    Arc::new(Field::new("sha256", DataType::FixedSizeBinary(32), false)),
	    Arc::new(Field::new("contents", DataType::Binary, true)),
	]
    }
}

/// FileType encodes as a u8 for appearances in directory parquet tables.
#[derive(Debug, Serialize_repr, Deserialize_repr, Clone, PartialEq, Eq, PartialOrd, Ord, Copy)]
#[repr(u8)]
pub enum FileType {
    Tree = 1,   // Directory structure
    Table = 2,  // One-shot table
    Series = 3, // Multi-part table
}

impl TryFrom<String> for FileType {
    type Error = anyhow::Error;

    // Must be a standard tool for this?
    fn try_from(ft: String) -> Result<Self, Self::Error> {
	match ft.to_lowercase().as_str() {
	    "tree" => Ok(Self::Tree),
	    "table" => Ok(Self::Table),
	    "series" => Ok(Self::Series),
	    _ => Err(anyhow!("invalid file type {}", ft)),
	}
    }
}

impl FileType {
    pub fn into_iter() -> core::array::IntoIter<FileType, 3> {
        [
            FileType::Tree,
	    FileType::Table,
	    FileType::Series,
        ]
        .into_iter()
    }
}

pub fn by2ft(x: u8) -> Option<FileType> {
    match x {
	1 => Some(FileType::Tree),
	2 => Some(FileType::Table),
	3 => Some(FileType::Series),
	_ => None,
    }
}

#[derive(Debug)]
pub struct Directory {
    pub path: PathBuf,
    pub relp: PathBuf,
    pub ents: BTreeSet<DirEntry>,
    pub subdirs: BTreeMap<String, Directory>,
    pub dirfnum: i32,
}

pub fn create_dir<P: AsRef<Path>>(
    path: P,
    relp: P,
) -> Result<Directory> {
    let path = path.as_ref();

    fs::create_dir(path)
	.with_context(|| format!("pond directory already exists: {}", path.display()))?;

    Ok(Directory{
	path: path.into(),
	relp: relp.as_ref().to_path_buf(),
	ents: BTreeSet::new(),
	subdirs: BTreeMap::new(),
	dirfnum: 0,
    })
}

fn read_entries<P: AsRef<Path>>(path: P) -> Result<BTreeSet<DirEntry>> {
    let file = File::open(&path)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
	.with_context(|| format!("could not open parquet {}", path.as_ref().display()))?;

    let schema = builder.schema();
    let pschema = arrow_to_parquet_schema(schema)?;

    let reader: ParquetRecordBatchReader = builder.with_projection(
	// Note: exclude the content field.
	ProjectionMask::leaves(&pschema, vec![0, 1, 2, 3, 4])
    ).build()?;

    let mut ents: BTreeSet<DirEntry> = BTreeSet::new();

    for rec in reader {
	let batch = rec?;

	let pfxs = as_string_array(batch.column(0));
	let nums = as_primitive_array::<Int32Type>(batch.column(1));
	let sizes = as_primitive_array::<UInt64Type>(batch.column(2));
	let ftypes = as_primitive_array::<UInt8Type>(batch.column(3));
	let sha256 = batch.column(4).as_fixed_size_binary();

	let comb = pfxs.iter()
	    .zip(nums.iter())
	    .zip(sizes.iter())
	    .zip(ftypes.iter())
	    .zip(sha256.iter());

	ents.extend(comb.map(|((((pfx, num), sz), ftype), sha): ((((Option<&str>, Option<i32>), Option<u64>), Option<u8>), Option<&[u8]>)| -> DirEntry {
	    
	    DirEntry{
		prefix: pfx.unwrap().to_string(),
		number: num.unwrap(),
		size: sz.unwrap(),
		ftype: by2ft(ftype.unwrap()).unwrap(),
		sha256: sha.unwrap().try_into().expect("sha256 has wrong length"),
		content: None,
	    }
	}));
    }

    Ok(ents)
}

pub fn open_dir<P: AsRef<Path>>(path: P, relp: P) -> Result<Directory> {
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

    let dirpath = path.join(format!("dir.{}.parquet", dirfnum));

    Ok(Directory{
	ents: read_entries(&dirpath)?,
	relp: PathBuf::new().join(relp),
	path: path.to_path_buf(),
	subdirs: BTreeMap::new(),
	dirfnum: dirfnum,
    })
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

    pub fn update<P: AsRef<Path>>(&mut self, writer: &mut MultiWriter, prefix: &str, newfile: P, seq: i32, ftype: FileType) -> Result<()> {
	let data = fs::read(newfile)?;
	let mut hasher = Sha256::new();

	hasher.write(data.as_slice())?;

	let digest = hasher.finalize();

	let de = DirEntry{
	    prefix: prefix.to_string(),
	    number: seq,
	    size: data.len() as u64,
	    ftype: ftype,
	    sha256: digest.into(),
	    content: None,
	};

	let mut cde = de.clone();

	self.ents.insert(de);

	cde.content = Some(data);
	cde.prefix = self.relp.join(prefix).to_string_lossy().to_string();

	writer.record(&cde)?;

	Ok(())
    }

    /// sync recursively closes this directory's children
    pub fn sync(&mut self, writer: &mut MultiWriter) -> Result<(PathBuf, i32)> {
	let mut drecs: Vec<(String, PathBuf, i32)> = Vec::new();

	for (base, ref mut sd) in self.subdirs.iter_mut() {
	    // subdir fullname, version number
	    let (dfn, num) = sd.sync(writer)?;
	    drecs.push((base.to_string(), dfn, num));
	}

	for dr in drecs {
	    self.update(writer, &dr.0, dr.1, dr.2, FileType::Tree)?;
	}
	
	let vents: Vec<DirEntry> = self.ents.iter().cloned().collect();

	self.dirfnum += 1;

	let full = self.real_path_of(format!("dir.{}.parquet", self.dirfnum));

	entry::write_dir(&full, &vents)?;

	return Ok((full, self.dirfnum))
    }
}
