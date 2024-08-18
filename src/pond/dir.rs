use crate::pond::writer::MultiWriter;
use crate::pond::writer::Writer;
use crate::pond::ForArrow;
use crate::pond::file::sha256_file;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow::array::as_string_array;
use arrow::array::as_primitive_array;
use arrow::array::AsArray;
use arrow::datatypes::{Int32Type,UInt64Type,UInt8Type};

use arrow::datatypes::{DataType, Field, FieldRef};
use parquet::file::reader::ChunkReader;

use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};

use sha2::Digest;
use std::fs;
use std::fs::File;
use std::sync::Arc;

use std::path::{Path,PathBuf};
use anyhow::{Context, Result, anyhow};

use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::collections::btree_set;

use arrow::array::ArrayRef;

pub trait TreeLike: std::fmt::Debug {
    fn fullname(&self, prefix: &str) -> PathBuf;

    fn realname(&self, prefix: &str) -> PathBuf;

    fn entries(&self) -> btree_set::Iter<'_, DirEntry>;

    fn subdir_mut(&mut self, prefix: &str) -> Option<Box<dyn TreeLike>>;

    fn lookup(&self, prefix: &str) -> Option<DirEntry>;

    fn create_subdir(&mut self, prefix: &str, newpath: &Path, newrelp: &Path) -> Result<()>;

    fn open_subdir(&mut self, prefix: &str, newpath: &Path, newrelp: &Path) -> Result<()>;
    
    fn modified(&self) -> bool;

    fn sync(&mut self, writer: &mut MultiWriter) -> Result<(PathBuf, i32)>;
}

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
    Tree = 1,    // Directory structure
    Table = 2,   // One-shot table
    Series = 3,  // Multi-part table
    Data = 4,    // Arbitrary data
    SynTree = 5, // Derivative tree
}

impl TryFrom<String> for FileType {
    type Error = anyhow::Error;

    // Must be a standard tool for this?
    fn try_from(ft: String) -> Result<Self, Self::Error> {
	match ft.to_lowercase().as_str() {
	    "tree" => Ok(Self::Tree),
	    "table" => Ok(Self::Table),
	    "series" => Ok(Self::Series),
	    "data" => Ok(Self::Data),
	    "syntree" => Ok(Self::SynTree),
	    _ => Err(anyhow!("invalid file type {}", ft)),
	}
    }
}

impl FileType {
    pub fn ext(&self) -> &'static str {
	match self {
            FileType::Tree => "",
	    FileType::Table => "parquet",
	    FileType::Series => "parquet",
	    FileType::Data => "data",
	    FileType::SynTree => "",
	}
    }

    pub fn into_iter() -> core::array::IntoIter<FileType, 5> {
        [
            FileType::Tree,
	    FileType::Table,
	    FileType::Series,
	    FileType::Data,
	    FileType::SynTree,
        ]
        .into_iter()
    }
}

pub fn by2ft(x: u8) -> Option<FileType> {
    match x {
	1 => Some(FileType::Tree),
	2 => Some(FileType::Table),
	3 => Some(FileType::Series),
	4 => Some(FileType::Data),
	5 => Some(FileType::SynTree),
	_ => None,
    }
}

#[derive(Debug)]
pub struct Directory {
    pub path: PathBuf,
    pub relp: PathBuf,
    pub ents: BTreeSet<DirEntry>,
    pub subdirs: BTreeMap<String, Box<dyn TreeLike>>,
    pub dirfnum: i32,
    pub modified: bool,
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
	modified: true,
    })
}

pub fn read_entries<P: AsRef<Path>>(path: P) -> Result<Vec<DirEntry>> {
    let file = File::open(&path)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
	.with_context(|| format!("could not open parquet {}", path.as_ref().display()))?;

    read_entries_from_builder(builder)
}

pub fn read_entries_from_builder<T: ChunkReader + 'static> (builder: ParquetRecordBatchReaderBuilder<T>) -> Result<Vec<DirEntry>> {
    let reader = builder.build()
	.with_context(|| "initialize reader failed")?;

    let mut ents = Vec::new();

    for rec in reader {
	let batch = rec?;
	let sha256: &ArrayRef = batch.column(4);
	let content: &ArrayRef = batch.column(5);
	let pfxs = as_string_array(batch.column(0));
	let nums = as_primitive_array::<Int32Type>(batch.column(1));
	let sizes = as_primitive_array::<UInt64Type>(batch.column(2));
	let ftypes = as_primitive_array::<UInt8Type>(batch.column(3));

	let comb = pfxs.iter()
	    .zip(nums.iter())
	    .zip(sizes.iter())
	    .zip(ftypes.iter())
	    .zip(sha256.as_fixed_size_binary().iter())
	    .zip(content.as_binary::<i32>().iter());
	
	ents.extend(comb.map(|(((((pfx, num), sz), ftype), sha), content): (((((Option<&str>, Option<i32>), Option<u64>), Option<u8>), Option<&[u8]>), Option<&[u8]>)| -> DirEntry {
	    
	    DirEntry{
		prefix: pfx.unwrap().to_string(),
		number: num.unwrap(),
		size: sz.unwrap(),
		ftype: by2ft(ftype.unwrap()).unwrap(),
		sha256: sha.unwrap().try_into().expect("sha256 has wrong length"),
		content: content.map(Vec::from),
	    }
	}));
    }

    Ok(ents)
}

pub fn open_dir<P: AsRef<Path>>(path: P, relp: P) -> Result<Directory> {
    let path = path.as_ref();

    let mut dirfnum: i32 = 0;

    let entries = fs::read_dir(path)
	.with_context(|| format!("could not read directory {}", path.display()))?;

    for entry_r in entries {
        let entry = entry_r?;
	let osname = entry.file_name();
	let name = osname
	    .into_string()
	    .map_err(|e| anyhow!("non-utf8 path name {:?}", e))?;
	
	if !name.starts_with("dir.") {
	    continue;
	}

	let numstr = name.trim_start_matches("dir.").trim_end_matches(".parquet");
	let num = numstr.parse::<i32>()?;
	dirfnum = std::cmp::max(dirfnum, num);
    }

    let dirpath = path.join(format!("dir.{}.parquet", dirfnum));

    Ok(Directory{
	ents: read_entries(&dirpath)?.into_iter().collect(),
	relp: PathBuf::new().join(relp),
	path: path.to_path_buf(),
	subdirs: BTreeMap::new(),
	dirfnum: dirfnum,
	modified: false,
    })
}

impl TreeLike for Directory {
    fn fullname(&self, prefix: &str) -> PathBuf {
	self.relp.join(prefix)
    }

    fn realname(&self, prefix: &str) -> PathBuf {
	self.path.join(prefix)
    }

    fn entries(&self) -> btree_set::Iter<'_, DirEntry> {
	self.ents.iter()
    }

    fn subdir_mut(&mut self, prefix: &str) -> Option<Box<dyn TreeLike>> {
	self.subdirs.get_mut(prefix).map(|x| *x)
    }

    fn lookup(&self, prefix: &str) -> Option<DirEntry> {
	self.ents 
	    .iter()
	    .filter(|x| x.prefix == prefix)
	    .reduce(|a, b| if a.number > b.number { a } else { b })
	    .cloned()
    }

    fn create_subdir(&mut self, prefix: &str, newpath: &Path, newrelp: &Path) -> Result<()>{
	self.subdirs.insert(prefix.to_string(), Box::new(create_dir(newpath, newrelp)?));
	Ok(())
    }

    fn open_subdir(&mut self, prefix: &str, newpath: &Path, newrelp: &Path) -> Result<()>{
	self.subdirs.insert(prefix.to_string(), Box::new(open_dir(newpath, newrelp)?));
	Ok(())
    }
    
    /// sync recursively closes this directory's children
    fn sync(&mut self, writer: &mut MultiWriter) -> Result<(PathBuf, i32)> {
	let mut drecs: Vec<(String, PathBuf, i32, usize)> = Vec::new();
	
	for (base, mut sd) in self.subdirs.iter_mut() {
	    // subdir fullname, version number
	    let chcnt = sd.entries().count();
	    let (dfn, num) = sd.sync(writer)?;

	    if !sd.modified() {
		continue
	    }
	    
	    drecs.push((base.to_string(), dfn, num, chcnt));
	}

	for dr in drecs {
	    self.update(writer, &dr.0, dr.1, dr.2, FileType::Tree, Some(dr.3))?;
	}

	// BTreeSet->Vec
	let vents: Vec<DirEntry> = self.ents.iter().cloned().collect();

	self.dirfnum += 1;

	let full = self.real_path_of(format!("dir.{}.parquet", self.dirfnum));

	self.write_dir(&full, &vents)?;

	return Ok((full, self.dirfnum))
    }

    fn modified(&self) -> bool {
	self.modified
    }
}

impl Directory {
    pub fn self_path(&self) -> PathBuf {
	self.path.to_path_buf()
    }

    pub fn real_path_of<P: AsRef<Path>>(&self, base: P) -> PathBuf {
	self.path.clone().join(base)
    }

    pub fn current_path_of(&self, prefix: &str) -> Result<PathBuf> {
	if let Some(cur) = self.lookup(prefix) {
	    Ok(self.prefix_num_path(prefix, cur.number, cur.ftype.ext()))
	} else {
	    Err(anyhow!("no current path: {} in {}", prefix, self.path.display()))
	}
    }

    pub fn prefix_num_path(&self, prefix: &str, num: i32, ext: &str) -> PathBuf {
	self.real_path_of(format!("{}.{}.{}", prefix, num, ext))
    }

    pub fn all_paths_of(&self, prefix: &str) -> Vec<PathBuf> {
	self.ents 
	    .iter()
	    .filter(|x| x.prefix == prefix)
	    .map(|x| self.real_path_of(format!("{}.{}.parquet", x.prefix, x.number)))
	    .collect()
    }

    pub fn update<P: AsRef<Path>>(&mut self, writer: &mut MultiWriter, prefix: &str, newfile: P, seq: i32, ftype: FileType, row_cnt: Option<usize>) -> Result<()> {

	let (hasher, size, content_opt) = sha256_file(newfile)?;

	let de = DirEntry{
	    prefix: prefix.to_string(),
	    number: seq,
	    size: size,
	    ftype: ftype,
	    sha256: hasher.finalize().into(),

	    // content in the local directory file is None,
	    // content_opt will be used below.
	    content: None,
	};

	let mut cde = de.clone();

	// Update the local file system.
	self.ents.insert(de);
	self.modified = true;

	// Record the full path for backup.
	cde.prefix = self.relp.join(prefix).to_string_lossy().to_string();

	eprintln!("update {ftype:?} '{}' size {size} (v{seq}) {}{}",
		  &cde.prefix,
		  if content_opt.is_some() { "✅" } else { "🟢" },
		  if row_cnt.is_some() { format!(" rows {}", row_cnt.unwrap()) } else { "".to_string() },
	);

	cde.content = content_opt;
	
	writer.record(&cde)?;

	Ok(())
    }

    fn write_dir(&self, full: &PathBuf, v: &[DirEntry]) -> Result<()> {
	let mut wr = Writer::new("local directory file".to_string());

	for ent in v {
	    wr.record(&ent)?;
	}

	wr.commit_to_local_file(full)
    }
}
