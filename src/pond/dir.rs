use crate::pond::derive::Duck;
use crate::pond::file::sha256_file;
use crate::pond::wd::WD;
use crate::pond::writer::Writer;
use crate::pond::ForArrow;
use crate::pond::Pond;

use anyhow::{anyhow, Context, Result};
use arrow::array::as_primitive_array;
use arrow::array::as_string_array;
use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::datatypes::{Int32Type, UInt64Type, UInt8Type};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use sha2::Digest;

use std::cell::RefCell;
use std::collections::btree_map::Entry::Occupied;
use std::collections::btree_map::Entry::Vacant;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

pub enum PondRead<'stmt> {
    File { file: File },
    Duck { duck: Duck<'stmt> },
}

pub trait TreeLike: std::fmt::Debug {
    fn subdir<'a>(&mut self, pond: &'a mut Pond, prefix: &str) -> Result<WD<'a>>;

    fn pondpath(&self, prefix: &str) -> PathBuf;

    fn realpath_of(&self) -> PathBuf;

    fn realpath_subdir(&self, prefix: &str) -> PathBuf {
        self.realpath_of().join(prefix)
    }

    fn realpath(&mut self, pond: &mut Pond, entry: &DirEntry) -> PathBuf {
        match entry.ftype {
            FileType::Tree => self.realpath_subdir(&entry.prefix),
            FileType::Data | FileType::Table | FileType::Series | FileType::SynTree => {
                self.realpath_version(pond, &entry.prefix, entry.number, entry.ftype.ext())
            }
        }
    }

    fn open_version<'a>(
        &mut self,
        pond: &'a mut Pond,
        prefix: &str,
        numf: i32,
        ext: &str,
    ) -> Result<PondRead<'a>> {
        let file = File::open(self.realpath_version(pond, prefix, numf, ext))?;
        Ok(PondRead::File { file })
    }

    fn realpath_current(&mut self, pond: &mut Pond, prefix: &str) -> Result<PathBuf> {
        if let Some(cur) = self.lookup(pond, prefix) {
            Ok(self.realpath_version(pond, prefix, cur.number, cur.ftype.ext()))
        } else {
            Err(anyhow!("no current path: {}", prefix,))
        }
    }

    fn realpath_version(&mut self, pond: &mut Pond, prefix: &str, numf: i32, ext: &str) -> PathBuf;

    fn realpath_all(&mut self, pond: &mut Pond, prefix: &str) -> Vec<PathBuf> {
        self.entries(pond)
            .iter()
            .filter(|x| x.prefix == prefix)
            .map(|x| self.realpath(pond, x))
            .collect()
    }

    fn entries(&mut self, pond: &mut Pond) -> BTreeSet<DirEntry>;

    fn sync(&mut self, pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)>;

    fn lookup(&mut self, pond: &mut Pond, prefix: &str) -> Option<DirEntry> {
        self.entries(pond)
            .iter()
            .filter(|x| x.prefix == prefix)
            .reduce(|a, b| if a.number > b.number { a } else { b })
            .cloned()
    }

    fn update(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        newfile: &PathBuf,
        seq: i32,
        ftype: FileType,
        row_cnt: Option<usize>,
    ) -> Result<()>;

    // fn id(&self) -> usize;
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
            Arc::new(Field::new("ftype", DataType::UInt8, false)),
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
            FileType::SynTree => "synth",
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

impl<'conn> Read for PondRead<'conn> {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, std::io::Error> {
        match self {
            PondRead::File { file } => file.read(buf),
            PondRead::Duck { duck } => duck.read(buf),
        }
    }
}

#[derive(Debug)]
pub struct RealFile {}

#[derive(Debug)]
pub struct Directory {
    pub path: PathBuf,
    pub relp: PathBuf,
    pub ents: BTreeSet<DirEntry>,
    pub subdirs: BTreeMap<String, usize>,
    pub dirfnum: i32,
    pub modified: bool,
}

pub fn create_dir<'a, P: AsRef<Path>>(path: P, relp: P) -> Result<Directory> {
    let path = path.as_ref();

    fs::create_dir(path)
        .with_context(|| format!("pond directory already exists: {}", path.display()))?;

    Ok(Directory {
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

pub fn read_entries_from_builder<T: ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<T>,
) -> Result<Vec<DirEntry>> {
    let reader = builder
        .build()
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

        let comb = pfxs
            .iter()
            .zip(nums.iter())
            .zip(sizes.iter())
            .zip(ftypes.iter())
            .zip(sha256.as_fixed_size_binary().iter())
            .zip(content.as_binary::<i32>().iter());

        ents.extend(comb.map(
            |(((((pfx, num), sz), ftype), sha), content): (
                (
                    (((Option<&str>, Option<i32>), Option<u64>), Option<u8>),
                    Option<&[u8]>,
                ),
                Option<&[u8]>,
            )|
             -> DirEntry {
                DirEntry {
                    prefix: pfx.unwrap().to_string(),
                    number: num.unwrap(),
                    size: sz.unwrap(),
                    ftype: by2ft(ftype.unwrap()).unwrap(),
                    sha256: sha.unwrap().try_into().expect("sha256 has wrong length"),
                    content: content.map(Vec::from),
                }
            },
        ));
    }

    Ok(ents)
}

pub fn open_dir<'a, P: AsRef<Path>>(path: P, relp: P) -> Result<Directory> {
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

    Ok(Directory {
        ents: read_entries(&dirpath)?.into_iter().collect(),
        relp: PathBuf::new().join(relp),
        path: path.to_path_buf(),
        subdirs: BTreeMap::new(),
        dirfnum,
        modified: false,
    })
}

impl TreeLike for Directory {
    fn realpath_of(&self) -> PathBuf {
        self.path.clone()
    }

    fn pondpath(&self, prefix: &str) -> PathBuf {
        if prefix.len() == 0 {
            self.relp.clone()
        } else {
            self.relp.join(prefix)
        }
    }

    fn realpath_version(&mut self, _pond: &mut Pond, prefix: &str, num: i32, ext: &str) -> PathBuf {
        self.path.join(format!("{}.{}.{}", prefix, num, ext))
    }

    fn entries(&mut self, _pond: &mut Pond) -> BTreeSet<DirEntry> {
        self.ents.clone()
    }

    fn subdir<'a>(&mut self, pond: &'a mut Pond, prefix: &str) -> Result<WD<'a>> {
        let newrelp = self.pondpath(prefix);
        let subdirpath = self.realpath_subdir(prefix);

        let find = self.lookup(pond, prefix);

        // Yuck! subdirpath is not an alias, but ...
        let ent_path = find.map(|x| (x.clone(), self.realpath(pond, &x)));

        let node = *match self.subdirs.entry(prefix.to_string()) {
            Occupied(e) => e.into_mut(),
            Vacant(e) => e.insert(match ent_path {
                Some((entry, newpath)) => {
                    if entry.ftype == FileType::SynTree {
                        pond.open_derived(&newpath, &newrelp, &entry)?
                    } else {
                        pond.insert(Rc::new(RefCell::new(open_dir(&newpath, &newrelp)?)))
                    }
                }
                None => pond.insert(Rc::new(RefCell::new(create_dir(&subdirpath, &newrelp)?))),
            }),
        };

        Ok(WD::new(pond, node))
    }

    /// sync recursively closes this directory's children
    fn sync(&mut self, pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)> {
        let mut drecs: Vec<(String, PathBuf, i32, usize)> = Vec::new();
        //eprintln!("sync {}", self.relp.display());

        for (base, sd) in self.subdirs.iter_mut() {
            // subdir pondpath, version number, child count, modified
            let (dfn, num, chcnt, modified) = pond.get(*sd).deref().borrow_mut().sync(pond)?;

            if !modified {
                continue;
            }

            drecs.push((base.to_string(), dfn, num, chcnt));
        }

        for dr in drecs {
            self.update(pond, &dr.0, &dr.1, dr.2, FileType::Tree, Some(dr.3))?;
        }

        // BTreeSet->Vec
        let vents: Vec<DirEntry> = self.ents.iter().cloned().collect();

        self.dirfnum += 1;

        let full = self.path.join(format!("dir.{}.parquet", self.dirfnum));

        self.write_dir(&full, &vents)?;

        return Ok((full, self.dirfnum, self.entries(pond).len(), self.modified));
    }

    fn update(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        newfile: &PathBuf,
        seq: i32,
        ftype: FileType,
        row_cnt: Option<usize>,
    ) -> Result<()> {
        let (hasher, size, content_opt) = sha256_file(newfile)?;

        let de = DirEntry {
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
        cde.prefix = self.pondpath(prefix).to_string_lossy().to_string();

        eprintln!(
            "update {ftype:?} '{}' size {size} (v{seq}) {}{}",
            &cde.prefix,
            if content_opt.is_some() { "✅" } else { "🟢" },
            if row_cnt.is_some() {
                format!(" rows {}", row_cnt.unwrap())
            } else {
                "".to_string()
            },
        );

        cde.content = content_opt;

        pond.writer.record(&cde)?;

        Ok(())
    }
}

impl Directory {
    fn write_dir(&self, full: &PathBuf, v: &[DirEntry]) -> Result<()> {
        let mut wr = Writer::new("local directory file".to_string());

        for ent in v {
            wr.record(&ent)?;
        }

        wr.commit_to_local_file(full)
    }
}
