use crate::pond::Deriver;
use crate::pond::ForArrow;
use crate::pond::Pond;
use crate::pond::derive::copy_parquet_to;
use crate::pond::file::sha256_file;
use crate::pond::wd::WD;
use crate::pond::writer::Writer;

use anyhow::{Context, Result, anyhow};
use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::array::as_primitive_array;
use arrow::array::as_string_array;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::datatypes::{Int32Type, UInt8Type, UInt64Type};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use sha2::Digest;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry::Occupied;
use std::collections::btree_map::Entry::Vacant;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

pub struct Lookup {
    pub prefix: String,
    pub entry: Option<DirEntry>,
    pub deri: Option<Rc<RefCell<Box<dyn Deriver>>>>,
}

pub trait TreeLike {
    fn subdir<'a>(&mut self, pond: &'a mut Pond, lookup: &Lookup) -> Result<WD<'a>>;

    fn pondpath(&self, prefix: &str) -> PathBuf;

    fn realpath_of(&self) -> PathBuf;

    fn realpath_subdir(&self, prefix: &str) -> PathBuf {
        self.realpath_of().join(prefix) // @@@ Option?
    }

    fn realpath(&mut self, pond: &mut Pond, entry: &DirEntry) -> Option<PathBuf> {
        match entry.ftype {
            FileType::Tree => Some(self.realpath_subdir(&entry.prefix)),
            FileType::Data
            | FileType::Table
            | FileType::Series
            | FileType::SynTree
            | FileType::SymLink => {
                Some(self.realpath_version(pond, &entry.prefix, entry.number, entry.ftype.ext())?)
            }
        }
    }

    fn copy_version_to<'a>(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        numf: i32,
        ext: &str,
        mut to: Box<dyn Write + Send + 'a>,
    ) -> Result<()> {
        // TODO: this could call sql_for_version for parquet files?
        // Is the 'SELECT * FROM' needed?
        if numf == 0 {
            let files = self.realpath_all(pond, prefix);
            let qs = format!("SELECT * FROM read_parquet({:?})", files);
            copy_parquet_to(qs, to)
        } else {
            let path = self
                .realpath_version(pond, prefix, numf, ext)
                .ok_or(anyhow!("no real path {}", prefix))?;
            let mut from = File::open(path)?;
            let _ = std::io::copy(&mut from, &mut to)?;
            Ok(())
        }
    }

    fn sql_for_version(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        numf: i32,
        ext: &str,
    ) -> Result<String> {
        if numf == 0 {
            let files = self.realpath_all(pond, prefix);
            Ok(format!("SELECT * FROM read_parquet({:?})", files))
        } else {
            let path = self
                .realpath_version(pond, prefix, numf, ext)
                .ok_or(anyhow!("no real path {}", prefix))?;
            Ok(format!("SELECT * FROM read_parquet({})", path.display()))
        }
    }

    fn realpath_current(&mut self, pond: &mut Pond, prefix: &str) -> Result<Option<PathBuf>> {
        let lookup = self.lookup(pond, prefix);
        if let Some(cur) = lookup.entry {
            Ok(self.realpath_version(pond, prefix, cur.number, cur.ftype.ext()))
        } else {
            Err(anyhow!("no current path: {}", prefix,))
        }
    }

    fn realpath_version(
        &mut self,
        pond: &mut Pond,
        prefix: &str,
        numf: i32,
        ext: &str,
    ) -> Option<PathBuf>;

    fn realpath_all(&mut self, pond: &mut Pond, prefix: &str) -> Vec<PathBuf> {
        let t: Vec<Option<_>> = self
            .entries(pond)
            .iter()
            .filter(|x| x.prefix == prefix)
            .map(|x| self.realpath(pond, x))
            .collect();
        t.into_iter()
            .collect::<Option<Vec<_>>>()
            .expect("real path here")
    }

    fn entries_syn(
        &mut self,
        pond: &mut Pond,
    ) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>>;

    fn entries(&mut self, pond: &mut Pond) -> BTreeSet<DirEntry> {
        self.entries_syn(pond).into_iter().map(|(x, _)| x).collect()
    }

    fn sync(&mut self, pond: &mut Pond) -> Result<(PathBuf, i32, usize, bool)>;

    fn lookup(&mut self, pond: &mut Pond, prefix: &str) -> Lookup {
        // Note: presently we will assume that symlinks act within a single
        // directory.
        let entries = self.entries_syn(pond);
        let mut pfx = prefix.to_string();
        loop {
            let one = entries
                .iter()
                .filter(|x| x.0.prefix == pfx)
                .reduce(|a, b| if a.0.number > b.0.number { a } else { b })
                .clone();

            match one {
                Some((found, deri)) => {
                    if let FileType::SymLink = found.ftype {
                        let dat = found.content.clone().expect("symlinks must");
                        // TODO: eprintln!("resolved link {}", new_base);
                        pfx = String::from_utf8_lossy(&dat).to_string();
                        continue;
                    }
                    return Lookup {
                        prefix: found.prefix.clone(),
                        entry: Some(found.clone()),
                        deri: deri.clone(),
                    };
                }
                _ => {
                    return Lookup {
                        prefix: pfx,
                        entry: None,
                        deri: None,
                    };
                }
            };
        }
    }

    fn lookup_all(&mut self, pond: &mut Pond, prefix: &str) -> Vec<DirEntry> {
        self.entries(pond)
            .iter()
            .filter(|x| x.prefix == prefix)
            .cloned()
            .collect::<Vec<_>>()
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

    fn create_symlink(&mut self, _pond: &mut Pond, _from: &str, _to: &str) -> Result<()> {
        Err(anyhow!("not implemented"))
    }
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
    SymLink = 6, // Symlink entry
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
            "symlink" => Ok(Self::SymLink),
            _ => Err(anyhow!("invalid file type {}", ft)),
        }
    }
}

impl FileType {
    pub fn is_relation(&self) -> bool {
        match self {
            FileType::Table | FileType::Series => true,
            _ => false,
        }
    }

    pub fn ext(&self) -> &'static str {
        match self {
            FileType::Tree => "",
            FileType::Table => "parquet",
            FileType::Series => "parquet",
            FileType::Data => "data",
            FileType::SynTree => "synth",
            FileType::SymLink => "symlink",
        }
    }

    pub fn into_iter() -> core::array::IntoIter<FileType, 6> {
        [
            FileType::Tree,
            FileType::Table,
            FileType::Series,
            FileType::Data,
            FileType::SynTree,
            FileType::SymLink,
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
        6 => Some(FileType::SymLink),
        _ => None,
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

pub fn create_dir<P: AsRef<Path>>(path: P, relp: P) -> Result<Directory> {
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

    fn realpath_version(
        &mut self,
        _pond: &mut Pond,
        prefix: &str,
        num: i32,
        ext: &str,
    ) -> Option<PathBuf> {
        Some(self.path.join(format!("{}.{}.{}", prefix, num, ext)))
    }

    fn entries_syn(
        &mut self,
        _pond: &mut Pond,
    ) -> BTreeMap<DirEntry, Option<Rc<RefCell<Box<dyn Deriver>>>>> {
        self.ents.iter().map(|x| (x.clone(), None)).collect()
    }

    fn subdir<'a>(&mut self, pond: &'a mut Pond, lookup: &Lookup) -> Result<WD<'a>> {
        let newrelp = self.pondpath(&lookup.prefix);
        let subdirpath = self.realpath_subdir(&lookup.prefix);

        let ent_path = lookup
            .entry
            .as_ref()
            .map(|x| (x.clone(), self.realpath(pond, &x).expect("real path here")));

        let node = *match self.subdirs.entry(lookup.prefix.to_string()) {
            Occupied(e) => e.into_mut(),
            Vacant(e) => e.insert(match ent_path {
                Some((entry, newpath)) => {
                    if entry.ftype == FileType::SynTree {
                        if let Some(deri) = lookup.deri.clone() {
                            deri.deref()
                                .borrow_mut()
                                .open_derived(pond, &newpath, &newrelp, &entry)?
                        } else {
                            pond.open_derived(&newpath, &newrelp, &entry)?
                        }
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

        if self.modified {
            self.dirfnum += 1;
        }
        let full = self.path.join(format!("dir.{}.parquet", self.dirfnum));

        if self.modified {
            let vents: Vec<DirEntry> = self.ents.iter().cloned().collect();
            self.write_dir(&full, &vents)?;
        }

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

    fn create_symlink(&mut self, pond: &mut Pond, from: &str, to: &str) -> Result<()> {
        let de = DirEntry {
            prefix: from.to_string(),
            number: 0,
            size: 0,
            ftype: FileType::SymLink,
            sha256: [0; 32],
            content: Some(to.as_bytes().into()),
        };

        let mut cde = de.clone();

        // Update the local file system.
        self.ents.insert(de);
        self.modified = true;

        // Record the full path for backup.
        cde.prefix = self.pondpath(from).to_string_lossy().to_string();

        eprintln!("update symlink '{}' → '{}'", from, to,);

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
