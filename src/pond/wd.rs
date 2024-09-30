use crate::pond::dir;
use crate::pond::dir::DirEntry;
use crate::pond::dir::FileType;
use crate::pond::dir::TreeLike;
use crate::pond::file;
use crate::pond::writer::MultiWriter;
use crate::pond::ForArrow;
use crate::pond::Pond;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

#[derive(Debug)]
pub struct WD<'a> {
    pond: &'a mut Pond,
    node: usize,
}

impl<'a> WD<'a> {
    pub fn new(pond: &'a mut Pond, node: usize) -> Self {
        WD { pond, node: node }
    }

    pub fn multiwriter(&mut self) -> &mut MultiWriter {
        &mut self.pond.writer
    }

    pub fn d(&mut self) -> Rc<RefCell<dyn TreeLike + 'a>> {
        self.pond.get(self.node)
    }

    pub fn entries(&mut self) -> BTreeSet<DirEntry> {
        let d = self.d();
        let x = d.deref().borrow_mut().entries(self.pond);
        x
    }

    pub fn pondpath(&mut self, prefix: &str) -> PathBuf {
        self.d().deref().borrow().pondpath(prefix)
    }

    pub fn realpath(&mut self, entry: &DirEntry) -> Option<PathBuf> {
        self.d().deref().borrow_mut().realpath(self.pond, entry)
    }

    pub fn unique(&mut self) -> BTreeSet<dir::DirEntry> {
        let mut sorted: BTreeMap<String, DirEntry> = BTreeMap::new();
        for ent in self.entries() {
            if let Some(has) = sorted.get(&ent.prefix) {
                if has.number > ent.number {
                    continue;
                }
            }
            sorted.insert(ent.prefix.clone(), ent.clone());
        }
        let res = sorted.iter().map(|(_x, y)| y.clone()).collect();
        res
    }

    pub fn in_path<P: AsRef<Path>, F, T>(&mut self, path: P, f: F) -> Result<T>
    where
        F: FnOnce(&mut WD) -> Result<T>,
    {
        // eprintln!(
        //     "in_path {}:{}",
        //     self.pondpath("").display(),
        //     path.as_ref().display()
        // );
        let mut comp = path.as_ref().components();
        let first = comp.next();

        match first {
            None => f(self),

            Some(part) => {
                let one: String;
                if let Component::Normal(oss) = part {
                    one = oss.to_str().ok_or(anyhow!("invalid utf-8"))?.to_string();
                } else {
                    return Err(anyhow!("invalid path {:?}", part));
                }

                // @@@ NOT ALWAYS WANTING TO CREATE HERE
                self.subdir(&one)?.in_path(comp.as_path(), f)
            }
        }
    }

    pub fn subdir(&mut self, prefix: &str) -> Result<WD> {
        self.d().deref().borrow_mut().subdir(self.pond, prefix)
    }

    pub fn read_file<T: for<'b> Deserialize<'b>>(&mut self, prefix: &str) -> Result<Vec<T>> {
        match self.lookup(prefix) {
            None => Err(anyhow!(
                "file not found: {}",
                self.d().deref().borrow().pondpath(prefix).display()
            )),
            Some(entry) => file::read_file(
                self.d()
                    .deref()
                    .borrow_mut()
                    .realpath(self.pond, &entry)
                    .expect("real path needed"),
            ),
        }
    }

    pub fn realpath_current(&mut self, prefix: &str) -> Result<Option<PathBuf>> {
        self.d()
            .deref()
            .borrow_mut()
            .realpath_current(self.pond, prefix)
    }

    pub fn realpath_all(&mut self, prefix: &str) -> Vec<PathBuf> {
        self.d()
            .deref()
            .borrow_mut()
            .realpath_all(self.pond, prefix)
    }

    pub fn lookup(&mut self, prefix: &str) -> Option<DirEntry> {
        self.d().deref().borrow_mut().lookup(self.pond, prefix)
    }

    pub fn realpath_version(&mut self, prefix: &str, num: i32, ext: &str) -> Option<PathBuf> {
        self.d()
            .deref()
            .borrow_mut()
            .realpath_version(self.pond, prefix, num, ext)
    }

    pub fn copy_version_to<T: Write + Send>(
        &mut self,
        prefix: &str,
        numf: i32,
        ext: &str,
        to: T,
    ) -> Result<()> {
        self.d()
            .deref()
            .borrow_mut()
            .copy_version_to(self.pond, prefix, numf, ext, Box::new(to))
    }

    pub fn copy_to<T: Write + Send>(&mut self, ent: &DirEntry, to: T) -> Result<()> {
        self.copy_version_to(&ent.prefix, ent.number, ent.ftype.ext(), to)
    }

    pub fn check(&mut self) -> Result<()> {
        let entries =
            std::fs::read_dir(&self.d().deref().borrow().realpath_of()).with_context(|| {
                format!(
                    "could not read directory {}",
                    self.d().deref().borrow().realpath_of().display()
                )
            })?;

        let mut prefix_idxs: BTreeMap<String, BTreeSet<i32>> = BTreeMap::new();

        for entry_r in entries {
            let entry = entry_r?;
            let osname = entry.file_name();
            let name = osname.into_string();
            if let Err(_) = name {
                return Err(anyhow!("difficult to display an OS string! sorry!!"));
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
                }
                None => {
                    let mut t: BTreeSet<i32> = BTreeSet::new();
                    t.insert(num);
                    prefix_idxs.insert(v[0].to_string(), t);
                }
            }
        }

        for ent in self.d().deref().borrow_mut().entries(self.pond) {
            if let FileType::Tree = ent.ftype {
                continue;
            }
            // Build the set of existing verions by prefix
            match prefix_idxs.get_mut(ent.prefix.as_str()) {
                Some(exist) => {
                    if let Some(_found) = exist.get(&ent.number) {
                        exist.remove(&ent.number);
                    } else {
                        return Err(anyhow!(
                            "prefix {} number {} is missing",
                            ent.prefix,
                            ent.number
                        ));
                    }
                }
                None => {
                    return Err(anyhow!(
                        "unknown prefix {} number {}",
                        ent.prefix,
                        ent.number
                    ));
                }
            }
            // Verify sha256 and size
            let (hasher, size, _content) = file::sha256_file(
                self.d()
                    .deref()
                    .borrow_mut()
                    .realpath_version(self.pond, ent.prefix.as_str(), ent.number, ent.ftype.ext())
                    .expect("real path here"),
            )?;

            if size != ent.size {
                return Err(anyhow!(
                    "size mismatch {} (v{}): {} != {}",
                    ent.prefix,
                    ent.number,
                    size,
                    ent.size
                ));
            }
            let sha: [u8; 32] = hasher.finalize().into();
            if sha != ent.sha256 {
                return Err(anyhow!(
                    "sha256 mismatch {} (v{}): {} != {}",
                    ent.prefix,
                    ent.number,
                    hex::encode(sha),
                    hex::encode(ent.sha256)
                ));
            }
        }

        for leftover in &prefix_idxs {
            if *leftover.0 == "dir".to_string() {
                // TODO: @@@ this is not finished.
                continue;
            }
            if leftover.1.len() != 0 {
                for idx in leftover.1.iter() {
                    eprintln!(
                        "unexpected file {}.{}.parquet",
                        self.d()
                            .deref()
                            .borrow()
                            .realpath_of()
                            .join(leftover.0)
                            .display(),
                        idx
                    );
                }
            }
        }

        Ok(())
    }

    /// create_any_file is for ad-hoc structures
    pub fn create_any_file<F>(&mut self, prefix: &str, ftype: FileType, f: F) -> Result<()>
    where
        F: FnOnce(&File) -> Result<()>,
    {
        let seq: i32;
        if let Some(cur) = self.lookup(prefix) {
            seq = cur.number + 1;
        } else {
            seq = 1;
        }
        let newpath = self
            .d()
            .deref()
            .borrow_mut()
            .realpath_version(self.pond, prefix, seq, ftype.ext())
            .expect("real path here");
        let file = File::create_new(&newpath)
            .with_context(|| format!("could not open {}", newpath.display()))?;
        f(&file)?;

        self.d()
            .deref()
            .borrow_mut()
            .update(self.pond, prefix, &newpath, seq, ftype, None)
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
            seq = cur.number + 1;
        } else {
            seq = 1;
        }
        let newfile = self
            .realpath_version(prefix, seq, ftype.ext())
            .expect("real path here");
        let rlen = records.len();

        file::write_file(&newfile, records, T::for_arrow().as_slice())?;

        self.d()
            .deref()
            .borrow_mut()
            .update(self.pond, prefix, &newfile, seq, ftype, Some(rlen))
    }
}
