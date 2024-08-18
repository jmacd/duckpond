use crate::pond::copy::split_path;
use crate::pond::crd::InboxSpec;
use crate::pond::dir::FileType;
use crate::pond::file::sha256_file;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Result};

use sha2::Digest;
use std::fs::File;
use std::path::PathBuf;
use wax::Glob;

pub fn init_func(_wd: &mut WD, uspec: &UniqueSpec<InboxSpec>) -> Result<Option<InitContinuation>> {
    let uspec = uspec.clone();
    Ok(Some(Box::new(|pond| {
        let uspec = uspec;
        pond.in_path(uspec.dirpath(), |wd| {
            let uspec = uspec;
            inbox(wd, &uspec)
        })
    })))
}

fn new_inbox(pattern: &str) -> Result<(PathBuf, Glob)> {
    let (prefix, glob) = Glob::new(pattern)?.partition();

    let dp = prefix.as_path();
    if !dp.has_root() || !dp.is_dir() {
        Err(anyhow!(
            "directory should be an existing, absolute directory path: {}",
            dp.display()
        ))
    } else {
        Ok((prefix, glob))
    }
}

pub fn run(pond: &mut Pond, uspec: &UniqueSpec<InboxSpec>) -> Result<()> {
    pond.in_path(uspec.dirpath(), |wd| inbox(wd, uspec))
}

pub fn start(
    pond: &mut Pond,
    uspec: &UniqueSpec<InboxSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    start_noop(pond, uspec)
}

fn inbox(wd: &mut WD, uspec: &UniqueSpec<InboxSpec>) -> Result<()> {
    let (prefix, glob) = new_inbox(&uspec.spec.pattern)?;

    // TODO: Would be cool to walk in alphabetical order...?
    for entry in glob.walk(prefix) {
        let entry = entry?;

        let fullpath = entry.path();
        let md = std::fs::metadata(fullpath)?;
        if !md.is_file() {
            continue;
        }

        let relpath = entry.to_candidate_path();

        let (reldir, relbase) = split_path(relpath.as_ref())?;

        let mut infile = File::open(fullpath)?;

        wd.in_path(&reldir, |wd| {
            let exists = wd.lookup(&relbase);
            if let Some(ent) = exists {
                let realpath = wd.prefix_num_path(&relbase, ent.number, ent.ftype.ext());

                let (hasher, size, _content_opt) = sha256_file(&realpath)?;

                let cursha: [u8; 32] = hasher.finalize().into();

                if size == ent.size && cursha == ent.sha256 {
                    return Ok(());
                }
            }

            wd.create_any_file(&relbase, FileType::Data, |mut outfile| {
                let copied = std::io::copy(&mut infile, &mut outfile)?;

                if copied != md.len() {
                    Err(anyhow!(
                        "size mismatch: {} copied {} was {}",
                        fullpath.display(),
                        copied,
                        md.len()
                    ))
                } else {
                    eprintln!(
                        "copy '{}' size {copied} to '{}/{relbase}'",
                        fullpath.display(),
                        reldir.display()
                    );
                    Ok(())
                }
            })
        })?;
    }

    Ok(())
}
