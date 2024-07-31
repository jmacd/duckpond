use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::wd::WD;
use crate::pond::crd::InboxSpec;
use crate::pond::file::sha256_file;
use crate::pond::writer::MultiWriter;

use anyhow::{Result, anyhow};

use std::path::PathBuf;

use sha2::Digest;
use wax::Glob;

use hex;

pub fn init_func(_wd: &mut WD, uspec: &mut UniqueSpec<InboxSpec>) -> Result<Option<InitContinuation>> {
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
	Err(anyhow!("directory should be an existing, absolute directory path: {}",
		    dp.display()))
    } else {
	Ok((prefix, glob))
    }
}

pub fn run(wd: &mut WD, uspec: &UniqueSpec<InboxSpec>) -> Result<()> {
    inbox(wd, uspec)
}

pub fn start(_pond: &mut Pond, _uspec: &UniqueSpec<InboxSpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {
    Ok(Box::new(|_pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	Ok(Box::new(|_| -> Result<()> { Ok(()) }))
    }))
}

fn inbox(_wd: &mut WD, uspec: &UniqueSpec<InboxSpec>) -> Result<()> {
    let (prefix, glob) = new_inbox(&uspec.spec.pattern)?;

    for entry in glob.walk(prefix) {
	let entry = entry?;
	let relp = entry.to_candidate_path();
	let path = entry.path();
	let frag = relp.as_ref();

	let md = std::fs::metadata(path)?;
	if !md.is_file() {
	    continue;
	}

	let (dig, sz) = sha256_file(path)?;
	let digest: [u8; 32] = dig.finalize().into();

	eprintln!("inbox matched file: {} : {} : {}", frag, sz, hex::encode(&digest));

	// TODO HERE YOU ARE
	// check for existing entry under wd
	// if exists, continue
	// otherwise, make new filetype
	// multipart upload??
	// expecting large files, how does the update work?
	// how does the writer see it?
    }
    
    Ok(())
}

