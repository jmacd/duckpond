use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::wd::WD;
use crate::pond::crd::InboxSpec;
use crate::pond::writer::MultiWriter;

use anyhow::{Result, anyhow};

use std::path::PathBuf;

use wax::Glob;

pub fn init_func(_wd: &mut WD, spec: &mut UniqueSpec<InboxSpec>) -> Result<Option<InitContinuation>> {
    let spec = spec.clone();
    Ok(Some(Box::new(|pond| pond.in_path(spec.dirpath(), |wd| inbox(wd, spec)))))
}

fn new_inbox(pattern: &str) -> Result<(PathBuf, Glob)> {
    let (prefix, glob) = Glob::new(pattern)?.partition();

    let dp = prefix.as_path();
    if !dp.has_root() || !dp.is_dir() {
	Err(anyhow!("directory should be an existing, absolute directory path: {}",
		    dp.display()))
    } else {
	eprintln!("p {:?} g {:?}", prefix, glob);

	Ok((prefix, glob))
    }
}

fn inbox(_wd: &mut WD, uspec: UniqueSpec<InboxSpec>) -> Result<()> {
    let (_prefix, _glob) = new_inbox(&uspec.spec.pattern)?;

    Ok(())
}

pub fn run(_wd: &mut WD, uspec: &UniqueSpec<InboxSpec>) -> Result<()> {
    let (prefix, glob) = new_inbox(&uspec.spec.pattern)?;

    eprintln!("prefix: {:?}", &uspec.spec.pattern);
    
    for entry in glob.walk(prefix) {
	let entry = entry?;

	// TODO
	eprintln!("inbox matched: {:?}", entry);
    }
    
    Ok(())
}

pub fn start(_pond: &mut Pond, _uspec: &UniqueSpec<InboxSpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {
    Ok(Box::new(|_pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	Ok(Box::new(|_| -> Result<()> { Ok(()) }))
    }))
}
