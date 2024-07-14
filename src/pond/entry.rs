
use std::path::PathBuf;
use crate::pond::dir::DirEntry;
use crate::pond::writer;
use anyhow::Result;

pub fn write_dir(full: &PathBuf, v: &[DirEntry]) -> Result<()> {
    let mut wr = writer::Writer::new();

    for ent in v {
	wr.record(&ent)?;
    }
    
    wr.commit_to_local_file(full)
}
