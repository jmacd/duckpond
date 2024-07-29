use crate::pond::Pond;
use crate::pond::InitContinuation;
use crate::pond::UniqueSpec;
use crate::pond::wd::WD;
use crate::pond::crd::InboxSpec;
use crate::pond::writer::MultiWriter;

use anyhow::Result;

pub fn init_func(_wd: &mut WD, spec: &mut UniqueSpec<InboxSpec>) -> Result<Option<InitContinuation>> {
    let spec = spec.clone();
    Ok(Some(Box::new(|pond| pond.in_path(spec.dirpath(), |wd| inbox(wd, spec)))))
}

fn inbox(_wd: &mut WD, _uspec: UniqueSpec<InboxSpec>) -> Result<()> {
    // let uspec = uspec;
    
    // if uspec.spec.count_min < 1 || uspec.spec.count_max <= uspec.spec.count_min {
    // 	return Err(anyhow!("count range error"));
    // }

    // let mut map: BTreeMap<FileType, f32> = BTreeMap::new();

    // for (t, p) in &uspec.spec.probs {
    // 	map.insert(FileType::try_from(t.clone())?, *p);
    // }

    // let mut cnts = FileType::into_iter().map(|x| (x, 0)).collect();

    //inbox_recursive(wd, &uspec.spec, &map, &mut cnts, 0)?;

    //eprintln!("Created {:?}", cnts);
	
    Ok(())
}

pub fn run(_wd: &mut WD, _spec: &UniqueSpec<InboxSpec>) -> Result<()> {
    Ok(())
}

pub fn start(_pond: &mut Pond, _uspec: &UniqueSpec<InboxSpec>) -> Result<Box<dyn for <'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>>> {
    Ok(Box::new(|_pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
	Ok(Box::new(|_| -> Result<()> { Ok(()) }))
    }))
}
