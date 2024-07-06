use crate::pond::InitContinuation;
use crate::pond::wd::WD;
use crate::pond::dir::FileType;
use crate::pond::crd::ScribbleSpec;

use std::collections::BTreeMap;
use std::iter;
use rand::prelude::thread_rng;
use rand::Rng;

use anyhow::{Result, anyhow};

pub fn init_func(_wd: &mut WD, spec: &ScribbleSpec) -> Result<Option<InitContinuation>> {
    let spec = spec.clone();
    Ok(Some(Box::new(|pond| pond.in_path("", |wd| {
	let spec = spec;

	if spec.count_min < 1 || spec.count_max <= spec.count_min {
	    return Err(anyhow!("count range error"));
	}

	let mut map: BTreeMap<FileType, f32> = BTreeMap::new();

	for (t, p) in &spec.probs {
	    map.insert(FileType::try_from(t.clone())?, *p);
	}

	let mut cnts = FileType::into_iter().map(|x| (x, 0)).collect();

	scribble_recursive(wd, &spec, &map, &mut cnts, 0)?;

	eprintln!("Created {:?}", cnts);
	
	Ok(())
    }))))
}

fn scribble_recursive(wd: &mut WD, spec: &ScribbleSpec, map: &BTreeMap<FileType, f32>, cnts: &mut BTreeMap<FileType, i32>, depth: i32) -> Result<()> {
    let mut rng = thread_rng();

    for _ in 0..rng.gen_range(spec.count_min..=spec.count_max) {

	let mut ch: f32 = rng.gen();

	// Note that if the table of probabilities is not full, we get nothing.
	// Calling this WAI.
	for (ft, p) in map {
	    let mut prob = *p;
	    if *ft == FileType::Tree {
		prob /= depth as f32;
	    }
	    if prob < ch {
		ch -= prob;
		continue;
	    }
	    
	    let newname = generate(rng.gen_range(1..=16));
	    *cnts.get_mut(ft).unwrap() += 1;

	    match ft {
		FileType::Tree => wd.in_path(newname, |nd| scribble_recursive(nd, spec, map, cnts, depth+1))?,
		_ => (),
	    }
	}
    }
    Ok(())
}

fn generate(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
    iter::repeat_with(one_char).take(len).collect()
}
