use crate::pond::crd::ScribbleSpec;
use crate::pond::dir::FileType;
use crate::pond::start_noop;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::ForArrow;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use serde::{Deserialize, Serialize};

use rand::prelude::thread_rng;
use rand::Rng;
use std::collections::BTreeMap;
use std::iter;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use arrow::datatypes::{DataType, Field, FieldRef};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScribbleData {
    pub name: String,
    pub region: String,
    pub quota: i32,
}

impl ForArrow for ScribbleData {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("region", DataType::Utf8, false)),
            Arc::new(Field::new("quota", DataType::Int32, false)),
        ]
    }
}

pub fn init_func(
    _wd: &mut WD,
    spec: &UniqueSpec<ScribbleSpec>,
) -> Result<Option<InitContinuation>> {
    let spec = spec.clone();
    Ok(Some(Box::new(|pond| {
        pond.in_path(spec.dirpath(), |wd| scribble(wd, spec))
    })))
}

fn scribble(wd: &mut WD, uspec: UniqueSpec<ScribbleSpec>) -> Result<()> {
    let uspec = uspec;

    if uspec.spec.count_min < 1 || uspec.spec.count_max <= uspec.spec.count_min {
        return Err(anyhow!("count range error"));
    }

    let mut map: BTreeMap<FileType, f32> = BTreeMap::new();

    for (t, p) in &uspec.spec.probs {
        map.insert(FileType::try_from(t.clone())?, *p);
    }

    let mut cnts = FileType::into_iter().map(|x| (x, 0)).collect();

    scribble_recursive(wd, &uspec.spec, &map, &mut cnts, 0)?;

    //eprintln!("Created {:?}", cnts);

    Ok(())
}

fn scribble_recursive(
    wd: &mut WD,
    spec: &ScribbleSpec,
    map: &BTreeMap<FileType, f32>,
    cnts: &mut BTreeMap<FileType, i32>,
    depth: i32,
) -> Result<()> {
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
                FileType::Tree => wd.in_path(newname, |nd| {
                    scribble_recursive(nd, spec, map, cnts, depth + 1)
                })?,
                FileType::Table => {
                    let mut data: Vec<ScribbleData> = Vec::new();

                    for _ in spec.count_min..=spec.count_max {
                        data.push(ScribbleData {
                            name: format!("{}", rng.gen_range(0..256)),
                            region: format!("{}", rng.gen_range(0..256)),
                            quota: rng.gen_range(0..256),
                        });
                    }
                    //eprintln!("Scribble new file {}/{}.{:?}", wd.d.relp.display(), newname, ft);
                    wd.write_whole_file(newname.as_str(), FileType::Table, &data)?;
                }
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

pub fn run(pond: &mut Pond, spec: &UniqueSpec<ScribbleSpec>) -> Result<()> {
    pond.in_path(spec.dirpath(), |wd| scribble(wd, spec.clone()))
}

pub fn start(
    pond: &mut Pond,
    uspec: &UniqueSpec<ScribbleSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    start_noop(pond, uspec)
}
