use serde::{Serialize, Deserialize};

use std::path::Path;
use std::fs::read_to_string;
use std::collections::BTreeMap;

use anyhow::{Context};

use crate::hydrovu::error::Error;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CRD<T> {
    pub api_version: String,
    pub kind: String,
    pub metadata: Option<BTreeMap<String,String>>,
    pub spec: T,
}

pub fn open<T: for<'a> Deserialize<'a>>(filename: &Path) -> Result<CRD<T>, Error> {
    let file = read_to_string(filename)
	.with_context(|| format!("could not read file {:?}", filename))?;
    let deser: CRD<T> = serde_yaml_ng::from_str(&file)
	.with_context(|| format!("could not parse yaml"))?;

    Ok(deser)
}

