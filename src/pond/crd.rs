use serde::{Serialize, Deserialize};

use std::path::Path;
use std::fs::read_to_string;
use std::collections::BTreeMap;

use anyhow::{Result, Context, Error};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HydrovuSpec {
    key: String,
    secret: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CRD<T>  {
    pub api_version: String,
    pub name: String,
    pub desc: String,
    pub metadata: Option<BTreeMap<String,String>>,
    pub spec: T,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum CRDSpec {
    HydroVu(CRD<HydrovuSpec>),
}

pub fn open<P: AsRef<Path>>(filename: P) -> Result<CRDSpec, Error> {
    let file = read_to_string(&filename)
	.with_context(|| format!("could not read file {}", filename.as_ref().display()))?;
    let deser = serde_yaml_ng::from_str(&file)
	.with_context(|| format!("could not parse yaml"))?;

    Ok(deser)
}

