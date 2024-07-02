use serde::{Serialize, Deserialize};

use std::path::Path;
use std::fs::read_to_string;
use std::collections::BTreeMap;

use anyhow::{Result, Context, Error};

// This file is a circular dependency mess.

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HydroVuSpec {
    pub key: String,
    pub secret: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3BackupSpec {
    pub bucket: String,
    pub key: String,
    pub secret: String,
    pub endpoint: String,    
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CRD<T>  {
    pub api_version: String,
    pub name: String,
    pub desc: String,
    pub metadata: Option<BTreeMap<String,String>>,
    pub spec: T,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind")]
pub enum CRDSpec {
    HydroVu(CRD<HydroVuSpec>),
    S3Backup(CRD<S3BackupSpec>),
}

pub fn open<P: AsRef<Path>>(filename: P) -> Result<CRDSpec, Error> {
    let file = read_to_string(&filename)
	.with_context(|| format!("could not read file {}", filename.as_ref().display()))?;
    let deser = serde_yaml_ng::from_str(&file)
	.with_context(|| format!("could not parse yaml"))?;

    Ok(deser)
}

