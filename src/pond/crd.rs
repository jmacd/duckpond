use serde::{Serialize, Deserialize};

use std::sync::Arc;
use std::path::Path;
use std::fs::read_to_string;
use std::collections::BTreeMap;

use crate::pond::{ForPond,ForArrow};

use arrow::datatypes::{DataType, Field, Fields, FieldRef};

use anyhow::{Result, Context, Error};

// This file is part of a circular dependency mess.  See the match
// statement inside pond::apply() lists each spec type and calls into
// the respective module.  Hmm.

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HydroVuSpec {
    pub key: String,
    pub secret: String,
}

impl ForArrow for HydroVuSpec {
    fn for_arrow() -> Vec<FieldRef> {
	vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("secret", DataType::Utf8, false)),
	]
    }
}

impl ForPond for HydroVuSpec {
    fn spec_kind() -> &'static str {
	"HydroVu"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3BackupSpec {
    pub bucket: String,
    pub region: String,
    pub key: String,
    pub secret: String,
    pub endpoint: String,    
}

impl ForArrow for S3BackupSpec {
    fn for_arrow() -> Vec<FieldRef> {
	vec![
            Arc::new(Field::new("bucket", DataType::Utf8, false)),
            Arc::new(Field::new("region", DataType::Utf8, false)),
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("secret", DataType::Utf8, false)),
            Arc::new(Field::new("endpoint", DataType::Utf8, false)),
	]
    }
}

impl ForPond for S3BackupSpec {
    fn spec_kind() -> &'static str {
	"S3Backup"
    }

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScribbleSpec {
    pub count_min: i32,
    pub count_max: i32,
    pub probs: BTreeMap<String, f32>,
}

impl ForArrow for ScribbleSpec {
    fn for_arrow() -> Vec<FieldRef> {
	vec![
            Arc::new(Field::new("count_min", DataType::Int32, false)),
            Arc::new(Field::new("count_max", DataType::Int32, false)),
            Arc::new(Field::new("probs",
				DataType::Map(
				    Arc::new(
					Field::new("entries",
						   DataType::Struct(Fields::from(vec![
						       Field::new("key", DataType::Utf8, false),
						       Field::new("value", DataType::Float32, false),
						   ])),
						   false,
					)),
				    false),
				false)),
	]
    }
}

impl ForPond for ScribbleSpec {
    fn spec_kind() -> &'static str {
	"Scribble"
    }
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
    Scribble(CRD<ScribbleSpec>),
}

pub fn open<P: AsRef<Path>>(filename: P) -> Result<CRDSpec, Error> {
    let file = read_to_string(&filename)
	.with_context(|| format!("could not read file {}", filename.as_ref().display()))?;
    let deser = serde_yaml_ng::from_str(&file)
	.with_context(|| format!("could not parse yaml"))?;

    Ok(deser)
}

