use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::path::Path;
use std::sync::Arc;

use crate::pond::{ForArrow, ForPond};

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use anyhow::{Context, Error, Result};

use tera;

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
pub struct S3Fields {
    pub bucket: String,
    pub region: String,
    pub key: String,
    pub secret: String,
    pub endpoint: String,
}

impl ForArrow for S3Fields {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackupSpec {
    #[serde(flatten)]
    pub s3: S3Fields,
}

impl ForArrow for BackupSpec {
    fn for_arrow() -> Vec<FieldRef> {
        // let mut fields = S3Fields::for_arrow();
        // fields.extend(vec![
        //     Arc::new(Field::new("pattern", DataType::Utf8, false)),
        // ]);
        // fields
        S3Fields::for_arrow()
    }
}

impl ForPond for BackupSpec {
    fn spec_kind() -> &'static str {
        "Backup"
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
            Arc::new(Field::new(
                "probs",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Float32, false),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            )),
        ]
    }
}

impl ForPond for ScribbleSpec {
    fn spec_kind() -> &'static str {
        "Scribble"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CopySpec {
    #[serde(flatten)]
    pub s3: S3Fields,

    pub backup_uuid: String,
}

impl ForArrow for CopySpec {
    fn for_arrow() -> Vec<FieldRef> {
        let mut fields: Vec<FieldRef> = Vec::new();
        fields.extend(S3Fields::for_arrow());
        fields.push(Arc::new(Field::new("backup_uuid", DataType::Utf8, false)));
        fields
    }
}

impl ForPond for CopySpec {
    fn spec_kind() -> &'static str {
        "Copy"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InboxSpec {
    pub pattern: String,
}

impl ForArrow for InboxSpec {
    fn for_arrow() -> Vec<FieldRef> {
        vec![Arc::new(Field::new("pattern", DataType::Utf8, false))]
    }
}

impl ForPond for InboxSpec {
    fn spec_kind() -> &'static str {
        "Inbox"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeriveSpec {
    pub collections: Vec<DeriveCollection>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeriveCollection {
    pub pattern: String,
    pub name: String,
    pub query: String,
}

impl ForArrow for DeriveCollection {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("pattern", DataType::Utf8, false)),
            Arc::new(Field::new("query", DataType::Utf8, false)),
        ]
    }
}

impl ForArrow for DeriveSpec {
    fn for_arrow() -> Vec<FieldRef> {
        vec![Arc::new(Field::new(
            "collections",
            DataType::List(Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(DeriveCollection::for_arrow())),
                false,
            ))),
            false,
        ))]
    }
}

impl ForPond for DeriveSpec {
    fn spec_kind() -> &'static str {
        "Derive"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CRD<T> {
    pub api_version: String,
    pub name: String,
    pub desc: String,
    pub metadata: Option<BTreeMap<String, String>>,
    pub spec: T,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind")]
pub enum CRDSpec {
    HydroVu(CRD<HydroVuSpec>),
    Backup(CRD<BackupSpec>),
    Copy(CRD<CopySpec>),
    Scribble(CRD<ScribbleSpec>),
    Inbox(CRD<InboxSpec>),
    Derive(CRD<DeriveSpec>),
}

pub fn open<P: AsRef<Path>>(filename: P, vars: &Vec<(String, String)>) -> Result<CRDSpec, Error> {
    let file = read_to_string(&filename)
        .with_context(|| format!("could not read file {}", filename.as_ref().display()))?;

    let mut ctx = tera::Context::new();
    for (k, v) in vars {
        ctx.insert(k, v);
    }
    let expanded = tera::Tera::one_off(&file, &ctx, false)?;

    let deser =
        serde_yaml_ng::from_str(&expanded).with_context(|| format!("could not parse yaml"))?;

    Ok(deser)
}
