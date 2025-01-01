use crate::pond::{ForArrow, ForPond, ForTera};

use anyhow::{Context, Error, Result};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::path::Path;
use std::sync::Arc;
use std::iter;
use tera;

// This file is part of a circular dependency mess.  See the match
// statement inside pond::apply() lists each spec type and calls into
// the respective module.  Hmm.

// HydroVu

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HydroVuSpec {
    pub key: String,
    pub secret: String,
    pub devices: Vec<HydroVuDevice>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HydroVuDevice {
    pub id: i64,
    pub name: String,
    pub scope: String,
    pub comment: Option<String>,
}

impl ForArrow for HydroVuSpec {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("secret", DataType::Utf8, false)),
            Arc::new(Field::new(
                "devices",
                DataType::List(Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(HydroVuDevice::for_arrow())),
                    false,
                ))),
                false,
            )),
        ]
    }
}

impl ForArrow for HydroVuDevice {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("scope", DataType::Utf8, false)),
            Arc::new(Field::new("comment", DataType::Utf8, true)),
        ]
    }
}

impl ForPond for HydroVuSpec {
    fn spec_kind() -> &'static str {
        "HydroVu"
    }
}

impl ForTera for HydroVuSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.devices.iter_mut().map(|x: &mut HydroVuDevice| x.for_tera()).flatten()
	    .chain(iter::once(&mut self.key))
	    .chain(iter::once(&mut self.secret))
    }
}

impl ForTera for HydroVuDevice {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![
	    &mut self.name,
	    &mut self.scope,
	].into_iter()
    }
}

// S3Fields

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

impl ForTera for S3Fields {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![
	    &mut self.bucket,
	    &mut self.region,
	    &mut self.key,
	    &mut self.secret,
	    &mut self.endpoint,
	].into_iter()
    }    
}

// Backup

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackupSpec {
    #[serde(flatten)]
    pub s3: S3Fields,
}

impl ForArrow for BackupSpec {
    fn for_arrow() -> Vec<FieldRef> {
        S3Fields::for_arrow()
    }
}

impl ForPond for BackupSpec {
    fn spec_kind() -> &'static str {
        "Backup"
    }
}

impl ForTera for BackupSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.s3.for_tera()
    }    
}

// Scribble

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

impl ForTera for ScribbleSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	iter::empty()
    }    
}

// Copy

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

impl ForTera for CopySpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.s3.for_tera()
	    .chain(iter::once(&mut self.backup_uuid))
    }    
}

// Inbox

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

impl ForTera for InboxSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![&mut self.pattern].into_iter()
    }    
}

// Derive

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

impl ForTera for DeriveSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.collections.iter_mut().map(|x| x.for_tera()).flatten()
    }    
}

impl ForTera for DeriveCollection {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![
	    &mut self.pattern,
	    &mut self.name,
	    &mut self.query,
	].into_iter()
    }    
}

// Combine

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CombineSpec {
    pub scopes: Vec<CombineScope>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CombineScope {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub series: Vec<CombineSeries>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CombineSeries {
    pub pattern: String,
}

impl ForArrow for CombineSpec {
    fn for_arrow() -> Vec<FieldRef> {
        vec![Arc::new(Field::new(
            "scopes",
            DataType::List(Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(CombineScope::for_arrow())),
                false,
            ))),
            false,
        ))]
    }
}

impl ForArrow for CombineScope {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new(
                "series",
                DataType::List(Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(CombineSeries::for_arrow())),
                    false,
                ))),
                false,
            )),
            Arc::new(Field::new(
                "columns",
                DataType::List(Arc::new(Field::new("entries", DataType::Utf8, false))),
                true,
            )),
        ]
    }
}

impl ForArrow for CombineSeries {
    fn for_arrow() -> Vec<FieldRef> {
        vec![Arc::new(Field::new("pattern", DataType::Utf8, false))]
    }
}

impl ForPond for CombineSpec {
    fn spec_kind() -> &'static str {
        "Combine"
    }
}

impl ForTera for CombineSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.scopes.iter_mut().map(|x| x.for_tera())
	    .flatten()
    }    
}

impl ForTera for CombineScope {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.series.iter_mut().map(|x| x.for_tera())
	    .flatten()
	    .chain(self.columns.iter_mut().map(|x| x.iter_mut()).flatten())
    }    
}

impl ForTera for CombineSeries {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![&mut self.pattern].into_iter()
    }
}

// Template

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSpec {
    pub collections: Vec<TemplateCollection>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateCollection {
    pub in_pattern: String, // this is a glob w/ wildcard exprs
    pub out_pattern: String, // this has numbered placeholders
    pub name: String,
    pub template: String,
}

impl ForArrow for TemplateCollection {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("in_pattern", DataType::Utf8, false)),
            Arc::new(Field::new("out_pattern", DataType::Utf8, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("template", DataType::Utf8, false)),
        ]
    }
}

impl ForArrow for TemplateSpec {
    fn for_arrow() -> Vec<FieldRef> {
        vec![Arc::new(Field::new(
            "collections",
            DataType::List(Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(TemplateCollection::for_arrow())),
                false,
            ))),
            false,
        ))]
    }
}

impl ForPond for TemplateSpec {
    fn spec_kind() -> &'static str {
        "Template"
    }
}

impl ForTera for TemplateSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.collections.iter_mut().map(|x| x.for_tera())
	    .flatten()
    }    
}

impl ForTera for TemplateCollection {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![
	    &mut self.in_pattern,
	    &mut self.out_pattern,
	    &mut self.name,
	    // self.template is excluded, to avoid twice templating
	].into_iter()
    }    
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CRD<T: ForTera> {
    pub api_version: String,
    pub name: String,
    pub desc: String,
    pub metadata: Option<BTreeMap<String, String>>,
    pub spec: T,
}

impl<T: ForTera> ForTera for CRD<T> {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![
	    &mut self.api_version,
	    &mut self.name,
	    &mut self.desc,
	    // Note: missing metadata
	].into_iter().chain(self.spec.for_tera())
    }
}

// Reduce

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReduceSpec {
    // in_pattern has globs.
    pub in_pattern: String,
    // out_pattern has number placeholders.
    pub out_pattern: String,

    // min_points is the minimum number of timestamped rows per file
    // unit at the leaf of the partitioned data.  e.g., set to
    // min=150, max=400:
    //
    // res=24h is 365 points/year => year is the partition leaf (
    // res=12h is 730 points/year => quarter is the partition leaf (182 pts/quarter)
    // res=1h is 365*24 points/year => 
    pub min_points: u32,
    pub max_points: u32,

    pub datasets: Vec<ReduceCollection>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReduceCollection {
    // e.g. 1d, 1h, etc.
    pub resolution: String,

    // these are SQL functions, e.g., avg().
    pub query: String,
}

impl ForArrow for ReduceCollection {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("resolution", DataType::Utf8, false)),
            Arc::new(Field::new("query", DataType::Utf8, false)),
        ]
    }
}

impl ForArrow for ReduceSpec {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("in_pattern", DataType::Utf8, false)),
            Arc::new(Field::new("out_pattern", DataType::Utf8, false)),
            Arc::new(Field::new("min_points", DataType::UInt32, false)),
            Arc::new(Field::new("max_points", DataType::UInt32, false)),
	    Arc::new(Field::new(
		"datasets",
		DataType::List(Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(ReduceCollection::for_arrow())),
                    false,
		))),
		false,
            )),
	]
    }
}

impl ForPond for ReduceSpec {
    fn spec_kind() -> &'static str {
        "Reduce"
    }
}

impl ForTera for ReduceSpec {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	self.datasets.iter_mut().map(|x| x.for_tera()).flatten()
    }    
}

impl ForTera for ReduceCollection {
    fn for_tera(&mut self) -> impl Iterator<Item = &mut String> {
	vec![
	    &mut self.resolution,
	    &mut self.query,
	].into_iter()
    }    
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
    Combine(CRD<CombineSpec>),
    Template(CRD<TemplateSpec>),
    Reduce(CRD<ReduceSpec>),
}

impl CRDSpec {
    // Note: would need to use Box for the variable-types of iterator
    // returned for each spec type, not an impl trait.  Passing in a
    // function doesn't work because you can't have impl traits on bound
    // function parameters.  Therefore we collect into a Vec<&mut String>.
    
    fn expand<F>(&mut self, f: F) -> Result<()>
    where
	F: Fn(Vec<&mut String>) -> Result<()>
    {
	match self {
	    CRDSpec::HydroVu(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Backup(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Copy(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Scribble(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Inbox(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Derive(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Combine(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Template(spec) => f(spec.for_tera().collect()),
	    CRDSpec::Reduce(spec) => f(spec.for_tera().collect()),
	}
    }
}

pub fn open<P: AsRef<Path>>(filename: P, vars: &Vec<(String, String)>) -> Result<CRDSpec, Error> {
    let file = read_to_string(&filename)
        .with_context(|| format!("could not read file {}", filename.as_ref().display()))?;

    let mut deser: CRDSpec =
        serde_yaml_ng::from_str(&file).with_context(|| format!("could not parse yaml"))?;

    let mut ctx = tera::Context::new();
    for (k, v) in vars {
        ctx.insert(k, v);
    }

    deser.expand(|x| {
	for p  in x {
	    *p = tera::Tera::one_off(p, &ctx, false)?;
	}
	Ok(())
    })?;
    
    Ok(deser)
}
