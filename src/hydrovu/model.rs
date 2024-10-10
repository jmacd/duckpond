use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::pond::ForArrow;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use anyhow::{anyhow, Error};

// Names is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug)]
pub struct Names {
    pub parameters: BTreeMap<String, String>,
    pub units: BTreeMap<String, String>,
}

// Location is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Location {
    pub description: String,
    pub id: i64,
    pub name: String,
    pub gps: LatLong,
}

// ScopedLocation is a Location and scope name
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScopedLocation {
    #[serde(flatten)] 
    pub location: Location,
    pub scope: String,
}

impl ForArrow for ScopedLocation {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("description", DataType::Utf8, false)),
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("scope", DataType::Utf8, false)),
            Arc::new(Field::new(
                "gps",
                DataType::Struct(Fields::from(vec![
                    Field::new("latitude", DataType::Float64, false),
                    Field::new("longitude", DataType::Float64, false),
                ])),
                false,
            )),
        ]
    }
}

// LatLong is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatLong {
    pub latitude: f64,
    pub longitude: f64,
}

// LocationReadings is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LocationReadings {
    pub location_id: i64,
    pub parameters: Vec<ParameterInfo>,
}

// ParameterInfo is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParameterInfo {
    pub custom_parameter: bool,
    pub parameter_id: String,
    pub unit_id: String,
    pub readings: Vec<Reading>,
}

// ParameterInfo is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug)]
pub struct Reading {
    pub timestamp: i64,
    pub value: f64,
}

// Mapping is the internal representation of a unit or parameter mapping table, having
// discarded the non-integer values and checked for i16 compatibility.
#[derive(Serialize, Deserialize, Debug)]
pub struct Mapping {
    pub index: i16,
    pub value: String,
}

impl ForArrow for Mapping {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("index", DataType::Int16, false)),
            Arc::new(Field::new("value", DataType::Utf8, false)),
        ]
    }
}

// Vu is the metadata of a hydrovu account.
#[derive(Serialize, Deserialize, Debug)]
pub struct Vu {
    pub units: BTreeMap<i16, String>,
    pub params: BTreeMap<i16, String>,
    pub locations: Vec<ScopedLocation>,
}

// Temporal represents the time ranges that have been collected by location ID.
#[derive(Serialize, Deserialize, Debug)]
pub struct Temporal {
    pub location_id: i64,
    pub min_time: i64,
    pub max_time: i64,
    pub record_time: i64,
    pub num_points: i64,
}

impl ForArrow for Temporal {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("location_id", DataType::Int64, false)),
            Arc::new(Field::new("min_time", DataType::Int64, false)),
            Arc::new(Field::new("max_time", DataType::Int64, false)),
            Arc::new(Field::new("record_time", DataType::Int64, false)),
            Arc::new(Field::new("num_points", DataType::Int64, false)),
        ]
    }
}

impl Vu {
    pub fn lookup_param_unit(&self, p: &ParameterInfo) -> Result<(String, String), Error> {
        let param = self
            .params
            .get(&p.parameter_id.parse::<i16>()?)
            .ok_or(anyhow!("unknown parameter_id {}", p.parameter_id))?;
        let unit = self
            .units
            .get(&p.unit_id.parse::<i16>()?)
            .ok_or(anyhow!("unknown unit_id {}", p.unit_id))?;
        Ok((param.to_string(), unit.to_string()))
    }
}
