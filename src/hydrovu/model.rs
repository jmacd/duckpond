use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

// LatLong is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatLong {
    pub latitude: f64,
    pub longitude: f64,
}

// LocationReadings is a batch of timeseries.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LocationReadings {
    pub location_id: i64,
    pub parameters: Vec<ParameterInfo>,
}

// ParameterInfo is a single timeseries.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParameterInfo {
    pub custom_parameter: bool,
    pub parameter_id: String,
    pub unit_id: String,
    pub readings: Vec<Reading>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reading {
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Mapping {
    pub index: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Vu {
    pub units: BTreeMap<String, String>,
    pub params: BTreeMap<String, String>,
    pub locations: Vec<Location>,
}
