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

// Vu is the metadata of a hydrovu account.
#[derive(Serialize, Deserialize, Debug)]
pub struct Vu {
    pub units: BTreeMap<i16, String>,
    pub params: BTreeMap<i16, String>,
    pub locations: Vec<Location>,
}

impl Vu {
    fn lookup_param_unit(&self, p: &ParameterInfo) -> Result<(String, String)> {
	Ok((self.params
	    .get(p.parameter_id)
	    .context("unknown parameter_id {}", p.parameter_id)?,
	    self.units
	    .get(p.unit_id)
	    .context("unknown unit_id {}", p.unit_id)?,
	))
    }
}
