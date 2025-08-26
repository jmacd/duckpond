use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use diagnostics::*;

/// Configuration for HydroVu OAuth credentials and device list
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HydroVuConfig {
    pub client_id: String,
    pub client_secret: String,
    pub pond_path: String,
    pub hydrovu_path: String, // Path within pond for hydrovu data (e.g., "/hydrovu")
    pub max_rows_per_run: Option<usize>,
    pub devices: Vec<HydroVuDevice>,
}

/// Device configuration specifying which HydroVu location to collect
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HydroVuDevice {
    pub id: i64,
    pub name: String,
    pub scope: String,
    pub comment: Option<String>,
}

/// Names mapping returned by the HydroVu API
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Names {
    pub parameters: BTreeMap<String, String>,
    pub units: BTreeMap<String, String>,
}

/// Location data structure from HydroVu API
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Location {
    pub description: String,
    pub id: i64,
    pub name: String,
    pub gps: LatLong,
}

/// GPS coordinates
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatLong {
    pub latitude: f64,
    pub longitude: f64,
}

/// Location readings response from HydroVu API
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LocationReadings {
    pub location_id: i64,
    pub parameters: Vec<ParameterInfo>,
}

/// Parameter information with readings
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParameterInfo {
    pub custom_parameter: bool,
    pub parameter_id: String,
    pub unit_id: String,
    pub readings: Vec<Reading>,
}

/// Individual sensor reading
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Reading {
    pub timestamp: i64,  // Unix timestamp in milliseconds
    pub value: f64,
}

/// Internal mapping structure for units and parameters
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mapping {
    pub index: String,
    pub value: String,
}

/// Flattened reading structure for Arrow storage (individual parameter reading)
#[derive(Debug, Clone)]
pub struct FlattenedReading {
    pub timestamp: DateTime<Utc>,
    pub location_id: i64,
    pub parameter_id: String,
    pub parameter_name: String,
    pub unit_id: String,
    pub unit_name: String,
    pub value: f64,
    pub custom_parameter: bool,
}

/// Wide record structure for Arrow storage (joined by timestamp)
#[derive(Debug, Clone)]
pub struct WideRecord {
    pub timestamp: DateTime<Utc>,
    pub location_id: i64,
    pub parameters: BTreeMap<String, Option<f64>>, // parameter_id -> value (None if missing at this timestamp)
}

impl WideRecord {
    /// Convert from API response to timestamp-joined wide records
    /// This follows the original implementation's approach of joining by timestamp
    /// Uses original HydroVu naming convention: {scope}.{param_name}.{unit_name}
    pub fn from_location_readings(
        location_readings: &LocationReadings,
        units: &BTreeMap<String, String>,
        parameters: &BTreeMap<String, String>,
        device: &crate::HydroVuDevice, // Add device for scope information
    ) -> Vec<Self> {
        use std::collections::BTreeSet;
        
        // Count total raw readings for debugging
        let total_raw_readings: usize = location_readings
            .parameters
            .iter()
            .map(|p| p.readings.len())
            .sum();
        
        // Collect all unique timestamps from all parameters
        let all_timestamps: BTreeSet<i64> = location_readings
            .parameters
            .iter()
            .flat_map(|p| p.readings.iter().map(|r| r.timestamp))
            .collect();
            
        println!("DEBUG: WideRecord conversion: {} raw readings -> {} unique timestamps", 
                 total_raw_readings, all_timestamps.len());
        
        // Create wide records, one per timestamp
        let mut wide_records = Vec::new();
        
        for timestamp_ms in all_timestamps {
            let timestamp = DateTime::from_timestamp_millis(timestamp_ms)
                .unwrap_or_else(|| Utc::now());
            
            // Create a map for all parameter values at this timestamp
            let mut parameter_values = BTreeMap::new();
            
            // Fill in values from each parameter that has a reading at this timestamp
            for param_info in &location_readings.parameters {
                let value = param_info
                    .readings
                    .iter()
                    .find(|r| r.timestamp == timestamp_ms)
                    .map(|r| r.value);
                
                // Create column name using original HydroVu convention: {scope}.{param_name}.{unit_name}
                let param_name = parameters.get(&param_info.parameter_id)
                    .unwrap_or(&param_info.parameter_id); // Fall back to ID if not found in dictionary
                let unit_name = units.get(&param_info.unit_id)
                    .unwrap_or(&param_info.unit_id); // Fall back to ID if not found in dictionary
                
                let column_name = format!("{}.{}.{}", device.scope, param_name, unit_name);
                
                parameter_values.insert(column_name, value);
            }
            
            wide_records.push(WideRecord {
                timestamp,
                location_id: location_readings.location_id,
                parameters: parameter_values,
            });
        }
        
        wide_records
    }
}

impl FlattenedReading {
    /// Convert from API response to flattened structure
    pub fn from_location_readings(
        location_readings: &LocationReadings,
        units: &BTreeMap<String, String>,
        parameters: &BTreeMap<String, String>,
    ) -> Vec<Self> {
        let mut flattened = Vec::new();
        
        for param_info in &location_readings.parameters {
            let parameter_name = parameters
                .get(&param_info.parameter_id)
                .unwrap_or(&param_info.parameter_id)
                .clone();
            
            let unit_name = units
                .get(&param_info.unit_id)
                .unwrap_or(&param_info.unit_id)
                .clone();
            
            for reading in &param_info.readings {
                let timestamp = DateTime::from_timestamp_millis(reading.timestamp)
                    .unwrap_or_else(|| Utc::now());
                
                flattened.push(FlattenedReading {
                    timestamp,
                    location_id: location_readings.location_id,
                    parameter_id: param_info.parameter_id.clone(),
                    parameter_name: parameter_name.clone(),
                    unit_id: param_info.unit_id.clone(),
                    unit_name: unit_name.clone(),
                    value: reading.value,
                    custom_parameter: param_info.custom_parameter,
                });
            }
        }
        
        // Sort by timestamp
        flattened.sort_by_key(|r| r.timestamp);
        flattened
    }
}

/// Schema signature for detecting schema boundaries
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct SchemaSignature {
    pub parameter_ids: Vec<String>,
    pub unit_mapping: BTreeMap<String, String>, // parameter_id -> unit_id
}

impl SchemaSignature {
    /// Create schema signature from a group of readings
    pub fn from_readings(readings: &[FlattenedReading]) -> Self {
        let mut parameter_ids: Vec<String> = readings
            .iter()
            .map(|r| r.parameter_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        parameter_ids.sort();
        
        let unit_mapping: BTreeMap<String, String> = readings
            .iter()
            .map(|r| (r.parameter_id.clone(), r.unit_id.clone()))
            .collect();
        
        Self {
            parameter_ids,
            unit_mapping,
        }
    }
}

impl Default for HydroVuConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            pond_path: "./pond".to_string(),
            hydrovu_path: "/hydrovu".to_string(),
            max_rows_per_run: Some(1000),
            devices: Vec::new(),
        }
    }
}
