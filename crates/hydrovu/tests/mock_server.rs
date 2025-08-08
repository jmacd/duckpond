use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::Filter;
use serde_json::Value;
use anyhow::{Result, Context};

/// Mock HydroVu server for testing
pub struct MockHydroVuServer {
    port: u16,
    test_data: TestData,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Test data loaded from JSON fixtures
#[derive(Debug, Clone)]
pub struct TestData {
    names: Value,
    locations: Value,
    device_data: HashMap<i64, Value>,
}

impl TestData {
    /// Load test data from the test_data directory
    pub async fn load() -> Result<Self> {
        let base_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/test_data");
        
        // Load names
        let names_path = base_path.join("names.json");
        let names = tokio::fs::read_to_string(&names_path).await
            .with_context(|| format!("Failed to read {:?}", names_path))?;
        let names: Value = serde_json::from_str(&names)
            .with_context(|| "Failed to parse names.json")?;
        
        // Load locations
        let locations_path = base_path.join("locations.json");
        let locations = tokio::fs::read_to_string(&locations_path).await
            .with_context(|| format!("Failed to read {:?}", locations_path))?;
        let locations: Value = serde_json::from_str(&locations)
            .with_context(|| "Failed to parse locations.json")?;
        
        // Load device data
        let mut device_data = HashMap::new();
        let device_data_dir = base_path.join("device_data");
        
        if device_data_dir.exists() {
            let mut entries = tokio::fs::read_dir(&device_data_dir).await
                .with_context(|| format!("Failed to read device data directory {:?}", device_data_dir))?;
            
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "json") {
                    let file_name = path.file_stem()
                        .and_then(|name| name.to_str())
                        .context("Invalid file name")?;
                    
                    // Extract device ID from filename (e.g., "device_123" -> 123)
                    if let Some(device_id_str) = file_name.strip_prefix("device_") {
                        if let Ok(device_id) = device_id_str.parse::<i64>() {
                            let content = tokio::fs::read_to_string(&path).await
                                .with_context(|| format!("Failed to read {:?}", path))?;
                            let data: Value = serde_json::from_str(&content)
                                .with_context(|| format!("Failed to parse {:?}", path))?;
                            device_data.insert(device_id, data);
                        }
                    }
                }
            }
        }
        
        Ok(TestData {
            names,
            locations,
            device_data,
        })
    }
}

impl MockHydroVuServer {
    /// Create a new mock server (but don't start it yet)
    pub async fn new() -> Result<Self> {
        let test_data = TestData::load().await?;
        
        Ok(MockHydroVuServer {
            port: 0, // Will be assigned when server starts
            test_data,
            server_handle: None,
        })
    }
    
    /// Start the mock server and return the base URL
    pub async fn start(&mut self) -> Result<String> {
        let test_data = Arc::new(Mutex::new(self.test_data.clone()));
        
        // OAuth token endpoint
        let oauth_token = warp::path!("oauth" / "token")
            .and(warp::post())
            .map(|| {
                warp::reply::json(&serde_json::json!({
                    "access_token": "mock_access_token_12345",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }))
            });
        
        // Names endpoint
        let test_data_names = test_data.clone();
        let names = warp::path!("public-api" / "v1" / "sispec" / "friendlynames")
            .and(warp::get())
            .and(warp::header::optional::<String>("authorization"))
            .and_then(move |auth_header: Option<String>| {
                let test_data = test_data_names.clone();
                async move {
                    // Check for authorization header with correct token
                    match auth_header {
                        Some(auth) if auth == "Bearer mock_access_token_12345" => {
                            // Valid token
                            let data = test_data.lock().await;
                            Ok(warp::reply::json(&data.names))
                        }
                        _ => {
                            // Missing or invalid token
                            Err(warp::reject::custom(UnauthorizedError))
                        }
                    }
                }
            });
        
        // Locations list endpoint  
        let test_data_locations = test_data.clone();
        let locations = warp::path!("public-api" / "v1" / "locations" / "list")
            .and(warp::get())
            .and(warp::header::optional::<String>("authorization"))
            .and_then(move |auth_header: Option<String>| {
                let test_data = test_data_locations.clone();
                async move {
                    // Check for authorization header with correct token
                    match auth_header {
                        Some(auth) if auth == "Bearer mock_access_token_12345" => {
                            // Valid token
                            let data = test_data.lock().await;
                            Ok(warp::reply::json(&data.locations))
                        }
                        _ => {
                            // Missing or invalid token
                            Err(warp::reject::custom(UnauthorizedError))
                        }
                    }
                }
            });
        
        // Location data endpoint with time filtering
        let test_data_device = test_data.clone();
        let location_data = warp::path!("public-api" / "v1" / "locations" / i64 / "data")
            .and(warp::get())
            .and(warp::query::<HashMap<String, String>>())
            .and(warp::header::optional::<String>("authorization"))
            .and_then(move |location_id: i64, params: HashMap<String, String>, auth_header: Option<String>| {
                let test_data = test_data_device.clone();
                async move {
                    // Check for authorization header with correct token
                    match auth_header {
                        Some(auth) if auth == "Bearer mock_access_token_12345" => {
                            // Valid token
                            let data = test_data.lock().await;
                            
                            if let Some(device_data) = data.device_data.get(&location_id) {
                                // Apply time filtering if requested
                                let filtered_data = apply_time_filter(device_data.clone(), &params)?;
                                Ok(warp::reply::json(&filtered_data))
                            } else {
                                // Return empty result for unknown devices
                                Ok(warp::reply::json(&serde_json::json!({
                                    "locationId": location_id,
                                    "parameters": []
                                })))
                            }
                        }
                        _ => {
                            // Missing or invalid token
                            Err(warp::reject::custom(UnauthorizedError))
                        }
                    }
                }
            });
        
        // Combine all routes
        let routes = oauth_token
            .or(names)
            .or(locations)
            .or(location_data)
            .recover(handle_rejection);
        
        // Start server on random port
        let (addr, server) = warp::serve(routes)
            .bind_ephemeral(([127, 0, 0, 1], 0));
        
        self.port = addr.port();
        
        // Spawn server task
        let handle = tokio::spawn(server);
        self.server_handle = Some(handle);
        
        let base_url = format!("http://127.0.0.1:{}", self.port);
        
        // Wait a bit for server to be ready
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        Ok(base_url)
    }
    
    /// Stop the mock server
    pub async fn stop(&mut self) {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for MockHydroVuServer {
    fn drop(&mut self) {
        if let Some(handle) = &self.server_handle {
            handle.abort();
        }
    }
}

/// Apply time filtering to device data based on query parameters
fn apply_time_filter(
    mut data: Value, 
    params: &HashMap<String, String>
) -> Result<Value, warp::Rejection> {
    let start_time = params.get("startTime")
        .and_then(|s| s.parse::<i64>().ok());
    let end_time = params.get("endTime")
        .and_then(|s| s.parse::<i64>().ok());
    
    if start_time.is_none() && end_time.is_none() {
        return Ok(data);
    }
    
    // Filter readings in each parameter
    if let Some(parameters) = data.get_mut("parameters").and_then(|p| p.as_array_mut()) {
        for parameter in parameters {
            if let Some(readings) = parameter.get_mut("readings").and_then(|r| r.as_array_mut()) {
                readings.retain(|reading| {
                    if let Some(timestamp) = reading.get("timestamp").and_then(|t| t.as_i64()) {
                        let after_start = start_time.map_or(true, |st| timestamp >= st);
                        let before_end = end_time.map_or(true, |et| timestamp <= et);
                        after_start && before_end
                    } else {
                        false
                    }
                });
            }
        }
    }
    
    Ok(data)
}

/// Custom error for unauthorized requests
#[derive(Debug)]
struct UnauthorizedError;
impl warp::reject::Reject for UnauthorizedError {}

/// Handle rejections and convert to proper HTTP responses
async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, std::convert::Infallible> {
    if err.find::<UnauthorizedError>().is_some() {
        Ok(warp::reply::with_status(
            "Unauthorized - Bearer token required",
            warp::http::StatusCode::UNAUTHORIZED,
        ))
    } else {
        Ok(warp::reply::with_status(
            "Internal Server Error",
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_mock_server_startup() {
        let mut server = MockHydroVuServer::new().await.unwrap();
        let base_url = server.start().await.unwrap();
        
        assert!(base_url.starts_with("http://127.0.0.1:"));
        assert!(server.port > 0);
        
        server.stop().await;
    }
    
    #[tokio::test]
    async fn test_load_test_data() {
        let test_data = TestData::load().await.unwrap();
        
        // Check that names were loaded
        assert!(test_data.names.get("parameters").is_some());
        assert!(test_data.names.get("units").is_some());
        
        // Check that locations were loaded  
        assert!(test_data.locations.as_array().is_some());
        
        // Check that device data was loaded
        assert!(test_data.device_data.contains_key(&123));
        assert!(test_data.device_data.contains_key(&456));
    }
}
