use anyhow::Result;
use hydrovu::{HydroVuConfig, HydroVuDevice};
use hydrovu::models::{WideRecord, FlattenedReading};

mod mock_server;
use mock_server::MockHydroVuServer;

/// Test client authentication with mock server
#[tokio::test]
async fn test_client_authentication() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    // Override the client to use mock server URL
    let client = create_test_client(&base_url).await?;
    
    // Test authentication
    client.test_authentication().await?;
    
    mock_server.stop().await;
    Ok(())
}

/// Test fetching names (parameter/unit dictionaries)
#[tokio::test]
async fn test_fetch_names() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    let names = client.fetch_names().await?;
    
    // Verify expected parameters and units from test data
    assert!(names.parameters.contains_key("1"));
    assert_eq!(names.parameters.get("1").unwrap(), "Temperature");
    
    assert!(names.units.contains_key("1"));
    assert_eq!(names.units.get("1").unwrap(), "°C");
    
    mock_server.stop().await;
    Ok(())
}

/// Test fetching locations list
#[tokio::test]
async fn test_fetch_locations() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    let locations = client.fetch_locations().await?;
    
    // Verify expected locations from test data
    assert_eq!(locations.len(), 2);
    
    let location_123 = locations.iter().find(|l| l.id == 123).unwrap();
    assert_eq!(location_123.name, "Test Station 1");
    assert_eq!(location_123.gps.latitude, 40.7128);
    
    mock_server.stop().await;
    Ok(())
}

/// Test fetching device data (complete scenario)
#[tokio::test]
async fn test_fetch_device_data_complete() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    
    // Fetch names for parameter/unit mapping
    let names = client.fetch_names().await?;
    
    // Fetch data for device 123
    let location_readings = client.fetch_location_data(123, 1609459200000, None).await?;
    
    // Verify basic structure
    assert_eq!(location_readings.location_id, 123);
    assert_eq!(location_readings.parameters.len(), 3); // Temperature, pH, Dissolved Oxygen
    
    // Verify specific parameter data
    let temp_param = location_readings.parameters.iter()
        .find(|p| p.parameter_id == "1")
        .unwrap();
    assert_eq!(temp_param.unit_id, "1");
    assert_eq!(temp_param.readings.len(), 5);
    assert_eq!(temp_param.readings[0].value, 20.5);
    
    // Test flattened conversion
    let flattened = FlattenedReading::from_location_readings(
        &location_readings, 
        &names.units, 
        &names.parameters
    );
    
    // Should have 15 readings total (3 parameters × 5 timestamps)
    assert_eq!(flattened.len(), 15);
    
    // Verify first reading details
    let first_reading = &flattened[0];
    assert_eq!(first_reading.location_id, 123);
    assert_eq!(first_reading.parameter_name, "Temperature");
    assert_eq!(first_reading.unit_name, "°C");
    assert_eq!(first_reading.value, 20.5);
    
    mock_server.stop().await;
    Ok(())
}

/// Test fetching device data with schema evolution (parameter changes)
#[tokio::test]
async fn test_fetch_device_data_schema_evolution() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    let names = client.fetch_names().await?;
    
    // Fetch data for device 456 (has schema evolution scenario)
    let location_readings = client.fetch_location_data(456, 1609459200000, None).await?;
    
    assert_eq!(location_readings.location_id, 456);
    assert_eq!(location_readings.parameters.len(), 3); // Temperature, Conductivity, Turbidity
    
    // Convert to flattened readings
    let flattened = FlattenedReading::from_location_readings(
        &location_readings, 
        &names.units, 
        &names.parameters
    );
    
    // Verify different parameters appear at different timestamps (schema evolution)
    let temp_readings: Vec<_> = flattened.iter()
        .filter(|r| r.parameter_id == "1")
        .collect();
    assert_eq!(temp_readings.len(), 3); // Temperature at all 3 timestamps
    
    let conductivity_readings: Vec<_> = flattened.iter()
        .filter(|r| r.parameter_id == "4") 
        .collect();
    assert_eq!(conductivity_readings.len(), 2); // Conductivity at only 2 timestamps
    
    let turbidity_readings: Vec<_> = flattened.iter()
        .filter(|r| r.parameter_id == "5")
        .collect();
    assert_eq!(turbidity_readings.len(), 2); // Turbidity at only 2 timestamps (different ones)
    
    mock_server.stop().await;
    Ok(())
}

/// Test wide record conversion (timestamp joining)
#[tokio::test]
async fn test_wide_record_conversion() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    let names = client.fetch_names().await?;
    
    // Fetch data for device 123
    let location_readings = client.fetch_location_data(123, 1609459200000, None).await?;
    
    // Create device info for wide record conversion
    let device = HydroVuDevice {
        id: 123,
        name: "Test Station 1".to_string(),
        scope: "test".to_string(),
        comment: None,
    };
    
    // Convert to wide records
    let wide_records = WideRecord::from_location_readings(
        &location_readings,
        &names.units,
        &names.parameters,
        &device,
    );
    
    // Should have 5 wide records (one per timestamp)
    assert_eq!(wide_records.len(), 5);
    
    // Verify first record structure
    let first_record = &wide_records[0];
    assert_eq!(first_record.location_id, 123);
    assert_eq!(first_record.parameters.len(), 3); // All 3 parameters should be present
    
    // Verify parameter naming: {scope}.{param_name}.{unit_name}
    assert!(first_record.parameters.contains_key("test.Temperature.°C"));
    assert!(first_record.parameters.contains_key("test.pH.pH units"));
    assert!(first_record.parameters.contains_key("test.Dissolved Oxygen.mg/L"));
    
    // Verify values
    assert_eq!(first_record.parameters.get("test.Temperature.°C").unwrap().unwrap(), 20.5);
    assert_eq!(first_record.parameters.get("test.pH.pH units").unwrap().unwrap(), 7.2);
    assert_eq!(first_record.parameters.get("test.Dissolved Oxygen.mg/L").unwrap().unwrap(), 8.5);
    
    mock_server.stop().await;
    Ok(())
}

/// Test time filtering
#[tokio::test]
async fn test_time_filtering() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    
    // Fetch with time range that should limit results
    let start_time = 1609459260000; // Second timestamp
    let end_time = 1609459320000;   // Third timestamp
    
    let location_readings = client.fetch_location_data(123, start_time, Some(end_time)).await?;
    
    // Should only have readings within the time range
    for param in &location_readings.parameters {
        for reading in &param.readings {
            assert!(reading.timestamp >= start_time);
            assert!(reading.timestamp <= end_time);
        }
        
        // Should have exactly 2 readings per parameter (timestamps 2 and 3)
        assert_eq!(param.readings.len(), 2);
    }
    
    mock_server.stop().await;
    Ok(())
}

/// Test error handling - unauthorized request
#[tokio::test] 
async fn test_unauthorized_request() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let reqwest_client = reqwest::Client::new();
    let url = format!("{}/public-api/v1/sispec/friendlynames", base_url);
    
    // Request without authorization header should fail
    let response = reqwest_client.get(&url).send().await?;
    assert!(!response.status().is_success());
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    
    // Request with invalid token should also fail
    let response = reqwest_client
        .get(&url)
        .bearer_auth("invalid_token_123")
        .send()
        .await?;
    assert!(!response.status().is_success());
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    
    mock_server.stop().await;
    Ok(())
}

/// Test fetching non-existent device
#[tokio::test]
async fn test_nonexistent_device() -> Result<()> {
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    let client = create_test_client(&base_url).await?;
    
    // Fetch data for non-existent device
    let location_readings = client.fetch_location_data(999, 1609459200000, None).await?;
    
    // Should return empty parameters array
    assert_eq!(location_readings.location_id, 999);
    assert_eq!(location_readings.parameters.len(), 0);
    
    mock_server.stop().await;
    Ok(())
}

/// Test configuration loading and validation
#[tokio::test]
async fn test_config_validation() -> Result<()> {
    // Test valid configuration
    let config = HydroVuConfig {
        client_id: "test_id".to_string(),
        client_secret: "test_secret".to_string(),
        pond_path: "/tmp/test_pond".to_string(),
        hydrovu_path: "/hydrovu".to_string(),
        max_points_per_run: Some(1000),
        devices: vec![
            HydroVuDevice {
                id: 123,
                name: "Test Device".to_string(),
                scope: "test".to_string(),
                comment: Some("Test comment".to_string()),
            }
        ],
    };
    
    // Should serialize/deserialize correctly
    let yaml = serde_yaml_ng::to_string(&config)?;
    let parsed: HydroVuConfig = serde_yaml_ng::from_str(&yaml)?;
    
    assert_eq!(parsed.client_id, "test_id");
    assert_eq!(parsed.devices.len(), 1);
    assert_eq!(parsed.devices[0].id, 123);
    
    Ok(())
}

/// Helper function to create a test client with mock server base URL
async fn create_test_client(base_url: &str) -> Result<TestClient> {
    Ok(TestClient {
        base_url: base_url.to_string(),
    })
}

/// Test client wrapper that uses mock server endpoints
struct TestClient {
    base_url: String,
}

impl TestClient {
    /// Test authentication by making a simple API call
    async fn test_authentication(&self) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/public-api/v1/sispec/friendlynames", self.base_url);
        
        let response = client
            .get(&url)
            .bearer_auth("mock_access_token_12345")
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Authentication failed: {}", response.status()));
        }
        
        Ok(())
    }
    
    /// Fetch names
    async fn fetch_names(&self) -> Result<hydrovu::models::Names> {
        let client = reqwest::Client::new();
        let url = format!("{}/public-api/v1/sispec/friendlynames", self.base_url);
        
        let response = client
            .get(&url)
            .bearer_auth("mock_access_token_12345")
            .send()
            .await?;
        
        let names = response.json().await?;
        Ok(names)
    }
    
    /// Fetch locations
    async fn fetch_locations(&self) -> Result<Vec<hydrovu::models::Location>> {
        let client = reqwest::Client::new();
        let url = format!("{}/public-api/v1/locations/list", self.base_url);
        
        let response = client
            .get(&url)
            .bearer_auth("mock_access_token_12345")
            .send()
            .await?;
        
        let locations = response.json().await?;
        Ok(locations)
    }
    
    /// Fetch location data
    async fn fetch_location_data(
        &self,
        location_id: i64,
        start_time: i64,
        end_time: Option<i64>,
    ) -> Result<hydrovu::models::LocationReadings> {
        let client = reqwest::Client::new();
        let mut url = format!(
            "{}/public-api/v1/locations/{}/data?startTime={}", 
            self.base_url, location_id, start_time
        );
        
        if let Some(end_time) = end_time {
            url.push_str(&format!("&endTime={}", end_time));
        }
        
        let response = client
            .get(&url)
            .bearer_auth("mock_access_token_12345")
            .send()
            .await?;
        
        let data = response.json().await?;
        Ok(data)
    }
}
