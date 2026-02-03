# HydroVu Integration Module Design

## Overview
We're creating a new HydroVu application module for Duckpond that will integrate with the HydroVu REST API to fetch sensor data. This module will be modeled on the original implementation but updated to use the new async architecture and factory system.

## Architecture Analysis

### Original HydroVu Implementation
From the original codebase (`./original/src/hydrovu/`), we identified:

1. **Configuration Structure** (`HydroVuSpec`):
   ```rust
   pub struct HydroVuSpec {
       pub key: String,      // OAuth client ID
       pub secret: String,   // OAuth client secret  
       pub devices: Vec<HydroVuDevice>,
   }
   
   pub struct HydroVuDevice {
       pub id: i64,          // Location ID from HydroVu API
       pub name: String,     // Human-readable name
       pub scope: String,    // Grouping/organization
       pub comment: Option<String>,
   }
   ```

2. **HTTP Client** (`client.rs`):
   - Synchronous reqwest-based implementation
   - OAuth2 client credentials flow
   - Paginated API responses with retry logic
   - Endpoints: names, locations, location data

3. **Data Models** (`model.rs`):
   - `Names`: parameter and unit mappings (dictionaries)
   - `LocationReadings`: timeseries data structure
   - `Temporal`: time range tracking (not needed in new implementation)

4. **API Endpoints**:
   - `/v1/sispec/friendlynames` - parameter/unit dictionaries
   - `/v1/locations/list` - available locations
   - `/v1/locations/{id}/data` - timeseries data with time filtering

### Duckpond Architecture Patterns

1. **Crate Structure**:
   - Each module is a separate crate in `./crates/`
   - Uses workspace dependencies
   - Follows 2024 edition standards

2. **Factory System** (`tlogfs/factory.rs`):
   - Dynamic node creation using `linkme` distributed slices
   - Context-aware factories with `FactoryContext` 
   - Configuration validation and creation functions
   - Registration via `register_dynamic_factory!` macro

3. **Command Integration** (`cmd/main.rs`):
   - Clap-based CLI with subcommands
   - Transaction support for write operations
   - File system abstraction (data/control)

4. **Steward Transaction Management**:
   - Transaction descriptors with command metadata
   - Post-commit action sequencing
   - Recovery capabilities

## Proposed Implementation Plan

### Phase 1: Standalone Proof of Concept

**Location**: `./crates/hydrovu/`

**Structure**:
```
crates/hydrovu/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Core HydroVu integration logic
│   ├── client.rs        # Async HTTP client
│   ├── models.rs        # Data structures
│   ├── config.rs        # YAML configuration management
│   ├── schema.rs        # Schema evolution handling
│   └── main.rs          # Standalone binary for proof of concept
```

**Key Changes from Original**:
- **Standalone Binary**: No pond CLI modifications needed
- **Async**: Replace `reqwest::blocking` with `reqwest` async
- **Union Schema Evolution**: Single file:series per device with schema evolution
- **Simplified Storage**: Direct pond integration without complex partitioning
- **Transaction Limits**: Configurable limits on points per collection run

### Phase 2: Data Storage Strategy

**Configuration**: External YAML file for standalone binary
- Simple file-based configuration (e.g., `hydrovu-config.yaml`)
- No pond CLI integration - pure proof of concept
- Standard YAML format for easy editing and version control

**Data Storage**: Direct pond integration
```
/pond/data/hydrovu/
├── units          # file:table mapping unit IDs to names
├── params         # file:table mapping parameter IDs to names  
└── devices/       # One file:series per device
    ├── device_123  # file:series with union schema evolution
    ├── device_456  # file:series with union schema evolution
    └── device_789  # file:series with union schema evolution
```

### Phase 3: Standalone Operation

**Standalone Binary**: `hydrovu-collector hydrovu-config.yaml`
- Reads configuration from external YAML file
- Updates dictionary tables (units, params) only when new instruments appear
- Fetches new data for each configured device
- Uses temporal metadata to find last timestamp per device
- Stores new readings in device-specific file:series nodes with schema evolution
- Limits number of points per transaction to control transaction size

**Usage Example**:
```bash
# Create configuration file
cat > hydrovu-config.yaml << EOF
client_id: "your_oauth_client_id"
client_secret: "your_oauth_client_secret"
pond_path: "/path/to/pond"
max_points_per_run: 1000
devices:
  - id: 123
    name: "Sensor 1"
    scope: "site_a"
EOF

# Test authentication (new debugging feature)
./target/debug/hydrovu-collector test hydrovu-config.yaml

# Run data collection (standalone binary)
./target/debug/hydrovu-collector collect hydrovu-config.yaml

# Check collected data (using pond CLI)
pond cat /hydrovu/devices/device_123

# List all hydrovu data
pond ls /hydrovu/
```

## Technical Specifications

### Configuration Format
```yaml
client_id: "your_oauth_client_id"
client_secret: "your_oauth_client_secret"
pond_path: "/path/to/pond"
max_points_per_run: 1000  # Limit transaction size
devices:
  - id: 123
    name: "Sensor 1"
    scope: "site_a"
    comment: "Primary temperature sensor"
  - id: 456
    name: "Sensor 2"
    scope: "site_b"
```

### Standalone Binary Design
- **No pond CLI modifications**: Completely independent proof of concept
- **Direct pond integration**: Uses TLogFS APIs directly to write data
- **Simple operation**: Single binary that reads config and writes data
- **Validation via pond CLI**: Use existing pond CLI to inspect results

### Async Client Design
- Use `tokio` for async runtime (ecosystem compatibility, not parallelism)
- `reqwest` with single concurrent request to respect rate limits
- Structured retry logic for network failures
- Pagination handling with point limits to control transaction size
- OAuth token management with refresh capability

### Data Flow
1. **Initialization**: Create storage directories and initialize dictionary tables if needed
2. **Dictionary Update**: Only update parameter/unit dictionaries when new instruments are encountered
3. **Device Processing**: For each configured device:
   - Query temporal metadata to find latest timestamp for the device file:series
   - Fetch up to `max_points_per_run` new data points from HydroVu API since last timestamp
   - **Schema Boundary Detection**: Group consecutive readings by schema signature
   - **Per-Schema Batch Processing**: For each schema group:
     - Validate schema compatibility with existing series schema on write path
     - If compatible: evolve schema and append batch
     - If incompatible: fail fast with clear error message
     - Handle overlapping timestamps by sorting within each batch
   - Update temporal metadata on successful commit
4. **Incremental Operation**: Frequent runs collect small amounts; initial runs may require multiple passes

## Implementation Considerations

### Error Handling
- Network failures with retry logic
- API rate limiting respect (single concurrent request)
- Invalid configuration detection
- Transaction size limits to prevent large rollbacks
- **Proper failure reporting**: Track and report individual device failures
- **Graceful partial failures**: Continue processing other devices when one fails
- **Detailed OAuth error messages**: Provide helpful diagnostics for authentication issues

### Performance
- Single-threaded operation with async I/O
- Incremental updates to avoid re-fetching historical data
- Efficient Arrow record batch storage
- Configurable transaction size limits for large initial collections

### Schema Evolution
- **Write-Path Validation**: Schema compatibility checked before any data is written
- **Union Schema Approach**: Create union schema containing all parameters from all batches in transaction
- **Parameter Removal Tolerance**: Handle sensor parameters that disappear (common in real sensor data)
- **Additive Changes**: Allow new optional columns, all parameter fields are nullable
- **Temporal Handling**: Sort readings by timestamp within each batch
- **Transaction Safety**: All-or-nothing approach to schema evolution

## Schema Management Strategy

### Write-Path Schema Validation
```rust
fn validate_and_evolve_schema(
    existing_schema: &Schema, 
    new_readings: &[Reading]
) -> Result<(Schema, Vec<Vec<Reading>>)> {
    // Group readings by schema signature (parameter set)
    let schema_groups = group_readings_by_schema(new_readings)?;
    
    // Create union schema containing all parameters from all groups
    let union_schema = create_union_schema_from_all_readings(existing_schema, new_readings)?;
    
    // Convert groups to batches (no compatibility checking needed with union schema)
    let validated_batches: Vec<Vec<Reading>> = schema_groups
        .into_iter()
        .map(|(_, readings)| readings)
        .collect();
    
    Ok((union_schema, validated_batches))
}

fn create_union_schema_from_all_readings(
    existing_schema: &Schema, 
    new_readings: &[Reading]
) -> Result<Schema> {
    // Collect all parameters from existing schema and new readings
    let mut all_parameters = collect_existing_parameters(existing_schema);
    
    // Add all new parameters
    for reading in new_readings {
        all_parameters.insert(reading.parameter_id.clone());
    }
    
    // Create schema with union of all parameters (all nullable)
    create_schema_with_parameters(all_parameters)
}
```

### Schema Boundary Detection
```rust
fn group_readings_by_schema(readings: &[Reading]) -> Result<Vec<(SchemaSignature, Vec<Reading>)>> {
    let mut groups = Vec::new();
    let mut current_group = Vec::new();
    let mut current_signature = None;
    
    for reading in readings {
        let signature = SchemaSignature::from_reading(reading);
        
        if current_signature.as_ref() != Some(&signature) {
            // Schema boundary detected
            if !current_group.is_empty() {
                groups.push((current_signature.unwrap(), current_group));
                current_group = Vec::new();
            }
            current_signature = Some(signature);
        }
        
        current_group.push(reading.clone());
    }
    
    if !current_group.is_empty() {
        groups.push((current_signature.unwrap(), current_group));
    }
    
    Ok(groups)
}

#[derive(Debug, PartialEq, Clone)]
struct SchemaSignature {
    parameter_ids: BTreeSet<i64>,
    unit_mapping: BTreeMap<i64, i64>, // parameter_id -> unit_id
}

impl SchemaSignature {
    fn from_reading(reading: &Reading) -> Self {
        Self {
            parameter_ids: reading.parameters.iter().map(|p| p.parameter_id).collect(),
            unit_mapping: reading.parameters.iter()
                .map(|p| (p.parameter_id, p.unit_id))
                .collect(),
        }
    }
}
```

### Schema Compatibility Checking
```rust
fn check_schema_compatibility(existing: &Schema, new: &Schema) -> Result<SchemaCompatibility> {
    let existing_fields: BTreeMap<String, &Field> = existing.fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();
    
    let new_fields: BTreeMap<String, &Field> = new.fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();
    
    // Check for type changes in existing fields
    for (name, existing_field) in &existing_fields {
        if let Some(new_field) = new_fields.get(name) {
            if !types_compatible(existing_field.data_type(), new_field.data_type()) {
                return Ok(SchemaCompatibility::Incompatible(
                    format!("Field '{}' changed from {:?} to {:?}", 
                           name, existing_field.data_type(), new_field.data_type())
                ));
            }
        }
    }
    
    // For sensor data, field removal is OK since all parameter fields are nullable
    // Parameters can legitimately disappear (sensors offline, out of range, etc.)
    
    // Check for new fields (allowed as optional)
    let new_field_names: BTreeSet<_> = new_fields.keys().collect();
    let existing_field_names: BTreeSet<_> = existing_fields.keys().collect();
    let added_fields: Vec<_> = new_field_names.difference(&existing_field_names).collect();
    
    if added_fields.is_empty() {
        Ok(SchemaCompatibility::Identical)
    } else {
        // Create evolved schema with new fields as optional
        let mut evolved_fields = existing.fields().clone().to_vec();
        for field_name in added_fields {
            let new_field = new_fields[*field_name];
            let optional_field = Field::new(
                new_field.name(),
                new_field.data_type().clone(),
                true // nullable
            );
            evolved_fields.push(Arc::new(optional_field));
        }
        
        Ok(SchemaCompatibility::AdditiveCohmpible(Schema::new(evolved_fields)))
    }
}
```

### Last Timestamp Discovery
```rust
async fn find_last_timestamp(pond_path: &Path, device_id: i64) -> Option<DateTime<Utc>> {
    let device_path = pond_path.join("data/hydrovu/devices")
        .join(format!("device_{}", device_id));
    
    // Query the temporal metadata of the device file:series
    get_series_temporal_metadata(&device_path).latest_time
}
```

### Schema Storage Strategy

**Efficient Incremental Schema Evolution via Latest Version:**

The key insight is that with **additive-only schema evolution**, the latest version always contains the union of all columns from previous versions. This enables extremely efficient schema validation:

1. **Single Version Query**: Query the latest OplogEntry to get the complete current schema
2. **Incremental Validation**: New data only needs validation against this latest schema  
3. **Column Accumulation**: Each write adds new columns to the evolving schema
4. **No Schema Caching**: Always read from the authoritative source (latest Parquet data)

```rust
async fn get_current_series_schema(
    &self,
    node_id: NodeID, 
    part_id: NodeID
) -> Result<Schema> {
    // Query latest OplogEntry for this series
    let records = self.query_records(&part_id.to_hex_string(), Some(&node_id.to_hex_string())).await?;
    
    if let Some(latest_record) = records.first() {
        if latest_record.file_type == tinyfs::EntryType::FileSeries {
            // Extract schema directly from latest Parquet content
            if let Some(content) = &latest_record.content {
                return extract_schema_from_parquet_content(content);
            }
        }
    }
    
    // If no existing data, return empty schema (first write)
    Ok(Schema::empty())
}

// Simple validation: always check against latest committed schema
async fn validate_new_data(
    &self, 
    node_id: NodeID, 
    part_id: NodeID, 
    new_readings: &[Reading]
) -> Result<Schema> {
    let current_schema = self.get_current_series_schema(node_id, part_id).await?;
    
    // Group and validate each schema boundary in the new data  
    let (evolved_schema, validated_batches) = validate_and_evolve_schema(&current_schema, new_readings)?;
    
    // evolved_schema now contains current_schema + any new columns from new_readings
    Ok(evolved_schema)
}
```

**Schema Evolution Property:**
- **Version N**: Contains columns {A, B, C}
- **Version N+1**: Contains columns {A, B, C, D} (additive)
- **Version N+2**: Contains columns {A, B, C, D, E} (additive)

Therefore: **Latest version always has complete schema** ✅

### Schema Compatibility Checking
```rust
fn types_compatible(existing: &DataType, new: &DataType) -> bool {
    match (existing, new) {
        // Exact same type
        (a, b) if a == b => true,
        
        // Nullable version of same type is compatible
        (DataType::Int64, DataType::Int64) => true,
        (DataType::Float64, DataType::Float64) => true,
        (DataType::Utf8, DataType::Utf8) => true,
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        
        // TODO: Add more compatibility rules as needed
        _ => false,
    }
}
```

### Schema Evolution Process
When new data arrives:
1. **Schema Boundary Detection**: Group consecutive readings by schema signature (parameter set + types)
2. **Union Schema Creation**: Create schema containing all parameters from existing schema + all new readings
3. **Nullable Parameter Fields**: All parameter fields are nullable to handle missing sensors/measurements
4. **Batch Processing**: Process each schema-homogeneous group as a separate Arrow batch
5. **Temporal Handling**: Sort readings within each batch by timestamp before writing

This approach provides:
- **Tolerance for parameter removal**: Common in sensor data when sensors go offline or out of range
- **Transaction safety**: Single union schema for entire transaction 
- **Schema boundary handling**: Multiple parameter sets within one transaction are properly handled
- **Temporal integrity**: Overlapping timestamps handled by sorting within batches
- **Clear error messages**: Only fail on actual type incompatibilities
- **Simple schema discovery**: Latest version is always the authoritative schema source
- **Real-world sensor behavior**: Handles dynamic parameter availability naturally

### Security
- OAuth token caching and refresh
- Secure credential storage
- Audit logging of data collection operations

### Testing Strategy

### Mock HydroVu Server for Testing
Create a simple HTTP mock server that implements the HydroVu REST API endpoints for testing purposes:

**Structure**:
```
crates/hydrovu/tests/
├── mock_server.rs       # Simple HTTP server implementing HydroVu API
├── test_data/           # JSON fixtures for different scenarios
│   ├── names.json       # Sample parameter/unit dictionaries
│   ├── locations.json   # Sample locations list
│   └── device_data/     # Device-specific test data
│       ├── device_123.json
│       └── device_456.json
└── integration_tests.rs # End-to-end tests using mock server
```

**Mock Server Features**:
- **OAuth Endpoint**: `/oauth/token` - Returns test access token
- **Names Endpoint**: `/v1/sispec/friendlynames` - Returns static parameter/unit mappings
- **Locations Endpoint**: `/v1/locations/list` - Returns configured test locations
- **Location Data Endpoint**: `/v1/locations/{id}/data` - Returns time-filtered test data
- **Configurable Scenarios**: Different response patterns for testing edge cases
- **Error Simulation**: HTTP error responses, rate limiting, network failures
- **Time Filtering**: Proper start/end time parameter handling

**Test Data Scenarios**:
1. **Normal Operation**: Complete data with multiple parameters
2. **Schema Evolution**: Parameter addition/removal between time periods
3. **Missing Data**: Gaps in readings, missing parameters
4. **Edge Cases**: Empty responses, single readings, overlapping timestamps
5. **Error Conditions**: Authentication failures, API errors, malformed data

### Unit Test Coverage
- **Client Module**: Authentication, API calls, error handling
- **Models Module**: Data structure serialization/deserialization
- **Schema Module**: Schema evolution and compatibility checking
- **Config Module**: YAML parsing and validation
- **Integration Tests**: End-to-end data collection with expected outputs

### Test Implementation Plan
1. **Phase 1**: Mock server with basic endpoints and test data fixtures
2. **Phase 2**: Comprehensive unit tests for all modules with mock responses
3. **Phase 3**: Integration tests comparing collected data with expected results
4. **Phase 4**: Error simulation and edge case testing

## Implementation Status

### ✅ Completed Features
1. **Standalone Binary**: `hydrovu-collector` with init/test/collect commands
2. **OAuth2 Authentication**: With detailed error messages and debugging
3. **Configuration Management**: YAML-based configuration with validation
4. **Async HTTP Client**: HydroVu API integration with proper error handling
5. **Schema Evolution**: Union schema approach handling parameter removal
6. **Error Reporting**: Proper failure tracking and device-level error reporting
7. **Test Infrastructure**: Simple test script for repeat testing

### 🚧 In Progress / Stub Implementation
1. **Pond Integration**: File system operations (currently stubbed)
2. **Data Storage**: Arrow RecordBatch creation and writing
3. **Temporal Metadata**: Last timestamp discovery and updates
4. **Dictionary Management**: Parameter/unit mapping tables

### 🔄 Real-World Testing
- Successfully authenticates with HydroVu API
- Handles real device data with 483 readings
- Detects and gracefully handles schema changes (parameter removal)
- Reports both successes and failures correctly

## Next Steps

1. Implement actual pond integration for writing data
2. Add Arrow RecordBatch creation from FlattenedReading batches
3. Implement temporal metadata queries and updates  
4. Add dictionary table management for parameters/units
5. Add retry logic for network failures
6. Performance testing with larger datasets

This implementation successfully proves the core concepts with real HydroVu API integration and handles the complex schema evolution challenges of sensor data.
