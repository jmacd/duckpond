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

# Run data collection (standalone binary)
./target/release/hydrovu-collector hydrovu-config.yaml

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
- Graceful handling of partial collection failures

### Performance
- Single-threaded operation with async I/O
- Incremental updates to avoid re-fetching historical data
- Efficient Arrow record batch storage
- Configurable transaction size limits for large initial collections

### Schema Evolution
- **Write-Path Validation**: Schema compatibility checked before any data is written
- **Schema Boundary Detection**: Detect when schema changes within a single API response
- **Fail-Fast Approach**: Reject entire transaction if incompatible schemas detected
- **Additive Changes Only**: Only allow new optional columns, no type changes or removals
- **Temporal Handling**: Sort readings by timestamp within schema-homogeneous groups
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
    let mut evolved_schema = existing_schema.clone();
    let mut validated_batches = Vec::new();
    
    for (schema_signature, readings) in schema_groups {
        // Create Arrow schema for this group
        let batch_schema = create_schema_from_readings(&readings)?;
        
        // Validate compatibility before proceeding
        let compatibility = check_schema_compatibility(&evolved_schema, &batch_schema)?;
        
        match compatibility {
            SchemaCompatibility::Identical => {
                // No changes needed
                validated_batches.push(readings);
            }
            SchemaCompatibility::AdditiveCohmpible(new_schema) => {
                // New optional columns - safe to evolve
                evolved_schema = new_schema;
                validated_batches.push(readings);
            }
            SchemaCompatibility::Incompatible(reason) => {
                // Fail fast with clear error
                return Err(anyhow!(
                    "Schema incompatibility detected: {}. Device schemas cannot change incompatibly within a series.", 
                    reason
                ));
            }
        }
    }
    
    Ok((evolved_schema, validated_batches))
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
#[derive(Debug)]
enum SchemaCompatibility {
    Identical,
    AdditiveCohmpible(Schema), // Returns evolved schema
    Incompatible(String),      // Returns reason
}

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
    
    // Check for removed fields (not allowed)
    for name in existing_fields.keys() {
        if !new_fields.contains_key(name) {
            return Ok(SchemaCompatibility::Incompatible(
                format!("Field '{}' was removed", name)
            ));
        }
    }
    
    // Check for new fields (allowed as optional)
    let new_field_names: BTreeSet<_> = new_fields.keys().collect();
    let existing_field_names: BTreeSet<_> = existing_fields.keys().collect();
    let added_fields: Vec<_> = new_field_names.difference(&existing_field_names).collect();
    
    if added_fields.is_empty() {
        Ok(SchemaCompatibility::Identical)
    } else {
        // Create evolved schema with new fields as optional
        let mut evolved_fields = existing.fields().clone();
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

1. **Single Version Read**: Only read the latest version to get the complete current schema
2. **Incremental Validation**: New data only needs validation against this latest schema
3. **Column Accumulation**: Each write adds new columns to the evolving schema
4. **No Historical Scanning**: Never need to read old versions to understand current schema

```rust
async fn get_current_schema(series_path: &Path) -> Result<Schema> {
    // 1. Get latest version from temporal metadata (fast - no data read)
    let latest_version = get_latest_version(series_path).await?;
    
    // 2. Read ONLY the latest Parquet version to get complete current schema
    //    This schema contains ALL columns from all previous versions due to additive evolution
    let parquet_data = read_series_version(series_path, latest_version).await?;
    let current_schema = extract_schema_from_parquet(&parquet_data)?;
    
    // 3. Validate timestamp column from extended attributes (fast - metadata only)
    let timestamp_column = get_timestamp_column_from_metadata(series_path).await?;
    validate_timestamp_column(&current_schema, &timestamp_column)?;
    
    Ok(current_schema)
}

// The magic: new data validation only needs to check against latest schema
async fn validate_new_data(series_path: &Path, new_readings: &[Reading]) -> Result<Schema> {
    let current_schema = get_current_schema(series_path).await?; // Single version read
    
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
2. **Write-Path Validation**: For each schema group, validate compatibility with existing series schema
3. **Fail Fast on Incompatibility**: Reject the entire transaction if any schema group is incompatible
4. **Additive Evolution**: If compatible, evolve schema to include new columns as optional
5. **Batch Processing**: Process each schema-homogeneous group as a separate Arrow batch
6. **Temporal Handling**: Sort readings within each batch by timestamp before writing

This approach provides:
- **Write-path validation**: Schema problems detected immediately, not deferred to read time
- **Transaction safety**: Entire transaction fails if any schema is incompatible
- **Schema boundary handling**: Multiple schemas within one transaction are properly separated
- **Temporal integrity**: Overlapping timestamps handled by sorting within schema groups
- **Clear error messages**: Specific feedback about schema compatibility issues
- **Efficient schema tracking**: Latest version always contains complete schema (O(1) schema reads)
- **Incremental evolution**: Each write only validates against current schema, not historical versions

### Security
- OAuth token caching and refresh
- Secure credential storage
- Audit logging of data collection operations

### Testing Strategy
- Unit tests for individual components
- Integration tests with mock HydroVu API
- End-to-end tests with real pond operations
- Configuration validation tests

## Open Questions

1. **Credential Storage**: How should OAuth credentials be securely stored in the configuration?
2. **Schema Type Evolution**: What other type compatibility rules should we support?
3. **Device Discovery**: Should we support automatic device discovery vs. manual configuration?
4. **Monitoring**: How do we expose collection status and error information?
5. **Data Retention**: Should we implement any data lifecycle management?

## Next Steps

1. Create standalone binary with basic async HTTP client
2. Implement OAuth and HydroVu API integration
3. Add schema evolution logic with union approach
4. Implement direct pond integration for writing data
5. Add configuration parsing and validation
6. Write integration tests with real pond
7. Add error handling and retry logic

This simplified design focuses on proving the core concepts without modifying the pond CLI, using a practical schema evolution approach that handles real-world sensor data challenges.
