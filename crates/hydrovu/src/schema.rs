// Simplified schema module for HydroVu timestamp-joined records
use arrow::datatypes::{DataType, Field, Schema};
use log::debug;
use std::sync::Arc;

/// Create base Arrow schema for HydroVu data with timestamp only
/// Following original HydroVu conventions - location_id goes in the file path, not the data
/// This serves as the minimal schema that can be extended with parameter fields
pub fn create_base_schema() -> Schema {
    let fields = vec![
        Arc::new(Field::new("timestamp", DataType::Timestamp(arrow_schema::TimeUnit::Second, Some(Arc::from("+00:00"))), false)),
    ];
    
    let field_count = fields.len();
    debug!("Created base schema with {field_count} fields");
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_base_schema() {
        let schema = create_base_schema();
        
        // Should have only timestamp (location_id goes in file path, not data)
        assert_eq!(schema.fields().len(), 1);
        
        // Verify field names
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"timestamp"));
        
        // Verify timestamp field type
        let timestamp_field = schema.field_with_name("timestamp").unwrap();
        if let DataType::Timestamp(_unit, _tz) = timestamp_field.data_type() {
            // Expected
        } else {
            panic!("Timestamp field should have Timestamp data type");
        }
    }
}
