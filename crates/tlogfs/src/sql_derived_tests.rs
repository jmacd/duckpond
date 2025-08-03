// Example usage of SQL-derived dynamic nodes
use crate::sql_derived::{SqlDerivedConfig, SqlDerivedDirectory};
use std::sync::Arc;

#[tokio::test]
async fn test_sql_derived_basic_functionality() {
    // This is a placeholder test - would need actual test data and persistence layer
    let config = SqlDerivedConfig {
        source_path: "some_node_id".to_string(),
        sql: "SELECT * FROM source LIMIT 10".to_string(),
        output_type: Some("table".to_string()),
    };
    
    // In a real test, we would set up a proper persistence layer with test data
    // For now, this just verifies that the structures compile and can be constructed
    println!("SQL-derived config created: {:?}", config);
}

#[tokio::test] 
async fn test_sql_config_validation() {
    use crate::sql_derived::validate_sql_derived_config;
    
    let yaml_config = r#"
source_path: "abc123"
sql: "SELECT count(*) FROM source"
output_type: "table"
"#;
    
    let result = validate_sql_derived_config(yaml_config.as_bytes());
    assert!(result.is_ok(), "Valid config should pass validation");
    
    let invalid_yaml = r#"
source_path: ""
sql: ""
"#;
    
    let result = validate_sql_derived_config(invalid_yaml.as_bytes());
    assert!(result.is_err(), "Invalid config should fail validation");
}
