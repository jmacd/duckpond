#[cfg(test)]
mod tests {
    use crate::sql_derived::{SqlDerivedConfig, SqlDerivedDirectory};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_sql_derived_config_parsing() {
        let yaml_content = r#"
source: "/path/original.series"
sql: "SELECT A as Apple, B as Berry FROM series"
"#;
        
        let config: SqlDerivedConfig = serde_yaml::from_str(yaml_content).unwrap();
        assert_eq!(config.source, "/path/original.series");
        assert_eq!(config.sql, "SELECT A as Apple, B as Berry FROM series");
    }

    #[tokio::test]
    async fn test_sql_derived_directory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config_content = r#"
source: "/nonexistent/source.series"
sql: "SELECT COUNT(*) as count FROM series"
"#;
        
        let config: SqlDerivedConfig = serde_yaml::from_str(config_content).unwrap();
        
        // Create a mock persistence layer
        let persistence = Arc::new(crate::persistence::OpLogPersistence::new(
            temp_dir.path().to_str().unwrap()
        ).await.unwrap());
        
        let directory = SqlDerivedDirectory::new(config, persistence);
        
        // Just test that it creates without panicking
        // We can't test query execution without setting up a real file series
        assert!(directory.is_ok());
    }

    #[test]
    fn test_sql_derived_config_fields() {
        // Test the new field structure matches cat command patterns
        let yaml_content = r#"
source: "/test/data.series"
sql: "SELECT column1, column2 FROM series WHERE column1 > 100"
"#;
        
        let config: SqlDerivedConfig = serde_yaml::from_str(yaml_content).unwrap();
        
        // Verify the fields are named consistently with cat command
        assert_eq!(config.source, "/test/data.series");
        assert!(config.sql.contains("FROM series"));
    }

    #[test]
    fn test_sql_derived_config_validation() {
        // Test empty SQL validation
        let yaml_invalid = r#"
source: "/test/data.series"
sql: ""
"#;
        
        let config_result: Result<SqlDerivedConfig, _> = serde_yaml::from_str(yaml_invalid);
        // Config parsing succeeds, but validation would catch empty SQL
        assert!(config_result.is_ok());
        let config = config_result.unwrap();
        assert!(config.sql.trim().is_empty());
    }
}