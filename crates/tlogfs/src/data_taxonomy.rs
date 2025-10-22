//! DuckPond data taxonomy for classifying and protecting sensitive configuration data.
//!
//! This module defines data classes for sensitive information in DuckPond configurations,
//! such as API keys, OAuth secrets, and service credentials. These classified types
//! automatically redact sensitive data when serialized or displayed, preventing accidental
//! exposure in logs, debugging output, or file listings.
//!
//! # Usage
//!
//! Wrap sensitive configuration fields with the appropriate classified type:
//!
//! ```rust
//! use tlogfs::data_taxonomy::{ApiKey, ApiSecret};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyConfig {
//!     pub client_id: ApiKey<String>,       // Shows as "[REDACTED]" when serialized
//!     pub client_secret: ApiSecret<String>, // Shows as "[REDACTED]" when serialized
//!     pub endpoint: String,                 // Normal field, not classified
//! }
//! ```
//!
//! # Programmatic Access
//!
//! To access the actual value of a classified field, use `.as_declassified()`:
//!
//! ```rust
//! use tlogfs::data_taxonomy::{ApiKey, ApiSecret};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyConfig {
//!     pub client_id: ApiKey<String>,
//!     pub client_secret: ApiSecret<String>,
//! }
//!
//! # let config = MyConfig {
//! #     client_id: ApiKey::new("test-id".to_string()),
//! #     client_secret: ApiSecret::new("test-secret".to_string()),
//! # };
//! let actual_client_id: &String = config.client_id.as_declassified();
//! ```
//!
//! # Integration with Serde
//!
//! Classified types implement custom `Serialize` that outputs "[REDACTED]" instead
//! of the actual value. This ensures that when configurations are displayed via
//! `pond cat` or similar commands, sensitive data is not exposed.

use data_privacy::taxonomy;

/// DuckPond data taxonomy for sensitive configuration fields
/// 
/// This enum defines the data classification categories. The `#[taxonomy]` macro
/// generates wrapper types (ApiKey<T>, ApiSecret<T>, ServiceEndpoint<T>) for each variant.
/// 
/// The macro generates:
/// - `Debug` trait: Shows `<duckpond/api_key:REDACTED>` to prevent leaking in logs
/// - `Serialize`/`Deserialize` traits: Pass through actual values for config file I/O
/// 
/// This means config files contain real credentials (as needed), but debug/log output
/// is automatically redacted to prevent accidental exposure.
#[taxonomy(duckpond)]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum DuckPondTaxonomy {
    /// API keys, client IDs, access keys - identifiers for authentication
    ApiKey,
    
    /// API secrets, client secrets, secret access keys - sensitive credentials
    ApiSecret,
    
    /// Service endpoints that may reveal infrastructure details
    ServiceEndpoint,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    struct TestConfig {
        pub client_id: ApiKey<String>,
        pub client_secret: ApiSecret<String>,
        pub endpoint: ServiceEndpoint<String>,
        pub public_field: String,
    }

    #[test]
    fn test_sensitive_fields_redacted_in_json() {
        let config = TestConfig {
            client_id: ApiKey::new("my-client-id-12345".to_string()),
            client_secret: ApiSecret::new("super-secret-key".to_string()),
            endpoint: ServiceEndpoint::new("https://api.example.com".to_string()),
            public_field: "public-data".to_string(),
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        
        // Sensitive fields should contain actual values for config file I/O
        assert!(json.contains(r#""client_id": "my-client-id-12345""#));
        assert!(json.contains(r#""client_secret": "super-secret-key""#));
        assert!(json.contains(r#""endpoint": "https://api.example.com""#));
        
        // Public field should be visible
        assert!(json.contains(r#""public_field": "public-data""#));
    }

    #[test]
    fn test_sensitive_fields_redacted_in_yaml() {
        let config = TestConfig {
            client_id: ApiKey::new("my-client-id-12345".to_string()),
            client_secret: ApiSecret::new("super-secret-key".to_string()),
            endpoint: ServiceEndpoint::new("https://api.example.com".to_string()),
            public_field: "public-data".to_string(),
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        
        // Sensitive fields should contain actual values for config file I/O
        assert!(yaml.contains("client_id: my-client-id-12345"));
        assert!(yaml.contains("client_secret: super-secret-key"));
        assert!(yaml.contains("endpoint: 'https://api.example.com'") || yaml.contains("endpoint: https://api.example.com"));
        
        // Public field should be visible
        assert!(yaml.contains("public_field: public-data"));
    }

    #[test]
    fn test_declassify_provides_actual_value() {
        let config = TestConfig {
            client_id: ApiKey::new("my-client-id-12345".to_string()),
            client_secret: ApiSecret::new("super-secret-key".to_string()),
            endpoint: ServiceEndpoint::new("https://api.example.com".to_string()),
            public_field: "public-data".to_string(),
        };

        // Programmatic access via declassify() works
        assert_eq!(config.client_id.declassify(), "my-client-id-12345");
        assert_eq!(config.client_secret.declassify(), "super-secret-key");
        assert_eq!(config.endpoint.declassify(), "https://api.example.com");
    }

    #[test]
    fn test_deserialization_from_yaml() {
        let yaml = r#"
client_id: my-client-id-12345
client_secret: super-secret-key
endpoint: https://api.example.com
public_field: public-data
"#;

        let config: TestConfig = serde_yaml::from_str(yaml).unwrap();
        
        // Deserialized values are accessible via declassify
        assert_eq!(config.client_id.declassify(), "my-client-id-12345");
        assert_eq!(config.client_secret.declassify(), "super-secret-key");
        assert_eq!(config.endpoint.declassify(), "https://api.example.com");
        assert_eq!(config.public_field, "public-data");
    }

    #[test]
    fn test_debug_output_is_redacted() {
        let api_key = ApiKey::new("my-secret-key".to_string());
        let debug_output = format!("{:?}", api_key);
        
        // Debug output should show the taxonomy and "REDACTED", not the actual value
        assert!(debug_output.contains("REDACTED"));
        assert!(!debug_output.contains("my-secret-key"));
    }
}
