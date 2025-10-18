//! Template expansion utilities for YAML configuration files
//!
//! This module provides template expansion functionality using the Tera template engine.
//! It allows configuration files to use template variables and built-in functions.

use anyhow::Result;
use std::collections::HashMap;
use tera::{Tera, Value};

/// Expand a YAML configuration file using Tera templates
///
/// This function takes YAML content as a string and applies Tera template expansion
/// with the provided variables. It also registers built-in functions like `env` for
/// reading environment variables.
///
/// # Arguments
///
/// * `yaml_content` - The YAML content as a string (may contain Tera template syntax)
/// * `variables` - Variables to make available in the template context
///
/// # Returns
///
/// The expanded YAML content as a string
///
/// # Example
///
/// ```text
/// # Input YAML with templates:
/// client_id: "{{ client_id }}"
/// client_secret: "{{ env(name='HYDRO_SECRET') }}"
/// max_points: 1000
///
/// # With variables: {"client_id": "my-client"}
/// # And env var HYDRO_SECRET="secret123"
/// # Output:
/// client_id: "my-client"
/// client_secret: "secret123"
/// max_points: 1000
/// ```
pub fn expand_yaml_template(
    yaml_content: &str,
    variables: &HashMap<String, String>,
) -> Result<String> {
    // Create a new Tera instance without loading any templates from disk
    let mut tera = Tera::default();

    // Register the env function for reading environment variables
    tera.register_function("env", env_function());

    // Create context with provided variables
    let mut context = tera::Context::new();
    for (key, value) in variables {
        context.insert(key, value);
    }

    // Render the template with detailed error reporting
    tera.render_str(yaml_content, &context)
        .map_err(|e| {
            // Build detailed error message with complete error chain
            let mut error_parts = vec![];
            
            // Add main error
            error_parts.push(format!("Template rendering failed: {}", e));
            
            // Add complete error chain
            let chain = collect_error_chain(&e);
            if chain.len() > 1 {
                error_parts.push(format!("Error chain ({} levels):", chain.len()));
                for (i, err_msg) in chain.iter().enumerate() {
                    if i == 0 {
                        error_parts.push(format!("  → {}", err_msg));
                    } else {
                        error_parts.push(format!("  ├─ Level {}: {}", i, err_msg));
                    }
                }
            }
            
            // Add available variables info
            if variables.is_empty() {
                error_parts.push("No template variables provided (use -v key=value to provide)".to_string());
            } else {
                error_parts.push(format!("Available variables: {:?}", variables.keys().collect::<Vec<_>>()));
            }
            
            anyhow::anyhow!("{}", error_parts.join("\n"))
        })
}

/// Collect complete error chain as strings
fn collect_error_chain(err: &dyn std::error::Error) -> Vec<String> {
    let mut chain = vec![err.to_string()];
    let mut source = err.source();

    while let Some(err) = source {
        chain.push(err.to_string());
        source = err.source();
    }

    chain
}

/// Built-in function to read environment variables
///
/// Usage in templates:
/// - `{{ env(name="VAR_NAME") }}` - Read environment variable, error if not set
/// - `{{ env(name="VAR_NAME", default="fallback") }}` - Read with default fallback
fn env_function() -> impl tera::Function {
    Box::new(
        move |args: &HashMap<String, Value>| -> tera::Result<Value> {
            // Get the environment variable name
            let var_name = args
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| tera::Error::msg("env function requires 'name' parameter"))?;

            // Check if a default value was provided
            let default_value = args.get("default").and_then(|v| v.as_str());

            // Read environment variable
            match std::env::var(var_name) {
                Ok(value) => Ok(Value::String(value)),
                Err(std::env::VarError::NotPresent) => {
                    if let Some(default) = default_value {
                        Ok(Value::String(default.to_string()))
                    } else {
                        Err(tera::Error::msg(format!(
                            "Environment variable '{}' not set and no default provided",
                            var_name
                        )))
                    }
                }
                Err(e) => Err(tera::Error::msg(format!(
                    "Failed to read environment variable '{}': {}",
                    var_name, e
                ))),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_yaml_with_simple_variables() {
        let yaml = r#"
name: "{{ device_name }}"
count: {{ item_count }}
"#;

        let mut vars = HashMap::new();
        vars.insert("device_name".to_string(), "TestDevice".to_string());
        vars.insert("item_count".to_string(), "42".to_string());

        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("TestDevice"));
        assert!(result.contains("42"));
    }

    #[test]
    fn test_expand_yaml_with_env_function() {
        // Set a test environment variable
        // SAFETY: This is safe in tests as we control the execution environment
        unsafe {
            std::env::set_var("TEST_ENV_VAR", "test_value");
        }

        let yaml = r#"
api_key: "{{ env(name='TEST_ENV_VAR') }}"
"#;

        let vars = HashMap::new();
        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("test_value"));

        // Clean up
        // SAFETY: This is safe in tests as we control the execution environment
        unsafe {
            std::env::remove_var("TEST_ENV_VAR");
        }
    }

    #[test]
    fn test_expand_yaml_with_env_default() {
        // Make sure the variable doesn't exist
        // SAFETY: This is safe in tests as we control the execution environment
        unsafe {
            std::env::remove_var("NONEXISTENT_VAR");
        }

        let yaml = r#"
value: "{{ env(name='NONEXISTENT_VAR', default='fallback') }}"
"#;

        let vars = HashMap::new();
        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("fallback"));
    }

    #[test]
    fn test_expand_yaml_env_missing_error() {
        // Make sure the variable doesn't exist
        // SAFETY: This is safe in tests as we control the execution environment
        unsafe {
            std::env::remove_var("MISSING_VAR");
        }

        let yaml = r#"
value: "{{ env(name='MISSING_VAR') }}"
"#;

        let vars = HashMap::new();
        let result = expand_yaml_template(yaml, &vars);
        assert!(result.is_err(), "Should fail when environment variable is missing");
    }

    #[test]
    fn test_expand_yaml_combined() {
        // SAFETY: This is safe in tests as we control the execution environment
        unsafe {
            std::env::set_var("TEST_SECRET", "secret123");
        }

        let yaml = r#"
client_id: "{{ client_id }}"
client_secret: "{{ env(name='TEST_SECRET') }}"
devices:
  - name: "{{ device_1 }}"
  - name: "{{ device_2 }}"
"#;

        let mut vars = HashMap::new();
        vars.insert("client_id".to_string(), "my-client".to_string());
        vars.insert("device_1".to_string(), "Device1".to_string());
        vars.insert("device_2".to_string(), "Device2".to_string());

        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("my-client"));
        assert!(result.contains("secret123"));
        assert!(result.contains("Device1"));
        assert!(result.contains("Device2"));

        // SAFETY: This is safe in tests as we control the execution environment
        unsafe {
            std::env::remove_var("TEST_SECRET");
        }
    }

    #[test]
    fn test_expand_yaml_no_templates() {
        // YAML without any templates should pass through unchanged
        let yaml = r#"
name: "plain"
value: 123
"#;

        let vars = HashMap::new();
        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert_eq!(result, yaml);
    }
}
