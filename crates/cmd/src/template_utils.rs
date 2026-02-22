// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Template expansion utilities for YAML configuration files
//!
//! This module provides template expansion functionality using the MiniJinja template engine.
//! It allows configuration files to use template variables and built-in functions.

use anyhow::Result;
use std::collections::HashMap;

#[cfg(test)]
#[allow(unsafe_code)]
fn env_set_var(key: impl AsRef<std::ffi::OsStr>, value: impl AsRef<std::ffi::OsStr>) {
    unsafe {
        std::env::set_var(key, value);
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
fn env_remove_var(key: impl AsRef<std::ffi::OsStr>) {
    unsafe {
        std::env::remove_var(key);
    }
}

/// Expand a YAML configuration file using MiniJinja templates
///
/// This function takes YAML content as a string and applies template expansion
/// with the provided variables. It also registers built-in functions like `env` for
/// reading environment variables.
///
/// # Arguments
///
/// * `yaml_content` - The YAML content as a string (may contain template syntax)
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
    let mut env = minijinja::Environment::new();
    env.set_keep_trailing_newline(true);

    // Register the env function for reading environment variables
    env.add_function("env", env_function);

    // Build context from provided variables
    let ctx = minijinja::Value::from_serialize(variables);

    // Render the template with detailed error reporting
    env.render_str(yaml_content, ctx).map_err(|e| {
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
                    error_parts.push(format!("  -> {}", err_msg));
                } else {
                    error_parts.push(format!("  |- Level {}: {}", i, err_msg));
                }
            }
        }

        // Add available variables info
        if variables.is_empty() {
            error_parts
                .push("No template variables provided (use -v key=value to provide)".to_string());
        } else {
            error_parts.push(format!(
                "Available variables: {:?}",
                variables.keys().collect::<Vec<_>>()
            ));
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
fn env_function(kwargs: minijinja::value::Kwargs) -> Result<String, minijinja::Error> {
    let name: String = kwargs.get("name")?;
    let default: Option<String> = kwargs.get("default")?;
    kwargs.assert_all_used()?;

    match std::env::var(&name) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => {
            if let Some(default) = default {
                Ok(default)
            } else {
                Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!(
                        "Environment variable '{}' not set and no default provided",
                        name
                    ),
                ))
            }
        }
        Err(e) => Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!("Failed to read environment variable '{}': {}", name, e),
        )),
    }
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
        _ = vars.insert("device_name".to_string(), "TestDevice".to_string());
        _ = vars.insert("item_count".to_string(), "42".to_string());

        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("TestDevice"));
        assert!(result.contains("42"));
    }

    #[test]
    fn test_expand_yaml_with_env_function() {
        env_set_var("TEST_ENV_VAR", "test_value");

        let yaml = r#"
api_key: "{{ env(name='TEST_ENV_VAR') }}"
"#;

        let vars = HashMap::new();
        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("test_value"));

        // Clean up
        env_remove_var("TEST_ENV_VAR");
    }

    #[test]
    fn test_expand_yaml_with_env_default() {
        // Make sure the variable doesn't exist
        env_remove_var("NONEXISTENT_VAR");

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
        env_remove_var("MISSING_VAR");

        let yaml = r#"
value: "{{ env(name='MISSING_VAR') }}"
"#;

        let vars = HashMap::new();
        let result = expand_yaml_template(yaml, &vars);
        assert!(
            result.is_err(),
            "Should fail when environment variable is missing"
        );
    }

    #[test]
    fn test_expand_yaml_combined() {
        env_set_var("TEST_SECRET", "secret123");

        let yaml = r#"
client_id: "{{ client_id }}"
client_secret: "{{ env(name='TEST_SECRET') }}"
devices:
  - name: "{{ device_1 }}"
  - name: "{{ device_2 }}"
"#;

        let mut vars = HashMap::new();
        _ = vars.insert("client_id".to_string(), "my-client".to_string());
        _ = vars.insert("device_1".to_string(), "Device1".to_string());
        _ = vars.insert("device_2".to_string(), "Device2".to_string());

        let result = expand_yaml_template(yaml, &vars).unwrap();
        assert!(result.contains("my-client"));
        assert!(result.contains("secret123"));
        assert!(result.contains("Device1"));
        assert!(result.contains("Device2"));

        env_remove_var("TEST_SECRET");
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
