//! SQL-derived file configuration
//!
//! Configuration types for SQL-based file generation with pattern matching
//! and optional table provider wrapping.

use crate::Error;
use datafusion::catalog::TableProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for SQL-derived file generation
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SqlDerivedConfig {
    /// Named patterns for matching files. Each pattern name becomes a table in the SQL query.
    /// Each pattern can match multiple files which are automatically harmonized with UNION ALL BY NAME.
    /// Example: {"vulink": "/data/vulink*.series", "at500": "/data/at500*.series"}
    #[serde(default)]
    pub patterns: HashMap<String, String>,

    /// SQL query to execute on the source data. Defaults to "SELECT * FROM source" if not specified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    
    /// Optional scope prefixes to apply to table columns (internal use by derivative factories)
    /// Maps table name to (scope_prefix, time_column_name)
    /// NOT serialized - must be set programmatically by factories like timeseries-join
    #[serde(skip, default)]
    pub scope_prefixes: Option<HashMap<String, (String, String)>>,
    
    /// Optional closure to wrap TableProviders before registration
    /// Used by timeseries-pivot to inject null_padding_table wrapping
    /// NOT serialized - must be set programmatically
    #[serde(skip, default)]
    pub provider_wrapper: Option<Arc<dyn Fn(Arc<dyn TableProvider>) -> Result<Arc<dyn TableProvider>, Error> + Send + Sync>>,
}

impl SqlDerivedConfig {
    /// Create a new SqlDerivedConfig with patterns and optional query
    pub fn new(patterns: HashMap<String, String>, query: Option<String>) -> Self {
        Self {
            patterns,
            query,
            scope_prefixes: None,
            provider_wrapper: None,
        }
    }
    
    /// Create a new SqlDerivedConfig with scope prefixes for column renaming
    pub fn new_scoped(
        patterns: HashMap<String, String>,
        query: Option<String>,
        scope_prefixes: HashMap<String, (String, String)>,
    ) -> Self {
        Self {
            patterns,
            query,
            scope_prefixes: Some(scope_prefixes),
            provider_wrapper: None,
        }
    }
    
    /// Builder method to add a provider wrapper closure
    pub fn with_provider_wrapper<F>(mut self, wrapper: F) -> Self 
    where
        F: Fn(Arc<dyn TableProvider>) -> Result<Arc<dyn TableProvider>, Error> + Send + Sync + 'static,
    {
        self.provider_wrapper = Some(Arc::new(wrapper));
        self
    }

    /// Get the effective SQL query with table name substitution
    /// String replacement is reliable for table names - no fallbacks needed
    #[must_use]
    pub fn get_effective_sql(&self, options: &crate::SqlTransformOptions) -> String {
        let default_query: String;
        let original_sql = if let Some(query) = &self.query {
            query.as_str()
        } else {
            // Generate smart default based on patterns
            if self.patterns.len() == 1 {
                let pattern_name = self.patterns.keys().next().expect("checked");
                default_query = format!("SELECT * FROM {}", pattern_name);
                &default_query
            } else {
                "SELECT * FROM <specify_pattern_name>"
            }
        };

        // Direct string replacement - reliable and deterministic
        crate::transform_sql(original_sql, options)
    }

    /// Get the effective SQL query that this SQL-derived file represents
    ///
    /// This returns the complete SQL query including WHERE, ORDER BY, and other clauses
    /// as specified by the user. Use this when you need the full query semantics preserved.
    #[must_use]
    pub fn get_effective_sql_query(&self) -> String {
        self.get_effective_sql(&crate::SqlTransformOptions {
            table_mappings: Some(
                self.patterns
                    .keys()
                    .map(|k| (k.clone(), k.clone()))
                    .collect(),
            ),
            source_replacement: None,
        })
    }
}

impl std::fmt::Debug for SqlDerivedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlDerivedConfig")
            .field("patterns", &self.patterns)
            .field("query", &self.query)
            .field("scope_prefixes", &self.scope_prefixes)
            .field("provider_wrapper", &self.provider_wrapper.as_ref().map(|_| "<closure>"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_derived_config_new() {
        let patterns = [("table1".to_string(), "/path/*.series".to_string())].into();
        let config = SqlDerivedConfig::new(patterns, Some("SELECT * FROM table1".to_string()));
        
        assert_eq!(config.patterns.len(), 1);
        assert!(config.query.is_some());
        assert!(config.scope_prefixes.is_none());
        assert!(config.provider_wrapper.is_none());
    }

    #[test]
    fn test_sql_derived_config_scoped() {
        let patterns = [("table1".to_string(), "/path/*.series".to_string())].into();
        let scope_prefixes = [("table1".to_string(), ("scope_".to_string(), "timestamp".to_string()))].into();
        
        let config = SqlDerivedConfig::new_scoped(patterns, None, scope_prefixes);
        
        assert!(config.scope_prefixes.is_some());
        assert_eq!(config.scope_prefixes.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_sql_derived_config_serialization() {
        let patterns = [("table1".to_string(), "/path/*.series".to_string())].into();
        let config = SqlDerivedConfig::new(patterns, Some("SELECT * FROM table1".to_string()));
        
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SqlDerivedConfig = serde_json::from_str(&json).unwrap();
        
        assert_eq!(config.patterns, deserialized.patterns);
        assert_eq!(config.query, deserialized.query);
    }
}
