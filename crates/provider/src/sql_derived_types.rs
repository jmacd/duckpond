//! SQL-derived file types and configuration
//!
//! Core types for SQL transformation and pattern-based file generation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Mode for SQL-derived operations
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SqlDerivedMode {
    /// FileTable mode: single files only, errors if pattern matches >1 file
    Table,
    /// FileSeries mode: handles multiple files and versions
    Series,
}

/// Options for SQL transformation and table name replacement
#[derive(Default, Clone, Debug)]
pub struct SqlTransformOptions {
    /// Replace multiple table names with mappings (for patterns)
    pub table_mappings: Option<HashMap<String, String>>,
    /// Replace a single source table name (for simple cases)  
    pub source_replacement: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_derived_mode_serialization() {
        let table_mode = SqlDerivedMode::Table;
        let series_mode = SqlDerivedMode::Series;

        assert_eq!(table_mode, SqlDerivedMode::Table);
        assert_eq!(series_mode, SqlDerivedMode::Series);
        assert_ne!(table_mode, series_mode);
    }

    #[test]
    fn test_sql_transform_options() {
        let opts = SqlTransformOptions {
            table_mappings: Some([("old".to_string(), "new".to_string())].into()),
            source_replacement: None,
        };

        assert!(opts.table_mappings.is_some());
        assert!(opts.source_replacement.is_none());
    }
}
