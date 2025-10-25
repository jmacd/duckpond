//! Transaction metadata that MUST be provided for all commits
//!
//! This module defines type-safe transaction metadata that ensures every
//! Delta Lake commit includes the original command information needed for
//! replication and backup/restore operations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Transaction metadata that MUST be included in every Delta Lake commit
///
/// This struct is required (not Option) to make it **impossible** to commit
/// without proper metadata at the type system level.
///
/// **Architecture**: 
/// - `txn_seq` is provided at `begin()` and managed by the transaction - NOT stored here
/// - `txn_id` (UUID7) is for Steward's recovery/debugging - links to control table
/// - `args` are the CLI command arguments (e.g., ["mkdir", "/data"])
/// - `vars` are key/value parameters (e.g., from -v flags in CLI)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondTxnMetadata {
    /// Unique transaction ID (UUID v7) for Steward recovery/debugging
    /// Links this transaction to control table records
    pub txn_id: String,
    
    /// Original CLI arguments that created this transaction
    /// Example: ["mkdir", "/etc"] or ["mknod", "hydrovu", "/etc/hydrovu"]
    pub args: Vec<String>,
    
    /// Key/value parameters (e.g., from -v CLI flags or environment)
    /// Example: {"user": "admin", "host": "prod1"}
    pub vars: HashMap<String, String>,
}

/// Delta Lake commit metadata wrapper with transaction sequence
///
/// This is the actual structure stored in Delta Lake commit metadata.
/// It includes both the PondTxnMetadata and the txn_seq that was active
/// during the transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaCommitMetadata {
    /// Transaction sequence number (Steward's authoritative ordering)
    pub txn_seq: i64,
    
    /// Transaction ID for debugging/recovery
    pub txn_id: String,
    
    /// CLI command arguments
    pub args: Vec<String>,
    
    /// Key/value parameters
    #[serde(default)]
    pub vars: HashMap<String, String>,
}

impl DeltaCommitMetadata {
    /// Create from PondTxnMetadata and txn_seq
    pub fn new(metadata: &PondTxnMetadata, txn_seq: i64) -> Self {
        Self {
            txn_seq,
            txn_id: metadata.txn_id.clone(),
            args: metadata.args.clone(),
            vars: metadata.vars.clone(),
        }
    }
    
    /// Convert to PondTxnMetadata (without txn_seq)
    pub fn to_pond_metadata(&self) -> PondTxnMetadata {
        PondTxnMetadata {
            txn_id: self.txn_id.clone(),
            args: self.args.clone(),
            vars: self.vars.clone(),
        }
    }
}

impl PondTxnMetadata {
    /// Create new transaction metadata
    ///
    /// # Arguments
    /// * `txn_id` - UUID7 identifier for recovery (Steward generates this)
    /// * `args` - CLI command arguments
    /// * `vars` - Key/value parameters (like -v flags)
    ///
    /// Note: `txn_seq` is NOT a parameter - it's provided to `begin(txn_seq, metadata)`
    pub fn new(txn_id: String, args: Vec<String>, vars: HashMap<String, String>) -> Self {
        Self {
            txn_id,
            args,
            vars,
        }
    }

    /// Convert to Delta Lake commit metadata format, injecting txn_seq
    ///
    /// The `txn_seq` parameter comes from `begin()`, ensuring sequence is
    /// specified once at transaction start, not at commit.
    ///
    /// Returns a HashMap ready to be passed to Delta Lake's commit operation
    pub fn to_delta_metadata(&self, txn_seq: i64) -> HashMap<String, serde_json::Value> {
        let mut metadata = HashMap::new();
        
        let delta_metadata = DeltaCommitMetadata::new(self, txn_seq);
        let pond_txn = serde_json::to_value(&delta_metadata)
            .expect("Failed to serialize DeltaCommitMetadata");
        
        metadata.insert("pond_txn".to_string(), pond_txn);
        metadata
    }

    /// Extract from Delta Lake commit metadata (for reading backups)
    ///
    /// Returns None if pond_txn field is missing or malformed.
    /// Note: txn_seq is stored in metadata but not returned (caller already knows it from context)
    pub fn from_delta_metadata(metadata: &HashMap<String, serde_json::Value>) -> Option<Self> {
        let pond_txn = metadata.get("pond_txn")?;
        let delta_metadata: DeltaCommitMetadata = serde_json::from_value(pond_txn.clone()).ok()?;
        Some(delta_metadata.to_pond_metadata())
    }
    
    /// Extract txn_seq from Delta Lake commit metadata
    ///
    /// This is used when reopening an existing pond to determine the last transaction sequence.
    pub fn extract_txn_seq(metadata: &HashMap<String, serde_json::Value>) -> Option<i64> {
        let pond_txn = metadata.get("pond_txn")?;
        let delta_metadata: DeltaCommitMetadata = serde_json::from_value(pond_txn.clone()).ok()?;
        Some(delta_metadata.txn_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_roundtrip() {
        let mut vars = HashMap::new();
        vars.insert("user".to_string(), "admin".to_string());
        vars.insert("host".to_string(), "prod1".to_string());
        
        let original = PondTxnMetadata::new(
            "019a19ee-fc31-7bb9-90ae-bcc74366be27".to_string(),
            vec!["mknod".to_string(), "hydrovu".to_string(), "/etc/hydrovu".to_string()],
            vars.clone(),
        );

        let delta_metadata = original.to_delta_metadata(4);  // txn_seq=4 for example
        let recovered = PondTxnMetadata::from_delta_metadata(&delta_metadata)
            .expect("Should recover metadata");

        assert_eq!(original.txn_id, recovered.txn_id);
        assert_eq!(original.args, recovered.args);
        assert_eq!(original.vars, recovered.vars);
    }

    #[test]
    fn test_metadata_format() {
        let mut vars = HashMap::new();
        vars.insert("env".to_string(), "test".to_string());
        
        let metadata = PondTxnMetadata::new(
            "test-txn-id".to_string(),
            vec!["mkdir".to_string(), "/etc".to_string()],
            vars,
        );

        let delta_metadata = metadata.to_delta_metadata(5);  // txn_seq=5
        
        assert!(delta_metadata.contains_key("pond_txn"));
        
        let pond_txn = &delta_metadata["pond_txn"];
        assert_eq!(pond_txn["txn_id"], "test-txn-id");
        assert_eq!(pond_txn["txn_seq"], 5);
        assert_eq!(pond_txn["vars"]["env"], "test");
    }
}
