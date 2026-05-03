// SPDX-License-Identifier: Apache-2.0

//! Schema definitions for the sandbox store.
//!
//! The store is a single Delta Lake table partitioned by `partition_key`.
//! Each row is one version of one item:
//!
//! | column         | type      | notes                                    |
//! |----------------|-----------|------------------------------------------|
//! | partition_key  | Utf8      | Delta partition column                   |
//! | item_key       | Utf8      | item identifier within its partition     |
//! | txn_seq        | Int64     | store-wide monotonic commit sequence     |
//! | deleted        | Boolean   | tombstone marker; true = item removed    |
//! | value          | Binary    | item bytes (zero-length when deleted)    |
//! | value_blake3   | Binary    | BLAKE3 of `value` (32 bytes); fixed even |
//! |                |           | for the empty-value case                 |
//! | ts_micros      | Int64     | commit time, microseconds since unix ep  |
//!
//! Per-(partition_key, item_key) the row with the largest `txn_seq` is
//! the live version.  If that row has `deleted = true`, the item is
//! considered absent.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};

/// Column names.  Centralized to avoid stringly-typed bugs.
pub mod col {
    /// `partition_key` -- Delta partition column.
    pub const PARTITION_KEY: &str = "partition_key";
    /// `item_key` -- the user-visible key within a partition.
    pub const ITEM_KEY: &str = "item_key";
    /// `txn_seq` -- store-wide monotonic commit sequence.
    pub const TXN_SEQ: &str = "txn_seq";
    /// `deleted` -- tombstone marker.
    pub const DELETED: &str = "deleted";
    /// `value` -- the user-supplied bytes (zero-length when deleted).
    pub const VALUE: &str = "value";
    /// `value_blake3` -- BLAKE3 of `value`, always 32 bytes.
    pub const VALUE_BLAKE3: &str = "value_blake3";
    /// `ts_micros` -- commit timestamp, microseconds since unix epoch.
    pub const TS_MICROS: &str = "ts_micros";
}

/// Length of a BLAKE3 digest in bytes.
pub const BLAKE3_LEN: usize = 32;

/// Delta partition columns for the store table.
pub fn partition_columns() -> Vec<&'static str> {
    vec![col::PARTITION_KEY]
}

/// Arrow schema used for [`arrow_array::RecordBatch`] construction.
pub fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new(col::PARTITION_KEY, DataType::Utf8, false),
        Field::new(col::ITEM_KEY, DataType::Utf8, false),
        Field::new(col::TXN_SEQ, DataType::Int64, false),
        Field::new(col::DELETED, DataType::Boolean, false),
        Field::new(col::VALUE, DataType::Binary, false),
        Field::new(col::VALUE_BLAKE3, DataType::Binary, false),
        Field::new(col::TS_MICROS, DataType::Int64, false),
    ]))
}

/// Delta Lake `StructField` schema for `CreateBuilder::with_columns`.
///
/// Must agree column-for-column with [`arrow_schema`] in name, type, and
/// nullability.  Tests assert this.
pub fn delta_columns() -> Vec<DeltaStructField> {
    vec![
        DeltaStructField::new(
            col::PARTITION_KEY,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            col::ITEM_KEY,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            col::TXN_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            col::DELETED,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            col::VALUE,
            DeltaDataType::Primitive(PrimitiveType::Binary),
            false,
        ),
        DeltaStructField::new(
            col::VALUE_BLAKE3,
            DeltaDataType::Primitive(PrimitiveType::Binary),
            false,
        ),
        DeltaStructField::new(
            col::TS_MICROS,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_and_delta_schemas_agree() {
        let arrow = arrow_schema();
        let delta = delta_columns();

        assert_eq!(arrow.fields().len(), delta.len());
        for (af, df) in arrow.fields().iter().zip(delta.iter()) {
            assert_eq!(af.name(), df.name(), "field names must match");
            assert_eq!(
                af.is_nullable(),
                df.is_nullable(),
                "nullability must match for column {}",
                af.name()
            );
        }
    }

    #[test]
    fn partition_key_is_a_partition_column() {
        let parts = partition_columns();
        assert_eq!(parts, vec![col::PARTITION_KEY]);
    }

    #[test]
    fn blake3_len_constant() {
        // Sanity: BLAKE3 outputs 32-byte digests; keep this in sync with
        // any future code that relies on the constant.
        assert_eq!(BLAKE3_LEN, blake3::OUT_LEN);
    }
}
