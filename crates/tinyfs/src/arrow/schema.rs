// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Schema definitions for Arrow integration

use arrow::datatypes::{DataType, FieldRef, TimeUnit};
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use std::collections::HashMap;

/// Trait for converting data structures to Arrow and Delta Lake schemas
///
/// This trait was moved from tlogfs to tinyfs to avoid circular dependencies
/// while allowing both tinyfs arrow support and tlogfs persistence to use it.
pub trait ForArrow {
    /// Define the Arrow schema for this type
    fn for_arrow() -> Vec<FieldRef>;

    /// Default implementation that converts Arrow schema to Delta Lake schema
    /// This enables compatibility with Delta Lake storage in tlogfs
    #[must_use]
    fn for_delta() -> Vec<DeltaStructField> {
        let afs = Self::for_arrow();

        afs.into_iter()
            .map(|af| {
                let prim_type = match af.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
                    DataType::Utf8 => PrimitiveType::String,
                    DataType::Binary => PrimitiveType::Binary,
                    DataType::Int64 => PrimitiveType::Long,
                    _ => panic!("configure this type: {:?}", af.data_type()),
                };

                DeltaStructField {
                    name: af.name().to_string(),
                    data_type: DeltaDataType::Primitive(prim_type),
                    nullable: af.is_nullable(),
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }
}
