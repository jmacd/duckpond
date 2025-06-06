use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use std::collections::HashMap;

use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};

use std::sync::Arc;
use serde::{Deserialize, Serialize};

pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    fn for_delta() -> Vec<DeltaStructField> {
        let afs = Self::for_arrow();

        afs.into_iter()
            .map(|af| {
                let prim_type = match af.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
                    DataType::Utf8 => PrimitiveType::String,
                    DataType::Binary => PrimitiveType::Binary,
                    DataType::Int64 => PrimitiveType::Long,
                    _ => panic!("configure this type"),
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    pub part_id: String,  // Hex encoded unsigned (partition key, a directory name)
    pub timestamp: i64,   // Microsecond precision
    pub version: i64,     // Incrementing
    pub content: Vec<u8>, // Content
}

impl ForArrow for Record {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(
                    // Delta requires "UTC"
                    // Arrow recommends "+00:00"
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                false,
            )),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, false)),
        ]
    }
}
