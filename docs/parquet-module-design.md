# Parquet Module Design

## Location

`crates/tinyfs/src/arrow/parquet.rs`

## Purpose

This module provides Parquet file integration for TinyFS, enabling structured data storage with automatic temporal metadata extraction.

---

## Conceptual Model

The module serves two distinct file types in DuckPond:

| File Type | Entry Type | Temporal Metadata | Use Case |
|-----------|------------|-------------------|----------|
| **Table** | `FileTablePhysical` | None | Static reference data, configurations |
| **Series** | `FileSeriesPhysical` | Yes (min/max timestamp) | Time-series sensor data, logs |

**Key insight**: A "Series" is just a parquet file with temporal bounds attached. The bounds enable efficient time-range queries without reading file contents.

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                      ParquetExt Trait                           │
│  (async methods on WD - user-facing API)                        │
├─────────────────────────────────────────────────────────────────┤
│  create_table_from_items()   read_table_as_items()              │
│  create_table_from_batch()   read_table_as_batch()              │
│  create_series_from_items()  create_series_from_batch()         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                  Core Functions                                  │
│  (sync, in-memory operations)                                   │
├─────────────────────────────────────────────────────────────────┤
│  serialize_batch_to_parquet()    parse_parquet_to_batch()       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│               Temporal Bounds Extraction                         │
│  (Arrow-native computation)                                     │
├─────────────────────────────────────────────────────────────────┤
│  extract_temporal_bounds_from_batch()     ← Arrow compute       │
│  extract_temporal_bounds_from_parquet_metadata()  ← Statistics  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Naming Conventions Explained

### "Table" vs "Series"

These are **DuckPond concepts**, not Arrow/Parquet terms:

- **Table** (`FileTablePhysical`): A parquet file treated as static data. No temporal indexing.
- **Series** (`FileSeriesPhysical`): A parquet file with temporal bounds stored in metadata. Enables time-range queries.

### Method Naming Pattern

| Pattern | Meaning |
|---------|---------|
| `*_from_items` | Takes `Vec<T>` where T implements `ForArrow + Serialize` |
| `*_from_batch` | Takes raw `RecordBatch` |
| `*_as_items` | Returns `Vec<T>` where T implements `ForArrow + Deserialize` |
| `*_as_batch` | Returns raw `RecordBatch` |
| `create_table_*` | Writes as `FileTablePhysical` (no temporal metadata) |
| `create_series_*` | Writes as `FileSeriesPhysical` (extracts temporal bounds) |

### Why "ForArrow" trait?

`ForArrow` defines the Arrow schema for a Rust struct. This enables automatic serialization via `serde_arrow`:

```rust
impl ForArrow for SensorReading {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("timestamp", DataType::Int64, false)),
            Arc::new(Field::new("value", DataType::Float64, false)),
        ]
    }
}
```

---

## Two Temporal Extraction Strategies

### 1. `extract_temporal_bounds_from_batch()` — Arrow Compute Kernels

**Used when**: Creating new series files with `create_series_from_*`

```rust
let (min, max) = extract_temporal_bounds_from_batch(&batch, "timestamp")?;
```

- Uses `arrow::compute::min()` and `arrow::compute::max()`
- Works on in-memory RecordBatch before serialization
- Zero parsing overhead
- Handles multiple timestamp types (Int64, TimestampSecond, TimestampMillisecond, etc.)

### 2. `extract_temporal_bounds_from_parquet_metadata()` — Parquet Statistics

**Used when**: Processing existing parquet files (e.g., `copy` command)

```rust
let (min, max) = extract_temporal_bounds_from_parquet_metadata(&metadata, &schema, "timestamp")?;
```

- Reads row group statistics from parquet footer
- Does NOT load actual data into memory
- Required for streaming imports where we don't have RecordBatch in memory

---

## Usage Examples

### Writing a Table (no temporal metadata)

```rust
let records = vec![
    Config { key: "setting1".into(), value: 42 },
    Config { key: "setting2".into(), value: 100 },
];

wd.create_table_from_items("config.parquet", &records, EntryType::FileTablePhysical).await?;
```

### Writing a Series (with temporal metadata)

```rust
let readings = vec![
    SensorReading { timestamp: 1703500000, value: 23.5 },
    SensorReading { timestamp: 1703500060, value: 24.1 },
];

// Returns (min_timestamp, max_timestamp)
let (min, max) = wd.create_series_from_items(
    "sensor/readings.parquet",
    &readings,
    Some("timestamp"),  // column name for temporal bounds
).await?;

// min = 1703500000, max = 1703500060
```

### Reading data back

```rust
// High-level: deserialize to structs
let readings: Vec<SensorReading> = wd.read_table_as_items("sensor/readings.parquet").await?;

// Low-level: raw RecordBatch
let batch = wd.read_table_as_batch("sensor/readings.parquet").await?;
```

---

## Design Decisions

### Why async trait on WD?

`WD` (Working Directory) is the primary filesystem abstraction. Adding `ParquetExt` as an extension trait:
- Keeps parquet logic separate from core filesystem code
- Methods naturally compose with other WD operations
- Async operations flow through the existing filesystem layer

### Why separate "batch" and "items" methods?

Two audiences:
- **Application code**: Uses `*_items` with Rust structs
- **Data processing code**: Uses `*_batch` with raw Arrow arrays

### Why WriteOptions struct (even if minimal)?

Follows the anti-duplication pattern. When we need additional options (compression, row group size, etc.), we extend the struct rather than adding new method variants.

---

## File Dependencies

```
parquet.rs
├── super::schema::ForArrow       (trait for Arrow schema definition)
├── crate::WD                     (working directory type)
├── crate::EntryType              (FileTablePhysical, FileSeriesPhysical, etc.)
├── arrow_array::*                (RecordBatch, typed arrays)
├── parquet::arrow::ArrowWriter   (serialization)
├── parquet::arrow::arrow_reader  (deserialization)
├── arrow::compute::{min, max}    (temporal extraction)
└── serde_arrow                   (struct ↔ RecordBatch conversion)
```
