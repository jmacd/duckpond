# System Patterns - OpLog Architecture

## Core Architecture Pattern: Two-Layer Data Storage

```
┌─────────────────────────────────────────────┐
│               DataFusion SQL                │
│          (Query Interface)                  │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│           ByteStreamTable                   │
│      (Custom TableProvider)                 │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│             Delta Lake                      │
│    Records: node_id, timestamp, version    │
│              content: Vec<u8>               │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│           Arrow IPC Stream                  │
│     Entries: name, node_id                  │
│        (Serialized in content)              │
└─────────────────────────────────────────────┘
```

## Key Patterns

### 1. Schema Abstraction - ForArrow Trait
```rust
trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;     // Arrow schema
    fn for_delta() -> Vec<DeltaStructField>; // Delta schema conversion
}
```
**Purpose**: Consistent schema definition across Arrow/Delta boundaries

### 2. Nested Serialization Pattern
- **Outer Layer**: Delta Lake with stable schema (Record)  
- **Inner Layer**: Arrow IPC with evolving schema (Entry)
- **Benefit**: Schema evolution without table migrations

### 3. Custom DataFusion Integration
```rust
ByteStreamTable -> ByteStreamExec -> RecordBatchStream
```
**Purpose**: Bridge Delta Lake storage to DataFusion query engine

## Component Relationships

### Data Flow
1. **Write Path**: Entry → Arrow IPC → Record.content → Delta Lake
2. **Read Path**: Delta Lake → Record.content → Arrow IPC → Entry → DataFusion

### Critical Implementation Points
- `encode_batch_to_buffer()`: Entry → Vec<u8> serialization
- `ByteStreamExec.execute()`: Vec<u8> → RecordBatch deserialization  
- Delta Lake partitioning by `node_id` for query performance

## Design Decisions
- **Partitioning Strategy**: Use `node_id` as partition key for locality
- **Time Precision**: Microsecond timestamps for high-resolution logging
- **Error Handling**: Custom error types with proper propagation
- **Async Design**: Full async/await pattern for I/O operations
