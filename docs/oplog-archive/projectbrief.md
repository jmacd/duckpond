# DuckPond OpLog - Project Brief

## Project Overview
The `oplog` crate implements an operation log system as part of the larger DuckPond project. This system provides a way to track and manage sequences of operations in a local system using a two-layered architecture.

## Core Requirements
1. **Delta Lake Integration**: Use delta-rs library for operation log storage with ACID guarantees
2. **DataFusion Integration**: Provide SQL query capabilities over stored operations  
3. **Nested Schema Support**: Enable storage of heterogeneous data with schema evolution
4. **Arrow IPC Serialization**: Use Arrow IPC format for efficient data serialization within Delta Lake

## Key Components
- **Record**: Core data structure with `node_id`, `timestamp`, `version`, `content` fields
- **Entry**: Embedded data structure stored within Record's `content` field
- **ByteStreamTable**: Custom DataFusion TableProvider for reading serialized data
- **ForArrow Trait**: Schema conversion between Arrow and Delta Lake formats

## Architecture Goals
- Local-first operation logging system
- Efficient query performance through DataFusion
- Schema flexibility through nested serialization
- Type-safe Rust implementation with proper error handling

## Current Focus
Building the foundational layer with Delta Lake storage and DataFusion query capabilities, learning the ecosystems as we implement.
