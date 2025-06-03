# Product Context - OpLog System

## Problem Statement
We need a robust operation logging system that can:
- Store sequences of operations with ACID guarantees
- Enable efficient querying and analysis of historical operations
- Support schema evolution as the system grows
- Maintain performance while handling large volumes of operation data

## Solution Approach
The OpLog system solves this through a two-layered data architecture:

### Layer 1: Delta Lake Storage
- Stores `Record` structures with metadata (node_id, timestamp, version)
- Provides ACID guarantees, time travel, and efficient storage
- Uses partitioning by `node_id` for query performance
- Handles the "outer" schema that rarely changes

### Layer 2: Arrow IPC Content
- Stores variable `Entry` data serialized as Arrow IPC in `content` field
- Enables schema evolution without Delta Lake schema migrations
- Supports heterogeneous data types within same table
- Provides efficient deserialization for query processing

## User Experience Goals
- **Developers**: Simple API for logging operations and querying historical data
- **System Administrators**: Reliable data storage with backup/recovery capabilities  
- **Data Analysts**: SQL query interface through DataFusion for operation analysis

## Key Benefits
1. **Flexibility**: Inner schema evolution without outer schema changes
2. **Performance**: DataFusion SQL queries with Arrow-native processing
3. **Reliability**: Delta Lake ACID guarantees and versioning
4. **Scalability**: Partitioned storage and efficient query execution
