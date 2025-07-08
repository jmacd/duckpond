# UUID7 Migration Plan - DuckPond ID System Overhaul

## Overview
Migrate from sequential integer NodeIDs to UUID7-based identifiers to eliminate expensive ID scanning, improve uniqueness guarantees, and simplify the architecture.

## Goals
1. **Performance**: Eliminate O(n) startup scanning for max NodeID
2. **Uniqueness**: UUID7 provides globally unique identifiers with time ordering
3. **Simplicity**: NodeID becomes self-sufficient, no coordination needed
4. **Display**: Truncate to 8 hex digits for display (like git SHAs)
5. **Storage**: Full UUID7 strings for actual identifiers and filenames

## Current State Analysis

### Current ID System Issues
- **NodeID**: Sequential integers requiring expensive max() scanning on startup
- **PartitionID**: String-based but tied to NodeID for directories
- **Root Assumption**: Hardcoded NodeID(0) as root directory
- **Coordination**: Global atomic counters need initialization from oplog

### Current Dependencies
- `tinyfs::NodeID` - wrapper around `usize`
- `tlogfs::persistence` - uses string conversion via `to_hex_string()`
- `steward` - relies on NodeID generation for transaction metadata
- Display logic - truncates leading zeros from hex

## Target Architecture

### New ID System
```rust
// NodeID becomes UUID7-based
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeID(String); // Full UUID7 string

impl NodeID {
    /// Generate new UUID7-based NodeID
    pub fn new() -> Self {
        Self(uuid7::Uuid::now_v7().to_string())
    }
    
    /// Parse from full UUID7 string
    pub fn from_string(s: &str) -> Result<Self, Error> {
        uuid7::Uuid::parse_str(s)?;
        Ok(Self(s.to_string()))
    }
    
    /// Get full UUID7 string for storage/filenames
    pub fn to_string(&self) -> String {
        self.0.clone()
    }
    
    /// Get shortened display version (8 chars like git)
    pub fn to_short_string(&self) -> String {
        self.0.chars().take(8).collect()
    }
    
    /// Root directory gets a special UUID7 (deterministic)
    pub fn root() -> Self {
        // Use a fixed UUID7 for root, or generate deterministically
        Self("01234567-89ab-7cde-8000-000000000000".to_string())
    }
}

// PartitionID remains string but uses same UUID7 format
pub type PartitionID = String;

pub fn new_partition_id() -> PartitionID {
    uuid7::Uuid::now_v7().to_string()
}
```

### Display Changes
```rust
// In list/show commands
format!("{} v{} {}", node_id.to_short_string(), version, name)
// Example: "a1b2c3d4 v2 /file.txt"
```

## Implementation Phases

### Phase 1: Core NodeID Migration
**Files to modify:**
- `crates/tinyfs/src/node.rs` - Update NodeID struct and methods
- `crates/tinyfs/src/lib.rs` - Add uuid7 dependency
- `Cargo.toml` - Add uuid7 crate dependency

**Changes:**
1. Replace `NodeID(usize)` with `NodeID(String)`
2. Implement UUID7-based generation
3. Update `to_hex_string()` → `to_string()`
4. Add `to_short_string()` for display
5. Remove sequential counter logic

### Phase 2: Storage Layer Updates
**Files to modify:**
- `crates/tlogfs/src/persistence.rs` - Remove expensive ID scanning
- `crates/tlogfs/src/schema.rs` - Ensure string fields handle full UUIDs
- `crates/tlogfs/src/directory.rs` - Update ID parsing

**Changes:**
1. Remove `initialize_node_id_counter()` - no longer needed
2. Update `OplogEntry.node_id` to store full UUID7 strings
3. Remove `next_sequential()` methods
4. Update parsing/serialization for longer IDs

### Phase 3: Display Layer Updates  
**Files to modify:**
- `crates/cmd/src/commands/list.rs` - Use short display format
- `crates/cmd/src/commands/show.rs` - Use short display format
- Any diagnostic/logging code - Use appropriate format per context

**Changes:**
1. Display commands use `node_id.to_short_string()`
2. Storage/filenames use `node_id.to_string()`
3. Update formatting in diagnostic logs

### Phase 4: Root Directory Handling
**Files to modify:**
- `crates/tinyfs/src/node.rs` - Remove ROOT_ID constant
- `crates/tlogfs/src/persistence.rs` - Update root detection logic
- `crates/tinyfs/src/fs.rs` - Update root creation

**Changes:**
1. Replace `ROOT_ID = NodeID(0)` with `NodeID::root()`
2. Update root directory detection logic
3. Ensure root is created with deterministic UUID7

### Phase 5: Steward Integration
**Files to modify:**
- `crates/steward/src/ship.rs` - Update transaction metadata generation
- Transaction sequence logic - May benefit from UUID7 for metadata files

**Changes:**
1. Transaction metadata files use UUID7-based names if desired
2. Verify steward works with new ID system
3. Update debugging output to use short display format

## Testing Strategy

### Unit Tests
1. **NodeID Generation**: Verify UUID7 format and uniqueness
2. **Display Formatting**: Test short vs full string methods
3. **Parsing**: Round-trip string ↔ NodeID conversion
4. **Root Handling**: Consistent root NodeID generation

### Integration Tests
1. **Persistence**: Full UUID7 strings stored and retrieved correctly
2. **Display**: Commands show shortened IDs correctly
3. **Cross-Session**: UUIDs persist across pond reopening
4. **Performance**: No expensive scanning on startup

### Migration Tests
1. **Backward Compatibility**: Can we read old integer-based oplogs?
2. **Mixed Environment**: Graceful handling of mixed ID formats during transition

## Dependencies

### New Crate Dependencies
```toml
[dependencies]
uuid7 = "1.0"  # Or latest version
```

### Affected Modules
- `tinyfs` - Core NodeID struct
- `tlogfs` - Persistence and serialization  
- `cmd` - Display formatting
- `steward` - Transaction coordination

## Risk Mitigation

### Breaking Changes
- **Impact**: All existing oplogs have integer NodeIDs
- **Mitigation**: Consider migration script or version detection
- **Alternative**: Start fresh with UUID7 (acceptable for development)

### Performance Considerations
- **UUID7 Generation**: Very fast (timestamp + random)
- **String Storage**: Slight increase in storage (16 bytes vs 8 for u64)
- **Parsing**: UUID parsing is efficient
- **Network**: Longer IDs in serialization (acceptable trade-off)

### Display Consistency
- **8-char Limit**: Same as git, familiar to developers
- **Collision Risk**: Extremely low with 8 hex chars (4 billion combinations)
- **Fallback**: Can always show full UUID if collision suspected

## Success Criteria

1. ✅ **No Startup Scanning**: Eliminate `initialize_node_id_counter()` calls
2. ✅ **Global Uniqueness**: NodeIDs unique across all ponds/partitions
3. ✅ **Clean Display**: 8-character IDs in list/show commands
4. ✅ **Full Functionality**: All existing commands work with new IDs
5. ✅ **Performance**: Faster initialization, no coordination overhead

## Implementation Order

1. **Phase 1**: Core NodeID changes (breaking but isolated)
2. **Phase 2**: Storage layer (ensure persistence works)
3. **Phase 3**: Display updates (user-visible improvements)
4. **Phase 4**: Root handling (clean up legacy assumptions)
5. **Phase 5**: Steward integration (transaction system)

This migration will significantly improve performance and eliminate the expensive ID coordination issues while providing a clean, git-like display experience.
