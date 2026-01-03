# Logfile Ingestion Factory Design

## Overview

A new factory for ingesting rotating log files from a host directory into the pond. This factory mirrors external log files, tracking them with bao-tree blake3 digests for efficient change detection.

## Location

`crates/tlogfs/src/deltadir/mod.rs` - alongside existing filesystem-related functionality

## Factory Type

Executable variety (like `hydrovu` and `remote` factories) - invoked via CLI commands rather than running as a continuous service.

## Configuration

Single source per configuration file. Patterns use shell globs (leveraging tinyfs glob support).

```yaml
name: casparwater
# Glob pattern for archived (immutable) log files
archived_pattern: "/var/log/casparwater-*.json"
# Glob pattern for the active (append-only) log file  
active_pattern: "/var/log/casparwater.json"
# Destination path within the pond
pond_path: "logs/casparwater"
```

## File Naming Convention

Source files follow a naming pattern:
- **Archived files**: `{prefix}-{identifier}.json` - the identifier portion (often a datestamp) is treated as an opaque unique string, not parsed
- **Active file**: `{prefix}.json` (no identifier suffix, currently being written)

Example:
```
casparwater-2025-10-20T08-18-50.905.json  # archived, immutable
casparwater-2025-10-31T06-40-11.205.json  # archived, immutable
casparwater.json                           # active, append-only
```

## Behavioral Assumptions

| File Type | Mutability | Expected Behavior |
|-----------|------------|-------------------|
| Archived (timestamped) | Immutable | Once created, content never changes |
| Active (no timestamp) | Append-only | Only grows; existing bytes don't change |

## Core Operation

When invoked, the factory:

1. **Enumerate source files** - Match both patterns to find all log files
2. **Read pond state** - Check which files are already mirrored and their digests
3. **Detect changes**:
   - New archived files → full ingest
   - Missing archived files → (policy question: delete from pond or preserve?)
   - Active file → incremental append detection via bao-tree
4. **Write differences** - Mirror new/changed content into the pond

## Bao-Tree Integration

The bao-tree blake3 digest enables:
- Efficient verification that archived files haven't changed (unexpected)
- Incremental hashing of the active file as it grows
- Detection of exactly which bytes are new in the append-only file

### Bao-Tree Outboard Storage

A new optional field `bao_outboard: Option<Vec<u8>>` in `OplogEntry` stores blake3 Merkle tree data for verified streaming. This applies to both `FilePhysicalVersion` and `FilePhysicalSeries` entries regardless of size.

#### Schema Addition

```rust
// In OplogEntry
/// Bao-tree outboard data for blake3 verified streaming
/// Binary array of (left_hash, right_hash) pairs in post-order traversal
///
/// For FilePhysicalVersion: complete outboard for content verification
/// For FilePhysicalSeries: incremental outboard covering cumulative content
///   - Stable nodes (left subtrees) come first in post-order
///   - Unstable nodes (rightmost path) at end
///   - Pending fragment is tail of content (no separate storage needed)
///
/// Format: Raw concatenated hash pairs, 64 bytes each (32 + 32)
/// Size: (blocks - 1) * 64 bytes for complete file, less for partial
/// Block size: 16KB (BlockSize::from_chunk_log(4))
pub bao_outboard: Option<Vec<u8>>,
```

#### Why This Works

The bao-tree post-order format naturally separates stable from unstable nodes:

```
Post-order traversal for 5 blocks (16KB each):
                 Root (unstable)
                /    \
            P1        P2 (unstable)
           /  \      /  \
         C0   C1   C2   P3 (unstable)
                       /  \
                     C3   C4 (partial)

Post-order output: [P1, P3, P2, Root]
                    ↑    ↑    ↑    ↑
                  stable |  unstable
                         |
                   (boundary determined by tree geometry)
```

Given cumulative content size `S` with 16KB block size:
- **Blocks**: `ceil(S / 16384)`
- **Pending bytes**: `S % 16384` (tail of last version's content)
- **Stable/unstable boundary**: Computed from tree structure

#### Storage Efficiency (16KB Block Size)

Using `BlockSize::from_chunk_log(4)` = 16KB blocks:

| Cumulative Size | Blocks | Outboard Size | Overhead |
|-----------------|--------|---------------|----------|
| 16 KB | 1 | 0 bytes | 0% |
| 50 KB | 4 | 192 bytes | 0.4% |
| 100 KB | 7 | 384 bytes | 0.4% |
| 1 MB | 64 | 4,032 bytes | 0.4% |
| 10 MB | 640 | 40 KB | 0.4% |
| 100 MB | 6,400 | 409 KB | 0.4% |

### Incremental Checksum Algorithm

For `FilePhysicalSeries`, each version's outboard covers cumulative content:

```
Version 1: 10KB written
  Cumulative: 10KB (1 block, partial)
  Outboard: [] (0 hash pairs - single partial block)
  Pending: 10KB (tail of v1 content)

Version 2: +30KB written  
  Cumulative: 40KB (3 blocks, partial)
  Outboard: [P1] (1 hash pair - blocks 0+1 merged)
  Pending: 8KB (tail of v2 content, 40KB % 16KB)

Version 3: +24KB written
  Cumulative: 64KB (4 blocks, complete)
  Outboard: [P1, P2, Root] (3 hash pairs, all stable now)
  Pending: 0 bytes
```

#### Computing New Outboard on Append

```rust
const BLOCK_SIZE: usize = 16384; // 16KB blocks

fn append_version(
    prev_outboard: &[u8],
    prev_cumulative_size: u64,
    new_content: &[u8],
) -> Vec<u8> {
    // 1. Determine pending bytes from previous state
    let pending_size = (prev_cumulative_size % BLOCK_SIZE as u64) as usize;
    
    // 2. Get pending bytes from tail of previous version's content
    //    (caller must provide this, or we reconstruct from storage)
    
    // 3. Continue hashing: pending + new_content
    //    Using blake3::hazmat::{HasherExt, merge_subtrees_non_root}
    
    // 4. Merge completed chunks into subtree stack
    
    // 5. Update unstable nodes, emit new outboard bytes
    
    // 6. Return new complete outboard
}
```

#### Implementation Using blake3::hazmat

```rust
use blake3::hazmat::{ChainingValue, HasherExt, merge_subtrees_non_root, merge_subtrees_root, Mode};

const BLOCK_SIZE: usize = 16384; // 16KB blocks (chunk_log=4)

/// Hash a 16KB block using blake3's subtree hashing
/// Block position encodes which 16KB block this is
fn hash_block(data: &[u8], block_num: u64, is_root: bool) -> blake3::Hash {
    debug_assert!(data.len() <= BLOCK_SIZE);
    if is_root {
        blake3::hash(data)
    } else {
        let mut hasher = blake3::Hasher::new();
        // set_input_offset takes bytes; block N starts at byte N * 16KB
        hasher.set_input_offset(block_num * BLOCK_SIZE as u64);
        hasher.update(data);
        blake3::Hash::from(hasher.finalize_non_root())
    }
}

/// Combine two child hashes into parent
fn parent_cv(left: &blake3::Hash, right: &blake3::Hash, is_root: bool) -> blake3::Hash {
    let left_cv: ChainingValue = *left.as_bytes();
    let right_cv: ChainingValue = *right.as_bytes();
    if is_root {
        merge_subtrees_root(&left_cv, &right_cv, Mode::Hash)
    } else {
        blake3::Hash::from(merge_subtrees_non_root(&left_cv, &right_cv, Mode::Hash))
    }
}
```

### Prefix Verification for Active Files

With the stored outboard, prefix verification is straightforward:

#### Sync Algorithm for Active Files

When syncing an active file where previous pond cumulative size is `P` bytes and current host size is `H` bytes:

1. **Size check**: `H >= P` (append-only assumption)

2. **Prefix verification** (strict mode):
   - Re-compute outboard from host file bytes `[0, P)`
   - Compare against stored `bao_outboard`
   - Mismatch → host file was modified → error/warning

3. **Append new data**:
   - Read pending bytes from tail of previous content: `prev_content[P % BLOCK_SIZE..]`
   - Read new bytes from host: `[P, H)`
   - Compute new outboard continuing from previous state
   - Store new version with updated outboard

```rust
// Pseudo-code for sync operation
async fn sync_active_file(tx: &mut TransactionGuard<'_>, host_path: &Path) -> Result<()> {
    let pond_file = tx.get_file_physical_series("active.log").await?;
    let prev_cumulative_size = pond_file.cumulative_size().await?;
    let prev_outboard = pond_file.latest_outboard().await?;
    
    let host_file = File::open(host_path).await?;
    let host_size = host_file.metadata().await?.len();
    
    if host_size < prev_cumulative_size {
        return Err(Error::FileShrunk); // Unexpected: file truncated
    }
    
    if host_size == prev_cumulative_size {
        return Ok(()); // No new data
    }
    
    // Get pending bytes from tail of previous version's content (16KB block size)
    const BLOCK_SIZE: u64 = 16384;
    let pending_size = (prev_cumulative_size % BLOCK_SIZE) as usize;
    let pending_bytes = pond_file.read_tail(pending_size).await?;
    
    // Read only the new bytes from host
    let new_bytes = read_range(&host_file, prev_cumulative_size..host_size).await?;
    
    // Compute new outboard, continuing from prev_outboard
    let new_outboard = append_to_outboard(
        &prev_outboard,
        prev_cumulative_size,
        &pending_bytes,
        &new_bytes,
    )?;
    
    // Append new version with updated outboard
    pond_file.append_version(new_bytes, new_outboard).await?;
    
    Ok(())
}
```

### Verification Modes

The factory supports two verification modes:

#### Trust Mode (Default for Append-Only Files)
- Assume the append-only invariant holds
- Only compute new outboard from new bytes
- Fast: reads only new bytes from host + pending tail from pond

#### Strict Mode (Optional)
- Re-hash prefix from host file
- Compare against stored outboard
- Slower but catches silent corruption or non-append modifications

```yaml
# Configuration option
verification_mode: trust  # or "strict"
```

## Behavioral Policies

### Deleted Source Files

If an archived file disappears from the source, the pond copy is **preserved**. This maintains an audit trail and protects against accidental deletion. Future enhancement: make this configurable.

### Corrupted Archived Files

If an archived file's content changes (violating the immutability assumption), the system will **log a warning** and **not re-ingest**. This is expected to be bit-rot on the source side, not a legitimate change. The pond preserves the original content.

### Active File Rotation Detection

Rotation is detected by checking both **file size** and **content hash** (via existing blake3 bao-tree data).

**Normal case (single rotation):**
- One new archived file appears matching `archived_pattern`
- Active file size has decreased
- Strategy: In tinyfs, move the active file content preserving its original prefix, append the remaining segment, and complete the archive with matching checksums

**Complex case (multiple rotations):**
- Multiple new archived files appear since last sync
- One of them likely originated from the former active file
- Detection: Match using content hash from blake3 bao-tree data
- The matched file can be completed in tinyfs; others are ingested as new archives

## Storage Format

### Pond Structure

Files are mirrored in a single pond directory using identical filenames as the host files (flat structure).

Example:
- Host: `/var/log/casparwater-2025-10-20T08-18-50.905.json`
- Pond: `logs/casparwater/casparwater-2025-10-20T08-18-50.905.json`

Future enhancement: Support hierarchical organization if needed.

### Metadata Storage

Bao-tree outboard data is stored directly in the `OplogEntry.bao_outboard` field:

- **FilePhysicalVersion**: Complete outboard for single-version verification
- **FilePhysicalSeries**: Incremental outboard covering cumulative content (includes unstable nodes)
- **Large files** (>64KB stored externally): Outboard stored per-segment in chunked parquet

The `bao_outboard` field eliminates the need for separate metadata storage - it's part of the standard OplogEntry schema.

## Performance

### Concurrency Handling

Uses **consistent read** strategy when reading the active file. This is safe given the append-only assumption - the factory is designed for use cases where files only grow during operation.

## CLI Interface

### Primary Command: `sync`

The `sync` command performs the core operation of mirroring host log files into the pond.

**Flags:**
- `--dry-run` - Show what would be synced without making changes

**Note:** Verification of pond contents is handled as a separate feature in the broader codebase, not specific to this component.

## Next Steps

1. **Add `bao_outboard` field to OplogEntry schema** - Binary field for post-order outboard data
2. Define detailed CLI argument structure
3. Implement skeleton with configuration parsing
4. Implement file enumeration and pattern matching (using tinyfs glob support)
5. Implement change detection logic
6. Implement incremental outboard computation using `blake3::hazmat`
7. Implement pond writing with bao-tree digests
8. Add incremental active-file handling for rotation cases

## Related Documentation

- [DuckPond System Patterns](duckpond-system-patterns.md) - transaction handling
- [Anti-Duplication Philosophy](anti-duplication.md) - code reuse patterns
- [Remote Backup Chunked Parquet Design](remote-backup-chunked-parquet-design.md) - 16MB segment storage with blake3 outboard
- [Large File Parquet Storage Design](large-file-parquet-storage-design.md) - chunked storage format
- Bao-tree library: `bao-tree-rs/` - verified streaming with hierarchical checksums
- [Bao-tree Encoding and Validation Guide](../bao-tree-rs/ENCODING_AND_VALIDATION.md) - incremental hashing for non-power-of-two writes
