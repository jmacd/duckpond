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

Bao-tree digests are stored using the pond's existing built-in infrastructure:
- **Large files** (~100MB): Segmented with separate outboard data for streaming verification
- **Small files**: Blake3 data stored, but delta-encoding not used (focus is on large file efficiency)

No additional metadata storage mechanism needed - leverages existing bao-tree integration.

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

1. Define detailed CLI argument structure
2. Implement skeleton with configuration parsing
3. Implement file enumeration and pattern matching (using tinyfs glob support)
4. Implement change detection logic
5. Implement pond writing with bao-tree digests
6. Add incremental active-file handling for rotation cases

## Related Documentation

- [DuckPond System Patterns](duckpond-system-patterns.md) - transaction handling
- [Anti-Duplication Philosophy](anti-duplication.md) - code reuse patterns
- Bao-tree integration in existing codebase
