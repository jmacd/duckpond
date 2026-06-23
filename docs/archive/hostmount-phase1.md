# Hostmount Phase 1 — Implementation Record

## What Was Built

Phase 1 implements a `PersistenceLayer` backed by `std::fs` / `tokio::fs`, making
a host directory tree accessible through TinyFS's existing `FS`, `WD`, `File`, and
`Directory` abstractions. The result: you can read, write, list, and create files
on the host filesystem using the same code paths that operate on pond-internal data.

## Files Created

### `crates/tinyfs/src/hostmount/mod.rs`

Module root. Exports the three core types and provides a convenience constructor:

```rust
pub async fn new_fs(root: PathBuf) -> Result<FS>
```

Creates a `HostmountPersistence`, wraps it in `FS::new()`, and returns a ready-to-use
filesystem where `/` maps to the given host directory.

### `crates/tinyfs/src/hostmount/file.rs` — `HostFile`

Implements the `File` trait (and `Metadata`) backed by direct host I/O.

- `async_reader()` → `tokio::fs::File::open`
- `async_writer()` → `tokio::fs::File::create`, wrapped in `HostFileWriter`
- No versioning, no bao-tree integrity, no OpLog
- `FileMetadataWriter` methods (`set_temporal_metadata`, `infer_temporal_bounds`)
  are no-ops / errors — host files don't track temporal metadata

Key design: `host_path` is `Option<PathBuf>`:

| Constructor | `host_path` | Meaning |
|---|---|---|
| `new(path, id)` | `Some(path)` | Known location, I/O works |
| `new_pending(id)` | `None` | Created by persistence, not yet placed |

Every I/O method calls `require_path()` which returns a clear error if the path
is `None`: *"HostFile {id} has no path — node was created but not yet inserted
into a directory"*.

### `crates/tinyfs/src/hostmount/directory.rs` — `HostDirectory`

Implements the `Directory` trait (and `Metadata`) backed by `std::fs::read_dir`.

**Read operations:**
- `get(name)` → joins name to `host_path`, stats the result, returns a `Node`.
  Includes a canonicalize-based path traversal guard.
- `entries()` → `std::fs::read_dir`, skips dotfiles, sorts alphabetically,
  returns a stream of `DirectoryEntry`.

**Write operations:**
- `insert(name, node)` → creates the file/dir on the host filesystem, then
  replaces the pending inner object with one that has the real path (see
  "The Pending-Path Pattern" below).
- `remove(name)` → `std::fs::remove_file` or `std::fs::remove_dir_all`.

**Child FileIDs are deterministic** via `FileID::from_content(parent_part_id, entry_type, name.as_bytes())`. Same directory + same filename = same FileID across runs.

Same `Option<PathBuf>` pattern as `HostFile`, with `require_path()` guard.

### `crates/tinyfs/src/hostmount/persistence.rs` — `HostmountPersistence`

Implements `PersistenceLayer` — the 16-method async trait that `FS` requires.

**State:**
- `root_path: PathBuf` — canonicalized host root
- `path_registry: Arc<Mutex<HashMap<FileID, PathBuf>>>` — reverse map from
  FileID → host path, populated as directories are traversed
- `txn_state: Arc<TransactionState>` — no-op (hostmount has no transactions)

**Key method behaviors:**

| Method | Behavior |
|---|---|
| `load_node(id)` | Looks up registry → stats host path → returns File or Directory node |
| `create_file_node(id)` | If registered, returns node with path. Otherwise returns **pending** node (`host_path=None`) |
| `create_directory_node(id)` | Same pending pattern as files |
| `store_node(node)` | No-op (Directory::insert handles host writes) |
| `metadata(id)` | Stats host path, returns `NodeMetadata` |
| `list_file_versions(id)` | Returns single version from file mtime |
| `create_symlink_node` | Error: not supported |
| `create_dynamic_node` | Error: not supported |
| `get_temporal_bounds` | Always `None` |

### `crates/tinyfs/src/hostmount/tests.rs`

24 tests covering:

**Persistence layer (9 tests):**
- Valid/invalid construction (nonexistent path, file-not-dir)
- Root node loading
- Unknown FileID errors
- Metadata on root
- Temporal bounds always None
- Symlink/dynamic node rejection

**FS + WD integration (15 tests):**
- Root access
- Directory listing (correct entries, dotfile exclusion, alphabetical sort)
- File reading (direct, nested, `read_file_path_to_vec`)
- File writing through `create_file_path_streaming_with_type` + verify on host
- Directory creation (`create_dir_path`, `create_dir_all` for nested)
- Error cases (file not found, directory already exists)
- Deterministic FileIDs (same path → same ID across two FS instances)
- Path traversal blocked (`../../../etc/passwd`)
- Entry type correctness (files → `FilePhysicalVersion`, dirs → `DirectoryPhysical`)

## Files Modified

### `crates/tinyfs/src/lib.rs`

Two additions:
- `pub mod hostmount;` — exposes the module
- `pub use hostmount::HostmountPersistence;` — re-export at crate root

### `crates/tinyfs/src/dir.rs`

Added `get_inner()` method to `dir::Handle`:

```rust
pub fn get_inner(&self) -> Arc<tokio::sync::Mutex<Box<dyn Directory>>> {
    self.0.clone()
}
```

This parallels the existing `file::Handle::get_file()` and is needed by
`HostDirectory::insert()` to replace the inner object through the shared Arc.

## The Pending-Path Pattern

The central design challenge: when `WD::create_file_path_streaming_with_type`
runs, it calls `create_file_node(id)` on the persistence layer *before*
`Directory::insert(name, node)` is called. At `create_file_node` time, we
don't yet know the host path — only the FileID.

The solution is a two-phase lifecycle:

```
1. create_file_node(id)     → HostFile { host_path: None, id }
                               (wrapped in Arc<Mutex<Box<dyn File>>>)

2. insert("foo.txt", node)  → std::fs::File::create(parent/foo.txt)
                               lock the Arc<Mutex<…>>
                               *inner = Box::new(HostFile::new(path, id))
```

Because `Handle` wraps `Arc<Mutex<…>>`, the replacement inside `insert()` is
visible to all code holding a clone of that Handle — including the caller that
received the Node from `create_file_node` and is about to call `async_writer()`.

If anything tries I/O on a pending (path-less) file *before* `insert()` runs,
`require_path()` returns a clear, actionable error instead of silently touching
a placeholder path.

## What Is NOT Included (Later Phases)

- **Phase 2:** Steward trait extraction (so hostmount can be used from CLI)
- **Phase 3:** `-d <dir>` CLI flag (ShipContext integration)
- **Phase 4:** URL scheme unification (`host://` uses hostmount internally)
- **Phase 5:** Sitegen restructuring (hostmount as output target)
- **Phase 6:** `pond copy` unification (pond↔host through same abstractions)
- **Phase 7:** Read-only mount mode (optional safety constraint)

## Test Results

```
test result: ok. 165 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

All 24 new hostmount tests + 141 existing tinyfs tests pass. Full workspace
compiles clean with no warnings.
