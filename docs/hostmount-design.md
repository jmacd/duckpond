# Design: Hostmount — Host Filesystem as TinyFS

## Problem Statement

DuckPond commands interact with the host filesystem through ad-hoc mechanisms:

- `pond copy host:///path` uses `strip_prefix` string parsing, not the provider URL system
- `pond cat host+csv:///path` bypasses the pond entirely with a special early-return path
- `pond run` always requires a pond — running sitegen on host files means manually
  copying everything into a throwaway pond first (see `water/render.sh`: 15+ `pond copy`
  calls to shuttle files in, then sitegen writes back to the host)

Meanwhile, `pond cat` has evolved a clean `host+format` URL model where `host+oteljson:///path`
reads a host file through a format provider with no pond required. The `copy-url-design.md`
extends this to `pond copy`. But these are point solutions — each command re-implements
host access differently.

## Vision

Make the host filesystem a first-class tinyfs citizen. A **hostmount** is a tinyfs
`Directory` implementation that maps a host directory tree onto tinyfs, so that pond
commands can operate on host files using the same path resolution, glob matching, and
factory infrastructure they use for pond files.

### The Motivating Example

Today's `water/render.sh`:
```bash
export POND="./water/.pond"
pond init
pond mkdir /content
for f in ./water/content/*.md; do
    pond copy "host://${f}" "/content/$(basename "$f")"
done
pond mkdir /etc && pond mkdir /etc/site
for f in ./water/site/*.md; do
    pond copy "host://${f}" "/etc/site/$(basename "$f")"
done
pond mknod sitegen /etc/site.yaml --config-path ./water/site.yaml
pond run /etc/site.yaml build ./dist
```

With hostmount:
```bash
pond run -d ./water host+sitegen://site.yaml build ./dist
```

One command. No throwaway pond. No manual file copying.

## Architecture

### How It Works

1. The CLI parses command arguments and determines the required context:
   - If arguments reference only `host+...://` URLs → construct a hostmount tinyfs
   - If arguments reference pond paths → open the pond (current behavior)
   - If both → construct both, route by URL

2. A hostmount is a `Directory` trait implementation whose:
   - `entries()` calls `std::fs::read_dir` and returns `DirectoryEntry` items
   - `get(name)` returns `Node`s with host-backed `File` or `Directory` handles
   - The tinyfs root `/` maps to a configurable host directory

3. Factory schemes in URLs (`host+sitegen://site.yaml`) tell the CLI:
   - **Source**: host filesystem (`host`)
   - **Interpretation**: sitegen factory config (`sitegen` scheme)
   - **Path**: `site.yaml` relative to the hostmount root

4. `-d <dir>` overrides the hostmount root (default: process working directory)

### Layer Placement

```
┌─────────────────────────────────────────────────────┐
│  cmd        CLI determines context from URLs        │
├─────────────────────────────────────────────────────┤
│  provider   Factory dispatch, format providers      │
├─────────────────────────────────────────────────────┤
│  tinyfs     FS / WD / Directory / File traits       │
│             ┌───────────────┬──────────────────┐    │
│             │ OpLog + Delta │  Hostmount        │   │
│             │ (tlogfs)      │  (std::fs)        │   │
│             └───────────────┴──────────────────┘    │
└─────────────────────────────────────────────────────┘
```

The hostmount is a **peer** of tlogfs at the tinyfs layer — both implement the
abstractions that `FS`, `WD`, and the provider layer consume.

## Design Decisions

### D1. `PersistenceLayer` implementation

The hostmount implements the `PersistenceLayer` trait (like tlogfs's `OpLogPersistence`),
backed by `std::fs`. This gives full integration: `FS::new(hostmount_persistence)` just
works, yielding a real `FS` and `WD` that the provider layer can consume.

`PersistenceLayer` methods that are tlogfs-specific get stub implementations:
- `transaction_state()` → a no-op transaction state (no commit/rollback)
- `list_file_versions()` → always returns a single version
- `get_temporal_bounds()` → always returns `None`

This is the deeper integration path and will require care, but it means every command
that works with `FS` + `WD` works with the hostmount automatically — no special cases.

### D2. Read-write, with liberal errors

The hostmount is read-write. Writes through tinyfs map to `std::fs` operations:
- `create_file_node` → creates a file on the host
- `create_directory_node` → `std::fs::create_dir`
- Writing with a `table` or `series` entry type → writes Parquet bytes to a host file

Entry types are **accepted on write but not preserved** on the host filesystem.
Reading the same Parquet file back requires `host+series://` or `host+table://` in the
URL to set the entry type — the hostmount itself always presents files as
`FilePhysicalVersion` (data). This is intentionally asymmetric.

Operations that don't make sense (e.g., versioned append to a host file) return errors.

### D3. Default entry type is `data`; URL scheme overrides

All host files are presented as `FilePhysicalVersion` (data) by the hostmount.
No extension inference. The URL scheme layering handles type interpretation:
- `host+series:///readings.parquet` → entry type `series`, implies Parquet format
- `host+table:///snapshot.parquet` → entry type `table`, implies Parquet format
- `host+csv:///data.csv` → format provider `csv`, no entry type override
- `host:///anything` → raw data bytes

This preserves the principle that entry type is metadata, not filename-derived.

### D4. Steward becomes a trait; hostmount gets a lightweight steward

The steward layer is refactored into a trait so that:
- **tlogfs steward** continues with full transaction support, control table, audit log
- **Hostmount steward** is lightweight — no real transactions, no control table

The hostmount steward:
- Provides a `FactoryContext` with hostmount persistence + standalone DataFusion session
- `FileID` is computed deterministically using `FileID::from_content()`, same as
  dynamic directories (see D8)
- `PondMetadata` and `txn_seq` are absent/zero
- If a factory attempts a write transaction that doesn't commit, logs an error

Factories that only produce host output (sitegen) work naturally. Factories that
require pond writes (hydrovu, remote) will error when run against a hostmount — this
is correct behavior, not a limitation.

### D5. Hostmount and pond coexist

The execution context consists of two optional filesystems:
- **Host FS** (hostmount) — always available (every process has a working directory)
- **Pond FS** (tlogfs) — available when `POND` is set or `--pond` is provided

Commands determine which they need from their URL arguments:
- `host+...://` URLs → routed to the hostmount
- Bare paths (`/data/...`) → routed to the pond (requires POND)
- Both in one command → both are constructed, URL routing dispatches

This means `pond copy host+csv:///data/raw.csv /tables/` constructs both filesystems
and copies from hostmount → pond, all through tinyfs APIs.

### D6. `-d` is a global flag; chroot semantics

`-d <dir>` is a **global CLI flag** that sets the hostmount root directory:
```
pond -d ./water run host+sitegen://site.yaml build ./dist
```

Semantics:
- The hostmount root `/` maps to the resolved absolute path of `-d`
- Default (no `-d`): the process working directory
- **Chroot behavior**: all host paths resolve relative to this root. A URL path like
  `host:///full/path` resolves as `<root>/full/path`, not as an absolute host path.
  No path can escape the root (like `chroot()`)
- `-d` does **not** affect pond paths — tlogfs paths are always absolute from the
  pond root

### D7. Site config uses host-native paths

The `water/site.yaml` config is restructured to use paths matching the host directory
layout. The `/etc/site/` pond convention is replaced with `/site/` to match
`./water/site/` on the host:

| Old path | New path | Host maps to |
|---|---|---|
| `/etc/site/index.md` | `/site/index.md` | `./water/site/index.md` |
| `/etc/site/param.md` | `/site/param.md` | `./water/site/param.md` |
| `/content/*.md` | `/content/*.md` | `./water/content/*.md` (no change) |

The sitegen test scripts will be updated accordingly. This is safe because the
site work is new and nothing external depends on the `/etc/site/` convention.

### D8. FileID via `FileID::from_content()` — same as dynamic dirs

Host files get deterministic `FileID`s using the same mechanism as
`DynamicDirDirectory`: `FileID::from_content(parent_part_id, entry_type, &hash_bytes)`.

For the hostmount:
- The root directory gets a fixed synthetic `FileID` (e.g., `FileID::from_content(
  PartID::root(), DirectoryPhysical, b"hostmount-root")`)
- Each child's `FileID` is derived from `hash(name)` with the parent's `NodeID` as
  `PartID`, recursively — same pattern as dynamic dir children
- Deterministic: same path always produces the same `FileID`, enabling caching

### D9. Unified scheme namespace — one registry, no conflicts

Format providers (`csv`, `oteljson`, `excelhtml`) and factory names (`sitegen`,
`hydrovu`, `remote`) share a **single namespace**. The URL scheme is looked up in one
unified registry. Name collisions between formats and factories are compile-time errors.

The grammar becomes:
```
[host+]scheme[+compression][+entrytype]:///path
```

Where `scheme` is resolved from the unified registry. The command determines how to
use the result — `pond cat` feeds through format providers, `pond run` dispatches to
factory execution.

---

## Implementation Phases

### Phase 1: Hostmount `PersistenceLayer`

Create a new `hostmount` crate (or module in tinyfs) implementing `PersistenceLayer`
backed by `std::fs`:
- `load_node()` → `stat()` the path, return `Node` with host-backed file/dir handle
- `create_file_node()` / `create_directory_node()` → `std::fs::File::create` / `create_dir`
- `list_file_versions()` → single version from file mtime
- `transaction_state()` → no-op
- `FileID` generation via `from_content()`
- Read-write: file writes go to `std::fs`, errors for unsupported operations
- Unit tests: mount a temp directory, verify `FS::new()` + `WD` operations work

### Phase 2: Steward trait extraction

Refactor steward from a concrete `Ship` into a trait:
- Extract trait from the current `Ship` / `StewardTransactionGuard` interface
- tlogfs steward implements the full trait (unchanged behavior)
- Hostmount steward implements a lightweight version (no control table, no real txn)
- `FactoryContext` constructed from either steward implementation

### Phase 3: CLI context determination + `-d` flag

Refactor `ShipContext` / main CLI dispatch:
- Add `-d <dir>` global flag for hostmount root
- Commands inspect URL arguments to determine required context:
  - `host+...://` → construct hostmount FS
  - Bare paths → require POND, construct tlogfs
  - Both → construct both
- `chroot` semantics for `-d`: all host paths resolve under the root

### Phase 4: Unified scheme registry

Merge format provider and factory registries into a single namespace:
- Compile-time or startup validation for name conflicts
- `provider::Url` parses scheme and resolves via unified registry
- `pond run host+sitegen://site.yaml` dispatches: parse URL → look up `sitegen` in
  registry → found as factory → extract config from host file → execute

### Phase 5: `pond run` on hostmount

Wire `pond run` to work with hostmount-only context:
- Factory name from URL scheme (no oplog lookup)
- Config bytes from host file (no `mknod` required)
- `FactoryContext` from hostmount steward + standalone DataFusion session
- Sitegen patterns resolve via hostmount `WD::collect_matches()`

### Phase 6: Site config restructuring

- Rename `/etc/site/` paths to `/site/` in `water/site.yaml`
- Update `render.sh` and sitegen test scripts
- Verify `pond run -d ./water host+sitegen://site.yaml build ./dist` works end-to-end

### Phase 7: Deprecate ad-hoc host access

- Remove `strip_prefix("host://")` from `pond copy`
- Remove `cat_host_impl` special path from `pond cat`
- All host access flows through hostmount tinyfs
- `host+` prefix in URLs routes to the hostmount FS in the execution context
