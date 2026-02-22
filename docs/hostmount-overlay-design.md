# Design: Hostmount Factory Overlay

## Problem Statement

The hostmount persistence layer (`HostmountPersistence`) presents a host directory
as a read-write TinyFS, but it rejects dynamic nodes: `create_dynamic_node` returns
`"Dynamic nodes are not supported on hostmount"`. This means factory hierarchies
(dynamic-dir, temporal-reduce, etc.) cannot be used in hostmount-only workflows.

Today, the water site build (`render-data.sh`) works around this by creating a
temporary pond, ingesting data, running `mknod` for each factory, and then running
sitegen -- even though the source data (`casparwater-sample.json`) already exists
on the host filesystem. The throwaway pond exists solely to host the factory nodes.

The hostmount design doc envisions a single command:
```bash
pond run -d ./water host+sitegen:///site.yaml build ./dist
```

But when sitegen reads an exports pattern like `/reduced/*/*/*.series`, it walks the
TinyFS tree. The `/reduced` directory does not exist on the host -- it is a factory
node defined by `reduce.yaml`. Without a way to mount factory configs onto the
hostmount FS, the path resolution returns `None` and the exports are empty.

## Vision

A `--hostmount` flag mounts factory definitions onto paths in the host filesystem,
so that host files and computed factory nodes coexist in a single FS namespace.
The same factory config (e.g., `reduce.yaml`) works in both scenarios:

- **Pond mode:** `pond mknod dynamic-dir /reduced --config-path reduce.yaml`
- **Host mode:** `pond run --hostmount /reduced=host+dyndir:///reduce.yaml host+sitegen:///site.yaml build ./dist`

The factory config is identical. The difference is where it lives.

## Current Architecture

```
HostSteward::begin()
  -> HostmountPersistence::new(root)     [std::fs backed]
     -> FS::from_arc(persistence)
        -> root directory = HostDirectory  [entries() reads host dir]
  -> HostTransaction { fs, session, persistence }
     -> provider_context() = ProviderContext(session, persistence)
        -> When factory calls context.filesystem(),
           it gets the same HostmountPersistence-backed FS
```

`HostDirectory::get(name)` checks only `host_path.join(name).exists()` on the
real filesystem. There is no fallback, no overlay, no dynamic-node resolution.

`DynamicDirDirectory`, by contrast, has factory-backed `get()` that creates child
nodes on demand from config entries. It implements the same `Directory` trait.

## Proposed Mechanism: Overlay Directory

### Core Idea

Introduce an `OverlayDirectory` that wraps a `HostDirectory` and adds
factory-backed children at specific mount points. When `get(name)` is called:

1. Check overlay entries first (factory-defined children take precedence)
2. Fall back to the underlying `HostDirectory` for real files

This is a union-mount where overlay entries shadow host files of the same name.

### The `--hostmount` CLI Flag

Syntax:
```
--hostmount <mount_path>=host+<factory>:///<config_path>
```

Examples:
```bash
# Mount a dynamic-dir factory at /reduced
--hostmount /reduced=host+dyndir:///reduce.yaml

# Multiple mounts
--hostmount /reduced=host+dyndir:///reduce.yaml --hostmount /computed=host+dyndir:///compute.yaml
```

The flag is global (like `-d`). Multiple `--hostmount` flags are allowed.
Each specifies:
- `mount_path`: where in the TinyFS namespace the factory node appears
- `factory`: the factory type (from the URL scheme, e.g., `dyndir` for `dynamic-dir`)
- `config_path`: path to the factory config file, relative to the hostmount root

### Implementation Layers

#### Layer 1: OverlayDirectory

```
OverlayDirectory {
    host_dir: HostDirectory,           // real filesystem
    overlays: HashMap<String, Node>,   // factory-mounted children
}
```

`Directory` trait implementation:

| Method | Behavior |
|--------|----------|
| `get(name)` | Check `overlays` first; if not found, delegate to `host_dir.get(name)` |
| `entries()` | Merge overlay entries with host entries (overlays shadow host entries with same name) |
| `insert(name, node)` | Delegate to `host_dir.insert(name, node)` (writes go to real FS) |
| `remove(name)` | If name is in overlays, error ("cannot remove overlay mount"); else delegate to host_dir |

The overlay entries appear as normal directory children. From the perspective of
any code walking the tree (sitegen, temporal-reduce, path resolution), they are
indistinguishable from real directories.

#### Layer 2: OverlayPersistence (wraps HostmountPersistence)

The `OverlayDirectory` instances live inside an `OverlayPersistence` that wraps
`HostmountPersistence`. This wrapper:

- Intercepts `load_root()` to return an `OverlayDirectory` (if mounts exist at root level)
- Intercepts path resolution at mount point boundaries
- Delegates all other operations to `HostmountPersistence`
- Stores the mount table: `Vec<(PathBuf, String, Vec<u8>)>` - mount path, factory name, config bytes

When the `OverlayPersistence` is constructed:

1. Parse mount specs from CLI args
2. Read each config file from the host filesystem (before wrapping)
3. For each mount, compute a deterministic `FileID` (same as `DynamicDirDirectory`)
4. Create the `FactoryContext` for each mounted factory
5. Call `FactoryRegistry::create_directory()` to get the factory `Node`
6. Store the resulting `Node` in the mount table

#### Layer 3: Mount Table and Path Resolution

The mount table needs to handle arbitrary mount depths:

```
--hostmount /reduced=host+dyndir:///reduce.yaml     -> mount at depth 1
--hostmount /data/live=host+dyndir:///live.yaml      -> mount at depth 2
```

For depth-1 mounts (`/reduced`), the root `OverlayDirectory` gets the overlay entry
directly. For deeper mounts (`/data/live`), intermediate directories need overlay
entries too -- `data` gets an overlay that contains `live`.

**Approach:** Pre-process mount specs into a tree of overlay entries:

```
MountTree {
    children: HashMap<String, MountTreeNode>,
}

MountTreeNode {
    kind: Passthrough | Mounted(factory_node) | Intermediate(MountTree),
}
```

For `/data/live=dyndir://live.yaml`:
- Root gets `children: { "data" => Intermediate({ "live" => Mounted(node) }) }`
- The `data` directory becomes an `OverlayDirectory` wrapping `HostDirectory("./data")` with overlay `{ "live" => factory_node }`

#### Layer 4: FactoryContext for Mounted Factories

When a factory like `temporal-reduce` runs inside a mounted `dynamic-dir`, it
calls `self.context.context.filesystem()` to resolve its `in_pattern`. This
filesystem must be the **same overlay-aware FS** -- not just the raw hostmount.

This works naturally because:
1. `OverlayPersistence` wraps `HostmountPersistence` and implements `PersistenceLayer`
2. `ProviderContext` holds `Arc<dyn PersistenceLayer>` (the overlay persistence)
3. `FactoryContext` inherits the same `ProviderContext`
4. When temporal-reduce calls `context.filesystem()`, it gets the overlay FS
5. Therefore, `in_pattern: "oteljson:///ingest/foo.json"` resolves through the overlay,
   finding the real host file at `./ingest/foo.json`

The key insight: the `ProviderContext.persistence` is the overlay persistence,
so all factory nodes see both real host files AND other factory mounts.

### Design Decisions

#### D1: Overlays shadow host files

If `./reduced/` exists as a real directory on the host AND is mounted as a
factory, the factory wins. This is intentional -- the mount is explicit and
should take precedence. Attempting to write to a shadowed path is an error.
This matches `mount --bind` semantics.

#### D2: Factory alias for `dynamic-dir`

The URL scheme uses `dyndir` as a short name for `dynamic-dir`. Both are
registered in the `SchemeRegistry`. The `dynamic-dir` name is verbose and
hyphens in URL schemes are unusual.

Alternatively, we could use the full name `dynamic-dir` in the URL:
```
--hostmount /reduced=host+dynamic-dir:///reduce.yaml
```
This is a naming decision, not an architecture decision.

#### D3: Config paths are relative to hostmount root

The config path in `host+dyndir:///reduce.yaml` is resolved relative to the
hostmount root (set by `-d`), not the process working directory. This is
consistent with how all host URLs work -- `host+sitegen:///site.yaml` reads
`<root>/site.yaml`.

#### D4: Mount points must be directories

A mount point always creates a directory node (factory directories implement
`Directory`). You cannot mount a factory at a path that should be a file.
This is enforced during mount table construction.

#### D5: Factored config unchanged

The `reduce.yaml` and `site.yaml` configs remain unchanged between pond and
host modes. The `in_pattern` in `reduce.yaml` uses pond-absolute paths like
`oteljson:///ingest/casparwater-sample.json`. On the hostmount, this resolves
as `<root>/ingest/casparwater-sample.json`.

This means the directory layout on the host should mirror the expected pond
layout, OR the configs should use paths that make sense in both contexts.
For the water example, ingest data lives at `./ingest/casparwater-sample.json`
relative to the water directory root.

#### D6: No transactions on overlay mounts

Factory nodes created by overlay mounts are ephemeral -- they exist only for
the duration of the command. There is no transaction to commit. This is
consistent with how dynamic-dir works: dynamic nodes are computed on read,
not stored. The overlay mount is just a way to inject dynamic-dir (and its
children) into the hostmount namespace without `mknod`.

### Full Example: Water Site with Overlay

**Directory layout:**
```
water/
  casparwater-sample.json    # raw oteljson data
  reduce.yaml                # temporal-reduce config (same as pond version)
  site.yaml                  # sitegen config
  content/                   # markdown pages
    water.md
    monitoring.md
    well-depth-history.md
    ...
  site/                      # site templates
    index.md
    sidebar.md
    data.md
```

**reduce.yaml** (note: in_pattern points to host path, not pond /ingest/):
```yaml
entries:
  - name: "well-depth"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson:///casparwater-sample.json"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns: ["well_depth_value"]
        - type: "min"
          columns: ["well_depth_value"]
        - type: "max"
          columns: ["well_depth_value"]
  # ... other metrics ...
```

**Command:**
```bash
pond run \
  -d ./water \
  --hostmount /reduced=host+dyndir:///reduce.yaml \
  host+sitegen:///site.yaml \
  build ./dist
```

**What happens:**
1. CLI parses `-d ./water` -> hostmount root = `./water`
2. CLI parses `--hostmount /reduced=host+dyndir:///reduce.yaml`:
   a. Reads `./water/reduce.yaml` from host
   b. Creates `DynamicDirDirectory` with temporal-reduce children
   c. Registers overlay at `/reduced` in the mount table
3. CLI parses `host+sitegen:///site.yaml` -> factory = sitegen, config = `site.yaml`
4. `OverlayPersistence` wraps `HostmountPersistence(./water)` with mount table
5. `HostSteward::begin()` -> `HostTransaction` with overlay FS
6. Sitegen reads `site.yaml`, finds `exports: pattern: "/reduced/*/*/*.series"`
7. Sitegen walks `/reduced` -> hits `OverlayDirectory` -> dispatches to `DynamicDirDirectory`
8. Dynamic-dir creates temporal-reduce children -> temporal-reduce resolves
   `oteljson:///casparwater-sample.json` -> reads the host file through overlay FS
9. Sitegen exports the aggregated parquet data and renders HTML
10. Output lands in `./dist`

## Implementation Phases

### Phase 1: OverlayDirectory

Create `crates/tinyfs/src/hostmount/overlay.rs`:

- `OverlayDirectory` struct wrapping `HostDirectory` + overlay entries
- `Directory` trait implementation with overlay-first lookup
- `MountTree` for multi-depth mount resolution
- Unit tests with in-memory overlays

### Phase 2: OverlayPersistence

Extend `HostmountPersistence` or create `OverlayPersistence` wrapper:

- Accept mount specs at construction time
- Read config files from host, create factory nodes
- Wire overlay entries into root (and intermediate) directories
- Implement `PersistenceLayer` by delegating to inner `HostmountPersistence`

### Phase 3: CLI `--hostmount` flag

In `crates/cmd/src/main.rs`:

- Add `--hostmount` repeatable global argument
- Parse `mount_path=host+factory:///config_path` syntax
- Pass mount specs through `ShipContext` to `HostSteward`
- `HostSteward::begin()` creates `OverlayPersistence` when mounts are present

### Phase 4: End-to-end validation

- Update `water/reduce.yaml` to use host-relative paths (`oteljson:///casparwater-sample.json`)
- Update `water/render.sh` (or create `water/render-data.sh`) to use
  `--hostmount /reduced=host+dyndir:///reduce.yaml`
- Verify the full pipeline works without a pond
- Add test to `testsuite/tests/`

### Phase 5: Factory alias registration

- Register `dyndir` as an alias for `dynamic-dir` in `SchemeRegistry`
- Or decide on the canonical URL scheme name

## Open Questions

1. **Should `--hostmount` work with pond commands too?** E.g., could you
   `--hostmount /external=host+dyndir:///ext.yaml` while also having a real
   pond open? This would be a true union of pond + host + factory nodes.
   Deferred -- start with host-only mode.

2. **What about `site.yaml` paths?** The `exports` pattern
   `/reduced/*/*/*.series` and `content` pattern `/content/*.md` use
   absolute tinyfs paths. These work because `-d ./water` maps `/` to
   `./water/`, so `/content/` -> `./water/content/` and `/reduced/` ->
   the overlay mount. No changes needed.

3. **Can temporal-reduce write its output?** No -- temporal-reduce in a
   dynamic-dir is computed on read (lazy). It creates `TemporalReduceSqlFile`
   nodes that generate data via SQL on demand. No writes required. This is
   why dynamic-dir works naturally -- no persistence needed for outputs.

4. **What about the logfile-ingest step?** The current `render-data.sh`
   ingests `casparwater-sample.json` via logfile-ingest into
   `/ingest/casparwater-sample.json`. With hostmount overlay, the file
   already exists at `./water/casparwater-sample.json` on the host. The
   `in_pattern` in `reduce.yaml` just points to it directly. No ingest needed.
