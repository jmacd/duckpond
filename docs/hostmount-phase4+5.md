# Hostmount Phase 4+5: Unified Scheme Registry + `pond run` on Hostmount

**Status:** Complete  
**Design:** [hostmount-design.md](hostmount-design.md) (Phase 4+5)

## Overview

Phases 4 and 5 of the hostmount design were merged into a single implementation phase.
The combined goal: introduce a unified scheme registry that classifies URL schemes as
builtin, format, or factory — then wire `pond run` to detect `host+factory://` URLs and
execute factories directly on the host filesystem, with no pond required.

The motivating command:
```bash
pond run -d ./water host+sitegen:///site.yaml build ./dist
```

## What Was Built

### SchemeRegistry (`crates/provider/src/registry.rs`)

A zero-sized struct with two static methods that unifies three previously independent
namespaces into a single classification lookup.

```rust
pub struct SchemeRegistry;

impl SchemeRegistry {
    pub fn classify(scheme: &str) -> Option<SchemeKind> { ... }
    pub fn find_conflicts() -> Vec<String> { ... }
}
```

**Classification priority** (`classify`):

| Priority | Source | Examples | Returns |
|----------|--------|----------|---------|
| 1 | `BUILTIN_SCHEMES` (hardcoded) | `file`, `series`, `table`, `data` | `SchemeKind::Builtin` |
| 2 | `FormatRegistry::get_provider()` | `csv`, `oteljson`, `excelhtml` | `SchemeKind::Format` |
| 3 | `FactoryRegistry::get_factory()` | `sitegen`, `hydrovu`, `remote` | `SchemeKind::Factory` |
| — | Not found | anything else | `None` |

**Conflict detection** (`find_conflicts`): Iterates all entries in the `DYNAMIC_FACTORIES`
`linkme` distributed slice. For each factory name, checks whether it collides with a
builtin scheme or format provider. Returns a `Vec<String>` of conflict descriptions (empty
means no conflicts). This is checked at test time — name collisions are test failures,
not runtime errors.

### SchemeKind Enum

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeKind {
    Builtin,  // file, series, table, data
    Format,   // csv, oteljson, excelhtml
    Factory,  // sitegen, hydrovu, remote, dynamic-dir, sql-derived-table, ...
}
```

Re-exported from `provider` crate root.

### `classify_target()` (`crates/cmd/src/common.rs`)

Determines whether a CLI argument refers to the pond or the host filesystem.

```rust
pub fn classify_target(arg: &str) -> TargetContext

pub enum TargetContext {
    Pond(String),   // pond-local path or pattern
    Host(String),   // host filesystem path (extracted from URL)
}
```

**Logic:**
1. Contains `"://"` → parse as `provider::Url`:
   - `url.is_host()` → `Host(url.path())`
   - Otherwise → `Pond(arg)` (e.g., `csv:///pond/data.csv`)
2. Starts with `"host+"` (no `://`) → `Host(rest)`
3. Everything else → `Pond(arg)`

Used by `run_command`, `cat_command`, and `list_command` as the dispatch point between
pond and host code paths.

### `run_host_command()` (`crates/cmd/src/commands/run.rs`)

The private async function that executes a factory on the host filesystem.

**Flow:**

1. **Parse URL**: `provider::Url::parse(config_path)` extracts the scheme (factory name)
   and path. E.g., `host+sitegen:///site.yaml` yields scheme `"sitegen"`, path `"/site.yaml"`.

2. **Validate via SchemeRegistry**: The extracted scheme must classify as `SchemeKind::Factory`.
   Format providers and builtins produce clear error messages:
   - `"'csv' is a format provider, not a factory. Use pond cat for format providers."`
   - `"'file' is a builtin scheme, not a factory."`

3. **Open host steward**: `ship_context.open_host()` creates a `Steward::Host(HostSteward)`
   using the `-d <dir>` flag (or `/` as default root).

4. **Begin transaction**: No-op on the hostmount — host writes have immediate consistency.

5. **Read config bytes**: Opens the host file via tinyfs (`async_reader_path`), reads
   the factory config YAML.

6. **Resolve FileID**: `root.resolve_path(host_path)` for the config node. Deterministic —
   same path always yields the same `FileID` via `FileID::from_content()`.

7. **Build FactoryContext + execute**: `FactoryRegistry::execute()` with
   `ExecutionContext::pond_readwriter(args)`.

8. **Commit**: No-op on host.

### CLI Dispatch (`run_command`)

```rust
pub async fn run_command(
    ship_context: &ShipContext,
    config_path: &str,
    extra_args: Vec<String>,
) -> Result<()> {
    match classify_target(config_path) {
        TargetContext::Host(_) => run_host_command(ship_context, config_path, extra_args).await,
        TargetContext::Pond(_) => run_pond_command(ship_context, config_path, extra_args).await,
    }
}
```

The full original URL (not just the extracted path) is passed to `run_host_command` so
it can extract the factory scheme.

### HostSteward (`crates/steward/src/host.rs`)

Lightweight steward for host filesystem operations. No Delta Lake, no control table,
no audit log.

```rust
pub struct HostSteward {
    root_path: PathBuf,
}

pub struct HostTransaction {
    fs: FS,
    session: Arc<SessionContext>,
    persistence: Arc<dyn PersistenceLayer>,
    meta: PondUserMetadata,
}
```

Key behaviors:
- `begin()` creates an `FS` from `HostmountPersistence` plus a standalone DataFusion
  `SessionContext`
- `get_factory_for_node()` always returns `Ok(None)` — the factory name comes from the
  URL scheme, not from the filesystem
- Implements `Deref<Target=FS>` for filesystem access
- No real commit/rollback needed

Integrated via the `Steward` dispatch enum:
```rust
pub enum Steward {
    Pond(Box<Ship>),
    Host(HostSteward),
}
```

### Force-Linking External Factories (`crates/cmd/src/lib.rs`)

Factory crates register themselves via `linkme::distributed_slice`. Without explicit
references, the linker would drop them. The cmd crate force-links them:

```rust
use hydrovu as _;
use remote as _;
use sitegen as _;
```

This ensures `SchemeRegistry::classify("sitegen")` returns `SchemeKind::Factory` in the
final binary, and `find_conflicts()` checks all factory names.

## Tests

### Unit/Integration Tests (16 tests)

**SchemeRegistry classification** (5 tests in `crates/provider/src/lib.rs`):

| Test | Verifies |
|------|----------|
| `test_scheme_registry_no_conflicts` | No factory name collides with builtin or format |
| `test_scheme_registry_classify_builtins` | `file`, `series`, `table`, `data` → `Builtin` |
| `test_scheme_registry_classify_format_providers` | `csv`, `oteljson`, `excelhtml` → `Format` |
| `test_scheme_registry_classify_factories` | `sql-derived-table`, `dynamic-dir` → `Factory` |
| `test_scheme_registry_classify_unknown` | `banana`, `nosuch` → `None` |

**Factory URL parsing** (4 tests in `crates/cmd/src/commands/run.rs`):

| Test | Verifies |
|------|----------|
| `test_no_scheme_conflicts` | Same conflict check with external factories linked |
| `test_executable_factories_classified` | `sitegen`, `hydrovu`, `remote` → `Factory` |
| `test_factory_scheme_url_parsing` | `host+sitegen:///site.yaml` parses correctly |
| `test_format_not_accepted_as_factory` | `csv` is `Format`, not `Factory` |

**Target classification** (7 tests in `crates/cmd/src/common.rs`):

| Test | Verifies |
|------|----------|
| `test_classify_bare_glob_is_pond` | `**/*` → `Pond` |
| `test_classify_absolute_path_is_pond` | `/data/file.csv` → `Pond` |
| `test_classify_host_file_url` | `host+file:///tmp/data/**/*` → `Host` |
| `test_classify_host_csv_url` | `host+csv:///tmp/data.csv` → `Host` |
| `test_classify_host_empty_path` | `host+file:///` → `Host("/")` |
| `test_classify_host_no_scheme_separator` | `host+somepath` → `Host("somepath")` |
| `test_classify_pond_url_no_host_prefix` | `csv:///pond/data.csv` → `Pond` |

### End-to-End Test

**`testsuite/tests/303-hostmount-sitegen.sh`** (13/13 checks):

Exercises the full `pond run -d <dir> host+sitegen:///site.yaml build <outdir>` workflow:
1. Creates a host directory tree with Markdown content, templates, sidebar, and `site.yaml`
2. Runs sitegen on the host filesystem — **no `pond init`, no POND env**
3. Verifies 5 output files exist (`index.html`, 3 content pages, `style.css`)
4. Validates HTML structure: layout classes, article wrappers, heading anchors,
   sidebar navigation, active page highlighting
5. Confirms hidden pages render but are excluded from navigation
6. Confirms no CDN scripts in page layout

## Design Decisions Exercised

| Decision | How it manifests |
|----------|-----------------|
| D4 (Steward trait) | `Steward` enum dispatches between `Ship` (pond) and `HostSteward` (host) |
| D5 (Coexistence) | `classify_target()` routes to pond or host code path based on URL |
| D8 (FileID via `from_content`) | Host files get deterministic `FileID`s — same path = same ID |
| D9 (Unified namespace) | `SchemeRegistry` checks all three namespaces; `find_conflicts()` enforces no collisions |

## What's Next

- **Phase 6**: Site config restructuring — rename `/etc/site/` paths to `/site/` in
  `water/site.yaml`, update `render.sh` to use `pond run -d ./water host+sitegen://...`
- **Phase 7**: Deprecate remaining ad-hoc host access — remove `strip_prefix("host://")`
  from `pond copy`, route all host access through hostmount tinyfs
