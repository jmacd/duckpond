# DuckPond - Copilot Instructions

## 🧠 SYSTEM MENTAL MODEL (Read This First)

DuckPond is a **transactional, query-native filesystem** for time-series data. It stores
files and directories in a Delta Lake–backed filesystem where every object has a *type*
that determines how it's stored, queried, and exported. The `pond` CLI is the primary
interface; behind it is a layered Rust architecture.

### Architecture Layers (bottom → top)

```
┌─────────────────────────────────────────────────────────┐
│  cmd          CLI commands (pond init/list/cat/copy/…)  │
│  hydrovu      HydroVu API collector (factory)           │
│  remote       S3 backup & replication (factory)         │
│  sitegen      Static site generator (factory)           │
├─────────────────────────────────────────────────────────┤
│  provider     URL-based data access, factory registry,  │
│               QueryableFile trait, table creation,      │
│               transform pipelines                       │
├─────────────────────────────────────────────────────────┤
│  steward      Transaction orchestration (Ship),         │
│               control table (audit log),                │
│               StewardTransactionGuard                   │
├─────────────────────────────────────────────────────────┤
│  tlogfs       Delta Lake persistence implementation,    │
│               OpLog, TransactionGuard, DataFusion       │
│               session context, query integration        │
├─────────────────────────────────────────────────────────┤
│  tinyfs       Pure filesystem abstractions: FS, WD,     │
│               Node, File, Directory, EntryType,         │
│               PersistenceLayer trait, ProviderContext    │
└─────────────────────────────────────────────────────────┘
```

**Rule of thumb:** `tinyfs` defines *what* a filesystem is. `tlogfs` implements *how*
it's stored (Delta Lake + OpLog). `steward` wraps transactions with lifecycle tracking.
`provider` makes files queryable via DataFusion. `cmd` wires it all into the CLI.

### On-Disk Pond Structure

```
{POND}/
├── data/                          # The actual filesystem (tlogfs)
│   ├── _delta_log/                # Delta Lake transaction log
│   ├── _large_files/              # Content-addressed store for files >64KB
│   └── part_id=<uuid>/           # Partitions (one per directory)
│       └── *.parquet              # OpLog entries (file content + metadata)
└── control/                       # Audit/control table (steward)
    ├── _delta_log/
    ├── record_category=metadata/
    └── record_category=transaction/
```

Every directory in the pond has its own **partition** (UUID). File data and metadata
are stored as Parquet rows within those partitions.

### Entry Types (What Makes DuckPond Different)

**Entry type is metadata stored in tinyfs, NOT derived from the filename.**
A file named `foo.series` is NOT necessarily a series — the type was set when
the entry was created. This type determines how every command behaves.

| Type | Created By | Storage | Description |
|------|-----------|---------|-------------|
| `data` | `pond copy` (default) | Raw bytes | Opaque file, any format |
| `data:series` | (internal) | Raw bytes, multi-version | Versioned raw byte file |
| `table` | `pond copy --format=table` | Parquet (single) | Queryable via SQL |
| `table:series` | `--format=series`, HydroVu | Parquet, multi-version | Time-series, queryable via SQL |
| `table:dynamic` | Factory nodes | Parquet, computed on read | Factory-generated series |
| `directory` | `pond mkdir`, factories | N/A | Physical directory (own partition) |
| `dynamic-directory` | Factory nodes | N/A | Factory-generated directory |
| `symlink` | (internal) | N/A | Symbolic link |

### How Entry Type Drives Command Behavior

**`pond cat`** — dispatches on entry type, NOT filename:
- **data types** → streams raw bytes to stdout
- **table/series types** → routes through DataFusion; default outputs Parquet bytes;
  use `--sql "SELECT ..."` or `--display=table` for readable output

**`pond copy` INTO pond** — `--format` flag sets the entry type:
- `--format=data` (default): raw bytes
- `--format=table`: validates Parquet (PAR1 magic)
- `--format=series`: validates Parquet + extracts temporal bounds

**`pond copy` OUT OF pond** (to `host://`) — determined by source entry type:
- table/series → exported as Parquet (via DataFusion)
- data → exported as raw bytes (bit-for-bit copy)
- `--format` flag is **ignored** on export

### URL Schemes (Provider Layer)

Factory configs and `pond cat` use URL schemes to control how files are interpreted:

| Scheme | Purpose | Used In |
|--------|---------|---------|
| `series:///path` | Multi-version Parquet series | Factory configs |
| `table:///path` | Single Parquet table | Factory configs |
| `csv:///path` | Parse raw data file as CSV | `pond cat`, factory configs |
| `csv+gzip:///path` | Gzipped CSV | Factory configs |
| `excelhtml:///path` | HydroVu Excel HTML exports | Factory configs |
| `file:///path` | Raw bytes or auto-detect | `pond cat` |
| `host:///path` | Host filesystem (not in pond) | `pond copy` |

### Factories (Physical vs Dynamic)

Factories create computed filesystem objects. Two kinds:

- **Dynamic nodes** (read-time computation): `sql-derived-table`, `sql-derived-series`,
  `temporal-reduce`, `dynamic-dir`, `timeseries-join`, `timeseries-pivot`,
  `column-rename`, `template`, `sitegen`. Created with `pond mknod`.
  Their content is computed on every read — no stored data.

- **Executable factories** (run-time side effects): `hydrovu`, `remote`.
  Created with `pond mknod`, executed with `pond run <path> <command>`.
  They write real data into the pond (`table:series` entries, backup bundles, etc.)

### The Transaction Model

Every `pond` CLI invocation is ONE transaction. Within that transaction there is
exactly ONE `TransactionGuard` (panics on duplicate). This guard gives access to:
- The filesystem (`root()`)
- The DataFusion session (`session_context()`)
- The state (`state()`)

**Never** create a second `OpLogPersistence::open()` or `persistence.begin()` inside
a running transaction. Pass `&mut tx` to helpers instead.

### SQL Conventions

- Table name is always `source` (e.g., `SELECT * FROM source WHERE ...`)
- DataFusion SQL dialect (Apache Arrow types)
- `pond cat --sql "..."` for ad-hoc queries
- `pond control --sql "..."` for control table queries

### TinyFS Data API (Parquet/Arrow Layer)

TinyFS is **not just a filesystem** — it has a rich Parquet/Arrow data layer built in.
Before writing new data-processing code, **check what already exists** in tinyfs.

#### Key Principles

1. **Arrow RecordBatch is the universal data type.** All reads and writes go through
   RecordBatch. Never collect batches into a `Vec<RecordBatch>` when you can stream
   them — `ArrowWriter::write(&batch)` accepts one batch at a time.

2. **Parquet metadata is the source of temporal bounds.** After writing batches
   through `ArrowWriter` and closing it, the parquet file contains column statistics
   (min/max per row group). Use `extract_temporal_bounds_from_parquet_metadata()` to
   read them — do NOT manually track min/max across batches.

3. **Compose existing pieces, don't reinvent.** The common write pattern is:
   - Stream batches → `ArrowWriter` → parquet bytes in `Vec<u8>`
   - Extract temporal bounds from the parquet metadata
   - Write bytes to tinyfs via `create_file_path_streaming_with_type`
   - Set temporal metadata via `writer.set_temporal_metadata(min, max, ts_col)`
   - `writer.shutdown()`
   
   The existing `create_series_from_batch` already does steps 2–5 for a single batch.
   For streaming, reuse the same tail end.

#### WD (Working Directory) — File Operations

| Method | Creates? | Appends? | Purpose |
|--------|----------|----------|---------|
| `create_file_path_streaming_with_type(path, type)` | ✅ New only | ❌ | Create file, get writer |
| `async_writer_path_with_type(path, type)` | ✅ If missing | ✅ If exists | Create-or-append |
| `read_file_path_to_vec(path)` | | | Read raw bytes (loads all into memory) |
| `async_reader_path(path)` | | | Streaming read |
| `write_file_path_from_slice(path, &[u8])` | ✅ If missing | ✅ If exists | Write raw bytes |
| `copy_to_parquet(pond_path, host_path, ctx)` | | | Export to host filesystem |

#### WD — Directory Operations

| Method | Behavior on exists |
|--------|-------------------|
| `create_dir_path(path)` | **Errors** `AlreadyExists` |
| `create_dir_all(path)` | **Succeeds** (mkdir -p semantics) |
| `open_dir_path(path)` | Returns `WD` for existing dir |
| `rename_entry(old, new)` | Rename in-place (preserves version history) |
| `remove(name)` | Unlinks name from directory (via `DirRef`) |

**Rule:** Use `create_dir_all` for idempotent operations (factories, `--overwrite`).
Use `create_dir_path` only when duplicate creation is a bug.

#### ParquetExt Trait (on WD)

| Method | Entry Type | Versioning |
|--------|-----------|------------|
| `create_table_from_batch(path, batch, type)` | Any | Single version |
| `create_series_from_batch(path, batch, ts_col)` | `TablePhysicalSeries` | First version, with temporal bounds |
| `write_series_from_batch(path, batch, ts_col)` | `TablePhysicalSeries` | Create **or** append version |
| `read_table_as_batch(path)` | Any | Reads latest version |

**Critical:** `write_series_from_batch` creates a new **version** — it does NOT merge
with existing data. After N appends, a series has N versions. Reading via table provider
merges all versions, but each version is stored separately. This is why **compaction**
matters: archive a multi-version series by reading through DataFusion (which merges)
and writing a single fresh version.

#### Temporal Bounds

Two extraction methods exist — use the right one:

| Function | Input | When to use |
|----------|-------|-------------|
| `extract_temporal_bounds_from_batch(batch, ts_col)` | `&RecordBatch` | You have a single batch in memory |
| `extract_temporal_bounds_from_parquet_metadata(meta, schema, ts_col)` | Parquet metadata | You have complete parquet bytes (post-`ArrowWriter::close()`) |

Both normalize to **microseconds**. The parquet metadata approach is preferred when
you've already written through `ArrowWriter` because the statistics are free.

#### QueryableFile / Table Provider

Files with entry types `table`, `table:series`, `table:dynamic` implement the
`QueryableFile` trait. This gives access to a DataFusion `TableProvider`:

```
file_guard.as_queryable()?.as_table_provider(node_id, &provider_ctx) → Arc<dyn TableProvider>
```

For `table:series`, the table provider automatically **merges all versions** into a
single logical table. This is the correct way to read the "current state" of a
multi-version series — never read individual versions manually unless you specifically
need version-level access.

---

## 📚 DOC MAP (Task-Oriented)

| Working on... | Read this |
|--------------|-----------|
| CLI commands, syntax, flags | `docs/cli-reference.md` — **always check first** |
| Factory YAML configs | `docs/cli-reference.md` § Factory Types |
| Transaction/persistence code | `docs/duckpond-system-patterns.md` §1-3 |
| Factories/providers/TableProvider | `docs/duckpond-system-patterns.md` §3+ |
| Large file storage (>64KB) | `docs/large-file-storage-implementation.md` |
| Architecture overview, crate map | `docs/duckpond-overview.md` |
| Design philosophy (no fallbacks) | `docs/fallback-antipattern-philosophy.md` |
| Sitegen (static site generator) | `docs/sitegen-design.md` |

---

## ⚡ THE CORE LOOP (Every Session)

**DuckPond development is test-driven discovery.** Every work session follows this loop:

```
1. UNDERSTAND what we're testing/building
2. RUN test in containerized sandbox (`testsuite/`)
3. CLASSIFY result: ✅ Success | ❌ Bug | ❓ Bad Docs | 🤷 Bad Interface
4. DISCUSS with user → get decision
5. FIX the right layer (code, docs, or interface)
6. SAVE successful test to `testsuite/tests/NNN-description.sh`
7. UPDATE `docs/cli-reference.md` if behavior was clarified
```

**Start every session by asking**: "What are we testing?" or "Show me the backlog" (`testsuite/BACKLOG.md`)

---

## 🧪 TESTSUITE FIRST

The testsuite is your primary tool. **Use it before writing production code.**

```bash
cd testsuite

# Run a test by number (finds NNN-*.sh in tests/)
./run-test.sh 032

# Run interactively (explore in container)
./run-test.sh --interactive

# Run with S3/MinIO for replication tests
docker-compose up -d minio
docker-compose run --rm duckpond ./tests/500-*.sh
```

### Test Script Template
```bash
#!/bin/bash
# EXPERIMENT: [Brief title]
# EXPECTED: [What should happen]
set -e  # Stop on first error

pond init
# ... test steps ...

echo "=== VERIFICATION ==="
pond list /path/to/check  # ALWAYS verify results
```

### After Every Test
1. **Success?** → Save to `testsuite/tests/NNN-name.sh`, update docs if needed
2. **Bug?** → Report to user with exact error, propose proper fix (NO BAND-AIDS)
3. **Docs wrong?** → Fix `docs/cli-reference.md` immediately
4. **Interface confusing?** → Discuss with user before changing

---

## HARD RULES (Violations = Wasted Time)

### Source Code: ASCII Only

**No emoji or non-ASCII characters in `.rs` files.** Use ASCII bracket tags for
log prefixes (e.g., `[OK]`, `[ERR]`, `[WARN]`, `[SEARCH]`) and ASCII art for
box drawing (`+`, `-`, `|`, `=`). This rule exists because non-ASCII bytes break
text-based editing tools and cause silent file corruption.

### Rust Code Anti-Patterns

| NEVER | INSTEAD | WHY |
|----------|-----------|-----|
| `persistence.begin()` twice | Pass `&mut tx` to helpers | Single transaction rule - **PANICS** |
| `OpLogPersistence::open(path)` in helpers | Use existing `tx.session_context()` | Creates duplicate transaction |
| `.unwrap_or(default)` | `.map_err(\|e\| Error::from(e))?` | Silent fallbacks mask bugs |
| `_v2` / `_with_options` suffixes | Options struct + single function | Duplicate code = tech debt |
| `Err("not implemented")` | `unimplemented!("reason")` | Runtime errors vs dev markers |

### Debugging Anti-Patterns

| ❌ NEVER | ✅ INSTEAD |
|----------|-----------|
| `cargo test \| grep ERROR` | `cargo test 2>&1 \| tee OUT` then `grep_search` on OUT |
| `cargo run \| tail -100` | Full output to file, then search |
| Truncated terminal output | Always capture complete logs |

### Development Anti-Patterns

| ❌ NEVER | ✅ INSTEAD |
|----------|-----------|
| "Simple fix" / "Quick patch" | Understand root cause, design proper solution |
| Guess at CLI syntax | Check `docs/cli-reference.md` first |
| Skip verification | Always `pond list` / `pond cat` to verify |
| Test multiple things at once | One concept per test |

### Common Agent Mistakes

| ❌ Wrong assumption | ✅ Reality |
|---------------------|-----------|
| Filename `.series` means it's a series | Entry type is metadata, not filename-derived |
| `pond cat` on a table prints readable text | Default output is Parquet bytes; use `--sql` or `--display=table` |
| `pond copy --format=table` works on CSV | Only Parquet input; CSV stays as `--format=data` |
| `--format` controls export format | Export format is determined by source entry type |
| `pond cat` dumps binary "because it's Parquet" | Behavior is driven by entry type, not file contents |
| Collect batches into `Vec<RecordBatch>` then concat | Stream through `ArrowWriter` one batch at a time |
| Track temporal min/max manually per batch | Parquet metadata has column statistics; use `extract_temporal_bounds_from_parquet_metadata` |
| `create_dir_path` is safe for idempotent init | It errors on `AlreadyExists`; use `create_dir_all` for factories/`--overwrite` |
| Need new tinyfs primitive for data transform | Compose existing pieces: table provider → stream → ArrowWriter → tinyfs write |
| Rename compacts a multi-version series | Rename only changes the name; versions are preserved. Read via DataFusion + write fresh for compaction |

---

## 🏷️ CLASSIFICATION GUIDE

When a test completes, classify and act:

| Result | Meaning | Action |
|--------|---------|--------|
| ✅ **Success** | Works as documented | Save test, verify docs are accurate |
| ❌ **BUG** | Code doesn't work as intended | Report to user, propose proper fix |
| ❓ **DOCS** | Documentation is wrong/missing | Fix `docs/cli-reference.md` immediately |
| 🤷 **UX** | Interface is confusing but works | Discuss with user before changing |
| 💀 **DESIGN** | Fundamental approach is wrong | Full discussion needed |

---

## 🔧 KEY CONSTANTS

- **LARGE_FILE_THRESHOLD = 64KB**: Files >64KB stored externally in `_large_files/`
- **Transaction Guard**: Only ONE active transaction per operation (enforced by panic)
- **Table name in SQL**: Always `source` (e.g., `SELECT * FROM source WHERE ...`)
