# Pond CLI Command Structure

> **Status:** Implemented. The restructuring described in this document
> has been applied. `pond log`, `pond sync`, and `pond config` are the
> primary interfaces. `pond control` remains as a hidden backward-compat alias.

## Current Command Inventory

Every `pond` CLI invocation is one transaction. The command set below
reflects the current state after the restructuring described in this document.

### Commands

| Command | Purpose | Transaction | Concern |
|---------|---------|-------------|---------|
| `pond init` | Create a new pond | Write | Lifecycle |
| `pond init --from-backup` | Restore from backup file | Write | Lifecycle |
| `pond init --config` | Restore from base64 config | Write | Lifecycle |
| `pond recover` | Repair crash damage | Write | Lifecycle |
| `pond show` | Pond summary (storage stats, partitions) | Read | Status |
| `pond list` | List files (glob patterns) | Read | Browsing |
| `pond describe` | Show file schemas/types | Read | Browsing |
| `pond cat` | Read/query file contents | Read | Data access |
| `pond copy` | Copy files in/out | Write | Data movement |
| `pond mkdir` | Create directories | Write | Structure |
| `pond mknod` | Create factory nodes | Write | Structure |
| `pond list-factories` | List available factory types | None | Discovery |
| `pond run` | Execute factory commands | Write | Execution |
| `pond detect-overlaps` | Find temporal overlaps | Read | Analysis |
| `pond set-temporal-bounds` | Override temporal metadata | Write | Maintenance |
| `pond export` | Export to partitioned Parquet | Read | Data movement |
| `pond log` | Transaction history and audit trail | Read | History |
| `pond sync` | Sync with remote storage | Write | Execution |
| `pond config` | Show/set pond configuration | Read/Write | Configuration |

That is 19 top-level commands. `pond control` is hidden but still functional.

### `pond run` Subcommands (Remote Factory)

The remote factory accepts these arguments via `pond run`:

| Subcommand | Purpose | Mode |
|-----------|---------|------|
| `push` | Push to backup | ControlWriter |
| `pull` | Pull from backup (replica) | ControlWriter |
| `replicate` | Generate replica config | PondReadWriter |
| `verify` | Verify backup integrity | PondReadWriter |
| `show` | Show files + verification script | PondReadWriter |

`list-files` and `show` overlap significantly. `show` is a superset
-- it lists files AND generates verification scripts.

---

## Problems

### 1. `pond control` conflates three concerns

The `control` namespace lumps together unrelated operations:

| Subcommand | Actual concern |
|-----------|----------------|
| `recent` | Transaction history |
| `detail --txn-seq N` | Transaction history |
| `incomplete` | Diagnostics |
| `sync` | Remote execution |
| `show-config` | Pond status/config |
| `set-config K V` | Configuration |

The name `control` leaks the implementation (it's the control
*table*). Users think in terms of "show me the log", "what's the
config", or "sync now" -- not "interact with the control table."

### 2. `pond show` and `pond control show-config` are half of one view

Both display the pond identity. `show` adds storage stats and
partition breakdown. `show-config` adds factory modes and settings.
An operator looking at a pond for the first time must run both to
get the full picture.

Current `pond show` output:

```
+============================================================================+
|                            POND SUMMARY                                    |
+============================================================================+

  Transactions       : 13
  Delta Lake Version : 13

  Storage Statistics
  ------------------
  Parquet Files      : 28
  Total Size         : 18.4 MB
  Partitions         : 6

  Partitions (by row count)
  -------------------------
  019603b2-9e66  /
    4 rows (1 dir, 2 files, 1 versions)
  ...
```

Current `pond control show-config` output:

```
Pond Configuration
==================

Pond ID:        019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00
Created:        2025-11-15 09:23:01 UTC
Created by:     dave@beagleplay

Factory Modes:
--------------
  remote:              push

Settings:
---------
  (none configured)
```

Note that `pond show` doesn't even display the Pond ID today.
`show-config` doesn't show storage stats. Neither alone gives
the operator the full picture.

### 3. `pond run` requires verbose factory paths

Users must type the full internal path:

```bash
pond run /system/run/10-septic push
```

The path convention (`/system/run/{name}`) is deterministic.
The system should accept short names:

```bash
pond run 10-septic push
```

### 4. `pond control sync` is hardcoded to one factory

The current implementation (line 504 of `control.rs`) hardcodes the
path `"/system/run/1-backup"`. With multiple remote factories
(self-backup + imports), this breaks. It should iterate all remote
factories under `/system/run/`.

### 5. `list-files` is a subset of `show`

`show` lists files AND generates verification scripts. `list-files`
just lists files. No reason to have both. `show` is the better name
anyway -- it matches the pattern of other Unix tools that provide
detailed output.

---

## Proposed Simplification

### Eliminate `pond control`

Promote its subcommands to top-level, renaming for clarity:

| Current | Proposed | Rationale |
|---------|----------|-----------|
| `pond control recent` | `pond log` | "Show me the log" |
| `pond control detail --txn-seq N` | `pond log --txn-seq N` | Flag on the same command |
| `pond control incomplete` | `pond log --incomplete` | Same command, filter flag |
| `pond control sync` | `pond sync` | Top-level action |
| `pond control show-config` | *(merged into `pond show`)* | One status view |
| `pond control set-config K V` | `pond config set K V` | Config namespace |

### Merge the two status views

`pond show` becomes the single "tell me about this pond" command.
It merges identity, storage, factories, and settings into one view:

```
$ pond show

+============================================================================+
|                            POND SUMMARY                                    |
+============================================================================+

  Pond ID              : 019603b2-8d55-7f9f-9b4c-6f3ea052b100
  Created              : 2026-01-10 14:00:00 UTC
  Created by           : ops@dashboard
  Transactions         : 13
  Delta Lake Version   : 13

  Storage
  -------
  Parquet Files        : 28
  Total Size           : 18.4 MB
  Partitions           : 6

  Factories
  ---------
  1-backup             push      s3://dashboard-backup
  10-septic            import    s3://septic-dev -> /sources/septic/

  Settings
  --------
  (none)

  Partitions (by row count)
  -------------------------
  019603b2-9e66  /
    4 rows (1 dir, 2 files, 1 versions)
  019603b2-af77  /system/run
    12 rows (1 dir, 3 files, 8 versions)
  019503a1-8e55  /sources/septic/  [imported from 019503a1-7c44...]
    142 rows (1 dir, 1 files, 38 versions)
```

One command, one complete picture. The `--mode` flag is retained
for `brief` (above), `concise`, and `detailed` modes.

### Introduce `pond log`

Transaction history gets its own top-level command with flags
for different views:

```bash
pond log                          # recent transactions (default --limit 10)
pond log --limit 20               # more history
pond log --txn-seq 13             # detail view for one transaction
pond log --incomplete             # incomplete/crashed only
```

### Introduce `pond sync`

Manual sync becomes top-level. It iterates ALL remote factories
under `/system/run/`, not just one hardcoded path:

```bash
pond sync                         # all factories
pond sync 10-septic               # just one (short name resolved)
pond sync --config <base64>       # recovery mode (existing behavior)
```

### Introduce `pond config`

Configuration management gets a minimal namespace:

```bash
pond config                       # show config only (factory modes + settings)
pond config set KEY VALUE         # set a config value
```

This is a lightweight alternative to putting config into `pond show`.
If an operator only wants to check/change a setting without the full
pond summary, `pond config` is the fast path.

Note: `pond show` displays the same information (and more). `pond config`
exists for the "change a setting" workflow, not as a status view.

### Allow short factory names in `pond run`

When the argument doesn't start with `/`, resolve it as a short name
under `/system/run/`:

```bash
pond run 10-septic show           # resolves to /system/run/10-septic
pond run 1-backup verify          # resolves to /system/run/1-backup
pond run /custom/path/factory cmd # full paths still work
```

Resolution logic:

1. If argument starts with `/`, use as-is (current behavior)
2. Otherwise, prepend `/system/run/` and resolve

### Drop `list-files` from remote factory

`show` already provides everything `list-files` does and more.
Remove `list-files` from `RemoteCommand`.

---

## Proposed Command Set

### Before (22 entry points)

```
pond init [--from-backup|--config]
pond recover
pond show [--mode]
pond control recent [--limit]
pond control detail [--txn-seq]
pond control incomplete
pond control sync [--config]
pond control show-config
pond control set-config KEY VALUE
pond list [pattern] [--all]
pond describe [pattern]
pond cat PATH [--format] [--sql] [--time-start] [--time-end]
pond copy SOURCES DEST [--strip-prefix]
pond mkdir PATH [-p]
pond mknod FACTORY PATH --config-path [--overwrite]
pond list-factories
pond run PATH ARGS...
pond detect-overlaps PATTERNS [--verbose] [--format]
pond set-temporal-bounds PATTERN [--min-time] [--max-time]
pond export --pattern --dir [--temporal] [--start-time] [--end-time]
```

Remote factory subcommands: `push`, `pull`, `replicate`,
`list-files`, `verify`, `show`

### After (19 entry points)

```
pond init [--from-backup|--config]
pond recover
pond show [--mode]
pond log [--limit] [--txn-seq N] [--incomplete]
pond sync [NAME] [--config]
pond config [set KEY VALUE]
pond list [pattern] [--all]
pond describe [pattern]
pond cat PATH [--format] [--sql] [--time-start] [--time-end]
pond copy SOURCES DEST [--strip-prefix]
pond mkdir PATH [-p]
pond mknod FACTORY PATH --config-path [--overwrite]
pond list-factories
pond run NAME|PATH ARGS...
pond detect-overlaps PATTERNS [--verbose] [--format]
pond set-temporal-bounds PATTERN [--min-time] [--max-time]
pond export --pattern --dir [--temporal] [--start-time] [--end-time]
```

Remote factory subcommands: `push`, `pull`, `replicate`, `verify`, `show`

### Change Summary

| Change | Type | Impact |
|--------|------|--------|
| `pond control` eliminated | Restructure | 6 subcommands -> 3 top-level commands |
| `pond log` introduced | New command | Replaces `control recent/detail/incomplete` |
| `pond sync` introduced | New command | Replaces `control sync`, fixes hardcoded path |
| `pond config` introduced | New command | Replaces `control show-config/set-config` |
| `pond show` absorbs identity/factories | Enhancement | Merges two half-views into one |
| `pond run` accepts short names | Enhancement | Less typing, same capability |
| `list-files` dropped | Cleanup | Absorbed by `show` |

Net: -3 entry points, -1 namespace, better discoverability.

---

## Command Grouping (Conceptual)

After the simplification, commands group naturally by concern:

**Lifecycle**
```
pond init              # create
pond recover           # repair
```

**Status**
```
pond show              # full pond overview
pond log               # transaction history
pond config            # configuration only
```

**Browsing**
```
pond list              # what's in the pond
pond describe          # what does it look like
pond cat               # what does it contain
```

**Data Movement**
```
pond copy              # in/out
pond export            # bulk export with partitioning
```

**Structure**
```
pond mkdir             # directories
pond mknod             # factory nodes
pond list-factories    # discovery
```

**Execution**
```
pond run               # run factory commands
pond sync              # trigger all/one remote factory
```

**Analysis**
```
pond detect-overlaps       # temporal overlap detection
pond set-temporal-bounds   # metadata repair
```

---

## Implementation Notes

### `pond control` backward compatibility

The `control` namespace could be kept as a hidden alias during a
transition period. Clap supports `#[command(hide = true)]` on
deprecated variants. This lets existing scripts and documentation
catch up.

### `pond sync` iteration logic

Replace the hardcoded `"/system/run/1-backup"` in `control.rs`
with a scan of `/system/run/` that:

1. Lists all entries in `/system/run/`
2. Filters for entries with an associated factory (via `get_factory_for_node`)
3. Reads each factory's mode from the control table
4. Executes each factory in mode-appropriate order:
   - `push` factories first (backup local data before importing)
   - `import` factories second (pull foreign data)
   - `pull` factories last (replica sync)

When a short name argument is given, only that factory is executed.

### `pond show` merge

The `show_command` in `show.rs` already has a `mode` parameter. The
`brief` mode should be extended to include Pond ID, Created, Factories,
and Settings by reading from the control table. This requires the
show command to open the control table in addition to the Delta table.

The partition listing should annotate imported partitions with
`[imported from <pond_id>]` when provenance data is available
(requires the `pond_id` column from Phase 1 of cross-pond import).

### Short name resolution

Add a helper function to `cmd` that resolves factory names:

```rust
fn resolve_factory_path(name: &str) -> String {
    if name.starts_with('/') {
        name.to_string()
    } else {
        format!("/system/run/{}", name)
    }
}
```

Used in both `pond run` and `pond sync`.

### `pond log` consolidation

The three `ControlMode` variants (`Recent`, `Detail`, `Incomplete`)
map to flags on one command:

```rust
enum LogMode {
    Recent { limit: usize },        // default
    Detail { txn_seq: i64 },        // --txn-seq
    Incomplete,                     // --incomplete
}
```

The underlying implementations (`show_recent_transactions`,
`show_transaction_detail`, `show_incomplete_operations`) remain
unchanged -- only the CLI dispatch layer changes.
