# Operator Guide Implementation Plan

## Problem Statement

`docs/operator-guide.md` describes a comprehensive operator experience for
DuckPond: contexts, `pond remote` CLI, `pond status`, enhanced logging, and
polished maintenance/recovery. Much of the backend infrastructure already
exists (remote push/pull, control table, factory execution), but the CLI
surface and operator-facing features are either absent or rough.

## Design Decisions (Resolved)

1. **Path convention**: `/sys/run/` (auto-exec) + `/sys/etc/` (manual).
   Behavior-based organization. The steward scans `/sys/run/*` after
   commits and executes factories based on their per-replica mode.
   Replaces the current `/system/run/` + `/system/etc/` paths.
   Breaking change to existing ponds is acceptable.

2. **Per-replica state**: Control table (current implementation).
   The control table is per-replica by nature (never replicated via
   push/pull). It stores pond identity, factory modes, import
   watermarks, and settings. No `local/` filesystem directory needed.

3. **Host mirror**: Deferred. Not in scope for this implementation.
   Factory configs are edited via `pond cat` + edit + `pond copy`.

4. **`pond sync` fate**: Replaced by `pond remote push/pull`.
   `pond sync` becomes a hidden alias.

## Current State vs. Operator Guide

### Already exists (backend ready)
| Feature | Where | Notes |
|---------|-------|-------|
| Remote push/pull | `crates/remote/src/factory.rs` | Factory subcommands: push, pull, replicate, verify, show, list-files |
| `pond sync` | `crates/cmd/src/commands/control.rs:504-735` | Scans `/system/run/*`, runs remote factories |
| `pond run` | `crates/cmd/src/commands/run.rs` | Full factory execution, template expansion |
| `pond maintain` | `crates/cmd/src/commands/maintain.rs` + `crates/steward/src/maintenance.rs` | Checkpoint, vacuum, optional compact on data+control tables |
| `pond recover` | `crates/cmd/src/commands/recover.rs` + `crates/steward/src/ship.rs` | Finds/marks incomplete txns |
| `pond log` | `crates/cmd/src/main.rs` (routes to control.rs) | `--limit`, `--txn-seq`, `--incomplete` already promoted from `pond control` |
| `pond config set` | `main.rs` + `control.rs` | Sets control table settings |
| `pond init --config` | `crates/cmd/src/commands/init.rs` | Restore/replica from remote |
| Post-commit auto-exec | `crates/steward/src/guard.rs` | Scans `/system/run/*` after writes |
| Per-replica state | `crates/steward/src/control_table.rs` | Factory modes, settings, import watermarks |

### Does NOT exist (needs implementation)
| Feature | Complexity | Description |
|---------|-----------|-------------|
| `pond context` | Medium | Named pond references in `~/.pond/config` |
| `pond remote` CLI | Low | Thin wrapper delegating to existing remote factory subcommands |
| `pond remote add` | Medium | Create remote factory + set mode in one step |
| `pond remote status` | Medium | Replication lag, last sync time, failure info |
| `pond remote list` | Low | List configured remotes with status summary |
| `pond status` | Medium | Aggregate health dashboard with exit codes |
| Path migration `/system/` -> `/sys/` | Medium | Rename constants, update steward guard, run.rs, init.rs, tests, docs |
| Output polish | Low | Format existing command output to match guide examples |

## Proposed Phases

### Phase 1: Path Migration (`/system/` -> `/sys/`)

Rename the filesystem path convention. This is a prerequisite for all
other phases since the operator guide uses `/sys/` paths throughout.

- Rename constants: `SYSTEM_RUN_DIR` -> `/sys/run`, `SYSTEM_ETC_DIR` -> `/sys/etc`
- Update steward guard post-commit scanning
- Update `pond run` short-name resolution (`resolve_factory_path`,
  `resolve_short_factory_name`)
- Update `pond sync` factory discovery
- Update `pond init` default directory creation
- Update all testsuite scripts and documentation
- No migration path needed (breaking change accepted)

### Phase 2: `pond context` (foundation for multi-pond UX)

- New `crates/cmd/src/commands/context.rs`
- `~/.pond/config` file (YAML): `current-context`, `contexts` map
- Subcommands: `add`, `use`, `list`, `current`, `remove`
- Pond path resolution: `--pond` > `$POND` > context
- Update `common.rs` `get_pond_path_with_override()` to check context config

### Phase 3: `pond remote` (thin CLI wrapper)

- New `crates/cmd/src/commands/remote.rs`
- Subcommands: `add`, `list`, `status`, `push`, `pull`, `show`, `verify`,
  `replicate`
- `add`: `pond mknod --config` + `set_factory_mode` in one step
- `list/status`: query control table for remote factory state
- Others: delegate to `pond run <path> <subcommand>`
- When one remote: name is implicit. Multiple: `--name=<name>`
- `pond sync` becomes hidden alias routing to `pond remote push/pull`

### Phase 4: `pond status` (health dashboard)

- New `crates/cmd/src/commands/status.rs`
- Aggregates: context info, remote replication status, maintenance state,
  recent transaction summary
- Exit codes: 0=healthy, 1=degraded, 2=error
- `--quiet` flag for monitoring scripts

### Phase 5: Output Polish

- `pond maintain`: match guide's formatted output (table name, operation,
  result for each step)
- `pond recover`: match guide's formatted output
- `pond config`: show pond ID, creation info, factory modes, settings
  (already partially done)
- `pond log`: formatting improvements (table output with aligned columns)

## Notes

- Phase 1 (path migration) should be done first since it changes paths
  referenced by all subsequent phases
- Phases 2-4 are relatively independent and could proceed in parallel
  after Phase 1
- Phase 5 (output polish) can be done incrementally alongside other phases
- `pond log` promotion is already complete (Phase 4 of original plan)
- The remote factory backend is solid; `pond remote` is mostly CLI sugar
- `pond maintain` and `pond recover` already work; guide enhancements
  are mostly output formatting
