# Configurable Settings: Control-Table-Managed Defaults

## Purpose

Several operational knobs in duckpond are currently hardcoded constants or
expressed only as `pond maintain` CLI flags. The maintenance retention work
(see `delta-cleanup-synchronization.md`) surfaced the question of where such
tunables belong. The answer for operator-facing defaults is the **steward
control table**: a setting written once with `pond config set` is read on every
subsequent pond open, so a single command changes behaviour for all later ticks
without recompiling or editing the per-instance `run-*.sh` wrapper.

This note records how settings are stored and read today and the convention for
promoting a hardcoded constant to a control-table-managed default.

## How settings are stored and read today

Settings live in the control table as `RecordKind::Setting` rows. The key is
held in the record's `txn_id` column and the value in `metadata_json`, with
`txn_seq = 0`. Mechanics:

- **Namespacing.** The steward wrapper stores operator settings under the
  `"setting:"` prefix and factory execution modes under `"factory_mode:"`
  (`crates/steward/src/control_table.rs:57-58`). The prefix is added/stripped
  internally; callers use the bare key.
- **Write.** `ControlTableWrapper::set_setting(key, value)`
  (`control_table.rs:260`) persists via `config_set(pond_id, "setting:{key}",
  value)` and updates the in-memory cache. The CLI entry point is
  `pond config set <key> <value>`, routed to `set_setting`
  (`crates/cmd/src/commands/control.rs:652`).
- **Read.** Settings are loaded once at pond open into a cached map
  (`control_table.rs:123` calls `config_list`). Reads use
  `get_setting(key) -> Option<String>` (`control_table.rs:254`), which is an
  in-memory lookup with **no per-commit Delta cost**. `pond config` (ShowConfig)
  prints the current settings.
- **Latest-write-wins.** `config_list` walks all `Setting` rows and keeps the
  most recent value per key by `ts_micros`
  (`crates/sync-steward/src/control_table.rs:739-752`).
- **Never pruned.** `--prune` and control-table horizon deletion preserve
  `Setting` rows explicitly (`sync-steward/src/control_table.rs:398,428-432`),
  so a configured default survives history trimming.
- **Per-instance, not replicated.** Settings are written under the local
  `pond_id` and describe this replica's operational state, not logical pond
  content. They are not pushed to remotes and are not restored automatically by
  a restore-from-remote; an operator re-applies them after a reset.

Existing keys that use this machinery include `store_id`, `birth_hostname`,
`birth_username` (`control_table.rs:860-876`) and the per-factory mode entries.

## Convention for a new configurable default

To promote a hardcoded constant (or a CLI-only flag) to a control-table setting:

1. **Define a stable key constant** in the owning crate, dotted under a topic
   namespace, e.g. `maintenance.control_log_retention_minutes`. Keep the bare
   key; the `"setting:"` prefix is applied by `set_setting`.
2. **Keep the code default.** The existing constant stays as the fallback when
   the setting is unset, so behaviour is unchanged out of the box. Treat unset
   and unparseable values identically: fall back to the default and, for the
   latter, log a warning.
3. **Read from the cache at the decision point**, not per commit. For example,
   `Ship::maintain` would read `self.control_table.get_setting(KEY)` once,
   parse it to the typed value, and pass it down to `maintenance::maintain_table`
   in place of the constant. Because each `pond` invocation opens the pond
   fresh, a setting change takes effect on the next tick.
4. **Validate and clamp.** Reject nonsensical values defensively at read time
   rather than trusting the stored string.
5. **Document the key** in `pond config` help and here.

### Worked example: control-log retention

The control table's `_delta_log` cleanup retention is currently the constant
`maintenance::CONTROL_LOG_RETENTION_MINUTES = 5`
(`crates/steward/src/maintenance.rs`). To make it operator-tunable:

```text
# one-time, persists across ticks for this instance:
pond config set maintenance.control_log_retention_minutes 1
```

`Ship::maintain` would resolve the retention as:

```rust
let minutes = self
    .control_table
    .get_setting("maintenance.control_log_retention_minutes")
    .and_then(|v| v.parse::<i64>().ok())
    .filter(|m| *m >= 0)
    .unwrap_or(maintenance::CONTROL_LOG_RETENTION_MINUTES);
```

and pass `Some(chrono::Duration::minutes(minutes))` to `maintain_table` for the
control table. The replicated data table keeps `None` (its 30-day table default)
because its commit log may still be needed for remote version diffing.

This fits the "abuse selfmon to harden maintenance" workflow: an operator can
dial the retention down on the `watershop-selfmon` instance to stress the
checkpoint-aligned cleanup without touching code or the deployed image.

## Candidates to migrate

Hardcoded maintenance constants and CLI-only flags that are good candidates for
control-table-managed defaults, once the read path above exists:

| Knob | Today | Location |
|------|-------|----------|
| Control-log retention | `const` 5 min | `steward/src/maintenance.rs` |
| Checkpoint interval | `const CHECKPOINT_INTERVAL = 10` | `steward/src/maintenance.rs` |
| Vacuum interval | `const VACUUM_INTERVAL = 10` | `steward/src/maintenance.rs` |
| Compact target size | `const COMPACT_TARGET_SIZE = 128 MiB` | `steward/src/maintenance.rs` |
| Prune `keep_txns` default | CLI flag, default 1000 | `cmd/src/main.rs` Maintain |
| Collapse-versions threshold | CLI flag, default 0 | `cmd/src/main.rs` Maintain |

CLI flags should remain as a per-invocation override; the setting provides the
default when the flag is absent. Resolution order: explicit flag > control-table
setting > code constant.

## Caveats

- **Settings are per-pond-instance.** After a destructive reset they must be
  re-applied (e.g. from the instance's `run-*.sh` or a one-shot `pond config
  set`). They do not travel with a remote backup.
- **Auto-maintain reads the same cache.** The post-commit auto-maintain path and
  explicit `pond maintain` both resolve the setting at open, so they agree
  within a process and pick up changes on the next open.
- **No schema migration.** Adding a key requires no table change; an absent key
  is simply the default.
