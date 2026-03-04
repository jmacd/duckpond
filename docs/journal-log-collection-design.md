# Journal Log Collection Design

## Goal

Collect systemd journal logs from `watershop.local` (Debian ARM64) into a
DuckPond instance running on that machine, making them available for offsite
browsing via the existing `remote` factory (S3 replication) and `sitegen`.

## Decisions

| Question | Decision |
|---|---|
| Ingestion approach | **Option B** -- new `journal-ingest` factory |
| Which units | **All units** (auto-discover from journal) |
| Collection frequency | **Hourly** (default, configurable) |
| Format provider name | **`jsonlogs://`** -- generic JSON Lines |
| Storage granularity | **Separate `FilePhysicalSeries` per unit** |
| Retention / compaction | **Deferred** -- worry about it later |
| Entry type | **Raw JSON Lines** as `FilePhysicalSeries` |
| Kernel ring buffer | **Yes** -- separate stream via `journalctl -k` |
| ARM64 build | **TODO** -- new build machine to set up |

## Current State of the Machine

| Property | Value |
|---|---|
| Host | `watershop.local` |
| OS | Debian 12.13, aarch64 |
| Journal size | ~8 MB on disk |
| Total entries | ~117,000 across 5 boots |
| Interesting units | ssh (340), NetworkManager (4006), bluetooth (2567), systemd-networkd (1063), kernel (1407), cron (292), gdm (425), systemd-logind (564), plus ~100K misc |
| User | `jmacd`, member of `systemd-journal` group (full journal access) |
| Pond | Not yet installed (pending ARM64 build) |

## Architecture: journal-ingest Factory (Option B)

```
                 watershop.local
  +----------------------------------------------+
  |                                              |
  |  cron / systemd-timer (hourly)               |
  |    pond run journal                          |
  |       |                                      |
  |       v                                      |
  |  journal-ingest factory                      |
  |    spawns: journalctl --output=json          |
  |      --after-cursor=<stored_cursor>          |
  |      --no-pager                              |
  |    reads stdout as JSON Lines                |
  |    groups by _SYSTEMD_UNIT                   |
  |    writes per-unit FilePhysicalSeries        |
  |    stores last cursor in pond                |
  |       |                                      |
  |       v                                      |
  |  /logs/watershop/                            |
  |    ssh.service.jsonl    (FilePhysicalSeries) |
  |    NetworkManager.jsonl                      |
  |    kernel.jsonl                              |
  |    cron.service.jsonl                        |
  |    ...                                       |
  |       |                                      |
  |       v                                      |
  |  pond run /system/run/20-backup (remote)     |
  |    replicates to S3 -> offsite browsing      |
  +----------------------------------------------+
```

### Key Design Choices

- **No intermediate files**: Factory spawns `journalctl` directly, reads
  stdout as JSON Lines stream
- **Cursor stored in pond**: `{pond_path}/.cursor` file contains the
  journalctl cursor string; no external state
- **Auto-discover units**: Reads all journal entries in one pass, groups by
  `_SYSTEMD_UNIT` field, creates one file per unit
- **Kernel buffer separate**: Entries with `_TRANSPORT=kernel` (and no
  `_SYSTEMD_UNIT`) go to `kernel.jsonl`
- **Hourly default**: External cron/systemd timer calls `pond run journal`

## Format Provider: jsonlogs://

Generic JSON Lines format provider, independent of journal.

- Scheme: `jsonlogs`
- All JSON keys become Utf8 columns (sorted alphabetically)
- Two-pass: discover keys, then build RecordBatch
- Strips null bytes, skips unparseable lines with log warning
- Works with compression: `jsonlogs+zstd:///path`

Type coercion happens in SQL, not in the format provider:

```sql
SELECT
  CAST(__REALTIME_TIMESTAMP AS BIGINT) as ts,
  CAST(PRIORITY AS INT) as priority,
  MESSAGE
FROM source
WHERE CAST(PRIORITY AS INT) <= 3
ORDER BY ts
```

## Storage Layout

```
/logs/
  watershop/                         # per-host directory
    ssh.service.jsonl                # FilePhysicalSeries, one per unit
    NetworkManager.service.jsonl
    kernel.jsonl                     # kernel ring buffer
    bluetooth.service.jsonl
    systemd-logind.service.jsonl
    cron.service.jsonl
    .cursor                          # last journalctl cursor position
    ...

/system/
  etc/
    journal/                         # journal-ingest factory node
  run/
    20-backup/                       # remote factory for S3 replication
```

## Browsing Offsite

Once replicated via `remote` factory:

```bash
# Pull replica
pond run /system/run/backup pull

# Browse SSH logs
pond cat jsonlogs:///logs/watershop/ssh.service.jsonl --format=table \
  --sql "SELECT MESSAGE, PRIORITY, __REALTIME_TIMESTAMP FROM source \
         ORDER BY __REALTIME_TIMESTAMP DESC LIMIT 20"

# Search errors across all units
pond cat 'jsonlogs:///logs/watershop/*.jsonl' \
  --sql "SELECT _SYSTEMD_UNIT, MESSAGE FROM source \
         WHERE MESSAGE LIKE '%error%'"
```

## Implementation Phases

Note pending:

	crates/provider/src/factory/journal_ingest.rs
	crates/provider/src/format/jsonlogs.rs

### Phase 0: Build and Install Pond on watershop

TODO: Set up ARM64 Linux build machine.

### Phase 1: JSON Lines Format Provider -- DONE

`crates/provider/src/format/jsonlogs.rs` -- registered as `"jsonlogs"`.

Test on Mac:
```bash
ssh watershop.local 'journalctl --output=json -n 100' > /tmp/journal-sample.jsonl
pond cat host+jsonlogs:///tmp/journal-sample.jsonl --format=table \
  --sql "SELECT MESSAGE, PRIORITY, _SYSTEMD_UNIT FROM source LIMIT 10"
```

### Phase 2: journal-ingest Factory -- DONE

New executable factory: `crates/provider/src/factory/journal_ingest.rs`
Registered in `crates/provider/src/factory/mod.rs`.

**Config** (YAML):
```yaml
pond_path: /logs/watershop
journalctl_command: journalctl
collect_kernel: true
```

**Subcommands:**
- `push` (default) -- collect new entries from journal
- `pull` -- no-op
- `status` -- show cursor position and per-unit entry counts

**Behavior:**
1. Read cursor from `{pond_path}/.cursor` (or start from beginning)
2. Spawn `journalctl --output=json --no-pager [--after-cursor=CURSOR]`
3. Read all stdout as JSON Lines
4. Group entries by `_SYSTEMD_UNIT`
5. For each unit: append JSON Lines blob as new version of
   `{pond_path}/{unit}.jsonl` (FilePhysicalSeries)
6. Kernel entries (`_TRANSPORT=kernel`, no unit) -> `kernel.jsonl`
7. Entries with no unit and non-kernel transport -> `other.jsonl`
8. Save last `__CURSOR` value to `{pond_path}/.cursor`

### Phase 3: Remote Replication

Standard `remote` factory in `/system/run/` on watershop. No new code.

### Phase 4: Offsite Browsing

Pull replica on Mac. Optional `sql-derived-table` + `sitegen` for HTML views.
