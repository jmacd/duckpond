# DuckPond Operator Guide

This guide is for the person who keeps a DuckPond running: creating
ponds, configuring remotes, managing replicas, running maintenance,
monitoring health, and recovering from failures.

It assumes familiarity with the data commands (`pond copy`, `pond cat`,
`pond list`, `pond mknod`, `pond run`).

---

## Concepts

A **pond** is a transactional filesystem backed by Delta Lake.  It
stores data files, time-series, and computed views.  Every write is
an atomic transaction recorded in the Delta commit log.

A **remote** is an S3-compatible bucket that stores backup copies of
the pond's data.  The primary pond pushes to the remote after each
write transaction.

A **replica** is a read-only copy of a pond, created by pulling from
a shared remote.  Multiple replicas can exist, each with independent
local state.  Replicas see the same data as the primary (after pull).

**Maintenance** is Delta Lake housekeeping: checkpointing the
transaction log, vacuuming deleted files, and optionally compacting
small files into larger ones.

A **context** is a named reference to a pond, stored in
`~/.pond/config`.  Contexts let you manage multiple ponds without
juggling environment variables.

### Pond Layout

```
/data/mysite/                     Pond root
  data/                           Delta Lake table (the actual filesystem)
    _delta_log/                   Transaction log + commit metadata
    _large_files/                 Content-addressed store (files >64KB)
    part_id=<uuid>/               Partitions (one per directory)
  local/                          Per-replica state (disposable)
    modes.yaml                    Factory execution modes
    imports/                      Import watermarks
    push-state.yaml               Push/sync watermarks
    settings.yaml                 Operator settings
```

### Filesystem Conventions

```
/                                 Pond root
  sys/                            System configuration (replicated)
    remotes/                      Remote factory configs (auto-exec)
      1-backup                    Primary remote
    etc/                          Manual factories
      20-hydrovu                  Data collector
      90-sitegen                  Static site generator
    site/                         Static content (templates)
  var/                            System state (replicated)
    metadata                      Pond identity
  ...                             User data
```

The `/sys/` and `/var/` paths are configurable via static properties
set at pond creation time (defaults shown above).

### Universal vs. Per-Replica State

Because multiple replicas share the same data, the system
distinguishes between:

| Kind | Examples | Where it lives |
|------|----------|----------------|
| **Universal** | Factory configs, pond identity, user data | Data FS (replicated to all) |
| **Per-replica** | Factory modes, import watermarks, push state | `local/` directory (never replicated) |

Per-replica state is disposable.  You can delete `local/` and
rebuild it.  Modes re-default (primary=push, replica=pull),
watermarks are recomputed from the data.

---

## 1. Contexts

### Why contexts?

Without contexts, every command requires `$POND` to be set or
`--pond` to be passed.  Contexts provide named shortcuts and
enable features like the host mirror.

### Setting up contexts

```
$ pond context add mysite --path /data/mysite
Context "mysite" added.
Set as current context.

$ pond context add septic --path /data/septic
Context "septic" added.

$ pond context list
  CURRENT  NAME     PATH              ROLE
  *        mysite   /data/mysite      primary
           septic   /data/septic      primary
```

### Switching contexts

```
$ pond context use septic
Switched to context "septic".

$ pond context current
septic (/data/septic)
```

### Pond path resolution

Commands resolve the pond path in this order:

1. `--pond /path` flag (explicit override)
2. `$POND` environment variable (scripts, cron jobs)
3. `~/.pond/config` current context (interactive use)

Scripts and cron jobs should continue using `$POND`.  Contexts are
for interactive use and the host mirror.

### Context configuration file

```yaml
# ~/.pond/config
current-context: mysite
contexts:
  mysite:
    path: /data/mysite
  septic:
    path: /data/septic
```

---

## 2. The Host Mirror

Each context has a **host mirror**: a checkout of the pond's system
configuration onto the host filesystem.  The mirror is always
readable, even when the pond is mid-transaction.

### Mirror layout

```
~/.pond/
  config                          Context configuration
  mysite/                         Mirror for "mysite"
    sys/                          From /sys/ (universal config)
      remotes/
        1-backup.yaml             Remote factory config
      etc/
        20-hydrovu.yaml           HydroVu collector config
        90-sitegen.yaml           Site generator config
      site/
        page.md                   Template file
    var/                          From /var/ (universal state)
      metadata.yaml               Pond identity
    local/                        Per-replica state (NOT from data FS)
      modes.yaml                  Factory modes for THIS replica
      imports/                    Import watermarks
      push-state.yaml             Push watermarks
      settings.yaml               Operator settings
    .mirror-state                 Mirror version tracking
  septic/
    ...
```

### Refreshing the mirror

The mirror refreshes automatically after each write transaction.
To manually refresh:

```
$ pond config checkout
Mirror updated from pond (version 1658).
```

### Editing configuration

Edit files in the mirror, then apply:

```
$ vi ~/.pond/mysite/sys/etc/20-hydrovu.yaml
  (make changes)

$ pond config diff
  sys/etc/20-hydrovu.yaml: modified
    - collect_interval: 60
    + collect_interval: 300

$ pond config apply
  Applied 1 change to pond.
  sys/etc/20-hydrovu.yaml -> /sys/etc/20-hydrovu (updated)
```

Template secrets (`{{ env(AWS_SECRET_ACCESS_KEY) }}`) are preserved
as templates in the mirror.  They are never expanded into files on
disk.

---

## 3. Remotes

### Adding a remote

Create a YAML config file with your S3 credentials:

```yaml
# backup.yaml
url: s3://my-bucket/backups/
region: us-west-2
access_key: "{{ env(AWS_ACCESS_KEY_ID) }}"
secret_key: "{{ env(AWS_SECRET_ACCESS_KEY) }}"
```

Then add it to the pond:

```
$ pond remote add backup.yaml
Remote "1-backup" configured at /sys/remotes/1-backup
  URL:  s3://my-bucket/backups/
  Mode: push (auto after each commit)
  Test connection... OK
```

This creates the factory node in the pond and sets the mode to
`push` in the per-replica local state.

For S3-compatible endpoints (MinIO, R2, etc.), add `endpoint` and
`allow_http`:

```yaml
# minio-backup.yaml
url: s3://my-bucket/backups/
region: us-east-1
endpoint: http://minio.local:9000
allow_http: true
access_key: "{{ env(MINIO_ACCESS_KEY) }}"
secret_key: "{{ env(MINIO_SECRET_KEY) }}"
```

#### Remote config reference

```yaml
# Required
url: <string>                   # S3 URL (s3://bucket/prefix/)

# Authentication
access_key: <string>            # AWS access key (or {{ env(...) }} template)
secret_key: <string>            # AWS secret key (or {{ env(...) }} template)

# Optional
region: <string>                # AWS region (default: "")
endpoint: <string>              # Custom S3 endpoint (MinIO, R2, etc.)
allow_http: <bool>              # Allow non-TLS connections (default: false)

# Import mode (for replicas pulling from a foreign pond)
import:
  source_path: <string>         # Path in the foreign pond (e.g., "/ingest")
  local_path: <string>          # Path in this pond (e.g., "/sources/septic")
```

### Listing remotes

```
$ pond remote list
  NAME        URL                         MODE   LAST SYNC
  1-backup    s3://my-bucket/backups/     push   2 min ago (v1658)
```

### Checking replication status

```
$ pond remote status
Remote: s3://my-bucket/backups/ (via /sys/remotes/1-backup)
  Mode:          push (this replica)
  Local version: 1658
  Remote version:1658
  Status:        IN SYNC
  Last push:     2026-04-05 21:30:12 UTC (seq 1658, 0.3s)
  Last failure:  none
```

When behind:

```
$ pond remote status
Remote: s3://my-bucket/backups/ (via /sys/remotes/1-backup)
  Mode:          push (this replica)
  Local version: 1661
  Remote version:1658
  Status:        BEHIND (3 versions)
  Last failure:  2026-04-05 21:31:00 UTC -- connection timeout
  Suggestion:    Run 'pond remote push' to retry
```

### Manual push and pull

```
# Retry a failed push
$ pond remote push
Pushing versions 1659-1661 to s3://my-bucket/backups/...
  1659: 3 files, 47 KB ... OK
  1660: 1 file, 12 KB ... OK
  1661: 5 files, 128 KB ... OK
Push complete: remote now at version 1661.

# Pull from remote (replica ponds)
$ pond remote pull
Pulling from s3://my-bucket/backups/...
  New versions: 1659-1661 (3 versions, 187 KB)
  Applied: 8 files
Pull complete: local now at version 1661.
```

### Exploring the remote

```
# List ponds in the bucket
$ pond remote list-ponds
POND ID                               VERSIONS  LAST PUSH
a1b2c3d4-e5f6-7890-abcd-ef1234567890  1658      2026-04-05 21:30 UTC
f9e8d7c6-b5a4-3210-fedc-ba0987654321  423       2026-04-04 15:12 UTC

# Show what's in the remote backup
$ pond remote show
Remote backup: s3://my-bucket/backups/ (pond a1b2c3d4-...)
  /hydrovu/
    /devices/
      /device-123/
        readings.series  (table:series, 847 versions)
      /device-456/
        readings.series  (table:series, 612 versions)
  /sys/
    /remotes/
      1-backup  (factory)
    /etc/
      20-hydrovu  (factory)

# Verify backup integrity
$ pond remote verify
Verifying remote backup...
  Checking 2,255 backed-up files...
  All files valid: checksums match, no missing chunks.
  Backup is complete through version 1658.
```

### `pond remote` is a thin wrapper

Every `pond remote` subcommand delegates to `pond run`:

```
pond remote push         =  pond run /sys/remotes/<name> push
pond remote pull         =  pond run /sys/remotes/<name> pull
pond remote show         =  pond run /sys/remotes/<name> show
pond remote verify       =  pond run /sys/remotes/<name> verify
pond remote list-ponds   =  pond run /sys/remotes/<name> list-ponds
pond remote replicate    =  pond run /sys/remotes/<name> replicate
```

When there is one remote, the name is implicit.  With multiple
remotes, specify the name:

```
$ pond remote push --name=archive
```

`pond run` continues to work for all factory types.  `pond remote`
is a convenience for the most common case.

---

## 4. Replicas

### Creating a replica

On the primary pond, generate a replication command:

```
$ pond remote replicate
To create a replica of this pond, run on the target machine:

  export POND=/path/to/replica
  pond init --config eyJidWNrZXQiOi...

The config string contains S3 credentials and pond identity
(base64-encoded).
```

On the target machine:

```
$ export POND=/data/replica
$ pond init --config eyJidWNrZXQiOi...
Restoring from s3://my-bucket/backups/...
  Downloading pond a1b2c3d4-...
  Applying 1,658 versions...
Restore complete: 847 MB, 1,643 files.
```

The replica is initialized with mode=pull (per-replica state).

### Setting up periodic pull

```bash
# /etc/cron.d/duckpond-pull
*/5 * * * *  duckpond  POND=/data/replica pond remote pull 2>&1 | logger -t duckpond-pull
```

### Cross-pond import

A replica can import specific directories from a foreign pond's
backup.  This is configured via the `import` section of the remote
config:

```yaml
# import-config.yaml
url: s3://my-bucket/backups/
region: us-west-2
access_key: "{{ env(AWS_ACCESS_KEY_ID) }}"
secret_key: "{{ env(AWS_SECRET_ACCESS_KEY) }}"
import:
  source_path: /ingest
  local_path: /sources/foreign-pond
```

```
$ pond remote add import-config.yaml --mode=pull
Remote "1-import" configured at /sys/remotes/1-import
  URL:  s3://my-bucket/backups/
  Mode: pull (import from foreign pond)
  Import: /ingest -> /sources/foreign-pond
```

---

## 5. Maintenance

### What maintenance does

| Operation | What | When |
|-----------|------|------|
| Checkpoint | Consolidates delta log into a snapshot | Auto after each commit |
| Vacuum | Removes unreferenced files | Auto after each commit |
| Log cleanup | Removes old delta log JSON entries | After checkpoint |
| Compact | Merges small parquet files into larger ones | Manual only |

Checkpoint and vacuum run automatically.  You only need `pond maintain`
for forced operations or compaction.

### Running maintenance

```
$ pond maintain
Maintenance: /data/mysite

  Data table:
    Checkpoint: created (version 1661)
    Log cleanup: removed 19 old entries
    Vacuum: 0 files removed (clean)

  Remote (s3://my-bucket/backups/):
    Push: OK (0 versions pending)
    Checkpoint: created (version 1661)
    Vacuum: 3 files removed
```

### Compaction

```
$ pond maintain --compact
Maintenance: /data/mysite

  Data table:
    Checkpoint: created (version 1661)
    Compact: merged 1,643 files -> 14 files (97% reduction)
    Vacuum: 1,629 old files removed
    Size: 847 MB -> 842 MB

  Remote:
    Push: OK (pushed compacted data)
    Compact: merged 1,643 files -> 14 files
    Vacuum: 1,629 files removed
```

### Push before cleanup

During maintenance, the pond pushes to the remote *before* cleaning
up delta log entries.  The remote needs those log entries to
determine which versions are new.  If the push fails, log cleanup
is deferred:

```
$ pond maintain
  Remote push: FAILED (connection timeout)
  Data table log cleanup: DEFERRED (needed for next push)
```

### Coordination

Maintenance uses Delta Lake's transaction conflict mechanism to
prevent concurrent maintenance processes from colliding.  If another
maintenance is running:

```
$ pond maintain
Error: Concurrent maintenance detected. Another process is
running maintenance on this table. Retry later.
```

---

## 6. Status

```
$ pond status

Context:     mysite
Pond:        /data/mysite
ID:          a1b2c3d4-e5f6-7890-abcd-ef1234567890
Role:        primary (push)

Remote:      s3://my-bucket/backups/ (/sys/remotes/1-backup)
  Status:    IN SYNC -- last push 2 min ago
  Behind:    0 versions

Maintenance: last auto 2 min ago
  Compact:   never run (1,643 small files)

Recent:      5 txns/hour, 0 failures
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Healthy |
| 1 | Degraded (replication behind, needs maintenance) |
| 2 | Error (replication failed, incomplete transactions) |

Use in monitoring:

```bash
pond status --quiet || alert "DuckPond needs attention"
```

---

## 7. Transaction Log

### Recent transactions

```
$ pond log

Pond: a1b2c3d4-... (/data/mysite)

SEQ   TYPE   COMMAND                         STATUS     DURATION  POST-COMMIT
1661  write  pond run 20-hydrovu collect     completed  2.3s      push: ok
1660  write  pond copy host:///tmp/f /data/   completed  0.1s      push: ok
1659  write  pond run 20-hydrovu collect     completed  1.8s      push: ok
1658  read   pond cat /data/readings.series  completed  0.4s      --
1657  write  pond run 20-hydrovu collect     completed  2.1s      push: failed
...
```

### Transaction detail

```
$ pond log --txn-seq 1657

Transaction 1657
  Type:    write
  Command: pond run 20-hydrovu collect
  Started: 2026-04-05 21:28:00.000 UTC
  Status:  completed (2.1s)

  Post-commit:
    /sys/remotes/1-backup:
      push started  21:28:02.150 UTC
      push FAILED   21:28:05.300 UTC -- connection timeout
```

### Incomplete operations

```
$ pond log --incomplete

Incomplete operations:
  SEQ 1662: write -- began 21:35:00 UTC, no commit recorded
    Likely cause: process crashed during write
    Recovery: run 'pond recover'
```

---

## 8. Configuration

### Viewing

```
$ pond config

Pond ID:    a1b2c3d4-e5f6-7890-abcd-ef1234567890
Created:    2025-06-15 08:30:00 UTC

Per-replica settings:
  mode:     push (primary)

Factory modes:
  /sys/remotes/1-backup: push
```

### Setting

```
$ pond config set maintenance_lock off
Maintenance lock cleared.
```

### Editing factory configs

See section 2 (Host Mirror).  Edit files in `~/.pond/<context>/sys/`,
then `pond config apply`.

---

## 9. Recovery

### From a crash

```
$ pond list
Error: Incomplete transaction detected (seq 1662).
Run 'pond recover' to restore consistency.

$ pond recover
Checking transaction log...
  Transaction 1662: begin recorded, no commit -- marking as failed.
  Data table: consistent (rolled back to version 1661).
Recovery complete.
```

### From a remote backup

```
# With the config file:
$ export POND=/data/restored
$ pond init --from-backup backup.yaml

# With the base64 config string:
$ export POND=/data/restored
$ pond init --config eyJidWNrZXQiOi...

Restoring from s3://my-bucket/backups/...
  Downloading pond a1b2c3d4-...
  Applying 1,658 versions...
Restore complete: 847 MB, 1,643 files.
```

### Resetting per-replica state

Per-replica state is disposable.  If it becomes corrupt:

```
$ rm -rf /data/mysite/local/
$ pond recover
Rebuilding per-replica state...
  Factory modes: defaulted (push)
  Push watermarks: recomputed from remote
  Import watermarks: recomputed from data
Recovery complete.
```

---

## 10. Cron / Systemd Integration

### Data collection (every minute)

```bash
# /etc/cron.d/duckpond-collect
* * * * *  duckpond  POND=/data/mysite pond run 20-hydrovu collect 2>&1 | logger -t duckpond
```

### Weekly compaction

```bash
# /etc/cron.d/duckpond-maintain
0 3 * * 0  duckpond  POND=/data/mysite pond maintain --compact 2>&1 | logger -t duckpond-maintain
```

### Health monitoring

```bash
# /etc/cron.d/duckpond-monitor
*/5 * * * *  duckpond  POND=/data/mysite pond status --quiet || echo "DuckPond unhealthy" | mail -s "Alert" ops@example.com
```

### Systemd timer

```ini
# /etc/systemd/system/duckpond-collect.timer
[Unit]
Description=DuckPond data collection

[Timer]
OnCalendar=minutely
Persistent=true

[Install]
WantedBy=timers.target
```

```ini
# /etc/systemd/system/duckpond-collect.service
[Unit]
Description=DuckPond data collection

[Service]
Type=oneshot
User=duckpond
Environment=POND=/data/mysite
ExecStart=/usr/local/bin/pond run 20-hydrovu collect
```

---

## 11. Operational Runbook

### "Replication is behind"

```
$ pond status
  Remote: BEHIND (3 versions)

$ pond remote push
  Pushing 3 versions... OK

$ pond status
  Remote: IN SYNC
```

### "Disk is filling up"

```
$ pond status
  Data size: 4.2 GB (12,847 files)
  Compact: never run

$ pond maintain --compact
  Compact: 12,847 files -> 42 files
  Vacuum: removed 12,805 old files
  Size: 4.2 GB -> 3.8 GB
```

### "Maintenance lock is stuck"

```
$ pond maintain
  Error: Concurrent maintenance detected.

# If you are sure no other maintenance is running:
$ pond config set maintenance_lock off
  Lock cleared.

$ pond maintain
  OK
```

### "Transaction failed, data inconsistent"

```
$ pond log --incomplete
  SEQ 1662: incomplete write

$ pond recover
  Transaction 1662 rolled back.
  Pond is consistent.
```

### "Need to set up a replica"

```
# On primary:
$ pond remote replicate
  pond init --config eyJidWNrZXQiOi...

# On target machine:
$ export POND=/data/replica
$ pond init --config eyJidWNrZXQiOi...
  Restored from remote.

# Set up periodic pull:
$ cat > crontab-entry
*/5 * * * *  duckpond  POND=/data/replica pond remote pull 2>&1 | logger -t duckpond-pull
```

### "Need multiple remotes"

```
$ pond remote add primary.yaml --name=primary
$ pond remote add archive.yaml --name=archive

$ pond remote status
  primary:  IN SYNC (s3://bucket-1/)
  archive:  BEHIND 12 versions (s3://bucket-2/)

$ pond remote push --name=archive
```

---

## Quick Reference

```
CONTEXTS
  pond context add <name> --path <path>     Add a context
  pond context use <name>                   Switch current context
  pond context list                         List all contexts
  pond context current                      Show current context

REMOTES
  pond remote add <config.yaml>             Configure a remote
  pond remote list                          List configured remotes
  pond remote status                        Show replication status
  pond remote push                          Push to remote
  pond remote pull                          Pull from remote
  pond remote show                          Show remote backup contents
  pond remote verify                        Verify backup integrity
  pond remote list-ponds                    List ponds in the bucket
  pond remote replicate                     Generate replica command

MAINTENANCE
  pond maintain                             Checkpoint + vacuum
  pond maintain --compact                   Also compact small files

MONITORING
  pond status                               Health dashboard
  pond status --quiet                       Exit code only (for scripts)
  pond log                                  Recent transactions
  pond log --txn-seq <N>                    Transaction detail
  pond log --incomplete                     Incomplete operations

CONFIGURATION
  pond config                               Show configuration
  pond config set <key> <value>             Set a setting
  pond config checkout                      Refresh mirror from pond
  pond config diff                          Show mirror changes
  pond config apply                         Commit mirror edits to pond

RECOVERY
  pond recover                              Fix incomplete transactions
  pond init --from-backup <config.yaml>     Restore from backup
  pond init --config <base64>               Restore from encoded config
```

---

## YAML Reference

### Remote config (`/sys/remotes/<name>`)

```yaml
url: s3://bucket/prefix/             # Required: S3-compatible URL
region: us-west-2                     # AWS region
access_key: "{{ env(AWS_ACCESS_KEY_ID) }}"    # Access key (template)
secret_key: "{{ env(AWS_SECRET_ACCESS_KEY) }}" # Secret key (template)
endpoint: http://minio:9000           # Custom S3 endpoint
allow_http: true                      # Allow non-TLS (MinIO, local dev)

# For import/replica mode:
import:
  source_path: /ingest                # Path in the foreign pond
  local_path: /sources/foreign        # Path in this pond
```

### Context config (`~/.pond/config`)

```yaml
current-context: mysite
contexts:
  mysite:
    path: /data/mysite
  septic:
    path: /data/septic
```

### Per-replica modes (`local/modes.yaml`)

```yaml
# Factory execution modes for THIS replica.
# Set by 'pond remote add --mode=...' or 'pond config set mode ...'.
# Primary ponds default to push; replicas default to pull.
/sys/remotes/1-backup: push
```

### Per-replica push state (`local/push-state.yaml`)

```yaml
# Tracks which data versions have been pushed to the remote.
# Managed automatically. Delete to force re-sync.
/sys/remotes/1-backup:
  last_pushed_version: 1658
  last_push_time: "2026-04-05T21:30:12Z"
```

### Per-replica import state (`local/imports/<factory>.yaml`)

```yaml
# Tracks which foreign partitions/versions have been imported.
# Managed automatically. Delete to force re-import.
partitions:
  - part_id: a1b2c3d4-...
    foreign_pond_id: f9e8d7c6-...
    watermark_txn_seq: 423
  - part_id: e5f6a7b8-...
    foreign_pond_id: f9e8d7c6-...
    watermark_txn_seq: 423
```
