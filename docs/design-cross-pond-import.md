# Cross-Pond Import: Design

## Overview

Every pond writes a `pond_id` column (its own UUID) into every OpLog
record and every remote bundle record. This is a required, non-nullable
field that every pond always writes for itself.

When importing data from another pond, we literally copy its parquet
files (reassembled from its backup bundles) into the local Delta Lake
table. The foreign files already contain the foreign pond's UUID in
their `pond_id` column. They fit right in -- just add them as Delta
Lake `add` actions. No deserialization, no re-writing, no transformation.

The `(pond_id, txn_seq)` composite key is globally unique. The
UUID-based `node_id` and `part_id` fields are already globally unique
(UUID v7). Together, this makes cross-pond data coexistence
structurally sound.

Import is not a new command. It is an extension of the existing remote
factory. A remote factory created via `pond mknod` can be configured to
point at a foreign pond's backup and name a path in the local pond
where the foreign content is permanently mounted. The factory's `pull`
subcommand downloads foreign partitions, adds them to the local Delta
table, and links them at the configured mount point.

---

## Motivating Example

The `septic` system runs on a BeaglePlay. Its pond ingests sensor data
and backs up to S3:

```
Septic pond (BeaglePlay):
/ingest/
  septicstation.json.series   (raw OTelJSON sensor data)
/reduced/
  pumps/
  cycle-times/
  flow-totals/
  ...                         (temporal-reduce aggregations)
/system/
  run/
    1-backup                  (remote factory, pushes to s3://septic-dev)
```

The septic pond pushes its backup to `s3://septic-dev`. We want a
central "dashboard" pond that imports the septic data and runs site
generation across multiple data sources:

```
Central pond (server):
/sources/
  septic/                      <-- mount point, linked to septic's /ingest/
    septicstation.json.series   (from septic pond, read-only)
  weather/                     <-- another imported source
    ...
/reduced/                      (local temporal-reduce over imported data)
/system/
  run/
    1-backup                   (backs up this pond, including imports)
    10-septic                  (remote factory, imports from s3://septic-dev)
    11-weather                 (remote factory, imports from another backup)
  site.yaml                    (sitegen reading from /reduced/)
```

Setup for the central pond:

```bash
pond init

# Own backup
pond mknod remote /system/run/1-backup --config-path backup.yaml

# Import septic data at /sources/septic
pond mknod remote /system/run/10-septic --config-path septic-import.yaml

# The mknod post-create function:
# 1. Opens the foreign backup at s3://septic-dev
# 2. Identifies the partition(s) for the configured source path (/ingest)
# 3. Downloads the parquet files via ChunkedReader (BLAKE3 verified)
# 4. Adds them to the local Delta table as `add` actions
# 5. Creates /sources/septic/ linking to the imported partition
#
# On subsequent pulls, only new/changed partitions are downloaded.
```

The `septic-import.yaml` config:

```yaml
# Foreign pond's backup location (full path including pond-{uuid})
url: "s3://septic-dev/pond-019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00"
region: "auto"
endpoint: "{{ env(name='R2_ENDPOINT') }}"
access_key: "{{ env(name='R2_KEY') }}"
secret_key: "{{ env(name='R2_SECRET') }}"

# Cross-pond import configuration
import:
  # Path in the foreign pond to import
  # Use /** suffix to recursively import all child directories
  source_path: "/ingest/**"
  # Path in this pond where imported content appears
  local_path: "/sources/septic"
```

---

## Schema Changes

### OpLog (tlogfs parquet rows)

Add a `pond_id: Utf8` column (non-nullable) to `OplogEntry`. Every
record carries the UUID of the pond that created it.

- For locally-created records, this is the local pond's UUID.
- For imported records (literal parquet file copies), this is the
  foreign pond's UUID -- already present in the file, no transformation.

The column is provenance information. tlogfs treats it as opaque; it
does not interpret the value. Its purpose is to scope `txn_seq` so
that `(pond_id, txn_seq)` is globally unique.

### Remote bundle (chunked file rows)

Add a `pond_id: Utf8` column (non-nullable) to `ChunkedFileRecord`.
Every backed-up file carries the UUID of the pond that created it.

### Remote bundle_id partitions

The `bundle_id` format changes from `FILE-META-{date}-{txn_seq}` to
`FILE-META-{pond_id}-{date}-{txn_seq}`, using the full pond UUID.
This prevents collisions if two ponds share a backup location and
makes provenance visible at the partition level.

### Delta commit metadata

The `PondTxnMetadata` struct gains a `pond_id` field. This is
redundant with the per-row `pond_id` column but convenient for quick
inspection of Delta commits without scanning rows.

### Control table (steward)

Already has `pond_id` in metadata records. No schema change needed,
but import operations are tracked as new record types (see below).

---

## Write Path

`OpLogPersistence` receives the local `pond_id` at creation/open time:

```
OpLogPersistence::create(path, pond_id)
OpLogPersistence::open(path, pond_id)
```

The persistence layer stores `pond_id` and stamps it into every
`OplogEntry` at write time. Callers never need to think about it.
tlogfs remains identity-agnostic in that it does not interpret the
value -- it simply writes it as a column.

For import, the normal write path is bypassed entirely. Foreign
parquet files are added to the local Delta table as raw `add` actions.
The `pond_id` column in those files already contains the foreign
pond's UUID.

---

## Import Mechanism

### Source

Import reads from a foreign pond's backup bundles. The data is stored
as chunked records in a remote Delta Lake table. The import process:

1. Reassembles chunks via `ChunkedReader` (BLAKE3 verified)
2. Writes the reassembled parquet file to the local object store
3. Adds it to the local Delta log as an `add` action

### Granularity

Import operates on whole partitions. A partition corresponds to one
directory and all its direct children. Importing a directory means
importing its entire partition, including records from all transaction
sequences accumulated in that partition.

Importing a directory tree (with nested subdirectories) means importing
multiple partitions -- one per directory in the tree.

### Schema Compatibility

The foreign pond's OpLog must have the same schema as the local pond's
OpLog, including the `pond_id` column. Ponds created before this
column was added cannot participate in cross-pond import. A validation
step checks schema compatibility before importing.

### Version Tracking

How to indicate which version of the foreign pond wrote a given bundle
is deferred for now. The initial implementation imports the latest
available state.

---

## Import Paths

> **Note:** The original design called these "mount points." Per Q3
> decision, we use "import path" to avoid confusion with hostmount
> overlays. The `import.local_path` config field replaced `mount_path`.

Imported content appears at a named path in the local pond. The
import path is configured in the remote factory's YAML as
`import.local_path` and created by the factory's `initialize` hook
during `mknod`.

### Lifecycle

1. `pond mknod remote /system/etc/10-septic --config-path septic-import.yaml`
   - The factory validates the config
   - The `initialize` hook (runs inside mknod transaction):
     a. Opens the foreign backup via `RemoteTable` (read-only)
     b. Reads foreign OpLog via `ChunkedAsyncFileReader`
     c. Navigates the foreign directory tree to find the `part_id`
        for the configured `source_path`
     d. Creates parent directories at `local_path` via `create_dir_all`
     e. Creates the final directory with the foreign `FileID`
        (`part_id` and `node_id` match the foreign directory)

2. `pond run /system/etc/10-septic pull` (data transfer):
   a. Lists foreign backup files, filters to target `part_id`
   b. Downloads parquet files via `ChunkedReader` (BLAKE3 verified)
   c. Writes to local object store
   d. Creates Delta commit with `Add` actions via `CommitBuilder`
   e. Data becomes visible immediately since `local_path` already
      references the foreign `part_id`

### Import Path Properties

- **Permanent**: the path exists as a real directory in the pond
- **Foreign partition**: the directory's `part_id` matches the foreign
  pond's partition, so imported parquet files land in the right place
- **Transparent**: other factories (temporal-reduce, sitegen) can read
  from the import path using ordinary pond paths, e.g.,
  `series:///sources/septic/septicstation.json`
- **Convention-only read restriction**: local writes are technically
  safe (scoped by `pond_id`) but not recommended (see Q1)

### Interaction with Other Factories

This is the key benefit. Because imported data lives at a real path
in the pond, it integrates naturally with the rest of the factory
pipeline:

```yaml
# reduce.yaml in the central pond -- reads from imported data
entries:
  - name: "pumps"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson:///sources/septic/septicstation.json"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns: ["orenco_RT_Pump1_Amps", ...]
```

```yaml
# site.yaml in the central pond -- exports from local reductions
exports:
  - name: "metrics"
    pattern: "/reduced/*/*/*.series"
```

The septic pond no longer needs to run sitegen or temporal-reduce.
It only ingests raw data and pushes its backup. The central pond
handles aggregation and site generation across all data sources.

---

## txn_seq Semantics

The `txn_seq` values in imported records are the foreign pond's
transaction numbers, scoped by `pond_id`. The local pond's `txn_seq`
monotonicity is unaffected -- it governs only the Delta commit
sequence, not the values inside imported rows.

`txn_seq` is primarily used by the control table and steward layer,
and appears in debugging commands like `pond show`. The import does
not interfere with these uses because:

- Delta commit metadata is always local (carries the local `txn_seq`)
- The `begin_write()` invariant (`txn_seq == last_txn_seq + 1`)
  governs Delta commits, not row contents
- Backup push scans Delta commit logs for new `add` actions, which
  naturally includes imported files in the commit that performed
  the import

---

## Remote Factory Extension

Import is handled by the existing remote factory. The factory config
gains an optional `import` section that distinguishes cross-pond
import from self-backup/replication.

### Configuration

When the `import` section is present, the factory operates in import
mode. When absent, it operates in the existing push/pull modes.

```yaml
# Self-backup (existing behavior, no import section)
url: "s3://my-backup"
compression_level: 3

# Cross-pond import (new behavior)
# NOTE: url must be the FULL backup table path including pond-{uuid}.
# The foreign pond's UUID is visible via 'list-ponds' or backup output.
url: "s3://septic-dev/pond-019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00"
import:
  source_path: "/ingest"          # flat: import only /ingest partition
  local_path: "/sources/septic"

# Recursive import — includes all child partitions
url: "s3://duckpond-linux/pond-019d0473-28a8-7169-9848-00d60bebbc3c"
import:
  source_path: "/logs/**"         # recursive: import /logs and all descendants
  local_path: "/sources/workshophost"
```

The `source_path` field supports two modes:
- **Flat** (`"/ingest"`): imports only the named directory's partition
- **Recursive** (`"/logs/**"`): imports the named directory and all
  descendant physical directories (each with their own partition)

When flat mode encounters a child directory that references an
unimported foreign partition, the system produces a clear error:
"Foreign partition {id} not imported. Use source_path with ** to
import recursively."

A pond can have multiple remote factories: one for its own backup
(mode `push`) and one or more for foreign imports.

### Import Lifecycle

Import is a two-step process, consistent with how other factories
(hydrovu, logfile-ingest) work:

1. **`pond mknod`** (fast, metadata only):
   - Opens the foreign backup read-only
   - Reads the foreign OpLog via `ChunkedAsyncFileReader` to discover
     directory structure
   - Finds the `part_id` for `source_path` by navigating the foreign
     directory tree
   - If `source_path` ends with `/**`, recursively discovers all
     descendant physical directories and their `part_id` values
   - Creates local directory at `local_path` with the foreign
     `FileID` (same `part_id` and `node_id` as the foreign directory)
   - For recursive imports, creates local directory entries for all
     discovered child directories with their foreign `FileID`s
   - Creates parent directories as needed via `create_dir_all`

2. **`pond run <factory> pull`** (data transfer):
   - Lists all backed-up files in the foreign backup
   - Filters to parquet files whose path matches ALL discovered
     `part_id` values (not just the top-level one)
   - Downloads each file via `ChunkedReader` (BLAKE3 verified)
   - Writes to local object store
   - Creates a Delta commit with `Add` actions using `CommitBuilder`
     (zero-copy: foreign parquet files are NOT re-serialized)

Since `local_path` already references the foreign `part_id` (from
step 1), the data becomes queryable as soon as it's downloaded.

### Execution Mode

Import factories are placed in `/system/etc/` (not `/system/run/`)
to avoid auto-pulling on every commit. They run manually via
`pond run` or `pond sync`. The remote factory detects import mode
from the config and allows `Pull` in `PondReadWriter` mode
(bypassing the `ControlWriter` requirement that normal push/pull has).

### Re-import Detection

The initial implementation downloads all matching partition files,
skipping files that already exist in the local object store (by
checking `object_store.head()`). Full incremental sync via control
table tracking is deferred to Phase 3.

---

## Impact on Backup

When the local pond pushes to its own backup, imported parquet files
are included. They appear as `add` actions in the Delta commit that
performed the import, and the push mechanism backs up all new `add`
actions.

This means Pond A's backup contains a copy of data that originally
came from Pond B. A third pond restoring from Pond A's backup gets
Pond B's data transitively. The `pond_id` column preserves
provenance, so there is no identity confusion.

This is the correct default. The backup should be a complete,
self-contained copy. Optimizations (e.g., referencing the original
foreign backup instead of copying) are deferred -- they would
interact with Delta Lake `vacuum` operations and create fragile
cross-backup dependencies.

---

## Implementation Phases

### Phase 1: Schema Foundation -- COMPLETE

- Added `pond_id: Utf8` (non-nullable) to `OplogEntry` in tlogfs
- Added `pond_id: Utf8` (non-nullable) to `ChunkedFileRecord` in remote
- Added `pond_id` field to `PondTxnMetadata`
- Updated `bundle_id` format to `FILE-META-{pond_id}-{date}-{txn_seq}`
- Pass `pond_id` into `OpLogPersistence::create()` and `open()`
- Stamp `pond_id` into every record at commit time
- All existing tests pass unchanged

### Phase 2: Cross-Pond Import via Remote Factory -- COMPLETE

- Extended `RemoteConfig` with optional `ImportConfig` section
  (`source_path`, `local_path`)
- Implemented `ChunkedAsyncFileReader` (parquet `AsyncFileReader`
  over chunked backup storage for reading foreign OpLog)
- `initialize_remote` hook at mknod time:
  - Opens foreign backup, reads OpLog via ChunkedAsyncFileReader
  - Navigates foreign directory tree to find `part_id` for source_path
  - Creates local directory at `local_path` with foreign `FileID`
    (using `FileID::new_from_ids` with the foreign part_id/node_id)
- `execute_import` on pull:
  - Lists foreign backup files, filters to target partition
  - Downloads parquet files via ChunkedReader (BLAKE3 verified)
  - Creates Delta commit with `Add` actions via `CommitBuilder`
    (zero-copy: foreign files not re-serialized)
- Import factories in `/system/etc/` can Pull in PondReadWriter mode
- End-to-end test (530) passes 6/6 checks with MinIO
- Remote exploration subcommands:
  - `list-ponds`: scans bucket for pond-{uuid}/ prefixes
  - `show`: reads foreign OpLog, displays full directory tree
  - Both accessible via `pond run host+remote:///config.yaml <cmd>`

### Phase 2.5: Recursive Import and Error Handling -- IN PROGRESS

- Parse `**` suffix from `source_path` to determine recursive mode
- During mknod: recursively walk foreign directory tree to discover
  all descendant physical directories and their `part_id` values
- Create local directory entries for ALL discovered partitions
- During pull: download parquet files for ALL discovered partitions
- When traversing an imported directory that references an
  un-imported foreign partition, produce a clear error:
  "Foreign partition {id} not imported. Use source_path with **
  to import recursively."

### Phase 3: Incremental Sync -- FUTURE

- On subsequent pulls, track which files were already imported
  (currently uses object_store.head() existence check)
- Full control table tracking of import state
- Support for importing updated partitions when the foreign pond
  adds new data

---

## Known Issues

### mDNS (.local) hostname resolution

The Rust HTTP client (reqwest/hyper) does not support mDNS. Hostnames
like `watershop.local` resolve via macOS Bonjour but not via the
standard DNS resolver used by the S3 object_store client. This causes
silent 57-second hangs instead of immediate DNS failures.

**Workaround**: use IP addresses or add entries to `/etc/hosts`.

**Root cause**: the object_store S3 client should have a short connect
timeout so unresolvable hosts fail fast. This is a library configuration
issue, not specific to cross-pond import.

---

## Remote Exploration Commands

The remote factory provides subcommands for exploring backup storage
without external S3 CLI tools:

```bash
# List all ponds in a bucket
pond run host+remote:///config.yaml list-ponds

# Show the full directory tree of a specific pond backup
pond run host+remote:///config.yaml show
```

`list-ponds` scans the bucket for `pond-{uuid}/` prefixes using
direct object_store listing. `show` reads the foreign OpLog via
`ChunkedAsyncFileReader` and recursively displays the directory tree
with entry types, sizes, and version counts.

---

## Administrative Reference

This section describes the end-state user experience for managing
ponds, remote backups, cross-pond imports, and the identifiers that
tie them together.

### Identifiers

| Identifier | Format | Scope | Purpose |
|-----------|--------|-------|---------|
| `pond_id` | UUID v7 | Global | Unique identity of a pond, assigned at `pond init`, preserved across replicas |
| `txn_seq` | Integer (1, 2, 3, ...) | Per-pond | Monotonically increasing transaction sequence within one pond |
| `(pond_id, txn_seq)` | Composite | Global | Uniquely identifies one transaction across all ponds |
| `node_id` | UUID v7 | Global | Identity of a file, directory, or symlink |
| `part_id` | UUID v7 | Global | Identity of a directory's partition in the Delta table |
| `txn_id` | UUID v7 | Global | Identity of one transaction execution (links control + data tables) |
| `bundle_id` | String | Per-backup | Partition key in the remote backup table (e.g., `FILE-META-{pond_id}-{date}-{txn_seq}`) |

**Key invariant:** `pond_id` scopes `txn_seq`. Two ponds can both
have `txn_seq=5`, but `(pond-A, 5)` and `(pond-B, 5)` are distinct.
All other identifiers (`node_id`, `part_id`, `txn_id`) are UUID v7
and globally unique without scoping.

### pond config

Displays the pond's identity, factory modes, and settings.

```
$ pond config

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

After cross-pond import, a pond with both self-backup and imports:

```
$ pond config

Pond Configuration
==================

Pond ID:        019603b2-8d55-7f9f-9b4c-6f3ea052b100
Created:        2026-01-10 14:00:00 UTC
Created by:     ops@dashboard

Factory Modes:
--------------
  remote:              push

Remote Factories:
-----------------
  /system/run/1-backup       mode: push     -> s3://dashboard-backup
  /system/run/10-septic      mode: import   -> s3://septic-dev
  /system/run/11-weather     mode: import   -> s3://weather-backup

Imports:
--------
  /sources/septic/             from pond 019503a1-7c44-... via /system/run/10-septic
  /sources/weather/            from pond 018f02c3-9e11-... via /system/run/11-weather

Settings:
---------
  (none configured)
```

The `Remote Factories` section lists every remote factory node found
under `/system/run/`, showing its mode and target URL. The `Imports`
section lists every active mount point, the foreign `pond_id` it
sources from, and the factory that manages it.

### pond log

Transaction history shows `pond_id` context when relevant:

```
$ pond log --limit 5

+- Transaction 12 (write) ----------------------------------------
|  Status       : COMPLETED
|  UUID         : 019703c4-ae66-7f0a-bc5d-7g4fb163c200
|  Command      : copy host+series:///var/log/sensors.csv /ingest/sensors.csv
+----------------------------------------------------------------

+- Transaction 13 (write) ----------------------------------------
|  Status       : COMPLETED
|  UUID         : 019703c5-bf77-7f1b-cd6e-8h5gc274d300
|  Command      : internal post-commit-factory
|  Post-commit  : 1-backup (push) -> OK
|  Post-commit  : 10-septic (import/pull) -> OK, 2 partitions updated
+----------------------------------------------------------------
```

Import-related post-commit tasks show the number of partitions
updated from the foreign pond. If no new data is available, the
import reports "up to date".

### pond log --txn-seq N

Transaction detail includes import metadata when the transaction
performed an import:

```
$ pond log --txn-seq 13

+- BEGIN write Transaction ----------------------------------------
|  Sequence     : 13
|  UUID         : 019703c5-bf77-7f1b-cd6e-8h5gc274d300
|  Command      : internal post-commit-factory
+----------------------------------------------------------------
|  [OK] DATA COMMITTED at 2026-01-15 09:00:01 UTC (version 13, duration: 12ms)

=== POST-COMMIT TASKS ============================================

+- POST-COMMIT TASK #1 PENDING -----------------------------------
|  Factory      : remote
|  Config       : /system/run/1-backup
|  [RUN] STARTED at 2026-01-15 09:00:01 UTC
|  [OK] COMPLETED at 2026-01-15 09:00:03 UTC (duration: 2100ms)
+----------------------------------------------------------------

+- POST-COMMIT TASK #2 PENDING -----------------------------------
|  Factory      : remote
|  Config       : /system/run/10-septic
|  Mode         : import
|  Source Pond   : 019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00
|  [RUN] STARTED at 2026-01-15 09:00:03 UTC
|  [OK] COMPLETED at 2026-01-15 09:00:05 UTC (duration: 1800ms)
|  Imported     : 2 partitions, 48 files, 12.3 MB
+----------------------------------------------------------------
```

### pond sync

Manual sync triggers all remote factories based on their mode. With
imports, `sync` pulls new data from foreign ponds:

```bash
# Sync all remote factories (push own backup, pull imports)
pond sync

# Sync with explicit config (recovery)
pond sync --config=<base64>
```

Output during sync:

```
[SYNC] Executing manual sync operation...
[SYNC] /system/run/1-backup (mode: push)
   [OK] All transactions already backed up
[SYNC] /system/run/10-septic (mode: import)
   Source pond: 019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00
   Remote has 45 transactions (local has through 42)
   Importing 3 new transactions...
      [OK] Imported partition 019503a1-... (2 files, 1.2 MB)
   [OK] Import complete
[OK] Sync operation completed
```

### pond run (remote factory commands)

The existing `pond run` commands work unchanged for self-backup. New
behavior appears for import-mode factories:

```bash
# Self-backup factory (mode: push)
pond run /system/run/1-backup push           # Push to own backup
pond run /system/run/1-backup verify         # Verify own backup
pond run /system/run/1-backup show           # List own backed-up files
pond run /system/run/1-backup replicate      # Generate replica config

# Import factory (mode: import)
pond run /system/run/10-septic pull          # Pull new data from foreign pond
pond run /system/run/10-septic show          # List files in foreign backup
pond run /system/run/10-septic verify        # Verify foreign backup integrity
pond run /system/run/10-septic status        # Show import sync status
```

The `status` subcommand (new) shows the state of the import:

```
$ pond run /system/run/10-septic status

Import Status: /system/run/10-septic
==========================================

Source Backup   : s3://septic-dev
Source Pond ID  : 019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00
Mount Path      : /sources/septic/
Source Path     : /ingest/

Sync Status:
  Remote transactions : 45
  Local imported      : 42
  Behind by           : 3 transactions

Imported Partitions:
  part_id=019503a1-8e55-...   /ingest/   (38 files, 4.2 MB)

Last Import     : 2026-01-15 09:00:05 UTC (txn_seq 13)
```

### pond show

The `pond show` summary includes imported data:

```
$ pond show

+============================================================================+
|                            POND SUMMARY                                    |
+============================================================================+

  Pond ID              : 019603b2-8d55-7f9f-9b4c-6f3ea052b100
  Transactions         : 13
  Delta Lake Version   : 13

  Storage Statistics
  ------------------
  Parquet Files        : 28
  Total Size           : 18.4 MB
  Partitions           : 6

  Data Provenance
  ---------------
  Local (this pond)    : 5 partitions, 16.1 MB
  Imported             : 1 partition, 2.3 MB (from 1 foreign pond)

  Partitions (by row count)
  -------------------------

  019603b2-9e66  /
    4 rows (1 dir, 2 files, 1 versions)

  019603b2-af77  /system/run
    12 rows (1 dir, 3 files, 8 versions)

  019503a1-8e55  /sources/septic/  [imported from 019503a1-7c44...]
    142 rows (1 dir, 1 files, 38 versions)
```

Imported partitions are annotated with `[imported from <pond_id>]` so
the operator can distinguish local from foreign data at a glance.

### pond list / pond describe

List and describe commands work transparently on imported content.
Foreign files appear at their mount path as if they were local:

```
$ pond list '/sources/septic/*'

/sources/septic/septicstation.json.series

$ pond describe '/sources/septic/*'

[FILE] /sources/septic/septicstation.json.series
   Type: TablePhysicalSeries
   Schema: 26 fields
   Format: Parquet
   Versions: 38
   Timestamp Column: timestamp
   Temporal Range: 2025-11-15 00:00:00 .. 2026-01-15 08:55:00
   Provenance: pond 019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00
```

The `Provenance` line appears only for imported files. Local files
omit it (the reader already knows they belong to this pond).

### pond cat

Querying imported data works identically to querying local data:

```bash
# SQL over imported series
pond cat /sources/septic/septicstation.json.series \
  --sql "SELECT timestamp, orenco_RT_Pump1_Amps FROM source ORDER BY timestamp DESC LIMIT 5" \
  --format=table

# Factory configs can reference imported paths
# In reduce.yaml:
#   in_pattern: "oteljson:///sources/septic/septicstation.json"
```

### pond mknod (creating import factories)

Creating an import factory triggers immediate first import:

```bash
$ pond mknod remote /system/run/10-septic --config-path septic-import.yaml

[INIT] Creating remote factory at /system/run/10-septic
[INIT] Config: import mode
   Source backup : s3://septic-dev
   Source path   : /ingest/
   Mount path    : /sources/septic/
[INIT] Opening foreign backup...
   Foreign pond  : 019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00
   Available transactions: 42
[INIT] Creating mount point /sources/septic/
[INIT] Importing partitions...
   Downloading partition 019503a1-8e55-... (38 files, 4.2 MB)
   Adding to local Delta table...
   Linking at /sources/septic/
[OK] Import factory created. Mount point: /sources/septic/
[OK] Factory mode set to 'import'
```

The config YAML:

```yaml
# septic-import.yaml
url: "s3://septic-dev/pond-019503a1-7c44-7f8e-8a3b-5e2d9f4c1a00"
region: "auto"
endpoint: "{{ env(name='R2_ENDPOINT') }}"
access_key: "{{ env(name='R2_KEY') }}"
secret_key: "{{ env(name='R2_SECRET') }}"

import:
  source_path: "/ingest"
  local_path: "/sources/septic"
```

A single pond can have many import factories. Each one is a separate
`mknod` with its own config pointing to a different foreign backup
and mounting at a different path. The self-backup factory
(`/system/run/1-backup`) is independent and has no `import` section.

### Replicas vs Imports

These are distinct concepts that share the remote factory:

| | Replica | Import |
|---|---------|--------|
| **Purpose** | Full copy of the same pond | Subset of a foreign pond |
| **pond_id** | Same as source (preserved) | Different (each pond has its own) |
| **Created by** | `pond init --config=<base64>` | `pond mknod remote ... --config-path import.yaml` |
| **Mode** | `pull` | `import` |
| **Scope** | Entire pond | Specific path(s) |
| **Mount point** | N/A (full mirror) | Named path (e.g., `/sources/septic/`) |
| **Read/Write** | Read-only (replica) | Read-only (imported paths) |
| **Local writes** | Not allowed | Allowed (outside mount points) |
| **Backup** | Source pushes, replica pulls | Local pond backs up everything including imports |

### Error Conditions

The administrative commands report clear errors:

```
# Foreign backup unreachable
$ pond sync
[SYNC] /system/run/10-septic (mode: import)
   [ERR] Cannot reach s3://septic-dev: connection refused
   Skipping import (mount point /sources/septic/ retains last synced data)

# Schema mismatch (foreign pond lacks pond_id column)
$ pond mknod remote /system/run/10-old --config-path old-import.yaml
[ERR] Foreign pond at s3://old-backup is not compatible:
      Missing required column 'pond_id' in OpLog schema.
      Cross-pond import requires both ponds to have the pond_id column.

# Mount path conflict
$ pond mknod remote /system/run/10-dup --config-path dup.yaml
[ERR] Mount path /sources/septic/ is already managed by /system/run/10-septic.
      Each mount path can only be managed by one import factory.

# Write to imported path
$ pond copy host:///tmp/data.csv /sources/septic/extra.csv
[ERR] Cannot write to /sources/septic/: path is an import mount point
      managed by /system/run/10-septic. Imported paths are read-only.
```

### Log Prefixes

Remote and import operations use consistent ASCII log prefixes:

| Prefix | Meaning |
|--------|---------|
| `[EXPORT]` | Pushing data to remote backup |
| `[DOWN]` | Pulling data from remote |
| `[SYNC]` | Manual or automatic sync operation |
| `[INIT]` | Factory initialization / first import |
| `[PKG]` | Packaging data (chunking, hashing) |
| `[OK]` | Operation completed successfully |
| `[ERR]` | Error condition |
| `[WARN]` | Warning (non-fatal) |
| `[SKIP]` | Skipping (already up to date) |
| `[INFO]` | Informational message |

---

## Open Design Questions

This section captures unresolved design questions that must be addressed
before implementation. The cross-pond import design above is structurally
sound, but several policy and interaction decisions remain open.

### Design Principle: Transparency

The strength of this design is that imported data is **transparent**.
Foreign parquet files become real Delta table entries at real pond paths.
The `pond_id` column provides provenance, and `(pond_id, txn_seq)` is
globally unique, but from every command's perspective imported files
are just files.

This means most commands need NO changes. `cat`, `copy`, `mkdir`,
`mknod`, `detect-overlaps`, `set-temporal-bounds`, and `export` all
work on imported data exactly as they do on local data. The `pond_id`
scoping handles data coexistence structurally — there is nothing
special about an imported file from the filesystem's perspective.

Commands that DO have cross-pond-specific behavior are limited to:
display commands that could show provenance (`show`, `describe`,
`list --long`), the import factory workflow itself (`mknod`, `run`,
`sync`), and transaction history (`log`).

### Q1: Read-only policy on import mount points

The document asserts import paths are read-only (see Mount Point
Properties above and Error Conditions), but the justification is
thin. The `pond_id` scoping means local writes to an imported
directory are technically safe: a locally-created file gets the local
`pond_id`, and a foreign file keeps the foreign `pond_id`. They
coexist without collision.

The argument for read-only is preventing confusion when the factory's
next `pull` runs — but the factory only adds/updates foreign files
(scoped by foreign `pond_id`). Local files with local `pond_id`
wouldn't be touched.

Counterargument: allowing local writes alongside imports could create
confusing mixed-provenance directories. Operators might not realize
some files are imported and some are local.

**Decision needed:** Enforce read-only on import directories, or
allow local writes alongside imported data?

### Q2: Provenance display depth

The document shows provenance in several places:

- `pond show` has a "Data Provenance" summary section
- `pond describe` shows `Provenance: pond <uuid>` for imported files
- `pond show` partition listing shows `[imported from <pond_id>]`

But the depth is inconsistent. How much provenance should be visible
by default vs opt-in?

Options:
- **Always show**: provenance appears in `list --long`, `describe`,
  and `show` whenever a file/partition has a foreign `pond_id`
- **Opt-in flag**: `--provenance` or similar flag on `list`/`describe`
- **Only in `pond show`**: provenance is a pond-level concern, not a
  per-file concern; `show` is the right place for the summary

**Decision needed:** What level of provenance display, and where?

### Q3: Import mount points vs hostmount overlays

Hostmount phases 1-3 have landed since this document was written.
The hostmount overlay system uses `OverlayPersistence` to inject
factory nodes at mount paths in the host filesystem. Cross-pond
import "mount points" are a completely different mechanism: real
directories in the pond's Delta table backed by foreign parquet files.

These are fundamentally different:
- **Hostmount overlay**: host filesystem -> TinyFS abstraction via
  `HostmountPersistence` implementing `PersistenceLayer`
- **Import mount**: foreign parquet files added to local Delta table,
  directory entry created at a named path. No new persistence layer;
  data lives in the same `OpLogPersistence` as local data.

Both use the word "mount" but the mechanisms don't overlap. However,
the terminology collision could cause confusion when discussing the
system.

**Decision needed:** Keep "mount point" for import paths, or use a
different term ("import link", "foreign directory", "import path")
to avoid confusion with hostmount?

### Q4: Factory-managed directory semantics

The document says "the remote factory instance owns the mount point"
and that mount points are "managed by the factory." But there is no
general concept of directory ownership by factories today. Other
factory outputs (temporal-reduce, sitegen) write to directories
without "owning" them.

This matters because ownership implies enforcement — the system would
need to know which directories are owned by which factories, and
refuse operations that conflict with ownership.

Options:
- **Introduce ownership**: a metadata flag on directories indicating
  which factory manages them, enforced in the write path
- **Convention only**: document that you shouldn't manually write
  under import paths, but don't enforce it programmatically
- **No ownership needed** (follows from Q1): if local writes are
  allowed alongside imports, ownership is moot

**Decision needed:** Introduce directory ownership as a concept, or
treat it as convention?

### Q5: Credential handling for import factories

Import factories need S3 credentials to access foreign backups. The
current `RemoteConfig` stores credentials in the factory config after
Tera template expansion. The `design-security-and-credentials.md`
document proposes runtime-only template expansion, OS keyring
integration, and age-encrypted configs — but none of that is
implemented yet.

The question is whether cross-pond import is blocked on credential
infrastructure improvements, or whether the current template-in-YAML
approach (where credentials come from environment variables at
`mknod` time via `{{ env(name='KEY') }}`) is acceptable for initial
implementation.

The risk: after `mknod`, the expanded credentials are stored
plaintext in the pond's filesystem. A backup of the importing pond
contains the credentials for the foreign pond's backup. This is the
same risk that exists for self-backup credentials today.

**Decision needed:** Ship cross-pond import with current credential
handling, or gate it on security infrastructure?

### Q6: Restoring a pond that contains imports

`pond init --from-backup` restores a complete pond from a backup
bundle. If that backup contains imported data (foreign `pond_id`
parquet files), the restored pond gets that data — this is correct
and handled by the design (see "Impact on Backup" section).

But the import factory nodes (under `/system/run/` or `/system/etc/`)
are also part of the pond filesystem. After restore:

1. The import factory configs exist (with credentials, source paths)
2. The imported data exists (at the mount paths, with foreign
   `pond_id`)
3. But the import state tracking (which foreign transactions have
   been imported) lives in the control table — is that also backed
   up and restored?

If the control table is restored with the import state intact, the
pond can resume incremental sync. If not, it would need to re-import
from scratch or reconcile.

Also: `pond init --config` (replication) creates a replica from a
base64 config. Can a replica of a pond that has imports continue to
pull those imports? Or does the replica only get a snapshot?

**Decision needed:** What is the restore and replication story for
ponds with imports?

### Q7: Import factory placement — `/system/run/` vs `/system/etc/`

The document places import factories under `/system/run/` (e.g.,
`/system/run/10-septic`). Factories in `/system/run/` auto-execute
on every post-commit transaction with their configured mode.

For self-backup (mode `push`), auto-execution makes sense — you want
every write transaction backed up promptly. But for import (mode
`import`), auto-execution means every local write transaction
triggers a pull from the foreign backup. This could be:

- **Expensive**: pulling from S3 on every local commit
- **Slow**: network latency added to every transaction
- **Unnecessary**: the foreign pond may not have new data

The existing system already handles this distinction:
- `/system/run/`: auto-executing on post-commit (remote push)
- `/system/etc/`: manually triggered (hydrovu, sitegen, column-rename)

Import factories are more like hydrovu (periodic data ingestion) than
backup (continuous replication). A `pond sync` or `pond run` invocation
seems more appropriate than auto-pull.

However, if imports go in `/system/etc/`, they won't be found by
`pond sync` (which only scans `/system/run/`). The sync command would
need to be updated to also scan `/system/etc/` for import-mode
factories, or a different mechanism would be needed.

**Decision needed:** Import factories in `/system/run/`
(auto-pull on every commit) or `/system/etc/` (manual sync only)?
If `/system/etc/`, how does `pond sync` find them?

---

## Decisions

The following decisions were made during Phase 1 implementation
(2026-03-17) and apply to Phase 2 and beyond.

### Q1: Read-only on import paths -> Convention only

No enforcement. The `pond_id` column makes coexistence structurally
sound. Local writes alongside imported data are technically safe.
Enforcement can be added later if mixed-provenance directories prove
confusing in practice.

### Q2: Provenance display -> Always show

Provenance appears in `describe`, `show`, and `list --long` whenever
a file or partition has a foreign `pond_id`. Local files omit the
provenance line (no noise). No opt-in flag needed.

### Q3: Terminology -> "import path", config field `local_path`

Use "import path" instead of "mount point" to avoid collision with
hostmount overlays. The config field is `import.local_path` (not
`local_path`):

```yaml
import:
  source_path: "/ingest"
  local_path: "/sources/septic"
```

### Q4: Directory ownership -> Convention only

No factory ownership metadata. The import factory creates a directory
entry pointing at the imported partition's root, but does not "own"
it in an enforced sense. Follows from Q1.

### Q5: Credentials -> Ship with current handling

Current template-in-YAML approach (env vars expanded at `mknod` time)
is acceptable. Same risk as self-backup credentials. Security
improvements apply uniformly to all factory configs later.

### Q6: Restore with imports -> Deferred

The control table is backed up with the pond (it's a Delta table in
the pond directory), so restore gets import state. Edge cases
(replicas continuing to pull, reconciliation) deferred until import
is working end-to-end.

### Q7: Import factory placement -> Operator's choice

No default placement. The operator chooses:
- `/system/run/` for auto-pull on every commit
- `/system/etc/` for manual-only (`pond run` / `pond sync`)

Docs note the tradeoff. No changes to `pond sync` scanning logic.

---

## Implementation Status

Phase 1 (Schema Foundation) implemented 2026-03-17.
Phase 2 (Cross-Pond Import) implemented 2026-03-19.

Key files changed:
- `crates/tlogfs/src/schema.rs` -- `pond_id` field on `OplogEntry`
- `crates/tlogfs/src/persistence.rs` -- `pond_id` threading and stamping
- `crates/tlogfs/src/txn_metadata.rs` -- `pond_id` in commit metadata
- `crates/remote/src/factory.rs` -- `ImportConfig`, `initialize_remote`,
  `execute_import`, mode detection
- `crates/remote/src/chunked_async_reader.rs` -- parquet `AsyncFileReader`
  over chunked backup storage (new file)
- `crates/remote/src/schema.rs` -- `pond_id` in `ChunkedFileRecord`,
  updated `bundle_id` format
- `crates/tinyfs/src/wd.rs` -- `insert_node` for foreign FileIDs
- `crates/utilities/src/chunked_files.rs` -- `pond_id` in Arrow schema
- `testsuite/tests/530-cross-pond-import-minio.sh` -- end-to-end test

---

## Prerequisites Already Met

The following changes were proposed alongside the original cross-pond
import design and have since been implemented:

- `pond log` replaces `pond control recent/detail/incomplete` — ✅
- `pond sync` replaces `pond control sync`, iterates all factories — ✅
- `pond config` replaces `pond control show-config/set-config` — ✅
- `pond run` accepts short factory names (resolves via
  `/system/run/` then `/system/etc/`) — ✅
- `list-files` remote subcommand removed (absorbed by `show`) — ✅
- `pond control` retained as hidden backward-compat alias — ✅

These are documented in `cli-command-structure.md`. The cross-pond
import implementation can build directly on the current CLI structure.