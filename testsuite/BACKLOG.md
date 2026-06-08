# Test Backlog

> Priority: P0 (blocking) → P1 (high) → P2 (medium) → P3 (low)
> Status: 🔴 Open | 🟡 In Progress | 🟢 Done | ⚪ Deferred

---

## 🔴 Open Items

### P3-001: Document factory configuration examples
- **Type**: DOCS
- **Description**: Factory YAML configs need more complete examples
- **Next Step**: Run factory tests to discover actual syntax

---

## 🟢 Done

### ✅ D8: Fix `host+table:///x.parquet` "not queryable"
- **Completed**: 2026-06-07
- **Type**: BUG FIX
- **Symptom (pre-fix)**: `pond cat host+table:///file.parquet` (and
  `host+series://`) errored `File '...' is not queryable (type:
  FilePhysicalVersion)`.
- **Root cause**: `host+table://` parses as builtin entry-type `table`, so
  `create_table_provider` routed to the tinyfs builtin path.  That resolves
  the host file through the filesystem, where a raw host file has entry
  type `FilePhysicalVersion` (not a table/series), so `as_queryable()`
  rejected it -- even though the bytes are valid Parquet.
- **Fix**: in `Provider::create_table_provider`, when `url.is_host()` and
  `entry_type` is `table`/`series`, read the host file directly as Parquet
  (`open_host_url` + `ParquetRecordBatchReaderBuilder` -> `MemTable`)
  instead of routing through tinyfs.  A non-Parquet file now yields a clear
  "not a valid Parquet file" error.
- **Files**: crates/provider/src/provider_api.rs
  (`create_host_parquet_table_provider` + routing).
- **Tests** (crates/provider/src/provider_api.rs):
  `test_host_table_reads_parquet_directly`,
  `test_host_table_rejects_non_parquet`.  Verified end-to-end via the
  `pond` binary (`host+table://` and `host+series://` query cleanly;
  `--format=table` and `--sql` both work).

### ✅ D7b: Fix P2-VERIFY-BOOTSTRAP-DRIFT (replicate the pond_init bundle)
- **Completed**: 2026-06-06
- **Type**: BUG / DESIGN GAP (discovered 2026-06-05 while wiring D6.1 `pond verify`)
- **Symptom (pre-fix)**: a replica bootstrapped from a remote reported a
  root-partition mismatch on `pond verify` even with no local writes.
- **True root cause** (confirmed at the Delta-log level): the producer's
  `pond_init` root-directory row lands at Delta **version 1** (version 0
  is the `CREATE TABLE`), but `Ship::create_pond` hard-coded
  `data_delta_version=0` (+ empty checksums) for seq=1.  `Remote::push`'s
  `data_delta_version==0` bootstrap-skip then never replicated the root
  row, so the replica was missing the producer's `root-dir` v1 leaf that
  the producer's recorded checksum includes -> root-partition Merkle
  differs -> mismatch.  (The earlier "create_replica synthesizes its own
  init rows" theory was wrong: `create_pond_for_restoration` makes an
  EMPTY table; the replica is simply missing the unreplicated row.)
- **Fix** (Option 1 - replicate the pond_init bundle):
  - `Ship::create_pond` records seq=1's REAL `data_delta_version`
    (`table.version()` after root init) and REAL partition checksums.
  - `Remote::push`'s `data_delta_version==0` skip stays only as a
    legacy/defensive guard; current ponds push seq=1 as a normal bundle.
  - `Remote::bootstrap_consumer` and `pond pull` seed `last_pulled_seq`
    to `oldest_available_seq - 1` (so new producers -> seed 0, pull seq=1+;
    legacy producers -> seed 1, unchanged) instead of hard-coding "1".
- **Result**: replicas are byte-identical to producers; `pond verify`
  passes symmetrically (producer AND replica).  Verified: remote carries
  txn_seq 1..N (pond_init replicated); producer verify `[OK]`.
- **Files**: crates/steward/src/ship.rs (create_pond);
  crates/sync-remote/src/remote.rs (push skip comment + bootstrap_consumer
  seed); crates/cmd/src/commands/pull.rs (first-pull seed).
- **Tests**: crates/steward/tests/remote_adapter_test.rs::
  `ship_replica_verify_matches_after_bootstrap` (the regression) and
  `ship_remote_push_replicates_pond_init` (rewritten from the old
  clean-skip test).

### ✅ D7: Fix P2-PRODUCER-COMPACT-BUNDLES (duckpond producers emit Compact bundles)
- **Completed**: 2026-06-06
- **Type**: FEATURE / DESIGN GAP (discovered 2026-06-05 while wiring D6.4
  `pond restart-from-compact`)
- **Symptom (pre-fix)**: `pond restart-from-compact <mirror>` always
  reported "no compact bundle to restart from" for a pure duckpond
  mirror, even after many writes + `pond maintain --compact`.
- **Root cause**: `pond maintain --compact` ran a best-effort Delta
  optimize that was NOT recorded in the control table, so
  `Remote::push` (which iterates `DataCommitted` records) never saw a
  `CommitKind::Compact` -- producers only ever pushed `Write` bundles.
  (Worse, the unrecorded optimize commit carried no `pond_txn` metadata,
  so reopening a pond after the old `maintain --compact` would reset
  `OpLogPersistence::last_txn_seq` to 0.)
- **Fix**: new `Ship::compact()` runs a RECORDED, pushable compaction
  (Begin / `DataCommitted(commit_kind=Compact)`), analogous to
  `sync_steward::Steward::compact`:
  1. allocate seq + write lock + Begin
  2. snapshot pre checksums
  3. Delta `optimize(Compact)` scoped to own `pond_id` partitions, with
     the compaction's `txn_seq` stamped into the commit's `pond_txn`
     metadata (so reopen recovers `last_txn_seq`)
  4. no-op -> Completed, `had_data=false`
  5. real -> snapshot post + assert checksum invariance (content
     unchanged) -> `DataCommitted(Compact)` at the new Delta version
  `pond maintain --compact` now routes data-table compaction through
  this path; checkpoint/vacuum stay best-effort; the control table
  (never pushed) keeps its best-effort optimize.
- **Result**: `Remote::push` emits a Compact bundle; `restart-from-compact`
  + `Remote::maintain` retention now close the mirror recovery loop
  end-to-end from duckpond alone.
- **Files**: crates/steward/src/ship.rs (`Ship::compact`, `maintain`
  routing, `assert_compaction_invariant`); crates/steward/src/maintenance.rs
  (`compact_pond_partitions`, `CompactStats`); crates/steward/src/control_table.rs
  (`record_compact_committed`); crates/cmd/src/commands/maintain.rs.
- **Tests** (`crates/steward/tests/remote_adapter_test.rs`):
  `ship_compact_records_pushable_compact_transaction`,
  `ship_compact_bundle_drives_restart_from_compact` (full mirror
  recovery loop), `ship_compact_survives_reopen`.

### ✅ D5.9: Fix P1-BUG-LF-REPLICATION (large-file blobs replicate over push/pull)
- **Completed**: 2026-06-03
- **Type**: BUG FIX (discovered via D5.8.5 testsuite revival)
- **Symptom (pre-fix)**: After `pond push origin` + `pond pull` to a fresh
  pond, files >64KiB (i.e., those that took the externalization path on
  the source) could be `pond list`-ed (correct metadata, size, blake3)
  but `pond cat` failed hard on read with exit code 1 and the error:
  `Error: Transaction aborted: Failed to open file for reading: Other:
  Large file not found: <blake3> at path _large_files/blake3=<hash>`.
  The receive-side `pond pull` was silent about the missing blob (Delta
  apply succeeded; only the read-time lookup at
  `crates/tlogfs/src/persistence.rs:2089` fell through to
  `TLogFSError::LargeFileNotFound`).  Initial notes in commit
  `fcbbdbef` (D5.8.5) and earlier checkpoints described the symptom as
  "returns zero bytes" because the smoking-gun shell test piped
  `pond cat ... 2>/dev/null | md5sum`, which hid both the stderr
  message and the non-zero exit code -- only the empty-string md5
  (`d41d8cd9...`) on stdout was visible to the test.  Re-verified
  2026-06-04 with a direct repro against the parent of `d5bf4b68`.
- **Root cause**: `Steward::actions_at_version` enumerated only Delta `Add`
  paths of the form `pond_id=<u>/part_id=<v>/part-N-N.parquet` -- the
  partition parquet that contains the OpLog *row* describing the file.
  For a large file, that row carries a `blake3` reference into
  `_large_files/blake3=<hash>.parquet`, which lives outside the partition
  tree and was therefore never enumerated, read, chunked, or sent in the
  bundle.
- **Fix**: Added an `external_blobs_referenced_by(add_path, add_bytes)`
  hook to the `RemoteSteward` trait with an empty default; duckpond's
  adapter overrides it to scan each partition parquet for rows where
  `content IS NULL` AND the entry is a file, then resolves each `blake3`
  to the on-disk hierarchical/flat path under `_large_files/`.  In
  `Remote::push`, blob paths are deduplicated across adds (a blob shared
  by two partition parquets is sent once) and emitted as additional
  `DataAdd` rows that reuse the existing schema.  In `apply_pulled_bundle`,
  incoming `adds` are split by `_large_files/` path prefix: partition
  parquets follow the existing Delta-write path, blobs are written to
  disk only (no Delta `Add` action, no `parse_pond_part_path` check,
  since they have no `pond_id` segment).  The trait's default
  `validate_local_data_path` was also broadened to allow the
  `_large_files/` prefix (content-addressed, cross-pond-safe by design).
- **Files changed**:
    - `crates/sync-remote/src/steward_trait.rs` -- new trait method,
      broadened path validator default.
    - `crates/sync-remote/src/remote.rs` -- push-side dedup loop emits
      DataAdd rows for partition parquets and external blobs.
    - `crates/steward/src/remote_adapter.rs` -- duckpond override scans
      parquet for blake3 refs; `apply_pulled_bundle` splits adds by
      `_large_files/` prefix.
    - `crates/steward/Cargo.toml` -- added `bytes` and `parquet` deps.
- **Test coverage added**:
    - `crates/cmd/tests/test_remote_cli.rs::pond_remote_push_pull_large_file_roundtrip`
      -- focused Rust integration test that pushes an 80 KiB file with a
      non-trivial byte pattern, pulls into a fresh pond, and verifies
      `read_file_path_to_vec` returns matching bytes.
    - `testsuite/tests/530-cross-pond-import-minio.sh` -- reinstated the
      80 KiB `big.bin` case in cross-pond import; verifies md5 round-trip
      via the foreign mount.
    - `testsuite/tests/510-synth-logs-replication-cycle.sh` -- bumped
      `INITIAL_ROWS` from 500 to 6000 (~120 KB) so the synth-logs
      replication cycle also exercises large-file replication.

### ✅ D5.8.9: Revive `531-recursive-cross-pond-import.sh` + add 3-deep Rust integration test
- **Completed**: 2026-06-03
- **Type**: REVIVAL (one shell script) + new Rust integration test
- **Description**: Pinned the **non-transitive cross-pond replication
  invariant** in both the testsuite and the Rust unit suite.  The
  legacy `DISABLED-D4` 531 stub tested the deleted `remote` factory's
  flat-vs-`/data/**` `source_path` filter — that surface no longer
  exists in D5.7b (cross-pond mounts the whole foreign pond root).
  Rewrote as a 3-deep A→B→C chain instead, per
  `docs/d5.8-resume.md` step 7 / § 291.
- **Test shape (shell, 14 checks)**: Three ponds against two MinIO
  buckets.  A writes `/data/a.txt`, pushes to bucket-A.  B mounts A
  at `/imports/A`, pulls, writes own `/data/b.txt`, pushes to
  bucket-B.  C mounts B at `/imports/B`, pulls.  Asserts:
    - C reads B's local content via `/imports/B/data/b.txt` (the
      replicated B-owned rows resolve correctly).
    - C's `/imports/B/imports/` IS present as a B-owned directory
      entry (B created it via `create_dir_all` when materializing
      its own `/imports/A` mount).
    - But **listing** `/imports/B/imports/` fails on C, and
      `pond cat /imports/B/imports/A/data/a.txt` fails — A's content
      is **not** transitively replicated.
    - A's pond_id tail never surfaces anywhere in C's `pond list /`.
    - A and B remain fully usable (B still reads A through its own
      mount after C pulled).
- **Test shape (Rust, `cross_pond_3deep_does_not_re_replicate_foreign_mount`)**:
  Same A→B→C topology against two `file://` remotes (no MinIO).
  Drives `init_command`, `add_remote_command`, `pull_command`,
  `push_command` directly via the library entrypoints.  Asserts:
    - 2-deep works (B reads A via `/imports/A/a.txt`).
    - 3-deep is blocked (C cannot read `/imports/B/imports/A/a.txt`).
    - `/imports/B` carries B's pond_id; `/imports/B/imports`
      carries B's pond_id (not A's).
    - The `A` child entry under `/imports/B/imports` is either
      absent, unlookable, or — if somehow present — does NOT carry
      A's pond_id (which would prove the push filter is broken).
    - B still reads A through its mount after C's pull.
- **Why this matters**: `steward::remote_adapter::actions_at_version`
  filters outbound Add/Remove actions to rows where
  `partition_values["pond_id"] == local pond's UUID`
  (`remote_adapter.rs:230-241`).  This is a deliberate contract: a
  pond pushes only its OWN rows, not foreign rows it has imported.
  Both new tests pin this — if anyone weakens the filter without a
  cross-pond-transitivity design change, both break loudly.
- **Observed corner**: listing `/imports/B/imports/` on C returns
  `"Partition not found: part_id=00000000-...-000000000000, node_id=00000000-...-000000000000"`.
  The directory entry exists but its body partition is empty
  (the only would-be child was the foreign-pond mount-entry row
  that got filtered).  Cosmetic — the read correctly fails; the
  error message could be friendlier in a future cleanup.
- **Pre-existing tests still green** (full 15-test sweep): 500, 501,
  510, 520, 521, 522, 523, 530, 531, 532, 533, 540, 541, 542, 550.
- **Workspace check**: `cargo fmt --all --check`, `cargo clippy
  --workspace --all-features -- -D warnings`, `cargo test
  --workspace` all clean.

### ✅ D5.8.8: Revive `550-recursive-sitegen.sh` (the renamed sitegen-over-mount test)
- **Completed**: 2026-06-03
- **Type**: REVIVAL (one script)
- **Description**: Replaced the `DISABLED-D4` stub (which drove the
  deleted `remote` factory with `import:` YAML) with a D5.7b version
  that exercises sitegen's `subsites:` directive over a cross-pond
  import: 22 checks.
- **Test shape**: Pond A creates synthetic timeseries (`dynamic-dir`
  + `synthetic-timeseries`), a `temporal-reduce` wrapper, page
  templates under `/site/`, and a `sitegen` factory at `/etc/site`.
  Producer's standalone build succeeds (3 sanity checks).  Pond A
  pushes via `pond backup add` + `pond push`.  Pond B attaches
  with `pond remote add upstream URL /sources/producer` + `pond pull`,
  then installs a top-level `sitegen` at `/system/etc/90-sitegen`
  whose YAML declares the producer as a `subsites:` entry pointing at
  `/sources/producer` with `config: /etc/site` and
  `base_url: /producer/`.
- **Verifies (in addition to the cross-pond plumbing)**:
    - The subsite reads its config from
      `/sources/producer/etc/site` via the mount (the consumer
      doesn't have a local copy; sitegen reads through the
      cross-pond mount at build time).
    - The subsite's `exports:` declaration pulls factory output
      (`/reduced/single_param/*/*.series`) from the producer's
      dynamic-dir + temporal-reduce chain through the cross-pond
      mount — confirms factories ARE executable when triggered
      explicitly from the consumer side (even though they are
      not auto-run, per invariant 2 in 533).
    - Output structure: top-level shared assets (`style.css`,
      `chart.js`, `overlay.js`) at root; per-site `theme.css` at
      both root and `/producer/`; subsite HTML pages
      (`producer/index.html`, `producer/temperature.html`,
      `producer/pressure.html`) and data exports
      (`producer/data/single_param/.../*.parquet`).
    - Theme isolation: top has consumer's `#1a365d`, subsite has
      producer's `#2d5016` -- subsite config really was read from
      the producer side, not the consumer's defaults.
- **No collisions, no new bugs**.  Test runs cleanly compose-mode in
  ~70 s (sitegen build + duckdb-wasm vendor copy dominate runtime).
- **Pre-existing tests still green**: 500, 501, 510, 520, 521, 522,
  523, 530, 532, 533, 540, 541, 542 (full sweep after this batch).

### ✅ D5.8.7: Revive the cross-pond import watermark group (540 + 541 + 542) and renumber 540-recursive-sitegen → 550
- **Completed**: 2026-06-03
- **Type**: REVIVAL (three scripts + one rename)
- **Description**: Replaced the three `DISABLED-D4` stubs that drove the
  deleted `remote` factory (`pond mknod remote /system/run/...` +
  `pond run ... pull` + `Watermark: N` log grep) with shell tests that
  exercise the D5.7b watermark surface: the persisted
  `last_pulled_seq:<url>` key in the steward control table, surfaced via
  `pond remote list`'s `LAST_PULLED_SEQ` column, and the
  `[OK] pull NAME: applied N bundle(s) from URL` log line emitted by
  `crates/cmd/src/commands/pull.rs:114`.
- **540 (incremental watermark — bandwidth-bug regression guard)**:
  10 checks.  Three pulls: initial (applied > 0, watermark advances
  above bootstrap), no-op (`applied 0 bundle(s)`, watermark unchanged
  — the regression guard against the production "re-walk 679 txns
  every tick" bug), and one-new-commit (applied >= 1, watermark
  advances by the delta, new file visible).  Captures
  `last_pulled_seq` between pulls and asserts monotonicity + zero
  reset to the bootstrap floor.
- **541 (watermark advances on ANY foreign txn shape)**:
  12 checks.  After initial pull, producer issues two mkdir-only
  commits (no file blobs) and a single file-content commit.  Asserts
  each produces a bundle, each advances the watermark by exactly one,
  the no-op pull between them still reports `applied 0 bundle(s)`,
  and the foreign directories are visible under the mount even
  though they carry no data payload.  Recast from the legacy
  filter-induced-skip bug (filter no longer exists in D5.7b) into a
  generic "bundle shape doesn't affect watermark persistence" guard.
- **542 (fresh-replica bootstrap reconstruction)**:
  11 checks.  Original consumer pulls and captures W_initial; pond
  directory is `rm -rf`'d entirely (the D5.7b analog of "terraform
  destroy + recreate" now that `pond init --from-backup` is removed,
  see `crates/cmd/src/commands/init.rs:15`); fresh consumer with same
  URL + mount path re-attaches and pulls.  Asserts: pre-pull
  `LAST_PULLED_SEQ` is "-" (unset); first pull reaches the SAME
  watermark and SAME applied count as the original; second pull is a
  no-op (regression guard that the rebuilt control table genuinely
  persisted the reconstructed watermark).
- **Numbering collision fix**: `git mv 540-recursive-sitegen.sh →
  550-recursive-sitegen.sh` (the sitegen-over-mount test was
  numerically colliding with the new watermark cluster; `run-test.sh`
  resolves numeric prefixes with `find -name 'N-*.sh' | head -1`,
  which would otherwise have aliased to the first alphabetical
  match).  550 still exits SKIP and is the next revival target.
- **Watermark values observed (for documentation)**: in a 3-file
  producer + 0-extra-history setup, pull #1 → watermark 4–7 (varies
  by mkdir count), pull #2 (no-op) → unchanged, pull #3 (one new
  commit) → watermark +1.  These numbers correspond directly to
  producer-side Delta versions; the bootstrap seed of "1" (`pull.rs:
  140-151`) skips the producer's pond_init txn at version 1.
- **Pre-existing tests still green**: 500, 501, 510, 520, 521, 522,
  523, 530, 532, 533 (full sweep run after this batch).

### ✅ D5.8.6: Revive `532-cross-pond-path-boundaries.sh` and `533-cross-pond-factory-resolution.sh`
- **Completed**: 2026-06-03
- **Type**: REVIVAL (both scripts)
- **Description**: Replaced the `DISABLED-D4` stubs (which mknod'd the
  long-removed `remote` factory with `import:` YAML blocks) with shell
  CLI versions of the two cross-pond invariants from `docs/d5.8-resume.md`
  § 3 that already had Rust integration coverage in
  `crates/cmd/tests/test_remote_cli.rs`.
- **532 (invariant 1: foreign mount is strictly read-only)**: 15 checks.
  After a cross-pond pull, every writing CLI verb that targets a path
  inside `/imports/A/*` must exit non-zero with a "read-only"-flavored
  error.  Verbs exercised: `pond copy host://... /imports/A/x`,
  `pond mkdir /imports/A/newdir`, `pond mkdir /imports/A/data/newsub`,
  `pond mknod sql-derived-table /imports/A/factory ...`.  Coexistence:
  writes to local (non-mount) paths still succeed.  Path-namespace
  isolation: B's local `/data/foo.txt` and `/imports/A/data/foo.txt`
  coexist with distinct content.
- **533 (invariant 2: foreign factories are NOT auto-run on consumer)**:
  12 checks.  Producer installs both kinds of factory entries
  (`/system/run/scratch` via `pond mknod sql-derived-table` and
  `/sys/remotes/origin` via `pond backup add`) and pushes.  Consumer
  cross-pond-imports, then performs a local write — bucket-A object
  count stays unchanged (both `discover_post_commit_factories` pond_id
  filter at `crates/steward/src/guard.rs:699` and the root-anchored
  `/sys/remotes/*` path pattern at `discover_sys_remotes` correctly
  skip the foreign entries).  Consumer then installs its own
  `/sys/remotes/origin-b` → bucket-B; next local write grows bucket-B,
  confirming local factories DO still auto-run.  Foreign entries
  remain visible by explicit traversal under the mount, decorated
  with A's pond_id tail — proof that filtering is per-entry skip at
  scan time, not entry deletion.
- **Findings during revival**:
    - There are TWO post-commit auto-exec mechanisms, with two distinct
      filter strategies for invariant 2:
        (a) `/sys/remotes/*` (D4+ `pond backup add` entries) is scanned
            with a root-anchored glob, so foreign mount entries simply
            do not match the pattern.
        (b) `/system/run/*` (legacy/manual `pond mknod` entries) is
            scanned with a glob that *can* cross mount boundaries, so
            an explicit `node.id().pond_id() != local_pond_id` filter
            skips foreign matches.
      The shell test exercises both by installing one of each on the
      producer side.
    - `pond list` does NOT truncate path components.  `/imports/A/sys`
      really is the directory name (the steward uses `/sys/` for
      its metadata; only the legacy/dynamic factory entries live under
      `/system/`).  These are two separate trees and both need to be
      consulted for cross-pond invariant testing.
    - `pond mkdir -p` is supported (long form `--parents`).

### ✅ D5.8.5: Revive `530-cross-pond-import-minio.sh` against the D5.7b cross-pond CLI
- **Completed**: 2026-06-03
- **Type**: REVIVAL (+ exposed P1-BUG-LF-REPLICATION above)
- **Description**: Replaced the `DISABLED-D4` stub — which mknod'd the
  long-removed `remote` factory with a YAML carrying an `import:` block —
  with a 13-check end-to-end test on top of the D5.7b CLI:
    1. Pond A: `init`, write a small tree under `/data` (csv, second csv,
       nested file), `pond backup add origin s3://BUCKET`, `pond push origin`.
    2. Pond B: `init` (distinct `pond_id`), seed a local `/local/note.txt`,
       `pond remote add upstream s3://BUCKET /imports/A`, `pond pull upstream`.
    3. Verify: every imported file is byte-faithful via the mount;
       Pond B's local data is unaffected; the `[<pond_id-tail>]` tag on
       the `/imports/A` entry shows Pond A's pond_id (provenance);
       a second pull is idempotent.
- **Findings during revival**:
    - `pond list PATH` returns the entry itself; `pond list PATH/` (trailing
      slash) returns its children.  Tests must use the trailing slash form
      to list directory contents.
    - `pond list` decorates every entry with `[<last-12-hex-of-pond_id>]`,
      which is the user-visible signal for cross-pond provenance.  No need
      to query the control table for this -- it shows up on the mount root.
    - `pond config` does not surface the foreign pond_id of an attached
      cross-pond remote in its human output (only the local pond_id).
      If a stable machine-readable view is wanted later, add it there.
    - Large-file replication is broken (see P1-BUG-LF-REPLICATION).  The
      80KiB `big.bin` case originally written into 530 was the smoking gun
      and has been removed from the green path; restore once the bug is
      fixed.
- **Regression**: 500 / 501 / 510 / 520 / 521 / 522 / 523 all clean.

### ✅ D5.8.4: Rewrite `crates/cmd/scripts/duckpond-emergency` for current Delta schema + revive `522-emergency-recovery-tool.sh`
- **Completed**: 2026-06-03
- **Type**: BUG (out-of-date disaster-recovery tool) + REVIVAL
- **Root Cause**: `duckpond-emergency` (v1.0.0) was written for the long-removed
  chunked-parquet remote schema (`bundle_id`, `path`, `root_hash`, `total_size`,
  `pond_txn_id`, `chunk_*` packed into a single parquet per bundle).  After
  D4.5 + D5.x replaced that with a Delta-Lake-native backup table
  (`partition_kind` in {manifest, checksum, data}, with explicit
  `file_path`/`file_action`/`file_blake3`/`chunk_data`/`chunk_blake3` columns
  and source-pond storage paths), the legacy script's SQL referenced
  non-existent columns and would have failed on every command.
- **Fix**: Rewrote the script (now v2.0.0) for the current schema:
  - `list` / `info` / `extract` / `verify` / `export-all` user-facing surface
    preserved.
  - All SQL targets `read_parquet('s3://BUCKET/partition_kind=*/*.parquet',
    hive_partitioning=true)` (works with configured `s3_*` settings, unlike
    `delta_scan()` which forces EC2 IMDS).
  - "Current snapshot" semantics implemented via a
    `ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY txn_seq DESC)` CTE
    that picks the latest `add` per `file_path` (so removed files are
    excluded from list/extract/verify).
  - BLOB extraction via `hex(chunk_data)` + `xxd -r -p` (portable across
    DuckDB versions, same trick as 521).
- **Tests**: `522-emergency-recovery-tool.sh` (21/21 passes) covers info /
  list / extract / verify / export-all + a BLAKE3 round-trip and
  parquet-readability spot check.  500 / 501 / 510 / 520 / 521 / 523
  regression-clean.
- **Notes for future work**: The remote backup only replicates the
  source-pond's parquet files (`pond_id=<u>/part_id=<v>/part-*.parquet`)
  and `_large_files/<hash>` blobs.  The source pond's own `_delta_log/`
  is *not* replicated (it's regenerable from the parquet files); the
  S3 bucket has its own separate `_delta_log/` describing the remote
  table itself.  Document this somewhere user-facing if/when we write
  a public disaster-recovery guide.

### ✅ D5.8.3: Revive `520-remote-show-verification.sh` + `521-external-tool-verification.sh`
- **Completed**: 2026-06-04
- **Type**: REVIVAL
- **Description**: Replaced the disabled (`DISABLED-D4`) scripts — both of which
  drove the removed `pond run /system/run/10-remote show` flow against a long-gone
  chunked-parquet schema (`bundle_id`, `path`, `root_hash`, `total_size`,
  `pond_txn_id`) — with D5.7b-shaped tests built on top of `pond backup add`,
  `pond push`, `pond log`, `pond config`, `mc ls`, and DuckDB over the current
  Delta-Lake-native backup schema (`partition_kind`, `txn_seq`, `file_path`,
  `file_blake3`, `chunk_data`, …).
  - **520**: 14/14 — verifies pond setup, push, transaction log shape, pond id
    config, `mc ls` of bucket layout (`_delta_log/`, `partition_kind=manifest/`,
    `partition_kind=checksum/`, `partition_kind=data/`), and DuckDB queries over
    `read_parquet('s3://.../partition_kind=*/*.parquet', hive_partitioning=true)`
    against partition_kind/file_path/manifest. Confirms the storage-level layout
    — distinct source `pond_id`, manifest `txn_seq` count, data partition with
    `pond_id=<uuid>/part_id=<uuid>/part-*.parquet` references — i.e. the backup
    replicates the *underlying storage*, not just the logical filesystem.
  - **521**: 5/5 — end-to-end "extract using only standard tools": push a tiny
    `hello.txt`, DuckDB picks the single-chunk Add row, `hex(chunk_data)` →
    `xxd -r -p` extracts bytes verbatim, `b3sum` of the extracted bytes equals
    both the stored `chunk_blake3` and `file_blake3` (single-chunk identity).
- **Notes**:
  - DuckDB's `delta_scan()` ignores configured `s3_*` settings and tries EC2
    IMDS (`169.254.169.254`); switched to `read_parquet(..., hive_partitioning
    =true)` over `s3://BUCKET/partition_kind=*/*.parquet`. For freshly-pushed
    tables (no vacuum / no removes) the row set is equivalent to `delta_scan`.
  - DuckDB binary BLOB extraction: `hex(...)` + `xxd -r -p` is the portable
    cross-version path (avoids `COPY (...) TO ... (FORMAT BLOB)` quirks).
- **Tests**: 520 (14/14), 521 (5/5); 500/501/510/523 regression-clean.

### ✅ D5.8.2: Revive `523-emergency-erase-and-auto-bucket.sh`
- **Completed**: 2026-06-03
- **Type**: REVIVAL
- **Description**: Rewrote the disabled (`DISABLED-D4`) script on top of the
  D5.7b backup CLI (`pond backup add`, `pond push`, `pond emergency
  erase-bucket`) after the legacy `remote` factory + `pond apply`
  configuration flow was removed.  Test now exercises four phases:
  Pond1 attach + push (auto-inits bucket Delta table with Pond1 id) →
  `pond emergency erase-bucket --dangerous` (with safety-flag refusal
  check) → Pond2 reattach (empty bucket auto-inits with Pond2 id) +
  push → Pond3 attach to occupied bucket must be refused with a
  `does not match` / foreign-pond error.
- **Test**: `523-emergency-erase-and-auto-bucket.sh` (11/11 passes);
  500, 501, 510 regression-clean.

### ✅ D5.8.1: Revive `510-synth-logs-replication-cycle.sh` + fix post-commit factory replication
- **Completed**: 2026-06-02
- **Type**: BUG + REVIVAL
- **Root Cause**: `StewardTransactionGuard::execute_post_commit_factory` opened a
  fresh `OpLogPersistence::begin_write`, wrote a real parquet, and committed it,
  but discarded the returned Delta version with `_ =` and never wrote any
  `Begin`/`DataCommitted`/`Completed` rows on the control table for that
  factory-allocated `txn_seq`. Replication's `Remote::push` then saw
  `NoSuchCommit(seq)` for the factory's commit and silently skipped the bundle
  — so logfile-ingest (and any other post-commit factory) data never replicated.
- **Fix**: `crates/steward/src/guard.rs` — read `last_txn_seq + 1` from the
  freshly-opened persistence (handles multi-factory case correctly), record
  `Begin` before `begin_write`, and after `factory_tx.commit()` record
  `DataCommitted` (with partition checksums snapshot) + `Completed`, or
  `Completed` for write-no-op, or `Failed` on commit error.
- **Test**: `510-synth-logs-replication-cycle.sh` (10/10 passes); 030-033
  logfile-ingest tests and 500 baseline replication continue to pass.

### ✅ D-007: Improved `pond list /` behavior (P2-002)
- **Completed**: 2026-02-03
- **Resolution**: `/` now lists root entries, trailing slash lists directory contents, updated cli-reference.md
- **Test**: `003-list-patterns.sh` - comprehensive pattern behavior verification

### ✅ D-006: Add `--sql` alias for `--query`
- **Completed**: 2026-02-03
- **Resolution**: Added `--sql` as primary flag with `--query` as visible alias, updated cli-reference.md

### ✅ D-001: Document glob patterns for `pond list`
- **Completed**: 2026-02-02
- **Resolution**: Added to cli-reference.md

### ✅ D-002: Document `host://` prefix for `pond copy`  
- **Completed**: 2026-02-02
- **Resolution**: Added to cli-reference.md

### ✅ D-003: Fix `--sql` → `--query` in docs
- **Completed**: 2026-02-02
- **Resolution**: Updated cli-reference.md and tests

### ✅ D-004: `--format=table` corrupts CSV files (P0 BUG)
- **Completed**: 2026-02-02
- **Root Cause**: `--format=table` should only accept Parquet files, not convert
- **Resolution**: Added PAR1 magic byte validation in copy.rs, helpful error message

### ✅ D-005: Table name inconsistency (`series` vs `source`)
- **Completed**: 2026-02-02
- **Resolution**: Changed code to use `source` to match documentation

---

## ⚪ Deferred

(none yet)

---

## Test Queue

Tests waiting to be run:

1. [ ] `400-dual-pond-observability.sh` - Multi-pond with log ingestion
2. [ ] `500-s3-replication-minio.sh` - S3 backup/restore cycle
3. [ ] `510-synth-logs-replication-cycle.sh` - **NEW** Full pipeline: synth-logs → logfile-ingest → S3 → replicate → verify (multi-round)
4. [ ] Factory node creation (`sql-derived-table`)
5. [ ] Control table queries
6. [ ] Time-series operations

---

## Quick Commands

```bash
# Run next test
cd /Volumes/sourcecode/src/duckpond/tests
./run-test.sh --save-result tests/XXX.sh

# Interactive exploration
./run-test.sh --interactive

# With S3/MinIO
docker-compose up -d minio
docker-compose run --rm duckpond
```
