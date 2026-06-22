# D4 Resumption: Replace `crates/remote` with `sync_remote::Remote`

**Scope of this document**: complete handoff so a fresh session can pick up D4 work without re-deriving any context.  Read top-to-bottom.

---

## 1. Where we are right now

### Branch + commits

- **Branch**: `jmacd/52`
- **Last commit**: `85eba5c4` — *D2 substantive: rewrite steward::ControlTable over sync_steward* (final D2 commit; pushed nowhere yet, lives only on this working tree).
- **Uncommitted working tree changes** (the "D4 foundation" — see §3 below):

  ```
  Cargo.lock                          |   1 +
  crates/steward/src/control_table.rs | 135 ++++++++++++++++++++++++++++++++++++
  crates/sync-remote/Cargo.toml       |   1 +
  crates/sync-remote/src/lib.rs       |   2 +
  crates/sync-remote/src/remote.rs    |  25 +++++--
  crates/sync-remote/src/verify.rs    |   7 +-
  crates/sync-steward/src/error.rs    |   8 +++
  7 files changed, 169 insertions(+), 10 deletions(-)
  ```

  Plus the new file: `crates/sync-remote/src/steward_trait.rs` (~200 lines).  See `git status` from the resumption session.

  These edits build cleanly and all existing tests pass (1387 unit tests + sync-* tests). They are a non-breaking refactor that prepares the integration surface.

### Decisions already locked in (from user, this session)

- **No back-compat required**.  Old format/operator config breakage is acceptable.
- **D4 first, then D3** (reordered from the original plan).  D3 (verify CLI verbs) becomes a small phase on top of D4.
- **Adopt sandbox bundle format / layout as-is** (`bundle=<pond_id>:txn=<seq>/`, 3-partition `manifest`/`checksum`/`data` Delta table).
- **FS convention for remote configs: `/sys/remotes/<name>`** (hard cutover in D4; old `/system/run/<N-remote>` path goes away).
- **Remote management CLI verbs: `pond remote add/remove/list`** (git-like; chosen over `bind/unbind` and over the plan's `attach/detach`).

### Status table (from `docs/remote-redesign.md` after this session, post-foundation)

| Phase | Status | Commit |
|---|---|---|
| Plan | done | `c2b8cc68` |
| D1 relocate sandbox crates | done | `bd965792` |
| D2 prep | done | `6c75e025` |
| D2 substantive refactor | done | `85eba5c4` |
| **D4 foundation (this doc)** | **uncommitted on working tree** | — |
| D4 remainder | **pending — main subject of next session** | — |
| D3 (verify + restart CLI verbs) | pending (after D4) | — |
| D5 (tlogfs partition by pond_id) | pending | fixes May 10 Bug 1 & 2 |
| D6 (cross-pond model + operator-guide rewrite) | pending | |

---

## 2. Why this design (the architectural insight)

`sync_remote::Remote::push` (and `pull`, `maintain`, etc.) operate on **Delta-Lake-action-level** primitives, NOT on the data store's row schema.  Specifically push reads:

1. `actions_at_version(version)` — returns `Add`/`Remove` file actions from the Delta commit log.  Schema-agnostic; works on ANY Delta table.
2. `read_data_file(path)` — slurps a parquet file's bytes by relative path.  Schema-agnostic.

And `pull`'s `apply_pulled_bundle` writes parquet bytes back and commits Delta actions — also schema-agnostic.

This means `sync_remote::Remote` can drive duckpond's `tlogfs` DeltaTable EXACTLY as well as it drives the sandbox's `sync_store::Store` — **as long as we extract a trait** isolating the surface `Remote` needs from a "steward".

The trait approach is much cleaner than the alternatives I considered and rejected:
- *Force duckpond to use `sync_store::Store` schema* — would require a complete rewrite of tlogfs to use the k/v schema, basically D5+D6 combined.
- *Run `sync_steward::Steward` alongside `Ship` as parallel data layers* — wasted storage, schema duplication.
- *Refactor `sync_remote::Remote` to take separate `&DeltaTable + &mut ControlTable + ...` args* — leaks abstractions across the boundary.

**Schema-touching APIs that DO require row-aware code** are limited to:
- `compute_partition_checksum` / `partitions` (used by `verify_against_remote`, i.e. D3).
- The duckpond-side `apply_pulled_bundle` validation of path format (sync_store uses `pond_id=<uuid>/partition_key=<value>/`; duckpond tlogfs uses just `part_id=<uuid>/`).

Both are confined to the adapter — `Remote` itself stays generic.

---

## 3. What's already done (uncommitted "D4 foundation")

### 3.1 `crates/sync-steward/src/error.rs`

Added `StewardError::Adapter(Box<dyn std::error::Error + Send + Sync>)` so duckpond adapter errors can flow through the trait boundary without coupling sync-steward to consumer-specific error hierarchies.

### 3.2 `crates/sync-remote/Cargo.toml`

Added `async-trait.workspace = true` to `[dependencies]`.

### 3.3 `crates/sync-remote/src/steward_trait.rs` (NEW, ~200 lines)

The trait extraction:

```rust
#[async_trait]
pub trait RemoteSteward: Send + Sync {
    fn store_id(&self) -> Uuid;
    fn path(&self) -> &Path;

    async fn data_committed_record(&self, pond_id: Uuid, txn_seq: i64)
        -> StewardResult<Option<ControlRecord>>;
    async fn log(&self, limit: Option<usize>) -> StewardResult<Vec<ControlRecord>>;
    async fn actions_at_version(&self, version: i64)
        -> StewardResult<(Vec<AddPath>, Vec<RemovePath>)>;
    fn read_data_file(&self, rel_path: &str) -> StewardResult<Vec<u8>>;

    async fn record_post_push_pending(&mut self, txn_seq: i64) -> StewardResult<String>;
    async fn record_post_push_completed(&mut self, txn_seq: i64, txn_id: String, pending_started_micros: i64)
        -> StewardResult<()>;
    async fn record_post_push_failed(&mut self, txn_seq: i64, txn_id: String, pending_started_micros: i64, reason: String)
        -> StewardResult<()>;

    async fn config_get(&self, key: &str) -> StewardResult<Option<String>>;
    async fn config_set(&mut self, key: &str, value: &str) -> StewardResult<()>;

    async fn apply_pulled_bundle(&mut self, bundle: PulledBundle) -> StewardResult<()>;
    async fn drop_pond_data(&mut self, pond_id: Uuid) -> StewardResult<()>;
    async fn compute_live_checksums(&self, pond_id: Uuid) -> StewardResult<PartitionChecksums>;
}

#[async_trait]
impl RemoteSteward for sync_steward::Steward { /* trivial delegation */ }
```

### 3.4 `crates/sync-remote/src/lib.rs`

- `mod steward_trait;`
- `pub use steward_trait::RemoteSteward;`

### 3.5 `crates/sync-remote/src/remote.rs`

Made these methods generic over `<S: RemoteSteward + ?Sized>` (was concrete `&mut Steward`):

- `pub async fn push<S>(&mut self, steward: &mut S, txn_seq: i64) -> Result<()>`
- `pub async fn pull<S>(&self, steward: &mut S) -> Result<PullReport>`
- `pub async fn restart_pond_from_compact<S>(&self, consumer: &mut S) -> Result<()>`
- `async fn build_and_commit_bundle<S>(&mut self, steward: &S, ...)`  (helper)
- `async fn reconcile_post_push_after_idempotent_skip<S>(&self, steward: &mut S, txn_seq: i64)`  (helper)

`restart_from_compact(consumer_path: &Path) -> Result<Steward>` (the MIRROR-mode version that creates a fresh Steward) was deliberately left Steward-specific.  Duckpond will need an equivalent that creates a fresh `Ship`/`OpLogPersistence` instead; that helper lives in duckpond, not in sync_remote.

`Remote::maintain` was also left as-is (it doesn't take a Steward).

### 3.6 `crates/sync-remote/src/verify.rs`

`verify_against_remote(remote: &Remote, steward: &Steward)` became `verify_against_remote<S: RemoteSteward + ?Sized>(remote: &Remote, steward: &S)`.

### 3.7 `crates/steward/src/control_table.rs`

Added six methods on the duckpond `ControlTable` wrapper so the upcoming `ShipRemoteSteward` adapter can talk to the underlying lean control table directly without going through the wrapper's prefix-munging settings API:

- `pub fn pond_id_uuid(&self) -> uuid::Uuid` — converts the cached `uuid7::Uuid` to `uuid::Uuid` once.
- `pub fn inner(&self) -> &sync_steward::ControlTable`
- `pub fn inner_mut(&mut self) -> &mut sync_steward::ControlTable`
- `pub async fn record_post_push_pending(&mut self, txn_seq) -> Result<String>` — writes `PostPushPending` directly (no `metadata_json` factory attribution).
- `pub async fn record_post_push_completed(&mut self, txn_seq, txn_id, pending_started_micros) -> Result<()>`
- `pub async fn record_post_push_failed(&mut self, txn_seq, txn_id, pending_started_micros, reason) -> Result<()>`
- `pub async fn raw_config_get(&self, key: &str) -> Result<Option<String>>` — bypasses the `"setting:"` prefix.
- `pub async fn raw_config_set(&mut self, key, value) -> Result<()>`

**Critical distinction**: the existing `set_setting`/`get_setting` add a `"setting:"` prefix for the duckpond user-facing API (so `pond control set-config foo bar` writes `setting:foo=bar`).  The sync_remote adapter must use `raw_config_*` because sync_remote writes keys like `last_pulled_seq:<url>` which must match the format `sync_steward::Steward::config_set` uses byte-for-byte.

### 3.8 Build + test status of the foundation

```
$ cargo build --workspace      # clean
$ cargo test -p steward --lib  # 16 passed / 0 failed
$ cargo test -p sync-steward -p sync-store -p sync-remote -p sync-tests
                               # all pass; sync_steward::Steward's blanket
                               # impl of RemoteSteward is transparent.
```

`cargo fmt --check` and `cargo clippy --workspace -- -D warnings` not yet run on the foundation — should run before/with the first D4 commit.

---

## 4. Plan for the rest of D4

Sixteen todos exist in the session SQLite DB (id pattern `d4-%`).  The five surveys/baselines/setup-decisions are already marked done.  Here's the remaining work in dependency order:

### Phase A: Adapter + smoke tests (foundational)

#### `d4-adapter-steward` — IMPLEMENT `ShipRemoteSteward`

**Where**: new file `crates/steward/src/remote_adapter.rs` (recommended).  Wire it from `crates/steward/src/lib.rs`.

**What**: a struct wrapping `&'a mut Ship` (or `&'a mut Ship + &'a OpLogPersistence` if Ship's data_persistence accessor is too restrictive — check first) that implements `sync_remote::RemoteSteward`.

```rust
pub struct ShipRemoteSteward<'a> {
    ship: &'a mut Ship,
}

#[async_trait]
impl<'a> RemoteSteward for ShipRemoteSteward<'a> {
    fn store_id(&self) -> Uuid {
        // ship.control_table().pond_id_uuid()
    }

    fn path(&self) -> &Path { /* ship.pond_path() */ }

    async fn data_committed_record(&self, pond_id, txn_seq) -> StewardResult<Option<ControlRecord>> {
        // self.ship.control_table().inner().all_records_for(pond_id).await
        //     .map(|all| all.into_iter().find(|r| ...DataCommitted, txn_seq...))
    }

    async fn log(&self, limit) -> StewardResult<Vec<ControlRecord>> {
        // self.ship.control_table().inner().all_records().await + trim
    }

    async fn actions_at_version(&self, version) -> StewardResult<(Vec<AddPath>, Vec<RemovePath>)> {
        // Port the body of sync_store::Store::actions_at_version (lines 618-644):
        // - self.ship.data_persistence().table().log_store().read_commit_entry(version).await
        // - deltalake::logstore::get_actions(version, &bytes)
        // - filter into AddPath/RemovePath
    }

    fn read_data_file(&self, rel_path: &str) -> StewardResult<Vec<u8>> {
        // std::fs::read(self.ship.pond_path().join("data").join(rel_path))
    }

    async fn record_post_push_pending(&mut self, txn_seq) -> StewardResult<String> {
        // self.ship.control_table_mut().record_post_push_pending(txn_seq).await
        //     .map_err(|e| StewardError::Adapter(Box::new(e)))
    }
    // ... record_post_push_completed/failed similar.

    async fn config_get(&self, key) -> StewardResult<Option<String>> {
        // self.ship.control_table().raw_config_get(key).await
        //     .map_err(|e| StewardError::Adapter(Box::new(e)))
    }
    async fn config_set(&mut self, key, value) -> StewardResult<()> { ... }

    async fn apply_pulled_bundle(&mut self, bundle: PulledBundle) -> StewardResult<()> {
        // This is the most substantial method.  Port sync_steward::Steward::apply_pulled_bundle
        // (lines 413-540 of crates/sync-steward/src/steward.rs) but using the tlogfs DeltaTable.
        //
        // Key differences from the sandbox version:
        //
        // 1. Path validation: sandbox requires "pond_id=<uuid>/partition_key=<value>" segment pair.
        //    Duckpond tlogfs paths look like "part_id=<uuid>/..." (no pond_id segment until D5).
        //    Adapt the validation accordingly.
        //
        // 2. Writing parquet bytes: same as sandbox -- write add.path bytes under <data_path>/<add.path>,
        //    creating parent dirs.
        //
        // 3. Committing Delta actions: use the OpLogPersistence's DeltaTable via CommitBuilder.
        //    See sync_store::Store::commit_actions (lines 661-676) for the pattern.
        //
        // 4. Writing the DataCommitted record: use self.ship.control_table_mut().inner_mut().write_record(...)
        //    directly (we already have inner_mut() exposed).
        //
        // 5. Advancing last_write_seq when mirror mode (bundle.pond_id == self.store_id()):
        //    Need a Ship method to do this; today Ship's last_write_seq is a private field that
        //    gets bumped only by begin_write().  Add a method like Ship::sync_last_write_seq(seq)
        //    that clamps to max(current, seq).  Note: tlogfs::OpLogPersistence has its own
        //    last_txn_seq that ALSO needs updating.  Check create_pond_for_restoration's existing
        //    code path for how it handles this.
    }

    async fn drop_pond_data(&mut self, pond_id) -> StewardResult<()> {
        // Two DELETE statements:
        // - On data table (tlogfs DeltaTable):
        //     DELETE FROM <data> WHERE pond_id = '<uuid>'
        //   pond_id is already an OplogEntry column, so this works pre-D5.
        // - On control table (sync_steward's): use self.ship.control_table_mut().inner_mut().drop_pond_records(pond_id).await
    }

    async fn compute_live_checksums(&self, pond_id) -> StewardResult<PartitionChecksums> {
        // FOR D4: stub with unimplemented!("verify is D3") OR implement a
        // tlogfs-aware version that walks OplogEntry rows for pond_id, groups by part_id,
        // and hashes the live (post-tombstone) content per partition.
        //
        // The duckpond row schema is fundamentally different from sync_store's, so
        // we cannot reuse sync_store::Store::compute_partition_checksum.  The hash
        // domain needs to be defined for tlogfs rows (probably: (file_id, version, content_hash)
        // tuples per partition, fed through Merkle or Homomorphic strategy).
        //
        // RECOMMEND deferring the implementation; D3 phase needs it but D4 doesn't.
        // For D4, return Ok(PartitionChecksums::new()) or unimplemented!() and revisit in D3.
    }
}
```

Open question: should the adapter take `&mut Ship` or `(&mut OpLogPersistence, &mut ControlTable)`?  Taking `&mut Ship` is simpler but ties the adapter to Ship's borrow lifecycle.  Taking the two pieces directly is more flexible but requires the caller to do the disaggregation.  **Recommend**: `&mut Ship` initially; refactor if borrow conflicts arise.

#### `d4-fs-convention` — Define `/sys/remotes/<name>` constant

**Where**: probably `crates/cmd/src/commands/control.rs` (alongside existing `SYSTEM_RUN_DIR` / `SYSTEM_ETC_DIR`) or a new `crates/cmd/src/commands/remote.rs`.

```rust
pub const SYSTEM_REMOTES_DIR: &str = "/sys/remotes";
```

### Phase B: CLI verbs (build on adapter)

#### `d4-pond-remote-add` — `pond remote add|remove|list`

**Where**: new file `crates/cmd/src/commands/remote.rs`, wired from `crates/cmd/src/main.rs` and `crates/cmd/src/cli.rs` (clap subcommand).

**`pond remote add <name> <url> [--push|--pull] [--credentials-env=KEY]`**:
- Resolve the FS path: `/sys/remotes/<name>`.
- Validate URL parses (`url::Url::parse`).
- Validate name is a single path segment (no slashes).
- Validate `/sys/remotes/` exists (create if not, with `create_dir_all` semantics).
- Validate `/sys/remotes/<name>` does NOT already exist (or honor `--overwrite`).
- Write a small YAML config (one of: `RemoteAttachment { url, credentials_env }`) to the FS node.
- Use `ControlTable::set_factory_mode("remote:<name>", "push"|"pull")` OR `raw_config_set("remote_mode:<name>", "push"|"pull")` to persist the operating mode per-replica.

**`pond remote remove <name>`**:
- Refuse if any pending PostPush* records reference this remote (would orphan them).
- Delete `/sys/remotes/<name>` node.
- Clear `last_pushed_seq:<url>` and `last_pulled_seq:<url>` settings.

**`pond remote list`**:
- Walk `/sys/remotes/*` collecting node names + URLs from configs.
- Render alongside mode + last sync info from control table.

#### `d4-pond-push` — `pond push [<name>]`

**Where**: new file `crates/cmd/src/commands/push.rs` or extend `control.rs`.

```
pond push           # push all remotes in push mode
pond push <name>    # push just that remote
```

For each target remote:
1. Read config from `/sys/remotes/<name>`.
2. `sync_remote::register_s3_handlers()` if URL is `s3://`.
3. Open remote: `let mut remote = Remote::open_at_url(url, storage_options).await?;`
4. Determine which `txn_seq`s need pushing: from `(raw_config_get("last_pushed_seq:<url>") || 0) + 1` up to current `last_write_seq`.
5. For each seq, `remote.push(&mut ShipRemoteSteward { ship }, seq).await?`.

Note: `Remote::push` already records `PostPushPending/Completed/Failed` via the steward.  The CLI just drives the loop.

#### `d4-pond-pull` — `pond pull [<name>]`

Analogous to push, but calls `remote.pull(&mut adapter).await?`.  The Remote internally walks bundles > `last_pulled_seq` and applies each via `apply_pulled_bundle`.

#### `d4-pond-maintain` — `pond maintain --remote=<name> [--keep-compact=N]`

Calls `remote.maintain(MaintainOptions { keep_compact_bundles: N, vacuum_after: true })`.

#### `d4-pond-restart-from-compact` — `pond restart-from-compact <name>`

Two flavors per the sandbox API:

- **`--per-pond`** (default for cross-pond import): `remote.restart_pond_from_compact(&mut adapter).await?` — operates in place on an existing pond.

- **`--mirror` (mirror-mode bootstrap)**: needs a duckpond-side reimplementation of `restart_from_compact(consumer_path)` because the sandbox version creates a fresh `sync_steward::Steward` at the path.  The duckpond equivalent:
  1. Validate consumer_path either doesn't exist or contains an empty/no-store_id pond.
  2. Wipe consumer_path.
  3. Create a fresh `Ship::create_pond_for_restoration(consumer_path, preserve_metadata)` with `pond_id = remote.store_id`.
  4. Read the baseline compact bundle from the remote, apply it via the adapter.
  5. Set `last_pulled_seq:<url>` = baseline.txn_seq.
  6. Call `remote.pull(&mut adapter)` to catch up.

  This is essentially `restart_from_compact` ported to duckpond data types.

### Phase C: Post-commit auto-push

#### `d4-post-commit-push` — Replace existing factory post-commit dispatch

**Where**: `crates/steward/src/guard.rs::run_post_commit_factories` (~line 480-540).

**Current behavior**: scans `/system/run/*` for factory nodes, executes each in `push` mode via `FactoryRegistry::execute`.

**New behavior**:
1. Scan `/sys/remotes/*` for remote configs.
2. For each remote with `raw_config_get("remote_mode:<name>") == Some("push")`:
   - Open the remote, push every seq from `last_pushed_seq + 1` up to current.
   - Use the same `ShipRemoteSteward` adapter as the CLI verb.
3. (Continue to scan `/system/run/*` for other post-commit factories until D6 cleans them up.)

The old "remote factory in /system/run/" path needs to coexist with the new `/sys/remotes/` path during D4, OR be hard-cutover (user's decision — they said hard cutover is fine).  Recommend: hard cutover, since "no compat required".

### Phase D: Deletion + test rewrites

#### `d4-delete-old-remote`

Files to delete:
- `crates/remote/src/changes.rs` (192 lines)
- `crates/remote/src/chunked_async_reader.rs` (149 lines)
- `crates/remote/src/reader.rs` (82 lines)
- `crates/remote/src/schema.rs` (259 lines)
- `crates/remote/src/table.rs` (1025 lines)
- `crates/remote/src/writer.rs` (78 lines)
- `crates/remote/src/s3_registration.rs` (102 lines) — replace with `sync_remote::register_s3_handlers`
- `crates/remote/tests/change_detection_tests.rs` — rewrite or delete

To keep but slim drastically:
- `crates/remote/src/factory.rs` (2881 lines today).  After D4 the "remote" factory dispatch is GONE — replaced by `/sys/remotes/` + new CLI verbs.  So most of this file deletes.  Might keep `RemoteConfig` struct as the YAML schema (now lives in `/sys/remotes/<name>` instead of `/sys/run/N-remote`).
- `crates/remote/src/lib.rs` — slim re-exports.
- `crates/remote/Cargo.toml` — drop deps no longer used.

External callers to update:
- `crates/cmd/src/commands/control.rs:678` — `remote::ReplicationConfig::from_base64` (sync command).  Either delete (replaced by `pond pull --config-from-base64`?) or port.
- `crates/cmd/src/commands/run.rs:344` — `remote::RemoteConfig` (factory dispatch).  Delete; the factory path goes away.
- `crates/cmd/src/commands/init.rs:97-221` — `remote::RemoteConfig`, `remote::register_s3_handlers`, `remote::RemoteTable::open`, `remote::factory::apply_parquet_files_from_remote`, `remote::factory::restore_large_files_from_remote`.  This is the `pond init --from-remote` path; needs full port to `sync_remote::Remote::restart_from_compact`-style flow.
- `crates/cmd/tests/test_init_restore.rs:14` — `use remote::RemoteTable;` — rewrite.

#### `d4-rewrite-remote-tests`

Beyond the callers above, also:
- `crates/cmd/src/commands/control.rs::execute_sync_for_path` and friends — the entire "sync via factory in /system/run/" path.  Replace with `pond push` / `pond pull` CLI shape, or expose `pond sync <name>` as an alias that picks mode from `remote_mode:<name>` setting.

### Phase E: CI gates + smoke

#### `d4-ci-gates`

```
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace
```

Expect a slight test-count change from baseline (1387 unit tests post-D2) due to deleted `remote::*` tests and added adapter tests.  Recommend +5-10 adapter unit tests, +1-2 integration tests under `crates/cmd/tests/`.

#### `d4-cli-smoke`

Manual or scripted end-to-end with MinIO (sync-tests already has the MinIO harness; can borrow).  Sequence:

```
mkdir -p $POND
pond init
echo hi > /tmp/f && pond copy host:///tmp/f /f
pond remote add origin file:///tmp/remote-pond --push
pond push
pond remote list

# replica
mkdir -p $POND2
pond init --from-remote file:///tmp/remote-pond   # or pond restart-from-compact origin --mirror
pond list /                                       # should show /f
```

S3 variant via MinIO once MinIO is running (`docker-compose up -d minio` in `testsuite/`).

### Phase F: Doc update

#### `d4-update-redesign-doc`

Update `docs/remote-redesign.md`:
- Status table: D4 done with commit SHA.
- Append "D4 substantive completion notes" subsection (parallel to D2 notes), capturing:
  - FS convention move to `/sys/remotes/`.
  - CLI verb naming `pond remote add/remove/list` (vs plan's `attach/detach`).
  - `RemoteSteward` trait extraction in sync-remote.
  - Deletion of `crates/remote/{table,schema,chunking}.rs` line counts.
  - Any deviations from the plan.

---

## 5. Known landmines / gotchas

1. **`Ship::data_persistence` is private** — needs either an accessor method or all adapter logic must go through Ship.  Check current Ship API; may need to add `pub fn data_persistence(&self) -> &OpLogPersistence` and `data_persistence_mut`.

2. **`Ship::last_write_seq` is private** — `apply_pulled_bundle` in mirror mode needs to bump this.  Add `Ship::sync_last_write_seq(seq)` or similar (clamp to max).

3. **`tlogfs::OpLogPersistence::last_txn_seq` is read-only** (computed from Delta metadata).  In mirror mode, `apply_pulled_bundle` must commit Delta actions with `pond_txn` metadata carrying the right `txn_seq`, so that `last_txn_seq()` re-reads correctly after.

4. **Path validation in `apply_pulled_bundle`** — sandbox enforces `pond_id=<uuid>/partition_key=<v>` segments.  Duckpond paths are `part_id=<uuid>/<filename>.parquet`.  Adapter version must validate accordingly.

5. **Pond identity preservation on restart-from-compact** — duckpond's `create_pond_for_restoration` takes a `PondMetadata` (full birth metadata).  When bootstrapping from a remote that only carries a `store_id`, we may need to invent a default `PondMetadata` (e.g., `birth_timestamp = bundle.ts_micros`, `birth_hostname = "from-remote"`, `birth_username = "from-remote"`).  Alternatively, store the source `PondMetadata` in the bundle's manifest as part of D4's bundle-format additions — but per "adopt sandbox format as-is", we shouldn't change the format.  **Recommend**: invent a placeholder; users who care can rewrite via `pond control set-config`.

6. **`sync_remote::Remote::push` requires the source's `last_write_seq`** indirectly — push expects the caller to drive the txn_seq loop.  Make sure `pond push` reads `ship.last_write_seq()` (need accessor) and iterates from there.

7. **Cross-pond import bundle paths** — when the duckpond consumer pulls from a SANDBOX-FORMAT remote (i.e., the remote was written by `sync_remote::Remote::push` from a sync_steward source), the bundle's `add.path` values use `pond_id=.../partition_key=.../...` Hive segments.  Duckpond's tlogfs has no such structure.  This means cross-pond import from a non-duckpond source WILL NOT WORK in D4.  Document this limitation; D5 (which adds pond_id to tlogfs partition columns) fixes it.

   For D4, only **duckpond ⇄ duckpond** push/pull is in scope.  Inside that closed loop, paths look like `part_id=<uuid>/<file>.parquet` end-to-end and everything roundtrips.

8. **`maintain_table` (duckpond's local Delta maintenance)** in `crates/steward/src/maintenance.rs` is unaffected — it operates on local DeltaTables directly, not through sync_remote.  But the new `pond maintain --remote=<name>` is a separate operation that calls `Remote::maintain` on the remote bucket.

---

## 6. Suggested commit structure for D4

To keep PRs reviewable:

**Commit D4.1** (foundation, ready now):
- `sync_steward::StewardError::Adapter` variant.
- `sync_remote::RemoteSteward` trait + blanket impl + generic refactor of push/pull/maintain/restart_pond_from_compact/verify_against_remote.
- Six new methods on `steward::ControlTable` (pond_id_uuid, inner, inner_mut, record_post_push_*, raw_config_*).
- 169 lines added, 10 removed, no behavior change.

**Commit D4.2**: adapter (`ShipRemoteSteward`) + 5-10 unit tests against an in-memory pond.

**Commit D4.3**: CLI verbs (pond remote add/remove/list + pond push + pond pull + pond maintain + pond restart-from-compact).  No behavior change to existing factories yet.

**Commit D4.4**: post-commit auto-push migration: scan `/sys/remotes/` instead of `/system/run/<N-remote>`.  Cutover.

**Commit D4.5**: delete old `crates/remote/{table,schema,chunking,changes,reader,writer,chunked_async_reader,s3_registration}.rs`.  Slim `factory.rs`.  Update external callers.  Drop unused deps.

**Commit D4.6**: rewrite/delete remote tests + cmd tests that referenced old types.  CI gates clean.

**Commit D4.7** (doc): update `docs/remote-redesign.md` status + completion notes.

Each commit is independently `cargo test --workspace` clean.

---

## 7. Test baseline

From this session, just before D4 work began:

```
$ cargo test --workspace
... 1387 passed; 0 failed; 9 ignored ...
```

The 9 ignored are S3/MinIO tests that need a running MinIO instance.

Target post-D4: same passing count or higher (deletions of old remote tests offset by new adapter tests + CLI integration tests).

---

## 8. Reference files for D4

- `docs/remote-redesign.md` — canonical plan; status table is authoritative.
- `docs/archive/sandbox-design.md` — original sandbox design (background on bundle format).
- `crates/sync-remote/src/remote.rs` — the API D4 wires duckpond into.
- `crates/sync-remote/src/steward_trait.rs` — the new trait (NEW this session).
- `crates/sync-steward/src/steward.rs` — reference for what `sync_steward::Steward`'s implementation of each trait method does (port these patterns to the adapter).
- `crates/steward/src/control_table.rs` — the duckpond control table wrapper now has the raw_config_* and record_post_push_* methods the adapter needs.
- `crates/steward/src/ship.rs` — the type the adapter wraps; may need `pub fn data_persistence(&self) -> &OpLogPersistence` and `pub fn last_write_seq(&self) -> i64` accessors.
- `crates/tlogfs/src/persistence.rs::OpLogPersistence::{table, set_table, store_path, create_empty, open_or_create}` — the data layer the adapter manipulates.
- `crates/remote/src/factory.rs` — the OLD remote layer to delete (~2881 lines).
- `crates/remote/src/table.rs` — the OLD `RemoteTable` to delete (~1025 lines).
- `crates/cmd/src/commands/control.rs` and `init.rs` — the OLD callers of `remote::*` that need rewiring.
- `testsuite/tests/540-import-watermark-incremental.sh` — testsuite shell test that exercises the OLD import path.  Will need updating or deleting in D4 (the import factory mode goes away).
- `crates/sync-tests/tests/s3_minio.rs` — reference for how to drive `Remote` against MinIO end-to-end.

---

## 9. Out-of-scope reminders

- **D5** (tlogfs partition by `(pond_id, part_id)` + bootstrap row pond_id) is what enables cross-pond import from non-duckpond sources.  D4 stays within duckpond⇄duckpond.
- **D6** (cross-pond model + operator-guide rewrite) is the final shape; D4 doesn't need to settle docs/operator-guide.md.
- **D3** (`pond verify`) is the next phase AFTER D4; the `RemoteSteward::compute_live_checksums` method can be stubbed `unimplemented!()` in D4 and properly implemented in D3.
- **May 10 production bugs**: D4 doesn't fix them directly.  D5 fixes Bug 1 + Bug 2.  D6 makes Bug 3 moot.

---

End of D4 resumption document.
