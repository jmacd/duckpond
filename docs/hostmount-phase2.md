# Hostmount Phase 2 — Steward Trait Extraction: Progress

## Status: Phase 2 COMPLETE -- Host Variant Added

Phase 2 aims to extract a steward abstraction so that hostmount can have its own
lightweight steward without transactions or a control table. Before extracting
the abstraction, we identified that the steward code had accumulated accidental coupling
that needed cleanup first (Phase 2A, now complete). Phase 2B introduced enum-based
dispatch types `Steward` and `Transaction` that wrap the concrete implementations.
Phase 2C narrowed `Transaction` to backend-agnostic methods. Phase 2D added the
`Host` variant, completing the steward abstraction.

---

## Planning: Design Q&A

These are the design questions asked and decisions made before implementation began.
Included here so the work can be resumed from scratch.

### Q1: Trait Scope — What goes on the Steward trait?

**Question:** Should the trait be narrow (just begin/commit/abort) or wide
(including ProviderContext, SQL execution, etc.)?

**Answer:** Needs more discussion — the code is poorly designed and is "almost the
oldest in the codebase." The guard mixes transaction lifecycle with tlogfs-specific
concerns. The user said: "None of your findings are purposeful, just accidents."
The consensus was to audit and refactor before deciding trait scope.

### Q2: Ship Scope — One Ship type or two?

**Question:** Should Ship split into PondShip / HostShip or use a unified enum?

**Answer:** Unified enum. Single `Ship` type (or a `Steward` enum) dispatching to
`PondSteward | HostSteward`. One type for CLI dispatch, avoids generic proliferation.

### Q3: Control Table — Should the trait expose it?

**Question:** Should `control_table()` be on the Steward trait?

**Answer:** No. Hostmount has no control table. Pond-specific callers access it
through the concrete type. The trait surface stays generic.

### Q4: Naming

**Question:** Trait name and concrete type names?

**Answer:** Trait: `Steward`. Concrete: `PondSteward` (wraps Ship, full tlogfs)
and `HostSteward` (lightweight, no transactions).

### Q5: Refactor Before Trait?

**Question:** Should we extract the trait first or clean up the coupling first?

**Answer:** Clean up first. The code isn't ready for trait extraction — the guard
mixes concerns that need to be separated. User said: "This code isn't ready.
Can we examine how to refactor before we proceed?"

### Q6: How to handle `state()` doing double duty?

**Question:** `tx.state()` returns `tlogfs::persistence::State` which is used for
two completely different purposes:
1. tlogfs-specific persistence access (transaction internals)
2. Getting a `ProviderContext` via `.as_provider_context()`

Should we refactor `state()` away?

**Answer:** Yes. Refactor `state()` away from the trait surface. The guard should
own ProviderContext construction directly — callers should get a `ProviderContext`
without reaching into tlogfs internals.

### Q7: Where should `execute_sql_on_file` / `get_file_schema` live?

**Question:** These take `&mut TransactionGuard` but only need a ProviderContext.
Should we change their signatures?

**Answer:** Yes. Change them to take `&ProviderContext` instead of `&mut TransactionGuard`.
This breaks the dependency on the concrete guard type.

### Q8: Where should `get_factory_for_node` live?

**Question:** Currently on `tlogfs::State` / `OpLogPersistence`. It queries the
oplog for factory name associated with a FileID. Should it move?

**Answer:** Move to `PersistenceLayer` trait. It only needs persistence-layer
operations (load node, read config bytes). Hostmount needs it too — on the host
filesystem, the factory name would come from the YAML config file itself.

### Q9: Where should `get_commit_history` live?

**Question:** Currently on `OpLogPersistence`, accessed through `tx.data_persistence()`.
It returns Delta Lake `CommitInfo` history.

**Answer:** Move to `ControlTable`. It's purely an audit/history query. The guard
shouldn't need to expose `data_persistence()` for this.

### Q10: Kill `transact()` or fix it?

**Question:** The old `transact()` has bad ergonomics (Box::pin, manual error
wrapping, exposes both tx and fs). Should we remove it outright or fix the
signature?

**Answer:** Fix it. A clean scoped-transaction API is valuable. The result is
`write_transaction()` which takes `async |fs| { ... }` closures with `AsyncFnOnce`.

### Q11: Task ordering?

**Question:** In what order should the 2A tasks be done?

**Answer:** Kill (fix) transact first — it's the highest-value cleanup with
the most callers. Then provider_context on guard, then function signature changes,
then move methods to their proper homes. One task at a time, compile-check between each.

---

## The Core Architectural Problem

The audit identified four categories of callers by what they need from the guard:

| Category | Example Callers | What They Actually Need |
|----------|----------------|----------------------|
| **FS-only** | mkdir, copy, hydrovu create | `&FS` (via Deref on guard) |
| **FS + provider context** | mknod, run (factories) | `&FS` + `ProviderContext` (for factory initialization) |
| **FS + SQL session** | cat, describe, show, export, temporal | `&FS` + `SessionContext` + `ProviderContext` (for DataFusion queries) |
| **Control table** | control, replicate | `&ControlTable` (audit queries) |

**The coupling chain:** To get a `ProviderContext`, callers currently must:
```
tx.state()                     → tlogfs::persistence::State
  .as_provider_context()       → ProviderContext
```

This means every caller that needs a `ProviderContext` (which is persistence-backend
agnostic) must go through `tlogfs::State` (which is tlogfs-specific). This coupling
makes trait extraction impossible — the trait can't expose `tlogfs::State`.

**The fix:** The guard constructs `ProviderContext` directly from its components
(persistence layer + session context + template variables). Callers never touch
`tlogfs::State`.

---

## Full Coupling Map: Guard Method → Callers

### `tx.state()` — 30+ call sites

**Production code in steward:**
- `ship.rs` — `begin_transaction()` and `begin_transaction_replay()` set template variables
- `guard.rs` — `discover_post_commit_factory_configs()` creates FS and calls `get_factory_for_node`
- `guard.rs` — `execute_post_commit_factory()` creates `FactoryContext`

**Production code in cmd:**
- `mknod.rs` — factory initialization (gets provider context)
- `run.rs` — factory execution (creates FS, gets factory, creates FactoryContext)
- `cat.rs` — gets provider context via `transaction_guard().state()`
- `describe.rs` — 3 call sites for schema retrieval and version queries
- `export.rs` — export pipeline (template variables, table provider registration)
- `control.rs` — backup recovery (factory execution)
- `temporal.rs` — temporal overlap detection (FS creation, version queries, provider context)
- `hydrovu.rs` — HydroVu data collection (FS creation)

**Test code:**
- `test_post_commit_factory.rs` — 6 call sites for factory setup
- `control_test.rs` — 1 call site (replica initialization)
- `test_executable_factory.rs` — 2 call sites

### `tx.session_context()` — 5 call sites

- `show.rs` — `show_brief_mode()` and `show_detailed_mode()`
- `temporal.rs` — temporal UNION queries
- `describe.rs` — version-level queries
- `export.rs` — export query execution

### `tx.data_persistence()` — 1 call site

- `show.rs` — `show_filesystem_transactions()` calls `get_commit_history()`

### `tx.object_store()` — 1 call site

- `temporal.rs` — ensures ObjectStore initialized for version access

### `tx.transaction_guard()` — 6 call sites

All of these call `.transaction_guard()` just to reach `.state()`:
- `cat.rs`, `describe.rs` (3×), `export.rs`, `copy.rs`

These will vanish once `provider_context()` is on the guard directly.

### `execute_sql_on_file` — 3 call sites

Defined in `tlogfs::query::sql_executor`. Takes `&mut TransactionGuard`:
- `copy.rs` — export series to host
- `tlogfs/tests/mod.rs` — 2 test call sites

### `get_file_schema` — 2 call sites

Defined in `tlogfs::query::sql_executor`. Takes `&mut TransactionGuard`:
- `describe.rs` — 2 call sites (series schema, table schema)

### `get_factory_for_node` — 3 call sites

Defined on `OpLogPersistence` (via `State`):
- `guard.rs` — post-commit factory discovery
- `run.rs` — factory lookup
- `control.rs` — backup recovery factory lookup

### Task 2A-6: Replace `transact()` with `write_transaction()` — COMPLETE

The old `transact()` API suffered from ergonomic problems:

1. **Manual error wrapping ceremony** — every tinyfs operation required
   `.map_err(|e| StewardError::DataInit(TLogFSError::TinyFS(e)))`, even though
   `From` impls already existed for each hop
2. **`Box::pin(async move { ... })` boilerplate** — required at every call site
3. **Exposed both `tx` and `fs`** — callers received `(&StewardTransactionGuard, &FS)`
   when most only used `&FS`

#### New API: `write_transaction()`

```rust
pub async fn write_transaction<F>(
    &mut self,
    meta: &PondUserMetadata,
    f: F,
) -> Result<(), StewardError>
where
    F: for<'a> AsyncFnOnce(&'a tinyfs::FS) -> Result<(), StewardError>,
```

Uses `std::ops::AsyncFnOnce` (stable in Rust 1.92) — no `Box::pin`, no manual
`Future` trait bounds. The closure receives only `&FS`, not the transaction guard.

#### Before / After

**Before:**
```rust
ship.transact(&meta, |_tx, fs| {
    Box::pin(async move {
        let root = fs.root().await
            .map_err(|e| StewardError::DataInit(TLogFSError::TinyFS(e)))?;
        _ = root.create_dir_path("/data").await
            .map_err(|e| StewardError::DataInit(TLogFSError::TinyFS(e)))?;
        Ok(())
    })
}).await?;
```

**After:**
```rust
ship.write_transaction(&meta, async |fs| {
    let root = fs.root().await?;
    _ = root.create_dir_path("/data").await?;
    Ok(())
}).await?;
```

#### Supporting Changes

**`From<tinyfs::Error> for StewardError`** — added in `crates/steward/src/lib.rs`
to bridge the two-hop error conversion chain. The existing `From` impls only
covered single hops (`tinyfs::Error → TLogFSError` and `TLogFSError → StewardError`),
but `?` can only convert through one `From` impl at a time. The new impl routes
through the intermediate type:

```rust
impl From<tinyfs::Error> for StewardError {
    fn from(e: tinyfs::Error) -> Self {
        StewardError::DataInit(tlogfs::TLogFSError::from(e))
    }
}
```

This is what eliminates all the manual `.map_err(...)` ceremony.

#### Migration Summary

| Location | Call Sites | Converted To |
|----------|-----------|--------------|
| `crates/steward/src/ship.rs` | 8 (1 prod + 7 test) | `write_transaction` |
| `crates/cmd/src/commands/mkdir.rs` | 1 | `write_transaction` |
| `crates/cmd/src/commands/hydrovu.rs` | 1 | `write_transaction` (also fixed `Vec<String>` type bug) |
| `crates/cmd/src/commands/copy.rs` | 5 (2 prod + 3 test) | `write_transaction` |
| `crates/cmd/src/commands/mknod.rs` | 1 | explicit `begin_write`/`commit` (needs `tx.state()`) |
| `crates/cmd/src/commands/cat.rs` | 3 (test) | `write_transaction` |
| `crates/cmd/src/commands/describe.rs` | 2 (test) | `write_transaction` |
| `crates/cmd/src/commands/show.rs` | 1 (test) | `write_transaction` |
| `crates/cmd/src/commands/control_test.rs` | 3 (test) | 2× `write_transaction`, 1× explicit (needs `tx.state()`) |
| `crates/steward/tests/debug_transaction_versions.rs` | 3 | `write_transaction` |
| `crates/steward/tests/ship_lifecycle_test.rs` | 3 | `write_transaction` |
| `crates/steward/tests/test_post_commit_factory.rs` | 1 | `write_transaction` |
| **Total** | **32** | **29** → `write_transaction`, **3** → explicit begin/commit |

The old `transact()` method has been removed. `replay_transaction()` (used by
remote restore) retains the old-style signature since it needs `(&StewardTransactionGuard, &FS)`.

All 88 cmd tests + 11 steward tests pass. Zero warnings.

---

## Remaining Phase 2A Tasks

### 2A-1/2A-2: Guard owns ProviderContext — COMPLETE

**Problem:** Getting a ProviderContext required `tx.state()?.as_provider_context()`,
which reaches into tlogfs internals. Every caller that needed a ProviderContext had
to know about `tlogfs::persistence::State`.

**Solution:** Added `provider_context()` method directly on `StewardTransactionGuard`.
Internally delegates to `self.state()?.as_provider_context()` for now, but the
public API no longer requires callers to touch `State`.

Also completed prerequisite cleanup:

- **Removed dead code:** `let _object_store = tx.object_store().await?` in temporal.rs
  (result was discarded with `_` prefix — pure dead code)
- **Eliminated `transaction_guard()?.state()?` indirection:** 4 call sites in
  describe.rs and cat.rs used `tx.transaction_guard()?.state()?` which is equivalent
  to `tx.state()?` — simplified all 4

#### Callers converted to `tx.provider_context()?`

| File | Sites | Notes |
|------|-------|-------|
| `cat.rs` | 1 | Was `tx.transaction_guard()?.state()?.as_provider_context()` |
| `control.rs` | 2 | Factory context creation (recovery + sync paths) |
| `mknod.rs` | 1 | Factory initialization |
| `run.rs` | 1 | Factory execution |
| `temporal.rs` | 1 | Version-specific table provider creation |
| `describe.rs` | 1 | Version stats querying |

#### Callers NOT converted (intentionally)

| File | Reason |
|------|--------|
| `export.rs` | Uses `tlogfs::TransactionGuard` (not steward guard) — inside `execute_direct_copy_query` |
| `test_executable_factory.rs` (2) | State variable reused for both `FS::new()` and `as_provider_context()` — converting just `as_provider_context()` would leave orphaned `state` usage |
| `test_post_commit_factory.rs` (3) | Same — state used for `FS::new()` too |
| `guard.rs` internal | Post-commit factory execution uses a separate `tlogfs::TransactionGuard` |

#### Remaining `.state()` usage in cmd production code after this task

| Caller | Purpose | Next task |
|--------|---------|-----------|
| `run.rs:73` | Redundant `FS::new(tx.state()?)` — guard already derefs to FS | Simple cleanup |
| `run.rs:97` | `state()?.get_factory_for_node(node_id)` | 2A-4 |
| `control.rs:503` | Redundant `FS::new(tx.state()?)` | Simple cleanup |
| `control.rs:528` | `state()?.get_factory_for_node(node_id)` | 2A-4 |
| `temporal.rs:45` | Redundant `FS::new(tx.state()?)` | Simple cleanup |
| `temporal.rs:117` | `state()?.query_records(node_id)` | Future |
| `hydrovu.rs:246` | `state` passed to `collector.collect_data(&state, &fs)` | Requires hydrovu API refactor |
| `describe.rs:353` | Passed to `tlogfs::get_file_schema(&root, path, &state)` | 2A-3 |
| `describe.rs:388` | Passed to `tlogfs::get_file_schema(&root, path, &state)` | 2A-3 |
| `export.rs:712` | Inside `tlogfs::TransactionGuard` for `as_provider_context()` | 2A-3 |

All 203 tests pass. Zero warnings.

### 2A-3: Refactor `execute_sql_on_file` / `get_file_schema` — NEXT PRIORITY

**Problem:** These functions live in `tlogfs::query::sql_executor` and take
`&mut TransactionGuard` (tlogfs) or `&State` (tlogfs). They only use these to
get a `SessionContext` and a `ProviderContext`. This couples cmd callers to
tlogfs types.

**What they actually need:**
- `execute_sql_on_file`: calls `tx.session_context()` and `tx.state()?.as_provider_context()` — both available from `ProviderContext`
- `get_file_schema`: calls `state.as_provider_context()` — trivially a `ProviderContext`

**Plan:** Change both signatures to take `&ProviderContext` directly.

**Current callers using tlogfs types through steward guard:**
- `copy.rs:633` — `tx.transaction_guard()?` passed to `execute_sql_on_file`
- `describe.rs:353` — `tx.state()?` passed to `get_file_schema` (series)
- `describe.rs:388` — `tx.state()?` passed to `get_file_schema` (table)
- `export.rs:712` — `tx.state()?` (inside `tlogfs::TransactionGuard`) for `as_provider_context()`

**Also eliminates `transaction_guard()` from cmd code:**
- `copy.rs:633` — last caller using `tx.transaction_guard()` for `execute_sql_on_file`
- `export.rs:156` — gets raw `TransactionGuard` for `discover_export_targets` and `export_target`

After this task, `transaction_guard()` will only be used from export.rs (which
passes the raw guard to deeper functions that themselves use `.state()`).

**Why this is the priority:** It's the only task that eliminates another guard
accessor (`transaction_guard()`) from cmd code AND reduces `state()` usage.
The change is mechanical and low-risk — the function bodies already have all the
info they need, just through the wrong parameter type.

### 2A-4: Move `get_factory_for_node` to guard or PersistenceLayer

**Problem:** Currently lives on `tlogfs::persistence::State`. Callers
must go through `tx.state()` -> `State` -> `get_factory_for_node()`. But the method
only needs persistence-layer operations (load node, read config bytes).

**Plan:** Either expose `get_factory_for_node()` directly on the steward guard
(thin wrapper), or move the implementation to `PersistenceLayer` trait. Hostmount
needs it too — on the host filesystem, the factory name would come from the YAML
config file itself.

**Current callers:**
- `run.rs:97` — `tx.state()?.get_factory_for_node(node_id)` (factory lookup)
- `control.rs:528` — `tx.state()?.get_factory_for_node(node_id)` (backup recovery)
- `guard.rs` internal — post-commit factory discovery (uses separate tlogfs transaction)

**Impact:** Eliminates 2 of the remaining 4 production `.state()` calls in cmd code.

### 2A-5: Move `get_commit_history` to `ControlTable`

**Problem:** Currently on `OpLogPersistence`, accessed through `tx.data_persistence()`.
It returns Delta Lake `CommitInfo` history. This is purely an audit/history query
that belongs on the control table, not on the data persistence layer.

**Plan:** Move to `ControlTable` where it belongs, keeping the guard's surface
area minimal.

**Affected callers:**
- `show.rs` — `show_filesystem_transactions()` (the only caller of `tx.data_persistence()`)

### ~~2A-7: Hide tlogfs internals behind concrete type~~ — DONE

**Problem:** Methods like `state()`, `transaction_guard()`, and `data_persistence()`
on `StewardTransactionGuard` expose tlogfs types directly. These cannot appear on
a trait.

**Result:**
- `state()` — made `pub(crate)`, all external callers (production + tests) migrated
- `data_persistence()` — made `pub(crate)`, zero external callers
- `transaction_guard()` — REMOVED (zero callers anywhere)
- `object_store()` — REMOVED earlier (dead code)
- Added `initialize_root_directory()` convenience method for the one test that needed `state()`
- Migrated 8 test callers of `state()` to use `tx.root()` (Deref) + `tx.provider_context()`

**Final public API:** See "Current Guard Public API (after 2A-7)" below.

**Final exposure:**

| Method | Visibility | Notes |
|--------|-----------|-------|
| ~~`transaction_guard()`~~ | **REMOVED** | Zero callers, deleted |
| ~~`object_store()`~~ | **REMOVED** | Dead code, deleted earlier |
| `state()` | `pub(crate)` | Used only by guard's own convenience methods |
| `data_persistence()` | `pub(crate)` | Used only by guard's own convenience methods |
| `session_context()` | `pub` | Clean -- backs DataFusion access |

---

### ~~Redundant `FS::new(tx.state()?)` calls~~ — DONE

Three sites replaced with `tx.root().await?` (using Deref<Target=FS>):
- ~~`run.rs:73`~~ — now `tx.root().await?`
- ~~`control.rs:503`~~ — now `tx.root().await?`
- ~~`temporal.rs:45`~~ — now `tx.root().await?`

The redundant calls created a second `FS` with its own `CachingPersistence`
wrapper — wasteful since the guard's deref chain already provides a cached FS.

### ~~HydroVu collector API~~ -- DONE

Removed dead `state` parameter from `collect_data()` and `collect_device()`.
The hydrovu.rs cmd now passes `&*tx` (deref to FS) directly instead of
creating a redundant `FS::new(state.clone())`.

---

## Priority Assessment

| Priority | Task | Eliminates | Risk |
|----------|------|-----------|------|
| ~~3~~ | ~~Remove redundant `FS::new()` calls~~ | ~~`state()` x3~~ | **DONE** |
| ~~4~~ | ~~Remove `object_store()` method~~ | ~~Dead method~~ | **DONE** |
| ~~1~~ | ~~2A-3: Refactor sql_executor signatures~~ | ~~`transaction_guard()` from copy.rs, `state()` from describe.rs x2~~ | **DONE** |
| ~~1~~ | ~~2A-4: Move `get_factory_for_node`~~ | ~~`state()` from run.rs, control.rs~~ | **DONE** |
| ~~2~~ | ~~2A-5: Move `get_commit_history` + `store_path`~~ | ~~`data_persistence()` from show.rs~~ | **DONE** |
| ~~+~~ | ~~Move `query_records`~~ | ~~`state()` from temporal.rs~~ | **DONE** |
| ~~1~~ | ~~Export.rs refactor~~ | ~~`transaction_guard()` + `state()` from export.rs~~ | **DONE** |
| ~~2~~ | ~~HydroVu refactor~~ | ~~`state()` from hydrovu.rs, dead `state` param~~ | **DONE** |
| ~~1~~ | ~~2A-7: Hide remaining internals~~ | ~~`state()` `pub(crate)`, `data_persistence()` `pub(crate)`, `transaction_guard()` REMOVED~~ | **DONE** |

**Phase 2A is COMPLETE.** All tlogfs internals are hidden behind `pub(crate)` or removed.
The guard's public API is now trait-ready for Phase 2B extraction.

---

## Phase 2B: Enum Dispatch (COMPLETE)

Phase 2B introduced `Steward` and `Transaction<'a>` enum types that wrap the
concrete `Ship` and `StewardTransactionGuard<'a>` types. All cmd production code
now uses the enum types; test code that directly tests Ship internals continues
using `Ship` directly (it remains public).

### Design Decision: Enum not Trait

User chose enum dispatch over trait objects. No new trait was introduced.
The `Steward` enum has a single `Pond(Ship)` variant; `Transaction<'a>` has a
single `Pond(StewardTransactionGuard<'a>)` variant. When the host filesystem
steward is implemented, a second variant will be added to each enum.

### New Types

```rust
// crates/steward/src/dispatch.rs

pub enum Steward {
    Pond(Ship),
    // Host(HostSteward) -- future
}

pub enum Transaction<'a> {
    Pond(StewardTransactionGuard<'a>),
    // Host(HostTransaction<'a>) -- future
}
```

### Steward Methods (forwarding to Ship)

- `create_pond()`, `open_pond()`, `create_pond_for_restoration()` -- constructors
- `begin_read()`, `begin_write()` -- returns `Transaction<'_>`
- `write_transaction()` -- scoped write with auto-commit/abort
- `control_table()`, `control_table_mut()` -- pond-specific
- `check_recovery_needed()`, `recover()` -- pond-specific
- `as_pond()`, `as_pond_mut()` -- escape hatch for Ship-specific operations

### Transaction Methods (forwarding to StewardTransactionGuard)

Backend-agnostic (kept on Transaction):
- `provider_context()`, `session_context()` -- data access
- `get_factory_for_node()` -- factory lookup (hostmount will need this)
- `commit()`, `abort()` -- lifecycle
- `as_pond()`, `as_pond_mut()` -- escape hatch for pond-specific operations
- `Deref<Target=FS>` -- filesystem access

Removed from Transaction in Phase 2C (access via `tx.as_pond().unwrap()`):
- ~~`txn_meta()`~~, ~~`transaction_type()`~~, ~~`is_write_transaction()`~~, ~~`is_read_transaction()`~~
- ~~`control_table()`~~
- ~~`query_records()`~~, ~~`get_commit_history()`~~, ~~`store_path()`~~
- ~~`initialize_root_directory()`~~

### Files Modified

| File | Change |
|------|--------|
| `crates/steward/src/dispatch.rs` | **NEW** -- `Steward` and `Transaction<'a>` enums with forwarding methods |
| `crates/steward/src/lib.rs` | Added `mod dispatch; pub use dispatch::{Steward, Transaction};` |
| `crates/cmd/src/common.rs` | `ShipContext::open_pond()`, `create_pond()`, `create_pond_for_restoration()` now return `Steward` |
| `crates/cmd/src/commands/cat.rs` | `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameter |
| `crates/cmd/src/commands/control.rs` | `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameter |
| `crates/cmd/src/commands/mknod.rs` | `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameter |
| `crates/cmd/src/commands/run.rs` | `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameter |
| `crates/cmd/src/commands/show.rs` | 3x `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameters |
| `crates/cmd/src/commands/describe.rs` | 5x `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameters |
| `crates/cmd/src/commands/export.rs` | 4x `StewardTransactionGuard<'_>` -> `Transaction<'_>` in parameters |
| `crates/cmd/src/commands/list.rs` | Updated Deref comment |

### Test Results

560 tests pass, 0 failures across steward, cmd, tlogfs, tinyfs, provider, hydrovu.

---

## Phase 2C: Transaction Pond-Specific Method Cleanup (COMPLETE)

Phase 2C narrowed the `Transaction` enum's public API to only backend-agnostic
methods, moving pond-specific operations behind an `as_pond()` downcast. This
makes `Transaction` ready for a `Host` variant without any pond-specific methods
that would need `unimplemented!()` stubs.

### Methods Removed from Transaction

| Removed Method | Reason |
|---------------|--------|
| `txn_meta()` | Pond metadata -- no callers in cmd |
| `transaction_type()` | Control table concept -- no callers in cmd |
| `is_write_transaction()` | Control table concept -- no callers in cmd |
| `is_read_transaction()` | Control table concept -- no callers in cmd |
| `control_table()` | Pond-specific audit table |
| `query_records()` | OpLog query -- tlogfs-specific |
| `get_commit_history()` | Delta Lake commit history -- tlogfs-specific |
| `store_path()` | On-disk tlogfs store path |
| `initialize_root_directory()` | OpLog root init -- tlogfs-specific |

### Methods Added to Transaction

| New Method | Returns |
|-----------|--------|
| `as_pond(&self)` | `Option<&StewardTransactionGuard<'a>>` |
| `as_pond_mut(&mut self)` | `Option<&mut StewardTransactionGuard<'a>>` |

### Methods Kept on Transaction (backend-agnostic)

| Method | Rationale |
|--------|----------|
| `provider_context()` | Every backend needs data access |
| `session_context()` | Every backend needs DataFusion |
| `get_factory_for_node()` | Hostmount will need factory support |
| `commit()` / `abort()` | Universal lifecycle |
| `Deref<Target=FS>` | Universal filesystem access |

### Call Sites Updated

| File | Call | Change |
|------|------|--------|
| `show.rs:46` | `tx.get_commit_history(None)` | `tx.as_pond().expect(...).get_commit_history(None)` |
| `show.rs:56` | `tx.store_path()` | `tx.as_pond().expect(...).store_path()` |
| `show.rs:91` | `tx.control_table()` | `tx.as_pond().expect(...).control_table()` |
| `show.rs:402` | `tx.control_table()` | `tx.as_pond().expect(...).control_table()` |
| `temporal.rs:111` | `tx.query_records(id)` | `tx.as_pond().expect(...).query_records(id)` |
| `control_test.rs:1420` | `tx.initialize_root_directory()` | `tx.as_pond().expect(...).initialize_root_directory()` |

### Unused Imports Cleaned Up in dispatch.rs

- Removed `TransactionType` (no longer dispatched)
- Removed `PondTxnMetadata` (no longer dispatched)
- Removed `PathBuf` (store_path removed)

### Test Results

560 tests pass, 0 failures across steward, cmd, tlogfs, tinyfs, provider, hydrovu.

---

## Phase 2D: Host Variant Added (COMPLETE)

Phase 2D added the `Steward::Host(HostSteward)` and `Transaction::Host(HostTransaction)`
variants, completing the enum dispatch abstraction. Commands can now operate on
either a pond or a host filesystem through the same `Steward` / `Transaction` API.

### New Files

| File | Purpose |
|------|--------|
| `crates/steward/src/host.rs` | `HostSteward` and `HostTransaction` structs |

### `HostSteward`

Lightweight steward wrapping a `PathBuf` root directory. No control table, no
audit log, no Delta Lake.

```rust
pub struct HostSteward {
    root_path: PathBuf,
}
```

- `new(root_path)` -- constructor (does not validate; validation happens in `begin()`)
- `root_path()` -- accessor
- `begin()` -- creates `HostmountPersistence` + `FS` + standalone `SessionContext`

### `HostTransaction`

Holds an `FS` (from hostmount persistence), a `SessionContext`, and the persistence
layer `Arc`. Implements `Deref<Target=FS>` for filesystem access.

```rust
pub struct HostTransaction {
    fs: FS,
    session: Arc<SessionContext>,
    persistence: Arc<dyn PersistenceLayer>,
}
```

- `provider_context()` -- constructs `ProviderContext` from session + persistence
- `session_context()` -- returns `Arc<SessionContext>`
- `get_factory_for_node(_id)` -- returns `Ok(None)` (stub for Phase 5)
- `Deref<Target=FS>` -- filesystem access

### Enum Dispatch Updates

**Steward enum:**
- Added `Host(HostSteward)` variant
- `open_host(root_path)` constructor
- `begin_read` / `begin_write` -- delegates to `host.begin()` (no read/write distinction)
- `write_transaction` -- creates host transaction, runs closure, no commit needed
- `control_table()` / `control_table_mut()` -- panics on Host (explicit contract)
- `check_recovery_needed()` / `recover()` -- no-ops on Host
- `as_host()` / `as_host_mut()` -- downcast accessors (mirrors `as_pond()`)

**Transaction enum:**
- Added `Host(HostTransaction)` variant
- `provider_context()` -- delegates to `host.provider_context()`
- `session_context()` -- delegates to `host.session_context()`
- `get_factory_for_node()` -- delegates with `tinyfs::Error` -> `TLogFSError` conversion
- `commit()` -- returns `Ok(Some(()))` (no-op: host writes are immediate)
- `abort()` -- returns `StewardError::Aborted` (no rollback mechanism)
- `Deref<Target=FS>` -- delegates to `host`
- `as_host()` / `as_host_mut()` -- downcast accessors

### Files Modified

| File | Change |
|------|--------|
| `crates/steward/src/host.rs` | **NEW** -- `HostSteward` and `HostTransaction` |
| `crates/steward/src/dispatch.rs` | Added `Host` variants to both enums, filled all match arms |
| `crates/steward/src/lib.rs` | Added `mod host; pub use host::{HostSteward, HostTransaction};` |

### Test Results

560 tests pass, 0 failures across steward, cmd, tlogfs, tinyfs, provider, hydrovu.

---

## Design Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Fix transact vs. remove it | Fix it (new `write_transaction`) | Less churn, clearer API, gradual migration |
| Error conversion | Add `From<tinyfs::Error>` for StewardError | Eliminates all manual `.map_err` ceremony |
| Async closure bound | `std::ops::AsyncFnOnce` | Stable in Rust 1.92 (not `async FnOnce` syntax which is still unstable) |
| Closure parameter | `&FS` only (not `&Guard + &FS`) | Most callers only need FS; callers needing guard use explicit begin/commit |
| `get_commit_history` | Move to ControlTable | Pure control-table query, doesn't belong on guard |
| `get_factory_for_node` | Move to PersistenceLayer | Only needs persistence ops; hostmount needs it too |
| Ship scope | Enum dispatch (`Steward::Pond(Ship)`) | Single type for CLI dispatch, no new traits |
| Control table on trait | No `control_table()` on enum signature contract | Hostmount has no control table; pond-specific callers access it through concrete type |
| Refactor order | Kill transact first, then provider_context, then signatures, then moves | Highest-value cleanup first, compile-check between each |
| `state()` on trait | Refactor away — not on trait | Exposes tlogfs internals; replaced by `provider_context()` |
| Pond-specific on Transaction | Remove, access via `as_pond()` downcast | Transaction must be Host-compatible; pond-specific methods accessed through concrete guard |
| Keep `get_factory_for_node` on Transaction | Yes | Hostmount will need factory support; may require refactoring for URL-based factory encoding |
| Host transaction semantics | No-op commit, `Aborted` error on abort | Host writes are immediate; no rollback mechanism exists |
| Host read/write distinction | None -- `begin()` for both | Host filesystem has no MVCC; read/write distinction is meaningless |
| `control_table()` on Host | Panics | Explicit contract: callers wanting control table must use `as_pond()` first |
| `FS::from_arc` vs `FS::new` for Host | `from_arc` (no caching wrapper) | Hostmount persistence reads from real filesystem; caching would serve stale data |
| `HostSteward` validation | Deferred to `begin()` | Constructor is infallible; `HostmountPersistence::new()` validates the directory |

---

## Current Guard Public API (after 2A-7 — FINAL)

From `crates/steward/src/guard.rs`:

```rust
impl StewardTransactionGuard {
    // Metadata
    pub fn txn_meta(&self) -> &PondTxnMetadata
    pub fn transaction_type(&self) -> TransactionType
    pub fn is_write_transaction(&self) -> bool
    pub fn is_read_transaction(&self) -> bool

    // Backend-agnostic access (TRAIT-READY)
    pub fn provider_context(&self) -> Result<ProviderContext>
    pub async fn session_context(&mut self) -> Result<Arc<SessionContext>>

    // Control table (pond-specific, NOT going on trait)
    pub fn control_table(&self) -> &ControlTable

    // Convenience methods (delegate to state/persistence internally)
    pub async fn get_factory_for_node(id) -> Result<Option<String>>
    pub async fn query_records(id) -> Result<Vec<OplogEntry>>
    pub async fn get_commit_history(limit) -> Result<Vec<CommitInfo>>
    pub fn store_path() -> Result<PathBuf>
    pub async fn initialize_root_directory() -> Result<()>

    // Internal only (pub(crate)):
    //   state()            -- used by convenience methods above
    //   data_persistence() -- used by convenience methods above
    //   take_transaction() -- used by commit/abort
    //   new()              -- constructor

    // REMOVED:
    //   transaction_guard() -- zero callers, deleted
    //   object_store()      -- dead code, deleted

    // Lifecycle
    pub async fn commit(self) -> Result<Option<()>, StewardError>
    pub async fn abort(self, error: impl Display) -> StewardError

    // Deref<Target=FS> -- provides filesystem access
}
```

**Target API after 2A:**

```rust
impl StewardTransactionGuard {
    // Metadata
    pub fn txn_meta(&self) -> &PondUserMetadata
    pub fn transaction_type(&self) -> TransactionType
    pub fn is_write_transaction(&self) -> bool
    pub fn is_read_transaction(&self) -> bool

    // Backend-agnostic access (TRAIT-READY)
    pub fn provider_context(&self) -> &ProviderContext    // NEW
    pub fn session_context(&self) -> &SessionContext       // KEPT

    // Pond-specific (concrete type only, not on trait)
    pub fn control_table(&mut self) -> &mut ControlTable   // KEPT, not on trait

    // Lifecycle (TRAIT-READY)
    pub async fn commit(self) -> Result<(), StewardError>
    pub async fn abort(self)

    // Deref<Target=FS> — provides filesystem access (TRAIT-READY)
}
```

## Files Modified in Phase 2A-7 (pub(crate) visibility + cleanup)

| File | Change |
|------|--------|
| `crates/steward/src/guard.rs` | `state()` -> `pub(crate)`, `data_persistence()` -> `pub(crate)`, `transaction_guard()` REMOVED (zero callers), added `initialize_root_directory()` convenience method |
| `crates/steward/tests/test_post_commit_factory.rs` | 6 sites: replaced `tx.state()` + `FS::new()` with `tx.root()` (Deref) + `tx.provider_context()` |
| `crates/cmd/tests/test_executable_factory.rs` | 2 sites: replaced `tx.state()` + `FS::new()` with `tx.root()` (Deref) + `tx.provider_context()` |
| `crates/cmd/src/commands/control_test.rs` | `tx.state()?.initialize_root_directory()` -> `tx.initialize_root_directory()` |

---

## Files Modified in Phase 2A-1/2A-2

| File | Change |
|------|--------|
| `crates/steward/src/guard.rs` | Added `provider_context()` method, doc comments on `state()` |
| `crates/cmd/src/commands/cat.rs` | `transaction_guard()?.state()?.as_provider_context()` -> `provider_context()?` |
| `crates/cmd/src/commands/control.rs` | 2 sites: `state()?.as_provider_context()` -> `provider_context()?` |
| `crates/cmd/src/commands/mknod.rs` | `state()?.as_provider_context()` -> `provider_context()?` |
| `crates/cmd/src/commands/run.rs` | `state()?.as_provider_context()` -> `provider_context()?` |
| `crates/cmd/src/commands/temporal.rs` | `state()?.as_provider_context()` -> `provider_context()?`, removed dead `object_store()` call |
| `crates/cmd/src/commands/describe.rs` | 3 sites: `transaction_guard()?.state()?` -> `state()?`, 1 site `state()?.as_provider_context()` -> `provider_context()?` |

---

## Files Modified in Phase 2A-4/2A-5 (convenience methods on guard)

| File | Change |
|------|--------|
| `crates/steward/src/guard.rs` | Added `get_factory_for_node()`, `query_records()`, `get_commit_history()`, `store_path()` convenience methods |
| `crates/cmd/src/commands/run.rs` | `tx.state()?.get_factory_for_node()` -> `tx.get_factory_for_node()` |
| `crates/cmd/src/commands/control.rs` | `tx.state()?.get_factory_for_node()` -> `tx.get_factory_for_node()` |
| `crates/cmd/src/commands/temporal.rs` | `tx.state()?.query_records()` -> `tx.query_records()` |
| `crates/cmd/src/commands/show.rs` | `tx.data_persistence()?.get_commit_history()` -> `tx.get_commit_history()`, `persistence.store_path()` -> `tx.store_path()` |

---

## Files Modified in Trivial Cleanup (FS::new + object_store removal)

| File | Change |
|------|--------|
| `crates/steward/src/guard.rs` | Removed dead `object_store()` method |
| `crates/cmd/src/commands/run.rs` | Replaced redundant `FS::new(tx.state()?)` with `tx.root()` via Deref |
| `crates/cmd/src/commands/control.rs` | Replaced redundant `FS::new(tx.state()?)` with `tx.root()` via Deref |
| `crates/cmd/src/commands/temporal.rs` | Replaced redundant `FS::new(tx.state()?)` with `tx.root()` via Deref |

---

## Files Modified in Export.rs Refactor

| File | Change |
|------|--------|
| `crates/cmd/src/commands/export.rs` | All internal pipeline functions changed from `&mut tlogfs::TransactionGuard` to `&StewardTransactionGuard` or `&ProviderContext`. Eliminated `transaction_guard()` and `state()` entirely. `execute_direct_copy_query` now takes `&ProviderContext`. |

## Files Modified in HydroVu Refactor

| File | Change |
|------|--------|
| `crates/hydrovu/src/lib.rs` | Removed dead `state: &State` parameter from `collect_data()` and `collect_device()` |
| `crates/hydrovu/src/factory.rs` | Updated `collect_data()` call to remove `&state` argument |
| `crates/cmd/src/commands/hydrovu.rs` | Uses `&*tx` (deref to FS) instead of `FS::new(state.clone())`, removed `state()` call |

---

## Files Modified in Phase 2A-3 (sql_executor signature refactor)

| File | Change |
|------|--------|
| `crates/tlogfs/src/query/sql_executor.rs` | `execute_sql_on_file`: `&mut TransactionGuard` -> `&ProviderContext`; `get_file_schema`: `&State` -> `&ProviderContext` |
| `crates/cmd/src/commands/copy.rs` | `tx.transaction_guard()?` -> `&tx.provider_context()?`, removed unnecessary `mut` |
| `crates/cmd/src/commands/describe.rs` | 2 sites: `tx.state()?` -> `&tx.provider_context()?` for `get_file_schema` calls |
| `crates/tlogfs/src/tests/mod.rs` | 2 sites: `&mut tx` -> `&tx.state()?.as_provider_context()` |

---

## Files Modified in Phase 2A-6

| File | Change |
|------|--------|
| `crates/steward/src/lib.rs` | Added `From<tinyfs::Error> for StewardError` |
| `crates/steward/src/ship.rs` | Added `write_transaction()`, removed `transact()`, converted 8 internal callers |
| `crates/cmd/src/commands/mkdir.rs` | Converted to `write_transaction` |
| `crates/cmd/src/commands/hydrovu.rs` | Converted + fixed type bug |
| `crates/cmd/src/commands/copy.rs` | Converted 5 callers |
| `crates/cmd/src/commands/mknod.rs` | Converted to explicit begin/commit |
| `crates/cmd/src/commands/cat.rs` | Converted 3 test callers |
| `crates/cmd/src/commands/describe.rs` | Converted 2 test callers |
| `crates/cmd/src/commands/show.rs` | Converted 1 test caller |
| `crates/cmd/src/commands/control_test.rs` | Converted 3 test callers |
| `crates/steward/tests/debug_transaction_versions.rs` | Converted 3 callers |
| `crates/steward/tests/ship_lifecycle_test.rs` | Converted 3 callers |
| `crates/steward/tests/test_post_commit_factory.rs` | Converted 1 caller |
