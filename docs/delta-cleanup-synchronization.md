# Delta Lake Maintenance: Coordination via Delta Protocol

## Problem

Both the local pond and the remote backup are Delta Lake tables that accumulate
small files and delta log entries without maintenance. There is no coordination
to prevent concurrent access during destructive operations (compact, vacuum,
log cleanup).

### Local pond (data/ and control/)
- 1,643 small parquet files (~61KB avg), never compacted
- 1,659 delta log JSON files, only 1 checkpoint, log cleanup ineffective
- Auto-maintenance runs checkpoint + vacuum but never compact
- No lock prevents `pond maintain --compact` from colliding with the 1-minute timer

### Remote table (S3)
- Every push creates new Delta versions with small parquet files (chunked backup records)
- No checkpoint, no vacuum, no compaction has ever run on the remote
- Multiple ponds could push concurrently to the same remote bucket
- No coordination mechanism exists

## Architecture: How the Steward Works Today

Understanding the current flow is essential. The steward manages two Delta Lake
tables (data and control) through several coordinated layers:

### Ship (steward/ship.rs)
- Owns `OpLogPersistence` (data table) and `ControlTable` (control table)
- Tracks `last_write_seq` for transaction sequence allocation
- `begin_txn()` allocates a sequence number, writes a Begin record to the control
  table, and opens a tlogfs TransactionGuard
- `maintain()` calls `maintenance::maintain_table()` on both data and control tables
- `commit_transaction()` calls `guard.commit()` then `self.maintain(false, false)`

### StewardTransactionGuard (steward/guard.rs)
- Wraps a tlogfs TransactionGuard with control table lifecycle tracking
- `commit()` commits the data transaction, records DataCommitted/Completed/Failed
  in the control table, then runs `run_post_commit_factories()` which:
  - Opens a NEW OpLogPersistence to discover /system/run/* factories
  - Executes each factory (e.g., remote push) with full lifecycle tracking
  - Records PostCommitPending/Started/Completed/Failed in control table

### ControlTable (steward/control_table.rs)
- A Delta Lake table partitioned by `record_category` (metadata, transaction, import)
- Stores TransactionRecord structs as Parquet rows via serde_arrow
- RecordType enum: Begin, DataCommitted, Completed, Failed,
  PostCommitPending/Started/Completed/Failed
- TransactionType enum: Read, Write, PostCommit
- Maintains cached pond_metadata, factory_modes, settings
- Each `write_record()` is an atomic Delta commit

### Current Maintenance Flow (maintenance.rs)
- `maintain_table(table, name, force, do_compact)` operates on any DeltaTable
- Runs: checkpoint (if forced or at interval) -> cleanup_metadata (if checkpoint
  was just created) -> vacuum (always) -> compact (if requested)
- Called from Ship in two contexts:
  1. Auto-maintain: `Ship::commit_transaction()` calls `self.maintain(false, false)`
     after each successful write -- IN-PROCESS, SEQUENTIAL, no concurrency risk
  2. Manual: `pond maintain [--compact]` calls `ship.maintain(true, compact)` --
     MAY collide with concurrent `pond run` processes

### Current CLI Commands
- `pond maintain [--compact]` -- opens pond, calls `ship.maintain(true, compact)`
- `pond log` -- queries control table for transaction history
- `pond sync [name]` -- discovers/executes remote factories in /system/run/
- `pond config [set key value]` -- reads/writes control table settings
- `pond control` -- hidden legacy interface for the above

## Proposed Design: Delta Protocol-Native Coordination

### Key Insight: Delta Lake's Transaction (appId) Mechanism

Delta Lake has a protocol-level primitive for exactly this: `Action::Txn(Transaction)`
with `(app_id, version, last_updated)`. When two concurrent commits both include a
`Txn` action with the **same app_id**, Delta's conflict checker raises
`CommitConflictError::ConcurrentTransaction` -- one writer wins, the other fails.

This is a **true compare-and-swap at the Delta protocol level**:
- Each writer reads version N, prepares changes, writes version N+1
- If another writer created N+1 first, the commit fails and must retry
- The `Txn` action adds app-level conflict detection ON TOP of file-level detection

From delta-rs `conflict_checker.rs`:
```
// Both transactions include Txn { app_id: "streaming_query_1" }
// Result: ConcurrentTransaction error for the loser
```

### delta-rs API Support

All maintenance operations support `with_commit_properties()`:
- `table.write(batches).with_commit_properties(props)` -- for record writes
- `table.optimize().with_commit_properties(props)` -- for compaction
- `table.vacuum().with_commit_properties(props)` -- for vacuum

The commit properties include application transactions:
```rust
CommitProperties::default()
    .with_application_transaction(
        Transaction::new("pond-maintenance", next_version)
    )
```

### Two-Layer Coordination

The design uses two complementary mechanisms:

1. **Hard lock (Delta Txn CAS)** -- All maintenance operations on a table include
   `Transaction::new("pond-maintenance-{table}", version)` in their commit
   properties. Two concurrent `pond maintain` commands will have one fail with
   `ConcurrentTransaction`. This works for BOTH local and remote (S3) tables.

2. **Soft signal (control table records)** -- `MaintenanceStarted`/`MaintenanceCompleted`
   records in the control table provide:
   - **Fast-fail for `begin_txn`**: A `pond run` starting during maintenance sees
     the record and fails immediately instead of doing work that may conflict
   - **Auditability**: `pond log` shows when maintenance ran and what it did
   - **Stale lock recovery**: `pond config set maintenance_lock off` clears orphans

Why both? The Delta Txn CAS prevents two maintenance processes from corrupting
tables. The control table signal prevents a `pond run` from starting work during
maintenance. Different failure modes, different solutions.

### Auto-Maintain is Exempt

`Ship::commit_transaction()` calls `self.maintain(false, false)` after every
successful write. This is IN-PROCESS and SEQUENTIAL -- it cannot collide with
itself or with the ongoing transaction. It does NOT need the maintenance lock
and does NOT write any lock records.

Only `pond maintain` (the explicit CLI command) uses the lock.

## Execution Order (Revised)

### Normal `pond run` (1-minute timer):
```
1. begin_txn           -- check control table for MaintenanceStarted,
                          fail fast if active
2. [factory work]      -- journal ingest, etc.
3. commit              -- writes to data table
4. post-commit push    -- backs up new version to remote (from guard.rs)
5. auto-maintain       -- checkpoint + vacuum on data + control
                          (NO compact, NO lock, no cleanup gating)
```

### `pond maintain [--compact]`:
```
LOCAL PHASE:
1. Write MaintenanceStarted record to control table
2. Try push                -- best-effort backup of pending versions
3. Maintain data table     -- checkpoint + cleanup(if push ok)
                              + vacuum + compact(if flag)
                              + Delta Txn("pond-maint-data", version)
4. Maintain control table  -- checkpoint + cleanup(always)
                              + vacuum + compact(if flag)
                              + Delta Txn("pond-maint-control", version)
5. Write MaintenanceCompleted record to control table

REMOTE PHASE (if reachable):
6. Maintain remote table   -- checkpoint + cleanup + vacuum + compact
                              + Delta Txn("pond-maint-remote", version)
```

If the remote is unreachable, phase 2 is deferred. Local maintenance proceeds
regardless, but log cleanup on the data table is skipped (logs may be needed
for next push).

## Push-Before-Cleanup Logic

The data table's delta log entries are needed for remote push (version diffing).
Cleaning them up before push means the remote factory can't determine which
versions are new.

**Rule**: Data table log cleanup is gated on `push_succeeded`:
- If push succeeds (or no remote factory exists) -> safe to clean up
- If push fails -> skip cleanup, logs preserved for next push attempt
- Control table cleanup always runs (not replicated)

The `do_cleanup` parameter on `maintain_table()` controls this:
```rust
pub async fn maintain_table(
    table: DeltaTable,
    table_name: &str,
    force: bool,
    do_compact: bool,
    do_cleanup: bool,           // NEW: gate log cleanup
    commit_props: Option<CommitProperties>,  // NEW: for Delta Txn
) -> (DeltaTable, MaintenanceResult)
```

Auto-maintain (called from `commit_transaction`) passes `do_cleanup=true` and
`commit_props=None` (no lock needed, cleanup is fine because push just ran).

Manual maintain passes `do_cleanup=push_succeeded` for data table and
`do_cleanup=true` for control table, with `commit_props` containing the
maintenance `Transaction` action.

## Implementation Details

### maintain_table with Delta Txn Support

```rust
pub async fn maintain_table(
    table: DeltaTable,
    table_name: &str,
    force: bool,
    do_compact: bool,
    do_cleanup: bool,
    commit_props: Option<CommitProperties>,
) -> (DeltaTable, MaintenanceResult) {
    // ... checkpoint as before ...

    // Log cleanup (gated by do_cleanup)
    if result.checkpoint_created && do_cleanup {
        // cleanup_metadata as before
    }

    // Vacuum with optional Delta Txn
    let mut vacuum_op = table.clone().vacuum()
        .with_retention_period(Duration::hours(0))
        .with_enforce_retention_duration(false);
    if let Some(ref props) = commit_props {
        vacuum_op = vacuum_op.with_commit_properties(props.clone());
    }
    // ... execute vacuum ...

    // Compact with optional Delta Txn
    if do_compact {
        let mut compact_op = table.clone().optimize()
            .with_type(OptimizeType::Compact)
            .with_target_size(COMPACT_TARGET_SIZE);
        if let Some(ref props) = commit_props {
            compact_op = compact_op.with_commit_properties(props.clone());
        }
        // ... execute compact ...
    }
}
```

### Maintenance Lock in Ship

```rust
impl Ship {
    /// Acquire maintenance lock by writing MaintenanceStarted to control table.
    /// Returns the version number used for Delta Txn coordination.
    pub async fn acquire_maintenance_lock(&mut self) -> Result<i64, StewardError> {
        // Check if maintenance is already active
        if self.control_table.is_maintenance_active().await? {
            return Err(StewardError::MaintenanceLock(
                "Maintenance already in progress".into()
            ));
        }
        let version = chrono::Utc::now().timestamp_micros();
        self.control_table.record_maintenance_started(version).await?;
        Ok(version)
    }

    /// Release maintenance lock.
    pub async fn release_maintenance_lock(&mut self, version: i64) -> Result<(), StewardError> {
        self.control_table.record_maintenance_completed(version).await
    }

    /// Try to push pending versions to remote. Returns true on success.
    pub async fn try_push(&mut self) -> bool {
        // Discover remote factory in /system/run/*
        // Execute push (best-effort)
        // Return success/failure
    }

    /// Run local maintenance with lock coordination.
    pub async fn maintain_locked(
        &mut self,
        force: bool,
        compact: bool,
        push_succeeded: bool,
        lock_version: i64,
    ) -> MaintenanceReport {
        let mut report = MaintenanceReport::default();

        // Build commit properties with Delta Txn for coordination
        let data_props = CommitProperties::default()
            .with_application_transaction(
                Transaction::new("pond-maint-data", lock_version)
            );
        let control_props = CommitProperties::default()
            .with_application_transaction(
                Transaction::new("pond-maint-control", lock_version)
            );

        // Maintain data table (cleanup gated on push success)
        let data_table = self.data_persistence.table().clone();
        let (new_data, data_result) = maintenance::maintain_table(
            data_table, "data", force, compact,
            push_succeeded,         // do_cleanup
            Some(data_props),       // Delta Txn coordination
        ).await;
        self.data_persistence.set_table(new_data);
        report.data = Some(data_result);

        // Maintain control table (always cleanup)
        let control_table = self.control_table.table().clone();
        let (new_control, control_result) = maintenance::maintain_table(
            control_table, "control", force, compact,
            true,                   // always cleanup control
            Some(control_props),    // Delta Txn coordination
        ).await;
        self.control_table.set_table(new_control);
        report.control = Some(control_result);

        report
    }
}
```

### Lock Check in begin_txn

```rust
async fn begin_txn(
    &mut self,
    is_write: bool,
    meta: &PondUserMetadata,
) -> Result<StewardTransactionGuard<'_>, StewardError> {
    // Check maintenance lock (write transactions only)
    if is_write && self.control_table.is_maintenance_active().await? {
        return Err(StewardError::MaintenanceLock(
            "Cannot start write transaction: maintenance in progress. \
             Retry after maintenance completes.".into()
        ));
    }

    // ... existing sequence allocation and begin logic ...
}
```

Note: Only write transactions check the lock. Read transactions (`pond list`,
`pond cat`, `pond log`) proceed normally during maintenance.

### Updated maintain_command

```rust
pub async fn maintain_command(ship_context: &ShipContext, compact: bool) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    // Phase 1: Acquire lock
    let lock_version = match ship.acquire_maintenance_lock().await {
        Ok(v) => v,
        Err(StewardError::MaintenanceLock(msg)) => {
            eprintln!("Cannot run maintenance: {}", msg);
            eprintln!("If maintenance crashed, run: pond config set maintenance_lock off");
            return Err(anyhow!(msg));
        }
        Err(e) => return Err(e.into()),
    };

    // Phase 2: Try push before cleanup
    let push_succeeded = ship.try_push().await;

    // Phase 3: Run local maintenance
    let report = ship.maintain_locked(true, compact, push_succeeded, lock_version).await;

    // Phase 4: Release lock
    ship.release_maintenance_lock(lock_version).await?;

    // Phase 5: Remote maintenance (if reachable, best-effort)
    let remote_result = match ship.try_remote_maintain(true, compact).await {
        Ok(result) => Some(result),
        Err(e) => {
            log::warn!("[MAINTAIN] Remote maintenance deferred: {}", e);
            None
        }
    };

    // Print combined report
    print_report(&report, &remote_result);
    Ok(())
}
```

### Remote Table Maintenance

```rust
impl RemoteTable {
    /// Run maintenance on the remote Delta table.
    /// Uses Delta Txn for concurrent-access coordination.
    pub async fn maintain(
        &mut self,
        force: bool,
        compact: bool,
    ) -> maintenance::MaintenanceResult {
        let version = chrono::Utc::now().timestamp_micros();
        let props = CommitProperties::default()
            .with_application_transaction(
                Transaction::new("pond-maint-remote", version)
            );

        let table = self.table.clone();
        let (new_table, result) = maintenance::maintain_table(
            table, "remote", force, compact, true, Some(props),
        ).await;
        self.table = new_table;

        // Re-register session context
        let _ = self.session_context.deregister_table("remote_files");
        let _ = self.session_context
            .register_table("remote_files", Arc::new(self.table.clone()));

        result
    }
}
```

If a ConcurrentTransaction error occurs during remote maintenance, it means
another pond is also maintaining the same remote -- the operation fails
gracefully and can be retried.

### Updated MaintenanceReport

```rust
pub struct MaintenanceReport {
    pub data: Option<MaintenanceResult>,
    pub control: Option<MaintenanceResult>,
    pub remote: Option<MaintenanceResult>,  // NEW
}
```

### New Error Variant

```rust
#[derive(Debug, Error)]
pub enum StewardError {
    // ... existing variants ...

    #[error("Maintenance lock: {0}")]
    MaintenanceLock(String),
}
```

### New Control Table Record Types

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordType {
    // ... existing variants ...
    MaintenanceStarted,
    MaintenanceCompleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    // ... existing variants ...
    Maintenance,
}
```

### Stale Lock Recovery

```
pond config set maintenance_lock off
```

This writes a MaintenanceCompleted record to the control table, clearing any
orphaned MaintenanceStarted record from a crashed maintenance process.

Implementation in `control.rs`:
```rust
if key == "maintenance_lock" && value == "off" {
    control_table.record_maintenance_completed(0).await?;
    println!("[OK] Maintenance lock cleared");
    return Ok(());
}
```

### is_maintenance_active Query

```sql
SELECT COUNT(*) as active FROM (
    SELECT
        MAX(CASE WHEN record_type = 'maintenance_started' THEN timestamp ELSE 0 END)
            as last_started,
        MAX(CASE WHEN record_type = 'maintenance_completed' THEN timestamp ELSE 0 END)
            as last_completed
    FROM transactions
    WHERE transaction_type = 'maintenance'
) WHERE last_started > last_completed
```

## Implementation Todos

### Phase 1: Core Infrastructure

1. **New enum variants** -- `RecordType::MaintenanceStarted/Completed`,
   `TransactionType::Maintenance` in control_table.rs

2. **`StewardError::MaintenanceLock`** -- new error variant in lib.rs

3. **`is_maintenance_active()`** -- SQL query in ControlTable, checks for
   unmatched MaintenanceStarted records

4. **`record_maintenance_started/completed()`** -- write methods in ControlTable

5. **`do_cleanup` + `commit_props` parameters on `maintain_table()`** -- gate
   log cleanup and inject Delta Txn actions

6. **Lock check in `begin_txn`** -- write transactions only, calls
   `is_maintenance_active()` before sequence allocation

### Phase 2: Locked Maintenance + Push

7. **`Ship::acquire/release_maintenance_lock()`** -- wraps control table methods
   with pre-check

8. **`Ship::try_push()`** -- best-effort remote push, discovers remote factory
   in /system/run/*, executes, returns bool

9. **`Ship::maintain_locked()`** -- combines data + control maintenance with
   push_succeeded gating and Delta Txn commit properties

10. **Update `maintain_command()`** -- full lock lifecycle with push-before-cleanup

### Phase 3: Remote Table Maintenance

11. **`RemoteTable::maintain()`** -- calls `maintain_table()` on the remote
    Delta table with `pond-maint-remote` Txn action

12. **`Ship::try_remote_maintain()`** -- discovers remote config, opens remote
    table, calls maintain, handles errors gracefully

13. **Wire remote into `maintain_command()`** -- after local phase, attempt remote

14. **Add `remote` field to `MaintenanceReport`**

### Phase 4: Recovery + CLI

15. **Stale lock recovery via `pond config set maintenance_lock off`** -- special
    handling in control.rs set_pond_config

16. **Update `pond log` output** -- show maintenance records alongside transactions

### Phase 5: Tests and Documentation

17. **Test: concurrent `pond maintain`** -- two processes, one gets
    ConcurrentTransaction

18. **Test: `pond run` during maintenance** -- fails fast with MaintenanceLock

19. **Test: push-before-cleanup** -- cleanup skipped when push fails

20. **Test: compaction** -- `pond maintain --compact` reduces file count

21. **Test: remote maintenance** -- remote table gets checkpointed

22. **Update docs** -- cli-reference.md, deltalake-efficiency.md

## Files to Change

| File | Changes |
|------|---------|
| `crates/steward/src/control_table.rs` | MaintenanceStarted/Completed record types, is_maintenance_active(), record_maintenance_started/completed() |
| `crates/steward/src/lib.rs` | `StewardError::MaintenanceLock` |
| `crates/steward/src/ship.rs` | Lock check in begin_txn (write only), acquire/release_maintenance_lock, try_push, maintain_locked |
| `crates/steward/src/maintenance.rs` | `do_cleanup` + `commit_props` params, remote field in report |
| `crates/remote/src/table.rs` | `RemoteTable::maintain()` with Delta Txn |
| `crates/cmd/src/commands/maintain.rs` | Full lock lifecycle, push, remote maintain |
| `crates/cmd/src/commands/control.rs` | Stale lock recovery in set_pond_config |
| `testsuite/tests/` | New test scripts |
| `docs/cli-reference.md` | Maintain command docs |

## Design Decisions

1. **Two-layer coordination** -- Delta Txn CAS provides hard mutual exclusion
   between maintenance processes. Control table records provide soft fast-fail
   signaling for normal operations and audit trail for operators.

2. **Auto-maintain is exempt** -- The in-process `Ship::commit_transaction()`
   auto-maintain cannot collide and does not need locks or records. Only the
   explicit `pond maintain` CLI command participates in coordination.

3. **Write-only lock check** -- `begin_txn` only checks the maintenance lock
   for write transactions. Read transactions (`pond list`, `pond cat`, `pond log`)
   proceed normally during maintenance.

4. **Push-before-cleanup** -- Data table log cleanup is gated on push success.
   Control table cleanup always runs. Auto-maintain always cleans up (push just
   ran in the post-commit phase).

5. **Stale lock recovery** -- `pond config set maintenance_lock off` writes a
   MaintenanceCompleted record to clear orphaned locks from crashed processes.

6. **Remote maintenance is best-effort** -- If the remote is unreachable, local
   maintenance proceeds normally (with log cleanup deferred for data table).
   Remote maintenance is attempted after the local phase and deferred on failure.

7. **Compact is manual-only** -- No auto-compact. Only `pond maintain --compact`
   triggers compaction. This avoids unexpected long-running operations during
   normal cron cycles.

8. **Separate Delta Txn app_ids per table** -- `pond-maint-data`,
   `pond-maint-control`, `pond-maint-remote` are independent. Maintaining the
   data table doesn't block maintaining the control table.
