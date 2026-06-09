# Remote redesign review findings (branch `jmacd/52`)

Code review of the remote-redesign branch (`jmacd/52`) and
`docs/remote-redesign.md`, recorded 2026-06-08.

## Baseline health

- `cargo build --workspace` -- clean.
- `cargo clippy --workspace --all-features -- -D warnings` -- clean.
- `cargo test --workspace` -- 1467 passed, 0 failed, 9 ignored
  (the 9 ignored require an S3/MinIO runtime).

Scope reviewed: 3 new crates (`sync-store`, `sync-steward`,
`sync-remote`), the rewritten `crates/steward/`, and the tlogfs
`(pond_id, part_id)` partitioning change (~37k insertions vs `main`).

The design doc is accurate: the phase table matches the actual
commits, and the "Open after D5.8" section honestly enumerates the
real carry-forwards (mirror-restart bootstrap is Rust-only; foreign
`pond_id` physical vacuum deferred).

## Verified correct (no action)

- **`tlogfs/src/transaction_guard.rs` `unsafe` block** -- the
  `ManuallyDrop` + `ptr::read` used to return `&'a mut OpLogPersistence`
  from `commit()` is sound: each field is read exactly once, `inner`
  is dropped explicitly, and the bypassed `Drop` only logs and nulls
  `state`/`fs`, which `commit()` replicates manually.
- **`peek_pond_id` / `reconstruct_txn_history`** (pond-identity
  recovery, `tlogfs/src/persistence.rs`) -- refuses pre-D5 layouts and
  walks history oldest-first for the first non-`apply_pulled_bundle`
  commit to disambiguate the local `pond_id`.
- **Large-file (D5.9) replication** -- blobs are re-enumerated
  per-bundle from each Add's parquet, so a referencing parquet and its
  blobs always travel together; compact bundles re-scan and re-emit.
  No cross-push dedup that could strip a blob.
- **Post-commit factory lifecycle records** (`steward/src/guard.rs`)
  -- Begin/DataCommitted/Completed are emitted, so `Remote::push`
  will not drop the bundle as `NoSuchCommit`; the fresh persistence is
  opened only after the outer transaction commits (single-guard rule
  respected).

## Findings

Ordered highest-value first.

### 1. [Security / design mismatch] S3 secrets are stored in replicated data; the A5 `${env:VAR}` credential model is not wired for remotes

- **Files:** `crates/steward/src/remote_config.rs:30-55` and `:61-88`;
  all `to_storage_options()` call sites
  (`crates/steward/src/remote_adapter.rs:925`,
  `crates/cmd/src/commands/remote.rs:217`, `:510`,
  `crates/cmd/src/commands/pull.rs:65`,
  `crates/cmd/src/commands/verify.rs:78`,
  `crates/cmd/src/commands/restart_from_compact.rs:54`).
- **Severity:** Medium (security; depends on operator usage).
- **Problem:** `RemoteAttachment` derives `Serialize` with
  `skip_serializing_if = "String::is_empty"`, so a literal
  `secret_access_key` is written into the YAML at `/sys/remotes/<name>`
  -- an oplog row that `Remote::push` replicates to every backup. The
  struct doc comment claims *"Pushing this config to a backup is
  safe."* Design decision A5 says credentials should be `${env:VAR}`
  references ("text replicates, env resolves locally per replica"), but
  **none** of the `to_storage_options()` call sites apply env
  substitution -- only factory configs do, via
  `crates/steward/src/guard.rs:875`. So `${env:VAR}` placed in a secret
  field would be passed literally to S3 and fail auth, effectively
  forcing operators to store the literal secret, which then replicates.
- **Fix:** Expand `${env:VAR}` in `to_storage_options()` (or reject
  literal secrets at `pond remote add` time and require an env ref),
  and correct the "safe to push" comment.
- **Status:** Resolved. `to_storage_options()` is now fallible and
  expands `${env:VAR}` per field (unset refs error instead of leaking a
  placeholder to S3); `pond remote add`/`backup add` rejects a literal
  `secret_access_key` and requires an env reference; the struct doc
  comment and `docs/cli-reference.md` examples were corrected.

### 2. [Correctness] Idempotent re-push never advances `last_pushed_seq`

- **File:** `crates/sync-remote/src/remote.rs:348-352`.
- **Severity:** Medium.
- **Problem:** When a manifest already exists on the remote for a
  `txn_seq` (the documented post-crash reconcile window), `push`
  returns `Ok(())` without `config_set` on the watermark -- unlike the
  success path (`:373-380`) and the `data_delta_version == 0`
  bootstrap-skip (`:337-339`), which both advance it. If a crash leaves
  the watermark at N while bundles N+1..M are already committed
  remotely, every subsequent `pond push` re-loops and idempotent-skips
  them, pinning the watermark at N -- producing false push-lag in
  `pond status` and a redundant probe per stuck seq. It self-heals only
  when a new real commit is pushed (the `max` guard then jumps the
  watermark forward).
- **Fix:** In the idempotent-skip branch, advance `last_pushed_seq` to
  `max(current, txn_seq)`, mirroring the success and bootstrap-skip
  branches.
- **Status:** Resolved. The watermark-advance logic is now a shared
  `Remote::advance_last_pushed_seq` helper called by all three exit
  paths (success, bootstrap-skip, idempotent-skip), so a confirmed-on-
  remote bundle always advances the watermark. Regression test:
  `crates/sync-remote/tests/push.rs::idempotent_re_push_advances_last_pushed_seq_watermark`
  (rewinds the watermark to simulate the crash window, re-pushes, and
  asserts the watermark is repaired).

### 3. [Convention] Silent `unwrap_or(0)` on corrupt or erroring watermarks

- **Files:** `crates/sync-remote/src/remote.rs:334` and `:375`
  (`parse::<i64>().unwrap_or(0)`);
  `crates/steward/src/remote_adapter.rs:931-940`
  (`.ok().flatten()...unwrap_or(0)` -- the `.ok()` also eats a genuine
  control-table read error).
- **Severity:** Low/Medium.
- **Problem:** A present-but-unparseable `last_pushed_seq` value (or a
  real read error in the steward driver) is silently coerced to `0`,
  resetting the push to seq 1 and re-enumerating the entire transaction
  history instead of surfacing the fault. `pull` already handles the
  identical parse correctly, returning `RemoteError::Schema`
  (`remote.rs:442-447`). This violates the repo's no-silent-fallback
  rule.
- **Fix:** Use the same `.map_err(...) -> RemoteError::Schema` /
  `adapt_err` treatment as `pull`; only `Ok(None)` should mean `0`.

### 4. [Robustness, low-probability] `apply_pulled_bundle` discards a snapshot error

- **File:** `crates/steward/src/remote_adapter.rs:688-691`.
- **Severity:** Medium consequence, low probability.
- **Problem:** The base snapshot is obtained with
  `table.snapshot().ok()`. `None` is the correct base for a fresh
  replica, but on an existing populated replica a transient `snapshot()`
  `Err` is discarded and `CommitBuilder::build(None, ...)` proceeds as
  if there were no base state -- committing `Add`/`Remove` actions
  (including removes of files that exist in the real snapshot) with no
  reference to the current version. The legitimate `None` case and the
  error case are indistinguishable.
- **Fix:** Distinguish "no snapshot because the table is empty/new"
  (legitimate `None`) from a `snapshot()` `Err`, and propagate the
  latter rather than silently committing against no base.

## Assessment

None of these block the architecture, which is solid and well-tested.
Finding #1 (credential exposure across replicas) is the one to fix
before any production cutover.
