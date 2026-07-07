# Write-path inefficiency: logfile-ingest prefix re-hash is O(history)

Status: **implemented** on `jmacd/101`. The hot per-tick full-prefix re-hashes
described below were replaced with a bao-tree frontier-resume verification that
reads at most one trailing `BLOCK_SIZE` block. The original analysis is retained
below for context; see the "Implementation" section at the end for what actually
landed. Citations use `crate/file.rs:line` and were verified against the working
tree when written.

This document hands off the one remaining per-tick `O(history)` operation on the
**write path** that we identified after the sitegen read-path work (P2/P3/P4).
Everything else in the steady-state write substrate is already `O(delta)` (see
`archive/efficiency-dataflow.md` section 2). This one is not, and it grows
without bound as an active logfile ages.

## 1. Symptom

The logfile-ingest factory re-reads and re-hashes the **entire tracked prefix**
of every growing active file on **every tick**, purely to verify the file was
not rotated out from under us. For a journald/weblog file that grows steadily
between rotations, per-tick cost is proportional to the current file size, i.e.
`O(history)` in the age of the current logfile generation, not `O(delta)` in the
bytes that arrived this tick.

Worse: for the common "active file grew" case there are **two** independent
full-prefix re-hashes per tick, at two different call sites (see section 3).

## 2. Where it lives

`crates/provider/src/factory/logfile_ingest.rs`. The append path is otherwise
correct and cheap: it seeks to `cumulative_size`, reads only the new tail bytes,
and writes them as a new `FilePhysicalSeries` version. TinyFS maintains the
cumulative blake3 and bao outboard incrementally on write. The waste is entirely
in the *verification* step, not the append itself.

### 2a. Call site A -- rotation detection, "active file grew" branch

`logfile_ingest.rs:266-291` (Step 3, rotation detection). When the host active
file is larger than the pond's tracked `cumulative_size`, the code reads the
first `cumulative_size` bytes back off disk and re-hashes them to decide whether
this is a normal append (prefix matches) or a rotation that already grew past the
old size (prefix changed):

```rust
let mut prefix_content = vec![0u8; pond_active.cumulative_size as usize];
prefix_file.read_exact(&mut prefix_content).map_other()?;
let mut hasher = IncrementalHashState::new();
hasher.ingest(&prefix_content);
let host_blake3 = hasher.root_hash().to_hex().to_string();
if host_blake3 == pond_active.blake3 { /* normal append */ } else { /* rotation */ }
```

The same-size branch (`logfile_ingest.rs:238-266`) does the identical full-prefix
read+hash. That one is less hot (same-size ticks are rare) but shares the fix.

### 2b. Call site B -- `ingest_append` prefix verification

`logfile_ingest.rs:817-840`. After rotation detection decides "this is an
append", `ingest_append` re-reads and re-hashes the whole prefix **again** to
guard against rotation between the size check and the read:

```rust
let mut prefix_content = vec![0u8; pond_state.cumulative_size as usize];
prefix_file.read_exact(&mut prefix_content).map_other()?;
let mut hasher = IncrementalHashState::new();
hasher.ingest(&prefix_content);
let prefix_blake3 = hasher.root_hash().to_hex().to_string();
if prefix_blake3 != pond_state.blake3 {
    return Err(/* "File may have been rotated during ingestion." */);
}
```

So a steady growing active file pays **2 x full-prefix read + full-prefix bao
hash** per tick.

### 2c. `find_rotated_file` -- acceptable, do not "fix"

`logfile_ingest.rs:865+` reads the full prefix of candidate archived files during
actual rotation. Rotation is a rare event (once per logrotate cycle), so this is
`O(history)` but not per-tick-hot. Leave it. Trying to make rotation matching
cheaper is a separate, lower-value task and risks correctness.

## 3. Why it's safe to make this O(delta)

The pond already stores, on the active file's `OplogEntry`, everything needed to
verify a growing prefix without re-reading it:

- `cumulative_size` and the cumulative `blake3` root.
- `bao_outboard` -> `SeriesOutboard` carrying the incremental **frontier**
  (`crates/utilities/src/bao_outboard.rs:151`, `:191`), i.e. the hashes of all
  complete stable subtrees of the tracked prefix.

`IncrementalHashState::resume(frontier, cumulative_size, verified_pending)`
(`bao_outboard.rs:932`) reconstructs the hash state from that frontier plus the
**partial trailing block only** (`verified_pending`, length
`cumulative_size % BLOCK_SIZE`). Feeding the new tail bytes and calling
`root_hash()` yields the new cumulative root. This is exactly the resume path the
write side already uses (`bao_outboard.rs:466`, `from_incremental_state` at
`:583`).

Cost of the frontier + partial-block resume is `O(delta + log(history))`, with a
bounded constant read of at most one `BLOCK_SIZE` trailing block off the host
file -- not the whole prefix.

## 4. Proposed change

Replace both hot full-prefix re-hashes (2a and 2b) with a frontier-resume
verification:

1. Load the active file's stored `SeriesOutboard` frontier and `cumulative_size`
   from the pond `OplogEntry` (already fetched into `PondFileState`; extend it to
   carry the frontier / raw `bao_outboard` bytes if not already present).
2. Read from the host file **only** the trailing partial block
   `[floor(cumulative_size / BLOCK_SIZE) * BLOCK_SIZE, cumulative_size)` as
   `verified_pending`, plus the new tail bytes `[cumulative_size, snapshot_size)`.
3. `resume(frontier, cumulative_size, verified_pending)`, then `ingest(new_tail)`,
   then compare `root_hash()` against the host content:
   - To distinguish **append** from **rotation**, first resume with only
     `verified_pending` and check `root_hash() == pond_state.blake3`. If the
     partial trailing block plus frontier no longer reproduce the stored root,
     the prefix changed -> rotation path.
   - If it matches, the append is verified; write the new version as today.
4. `ingest_append` (call site B) then does **not** need a second full re-hash --
   the resume in step 3 already verified the prefix. Collapse the two call sites
   so the prefix is verified once per tick.

### Correctness note (read carefully -- user prefers hard-fail over soft fallback)

Frontier-resume verifies the prefix **cryptographically against the pond's own
committed hashes**, not against a fresh full re-read of the host prefix. This
detects:

- any change to the size / trailing-block relationship (the overwhelmingly
  common rotation signal), and
- any change to the trailing partial block.

It does **not** re-read interior prefix blocks, so a same-size, same-trailing-
block file whose *interior* bytes were rewritten would not be caught by the fast
path. That is astronomically unlikely for append-only logs and is already the
pond's committed content. If we want to keep the strict "interior could have been
tampered" guarantee for the rare same-size case, keep the existing full-prefix
check **only** in the same-size branch (2a same-size) and apply the fast
frontier-resume path only to the hot "grew" branch. That preserves today's
strongest guarantee exactly where it is cheap to do so, and removes the O(history)
cost from the branch that dominates per-tick work.

On any verification failure, **hard-fail** with the existing rotation error
message (`logfile_ingest.rs:829`); do not silently fall back to a full re-scan.

## 5. Acceptance / tests

- New unit test: steady append across N ticks does **not** grow per-tick bytes
  read from the host beyond `BLOCK_SIZE + delta` (assert via a counting reader or
  by measuring the slice bounds requested).
- Existing rotation tests must still pass: shrink, same-size-different-content,
  grew-past-old-size rotation, and archived-file matching.
- Backfill/out-of-order is not relevant here (logfiles are append-only), but the
  cumulative `root_hash()` after resume must byte-match the legacy full-hash for
  identical content (golden test on a multi-block file).
- Confirm `SeriesOutboard` frontier is actually populated for active files < and
  > 16KB; below the outboard threshold the frontier is empty and resume degrades
  to hashing the whole (tiny) prefix, which is fine.

## 6. Out of scope

- Read-path / sitegen export incrementality (covered by the P2/P3/P4 work on
  `jmacd/102`; see `archive/sitegen-export-memory.md` and
  `archive/incremental-rollup-implementation.md`).
- `find_rotated_file` rotation-time full reads (section 2c) -- rare, leave as-is.
- Maintenance/compaction/sync hotspots -- see `archive/efficiency-priorities.md`.

## 7. Implementation (landed on `jmacd/101`)

Implemented in `crates/provider/src/factory/logfile_ingest.rs`:

- `PondFileState` now carries `frontier: Option<Vec<(u32, [u8; 32], u64)>>`,
  populated from the `SeriesOutboard.incremental.frontier` that was already being
  parsed to extract `cumulative_size`. No extra read to obtain it.
- New helper `verify_prefix_matches(host_path, pond_state) -> (bool, host_root)`:
  seeks to `floor(cumulative_size / BLOCK_SIZE) * BLOCK_SIZE`, reads only the
  trailing partial block as `verified_pending`, calls
  `IncrementalHashState::resume(frontier, cumulative_size, verified_pending)`, and
  compares `root_hash()` against the committed `blake3`. Cost is
  `O(delta + log history)` with a bounded read of at most one `BLOCK_SIZE` block.
- Both hot call sites (rotation-detection "grew" branch and `ingest_append`'s
  TOCTOU guard) now use this helper.

Two deliberate deviations from the proposal above:

- **The same-size branch keeps the full-prefix read** (section 4 correctness
  note), preserving the strict interior-tamper guarantee exactly where it is
  cheap and rare.
- **The two call sites were NOT collapsed.** `ingest_append`'s re-verify is a
  genuine TOCTOU guard against rotation between the directory scan and the append
  read; with the fast path it costs a single bounded trailing-block read, so
  keeping it preserves defense-in-depth at negligible cost rather than trading it
  away for a marginal saving.

**Missing-frontier policy:** when no frontier is stored (a legacy entry with no
`bao_outboard`) the helper falls back to the historical full-prefix read. This is
a *precondition* fallback (the fast path needs a frontier), not a *verification*
fallback: a genuine hash mismatch still hard-fails via the existing rotation
error. FilePhysicalSeries always writes an outboard, so this path is only reached
by degraded/legacy rows.

Tests: unit tests `verify_prefix_matches_*` in the module (multi-block parity +
rotation, appended-tail ignored, sub-block empty-frontier, no-frontier fallback);
e2e suite green (`030,031,032,033,048,632,633,634`), with `633` exercising the
grew-past-old-size rotation path directly.
