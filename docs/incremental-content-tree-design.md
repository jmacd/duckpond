# Incremental Content Tree: O(change) Commits

> **Status:** Active design, implementation in progress. **Supersedes**
> [`content-addressed-pond-design.md`](./content-addressed-pond-design.md),
> which remains a valid description of the object model and transparency log but
> describes per-commit hashing costs that this note replaces. Where the two
> disagree about *how* the commit spine is computed and stored, **this document
> wins**. The object model (blobs, trees, commits, the transparency log file
> layout) is carried forward unchanged.

---

## 0. Implementation progress

Phases track the plan in Section 8; each is independently reviewable.

| Phase | Scope | Status |
|---|---|---|
| Tier 0 | Single narrow post-commit scan (no inline blob bytes) + checksum row-leaf reset | **Done** |
| 2 | Reserved delta-versioned index node (persisted node manifest) | **Done** |
| 3 | Incremental node-keyed Merkle (updater + `O(n)` rebuilder oracle) | Not started |
| 4 | Incremental commit fold (both roots from changeset + index node) | Not started |
| 5 | `commit_object` reset (flat `manifest_hash` -> Merkle root) | Not started |
| 6 | Validation (equivalence tests; keep tlog/pull/719 green; presubmit) | Not started |

**Tier 0 (done).** Landed on branch `jmacd/65`. The two full-table `SELECT *`
post-commit scans (content-tree fold + partition checksums) are now one shared
**narrow** scan that never reads inline file `content`/`bao_outboard` bytes; the
partition-checksum row leaf was redefined to exclude those redundant bytes (a
coordinated Decision D2 reset). `commit_object`, `root_tree_hash`, the node
manifest, and every transparency-log byte are unchanged -- only the
partition-checksum *values* reset. Code:

- `content_tree::scan_live_rows` / `scan_live_rows_ctx` -- narrow projection
  (`arrow_cast(NULL, 'Binary')` for the two byte columns) plus a `WHERE blake3
  IS NULL` query that splices back the small `content` of the only rows the fold
  decodes (directories, symlinks, dynamic-node configs).
- `content_tree::fold_rows` -- split out of the old `scan_and_fold`; fold logic
  unchanged.
- `content_tree::compute_commit_snapshot` -- one scan yielding both content
  roots **and** the partition checksums; `assemble_commit_spine` does only the
  parent lookup. The guard's main commit path now scans once instead of twice.
- `remote_adapter::row_leaf_digest` -- the single leaf definition, shared by the
  per-commit checksum and `fsck`; `partition_checksums_from_rows` computes from
  already-scanned rows.
- Unit tests cover the narrow-scan splice and full-vs-narrow leaf equivalence;
  full workspace suite, clippy, and fmt are green.

See Section 7 for the rationale and Section 6 for the cost table.

**Phase 2 (done).** The reserved node-manifest index node is implemented
(Approach A: written in-transaction, pre-commit). One well-known `FileID`
(`tinyfs::INDEX_NODE_UUID = 00000000-0000-7700-8000-000000000000`, the `0x7`
low nibble in byte 6 encoding `FilePhysicalSeries`) holds the full node manifest
as a raw-byte series node named `.pond-node-index` under the data-FS root. Each
write transaction folds the in-transaction live state (committed rows plus this
transaction's pending records and synthesized modified-directory snapshots) into
the complete manifest and appends it as one index-node version; a *collapsing*
write keeps the node at a single live version, so it never becomes a
user-visible compaction candidate. The reserved node is excluded from the fold
(so `root_tree_hash` and the manifest are unchanged by its presence) and hidden
from all user-facing directory enumeration. Phase 2 still keeps the post-commit
full-rescan fold for the committed roots; Phase 4 retires that and switches the
index node from a full manifest to a touched-only delta. Code:

- `tinyfs::INDEX_NODE_UUID` / `index_node_uuid()` / `NodeID::is_index()`
  (`node.rs`); enumeration filter in `WD::get_entries`/`entries` (`wd.rs`).
- `tlogfs::persistence::State::uncommitted_live_rows` -- non-mutating pending +
  synthesized modified-directory rows for the in-transaction fold.
- `content_tree::in_txn_manifest_bytes` -- merges committed + uncommitted rows,
  orders latest-version-wins, folds, and encodes the manifest; `hash_directory`
  skips the reserved node id (fold exclusion).
- `guard::write_index_node` -- called in `commit()` before `data_tx.commit()`
  for write transactions; `create_file_with_id` on first commit, collapsing
  series write thereafter. Unit test `ship::test_index_node_phase2`.

---

## 1. Why this document exists

The content-addressed design computes a `root_tree_hash` and a
`node_manifest_hash` for every commit; both are the leaf of the transparency
log and the tip of the content tree. The original note idealized an
incremental fold but the shipped commit path does not implement it, and one of
the two hashes could not be made incremental as specified at all. The result is
that **every commit costs `O(#nodes)`** regardless of how small the change is.

This document specifies a commit path whose cost is proportional to the
**change**, not the pond size, while keeping the content-addressed object model
and the transparency log byte-for-byte compatible in layout.

The single organizing idea:

> **All derived per-node hashes live in one reserved, delta-versioned index
> node inside the data filesystem. Directories are never rewritten for a
> descendant change, both content roots (`root_tree_hash` and the manifest
> root) update along the touched path only, and the transparency log is
> unaffected.**

---

## 2. What is wrong today (measured)

Three independent `O(#nodes)` costs run on every commit:

1. **`content_tree::scan_and_fold`** issues
   `SELECT * FROM content_live ORDER BY pond_id, part_id, node_id, version`
   -- a full read of every row and version, including inline file `content`
   bytes (up to the 64 KB large-file threshold) that the fold never uses --
   then folds the whole tree bottom-up.
2. **`remote_adapter::compute_live_checksums_for_table`** issues a second full
   `SELECT *` over the same rows in the same order, for partition checksums.
3. **`manifest_hash`** (`sync-store/content/manifest.rs`) is a **flat** hash
   over the entire sorted list of node entries, re-encoded and re-hashed every
   commit inside `compute_commit_roots_for_table`. It is part of `commit_object`
   (Decision D8), so it is mandatory and cannot be skipped.

Costs 1 and 2 are two full table scans; cost 3 is a full re-hash. A single-file
edit in a million-node pond pays all three at full size.

### Why storing tree hashes in directory records is not the fix

The recursive fold means that when a deep file changes, every ancestor
directory's `tree_hash` genuinely changes. If those hashes were stored *inside*
each directory's own record, every deep write would append a new version of
every ancestor directory -- across as many partitions as there are ancestors --
which is the worst case for a log-structured store (many small files, fast
metadata growth). The current filesystem deliberately avoids this: a directory
row is rewritten **only when its own child listing changes**
(`flush_directory_operations` skips any directory not marked `modified`), so
adding a deep file rewrites only the immediate parent. **This property must be
preserved.** The derived ancestor hashes have to live somewhere that is *not*
the directory records.

### Why the control filesystem is not that place

The control filesystem (`{POND}/control`) is **disposable**: it supervises the
pond and is not replicated to the remote. Any durable, pond-derived content
must live in the data filesystem so it survives and replicates. Derived hashes
are pond content; they belong in the data FS.

---

## 3. The reserved index node

Introduce one node with a **well-known, reserved `FileID`** (the same mechanism
that gives the root directory its fixed id, `ROOT_UUID`). It lives in the data
filesystem, so it is durable and replicated. It holds the **persisted node
manifest**: one entry per node,

```
node_id -> ( parent_node_id, name, entry_type, child_hash )
```

for every node in the pond, plus tombstones for deleted nodes. This is a
superset of the per-directory `tree_hash` table the original design called for
(Section 5.2 there): file leaf hashes are included too, so the fold and the
manifest read exactly one structure and never touch file rows for a child hash.

Four properties define it:

- **Delta-versioned.** Each commit appends **one** new version containing only
  that commit's touched entries and tombstones -- the touched ancestor chain
  becomes *rows in one node version*, never files across partitions. Reads
  merge versions latest-wins, exactly the existing series merge-on-read;
  accumulated versions are collapsed by the existing `collapsed_through`
  compaction sentinel.
- **Durable and replicated.** It is an ordinary data-FS node, so it is backed
  up and synced with everything else.
- **Excluded from the fold.** Because its content is derived from the very
  hashes it stores, including it in `root_tree_hash` would be self-referential.
  It is skipped by the fold and by the manifest, the same way unresolved
  cross-pond mounts are skipped. It is therefore not attested by
  `root_tree_hash` and not transferred on content-addressed pull -- a consumer
  **rebuilds** it from the tree it receives. It is a cache with a durable
  backing, not a source of truth.
- **Single writer per commit.** One node, one new version per transaction: the
  minimum possible storage operation.

### Tombstones

Series data has no native deletion. A deleted node is recorded as an explicit
tombstone entry in the delta so the merged view drops it. Compaction discards
tombstones whose target is absent from the baseline.

---

## 4. The two content roots, computed incrementally

Both roots the commit needs are now derived from the index node plus the
transaction's in-memory changeset. The transaction already knows exactly which
nodes it touched (`pending_records`, modified directories), so nothing is
re-scanned to discover the change.

### 4.1 `root_tree_hash` -- incremental fold

```
1. Merge the index node's latest state into a node_id -> child_hash view.
2. touched = union of ancestor chains of the changed nodes.
3. For each touched directory, bottom-up, recompute
      tree_hash(dir) = fold(sorted (name, entry_type, child_hash) of its children)
   reading unchanged children's hashes from the merged view.
4. root_tree_hash = the recomputed root.
```

Work is proportional to the touched paths' breadth, not the pond. Unchanged
subtrees are never visited.

### 4.2 `node_manifest_hash` -> node-keyed Merkle root

The flat manifest hash is **replaced** by the root of a **node-keyed Merkle**
over the manifest entries -- a sparse Merkle tree / Merkle search tree keyed by
`node_id`, whose root recomputes only along the touched leaves' paths on a point
update (`O(change * log n)`), instead of re-hashing all entries (`O(#nodes)`).

The existing `sync-store/checksum::Merkle` is **not** reused for this: it is a
rebuild-only, sorted-array construction (`O(n log n)`, sorts every leaf) and its
root differs from a sparse-tree root for the same set. Instead we define **one**
node-keyed construction with two implementations that must agree byte-for-byte:

- an **incremental updater** used on the commit hot path, and
- a whole-set **`O(n)` rebuilder** used to build or repair the structure and as
  a test oracle,

with an equivalence test asserting identical roots across long random mutation
sequences. This mirrors the transparency-log tile writer, which already pairs an
incremental writer with an `O(n)` rebuilder and asserts byte-identical output.

### 4.3 `commit_object` and the format reset

```
commit_object = BLAKE3( root_tree_hash,
                        parent_commit_hash,
                        node_manifest_root,   // was: node_manifest_hash (flat)
                        provenance )
```

Swapping the flat manifest hash for the Merkle root changes `commit_object`'s
bytes, and therefore the **value** of every transparency-log leaf. This is a
permitted format change under Decision D2 (the project resets rather than
migrates; there are no legacy ponds), and it requires a coordinated reset of any
peers that sync. The transparency-log **file layout is unchanged**: the leaf is
still `SHA-256(commit_object)`, the tiles and checkpoint are byte-identical in
structure, only the leaf values differ.

---

## 5. The transparency log is unaffected in shape

The log's authoritative leaf sequence is still the ordered `DataCommitted`
records in the control table, each storing `commit_object`. The C2SP
`tlog-tiles` under `{POND}/tlog` remain a derived, re-materializable export
(Decision D5). Three sequences advance one-per-commit in lockstep:

| Per commit | Lives in | Role |
|---|---|---|
| `DataCommitted` (`commit_object`) | control table (disposable) | authoritative leaf log |
| tile + checkpoint | `{POND}/tlog` | derived export for external witnesses |
| index node version | data FS (replicated) | cache making both roots `O(change)` |

`pond tlog show` / `pond tlog verify` are unchanged. Only the computation and
storage of the two content roots move; everything downstream of `commit_object`
is identical.

---

## 6. Cost analysis

Per commit, for a change touching `k` nodes across paths of depth `d` in a pond
of `n` nodes:

| Cost | Before | After |
|---|---|---|
| Content-tree fold | `O(n)` full scan + fold | `O(k*d)` touched paths |
| Partition checksum scan | `O(n)` second full scan | folded into one shared scan (Tier 0) |
| Manifest hash | `O(n)` flat re-hash | `O(k*log n)` Merkle path updates |
| Data-table reads for child hashes | inline `content` bytes dragged in | none: hashes read from the index node |
| New storage operations | directory + node rows | + **one** index-node version (delta) |

The dominant `O(n)` terms all become proportional to the change. The only new
per-commit storage operation is a single small delta version of the index node.

---

## 7. Tier 0: an independent, immediate win

Before any of the incremental machinery, one change stands alone and ships
first:

- Replace the two `SELECT *` post-commit scans with a **narrow metadata
  projection** that never reads inline file `content` or `bao_outboard` bytes,
  and **unify them into one shared pass** feeding both the partition-checksum
  computation and the content-tree fold.

A file row's inline `content` is redundant with its `blake3` (which is
`blake3(content)` for physical versions), and `bao_outboard` is a
verified-streaming tree derived from the same content; both are dropped from the
scan. The only rows whose small `content` the fold still needs -- directories,
symlinks, and dynamic-node configs -- carry no `blake3`, so they are fetched by
a second narrow query filtered to `blake3 IS NULL`.

Because the partition-checksum leaf previously hashed the **whole** serialized
row (including those inline bytes), dropping them requires **redefining the row
leaf hash** to exclude `content`/`bao_outboard` and rely on `blake3` instead.
This changes the partition-checksum *values* -- a coordinated reset under
Decision D2 (the checksum lives in the disposable control table and replicated
bundles; there are no legacy ponds) -- but it does **not** touch `commit_object`,
`root_tree_hash`, the node manifest, or any transparency-log bytes: those are
computed by the fold, which is unaffected. The single leaf definition is shared
by the per-commit checksum and `fsck`, so both stay consistent.

This cuts per-commit read volume substantially (no inline blob bytes) and halves
the scan count, while leaving `commit_object` and the content roots byte-for-byte
identical. It is a safe first pull request while the incremental path lands
behind it.

**Status: implemented.** `content_tree::scan_live_rows` (narrow scan + splice),
`content_tree::fold_rows`, `content_tree::compute_commit_snapshot` (one scan ->
both roots + checksums), `remote_adapter::row_leaf_digest` (shared with `fsck`),
and `remote_adapter::partition_checksums_from_rows`.

---

## 8. Implementation plan

Roughly in dependency order; each phase is independently reviewable.

1. **Tier 0 -- narrow scan + unification + checksum leaf reset.** Project only
   the metadata the fold and checksum need (no inline `content`/`bao_outboard`;
   structural rows re-fetched via `blake3 IS NULL`); run one shared post-commit
   scan instead of two; redefine the checksum row leaf to exclude the inline
   bytes (a D2 checksum reset, `commit_object` unchanged). (`content_tree.rs`,
   `remote_adapter.rs`, `fsck.rs`.) **Done.**
2. **Reserved index node.** Allocate the well-known `FileID`; write a
   delta-versioned manifest node (all nodes + tombstones); reuse series
   merge-on-read and `collapsed_through` compaction; exclude it from the fold.
   **Done.**
3. **Incremental node-keyed Merkle.** One construction, incremental updater plus
   `O(n)` rebuilder oracle, byte-identical equivalence test.
4. **Incremental commit fold.** Compute both roots from the changeset plus the
   index node along the touched path; feed `commit_object`; retire the
   post-commit full-rescan fold, keeping the whole-tree fold as rebuilder and
   oracle.
5. **`commit_object` reset.** Replace flat `manifest_hash` with the Merkle root;
   update encode/decode; keep `SHA-256(commit_object)` as the leaf; update the
   decisions record below and in the superseded document.
6. **Validation.** Equivalence tests for both roots (incremental vs rebuild)
   across random mutation sequences; keep `tlog_materialize_test`,
   `content_pull_test`, and `testsuite/tests/719-tlog-verify.sh` green;
   presubmit (`cargo fmt --all`, `cargo clippy --workspace --all-features -- -D
   warnings`, `cargo test --workspace`).

---

## 9. Decisions record (relative to the superseded document)

- **Supersedes D3.** "Persist tree hashes; recompute only the touched ancestor
  chain" is retained in spirit but relocated: the persisted structure is the
  reserved delta-versioned index node in the data FS, not a per-directory field
  and not a separate control-side table. The incremental fold this enables is
  now mandatory, not idealized.
- **Adjusts D1.** Rows stay keyed by `(pond_id, node_id, version)` and the index
  node reuses that same versioned-node lifecycle (ordinary retention/vacuum, no
  new GC), so D1's rejection of at-rest content-addressing still holds; the
  index node is simply another versioned node under that same regime.
- **New: reserved index node.** All derived per-node hashes (the persisted node
  manifest, all nodes) live in one reserved well-known `FileID` in the data FS,
  delta-versioned, excluded from the fold, rebuilt on pull.
- **New: node-keyed Merkle manifest root.** `commit_object` carries an
  incremental node-keyed Merkle root in place of the flat `manifest_hash`; this
  is a D2 format reset that changes transparency-log leaf values but not the log
  file layout.
- **Unchanged:** the object model (blob/tree/commit/series/recipe hashing),
  D4 recipe hashes, D6 single delta-managed remote, D7 external large blobs,
  D8 node-id adoption via the manifest, D5 checkpoint-every-commit tiles, and
  the deferred items (signing, writable fork).
