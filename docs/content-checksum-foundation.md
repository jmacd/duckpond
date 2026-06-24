# Content Checksum Foundation: Making Today's Checksums Carry Forward

> **Status:** Plan-ahead design note. Captures decisions that let the
> *existing* content checksums serve replica comparison now and a
> standards-compliant transparency log later -- **without** committing to
> signing, key custody, or witnesses yet. Those are deferred on purpose
> (see [Deferred](#deferred-explicitly-out-of-scope)).
>
> Companion to `docs/design-attestation-and-publishing.md` (the full
> transparency-log / publishing design). This note is the narrower
> question: *what shape should the checksum substrate take so nothing has
> to be re-derived when we get there?*

---

## 1. Goal

Make the content hash we already compute a sound, reusable foundation for
three consumers, in priority order:

1. **Replica / state comparison** (today): is replica B byte-identical to
   pond A? -- already shipped as `pond fsck`.
2. **Content-addressed, git-style versioning** (the real target): a
   **pure, lineage-independent content hash** (a git "tree" hash) so two
   ponds can compare roots, find divergent subtrees in O(difference), and
   push/pull only the objects the other side lacks -- regardless of
   `pond_id` or creation time. See Section 6.
3. **Transparency log** (later): publish an append-only, independently
   verifiable log of the data, using industry-standard tooling.

The throughline is a single **leaf atom** (Section 3) that every consumer
reuses. The two tree *shapes* built over that atom are different and must
not be conflated (Section 4). **Section 8 describes the clean-slate target
architecture** -- what the replication layer would be if rebuilt around
content addressing -- and how Sections 3-7 are the road to it, not a
detour.

**A note on "clone comparison" vs "content addressing."** An earlier draft
framed goal 2 as comparing *clones* with an identifier-keyed digest. That
is a strictly weaker thing: it matches replicas that share a `pond_id` but
NOT two ponds that independently hold the same content. Git-style
versioning needs the stronger, **pure content hash** (Section 6), which
keys by name/path and commits to content only. This document targets the
pure version; clone comparison falls out as a degenerate case.

---

## 2. What exists today (the substrate)

| Piece | Where | What it gives us |
|-------|-------|------------------|
| Per-version content hash | `OplogEntry.blake3` (`crates/tlogfs/src/schema.rs`) | `blake3(version bytes)` for `*PhysicalVersion` / `TablePhysicalSeries` |
| Cumulative series hash | `FilePhysicalSeries.blake3` = `bao_root(v1\|\|...\|\|vN)` | content+history hash of a whole series |
| Verified streaming | `bao-tree 0.16`, `crates/tinyfs/src/bao_validating_reader.rs` | byte-range inclusion proofs *within one object* |
| Content-addressed blobs | `_large_files/blake3=<hash>.parquet` | dedup + content addressing for files > 64 KiB |
| State Merkle | `sync_store::checksum::Merkle` | per-partition + (via fsck) root membership checksum |
| `pond fsck` | `crates/steward/src/fsck.rs` | two-level state Merkle + content re-hash pass |

All of this is **BLAKE3-based**. There is currently **no `sha2`
dependency** in the workspace.

---

## 3. The leaf atom (the durable contract)

Everything downstream consumes one atom. We fix its contract now:

> **A version's content identity is `blake3(version bytes)`, recorded once
> at ingest, and never recomputed.** It commits to exact stored bytes
> (**byte equality**, including the physical parquet encoding), and it is
> **invariant under compaction** because compaction reorganizes the live
> queryable set but never rewrites already-recorded version rows.

Consequences of this contract:

- **Byte equality for parquet is deliberate and simplifying.** Two parquet
  files with identical logical rows but different encodings hash
  differently. We accept that: the atom proves "these exact bytes," not
  "this equivalent data." Logical-data equality is explicitly a non-goal
  (it would require a canonical Arrow encoding -- out of scope).
- **Append-once dissolves the compaction tension.** Because the atom is
  written at ingest and never recomputed, a future transparency log can
  treat each version hash as an immutable leaf even though
  `pond maintain --compact` rewrites parquet layout. The *state* Merkle
  (Section 4a) is recomputed and stays compaction-invariant by hashing row
  content, not file layout; the *history* log (Section 4b) never recomputes
  at all.
- **Large files are already content-addressed**, so their atom is their
  storage key for free.

`FilePhysicalSeries` is the one special case: its `blake3` is a *cumulative
bao root*, not a single version's hash. Consumers that need a per-version
leaf for a series must use the per-version digest, not the cumulative
field. (`pond fsck` already special-cases this -- see `fsck.rs`
`verify_row_content`.)

---

## 4. Two Merkle shapes (do not conflate)

The same leaf atom feeds two structurally different trees. This is the
central point of this document.

### 4a. State Merkle -- "are these two states equal?" (have it)

`sync_store::checksum::Merkle` (`crates/sync-store/src/checksum/merkle.rs`):

- `leaf  = BLAKE3("L" || key.len() u32le || key || value_blake3)`
- `node  = BLAKE3("N" || left || right)`; odd node **paired with itself**
- leaves **sorted by `(key, value)`**; empty tree tag `"E"`

This is a **sorted-set (membership) tree**. It answers state equality
perfectly and is the right tool for replica comparison and dedup. It does
**not** model record position or log prefix, so it **cannot** produce
inclusion or consistency proofs -- and that is fine, because that is not
its job.

`pond fsck` folds it twice today: per-partition (`node_id/version` leaves)
then a root over `pond_id/part_id` partition digests.

### 4b. History log -- "is record N present; did the log only grow?" (build later)

A transparency log is an **append-only** tree:

- leaves in **commit order** (position = index), never sorted
- odd node **promoted** (carried up unchanged), never paired with self
- supports **inclusion** (O(lg N)) and **consistency** (O(lg N)) proofs
- **append-once**: never recomputed

This is the **RFC 6962 / C2SP tlog-tiles** construction. It is a *second
tree*, not a reinterpretation of 4a. Its leaves take the Section 3 atom as
input (the record payload); the tree hash is computed fresh (Section 5).

### Why the distinction is load-bearing

| | State Merkle (4a) | History log (4b) |
|---|---|---|
| Leaf order | sorted by key | append/commit order |
| Odd node | paired with self | promoted |
| Recompute on compaction | yes (layout-independent) | never |
| Proofs | none -- root compare only | inclusion + consistency |
| Hash function | BLAKE3 (Section 5) | **SHA-256** (Section 5) |

Note: BLAKE3's *own internal tree* (and bao) is append-order /
left-complete / odd-node-promoted -- structurally **closer to 4b than 4a
is**. The obstacle to reusing 4a as a log was never the hash function; it
is the sorted/pair-with-self construction. The hash choice in Section 5 is
about ecosystem interop, not tree shape.

---

## 5. Hash layering: BLAKE3/bao for content, SHA-256 for the published log

**Decision:** run two hash functions at two layers. This is normal and
correct (content identity != attestation tree); Certificate Transparency
does the same conceptually.

| Layer | Hash | Rationale |
|-------|------|-----------|
| **Content identity** (per-version, bao streaming, large-file CAS, state Merkle) | **BLAKE3** | already shipped; bao verified streaming + CAS; fast on large sensor files; switching away would discard bao and gain nothing |
| **Published transparency-log tree** (leaves, interior nodes, tree head, checkpoint) | **SHA-256** | standards compliance (RFC 6962 / C2SP), off-the-shelf verifiers, and access to the **public witness** ecosystem for anti-equivocation |

Why SHA-256 for the log specifically:

- **Interop / witnesses.** Public witnesses co-sign only standard SHA-256
  checkpoints; standard verifiers (Go sumdb, CT tooling, Cloudflare Azul
  `tlog_tiles` / `signed_note`, browser WASM) assume SHA-256. The whole
  point of "verify with nothing but a browser" depends on speaking the
  standard.
- **Compliance optics.** SHA-256 is FIPS 180-4 and decades-scrutinized --
  meaningful for a regulator even though BLAKE3 is cryptographically strong.
- **Cost is irrelevant here.** A log hashes only 32-byte record digests and
  O(lg N) interior nodes per append; BLAKE3's speed advantage does not
  matter at the log layer. Speed is a reason to *keep* BLAKE3 for content,
  not to adopt SHA-256 for it.

**Therefore:** do **not** migrate content hashing off BLAKE3. Add SHA-256
only at the publish boundary, where the log leaf carries the BLAKE3 content
digest as payload and the signed/witnessed tree head is SHA-256.

### bao chains under the log (a bonus, not the log)

bao gives byte-range inclusion proofs *within* a file; the tlog gives
record inclusion proofs *across* the log. Chained, they yield end-to-end
provenance:

```
this row-range  --(bao)-->  is in file with blake3 H
file blake3 H   --(tlog)--> is record N in the attested log
```

bao is an asset for this chaining, but it is **not** the log tree and does
not affect the SHA-256 decision.

---

## 6. Content-addressed object model (the real target: git-style versioning)

The goal is a **pure content hash** so DuckPond can do git-style
push-pull: compare two ponds by a single hash, locate divergent subtrees
in O(difference), and transfer only the objects the other side lacks --
**independent of `pond_id`, `part_id`, or creation time**. That requires
modeling the filesystem as a content-addressed object graph, exactly like
git.

### 6.1 The objects

| Git | DuckPond object | Hash over | Status |
|-----|-----------------|-----------|--------|
| **blob** | one file version | `blake3(version bytes)` (Section 3 atom); series may use cumulative bao root | **have** (large files already CAS) |
| **tree** | one directory | sorted list of `(name, entry_type, child_hash)` -- **content only** | **net-new** -- *this is the pure content hash* |
| **commit** | one transaction | `(root_tree_hash, parent_commit_hash, author, timestamp, txn metadata)` | **partial** -- have txn_seq/pond_txn/timestamp; add root-tree + parent |
| **ref** | a branch/remote tip | a commit hash | net-new |

The **tree hash is the content hash.** It is pure because it commits to
**names + child content**, and deliberately **excludes** `pond_id`,
`part_id`, `node_id`, `txn_seq`, and `timestamp`. Two ponds that ingested
the same bytes therefore produce identical blob hashes -> identical tree
hashes -> identical root, regardless of lineage.

### 6.2 Keying: by name/path, content-only (this is required, not optional)

A tree's entries are keyed and sorted **by name**, and each entry's value
is the child's **content hash** (blob or subtree) plus its `entry_type` --
nothing else. This is the inversion of the shipped `fsck`/state Merkle,
which hashes the whole `OplogEntry` (identity included) and keys by
`node_id`/`part_id`. Both are legitimate; they answer different questions:

| | State Merkle (4a, `fsck`) | Content tree (this section) |
|---|---|---|
| Leaf commits to | whole row (content + identity) | content only |
| Keyed by | `node_id` / `part_id` | **name / path** |
| Equal across | clones (same `pond_id`) | **any same-content pond** (lineage-independent) |
| Enables | replica drift detection | git-style compare + fetch |

Clone comparison falls out of the content tree as a degenerate case (a
clone has the same content, so the same root), so the content tree
*subsumes* the weaker clone-comparison goal.

### 6.3 What it unlocks immediately -- and the gap

On the object graph, **one-way sync** (git `clone` / `fetch`) works
directly:

- **Compare:** equal root tree hash ⇒ identical content; otherwise descend
  by child hash to the divergent subtrees in O(difference).
- **Transfer:** send only objects (blobs/trees/commits) the peer lacks,
  named by hash. Works between *any* two ponds.

**Full bidirectional push-pull needs two more pieces git has and DuckPond
does not yet:**

1. **A commit DAG with refs/parents.** Today the history is a linear
   per-pond `txn_seq`; git has branches. Add parent pointers + per-remote
   tip refs.
2. **A merge model.** When two clones both advance from a common base,
   content hashes *detect* divergence but do not *resolve* it; git uses
   3-way merge per path. DuckPond is single-writer-per-pond today, and the
   remote redesign explicitly parked "git-clone-like symmetric" sync as
   future (`docs/remote-redesign.md`).

So the object model (6.1-6.2) is the **prerequisite and the ~80%**: it
delivers compare + dedup + one-way fetch now; the commit-DAG + merge layer
is the remaining work for divergent push-pull.

### 6.4 Hash function

The content object graph is **internal** (pond-to-pond), so it reuses
**BLAKE3** -- your existing blob hashes, bao, and CAS. No external verifier
consumes it, so there is no reason to switch it to SHA-256. The
transparency log (Section 4b/5) remains **SHA-256** for standards interop;
a commit's `root_tree_hash` is exactly what a published checkpoint later
attests, re-expressed as a SHA-256 log leaf. The two layers stack.

### 6.5 Cost to design around

Naive Merkle propagation rehashes every ancestor on each deep write and
serializes through the root. Mitigation: compute touched trees at **commit
time** only -- DuckPond is already per-transaction, so this is bounded by
the txn working set (the git model: trees are built at commit, not on every
file touch). The `Homomorphic` `PartitionChecksum` strategy already present
offers O(1) incremental partition updates if measured to matter.

---

## 7. Reusable vs net-new (summary)

| Capability | Status | Reuses |
|-----------|--------|--------|
| Per-version content atom (blob) | have | BLAKE3, schema row |
| Verified byte-range streaming | have | bao-tree |
| Large-file CAS | have | `_large_files/blake3=` |
| State equality / replica compare | have | state Merkle, `pond fsck` |
| **Pure content tree hash** (git tree) | **net-new** | blob hashes + commit-time fold, name-keyed |
| **Commit object** (root tree + parent) | partial | txn_seq / pond_txn / timestamp |
| One-way fetch / clone / dedup | near-term | content object graph |
| Refs + merge (bidirectional push-pull) | later | commit DAG + 3-way merge |
| Append-only log tree (SHA-256, tiles) | later | atom as leaf payload; new tree + `sha2` dep |
| Inclusion / consistency proofs | later | append-only tree |
| Signed checkpoints, witnesses, publishing | **deferred** | -- |

---

## 8. The ideal target architecture (clean-slate replication layer)

Sections 3-7 describe how to *carry today's checksums forward*. This section
describes the **end state we would build if starting from scratch** -- and it
is what the replication layer was meant to be in the first place: a
content-addressed object store, synchronized like git, with provenance and
publishing as thin layers on top rather than as machinery woven through every
row.

### 8.1 The one change that collapses the apparatus

Today, **provenance is smeared into every content hash.** Each row's leaf is
`blake3(serde_json(OplogEntry))`, which bakes in `pond_id`, `part_id`,
`node_id`, `txn_seq`, and `timestamp` (Section 3). Because content and lineage
are entangled at the leaf, *every* comparison drags lineage along. That single
entanglement is the root cause of:

- two Merkle shapes that must not be conflated (Section 4);
- a per-partition checksum **bundle format** for replication;
- `(pond_id, seq)` **frontier sequencing**;
- **compaction-invariance assertions** (`ship.rs` asserts pre/post checksum
  equality because a rewrite must not "shift" the lineage-bearing hash).

The ideal design **isolates provenance in one place -- the commit object --
and keeps content pure.** Everything below follows from that.

### 8.2 The object store (the whole model)

Four object kinds, BLAKE3-addressed, with the store invariant **name ==
hash(bytes)**:

| Object | Hash over | Carries lineage? |
|--------|-----------|------------------|
| **blob** | exact stored bytes of one file version | no |
| **tree** | sorted `(name, entry_type, child_hash)` for one directory | no |
| **commit** | `(root_tree_hash, parent_commit_hash(es), provenance)` | **yes -- the only place** |
| **ref** | a commit hash (a pond's tip, or an imported pond's tip) | (pointer) |

`provenance` in the commit = `pond_id`, `seq`, `time`, author, original CLI
args -- exactly the `pond_txn` blob already stamped into Delta commitInfo
today, but now attached to a commit object instead of dissolved into every
row. **Blobs and trees are pure**: two ponds that ingested the same bytes
produce identical blob hashes -> identical tree hashes -> identical root,
regardless of who wrote them or when.

This is the git object model. DuckPond directories *are* trees; file versions
*are* blobs; transactions *are* commits.

### 8.3 Corruption checking becomes the store invariant, not a separate Merkle

In a content-addressed store, an object's **name is its hash**, so "is this
object intact?" = "re-hash the bytes; do they match the name?" There is no
"recorded hash vs actual bytes" gap to bridge, because the name *is* the
recorded hash. The scrub (today's most valuable fsck capability -- re-read
bytes, check BLAKE3, validate bao chains) becomes simply:

> walk all objects reachable from the ref, re-hash each, verify name ==
> hash, and verify bao chains for large blobs.

No separate state Merkle is computed for integrity. `pond fsck` becomes
"verify the CAS invariant + reachability," and the *equality* question it also
answers today is subsumed by comparing root commit/tree hashes (8.4).

### 8.4 Replication = commit-DAG fetch + fast-forward

Sync becomes git fetch, verbatim:

- **Compare:** equal root tree hash ⇒ identical content; otherwise descend by
  child hash to the divergent subtrees in O(difference).
- **Transfer:** "I have commit X, you have commit Y -- send me the objects
  reachable from Y that I lack," named by hash. Works between *any* two ponds.
- **Frontier:** a single **commit hash**, not a `(pond_id, seq)` pair. The
  per-pond seq allocator and the bundle's per-partition checksum list both
  disappear; reachability replaces them.

This is the replication layer as originally intended: peers exchange objects
by content hash, and "are we in sync?" is one hash comparison.

### 8.5 Single-writer ⇒ fast-forward, no 3-way merge

DuckPond is **single-writer-per-pond**, so each pond's commit chain is
**linear** -- there are never two concurrent commits on the same ref to
reconcile. Therefore push-pull is **fast-forward**, and cross-pond import is
**grafting** another pond's chain in as a *distinct ref* (its own lineage,
its own commits). DuckPond never needs git's hardest part, **3-way merge**.
This refines Sections 6.3 and "Deferred": the merge engine is only required if
you permit concurrent writers to the *same* ref; the single-writer model
sidesteps it entirely.

### 8.6 Compaction = a rewrite commit (the invariance assertion disappears)

With byte-equality blobs (Section 3, your choice), compaction legitimately
produces **new** blobs and trees and therefore a **new** content hash. In the
ideal model that is fine and expected: compaction is simply a commit whose
trees were rewritten, recorded with a "compaction" relationship to its parent.
The pre/post checksum-invariance assertion in `ship.rs` -- which exists only
because the lineage-bearing hash was expected to stay stable across a rewrite
-- is no longer needed. The rewrite is a first-class, recorded fact, not an
invariant to defend.

### 8.7 Partitions are physical, not logical

`part_id` (one Delta partition per directory) is a **storage** optimization,
not a logical necessity. In the object model the logical structure is just the
tree; partitioning is how blobs/trees happen to be laid out on disk. So
`part_id` leaves the *logical/content* layer entirely and survives only as a
physical detail of the Delta/parquet store. One less identifier in the model.

### 8.8 The publishing boundary: one SHA-256 log above the graph

The content graph stays **BLAKE3** (internal; reuses blob hashes, bao, CAS --
Section 6.4). The **only** append-only / promotion Merkle in the whole system
is the published **transparency log**, and it lives strictly *above* the
content graph: its leaves are **commit hashes** (or periodic checkpoints),
hashed with **SHA-256** for standards interop and external witnesses (Sections
4b, 5). The two layers stack cleanly and never mix:

```
publish:   SHA-256 append-only log   (leaves = commit hashes)   <- the ONLY append-only tree
              |
content:   BLAKE3 object graph        (commit -> tree -> blob)
              |
bytes:     bao-tree byte-range proofs (within a large blob)
```

### 8.9 What disappears, relative to today

| Apparatus today | Replaced by |
|-----------------|-------------|
| identity-bearing per-row state Merkle | pure tree hashes + provenance isolated in commits |
| per-partition checksum **bundle format** | objects reachable from a commit |
| `(pond_id, seq)` frontier sequencing | a single **commit hash** ref |
| compaction-invariance assertions | compaction recorded as a rewrite commit |
| `part_id` in the logical model | physical storage detail only |
| "two Merkle shapes" tension | one content tree (state) + one log (history), cleanly separated because provenance no longer pollutes content |

### 8.10 What stays

- **BLAKE3** for the content graph; **bao-tree** for intra-file byte-range
  inclusion proofs (composes *under* a blob).
- A **scrub** (8.3) -- now expressed as the CAS invariant rather than a
  separate computation.
- An **append-only SHA-256 log** -- but only at the publishing boundary
  (8.8), never inside the content graph.
- **Single-writer-per-pond**, which is what keeps sync at fast-forward.

### 8.11 The migration gap (honest)

This is a genuine rewrite of the **replication / verify / sequencing** layer,
not a flag flip. What is load-bearing today and would change:

- the **bundle format** records `partition_checksums`
  (`remote_adapter.rs:536,738`);
- `verify_against_remote` compares those per-partition checksums;
- the **per-pond `seq` allocator** and control-table reconstruction assume the
  lineage-bearing row hash;
- `Ship::compact` asserts checksum invariance (`ship.rs:920,989`).

The object model makes all four derivable or unnecessary, but they exist
because the current replication layer predates content addressing. The path
is: (1) introduce tree/commit objects computed at commit time alongside the
existing checksums; (2) move provenance into the commit; (3) switch sync to
commit-DAG fetch; (4) retire the bundle/frontier apparatus once verify runs on
the object graph. Sections 3-7 are deliberately compatible with this --
they reuse the same blob atom -- so the carry-forward work and the clean-slate
target are the same road, not two.

---

## Deferred (explicitly out of scope)

These need decisions we are intentionally **not** making yet:

- **Signing & key custody.** No signing exists today; no key model is
  chosen. The transparency log's trust root is a signing key -- but none of
  Sections 3, 4a, 5 (content layer), or 6 require it.
- **Witnesses / anti-equivocation.** Depends on the published SHA-256
  format; design later.
- **Leaf granularity** (per-transaction vs per-row). Per-transaction is the
  cheap strong default; per-row layers on later.
- **Merge model for divergent push-pull.** The content object graph
  (Section 6) enables one-way fetch now; *bidirectional* sync needs a commit
  DAG with refs. Note (Section 8.5): because DuckPond is
  single-writer-per-pond, each ref is linear, so push-pull is **fast-forward**
  and cross-pond import is **grafting** a distinct ref -- a full 3-way merge
  engine is only required if concurrent writers share one ref, which the
  single-writer model avoids. The object model is the prerequisite either way
  (`docs/remote-redesign.md` parks "git-clone-like symmetric" sync as
  future).
- **Tile layout, multi-channel publishing, sitegen integration.** Covered
  by `docs/design-attestation-and-publishing.md`; not part of the
  foundation.

---

## References

- `docs/design-attestation-and-publishing.md` -- full transparency-log /
  publishing design (tlog-tiles, C2SP, witnesses, claimant model).
- `crates/sync-store/src/checksum/merkle.rs` -- the state Merkle.
- `crates/steward/src/fsck.rs` -- two-level state Merkle + content pass.
- `testsuite/tests/718-fsck-replica-equality.sh` -- demonstrates clone
  identifier preservation.
- RFC 6962 (Certificate Transparency); C2SP `tlog-tiles`,
  `tlog-checkpoint`, `signed-note`; Russ Cox, "Transparent Logs for
  Skeptical Clients."
