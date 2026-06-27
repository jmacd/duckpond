# Content-Addressed Pond: A Clean-Slate Design

> **Status:** First-principles design note. Describes the pond as a
> content-addressed object store with a built-in transparency log, designed
> from scratch. It does not assume the current bundle/frontier replication
> machinery and is deliberately written so it can be read on its own.

---

## 1. Thesis

A pond is a **content-addressed store of immutable objects**, advanced by an
**append-only sequence of commits**, one commit per transaction. Everything
the system needs -- integrity checking, replica comparison, peer-to-peer
sync, and an externally verifiable transparency log -- is a thin layer over
that single shape.

The whole design follows from one rule:

> **Content is addressed by the hash of its bytes; lineage lives only in the
> commit.** No content hash anywhere depends on `pond_id`, node identity,
> sequence number, or wall-clock time.

That separation is what makes content comparable across ponds, makes sync a
hash-set difference, and makes the transparency log a derived index rather
than a second source of truth.

---

## 2. Goals (in priority order)

1. **Integrity.** Detect any corruption of stored bytes, cheaply and
   locally, with no separate bookkeeping to fall out of sync.
2. **Comparison.** Decide "are these two ponds (or two subtrees) identical?"
   by comparing a single hash, and localize any difference proportional to
   the size of the difference, independent of lineage.
3. **Sync.** Transfer to a peer only the objects it lacks, by hash. Both
   one-way clone/fetch and, because each pond has a single writer,
   fast-forward push-pull and cross-pond grafting.
4. **Transparency.** Publish an append-only, independently verifiable log of
   commits, using standard tooling, so a skeptical third party can check
   inclusion and append-only growth without trusting the operator.

Goals 1-3 are served by the content graph (Sections 4-6). Goal 4 is the log
(Section 7), which sits on the same commit spine.

---

## 3. Three Merkle layers, one spine

There are exactly three hashing structures, and they never compete because
they are oriented differently:

```
TIME    transparency log    Merkle over the commit sequence     append order
          | leaf = a commit
SPACE   content tree        commit -> tree -> tree -> blob       filesystem shape
          | a directory's child hash IS the child's tree hash
BYTES   verified streaming   byte ranges within one blob          byte offset
```

The **commit** is the shared spine: it is the *top* of the SPACE tree (it
names a `root_tree_hash`) and the *leaf* of the TIME log (the log orders
commit hashes). SPACE answers "what does the pond contain, and what
diverged?"; TIME answers "is this commit recorded, and did the log only
grow?"; BYTES answers "is this byte range inside this object?".

---

## 4. The objects

Four immutable object kinds, each addressed by the BLAKE3 hash of its bytes
(the store invariant is **name == hash(bytes)**):

| Object  | Hashes over | Carries lineage? |
|---------|-------------|------------------|
| blob    | the exact stored bytes of one file version | no |
| tree    | the sorted entries of one directory | no |
| commit  | `(root_tree_hash, parent_commit_hash, provenance)` | **yes -- the only place** |
| ref     | a commit hash (a pond's tip, or a grafted foreign tip) | pointer |

### 4.1 Blob -- the leaf atom

A file version's identity is `blake3(version bytes)`, computed once at ingest
and never recomputed. It commits to the exact stored bytes -- byte equality,
not logical equality. Two files with identical logical content but different
physical encodings hash differently; that is accepted and simplifying.
Large blobs additionally carry a byte-range verification structure (the BYTES
layer) so a reader can verify any range without the whole object.

### 4.2 Tree -- a directory, content only

A tree hashes a directory as its sorted entries:

```
tree_hash(dir) = blake3( sorted_by_name[ (name, entry_type, child_hash) ] )
```

where a subdirectory's `child_hash` is that subdirectory's `tree_hash`, and a
file's `child_hash` is its blob hash. The entry value is **content plus
type, nothing else** -- no node identity, no partition, no timestamp.

Two consequences, both load-bearing:

- **Recursive:** equal `tree_hash` means identical subtree in one compare;
  unequal means descend by `child_hash` to the divergent children. Subtree
  identity is a single-hash question.
- **Why content-only is required, not just nice:** a hash that is both
  content-only and *child-independent* cannot uniquely identify a subtree.
  Two directories with the same local entry names but different descendants
  would share a hash, and a flat set of `(parent, name, child)` edges keyed
  by such hashes cannot tell a swap apart -- two ponds with subtrees
  exchanged would hash equal while differing. The recursive fold is exactly
  what removes that collision: folding the children in makes the parent hash
  change whenever any descendant changes. (A positional, path-keyed manifest
  also avoids the collision and avoids the per-ancestor write, but it cannot
  answer subtree identity by a single hash and does not dedup moved
  subtrees; it is the right choice only if subtree-level operations are never
  needed. This design keeps the recursive tree.)

### 4.3 Commit -- where lineage lives

A commit wraps one transaction:

```
commit = ( root_tree_hash, parent_commit_hash, provenance )
provenance = pond_id, seq, time, author, original request
```

This is the inversion the whole design rests on: blobs and trees are pure
content; **all provenance is isolated in the commit.** That keeps subtree
hashes comparable across ponds while preserving full lineage for audit and
for the log. A pond has a single writer, so its commits form a **linear
chain**; `parent_commit_hash` makes that chain a hash chain already.

### 4.4 Ref

A ref is a named pointer to a commit hash -- a pond's own tip, or the tip of
a foreign pond that has been grafted in. Sync moves refs forward.

---

## 5. Storage: the commit spine and the tree-hash table

The object model above is logical. Two physical decisions make it cheap on a
columnar, log-structured substrate where each directory is its own partition
and each write appends a file.

### 5.1 Recursive hashing writes the ancestor chain -- accept it

When a deep file changes, every ancestor directory's `tree_hash` genuinely
changes, because each ancestor's hash is a fold over its descendants. So a
commit must record a new tree hash for every directory on the path from the
change to the root. The **count** of touched ancestors is intrinsic to
recursive hashing and cannot be avoided. What can be avoided is the **cost
per touched ancestor**.

### 5.2 One tree-hash table, one file per commit

Do **not** store a directory's `tree_hash` inside the directory's own content
record. Doing so would rewrite the full directory snapshot to change 32
bytes, and -- because each directory is a separate partition -- would add a
new file to every ancestor partition on every deep write. That is the worst
case for a log-structured store: many small files, fast metadata growth.

Instead, route all of a commit's touched `(node, tree_hash)` updates into a
**single tree-hash table**, partitioned by `pond_id`, written as **one file
per transaction**. The ancestor chain becomes *rows in one file* rather than
*files across many partitions*. Directory content records are never rewritten
for a descendant-only change.

This also keeps the two digests cleanly separated:

- the directory's own content record stays the **own-content** digest --
  child-independent, unchanged when only descendants change;
- the tree-hash table holds the **recursive** digest -- the content-only
  fold, beside the data, not inside it.

The row is keyed by **node identity plus version** (`(pond_id, node_id,
version)`), a per-node index whose lifecycle is identical to the existing
per-node version history -- ordinary retention and vacuum manage it, with no
separate garbage collection. The row *values* are content hashes, so sync
still transfers and dedups **by hash on the wire** regardless of the storage
key; content-addressing for transfer does not require content-addressing at
rest. A content-addressed-at-rest store (rows keyed by `tree_hash`, so
identical subtrees collapse across commits and ponds) is a possible later
optimization, but it saves almost nothing -- the rows are tens of bytes -- and
it would require a reachability/GC subsystem over the commit DAG, so this
design does not adopt it. Either way it is one file per commit; object-per-file
storage is the shape to avoid. See Decision D1 in Section 11.

### 5.3 The top-level commit record is the spine

Per commit, one small, permanent record:

```
( seq, pond_id, root_tree_hash, parent_commit_hash, provenance )
```

This is simultaneously the top of the SPACE tree and -- as Section 7 shows --
the leaf of the TIME log. It is tiny, so it is kept forever. The bulky
per-node tree-hash deltas of Section 5.2 are a compactable detail and may be
checkpointed and vacuumed; the commit spine is append-once.

Because the commit record carries `root_tree_hash`, and that hash **is** the
transparency-log leaf (Section 7), the root fold must be computed **at commit
time** -- there is no leaf to log otherwise. Computing it incrementally
(folding only the touched ancestor chain, reading unchanged children from the
tree-hash table) is exactly why the table persists tree hashes rather than
recomputing them at compare time. Persisting is therefore not a tuning choice
but a requirement of the log unification. See Decision D3 in Section 11.

---

## 6. Integrity and comparison

### 6.1 Integrity is the store invariant

Because an object's name is its hash, "is this object intact?" is "re-hash
the bytes; do they equal the name?" There is no separate recorded-hash to
drift from the bytes, because the name is the recorded hash. A scrub walks
all objects reachable from a ref, re-hashes each, and verifies the byte-range
chains of large blobs. No separate integrity Merkle is computed.

### 6.2 Comparison falls out of the tree

- **Equal `root_tree_hash`** -> the two ponds have identical content. Replica
  equality is the degenerate case: a clone has identical content, hence an
  identical root.
- **Unequal** -> descend by `child_hash` to the divergent subtrees, work
  proportional to the difference.

Both are independent of `pond_id` and lineage, because the tree is
content-only.

---

## 7. The transparency log is the spine, indexed

The log is not a second structure. The append-only sequence of commit records
from Section 5.3, ordered by `seq`, **is** the log's leaf sequence. The log
adds three things over that existing chain:

1. **A standard leaf hash.** The content graph is BLAKE3; the published log
   leaf is hashed with **SHA-256**, so off-the-shelf witnesses and verifiers
   can co-sign it.
2. **A proof index.** A binary, append-order Merkle (the RFC 6962 /
   tlog-tiles construction) over the leaves, giving O(log n) inclusion and
   consistency proofs instead of an O(n) chain walk.
3. **Signed checkpoints** (deferred -- the trust root is a signing key).

The seam between the two hash functions is exactly the leaf: the leaf payload
is the BLAKE3 `root_tree_hash` plus provenance; the log hashes that leaf's
serialization with SHA-256. Internally the system never leaves BLAKE3;
SHA-256 appears only at the publishing boundary.

So one spine carries two Merkle indices: the content tree folds **below** each
leaf (SPACE, BLAKE3, n-ary, sorted by name); the log Merkle folds **over** the
sequence of leaves (TIME, SHA-256, binary, append order). They share leaves,
never shapes.

End-to-end, a claim chains across all three layers:

```
this byte range  --(BYTES)-->  is in blob H
blob H           --(SPACE)-->  is at path P in commit C's tree
commit C         --(TIME)-->   is record N in the attested log
```

---

## 8. Sync

Because each pond has a single writer, its commit chain is linear and there
are never two concurrent commits on one ref. Sync is therefore git-shaped and
needs no merge engine:

- **Compare:** exchange tip commit hashes; equal `root_tree_hash` means
  identical, else descend by child hash to the divergent objects.
- **Transfer:** "I have commit X, you have Y -- send me the objects reachable
  from Y that I lack," by hash. Works between any two ponds with the same
  content, regardless of their lineage.
- **Push-pull** is fast-forward; **cross-pond import** is grafting another
  pond's linear chain in as a distinct ref, carrying its commits and, later,
  its checkpoints.

The sync frontier is a **single commit hash**, not a per-partition checksum
list and not a `(pond_id, seq)` pair; reachability replaces both.

### 8.1 One system, not two: the remote is a delta-managed object table

This replaces the prior bundle/frontier remote outright. There is no
coexistence period and no backwards compatibility: `(pond_id, seq)` frontiers,
per-bundle manifests, and per-partition checksum lists are **deleted**, not
wrapped. A backup is reconstructible -- the source pond is the single writer
and holds all content -- so the migration is a clean reset: re-push the full
object graph to a freshly-formatted remote.

The remote stays **delta-managed**; content-addressing is expressed *inside*
Delta, not as a hand-rolled object store beside it. The remote is one Delta
table whose rows are content objects:

```
( object_hash, object_kind, value, value_blake3, ... )   -- one row per object
( ref_name, tip_commit_hash )                            -- the pond's tip ref
```

Object rows are keyed by their content hash, so identical blobs, trees, and
commits dedup naturally; `value_blake3` already exists in the store schema and
*is* the object id. The tip ref is a distinguished row.

### 8.2 Atomicity comes from Delta, not from object ordering

A push is **one Delta commit** that writes the new object rows *and* advances
the tip ref together. Delta's transaction-log commit -- backed by
`object_store`'s S3 support (the conditional-put / lock the Delta protocol
already requires on S3) -- makes that commit atomic. Therefore:

- The tip ref can never advance without its reachable object closure, because
  both land in the same Delta commit. No "objects-before-ref" two-phase write
  is needed; Delta provides the invariant.
- No separate compare-and-swap, lock service, or ref-pointer file is
  introduced. The single storage abstraction (delta-rs over object_store) is
  the only thing the remote relies on.

### 8.3 Push and pull computation

- **Push (producer):** the single writer knows the tip it last pushed.
  Everything reachable from that tip is already on the remote, so the new-object
  set is computed **locally** -- the current commit's reachable inventory minus
  the last-pushed tip's reachable inventory (the `missing_from` set). It writes
  those object rows and the new tip in one Delta commit. No remote listing.
- **Pull (consumer):** read the remote tip ref; if it equals the local tip,
  done. Otherwise walk the object graph from the remote tip by child hash,
  fetching only object rows whose hash is absent locally, and pruning subtrees
  whose `child_hash` already matches. Apply, then set the local tip.

The passive remote needs only Delta's ordinary read/append; no server-side
compute, consistent with the single-writer, git-shaped model above.

### 8.4 Large-file blobs stay external (D7)

A file larger than the large-file threshold is stored out-of-row in the
content-addressed large-file store (`_large_files/blake3=<hash>.parquet`)
precisely so it is never loaded into a row. Object materialization preserves
this: a **small** blob's bytes become an inline `objects` row, but a **large**
blob is *not* inlined. It transfers by hash through the existing external-blob
path, which already replicates `_large_files/blake3=*` content-addressed.

So materialization yields two sets: inline objects (trees, commits, series
manifests, symlinks, recipes, and small blobs) written as `objects` rows in
the atomic push commit, and a set of large-blob hashes transferred externally.
Both are addressed by the same BLAKE3 hash, so reachability, dedup, and the
consumer's "fetch only what I lack" walk are identical regardless of where a
blob's bytes physically live. This keeps the streaming, do-not-collect
discipline: multi-gigabyte files never materialize into memory or into the
remote's row table.

---

## 9. Special cases to pin

The `child_hash` an entry contributes to its parent's `tree_hash` depends on
the node kind:

| Node kind | `child_hash` |
|-----------|--------------|
| file (physical version) | `blake3(version bytes)` |
| series / multi-version file | cumulative content-and-history hash (a stable bao root over all versions), not any single version's hash |
| directory | `tree_hash` -- the recursive fold |
| symlink | `blake3(target path)` |
| dynamic dir / read-time `table:dynamic` | `blake3(stored config bytes)` -- the recipe, hashed byte-for-byte |

- **Series.** Uses the cumulative hash above so the entry commits to the whole
  history, stable across appends.
- **Dynamic and computed nodes.** Nodes whose content is computed on read have
  no stored bytes. Their `child_hash` is the hash of their **definition**
  (factory type plus configuration), not their output. The semantics are
  explicit: for a dynamic node, tree-hash equality means *the recipe is
  identical*, not that the output bytes are. Its dynamically generated children
  are derived, not stored, so they are **not** folded into its hash. This is
  correct for sync -- the recipe and its real upstream source data (ordinary
  blobs/series that sync normally) transfer, and the consumer recomputes
  downstream. An *executed* factory that writes real data (for example, a
  collector that materializes a `table:series`) produces physical blobs and is
  hashed as a series, not by this rule; only genuinely read-time-computed nodes
  use the recipe hash. The recipe is hashed **byte-for-byte over the stored
  config bytes** -- no canonicalization. This is the same byte-equality stance
  the whole design takes (Section 4.1): reformatting a config (whitespace, key
  order) reads as a change, which is accepted as simpler than defining a
  canonical form. See Decision D2 in Section 11.
- **Compaction / rewrite.** Reorganizing storage legitimately produces new
  blobs and trees and therefore a new content hash. That is recorded honestly
  as a new commit whose trees were rewritten, with a rewrite relationship to
  its parent. There is no "content unchanged across a rewrite" invariant to
  maintain, because byte-equality blobs make a rewrite genuinely new content.

---

## 10. What this is not (deferred)

- **Signing, key custody, witnesses.** The content layers need no key; the
  log's trust root is a signing key, chosen later.
- **Logical (vs byte) equality.** Would require a canonical encoding; out of
  scope. Comparison is byte-exact.
- **A merge model.** One-way fetch and fast-forward push-pull are covered by
  the single-writer model. A three-way merge is needed only if concurrent
  writers ever share one ref, which the model avoids.
- **Log leaf granularity finer than per-commit.** Per-commit leaves are the
  default; per-row proofs can layer on later.

---

## 11. Decisions and remaining open items

### Decided

- **D1 -- Tree-hash row key: identity index, not content-addressed at rest.**
  Rows are keyed `(pond_id, node_id, version)`. Sync still dedups by hash on
  the wire because the values are content hashes. At-rest content-addressing
  (keying by `tree_hash`) is rejected for now: it saves negligible storage and
  would require reachability/GC over the commit DAG. It remains a possible
  later optimization (Section 5.2).
- **D3 -- Persist tree hashes at commit, do not recompute at compare.** Forced
  by the log unification: the commit's `root_tree_hash` is the log leaf, so the
  fold must run at commit time. Recomputing only the touched ancestor chain
  (children read from the persisted table) is the cheap, incremental way to
  produce it (Sections 5.3, 7).
- **D4 -- Dynamic-node `child_hash` is the recipe hash.** Read-time-computed
  nodes hash `blake3(stored config bytes)`; tree equality means recipe
  equality, not output equality; generated children are not folded (Section 9).
- **D2 -- Encodings start simple and are not frozen.** Two byte encodings are
  needed: (a) the **tree wire format** -- the byte layout of a tree's entry
  list (delimiters, `entry_type` representation, name encoding, sort
  collation); and (b) the **config bytes** for D4, which are taken **as-is**,
  byte-for-byte, with no canonicalization. Both start with the simplest
  reasonable choice. Because the project allows a **clean reset at will** (no
  legacy ponds to migrate), these formats are *not* permanently frozen -- they
  can be improved later by resetting. The one constraint: any two ponds that
  sync, and any one pond across a format change, must share the same encoding
  version, since hashes do not match across versions; a format change is a
  coordinated reset of all participating ponds.
- **D6 -- One delta-managed remote; no bundle/frontier coexistence.** The
  content-addressed remote is a single system that replaces bundle/frontier and
  `(pond_id, seq)` outright -- no backwards compatibility, no dual-path
  migration window. The remote stays delta-managed: content objects are rows in
  one Delta table keyed by content hash, and the tip ref advances in the **same
  atomic Delta commit** that writes the object rows. Atomicity comes from
  delta-rs over `object_store` S3 support, not a hand-rolled CAS or an
  objects-before-ref ordering. A backup is reconstructible from the
  single-writer source, so the switch is a clean re-push to a freshly-formatted
  remote (Section 8.1-8.3).
- **D7 -- Large-file blobs stay external; only small blobs become object
  rows.** Object materialization does not inline a file above the large-file
  threshold; it transfers by hash through the existing external-blob path
  (`_large_files/blake3=*`), while small blobs, trees, commits, series
  manifests, symlinks, and recipes are inline `objects` rows. Same BLAKE3 hash
  either way, so reachability/dedup/walk are uniform and large files never load
  into memory or the remote row table (Section 8.4).

### Open

- **D5 -- Checkpoint cadence and the SHA-256 publish format.** When the log
  materializes tiles and emits checkpoints; deferred together with signing
  (Section 10).

---

## 12. The shape in one picture

```
              ref ---> commit_N ---> commit_{N-1} ---> ... ---> commit_0
                          |                                       (TIME: SHA-256
                          | root_tree_hash                         Merkle over this
                          v                                        linear sequence;
                        tree(root)                                 each commit is a leaf)
                        /   |    \
                   tree   blob   tree        (SPACE: BLAKE3 recursive fold;
                   /  \           |           content only, no lineage)
                blob  blob       blob
                  |                            (BYTES: per-blob byte-range
              byte ranges                       verification)
```

One linear spine of commits per pond. Below each commit, a content-only
BLAKE3 tree. Over the spine, a SHA-256 transparency log. Lineage lives in the
commits and nowhere else.
