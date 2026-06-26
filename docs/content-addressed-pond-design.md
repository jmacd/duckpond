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

The row key has one open sub-choice (Section 11): node identity plus version
(a simple per-node index) or the `tree_hash` itself (content-addressed, so
identical subtrees collapse across commits and across ponds). Either way it
is one file per commit; object-per-file storage is the shape to avoid.

### 5.3 The top-level commit record is the spine

Per commit, one small, permanent record:

```
( seq, pond_id, root_tree_hash, parent_commit_hash, provenance )
```

This is simultaneously the top of the SPACE tree and -- as Section 7 shows --
the leaf of the TIME log. It is tiny, so it is kept forever. The bulky
per-node tree-hash deltas of Section 5.2 are a compactable detail and may be
checkpointed and vacuumed; the commit spine is append-once.

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

---

## 9. Special cases to pin

- **Series / multi-version files.** A file that accumulates versions uses a
  cumulative content-and-history hash as its blob-level `child_hash`, stable
  and well-defined, rather than any single version's hash.
- **Dynamic and computed nodes.** Nodes whose content is computed on read,
  rather than stored, have no stored bytes to hash. Each needs an explicit
  rule for its `child_hash` -- for example, the hash of its generating
  configuration, or exclusion from the content tree. This must be decided
  before the tree is well-defined (Section 11).
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

## 11. Open decisions

1. **Tree wire format.** The exact byte encoding of a tree's entry list
   (delimiters, `entry_type` representation, name encoding, sort collation).
   A content-addressed format is permanent and must be pinned before any
   object is written.
2. **Tree-hash row key.** Node-identity index versus content-addressed by
   `tree_hash` (Section 5.2). Content-addressed enables cross-commit and
   cross-pond subtree dedup at the cost of a reachability/GC story.
3. **Dynamic-node hashing.** The `child_hash` rule for computed nodes
   (Section 9).
4. **Persist vs recompute.** This design persists tree hashes at commit time
   so comparison is O(1) on cached subtrees. The alternative -- store only
   own-content digests and recompute the fold at compare time -- makes commits
   cheaper but every comparison O(whole tree). Persisting is the default here.
5. **Checkpoint cadence and the SHA-256 publish format.** When the log
   materializes tiles and emits checkpoints; deferred with signing.

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
