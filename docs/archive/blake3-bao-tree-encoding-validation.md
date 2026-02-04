# Binary Encoding and On-the-Fly Validation Guide

This document explains how to calculate the binary tree encoding (outboard) while storing data, and how to validate data on-the-fly during reading.

## Table of Contents

1. [Overview](#overview)
2. [Tree Structure](#tree-structure)
3. [Creating the Outboard (Encoding)](#creating-the-outboard-encoding)
4. [On-the-Fly Validation (Decoding)](#on-the-fly-validation-decoding)
5. [Complete Examples](#complete-examples)

---

## Overview

The bao-tree library implements BLAKE3-based verified streaming using a Merkle tree structure. The system has two main components:

1. **Data file**: Your actual content (video, document, etc.)
2. **Outboard file**: A separate file containing hash pairs for tree nodes

The outboard allows you to:
- Verify data integrity without reading the entire file
- Stream and validate data incrementally
- Share specific byte ranges with cryptographic proof

---

## Tree Structure

### Basic Concepts

- **Chunk**: 1024 bytes (1 KiB) - the fundamental unit
- **Block**: Group of chunks, configurable (e.g., 16 chunks = 16 KiB)
- **Block Size**: Expressed as log₂ of chunk count
  - `BlockSize(0)` = 1 chunk = 1024 bytes
  - `BlockSize(4)` = 16 chunks = 16 KiB ✓ (recommended)
  - `BlockSize(10)` = 1024 chunks = 1 MiB

### Tree Geometry

For a file of size `S` bytes with block size `B`:

```
blocks = ceil(S / (1024 << B))
chunks = ceil(S / 1024)
```

The tree is a complete binary tree where:
- **Leaf nodes** represent blocks of data (up to 1024 << B bytes)
- **Parent nodes** contain hashes of their two children
- **Root node** is the top-level hash representing the entire file

### Example: 48 KiB file with 16 KiB blocks

```
                    Root
                   /    \
                  H₀    H₁
                 /  \     \
               H₀₀  H₀₁   H₁₀
               |    |      |
            Block0 Block1 Block2
            (16KB) (16KB) (16KB)
```

---

## Creating the Outboard (Encoding)

### Step 1: Divide Data into Blocks

```rust
use bao_tree::{BaoTree, BlockSize};

let data = std::fs::read("input.bin")?;
let block_size = BlockSize::from_chunk_log(4); // 16 KiB blocks
let tree = BaoTree::new(data.len() as u64, block_size);
```

### Step 2: Compute Leaf Hashes (Bottom-Up)

For each block of data (up to 16 KiB), compute its hash:

```rust
use blake3::hazmat::HasherExt;

fn hash_block(start_chunk: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    if is_root && data.len() <= 16384 {
        // Single block file - use root hash
        blake3::hash(data)
    } else {
        // Multi-block file - use chunked hash with offset
        let mut hasher = blake3::Hasher::new();
        hasher.set_input_offset(start_chunk * 1024);
        hasher.update(data);
        blake3::Hash::from(hasher.finalize_non_root())
    }
}
```

**Key points:**
- Use `blake3::hash()` only for single-block files
- Use chunked hashing with offset for multi-block files
- Each leaf hash covers exactly one block (up to 16 KiB)

### Step 3: Compute Parent Hashes (Bottom-Up)

Parent nodes combine two child hashes:

```rust
use blake3::hazmat::{merge_subtrees_root, merge_subtrees_non_root, ChainingValue, Mode};

fn parent_hash(
    left_hash: &blake3::Hash,
    right_hash: &blake3::Hash,
    is_root: bool
) -> blake3::Hash {
    let left: ChainingValue = *left_hash.as_bytes();
    let right: ChainingValue = *right_hash.as_bytes();
    
    if is_root {
        merge_subtrees_root(&left, &right, Mode::Hash)
    } else {
        blake3::Hash::from(merge_subtrees_non_root(&left, &right, Mode::Hash))
    }
}
```

### Step 4: Store Hash Pairs in Binary Format

The outboard stores **parent nodes only** (not leaf hashes). Each parent node is 64 bytes:

```
[Left Child Hash (32 bytes)][Right Child Hash (32 bytes)]
```

#### Pre-Order Traversal (Default)

Visit each node: root, then left subtree, then right subtree.

```rust
use bao_tree::io::{outboard::PreOrderOutboard, sync::CreateOutboard};

// Create outboard from data
let file = std::fs::File::open("input.bin")?;
let outboard = PreOrderOutboard::<Vec<u8>>::create(&file, block_size)?;

// Access the binary data
let root_hash = outboard.root;           // 32 bytes
let hash_pairs = outboard.data;          // Vec<u8> with 64 * (blocks-1) bytes
let size = outboard.tree.outboard_size(); // Total size in bytes
```

#### Post-Order Traversal (For Append-Only Files)

Visit left subtree, right subtree, then node. Better for files that grow over time.

```rust
use bao_tree::io::outboard::PostOrderMemOutboard;

let outboard = PostOrderMemOutboard::create(&data, block_size);
```

### Step 5: Calculate Outboard Size

```rust
let outboard_size = tree.outboard_size();
// Formula: (blocks - 1) * 64 bytes

// For 16 KiB blocks:
// 1 MB file  = 64 blocks  → 63 * 64 = 4,032 bytes (~0.39%)
// 1 GB file  = 65,536 blocks → 65,535 * 64 = 4,194,240 bytes (~0.39%)
```

### Complete Encoding Example

```rust
use std::io;
use bao_tree::{
    BaoTree, BlockSize,
    io::{
        outboard::PreOrderOutboard,
        sync::{CreateOutboard, Outboard as OutboardTrait},
    },
};

fn create_outboard_file(
    data_path: &str,
    outboard_path: &str,
) -> io::Result<blake3::Hash> {
    let block_size = BlockSize::from_chunk_log(4); // 16 KiB
    
    // Open data file
    let data_file = std::fs::File::open(data_path)?;
    
    // Create in-memory outboard
    let outboard = PreOrderOutboard::<Vec<u8>>::create(&data_file, block_size)?;
    
    // Save outboard to disk
    std::fs::write(outboard_path, &outboard.data)?;
    
    // Return root hash (this is the "fingerprint" of your file)
    Ok(outboard.root)
}
```

---

## On-the-Fly Validation (Decoding)

### Overview of Validation Process

When reading data with validation:

1. Start with the **root hash** (obtained from trusted source)
2. Request specific **byte ranges** to read
3. Receive **hash pairs** for parent nodes + **data chunks**
4. Validate each chunk against its hash
5. Write validated data to output

### Step 1: Set Up for Validation

```rust
use bao_tree::{
    BaoTree, BlockSize, ChunkRanges,
    io::{
        outboard::PreOrderOutboard,
        sync::decode_ranges,
        round_up_to_chunks,
    },
    ByteRanges,
};

// Known parameters (from sender or file metadata)
let root_hash = blake3::Hash::from_hex("abc123...")?;
let file_size = 1_048_576u64; // 1 MB
let block_size = BlockSize::from_chunk_log(4);

// Create tree geometry
let tree = BaoTree::new(file_size, block_size);

// Specify what byte ranges you want to read
let byte_ranges = ByteRanges::from(0..100_000); // First 100 KB
let chunk_ranges = round_up_to_chunks(&byte_ranges);
```

### Step 2: Stream Format

The encoded stream contains interleaved hash pairs and data:

```
[Parent Hash Pair 1 (64 bytes)]
[Parent Hash Pair 2 (64 bytes)]
...
[Data Block 1 (up to 16 KiB)]
[Parent Hash Pair N (64 bytes)]
[Data Block 2 (up to 16 KiB)]
...
```

The order follows **pre-order tree traversal** of the requested ranges.

### Step 3: Validation Algorithm

```rust
use bao_tree::io::sync::DecodeResponseIter;
use std::io::{self, Write};

fn validate_and_write(
    encoded_stream: impl std::io::Read,
    root_hash: blake3::Hash,
    tree: BaoTree,
    chunk_ranges: &ChunkRanges,
    output: &mut impl Write,
) -> io::Result<()> {
    // Create validation iterator
    let iter = DecodeResponseIter::new(
        root_hash,
        tree,
        encoded_stream,
        chunk_ranges.as_ref(),
    );
    
    // Process each item
    for item in iter {
        let item = item?; // Will error if validation fails
        
        match item {
            BaoContentItem::Parent(parent) => {
                // Store hash pair for later validation
                // (typically done automatically by the iterator)
                println!("Validated parent node: {:?}", parent.node);
            }
            BaoContentItem::Leaf(leaf) => {
                // Write validated data
                output.write_all(&leaf.data)?;
                println!("Validated {} bytes at offset {}", 
                    leaf.data.len(), leaf.offset);
            }
        }
    }
    
    Ok(())
}
```

### Step 4: Hash Validation Details

The iterator maintains a **hash stack** and validates each node:

#### For Parent Nodes:
```rust
// 1. Pop expected hash from stack
let expected_hash = stack.pop();

// 2. Read 64-byte hash pair from stream
let (left_hash, right_hash) = read_hash_pair(stream);

// 3. Compute actual parent hash
let actual_hash = parent_hash(&left_hash, &right_hash, is_root);

// 4. Validate
if expected_hash != actual_hash {
    return Err("Hash mismatch at parent node");
}

// 5. Push children onto stack (in reverse order for pre-order)
if needs_right_child { stack.push(right_hash); }
if needs_left_child { stack.push(left_hash); }
```

#### For Leaf Nodes (Data Blocks):
```rust
// 1. Pop expected hash from stack
let expected_hash = stack.pop();

// 2. Read block data from stream (up to 16 KiB)
let mut block_data = vec![0u8; block_size];
stream.read_exact(&mut block_data)?;

// 3. Compute actual hash of data
let actual_hash = hash_block(start_chunk, &block_data, is_root);

// 4. Validate
if expected_hash != actual_hash {
    return Err("Hash mismatch at leaf node");
}

// 5. Data is valid - write to output
output.write_all(&block_data)?;
```

### Step 5: Error Handling

Validation can fail at several points:

```rust
use bao_tree::io::DecodeError;

match validate_and_write(...) {
    Ok(()) => println!("All data validated successfully"),
    Err(e) => {
        if let Some(decode_err) = e.downcast_ref::<DecodeError>() {
            match decode_err {
                DecodeError::ParentHashMismatch(node) => {
                    eprintln!("Parent hash mismatch at node {}", node);
                    // Data corruption or tampering detected
                }
                DecodeError::LeafHashMismatch(chunk) => {
                    eprintln!("Leaf hash mismatch at chunk {}", chunk);
                    // Data corruption or tampering detected
                }
                _ => eprintln!("Decode error: {}", e),
            }
        }
    }
}
```

---

## Complete Examples

### Example 1: Create Outboard and Store

```rust
use std::io;
use bao_tree::{
    BlockSize,
    io::{
        outboard::PreOrderOutboard,
        sync::CreateOutboard,
    },
};

fn main() -> io::Result<()> {
    let block_size = BlockSize::from_chunk_log(4); // 16 KiB
    
    // 1. Open your data file
    let data_file = std::fs::File::open("video.mp4")?;
    
    // 2. Create outboard
    let outboard = PreOrderOutboard::<Vec<u8>>::create(&data_file, block_size)?;
    
    // 3. Store root hash (32 bytes) - this is your file's fingerprint
    let root_hash = outboard.root;
    println!("Root hash: {}", root_hash);
    
    // 4. Store outboard data alongside your file
    std::fs::write("video.mp4.bao", &outboard.data)?;
    
    // 5. Check overhead
    let data_size = std::fs::metadata("video.mp4")?.len();
    let outboard_size = outboard.data.len() as u64;
    let overhead_pct = (outboard_size as f64 / data_size as f64) * 100.0;
    println!("Data: {} bytes", data_size);
    println!("Outboard: {} bytes ({:.2}% overhead)", outboard_size, overhead_pct);
    
    Ok(())
}
```

### Example 2: Validate While Reading Ranges

```rust
use std::io::{self, Cursor};
use bao_tree::{
    BlockSize, ByteRanges, ChunkRanges,
    io::{
        outboard::PreOrderOutboard,
        sync::{encode_ranges_validated, decode_ranges},
        round_up_to_chunks,
    },
};

fn main() -> io::Result<()> {
    let block_size = BlockSize::from_chunk_log(4);
    
    // === SENDER SIDE ===
    
    // Load data and outboard
    let data_file = std::fs::File::open("video.mp4")?;
    let outboard_data = std::fs::read("video.mp4.bao")?;
    let root_hash = blake3::Hash::from_hex("...")?; // Previously computed
    
    let tree = bao_tree::BaoTree::new(
        std::fs::metadata("video.mp4")?.len(),
        block_size,
    );
    
    let outboard = PreOrderOutboard {
        root: root_hash,
        tree,
        data: outboard_data,
    };
    
    // Client requests bytes 0-100,000
    let requested_bytes = ByteRanges::from(0..100_000);
    let chunk_ranges = round_up_to_chunks(&requested_bytes);
    
    // Encode and send
    let mut encoded_stream = Vec::new();
    encode_ranges_validated(&data_file, &outboard, &chunk_ranges, &mut encoded_stream)?;
    
    // === RECEIVER SIDE ===
    
    // Receive encoded stream
    let received_stream = Cursor::new(encoded_stream);
    
    // Prepare to validate and write
    let mut validated_output = std::fs::File::create("video_part.mp4")?;
    
    // Start with empty outboard (will be populated during decode)
    let mut receiver_outboard = PreOrderOutboard {
        root: root_hash,
        tree,
        data: vec![],
    };
    
    // Decode and validate
    decode_ranges(
        received_stream,
        &chunk_ranges,
        &mut validated_output,
        &mut receiver_outboard,
    )?;
    
    println!("✓ Data validated and written successfully");
    println!("✓ Received {} bytes of tree data", receiver_outboard.data.len());
    
    Ok(())
}
```

### Example 3: Streaming with Progressive Validation

```rust
use bao_tree::io::{sync::DecodeResponseIter, BaoContentItem};

fn stream_and_validate(
    encoded_stream: impl std::io::Read,
    root_hash: blake3::Hash,
    tree: bao_tree::BaoTree,
    ranges: &bao_tree::ChunkRanges,
) -> io::Result<Vec<u8>> {
    let mut validated_data = Vec::new();
    let mut tree_hashes = Vec::new();
    
    let decoder = DecodeResponseIter::new(
        root_hash,
        tree,
        encoded_stream,
        ranges.as_ref(),
    );
    
    for item in decoder {
        match item? {
            BaoContentItem::Parent(parent) => {
                // Store hash pair for our records
                tree_hashes.push(parent.pair);
                println!("✓ Validated parent at node {}", parent.node);
            }
            BaoContentItem::Leaf(leaf) => {
                // Append validated data
                validated_data.extend_from_slice(&leaf.data);
                println!("✓ Validated {} bytes at offset {}", 
                    leaf.data.len(), leaf.offset);
            }
        }
    }
    
    println!("Total validated: {} bytes", validated_data.len());
    println!("Total tree nodes: {}", tree_hashes.len());
    
    Ok(validated_data)
}
```

---

## Key Takeaways

### For Encoding (Creating Outboard):

1. **Block size matters**: 16 KiB (`BlockSize(4)`) is recommended
2. **Overhead is minimal**: ~0.39% for 16 KiB blocks
3. **Traversal order**: Pre-order (default) or post-order (append-only)
4. **Storage**: Root hash (32 bytes) + outboard data (64 × parent_nodes)

### For Validation (On-the-Fly):

1. **Trust root hash**: Must be obtained from secure source
2. **Streaming friendly**: Validates incrementally as data arrives
3. **Range queries**: Request only what you need
4. **Early failure**: Stops immediately on hash mismatch
5. **Zero-copy**: No need to buffer entire file

### Advantages Over Traditional Methods:

- **vs MD5/SHA256**: Can verify parts without reading whole file
- **vs rsync**: Cryptographically secure + Merkle tree structure
- **vs signatures**: Efficient for large files and range queries
- **Progressive verification**: No need to wait for complete download

---

## Hierarchical Segmented Storage

This section explains how to store large files in separate segments (e.g., 10MB segments for a 100MB file) while maintaining the ability to validate both individual segments and the complete file.

### The Challenge

For a 100MB file with 16KB blocks and 10MB segments:
- **Total file**: 100 MB = 6,400 blocks (16KB each) = 102,400 chunks (1KB each)
- **Per segment**: 10 MB = 640 blocks = 10,240 chunks
- **Segment count**: 10 segments

The key insight is that BLAKE3's tree structure supports **subtree hashing** with explicit `start_chunk` offsets and `is_root` flags. This allows:
1. Computing a segment's subtree hash as a **non-root chaining value**
2. Combining segment hashes into a higher-level tree
3. Validating segments independently OR as part of the whole

### Two-Level Tree Architecture

```
                         ┌─────────────────┐
                         │   Root Hash     │  ← Whole file hash
                         │   (is_root=true)│
                         └────────┬────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
        ┌─────┴─────┐       ┌─────┴─────┐       ┌─────┴─────┐
        │  Level 1  │       │  Level 1  │       │  Level 1  │
        │  Parents  │       │  Parents  │       │  Parents  │
        └─────┬─────┘       └─────┬─────┘       └─────┴─────┘
              │                   │                   │
     ┌────────┴────────┐         ...                 ...
     │                 │
┌────┴────┐       ┌────┴────┐
│Segment 0│       │Segment 1│    ...    │Segment 9│
│ 10 MB   │       │ 10 MB   │           │ 10 MB   │
│640 blks │       │640 blks │           │640 blks │
│is_root= │       │is_root= │           │is_root= │
│ false   │       │ false   │           │ false   │
└─────────┘       └─────────┘           └─────────┘
     ↓                 ↓                     ↓
┌─────────┐       ┌─────────┐           ┌─────────┐
│Segment  │       │Segment  │           │Segment  │
│Outboard │       │Outboard │           │Outboard │
│ 0.bao   │       │ 1.bao   │           │ 9.bao   │
└─────────┘       └─────────┘           └─────────┘
```

### Understanding start_chunk and is_root

BLAKE3's tree uses two critical parameters:

```rust
fn hash_subtree(start_chunk: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    use blake3::hazmat::{ChainingValue, HasherExt};
    if is_root {
        // Root hash - used only for the final file hash
        blake3::hash(data)
    } else {
        // Subtree hash - includes position information
        let mut hasher = blake3::Hasher::new();
        hasher.set_input_offset(start_chunk * 1024);  // Position matters!
        hasher.update(data);
        blake3::Hash::from(hasher.finalize_non_root())
    }
}
```

**Key points:**
- `start_chunk`: The chunk offset where this subtree begins (0 for segment 0, 10240 for segment 1, etc.)
- `is_root`: Must be `false` for segments, `true` only for the final combined hash
- The position is **cryptographically bound** into the hash

### Implementation: Segmented Storage

#### Step 1: Define Segment Parameters

```rust
use bao_tree::{BaoTree, BlockSize, ChunkNum};

const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);  // 16 KiB blocks
const SEGMENT_SIZE: u64 = 10 * 1024 * 1024;  // 10 MB segments
const CHUNKS_PER_SEGMENT: u64 = SEGMENT_SIZE / 1024;  // 10,240 chunks

/// Metadata for a segment
#[derive(Clone)]
struct SegmentInfo {
    /// Segment index (0, 1, 2, ...)
    index: u64,
    /// Start chunk number in the overall file
    start_chunk: ChunkNum,
    /// Size of this segment in bytes
    size: u64,
    /// Subtree hash (non-root chaining value)
    subtree_hash: blake3::Hash,
}
```

#### Step 2: Create Per-Segment Outboards

```rust
use bao_tree::io::outboard::PostOrderMemOutboard;
use blake3::hazmat::{ChainingValue, HasherExt, merge_subtrees_non_root, Mode};

/// Create outboard for a single segment
fn create_segment_outboard(
    segment_data: &[u8],
    segment_index: u64,
) -> (PostOrderMemOutboard, blake3::Hash) {
    let start_chunk = segment_index * CHUNKS_PER_SEGMENT;
    let tree = BaoTree::new(segment_data.len() as u64, BLOCK_SIZE);
    
    // Create internal outboard (same structure as normal)
    let outboard = PostOrderMemOutboard::create(segment_data, BLOCK_SIZE);
    
    // The outboard.root is computed as if this were a standalone file (is_root=true)
    // We need to recompute the subtree hash with is_root=false for combining
    let subtree_hash = compute_subtree_hash(segment_data, start_chunk);
    
    (outboard, subtree_hash)
}

/// Compute hash of a data block as a non-root subtree
fn compute_subtree_hash(data: &[u8], start_chunk: u64) -> blake3::Hash {
    // For multi-block data, we need to traverse the tree
    compute_subtree_hash_recursive(data, start_chunk, false)
}

fn compute_subtree_hash_recursive(
    data: &[u8],
    start_chunk: u64,
    is_root: bool,
) -> blake3::Hash {
    use blake3::hazmat::{ChainingValue, HasherExt};
    
    const CHUNK_LEN: usize = 1024;
    
    if data.len() <= CHUNK_LEN {
        // Leaf node
        if is_root {
            blake3::hash(data)
        } else {
            let mut hasher = blake3::Hasher::new();
            hasher.set_input_offset(start_chunk * 1024);
            hasher.update(data);
            blake3::Hash::from(hasher.finalize_non_root())
        }
    } else {
        // Parent node - split and recurse
        let chunks = (data.len() + CHUNK_LEN - 1) / CHUNK_LEN;
        let chunks = chunks.next_power_of_two();
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        
        let left = compute_subtree_hash_recursive(
            &data[..mid_bytes.min(data.len())],
            start_chunk,
            false,
        );
        
        let right = if mid_bytes < data.len() {
            compute_subtree_hash_recursive(
                &data[mid_bytes..],
                start_chunk + mid as u64,
                false,
            )
        } else {
            // Handle case where right side is empty
            return left;
        };
        
        // Combine child hashes
        let left_cv: ChainingValue = *left.as_bytes();
        let right_cv: ChainingValue = *right.as_bytes();
        
        if is_root {
            blake3::hazmat::merge_subtrees_root(&left_cv, &right_cv, Mode::Hash)
        } else {
            blake3::Hash::from(blake3::hazmat::merge_subtrees_non_root(
                &left_cv,
                &right_cv,
                Mode::Hash,
            ))
        }
    }
}
```

#### Step 3: Store Segment Data and Outboards

```rust
use std::io::{self, Write};

/// Storage format for a segment
struct SegmentStorage {
    /// Segment data file path
    data_path: String,
    /// Segment outboard file path  
    outboard_path: String,
    /// Segment metadata
    info: SegmentInfo,
}

fn store_file_as_segments(
    file_path: &str,
    output_dir: &str,
) -> io::Result<Vec<SegmentStorage>> {
    let file_data = std::fs::read(file_path)?;
    let file_size = file_data.len() as u64;
    
    let segment_count = (file_size + SEGMENT_SIZE - 1) / SEGMENT_SIZE;
    let mut segments = Vec::new();
    
    for i in 0..segment_count {
        let start = (i * SEGMENT_SIZE) as usize;
        let end = ((i + 1) * SEGMENT_SIZE).min(file_size) as usize;
        let segment_data = &file_data[start..end];
        
        // Create outboard for this segment
        let (outboard, subtree_hash) = create_segment_outboard(segment_data, i);
        
        // Save segment data
        let data_path = format!("{}/segment_{:04}.dat", output_dir, i);
        std::fs::write(&data_path, segment_data)?;
        
        // Save segment outboard
        let outboard_path = format!("{}/segment_{:04}.bao", output_dir, i);
        std::fs::write(&outboard_path, &outboard.data)?;
        
        segments.push(SegmentStorage {
            data_path,
            outboard_path,
            info: SegmentInfo {
                index: i,
                start_chunk: ChunkNum(i * CHUNKS_PER_SEGMENT),
                size: segment_data.len() as u64,
                subtree_hash,
            },
        });
    }
    
    Ok(segments)
}
```

#### Step 4: Create Top-Level Tree

```rust
/// Top-level tree combining all segment hashes
struct TopLevelTree {
    /// Root hash of the entire file
    root_hash: blake3::Hash,
    /// Segment subtree hashes (in order)
    segment_hashes: Vec<blake3::Hash>,
    /// Binary tree of parent hashes for combining segments
    parent_hashes: Vec<(blake3::Hash, blake3::Hash)>,
}

fn create_top_level_tree(segments: &[SegmentInfo]) -> TopLevelTree {
    let segment_hashes: Vec<_> = segments.iter()
        .map(|s| s.subtree_hash)
        .collect();
    
    // Build binary tree bottom-up
    let (root_hash, parent_hashes) = build_merkle_tree(&segment_hashes, true);
    
    TopLevelTree {
        root_hash,
        segment_hashes,
        parent_hashes,
    }
}

/// Build a Merkle tree from leaf hashes
fn build_merkle_tree(
    leaves: &[blake3::Hash],
    is_root_level: bool,
) -> (blake3::Hash, Vec<(blake3::Hash, blake3::Hash)>) {
    use blake3::hazmat::{ChainingValue, merge_subtrees_root, merge_subtrees_non_root, Mode};
    
    if leaves.is_empty() {
        return (blake3::hash(&[]), vec![]);
    }
    
    if leaves.len() == 1 {
        // Single segment - need special handling
        // The segment hash becomes the root
        return (leaves[0], vec![]);
    }
    
    let mut parent_pairs = Vec::new();
    let mut current_level = leaves.to_vec();
    
    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        let is_final_level = current_level.len() <= 2;
        
        for chunk in current_level.chunks(2) {
            match chunk {
                [left, right] => {
                    parent_pairs.push((*left, *right));
                    
                    let left_cv: ChainingValue = *left.as_bytes();
                    let right_cv: ChainingValue = *right.as_bytes();
                    
                    let parent = if is_final_level && is_root_level {
                        merge_subtrees_root(&left_cv, &right_cv, Mode::Hash)
                    } else {
                        blake3::Hash::from(merge_subtrees_non_root(
                            &left_cv,
                            &right_cv,
                            Mode::Hash,
                        ))
                    };
                    
                    next_level.push(parent);
                }
                [single] => {
                    // Odd number - carry forward
                    next_level.push(*single);
                }
                _ => unreachable!(),
            }
        }
        
        current_level = next_level;
    }
    
    (current_level[0], parent_pairs)
}
```

#### Step 5: Save Complete Manifest

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct FileManifest {
    /// Total file size
    total_size: u64,
    /// Block size used
    block_size: u8,
    /// Segment size
    segment_size: u64,
    /// Root hash of entire file (hex encoded)
    root_hash: String,
    /// Individual segment info
    segments: Vec<SerializedSegmentInfo>,
    /// Top-level tree parent hashes (for combining segments)
    top_level_hashes: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize)]
struct SerializedSegmentInfo {
    index: u64,
    start_chunk: u64,
    size: u64,
    subtree_hash: String,  // Hex encoded
    data_file: String,
    outboard_file: String,
}

fn save_manifest(
    output_dir: &str,
    segments: &[SegmentStorage],
    top_level: &TopLevelTree,
    total_size: u64,
) -> io::Result<()> {
    let manifest = FileManifest {
        total_size,
        block_size: BLOCK_SIZE.chunk_log(),
        segment_size: SEGMENT_SIZE,
        root_hash: top_level.root_hash.to_hex().to_string(),
        segments: segments.iter().map(|s| SerializedSegmentInfo {
            index: s.info.index,
            start_chunk: s.info.start_chunk.0,
            size: s.info.size,
            subtree_hash: s.info.subtree_hash.to_hex().to_string(),
            data_file: s.data_path.clone(),
            outboard_file: s.outboard_path.clone(),
        }).collect(),
        top_level_hashes: top_level.parent_hashes.iter()
            .map(|(l, r)| (l.to_hex().to_string(), r.to_hex().to_string()))
            .collect(),
    };
    
    let json = serde_json::to_string_pretty(&manifest)?;
    std::fs::write(format!("{}/manifest.json", output_dir), json)?;
    
    Ok(())
}
```

### Validation Modes

#### Mode 1: Validate Single Segment Independently

```rust
use bao_tree::io::sync::DecodeResponseIter;

fn validate_segment(
    segment: &SegmentStorage,
) -> io::Result<bool> {
    let data = std::fs::read(&segment.data_path)?;
    let outboard_data = std::fs::read(&segment.outboard_path)?;
    
    // Recompute the subtree hash
    let computed_hash = compute_subtree_hash(
        &data, 
        segment.info.start_chunk.0,
    );
    
    // Compare with stored hash
    Ok(computed_hash == segment.info.subtree_hash)
}
```

#### Mode 2: Validate Entire File by Streaming Segments

```rust
fn validate_entire_file(
    manifest: &FileManifest,
) -> io::Result<bool> {
    // 1. Validate each segment independently
    let mut segment_hashes = Vec::new();
    
    for seg_info in &manifest.segments {
        let data = std::fs::read(&seg_info.data_file)?;
        let computed = compute_subtree_hash(&data, seg_info.start_chunk);
        let expected = blake3::Hash::from_hex(&seg_info.subtree_hash)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        if computed != expected {
            eprintln!("Segment {} hash mismatch!", seg_info.index);
            return Ok(false);
        }
        
        segment_hashes.push(computed);
    }
    
    // 2. Build top-level tree from segment hashes
    let (computed_root, _) = build_merkle_tree(&segment_hashes, true);
    
    // 3. Compare with manifest root
    let expected_root = blake3::Hash::from_hex(&manifest.root_hash)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    Ok(computed_root == expected_root)
}
```

#### Mode 3: Stream Segments and Validate On-the-Fly

```rust
use std::io::Write;

fn stream_validated_file(
    manifest: &FileManifest,
    output: &mut impl Write,
) -> io::Result<()> {
    let mut segment_hashes = Vec::new();
    
    for seg_info in &manifest.segments {
        // Read segment data
        let data = std::fs::read(&seg_info.data_file)?;
        
        // Validate segment hash
        let computed = compute_subtree_hash(&data, seg_info.start_chunk);
        let expected = blake3::Hash::from_hex(&seg_info.subtree_hash)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        if computed != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Segment {} validation failed", seg_info.index),
            ));
        }
        
        // Write validated data
        output.write_all(&data)?;
        segment_hashes.push(computed);
        
        println!("✓ Segment {} validated and written ({} bytes)", 
            seg_info.index, data.len());
    }
    
    // Final validation: check root hash
    let (computed_root, _) = build_merkle_tree(&segment_hashes, true);
    let expected_root = blake3::Hash::from_hex(&manifest.root_hash)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    if computed_root != expected_root {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Root hash validation failed",
        ));
    }
    
    println!("✓ Complete file validated");
    Ok(())
}
```

### Storage Layout Example

For a 100MB file stored as 10 segments:

```
output_directory/
├── manifest.json           # Complete file manifest
├── segment_0000.dat        # 10 MB data
├── segment_0000.bao        # ~25 KB outboard (639 × 64 bytes)
├── segment_0001.dat
├── segment_0001.bao
├── ...
├── segment_0009.dat
└── segment_0009.bao

manifest.json contents:
{
  "total_size": 104857600,
  "block_size": 4,
  "segment_size": 10485760,
  "root_hash": "abc123...",
  "segments": [
    {
      "index": 0,
      "start_chunk": 0,
      "size": 10485760,
      "subtree_hash": "def456...",
      "data_file": "segment_0000.dat",
      "outboard_file": "segment_0000.bao"
    },
    ...
  ],
  "top_level_hashes": [
    ["hash_left_0", "hash_right_0"],
    ...
  ]
}
```

### Overhead Analysis

For 100MB file with 10MB segments and 16KB blocks:

| Component | Size | Overhead |
|-----------|------|----------|
| Per-segment outboard | 639 × 64 = 40,896 bytes | 0.39% |
| 10 segment outboards | 408,960 bytes | 0.39% |
| Top-level tree | ~9 × 64 = 576 bytes | negligible |
| Manifest JSON | ~2 KB | negligible |
| **Total overhead** | ~411 KB | **~0.39%** |

### Power-of-2 Segment Sizes (Recommended)

For cleaner tree alignment, consider using power-of-2 segment sizes:

| Segment Size | Blocks per Segment | Segments for 100MB |
|--------------|-------------------|-------------------|
| 8 MB (2²³) | 512 | 13 |
| 16 MB (2²⁴) | 1,024 | 7 |
| 32 MB (2²⁵) | 2,048 | 4 |

Power-of-2 sizes align perfectly with the binary tree structure.

### Summary

✅ **Yes, you can absolutely encode per-segment blake3 tree data** that allows:

1. **Individual segment validation**: Each segment has its own outboard
2. **Whole-file validation**: Combine segment subtree hashes into a top-level tree
3. **Streaming validation**: Validate segments progressively as they're read
4. **Position binding**: Each segment's hash includes its position (`start_chunk`)

The key is computing segment hashes with `is_root=false` and the correct `start_chunk` offset, then combining them using the standard BLAKE3 parent hash function.

---

## Incremental Ingestion with Non-Power-of-Two Sizes

This section addresses how to incrementally compute checksums when data arrives in arbitrary-sized chunks that don't align to power-of-two boundaries.

### The Challenge: Your Example

**Version 1**: 1500 bytes
- Chunk 0: bytes 0-1023 (1024 bytes, full)
- Chunk 1: bytes 1024-1499 (476 bytes, partial)
- **Tree**: 2 chunks

**Version 2**: 4100 bytes (appended 2600 bytes)
- Chunk 0: bytes 0-1023 (1024 bytes, full) ✓ unchanged
- Chunk 1: bytes 1024-2047 (1024 bytes, full) ← was partial, now full
- Chunk 2: bytes 2048-3071 (1024 bytes, full) ← new
- Chunk 3: bytes 3072-4095 (1024 bytes, full) ← new
- Chunk 4: bytes 4096-4099 (4 bytes, partial) ← new
- **Tree**: 5 chunks

### Key Insight: What Changes on Append

When data is appended, only the **rightmost path** (spine) of the tree changes:

```
Version 1 (1500 bytes):          Version 2 (4100 bytes):
                                 
      Root*                              Root*
       |                                /    \
      H₀*  ← unstable               H₀       H₁*  ← unstable
     /   \                         /   \    /   \
   C₀    C₁*  ← partial         C₀    C₁  C₂   H₂*  ← unstable
                                                /  \
                                              C₃   C₄*  ← partial

* = changes on append
```

### The Stable vs Unstable Distinction

The bao-tree library explicitly tracks this via `PostOrderOffset`:

```rust
pub enum PostOrderOffset {
    /// The node is stable and won't change when appending data
    Stable(u64),
    /// The node is unstable and will change when appending data  
    Unstable(u64),
}
```

**Stable nodes**: Left subtrees that are complete (all data present)
**Unstable nodes**: The rightmost path from root to the partial chunk

### Incremental Computation Strategy

#### State to Maintain Between Snapshots

```rust
/// State for incremental hashing
#[derive(Clone)]
struct IncrementalHashState {
    /// Total bytes processed so far
    total_size: u64,
    
    /// Pending partial chunk data (0 to 1023 bytes)
    pending_chunk: Vec<u8>,
    
    /// Stack of completed left subtree hashes
    /// Each entry: (level, hash, start_chunk)
    /// Level 0 = single chunk, level 1 = 2 chunks, etc.
    completed_subtrees: Vec<(u32, blake3::Hash, u64)>,
}

impl IncrementalHashState {
    fn new() -> Self {
        Self {
            total_size: 0,
            pending_chunk: Vec::with_capacity(1024),
            completed_subtrees: Vec::new(),
        }
    }
}
```

#### Processing Your Example Step-by-Step

**Initial state:**
```rust
let mut state = IncrementalHashState::new();
// total_size: 0
// pending_chunk: []
// completed_subtrees: []
```

**After ingesting 1500 bytes:**

```rust
fn ingest_data(state: &mut IncrementalHashState, data: &[u8]) {
    let mut offset = 0;
    
    // 1. Complete any pending partial chunk
    if !state.pending_chunk.is_empty() {
        let needed = 1024 - state.pending_chunk.len();
        let available = data.len().min(needed);
        state.pending_chunk.extend_from_slice(&data[..available]);
        offset = available;
        
        if state.pending_chunk.len() == 1024 {
            // Chunk is complete - hash it and add to tree
            let chunk_num = state.total_size / 1024;
            let hash = hash_chunk(&state.pending_chunk, chunk_num);
            add_completed_chunk(state, hash, chunk_num);
            state.pending_chunk.clear();
        }
    }
    
    // 2. Process full chunks from new data
    while offset + 1024 <= data.len() {
        let chunk_num = (state.total_size + offset as u64) / 1024;
        let hash = hash_chunk(&data[offset..offset + 1024], chunk_num);
        add_completed_chunk(state, hash, chunk_num);
        offset += 1024;
    }
    
    // 3. Save remaining partial chunk
    if offset < data.len() {
        state.pending_chunk.extend_from_slice(&data[offset..]);
    }
    
    state.total_size += data.len() as u64;
}
```

**After 1500 bytes:**
```
total_size: 1500
pending_chunk: [476 bytes of partial chunk 1]
completed_subtrees: [(level=0, hash_of_chunk_0, chunk=0)]
```

**After appending 2600 more bytes (total 4100):**
```
total_size: 4100  
pending_chunk: [4 bytes of partial chunk 4]
completed_subtrees: [
    (level=1, hash_of_chunks_0_1, chunk=0),  // Merged!
    (level=1, hash_of_chunks_2_3, chunk=2),  // Merged!
]
```

#### The Merge Algorithm

When a new chunk completes, try to merge with previous subtrees at the same level:

```rust
fn add_completed_chunk(
    state: &mut IncrementalHashState,
    hash: blake3::Hash,
    chunk_num: u64,
) {
    let mut current_hash = hash;
    let mut current_level = 0u32;
    let mut current_start = chunk_num;
    
    // Try to merge with completed subtrees of the same level
    while let Some(&(prev_level, prev_hash, prev_start)) = state.completed_subtrees.last() {
        if prev_level != current_level {
            break;
        }
        
        // Check if these are adjacent subtrees that can merge
        let expected_start = prev_start + (1u64 << current_level);
        if current_start != expected_start {
            break;
        }
        
        // Merge: prev is left child, current is right child
        state.completed_subtrees.pop();
        current_hash = parent_cv(&prev_hash, &current_hash, false);
        current_level += 1;
        current_start = prev_start;
    }
    
    state.completed_subtrees.push((current_level, current_hash, current_start));
}

fn hash_chunk(data: &[u8], chunk_num: u64) -> blake3::Hash {
    use blake3::hazmat::{ChainingValue, HasherExt};
    let mut hasher = blake3::Hasher::new();
    hasher.set_input_offset(chunk_num * 1024);
    hasher.update(data);
    blake3::Hash::from(hasher.finalize_non_root())
}
```

#### Computing the Current Root Hash

At any point, you can compute the current root hash by merging all subtrees:

```rust
fn compute_root_hash(state: &IncrementalHashState) -> blake3::Hash {
    let mut subtrees = state.completed_subtrees.clone();
    
    // Add the pending partial chunk if present
    if !state.pending_chunk.is_empty() {
        let chunk_num = state.total_size / 1024;
        let hash = hash_partial_chunk(&state.pending_chunk, chunk_num, state.total_size);
        subtrees.push((0, hash, chunk_num));
    }
    
    if subtrees.is_empty() {
        return blake3::hash(&[]);  // Empty file
    }
    
    // Merge all subtrees from right to left
    // The rightmost subtrees are "smaller" and get merged into larger ones
    while subtrees.len() > 1 {
        let (right_level, right_hash, _) = subtrees.pop().unwrap();
        let (left_level, left_hash, left_start) = subtrees.pop().unwrap();
        
        // Determine if this merge produces the final root
        let is_final = subtrees.is_empty();
        
        let merged = parent_cv(&left_hash, &right_hash, is_final);
        let merged_level = left_level.max(right_level) + 1;
        
        subtrees.push((merged_level, merged, left_start));
    }
    
    subtrees[0].1
}

fn hash_partial_chunk(data: &[u8], chunk_num: u64, total_size: u64) -> blake3::Hash {
    use blake3::hazmat::{ChainingValue, HasherExt};
    
    // A partial chunk at the end might be the entire file (is_root=true)
    let is_single_chunk_file = total_size <= 1024;
    
    if is_single_chunk_file {
        blake3::hash(data)
    } else {
        let mut hasher = blake3::Hasher::new();
        hasher.set_input_offset(chunk_num * 1024);
        hasher.update(data);
        blake3::Hash::from(hasher.finalize_non_root())
    }
}
```

### Visual Walkthrough of Your Example

#### Version 1: 1500 bytes

```
Step 1: Ingest bytes 0-1023 (chunk 0, complete)
  pending_chunk: []
  completed_subtrees: [(L0, H(C0), 0)]
  
Step 2: Ingest bytes 1024-1499 (chunk 1, partial - 476 bytes)
  pending_chunk: [476 bytes]
  completed_subtrees: [(L0, H(C0), 0)]

To compute root hash:
  - Hash pending as partial: H(C1_partial)
  - Merge: parent_cv(H(C0), H(C1_partial), is_root=true)
  
Tree structure:
        Root (is_root=true)
        /    \
     H(C0)  H(C1_partial)
```

#### Version 2: Append 2600 bytes (total 4100)

```
Step 1: Complete pending chunk 1 (need 548 more bytes, have 2600)
  - Chunk 1 now complete: bytes 1024-2047
  - Hash H(C1) and add to tree
  
  After adding C1:
    completed_subtrees: [(L0, H(C0), 0), (L0, H(C1), 1)]
    
  Merge check: L0 at chunk 0 + L0 at chunk 1 → can merge!
    completed_subtrees: [(L1, parent_cv(H(C0), H(C1)), 0)]

Step 2: Process chunk 2 (bytes 2048-3071)
  completed_subtrees: [(L1, H01, 0), (L0, H(C2), 2)]
  No merge (different levels)

Step 3: Process chunk 3 (bytes 3072-4095)  
  completed_subtrees: [(L1, H01, 0), (L0, H(C2), 2), (L0, H(C3), 3)]
  Merge C2+C3:
  completed_subtrees: [(L1, H01, 0), (L1, H23, 2)]
  
  Merge H01+H23? No - they're at same level but merging them
  would only happen at finalization (they span chunks 0-3)

Step 4: Remaining 4 bytes → pending chunk
  pending_chunk: [4 bytes]
  completed_subtrees: [(L1, H01, 0), (L1, H23, 2)]

To compute root hash:
  - Hash pending: H(C4_partial)
  - Stack becomes: [(L1, H01, 0), (L1, H23, 2), (L0, H(C4), 4)]
  - Merge H23 + H(C4): [(L1, H01, 0), (L2, H234, 2)]
  - Merge H01 + H234: [(L3, Root, 0)] with is_root=true
  
Tree structure:
              Root (is_root=true)
             /    \
          H01      H234 (unbalanced)
         /   \    /    \
       C0    C1  H23   C4(partial)
                /   \
              C2    C3
```

### What Gets Stored (Outboard Data)

For incremental computation, you only need to store:

```rust
#[derive(Serialize, Deserialize)]
struct IncrementalOutboard {
    /// Current total size
    total_size: u64,
    
    /// Pending partial chunk (if any)
    pending_bytes: Vec<u8>,
    
    /// Completed subtree hashes (the "stack")
    /// Only the rightmost path needs to be kept
    stack: Vec<SubtreeInfo>,
    
    /// Stable outboard data (won't change on append)
    /// This grows as left subtrees complete
    stable_hashes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct SubtreeInfo {
    level: u32,
    hash: [u8; 32],
    start_chunk: u64,
}
```

For your example at 4100 bytes:
- `pending_bytes`: 4 bytes
- `stack`: 2 entries (H01 at level 1, H23 at level 1)
- `stable_hashes`: H(C0), H(C1) pairs only (64 bytes)

### Integration with Block Size

When using 16KB blocks (`BlockSize(4)`), the same principles apply but at the block level:

```rust
const BLOCK_SIZE: usize = 16 * 1024;  // 16 KB

// With 16KB blocks:
// - 1500 bytes = partial block 0
// - 4100 bytes = partial block 0 (still! needs 16KB to complete)
// - 20000 bytes = 1 complete block + partial block 1
```

For very small files like your example, you'd typically use `BlockSize(0)` (1KB blocks) or just buffer until you have meaningful amounts of data.

### Summary

| Aspect | What to Store | When to Update |
|--------|--------------|----------------|
| Pending data | 0-1023 bytes | Every write |
| Stack of subtrees | O(log n) hashes | When chunks complete |
| Stable outboard | Left subtrees | When right sibling completes |
| Root hash | Computed on demand | Final merge of stack |

**Key insight**: You never need to re-read completed left subtrees. The incremental state is O(log n) where n is the number of chunks, regardless of file size.

---

## References

- BLAKE3 specification: https://github.com/BLAKE3-team/BLAKE3-specs
- Original bao crate: https://github.com/oconnor663/bao
- bao-tree source: https://github.com/n0-computer/bao-tree
