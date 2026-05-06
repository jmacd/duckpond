# sandbox

Throwaway prototype for the remote-sync redesign described in
`../docs/efficiency-priorities.md` (and refined in subsequent work).

This is **not part of the duckpond runtime**.  It is a standalone Cargo
workspace that exercises the sync/checkpoint/vacuum/verify design in
isolation, without tinyfs/tlogfs/provider/factory/cli machinery.

## Build and test

```bash
cd duckpond/sandbox
cargo test --workspace
cargo clippy --workspace --all-features -- -D warnings
cargo fmt --all -- --check
```

The duckpond workspace at `duckpond/Cargo.toml` does **not** include
sandbox as a member; cargo commands in `duckpond/` ignore it.

## Crates

| Crate | Role |
|---|---|
| `store` | Delta-Lake-backed key/value store + per-partition checksum primitive |
| `steward` | Transaction lifecycle + control table + maintenance + verification |
| `remote` | Bundle-manifest layer (push/pull) over a local "remote" Delta table |
| `tests` | End-to-end and property-based tests across the other crates |

No CLI binary.  The library API is the public surface; tests exercise it
directly.

## Local-only Delta Lake

The "remote" is a local Delta table at a different path.  No S3, no
MinIO, no testcontainers.  When the design is mapped back to duckpond,
swapping `LocalFileSystem` for `S3` in `object_store` is a configuration
change; the mechanics in `remote/` do not change.

## Disposability

`git rm -r duckpond/sandbox` plus revert any CI/Makefile additions and
this prototype is gone.  Nothing in `duckpond/crates/` imports it; the
prototype's value is the design, not the code.
