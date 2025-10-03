Log migration audit — emit-rs → log + env_logger

Purpose

This audit collects every place in the DuckPond workspace that currently depends on the emit-rs-based diagnostics stack or the `DUCKPOND_LOG` environment variable. The goal: provide a concise migration plan and identify non-trivial sites that will require manual work.

High-level summary

- Quick scan found many references to `emit` and `DUCKPOND_LOG` across the repository. Several are documentation examples inside the local `emit-rs/` copy; others are real usages from our crates.
- Key affected crates: `crates/diagnostics`, `crates/cmd`, `crates/tlogfs`, `crates/tinyfs`, and their tests and docs.
- Two classes of work:
  1. Mechanical replacements (macros like `log_info!`, `log_debug!`, `diagnostics::log_info!`) that can be routed to `log`/`env_logger` with a codemod.
  2. Non-trivial features (structured macros `#[emit::as_debug]`, `#[emit::span]`, emit runtime setup and custom emitters, metrics APIs) that need manual redesign or textual fallbacks.

Files with actionable uses (non-exhaustive sample)

- `crates/diagnostics/src/lib.rs` — central diagnostics crate (parses `DUCKPOND_LOG`, calls `emit::setup()`, and exports macros such as `log_info!`, `log_debug!`, `info!`, `debug!`). Example lines:
  - `let log_level = std::env::var("DUCKPOND_LOG").unwrap_or_else(|_| "off".to_string());`
  - `"debug" => emit::setup().emit_to(...).emit_when(...).init(),`
  - `macro_rules! info { ... $crate::emit::info!($($arg)*) }`

- `crates/cmd/src/main.rs` and `crates/cmd/src/commands/*.rs` — many CLI commands call `log_info!` / `log_debug!` and `diagnostics::log_*` macros. Examples:
  - `diagnostics::log_debug!("Main function started");`
  - `log_info!("Pond initialized successfully with transaction #1");`

- `crates/tlogfs/src/persistence.rs` and `crates/tlogfs/src/query/nodes.rs` — hot codepaths with many `diagnostics::log_info!` / `log_debug!` calls. Examples:
  - `diagnostics::log_info!("set_extended_attributes processing attributes for node {node_id_str} at index {index}", attrs_count: attrs_count);`
  - `log_debug!("NodeTable entry - version: {version}, min_event_time: {min_time} ...", ...)`

- `crates/tinyfs/src/dir.rs` — internal library code using `log_debug!` at multiple call sites.

- Docs and examples: `.github/instructions/diagnostics.md`, `crates/docs/*` — many references to `DUCKPOND_LOG` and `emit` examples that will need rewriting.

Non-trivial replacement candidates (manual attention required)

- `#[emit::span(...)]` attribute macros and span tracing examples (e.g., in `emit-rs/examples/*` and some docs). These implement structured spans/traces not directly available in `log`. Decide: (A) convert spans to plain log lines, (B) adopt `tracing` for structured traces (out of scope for this migration), or (C) implement a small textual-span shim. Record these sites for manual review.

- `emit::setup()` and custom emitter initialization (e.g., calls that create a runtime, custom emitters, flushing on drop). These must be converted to `env_logger` initialization or removed where not needed. See `crates/diagnostics/src/lib.rs` and examples inside the `emit-rs/` subtree.

- `emit::metric` and metric-related types present in the `emit-rs` crate. If the application depends on emit metrics, pick an alternative metrics path (prometheus, metrics crate, or textual fallback) and schedule a follow-up.

Quick migration recommendations (recommended staged approach)

1. Short-term compatibility shim (low-risk):
   - Modify `crates/diagnostics` so its `ensure_init()` reads `RUST_LOG` (via `env_logger::Env`) instead of `DUCKPOND_LOG` and initializes `env_logger` (or a small wrapper around it).
   - Re-implement all diagnostic macros in `crates/diagnostics` so they call `log::info!`, `log::debug!`, `log::warn!`, `log::error!`. This preserves the existing call-sites and makes the rest of the repo compile with minimal edits.
   - Remove direct `emit::setup()` calls from `crates/diagnostics` and replace with `env_logger` builder usage. Ensure safe one-time initialization (use `Once`).
   - Acceptance criteria: running `RUST_LOG=debug cargo run --bin pond` produces debug output; `DUCKPOND_LOG` is no longer used.

2. Codemod pass (medium-risk automation):
   - With diagnostics crate changed to use `log`, run a codemod to replace `diagnostics::log_info!` / `log_info!` etc. with `log::info!` or unqualified `info!` after importing `log` macros where appropriate.
   - Replace simple `emit::info!` -> `log::info!` at call sites outside the `emit-rs/` crate.
   - Acceptance criteria: automated changes applied to trivial sites; compile errors are reported for manual fixes.

3. Manual fixes (high-effort):
   - Address structured macros and span/metric usages.
   - Replace custom emit runtime usages; where functionality is important consider porting to `tracing` + `tracing-subscriber` or a metrics solution.

4. Remove `emit-rs/` crate and docs once all code and docs are migrated.

Suggested immediate work items (from this audit)

- Change `crates/diagnostics/src/lib.rs` to:
  - Stop reading `DUCKPOND_LOG` and use `env_logger::Env::default()` / `RUST_LOG`.
  - Replace `$crate::emit::...` macro invocations in macro definitions with `log::...` equivalents so short macros continue to work during migration.
  - Add a `log::set_max_level` mapping if needed.

- Add a test helper to initialize `env_logger` once for tests (`tests/support/logging.rs` or `crates/tests-common`).

- Build a small codemod script (suggestion: `scripts/migrate-emit-to-log.sh`) that performs straightforward text replacements and prints a report of ambiguous/complex sites to a file (`crates/docs/log-migration-nontrivial.csv`).

Appendix: sample mapping table

- `emit::trace!(...)` -> `log::trace!(...)`
- `emit::debug!(...)` -> `log::debug!(...)`
- `emit::info!(...)` -> `log::info!(...)`
- `emit::warn!(...)` -> `log::warn!(...)`
- `emit::error!(...)` -> `log::error!(...)`
- `diagnostics::log_info!(...)` -> `log::info!(...)` (or keep macro and change implementation)
- `emit::setup()` / runtime init -> `env_logger` initialization; remove any runtime-specific plumbing

Notes and risks

- Emit-rs provides structured capture attributes (e.g., `#[emit::as_debug]`) and attribute macros that transform call-sites. Mapping these to `log` will lose the structured capture unless code is redesigned.
- Tests that asserted exact emitted event shapes will need updating to check textual output or be ported to a structured logging solution.
- Docs and examples must be updated, including `.github/instructions/diagnostics.md`.

Next actions (this audit's deliverables)

- This file (`crates/docs/log-migration-audit.md`) was created from a repo-wide scan. It lists the obvious hotspots and non-trivial areas. The next practical step is to start the short-term compatibility shim inside `crates/diagnostics` so the rest of the codebase keeps working; then add `log` and `env_logger` dependencies and run `cargo check`.

Commands to reproduce the scan locally

- rg "emit::|\bemit!\b|emit_rs|emit-rs|DUCKPOND_LOG" -n
- rg "\b(diagnostics::log_info|log_info|log_debug|log_warn|log_error)!" -n crates | sed 's/^/  - /'

## COMPLETED: Full Migration to Standard Log Crate (September 19, 2025)

### What Was Accomplished

The complete migration from the diagnostics crate to standard log crate has been **successfully completed** and validated:

✅ **All Crates Migrated**:
- `crates/steward/`: Clean migration completed
- `crates/cmd/`: Systematic restoration and migration completed  
- `crates/tlogfs/`: Complex logging patterns migrated successfully
- `crates/hydrovu/` & `crates/tinyfs/`: Already clean, no changes needed

✅ **Import Pattern Standardized**:
- Removed: `use diagnostics::*;` and `use diagnostics;`
- Added: `use log::{debug, info};` (specific imports as needed)

✅ **Macro Syntax Modernized**:
- Old format: `diagnostics::log_debug!("message {param}", param: value)`
- New format: `log::debug!("message {value}")`  
- All complex parameter-based diagnostic macros converted to standard log string interpolation

✅ **Comprehensive Validation**:
- **219 tests passed, 0 failed** - Full test suite validation
- Clean compilation with zero errors
- Focused git diff showing only diagnostics-related changes

### Technical Decision: Why Standard Log Crate

**Key Reason**: Despite repeated attempts to work with emit-rs/diagnostics syntax, **GitHub Copilot consistently tripped over the complex diagnostic macro patterns**, leading to:
- Systematic corruption of files during previous migration attempts
- Repeated syntax errors with emit-rs structured logging features
- Difficulty maintaining the complex parameter-based macro syntax

**Practical Benefits Realized**:
- **Copilot Familiarity**: GitHub Copilot is extensively trained on standard `log` crate patterns
- **Ecosystem Standard**: `log` + `env_logger` is the de facto Rust logging standard
- **Simplified Syntax**: String interpolation is more intuitive than parameter-based macros
- **Maintainability**: Future developers will immediately understand standard log patterns

### Migration Strategy Used

1. **Corruption Detection & Recovery**: Implemented systematic file restoration when non-diagnostics changes were detected
2. **Strict Discipline**: "We only want to remove 'use diagnostics::*' or 'use diagnostics' and insert 'use log::*'! If we lost an anyhow import, START OVER."  
3. **Systematic Validation**: Used `cargo check` and `cargo test` after each major change
4. **Clean Diff Review**: Ensured final `git diff HEAD` shows only intended diagnostics changes

### Final State

The migration is **complete and production-ready**:
- All code compiles cleanly
- Full test suite passes (219/219 tests)  
- Clean git diff focused purely on logging migration
- Standard Rust logging patterns throughout codebase
- Ready for commit and deployment

**For posterity**: We chose standard log over emit-rs not due to technical limitations of emit-rs, but because GitHub Copilot's training on standard log patterns made it the pragmatic choice for maintainable, AI-assisted development.


