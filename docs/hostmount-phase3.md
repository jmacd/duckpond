# Hostmount Phase 3 -- Design & Implementation Record

**Status: COMPLETE** (Phase 3 + Phase 3B)

## What Phase 3 Proposes

Phase 3 adds a `-d <dir>` global CLI flag, refactors `ShipContext` so commands can
determine whether they need a pond, hostmount, or both based on URL arguments, and
applies chroot semantics to the hostmount root.

## What Was Already in Place (Pre-Phase 3)

| Component | Pre-Phase-3 Status | Location |
|-----------|--------|----------|
| `HostmountPersistence` | Complete, 24 tests | `crates/tinyfs/src/hostmount/` |
| `Steward::Host(HostSteward)` | Variant exists, fully wired | `crates/steward/src/dispatch.rs` |
| `Steward::open_host(root_path)` | Ready | `dispatch.rs:70` |
| `Transaction::Host(HostTransaction)` | `Deref<Target=FS>`, `provider_context()`, `session_context()` all work | `crates/steward/src/host.rs` |
| `HostTransaction::get_factory_for_node()` | Stub: always returns `None` | `host.rs:94` |
| `cat_host_impl` | Pre-existing bypass, no tinyfs involvement | `cat.rs:168-170` |
| `copy.rs` host detection | `strip_prefix("host://")`, separate from provider `Url` | `copy.rs:23-27` |

**Key observation at the time:** `Steward::Host` was fully functional but *never constructed
from any CLI code path*. No command called `Steward::open_host()`. The entire Phase 2
host infrastructure was unused plumbing. Phase 3 + 3B resolved this.

## Command Landscape (Pre-Phase 3)

| Command | Pre-Phase-3 host behavior | Needs pond? |
|---------|----------------------|-------------|
| `cat` | `host+` prefix -> `cat_host_impl` (standalone, no tinyfs) | Conditional |
| `copy` | `host://` prefix -> string stripping, always opens pond | Yes |
| `run` | Always opens pond | Yes |
| `list` | Always opens pond | Yes |
| `list-factories` | No pond at all | No |
| Everything else | Always opens pond | Yes |

**Post-Phase 3B:** `cat`, `copy`, and `list` all route host URLs through
`Steward::Host` via `classify_target()`. `cat_host_impl` was removed.
`copy` uses dual-steward (host + pond) for both directions.

## Interaction with copy-url-design.md

The `copy-url-design.md` proposes a second, conflicting URL grammar for host access.
These two designs must be reconciled before Phase 3 work begins.

### The conflict: two ways to say "host file with format"

| Design | URL syntax | Example |
|--------|-----------|---------|
| Hostmount | `host+scheme:///path` (prefix) | `host+csv:///tmp/data.csv` |
| Copy-URL | `scheme://host/path` (authority) | `csv://host/tmp/data.csv` |

Both express the same intent ("read a host file through a format provider") but
with incompatible URL structures. Today `pond cat` already uses the prefix style
(`host+csv:///path`). The copy-url-design proposes the authority style for `pond copy`.

If both are implemented, we'd have `cat` using prefix syntax and `copy` using
authority syntax for the same operation -- exactly the kind of inconsistency the
hostmount design was created to eliminate.

### Where they agree

- Raw host copy: both use `host:///path` (scheme = `host`, no format)
- Entry type modifiers: copy-url adds `+table`/`+series` suffixes -- orthogonal,
  not conflicting. Hostmount doesn't propose these but doesn't contradict them.
- Both want to replace `strip_prefix("host://")` with proper URL parsing.

### Where they diverge

| Topic | Hostmount design | Copy-URL design |
|-------|-----------------|-----------------|
| `host` position | Prefix on scheme (`host+csv`) | Authority (`csv://host`) |
| Format-driven host access | Through hostmount FS + provider | Direct `tokio::fs` + provider |
| `-d` / chroot | Yes | Not mentioned |
| Entry type in URL | Not proposed | `+table`/`+series` suffixes |
| `provider::Url` changes | `is_host` flag (exists) | `entry_type` field + authority parsing (new) |

### The reconciliation question (new Q0)

These designs were written at different times. Before Phase 3 proceeds, the URL
grammar must be unified. See Q0 below.

---

## Open Questions

### Q0: Unified URL grammar -- prefix vs authority for `host`

The two designs propose incompatible grammars:

**Option A: Prefix only** (`host+scheme:///path`) -- what `cat` already uses.
All host URLs start with `host+`. Simple to detect (`starts_with("host+")`).
`host` is always the leftmost segment. `provider::Url` already has `is_host` for this.

```
host+csv:///data.csv           -> host file, CSV format
host+csv+gzip:///data.csv.gz   -> host file, CSV, gzipped
host+table:///data.parquet     -> host file, table entry type
host+series:///data.parquet    -> host file, series entry type
host+csv+series:///data.csv    -> host file, CSV format, series entry type
```

**Option B: Authority only** (`scheme://host/path`) -- what copy-url proposes for
format-driven access. `host` goes in the URL authority position (between `://` and path).
More "correct" URL semantics (authority = where, scheme = what).

```
csv://host/data.csv            -> host file, CSV format
csv+gzip://host/data.csv.gz   -> host file, CSV, gzipped
host:///data.parquet           -> host file, raw bytes (no authority)
```

**Option C: Both** -- prefix for raw/entrytype, authority for format. This is what
the two docs collectively propose, but it means two different syntaxes for "host file"
depending on whether a format provider is involved:
- `host+table:///f.parquet` (prefix, no format provider)
- `csv://host/f.csv` (authority, format provider)

**Option D: Prefix always, kill authority** -- standardize on `host+` prefix everywhere.
`csv://host/path` is never valid. This means copy-url-design's authority approach is
rejected in favor of consistency with the existing `cat` syntax.

My read: Option D (prefix always) is simplest. The existing `pond cat host+csv:///path`
syntax works and is already implemented. Introducing a second syntax for the same
concept adds complexity without clear benefit. The "semantic correctness" of authority
position doesn't outweigh the cost of two syntaxes.

If D is chosen, the copy-url-design needs to be updated to use prefix syntax:
```
pond copy host+csv:///data.csv /tables/         -> copy IN with CSV parsing
pond copy host+csv+gzip:///data.csv.gz /tables/ -> copy IN with CSV+gzip
pond copy host+series:///data.parquet /tables/   -> copy IN as series
```

**Decision:**

(D). This was an unintentional divergence. The layering of schemes makes more sense, and I do not want to keep the other style.

### Q1: Deliverable scope -- what does Phase 3 ship?

The motivating example (`pond run -d ./water host+sitegen://site.yaml build ./dist`)
requires Phases 3 + 4 + 5 together. Phase 3 alone delivers no user-visible behavior
change.

Two options:

**A. Narrow Phase 3** -- plumbing only:
- Add `-d <dir>` flag to `Cli` struct
- Add `host_root: Option<PathBuf>` to `ShipContext`
- Add `ShipContext::open_host()` -> `Steward::Host(...)`
- Don't change any command -- no command uses this path yet
- Value: clean foundation, no risk

**B. Phase 3 includes one command migration** -- plumbing + proof of life:
- Everything in (A), plus
- Migrate `pond cat host+...://` to use the hostmount path instead of `cat_host_impl`
- Value: proves the architecture works end-to-end

Which scope?
**Decision:**

(A) narrow

### Q2: Where does "do I need a pond?" logic live?

Commands have different argument shapes (`cat` takes one path, `copy` takes N
sources + dest, `run` takes path + args). The "inspect arguments to determine
context" logic needs a home.

**A. ShipContext method** -- e.g. `ship_context.open_for_urls(&["host+csv:///a.csv", "/tables/"])`
inspects the URLs and returns the right `Steward` variant (or both).

**B. Per-command** -- each command checks its own args, calls `open_pond()` or
`open_host()` directly. This is what `cat` already does with the `host+` prefix check.

**C. CLI-level resolver** -- parse all path arguments at the top level (before command
dispatch) and determine the context once. Pass a resolved environment to commands.

My read: (B) is simplest and matches the existing pattern. The context determination
is command-specific anyway (`copy` cares about source vs dest direction; `cat` doesn't).

**Decision:**
I prefer (C) and (B). A list of URLs is specific to the command syntax, but the logic of determining which can be combined somewhere in crates/cmd.

### Q3: `-d` default -- CWD or explicit-only?

The design says the default (no `-d`) is the process working directory.

This means `pond cat host+csv:///data.csv` with no `-d` resolves `/data.csv` relative
to CWD, giving `./data.csv`. But today, `host+csv:///data.csv` means the *absolute*
host path `/data.csv` (the URL path is treated literally).

Three options:

**A. `-d` only takes effect when explicitly provided.** Without `-d`, `host+` URLs
use absolute paths (current behavior preserved). With `-d ./water`, paths resolve
under `./water/`.

**B. `-d` always defaults to CWD.** This is a breaking change for existing
`host+csv:///absolute/path` URLs.

**C. Distinguish absolute from relative** in the URL:
- `host+csv:///full/path` (triple slash = absolute, ignores `-d`)
- `host+csv://relative/path` (double slash = relative, uses `-d`)

My read: (A) avoids breaking current behavior and is simplest. The chroot semantics
only activate when `-d` is explicit. This also avoids interaction with the copy-url
design -- `+table`/`+series` suffixes work regardless of how the path is resolved.

**Decision:**
(A)

### Q3a: Entry type suffixes (`+table`/`+series`) -- Phase 3 or separate?

The copy-url-design proposes extending `provider::Url` with `+table`/`+series`
suffixes. This changes the URL parser that hostmount Phase 3 would use.

Options:
- **Do it in Phase 3** -- extend `provider::Url` with entry type parsing before
  wiring hostmount, so the URL model is complete
- **Do it separately** -- the copy-url-design's Phase 1 (extend `provider::Url`)
  can happen independently as a prerequisite, then Phase 3 builds on top

Either way, the entry type suffixes don't conflict with hostmount -- they're additive.
But the work ordering matters for avoiding merge conflicts in `provider::Url`.

**Decision:**
Do this in phase3. I want to ensure the URL structure and interpretation is powerful and consistent. I think the +table and +series schemes should be optional and that the default behavior may be conditioned on the command or the file type with different capabilities for host and pond. Pond knows entry type so +table and +series can be implicit, but for host it will be required in some cases.

### Q4: Mixed operations -- two stewards in one command?

`pond copy host+csv:///data.csv /tables/` needs both a hostmount (source) and a
pond (destination). Today this works by string-stripping `host://` and using raw
`tokio::fs` for the host side.

The copy-url-design addresses this same use case but doesn't use the hostmount at
all -- it proposes reading host files via `tokio::fs` + format providers, with no
tinyfs involvement on the host side. The hostmount design's Phase 7 says the opposite:
replace all ad-hoc host access with hostmount tinyfs.

This is a crucial architectural question: **does `pond copy` host access go through
hostmount, or remain direct `tokio::fs`?**

**A. Hostmount for everything** -- `copy` opens a `Steward::Host` for source,
`Steward::Pond` for dest, reads through tinyfs on both sides. Clean but means two
active stewards/transactions per command.

**B. Direct I/O for copy, hostmount for factories** -- `pond copy` keeps its
direct host file reading (maybe cleaned up with `provider::Url` parsing). Hostmount
is only used by commands that need a full FS context (like `pond run` with sitegen).
This matches the copy-url-design.

**C. Defer** -- Phase 3 doesn't touch `copy` at all. For Phase 3, only commands
that are purely host-side (like `cat host+...`) would use the hostmount. `copy`
is migrated later.

My read: (C) for Phase 3 scope, with (B) as the long-term answer. `copy` doesn't
need a full tinyfs FS for its host side -- it just needs to read bytes. The hostmount's
value is for commands that navigate directory trees and resolve paths (factories, list).

**Decision:**
(A) Yes we want two active stewards in this case.

### Q5: Does `-d` interact with `--pond`?

Valid combinations:

| Flags | Meaning |
|-------|---------|
| `--pond /my/pond` | Pond only (current) |
| `-d ./water` | Hostmount only, rooted at `./water` |
| `--pond /my/pond -d ./water` | Both available; command routes by URL |
| Neither | `POND` env var for pond (if needed); no hostmount |

Questions:
- Is `-d` without `--pond`/`POND` a valid configuration? (Yes for `cat host+...`,
  no for `list /something`)
- Should `-d` set a `HOSTMOUNT_ROOT` env var as fallback, analogous to `POND`?
- When `-d` is set but the command needs a pond (no `host+` URLs), should the error
  message say "this command requires a pond" or silently ignore `-d`?

**Decision:**
You should only require POND or --pond when the command requires a pond. If the command doesn't, b/c e.g., it uses only host access, then it's OK to omit. The host filesystem always exists, so this is not symmetric. You can always access a host file path, and -d is always optional. OK to supply -d even when no host file access.

### Q6: Should `cat_host_impl` be migrated or left alone?

`cat_host_impl` works today without any tinyfs involvement:
host file -> format provider -> MemTable -> DataFusion -> output.

Routing through hostmount means: `HostSteward` -> `HostTransaction` -> `FS` -> `WD`
-> resolve path -> get reader -> format provider -> MemTable -> query. More machinery.

Benefits of migration:
- Consistency: every command follows the same pattern
- Glob support: `pond cat host+csv:///data/*.csv` could work (cat currently takes
  exact paths though)
- Path resolution: hostmount handles `..` traversal guard, symlink semantics

Benefits of leaving it:
- Simpler code path for a simple operation
- No regression risk
- Performance: fewer allocations, no persistence layer overhead

This is a Phase 7 question per the design doc, but it determines whether Phase 3
has a user-visible deliverable.

**Decision:**
Migrate it. I want consistent use of tinyfs.

### Q7: What about `pond list -d ./water`?

If hostmount is available, `pond list` could list host files through tinyfs.
This would be useful for debugging ("what does the hostmount see?") but it's
not in the design.

Should `pond list` gain host awareness in Phase 3, or is that a later concern?

**Decision:**
Hostmount is always available, using CWD or -d to override. This question seems to almost answer itself. The HostMount tinyfs provider will just work.

### Q8: HostTransaction metadata

`Steward::begin_read/write` takes `&PondUserMetadata` for audit logging. The Host
variant silently drops it:

```rust
Steward::Host(host) => Ok(Transaction::Host(host.begin().await?)),
```

Should `HostTransaction` carry the metadata for debugging/logging purposes, even
without a control table? Minor point, but easier to add now than retrofit.

**Decision:**
I feel this is useful for debugging purposes. You can observe the host transaction even if it can't abort, will be helpful for debugging.

### Q9: Error message improvements

Today, `pond list` without `POND` set gives: `"POND environment variable not set"`.

With `-d` available, should the error suggest using `-d` for host operations?
Example: `"POND not set. Use -d <dir> for host filesystem operations or set POND
for pond access."`

This is UX polish but affects discoverability of the hostmount feature.

**Decision:**

I think it's OK to keep the default URL as a pond URL, meaning you have to use the full URL syntax to access host files but otherise a path like "/" expands into the active pond root dir, so "pond list" with its default will require a POND. No need to mention -d for this scenario.

---

## Decision Summary

| Question | Decision |
|----------|----------|
| Q0: URL grammar | (D) Prefix always (`host+scheme:///path`). Kill authority style. Update copy-url-design. |
| Q1: Phase 3 scope | (A) Narrow plumbing + `list` as proof-of-life. `cat` migration deferred to 3B. |
| Q2: Context routing | (C)+(B) — shared URL classification in cmd crate, commands pass their own args. |
| Q3: `-d` default | (A) — explicit only; without `-d`, `host+` URLs use absolute paths. |
| Q3a: `+table`/`+series` | Do in Phase 3. Defaults depend on command/context. Pond knows entry type; host requires explicit in some cases. |
| Q4: Mixed operations | (A) Two active stewards for commands that touch both host and pond. |
| Q5: `-d` vs `--pond` | POND only required when command needs pond. Host always available. `-d` always optional. |
| Q6: `cat_host_impl` | Migrate to hostmount tinyfs for consistency. |
| Q7: `pond list` on host | Should just work via hostmount — not a separate feature. |
| Q8: HostTransaction metadata | Yes, carry `PondUserMetadata` for debugging. |
| Q9: Error messages | No change. Bare paths remain pond paths; no `-d` hint needed. |

### Tensions to resolve

**Q1 vs Q6 — resolved by choosing `list` as proof-of-life instead of `cat`.**
`list` is pure FS — directory traversal and glob matching, no format providers,
no DataFusion, no MemTable. It exercises exactly the hostmount value (directory
tree through tinyfs) without the `cat` migration's complexity (format pipeline,
SQL, stream handling, replacing `cat_host_impl`). `list` has no existing host
bypass to replace — it's a clean addition. `cat` migration (Q6) is deferred to
Phase 3B.

`pond list host+file:///tmp/data/` (or `pond -d /tmp/data list`) becomes the
Phase 3 proof-of-life: it opens a `Steward::Host`, traverses host directories
through tinyfs, and prints entries. If that works, the hostmount plumbing is proven.

---

## Suggested Task Breakdown

Decisions narrow Phase 3 to: URL parser extensions, `-d` plumbing, context routing,
and `list` as the single proof-of-life command. `cat` migration and `copy` dual-steward
are Phase 3B+.

### Phase 3: URL parser + plumbing + `list` proof-of-life

| Task | Description | Dependencies |
|------|-------------|-------------|
| 3-1 | Update copy-url-design.md: replace authority style with prefix style (Q0) | None |
| 3-2 | Extend `provider::Url` with `+table`/`+series` entry type parsing (Q3a) | None |
| 3-3 | Add `-d <dir>` global flag to `Cli` struct | None |
| 3-4 | Add `host_root: Option<PathBuf>` to `ShipContext`; `-d` populates it | 3-3 |
| 3-5 | Add `ShipContext::open_host()` -> `Steward::Host(...)` | 3-4 |
| 3-6 | `HostTransaction` carries `PondUserMetadata` for debugging (Q8) | None |
| 3-7 | Shared URL classification in cmd: detect `host+` URLs, determine context (Q2) | 3-2 |
| 3-8 | Wire `list` to use host steward when pattern starts with `host+` (proof-of-life) | 3-5, 3-7 |
| 3-9 | Unit + integration tests for all of the above | 3-2..8 |

### Phase 3B: Remaining command migrations

| Task | Description | Dependencies |
|------|-------------|-------------|
| 3B-1 | Migrate `cat` to use hostmount (replaces `cat_host_impl`) (Q6) | Phase 3 |
| 3B-2 | Context routing for dual-steward commands (Q4) | Phase 3 |
| 3B-3 | Migrate `copy` host handling to hostmount | 3B-2 |
| 3B-4 | Integration tests: cat + copy on host files via hostmount | 3B-1, 3B-3 |

### Not in Phase 3 (future)

| Task | Phase | Description |
|------|-------|-------------|
| `pond run` on hostmount | Phase 5 | Factory dispatch from host FS |
| Unified scheme registry | Phase 4 | Factory names in URL schemes |
| Deprecate `--format` flag on copy | Phase 4+ | Entry type from URL replaces flag |

---

## Implementation Record

All Phase 3 and Phase 3B tasks are **complete**. Summary of what was
implemented and where.

### Task 3-1: Update copy-url-design.md (Q0 -- prefix always)

Authority-style host URLs (`csv://host/path`) replaced with prefix-style
(`host+csv:///path`) throughout `docs/copy-url-design.md`. 10 replacements.
The document now matches the Q0=D decision.

### Task 3-2: Extend `provider::Url` with entry type parsing (Q3a)

`provider::Url` gained a multi-segment `+` suffix classifier that categorizes
scheme components into format, compression, and entry type.

| File | Changes |
|------|---------|
| [crates/provider/src/lib.rs](crates/provider/src/lib.rs) | New fields: `entry_type`, `full_scheme_str`, `is_host`. Classifier uses `KNOWN_ENTRY_TYPES` (`table`, `series`) and `KNOWN_COMPRESSIONS` (`zstd`, `gzip`, `bzip2`). Bare `series:///path` canonicalizes to format=`file` + entry_type=`series`, Display: `file+series:///path`. 11 new tests (136 total passing). |
| [crates/provider/src/factory/temporal_reduce.rs](crates/provider/src/factory/temporal_reduce.rs) | `source_url()` uses `full_scheme()` instead of `scheme()` to preserve multi-segment schemes. |
| [crates/provider/src/factory/timeseries_pivot.rs](crates/provider/src/factory/timeseries_pivot.rs) | `series://` changed to `file+series://` for canonical URL construction. |
| [crates/provider/src/url_pattern_matcher.rs](crates/provider/src/url_pattern_matcher.rs) | Dispatch uses `entry_type()` priority over `scheme()` for builtin type matching. `series:///path` now correctly filters to `TablePhysicalSeries` + `TableDynamic`. |

### Task 3-3 / 3-4 / 3-5: `-d` flag, `ShipContext`, `open_host()`

| File | Changes |
|------|---------|
| [crates/cmd/src/main.rs](crates/cmd/src/main.rs) | `-d` / `--directory` global clap flag added to `Cli` struct. Passed to `ShipContext::new()`. |
| [crates/cmd/src/common.rs](crates/cmd/src/common.rs) | `ShipContext` gained `host_root: Option<PathBuf>` field. `open_host()` method constructs `Steward::Host(...)` using `-d` path (or `/` default). `pond_only()` convenience constructor added for test sites that never use host access. |

### Task 3-6: HostTransaction carries PondUserMetadata (Q8)

| File | Changes |
|------|---------|
| [crates/steward/src/host.rs](crates/steward/src/host.rs) | `begin()` accepts `&PondUserMetadata`. `HostTransaction` stores `meta` field with `meta()` accessor. |
| [crates/steward/src/dispatch.rs](crates/steward/src/dispatch.rs) | `begin_read/write` pass metadata through to `host.begin(meta)`. |

### Task 3-7: Shared URL classification (Q2)

| File | Changes |
|------|---------|
| [crates/cmd/src/common.rs](crates/cmd/src/common.rs) | `classify_target()` function uses `provider::Url::parse` to determine `TargetContext::Pond(path)` vs `TargetContext::Host(path)`. 12 unit tests covering host URLs, pond URLs, bare paths, globs, absolute paths. |

### Task 3-8: Wire `list` command to host steward

| File | Changes |
|------|---------|
| [crates/cmd/src/commands/list.rs](crates/cmd/src/commands/list.rs) | `list_command()` calls `classify_target(pattern)` and dispatches to `ship_context.open_host()` for `TargetContext::Host`, `ship_context.open_pond()` for `TargetContext::Pond`. |

### Task 3-9: Tests

| Test | Description |
|------|-------------|
| `crates/cmd/src/common.rs` (12 unit tests) | `classify_target` for host URLs, pond URLs, bare paths, globs, absolute paths, edge cases. |
| `crates/provider/src/lib.rs` (11 new tests) | Entry type parsing, multi-segment classifier, canonicalization, `is_host` detection. |
| [testsuite/tests/300-hostmount-list-proof-of-life.sh](testsuite/tests/300-hostmount-list-proof-of-life.sh) | Integration test: host URL listing, recursive glob, `-d` flag, no POND required. |

### Follow-up fixes (post-task)

| Fix | Description |
|-----|-------------|
| `url_pattern_matcher` dispatch | Changed to use `entry_type().unwrap_or_else(\|\| scheme())` as type key, so `series:///path` matches `TablePhysicalSeries` correctly. |
| `classify_target` proper parsing | Replaced ad-hoc string checks with `provider::Url::parse` for robust host detection. |
| `ShipContext::pond_only()` | Added convenience constructor; converted 22 test call sites from `ShipContext::new(..., None::<&str>, ...)` to eliminate turbofish noise. |

### Test results

All tests pass across the three modified crates:

| Crate | Tests |
|-------|-------|
| `provider` | 136 |
| `cmd` | 101 |
| `steward` | 16 |
| **Total** | **253** |

### What's next: Phase 3B

Phase 3 proved the hostmount plumbing works end-to-end with `list`. Phase 3B
migrates additional commands:

| Task | Description | Status |
|------|-------------|--------|
| 3B-1 | Migrate `cat` to use hostmount (replaces `cat_host_impl`) | **COMPLETE** |
| 3B-2 | Context routing for dual-steward commands | **COMPLETE** |
| 3B-3 | Migrate `copy` host handling to hostmount | **COMPLETE** |
| 3B-4 | Integration tests for cat + copy on host files via hostmount | **COMPLETE** |

### Task 3B-1: Migrate `cat` to hostmount — COMPLETE

Replaced the ad-hoc `cat_host_impl` bypass with proper hostmount tinyfs routing.
Host files are now read through the same `Provider` + `FS` pipeline as pond files.

| File | Changes |
|------|---------|
| [crates/cmd/src/commands/cat.rs](crates/cmd/src/commands/cat.rs) | `cat_command`: replaced `starts_with("host+")` check with `classify_target()` dispatch. Opens `Steward::Host` for host URLs, `Steward::Pond` for pond paths. Both pass through `cat_impl`. Removed `cat_host_impl` function (40 lines). |
| [crates/cmd/src/commands/cat.rs](crates/cmd/src/commands/cat.rs) | `cat_impl`: replaced `url.strip_prefix("file://")` string matching with `provider::Url::parse()` for proper URL classification. Now correctly handles both `file:///path` (pond) and `host+file:///path` (host) for raw byte streaming. Added `entry_type().is_none()` guard so `host+series:///` and `host+table:///` URLs skip the raw-streaming path. |
| [crates/cmd/src/commands/cat.rs](crates/cmd/src/commands/cat.rs) | Default SQL query changed from `SELECT * FROM source ORDER BY timestamp` to `SELECT * FROM source`. The `ORDER BY timestamp` assumed time-series data, which fails for files without a `timestamp` column (e.g., host CSV files). |
| [crates/cmd/src/commands/cat.rs](crates/cmd/src/commands/cat.rs) | 5 new host cat tests: raw file streaming, CSV format provider, CSV with SQL query, absolute path (no `-d`), nonexistent file error. |

#### How it works

For `pond cat host+csv:///data.csv` (with `-d ./mydir`):

1. `cat_command` calls `classify_target("host+csv:///data.csv")` -> `TargetContext::Host`
2. Opens `Steward::Host(root=./mydir)` via `ship_context.open_host()`
3. Begins a host transaction (creates hostmount `FS` + standalone DataFusion session)
4. Calls `cat_impl(&mut tx, "host+csv:///data.csv", ...)`
5. `cat_impl` parses URL: scheme=`csv`, is_host=true, path=`/data.csv`
6. Scheme is not `file`, so skips raw-streaming check
7. Creates `Provider::with_context(hostmount_fs, host_provider_context)`
8. `Provider.for_each_match("host+csv:///data.csv", ...)`:
   - scheme=`csv` -> format provider lookup -> csv provider
   - `create_memtable_from_url` -> `fs.open_url(&url)` -> hostmount reads `./mydir/data.csv`
   - CSV format provider parses -> MemTable
9. Registers MemTable as `source`, runs SQL, streams output

For `pond cat host+file:///readme.txt`:
- scheme=`file`, entry_type=None -> metadata check -> `FilePhysicalVersion` (not queryable)
- Streams raw bytes via `stream_file_to_stdout` through hostmount `async_reader_path`

### Tasks 3B-2/3B-3: Dual-steward copy + hostmount migration — COMPLETE

Replaced all direct `tokio::fs`/`std::fs` host file access in `copy_out` with
dual-steward hostmount tinyfs. Both copy directions now go through tinyfs:
copy-in uses host steward (read) + pond steward (write), copy-out uses
pond steward (read) + host steward (write).

| File | Changes |
|------|---------|
| [crates/cmd/src/commands/copy.rs](crates/cmd/src/commands/copy.rs) | `copy_out()`: opens host steward (write) + pond steward (read). Uses `host_root.create_dir_all()` for directory creation. No direct filesystem calls. |
| [crates/cmd/src/commands/copy.rs](crates/cmd/src/commands/copy.rs) | `export_queryable_file_as_parquet()`: replaced sync `ArrowWriter` + `std::fs::File` with `AsyncArrowWriter` + `host_wd.async_writer_path()`. Streams batches from DataFusion SQL directly to hostmount. |
| [crates/cmd/src/commands/copy.rs](crates/cmd/src/commands/copy.rs) | `export_raw_file()`: replaced `tokio::fs::File::create` with `host_wd.async_writer_path()` + `tokio::io::copy`. |
| [crates/tlogfs/src/query/sql_executor.rs](crates/tlogfs/src/query/sql_executor.rs) | Table name standardized to `"source"` (was `"series"`). Deregisters stale table before registering, and deregisters after `execute_stream()`. |
| [crates/cmd/src/commands/cat.rs](crates/cmd/src/commands/cat.rs) | Added `ctx.deregister_table("source")` after `df.execute_stream().await`. |
| [crates/cmd/src/commands/export.rs](crates/cmd/src/commands/export.rs) | SQL template changed `FROM series` to `FROM source`. Added deregister after COPY. |
| [crates/tlogfs/src/tests/mod.rs](crates/tlogfs/src/tests/mod.rs) | Test SQL queries updated `FROM series` to `FROM source`. |
| [crates/tinyfs/src/wd.rs](crates/tinyfs/src/wd.rs) | Removed dead `copy_to_parquet` method. |

#### DataFusion table registration conventions

- Table name is always `"source"` (matching existing CLI docs and SQL convention)
- Deregister before registering (handles re-use within same session)
- Deregister after `execute_stream()` — execution plan holds its own `Arc<dyn TableProvider>`,
  so removing from the catalog is safe and prevents stale bindings

### Task 3B-4: Integration tests — COMPLETE

| Test | Description |
|------|-------------|
| [testsuite/tests/302-hostmount-copy-out.sh](testsuite/tests/302-hostmount-copy-out.sh) | 6 test scenarios, 14 checks: raw data export, series-as-parquet export (PAR1 magic + size verification), directory structure preservation, glob pattern export, full roundtrip (out then back in with content verification), `-d` flag support. |

#### Test results

All unit and integration tests pass:

| Suite | Tests |
|-------|-------|
| `cmd` (unit) | 202 |
| `provider` (unit) | 143 |
| `tlogfs` (unit) | 58 |
| `steward` (unit) | 16 |
| Testsuite (integration) | 38+ including 302 |

---

## Final Summary

Phase 3 and Phase 3B are **complete**. All 13 tasks (3-1 through 3-9 and 3B-1
through 3B-4) are implemented, tested, and documented.

### What was delivered

**Phase 3** established the hostmount plumbing:

- Unified URL grammar: `host+scheme:///path` prefix style everywhere (Q0=D)
- `provider::Url` extended with `+table`/`+series` entry type parsing
- `-d <dir>` global CLI flag with `ShipContext` integration
- `classify_target()` shared URL classification
- `pond list` host awareness as proof-of-life
- `HostTransaction` carries `PondUserMetadata` for debugging

**Phase 3B** migrated all commands to use hostmount consistently:

- `cat`: replaced ad-hoc `cat_host_impl` with hostmount tinyfs routing
- `copy` (both directions): dual-steward pattern -- host steward + pond steward
  operating simultaneously, with `AsyncArrowWriter` for parquet export
- DataFusion table name standardized to `"source"` with proper deregistration
- Dead code removed (`copy_to_parquet`, `cat_host_impl`)

### Architecture after Phase 3B

Every CLI command that touches host files now goes through the same path:

```
CLI argument
  -> classify_target() -> TargetContext::Host | TargetContext::Pond
  -> Steward::Host (hostmount tinyfs) | Steward::Pond (tlogfs)
  -> Transaction -> FS -> WD -> read/write operations
```

For dual-steward commands (`pond copy`), both stewards are open simultaneously:
- Copy IN: host steward (read) + pond steward (write)
- Copy OUT: pond steward (read) + host steward (write)

No production code uses `tokio::fs` or `std::fs` for host file access.

### Files modified

| Crate | Files | Nature of changes |
|-------|-------|-------------------|
| `cmd` | `main.rs`, `common.rs`, `cat.rs`, `copy.rs`, `export.rs` | `-d` flag, URL classification, hostmount routing, dual-steward, AsyncArrowWriter |
| `provider` | `lib.rs`, `url_pattern_matcher.rs`, `temporal_reduce.rs`, `timeseries_pivot.rs` | Entry type parsing, multi-segment URL classifier |
| `steward` | `host.rs`, `dispatch.rs` | PondUserMetadata on HostTransaction |
| `tlogfs` | `sql_executor.rs`, `tests/mod.rs` | Table name "source", deregistration |
| `tinyfs` | `wd.rs` | Removed dead `copy_to_parquet` |
| `docs` | `copy-url-design.md`, `hostmount-phase3-questions.md` | Authority->prefix URL style, implementation records |

### Integration tests

| Test | Covers |
|------|--------|
| `300-hostmount-list-proof-of-life.sh` | `list` via host steward, `-d` flag, recursive glob |
| `301-hostmount-cat-variations.sh` | `cat` host files: raw, CSV, CSV+SQL, CSV+series, zstd, no-POND |
| `302-hostmount-copy-out.sh` | `copy` out: raw export, parquet export, directory structure, glob, roundtrip, `-d` flag |

### What's next

| Task | Phase | Description |
|------|-------|-------------|
| `pond run` on hostmount | Phase 5 | Factory dispatch from host FS |
| Unified scheme registry | Phase 4 | Factory names in URL schemes |
| Deprecate `--format` flag on copy | Phase 4+ | Entry type from URL replaces flag |
