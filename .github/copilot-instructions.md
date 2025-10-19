# DuckPond - Copilot Instructions

## 🛑 MANDATORY: Read docs BEFORE suggesting code

### When user mentions these keywords → Read the doc FIRST:

| Trigger Words | Read This Doc | Why |
|--------------|---------------|-----|
| transaction, State, SessionContext, persistence, begin, commit, OpLogPersistence, TransactionGuard | `docs/duckpond-system-patterns.md` | Single transaction rule - most common violation |
| similar function, like existing, another version, _v2, _with_ | `docs/anti-duplication.md` | Refactor, don't duplicate |
| error handling, fallback, unwrap_or, default, .or_else() | `docs/fallback-antipattern-philosophy.md` | Fail fast, no silent fallbacks |
| debug, test, grep, tail, head, output, logs | `docs/large-output-debugging.md` | Redirect to file, don't truncate |
| node_id, part_id, TableProvider, QueryableFile | `docs/duckpond-system-patterns.md` | NodeID/PartID relationships |

### Critical Rules (Inline - No Excuses)

#### ❌ NEVER: Multiple transactions
```rust
let tx1 = persistence.begin().await?;  // Only ONE per operation
let tx2 = persistence.begin().await?;  // ❌ PANIC!
```

#### ❌ NEVER: Helper functions taking store_path
```rust
// ❌ WRONG
async fn helper(store_path: &str) -> Result<T> {
    let persistence = OpLogPersistence::open(store_path).await?;  // Creates duplicate transaction!
}

// ✅ CORRECT  
async fn helper(tx: &mut StewardTransactionGuard<'_>) -> Result<T> {
    let ctx = tx.session_context().await?;  // Reuse existing transaction
}
```

#### ❌ NEVER: Silent fallbacks
```rust
.unwrap_or(default)  // ❌ Masks errors
.map_err(|e| Error::from(e))?  // ✅ Fail fast
```

#### ❌ NEVER: Duplicate code
If writing a function similar to existing code → STOP and refactor with options struct.

#### ✅ ALWAYS: Debug with file redirection
```bash
# ❌ WRONG: grep/tail truncates output
cargo test 2>&1 | grep ERROR

# ✅ CORRECT: Full output to file
cargo test 1> OUT 2> OUT
# Then use grep_search tool on OUT file
```

## Response Protocol

Before ANY code suggestion involving transactions/State/errors/debugging:

1. **State which trigger word** you detected
2. **Confirm you read the doc**: "✅ Read `[doc-name]`"  
3. **State which pattern applies**: e.g., "Single transaction rule applies"
4. **Then suggest code**

## Project Context

- Read `docs/duckpond-overview.md` for architecture understanding
- Read `docs/duckpond-system-patterns.md` for detailed patterns

VIOLATION = WASTED TIME. Check triggers FIRST.
