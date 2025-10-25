# Anti-Duplication Instructions for GitHub Copilot

## CRITICAL: Never Write Near-Duplicate Functions

**STOP** before writing any function that resembles an existing one. Code duplication is one of the worst programming sins and creates massive technical debt.

## Red Flags That Should Trigger This Warning

### 🚨 IMMEDIATE STOP SIGNALS 🚨

1. **Function names with suffixes like:**
   - `_with_tracking`
   - `_with_options` 
   - `_extended`
   - `_v2`
   - `_async` (when sync version exists)

2. **Copy-pasting a function and making "small changes"**

3. **Writing functions that are 80%+ identical**

4. **Adding boolean parameters to control behavior variants**

5. **Multiple functions doing "almost the same thing"**

## The Right Way: Options Pattern

### ❌ WRONG - Duplication
```rust
pub async fn collect_data() -> Result<()>
pub async fn collect_data_with_tracking() -> Result<HashMap<i64, i64>>
pub async fn collect_data_with_options() -> Result<CollectionResult>
```

### ✅ RIGHT - Single Configurable Function
```rust
#[derive(Default)]
pub struct CollectionOptions {
    pub track_timestamps: bool,
    pub max_items: Option<usize>,
}

pub async fn collect_data(options: CollectionOptions) -> Result<CollectionResult>

// Convenience wrappers (thin, no logic duplication)
pub async fn collect_data_simple() -> Result<()> {
    collect_data(CollectionOptions::default()).await?;
    Ok(())
}
```

## Design Principles

### 1. **Configuration Over Duplication**
- Use options structs with `#[derive(Default)]`
- One core function that handles all variations
- Thin convenience wrappers for common cases

### 2. **Composition Over Inheritance**
- Break functionality into small, composable pieces
- Use generics and traits for flexibility
- Prefer builder patterns for complex configurations

### 3. **DRY (Don't Repeat Yourself)**
- If you're copying code, you're doing it wrong
- Extract common logic into shared functions
- Use macros only as last resort for code generation

## Implementation Strategy

### Step 1: Identify the Core Operation
What is the fundamental thing this function does?

### Step 2: Identify the Variations
What are the different ways this operation can be configured?

### Step 3: Create Options Struct
```rust
#[derive(Default, Clone)]
pub struct OperationOptions {
    pub variation_1: bool,
    pub variation_2: Option<String>,
    pub variation_3: SomeEnum,
}
```

### Step 4: Single Implementation
One function that handles all variations using the options.

### Step 5: Convenience Wrappers
Thin wrappers for common usage patterns (if needed for API compatibility).

## Examples of Good Patterns

### File Operations
```rust
#[derive(Default)]
pub struct WriteOptions {
    pub create_dirs: bool,
    pub overwrite: bool,
    pub backup: bool,
}

pub fn write_file(path: &Path, content: &[u8], options: WriteOptions) -> Result<()>
```

### HTTP Requests  
```rust
#[derive(Default)]
pub struct RequestOptions {
    pub timeout: Option<Duration>,
    pub retry_count: u32,
    pub headers: HashMap<String, String>,
}

pub async fn make_request(url: &str, options: RequestOptions) -> Result<Response>
```

### Data Processing
```rust
#[derive(Default)]  
pub struct ProcessingOptions {
    pub parallel: bool,
    pub batch_size: Option<usize>,
    pub progress_callback: Option<Box<dyn Fn(f64)>>,
}

pub fn process_data<T>(data: &[T], options: ProcessingOptions) -> Result<Vec<T>>
```

## When Duplication Might Be Acceptable

### Truly Different Algorithms
If functions use fundamentally different algorithms (not just configuration), separate functions may be appropriate.

### Different Error Types
If functions have genuinely different error handling requirements.

### Performance Critical Paths
If the options pattern introduces unacceptable overhead (rare).

## Production API Changes That Affect Tests

### The Problem: Breaking 100+ Test Sites

Sometimes production code needs stricter guarantees that tests don't care about.

**Example**: `create()` now requires `(txn_id, cli_args)` for audit trails.
- **Production call sites**: ~5 locations that SHOULD pass real metadata
- **Test call sites**: 30+ locations that DON'T CARE about metadata

### ❌ WRONG APPROACH: Update All Test Sites
```rust
// Now you have to update 30+ test files with boilerplate:
let mut persistence = OpLogPersistence::create(
    &store_path,
    uuid7::uuid7().to_string(),  // ← Noise in every test
    vec!["test".to_string()],     // ← Nobody cares about this
).await?;
```

**Problems**:
- Massive diff touching unrelated files
- Adds cognitive load to every test
- Creates merge conflicts
- Obscures the actual test logic

### ✅ RIGHT APPROACH: Test Helper with `#[cfg(test)]`

```rust
// In production code (persistence.rs):
impl OpLogPersistence {
    /// Production API: requires real metadata for audit trails
    pub async fn create(
        path: &str,
        txn_id: String,
        cli_args: Vec<String>,
    ) -> Result<Self, TLogFSError> {
        Self::open_or_create(path, true, Some((txn_id, cli_args))).await
    }

    /// Test helper: supplies synthetic metadata automatically
    #[cfg(test)]
    pub async fn create_test(path: &str) -> Result<Self, TLogFSError> {
        Self::create(
            path,
            uuid7::uuid7().to_string(),
            vec!["test".to_string(), "create".to_string()],
        )
        .await
    }
}
```

**Now tests stay clean**:
```rust
// Before: noisy boilerplate
let mut persistence = OpLogPersistence::create(
    &store_path,
    uuid7::uuid7().to_string(),
    vec!["test".to_string()],
).await?;

// After: clear intent
let mut persistence = OpLogPersistence::create_test(&store_path).await?;
```

### When to Use This Pattern

Use a `#[cfg(test)]` helper when:

1. **Production API needs stricter parameters** (metadata, validation, etc.)
2. **Tests don't care about those parameters** (any synthetic value works)
3. **You have 10+ test call sites** vs. <10 production call sites
4. **The helper just supplies defaults** (no logic, just parameter filling)

### Examples in DuckPond

- **`commit_test()`**: Production needs metadata, tests use txn_seq=2 default
- **`create_test()`**: Production needs audit metadata, tests use synthetic
- **`commit_test_with_sequence(seq)`**: Rare case needing explicit sequence

### Decision Tree

```
Production API needs new required parameter?
├─ Does it affect production behavior? YES → Keep production strict
│
├─ Do tests care about specific value? NO
│  ├─ 10+ test call sites? YES → Create #[cfg(test)] helper
│  └─ < 10 test call sites? NO → Update manually (not worth helper)
│
└─ Do tests care about specific value? YES → Tests should provide it
```

### Anti-Pattern: Overusing Test Helpers

**Don't** create test helpers for:
- Values tests SHOULD specify (like expected data)
- Complex setup that varies per test
- Logic that should be in test utilities (not production code)

**Do** create test helpers for:
- Boilerplate parameters tests don't care about
- Default configurations for common test scenarios
- Reducing noise that obscures test intent

## Testing Strategy

Test the core function with different option combinations, not separate functions.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_with_default_options() {
        let result = operation(OperationOptions::default());
        // assertions
    }

    #[test]  
    fn test_operation_with_custom_options() {
        let result = operation(OperationOptions {
            variation_1: true,
            variation_2: Some("test".into()),
            ..Default::default()
        });
        // assertions
    }
}
```

## Code Review Questions

Before merging any code, ask:

1. **Is this function similar to an existing one?**
2. **Could these functions be combined with options?**  
3. **Am I repeating logic that could be shared?**
4. **Would a future developer understand why these are separate?**

## Anti-Patterns to Avoid

### Boolean Parameter Hell
```rust
❌ fn process(data: &[u8], sort: bool, dedupe: bool, validate: bool)
✅ fn process(data: &[u8], options: ProcessOptions) 
```

### Version Suffixes
```rust
❌ fn calculate_v1() -> f64
❌ fn calculate_v2() -> f64  
✅ fn calculate(method: CalculationMethod) -> f64
```

### Async/Sync Variants
```rust
❌ fn read_file(path: &Path) -> Result<String>
❌ async fn read_file_async(path: &Path) -> Result<String>
✅ Use async throughout, provide blocking wrapper if needed
```

## Summary

**The Golden Rule**: If you're about to write a function that looks like another function, STOP and refactor into a configurable solution.

**Remember**: Code is read more than it's written. Prioritize clarity and maintainability over initial convenience.

**The Test**: If you can't explain why two functions need to exist separately in one sentence, they should be one function.
