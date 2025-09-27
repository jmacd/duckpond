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
