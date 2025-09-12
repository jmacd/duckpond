# Quick Start Instructions for AI Assistant

## 🎯 **Give Me This Context At Session Start**

```markdown
**ARCHITECTURE CONTEXT:**
- **Component**: [CLI/Steward/TLogFS/TinyFS] 
- **Dependencies**: [From component-relationships.md matrix]
- **Transaction**: [State threading pattern needed]
- **Objective**: [Eliminate X duplication / Fix Y / Add Z]
- **Constraints**: [Key architectural rules for this component]

**CURRENT SITUATION:**  
- **Problem**: [Specific issue description]
- **Files Involved**: [List of files you expect to change]
- **Expected Outcome**: [What success looks like]

**CRITICAL REMINDERS:**
- Zero tolerance for duplication - eliminate, don't add
- Check dependency matrix before any imports
- Thread State parameter down, don't recreate
- Use DataFusion/Arrow instead of custom implementations
```

## 📚 **Essential Reading Order**
1. **First**: `/crates/docs/session-startup-instructions.md` - The checklist
2. **Then**: `/crates/docs/component-relationships.md` - The architecture map  
3. **When stuck**: `/crates/docs/troubleshooting-guide.md` - The debugging guide
4. **For context**: `/crates/docs/complexity-management-guide.md` - The deep analysis

## 🚫 **Immediate Stop Signals**
If I start to do ANY of these, interrupt me immediately:

- **Adding imports** from upper layers to lower layers
- **Creating new trait methods** when downcasting exists
- **Writing custom implementations** of DataFusion/Arrow functionality  
- **Recreating State** instead of threading parameters
- **Adding fallback logic** instead of fixing root cause

## ⚡ **Quick Commands for You**

### **Check Architecture**
```bash
# Verify no circular dependencies 
cargo check --package [COMPONENT]

# Check for forbidden imports
grep -r "use steward" crates/tlogfs/    # Should be empty
grep -r "use tlogfs" crates/tinyfs/     # Should be empty
```

### **Find Duplication**
```bash
# Look for similar implementations
grep -r "struct.*Table" crates/tlogfs/
grep -r "impl.*File for" crates/
grep -r "as_any" crates/ | head -10
```

### **Trace Transaction Context**
```bash
# Find State threading
grep -r "\.state()" crates/
grep -r "persistence::State" crates/
```

## 🎪 **Session Flow Template**

### **1. Context Establishment** (2 minutes)
- Read the architecture context you provide
- Identify component responsibilities and constraints  
- Confirm transaction threading requirements
- Verify duplication elimination objectives

### **2. Pre-Change Analysis** (3 minutes)
- Check dependency matrix for any new imports needed
- Scan for existing implementations before writing new code
- Identify State/Guard threading path through layers
- Assess impact on other components

### **3. Implementation** (focused coding)
- Make minimal changes that eliminate duplication
- Thread parameters down, never import up
- Use external libraries over custom implementations
- Follow established patterns for downcasting/access

### **4. Verification** (2 minutes)
- Compile each component individually to catch cycles early
- Verify State threading works end-to-end
- Confirm duplication was actually eliminated
- Check that architectural boundaries were preserved

## 💬 **Communication Shortcuts**

### **When I Need Guidance:**
```
"ARCHITECTURE QUESTION: Should [COMPONENT A] depend on [COMPONENT B] for [PURPOSE], 
or should I thread [PARAMETER] down from [UPPER LAYER]?"
```

### **When I'm Stuck:**
```  
"STUCK ON: [SPECIFIC TECHNICAL ISSUE]
TRIED: [APPROACHES ATTEMPTED]
CONSTRAINT: [ARCHITECTURAL RULE BLOCKING ME]
NEED: [SPECIFIC GUIDANCE REQUEST]"
```

### **Before Major Changes:**
```
"IMPACT CHECK: This touches [COMPONENTS], modifies [DEPENDENCIES], 
threads [TRANSACTION CONTEXT], eliminates [DUPLICATION]. 
Risk assessment: [CONCERNS]. Proceed?"
```

## 🔄 **Pattern Library Quick Reference**

### **Transaction Threading**
```rust
// ✅ Extract State from guard, pass down
let state = steward_guard.state()?;
tlogfs_operation(state).await?;

// ❌ Don't pass guard across boundaries  
tlogfs_operation(&steward_guard).await?;
```

### **File Downcasting**
```rust
// ✅ Use existing infrastructure
let file_arc = file_handle.handle.get_file().await;
let file_guard = file_arc.lock().await;  
let oplog_file = file_guard.as_any().downcast_ref::<OpLogFile>()?;

// ❌ Don't add new trait methods
trait File { fn extract_ids(&self) -> (NodeID, NodeID); }
```

### **DataFusion Integration**  
```rust
// ✅ Use proven DataFusion components
let listing_table = ListingTableConfig::new(table_url)
    .with_listing_options(listing_options)
    .infer_schema(&ctx.state()).await?;

// ❌ Don't create custom table implementations
struct CustomFileTable { ... }
```

## 📋 **End-of-Session Checklist**

- [ ] **Compilation**: All packages compile without errors
- [ ] **Dependencies**: No new cycles in dependency graph
- [ ] **Duplication**: More code eliminated than added
- [ ] **Architecture**: Component boundaries respected
- [ ] **Documentation**: Decisions recorded for future reference

## 🆘 **Emergency Reset Procedure**

If we get confused or make mistakes:

1. **STOP all coding immediately**
2. **State the current component and objective clearly**  
3. **Review the component responsibilities from relationships.md**
4. **Ask explicit architectural questions before proceeding**
5. **Start with the smallest possible change that eliminates duplication**

---

**Remember: Always start sessions by giving me the architectural context template above. This prevents 90% of the confusion and mistakes we've been making.**
