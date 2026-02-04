# Session Startup Instructions for AI Assistant

## 🚀 **MANDATORY Pre-Work Checklist** 
*Read this BEFORE making any code changes*

### **Architecture Review** (2 minutes)
- [ ] **Component Identity**: Which component am I working in? (CLI/Steward/TLogFS/TinyFS)
- [ ] **Dependency Check**: Am I violating the dependency rules matrix?
- [ ] **Transaction Context**: What State/Guard threading is required?
- [ ] **Purpose Alignment**: Does this change align with the component's single responsibility?

### **Duplication Elimination Check** (3 minutes)
- [ ] **Existing Solutions**: Does DataFusion/Arrow already solve this problem?
- [ ] **Code Removal**: Am I eliminating more code than I'm adding?
- [ ] **API Reuse**: Can I use existing downcasting/threading patterns?
- [ ] **Zero Tolerance**: Am I maintaining zero tolerance for duplication?

### **Impact Analysis** (2 minutes)  
- [ ] **Compilation**: Which packages will need recompilation?
- [ ] **Transaction Threading**: How does State flow through my changes?
- [ ] **Error Propagation**: Am I following fail-fast philosophy?

## 🎯 **Context Declaration Template**
*Start every session with this*

```
WORKING IN: [CLI/Steward/TLogFS/TinyFS]
DEPENDENCIES: [List allowed dependencies from matrix]
TRANSACTION CONTEXT: [State/Guard threading pattern needed]
OBJECTIVE: [Eliminate X duplication / Fix Y bug / Add Z feature]
ARCHITECTURAL CONSTRAINTS: [Key rules from component responsibilities]
```

## 🚫 **Forbidden Patterns Checklist**
*If you catch yourself doing these, STOP immediately*

### **Circular Dependencies**
- [ ] TLogFS importing Steward types
- [ ] TinyFS importing business logic  
- [ ] Lower layers depending on upper layers

### **API Proliferation** 
- [ ] Adding new trait methods when downcasting works
- [ ] Creating new interfaces instead of using existing ones
- [ ] Custom implementations when external libraries suffice

### **Transaction Anti-Patterns**
- [ ] Recreating State instead of threading it
- [ ] Passing Guards across component boundaries
- [ ] Missing transaction context in operations

### **Duplication Violations**
- [ ] Custom file handling instead of DataFusion
- [ ] Fallback logic instead of architectural fixes
- [ ] Multiple implementations of the same concept

## 🔧 **Emergency Recovery Procedures**

### **"I'm Lost" Protocol** (5 minutes)
1. **STOP coding immediately**
2. **Open**: `/crates/docs/component-relationships.md`
3. **Identify**: Which component am I in? What are its rules?
4. **Ask**: "Should component X depend on component Y?"
5. **Reset**: Start with architecture, then implement

### **Compilation Failure Protocol**
1. **Check imports**: Are they violating dependency matrix?
2. **Check parameters**: Am I threading State correctly?
3. **Check traits**: Did I break an interface contract?
4. **Check circular deps**: Use `cargo check --package X` to isolate

### **"This Seems Too Complex" Protocol**  
1. **Question the approach**: Am I adding or eliminating?
2. **Check for duplication**: What existing code does this?
3. **Simplify ruthlessly**: What's the minimal change?
4. **Ask for help**: State the architectural dilemma clearly

## 📋 **Common Task Checklists**

### **Adding New Functionality**
- [ ] Does this functionality already exist elsewhere?
- [ ] Which component should own this responsibility?  
- [ ] How do I thread transaction context through?
- [ ] What external libraries can I leverage?
- [ ] What code can I eliminate as a side effect?

### **Fixing Bugs**
- [ ] What's the root architectural cause?
- [ ] Is this a symptom of a larger design problem?
- [ ] Can I fix by eliminating code instead of adding?
- [ ] Does this reveal other similar patterns to fix?

### **Refactoring Code**
- [ ] What duplication am I eliminating?
- [ ] Am I improving or degrading component boundaries?
- [ ] How does transaction threading change?
- [ ] What dependencies am I removing?

## 🗣️ **Communication Protocols**

### **When Asking for Help**
```
I'm working in [COMPONENT] trying to [OBJECTIVE]. 

The architectural challenge is: [SPECIFIC BOUNDARY/DEPENDENCY ISSUE]

I'm confused about: [SPECIFIC RELATIONSHIP/INTERFACE QUESTION]

I've considered: [ALTERNATIVES EVALUATED]

The tradeoffs seem to be: [SPECIFIC CONCERNS]
```

### **Before Major Changes**
```
This change will:
- Touch components: [LIST]  
- Modify dependencies: [CHANGES TO DEPENDENCY MATRIX]
- Thread transaction context: [HOW STATE FLOWS]
- Eliminate duplication: [WHAT CODE GETS REMOVED]
- Risk assessment: [WHAT COULD GO WRONG]
```

### **When Stuck on Implementation**
```
I need to [SPECIFIC OPERATION] but I'm stuck on:
- Which layer should handle this: [COMPONENT RESPONSIBILITY QUESTION]  
- How to access [DATA/STATE]: [INTERFACE/THREADING QUESTION]
- Whether to use [APPROACH A] vs [APPROACH B]: [TRADEOFF ANALYSIS]
```

## 🎓 **Learning Accelerators**

### **Before Each Session**
1. **Review last session's changes**: What patterns did we establish?
2. **Check component docs**: Re-read the responsibilities
3. **Scan recent commits**: What architectural decisions were made?

### **During Each Session** 
1. **Think in layers**: Data flows down, never up
2. **Default to elimination**: Remove more than you add
3. **Leverage external libraries**: Don't reinvent DataFusion/Arrow
4. **Thread context explicitly**: State flows through parameters

### **After Each Session**
1. **Document decisions**: Why did we choose this approach?  
2. **Update architectural notes**: What did we learn about relationships?
3. **Identify patterns**: What can we reuse next time?

## ⚡ **Quick Reference**

### **Dependency Matrix**
- CLI → Steward ✅
- Steward → TLogFS ✅  
- TLogFS → TinyFS ✅
- Everything else → External libraries ✅
- **All other combinations** ❌

### **Transaction Threading**
```
StewardTransactionGuard.state() → persistence::State → operations
```

### **File Access Chain**
```
Path → WD.resolve_path() → Pathed<FileHandle> → .handle.get_file() → downcast → OpLogFile
```

### **Zero Tolerance Duplication**
- Use DataFusion ListingTable, not custom tables
- Use Arrow schemas, not custom formats  
- Use existing downcasting, not new trait methods
- Eliminate code, don't add parallel implementations

---
*Keep this file open during all coding sessions. Refer to it when making any architectural decisions.*
