# DuckPond Experiment Iteration Workflow

## ⛔ CRITICAL: NO "SIMPLE FIXES"

**NEVER propose or implement "the simplest fix" or "a quick patch."**

When you encounter a problem:
- ❌ DO NOT take shortcuts
- ❌ DO NOT apply band-aids
- ❌ DO NOT say "the simplest approach would be..."
- ❌ DO NOT hack around the issue
- ✅ DO understand the root cause fully
- ✅ DO design a proper solution that fits the architecture
- ✅ DO consider how similar problems should be handled consistently
- ✅ DO implement it correctly the first time

"Simple" fixes create technical debt, hide real problems, and waste everyone's time when they inevitably need to be redone properly. Take the time to do it right.

---

## The Loop

```
┌─────────────────────────────────────────────────────────────┐
│  1. RUN EXPERIMENT                                          │
│     - Pick highest priority item from backlog               │
│     - Execute in container                                  │
│     - Capture output                                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  2. CLASSIFY RESULT                                         │
│     ✅ Success → Add to library, move to next               │
│     ❌ Failure → Categorize:                                │
│        - BUG: Code doesn't work as intended                 │
│        - DOCS: Documentation is wrong/missing               │
│        - UX: Interface is confusing but works               │
│        - DESIGN: Fundamental approach is wrong              │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  3. PRESENT TO USER                                         │
│     - What we tried                                         │
│     - What happened                                         │
│     - Classification + rationale                            │
│     - Proposed fix (if any)                                 │
│     - Decision needed (if any)                              │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  4. USER DECISION                                           │
│     - Approve fix → Implement                               │
│     - Reject/Defer → Add to backlog with notes              │
│     - Redirect → Change priority or approach                │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  5. IMPLEMENT (if approved)                                 │
│     - Make the change (code/docs/interface)                 │
│     - Re-run experiment to verify                           │
│     - Update backlog                                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      └──────────────► REPEAT
```

## Priority Levels

| Priority | Meaning | Examples |
|----------|---------|----------|
| P0 | Blocking | Can't proceed with testing, crashes |
| P1 | High | Common operation fails, very confusing |
| P2 | Medium | Works but confusing, needs docs |
| P3 | Low | Nice-to-have improvements |

## Session Format

Each iteration round:

### 🔍 Current Focus
What we're investigating this round.

### 📊 Result
What happened when we ran it.

### 🏷️ Classification
- **Type**: BUG / DOCS / UX / DESIGN
- **Priority**: P0-P3
- **Rationale**: Why this classification

### 💡 Proposal
Suggested fix or action.

### ❓ Decision Needed
- [ ] Approve proposal
- [ ] Defer (add to backlog)
- [ ] Alternative approach: ___

### 📝 Notes
Any additional context or related issues discovered.

---

## Starting the Loop

To begin an iteration session:

```
Let's iterate. Show me the current backlog and pick the highest priority item.
```

To continue after a decision:

```
Approved. Implement and continue.
```
or
```
Defer. Move to next item.
```
or
```
Let's discuss: [your thoughts]
```
