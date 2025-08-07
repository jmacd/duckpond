# DuckPond - Copilot Instructions

## Project Overview

ALWAYS review the project overview before writing a new body of code at
crates/docs/duckpond-overview.md

## Using Diagnostics in Code

REQUIRED: Before adding any logging, FIRST read ./instructions/diagnostics.md completely.
Do not guess at the API - follow the documented patterns exactly.

When adding or modifying diagnostics in this code:
1. READ ./instructions/diagnostics.md first
2. Use ONLY the patterns shown in that document
3. If unclear, ask user to clarify rather than guessing

STOP: Before writing any logging code, you must read ./instructions/diagnostics.md
Report back what you learned from that file before proceeding.

## Do Not Guess

### Mandatory Documentation Reading

1. When encountering a new API or framework, ALWAYS read the provided documentation files first before attempting any implementation
2. After reading documentation, explicitly summarize what you learned before proceeding
3. If documentation references are provided, treat them as blocking requirements - do not proceed without reading them API Pattern Verification

When unsure about API usage, ask for clarification or examples rather than trying multiple variations
If a tool call fails, read relevant documentation before retrying with different parameters
When you see mixed API usage patterns in existing code (like both log_debug! and debug!), flag this inconsistency and ask for guidance.
