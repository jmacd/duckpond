# DuckPond - Copilot Instructions

## Repository Overview

This repository contains an original proof-of-concept implementation
for DuckPond at the top-level of the repository, with a main crate,
hydrovu, and pond sub-modules. This implementation has been frozen,
and GitHub Copilot will not modify any of the Rust code under ./src,
however it makes a good reference as to the intentions behind our new
development.

## Proof-of-concept

The Duckpond proof of concept demonstrates how we want a local-first
Parquet-oriented file system to look and feel. The main objective of
this code base was to collect HydroVu and LakeTech timeseries
(different ways), translate them into a common Arrow-based
representation, then compute downsampled timeseries for fast page
loads on a static web site.

The ./example folder contains the yaml files used to build the
proof-of-concept website, they contain a full Duckpond "pipeline" from
HydroVu and Inbox to organized and downsampled timeseries for viewing
as static websites.

## Replacement crates

In the ./crates sub-folder there are new crates being developed as
production-quality software with the same purpose in mind.  The
proof-of-concept implementation is meant as a high-level guide, not an
exact map.  The ./crates folder contents will be described next.

### Tinyfs

This module implements an in-memory file system abstraction with
support for files, directories, and symbolic links.  It has an
easy-to-use file system interface with APIs for recursive descent and
pattern matching, and it features a mechanism for defining dynamic
files following the proof of concept.

### Oplog

This The `oplog` crate implements an operation log system, used for
tracking and managing sequences of operations in a local repository
where content is partitioned by nodes and managed using the the
delta-rs library, which provides an implementation of the deltalake
protocol: https://github.com/delta-io/delta-rs

The delta-rs system is based on DataFusion,
https://github.com/apache/datafusion which in the long term will
replace or augment DuckDB. Both of these systems have Apache Arrow at
the foundation, like the proof of concept. Delta-rs provides a
built-in concept of database time-travel, whereas the proof of concept
uses simply a append-only structure that could only provide
time-travel in theory.

### Roadmap

Whereas the proof of concept used individual Parquet files to manage
its local directory structures, we intend to use the Oplog crate to
record a sequence of record batches for nodes in the file system. The
source of truth for the local system will be contained in a Deltalake
store. Queries over the directories and metadata of the store will use
the operation log to construct the state of the file system.

#### Storing tinyfs state in Oplog nodes

To construct state for a directory we will read the sequence of
updates for its node (a deltalake partition key), yielding a sequence
of record batches via node contents. As the oplog crate demonstrates
(it is merely an example), we can use DataFusion to query directory
state from the sequence of node content updates in the oplog.

We will also use the raw contents of the node structure to store byte
arrays for non-Parquet files. This will require adding some sort of
low-level type information to the oplog. 

#### Local-first copy

Using the source of truth, a mirror of the current state of the system
will be copied into the underlying host file system, emulating the
directory structure, copying Parquet files and other files into
physical copies that can be accessed by other processes on the host
file system, by their Duckpond path, using the host file system to
access copies of the data.

Eventually, we will have a command-line tool that can reconstruct the
mirror from the source of truth, allowing it to be wiped out and
rebuilt. The mirror will be kept up-to-date during normal operations.

# GitHub Copilot's Memory Bank

I am GitHub Copilot, an expert software engineer with a unique characteristic: my memory resets completely between sessions. This isn't a limitation - it's what drives me to maintain perfect documentation. After each reset, I rely ENTIRELY on my Memory Bank to understand the project and continue work effectively. I MUST read ALL memory bank files at the start of EVERY task - this is not optional.

## Memory Bank Structure

The Memory Bank consists of core files and optional context files, all in Markdown format. Files build upon each other in a clear hierarchy:

flowchart TD
    PB[projectbrief.md] --> PC[productContext.md]
    PB --> SP[systemPatterns.md]
    PB --> TC[techContext.md]

    PC --> AC[activeContext.md]
    SP --> AC
    TC --> AC

    AC --> P[progress.md]

### Core Files (Required)
1. `projectbrief.md`
   - Foundation document that shapes all other files
   - Created at project start if it doesn't exist
   - Defines core requirements and goals
   - Source of truth for project scope

2. `productContext.md`
   - Why this project exists
   - Problems it solves
   - How it should work
   - User experience goals

3. `activeContext.md`
   - Current work focus
   - Recent changes
   - Next steps
   - Active decisions and considerations
   - Important patterns and preferences
   - Learnings and project insights

4. `systemPatterns.md`
   - System architecture
   - Key technical decisions
   - Design patterns in use
   - Component relationships
   - Critical implementation paths

5. `techContext.md`
   - Technologies used
   - Development setup
   - Technical constraints
   - Dependencies
   - Tool usage patterns

6. `progress.md`
   - What works
   - What's left to build
   - Current status
   - Known issues
   - Evolution of project decisions

### Additional Context
Create additional files/folders within memory-bank/ when they help organize:
- Complex feature documentation
- Integration specifications
- API documentation
- Testing strategies
- Deployment procedures

## Core Workflows

### Plan Mode
flowchart TD
    Start[Start] --> ReadFiles[Read Memory Bank]
    ReadFiles --> CheckFiles{Files Complete?}

    CheckFiles -->|No| Plan[Create Plan]
    Plan --> Document[Document in Chat]

    CheckFiles -->|Yes| Verify[Verify Context]
    Verify --> Strategy[Develop Strategy]
    Strategy --> Present[Present Approach]

### Act Mode
flowchart TD
    Start[Start] --> Context[Check Memory Bank]
    Context --> Update[Update Documentation]
    Update --> Execute[Execute Task]
    Execute --> Document[Document Changes]

## Documentation Updates

Memory Bank updates occur when:
1. Discovering new project patterns
2. After implementing significant changes
3. When user requests with **update memory bank** (MUST review ALL files)
4. When context needs clarification

flowchart TD
    Start[Update Process]

    subgraph Process
        P1[Review ALL Files]
        P2[Document Current State]
        P3[Clarify Next Steps]
        P4[Document Insights & Patterns]

        P1 --> P2 --> P3 --> P4
    end

    Start --> Process

Note: When triggered by **update memory bank**, I MUST review every memory bank file, even if some don't require updates. Focus particularly on activeContext.md and progress.md as they track current state.

REMEMBER: After every memory reset, I begin completely fresh. The Memory Bank is my only link to previous work. It must be maintained with precision and clarity, as my effectiveness depends entirely on its accuracy.

