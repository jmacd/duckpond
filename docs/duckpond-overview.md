## Repository Overview

DuckPond is an application-level file system for storage and archival
of multi-variate timeseries, intended for small telemetry systems.

## File System Architecture

As an application-level file system, DuckPond is designed around
hierarchical directories and files, containing heterogeneous types of
file. Several features distinguish the DuckPond file system:

- File system transactions.
- Built-in timeseries file type.
- Dynamic file/directories support derived content.

Like a traditional file system, DuckPond provides a namespace used to
facilitate pattern matching. Built-in pattern-matching libraries
support traversal and capturing of wildcard variables

## Application-specific layout

An application file system is designed with a specific purpose:

- Create empty directories using `pond mkdir`
- Copy static files using `pond copy`
- Create dynamic and executable files using `pond mknod`
- Execute runnable files to incorporate new data.
- Use path expressions to read and export structured data.

As an example, create an empty `/etc` directory, then define
an executable configuration.

```
pond mkdir /etc
pond mknod hydrovu /etc/hydrovu --config-path hydrovu.yaml
```

This creates a folder for configuration and configures a "hydrovu"
factory named `/etc/hydrovu`. To run this configuration:

```
pond run /etc/hydrovu [COMMAND [ARGS]]
```

where a command (e.g., "collect") invokes factory-specific logic, in
this case to collect new timeseries data. The factory in this example
appends to a series per device, and a `pond cat` command displays
the data set:

```
pond cat --format=table /hydrovu/devices/4990594334457856/Name.series
```

the `pond list` command expands wildcards, for example to list all
devices:

```
pond list /hydrovu/**/*.series
```

## Built-in Factories

Each factory type is configured with a specific YAML schema. There are
a number of built-in factories:

- `dynamic-dir`: Meta-factory for configuring a directory of 
  factories with nested configuration.
- `timeseries-join`: Combine multiple timeseries having potentially
  different schemas into a single, multivariate series.
- `timeseries-pivot`: Split a set of columns into a new multivariate
  timeseries.
- `temporal-reduce`: Downsample timeseries into coarser temporal 
  resolutions.
- `template`: Render django-style templates using shell-wildcard
  captured strings.
- `remote`: Commands for replicating pond data to remote storage.

A complete example using all the factories can be found in
`${REPO_ROOT}/noyo`.

## Project Crates

### Tinyfs: an abstract file system with dynamic files

Tinyfs implements a strongly-typed Rust file system interface with
pattern-matching and navigational primitives. File system nodes are
identified by a two-part identifier `FileID` which consists of a
partition ID and a node ID: the partition corresponds with a physical
directory and physical directories have their partition ID equal to
their node ID.

To implement a `tinyfs` file system, an implementor provides the
`Persistence` trait. A built-in memory file system is provided mainly
for testing.

### Provider: for deriving file contents

A number of data providers are implemented here, where they depend on
TinyFS and DataFusion, properties of files but not the file system
itself. Examples include schema modification (e.g., modifying column
names) and file compression, generally utilities for manipulating data
that are not directly tied to the real file system `TLogFS`.

Provider context mainly carries a reference to the TinyFS and the
DataFusion session context.

As a central interface, the QueryableFile trait extends tinyfs with
support for datafusion table providers backed by real and dynamic
files.

### TLogFS: a DeltaLake implementation of TinyFS 

TLogFS is a TinyFS implementation based on the DeltaLake library,
which is itself based on DataFusion. DataFusion is used to access
nodes and directory content for physical files and dynamic files with
physical parents.

TLogFS is meant to be used as a single-threaded, single-application
file system.  It does not support concurrent transactions, however
transactions are atomic.

Transactions are guarded: only one transaction may borrow the TLogFS
instance at a time.

Large files are stored outside Deltalake and referred to by sha256.

### Steward: a transaction coordinator for TLogFS

Steward simply protects access to the underlying TLogFS.

The steward library coordinates transactions in TLogFS. It's main
purpose is to ensure that metadata is recorded about each transaction
and that post-commit reader transactions are run in sequence following
commit.

The "remote" factory is used for replicating content to remote storage

### Pond: the command-line utility

The "pond" command lets the user access the TLogFS instance, run
factory commands, as well as a number of other diagnostic utilities.
