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

The ./src/config/example folder contains the yaml files used to build the
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

