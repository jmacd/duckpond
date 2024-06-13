# Duckpond

Duckpond is a very small data lake. :-)

Duckpond is built by the [Caspar Water System](https://github.com/jmacd/caspar.water).

Duckpond writes timeseries data into multi-Parquet file databases and
assembles them for export using DuckDB.  A sibling project [Noyo Blue
Economy](https://github.com/jmacd/noyo-blue-econ) shows how to combine
this output within [Observable
Framework](https://observablehq.com/framework/) markdown.

Warning! Work-in-progress. This is really not working much at all.

![Caspar, California Duck Pond](./caspar_duckpond.jpg)

## Usage

### Init

To initialize a new pond in the current working directory or `$POND`,
if set.  The pond directory is named ".pond".  To create a new pond:

```
duckpond init
```

### Apply

To apply a resource definition, such as to create a new temporal data
set.  For example:

```
duckpond apply -f noyo.yaml
```

### Run

Fetches new data from registered resources.  For example:

```
duckpond run
```

### Check

Determines whether expected files are present in the pond and various
other consistency checks.  WIP: Simply prints the unexpected files.

```
duckpond check
```

### Export

Writes a single Parquet file per instrument definition into the
current directory for a given resource type and UUID.

```
POND=$HOME/.pond duckpond export {Resource}/{UUID}
```
