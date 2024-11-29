# Duckpond

Duckpond is a very small data lake. :-)

Duckpond is built by the [Caspar Water System](https://github.com/jmacd/caspar.water).

Duckpond writes timeseries data into multi-Parquet file databases and
assembles them for export using DuckDB.  A sibling project [Noyo Blue
Economy](https://github.com/jmacd/noyo-blue-econ) shows how to combine
this output within [Observable
Framework](https://observablehq.com/framework/) markdown.

Warning! Work-in-progress. This works and needs testing! :-)

![Caspar, California Duck Pond](./caspar_duckpond.jpg)

## Resource categories

### HydroVu

A receiver for www.hydrovu.com environmental monitoring data.  To
instantiate one of these, for example:

```
apiVersion: www.hydrovu.com/v1
kind: HydroVu
name: noyo-harbor
desc: Noyo Harbor Blue Economy
spec:
  key: ...
  secret: ...
```

This downloads the complete set of data for all locations and
instruments.  Configure key and secret fields using values supplied by
the HydroVu system.

### Backup

An exporter for backing up all files in the Pond to a storage bucket.

```
apiVersion: github.com/jmacd/duckpond/v1
kind: Backup
name: cloudflare
desc: Backup to Cloudflare R2
spec:
  bucket: noyoharbor
  region: ...
  key: ...
  secret: ...
  endpoint: ...
```

Configure region, key, secret, and endpoint fields using values
supplied by your service provider.

### Copy

Loads from a Backup storage bucket.

```
apiVersion: github.com/jmacd/duckpond/v1
kind: Copy
name: cloudflare
desc: Backup from storage
spec:
  bucket: noyoharbor
  region: ...
  key: ...
  secret: ...
  endpoint: ...
  backup_uuid: 6fe1e0e1-b262-4d99-ae6f-3cae39e1196a
```

Configure region, key, secret, and endpoint the same as you 
would for the corresponding Backup.

### Inbox

Ingest files from a local directory on the host file system.

```
apiVersion: github.com/jmacd/duckpond/v1
kind: Inbox
name: csvin
desc: CSV inbox
spec:
  pattern: /home/user/inbox/csv/**
```

The files will be placed in a directory named `/Inbox/{UUID}`.

### Derive

Register a SQL query to transform arbitrary data (e.g., from the
Inbox) into a desired representation.  The query will be executed once
per target file matching the pattern in the Pond.

```
apiVersion: github.com/jmacd/duckpond/v1
kind: Derive
name: csvextract
desc: Extract from CSV
spec:
  collections:
  - pattern: {{ resource }}/*surface*.csv
    name: SurfaceMeasurements
    query: >
      
      WITH INPUT as ... 
	  SELECT ... 
	  FROM read_csv('$1')
	  ...
```

The placeholder `$1` is replaced by the real path of each file
matching the configured pattern, and the resulting derived files are
populated with the results of the `query` in a synthetic Pond
directory named `/Derive/{UUID}/{spec.name}`.

### Combine

Use the Combine resource to merge a set of files with different names
and identical schemas.  This is accomplished by an automatically
generated UNION statement executed by DuckDB.

```
apiVersion: www.hydrovu.com/v1
kind: Combine
name: noyo-data
desc: Noyo harbor instrument collections
spec:
  scopes:
  - name: FieldStation-Surface
    series:
    - pattern: {{ derive }}/SurfaceMeasurements/*
      attrs:
        source: laketech
        device: at500
        where: surface
```

### Template

Use the Template resource to synthesize content generated through a
template engine.  Each named collection applies the supplied pattern,
which must capture a single variable.  For each match, the captured
value becomes the basename of a file with the contents of the expanded
template.

```
apiVersion: github.com/jmacd/duckpond/v1
kind: Template
name: website
desc: observable
spec:
  collections:
  - name: details
    in_pattern: "{{ combine }}/*/combine"
    out_pattern: "$0.md"
    template: |-
      ---
      title: Detail with combined data
      ---
	  {% for field in schema.fields %}
		{{ field.name }}
	  {% endfor %}
```

The template context includes the schema of the matching file.

TODO: how to regularize the field name conventions used here?  I.e.,
not all data will use Instrument.Parameter.Unit.

### Scribble

Synthetic data generator for testing.

```
apiVersion: github.com/jmacd/duckpond/v1
kind: Scribble
name: first-scribble
desc: Test data generator
spec:
  count_min: 5
  count_max: 10
  probs:
    tree: 0.05
    table: 0.4
```

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

### List

Produces a directory listing.  Accepts shell wildcards.

```
duckpond list PATTERN
```

### Cat

Writes a file to the standard output.

```
duckpond cat PATH
```
