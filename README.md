# Duckpond

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/jmacd/duckpond/badge)](https://scorecard.dev/viewer/?uri=github.com/jmacd/duckpond)
[![SLSA 3](https://slsa.dev/images/gh-badge-level3.svg)](https://slsa.dev)

DuckPond is a file machine.<br>
DuckPond is a timeseries database.<br>
DuckPond is a site generator.<br>
DuckPond is a telemetry system.<br>

DuckPond is local-first.<br>
DuckPond is transactional.<br>
DuckPond is replicated.<br>
DuckPond is small.<br>

## What is DuckPond?

DuckPond is a file system tool to help organize process files and
file-based processes. DuckPond lets you place your CSV, Parquet, and
JSON files into application-specific paths, then pattern-match and
query them using pre-built file factories or ad-hoc SQL statements.

DuckPond is built in Rust, using Apache DataFusion embedded query
engine and DeltaLake transaction system. DuckPond reads and writes
local storage, cloud storage, git repositories, REST APIs, and more.

DuckPond's abstract file system has first-class support for tabular,
time-series, and multi-version file data. File and directory factory
instances can be registered to create dynamic, derivative file
content. DuckPond includes built-in factories for combining, joining,
and reducing timeseries.

DuckPond is useful for data collection, analysis, and monitoring in
small industrial settings.

## Who is building DuckPond?

DuckPond is built by the [Caspar Water System](https://casparwater.us)
for small water systems everywhere.

The author is [Joshua MacDonald](https://github.com/jmacd), an
open-source lead and software engineer at Microsoft, working in
telemetry systems. Joshua is a member of the OpenTelemetry technical
committee and co-founder of the OpenTelemetry-Arrow project, which is
bringing a high-performance telemetry pipeline in Rust to
OpenTelemetry.

## Who is using DuckPond?

Caspar Water uses DuckPond for telemetry, monitoring, its public
portal, and more.

The [Noyo Center for Marine Sciences](https://www.noyocenter.org/) in
Fort Bragg, CA was DuckPond's first user, where DuckPond gathers water
quality data for its public portal.

![Caspar Duck Pond](./caspar_duckpond.jpg)

## CLI Reference

See [docs/cli-reference.md](docs/cli-reference.md) for the complete
command reference.  Common commands:

```bash
pond init                           # Create a new pond
pond list '/**'                     # List all entries
pond cat /path/to/file              # Read a file
pond cat --sql "SELECT * FROM source WHERE ..." /path  # Query a table
pond copy host:///local/file /pond/path                # Import a file
pond copy host+series:///data.parquet /pond/series      # Import time-series
pond mkdir /dir                     # Create a directory
pond mknod <factory> /path --config-path config.yaml   # Install a factory
pond run /path/to/factory <command>                     # Execute a factory
pond log                            # Transaction history
```

**Host mode** (no pond required):
```bash
pond cat host+csv:///tmp/data.csv --format=table       # Query a local CSV
pond run host+remote:///config.yaml list-ponds          # Browse S3 backups
pond run host+sitegen:///site.yaml build ./dist         # Generate a site
```

## Integration Tests

Tests live in `testsuite/tests/` as numbered shell scripts.  Each test
runs in a fresh Docker container with the `pond` binary:

```bash
make test-image                     # Build the test Docker image
make integration                    # Run all tests (skips browser tests)
make integration-all                # Run all tests including browser

# Run a single test
cd testsuite && ./run-test.sh 201

# Run interactively (explore in container)
cd testsuite && ./run-test.sh --interactive
```

## License

Apache-2.0 — see [LICENSES/](LICENSES/) for details.
