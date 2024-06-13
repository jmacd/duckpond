#!/bin/sh

duckdb :memory: <<EOF
.maxrows 2000
SELECT * FROM read_parquet('$1')
EOF
