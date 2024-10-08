#!/bin/sh

duckdb :memory: <<EOF
.maxrows 5000
SELECT * FROM read_parquet('$1')
EOF
