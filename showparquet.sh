#!/bin/sh

cat - > out.parquet

duckdb :memory: <<EOF
.maxrows 5000
SELECT * FROM read_parquet('out.parquet')
EOF
