#!/bin/sh

duckdb :memory: "SELECT * FROM read_parquet('$1')"
