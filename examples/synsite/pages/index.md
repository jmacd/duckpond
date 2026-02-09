---
title: "Synthetic Example — DuckPond"
layout: default
---

# Synthetic Example

DuckPond End-to-End Pipeline Demo

## About This Example

This dashboard is generated entirely from **synthetic waveform data**
using the full DuckPond pipeline:

- **synthetic-timeseries** — generates sine/triangle/square waveforms
- **timeseries-join** — combines parameters per site
- **timeseries-pivot** — extracts one parameter across all sites
- **temporal-reduce** — downsamples to hourly avg/min/max
- **sitegen** — renders Markdown pages to HTML with Maud layouts
- **pond export** — writes Parquet + HTML to disk

## Data

Two fictional monitoring sites, each measuring two water quality parameters:

- **NorthDock** — Temperature (°C) and Dissolved Oxygen (mg/L)
- **SouthDock** — Temperature (°C) and Dissolved Oxygen (mg/L)

All data covers **2025-01-01 through 2025-12-31** at 1-hour resolution
(~8,760 points per series). Waveforms include daily cycles and seasonal variation.

## Navigation

Use the sidebar to explore data **by parameter** (comparing sites)
or **by site** (viewing all parameters at one location).
Each data page loads Parquet files directly in the browser via DuckDB-WASM
and renders interactive charts with Observable Plot.
