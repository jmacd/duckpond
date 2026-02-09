# DuckPond Synthetic Example

A self-contained example that exercises the full DuckPond pipeline with synthetic
data, from raw waveforms through to a browsable static site.

## What It Builds

A water quality monitoring dashboard for two fictional sites, each with two
parameters:

| Site | Parameters | Waveform |
|------|-----------|----------|
| **NorthDock** | Temperature (°C), DO (mg/L) | sine + daily drift |
| **SouthDock** | Temperature (°C), DO (mg/L) | triangle + square |

Data covers **2025-01-01 to 2025-12-31** at **1 hour** resolution per source
sensor (~8,760 points per series).

## Pipeline

```
synthetic-timeseries     (4 raw series: 2 sites × 2 params)
        │
        ▼
timeseries-join          (2 joins: NorthDock, SouthDock — combines both params per site)
        │
        ▼
timeseries-pivot         (2 pivots: Temperature, DO — extracts one param across both sites)
        │
        ▼
temporal-reduce          (downsample to 1h with avg/min/max)
        │
        ▼
template                 (HTML pages: index, per-param, per-site)
        │
        ▼
pond export              (parquet + HTML to ./dist)
```

## Quick Start

### In the testsuite (containerized)

```bash
cd testsuite
./run-test.sh 200
```

### Manual (requires `pond` binary)

```bash
cd example
./build.sh
```

Then serve the output:

```bash
npx serve dist
```

Open http://localhost:3000. (Don't use `npx vite` — Vite's dev server
tries to transform inline `<script type="module">` tags and breaks
pre-built HTML with CDN imports.)

## Directory Structure

```
example/
├── README.md              ← this file
├── build.sh               ← end-to-end build + export script
├── configs/
│   ├── sensors.yaml       ← synthetic sources + join + pivot (dynamic-dir)
│   ├── reduce.yaml        ← temporal reduce (1h, avg/min/max)
│   └── template.yaml      ← template factory (HTML generation)
└── templates/
    ├── data.html.tmpl     ← per-param and per-site data page
    ├── index.html.tmpl    ← landing page
    └── page.html.tmpl     ← sidebar navigation partial
```

## What Gets Exported

```
dist/
├── index.html
├── Temperature.html
├── Temperature/
│   └── res=1h/ year=2025/ month=1..12/ *.parquet
├── DO.html
├── DO/
│   └── res=1h/ year=2025/ month=1..12/ *.parquet
├── NorthDock.html
├── NorthDock/
│   └── res=1h/ year=2025/ month=1..12/ *.parquet
├── SouthDock.html
└── SouthDock/
    └── res=1h/ year=2025/ month=1..12/ *.parquet
```
