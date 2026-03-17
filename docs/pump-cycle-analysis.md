# Pump Cycle Analysis & Horner Plot Toolkit

Interactive visualization of well pump cycles for aquifer health monitoring.
Designed for water system operators to detect leaks, track seasonal recharge,
and evaluate aquifer transmissivity from routine pump operation data.

Live at: `https://casparwater.us/analysis/pump-cycles`

## Architecture

```
Raw telemetry (30s samples)
  |
  v
sql-derived-series factories (DataFusion SQL)
  |-- pump-cycles: per-sample aligned data (elapsed time, depth, phase)
  |-- cycle-summary: per-cycle aggregates (duration, drawdown, duty cycle)
  |
  v
Parquet export (pond cat > file)
  |
  v
Static HTML page (sitegen) + overlay.js
  |-- DuckDB-WASM: loads parquet, runs SQL filters client-side
  |-- D3: overview chart with zoom/brush interaction
  |-- Observable Plot: 7 analysis charts, re-rendered on brush
```

### Data flow

1. **Ingestion**: OtelJSON telemetry from the well sensor → pond
2. **Reduction**: `temporal-reduce` factory produces 1-minute averages
3. **Analysis**: `sql-derived-series` factories detect pump cycles via
   42m depth threshold and compute aligned datasets
4. **Export**: `pond cat file:///analysis/pump-cycles > all.parquet`
5. **Visualization**: Browser loads parquet via DuckDB-WASM, renders
   interactive charts with D3 brush for time-range filtering

## Pump Cycle Detection

A pump cycle is detected when well depth crosses below 42 meters:

- **Draw phase**: depth < 42m (pump is on, water level dropping)
- **Recovery phase**: depth >= 42m (pump is off, water level rising)
- Each crossing below 42m starts a new pump event

The 42m threshold cleanly separates pump-on from pump-off. Static water
level ranges from ~42m (late summer) to ~45m (winter peak recharge).

### Static water level

The true pre-pump static level is computed as a lookback average:

```sql
AVG(depth) OVER (ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 10 PRECEDING)
```

This captures the water table 10-20 readings before the pump starts,
giving the undisturbed level before drawdown begins.

## Data Quality Filters

Five filters remove bad cycles caused by power outages, sensor glitches,
and operator manual pump runs:

| Filter | Condition | What it catches |
|--------|-----------|----------------|
| **Data gap** | Gap before pump start < 5 min | Power outage: pump started before computer rebooted |
| **Static depth range** | static_depth between 40-50m | Corrupted lookback average spanning a data gap |
| **Minimum draw duration** | >= 20 min of draw phase | Operator hand-pumps for chlorine tank pressure |
| **First reading check** | First depth reading >= 40m | Mid-cycle starts where pump was already running |
| **Minimum total drawdown** | static - min_depth >= 1.5m | Threshold grazers where water barely crossed 42m |
| **Stale sensor** | Depth drops >= 0.3m in first 5 min | Frozen sensor reporting identical readings |

Starting from ~4,100 raw cycles, filters produce ~3,400 clean cycles.

### Filter implementation

Filters are implemented as CTE subqueries joined to the main result:

```sql
-- Minimum draw duration (30s data = 40 samples for 20 min)
long_enough AS (
  SELECT pump_event_id FROM with_meta
  WHERE is_draw = true
  GROUP BY pump_event_id
  HAVING COUNT(*) >= 40
),
-- First reading must be near threshold, not mid-drawdown
clean_start AS (
  SELECT pump_event_id FROM with_meta
  WHERE EXTRACT(EPOCH FROM (timestamp - event_start)) < 60
  GROUP BY pump_event_id
  HAVING MIN(depth) >= 40.0
),
-- Must have real drawdown, not just threshold grazing
real_drawdown AS (
  SELECT pump_event_id FROM with_meta
  GROUP BY pump_event_id
  HAVING MAX(static_depth) - MIN(depth) >= 1.5
),
-- Sensor must show movement (not frozen/stale readings)
not_stale AS (
  SELECT pump_event_id FROM with_meta
  GROUP BY pump_event_id
  HAVING MAX(CASE WHEN ... < 30 THEN depth END)
       - MIN(CASE WHEN ... BETWEEN 270 AND 330 THEN depth END) >= 0.3
)
-- All filters applied via INNER JOIN
FROM with_meta m
INNER JOIN long_enough le ON m.pump_event_id = le.pump_event_id
INNER JOIN clean_start cs ON m.pump_event_id = cs.pump_event_id
INNER JOIN real_drawdown rd ON m.pump_event_id = rd.pump_event_id
INNER JOIN not_stale ns ON m.pump_event_id = ns.pump_event_id
```

## Duty Cycle

Duty cycle = `draw_duration / inter_pump_interval`

Where `inter_pump_interval` is the time from this pump start to the next
pump start, computed via:

```sql
LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp
```

This definition captures the true fraction of time the pump spends running.
As leaks worsen, the tank drains faster, shortening the interval between
pump cycles:

- **Normal**: duty cycle ~15-20%, pump runs every 8-10 hours
- **Leak**: duty cycle rises toward 50%+, pump runs every 2-3 hours
- **Severe leak**: duty cycle near 100%, pump runs almost continuously

Cycles per day ~ `duty_cycle / draw_duration`, which diverges as
duty cycle approaches 1.

## Charts

### 1. Well Depth Timeline (overview)

D3 chart showing 4 years of pump cycles as vertical bars from static
water level down to minimum depth, colored by month. Supports:

- **Brush**: click+drag to select a time range
- **Zoom**: releasing the brush zooms the timeline into the selection
- **Reset**: button returns to full 4-year view
- All charts below re-render to show only the selected cycles

### 2. Pump Cycle Overlay

All pump cycles aligned at t=0 (pump start), plotted as depth vs elapsed
time. Each line is one cycle, colored by month. A synthetic pre-pump point
at t=-1 min shows the static water level before the pump starts.

Reveals seasonal patterns: winter cycles (blue) start higher and draw
less; summer cycles (red/orange) start lower and draw deeper.

### 3. Horner Plot - Recovery Analysis

Classic petroleum engineering technique applied to well recovery data.
Plots residual drawdown vs log10((t_pump + dt') / dt') where:

- `t_pump` = total pumping duration
- `dt'` = time since pump stopped

A straight line on this semi-log plot indicates constant transmissivity.
A **knee** (slope change) indicates a boundary effect:

- Steeper slope after knee = no-flow boundary (fault, clay layer)
- Flatter slope after knee = recharge boundary (stream, fracture network)

### 4. Drawdown Phase Detail

Pump-on phase only, first 60 minutes. Shows how quickly the water level
drops. Seasonal variation in drawdown rate reflects aquifer recharge state.

### 5. Recovery Phase Detail

Aligned at pump shutoff, showing meters recovered vs elapsed time.
Faster recovery indicates better aquifer connectivity.

### 6. Pump Duty Cycle

Duty cycle over time. Spikes toward 1.0 flag periods of potential leaks
or high demand.

### 7. Maximum Drawdown per Cycle

Peak drawdown over time. Deeper drawdown at the same duty cycle suggests
declining aquifer capacity.

### 8. Pump Duration per Cycle

Minutes spent pumping. Longer pump times at constant consumption indicate
reduced well yield.

## Client-Side Technology

`overlay.js` loads three libraries via CDN:

- **DuckDB-WASM** (1.29.0): Parquet loading and SQL queries in-browser
- **D3** (v7): Overview chart rendering and brush/zoom interaction
- **Observable Plot** (0.6): Declarative chart creation for analysis views

Data is loaded once into DuckDB-WASM. The D3 brush filters cycle-summary
rows by timestamp, collects matching `pump_event_id`s, filters pump-cycles
rows, and re-renders all Observable Plot charts. Debounced at 100ms.

Performance: ~800K pump-cycle rows and ~3,400 summary rows. Filtering
in JavaScript is instantaneous; SVG rendering with thousands of lines
takes ~200ms per chart.

## Configuration Files

| File | Purpose |
|------|---------|
| `water/analysis.yaml` | Hostmount version (reads from 1-min reduced data) |
| `water/analysis-remote.yaml` | Pond version (reads from raw OtelJSON, 30s data) |
| `water/site.yaml` | Sitegen config: exports and routes |
| `water/site/analysis.md` | Page template with `{{ overlay_chart /}}` shortcode |
| `crates/sitegen/assets/overlay.js` | Client-side visualization (built into sitegen binary) |
| `crates/sitegen/src/shortcodes.rs` | `overlay_chart` shortcode implementation |

## Deployment

```bash
cd water

# Export data from pond
POND=./pond pond cat "file:///analysis/pump-cycles" 2>/dev/null \
  > export/data/pump-cycles/all.parquet
POND=./pond pond cat "file:///analysis/cycle-summary" 2>/dev/null \
  > export/data/cycle-summary/all.parquet

# Update factory (after SQL changes)
POND=./pond pond mknod dynamic-dir /analysis \
  --config-path analysis-remote.yaml --overwrite

# Full site regeneration (includes overlay.js)
./render.sh --no-open
```

## Testing

```bash
# Docker-based end-to-end test (20 checks)
cd testsuite && ./run-test.sh 305

# Browser test (Puppeteer, verifies chart rendering)
cd testsuite/browser && npm run test:305
```
