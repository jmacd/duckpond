---
title: Well Depth History
weight: 45
layout: page
section: Technical
---

## Well Depth History

This page presents the full history of our well depth measurement, an annotated timeline showing how the aquifer responds to rainfall, consumption, leaks, and repairs.

The well depth sensor measures the height of the water column from the bottom of the well. Higher values mean more water; lower values mean the aquifer is being drawn down. Typical values range from about 35 to 50 feet depending on season and demand.

### Reading the Chart

On the [Well Depth](/data/well-depth.html) data page you can see the current well depth measurements with interactive zoom. Below is a guide to interpreting what you see in the multi-year record.

### Annotated Events

The following events are visible in the well depth record. Use the time range buttons on the data page to zoom into these periods.

#### 2022

| Date (approx.) | Event | Impact |
|----------------|-------|--------|
| Aug 2022 | **Monitoring begins** | First high-resolution well depth readings via Opto 22 / GRV-R7 data acquisition unit. The system samples every 30 seconds. |
| Oct 2022 | **End of dry season** | Well depth at seasonal low, around 35-37 ft. This is the tail end of a drought period. |
| Nov-Dec 2022 | **First winter rains** | Aquifer begins recharging. Well depth rises steadily through the rainy season. |

#### 2023

| Date (approx.) | Event | Impact |
|----------------|-------|--------|
| Jan-Mar 2023 | **Peak recharge** | Heavy winter rains drive well depth to seasonal highs, around 48-50 ft. |
| Apr-May 2023 | **Spring drawdown begins** | As rain tapers off and temperatures rise, the water table begins its annual decline. |
| Summer 2023 | **Leak detected** | Unusual overnight drawdown patterns visible in the data. Well depth dropping faster than expected for normal consumption. The high-resolution data reveals the characteristic signature of a leak: steady decline even during low-use hours. |
| Late Summer 2023 | **Leak repaired** | After repair, overnight well depth stabilizes. The drawdown rate returns to normal seasonal patterns. |
| Oct-Nov 2023 | **Dry season low** | Seasonal minimum before winter rains begin. |

#### 2024

| Date (approx.) | Event | Impact |
|----------------|-------|--------|
| Winter 2024 | **Recharge season** | Normal winter recharge pattern. |
| Spring 2024 | **Hose left running** | A garden hose left running is visible as a multi-day period of elevated drawdown. Even a low-flow garden hose affects the well depth record measurably. |
| Summer 2024 | **Normal seasonal draw** | Typical summer usage pattern. |

#### 2025

| Date (approx.) | Event | Impact |
|----------------|-------|--------|
| Jan-Mar 2025 | **Winter recharge** | Aquifer recovering after dry season. |
| Mar 2025 onward | **Current monitoring via OtelJSON** | New telemetry pipeline via OpenTelemetry Collectors. Data is collected locally and ingested into DuckPond. |

### What We Learn

The well depth record demonstrates several important facts about our water system:

1. **Seasonal cycle is reliable.** Winter rain reliably recharges the aquifer. Our well has never approached dangerous levels during the monitoring period.

2. **Leaks are visible.** High-resolution monitoring makes it possible to detect leaks early by observing overnight drawdown rates. A healthy system shows flat or slightly rising well depth overnight; a leak shows continuing decline.

3. **Small consumption matters.** Even a garden hose left running for a day is clearly visible in the data. This resolution helps ensure accountability for water use.

4. **Transparency builds trust.** Publishing this data publicly lets our community see exactly how the water system performs, without having to take anyone's word for it.

### Technical Notes

- **Sensor:** Opto 22 GRV-R7-MM1001-10 data acquisition unit
- **Measurement:** Pressure transducer at well bottom, converted to feet of water column
- **Sample rate:** Every 30 seconds
- **Data pipeline:** OpenTelemetry Collector &rarr; OtelJSON logfiles &rarr; DuckPond ingest &rarr; temporal-reduce aggregation &rarr; sitegen export
- **Aggregation:** 1-hour, 6-hour, and 1-day resolution tiers with min/avg/max per window
