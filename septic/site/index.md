---
title: "Septic Station Monitor"
layout: default
---

# Septic Station Monitor

## About

This site monitors an Orenco septic system via Modbus registers,
collected by an [OpenTelemetry collector](https://opentelemetry.io/)
running on a BeaglePlay ARM64 board (`septicplaystation.local`),
exported as OtelJSON, and ingested into
[DuckPond](https://github.com/jmacd/caspar.water).

## Pump Amps

| Tank | Pump | Metric |
|------|------|--------|
| **Recirculation (RT)** | Pump 1 | `orenco_RT_Pump1_Amps` |
| **Recirculation (RT)** | Pump 2 | `orenco_RT_Pump2_Amps` |
| **Dosing (DT)** | Pump 3 | `orenco_DT_Pump3_Amps` |
| **Dosing (DT)** | Pump 4 | `orenco_DT_Pump4_Amps` |

Use the sidebar to explore the data at different time resolutions.
