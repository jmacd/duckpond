---
title: Monitoring
weight: 40
layout: page
---

## Monitoring

Owner/operator Joshua MacDonald is a software engineer with professional experience in telemetry systems, hence our monitoring system uses "cloud-native" software practices. We monitor five instruments:

- **Well depth:** measures the height of the water column relative to the bottom of the well.
- **Chlorine tank level:** lets us observe that the chlorine pump is operational.
- **Water tank level:** tells us how much treated water is in storage.
- **System pressure:** lets us observe dynamic pressure and see that the aeration pump is running.
- **pH level:** An in-tank probe measures the pH of the water, lets us see that our aeration process is effective.

Operators access our [Influxdb](https://casparwater.us:8086) instance with live monitoring data collected through several OpenTelemetry Collectors.

We have high-resolution well depth measurements dating back to August 2022, with which we can see the history of leaks, leak repairs, faucets left running, and other kinds of fine detail about our impact on the aquifer.

🚧 [Our well-depth data is now online. We are working to have it update automatically](well_depth/index.html).
