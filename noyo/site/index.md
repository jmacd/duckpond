---
title: "Noyo Harbor Blue Economy"
layout: default
---

# Noyo Harbor Blue Economy

Water Quality Monitoring Dashboard

<div id="map" style="height:400px; border-radius:8px; margin:2rem 0;"></div>

## About This Project

The Noyo Harbor Blue Economy project monitors water quality at key locations
throughout Noyo Harbor in Fort Bragg, California. Real-time sensors measure:

- **Dissolved Oxygen (DO)** — Critical for marine life health
- **Salinity** — Indicates freshwater/seawater mixing
- **Temperature** — Affects oxygen levels and species habitat

Use the navigation on the left to explore data **by parameter** (comparing all
sites) or **by site** (viewing all parameters at one location).

<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script type="module">
import * as L from "https://cdn.jsdelivr.net/npm/leaflet@1.9.4/+esm";
const map = L.map("map", { scrollWheelZoom: false }).setView([39.4252, -123.8037], 16);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' }).addTo(map);
L.circle([39.42769, -123.80585], { radius: 20, color: "#667eea" }).bindPopup("<strong>Princess Seafood</strong>").addTo(map);
L.circle([39.42630, -123.80508], { radius: 20, color: "#667eea" }).bindPopup("<strong>The Wharf</strong>").addTo(map);
L.circle([39.42360, -123.80380], { radius: 20, color: "#667eea" }).bindPopup("<strong>Field Station</strong>").addTo(map);
L.circle([39.42399, -123.80215], { radius: 20, color: "#667eea" }).bindPopup("<strong>B-Dock</strong>").addTo(map);
</script>
