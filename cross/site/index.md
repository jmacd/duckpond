---
title: Caspar Infrastructure
layout: default
---

# Caspar Infrastructure

Combined monitoring dashboard for all three systems, powered by
[cross-pond import](https://github.com/jmacd/duckpond).

## Systems

- **[Septic](/septic/index.html)** -- Orenco septic system (BeaglePlay)
- **[Water](/water/index.html)** -- Caspar water system (workshophost)
- **[Noyo Harbor](/noyo/params/index.html)** -- HydroVu water quality (parameters and sites)

Each system runs its own collector independently and pushes backups to S3.
This site imports all three and generates a unified view.
