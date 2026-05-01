# Pump-Cycle Analysis: Coupling Note

A note on the current packaging of the pump-cycle / Horner-plot analysis page,
the long-distance coupling between its three pieces, and what a "duckpond
module" packaging would need in order to make it shareable across water
systems.

## The page in question

`https://casparwater.us/analysis/pump-cycles` -- interactive pump cycle
analysis: well-depth overview with brush-to-zoom, drawdown/recovery overlays,
Horner plot, duty-cycle charts. Documented in
[`pump-cycle-analysis.md`](pump-cycle-analysis.md).

## The three places its logic lives

```
caspar.water/config/water.yaml           duckpond/crates/sitegen/assets/      caspar.water/config/site.yaml
  sql-derived-series factories      <->    overlay.js                    <->    /sources/water/analysis/*
    pump-cycles                            (D3 + DuckDB-WASM)                    site/templates/analysis.md
    cycle-summary                          knows pump_event_id,                  {{ overlay_chart }}
  (42m threshold, filter CTEs,             elapsed_s, static_depth,              nav entry
   20 min / 1.5m / 0.3m constants)         draw_duration_s, phase, ...
              |                                       |                                  |
              +------------------- shared column schema, no contract ------------------- +
                          + {{ overlay_chart }} shortcode is compiled into duckpond
                            (crates/sitegen/src/shortcodes.rs)
```

1. **SQL** -- `caspar.water/config/water.yaml`. Two `sql-derived-series`
   factories (`pump-cycles`, `cycle-summary`) under a `dynamic-dir`. Detects
   pump events, computes static water level, applies five CTE-based
   data-quality filters, exposes per-sample and per-cycle parquet.
2. **JavaScript** -- `duckpond/crates/sitegen/assets/overlay.js`. Loads the
   parquet via DuckDB-WASM, classifies which file is which by probing
   columns, then renders the overview + 7 analysis charts (overlay, Horner,
   drawdown, recovery, duty cycle, cycles/day, dry days).
3. **Site wiring** -- `caspar.water/config/site.yaml` (export pattern
   `/sources/water/analysis/*`, nav entry) and
   `caspar.water/site/templates/analysis.md` (`{{ overlay_chart /}}`).

A fourth implicit piece: the `{{ overlay_chart }}` shortcode and
`render_overlay_chart()` in `crates/sitegen/src/shortcodes.rs` -- compiled
into the duckpond binary, not part of either repo's config.

## What is actually coupled

- **Implicit schema contract.** `overlay.js` hardcodes column names
  (`pump_event_id`, `elapsed_s`, `depth`, `static_depth`, `phase`, `month`,
  `draw_duration_s`, ...). The SQL in `water.yaml` produces exactly those
  names. Nothing declares this contract anywhere. Rename a column in one
  place and the page silently breaks.

- **Hard-wired shortcode.** `{{ overlay_chart }}` lives in
  `crates/sitegen/src/shortcodes.rs:106` and emits a container that
  `overlay.js` (also baked-in at `crates/sitegen/assets/overlay.js`) picks
  up. A second water system cannot install this feature without rebuilding
  the duckpond binary -- the renderer is not configurable.

- **Per-site constants buried in invariant logic.** The SQL mixes universal
  pump-cycle algorithm (event detection via threshold crossings, lookback
  static level, Horner inputs) with values tuned for the Caspar well: 42m
  threshold, 40-50m static-depth range, 20 min minimum draw, 1.5m minimum
  drawdown, 0.3m stale-sensor delta. They live in the same CTEs and are
  visually indistinguishable.

- **Export-path convention coupling.** `site.yaml` exports
  `/sources/water/analysis/*`, which only matches because `water.yaml`
  happens to mknod the factories under a path that lands there after
  cross-pond import. Change a path in one file, page goes empty -- no
  declared link.

## What a self-contained module would look like

A directory bundling the four artifacts:

```
pump-cycle-analysis/
  factories.yaml      # parameterized mknod: ${threshold_depth},
                      # ${input_series}, ${output_root}, filter knobs
  page.md             # sitegen markdown using {{ overlay_chart }}
                      # (or a module-named shortcode)
  overlay.js          # the renderer
  schema.md           # declared column contract: types, units, semantics
  site-include.yaml   # export pattern + nav fragments to merge into
                      # the host site.yaml
  README.md           # "pond apply factories.yaml; merge site-include"
```

An operator at another water system would `pond apply factories.yaml`
against their pond, merge the site fragments into their `site.yaml`, set
the threshold and filter parameters for their well, and get the same
analysis page.

## What blocks true modularity today

You can ship items 1, 2, 4, 5, 6 as a directory **right now** with no
duckpond changes -- it kills the first-order pain (everything pump-related
in one place, parameterized for the host well). What you cannot ship is
`overlay.js` itself: it has to be inside `crates/sitegen/assets/`, and
`{{ overlay_chart }}` has to be registered in `crates/sitegen/src/shortcodes.rs`.

To fully decouple, sitegen needs two additive features:

- **Module-contributed assets.** Sitegen should pick up JS/CSS from a
  configurable directory (or from a pond path such as
  `/system/site/modules/<name>/`), not only from compile-time
  `crates/sitegen/assets/`.

- **Module-contributed shortcodes.** Either a generic
  `{{ chart variant="overlay" /}}` that dispatches by name, or a
  registration mechanism where a module's directory contributes a
  shortcode handler. Today, adding a new chart type means editing the
  duckpond binary.

## What is **not** worth doing

Do not try to make `overlay.js` schema-polymorphic across analysis types.
The JS knows about Horner plots, month colors, draw/recovery phases --
that is domain logic, and it *should* be specific to pump-cycle data. The
generic piece is the loader (the `LIMIT 1` column probe already does
this). The renderer belongs to the module.

Likewise, do not try to factor the per-site constants out of the SQL by
making the SQL fully generic. Different wells will need different
preconditions (some have no clean threshold; some have multi-pump
sites). The right axis of variation is "different module, same shape" --
parameterize what makes sense, fork the SQL where the algorithm itself
differs.

## Recommended path

1. **Now, no code changes.** Extract the four artifacts from the
   `caspar.water` repo and `crates/sitegen/assets/overlay.js` into a
   prototype `pump-cycle-analysis/` directory with parameterized YAML.
   Document the column contract in `schema.md`. This validates the
   module boundary against a single deployment before generalizing.

2. **If step 1 holds up**, add the two sitegen features above (asset
   loading from a pond path, dynamic shortcode registration) so the
   module can ship `overlay.js` and its shortcode itself. That is the
   change that actually eliminates the long-distance coupling.

Step 1 is mostly mechanical and reversible. Step 2 is the design work.
