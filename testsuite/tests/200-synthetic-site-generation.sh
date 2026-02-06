#!/bin/bash
# EXPERIMENT: End-to-end synthetic site generation
# EXPECTED: Full pipeline from synthetic waveforms through join/pivot/reduce
#           to template rendering and export. Produces a browsable static site
#           with per-param and per-site pages containing Parquet data files.
#
# Pipeline: synthetic-timeseries → timeseries-join → timeseries-pivot
#           → temporal-reduce → template → pond export
#
# Data: 2 sites (NorthDock, SouthDock) × 2 params (Temperature, DO)
#       1 year (2025) at 1h resolution
set -e

echo "=== Experiment: End-to-End Synthetic Site Generation ==="

pond init

OUTDIR=/tmp/example-dist

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create synthetic sensor data
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create synthetic sensors ---"

cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "north_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: sine
              amplitude: 4.0
              period: "24h"
              offset: 14.0
            - type: sine
              amplitude: 6.0
              period: "8760h"
              phase: -1.5708

  - name: "north_do"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.5
              period: "24h"
              offset: 8.0
              phase: 3.1416
            - type: sine
              amplitude: 2.0
              period: "8760h"
              phase: 1.5708

  - name: "south_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: triangle
              amplitude: 3.5
              period: "24h"
              offset: 15.0
            - type: sine
              amplitude: 5.5
              period: "8760h"
              phase: -1.5708

  - name: "south_do"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.2
              period: "24h"
              offset: 7.5
              phase: 3.1416
            - type: square
              amplitude: 0.5
              period: "4380h"
              offset: 0.0
            - type: sine
              amplitude: 1.8
              period: "8760h"
              phase: 1.5708
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "✓ /sensors created"

pond list /sensors

# Verify synthetic data
echo ""
echo "North temperature: row count and range"
pond cat /sensors/north_temperature --sql "
  SELECT COUNT(*) AS rows,
         MIN(timestamp) AS first_ts,
         MAX(timestamp) AS last_ts,
         ROUND(MIN(\"temperature.C\"), 2) AS temp_min,
         ROUND(MAX(\"temperature.C\"), 2) AS temp_max
  FROM source
"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create per-site joins at /combined
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create per-site joins ---"

cat > /tmp/combined.yaml << 'YAML'
entries:
  - name: "NorthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/north_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/north_do"
          scope: DOProbe

  - name: "SouthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/south_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/south_do"
          scope: DOProbe
YAML

pond mknod dynamic-dir /combined --config-path /tmp/combined.yaml
echo "✓ /combined created"

pond list /combined

echo ""
echo "NorthDock combined schema:"
pond describe /combined/NorthDock

echo ""
echo "NorthDock combined: row count"
pond cat /combined/NorthDock --sql "
  SELECT COUNT(*) AS rows,
         MIN(timestamp) AS first_ts,
         MAX(timestamp) AS last_ts
  FROM source
"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Create per-param pivots at /singled
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Create per-param pivots ---"

cat > /tmp/singled.yaml << 'YAML'
entries:
  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "TempProbe.temperature.C"

  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "DOProbe.do.mgL"
YAML

pond mknod dynamic-dir /singled --config-path /tmp/singled.yaml
echo "✓ /singled created"

pond list /singled

echo ""
echo "Temperature pivot schema:"
pond describe /singled/Temperature

echo ""
echo "Temperature pivot sample:"
pond cat /singled/Temperature --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 3
"

echo ""
echo "DO pivot sample:"
pond cat /singled/DO --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 3
"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Create temporal reduce at /reduced
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Create temporal reduce (1h avg/min/max) ---"

cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "single_site"
    factory: "temporal-reduce"
    config:
      in_pattern: "/combined/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]

  - name: "single_param"
    factory: "temporal-reduce"
    config:
      in_pattern: "/singled/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ /reduced created"

echo ""
echo "Reduced directory tree:"
pond list /reduced/**

echo ""
echo "Reduced single_param/Temperature schema:"
pond describe /reduced/single_param/Temperature/res=1h.series

echo ""
echo "Reduced single_param/Temperature sample (first 5 rows):"
pond cat /reduced/single_param/Temperature/res=1h.series --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 5
"

echo ""
echo "Reduced single_param/Temperature row count:"
pond cat /reduced/single_param/Temperature/res=1h.series --sql "
  SELECT COUNT(*) AS rows FROM source
"

echo ""
echo "Reduced single_site/NorthDock sample (first 5 rows):"
pond cat /reduced/single_site/NorthDock/res=1h.series --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 5
"

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Create templates
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 5: Load templates and create template factory ---"

# Copy templates into pond at /etc
cat > /tmp/data.html.tmpl << 'TMPLEOF'
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{ args[0] }} - Synthetic Example</title>
  <style>
    :root { --accent: #667eea; --bg: #0f0f1a; --fg: #e4e4e7; --muted: #71717a; --sidebar-w: 220px; }
    * { box-sizing: border-box; }
    html { font: 16px/1.5 system-ui; background: var(--bg); color: var(--fg); }
    body { margin: 0; display: flex; min-height: 100vh; }
    .sidebar { position: fixed; left: 0; top: 0; width: var(--sidebar-w); height: 100vh; background: #1a1a2e; padding: 1.5rem 0; overflow-y: auto; border-right: 1px solid rgba(255,255,255,0.1); }
    .sidebar h1 { font-size: 1.1rem; padding: 0 1rem; margin: 0 0 1rem; }
    .sidebar .section-title { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; color: var(--muted); padding: 0 1rem; margin: 1rem 0 0.3rem; }
    .sidebar a { display: block; padding: 0.3rem 1rem; color: #a1a1aa; text-decoration: none; font-size: 0.875rem; }
    .sidebar a:hover { background: rgba(255,255,255,0.05); color: var(--fg); }
    main { margin-left: var(--sidebar-w); flex: 1; padding: 2rem; max-width: 1000px; }
    h1.page-title { color: var(--accent); font-size: 2rem; margin-bottom: 0.5rem; }
    .breadcrumb { color: var(--muted); font-size: 0.9rem; margin-bottom: 2rem; }
    .breadcrumb a { color: var(--accent); text-decoration: none; }
    .time-picker { margin-bottom: 2rem; padding: 1rem; background: #16213e; border-radius: 8px; }
    .time-picker label { display: block; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; color: var(--muted); margin-bottom: 0.5rem; }
    .time-picker button { padding: 0.4rem 0.8rem; border: 1px solid rgba(255,255,255,0.1); background: transparent; color: #a1a1aa; border-radius: 4px; cursor: pointer; margin: 0.2rem; font-size: 0.85rem; }
    .time-picker button.active { background: var(--accent); border-color: var(--accent); color: #fff; }
    .chart { margin-bottom: 2rem; background: #16213e; border-radius: 8px; padding: 1rem; }
    .chart svg { width: 100%; height: auto; }
    .empty-state { padding: 2rem; text-align: center; background: #fef3c7; border-radius: 8px; color: #92400e; }
  </style>
</head>
<body>
  {{ insert_from_path(path="/templates/page/page.html") }}
  <main>
    <h1 class="page-title">{{ args[0] }}</h1>
    <div class="breadcrumb"><a href="index.html">Home</a> › Data</div>
    <div id="time-picker"></div>
    <div id="charts"></div>
  </main>
  <script type="module">
    import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";
    import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28/+esm";
    const timelist = [["1 Month",30],["3 Months",90],["6 Months",180],["12 Months",365]];
    class State { constructor(v){this.value=v;this.subs=new Set()} set(v){this.value=v;this.subs.forEach(fn=>fn(v))} subscribe(fn){this.subs.add(fn);fn(this.value)} }
    const timeState = new State(90);
    function createTimePicker(el){
      const d=document.createElement("div"); d.className="time-picker";
      d.innerHTML=`<label>Time Range</label><div>${timelist.map(([l,v])=>`<button data-d="${v}"${v===timeState.value?' class="active"':''}>${l}</button>`).join("")}</div>`;
      d.querySelectorAll("button").forEach(b=>b.addEventListener("click",()=>{d.querySelectorAll("button").forEach(x=>x.classList.remove("active"));b.classList.add("active");timeState.set(+b.dataset.d)}));
      el.appendChild(d);
    }
    let duck=null;
    async function getDuckDB(){if(!duck){const B=duckdb.getJsDelivrBundles(),b=await duckdb.selectBundle(B),w=new Worker(URL.createObjectURL(new Blob([`importScripts("${b.mainWorker}");`],{type:"text/javascript"}))),db=new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(),w);await db.instantiate(b.mainModule,b.pthreadWorker);duck=await db.connect()}return duck}
    async function queryData(df,days){
      const now=new Date("2025-12-31T23:00:00Z").getTime(), begin=now-days*864e5;
      const files=(df["res=1h"]||[]).filter(f=>f.start_time*1000<=now&&f.end_time*1000>=begin);
      const urls=files.map(f=>new URL(f.file,location.href).href);
      if(!urls.length)return[];
      const db=await getDuckDB();
      return(await db.query(`SELECT epoch_us(timestamp)/1000.0::DOUBLE as timestamp,* EXCLUDE timestamp FROM read_parquet(${JSON.stringify(urls)}) WHERE timestamp>=epoch_ms(${begin}) ORDER BY timestamp`)).toArray();
    }
    const datafiles={
    {%- for k2, v2 in export[args[0]] %}
      "{{ k2 }}":[
      {%- for v3 in v2.files %}
        {start_time:{{ v3.start_time }},end_time:{{ v3.end_time }},file:"{{ v3.file }}"},
      {%- endfor %}
      ],
    {%- endfor %}
    };
    const chartConfigs=[
    {% for key, fields in group(by="name",in=export[args[0]][args[1]].schema.fields) %}
      {name:"{{ key }}",unit:"{{ fields[0].unit }}",marks:(data)=>[
      {%- for inst, ifields in group(by="instrument",in=fields) -%}
        {%- set field = ifields[0] -%}
        Plot.lineY(data,{x:"timestamp",y:"{{ field.instrument }}.{{ field.name }}.{{ field.unit }}.avg",stroke:()=>"{{ field.instrument }}"}),
        {% if ifields | length > 1 -%}
        Plot.areaY(data,{x:"timestamp",y1:"{{ field.instrument }}.{{ field.name }}.{{ field.unit }}.min",y2:"{{ field.instrument }}.{{ field.name }}.{{ field.unit }}.max",fillOpacity:0.15,fill:()=>"{{ field.instrument }}"}),
        {% endif -%}
      {%- endfor -%}
      ]},
    {% endfor %}
    ];
    const pEl=document.getElementById("time-picker"), cEl=document.getElementById("charts");
    createTimePicker(pEl);
    timeState.subscribe(async days=>{
      const data=await queryData(datafiles,days); cEl.innerHTML="";
      if(!data.length){cEl.innerHTML=`<div class="empty-state">No data</div>`;return}
      const now=new Date("2025-12-31T23:00:00Z").getTime(),begin=now-days*864e5,w=cEl.clientWidth-32;
      for(const c of chartConfigs){
        const d=document.createElement("div");d.className="chart";
        d.appendChild(Plot.plot({title:c.name,width:w,x:{grid:true,type:"time",label:"Date",domain:[begin,now]},y:{grid:true,label:c.unit,zero:true},color:{legend:true},marks:c.marks(data)}));
        cEl.appendChild(d);
      }
    });
  </script>
</body>
</html>
TMPLEOF

cat > /tmp/index.html.tmpl << 'TMPLEOF'
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Synthetic Example — DuckPond</title>
  <style>
    :root { --accent: #667eea; --bg: #0f0f1a; --fg: #e4e4e7; --muted: #71717a; --sidebar-w: 220px; }
    * { box-sizing: border-box; }
    html { font: 16px/1.5 system-ui; background: var(--bg); color: var(--fg); }
    body { margin: 0; display: flex; min-height: 100vh; }
    .sidebar { position: fixed; left: 0; top: 0; width: var(--sidebar-w); height: 100vh; background: #1a1a2e; padding: 1.5rem 0; overflow-y: auto; border-right: 1px solid rgba(255,255,255,0.1); }
    .sidebar h1 { font-size: 1.1rem; padding: 0 1rem; margin: 0 0 1rem; }
    .sidebar .section-title { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; color: var(--muted); padding: 0 1rem; margin: 1rem 0 0.3rem; }
    .sidebar a { display: block; padding: 0.3rem 1rem; color: #a1a1aa; text-decoration: none; font-size: 0.875rem; }
    .sidebar a:hover { background: rgba(255,255,255,0.05); color: var(--fg); }
    main { margin-left: var(--sidebar-w); flex: 1; padding: 2rem; max-width: 1000px; }
    .hero { text-align: center; margin: 4rem 0; }
    .hero h1 { font-size: 3rem; font-weight: 900; background: linear-gradient(30deg, #667eea, #e4e4e7); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
    .hero h2 { color: var(--muted); font-weight: 400; }
    .about { max-width: 700px; margin: 2rem auto; line-height: 1.7; }
    .about h2 { color: var(--accent); border-bottom: 2px solid var(--accent); padding-bottom: 0.5rem; }
    .about ul { padding-left: 1.5rem; }
    .about li { margin-bottom: 0.5rem; }
  </style>
</head>
<body>
  {{ insert_from_path(path="/templates/page/page.html") }}
  <main>
    <div class="hero">
      <h1>Synthetic Example</h1>
      <h2>DuckPond End-to-End Pipeline Demo</h2>
    </div>
    <div class="about">
      <h2>Data</h2>
      <p>Two monitoring sites, each with two parameters, generated from synthetic waveforms:</p>
      <ul>
        <li><strong>NorthDock</strong> &amp; <strong>SouthDock</strong></li>
        <li><strong>Temperature</strong> (°C) &amp; <strong>DO</strong> (mg/L)</li>
      </ul>
      <p>Full year 2025 at hourly resolution (~8,760 points per series).</p>
      <h2>Pipeline</h2>
      <ul>
        <li>synthetic-timeseries → timeseries-join → timeseries-pivot</li>
        <li>→ temporal-reduce (1h avg/min/max)</li>
        <li>→ template → pond export</li>
      </ul>
    </div>
  </main>
</body>
</html>
TMPLEOF

cat > /tmp/page.html.tmpl << 'TMPLEOF'
<nav class="sidebar">
  <h1>🌡️ Synth Example</h1>
  <div class="section-title">Overview</div>
  <a href="index.html">Home</a>
  <div class="section-title">By Parameter</div>
  {% for param in export.param | keys | sort %}
  <a href="{{ param }}.html">{{ param }}</a>
  {% endfor %}
  <div class="section-title">By Site</div>
  {% for site in export.site | keys | sort %}
  <a href="{{ site }}.html">{{ site }}</a>
  {% endfor %}
</nav>
TMPLEOF

pond mkdir -p /etc
pond copy host:///tmp/data.html.tmpl  /etc/data.html.tmpl
pond copy host:///tmp/index.html.tmpl /etc/index.html.tmpl
pond copy host:///tmp/page.html.tmpl  /etc/page.html.tmpl
echo "✓ templates loaded into pond"

# Create template factory
cat > /tmp/template.yaml << 'YAML'
entries:
  - name: "params"
    factory: "template"
    config:
      in_pattern: "/reduced/single_param/**/*.series"
      out_pattern: "$0.html"
      template_file: "/etc/data.html.tmpl"

  - name: "sites"
    factory: "template"
    config:
      in_pattern: "/reduced/single_site/**/*.series"
      out_pattern: "$0.html"
      template_file: "/etc/data.html.tmpl"

  - name: "index"
    factory: "template"
    config:
      in_pattern: "/reduced/single_*/**/*.series"
      out_pattern: "index.html"
      template_file: "/etc/index.html.tmpl"

  - name: "page"
    factory: "template"
    config:
      in_pattern: "/reduced/single_*/**/*.series"
      out_pattern: "page.html"
      template_file: "/etc/page.html.tmpl"
YAML

pond mknod dynamic-dir /templates --config-path /tmp/template.yaml
echo "✓ /templates created"

echo ""
echo "Template directory:"
pond list /templates/**

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Export to dist/
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 6: Export ---"
rm -rf "${OUTDIR}"

# Per-param: parquet data + HTML pages
pond export \
  --pattern '/reduced/single_param/*/*.series' \
  --pattern '/templates/params/*' \
  --dir "${OUTDIR}" \
  --temporal "year,month"

# Per-site: parquet data + HTML pages
pond export \
  --pattern '/reduced/single_site/*/*.series' \
  --pattern '/templates/sites/*' \
  --dir "${OUTDIR}" \
  --temporal "year,month"

# Index page
pond export \
  --pattern '/templates/index/*' \
  --dir "${OUTDIR}"

echo "✓ export complete"

# ══════════════════════════════════════════════════════════════════════════════
# Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

echo ""
echo "Output tree:"
find "${OUTDIR}" -type f | sort | head -50

echo ""
echo "File counts:"
HTML_COUNT=$(find "${OUTDIR}" -name '*.html' | wc -l | tr -d ' ')
PARQUET_COUNT=$(find "${OUTDIR}" -name '*.parquet' | wc -l | tr -d ' ')
echo "  HTML:    ${HTML_COUNT}"
echo "  Parquet: ${PARQUET_COUNT}"

# Verify minimum expected files
echo ""
echo "Expected files check:"

PASS=true

# Index
if [ -f "${OUTDIR}/index.html" ]; then
  echo "  ✓ index.html"
else
  echo "  ✗ MISSING: index.html"
  PASS=false
fi

# Param pages
for p in Temperature DO; do
  found=false
  for loc in "${OUTDIR}/params/${p}.html" "${OUTDIR}/${p}.html"; do
    if [ -f "$loc" ]; then
      echo "  ✓ param page: ${p} ($(basename $(dirname $loc))/$(basename $loc))"
      found=true
      break
    fi
  done
  if [ "$found" = false ]; then
    echo "  ✗ MISSING param page: ${p}"
    PASS=false
  fi
done

# Site pages
for s in NorthDock SouthDock; do
  found=false
  for loc in "${OUTDIR}/sites/${s}.html" "${OUTDIR}/${s}.html"; do
    if [ -f "$loc" ]; then
      echo "  ✓ site page: ${s} ($(basename $(dirname $loc))/$(basename $loc))"
      found=true
      break
    fi
  done
  if [ "$found" = false ]; then
    echo "  ✗ MISSING site page: ${s}"
    PASS=false
  fi
done

# Parquet files exist
if [ "${PARQUET_COUNT}" -gt 0 ]; then
  echo "  ✓ ${PARQUET_COUNT} parquet files exported"
else
  echo "  ✗ No parquet files exported"
  PASS=false
fi

# At least one HTML file references duckdb-wasm
if grep -q "duckdb-wasm" "${OUTDIR}"/*.html 2>/dev/null || \
   grep -rq "duckdb-wasm" "${OUTDIR}"/params/ 2>/dev/null || \
   grep -rq "duckdb-wasm" "${OUTDIR}"/sites/ 2>/dev/null; then
  echo "  ✓ HTML contains DuckDB-WASM references"
else
  echo "  ⚠ No DuckDB-WASM references found in HTML (may be expected)"
fi

echo ""
if [ "$PASS" = true ]; then
  echo "=== ALL CHECKS PASSED ==="
else
  echo "=== SOME CHECKS FAILED ==="
  exit 1
fi
