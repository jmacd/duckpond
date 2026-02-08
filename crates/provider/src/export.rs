// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared export logic for writing pond series/tables as Hive-partitioned parquet.
//!
//! This module contains the reusable core of `pond export` — the DataFusion
//! `COPY TO ... PARTITIONED BY` pipeline, output scanning, and temporal bounds
//! extraction. It is used by both the CLI `export` command and the `sitegen`
//! factory.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Schema information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateSchema {
    pub fields: Vec<TemplateField>,
}

/// Field information for templates (matches original format)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TemplateField {
    pub name: String,       // Field name
    pub instrument: String, // Instrument path (parsed from field name)
    pub unit: String,       // Unit (parsed from field name)
    pub agg: String,        // Aggregation type (parsed from field name)
}

/// Metadata about an exported file
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportOutput {
    /// Relative path to exported file
    pub file: PathBuf,
    /// UTC timestamp for start of data (seconds)
    pub start_time: Option<i64>,
    /// UTC timestamp for end of data (seconds)
    pub end_time: Option<i64>,
}

/// Export leaf containing files and schema information
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportLeaf {
    pub files: Vec<ExportOutput>,
    pub schema: TemplateSchema,
}

/// Hierarchical metadata structure for export results
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ExportSet {
    Empty,
    Files(ExportLeaf),
    Map(HashMap<String, Box<ExportSet>>),
}

impl ExportSet {
    /// Construct hierarchical export set from capture groups, outputs, and schema
    #[must_use]
    pub fn construct_with_schema(
        inputs: Vec<(Vec<String>, ExportOutput)>,
        schema: TemplateSchema,
    ) -> Self {
        let mut eset = ExportSet::Empty;
        for (captures, output) in inputs {
            eset.insert_with_schema(&captures, output, &schema);
        }
        eset
    }

    /// Construct hierarchical export set from capture groups and outputs (legacy compatibility)
    #[must_use]
    pub fn construct(inputs: Vec<(Vec<String>, ExportOutput)>) -> Self {
        let empty_schema = TemplateSchema { fields: vec![] };
        Self::construct_with_schema(inputs, empty_schema)
    }

    /// Insert output at path specified by capture groups with schema
    fn insert_with_schema(
        &mut self,
        captures: &[String],
        output: ExportOutput,
        schema: &TemplateSchema,
    ) {
        if captures.is_empty() {
            if let ExportSet::Empty = self {
                *self = ExportSet::Files(ExportLeaf {
                    files: vec![],
                    schema: schema.clone(),
                });
            }
            if let ExportSet::Files(leaf) = self {
                leaf.files.push(output);
            }
            return;
        }

        if let ExportSet::Empty = self {
            *self = ExportSet::Map(HashMap::new());
        }
        if let ExportSet::Map(map) = self {
            _ = map
                .entry(captures[0].clone())
                .and_modify(|e| {
                    e.insert_with_schema(&captures[1..], output.clone(), schema);
                })
                .or_insert_with(|| {
                    let mut x = ExportSet::Empty;
                    x.insert_with_schema(&captures[1..], output.clone(), schema);
                    Box::new(x)
                });
        }
    }

    /// Legacy insert method for backwards compatibility
    pub fn insert(&mut self, captures: &[String], output: ExportOutput) {
        let empty_schema = TemplateSchema { fields: vec![] };
        self.insert_with_schema(captures, output, &empty_schema);
    }

    /// Filter export set to only include entries matching the given capture path
    #[must_use]
    pub fn filter_by_captures(&self, target_captures: &[String]) -> ExportSet {
        match self {
            ExportSet::Empty => ExportSet::Empty,
            ExportSet::Files(_leaf) => {
                if target_captures.is_empty() {
                    self.clone()
                } else {
                    ExportSet::Empty
                }
            }
            ExportSet::Map(map) => {
                if target_captures.is_empty() {
                    ExportSet::Empty
                } else {
                    let first_capture = &target_captures[0];
                    if let Some(nested_set) = map.get(first_capture) {
                        nested_set.filter_by_captures(&target_captures[1..])
                    } else {
                        ExportSet::Empty
                    }
                }
            }
        }
    }
}

/// Merge two ExportSets, preserving per-target schemas
#[must_use]
pub fn merge_export_sets(base: ExportSet, other: ExportSet) -> ExportSet {
    match (base, other) {
        (ExportSet::Empty, other) => other,
        (base, ExportSet::Empty) => base,
        (ExportSet::Files(mut base_leaf), ExportSet::Files(other_leaf)) => {
            base_leaf.files.extend(other_leaf.files);
            if base_leaf.schema.fields.is_empty() && !other_leaf.schema.fields.is_empty() {
                base_leaf.schema = other_leaf.schema;
            }
            ExportSet::Files(base_leaf)
        }
        (ExportSet::Map(mut base_map), ExportSet::Map(other_map)) => {
            for (key, other_set) in other_map {
                _ = base_map
                    .entry(key)
                    .and_modify(|existing| {
                        let merged =
                            merge_export_sets(*existing.clone(), *other_set.clone());
                        **existing = merged;
                    })
                    .or_insert(other_set);
            }
            ExportSet::Map(base_map)
        }
        // Incompatible variants — prefer the non-empty one
        (base, _) => base,
    }
}

// ---------------------------------------------------------------------------
// Export a series file to Hive-partitioned parquet
// ---------------------------------------------------------------------------

/// Export a queryable pond file (series/table) to Hive-partitioned parquet.
///
/// This is the shared core of `pond export` — registers the file as a
/// DataFusion table, runs `COPY TO ... PARTITIONED BY`, scans the output
/// directory, and extracts temporal bounds from the Hive partition paths.
///
/// Returns `(Vec<(captures, ExportOutput)>, TemplateSchema)`.
pub async fn export_series_to_parquet(
    root: &tinyfs::WD,
    pond_path: &str,
    export_dir: &Path,
    temporal_parts: &[String],
    captures: &[String],
    base_output_dir: &Path,
    provider_ctx: &tinyfs::ProviderContext,
) -> Result<(Vec<(Vec<String>, ExportOutput)>, TemplateSchema)> {
    log::debug!(
        "export_series_to_parquet: pond_path={}, export_dir={}",
        pond_path,
        export_dir.display()
    );

    // Build SQL query with temporal partitioning columns
    let temporal_columns = temporal_parts
        .iter()
        .map(|part| match part.as_str() {
            "year" => "date_part('year', timestamp) as year".to_string(),
            "month" => "date_part('month', timestamp) as month".to_string(),
            "day" => "date_part('day', timestamp) as day".to_string(),
            "hour" => "date_part('hour', timestamp) as hour".to_string(),
            "minute" => "date_part('minute', timestamp) as minute".to_string(),
            _ => format!("date_part('{}', timestamp) as {}", part, part),
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Generate unique table name
    let unique_table_name = format!(
        "series_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("ok")
            .as_nanos()
    );

    // Build SELECT clause
    let select_clause = if temporal_columns.is_empty() {
        format!("SELECT * FROM \"{}\"", unique_table_name)
    } else {
        format!(
            "SELECT *, {} FROM \"{}\"",
            temporal_columns, unique_table_name
        )
    };

    // Create output directory
    std::fs::create_dir_all(export_dir)?;

    // Register the series as a DataFusion table
    let (_, lookup) = root.resolve_path(pond_path).await.map_err(|e| {
        anyhow::anyhow!("Failed to resolve path '{}': {}", pond_path, e)
    })?;

    let node_path = match lookup {
        tinyfs::Lookup::Found(np) => np,
        _ => return Err(anyhow::anyhow!("Path not found: {}", pond_path)),
    };

    let file_handle = node_path.as_file().await.map_err(|e| {
        anyhow::anyhow!("'{}' is not a file: {}", pond_path, e)
    })?;

    let file_arc = file_handle.handle.get_file().await;
    let file_guard = file_arc.lock().await;

    let queryable = file_guard.as_queryable().ok_or_else(|| {
        anyhow::anyhow!("'{}' is not a queryable file", pond_path)
    })?;

    let node_id = node_path.id();
    let table_provider = queryable
        .as_table_provider(node_id, provider_ctx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get table provider: {}", e))?;

    drop(file_guard);

    let ctx = &provider_ctx.datafusion_session;
    _ = ctx
        .register_table(
            datafusion::sql::TableReference::bare(unique_table_name.as_str()),
            table_provider,
        )
        .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))?;

    // Build COPY command
    let mut copy_sql = format!(
        "COPY ({}) TO '{}' STORED AS PARQUET",
        select_clause,
        export_dir.to_string_lossy()
    );

    if !temporal_parts.is_empty() {
        copy_sql.push_str(&format!(
            " PARTITIONED BY ({})",
            temporal_parts.join(", ")
        ));
    }

    log::debug!("export SQL: {}", copy_sql);

    // Execute
    let df = ctx
        .sql(&copy_sql)
        .await
        .map_err(|e| anyhow::anyhow!("COPY failed for '{}': {}", pond_path, e))?;
    let _ = df
        .collect()
        .await
        .map_err(|e| anyhow::anyhow!("COPY stream failed for '{}': {}", pond_path, e))?;

    // Scan output directory for exported files
    let exported_files = discover_exported_files(export_dir, base_output_dir)?;
    log::debug!(
        "Discovered {} exported files for {}",
        exported_files.len(),
        pond_path
    );

    // Read schema from first parquet file
    let schema = read_parquet_schema(export_dir)?;

    // Build results
    let results: Vec<(Vec<String>, ExportOutput)> = exported_files
        .into_iter()
        .map(|f| (captures.to_vec(), f))
        .collect();

    if results.is_empty() {
        return Err(anyhow::anyhow!(
            "No files were exported for: {}",
            pond_path
        ));
    }

    Ok((results, schema))
}

// ---------------------------------------------------------------------------
// Output scanning
// ---------------------------------------------------------------------------

/// Discover all parquet files in the export directory, returning relative paths
/// from `base_path` with temporal bounds extracted from Hive partition dirs.
pub fn discover_exported_files(
    export_path: &Path,
    base_path: &Path,
) -> Result<Vec<ExportOutput>> {
    let mut files = Vec::new();

    fn collect_parquet_files(
        dir: &Path,
        base_path: &Path,
        files: &mut Vec<ExportOutput>,
    ) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                collect_parquet_files(&path, base_path, files)?;
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                let relative_path = path
                    .strip_prefix(base_path)
                    .map_err(|e| anyhow::anyhow!("Failed to compute relative path: {}", e))?
                    .to_path_buf();

                // Reject invalid partitions
                let path_str = relative_path.to_string_lossy();
                if path_str.contains("year=0") || path_str.contains("month=0") {
                    return Err(anyhow::anyhow!(
                        "Invalid temporal partition detected: '{}'. \
                        This indicates nullable timestamp data.",
                        relative_path.display()
                    ));
                }

                let (start_time, end_time) = extract_timestamps_from_path(&relative_path)?;

                files.push(ExportOutput {
                    file: relative_path,
                    start_time,
                    end_time,
                });
            }
        }
        Ok(())
    }

    collect_parquet_files(export_path, base_path, &mut files)?;
    files.sort_by(|a, b| a.file.cmp(&b.file));
    Ok(files)
}

// ---------------------------------------------------------------------------
// Temporal bounds extraction
// ---------------------------------------------------------------------------

/// Extract start/end timestamps from Hive-style partition path components.
///
/// e.g. `Temperature/res=1h/year=2025/month=7/data.parquet`
/// → `(Some(1751328000), Some(1753920000))`  (2025-07-01 → 2025-08-01)
pub fn extract_timestamps_from_path(
    relative_path: &Path,
) -> Result<(Option<i64>, Option<i64>)> {
    let components = relative_path.components();
    let mut temporal_parts = HashMap::new();
    let mut parsed_parts = Vec::new();

    for component in components {
        if let Some(dir_name) = component.as_os_str().to_str()
            && dir_name.contains('=')
        {
            let parts: Vec<&str> = dir_name.split('=').collect();
            if parts.len() == 2 {
                let part_name = parts[0];
                let part_value_str = parts[1];

                if !["year", "month", "day", "hour", "minute", "second"].contains(&part_name) {
                    continue;
                }

                let part_value = part_value_str.parse::<i32>().map_err(|e| {
                    anyhow::anyhow!(
                        "Invalid temporal partition value '{}={}' in '{}': {}",
                        part_name,
                        part_value_str,
                        relative_path.display(),
                        e
                    )
                })?;

                _ = temporal_parts.insert(part_name, part_value);
                parsed_parts.push(part_name);
            }
        }
    }

    if parsed_parts.is_empty() {
        return Ok((None, None));
    }

    // Fill defaults for missing temporal parts
    if !temporal_parts.contains_key("month") {
        _ = temporal_parts.insert("month", 1);
    }
    if !temporal_parts.contains_key("day") {
        _ = temporal_parts.insert("day", 1);
    }
    if !temporal_parts.contains_key("hour") {
        _ = temporal_parts.insert("hour", 0);
    }
    if !temporal_parts.contains_key("minute") {
        _ = temporal_parts.insert("minute", 0);
    }
    if !temporal_parts.contains_key("second") {
        _ = temporal_parts.insert("second", 0);
    }

    let start_time = build_utc_timestamp(&temporal_parts)?;

    if let Some(last_part) = parsed_parts.last() {
        let end_time = calculate_end_time(&temporal_parts, last_part)?;
        Ok((Some(start_time), Some(end_time)))
    } else {
        Ok((Some(start_time), None))
    }
}

/// Build UTC epoch seconds from temporal part values.
pub fn build_utc_timestamp(parts: &HashMap<&str, i32>) -> Result<i64> {
    use chrono::{TimeZone, Utc};

    let year = *parts
        .get("year")
        .ok_or_else(|| anyhow::anyhow!("Missing year"))?;
    let month = *parts.get("month").unwrap_or(&1) as u32;
    let day = *parts.get("day").unwrap_or(&1) as u32;
    let hour = *parts.get("hour").unwrap_or(&0) as u32;
    let minute = *parts.get("minute").unwrap_or(&0) as u32;
    let second = *parts.get("second").unwrap_or(&0) as u32;

    if year <= 0 {
        return Err(anyhow::anyhow!("Invalid year {}", year));
    }
    if !(1..=12).contains(&month) {
        return Err(anyhow::anyhow!("Invalid month {}", month));
    }

    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .map(|dt| dt.timestamp())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid date: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                year,
                month,
                day,
                hour,
                minute,
                second
            )
        })
}

/// Calculate end time by incrementing the finest temporal granularity.
pub fn calculate_end_time(parts: &HashMap<&str, i32>, last_part: &str) -> Result<i64> {
    use chrono::{Datelike, TimeZone, Utc};

    let year = *parts.get("year").ok_or_else(|| anyhow::anyhow!("Missing year"))?;
    let month = *parts.get("month").unwrap_or(&1) as u32;
    let day = *parts.get("day").unwrap_or(&1) as u32;
    let hour = *parts.get("hour").unwrap_or(&0) as u32;
    let minute = *parts.get("minute").unwrap_or(&0) as u32;
    let second = *parts.get("second").unwrap_or(&0) as u32;

    if year <= 0 {
        return Err(anyhow::anyhow!("Invalid year {}", year));
    }

    let start_dt = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid date: {}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, minute, second
            )
        })?;

    let end_dt = match last_part {
        "year" => start_dt
            .with_year(start_dt.year() + 1)
            .ok_or_else(|| anyhow::anyhow!("Failed to add 1 year"))?,
        "month" => {
            if start_dt.month() == 12 {
                start_dt
                    .with_year(start_dt.year() + 1)
                    .and_then(|dt| dt.with_month(1))
                    .ok_or_else(|| anyhow::anyhow!("Failed to roll over year from December"))?
            } else {
                start_dt
                    .with_month(start_dt.month() + 1)
                    .ok_or_else(|| anyhow::anyhow!("Failed to add 1 month"))?
            }
        }
        "day" => start_dt + chrono::Duration::days(1),
        "hour" => start_dt + chrono::Duration::hours(1),
        "minute" => start_dt + chrono::Duration::minutes(1),
        "second" => start_dt + chrono::Duration::seconds(1),
        _ => return Err(anyhow::anyhow!("Unknown temporal part: {}", last_part)),
    };

    Ok(end_dt.timestamp())
}

// ---------------------------------------------------------------------------
// Schema extraction
// ---------------------------------------------------------------------------

/// Read the Arrow schema from the first parquet file in a directory and
/// transform it into `TemplateSchema`.
pub fn read_parquet_schema(export_dir: &Path) -> Result<TemplateSchema> {
    let first_parquet = find_first_parquet_file(export_dir)?;

    let file = std::fs::File::open(&first_parquet).map_err(|e| {
        anyhow::anyhow!("Failed to open parquet file {}: {}", first_parquet.display(), e)
    })?;

    let reader_builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).map_err(
            |e| {
                anyhow::anyhow!(
                    "Failed to read parquet schema from {}: {}",
                    first_parquet.display(),
                    e
                )
            },
        )?;

    let arrow_schema = reader_builder.schema();
    transform_arrow_to_template_schema(arrow_schema)
}

fn find_first_parquet_file(dir: &Path) -> Result<PathBuf> {
    fn find_recursive(dir: &Path) -> Option<PathBuf> {
        let entries = std::fs::read_dir(dir).ok()?;
        for entry in entries {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
                return Some(path);
            } else if path.is_dir() && let Some(found) = find_recursive(&path) {
                return Some(found);
            }
        }
        None
    }

    find_recursive(dir).ok_or_else(|| {
        anyhow::anyhow!("No parquet files found in {}", dir.display())
    })
}

/// Transform Arrow schema to template schema format.
pub fn transform_arrow_to_template_schema(
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<TemplateSchema> {
    let mut fields = Vec::new();

    for field in arrow_schema.fields() {
        let field_name_lower = field.name().to_lowercase();
        if matches!(field_name_lower.as_str(), "timestamp" | "rtimestamp") {
            continue;
        }

        let template_field = parse_field_name(field.name())?;
        fields.push(template_field);
    }

    Ok(TemplateSchema { fields })
}

/// Parse field name into components.
///
/// Expected format: `"instrument.name.unit.agg"` → `TemplateField`.
pub fn parse_field_name(field_name: &str) -> Result<TemplateField> {
    if field_name == "timestamp.count" {
        return Ok(TemplateField {
            instrument: "system".to_string(),
            name: "timestamp".to_string(),
            unit: "count".to_string(),
            agg: "count".to_string(),
        });
    }

    let mut parts: Vec<&str> = field_name.split('.').collect();

    if parts.len() < 4 {
        return Err(anyhow::anyhow!(
            "field name: unknown format: {}",
            field_name
        ));
    }

    let agg = parts.pop().expect("ok").to_string();
    let unit = parts.pop().expect("ok").to_string();
    let name = parts.pop().expect("ok").to_string();
    let instrument = parts.join(".");

    Ok(TemplateField {
        instrument,
        name,
        unit,
        agg,
    })
}

/// Count files in an ExportSet.
#[must_use]
pub fn count_export_set_files(export_set: &ExportSet) -> usize {
    match export_set {
        ExportSet::Empty => 0,
        ExportSet::Files(leaf) => leaf.files.len(),
        ExportSet::Map(map) => map.values().map(|v| count_export_set_files(v)).sum(),
    }
}

/// Helper to print export set structure for debugging.
pub fn print_export_set(export_set: &ExportSet, indent: &str) {
    match export_set {
        ExportSet::Empty => {
            log::debug!("{}(no files)", indent);
        }
        ExportSet::Files(leaf) => {
            log::debug!("{}📄 {} exported files:", indent, leaf.files.len());
            for file_output in &leaf.files {
                let start_str = file_output
                    .start_time
                    .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
                    .map(|dt| format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
                    .unwrap_or_else(|| "N/A".to_string());
                let end_str = file_output
                    .end_time
                    .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
                    .map(|dt| format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
                    .unwrap_or_else(|| "N/A".to_string());
                log::debug!(
                    "{}  📄 {} (🕐 {} → {})",
                    indent,
                    file_output.file.display(),
                    start_str,
                    end_str
                );
            }
        }
        ExportSet::Map(map) => {
            log::debug!("{}🗂️  {} capture groups:", indent, map.len());
            for (key, nested_set) in map {
                log::debug!("{}  📁 '{}' wildcard capture:", indent, key);
                print_export_set(nested_set, &format!("{}    ", indent));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_extract_timestamps_year_month() {
        let path = PathBuf::from("Temperature/res=1h/year=2025/month=7/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();

        let expected_start = chrono::Utc
            .with_ymd_and_hms(2025, 7, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let expected_end = chrono::Utc
            .with_ymd_and_hms(2025, 8, 1, 0, 0, 0)
            .unwrap()
            .timestamp();

        assert_eq!(start, Some(expected_start));
        assert_eq!(end, Some(expected_end));
    }

    #[test]
    fn test_extract_timestamps_no_temporal() {
        let path = PathBuf::from("Temperature/res=1h/data.parquet");
        let (start, end) = extract_timestamps_from_path(&path).unwrap();
        assert_eq!(start, None);
        assert_eq!(end, None);
    }

    #[test]
    fn test_december_rollover() {
        let mut parts = HashMap::new();
        _ = parts.insert("year", 2024);
        _ = parts.insert("month", 12);
        _ = parts.insert("day", 1);
        _ = parts.insert("hour", 0);
        _ = parts.insert("minute", 0);
        _ = parts.insert("second", 0);

        let end_time = calculate_end_time(&parts, "month").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected);
    }

    #[test]
    fn test_year_rollover() {
        let mut parts = HashMap::new();
        _ = parts.insert("year", 2024);
        _ = parts.insert("month", 1);
        _ = parts.insert("day", 1);
        _ = parts.insert("hour", 0);
        _ = parts.insert("minute", 0);
        _ = parts.insert("second", 0);

        let end_time = calculate_end_time(&parts, "year").unwrap();
        let expected = chrono::Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(end_time, expected);
    }

    #[test]
    fn test_parse_field_name() {
        let f = parse_field_name("north.temperature.celsius.avg").unwrap();
        assert_eq!(f.instrument, "north");
        assert_eq!(f.name, "temperature");
        assert_eq!(f.unit, "celsius");
        assert_eq!(f.agg, "avg");
    }

    #[test]
    fn test_parse_field_name_dotted_instrument() {
        let f = parse_field_name("bay.area.sensor.temperature.celsius.avg").unwrap();
        assert_eq!(f.instrument, "bay.area.sensor");
        assert_eq!(f.name, "temperature");
        assert_eq!(f.unit, "celsius");
        assert_eq!(f.agg, "avg");
    }
}
