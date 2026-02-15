// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Automatic temporal partition computation for sitegen exports.
//!
//! Given a set of resolutions (from a temporal-reduce factory) and display
//! parameters (target pixel width), computes the right temporal partition
//! level for each resolution so that the ≤ 2 file invariant holds.
//!
//! # The ≤ 2 File Invariant
//!
//! For any viewport width, the chart viewer picks the finest resolution
//! where at most 2 parquet files cover the visible time range.  This
//! means each resolution's temporal partition must be wide enough that
//! the maximum viewport using that resolution spans at most 2 partitions.
//!
//! # Algorithm
//!
//! For each resolution `R` (a duration like 1h, 6h, 24h):
//!
//! 1. The maximum viewport width where `R` is appropriate is
//!    `max_viewport = R × target_points`.  Beyond this, the resolution
//!    produces more points than the screen has pixels — the viewer will
//!    switch to a coarser resolution.
//!
//! 2. For ≤ 2 files: `partition_width ≥ max_viewport / 2`.
//!
//! 3. Quantize upward to the nearest standard temporal partition level
//!    (minute, hour, day, month, quarter, year).
//!
//! The coarsest resolution's partition must accommodate `max_width` (the
//! widest possible viewport — typically the full data span).

use std::collections::BTreeMap;

/// Display parameters that drive partition computation.
#[derive(Debug, Clone)]
pub struct DisplayConfig {
    /// Target number of data points per screen width (≈ pixels).
    /// Typical value: 1000–2000.
    pub target_points: u64,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            target_points: 1500,
        }
    }
}

/// A computed partition plan for one resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionPartition {
    /// The resolution string (e.g., "1h", "6h", "24h")
    pub resolution: String,
    /// The resolution as a duration
    pub resolution_secs: u64,
    /// The temporal partition keys to use (e.g., ["year", "month"])
    pub temporal: Vec<String>,
    /// The approximate partition width in seconds
    pub partition_width_secs: u64,
    /// Maximum number of data points per partition file
    pub max_points_per_file: u64,
    /// Maximum viewport width (seconds) where this resolution is used
    pub max_viewport_secs: u64,
}

/// Compute temporal partitions for a set of resolutions.
///
/// Returns a map from resolution string (e.g., "1h") to partition plan,
/// plus any warnings about the configuration.
///
/// # Arguments
///
/// * `resolutions` — Resolution strings from the temporal-reduce config
///   (e.g., `["1h", "2h", "4h", "12h", "24h"]`)
/// * `display` — Display parameters (target_points)
/// * `max_width_secs` — Maximum viewport width in seconds (typically the
///   data span, or a configured maximum)
pub fn compute_partitions(
    resolutions: &[String],
    display: &DisplayConfig,
    max_width_secs: u64,
) -> (BTreeMap<String, ResolutionPartition>, Vec<String>) {
    let mut result = BTreeMap::new();
    let mut warnings = Vec::new();

    // Parse and sort resolutions finest-first
    let mut parsed: Vec<(String, u64)> = resolutions
        .iter()
        .filter_map(|r| parse_duration_secs(r).map(|secs| (r.clone(), secs)))
        .collect();
    parsed.sort_by_key(|(_, secs)| *secs);

    if parsed.is_empty() {
        warnings.push("No valid resolutions found".to_string());
        return (result, warnings);
    }

    let tp = display.target_points;

    for (i, (res_str, res_secs)) in parsed.iter().enumerate() {
        // Maximum viewport width where this resolution is appropriate:
        // Beyond this, it produces more points than target_points.
        //
        // For the coarsest resolution (last), use max_width_secs as the
        // upper bound — it must handle the widest possible view.
        let max_viewport = if i == parsed.len() - 1 {
            // Coarsest resolution: must handle max_width
            max_width_secs.max(res_secs * tp)
        } else {
            // The handoff point is where the next-coarser resolution takes over.
            // That's when this resolution would produce target_points data points.
            res_secs * tp
        };

        // For ≤ 2 files: partition_width >= max_viewport / 2
        let min_partition_width = max_viewport / 2;

        // Quantize upward to standard temporal partition level
        let (temporal, actual_width) = quantize_partition(min_partition_width);

        let max_points = actual_width / res_secs;

        result.insert(
            res_str.clone(),
            ResolutionPartition {
                resolution: res_str.clone(),
                resolution_secs: *res_secs,
                temporal: temporal.clone(),
                partition_width_secs: actual_width,
                max_points_per_file: max_points,
                max_viewport_secs: max_viewport,
            },
        );
    }

    // Check coarsest resolution can handle max_width in ≤ 2 files
    if let Some((coarsest_str, _)) = parsed.last()
        && let Some(plan) = result.get(coarsest_str)
        && plan.partition_width_secs * 2 < max_width_secs
    {
        warnings.push(format!(
            "Coarsest resolution '{}' with partition {:?} ({}s) cannot cover \
                     max_width ({}s) in ≤ 2 files. Consider adding a coarser resolution.",
            coarsest_str, plan.temporal, plan.partition_width_secs, max_width_secs,
        ));
    }

    // Check finest resolution isn't too coarse for narrow views
    if let Some((finest_str, finest_secs)) = parsed.first() {
        let min_useful_viewport = finest_secs * 10; // at least 10 data points
        if min_useful_viewport > 86_400 {
            // finest resolution > ~2.4h means even a 1-day view has < 10 points
            warnings.push(format!(
                "Finest resolution '{}' ({}s) may be too coarse for narrow zoom views. \
                 Consider adding a finer resolution.",
                finest_str, finest_secs,
            ));
        }
    }

    (result, warnings)
}

/// Quantize a minimum partition width to the finest standard temporal
/// partition level that's wide enough.
///
/// Returns `(temporal_keys, actual_width_secs)`.
///
/// The temporal keys are cumulative: `["year", "month"]` means the partition
/// is at monthly granularity (each Hive partition has `year=YYYY/month=MM`).
fn quantize_partition(min_width_secs: u64) -> (Vec<String>, u64) {
    // Walk from finest to coarsest, find the first level that's wide enough.
    // The temporal keys are the coarsest level plus all parent levels.
    //
    // Temporal hierarchy (DataFusion PARTITIONED BY):
    //   ["year"]                    → one file per year  (31.5M sec)
    //   ["year", "quarter"]        → one file per quarter (7.8M sec)
    //   ["year", "month"]          → one file per month   (2.6M sec)
    //   ["year", "month", "day"]   → one file per day     (86.4K sec)
    //   ["year", "month", "day", "hour"]   → one file per hour (3.6K sec)
    //   ["year", "month", "day", "hour", "minute"] → per minute (60 sec)

    // Partition specs with their approximate widths, from finest to coarsest
    let specs: &[(Vec<&str>, u64)] = &[
        (vec!["year", "month", "day", "hour", "minute"], 60),
        (vec!["year", "month", "day", "hour"], 3_600),
        (vec!["year", "month", "day"], 86_400),
        (vec!["year", "month"], 2_592_000),
        (vec!["year", "quarter"], 7_776_000),
        (vec!["year"], 31_536_000),
    ];

    for (keys, width) in specs {
        if *width >= min_width_secs {
            return (keys.iter().map(|s| s.to_string()).collect(), *width);
        }
    }

    // Nothing wide enough — use year (coarsest available)
    (vec!["year".to_string()], 31_536_000)
}

/// Parse a duration string like "1h", "30m", "6h", "1d", "1s" to seconds.
pub fn parse_duration_secs(s: &str) -> Option<u64> {
    // Try humantime-style parsing first
    if let Ok(d) = humantime::parse_duration(s) {
        return Some(d.as_secs());
    }

    // Fallback: simple NNu pattern
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let n: u64 = num_str.parse().ok()?;
    match unit {
        "s" => Some(n),
        "m" => Some(n * 60),
        "h" => Some(n * 3600),
        "d" => Some(n * 86400),
        _ => None,
    }
}

/// Extract the resolution string from a set of captures (e.g., find "res=1h"
/// and return "1h").
pub fn extract_resolution_from_captures(captures: &[String]) -> Option<String> {
    for cap in captures {
        if let Some(res) = cap.strip_prefix("res=") {
            // strip .series suffix if present
            let res = res.strip_suffix(".series").unwrap_or(res);
            return Some(res.to_string());
        }
    }
    None
}

/// Validate that the temporal-reduce resolutions are well-suited for the
/// display configuration. Returns warnings if adjustments are recommended.
pub fn validate_resolutions(
    resolutions: &[String],
    display: &DisplayConfig,
    data_span_secs: u64,
) -> Vec<String> {
    let mut warnings = Vec::new();

    let mut parsed: Vec<(String, u64)> = resolutions
        .iter()
        .filter_map(|r| parse_duration_secs(r).map(|s| (r.clone(), s)))
        .collect();
    parsed.sort_by_key(|(_, s)| *s);

    if parsed.is_empty() {
        warnings.push("No valid resolutions found".to_string());
        return warnings;
    }

    // Check: coarsest resolution should cover data_span in target_points
    let (coarsest_str, coarsest_secs) = parsed.last().unwrap();
    let points_at_full_span = data_span_secs / coarsest_secs;
    if points_at_full_span > display.target_points * 3 {
        // The coarsest resolution still has way too many points at full zoom-out.
        // Suggest a coarser resolution.
        let suggested_secs = data_span_secs / display.target_points;
        let suggested = format_duration_secs(suggested_secs);
        warnings.push(format!(
            "Coarsest resolution '{}' produces ~{} points at the full data span ({}s). \
             Consider adding a '{}' resolution to the temporal-reduce factory.",
            coarsest_str, points_at_full_span, data_span_secs, suggested,
        ));
    }

    // Check: resolution gaps shouldn't be too large (> 4x)
    for i in 0..parsed.len() - 1 {
        let (r1_str, r1_secs) = &parsed[i];
        let (r2_str, r2_secs) = &parsed[i + 1];
        let ratio = r2_secs / r1_secs;
        if ratio > 6 {
            let gap_secs = (r1_secs + r2_secs) / 2;
            let suggested = format_duration_secs(gap_secs);
            warnings.push(format!(
                "Large gap ({}x) between resolutions '{}' and '{}'. \
                 Consider adding a '{}' resolution.",
                ratio, r1_str, r2_str, suggested,
            ));
        }
    }

    warnings
}

/// Format seconds as a human-readable duration string.
fn format_duration_secs(secs: u64) -> String {
    if secs >= 86400 && secs.is_multiple_of(86400) {
        format!("{}d", secs / 86400)
    } else if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration_secs("1h"), Some(3600));
        assert_eq!(parse_duration_secs("6h"), Some(21600));
        assert_eq!(parse_duration_secs("24h"), Some(86400));
        assert_eq!(parse_duration_secs("1d"), Some(86400));
        assert_eq!(parse_duration_secs("30m"), Some(1800));
        assert_eq!(parse_duration_secs("1s"), Some(1));
    }

    #[test]
    fn test_quantize_partition() {
        // 30 seconds → minute (60s)
        assert_eq!(
            quantize_partition(30),
            (
                vec!["year", "month", "day", "hour", "minute"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                60
            )
        );

        // 1800 seconds (30 min) → hour (3600s)
        assert_eq!(
            quantize_partition(1800),
            (
                vec!["year", "month", "day", "hour"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                3600
            )
        );

        // 43200 seconds (12h) → day (86400s)
        assert_eq!(
            quantize_partition(43200),
            (
                vec!["year", "month", "day"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                86400
            )
        );

        // 1_000_000 seconds (~11.5 days) → month (2.6M s)
        assert_eq!(
            quantize_partition(1_000_000),
            (
                vec!["year", "month"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                2_592_000
            )
        );

        // 5_000_000 seconds (~58 days) → quarter (7.8M s)
        assert_eq!(
            quantize_partition(5_000_000),
            (
                vec!["year", "quarter"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                7_776_000
            )
        );

        // 10_000_000 seconds (~116 days) → year (31.5M s)
        assert_eq!(
            quantize_partition(10_000_000),
            (
                vec!["year"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
                31_536_000
            )
        );
    }

    #[test]
    fn test_compute_partitions_test201() {
        // Test 201: 1 year of data, resolutions [1h, 2h, 4h, 12h, 24h]
        // target_points = 1500
        let resolutions = vec![
            "1h".to_string(),
            "2h".to_string(),
            "4h".to_string(),
            "12h".to_string(),
            "24h".to_string(),
        ];
        let display = DisplayConfig {
            target_points: 1500,
        };
        let max_width = 365 * 86400; // 1 year

        let (partitions, warnings) = compute_partitions(&resolutions, &display, max_width);

        // Verify all resolutions have plans
        assert_eq!(partitions.len(), 5);

        // 1h: max_viewport = 3600 * 1500 = 5,400,000s (~62.5 days)
        //     min_partition = 2,700,000s → month (2,592,000)... but that's < 2.7M
        //     so it goes to quarter (7,776,000)
        let p1h = &partitions["1h"];
        println!(
            "1h: temporal={:?}, width={}s, max_viewport={}s, max_points={}",
            p1h.temporal, p1h.partition_width_secs, p1h.max_viewport_secs, p1h.max_points_per_file
        );

        // 24h (coarsest): must handle 1 year
        //   max_viewport = max(365*86400, 86400*1500) = max(31.5M, 129.6M) = 129.6M
        //   min_partition = 64.8M → year (31.5M)... but 31.5M < 64.8M!
        //   year is the coarsest we have, so it picks year.
        //   Warning should fire: 2*31.5M = 63M < 129.6M
        let p24h = &partitions["24h"];
        println!(
            "24h: temporal={:?}, width={}s, max_viewport={}s, max_points={}",
            p24h.temporal,
            p24h.partition_width_secs,
            p24h.max_viewport_secs,
            p24h.max_points_per_file
        );

        // For 24h with 1 year of data, the coarsest resolution should use yearly
        assert_eq!(p24h.temporal, vec!["year"]);

        // Print all for inspection
        for (res, plan) in &partitions {
            println!(
                "{}: {:?} ({}s partition, max {}pts/file, viewport {}s)",
                res,
                plan.temporal,
                plan.partition_width_secs,
                plan.max_points_per_file,
                plan.max_viewport_secs
            );
        }
        println!("Warnings: {:?}", warnings);
    }

    #[test]
    fn test_compute_partitions_septic() {
        // Septic: ~44 days of data, resolutions [1h, 6h, 1d]
        let resolutions = vec!["1h".to_string(), "6h".to_string(), "1d".to_string()];
        let display = DisplayConfig {
            target_points: 1500,
        };
        let max_width = 44 * 86400; // 44 days

        let (partitions, warnings) = compute_partitions(&resolutions, &display, max_width);

        for (res, plan) in &partitions {
            println!(
                "{}: {:?} ({}s partition, max {}pts/file, viewport {}s)",
                res,
                plan.temporal,
                plan.partition_width_secs,
                plan.max_points_per_file,
                plan.max_viewport_secs
            );
        }
        println!("Warnings: {:?}", warnings);

        // 1h with 44 days: max_viewport = 3600*1500 = 5.4M sec, min_partition = 2.7M → quarter
        // 6h: max_viewport = 21600*1500 = 32.4M → min_partition = 16.2M → year
        // 1d (coarsest): max(44*86400, 86400*1500) = max(3.8M, 129.6M) = 129.6M
        //   min_partition = 64.8M → year
        assert_eq!(partitions.len(), 3);
    }

    #[test]
    fn test_extract_resolution() {
        assert_eq!(
            extract_resolution_from_captures(&["Temperature".into(), "res=1h".into()]),
            Some("1h".into())
        );
        assert_eq!(
            extract_resolution_from_captures(&["data".into(), "res=6h.series".into()]),
            Some("6h".into())
        );
        assert_eq!(
            extract_resolution_from_captures(&["just_a_name".into()]),
            None
        );
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration_secs(3600), "1h");
        assert_eq!(format_duration_secs(86400), "1d");
        assert_eq!(format_duration_secs(7200), "2h");
        assert_eq!(format_duration_secs(1800), "30m");
        assert_eq!(format_duration_secs(45), "45s");
    }

    #[test]
    fn test_validate_resolutions() {
        // 5 years of data with only 1h and 24h — big gap
        let resolutions = vec!["1h".to_string(), "24h".to_string()];
        let display = DisplayConfig {
            target_points: 1500,
        };
        let warnings = validate_resolutions(&resolutions, &display, 5 * 365 * 86400);
        println!("Validation warnings: {:?}", warnings);
        assert!(warnings.iter().any(|w| w.contains("gap")));
    }
}
