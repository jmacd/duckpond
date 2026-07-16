// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Durable per-unit status summary for the `status_grid` shortcode.
//!
//! The selfmon dashboard renders one card per systemd unit from an append-only
//! journal `FilePhysicalSeries`. Reading the whole series on every ~1-min build
//! costs O(total history) memory (see
//! `docs/journal-status-bounded-memory-design.md`). This module keeps a small,
//! durable sidecar per unit -- last ok/err/peak `(ts, message)`, last-seen, and
//! a bounded message tail -- maintained by a **version watermark**: each build
//! folds only journal versions newer than the highest already processed, so the
//! per-build scan is bounded to the new versions while the summary reflects all
//! history.
//!
//! The sidecar lives under the pond's filesystem `cache_dir` (a derived,
//! rebuildable artifact, exactly like the temporal-reduce merged cache): if it
//! is deleted the next build re-seeds it from a bounded hot-window backfill.
//! All journal history stays on disk and remains fully queryable; only this
//! default status render is bounded.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// One retained tail line: the event timestamp (epoch µs) and its message.
/// Kept alongside `ts_us` so successive folds can merge tails and keep the
/// newest `tail_lines` across build boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailLine {
    pub ts_us: i64,
    pub msg: String,
}

/// Newly-folded fields from the versions above the watermark. Every field is a
/// "latest matching" value paired to its own timestamp, so merging with the
/// prior summary is a per-field max-by-timestamp.
#[derive(Debug, Clone, Default)]
pub struct FoldResult {
    pub last_seen_us: Option<i64>,
    pub last_ok_us: Option<i64>,
    pub last_ok_msg: Option<String>,
    pub last_err_us: Option<i64>,
    pub last_err_msg: Option<String>,
    pub last_peak_us: Option<i64>,
    pub last_peak_msg: Option<String>,
    /// Newest tail lines from the folded versions, newest-first.
    pub tail: Vec<TailLine>,
}

/// Durable per-unit summary. Bounded in size (`O(tail_lines)` plus a handful of
/// scalar fields), independent of total journal history.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UnitSummary {
    /// Highest journal version number already folded into this summary. Folds
    /// read strictly-greater versions, so every version is processed exactly
    /// once even across sitegen downtime or late-arriving (old event-time,
    /// new version) data.
    pub processed_version: u64,
    pub last_seen_us: Option<i64>,
    pub last_ok_us: Option<i64>,
    pub last_ok_msg: Option<String>,
    pub last_err_us: Option<i64>,
    pub last_err_msg: Option<String>,
    pub last_peak_us: Option<i64>,
    pub last_peak_msg: Option<String>,
    /// Retained tail lines, newest-first, capped at the configured `tail_lines`.
    pub tail: Vec<TailLine>,
}

/// Sanitize a systemd unit name into a filesystem-safe sidecar basename.
/// Mirrors the table-name sanitization used elsewhere in `status_grid` so a
/// unit maps to a single stable file.
fn sanitize_unit(unit: &str) -> String {
    unit.replace(
        [
            '@', '.', '-', ':', '/', '\\', ' ', '+', '=', ',', ';', '(', ')',
        ],
        "_",
    )
}

/// Directory holding all per-unit summary sidecars under `cache_dir`.
pub fn summary_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("status-summary")
}

/// Path to a single unit's summary sidecar.
pub fn summary_path(cache_dir: &Path, unit: &str) -> PathBuf {
    summary_dir(cache_dir).join(format!("{}.json", sanitize_unit(unit)))
}

/// Load a unit's summary sidecar.
///
/// Returns `Ok(None)` when the sidecar does not yet exist (first run -> the
/// caller backfills). A corrupt sidecar is an `Err`; the caller logs it and
/// treats the unit as a backfill, re-seeding the derived artifact rather than
/// breaking the whole dashboard.
pub fn load(cache_dir: &Path, unit: &str) -> std::io::Result<Option<UnitSummary>> {
    let path = summary_path(cache_dir, unit);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let summary: UnitSummary = serde_json::from_slice(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("corrupt status summary {}: {}", path.display(), e),
                )
            })?;
            Ok(Some(summary))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Persist a unit's summary sidecar atomically (write a temp file in the same
/// directory, then rename over the target) so a concurrent reader never sees a
/// partially-written file.
pub fn save(cache_dir: &Path, unit: &str, summary: &UnitSummary) -> std::io::Result<()> {
    let dir = summary_dir(cache_dir);
    std::fs::create_dir_all(&dir)?;
    let path = summary_path(cache_dir, unit);
    let tmp = dir.join(format!("{}.json.tmp", sanitize_unit(unit)));
    let bytes = serde_json::to_vec(summary).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize status summary for {}: {}", unit, e),
        )
    })?;
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Between two `(ts, msg)` candidates, keep the one with the greater timestamp.
/// `None` timestamps lose to any `Some`.
fn keep_later(
    a_ts: Option<i64>,
    a_msg: Option<String>,
    b_ts: Option<i64>,
    b_msg: Option<String>,
) -> (Option<i64>, Option<String>) {
    match (a_ts, b_ts) {
        (Some(a), Some(b)) => {
            if b >= a {
                (b_ts, b_msg)
            } else {
                (a_ts, a_msg)
            }
        }
        (Some(_), None) => (a_ts, a_msg),
        (None, Some(_)) => (b_ts, b_msg),
        (None, None) => (None, None),
    }
}

/// Fold a `FoldResult` (versions above the prior watermark) into the prior
/// summary, advancing the watermark to `new_watermark` and capping the tail at
/// `tail_lines`. All merged fields are max-by-timestamp, so re-folding the same
/// logical events (e.g. after a series collapse produces a new merged version)
/// is idempotent.
pub fn merge(
    prior: UnitSummary,
    fold: FoldResult,
    new_watermark: u64,
    tail_lines: usize,
) -> UnitSummary {
    let last_seen_us = match (prior.last_seen_us, fold.last_seen_us) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    let (last_ok_us, last_ok_msg) = keep_later(
        prior.last_ok_us,
        prior.last_ok_msg,
        fold.last_ok_us,
        fold.last_ok_msg,
    );
    let (last_err_us, last_err_msg) = keep_later(
        prior.last_err_us,
        prior.last_err_msg,
        fold.last_err_us,
        fold.last_err_msg,
    );
    let (last_peak_us, last_peak_msg) = keep_later(
        prior.last_peak_us,
        prior.last_peak_msg,
        fold.last_peak_us,
        fold.last_peak_msg,
    );

    // Merge tails: combine, dedupe by (ts, msg), keep the newest `tail_lines`.
    let mut seen: HashSet<(i64, String)> = HashSet::new();
    let mut tail: Vec<TailLine> = Vec::new();
    for line in fold.tail.into_iter().chain(prior.tail) {
        if seen.insert((line.ts_us, line.msg.clone())) {
            tail.push(line);
        }
    }
    tail.sort_by_key(|b| std::cmp::Reverse(b.ts_us)); // newest-first
    tail.truncate(tail_lines);

    UnitSummary {
        processed_version: new_watermark.max(prior.processed_version),
        last_seen_us,
        last_ok_us,
        last_ok_msg,
        last_err_us,
        last_err_msg,
        last_peak_us,
        last_peak_msg,
        tail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tl(ts: i64, m: &str) -> TailLine {
        TailLine {
            ts_us: ts,
            msg: m.to_string(),
        }
    }

    #[test]
    fn backfill_merge_from_empty_prior() {
        let prior = UnitSummary::default();
        let fold = FoldResult {
            last_seen_us: Some(300),
            last_ok_us: Some(200),
            last_ok_msg: Some("ok@200".into()),
            last_err_us: None,
            last_err_msg: None,
            last_peak_us: Some(150),
            last_peak_msg: Some("peak@150".into()),
            tail: vec![tl(300, "c"), tl(200, "b"), tl(100, "a")],
        };
        let s = merge(prior, fold, 7, 10);
        assert_eq!(s.processed_version, 7);
        assert_eq!(s.last_seen_us, Some(300));
        assert_eq!(s.last_ok_msg.as_deref(), Some("ok@200"));
        assert_eq!(s.last_peak_msg.as_deref(), Some("peak@150"));
        assert_eq!(s.tail.len(), 3);
        assert_eq!(s.tail[0].ts_us, 300); // newest-first
    }

    #[test]
    fn incremental_keeps_older_field_when_no_new_one() {
        // Prior has an old error; the new fold has only ok lines. The error must
        // survive (a silent-since-error unit keeps its red card).
        let prior = UnitSummary {
            processed_version: 5,
            last_seen_us: Some(500),
            last_err_us: Some(400),
            last_err_msg: Some("boom@400".into()),
            ..Default::default()
        };
        let fold = FoldResult {
            last_seen_us: Some(600),
            last_ok_us: Some(600),
            last_ok_msg: Some("ok@600".into()),
            ..Default::default()
        };
        let s = merge(prior, fold, 6, 10);
        assert_eq!(s.processed_version, 6);
        assert_eq!(s.last_seen_us, Some(600));
        assert_eq!(s.last_err_msg.as_deref(), Some("boom@400")); // preserved
        assert_eq!(s.last_ok_msg.as_deref(), Some("ok@600"));
    }

    #[test]
    fn newer_fold_supersedes_prior_field() {
        let prior = UnitSummary {
            processed_version: 5,
            last_ok_us: Some(100),
            last_ok_msg: Some("ok@100".into()),
            ..Default::default()
        };
        let fold = FoldResult {
            last_ok_us: Some(900),
            last_ok_msg: Some("ok@900".into()),
            ..Default::default()
        };
        let s = merge(prior, fold, 6, 10);
        assert_eq!(s.last_ok_us, Some(900));
        assert_eq!(s.last_ok_msg.as_deref(), Some("ok@900"));
    }

    #[test]
    fn tail_dedupes_and_caps_newest() {
        let prior = UnitSummary {
            tail: vec![tl(200, "b"), tl(100, "a")],
            ..Default::default()
        };
        let fold = FoldResult {
            // (200,"b") duplicates prior; (300,"c") is new and newest.
            tail: vec![tl(300, "c"), tl(200, "b")],
            ..Default::default()
        };
        let s = merge(prior, fold, 1, 2);
        // Cap at 2, newest-first, no duplicate of (200,"b").
        assert_eq!(s.tail.len(), 2);
        assert_eq!(s.tail[0], tl(300, "c"));
        assert_eq!(s.tail[1], tl(200, "b"));
    }

    #[test]
    fn watermark_never_regresses() {
        let prior = UnitSummary {
            processed_version: 9,
            ..Default::default()
        };
        // A stale/empty fold reports a smaller watermark; must not regress.
        let s = merge(prior, FoldResult::default(), 3, 10);
        assert_eq!(s.processed_version, 9);
    }
}
