// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Date parsing/formatting helpers (`YYYY-MM-DD`).

use chrono::NaiveDate;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("invalid date `{input}`: expected YYYY-MM-DD")]
pub struct DateParseError {
    pub input: String,
}

pub fn parse_ymd(s: &str) -> Result<NaiveDate, DateParseError> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| DateParseError {
        input: s.to_string(),
    })
}

#[must_use]
pub fn fmt_ymd(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

#[must_use]
pub fn fmt_ymd_opt(d: Option<NaiveDate>) -> String {
    match d {
        Some(d) => fmt_ymd(d),
        None => "(none)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid() {
        assert_eq!(
            parse_ymd("2024-10-01").unwrap(),
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap()
        );
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_ymd("10/01/2024").is_err());
        assert!(parse_ymd("2024-13-01").is_err());
        assert!(parse_ymd("not-a-date").is_err());
    }

    #[test]
    fn fmt_optional() {
        assert_eq!(
            fmt_ymd_opt(Some(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap())),
            "2024-01-02"
        );
        assert_eq!(fmt_ymd_opt(None), "(none)");
    }
}
