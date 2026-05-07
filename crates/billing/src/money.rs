// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `Cents` -- monetary amount in integer cents.
//!
//! All money in the bookkeeping system is `Int64` cents. The display layer
//! parses/formats `$1,234.56` strings; storage and arithmetic stay in
//! integer cents. This mirrors the existing Go program's
//! `currency.Amount.units` so that v2 produces bit-identical bills given
//! identical inputs.
//!
//! Operations:
//! - `parse_dollars("$1,234.56") -> Cents(123456)` (and a `-` prefix)
//! - `Cents.display() -> "$1,234.56"`
//! - `Cents.split(n) -> Vec<Cents>` mirrors Go-money's behavior: each chunk
//!   is `floor(total / n)`; the remainder `total % n` cents is distributed
//!   one each to the FIRST `|remainder|` chunks. Used by the
//!   `share-by-weight` allocator.
//! - `Cents.scale_f64(f)` mirrors Go's `Amount.Scale(f float64)`:
//!   `Cents((f * (units as f64)) as i64)` -- truncation toward zero.
//! - `Cents.scale_ratio(numer, denom)` for exact integer scaling via i128.

use regex::Regex;
use std::sync::OnceLock;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Cents(pub i64);

#[derive(Debug, Error)]
#[error("invalid dollar amount `{input}`: expected $1,234.56 (or -$1.00)")]
pub struct ParseError {
    pub input: String,
}

static DOLLAR_RE: OnceLock<Regex> = OnceLock::new();
fn dollar_regex() -> &'static Regex {
    DOLLAR_RE.get_or_init(|| {
        // Sign and `$` are both optional. The integer part requires either
        // 1-3 digits, or groups separated by commas (1-3 then groups of 3).
        // The cents part is optional (defaults to .00 when omitted, so
        // operators can write `$0`, `$5`, etc. without trailing zeros).
        Regex::new(r"^\s*(-)?\$?(\d{1,3}(?:,\d{3})+|\d+)(?:\.(\d{2}))?\s*$")
            .expect("regex compiles")
    })
}

impl Cents {
    pub const ZERO: Cents = Cents(0);

    #[must_use]
    pub fn from_units(units: i64) -> Self {
        Cents(units)
    }

    #[must_use]
    pub fn units(self) -> i64 {
        self.0
    }

    pub fn parse_dollars(s: &str) -> Result<Self, ParseError> {
        let caps = dollar_regex().captures(s).ok_or_else(|| ParseError {
            input: s.to_string(),
        })?;
        let sign: i64 = if caps.get(1).is_some() { -1 } else { 1 };
        let dollars_part = caps
            .get(2)
            .expect("dollars match")
            .as_str()
            .replace(',', "");
        let dollars: i64 = dollars_part.parse().map_err(|_| ParseError {
            input: s.to_string(),
        })?;
        let cents: i64 = match caps.get(3) {
            Some(m) => m.as_str().parse().map_err(|_| ParseError {
                input: s.to_string(),
            })?,
            None => 0,
        };
        // Use checked arithmetic to refuse overflow.
        let abs = dollars
            .checked_mul(100)
            .and_then(|d| d.checked_add(cents))
            .ok_or_else(|| ParseError {
                input: s.to_string(),
            })?;
        Ok(Cents(sign * abs))
    }

    /// Format as `$1,234.56` (or `-$1.00`).
    #[must_use]
    pub fn display(self) -> String {
        let negative = self.0 < 0;
        let abs = self.0.unsigned_abs();
        let dollars = abs / 100;
        let cents = abs % 100;
        let dollar_str = with_thousands_sep(dollars);
        if negative {
            format!("-${dollar_str}.{cents:02}")
        } else {
            format!("${dollar_str}.{cents:02}")
        }
    }

    /// Mirror of Go-money's `Money.Split(n)`: each chunk is
    /// `floor(total / n)`; the remainder `|total| % n` cents are distributed
    /// one each to the first `|remainder|` chunks (sign preserved).
    ///
    /// Invariants the caller can rely on:
    /// - `out.len() == n`
    /// - `out.iter().sum::<Cents>() == self`
    /// - For positive totals, all chunks are >= 0; for negative totals, all
    ///   are <= 0.
    ///
    /// Panics if `n == 0`.
    #[must_use]
    pub fn split(self, n: usize) -> Vec<Cents> {
        assert!(n > 0, "Cents::split(0) would divide by zero");
        // Defensive: i64::MIN.unsigned_abs() doesn't fit in i64. Refuse.
        if self.0 == i64::MIN {
            panic!("Cents::split overflow on i64::MIN");
        }
        let n_i64 = n as i64;
        let abs = self.0.unsigned_abs() as i64;
        let base = abs / n_i64;
        let remainder = (abs % n_i64) as usize;
        let sign: i64 = if self.0 < 0 { -1 } else { 1 };
        (0..n)
            .map(|i| {
                let bump = if i < remainder { 1 } else { 0 };
                Cents(sign * (base + bump))
            })
            .collect()
    }

    /// Multiply by `numer/denom` using `i128` intermediate. Truncates toward
    /// zero. Panics if `denom == 0`.
    #[must_use]
    pub fn scale_ratio(self, numer: i64, denom: i64) -> Cents {
        assert!(denom != 0, "Cents::scale_ratio: denom = 0");
        let big = (self.0 as i128) * (numer as i128) / (denom as i128);
        Cents(big as i64)
    }

    /// Multiply by `f` and truncate to i64. Mirrors Go's
    /// `Amount.Scale(f float64) Amount { return Amount{units: int64(f *
    /// float64(a.units))} }` so that v2 produces bit-identical totals to
    /// the Go program for inputs that pass through this function (e.g.,
    /// `total = sumExpenses.Scale(1.0 + margin)`).
    #[must_use]
    pub fn scale_f64(self, f: f64) -> Cents {
        Cents((f * (self.0 as f64)) as i64)
    }
}

impl std::ops::Add for Cents {
    type Output = Cents;
    fn add(self, rhs: Cents) -> Cents {
        Cents(self.0 + rhs.0)
    }
}
impl std::ops::Sub for Cents {
    type Output = Cents;
    fn sub(self, rhs: Cents) -> Cents {
        Cents(self.0 - rhs.0)
    }
}
impl std::iter::Sum for Cents {
    fn sum<I: Iterator<Item = Cents>>(iter: I) -> Cents {
        Cents(iter.map(|c| c.0).sum())
    }
}

impl std::fmt::Display for Cents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
}

fn with_thousands_sep(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() + bytes.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(b',');
        }
        out.push(*b);
    }
    String::from_utf8(out).expect("ascii digits")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_dollar_sign() {
        assert_eq!(Cents::parse_dollars("$1.00").unwrap(), Cents(100));
        assert_eq!(Cents::parse_dollars("$0.05").unwrap(), Cents(5));
        assert_eq!(Cents::parse_dollars("$1,234.56").unwrap(), Cents(123456));
        assert_eq!(
            Cents::parse_dollars("$1,234,567.89").unwrap(),
            Cents(123_456_789)
        );
    }

    #[test]
    fn parse_without_dollar_sign() {
        assert_eq!(Cents::parse_dollars("1.00").unwrap(), Cents(100));
        assert_eq!(Cents::parse_dollars("123.45").unwrap(), Cents(12345));
    }

    #[test]
    fn parse_negative() {
        assert_eq!(Cents::parse_dollars("-$1.00").unwrap(), Cents(-100));
        assert_eq!(Cents::parse_dollars("-1.00").unwrap(), Cents(-100));
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(Cents::parse_dollars("").is_err());
        assert!(Cents::parse_dollars("$1.2").is_err()); // requires exactly NN cents when present
        assert!(Cents::parse_dollars("$1.234").is_err());
        assert!(Cents::parse_dollars("$1,2.00").is_err()); // bad grouping
        assert!(Cents::parse_dollars("not money").is_err());
    }

    #[test]
    fn parse_optional_cents() {
        assert_eq!(Cents::parse_dollars("$0").unwrap(), Cents::ZERO);
        assert_eq!(Cents::parse_dollars("$5").unwrap(), Cents(500));
        assert_eq!(Cents::parse_dollars("0").unwrap(), Cents::ZERO);
        assert_eq!(Cents::parse_dollars("-$5").unwrap(), Cents(-500));
        assert_eq!(Cents::parse_dollars("$1,000").unwrap(), Cents(100_000));
    }

    #[test]
    fn display_format() {
        assert_eq!(Cents(0).display(), "$0.00");
        assert_eq!(Cents(5).display(), "$0.05");
        assert_eq!(Cents(100).display(), "$1.00");
        assert_eq!(Cents(123456).display(), "$1,234.56");
        assert_eq!(Cents(123_456_789).display(), "$1,234,567.89");
        assert_eq!(Cents(-100).display(), "-$1.00");
        assert_eq!(Cents(-123456).display(), "-$1,234.56");
    }

    #[test]
    fn parse_display_roundtrip() {
        for s in [
            "$0.00",
            "$0.05",
            "$1.00",
            "$1,234.56",
            "$1,234,567.89",
            "-$1.00",
            "-$1,234.56",
        ] {
            let c = Cents::parse_dollars(s).expect(s);
            assert_eq!(c.display(), s, "roundtrip {s}");
        }
    }

    #[test]
    fn split_distributes_remainder_to_first_chunks() {
        // $7.00 / 3 = 233, 233 + remainder 1 cent on first chunk -> 234, 233, 233
        let chunks = Cents(700).split(3);
        assert_eq!(chunks, vec![Cents(234), Cents(233), Cents(233)]);
        assert_eq!(chunks.iter().copied().sum::<Cents>(), Cents(700));
    }

    #[test]
    fn split_exact_division() {
        let chunks = Cents(900).split(3);
        assert_eq!(chunks, vec![Cents(300); 3]);
    }

    #[test]
    fn split_negative_preserves_sign() {
        let chunks = Cents(-700).split(3);
        assert_eq!(chunks, vec![Cents(-234), Cents(-233), Cents(-233)]);
        assert_eq!(chunks.iter().copied().sum::<Cents>(), Cents(-700));
    }

    #[test]
    fn split_count_one() {
        assert_eq!(Cents(123).split(1), vec![Cents(123)]);
    }

    #[test]
    fn split_zero_total() {
        let chunks = Cents(0).split(5);
        assert_eq!(chunks, vec![Cents(0); 5]);
    }

    #[test]
    #[should_panic]
    fn split_zero_n_panics() {
        let _ = Cents(100).split(0);
    }

    #[test]
    fn scale_ratio_basic() {
        // 100 cents * 1/2 = 50
        assert_eq!(Cents(100).scale_ratio(1, 2), Cents(50));
        // 7 * 2/3 = 4 (truncated from 4.666...)
        assert_eq!(Cents(7).scale_ratio(2, 3), Cents(4));
        // negatives truncate toward zero (matches i128 division semantics)
        assert_eq!(Cents(-7).scale_ratio(2, 3), Cents(-4));
    }

    #[test]
    fn scale_ratio_no_overflow() {
        // i64::MAX * 2 / 1 would overflow i64 -- via i128 it's fine before
        // the final cast. We just verify the i128 path works for moderate
        // amounts.
        let big = Cents(1_000_000_000_000); // $10 billion in cents
        assert_eq!(big.scale_ratio(7, 14), Cents(500_000_000_000));
    }

    #[test]
    fn scale_f64_matches_go() {
        // Go: int64(0.2 * float64(123456)) = int64(24691.2) = 24691
        assert_eq!(Cents(123456).scale_f64(0.2), Cents(24691));
        // 1.2x cycle margin example: $7,000 cycle costs * 1.2 = $8,400
        assert_eq!(Cents(700_000).scale_f64(1.2), Cents(840_000));
    }

    #[test]
    fn add_sub_sum() {
        assert_eq!(Cents(100) + Cents(250), Cents(350));
        assert_eq!(Cents(500) - Cents(123), Cents(377));
        let total: Cents = vec![Cents(100), Cents(200), Cents(300)].into_iter().sum();
        assert_eq!(total, Cents(600));
    }
}
