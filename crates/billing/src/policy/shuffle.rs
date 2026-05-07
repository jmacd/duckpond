// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Deterministic shuffle for rounding-distribution.
//!
//! Strategies that need to spread `total - sum(raw_share)` rounding
//! pennies across connections use this module's `shuffle` to pick a
//! reproducible order seeded from the cycle's period-close timestamp.
//!
//! Implementation: SplitMix64. Independent of any RNG crate; deterministic
//! across platforms and Rust versions; ~30 LOC of well-known
//! constants.
//!
//! **Note on Go parity** (per the design doc): the Caspar Water Go
//! program seeds Go's standard `rand.Source` (a 607-element lagged
//! Fibonacci generator) with the same nanosecond timestamp and uses
//! Fisher-Yates over its own slot pool. Our shuffle is deterministic
//! but uses different bits, so v2's per-connection bills can differ
//! from Go's by up to 1c on commercial connections (the per-cycle total
//! still matches to the cent). Replacing this module with a Go-LFSR port
//! is a future option if exact historical parity becomes required.

/// Reproducible PRNG. Same `seed` always produces the same shuffle.
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `[0, n)`. Uses Lemire's unbiased rejection
    /// method (negligible bias for `n` much smaller than 2^64, but cheap
    /// and pedantically correct).
    pub fn gen_below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        let mut x = self.next_u64();
        let mut m = (x as u128) * (n as u128);
        let mut l = m as u64;
        if l < n {
            let t = (n.wrapping_neg()) % n;
            while l < t {
                x = self.next_u64();
                m = (x as u128) * (n as u128);
                l = m as u64;
            }
        }
        (m >> 64) as u64
    }

    /// Fisher-Yates shuffle in place. After the call the slice contains
    /// the same elements in a permuted order.
    pub fn shuffle<T>(&mut self, xs: &mut [T]) {
        for i in (1..xs.len()).rev() {
            let j = self.gen_below((i + 1) as u64) as usize;
            xs.swap(i, j);
        }
    }
}

/// Construct a SplitMix64 seeded from a signed nanosecond timestamp
/// (e.g., `cycle.period_end.and_utc().timestamp_nanos_opt()`).
#[must_use]
pub fn from_unix_nanos(nanos: i64) -> SplitMix64 {
    SplitMix64::new(nanos as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_runs() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge_quickly() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        let mut diffs = 0;
        for _ in 0..32 {
            if a.next_u64() != b.next_u64() {
                diffs += 1;
            }
        }
        assert!(diffs > 25, "got only {diffs} diverging draws in 32");
    }

    #[test]
    fn gen_below_is_in_range() {
        let mut rng = SplitMix64::new(123);
        for _ in 0..1000 {
            let v = rng.gen_below(14);
            assert!(v < 14);
        }
    }

    #[test]
    fn shuffle_preserves_elements() {
        let mut rng = SplitMix64::new(2024);
        let original: Vec<i32> = (0..20).collect();
        let mut shuffled = original.clone();
        rng.shuffle(&mut shuffled);
        let mut sorted = shuffled.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, original, "shuffle must be a permutation");
        // For 20 elements with this seed, expect at least some movement.
        assert_ne!(shuffled, original);
    }

    #[test]
    fn shuffle_is_deterministic() {
        let original: Vec<i32> = (0..14).collect();
        let mut a = original.clone();
        let mut b = original.clone();
        SplitMix64::new(20_241_001).shuffle(&mut a);
        SplitMix64::new(20_241_001).shuffle(&mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn shuffle_one_element_is_noop() {
        let mut rng = SplitMix64::new(1);
        let mut v = vec![42];
        rng.shuffle(&mut v);
        assert_eq!(v, vec![42]);
    }

    #[test]
    fn shuffle_empty_does_not_panic() {
        let mut rng = SplitMix64::new(1);
        let mut v: Vec<i32> = vec![];
        rng.shuffle(&mut v);
    }
}
