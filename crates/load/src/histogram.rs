//! Latencies in microseconds, counted in log-linear buckets: exact below
//! `2 << bits`, and above that `1 << bits` buckets to each power of two, so
//! a percentile is off by at most one part in `1 << bits`. Histograms from
//! many hosts merge by adding their counts.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Histogram {
    /// Sub-buckets per power of two, as a power of two.
    bits: u32,
    /// Counts by bucket, leaving out empty ones.
    counts: BTreeMap<u32, u64>,
    count: u64,
    sum: u64,
    max: u64,
}

impl Histogram {
    /// Percentiles within 1%, for the latencies each step reports.
    pub fn fine() -> Histogram {
        Histogram::with_bits(7)
    }

    /// Percentiles within 13%, for each second of a timeline.
    pub fn coarse() -> Histogram {
        Histogram::with_bits(3)
    }

    fn with_bits(bits: u32) -> Histogram {
        Histogram {
            bits,
            ..Histogram::default()
        }
    }

    pub fn record(&mut self, value: u64) {
        *self.counts.entry(self.bucket(value)).or_default() += 1;
        self.count += 1;
        self.sum = self.sum.saturating_add(value);
        self.max = self.max.max(value);
    }

    pub fn merge(&mut self, other: &Histogram) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.bits = other.bits;
        }
        assert_eq!(self.bits, other.bits, "histograms of one precision merge");
        for (&bucket, &count) in &other.counts {
            *self.counts.entry(bucket).or_default() += count;
        }
        self.count += other.count;
        self.sum = self.sum.saturating_add(other.sum);
        self.max = self.max.max(other.max);
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// The value at or below which `share` of the values fall: the highest
    /// value of that value's bucket, and never above the largest recorded.
    pub fn percentile(&self, share: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let rank = ((share * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0;
        for (&bucket, &count) in &self.counts {
            seen += count;
            if seen >= rank {
                return self.highest(bucket).min(self.max);
            }
        }
        self.max
    }

    fn bucket(&self, value: u64) -> u32 {
        let exact = 2u64 << self.bits;
        if value < exact {
            return value as u32;
        }
        let power = 63 - value.leading_zeros();
        let shift = power - self.bits;
        let top = (value >> shift) as u32;
        ((shift + 1) << self.bits) + top - (1 << self.bits)
    }

    /// The highest value bucket `bucket` holds.
    fn highest(&self, bucket: u32) -> u64 {
        let exact = 2u32 << self.bits;
        if bucket < exact {
            return u64::from(bucket);
        }
        let shift = (bucket >> self.bits) - 1;
        let top = u64::from(bucket & ((1 << self.bits) - 1)) + (1 << self.bits);
        let low = top << shift;
        low + ((1u64 << shift) - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_values_are_exact() {
        let mut histogram = Histogram::fine();
        for value in 0..256 {
            histogram.record(value);
        }
        assert_eq!(histogram.percentile(0.5), 127);
        assert_eq!(histogram.percentile(1.0), 255);
        assert_eq!(histogram.percentile(0.0), 0);
    }

    #[test]
    fn buckets_are_contiguous_and_hold_their_values() {
        for histogram in [Histogram::fine(), Histogram::coarse()] {
            let mut last = 0;
            for value in 0..1_000_000u64 {
                let bucket = histogram.bucket(value);
                assert!(histogram.highest(bucket) >= value, "{value}");
                assert!(bucket == last || bucket == last + 1, "{value}");
                last = bucket;
            }
            for value in [u64::MAX / 2, u64::MAX] {
                assert!(histogram.highest(histogram.bucket(value)) >= value);
            }
        }
    }

    #[test]
    fn percentiles_are_within_a_percent() {
        let mut histogram = Histogram::fine();
        for value in 1..=100_000u64 {
            histogram.record(value * 10);
        }
        for (share, exact) in [(0.5, 500_000.0), (0.99, 990_000.0), (0.999, 999_000.0)] {
            let found = histogram.percentile(share) as f64;
            assert!((found - exact).abs() / exact < 0.01, "{share}: {found}");
        }
        assert_eq!(histogram.percentile(1.0), 1_000_000);
        assert_eq!(histogram.sum / histogram.count, 500_005);
    }

    #[test]
    fn merged_histograms_count_both() {
        let (mut left, mut right) = (Histogram::fine(), Histogram::fine());
        for value in 0..1_000 {
            left.record(value);
            right.record(value + 1_000);
        }
        let mut merged = Histogram::default();
        merged.merge(&left);
        merged.merge(&right);
        assert_eq!(merged.count, 2_000);
        assert_eq!(merged.max(), 1_999);
        let median = merged.percentile(0.5);
        assert!((995..=1_005).contains(&median), "{median}");
    }
}
