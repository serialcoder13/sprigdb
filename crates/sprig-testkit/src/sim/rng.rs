//! The simulation's one source of randomness: xoshiro256** seeded through
//! splitmix64, shared by every task of a run.
//!
//! A run draws numbers in the order its tasks execute; that order is fixed
//! by the seed, so the numbers are too.

use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// A seeded, cloneable random number generator. Clones share one state.
#[derive(Debug, Clone)]
pub struct SimRng {
    state: Arc<Mutex<[u64; 4]>>,
}

fn splitmix64(z: &mut u64) -> u64 {
    *z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut x = *z;
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

impl SimRng {
    /// A generator for `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut z = seed;
        let s = [
            splitmix64(&mut z),
            splitmix64(&mut z),
            splitmix64(&mut z),
            splitmix64(&mut z),
        ];
        Self {
            state: Arc::new(Mutex::new(s)),
        }
    }

    /// An independent generator derived from this one's next output, for a
    /// component that must not shift the main sequence when it draws more
    /// or fewer numbers.
    #[must_use]
    pub fn fork(&self) -> Self {
        Self::new(self.next_u64())
    }

    /// The next 64 random bits.
    #[must_use]
    pub fn next_u64(&self) -> u64 {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    /// A number in `0..n`; 0 when `n` is 0.
    #[must_use]
    pub fn below(&self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        // Lemire's multiply-shift; the tiny bias is irrelevant here.
        let wide = u128::from(self.next_u64()) * u128::from(n);
        // The high half of a 128-bit product of two u64s fits in u64.
        #[allow(clippy::cast_possible_truncation)]
        let r = (wide >> 64) as u64;
        r
    }

    /// A number in `range`; `range.start` when it is empty.
    #[must_use]
    pub fn range(&self, range: Range<u64>) -> u64 {
        range.start + self.below(range.end.saturating_sub(range.start))
    }

    /// An index into a collection of `len` items.
    ///
    /// # Panics
    /// If `len` is 0.
    #[must_use]
    pub fn index(&self, len: usize) -> usize {
        assert!(len > 0, "SimRng::index of an empty collection");
        // usize fits in u64 on every supported target, and the result is < len.
        #[allow(clippy::cast_possible_truncation)]
        let i = self.below(len as u64) as usize;
        i
    }

    /// `true` with probability `p` (clamped to 0..=1).
    #[must_use]
    pub fn chance(&self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        // 53 random bits make a uniform f64 in [0, 1).
        #[allow(clippy::cast_precision_loss)]
        let x = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        x < p
    }

    /// A duration in `[lo, hi]`, at millisecond granularity below a second
    /// and microsecond granularity otherwise.
    #[must_use]
    pub fn duration(&self, lo: Duration, hi: Duration) -> Duration {
        if hi <= lo {
            return lo;
        }
        let span = u64::try_from(hi.saturating_sub(lo).as_micros()).unwrap_or(u64::MAX);
        lo + Duration::from_micros(self.below(span.saturating_add(1)))
    }

    /// One element of `items`.
    ///
    /// # Panics
    /// If `items` is empty.
    #[must_use]
    pub fn pick<'a, T>(&self, items: &'a [T]) -> &'a T {
        &items[self.index(items.len())]
    }

    /// Shuffles `items` in place (Fisher-Yates).
    pub fn shuffle<T>(&self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.index(i + 1);
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
#[allow(clippy::many_single_char_names)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_numbers() {
        let a = SimRng::new(7);
        let b = SimRng::new(7);
        let xs: Vec<u64> = (0..100).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..100).map(|_| b.next_u64()).collect();
        assert_eq!(xs, ys);
        assert_ne!(SimRng::new(8).next_u64(), SimRng::new(7).next_u64());
    }

    #[test]
    fn clones_share_state() {
        let a = SimRng::new(1);
        let b = a.clone();
        let x = a.next_u64();
        let y = b.next_u64();
        assert_ne!(x, y);
        let c = SimRng::new(1);
        assert_eq!(c.next_u64(), x);
        assert_eq!(c.next_u64(), y);
    }

    #[test]
    fn helpers_stay_in_bounds() {
        let r = SimRng::new(3);
        for _ in 0..1000 {
            assert!(r.below(10) < 10);
            let v = r.range(5..9);
            assert!((5..9).contains(&v));
            let d = r.duration(Duration::from_millis(10), Duration::from_millis(20));
            assert!(d >= Duration::from_millis(10) && d <= Duration::from_millis(20));
        }
        assert_eq!(r.below(0), 0);
        assert!(!r.chance(0.0));
        assert!(r.chance(1.0));
        let mut v: Vec<u32> = (0..20).collect();
        r.shuffle(&mut v);
        let mut sorted = v.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..20).collect::<Vec<_>>());
    }
}
