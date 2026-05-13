//! Reservoir sampling (Vitter 1985, Algorithm R) for bounded-memory
//! distribution estimation over arbitrarily long event streams.
//!
//! Each `Reservoir<T>` holds at most `capacity` items uniformly sampled
//! from the N items it has observed. After N >> capacity, the buffer
//! is a representative uniform subsample whose order statistics
//! approximate the underlying distribution's percentiles.
//!
//! Use case in this crate: the per-(node, kind) per-round sketch
//! samples (`intersection`, `a_only`, `b_only`) would otherwise grow
//! ~8 GB across a 26h LN-snapshot run. With per-reservoir capacity
//! 1024 (default), each node's three reservoirs × three kinds ride
//! along for ~36 KB regardless of run length.
//!
//! ## Reproducibility
//!
//! Each reservoir owns its own `ChaCha8Rng` seeded from `cfg.seed`
//! xor'd with node id and kind tag — the same recipe other
//! deterministic sub-RNGs in this crate use (`sample_phase` for
//! ticker offsets, etc). Two runs with the same TOML produce
//! bit-identical reservoir contents.
//!
//! ## Buffer allocation
//!
//! `new(capacity, seed)` preallocates `Vec::with_capacity(capacity)`,
//! so the only growth happens during the warmup phase (first
//! `capacity` observations). After warmup, every `observe()` is a
//! bounds check plus at most one in-place swap — no heap traffic.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Debug, Clone)]
pub struct Reservoir<T> {
    buf: Vec<T>,
    capacity: usize,
    seen: u64,
    rng: ChaCha8Rng,
}

impl<T: Copy> Reservoir<T> {
    pub fn new(capacity: usize, seed: u64) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            capacity,
            seen: 0,
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Observe one item. Standard Algorithm R: fill the buffer until
    /// full, then for the n-th item (0-indexed) replace a uniformly
    /// random slot with probability `capacity / (n + 1)`.
    pub fn observe(&mut self, x: T) {
        if self.buf.len() < self.capacity {
            self.buf.push(x);
        } else {
            // gen_range is inclusive on the upper bound's exclusion;
            // pick j in [0, seen]. If j < capacity, replace.
            let j = self.rng.random_range(0..=self.seen);
            if let Ok(j_usize) = usize::try_from(j)
                && j_usize < self.capacity {
                    self.buf[j_usize] = x;
                }
        }
        self.seen += 1;
    }

    pub fn samples(&self) -> &[T] {
        &self.buf
    }

    pub fn seen(&self) -> u64 {
        self.seen
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Consume the reservoir, returning its sample buffer. Used to
    /// `mem::take` the samples out for end-of-run shipping without an
    /// extra clone.
    pub fn into_inner(self) -> Vec<T> {
        self.buf
    }
}

impl<T: Copy + Default> Default for Reservoir<T> {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_during_warmup() {
        let mut r = Reservoir::<u32>::new(4, 7);
        for i in 0..3 {
            r.observe(i);
        }
        assert_eq!(r.samples().len(), 3);
        assert_eq!(r.seen(), 3);
        r.observe(99);
        assert_eq!(r.samples().len(), 4);
        assert_eq!(r.seen(), 4);
    }

    #[test]
    fn capacity_caps_buffer() {
        let mut r = Reservoir::<u32>::new(4, 7);
        for i in 0..1000 {
            r.observe(i);
        }
        assert_eq!(r.samples().len(), 4);
        assert_eq!(r.seen(), 1000);
    }

    #[test]
    fn deterministic_under_same_seed() {
        let mut a = Reservoir::<u32>::new(8, 0xdead_beef);
        let mut b = Reservoir::<u32>::new(8, 0xdead_beef);
        for i in 0..10_000u32 {
            a.observe(i);
            b.observe(i);
        }
        assert_eq!(a.samples(), b.samples());
        assert_eq!(a.seen(), b.seen());
    }

    #[test]
    fn approximate_uniform_sampling() {
        // 100k uniformly-distributed values 0..100000 into a 1024
        // reservoir should give a sample whose mean is close to 50000.
        let mut r = Reservoir::<u32>::new(1024, 42);
        for i in 0..100_000u32 {
            r.observe(i);
        }
        let sum: u64 = r.samples().iter().map(|&x| x as u64).sum();
        let mean = sum / r.samples().len() as u64;
        assert!(
            mean > 45_000 && mean < 55_000,
            "reservoir mean drift: {mean}"
        );
    }

    #[test]
    fn zero_capacity_drops_everything() {
        let mut r = Reservoir::<u32>::new(0, 1);
        for i in 0..10 {
            r.observe(i);
        }
        assert!(r.samples().is_empty());
        assert_eq!(r.seen(), 10);
    }
}
