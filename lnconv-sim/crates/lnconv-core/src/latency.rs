//! Per-edge latency distributions.
//!
//! Stub for now: only `Constant` exists, and only the flooding algorithm
//! consumes it (as its forward delay). Stagger algorithms still treat
//! sends as instant — adding latency there is on the roadmap.

use std::time::Duration;

pub trait LatencyDist: Send + Sync {
    fn sample(&mut self, src: u32, dst: u32) -> Duration;
}

pub struct Constant(pub Duration);

impl LatencyDist for Constant {
    fn sample(&mut self, _src: u32, _dst: u32) -> Duration {
        self.0
    }
}
