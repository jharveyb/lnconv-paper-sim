//! First-seen event recording and post-run percentile summaries.
//!
//! Every node holds a clone of the same `MetricsHandle` (an
//! `Arc<Mutex<...>>`). On each genuinely-new gossip arrival, the node
//! pushes a `(MsgId, NodeId, time-ns-since-EPOCH)` tuple. The mutex is
//! the simplest correct option — model recvs from different nodes can
//! run in parallel under NeXosim's executor — and at observed throughput
//! (~12M events/wall-second) it has not been a bottleneck.
//!
//! After the run, `per_message_stats` groups the tuples by `MsgId`,
//! sorts each group's times, and reports the time-from-origin until
//! the configured percentile of nodes had received the message. The
//! origin is the earliest first-seen time for that message (i.e. the
//! originating node).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexosim::time::MonotonicTime;

use crate::message::{MsgId, NodeId};

#[derive(Default)]
pub struct Metrics {
    /// `(msg, node, ns_since_epoch)` for every (msg, node) first-seen
    /// pair. `ns` is computed once at record time so the post-run
    /// summary doesn't need a `MonotonicTime` API.
    seen: Vec<(MsgId, NodeId, u64)>,
}

#[derive(Clone, Default)]
pub struct MetricsHandle(Arc<Mutex<Metrics>>);

impl MetricsHandle {
    pub fn record_first_seen(&self, node: NodeId, msg: MsgId, t: MonotonicTime) {
        let ns = ns_since_epoch(t);
        self.0.lock().unwrap().seen.push((msg, node, ns));
    }

    pub fn total_first_seen(&self) -> usize {
        self.0.lock().unwrap().seen.len()
    }

    /// Returns one `MsgStats` per message, sorted by `MsgId`. Each
    /// `percentiles[i]` is the time from this message's first appearance
    /// (its origin) until `percentiles[i].0` fraction of recipients had
    /// seen it. p100 is the network-wide convergence time.
    pub fn per_message_stats(&self, percentiles: &[f64]) -> Vec<MsgStats> {
        let g = self.0.lock().unwrap();
        let mut grouped: HashMap<MsgId, Vec<u64>> = HashMap::new();
        for &(m, _n, ns) in &g.seen {
            grouped.entry(m).or_default().push(ns);
        }
        let mut out: Vec<MsgStats> = grouped
            .into_iter()
            .map(|(id, mut times)| {
                times.sort_unstable();
                let origin_ns = times[0];
                let coverage = times.len();
                let pcts = percentiles
                    .iter()
                    .map(|&p| {
                        let target_idx = pct_to_index(p, coverage);
                        let abs_ns = times[target_idx];
                        let dt_ns = abs_ns.saturating_sub(origin_ns);
                        (p, Duration::from_nanos(dt_ns))
                    })
                    .collect();
                MsgStats {
                    id,
                    coverage,
                    origin_ns,
                    last_ns: *times.last().unwrap(),
                    percentiles: pcts,
                }
            })
            .collect();
        out.sort_by_key(|s| s.id);
        out
    }
}

#[derive(Debug)]
pub struct MsgStats {
    pub id: MsgId,
    pub coverage: usize,
    pub origin_ns: u64,
    pub last_ns: u64,
    /// (percentile_fraction, time_to_reach_percentile_from_origin)
    pub percentiles: Vec<(f64, Duration)>,
}

fn ns_since_epoch(t: MonotonicTime) -> u64 {
    // `MonotonicTime` is from the `tai-time` crate (i64 secs + u32
    // nanos relative to 1970-01-01 TAI). The simulation starts at
    // `MonotonicTime::EPOCH`, so duration since epoch is sim time.
    let d: Duration = t.duration_since(MonotonicTime::EPOCH);
    d.as_nanos() as u64
}

/// Index in a sorted ascending list of `n` items whose value first reaches
/// the `pct`-th percentile (interpreted as "fraction of items at or before").
/// `pct` is in [0.0, 1.0]. Index is clamped to `[0, n-1]`.
fn pct_to_index(pct: f64, n: usize) -> usize {
    let p = pct.clamp(0.0, 1.0);
    if n == 0 {
        return 0;
    }
    // ceil(p * n) - 1, clamped — so 100% maps to n-1, 50% to ceil(n/2)-1.
    let raw = (p * n as f64).ceil() as isize - 1;
    raw.max(0).min(n as isize - 1) as usize
}
