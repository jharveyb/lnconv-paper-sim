//! Per-node + per-MsgId metrics surface used by every node model.
//!
//! Two categories of metric, with very different costs and threading
//! shapes:
//!
//! 1. **Per-node counters** (`bytes_in/out`, `duplicates`, `sketch_*`)
//!    — owned **directly on the node model** as plain `u64` fields
//!    inside [`PerNodeMetrics`]. Each node mailbox is single-threaded
//!    by NeXosim, so plain `+=` is sound and avoids the per-event
//!    atomic that the previous shared `Vec<AtomicU64>` version paid.
//!    At end-of-run a one-shot `flush_summary` schedulable on each
//!    node sends a [`MetricsEvent::NodeSummary`] to the aggregator,
//!    which collects them into a Vec returned via the join handle.
//!
//! 2. **Per-MsgId in-flight tracking** (`record_first_seen`) — worker
//!    threads write [`MetricsEvent::FirstSeen`] into a cloned
//!    [`nexosim::ports::EventQueueWriter`]. A dedicated aggregator
//!    thread (see [`crate::metrics_aggregator`]) drains the queue,
//!    owns plain `HashMap`s for in-flight / supersession state, and
//!    forwards finalised `MsgStats` rows to the existing Parquet
//!    writer thread.
//!
//! ## Lifecycle
//!
//! [`MetricsHandle::new`] spawns the aggregator (and the Parquet
//! writer if a path is configured). Each node model holds a cloned
//! `MetricsHandle` plus its own `PerNodeMetrics`.
//! [`MetricsHandle::completed_stats`] sends the
//! `FinalizeAndShutdown` sentinel, joins the aggregator, and returns
//! the sorted `Vec<MsgStats>`. [`MetricsHandle::per_node_summary`]
//! returns the aggregator's collected per-node Vec (also populated
//! during the join).
//!
//! ## `Default`
//!
//! `MetricsHandle::default()` returns a handle whose `events_tx` is
//! `None` — `record_first_seen` and `send_node_summary` are no-ops.
//! Used only to satisfy the `#[derive(Default)]` on each node `Model`
//! struct (the runner always builds a real handle via `::new` once
//! `n_nodes` is known).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use nexosim::ports::{EventQueueWriter, EventSinkWriter};
use nexosim::time::MonotonicTime;
use parking_lot::Mutex;

use crate::message::{Gossip, MsgId, NodeIdx};
use crate::metrics_aggregator::{self, MetricsEvent};

pub struct Metrics {
    /// Live counter shared with the aggregator thread. Incremented by
    /// the aggregator on every supersession finalisation; read by the
    /// CLI summary.
    superseded_count: Arc<AtomicUsize>,
    /// Live counter incremented by `record_first_seen` on the worker
    /// thread BEFORE the event is enqueued. Lets `drive_simulation`'s
    /// progress lines reflect real-time event count without waiting
    /// on the aggregator.
    total_first_seen: AtomicUsize,
    /// Cloneable producer half of the aggregator's event queue. `None`
    /// only on `Default::default()` (see module docstring).
    events_tx: Option<EventQueueWriter<MetricsEvent>>,
    /// Aggregator thread join handle. `Mutex<Option>` so the shutdown
    /// path can `take()` it once. Returns `(per-MsgId stats,
    /// per-node summaries)` on join.
    aggregator_join: Mutex<Option<JoinHandle<AggregatorOutput>>>,
    /// In-memory mirrors returned by the aggregator after shutdown.
    /// `None` until the first `completed_stats()` / `per_node_summary()`
    /// call, set by `finalize_remaining`.
    finalized_stats: Mutex<Option<Vec<MsgStats>>>,
    finalized_per_node: Mutex<Option<Vec<NodeSummary>>>,
    /// Path the aggregator's Parquet writer is writing to.
    #[allow(dead_code)]
    stats_path: Option<PathBuf>,
}

/// What the aggregator thread returns on join: per-MsgId finalised
/// stats + per-node summaries.
pub type AggregatorOutput = (Vec<MsgStats>, Vec<NodeSummary>);

/// Per-node accounting recorded directly on the node model. Plain
/// `u64` fields — the NeXosim mailbox guarantees single-threaded
/// access per node, so no atomics are needed. At end-of-run each
/// node sends its accumulated counters to the aggregator via
/// [`MetricsEvent::NodeSummary`].
#[derive(Clone, Debug, Default)]
pub struct PerNodeMetrics {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub duplicates: u64,
    pub sketches_sent: u64,
    pub sketches_received: u64,
    pub sketches_overflowed: u64,
    pub intersection_total: u64,
    pub a_only_total: u64,
    pub b_only_total: u64,
}

/// Snapshot of a single node's counters, returned by
/// [`MetricsHandle::per_node_summary`] for CLI reporting. Same shape
/// as [`PerNodeMetrics`] plus the owning `NodeIdx`.
#[derive(Clone, Debug, Default)]
pub struct NodeSummary {
    pub idx: NodeIdx,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub duplicates: u64,
    pub sketches_sent: u64,
    pub sketches_received: u64,
    pub sketches_overflowed: u64,
    pub intersection_total: u64,
    pub a_only_total: u64,
    pub b_only_total: u64,
}

impl NodeSummary {
    pub(crate) fn from_per_node(idx: NodeIdx, m: &PerNodeMetrics) -> Self {
        Self {
            idx,
            bytes_in: m.bytes_in,
            bytes_out: m.bytes_out,
            duplicates: m.duplicates,
            sketches_sent: m.sketches_sent,
            sketches_received: m.sketches_received,
            sketches_overflowed: m.sketches_overflowed,
            intersection_total: m.intersection_total,
            a_only_total: m.a_only_total,
            b_only_total: m.b_only_total,
        }
    }
}

#[derive(Clone)]
pub struct MetricsHandle(Arc<Metrics>);

impl Default for MetricsHandle {
    fn default() -> Self {
        Self::new(0, Vec::new(), None)
    }
}

impl MetricsHandle {
    pub fn new(n_nodes: usize, percentiles: Vec<f64>, stats_path: Option<PathBuf>) -> Self {
        let superseded_count = Arc::new(AtomicUsize::new(0));
        // Skip aggregator spawn for the no-op Default handle (n=0). It
        // saves a thread for every Model::default() that the serde
        // derive constructs.
        let (events_tx, aggregator_join) = if n_nodes == 0 {
            (None, None)
        } else {
            let (tx, h) = metrics_aggregator::spawn(
                n_nodes,
                percentiles,
                superseded_count.clone(),
                stats_path.clone(),
            );
            (Some(tx), Some(h))
        };
        Self(Arc::new(Metrics {
            superseded_count,
            total_first_seen: AtomicUsize::new(0),
            events_tx,
            aggregator_join: Mutex::new(aggregator_join),
            finalized_stats: Mutex::new(None),
            finalized_per_node: Mutex::new(None),
            stats_path,
        }))
    }

    /// Idempotent over `(MsgId, NodeIdx)` once the aggregator has seen
    /// the event — duplicate slot stores are silently dropped on the
    /// aggregator side. The worker-thread cost here is just the queue
    /// write + one atomic increment for the live progress counter.
    pub fn record_first_seen(&self, node: NodeIdx, gossip: &Gossip, t: MonotonicTime) {
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::FirstSeen {
                idx: node,
                gossip: *gossip,
                time_ns: ns_since_epoch(t),
            });
            self.0.total_first_seen.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Send a node's accumulated `PerNodeMetrics` to the aggregator
    /// at end-of-run. The node's [`flush_summary`] schedulable wraps
    /// this call.
    ///
    /// [`flush_summary`]: crate::node
    pub fn send_node_summary(&self, idx: NodeIdx, summary: &PerNodeMetrics) {
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::NodeSummary {
                idx,
                summary: summary.clone(),
            });
        }
    }

    pub fn total_first_seen(&self) -> usize {
        self.0.total_first_seen.load(Ordering::Relaxed)
    }

    /// Number of messages that finalized via supersession (an older
    /// channel version was killed by a newer one before reaching 100%
    /// coverage).
    pub fn superseded_count(&self) -> usize {
        self.0.superseded_count.load(Ordering::Relaxed)
    }

    /// Send the `FinalizeAndShutdown` sentinel through the queue and
    /// join the aggregator thread. Stashes both returned mirrors so
    /// subsequent `completed_stats()` / `per_node_summary()` calls
    /// return them. Idempotent.
    pub fn finalize_remaining(&self) {
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::FinalizeAndShutdown);
        }
        if let Some(h) = self.0.aggregator_join.lock().take()
            && let Ok((stats, per_node)) = h.join()
        {
            *self.0.finalized_stats.lock() = Some(stats);
            *self.0.finalized_per_node.lock() = Some(per_node);
        }
    }

    /// Returns the sorted `Vec<MsgStats>` accumulated by the
    /// aggregator. First call also performs `finalize_remaining` if
    /// the runner hasn't already.
    pub fn completed_stats(&self) -> Vec<MsgStats> {
        self.finalize_remaining();
        let mut v = self
            .0
            .finalized_stats
            .lock()
            .clone()
            .unwrap_or_default();
        v.sort_by_key(|s| s.id);
        v
    }

    /// Returns the per-node `NodeSummary` Vec collected by the
    /// aggregator from each node's end-of-run flush event. First call
    /// also performs `finalize_remaining` if the runner hasn't already.
    /// The returned Vec is indexed by `NodeIdx`.
    pub fn per_node_summary(&self) -> Vec<NodeSummary> {
        self.finalize_remaining();
        self.0
            .finalized_per_node
            .lock()
            .clone()
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub struct MsgStats {
    pub id: MsgId,
    /// Number of nodes that ever recorded a first-seen for this message.
    /// Equals `n_nodes` for fully-converged messages; less for messages
    /// killed mid-spread by supersession.
    pub coverage: usize,
    /// Total nodes in the simulation — the denominator for the
    /// `percentiles` below.
    pub n_nodes: usize,
    pub origin_ns: u64,
    pub last_ns: u64,
    /// `(percentile_fraction, time_to_reach_percentile_from_origin)`.
    /// Each percentile `p` is interpreted *absolute* — the time at which
    /// at least `ceil(p * n_nodes)` nodes had received the message. If
    /// the message's coverage never reached that count, the time is
    /// `None`.
    pub percentiles: Vec<(f64, Option<Duration>)>,
}

#[inline]
pub(crate) fn ns_since_epoch(t: MonotonicTime) -> u64 {
    let d: Duration = t.duration_since(MonotonicTime::EPOCH);
    d.as_nanos() as u64
}

/// Convert a percentile fraction in [0.0, 1.0] to an index into the
/// sorted-times Vec of length `n`. `ceil(p * n) - 1` clamped to
/// `[0, n-1]`. Used both by the aggregator (when finalising a row)
/// and by tests; kept here so it has one canonical implementation.
pub fn pct_to_index(pct: f64, n: usize) -> usize {
    let p = pct.clamp(0.0, 1.0);
    if n == 0 {
        return 0;
    }
    let raw = (p * n as f64).ceil() as isize - 1;
    raw.max(0).min(n as isize - 1) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc as StdArc;
    use std::thread;

    fn dummy_gossip(id: MsgId, ts: u32) -> Gossip {
        Gossip {
            id,
            origin: None,
            kind: crate::message::GossipKind::ChannelUpdate,
            size_bytes: 0,
            scid: Some(1),
            direction: 0,
            timestamp: ts,
        }
    }

    /// Many threads racing on the same (msg, node) slot: only one
    /// first-seen is recorded (aggregator-side dedup). The remaining
    /// recorders all enqueue events but the aggregator drops the
    /// duplicates.
    #[test]
    fn concurrent_record_does_not_double_count() {
        let metrics = MetricsHandle::new(4, vec![1.0], None);
        let g = dummy_gossip(7, 100);
        let t = MonotonicTime::EPOCH + Duration::from_micros(50);

        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = metrics.clone();
            handles.push(thread::spawn(move || {
                m.record_first_seen(0, &g, t);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(metrics.total_first_seen(), 16);
        let _ = metrics.completed_stats();
    }

    /// `send_node_summary` round-trips a NodeSummary through the
    /// aggregator and into `per_node_summary()`.
    #[test]
    fn node_summary_round_trips() {
        let m = MetricsHandle::new(3, vec![1.0], None);
        let mut s0 = PerNodeMetrics::default();
        s0.bytes_in = 100;
        s0.duplicates = 5;
        let mut s2 = PerNodeMetrics::default();
        s2.bytes_out = 200;
        s2.sketches_sent = 7;
        m.send_node_summary(0, &s0);
        m.send_node_summary(2, &s2);
        let summary = m.per_node_summary();
        assert_eq!(summary.len(), 3);
        assert_eq!(summary[0].bytes_in, 100);
        assert_eq!(summary[0].duplicates, 5);
        assert_eq!(summary[1].bytes_in, 0); // never sent
        assert_eq!(summary[2].bytes_out, 200);
        assert_eq!(summary[2].sketches_sent, 7);
    }

    /// N threads recording N distinct nodes for the same message ⇒
    /// the message finalises exactly once and lands in completed_stats.
    #[test]
    fn concurrent_complete_finalises_once() {
        let n_nodes: usize = 32;
        let metrics = MetricsHandle::new(n_nodes, vec![1.0], None);
        let g = StdArc::new(dummy_gossip(11, 200));
        let mut handles = Vec::new();
        for i in 0..n_nodes {
            let m = metrics.clone();
            let g = g.clone();
            handles.push(thread::spawn(move || {
                let t = MonotonicTime::EPOCH + Duration::from_micros(i as u64);
                m.record_first_seen(i as NodeIdx, &g, t);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let stats = metrics.completed_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].id, 11);
        assert_eq!(stats[0].coverage, n_nodes);
        assert_eq!(metrics.total_first_seen(), n_nodes);
    }
}
