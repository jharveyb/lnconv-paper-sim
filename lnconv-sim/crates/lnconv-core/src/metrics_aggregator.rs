//! Single-threaded background aggregator for per-MsgId metrics state.
//!
//! Worker threads (each node's mailbox handler) emit `MetricsEvent`s
//! through a [`nexosim::ports::EventQueueWriter`] cloned into every
//! `MetricsHandle`. A dedicated aggregator thread owns the
//! [`EventQueueReader`], drains it in a blocking loop, and updates the
//! per-MsgId in-flight tracking + supersession + finalisation logic.
//!
//! Why move this off the worker threads:
//!
//! * Worker threads' hot path used to hit `scc::HashMap::entry_sync`
//!   bucket locks 3–5 times per `record_first_seen` call (finalized
//!   check, latest_version, in_flight, inflight_by_channel). Each
//!   bucket lock is an atomic op + bookkeeping. Moving the work to a
//!   single owner replaces those with a plain `HashMap::entry`.
//! * Per-node bandwidth/duplicate/sketch counters stay as direct
//!   `AtomicU64::fetch_add` on the worker thread — those are already
//!   cheap; queueing them would be slower than the atomic.
//!
//! Aggregator capacity: a single thread does ~50–200 ns per event in
//! steady state, so it scales to ~5–20 M events/s. Sketch mode tops
//! out around 250 k events/wall-s, leaving plenty of headroom.
//!
//! Shutdown: the runner sends `MetricsEvent::FinalizeAndShutdown`
//! through the queue; the aggregator drains everything queued before
//! it (FIFO), processes the sentinel, drains its own in-flight Vec
//! into final `MsgStats` rows, and returns them via the join handle.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nexosim::ports::{EventQueueWriter, EventSinkReader, SinkState, event_queue};
use nohash_hasher::{IntMap, IntSet};

use crate::message::{Gossip, GossipKind, MsgId, NodeIdx};
use crate::metrics::{AggregatorOutput, MsgStats, NodeSummary, PerNodeMetrics, pct_to_index};
use crate::state::pack_cu_key;
use crate::stats_writer;

/// Sentinel value in `MsgInflight::times` indicating the slot has
/// not yet been claimed.
const NOT_SEEN: u64 = u64::MAX;

/// Events the worker threads emit. Per-MsgId state is queued via
/// `FirstSeen`; per-node counters live on the node model and arrive
/// once at end-of-run via `NodeSummary`.
#[derive(Debug)]
pub enum MetricsEvent {
    FirstSeen {
        idx: NodeIdx,
        gossip: Gossip,
        time_ns: u64,
    },
    /// One per node, sent by the node's `flush_summary` schedulable
    /// just before the run ends. The aggregator places `summary` at
    /// `per_node[idx]` for the join return.
    NodeSummary {
        idx: NodeIdx,
        summary: PerNodeMetrics,
    },
    FinalizeAndShutdown,
}

/// Per-MsgId in-flight state owned by the aggregator. No atomics
/// needed because the aggregator is single-threaded.
struct MsgInflight {
    /// `times[i] == NOT_SEEN` if node `i` hasn't seen this message
    /// yet, else the ns-since-EPOCH of its first-seen.
    times: Vec<u64>,
    coverage: usize,
    origin_ns: u64,
}

impl MsgInflight {
    fn new(n_nodes: usize, first_ns: u64) -> Self {
        Self {
            times: vec![NOT_SEEN; n_nodes],
            coverage: 0,
            origin_ns: first_ns,
        }
    }
}

struct AggregatorState {
    n_nodes: usize,
    percentiles: Vec<f64>,
    /// Per-MsgId in-flight tracking. `MsgId` is a `xxhash3_64` output,
    /// so `IntMap`'s identity-hashing is sound and avoids re-hashing
    /// on every lookup.
    in_flight: IntMap<MsgId, MsgInflight>,
    /// `(scid, direction) -> set of in-flight MsgIds for that channel`.
    /// Keys are packed via [`crate::state::pack_cu_key`] so they fit
    /// in a single `u64` (and IntMap can identity-hash them); MsgId
    /// values use `IntSet`.
    inflight_by_channel: IntMap<u64, IntSet<MsgId>>,
    /// Latest timestamp seen for each `(scid, direction)`, packed key.
    latest_version: IntMap<u64, u32>,
    /// MsgIds whose stats have already been finalized.
    finalized: IntSet<MsgId>,
    /// Shared with `MetricsHandle` so the runner can read it live for
    /// progress lines / CLI summary without joining the aggregator.
    superseded_count: Arc<AtomicUsize>,
    /// Forward-channel into the existing background Parquet writer
    /// thread. `None` when no path was configured.
    parquet_tx: Option<Sender<MsgStats>>,
    /// In-memory mirror returned via the join handle. Used by
    /// `completed_stats()` and as the fallback when no Parquet path
    /// was configured.
    mirror: Vec<MsgStats>,
    /// Per-node summaries collected from each node's end-of-run
    /// `flush_summary` event. Indexed by `NodeIdx`; pre-sized to
    /// `n_nodes` so missed nodes show as zero rather than out-of-range.
    per_node: Vec<NodeSummary>,
}

impl AggregatorState {
    fn new(
        n_nodes: usize,
        percentiles: Vec<f64>,
        superseded_count: Arc<AtomicUsize>,
        parquet_tx: Option<Sender<MsgStats>>,
    ) -> Self {
        let mut per_node = Vec::with_capacity(n_nodes);
        for i in 0..n_nodes {
            let mut s = NodeSummary::default();
            s.idx = i as NodeIdx;
            per_node.push(s);
        }
        Self {
            n_nodes,
            percentiles,
            in_flight: IntMap::default(),
            inflight_by_channel: IntMap::default(),
            latest_version: IntMap::default(),
            finalized: IntSet::default(),
            superseded_count,
            parquet_tx,
            mirror: Vec::new(),
            per_node,
        }
    }

    fn handle_node_summary(&mut self, idx: NodeIdx, summary: PerNodeMetrics) {
        if let Some(slot) = self.per_node.get_mut(idx as usize) {
            *slot = NodeSummary::from_per_node(idx, &summary);
        }
    }

    fn handle_first_seen(&mut self, node: NodeIdx, gossip: Gossip, ns: u64) {
        // Pack `(scid, direction)` into a `u64` so the IntMap can
        // identity-hash it. None for non-ChannelUpdate gossip kinds.
        let chan_key: Option<u64> = gossip
            .scid
            .filter(|_| gossip.kind == GossipKind::ChannelUpdate)
            .map(|s| pack_cu_key(s, gossip.direction));

        if self.finalized.contains(&gossip.id) {
            return;
        }

        // Step 1: BOLT 7 supersession (ChannelUpdate only).
        if let Some(key) = chan_key {
            let do_supersede = match self.latest_version.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut occ) => {
                    if gossip.timestamp > *occ.get() {
                        *occ.get_mut() = gossip.timestamp;
                        true
                    } else {
                        false
                    }
                }
                std::collections::hash_map::Entry::Vacant(vac) => {
                    vac.insert(gossip.timestamp);
                    true
                }
            };
            if do_supersede {
                let drained: Vec<MsgId> = self
                    .inflight_by_channel
                    .get_mut(&key)
                    .map(std::mem::take)
                    .map(|s| s.into_iter().collect())
                    .unwrap_or_default();
                for old_id in drained {
                    if old_id == gossip.id {
                        continue;
                    }
                    if let Some(inflight) = self.in_flight.remove(&old_id) {
                        let stats = finalize_msg(old_id, inflight, &self.percentiles, self.n_nodes);
                        self.push_finalized(stats);
                        self.finalized.insert(old_id);
                        self.superseded_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        let inflight = self
            .in_flight
            .entry(gossip.id)
            .or_insert_with(|| MsgInflight::new(self.n_nodes, ns));

        // Step 2: per-slot first-seen mark. With single-threaded
        // aggregator, no CAS is needed — this is a plain compare-and-set.
        let slot = &mut inflight.times[node as usize];
        if *slot != NOT_SEEN {
            // Worker-side dedup ensures this rarely fires; defend
            // against re-runs / re-entries anyway.
            return;
        }
        *slot = ns;
        if ns < inflight.origin_ns {
            inflight.origin_ns = ns;
        }
        inflight.coverage += 1;
        let new_coverage = inflight.coverage;

        if new_coverage == self.n_nodes {
            // Sole completer: finalise.
            if let Some(inflight) = self.in_flight.remove(&gossip.id) {
                let stats = finalize_msg(gossip.id, inflight, &self.percentiles, self.n_nodes);
                self.push_finalized(stats);
                self.finalized.insert(gossip.id);
            }
            // Maintain the per-channel index (ChannelUpdate only).
            if let Some(key) = chan_key
                && let Some(set) = self.inflight_by_channel.get_mut(&key)
            {
                set.remove(&gossip.id);
            }
        } else if let Some(key) = chan_key {
            self.inflight_by_channel
                .entry(key)
                .or_default()
                .insert(gossip.id);
        }
    }

    fn finalize_remaining(&mut self) {
        let percentiles = self.percentiles.clone();
        let n_nodes = self.n_nodes;
        let drained: Vec<(MsgId, MsgInflight)> = self.in_flight.drain().collect();
        for (id, inflight) in drained {
            if self.finalized.contains(&id) {
                continue;
            }
            let stats = finalize_msg(id, inflight, &percentiles, n_nodes);
            self.push_finalized(stats);
            self.finalized.insert(id);
        }
        self.inflight_by_channel.clear();
    }

    fn push_finalized(&mut self, stats: MsgStats) {
        self.mirror.push(stats.clone());
        if let Some(tx) = &self.parquet_tx {
            // Channel only fails when the writer thread has died — fall
            // through silently; the mirror still has the row.
            let _ = tx.send(stats);
        }
    }

}

/// Spawn the aggregator + Parquet-writer threads. Returns the writer
/// half of the event queue (cloned into every `MetricsHandle`) plus a
/// join handle whose `.join()` returns the in-memory `MsgStats`
/// mirror collected during the run.
pub fn spawn(
    n_nodes: usize,
    percentiles: Vec<f64>,
    superseded_count: Arc<AtomicUsize>,
    parquet_path: Option<PathBuf>,
) -> (EventQueueWriter<MetricsEvent>, JoinHandle<AggregatorOutput>) {
    let (writer, reader) = event_queue::<MetricsEvent>(SinkState::Enabled);
    // The aggregator owns the parquet writer Sender; the parquet
    // thread is joined inside the aggregator's main thread on
    // shutdown so the join handle returned from here represents the
    // whole metrics pipeline.
    let handle = thread::spawn(move || {
        let (parquet_tx, parquet_join) = match &parquet_path {
            Some(path) => {
                let (tx, h) = stats_writer::spawn(path.clone(), percentiles.clone());
                (Some(tx), Some(h))
            }
            None => (None, None),
        };
        let mut state =
            AggregatorState::new(n_nodes, percentiles, superseded_count, parquet_tx);
        let mut reader = reader;
        loop {
            match reader.read() {
                Some(MetricsEvent::FirstSeen {
                    idx,
                    gossip,
                    time_ns,
                }) => {
                    state.handle_first_seen(idx, gossip, time_ns);
                }
                Some(MetricsEvent::NodeSummary { idx, summary }) => {
                    state.handle_node_summary(idx, summary);
                }
                Some(MetricsEvent::FinalizeAndShutdown) | None => {
                    // FinalizeAndShutdown is the runner's signal at end-of-run.
                    // None means all writers dropped without a sentinel — treat
                    // the same way: drain in-flight, exit.
                    state.finalize_remaining();
                    break;
                }
            }
        }
        // Drop `state` to release the parquet Sender (signals EOF to
        // the parquet writer thread). Pull both mirrors out first so
        // we can return them.
        let mirror = std::mem::take(&mut state.mirror);
        let per_node = std::mem::take(&mut state.per_node);
        drop(state);
        if let Some(h) = parquet_join {
            let _ = h.join();
        }
        (mirror, per_node)
    });
    (writer, handle)
}

/// Build final `MsgStats` from an inflight slot. Same shape as the
/// previous `finalize_msg` helper in `metrics.rs` — moved here since
/// the aggregator is now its sole caller.
fn finalize_msg(
    id: MsgId,
    inflight: MsgInflight,
    percentiles: &[f64],
    n_nodes: usize,
) -> MsgStats {
    let MsgInflight {
        times,
        coverage,
        origin_ns,
    } = inflight;
    let mut sorted: Vec<u64> = times.into_iter().filter(|&t| t != NOT_SEEN).collect();
    sorted.sort_unstable();
    let last_ns = *sorted.last().unwrap_or(&origin_ns);
    let pcts: Vec<(f64, Option<Duration>)> = percentiles
        .iter()
        .map(|&p| {
            let idx = pct_to_index(p, n_nodes);
            let val = if idx < coverage {
                Some(Duration::from_nanos(sorted[idx].saturating_sub(origin_ns)))
            } else {
                None
            };
            (p, val)
        })
        .collect();
    MsgStats {
        id,
        coverage,
        n_nodes,
        origin_ns,
        last_ns,
        percentiles: pcts,
    }
}
