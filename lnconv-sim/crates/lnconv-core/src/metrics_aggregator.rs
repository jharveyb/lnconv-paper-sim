//! Single-threaded background aggregator for per-MsgId metrics state
//! and the dispatch hub for the multi-Parquet writer.
//!
//! Worker threads emit [`MetricsEvent`]s via the
//! [`nexosim::ports::EventQueueWriter`] cloned into every
//! `MetricsHandle`. A dedicated aggregator thread drains the queue and:
//!
//! * Maintains in-flight + supersession state for per-message
//!   convergence percentiles (writes finalised rows to
//!   `msg_stats.parquet`).
//! * Forwards each periodic `NodeCountersDelta` event into a
//!   `NodeCountersRow` (one row per (node, flush event) in
//!   `node_counters.parquet`) plus drained `OverflowEventRow`s.
//! * Forwards each end-of-run `NodeReservoirDump` into a batch of
//!   `NodeReservoirRow`s.
//!
//! Shutdown: `MetricsEvent::FinalizeAndShutdown` is the sentinel from
//! the runner. The aggregator drains in-flight, drops its sender
//! clone of the writer channel, and returns the in-memory
//! `Vec<MsgStats>` mirror via the join handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nexosim::ports::{EventQueueWriter, EventSinkReader, SinkState, event_queue};
use nohash_hasher::{IntMap, IntSet};

use crate::message::{Gossip, GossipKind, MsgId, NodeIdx};
use crate::metrics::{
    AggregatorOutput, FirstSeenEntry, KindReservoirSamples, MsgStats, NodeCounters, OverflowEvent,
    pct_to_index,
};
use crate::state::pack_cu_key;
use crate::stats_writer::{
    NodeCountersRow, NodeReservoirRow, OverflowEventRow, RowSender, WriterRow,
};

const NOT_SEEN: u64 = u64::MAX;

#[derive(Debug)]
pub enum MetricsEvent {
    /// Per-node batched first-seen tuples. Drained from each node's
    /// `first_seen_pending` buffer at flush time. Each entry carries
    /// its own `time_ns` (captured at the absorb site) so the
    /// aggregator's percentile math is independent of the delivery
    /// time.
    FirstSeenBatch {
        idx: NodeIdx,
        entries: Vec<FirstSeenEntry>,
    },
    /// Periodic + final flush — counter snapshot only (~120 B). The
    /// drained overflow buffer rides along (moved, not cloned).
    NodeCountersDelta {
        idx: NodeIdx,
        time_ns: u64,
        counters: NodeCounters,
        drained_overflow: Vec<OverflowEvent>,
    },
    /// One-shot per node at end-of-run — moves the reservoir buffers
    /// out of the node's [`crate::metrics::SketchKindStats`].
    NodeReservoirDump {
        idx: NodeIdx,
        chan_updates: KindReservoirSamples,
        node_anns: KindReservoirSamples,
        chan_anns: KindReservoirSamples,
    },
    FinalizeAndShutdown,
}

struct MsgInflight {
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
    in_flight: IntMap<MsgId, MsgInflight>,
    inflight_by_channel: IntMap<u64, IntSet<MsgId>>,
    latest_version: IntMap<u64, u32>,
    finalized: IntSet<MsgId>,
    superseded_count: Arc<AtomicUsize>,
    /// Sender into the multi-Parquet writer thread. Cloned from the
    /// `Writer` held by the runner; the writer thread doesn't exit
    /// until ALL senders drop, so the runner's clone keeps it open
    /// until `MetricsHandle::completed_stats` is called.
    writer_tx: Option<RowSender>,
    /// In-memory mirror returned via the join handle.
    mirror: Vec<MsgStats>,
}

impl AggregatorState {
    fn new(
        n_nodes: usize,
        percentiles: Vec<f64>,
        superseded_count: Arc<AtomicUsize>,
        writer_tx: Option<RowSender>,
    ) -> Self {
        Self {
            n_nodes,
            percentiles,
            in_flight: IntMap::default(),
            inflight_by_channel: IntMap::default(),
            latest_version: IntMap::default(),
            finalized: IntSet::default(),
            superseded_count,
            writer_tx,
            mirror: Vec::new(),
        }
    }

    fn handle_counters_delta(
        &mut self,
        idx: NodeIdx,
        time_ns: u64,
        counters: NodeCounters,
        drained_overflow: Vec<OverflowEvent>,
    ) {
        let tx = match &self.writer_tx {
            Some(tx) => tx,
            None => return,
        };
        for ev in &drained_overflow {
            tx.send(WriterRow::OverflowEvent(OverflowEventRow::from_event(ev)));
        }
        tx.send(WriterRow::NodeCounters(NodeCountersRow::from_counters(
            idx, time_ns, &counters,
        )));
    }

    fn handle_reservoir_dump(
        &mut self,
        idx: NodeIdx,
        chan_updates: KindReservoirSamples,
        node_anns: KindReservoirSamples,
        chan_anns: KindReservoirSamples,
    ) {
        let tx = match &self.writer_tx {
            Some(tx) => tx,
            None => return,
        };
        for (kind_code, s) in [
            (crate::message::SketchKind::ChanUpdates.as_u8(), &chan_updates),
            (crate::message::SketchKind::NodeAnns.as_u8(), &node_anns),
            (crate::message::SketchKind::ChanAnns.as_u8(), &chan_anns),
        ] {
            let n = s.intersection.len().min(s.a_only.len()).min(s.b_only.len());
            for i in 0..n {
                let row = NodeReservoirRow {
                    node_idx: idx,
                    kind: kind_code,
                    intersection: s.intersection[i],
                    a_only: s.a_only[i],
                    b_only: s.b_only[i],
                    total_seen: s.total_seen,
                };
                tx.send(WriterRow::NodeReservoir(row));
            }
        }
    }

    /// Drain an entire batch of first-seen tuples from one node. Each
    /// entry runs through the existing per-gossip handler. Same logic
    /// as the previous per-event path; just amortises the
    /// event-queue overhead across the whole batch.
    fn handle_first_seen_batch(&mut self, node: NodeIdx, entries: Vec<FirstSeenEntry>) {
        for e in entries {
            self.handle_one_first_seen(node, e.gossip, e.time_ns);
        }
    }

    fn handle_one_first_seen(&mut self, node: NodeIdx, gossip: Gossip, ns: u64) {
        let chan_key: Option<u64> = gossip
            .scid
            .filter(|_| gossip.kind == GossipKind::ChannelUpdate)
            .map(|s| pack_cu_key(s, gossip.direction));

        if self.finalized.contains(&gossip.id) {
            return;
        }

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

        let slot = &mut inflight.times[node as usize];
        if *slot != NOT_SEEN {
            return;
        }
        *slot = ns;
        if ns < inflight.origin_ns {
            inflight.origin_ns = ns;
        }
        inflight.coverage += 1;
        let new_coverage = inflight.coverage;

        if new_coverage == self.n_nodes {
            if let Some(inflight) = self.in_flight.remove(&gossip.id) {
                let stats = finalize_msg(gossip.id, inflight, &self.percentiles, self.n_nodes);
                self.push_finalized(stats);
                self.finalized.insert(gossip.id);
            }
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
        if let Some(tx) = &self.writer_tx {
            tx.send(WriterRow::MsgStats(stats));
        }
    }
}

/// Spawn the aggregator thread.
pub fn spawn(
    n_nodes: usize,
    percentiles: Vec<f64>,
    superseded_count: Arc<AtomicUsize>,
    writer_tx: Option<RowSender>,
) -> (EventQueueWriter<MetricsEvent>, JoinHandle<AggregatorOutput>) {
    let (writer, reader) = event_queue::<MetricsEvent>(SinkState::Enabled);
    let handle = thread::spawn(move || {
        let mut state = AggregatorState::new(n_nodes, percentiles, superseded_count, writer_tx);
        let mut reader = reader;
        loop {
            match reader.read() {
                Some(MetricsEvent::FirstSeenBatch { idx, entries }) => {
                    state.handle_first_seen_batch(idx, entries);
                }
                Some(MetricsEvent::NodeCountersDelta {
                    idx,
                    time_ns,
                    counters,
                    drained_overflow,
                }) => {
                    state.handle_counters_delta(idx, time_ns, counters, drained_overflow);
                }
                Some(MetricsEvent::NodeReservoirDump {
                    idx,
                    chan_updates,
                    node_anns,
                    chan_anns,
                }) => {
                    state.handle_reservoir_dump(idx, chan_updates, node_anns, chan_anns);
                }
                Some(MetricsEvent::FinalizeAndShutdown) | None => {
                    state.finalize_remaining();
                    break;
                }
            }
        }
        let mirror = std::mem::take(&mut state.mirror);
        // Drop our sender clone so the writer can exit once the runner
        // drops its clone too.
        drop(state.writer_tx.take());
        mirror
    });
    (writer, handle)
}

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
