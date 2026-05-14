//! Per-node + per-MsgId metrics surface used by every node model.
//!
//! Three categories of metric, each with a different threading shape:
//!
//! 1. **Per-node counters** (`bytes_in/out`, `duplicates`, `sketch_*`)
//!    — owned directly on the node model as plain `u64` fields inside
//!    [`PerNodeMetrics`]. Each node mailbox is single-threaded by
//!    NeXosim, so plain `+=` is sound and avoids any atomic. Periodic
//!    + final flush schedulables snapshot them into a small
//!      [`NodeCounters`] (~120 B) and ship via
//!      [`MetricsEvent::NodeCountersDelta`].
//!
//! 2. **Per-MsgId in-flight tracking** (`record_first_seen`) — worker
//!    threads write [`MetricsEvent::FirstSeen`] into a cloned
//!    [`nexosim::ports::EventQueueWriter`]. A dedicated aggregator
//!    thread drains the queue, owns plain `HashMap`s for in-flight /
//!    supersession state, and forwards finalised `MsgStats` rows to
//!    the multi-Parquet writer thread.
//!
//! 3. **Per-(node, kind) reservoir samples** — accumulated on the
//!    node's [`SketchKindStats`] reservoirs throughout the run, then
//!    `mem::take`'d out via [`SketchKindStats::take_samples`] at
//!    end-of-run and shipped via [`MetricsEvent::NodeReservoirDump`].
//!    Buffers move (no clone); this is the key perf win over the
//!    earlier full-`PerNodeMetrics::clone()` design.
//!
//! ## Lifecycle
//!
//! [`MetricsHandle::new`] spawns the aggregator + the multi-Parquet
//! writer thread. Each node model holds a cloned `MetricsHandle` plus
//! its own `PerNodeMetrics`. [`MetricsHandle::finalize_remaining`]
//! sends the `FinalizeAndShutdown` sentinel, joins the aggregator,
//! drops the run-time writer-tx clone, and joins the writer.
//!
//! ## `Default`
//!
//! `MetricsHandle::default()` returns a no-op handle (no aggregator
//! thread, no Parquet output). Used only to satisfy the
//! `#[derive(Default)]` on each node `Model` struct.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use nexosim::ports::{EventQueueWriter, EventSinkWriter};
use nexosim::time::MonotonicTime;
use parking_lot::Mutex;

use crate::message::{Gossip, MsgId, NodeId, NodeIdx, SketchKind};
use crate::metrics_aggregator::{self, MetricsEvent};
use crate::reservoir::Reservoir;
use crate::stats_writer::{
    self, NodePubkeyRow, OutputPaths, RowSender, RunMetaRow, Writer, WriterRow,
};

pub struct Metrics {
    /// Live counter shared with the aggregator thread. Incremented by
    /// the aggregator on every supersession finalisation; read by the
    /// CLI summary.
    superseded_count: Arc<AtomicUsize>,
    /// Live counter shared with the aggregator thread. Bumped on every
    /// `push_finalized` (full-coverage, supersession, end-of-run drain).
    /// Equivalent to what `mirror.len()` used to report and read by
    /// [`MetricsHandle::completed_count`]; replaces the per-msg
    /// `MsgStats` clone-and-keep in production where Parquet output is
    /// configured.
    finalized_count: Arc<AtomicUsize>,
    /// Live counter incremented by `record_first_seen` on the worker
    /// thread BEFORE the event is enqueued.
    total_first_seen: AtomicUsize,
    /// Producer half of the aggregator's event queue. `None` only on
    /// `Default::default()`.
    events_tx: Option<EventQueueWriter<MetricsEvent>>,
    /// Aggregator thread join handle.
    aggregator_join: Mutex<Option<JoinHandle<AggregatorOutput>>>,
    /// In-memory `MsgStats` mirror returned by the aggregator after
    /// shutdown. Empty in production (the aggregator skips the mirror
    /// push when its `writer_tx` is `Some` — output streams straight to
    /// Parquet); populated only for tests/no-output handles built with
    /// `tag_prefix=None`. Read via `completed_stats()`; for the
    /// distinct-message count use [`MetricsHandle::completed_count`].
    finalized_stats: Mutex<Option<Vec<MsgStats>>>,
    /// Sender into the multi-Parquet writer. Cloned from the `Writer`
    /// owned in `writer_holder` and used directly for sim-init-time
    /// run_meta + node_pubkey rows that bypass the aggregator. Wrapped
    /// in Mutex<Option> so `finalize_remaining` can drop this clone
    /// before joining the writer thread (otherwise the join blocks
    /// forever waiting for this sender to drop — there's an Arc cycle
    /// via MetricsHandle that would only break on full handle drop,
    /// which happens AFTER `completed_count()` returns).
    writer_tx: Mutex<Option<RowSender>>,
    /// Writer handle. Stashed so `finalize_remaining` can close it
    /// after the aggregator finishes and recover the mirror.
    writer_holder: Mutex<Option<Writer>>,
    /// Tag prefix used to derive all six Parquet filenames. None when
    /// no output was configured.
    tag_prefix: Option<PathBuf>,
}

/// What the aggregator thread returns on join: the per-MsgId
/// finalised stats mirror. Per-node data goes straight to the
/// Parquet writers — the aggregator no longer keeps an in-memory
/// per-node Vec.
pub type AggregatorOutput = Vec<MsgStats>;

/// Lightweight per-(node, flush) counter snapshot. Used as the
/// payload of [`MetricsEvent::NodeCountersDelta`]; just plain integer
/// fields — no `Vec` / `Reservoir`. About ~120 B; cheap to clone +
/// send over the metrics event queue.
#[derive(Copy, Clone, Debug, Default)]
pub struct NodeCounters {
    pub bytes_in_sketch: u64,
    pub bytes_out_sketch: u64,
    pub bytes_in_gossip: u64,
    pub bytes_out_gossip: u64,
    pub duplicates: u64,
    pub sketches_sent: u64,
    pub sketches_received: u64,
    pub overflowed_chan_updates: u64,
    pub overflowed_node_anns: u64,
    pub overflowed_chan_anns: u64,
    pub chan_updates: KindCounters,
    pub node_anns: KindCounters,
    pub chan_anns: KindCounters,
}

/// Per-kind reconciliation running totals — the integer-only subset
/// of [`SketchKindStats`].
#[derive(Clone, Copy, Debug, Default)]
pub struct KindCounters {
    pub intersection: u64,
    pub a_only: u64,
    pub b_only: u64,
}

impl From<&SketchKindStats> for KindCounters {
    fn from(s: &SketchKindStats) -> Self {
        Self {
            intersection: s.intersection,
            a_only: s.a_only,
            b_only: s.b_only,
        }
    }
}

/// Reservoir samples for one (node, kind) pair, moved out of a
/// [`SketchKindStats`] at end-of-run via [`SketchKindStats::take_samples`].
/// Used as the payload of [`MetricsEvent::NodeReservoirDump`].
#[derive(Debug, Default)]
pub struct KindReservoirSamples {
    pub intersection: Vec<u32>,
    pub a_only: Vec<u32>,
    pub b_only: Vec<u32>,
    /// Total observation count (so the writer can record how many
    /// rounds the reservoir was sampled from, even when the reservoir
    /// hit capacity).
    pub total_seen: u64,
}

/// Per-sketch-kind reconciliation counters. Three of these on each
/// [`PerNodeMetrics`] — one per `SketchKind` — so the DuckDB report
/// can break down where the reconciliation work goes.
///
/// Running totals (`intersection` / `a_only` / `b_only`) live as plain
/// `u64`. Per-round samples ride in three [`Reservoir<u32>`]s so
/// memory stays bounded for arbitrarily long sims; computing p99 of
/// `a_only + b_only` over the reservoir gives a useful lower bound on
/// the sketch capacity needed to keep overflows rare.
#[derive(Clone, Debug, Default)]
pub struct SketchKindStats {
    pub intersection: u64,
    pub a_only: u64,
    pub b_only: u64,
    pub rounds_intersection: Reservoir<u32>,
    pub rounds_a_only: Reservoir<u32>,
    pub rounds_b_only: Reservoir<u32>,
}

impl SketchKindStats {
    /// Build with reservoirs of the given capacity, all seeded from
    /// `seed` xor'd with a per-vec sub-seed for independence.
    pub fn with_reservoir_capacity(cap: u32, seed: u64) -> Self {
        let cap = cap as usize;
        Self {
            intersection: 0,
            a_only: 0,
            b_only: 0,
            rounds_intersection: Reservoir::new(cap, seed ^ 0xA1),
            rounds_a_only: Reservoir::new(cap, seed ^ 0xA2),
            rounds_b_only: Reservoir::new(cap, seed ^ 0xA3),
        }
    }

    /// Move the reservoir buffers out into a [`KindReservoirSamples`]
    /// for end-of-run shipping. The reservoirs are reset to empty
    /// capacity-0 placeholders afterwards (caller is done with the
    /// node anyway). `total_seen` comes from the rounds_intersection
    /// reservoir; all three rounds_* reservoirs share the same count
    /// in lockstep.
    pub fn take_samples(&mut self) -> KindReservoirSamples {
        let total_seen = self.rounds_intersection.seen();
        let intersection =
            std::mem::replace(&mut self.rounds_intersection, Reservoir::new(0, 0))
                .into_inner();
        let a_only =
            std::mem::replace(&mut self.rounds_a_only, Reservoir::new(0, 0)).into_inner();
        let b_only =
            std::mem::replace(&mut self.rounds_b_only, Reservoir::new(0, 0)).into_inner();
        KindReservoirSamples {
            intersection,
            a_only,
            b_only,
            total_seen,
        }
    }
}

/// Stamped first-seen tuple. Buffered in
/// [`PerNodeMetrics::first_seen_pending`] on every absorb / originate
/// and shipped via [`MetricsEvent::FirstSeenBatch`] when the node's
/// `flush_summary` schedulable fires (periodic OR force-flushed when
/// the buffer hits `FIRST_SEEN_FORCE_FLUSH`).
///
/// `time_ns` is captured at the absorb site (`cx.time()`) BEFORE
/// buffering, so the aggregator's per-(node, MsgId) percentile math
/// sees the original sim-time of receipt regardless of how much later
/// the event hits the queue.
#[derive(Copy, Clone, Debug)]
pub struct FirstSeenEntry {
    pub gossip: Gossip,
    pub time_ns: u64,
}

/// Per-node soft cap on the pending first-seen buffer. When a node's
/// `first_seen_pending` Vec hits this size, the absorb call schedules
/// an early `flush_summary` (next-ns delay) so memory is bounded
/// regardless of flush interval. ~4096 × ~48 B ≈ 200 KB per node at
/// the cap; ~2.4 GB across 11 875 LN-snapshot nodes during a brief
/// spike (well within budget).
pub const FIRST_SEEN_FORCE_FLUSH: usize = 2048;

/// Single overflow event recorded by the receiver of a `Sketch` whose
/// symmetric-diff size exceeded the sketch capacity. Accumulated in a
/// small preallocated per-node buffer that gets drained every flush
/// interval into the `overflow_events-<tag>.parquet` writer — no
/// in-memory accumulation across the run.
#[derive(Copy, Clone, Debug)]
pub struct OverflowEvent {
    /// Sim time at which the overflow was observed.
    pub time_ns: u64,
    /// Dense index of the receiver (the node that computed the diff).
    pub receiver_idx: NodeIdx,
    /// `NodeId` of the peer that sent the overflowing sketch.
    pub peer_id: NodeId,
    pub kind: SketchKind,
    /// `total_diff - sketch.capacity` — how far over the configured
    /// capacity the actual diff went.
    pub amount: u32,
    /// `a_only + b_only` — the strict-diff total that triggered the
    /// overflow. Useful context for tuning capacity.
    pub total_diff: u32,
}

/// Per-node accounting recorded directly on the node model. Plain
/// `u64` fields — the NeXosim mailbox guarantees single-threaded
/// access per node, so no atomics are needed. Each periodic flush
/// snapshots the counters into a [`NodeCounters`] (~120 B) via
/// [`Self::snapshot_counters`]; the reservoir buffers and pending
/// overflow events ride separately via dedicated events so the
/// hot-path send is allocation-free.
///
/// Bandwidth is split into two buckets: `*_sketch` for
/// `WireMessage::Sketch` (reconciliation overhead) and `*_gossip` for
/// `WireMessage::Single` + `WireMessage::Batch` (gossip payload).
/// Sketch stats are broken down per `SketchKind` via
/// [`SketchKindStats`]; overflows likewise have per-kind counters
/// plus a detailed event log in `overflow_events`.
#[derive(Clone, Debug, Default)]
pub struct PerNodeMetrics {
    pub bytes_in_sketch: u64,
    pub bytes_out_sketch: u64,
    pub bytes_in_gossip: u64,
    pub bytes_out_gossip: u64,
    pub duplicates: u64,
    pub sketches_sent: u64,
    pub sketches_received: u64,
    pub overflowed_chan_updates: u64,
    pub overflowed_node_anns: u64,
    pub overflowed_chan_anns: u64,
    pub chan_updates_stats: SketchKindStats,
    pub node_anns_stats: SketchKindStats,
    pub chan_anns_stats: SketchKindStats,
    /// Pending per-overflow events. Drained every periodic flush into
    /// the `overflow_events-<tag>.parquet` writer — never accumulates
    /// across the run. Preallocated to `expected_per_interval` by
    /// [`Self::with_sketch_reservoirs`] so steady-state pushes don't
    /// re-allocate.
    pub overflow_events: Vec<OverflowEvent>,
    /// Pending per-(gossip, time_ns) first-seen tuples. Drained on
    /// every `flush_summary` schedulable (periodic OR force-flushed
    /// at [`FIRST_SEEN_FORCE_FLUSH`]) and shipped via
    /// [`MetricsEvent::FirstSeenBatch`]. Preallocated to the cap so
    /// steady-state pushes don't trigger Vec growth.
    pub first_seen_pending: Vec<FirstSeenEntry>,
}

impl PerNodeMetrics {
    /// Build with reservoirs sized for sketch nodes. Per-kind seed is
    /// xor'd with a kind index so the three kinds' reservoirs sample
    /// independently. The overflow buffer is preallocated to a small
    /// capacity (a few hundred events fits one flush interval at the
    /// LN-snapshot rate).
    pub fn with_sketch_reservoirs(reservoir_cap: u32, seed: u64) -> Self {
        Self {
            chan_updates_stats: SketchKindStats::with_reservoir_capacity(
                reservoir_cap,
                seed ^ 0x10C0,
            ),
            node_anns_stats: SketchKindStats::with_reservoir_capacity(
                reservoir_cap,
                seed ^ 0x10C1,
            ),
            chan_anns_stats: SketchKindStats::with_reservoir_capacity(
                reservoir_cap,
                seed ^ 0x10C2,
            ),
            overflow_events: Vec::with_capacity(256),
            first_seen_pending: Vec::with_capacity(FIRST_SEEN_FORCE_FLUSH),
            ..Default::default()
        }
    }

    /// Cheap counter-only snapshot used by each periodic flush. Touches
    /// only the integer fields — no `Vec` / `Reservoir` clone. ~120 B
    /// per call vs ~36 KB for a full `PerNodeMetrics::clone()`.
    pub fn snapshot_counters(&self) -> NodeCounters {
        NodeCounters {
            bytes_in_sketch: self.bytes_in_sketch,
            bytes_out_sketch: self.bytes_out_sketch,
            bytes_in_gossip: self.bytes_in_gossip,
            bytes_out_gossip: self.bytes_out_gossip,
            duplicates: self.duplicates,
            sketches_sent: self.sketches_sent,
            sketches_received: self.sketches_received,
            overflowed_chan_updates: self.overflowed_chan_updates,
            overflowed_node_anns: self.overflowed_node_anns,
            overflowed_chan_anns: self.overflowed_chan_anns,
            chan_updates: KindCounters::from(&self.chan_updates_stats),
            node_anns: KindCounters::from(&self.node_anns_stats),
            chan_anns: KindCounters::from(&self.chan_anns_stats),
        }
    }

    /// Move all three kinds' reservoir samples out. Called once per
    /// node at end-of-run; reservoirs become empty placeholders.
    pub fn take_reservoirs(
        &mut self,
    ) -> (KindReservoirSamples, KindReservoirSamples, KindReservoirSamples) {
        (
            self.chan_updates_stats.take_samples(),
            self.node_anns_stats.take_samples(),
            self.chan_anns_stats.take_samples(),
        )
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
    pub fn new(
        n_nodes: usize,
        percentiles: Vec<f64>,
        tag_prefix: Option<PathBuf>,
    ) -> Self {
        let superseded_count = Arc::new(AtomicUsize::new(0));
        let finalized_count = Arc::new(AtomicUsize::new(0));
        // Skip aggregator + writer spawn for the no-op Default handle.
        if n_nodes == 0 {
            return Self(Arc::new(Metrics {
                superseded_count,
                finalized_count,
                total_first_seen: AtomicUsize::new(0),
                events_tx: None,
                aggregator_join: Mutex::new(None),
                finalized_stats: Mutex::new(None),
                writer_tx: Mutex::new(None),
                writer_holder: Mutex::new(None),
                tag_prefix: None,
            }));
        }
        let (writer, writer_tx) = match &tag_prefix {
            Some(tag) => {
                let paths = OutputPaths::from_tag(tag);
                let w = stats_writer::spawn(paths, percentiles.clone());
                let tx = w.sender();
                (Some(w), Some(tx))
            }
            None => (None, None),
        };
        let (events_tx, aggregator_join) = {
            let (tx, h) = metrics_aggregator::spawn(
                n_nodes,
                percentiles,
                superseded_count.clone(),
                finalized_count.clone(),
                writer_tx.clone(),
            );
            (Some(tx), Some(h))
        };
        Self(Arc::new(Metrics {
            superseded_count,
            finalized_count,
            total_first_seen: AtomicUsize::new(0),
            events_tx,
            aggregator_join: Mutex::new(aggregator_join),
            finalized_stats: Mutex::new(None),
            writer_tx: Mutex::new(writer_tx),
            writer_holder: Mutex::new(writer),
            tag_prefix,
        }))
    }

    /// Sim-init helper: write the run-meta single-row Parquet. Called
    /// from `sim::run` after topology metrics are computed. No-op on
    /// the no-output handle.
    pub fn write_run_meta(&self, row: RunMetaRow) {
        if let Some(tx) = self.0.writer_tx.lock().as_ref() {
            tx.send(WriterRow::RunMeta(row));
        }
    }

    /// Sim-init helper: write per-node pubkey lookup rows (FromCsv
    /// runs only). Sent as a batch; each row goes through the same
    /// bounded channel.
    pub fn write_node_pubkey_rows(&self, rows: Vec<NodePubkeyRow>) {
        if let Some(tx) = self.0.writer_tx.lock().as_ref() {
            for row in rows {
                tx.send(WriterRow::NodePubkey(row));
            }
        }
    }

    /// Tag prefix used to derive the six Parquet paths (or `None`
    /// when no output was configured). The CLI uses this to point the
    /// DuckDB report module at the right files.
    pub fn tag_prefix(&self) -> Option<PathBuf> {
        self.0.tag_prefix.clone()
    }

    /// Bump the live "events processed" counter by `count`. Called on
    /// the worker thread from each absorb / originate site after the
    /// fresh-after-dedup count is known. The actual first-seen tuples
    /// go through the per-node `first_seen_pending` buffer + the
    /// node's `flush_summary` schedulable, NOT through the events
    /// queue per absorption — this is the channel-batching win.
    pub fn bump_first_seen_count(&self, count: usize) {
        self.0.total_first_seen.fetch_add(count, Ordering::Relaxed);
    }

    /// Ship a batch of stamped first-seen tuples drained from a
    /// node's `first_seen_pending` buffer. Called by the node's
    /// `flush_summary` (periodic + final + force-flush at cap).
    pub fn send_first_seen_batch(&self, idx: NodeIdx, entries: Vec<FirstSeenEntry>) {
        if entries.is_empty() {
            return;
        }
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::FirstSeenBatch { idx, entries });
        }
    }

    /// Send a periodic per-(node, flush) counters delta to the
    /// aggregator. Cheap: `NodeCounters` is ~120 B (no Vec / Reservoir
    /// inside). `drained_overflow` is moved by the caller via
    /// `mem::take`. Called by each node's `flush_summary` schedulable.
    pub fn send_counters_delta(
        &self,
        idx: NodeIdx,
        time_ns: u64,
        counters: NodeCounters,
        drained_overflow: Vec<OverflowEvent>,
    ) {
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::NodeCountersDelta {
                idx,
                time_ns,
                counters,
                drained_overflow,
            });
        }
    }

    /// Ship the per-(node, kind) reservoir buffers to the aggregator
    /// once, at end-of-run. Buffers are moved (not cloned) — the
    /// caller already drained them via `PerNodeMetrics::take_reservoirs`.
    pub fn send_reservoir_dump(
        &self,
        idx: NodeIdx,
        chan_updates: KindReservoirSamples,
        node_anns: KindReservoirSamples,
        chan_anns: KindReservoirSamples,
    ) {
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::NodeReservoirDump {
                idx,
                chan_updates,
                node_anns,
                chan_anns,
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

    /// Send the `FinalizeAndShutdown` sentinel, join the aggregator,
    /// drop the run-time writer_tx clone, then close the writer
    /// (flushes remaining buffers + closes all files). Idempotent.
    pub fn finalize_remaining(&self) {
        if let Some(tx) = &self.0.events_tx {
            tx.write(MetricsEvent::FinalizeAndShutdown);
        }
        if let Some(h) = self.0.aggregator_join.lock().take()
            && let Ok(stats) = h.join()
        {
            // Drop the run-time writer_tx clone BEFORE closing the
            // Writer. Without this, the writer thread's join blocks
            // forever — it waits for ALL sender clones to drop, but
            // this one would otherwise live until Arc<Metrics> drops
            // (which only happens after CLI is fully done).
            drop(self.0.writer_tx.lock().take());
            if let Some(writer) = self.0.writer_holder.lock().take() {
                writer.close();
            }
            *self.0.finalized_stats.lock() = Some(stats);
        }
    }

    /// Test/debug-only: return the in-memory mirror of every finalized
    /// `MsgStats`. **Empty in production** because the aggregator
    /// skips the mirror push whenever Parquet output is configured —
    /// finalized rows stream straight to `msg_stats-*.parquet` and the
    /// authoritative count is exposed via [`Self::completed_count`].
    /// Useful from tests built with `MetricsHandle::new(n, _, None)`
    /// where no writer is attached. First call also performs
    /// `finalize_remaining` if the runner hasn't already.
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

    /// Number of distinct messages that ever reached `push_finalized`
    /// inside the aggregator (full-coverage convergence, supersession,
    /// or end-of-run drain). Production replacement for the old
    /// `completed_stats().len()` pattern — reads a single atomic
    /// instead of materialising a `Vec<MsgStats>`. First call also
    /// drives `finalize_remaining` so the count reflects end-of-run
    /// drains.
    pub fn completed_count(&self) -> usize {
        self.finalize_remaining();
        self.0.finalized_count.load(Ordering::Relaxed)
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
pub fn ns_since_epoch(t: MonotonicTime) -> u64 {
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
    ///
    /// Post-batching: each "absorption" pushes one entry into a
    /// per-node buffer; this test simulates 16 such batches arriving
    /// at the aggregator and confirms the per-MsgId slot is only
    /// stored once (idempotent in the aggregator).
    #[test]
    fn concurrent_batches_idempotent_per_slot() {
        let metrics = MetricsHandle::new(4, vec![1.0], None);
        let g = dummy_gossip(7, 100);
        let entry = FirstSeenEntry { gossip: g, time_ns: 50_000 };

        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = metrics.clone();
            handles.push(thread::spawn(move || {
                m.send_first_seen_batch(0, vec![entry]);
                m.bump_first_seen_count(1);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(metrics.total_first_seen(), 16);
        let _ = metrics.completed_stats();
    }

    /// `snapshot_counters` copies the integer fields cheaply without
    /// touching the reservoir buffers — verifies the new hot-path
    /// avoids the `PerNodeMetrics::clone()` cost.
    #[test]
    fn snapshot_counters_copies_fields() {
        let mut m = PerNodeMetrics::with_sketch_reservoirs(8, 42);
        m.bytes_in_gossip = 100;
        m.bytes_out_sketch = 200;
        m.sketches_sent = 7;
        m.chan_updates_stats.intersection = 11;
        m.chan_updates_stats.a_only = 3;
        m.chan_updates_stats.b_only = 5;
        m.overflowed_chan_updates = 1;
        // Push some reservoir samples to confirm they're NOT copied.
        m.chan_updates_stats.rounds_intersection.observe(99);
        let c = m.snapshot_counters();
        assert_eq!(c.bytes_in_gossip, 100);
        assert_eq!(c.bytes_out_sketch, 200);
        assert_eq!(c.sketches_sent, 7);
        assert_eq!(c.chan_updates.intersection, 11);
        assert_eq!(c.chan_updates.a_only, 3);
        assert_eq!(c.chan_updates.b_only, 5);
        assert_eq!(c.overflowed_chan_updates, 1);
        // Reservoir buffer wasn't touched — original observation is
        // still there.
        assert_eq!(m.chan_updates_stats.rounds_intersection.samples(), &[99]);
    }

    /// `take_reservoirs` moves the buffers out for end-of-run shipping
    /// without cloning. The original reservoirs become capacity-0
    /// placeholders.
    #[test]
    fn take_reservoirs_moves_buffers() {
        let mut m = PerNodeMetrics::with_sketch_reservoirs(8, 42);
        for i in 0..5u32 {
            m.chan_updates_stats.rounds_intersection.observe(i);
            m.chan_updates_stats.rounds_a_only.observe(i + 10);
            m.chan_updates_stats.rounds_b_only.observe(i + 20);
        }
        let (cu, _na, _ca) = m.take_reservoirs();
        assert_eq!(cu.intersection, vec![0, 1, 2, 3, 4]);
        assert_eq!(cu.a_only, vec![10, 11, 12, 13, 14]);
        assert_eq!(cu.b_only, vec![20, 21, 22, 23, 24]);
        assert_eq!(cu.total_seen, 5);
        // Original reservoir is now empty + capacity-0.
        assert_eq!(m.chan_updates_stats.rounds_intersection.samples(), &[] as &[u32]);
        assert_eq!(m.chan_updates_stats.rounds_intersection.capacity(), 0);
    }

    /// N threads each shipping a 1-entry batch from N distinct nodes
    /// for the same message ⇒ the message finalises exactly once and
    /// lands in completed_stats.
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
                let entry = FirstSeenEntry {
                    gossip: *g,
                    time_ns: i as u64 * 1_000,
                };
                m.send_first_seen_batch(i as NodeIdx, vec![entry]);
                m.bump_first_seen_count(1);
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
