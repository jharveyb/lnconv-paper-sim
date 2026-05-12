//! Set-reconciliation node — minisketch-style.
//!
//! Each node never floods gossip on `recv`. Instead, every
//! `(node, peer)` pair has its own periodic ticker that fires once
//! per `stagger` interval at a deterministic per-peer offset within
//! `(0, peer_offset_max]`. The tick handler builds a triple of
//! [`Sketch`]es — one per `SketchKind` — at per-kind capacities and
//! sends them to that one peer.
//!
//! When peer B receives a Sketch from A, it reads both A's and its
//! own [`crate::state::NodeState`] for the kind in question (via
//! the `peer_states` Arc registry) and computes the symmetric diff.
//! If the diff fits inside the sketch capacity, B replies with a
//! `WireMessage::Batch` containing the gossips A is **strictly
//! missing** (i.e. items B has under a newer or absent-on-A
//! identity). Stale versions are never sent.
//!
//! Receiving a `WireMessage::Single` or `WireMessage::Batch` (from
//! the reply path) updates the local state under per-kind dedup,
//! exactly like the other protocols. Sketches don't fan-out, so
//! `recv` never re-broadcasts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{
    Gossip, GossipBatch, GossipKind, MsgId, NodeId, NodeIdx, Sketch, SketchKind, WireMessage,
};
use crate::metrics::{MetricsHandle, PerNodeMetrics};
use crate::state::{SharedNodeState, WhichSide, compute_diff, originate_stamp};

#[derive(Default, Serialize, Deserialize)]
pub struct SketchNode {
    pub id: NodeId,
    pub idx: NodeIdx,
    pub outputs: Vec<Output<WireMessage>>,
    pub peer_ids: Vec<NodeId>,
    pub peer_id_to_local: HashMap<NodeId, usize>,
    /// Per-peer state Arcs, aligned with `outputs` / `peer_ids`.
    /// Used when a peer's Sketch arrives and we need to read that
    /// peer's gossip state to compute the diff.
    #[serde(skip)]
    peer_states: Vec<SharedNodeState>,
    stagger: Duration,
    /// `per_peer_offsets[i]` = offset of local-peer i's first
    /// reconciliation within each stagger window. Sampled
    /// deterministically at sim init.
    per_peer_offsets: Vec<Duration>,
    /// Per-kind capacities (chan_updates, node_anns, chan_anns).
    /// Each tick fires three sketches with these caps.
    cap_chan_updates: u32,
    cap_node_anns: u32,
    cap_chan_anns: u32,
    next_sketch_id: u32,
    /// How long until end of run; used to schedule the one-shot
    /// `flush_summary` event in `arm_per_peer_tickers`.
    run_duration: Duration,
    #[serde(skip)]
    state: SharedNodeState,
    /// Plain-u64 per-node counters. Single-threaded mailbox access ⇒
    /// no atomic needed; flushed to the aggregator at end-of-run.
    #[serde(skip)]
    metrics_local: PerNodeMetrics,
    #[serde(skip)]
    metrics: MetricsHandle,
}

impl SketchNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        stagger: Duration,
        cap_chan_updates: u32,
        cap_node_anns: u32,
        cap_chan_anns: u32,
        reservoir_cap: u32,
        reservoir_seed: u64,
        run_duration: Duration,
        state: SharedNodeState,
        peer_states: Vec<SharedNodeState>,
        metrics: MetricsHandle,
    ) -> Self {
        Self {
            id,
            idx,
            outputs: Vec::new(),
            peer_ids: Vec::new(),
            peer_id_to_local: HashMap::new(),
            peer_states,
            stagger,
            per_peer_offsets: Vec::new(),
            cap_chan_updates,
            cap_node_anns,
            cap_chan_anns,
            next_sketch_id: 0,
            run_duration,
            state,
            metrics_local: PerNodeMetrics::with_sketch_reservoirs(reservoir_cap, reservoir_seed),
            metrics,
        }
    }

    pub fn add_peer(&mut self, peer_id: NodeId, out: Output<WireMessage>) {
        super::push_peer(
            &mut self.outputs,
            &mut self.peer_ids,
            &mut self.peer_id_to_local,
            peer_id,
            out,
        );
    }

    /// Set the per-peer offsets after `add_peer` has been called for
    /// every peer. The Vec must have the same length as `outputs`.
    pub fn set_per_peer_offsets(&mut self, offsets: Vec<Duration>) {
        assert_eq!(
            offsets.len(),
            self.outputs.len(),
            "per-peer offsets length mismatch ({} != {})",
            offsets.len(),
            self.outputs.len(),
        );
        self.per_peer_offsets = offsets;
    }

    /// Expose the stagger interval so sim init can sample per-peer
    /// offsets uniformly within `(0, stagger]`.
    pub fn stagger_for_offset_sample(&self) -> Duration {
        self.stagger
    }
}

#[Model]
impl SketchNode {
    /// Arm one periodic ticker per peer at its individual offset.
    /// Each tick fires three sketches (one per kind) to that peer.
    /// Also schedule a one-shot `flush_summary` event at the end of
    /// the run so the per-node counters get pushed to the aggregator.
    #[nexosim(init)]
    async fn arm_per_peer_tickers(&mut self, cx: &Context<Self>) {
        for (i, &offset) in self.per_peer_offsets.iter().enumerate() {
            cx.schedule_periodic_event(offset, self.stagger, schedulable!(Self::tick_for_peer), i)
                .expect("schedule per-peer sketch tick");
        }
        cx.schedule_event(self.run_duration, schedulable!(Self::flush_summary), ())
            .expect("schedule sketch flush_summary");
    }

    /// One-shot end-of-run handler: send the accumulated per-node
    /// counters to the aggregator. Idempotent in spirit — the runner
    /// only schedules it once.
    #[nexosim(schedulable)]
    async fn flush_summary(&mut self, _: ()) {
        self.metrics.send_node_summary(self.idx, &self.metrics_local);
    }

    /// Inbound port. Three message kinds:
    ///
    /// * `Single` / `Batch` — Gossips arriving as a sketch reply
    ///   (or from a non-sketch peer in a mixed population). Run
    ///   per-kind dedup against our state; record duplicates.
    ///   Sketch nodes never re-broadcast — propagation happens
    ///   exclusively via reconciliation.
    /// * `Sketch` — schedule the diff/reply via a schedulable
    ///   helper because `recv` is sync and can't `.await` on Outputs.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        let wire_size = wire.wire_size();
        match wire {
            WireMessage::Single(g) => {
                self.metrics_local.bytes_in_gossip += wire_size;
                self.absorb_one(g, cx);
            }
            WireMessage::Batch(batch) => {
                self.metrics_local.bytes_in_gossip += wire_size;
                self.absorb_chan_updates(&batch.chan_updates, cx);
                self.absorb_node_anns(&batch.node_anns, cx);
                self.absorb_chan_anns(&batch.chan_anns, cx);
            }
            WireMessage::Sketch(sketch) => {
                self.metrics_local.bytes_in_sketch += wire_size;
                cx.schedule_event(
                    // 1 ms delay should give some breathing room.
                    Duration::from_nanos(1000000),
                    schedulable!(Self::handle_sketch),
                    sketch,
                )
                .expect("schedule handle_sketch");
            }
        }
    }

    /// Originated messages: stamp the timestamp via the per-kind
    /// state lock, then *only* mark our own state. No fan-out — the
    /// gossip will propagate to peers via the next round of sketches.
    pub fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        if !originate_stamp(&self.state, self.id, &mut msg, cx.time()) {
            return;
        }
        self.metrics.record_first_seen(self.idx, &msg, cx.time());
    }

    /// Per-peer ticker handler. Builds three sketches (one per kind)
    /// and sends them to one peer in sequence.
    #[nexosim(schedulable)]
    async fn tick_for_peer(&mut self, peer_local: usize) {
        for (kind, capacity) in [
            (SketchKind::ChanUpdates, self.cap_chan_updates),
            (SketchKind::NodeAnns, self.cap_node_anns),
            (SketchKind::ChanAnns, self.cap_chan_anns),
        ] {
            self.next_sketch_id = self.next_sketch_id.wrapping_add(1);
            let id = self.next_sketch_id as MsgId;
            let sketch = Sketch {
                id,
                from: self.id,
                kind,
                capacity,
                size_bytes: ((capacity as usize * 8).min(u16::MAX as usize)) as u16,
            };
            self.metrics_local.sketches_sent += 1;
            self.metrics_local.bytes_out_sketch += sketch.size_bytes as u64;
            if let Some(out) = self.outputs.get_mut(peer_local) {
                out.send(WireMessage::Sketch(sketch)).await;
            }
        }
    }

    /// Compute symmetric diff with the sender; reply (if within
    /// capacity) with the gossips the sender is **strictly missing**
    /// — stale-side entries are excluded so we never propagate
    /// superseded versions. Capacity overflow uses the **strict**
    /// counts (which include both stale and newer sides), matching
    /// how a real minisketch decode would fail.
    #[nexosim(schedulable)]
    async fn handle_sketch(&mut self, sketch: Sketch, _cx: &Context<Self>) {
        let local = match self.peer_id_to_local.get(&sketch.from).copied() {
            Some(i) => i,
            None => {
                return;
            }
        };
        let peer_state = &self.peer_states[local];
        let diff = compute_diff(peer_state, &self.state, sketch.kind, WhichSide::B);
        let total_diff = diff.a_only_count + diff.b_only_count;
        let overflow = total_diff > sketch.capacity as usize;
        self.metrics_local.sketches_received += 1;
        // Per-kind reconciliation stats.
        let kind_stats = match sketch.kind {
            SketchKind::ChanUpdates => &mut self.metrics_local.chan_updates_stats,
            SketchKind::NodeAnns => &mut self.metrics_local.node_anns_stats,
            SketchKind::ChanAnns => &mut self.metrics_local.chan_anns_stats,
        };
        kind_stats.intersection += diff.intersection as u64;
        kind_stats.a_only += diff.a_only_count as u64;
        kind_stats.b_only += diff.b_only_count as u64;
        kind_stats
            .rounds_intersection
            .observe(diff.intersection.min(u32::MAX as usize) as u32);
        kind_stats
            .rounds_a_only
            .observe(diff.a_only_count.min(u32::MAX as usize) as u32);
        kind_stats
            .rounds_b_only
            .observe(diff.b_only_count.min(u32::MAX as usize) as u32);
        if overflow {
            // Per-kind overflow count.
            match sketch.kind {
                SketchKind::ChanUpdates => self.metrics_local.overflowed_chan_updates += 1,
                SketchKind::NodeAnns => self.metrics_local.overflowed_node_anns += 1,
                SketchKind::ChanAnns => self.metrics_local.overflowed_chan_anns += 1,
            }
            let amount = (total_diff - sketch.capacity as usize).min(u32::MAX as usize) as u32;
            let total_diff_u32 = total_diff.min(u32::MAX as usize) as u32;
            let time_ns = _cx
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_nanos() as u64;
            self.metrics_local.overflow_events.push(crate::metrics::OverflowEvent {
                time_ns,
                receiver_idx: self.idx,
                peer_id: sketch.from,
                kind: sketch.kind,
                amount,
                total_diff: total_diff_u32,
            });
            return;
        }
        // Reply with items the sender is missing AND that are
        // strictly newer on our side. From compute_diff(peer, self):
        // a_newer = peer's strictly newer; b_newer = self's strictly
        // newer. We send b_newer back.
        if diff.b_newer.is_empty() {
            return;
        }
        let bytes: u64 = diff.b_newer.iter().map(|g| g.size_bytes as u64).sum();
        self.metrics_local.bytes_out_gossip += bytes;
        let batch = Arc::new(GossipBatch::from_mixed_for_kind(diff.b_newer, sketch.kind.to_gossip()));
        if let Some(out) = self.outputs.get_mut(local) {
            out.send(WireMessage::Batch(batch)).await;
        }
    }
}

impl SketchNode {
    fn absorb_one(&mut self, g: Gossip, cx: &Context<Self>) {
        match g.kind {
            GossipKind::ChannelUpdate => self.absorb_chan_updates(std::slice::from_ref(&g), cx),
            GossipKind::NodeAnnouncement => self.absorb_node_anns(std::slice::from_ref(&g), cx),
            GossipKind::ChannelAnnouncement => self.absorb_chan_anns(std::slice::from_ref(&g), cx),
        }
    }

    fn absorb_chan_updates(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        // Phase A: write lock held only for dedup + insert. Record
        // duplicates inline (lock-free atomic); collect fresh indices
        // so the scc bucket-lock storm of record_first_seen happens
        // AFTER the write lock is released.
        let mut fresh: Vec<usize> = Vec::with_capacity(gs.len());
        {
            let mut m = self.state.chan_updates.write();
            for (i, g) in gs.iter().enumerate() {
                let scid = g.scid.expect("ChannelUpdate must carry scid");
                let key = crate::state::pack_cu_key(scid, g.direction);
                let supersedes = m
                    .get(&key)
                    .map(|(stored, _)| g.timestamp > *stored)
                    .unwrap_or(true);
                if supersedes {
                    m.insert(key, (g.timestamp, g.size_bytes));
                    fresh.push(i);
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        // Phase B: outside the lock.
        let now = cx.time();
        for &i in &fresh {
            self.metrics.record_first_seen(self.idx, &gs[i], now);
        }
    }

    fn absorb_node_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let mut fresh: Vec<usize> = Vec::with_capacity(gs.len());
        {
            let mut m = self.state.node_anns.write();
            for (i, g) in gs.iter().enumerate() {
                let origin = g.origin.expect("NodeAnnouncement must carry origin");
                let supersedes = m
                    .get(&origin)
                    .map(|(stored, _)| g.timestamp > *stored)
                    .unwrap_or(true);
                if supersedes {
                    m.insert(origin, (g.timestamp, g.size_bytes));
                    fresh.push(i);
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        let now = cx.time();
        for &i in &fresh {
            self.metrics.record_first_seen(self.idx, &gs[i], now);
        }
    }

    fn absorb_chan_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let mut fresh: Vec<usize> = Vec::with_capacity(gs.len());
        {
            let mut m = self.state.chan_anns.write();
            for (i, g) in gs.iter().enumerate() {
                let scid = g.scid.expect("ChannelAnnouncement must carry scid");
                if m.insert(scid, g.size_bytes).is_none() {
                    fresh.push(i);
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        let now = cx.time();
        for &i in &fresh {
            self.metrics.record_first_seen(self.idx, &gs[i], now);
        }
    }
}
