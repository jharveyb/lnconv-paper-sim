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
use std::slice::from_ref;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{
    Gossip, GossipBatch, GossipKind, InventoryMsg, MsgId, NodeId, NodeIdx, Sketch, SketchKind,
    WireMessage,
};
use crate::metrics::{
    FIRST_SEEN_FORCE_FLUSH, FirstSeenEntry, MetricsHandle, PerNodeMetrics, ns_since_epoch,
};
use crate::state::{
    SharedNodeState, WhichSide, compute_diff, originate_stamp, synth_chan_ann,
    synth_chan_update, synth_node_ann, unpack_cu_key,
};

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
    /// When `true`, `originate` ALSO broadcasts the freshly-stamped
    /// gossip as `WireMessage::Single` to every connected peer
    /// (sketch + flooding hybrid). Defaults to `false`.
    flood_on_originate: bool,
    /// When `true`, on receiving a sketch this node computes the
    /// diff with `WhichSide::Both` and additionally sends a
    /// `WireMessage::Inventory` asking the sketch's sender for items
    /// present on its side that this node is missing. Defaults to
    /// `false`.
    full_reconciliation: bool,
    next_sketch_id: u32,
    /// How long until end of run; used to schedule the final
    /// `flush_summary` event.
    run_duration: Duration,
    /// Periodic per-node metrics flush interval. Each firing drains
    /// pending overflow events + sends a counter snapshot to the
    /// aggregator (Parquet `node_counters` row + per-event overflow
    /// rows). Zero disables periodic flushing.
    flush_interval: Duration,
    /// Deterministic per-node phase offset for the first periodic
    /// flush. Spreads load across the interval so 11 875 nodes don't
    /// all flush at the same sim instant.
    flush_phase: Duration,
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
        flood_on_originate: bool,
        full_reconciliation: bool,
        reservoir_cap: u32,
        reservoir_seed: u64,
        run_duration: Duration,
        flush_interval: Duration,
        flush_phase: Duration,
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
            flood_on_originate,
            full_reconciliation,
            next_sketch_id: 0,
            run_duration,
            flush_interval,
            flush_phase,
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
    /// Also schedules:
    /// * a **periodic** `flush_summary` every `flush_interval` (starting
    ///   at the per-node `flush_phase`) so memory stays bounded and the
    ///   `node_counters.parquet` accumulates a time series;
    /// * a **one-shot** `flush_summary` at `run_duration` so the final
    ///   snapshot is guaranteed regardless of how the periodic
    ///   schedule lines up with the deadline.
    #[nexosim(init)]
    async fn arm_per_peer_tickers(&mut self, cx: &Context<Self>) {
        for (i, &offset) in self.per_peer_offsets.iter().enumerate() {
            cx.schedule_periodic_event(offset, self.stagger, schedulable!(Self::tick_for_peer), i)
                .expect("schedule per-peer sketch tick");
        }
        if self.flush_interval > Duration::ZERO {
            cx.schedule_periodic_event(
                self.flush_phase,
                self.flush_interval,
                schedulable!(Self::flush_summary),
                (),
            )
            .expect("schedule periodic sketch flush_summary");
        }
        cx.schedule_event(self.run_duration, schedulable!(Self::flush_summary), ())
            .expect("schedule sketch final flush_summary");
    }

    /// Periodic + final flush. Drains:
    ///   - the first-seen pending buffer (moved, not cloned)
    ///   - the pending overflow events (moved)
    ///   - a lightweight `NodeCounters` snapshot (~120 B copy)
    /// At end-of-run (`run_duration`), additionally ships the
    /// reservoir buffers once via `NodeReservoirDump`.
    #[nexosim(schedulable)]
    async fn flush_summary(&mut self, _: (), cx: &Context<Self>) {
        let time_ns = ns_since_epoch(cx.time());

        // Drain first-seen tuples — the busiest channel in the system.
        // Moving the Vec means zero copies; reset to a fresh
        // pre-allocated buffer for the next interval.
        let first_seen = std::mem::take(&mut self.metrics_local.first_seen_pending);
        self.metrics_local.first_seen_pending = Vec::with_capacity(FIRST_SEEN_FORCE_FLUSH);
        self.metrics.send_first_seen_batch(self.idx, first_seen);

        let counters = self.metrics_local.snapshot_counters();
        let drained = std::mem::take(&mut self.metrics_local.overflow_events);
        self.metrics_local.overflow_events = Vec::with_capacity(256);
        self.metrics.send_counters_delta(self.idx, time_ns, counters, drained);

        if time_ns >= self.run_duration.as_nanos() as u64 {
            let (cu, na, ca) = self.metrics_local.take_reservoirs();
            self.metrics.send_reservoir_dump(self.idx, cu, na, ca);
        }
    }

    /// Inbound port. Four message kinds:
    ///
    /// * `Single` / `Batch` — Gossips arriving as a sketch reply, an
    ///   inventory response, a flood-on-originate broadcast from a
    ///   peer sketch node, or from a non-sketch peer in a mixed
    ///   population. Run per-kind dedup against our state; record
    ///   duplicates. Sketch nodes never re-broadcast — propagation
    ///   happens via reconciliation (and optionally flood-on-originate).
    /// * `Sketch` — schedule the diff/reply via a schedulable
    ///   helper because `recv` is sync and can't `.await` on Outputs.
    /// * `Inventory` — schedule a handler that synthesises the
    ///   requested Gossips and replies with a `Batch`.
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
                    Duration::from_millis(1),
                    schedulable!(Self::handle_sketch),
                    sketch,
                )
                .expect("schedule handle_sketch");
            }
            WireMessage::Inventory(inv) => {
                self.metrics_local.bytes_in_inventory += wire_size;
                self.metrics_local.inventories_received += 1;
                cx.schedule_event(
                    Duration::from_millis(1),
                    schedulable!(Self::handle_inventory),
                    inv,
                )
                .expect("schedule handle_inventory");
            }
        }
    }

    /// Originated messages: stamp the timestamp via the per-kind
    /// state lock, then mark our own state. If `flood_on_originate`
    /// is set, ALSO broadcast to every peer as `WireMessage::Single`
    /// (sketch + flooding hybrid). Otherwise, no fan-out — the gossip
    /// propagates only via the next round of sketches.
    pub async fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        if !originate_stamp(&self.state, self.id, &mut msg, cx.time()) {
            return;
        }
        self.metrics_local.first_seen_pending.push(FirstSeenEntry {
            gossip: msg,
            time_ns: ns_since_epoch(cx.time()),
        });
        self.metrics.bump_first_seen_count(1);

        if self.flood_on_originate && !self.outputs.is_empty() {
            // Gossip is Copy, so cloning the wire message is the only
            // real cost per peer. Receivers absorb via the existing
            // WireMessage::Single → absorb_one path.
            let wire = WireMessage::Single(msg);
            let per_peer = wire.wire_size();
            for out in self.outputs.iter_mut() {
                out.send(wire.clone()).await;
            }
            self.metrics_local.bytes_out_gossip = self
                .metrics_local
                .bytes_out_gossip
                .saturating_add(per_peer.saturating_mul(self.outputs.len() as u64));
        }
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
            self.next_sketch_id += 1;
            let id = self.next_sketch_id as MsgId;
            let sketch = Sketch {
                id,
                from: self.id,
                kind,
                capacity,
                // Caller must limit capacity to 8192
                size_bytes: (capacity as usize * 8) as u16,
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
    ///
    /// If `full_reconciliation` is set, this ALSO sends an
    /// [`InventoryMsg`] asking the sender for items present on the
    /// sender's side that we're missing (the `a_newer` half of the
    /// diff). The sender responds with a `WireMessage::Batch`.
    #[nexosim(schedulable)]
    async fn handle_sketch(&mut self, sketch: Sketch, _cx: &Context<Self>) {
        let local = match self.peer_id_to_local.get(&sketch.from).copied() {
            Some(i) => i,
            None => {
                return;
            }
        };
        let peer_state = &self.peer_states[local];
        // With full_reconciliation we materialise BOTH sides so we
        // can send b_newer as the reply AND build an Inventory from
        // a_newer's keys. Lock order is min-NodeIdx-first inside
        // compute_diff (CLAUDE.md invariant 24) — no caller-side
        // change needed.
        let which = if self.full_reconciliation {
            WhichSide::Both
        } else {
            WhichSide::B
        };
        let diff = compute_diff(peer_state, &self.state, sketch.kind, which);
        let difference = diff.difference;
        let overflow = difference > sketch.capacity as usize;
        self.metrics_local.sketches_received += 1;
        // Per-kind reconciliation stats.
        let kind_stats = match sketch.kind {
            SketchKind::ChanUpdates => &mut self.metrics_local.chan_updates_stats,
            SketchKind::NodeAnns => &mut self.metrics_local.node_anns_stats,
            SketchKind::ChanAnns => &mut self.metrics_local.chan_anns_stats,
        };
        kind_stats.intersection += diff.intersection as u64;
        kind_stats.difference += diff.difference as u64;
        kind_stats.a_only += diff.a_only_count as u64;
        kind_stats.b_only += diff.b_only_count as u64;
        kind_stats
            .rounds_intersection
            .observe(diff.intersection as u32);
        kind_stats
            .rounds_difference
            .observe(diff.difference as u32);
        kind_stats
            .rounds_a_only
            .observe(diff.a_only_count as u32);
        kind_stats
            .rounds_b_only
            .observe(diff.b_only_count as u32);
        if overflow {
            // Per-kind overflow count.
            match sketch.kind {
                SketchKind::ChanUpdates => self.metrics_local.overflowed_chan_updates += 1,
                SketchKind::NodeAnns => self.metrics_local.overflowed_node_anns += 1,
                SketchKind::ChanAnns => self.metrics_local.overflowed_chan_anns += 1,
            }
            let amount = (difference - sketch.capacity as usize) as u32;
            let difference_u32 = difference as u32;
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
                difference: difference_u32,
            });
            return;
        }
        // Reply with items the sender is missing AND that are
        // strictly newer on our side. From compute_diff(peer, self):
        // a_newer = peer's strictly newer; b_newer = self's strictly
        // newer. We send b_newer back.
        if !diff.b_newer.is_empty() {
            let bytes: u64 = diff.b_newer.iter().map(|g| g.size_bytes as u64).sum();
            self.metrics_local.bytes_out_gossip += bytes;
            let batch = Arc::new(GossipBatch::from_mixed_for_kind(
                diff.b_newer,
                sketch.kind.to_gossip(),
            ));
            if let Some(out) = self.outputs.get_mut(local) {
                out.send(WireMessage::Batch(batch)).await;
            }
        }
        // Full-reconciliation follow-up: ask the peer for items it
        // has that we don't. a_newer is only populated when
        // WhichSide::Both was requested.
        if self.full_reconciliation && !diff.a_newer.is_empty() {
            let keys: Vec<u64> = diff.a_newer.iter().map(|g| g.state_key()).collect();
            let inv = InventoryMsg::new(self.id, sketch.kind.to_gossip(), keys);
            let key_count = inv.keys.len() as u64;
            self.metrics_local.bytes_out_inventory += inv.size_bytes;
            self.metrics_local.inventories_sent += 1;
            self.metrics_local.inv_keys_sent_sum =
                self.metrics_local.inv_keys_sent_sum.saturating_add(key_count);
            // 0 means "no inventory sent yet" — since we early-return
            // on empty diff above, key_count >= 1 whenever this runs.
            if self.metrics_local.inv_keys_sent_min == 0
                || key_count < self.metrics_local.inv_keys_sent_min
            {
                self.metrics_local.inv_keys_sent_min = key_count;
            }
            if key_count > self.metrics_local.inv_keys_sent_max {
                self.metrics_local.inv_keys_sent_max = key_count;
            }
            if let Some(out) = self.outputs.get_mut(local) {
                out.send(WireMessage::Inventory(inv)).await;
            }
        }
    }


    /// Inventory request from a peer running full-reconciliation:
    /// look up each requested state-map key in our own state, then
    /// reply with a `Batch` of the corresponding synthesised
    /// [`Gossip`]s. Missing keys are silently dropped.
    #[nexosim(schedulable)]
    async fn handle_inventory(&mut self, inv: InventoryMsg, _cx: &Context<Self>) {
        let Some(local) = self.peer_id_to_local.get(&inv.from).copied() else {
            return;
        };
        let gossips: Vec<Gossip> = match inv.kind {
            GossipKind::ChannelUpdate => {
                let map = self.state.chan_updates.read();
                inv.keys
                    .iter()
                    .filter_map(|&k| {
                        map.get(k).map(|(ts, size)| {
                            let (scid, direction) = unpack_cu_key(k);
                            synth_chan_update(scid, direction, ts, size)
                        })
                    })
                    .collect()
            }
            GossipKind::NodeAnnouncement => {
                let map = self.state.node_anns.read();
                inv.keys
                    .iter()
                    .filter_map(|&k| {
                        map.get(k)
                            .map(|(ts, size)| synth_node_ann(k, ts, size))
                    })
                    .collect()
            }
            GossipKind::ChannelAnnouncement => {
                let map = self.state.chan_anns.read();
                inv.keys
                    .iter()
                    .filter_map(|&k| map.get(k).map(|size| synth_chan_ann(k, size)))
                    .collect()
            }
        };
        if gossips.is_empty() {
            return;
        }
        let bytes: u64 = gossips.iter().map(|g| g.size_bytes as u64).sum();
        self.metrics_local.bytes_out_gossip += bytes;
        let batch = Arc::new(GossipBatch::from_mixed_for_kind(gossips, inv.kind));
        if let Some(out) = self.outputs.get_mut(local) {
            out.send(WireMessage::Batch(batch)).await;
        }
    }
}

impl SketchNode {
    fn absorb_one(&mut self, g: Gossip, cx: &Context<Self>) {
        match g.kind {
            GossipKind::ChannelUpdate => self.absorb_chan_updates(from_ref(&g), cx),
            GossipKind::NodeAnnouncement => self.absorb_node_anns(from_ref(&g), cx),
            GossipKind::ChannelAnnouncement => self.absorb_chan_anns(from_ref(&g), cx),
        }
    }

    fn absorb_chan_updates(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        // Phase A: write lock held only for dedup + insert. Fresh
        // first-seen tuples are pushed straight into the per-node
        // pending buffer (no event-queue write per gossip — the
        // batched delivery to the aggregator happens via flush_summary).
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh_count: usize = 0;
        {
            let mut m = self.state.chan_updates.write();
            for g in gs {
                let scid = g.scid.expect("ChannelUpdate must carry scid");
                let key = crate::state::pack_cu_key(scid, g.direction);
                let supersedes = m
                    .get_ts(key)
                    .map(|stored| g.timestamp > stored)
                    .unwrap_or(true);
                if supersedes {
                    m.insert(key, g.timestamp);
                    self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                        gossip: *g,
                        time_ns: now_ns,
                    });
                    fresh_count += 1;
                } else {
                    self.metrics_local.duplicates += 1;
                    self.metrics_local.duplicates_bytes += g.size_bytes as u64;
                }
            }
        }
        if fresh_count > 0 {
            self.metrics.bump_first_seen_count(fresh_count);
            self.maybe_force_flush(cx);
        }
    }

    fn absorb_node_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh_count: usize = 0;
        {
            let mut m = self.state.node_anns.write();
            for g in gs {
                let origin = g.origin.expect("NodeAnnouncement must carry origin");
                let supersedes = m
                    .get_ts(origin)
                    .map(|stored| g.timestamp > stored)
                    .unwrap_or(true);
                if supersedes {
                    m.insert(origin, g.timestamp);
                    self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                        gossip: *g,
                        time_ns: now_ns,
                    });
                    fresh_count += 1;
                } else {
                    self.metrics_local.duplicates += 1;
                    self.metrics_local.duplicates_bytes += g.size_bytes as u64;
                }
            }
        }
        if fresh_count > 0 {
            self.metrics.bump_first_seen_count(fresh_count);
            self.maybe_force_flush(cx);
        }
    }

    fn absorb_chan_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh_count: usize = 0;
        {
            let mut m = self.state.chan_anns.write();
            for g in gs {
                let scid = g.scid.expect("ChannelAnnouncement must carry scid");
                if m.insert_present(scid) {
                    self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                        gossip: *g,
                        time_ns: now_ns,
                    });
                    fresh_count += 1;
                } else {
                    self.metrics_local.duplicates += 1;
                    self.metrics_local.duplicates_bytes += g.size_bytes as u64;
                }
            }
        }
        if fresh_count > 0 {
            self.metrics.bump_first_seen_count(fresh_count);
            self.maybe_force_flush(cx);
        }
    }

    /// Schedule an early `flush_summary` if the first-seen pending
    /// buffer is at the soft cap. Caps per-node memory regardless of
    /// the configured `flush_interval`.
    fn maybe_force_flush(&self, cx: &Context<Self>) {
        if self.metrics_local.first_seen_pending.len() >= FIRST_SEEN_FORCE_FLUSH {
            let _ = cx.schedule_event(
                Duration::from_nanos(1),
                schedulable!(Self::flush_summary),
                (),
            );
        }
    }
}

