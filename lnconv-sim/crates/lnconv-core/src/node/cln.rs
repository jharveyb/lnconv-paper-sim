//! c-lightning style stagger node.
//!
//! Receives go straight into a per-node `pending` queue (after BOLT 7
//! dedup). A periodic tick every `stagger` ms drains the queue into a
//! single `WireMessage::Batch` and broadcasts. There is no per-batch
//! trickle and no batch-size cap — whatever's pending goes out in one
//! shot.
//!
//! Each node samples a random `first_tick` offset in (0, stagger] at
//! construction time so different nodes' tick boundaries don't all line
//! up. Without this the simulator would let messages cascade many hops in
//! one time step, biasing convergence times much faster than the
//! algorithm allows.
//!
//! See [`super::flooding`] for notes on per-peer outputs and shared
//! `NodeState` — same shape here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

use crate::message::{Gossip, GossipBatch, GossipKind, NodeId, NodeIdx, WireMessage};
use crate::metrics::{
    FIRST_SEEN_FORCE_FLUSH, FirstSeenEntry, MetricsHandle, PerNodeMetrics, ns_since_epoch,
};
use crate::state::{SharedNodeState, originate_stamp};

#[derive(Default, Serialize, Deserialize)]
pub struct ClnNode {
    pub id: NodeId,
    pub idx: NodeIdx,
    pub outputs: Vec<Output<WireMessage>>,
    pub peer_ids: Vec<NodeId>,
    pub peer_id_to_local: HashMap<NodeId, usize>,
    /// Period between drain ticks.
    stagger: Duration,
    /// Absolute time of this node's *first* tick. Sampled uniformly in
    /// (0, stagger] by the runner — see `sim::sample_phase`.
    first_tick: Duration,
    /// How long until end of run; used to schedule the final
    /// `flush_summary`.
    run_duration: Duration,
    flush_interval: Duration,
    flush_phase: Duration,
    #[serde(skip)]
    state: SharedNodeState,
    #[serde(skip)]
    #[allow(dead_code)]
    peer_states: Vec<SharedNodeState>,
    /// Plain-u64 per-node counters; flushed periodically + at end-of-run.
    #[serde(skip)]
    metrics_local: PerNodeMetrics,
    #[serde(skip)]
    metrics: MetricsHandle,
    /// Gossip awaiting the next stagger tick.
    pending: Vec<Gossip>,
}

impl ClnNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        stagger: Duration,
        first_tick: Duration,
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
            stagger,
            first_tick,
            run_duration,
            flush_interval,
            flush_phase,
            state,
            peer_states,
            metrics_local: PerNodeMetrics::default(),
            metrics,
            pending: Vec::new(),
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
}

#[Model]
impl ClnNode {
    /// One-time setup at sim start. Arms the periodic stagger tick +
    /// the periodic and final `flush_summary` schedules.
    #[nexosim(init)]
    async fn arm_ticks(&mut self, cx: &Context<Self>) {
        cx.schedule_periodic_event(self.first_tick, self.stagger, schedulable!(Self::tick), ())
            .expect("schedule cln tick");
        if self.flush_interval > Duration::ZERO {
            cx.schedule_periodic_event(
                self.flush_phase,
                self.flush_interval,
                schedulable!(Self::flush_summary),
                (),
            )
            .expect("schedule periodic cln flush_summary");
        }
        cx.schedule_event(self.run_duration, schedulable!(Self::flush_summary), ())
            .expect("schedule cln final flush_summary");
    }

    /// Periodic + final flush. Drains first-seen + overflow buffers
    /// and sends a counter snapshot.
    #[nexosim(schedulable)]
    async fn flush_summary(&mut self, _: (), cx: &Context<Self>) {
        let time_ns = ns_since_epoch(cx.time());
        let first_seen = std::mem::take(&mut self.metrics_local.first_seen_pending);
        self.metrics_local.first_seen_pending = Vec::with_capacity(FIRST_SEEN_FORCE_FLUSH);
        self.metrics.send_first_seen_batch(self.idx, first_seen);
        let counters = self.metrics_local.snapshot_counters();
        let drained = std::mem::take(&mut self.metrics_local.overflow_events);
        self.metrics.send_counters_delta(self.idx, time_ns, counters, drained);
    }

    /// Input port. BOLT 7 per-kind dedup, then queue for next tick.
    pub async fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        self.metrics_local.bytes_in_gossip += wire.wire_size();
        match &wire {
            WireMessage::Single(g) => {
                let g_copy = *g;
                self.absorb_one(g_copy, cx);
            }
            WireMessage::Batch(batch) => {
                self.absorb_chan_anns(&batch.chan_anns, cx);
                self.absorb_chan_updates(&batch.chan_updates, cx);
                self.absorb_node_anns(&batch.node_anns, cx);
            }
            WireMessage::Sketch(_) => {}
            WireMessage::Inventory(_) => {}
        }
    }

    /// Originated messages broadcast immediately (CLN behaviour:
    /// local updates aren't held by the stagger window).
    pub async fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        if !originate_stamp(&self.state, self.id, &mut msg, cx.time()) {
            return;
        }
        self.metrics_local.first_seen_pending.push(FirstSeenEntry {
            gossip: msg,
            time_ns: ns_since_epoch(cx.time()),
        });
        self.metrics.bump_first_seen_count(1);
        let n_peers = self.outputs.len() as u64;
        self.metrics_local.bytes_out_gossip += msg.size_bytes as u64 * n_peers;
        for out in &mut self.outputs {
            out.send(WireMessage::Single(msg)).await;
        }
    }

    /// Periodic broadcast. If anything's pending, partition by kind
    /// into a `GossipBatch` (so receivers can take each per-kind
    /// `RwLock` exactly once) and send to each peer.
    #[nexosim(schedulable)]
    async fn tick(&mut self, _: ()) {
        if self.pending.is_empty() {
            return;
        }
        let drained = std::mem::take(&mut self.pending);
        let bytes_per_peer: u64 = drained.iter().map(|g| g.size_bytes as u64).sum();
        let n_peers = self.outputs.len() as u64;
        self.metrics_local.bytes_out_gossip += bytes_per_peer * n_peers;
        let arc_batch = Arc::new(GossipBatch::from_mixed(&drained));
        for out in &mut self.outputs {
            out.send(WireMessage::Batch(arc_batch.clone())).await;
        }
    }
}

impl ClnNode {
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
                    self.pending.push(*g);
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
                    self.pending.push(*g);
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
                    self.pending.push(*g);
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
