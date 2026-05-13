//! Flooding node — the simplest gossip strategy.
//!
//! On a fresh `(scid, direction, timestamp)` tuple (per BOLT 7 dedup),
//! schedule a forward to all peers after `forward_delay` (a stand-in for
//! one-way wire latency). Old or equal timestamps are dropped silently
//! and counted as per-node duplicates.
//!
//! Convergence time on a connected graph: `diameter × forward_delay`.
//!
//! ## Per-peer outputs
//!
//! `outputs` is a `Vec<Output<WireMessage>>` with one Output per peer
//! (each Output has exactly one connected mailbox). Broadcast is
//! "iterate and send"; `peer_id_to_local` maps a peer's `NodeId` to
//! its position in the Vec. The arrangement is driven from sim init
//! by walking `topology.peers.neighbors(nx)` deterministically — see
//! sim.rs.
//!
//! ## Shared dedup state
//!
//! `state: SharedNodeState` lives behind `Arc<NodeState>`. Sketch
//! protocol nodes elsewhere can read this node's state via the same
//! Arc to compute set-recon diffs. The dedup writes themselves take
//! brief per-kind RwLock writes; the rest of the recv loop is
//! lock-free. `peer_states` is unused by Flooding (it doesn't run
//! reconciliation) but the field is present so sim.rs can wire all
//! node kinds through one helper.

use std::collections::HashMap;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

use crate::message::{Gossip, GossipKind, NodeId, NodeIdx, WireMessage};
use crate::metrics::{
    FIRST_SEEN_FORCE_FLUSH, FirstSeenEntry, MetricsHandle, PerNodeMetrics, ns_since_epoch,
};
use crate::state::{SharedNodeState, originate_stamp};

#[derive(Default, Serialize, Deserialize)]
pub struct FloodingNode {
    /// Stable identifier (sparse u64 hash for CSV; dense 0..n for
    /// synthetic). Used as `gossip.origin` on outgoing messages.
    pub id: NodeId,
    /// Dense index `0..n_nodes` used by metrics for `Vec` storage.
    pub idx: NodeIdx,
    /// One Output per peer. Each Output has exactly one connected
    /// mailbox; "broadcast" is the explicit `for out in &mut outputs`
    /// loop in `do_send` / `originate`.
    pub outputs: Vec<Output<WireMessage>>,
    /// `peer_ids[i]` is the `NodeId` of `outputs[i]`'s recipient.
    pub peer_ids: Vec<NodeId>,
    /// Reverse map: `NodeId -> local index into outputs/peer_ids`.
    /// Lets reply paths (sketch) find the right per-peer Output.
    pub peer_id_to_local: HashMap<NodeId, usize>,
    /// How long to wait between receiving a new message and forwarding
    /// it. Stands in for one-way network latency.
    forward_delay: Duration,
    /// How long until end of run; used to schedule the final
    /// `flush_summary`.
    run_duration: Duration,
    flush_interval: Duration,
    flush_phase: Duration,
    /// Per-node dedup state owned by the registry (see [`crate::state`]).
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
}

impl FloodingNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        forward_delay: Duration,
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
            forward_delay,
            run_duration,
            flush_interval,
            flush_phase,
            state,
            peer_states,
            metrics_local: PerNodeMetrics::default(),
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
}

#[Model]
impl FloodingNode {
    /// One-time setup: arm the periodic + final `flush_summary`.
    /// Flooding has no per-protocol ticker (unlike cln/lnd/sketch).
    #[nexosim(init)]
    async fn arm_flush(&mut self, cx: &Context<Self>) {
        if self.flush_interval > Duration::ZERO {
            cx.schedule_periodic_event(
                self.flush_phase,
                self.flush_interval,
                schedulable!(Self::flush_summary),
                (),
            )
            .expect("schedule periodic flooding flush_summary");
        }
        cx.schedule_event(self.run_duration, schedulable!(Self::flush_summary), ())
            .expect("schedule flooding final flush_summary");
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

    /// Input port: a peer just delivered a `WireMessage`. For each
    /// inner gossip, dispatch on `kind`:
    ///
    /// * `ChannelUpdate` — keep if `timestamp` strictly exceeds the
    ///   stored `(scid, direction)` value.
    /// * `NodeAnnouncement` — keep if `timestamp` strictly exceeds the
    ///   stored value for `origin`.
    /// * `ChannelAnnouncement` — keep if this is the first time we've
    ///   seen this `scid`.
    ///
    /// Duplicates bump the per-node duplicate counter and are
    /// dropped. Kept messages are recorded in metrics and scheduled
    /// for forward after `forward_delay`.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        self.metrics_local.bytes_in_gossip += wire.wire_size();
        match &wire {
            WireMessage::Single(g) => {
                let g_copy = *g;
                self.absorb_one(g_copy, cx);
            }
            WireMessage::Batch(batch) => {
                self.absorb_chan_updates(&batch.chan_updates, cx);
                self.absorb_node_anns(&batch.node_anns, cx);
                self.absorb_chan_anns(&batch.chan_anns, cx);
            }
            WireMessage::Sketch(_) => {
                // Flooding doesn't speak sketch protocol — silently ignore.
            }
        }
    }

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
        let mut fresh: Vec<Gossip> = Vec::with_capacity(gs.len());
        {
            let mut m = self.state.chan_updates.write();
            for g in gs {
                let scid = g.scid.expect("ChannelUpdate must carry scid");
                let key = crate::state::pack_cu_key(scid, g.direction);
                let supersedes = m
                    .get(&key)
                    .map(|(stored, _)| g.timestamp > *stored)
                    .unwrap_or(true);
                if supersedes {
                    m.insert(key, (g.timestamp, g.size_bytes));
                    fresh.push(*g);
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        if !fresh.is_empty() {
            self.metrics.bump_first_seen_count(fresh.len());
            for &g in &fresh {
                self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                    gossip: g,
                    time_ns: now_ns,
                });
                cx.schedule_event(self.forward_delay, schedulable!(FloodingNode::do_send), g)
                    .expect("schedule do_send");
            }
            self.maybe_force_flush(cx);
        }
    }

    fn absorb_node_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh: Vec<Gossip> = Vec::with_capacity(gs.len());
        {
            let mut m = self.state.node_anns.write();
            for g in gs {
                let origin = g.origin.expect("NodeAnnouncement must carry origin");
                let supersedes = m
                    .get(&origin)
                    .map(|(stored, _)| g.timestamp > *stored)
                    .unwrap_or(true);
                if supersedes {
                    m.insert(origin, (g.timestamp, g.size_bytes));
                    fresh.push(*g);
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        if !fresh.is_empty() {
            self.metrics.bump_first_seen_count(fresh.len());
            for &g in &fresh {
                self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                    gossip: g,
                    time_ns: now_ns,
                });
                cx.schedule_event(self.forward_delay, schedulable!(FloodingNode::do_send), g)
                    .expect("schedule do_send");
            }
            self.maybe_force_flush(cx);
        }
    }

    fn absorb_chan_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh: Vec<Gossip> = Vec::with_capacity(gs.len());
        {
            let mut m = self.state.chan_anns.write();
            for g in gs {
                let scid = g.scid.expect("ChannelAnnouncement must carry scid");
                if m.insert(scid, g.size_bytes).is_none() {
                    fresh.push(*g);
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        if !fresh.is_empty() {
            self.metrics.bump_first_seen_count(fresh.len());
            for &g in &fresh {
                self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                    gossip: g,
                    time_ns: now_ns,
                });
                cx.schedule_event(self.forward_delay, schedulable!(FloodingNode::do_send), g)
                    .expect("schedule do_send");
            }
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

    /// Input port for the per-node `EventSource`. Originated messages
    /// arrive without a meaningful `timestamp`; for `ChannelUpdate` and
    /// `NodeAnnouncement` we stamp from current sim time, bumping past
    /// any stored value so the per-key timestamp is strictly increasing
    /// (BOLT 7 requires it). For `ChannelAnnouncement` we leave
    /// `timestamp` alone but skip emission entirely if we've already
    /// seen this SCID.
    pub async fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        if !originate_stamp(&self.state, self.id, &mut msg, cx.time()) {
            return;
        }
        self.metrics_local.first_seen_pending.push(FirstSeenEntry {
            gossip: msg,
            time_ns: ns_since_epoch(cx.time()),
        });
        self.metrics.bump_first_seen_count(1);
        self.broadcast_single(msg).await;
    }

    /// Helper invoked by `recv` after `forward_delay`. Marked
    /// `#[nexosim(schedulable)]` so it can be referenced via
    /// `schedulable!(Self::do_send)` from the scheduler.
    #[nexosim(schedulable)]
    pub async fn do_send(&mut self, msg: Gossip) {
        self.broadcast_single(msg).await;
    }
}

impl FloodingNode {
    /// Send `msg` as `WireMessage::Single` to every per-peer Output.
    /// Each Output has exactly one connected mailbox, so this is N
    /// independent sends rather than one broadcast iteration. Bandwidth
    /// metric is recorded once with the per-recipient multiplier baked
    /// in, on the local plain-u64 counter.
    async fn broadcast_single(&mut self, msg: Gossip) {
        let n_peers = self.outputs.len() as u64;
        self.metrics_local.bytes_out_gossip += msg.size_bytes as u64 * n_peers;
        for out in &mut self.outputs {
            out.send(WireMessage::Single(msg)).await;
        }
    }
}


