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
use crate::metrics::MetricsHandle;
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
    #[serde(skip)]
    state: SharedNodeState,
    #[serde(skip)]
    #[allow(dead_code)]
    peer_states: Vec<SharedNodeState>,
    #[serde(skip)]
    metrics: MetricsHandle,
    /// Gossip awaiting the next stagger tick.
    pending: Vec<Gossip>,
}

impl ClnNode {
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        stagger: Duration,
        first_tick: Duration,
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
            state,
            peer_states,
            metrics,
            pending: Vec::new(),
        }
    }

    pub fn add_peer(&mut self, peer_id: NodeId, out: Output<WireMessage>) {
        let local = self.outputs.len();
        self.outputs.push(out);
        self.peer_ids.push(peer_id);
        self.peer_id_to_local.insert(peer_id, local);
    }
}

#[Model]
impl ClnNode {
    /// One-time setup at sim start. Arms the periodic stagger tick:
    /// first fire at `first_tick`, then every `stagger` thereafter.
    #[nexosim(init)]
    async fn arm_ticks(&mut self, cx: &Context<Self>) {
        cx.schedule_periodic_event(self.first_tick, self.stagger, schedulable!(Self::tick), ())
            .expect("schedule cln tick");
    }

    /// Input port. BOLT 7 per-kind dedup, then queue for next tick.
    pub async fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        self.metrics.record_bytes_in(self.idx, wire.wire_size());
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
            WireMessage::Sketch(_) => {}
        }
    }

    /// Originated messages broadcast immediately (CLN behaviour:
    /// local updates aren't held by the stagger window).
    pub async fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        if !originate_stamp(&self.state, self.id, &mut msg, cx.time()) {
            return;
        }
        self.metrics.record_first_seen(self.idx, &msg, cx.time());
        let n_peers = self.outputs.len() as u64;
        self.metrics
            .record_bytes_out(self.idx, msg.size_bytes as u64 * n_peers);
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
        self.metrics.record_bytes_out(self.idx, bytes_per_peer * n_peers);
        let arc_batch = Arc::new(GossipBatch::from_mixed(drained));
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
        let mut m = self.state.chan_updates.write().expect("chan_updates poisoned");
        for g in gs {
            let scid = g.scid.expect("ChannelUpdate must carry scid");
            let key = (scid, g.direction);
            let supersedes = m
                .get(&key)
                .map(|(stored, _)| g.timestamp > *stored)
                .unwrap_or(true);
            if supersedes {
                m.insert(key, (g.timestamp, g.size_bytes));
                self.metrics.record_first_seen(self.idx, g, cx.time());
                self.pending.push(*g);
            } else {
                self.metrics.record_duplicate(self.idx);
            }
        }
    }

    fn absorb_node_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let mut m = self.state.node_anns.write().expect("node_anns poisoned");
        for g in gs {
            let origin = g.origin.expect("NodeAnnouncement must carry origin");
            let supersedes = m
                .get(&origin)
                .map(|(stored, _)| g.timestamp > *stored)
                .unwrap_or(true);
            if supersedes {
                m.insert(origin, (g.timestamp, g.size_bytes));
                self.metrics.record_first_seen(self.idx, g, cx.time());
                self.pending.push(*g);
            } else {
                self.metrics.record_duplicate(self.idx);
            }
        }
    }

    fn absorb_chan_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let mut m = self.state.chan_anns.write().expect("chan_anns poisoned");
        for g in gs {
            let scid = g.scid.expect("ChannelAnnouncement must carry scid");
            let fresh = m.insert(scid, g.size_bytes).is_none();
            if fresh {
                self.metrics.record_first_seen(self.idx, g, cx.time());
                self.pending.push(*g);
            } else {
                self.metrics.record_duplicate(self.idx);
            }
        }
    }
}
