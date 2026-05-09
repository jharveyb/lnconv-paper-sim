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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{Direction, Gossip, GossipKind, NodeId, NodeIdx, Scid, WireMessage};
use crate::metrics::MetricsHandle;

#[derive(Default, Serialize, Deserialize)]
pub struct ClnNode {
    pub id: NodeId,
    pub idx: NodeIdx,
    pub out: Output<WireMessage>,
    /// Period between drain ticks.
    stagger: Duration,
    /// Absolute time of this node's *first* tick. Sampled uniformly in
    /// (0, stagger] by the runner — see `sim::sample_phase`.
    first_tick: Duration,
    #[serde(skip)]
    metrics: MetricsHandle,
    /// `channel_update` dedup, lifetime not per-tick.
    chan_updates: HashMap<(Scid, Direction), u32>,
    /// `node_announcement` dedup, lifetime not per-tick.
    node_anns: HashMap<NodeId, u32>,
    /// `channel_announcement` first-seen set.
    chan_anns: HashSet<Scid>,
    /// Gossip awaiting the next stagger tick.
    pending: Vec<Gossip>,
}

impl ClnNode {
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        stagger: Duration,
        first_tick: Duration,
        metrics: MetricsHandle,
    ) -> Self {
        Self {
            id,
            idx,
            out: Output::default(),
            stagger,
            first_tick,
            metrics,
            chan_updates: HashMap::new(),
            node_anns: HashMap::new(),
            chan_anns: HashSet::new(),
            pending: Vec::new(),
        }
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
        for g in wire.iter_gossips() {
            let fresh = match g.kind {
                GossipKind::ChannelUpdate => {
                    let key = (g.scid, g.direction);
                    let supersedes = self
                        .chan_updates
                        .get(&key)
                        .map(|&s| g.timestamp > s)
                        .unwrap_or(true);
                    if supersedes {
                        self.chan_updates.insert(key, g.timestamp);
                    }
                    supersedes
                }
                GossipKind::NodeAnnouncement => {
                    let supersedes = self
                        .node_anns
                        .get(&g.origin)
                        .map(|&s| g.timestamp > s)
                        .unwrap_or(true);
                    if supersedes {
                        self.node_anns.insert(g.origin, g.timestamp);
                    }
                    supersedes
                }
                GossipKind::ChannelAnnouncement => self.chan_anns.insert(g.scid),
            };
            if !fresh {
                continue;
            }
            self.metrics.record_first_seen(self.idx, g, cx.time());
            self.pending.push(*g);
        }
    }

    /// Input port for `EventSource`-driven originations. Unlike forwarded
    /// gossip (which waits for the next stagger tick), an originated
    /// message broadcasts to all connected peers *immediately* as a
    /// `WireMessage::Single` — matches CLN's behavior where local
    /// `channel_update`s aren't held back by the stagger window.
    ///
    /// `ChannelUpdate` and `NodeAnnouncement` are timestamped from
    /// current sim time, bumped past any stored value (BOLT 7
    /// monotonicity). `ChannelAnnouncement` keeps its incoming
    /// timestamp and is suppressed if we've already broadcast this
    /// SCID — prevents the parquet replay's two-endpoint duplicate
    /// origination from producing two cascades.
    pub async fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        let now_secs = cx
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs() as u32;
        match msg.kind {
            GossipKind::ChannelUpdate => {
                let key = (msg.scid, msg.direction);
                let next_ts = match self.chan_updates.get(&key) {
                    Some(&stored) => stored.saturating_add(1).max(now_secs),
                    None => now_secs,
                };
                msg.timestamp = next_ts;
                self.chan_updates.insert(key, next_ts);
            }
            GossipKind::NodeAnnouncement => {
                let next_ts = match self.node_anns.get(&msg.origin) {
                    Some(&stored) => stored.saturating_add(1).max(now_secs),
                    None => now_secs,
                };
                msg.timestamp = next_ts;
                self.node_anns.insert(msg.origin, next_ts);
            }
            GossipKind::ChannelAnnouncement => {
                if !self.chan_anns.insert(msg.scid) {
                    return;
                }
            }
        }
        self.metrics.record_first_seen(self.idx, &msg, cx.time());
        self.out.send(WireMessage::Single(msg)).await;
    }

    /// Periodic broadcast. If anything's pending, take it all and send
    /// as one batch. Most ticks for most nodes have empty pending.
    #[nexosim(schedulable)]
    async fn tick(&mut self, _: ()) {
        if self.pending.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.pending);
        self.out.send(WireMessage::Batch(Arc::new(batch))).await;
    }
}
