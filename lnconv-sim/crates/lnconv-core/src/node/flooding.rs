//! Flooding node — the simplest gossip strategy.
//!
//! On a fresh `(scid, direction, timestamp)` tuple (per BOLT 7 dedup),
//! schedule a forward to all peers after `forward_delay` (a stand-in for
//! one-way wire latency). Old or equal timestamps are dropped silently.
//!
//! Convergence time on a connected graph: `diameter × forward_delay`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{Direction, Gossip, GossipKind, NodeId, NodeIdx, Scid, WireMessage};
use crate::metrics::MetricsHandle;

/// NeXosim requires `Serialize + Deserialize` on every `Model` (for
/// optional save/restore); fields that aren't naturally serde-able
/// (`MetricsHandle`, which holds an `Arc<Mutex<...>>`) are marked
/// `#[serde(skip)]` and rely on `Default` for deserialization.
#[derive(Default, Serialize, Deserialize)]
pub struct FloodingNode {
    /// Stable identifier (sparse u64 hash for CSV; dense 0..n for
    /// synthetic). Used as `gossip.origin` on outgoing messages.
    pub id: NodeId,
    /// Dense index `0..n_nodes` used by metrics for `Vec` storage.
    pub idx: NodeIdx,
    /// Broadcast port. Wired up at sim init: `out.connect(peer_recv,
    /// &peer_mailbox)` once per peer, then a single `out.send(...)` fans
    /// out to all of them.
    pub out: Output<WireMessage>,
    /// How long to wait between receiving a new message and forwarding
    /// it. Stands in for one-way network latency.
    forward_delay: Duration,
    #[serde(skip)]
    metrics: MetricsHandle,
    /// `channel_update` dedup: latest timestamp seen per
    /// `(scid, direction)`. Equal/older arrivals are dropped,
    /// strictly-newer ones supersede and re-broadcast.
    chan_updates: HashMap<(Scid, Direction), u32>,
    /// `node_announcement` dedup: latest timestamp seen per origin node.
    /// Same monotonic rule as `chan_updates`.
    node_anns: HashMap<NodeId, u32>,
    /// `channel_announcement` dedup: SCIDs we've already seen. BOLT 7
    /// channel announcements are not timestamped — first arrival wins
    /// and any subsequent arrival is dropped silently.
    chan_anns: HashSet<Scid>,
}

impl FloodingNode {
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        forward_delay: Duration,
        metrics: MetricsHandle,
    ) -> Self {
        Self {
            id,
            idx,
            out: Output::default(),
            forward_delay,
            metrics,
            chan_updates: HashMap::new(),
            node_anns: HashMap::new(),
            chan_anns: HashSet::new(),
        }
    }
}

#[Model]
impl FloodingNode {
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
    /// Kept messages are recorded in metrics and scheduled for forward
    /// after `forward_delay` via the schedulable helper.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
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
            cx.schedule_event(
                self.forward_delay,
                schedulable!(Self::do_send),
                *g,
            )
            .expect("schedule do_send");
        }
    }

    /// Input port for the per-node `EventSource`. Originated messages
    /// arrive without a meaningful `timestamp`; for `ChannelUpdate` and
    /// `NodeAnnouncement` we stamp from current sim time, bumping past
    /// any stored value so the per-key timestamp is strictly increasing
    /// (BOLT 7 requires it). For `ChannelAnnouncement` we leave
    /// `timestamp` alone but skip emission entirely if we've already
    /// seen this SCID — this is what collapses the parquet replay's
    /// "both endpoints emit" doubling into a single broadcast cascade.
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

    /// Helper invoked by `recv` after `forward_delay`. Marked
    /// `#[nexosim(schedulable)]` so it can be referenced via
    /// `schedulable!(Self::do_send)` from the scheduler.
    #[nexosim(schedulable)]
    pub async fn do_send(&mut self, msg: Gossip) {
        self.out.send(WireMessage::Single(msg)).await;
    }
}
