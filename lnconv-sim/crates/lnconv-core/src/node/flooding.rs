//! Flooding node — the simplest gossip strategy.
//!
//! On a fresh `(scid, direction, timestamp)` tuple (per BOLT 7 dedup),
//! schedule a forward to all peers after `forward_delay` (a stand-in for
//! one-way wire latency). Old or equal timestamps are dropped silently.
//!
//! Convergence time on a connected graph: `diameter × forward_delay`.

use std::collections::HashMap;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{Direction, Gossip, NodeId, NodeIdx, Scid, WireMessage};
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
    /// BOLT 7 LN graph state: latest timestamp seen per `(scid,
    /// direction)`. Lifetime, not per-tick: equal/older arrivals are
    /// dropped, strictly-newer ones supersede and re-broadcast.
    lngraph: HashMap<(Scid, Direction), u32>,
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
            lngraph: HashMap::new(),
        }
    }
}

#[Model]
impl FloodingNode {
    /// Input port: a peer just delivered a `WireMessage`. For each inner
    /// gossip whose timestamp is strictly newer than what we have stored
    /// for `(scid, direction)`, update the lngraph, record the first-seen
    /// time, and schedule a forward after `forward_delay`. Forwarding
    /// goes through `do_send` (a schedulable helper) because `recv` is a
    /// sync handler that can't `.await`.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        for g in wire.iter_gossips() {
            let key = (g.scid, g.direction);
            if let Some(&stored) = self.lngraph.get(&key)
                && g.timestamp <= stored {
                    continue;
                }
            self.lngraph.insert(key, g.timestamp);
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
    /// arrive without a meaningful `timestamp`; we stamp it here from the
    /// current sim time, bumping past any existing stored timestamp for
    /// this `(scid, direction)` so the value is strictly increasing
    /// (BOLT 7 requires it). Then broadcast immediately — the originator
    /// is the source so there's nothing to "forward from" with delay.
    pub async fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        let now_secs = cx
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs() as u32;
        let key = (msg.scid, msg.direction);
        let next_ts = match self.lngraph.get(&key) {
            Some(&stored) => stored.saturating_add(1).max(now_secs),
            None => now_secs,
        };
        msg.timestamp = next_ts;
        self.lngraph.insert(key, next_ts);
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
