//! Flooding node — the simplest gossip strategy.
//!
//! On first sight of a fresh message, schedule a forward to all peers
//! after `forward_delay` (which we use to model one-way wire latency).
//! Already-seen messages are dropped silently. There is no batching, no
//! periodic tick, no inventory exchange — every new message becomes one
//! `WireMessage::Single` to every wired peer.
//!
//! Convergence time on a connected graph: `diameter × forward_delay`.

use std::collections::HashSet;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

use crate::message::{Gossip, MsgId, NodeId, WireMessage};
use crate::metrics::MetricsHandle;

/// NeXosim requires `Serialize + Deserialize` on every `Model` (for
/// optional save/restore); fields that aren't naturally serde-able
/// (`MetricsHandle`, which holds an `Arc<Mutex<...>>`) are marked
/// `#[serde(skip)]` and rely on `Default` for deserialization.
#[derive(Default, Serialize, Deserialize)]
pub struct FloodingNode {
    pub id: NodeId,
    /// Broadcast port. Wired up at sim init: `out.connect(peer_recv,
    /// &peer_mailbox)` once per peer, then a single `out.send(...)` fans
    /// out to all of them.
    pub out: Output<WireMessage>,
    /// How long to wait between receiving a new message and forwarding
    /// it. Stands in for one-way network latency.
    forward_delay: Duration,
    #[serde(skip)]
    metrics: MetricsHandle,
    /// Dedup set; receiving a duplicate is the most common case and we
    /// must do nothing.
    seen: HashSet<MsgId>,
}

impl FloodingNode {
    pub fn new(id: NodeId, forward_delay: Duration, metrics: MetricsHandle) -> Self {
        Self {
            id,
            out: Output::default(),
            forward_delay,
            metrics,
            seen: HashSet::new(),
        }
    }
}

#[Model]
impl FloodingNode {
    /// Input port: a peer just delivered a `WireMessage` to us. For each
    /// inner `Gossip` we haven't seen before, record the first-seen time
    /// and schedule a forward after `forward_delay`. We forward via
    /// `do_send` rather than `out.send` directly, because `recv` is a
    /// sync handler that can't `.await`.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        for g in wire.iter_gossips() {
            if !self.seen.insert(g.id) {
                continue;
            }
            self.metrics.record_first_seen(self.id, g.id, cx.time());
            cx.schedule_event(
                self.forward_delay,
                schedulable!(Self::do_send),
                g.clone(),
            )
            .expect("schedule do_send");
        }
    }

    /// Input port for the per-node `EventSource`. A scheduled origination
    /// arrives here with the gossip the runner wants this node to inject.
    /// Async because we can `.await out.send` here directly, with no
    /// `forward_delay`: the originator is the source so there is nothing
    /// to "forward from". Receivers will still apply their own
    /// `forward_delay` when they re-broadcast.
    pub async fn originate(&mut self, msg: Gossip, cx: &Context<Self>) {
        if !self.seen.insert(msg.id) {
            return;
        }
        self.metrics.record_first_seen(self.id, msg.id, cx.time());
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
