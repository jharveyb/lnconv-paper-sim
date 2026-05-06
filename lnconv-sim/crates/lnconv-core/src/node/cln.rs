//! c-lightning style stagger node.
//!
//! Receives go straight into a per-node `pending` queue (after dedup). A
//! periodic tick every `stagger` ms drains the queue into a single
//! `WireMessage::Batch` and broadcasts. There is no per-batch trickle and
//! no batch-size cap — whatever's pending goes out in one shot.
//!
//! Each node samples a random `first_tick` offset in (0, stagger] at
//! construction time so different nodes' tick boundaries don't all line
//! up. Without this the simulator would let messages cascade many hops in
//! one time step, biasing convergence times much faster than the
//! algorithm allows. With it, expected per-hop wait is `stagger/2`.

use std::collections::HashSet;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

use crate::message::{Gossip, MsgId, NodeId, WireMessage};
use crate::metrics::MetricsHandle;

#[derive(Default, Serialize, Deserialize)]
pub struct ClnNode {
    pub id: NodeId,
    pub out: Output<WireMessage>,
    /// Period between drain ticks.
    stagger: Duration,
    /// Absolute time of this node's *first* tick. Sampled uniformly in
    /// (0, stagger] by the runner — see `sim::sample_phase`.
    first_tick: Duration,
    #[serde(skip)]
    metrics: MetricsHandle,
    seen: HashSet<MsgId>,
    /// Gossip awaiting the next stagger tick.
    pending: Vec<Gossip>,
}

impl ClnNode {
    pub fn new(
        id: NodeId,
        stagger: Duration,
        first_tick: Duration,
        metrics: MetricsHandle,
    ) -> Self {
        Self {
            id,
            out: Output::default(),
            stagger,
            first_tick,
            metrics,
            seen: HashSet::new(),
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

    /// Input port. Just queue new gossip; sending happens on `tick`.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        for g in wire.iter_gossips() {
            if !self.seen.insert(g.id) {
                continue;
            }
            self.metrics.record_first_seen(self.id, g.id, cx.time());
            self.pending.push(g.clone());
        }
    }

    /// Input port for `EventSource`-driven originations. Treats the new
    /// message exactly like a received one — it goes in `pending` and
    /// waits for the next stagger tick. Matches CLN's actual behavior:
    /// originated updates aren't broadcast immediately.
    pub fn originate(&mut self, msg: Gossip, cx: &Context<Self>) {
        if !self.seen.insert(msg.id) {
            return;
        }
        self.metrics.record_first_seen(self.id, msg.id, cx.time());
        self.pending.push(msg);
    }

    /// Periodic broadcast. If anything's pending, take it all and send
    /// as one batch. The empty-queue path is hit a lot — most ticks for
    /// most nodes don't have anything to send.
    #[nexosim(schedulable)]
    async fn tick(&mut self, _: ()) {
        if self.pending.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.pending);
        self.out.send(WireMessage::Batch(batch)).await;
    }
}
