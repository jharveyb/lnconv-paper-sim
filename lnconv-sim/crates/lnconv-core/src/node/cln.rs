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

use std::collections::HashMap;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{Direction, Gossip, NodeId, NodeIdx, Scid, WireMessage};
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
    /// BOLT 7 LN graph state: latest timestamp seen per `(scid,
    /// direction)`. Lifetime, not per-tick.
    lngraph: HashMap<(Scid, Direction), u32>,
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
            lngraph: HashMap::new(),
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

    /// Input port. BOLT 7 dedup, then queue for next tick.
    pub async fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        for g in wire.iter_gossips() {
            let key = (g.scid, g.direction);
            if let Some(&stored) = self.lngraph.get(&key)
                && g.timestamp <= stored {
                    continue;
                }
            self.lngraph.insert(key, g.timestamp);
            self.metrics.record_first_seen(self.idx, g, cx.time());
            self.pending.push(*g);
        }
    }

    /// Input port for `EventSource`-driven originations. Unlike forwarded
    /// gossip (which waits for the next stagger tick), an originated
    /// message broadcasts to all connected peers *immediately* as a
    /// `WireMessage::Single` — matches CLN's behavior where local
    /// `channel_update`s aren't held back by the stagger window. Async
    /// because we `.await` the broadcast directly here.
    ///
    /// Timestamp stamped from current sim time, bumped past any existing
    /// entry to guarantee strict monotonicity (BOLT 7 requirement).
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

    /// Periodic broadcast. If anything's pending, take it all and send
    /// as one batch. Most ticks for most nodes have empty pending.
    #[nexosim(schedulable)]
    async fn tick(&mut self, _: ()) {
        if self.pending.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.pending);
        self.out.send(WireMessage::Batch(batch)).await;
    }
}
