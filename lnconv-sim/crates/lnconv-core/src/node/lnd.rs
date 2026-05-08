//! LND-style stagger node.
//!
//! Same lifecycle as [`super::cln::ClnNode`] (queue on recv/originate,
//! periodic tick, random first-tick offset, BOLT 7 dedup) plus two
//! refinements that match the LND defaults:
//!
//! * **min_batch_size** — drained pending is split into chunks of this
//!   size before being sent. Each chunk becomes one `WireMessage::Batch`.
//! * **trickle** — only the first chunk goes out immediately on the tick;
//!   chunk `i > 0` is scheduled for `now + i * trickle`. This spreads
//!   bandwidth across the stagger window instead of bursting it all at
//!   once, matching LND's "trickle out a few updates at a time" design.
//!
//! For a single in-flight message there is nothing to chunk and trickle
//! never engages — LND and CLN converge identically. The trickle path is
//! exercised by workloads with concurrent messages per tick (e.g.
//! `OneShotAll`, `PoissonRandom` at high rate).

use std::collections::HashMap;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

use crate::message::{Direction, Gossip, NodeId, Scid, WireMessage};
use crate::metrics::MetricsHandle;

#[derive(Default, Serialize, Deserialize)]
pub struct LndNode {
    pub id: NodeId,
    pub out: Output<WireMessage>,
    stagger: Duration,
    /// Sampled offset of this node's first stagger tick — see
    /// `sim::sample_phase`.
    first_tick: Duration,
    /// Inter-chunk spread within a single stagger window.
    trickle: Duration,
    /// Each chunk sent on a tick contains at most this many gossips.
    min_batch_size: usize,
    #[serde(skip)]
    metrics: MetricsHandle,
    /// BOLT 7 LN graph state: latest timestamp seen per `(scid,
    /// direction)`. Lifetime, not per-tick.
    lngraph: HashMap<(Scid, Direction), u32>,
    pending: Vec<Gossip>,
}

impl LndNode {
    pub fn new(
        id: NodeId,
        stagger: Duration,
        first_tick: Duration,
        trickle: Duration,
        min_batch_size: usize,
        metrics: MetricsHandle,
    ) -> Self {
        Self {
            id,
            out: Output::default(),
            stagger,
            first_tick,
            trickle,
            min_batch_size: min_batch_size.max(1),
            metrics,
            lngraph: HashMap::new(),
            pending: Vec::new(),
        }
    }
}

#[Model]
impl LndNode {
    /// Arm the periodic stagger tick — first fire at `first_tick`, then
    /// every `stagger` thereafter.
    #[nexosim(init)]
    async fn arm_ticks(&mut self, cx: &Context<Self>) {
        cx.schedule_periodic_event(self.first_tick, self.stagger, schedulable!(Self::tick), ())
            .expect("schedule lnd tick");
    }

    /// Input port. BOLT 7 dedup, then queue.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        for g in wire.iter_gossips() {
            let key = (g.scid, g.direction);
            if let Some(&stored) = self.lngraph.get(&key)
                && g.timestamp <= stored {
                    continue;
                }
            self.lngraph.insert(key, g.timestamp);
            self.metrics.record_first_seen(self.id, g, cx.time());
            self.pending.push(*g);
        }
    }

    /// Origination input. Originated messages still wait for the next
    /// stagger tick (they don't bypass it like CLN's do), but they're
    /// pushed to the *front* of the pending queue so they go out in the
    /// first chunk of the next tick — ahead of forwarded messages and
    /// before any trickle delay applies. Matches LND's "give locally
    /// originated updates priority over re-broadcast traffic".
    /// Timestamp stamped from current sim time, bumped past any stored
    /// entry to maintain strict monotonicity.
    pub fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
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
        self.metrics.record_first_seen(self.id, &msg, cx.time());
        // Front of queue, not back — see method docstring.
        self.pending.insert(0, msg);
    }

    /// Drain pending into batches of `min_batch_size`. Send the first
    /// batch immediately, then schedule each subsequent batch at offset
    /// `i * trickle` from now (`i` starts at 1 for the second batch).
    #[nexosim(schedulable)]
    async fn tick(&mut self, _: (), cx: &Context<Self>) {
        if self.pending.is_empty() {
            return;
        }
        let drained = std::mem::take(&mut self.pending);
        let chunks: Vec<Vec<Gossip>> = drained
            .chunks(self.min_batch_size)
            .map(|c| c.to_vec())
            .collect();
        let mut iter = chunks.into_iter();
        if let Some(first) = iter.next() {
            self.out.send(WireMessage::Batch(first)).await;
        }
        for (i, chunk) in iter.enumerate() {
            let offset = self.trickle * (i as u32 + 1);
            cx.schedule_event(offset, schedulable!(Self::send_batch), chunk)
                .expect("schedule trickle batch");
        }
    }

    /// Trickled-batch send target. Identical body to ClnNode's tick,
    /// just invoked from the scheduler at trickle offsets.
    #[nexosim(schedulable)]
    async fn send_batch(&mut self, batch: Vec<Gossip>) {
        self.out.send(WireMessage::Batch(batch)).await;
    }
}
