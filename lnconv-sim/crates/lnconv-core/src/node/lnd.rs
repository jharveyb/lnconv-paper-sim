//! LND-style stagger node.
//!
//! Same lifecycle as [`super::cln::ClnNode`] (queue on recv/originate,
//! periodic tick, random first-tick offset for phase mixing) plus two
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

use std::collections::HashSet;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

use crate::message::{Gossip, MsgId, NodeId, WireMessage};
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
    seen: HashSet<MsgId>,
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
            seen: HashSet::new(),
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

    /// Input port. Queue any new gossip; sending is deferred to `tick`.
    pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
        for g in wire.iter_gossips() {
            if !self.seen.insert(g.id) {
                continue;
            }
            self.metrics.record_first_seen(self.id, g.id, cx.time());
            self.pending.push(g.clone());
        }
    }

    /// Origination input. Like CLN, originated messages wait for the
    /// next tick rather than going out immediately.
    pub fn originate(&mut self, msg: Gossip, cx: &Context<Self>) {
        if !self.seen.insert(msg.id) {
            return;
        }
        self.metrics.record_first_seen(self.id, msg.id, cx.time());
        self.pending.push(msg);
    }

    /// Drain pending into batches of `min_batch_size`. Send the first
    /// batch immediately, then schedule each subsequent batch at offset
    /// `i * trickle` from now (`i` starts at 1 for the second batch).
    /// Schedulable methods can take a `&Context<Self>` as their second
    /// arg, which is what we use here to call `schedule_event` from
    /// inside the tick.
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
