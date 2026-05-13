//! LND-style stagger node.
//!
//! Same lifecycle as [`super::cln::ClnNode`] (queue on recv/originate,
//! periodic tick, random first-tick offset, BOLT 7 dedup) plus two
//! refinements that match the LND defaults:
//!
//! * **min_batch_size** — lower bound on per-chunk size. The actual
//!   chunk size is computed dynamically per tick (see
//!   [`calculate_sub_batch_size`]) so all chunks fit inside the stagger
//!   window. Each chunk becomes one `WireMessage::Batch`.
//! * **trickle** — only the first chunk goes out immediately on the tick;
//!   chunk `i > 0` is scheduled for `now + i * trickle`. This spreads
//!   bandwidth across the stagger window instead of bursting it all at
//!   once, matching LND's "trickle out a few updates at a time" design.
//!
//! Sub-batch size is sized so that `n_chunks * trickle <= stagger`
//! whenever pending is large enough. Concretely:
//! `chunk = max(min_batch_size, ceil(pending * trickle / stagger))`.
//! With `stagger=90s, trickle=5s` you get at most 18 chunks per tick;
//! pending=360 → chunk=20 (18 chunks), pending=30 → chunk=10 (3 chunks).
//!
//! For a single in-flight message there is nothing to chunk and trickle
//! never engages — LND and CLN converge identically. The trickle path is
//! exercised by workloads with concurrent messages per tick (e.g.
//! `OneShotAll`, `PoissonRandom` at high rate).
//!
//! See [`super::flooding`] for notes on per-peer outputs and shared
//! `NodeState`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

use crate::message::{Gossip, GossipBatch, GossipKind, NodeId, NodeIdx, WireMessage};
use crate::metrics::{
    FIRST_SEEN_FORCE_FLUSH, FirstSeenEntry, MetricsHandle, PerNodeMetrics, ns_since_epoch,
};
use crate::state::{SharedNodeState, originate_stamp};

#[derive(Default, Serialize, Deserialize)]
pub struct LndNode {
    pub id: NodeId,
    pub idx: NodeIdx,
    pub outputs: Vec<Output<WireMessage>>,
    pub peer_ids: Vec<NodeId>,
    pub peer_id_to_local: HashMap<NodeId, usize>,
    stagger: Duration,
    /// Sampled offset of this node's first stagger tick — see
    /// `sim::sample_phase`.
    first_tick: Duration,
    /// Inter-chunk spread within a single stagger window.
    trickle: Duration,
    /// Each chunk sent on a tick contains at least this many gossips.
    min_batch_size: usize,
    /// How long until end of run; used to schedule the final
    /// `flush_summary`.
    run_duration: Duration,
    flush_interval: Duration,
    flush_phase: Duration,
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
    pending: Vec<Gossip>,
}

impl LndNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        idx: NodeIdx,
        stagger: Duration,
        first_tick: Duration,
        trickle: Duration,
        min_batch_size: usize,
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
            stagger,
            first_tick,
            trickle,
            min_batch_size: min_batch_size.max(1),
            run_duration,
            flush_interval,
            flush_phase,
            state,
            peer_states,
            metrics_local: PerNodeMetrics::default(),
            metrics,
            pending: Vec::new(),
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
impl LndNode {
    /// Arm the periodic stagger tick + the periodic and final
    /// `flush_summary` schedules.
    #[nexosim(init)]
    async fn arm_ticks(&mut self, cx: &Context<Self>) {
        cx.schedule_periodic_event(self.first_tick, self.stagger, schedulable!(Self::tick), ())
            .expect("schedule lnd tick");
        if self.flush_interval > Duration::ZERO {
            cx.schedule_periodic_event(
                self.flush_phase,
                self.flush_interval,
                schedulable!(Self::flush_summary),
                (),
            )
            .expect("schedule periodic lnd flush_summary");
        }
        cx.schedule_event(self.run_duration, schedulable!(Self::flush_summary), ())
            .expect("schedule lnd final flush_summary");
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

    /// Input port. BOLT 7 per-kind dedup, then queue.
    pub async fn recv(&mut self, wire: WireMessage, cx: &Context<Self>) {
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
            WireMessage::Sketch(_) => {}
        }
    }

    /// Originated messages still wait for the next stagger tick (they
    /// don't bypass it like CLN's do), but they're pushed to the
    /// *front* of the pending queue so they go out in the first chunk
    /// of the next tick — ahead of forwarded messages and before any
    /// trickle delay applies. Matches LND's "give locally originated
    /// updates priority over re-broadcast traffic".
    pub fn originate(&mut self, mut msg: Gossip, cx: &Context<Self>) {
        if !originate_stamp(&self.state, self.id, &mut msg, cx.time()) {
            return;
        }
        self.metrics_local.first_seen_pending.push(FirstSeenEntry {
            gossip: msg,
            time_ns: ns_since_epoch(cx.time()),
        });
        self.metrics.bump_first_seen_count(1);
        // Front of queue, not back — see method docstring.
        self.pending.insert(0, msg);
    }

    /// Drain pending into batches sized so the trickle-out finishes
    /// inside the stagger window. Send the first batch immediately,
    /// then schedule each subsequent batch at offset `i * trickle` from
    /// now (`i` starts at 1 for the second batch).
    #[nexosim(schedulable)]
    async fn tick(&mut self, _: (), cx: &Context<Self>) {
        if self.pending.is_empty() {
            return;
        }
        let drained = std::mem::take(&mut self.pending);
        let sub = calculate_sub_batch_size(
            self.stagger,
            self.trickle,
            self.min_batch_size,
            drained.len(),
        );
        let chunks: Vec<Arc<GossipBatch>> = drained
            .chunks(sub)
            .map(|c| Arc::new(GossipBatch::from_mixed(c.to_vec())))
            .collect();
        let mut iter = chunks.into_iter();
        if let Some(first) = iter.next() {
            self.broadcast_arc(first).await;
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
    async fn send_batch(&mut self, batch: Arc<GossipBatch>) {
        self.broadcast_arc(batch).await;
    }

    async fn broadcast_arc(&mut self, batch: Arc<GossipBatch>) {
        let bytes_per_peer: u64 = batch.wire_size();
        let n_peers = self.outputs.len() as u64;
        self.metrics_local.bytes_out_gossip += bytes_per_peer * n_peers;
        for out in &mut self.outputs {
            out.send(WireMessage::Batch(batch.clone())).await;
        }
    }
}

impl LndNode {
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
        let mut fresh_count: usize = 0;
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
                    self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                        gossip: *g,
                        time_ns: now_ns,
                    });
                    self.pending.push(*g);
                    fresh_count += 1;
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        if fresh_count > 0 {
            self.metrics.bump_first_seen_count(fresh_count);
            self.maybe_force_flush(cx);
        }
    }

    fn absorb_node_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh_count: usize = 0;
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
                    self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                        gossip: *g,
                        time_ns: now_ns,
                    });
                    self.pending.push(*g);
                    fresh_count += 1;
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        if fresh_count > 0 {
            self.metrics.bump_first_seen_count(fresh_count);
            self.maybe_force_flush(cx);
        }
    }

    fn absorb_chan_anns(&mut self, gs: &[Gossip], cx: &Context<Self>) {
        if gs.is_empty() {
            return;
        }
        let now_ns = ns_since_epoch(cx.time());
        let mut fresh_count: usize = 0;
        {
            let mut m = self.state.chan_anns.write();
            for g in gs {
                let scid = g.scid.expect("ChannelAnnouncement must carry scid");
                if m.insert(scid, g.size_bytes).is_none() {
                    self.metrics_local.first_seen_pending.push(FirstSeenEntry {
                        gossip: *g,
                        time_ns: now_ns,
                    });
                    self.pending.push(*g);
                    fresh_count += 1;
                } else {
                    self.metrics_local.duplicates += 1;
                }
            }
        }
        if fresh_count > 0 {
            self.metrics.bump_first_seen_count(fresh_count);
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
}

/// LND's per-tick sub-batch sizing. Mirrors `calculateSubBatchSize` from
/// the Go LND reference (`discovery/sync_manager.go`).
///
/// Returns the chunk size to use when partitioning `batch_size` items
/// across the stagger window. The result satisfies:
///
/// * `chunk >= minimum_batch_size`, and
/// * `ceil(batch_size / chunk) * sub_batch_delay <= total_delay`
///   whenever `batch_size > minimum_batch_size`,
///
/// so the trickle-out for one tick never overflows the next stagger
/// window. Edge case: if `sub_batch_delay >= total_delay`, sub-batching
/// would be pointless (every chunk's slot exceeds the window) — return
/// the whole batch as one chunk.
fn calculate_sub_batch_size(
    total_delay: Duration,
    sub_batch_delay: Duration,
    minimum_batch_size: usize,
    batch_size: usize,
) -> usize {
    if sub_batch_delay >= total_delay {
        return batch_size;
    }
    let total = total_delay.as_secs();
    let sub = sub_batch_delay.as_secs();
    // ceil(batch_size * sub / total) using integer arithmetic.
    let computed = (batch_size as u64 * sub).div_ceil(total) as usize;
    computed.max(minimum_batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference values from the Go LND implementation. With
    /// stagger=90s, trickle=5s, min=10:
    ///
    /// * pending=360 → chunk=20 (18 sub-batches over 85s)
    /// * pending=30  → chunk=10 (3 sub-batches over 10s)
    /// * pending=2   → chunk=10 (1 sub-batch, the whole pending)
    #[test]
    fn calculate_sub_batch_size_matches_lnd() {
        let total = Duration::from_secs(90);
        let sub = Duration::from_secs(5);
        let min = 10;
        assert_eq!(calculate_sub_batch_size(total, sub, min, 360), 20);
        assert_eq!(calculate_sub_batch_size(total, sub, min, 30), 10);
        assert_eq!(calculate_sub_batch_size(total, sub, min, 2), 10);
    }

    #[test]
    fn calculate_sub_batch_size_no_subbatching_when_trickle_exceeds_stagger() {
        let total = Duration::from_secs(5);
        let sub = Duration::from_secs(10);
        assert_eq!(calculate_sub_batch_size(total, sub, 1, 100), 100);
    }

    #[test]
    fn calculate_sub_batch_size_clamps_at_minimum() {
        let total = Duration::from_secs(90);
        let sub = Duration::from_secs(5);
        // ceil(20 * 5 / 90) = 2, clamped to min=10.
        assert_eq!(calculate_sub_batch_size(total, sub, 10, 20), 10);
    }
}
