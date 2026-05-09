//! First-seen tracking with bounded memory and BOLT 7-aware supersession.
//!
//! Every node holds a clone of the same `MetricsHandle` and calls
//! `record_first_seen(&Gossip)` whenever it sees a fresh
//! `(scid, direction, timestamp)` tuple. The handle does per-`(MsgId,
//! NodeId)` dedup internally, so node-side dedup decisions don't
//! double-count.
//!
//! ## Concurrency
//!
//! All shared state lives behind `scc::HashMap` / `scc::HashSet` (per-bucket
//! locking, no global mutex) plus a small set of `Atomic*` counters. The
//! per-recv hot path takes only fine-grained bucket locks and never blocks
//! the rest of the worker pool. The previous design — a single
//! `Arc<Mutex<Metrics>>` — serialized every `recv` across all worker
//! threads and was the cause of the post-BOLT-7 throughput regression.
//!
//! ## Design
//!
//! There are three things to keep in mind for each in-flight message:
//!
//! 1. **Per-MsgId in-flight tracking.** A `MsgInflight` is created when a
//!    new MsgId is first observed. It stores a flat `Vec<u64>` of length
//!    `n_nodes` (indexed by `NodeId`, `u64::MAX` = not seen) plus a
//!    coverage counter and the originator's first-seen time. As soon as
//!    coverage reaches `n_nodes` we sort the times, compute the
//!    configured percentiles, push a small `MsgStats` into `completed`,
//!    and drop the inflight. Memory is bounded by
//!    `concurrent_in_flight × n_nodes × 8 B`.
//!
//! 2. **BOLT 7 supersession.** When a record arrives with timestamp
//!    *strictly greater* than what we have stored for that
//!    `(scid, direction)`, every older MsgId for the same channel will
//!    never get more arrivals — peers will drop those at recv time. We
//!    eagerly finalize them (with whatever partial coverage they had)
//!    and update `latest_version`. Without this step a 1-hour Poisson
//!    run with a small SCID pool would leak ~`num_old_versions × n_nodes
//!    × 8 B` indefinitely.
//!
//! 3. **End-of-run drain.** `finalize_remaining` converts any messages
//!    still in-flight at the deadline into final stats, using whatever
//!    partial coverage they reached.
//!
//! Auxiliary `inflight_by_channel` maps `(scid, direction)` to the set
//! of MsgIds currently tracked for that channel, so supersession can
//! find old MsgIds in O(1) without scanning all in-flights.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use nexosim::time::MonotonicTime;

use crate::message::{Direction, Gossip, MsgId, NodeIdx, Scid};

const NOT_SEEN: u64 = u64::MAX;

pub struct Metrics {
    n_nodes: usize,
    percentiles: Vec<f64>,
    /// Per-MsgId in-flight tracking. The bucket lock from
    /// `entry_sync(id)` *is* the per-message critical section — no
    /// inner Mutex needed.
    in_flight: scc::HashMap<MsgId, MsgInflight>,
    /// Lookup index: `(scid, direction) → MsgIds currently in flight`.
    /// Used by the supersession path to find old versions to finalize.
    inflight_by_channel: scc::HashMap<(Scid, Direction), HashSet<MsgId>>,
    /// Latest timestamp the metric has *ever* observed for each channel.
    /// Stored as `u32` (a single `entry_sync` is the synchronization
    /// boundary; no atomic needed once we hold the bucket lock).
    latest_version: scc::HashMap<(Scid, Direction), u32>,
    /// MsgIds whose stats have already been finalized. Subsequent
    /// `record_first_seen` calls for these are silently dropped.
    finalized: scc::HashSet<MsgId>,
    /// Push-only stash of finalized message stats. Held under a Mutex
    /// because pushes happen only on finalization (rare relative to
    /// per-recv work).
    completed: Mutex<Vec<MsgStats>>,
    superseded_count: AtomicUsize,
    total_first_seen: AtomicUsize,
}

#[derive(Default)]
struct MsgInflight {
    /// `times[i] = NOT_SEEN` if node `i` hasn't seen this message yet,
    /// else the ns-since-EPOCH of its first-seen.
    times: Vec<u64>,
    coverage: usize,
    /// Earliest first-seen time across all nodes (== originator's time).
    origin_ns: u64,
    /// `(scid, direction)` of the message — kept on the in-flight so
    /// supersession/finalization paths can look up the matching entry
    /// in `inflight_by_channel` without having to re-derive it.
    #[allow(dead_code)]
    channel: (Scid, Direction),
}

#[derive(Clone)]
pub struct MetricsHandle(Arc<Metrics>);

/// Default exists only to satisfy `#[derive(Default)]` on the node
/// models (NeXosim's `Model` macro requires `Serialize + Deserialize`,
/// and we mark this field `#[serde(skip)]` for the obvious reason it
/// doesn't serialize). The instance returned here is unusable for real
/// recording — the runner always calls `MetricsHandle::new` once the
/// node count is known and clones that into every model.
impl Default for MetricsHandle {
    fn default() -> Self {
        Self::new(0, Vec::new())
    }
}

impl MetricsHandle {
    pub fn new(n_nodes: usize, percentiles: Vec<f64>) -> Self {
        Self(Arc::new(Metrics {
            n_nodes,
            percentiles,
            in_flight: scc::HashMap::default(),
            inflight_by_channel: scc::HashMap::default(),
            latest_version: scc::HashMap::default(),
            finalized: scc::HashSet::default(),
            completed: Mutex::new(Vec::new()),
            superseded_count: AtomicUsize::new(0),
            total_first_seen: AtomicUsize::new(0),
        }))
    }

    /// Idempotent over `(MsgId, NodeId)`. The metric also tracks BOLT 7
    /// supersession internally: a record with a timestamp strictly
    /// greater than what's stored for `(scid, direction)` triggers
    /// immediate finalization of any older-MsgId in-flights for that
    /// channel.
    pub fn record_first_seen(&self, node: NodeIdx, gossip: &Gossip, t: MonotonicTime) {
        let m = &*self.0;
        let ns = ns_since_epoch(t);
        let key = (gossip.scid, gossip.direction);

        // Early-out for already-finalized messages. Cheap concurrent
        // read on the finalized set.
        if m.finalized.contains_sync(&gossip.id) {
            return;
        }

        // Step 1: Supersession — atomically advance the channel's
        // latest_version. The thread that wins the advance owns the
        // drain of older MsgIds.
        let do_supersede = match m.latest_version.entry_sync(key) {
            scc::hash_map::Entry::Occupied(mut occ) => {
                let v = occ.get_mut();
                if gossip.timestamp > *v {
                    *v = gossip.timestamp;
                    true
                } else {
                    false
                }
            }
            scc::hash_map::Entry::Vacant(vac) => {
                vac.insert_entry(gossip.timestamp);
                true
            }
        };

        if do_supersede {
            // Take the set of in-flight MsgIds on this channel.
            let drained: Vec<MsgId> = m
                .inflight_by_channel
                .get_sync(&key)
                .map(|mut occ| std::mem::take(occ.get_mut()).into_iter().collect())
                .unwrap_or_default();
            for old_id in drained {
                if old_id == gossip.id {
                    continue; // we'll re-insert this one below
                }
                if let Some(occ) = m.in_flight.get_sync(&old_id) {
                    let m_owned = occ.remove();
                    let stats = finalize_msg(old_id, m_owned, &m.percentiles, m.n_nodes);
                    m.completed.lock().unwrap().push(stats);
                    let _ = m.finalized.insert_sync(old_id);
                    m.superseded_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Step 2: per-MsgId in-flight insert. The bucket lock from
        // entry_sync is our per-MsgId critical section. We do the mark
        // *and* the maybe-finalize in one atomic block so we never
        // race a finalize against a concurrent mark.
        let (was_new, completed_now) = {
            let entry = m.in_flight.entry_sync(gossip.id);
            let mut occ = entry.or_insert_with(|| MsgInflight::new(m.n_nodes, ns, key));
            let inflight = occ.get_mut();
            let slot = &mut inflight.times[node as usize];
            if *slot != NOT_SEEN {
                (false, false)
            } else {
                *slot = ns;
                inflight.coverage += 1;
                if ns < inflight.origin_ns {
                    inflight.origin_ns = ns;
                }
                let completed = inflight.coverage >= m.n_nodes;
                if completed {
                    let m_owned = occ.remove();
                    let stats = finalize_msg(gossip.id, m_owned, &m.percentiles, m.n_nodes);
                    m.completed.lock().unwrap().push(stats);
                    let _ = m.finalized.insert_sync(gossip.id);
                }
                (true, completed)
            }
        };

        if !was_new {
            return;
        }
        m.total_first_seen.fetch_add(1, Ordering::Relaxed);

        // Maintain the per-channel in-flight index. If the message
        // just completed, remove our entry; otherwise insert it. Both
        // ops are tolerant of concurrent supersession draining the
        // same set.
        if completed_now {
            let _ = m.inflight_by_channel.update_sync(&key, |_, set| {
                set.remove(&gossip.id);
            });
        } else {
            let entry = m.inflight_by_channel.entry_sync(key);
            let mut occ = entry.or_default();
            occ.get_mut().insert(gossip.id);
        }
    }

    pub fn total_first_seen(&self) -> usize {
        self.0.total_first_seen.load(Ordering::Relaxed)
    }

    /// Number of messages that finalized via supersession (an older
    /// channel version was killed by a newer one before reaching 100%
    /// coverage).
    pub fn superseded_count(&self) -> usize {
        self.0.superseded_count.load(Ordering::Relaxed)
    }

    /// Drain any messages that didn't reach 100% coverage and didn't
    /// get superseded. Call once at the end of a simulation, before
    /// reading `completed_stats`.
    pub fn finalize_remaining(&self) {
        let m = &*self.0;
        let percentiles = m.percentiles.clone();
        let n_nodes = m.n_nodes;
        // retain_sync with `false` removes each entry; we take ownership
        // of the value via mem::take (MsgInflight: Default).
        m.in_flight.retain_sync(|id, inflight| {
            let inflight_owned = std::mem::take(inflight);
            let stats = finalize_msg(*id, inflight_owned, &percentiles, n_nodes);
            m.completed.lock().unwrap().push(stats);
            let _ = m.finalized.insert_sync(*id);
            false
        });
        m.inflight_by_channel.clear_sync();
    }

    pub fn completed_stats(&self) -> Vec<MsgStats> {
        let mut v = self.0.completed.lock().unwrap().clone();
        v.sort_by_key(|s| s.id);
        v
    }
}

impl MsgInflight {
    fn new(n_nodes: usize, first_ns: u64, channel: (Scid, Direction)) -> Self {
        Self {
            times: vec![NOT_SEEN; n_nodes],
            coverage: 0,
            origin_ns: first_ns,
            channel,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MsgStats {
    pub id: MsgId,
    /// Number of nodes that ever recorded a first-seen for this message.
    /// Equals `n_nodes` for fully-converged messages; less for messages
    /// killed mid-spread by supersession.
    pub coverage: usize,
    /// Total nodes in the simulation — the denominator for the
    /// `percentiles` below.
    pub n_nodes: usize,
    pub origin_ns: u64,
    pub last_ns: u64,
    /// `(percentile_fraction, time_to_reach_percentile_from_origin)`.
    /// Each percentile `p` is interpreted *absolute* — the time at which
    /// at least `ceil(p * n_nodes)` nodes had received the message. If
    /// the message's coverage never reached that count, the time is
    /// `None`.
    pub percentiles: Vec<(f64, Option<Duration>)>,
}

fn finalize_msg(
    id: MsgId,
    inflight: MsgInflight,
    percentiles: &[f64],
    n_nodes: usize,
) -> MsgStats {
    let MsgInflight {
        times,
        coverage,
        origin_ns,
        channel: _,
    } = inflight;
    let mut sorted: Vec<u64> = times.into_iter().filter(|&t| t != NOT_SEEN).collect();
    sorted.sort_unstable();
    let last_ns = *sorted.last().unwrap_or(&origin_ns);
    let pcts: Vec<(f64, Option<Duration>)> = percentiles
        .iter()
        .map(|&p| {
            // Index is over n_nodes (absolute interpretation), not
            // coverage. If the message never reached that many nodes,
            // the percentile is undefined.
            let idx = pct_to_index(p, n_nodes);
            let val = if idx < coverage {
                Some(Duration::from_nanos(
                    sorted[idx].saturating_sub(origin_ns),
                ))
            } else {
                None
            };
            (p, val)
        })
        .collect();
    MsgStats {
        id,
        coverage,
        n_nodes,
        origin_ns,
        last_ns,
        percentiles: pcts,
    }
}

fn ns_since_epoch(t: MonotonicTime) -> u64 {
    let d: Duration = t.duration_since(MonotonicTime::EPOCH);
    d.as_nanos() as u64
}

fn pct_to_index(pct: f64, n: usize) -> usize {
    let p = pct.clamp(0.0, 1.0);
    if n == 0 {
        return 0;
    }
    let raw = (p * n as f64).ceil() as isize - 1;
    raw.max(0).min(n as isize - 1) as usize
}
