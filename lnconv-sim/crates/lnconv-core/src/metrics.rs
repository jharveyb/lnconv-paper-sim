//! First-seen tracking with bounded memory and BOLT 7-aware supersession.
//!
//! Every node holds a clone of the same `MetricsHandle` (an
//! `Arc<Mutex<...>>`) and calls `record_first_seen(&Gossip)` whenever it
//! sees a fresh `(scid, direction, timestamp)` tuple. The handle does
//! per-`(MsgId, NodeId)` dedup internally, so node-side dedup decisions
//! don't double-count.
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
//!    `concurrent_in_flight × n_nodes × 8 B`, not `total_events × 24 B`.
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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexosim::time::MonotonicTime;

use crate::message::{Direction, Gossip, MsgId, NodeId, Scid};

const NOT_SEEN: u64 = u64::MAX;

pub struct Metrics {
    n_nodes: usize,
    percentiles: Vec<f64>,
    in_flight: HashMap<MsgId, MsgInflight>,
    completed: Vec<MsgStats>,
    /// Latest timestamp the metric has *ever* observed for each channel.
    /// Used to detect supersession.
    latest_version: HashMap<(Scid, Direction), u32>,
    /// MsgIds currently in-flight for each channel — drained on
    /// supersession or full-coverage finalization.
    inflight_by_channel: HashMap<(Scid, Direction), HashSet<MsgId>>,
    /// MsgIds whose stats have already been finalized. Subsequent
    /// `record_first_seen` calls for these are silently dropped.
    finalized: HashSet<MsgId>,
    /// Number of messages finalized via supersession (i.e. coverage
    /// stopped accumulating because a newer version of the channel
    /// arrived). Reported at end of run.
    superseded_count: usize,
    total_first_seen: usize,
}

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
pub struct MetricsHandle(Arc<Mutex<Metrics>>);

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
        Self(Arc::new(Mutex::new(Metrics {
            n_nodes,
            percentiles,
            in_flight: HashMap::new(),
            completed: Vec::new(),
            latest_version: HashMap::new(),
            inflight_by_channel: HashMap::new(),
            finalized: HashSet::new(),
            superseded_count: 0,
            total_first_seen: 0,
        })))
    }

    /// Idempotent over `(MsgId, NodeId)`. The metric also tracks BOLT 7
    /// supersession internally: a record with a timestamp strictly
    /// greater than what's stored for `(scid, direction)` triggers
    /// immediate finalization of any older-MsgId in-flights for that
    /// channel.
    pub fn record_first_seen(&self, node: NodeId, gossip: &Gossip, t: MonotonicTime) {
        let ns = ns_since_epoch(t);
        let mut g = self.0.lock().unwrap();
        if g.finalized.contains(&gossip.id) {
            return;
        }
        let n_nodes = g.n_nodes;
        let key = (gossip.scid, gossip.direction);

        // Step 1: supersession check. If this record's timestamp beats
        // the network-wide latest for the channel, finalize all older
        // in-flight MsgIds on the channel before tracking this one.
        let do_supersede = match g.latest_version.get(&key) {
            Some(&stored) => gossip.timestamp > stored,
            None => true,
        };
        if do_supersede {
            // Take the set of in-flight MsgIds on this channel.
            let drained: Vec<MsgId> = g
                .inflight_by_channel
                .remove(&key)
                .map(|s| s.into_iter().collect())
                .unwrap_or_default();
            let percentiles = g.percentiles.clone();
            for old_id in drained {
                if old_id == gossip.id {
                    continue; // we'll re-insert this one below
                }
                if let Some(inflight) = g.in_flight.remove(&old_id) {
                    g.completed
                        .push(finalize_msg(old_id, inflight, &percentiles, n_nodes));
                    g.finalized.insert(old_id);
                    g.superseded_count += 1;
                }
            }
            g.latest_version.insert(key, gossip.timestamp);
        }

        // Step 2: per-MsgId in-flight insert (drop borrow before
        // touching `g` again to bump counters or remove the entry).
        let (was_new, completed) = {
            let inflight = g
                .in_flight
                .entry(gossip.id)
                .or_insert_with(|| MsgInflight::new(n_nodes, ns, key));
            let slot = &mut inflight.times[node as usize];
            if *slot != NOT_SEEN {
                (false, false)
            } else {
                *slot = ns;
                inflight.coverage += 1;
                if ns < inflight.origin_ns {
                    inflight.origin_ns = ns;
                }
                (true, inflight.coverage >= n_nodes)
            }
        };
        if !was_new {
            return;
        }
        g.total_first_seen += 1;
        g.inflight_by_channel
            .entry(key)
            .or_default()
            .insert(gossip.id);
        if completed {
            let inflight = g.in_flight.remove(&gossip.id).unwrap();
            // Remove from per-channel index.
            if let Some(set) = g.inflight_by_channel.get_mut(&key) {
                set.remove(&gossip.id);
                if set.is_empty() {
                    g.inflight_by_channel.remove(&key);
                }
            }
            let percentiles = g.percentiles.clone();
            g.completed.push(finalize_msg(gossip.id, inflight, &percentiles, n_nodes));
            g.finalized.insert(gossip.id);
        }
    }

    pub fn total_first_seen(&self) -> usize {
        self.0.lock().unwrap().total_first_seen
    }

    /// Number of messages that finalized via supersession (an older
    /// channel version was killed by a newer one before reaching 100%
    /// coverage).
    pub fn superseded_count(&self) -> usize {
        self.0.lock().unwrap().superseded_count
    }

    /// Drain any messages that didn't reach 100% coverage and didn't
    /// get superseded. Call once at the end of a simulation, before
    /// reading `completed_stats`.
    pub fn finalize_remaining(&self) {
        let mut g = self.0.lock().unwrap();
        let percentiles = g.percentiles.clone();
        let n_nodes = g.n_nodes;
        let drained: Vec<(MsgId, MsgInflight)> = g.in_flight.drain().collect();
        for (id, inflight) in drained {
            g.completed
                .push(finalize_msg(id, inflight, &percentiles, n_nodes));
            g.finalized.insert(id);
        }
        g.inflight_by_channel.clear();
    }

    pub fn completed_stats(&self) -> Vec<MsgStats> {
        let mut v = self.0.lock().unwrap().completed.clone();
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
