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
//! All shared state lives behind `scc::HashMap` / `scc::HashSet`
//! (per-bucket locking, no global mutex) plus a small set of `Atomic*`
//! counters. Per-MsgId in-flight state is wrapped in
//! `Arc<MsgInflight>` and the inner `times` / `coverage` / `origin_ns`
//! fields are themselves atomic — so the bucket lock is held only for
//! the brief get-or-create + finalise paths, never for the per-slot
//! mark-and-bump that runs on every `recv`. This is what unlocks
//! per-MsgId parallelism: N worker threads recording N different
//! `(msg, node)` slots make N completely-independent atomic stores.
//!
//! ## Design
//!
//! There are three things to keep in mind for each in-flight message:
//!
//! 1. **Per-MsgId in-flight tracking.** A `MsgInflight` is created
//!    when a new MsgId is first observed. It stores `Vec<AtomicU64>`
//!    of length `n_nodes` (indexed by `NodeIdx`, `u64::MAX` = not
//!    seen) plus an `AtomicUsize` coverage counter and an `AtomicU64`
//!    origin-time min. As soon as coverage reaches `n_nodes` we sort
//!    the times, compute the configured percentiles, push a small
//!    `MsgStats` into `completed`, and drop the inflight. Memory is
//!    bounded by `concurrent_in_flight × n_nodes × 8 B`.
//!
//! 2. **BOLT 7 supersession.** When a record arrives with timestamp
//!    *strictly greater* than what we have stored for that
//!    `(scid, direction)`, every older MsgId for the same channel
//!    will never get more arrivals — peers will drop those at recv
//!    time. We eagerly finalize them (with whatever partial coverage
//!    they had) and update `latest_version`. Without this step a
//!    1-hour Poisson run with a small SCID pool would leak
//!    ~`num_old_versions × n_nodes × 8 B` indefinitely.
//!
//! 3. **End-of-run drain.** `finalize_remaining` converts any
//!    messages still in-flight at the deadline into final stats,
//!    using whatever partial coverage they reached.
//!
//! Auxiliary `inflight_by_channel` maps `(scid, direction)` to the
//! set of MsgIds currently tracked for that channel, so supersession
//! can find old MsgIds in O(1) without scanning all in-flights.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use nexosim::time::MonotonicTime;

use crate::message::{Direction, Gossip, MsgId, NodeIdx, Scid};

const NOT_SEEN: u64 = u64::MAX;

pub struct Metrics {
    n_nodes: usize,
    percentiles: Vec<f64>,
    /// Per-MsgId in-flight tracking. Wrapped in `Arc` so workers can
    /// keep a clone alive while the bucket lock from `entry_sync` is
    /// already released — that releases the per-MsgId-bottleneck on
    /// the per-slot CAS path.
    in_flight: scc::HashMap<MsgId, Arc<MsgInflight>>,
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
    /// `times[i] == NOT_SEEN` if node `i` hasn't seen this message
    /// yet, else the ns-since-EPOCH of its first-seen. Per-slot
    /// `compare_exchange(NOT_SEEN, ns)` is the dedup primitive — the
    /// thread that wins the CAS is the unique recorder for `(msg, i)`.
    times: Vec<AtomicU64>,
    /// Number of slots successfully claimed via the CAS above. The
    /// thread whose `fetch_add` returns `n_nodes - 1` is the unique
    /// completer.
    coverage: AtomicUsize,
    /// Earliest first-seen time across all nodes (== originator's
    /// time). Updated via a CAS-min loop.
    origin_ns: AtomicU64,
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
                if let Some((_, m_owned_arc)) = m.in_flight.remove_sync(&old_id) {
                    let inflight_owned = unwrap_or_snapshot(m_owned_arc);
                    let stats =
                        finalize_msg(old_id, inflight_owned, &m.percentiles, m.n_nodes);
                    m.completed.lock().unwrap().push(stats);
                    let _ = m.finalized.insert_sync(old_id);
                    m.superseded_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Step 2: get-or-create the per-MsgId Arc<MsgInflight>. The
        // bucket lock from entry_sync is held only for the duration
        // of the get-or-insert; we clone the Arc out and the guard
        // is dropped at the end of the scope.
        let inflight: Arc<MsgInflight> = {
            let entry = m.in_flight.entry_sync(gossip.id);
            let occ = entry.or_insert_with(|| {
                Arc::new(MsgInflight::new(m.n_nodes, ns, key))
            });
            occ.get().clone()
        };

        // Step 3: lock-free per-slot store. Whoever wins the CAS is
        // the unique recorder for this `(msg, node)` pair — every
        // other thread sees the slot as already taken and bails.
        if inflight.times[node as usize]
            .compare_exchange(NOT_SEEN, ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        // Step 4: update origin_ns to the running min (CAS loop) and
        // bump coverage. The thread whose fetch_add returns
        // `n_nodes - 1` is the unique completer.
        let mut cur_origin = inflight.origin_ns.load(Ordering::Relaxed);
        while ns < cur_origin {
            match inflight.origin_ns.compare_exchange_weak(
                cur_origin,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur_origin = c,
            }
        }
        let new_coverage = inflight.coverage.fetch_add(1, Ordering::Relaxed) + 1;
        m.total_first_seen.fetch_add(1, Ordering::Relaxed);

        if new_coverage == m.n_nodes {
            // Sole completer: pull the entry from in_flight, finalize,
            // record finalized.
            if let Some((_, m_owned_arc)) = m.in_flight.remove_sync(&gossip.id) {
                let inflight_owned = unwrap_or_snapshot(m_owned_arc);
                let stats = finalize_msg(gossip.id, inflight_owned, &m.percentiles, m.n_nodes);
                m.completed.lock().unwrap().push(stats);
                let _ = m.finalized.insert_sync(gossip.id);
            }
            // Maintain the per-channel index.
            let _ = m.inflight_by_channel.update_sync(&key, |_, set| {
                set.remove(&gossip.id);
            });
        } else {
            // First-seen on this slot but message hasn't completed
            // yet — make sure the per-channel index has us listed.
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
        // retain_sync over scc::HashMap: false ⇒ remove. Each entry
        // is finalised inline; we replace the Arc with a cheap
        // placeholder so we can take ownership without scc borrow
        // contention.
        m.in_flight.retain_sync(|id, inflight_arc_slot| {
            // Defensive: skip if already finalised by the rare race
            // window between get-or-create and supersession.
            if m.finalized.contains_sync(id) {
                return false;
            }
            let placeholder = Arc::new(MsgInflight::new(0, 0, (0, 0)));
            let inflight_arc = std::mem::replace(inflight_arc_slot, placeholder);
            let inflight_owned = unwrap_or_snapshot(inflight_arc);
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
        let mut times = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            times.push(AtomicU64::new(NOT_SEEN));
        }
        Self {
            times,
            coverage: AtomicUsize::new(0),
            origin_ns: AtomicU64::new(first_ns),
            channel,
        }
    }

    /// Snapshot all atomic fields into a fresh, owned `MsgInflight`.
    /// Used on the rare path where another worker still holds an Arc
    /// clone when the completer/superseder needs to finalise. The
    /// returned value has fresh atomics carrying the snapshot values.
    fn snapshot(&self) -> Self {
        let times = self
            .times
            .iter()
            .map(|a| AtomicU64::new(a.load(Ordering::Relaxed)))
            .collect();
        Self {
            times,
            coverage: AtomicUsize::new(self.coverage.load(Ordering::Relaxed)),
            origin_ns: AtomicU64::new(self.origin_ns.load(Ordering::Relaxed)),
            channel: self.channel,
        }
    }
}

/// Try to unwrap an `Arc<MsgInflight>` exclusively; on the rare race
/// where another worker still holds a clone, snapshot the contents
/// instead.
fn unwrap_or_snapshot(arc: Arc<MsgInflight>) -> MsgInflight {
    Arc::try_unwrap(arc).unwrap_or_else(|a| a.snapshot())
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
    let coverage = coverage.into_inner();
    let origin_ns = origin_ns.into_inner();
    let mut sorted: Vec<u64> = times
        .into_iter()
        .map(|a| a.into_inner())
        .filter(|&t| t != NOT_SEEN)
        .collect();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc as StdArc;
    use std::thread;

    fn dummy_gossip(id: MsgId, ts: u32) -> Gossip {
        Gossip {
            id,
            origin: 0,
            kind: crate::message::GossipKind::ChannelUpdate,
            size_bytes: 0,
            scid: 1,
            direction: 0,
            timestamp: ts,
        }
    }

    /// Many threads all racing on the same (msg, node) slot — only
    /// one of them should observe a fresh first-seen. The remaining
    /// CAS losers must early-return without touching coverage or
    /// total_first_seen.
    #[test]
    fn concurrent_record_does_not_double_count() {
        let metrics = MetricsHandle::new(4, vec![1.0]);
        let g = dummy_gossip(7, 100);
        let t = MonotonicTime::EPOCH + Duration::from_micros(50);

        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = metrics.clone();
            handles.push(thread::spawn(move || {
                m.record_first_seen(0, &g, t);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // n_nodes=4: only node 0 was recorded. coverage should be 1.
        assert_eq!(metrics.total_first_seen(), 1);
    }

    /// N threads recording N distinct nodes for the same message ⇒
    /// the message finalises exactly once and lands in completed_stats.
    #[test]
    fn concurrent_complete_finalises_once() {
        let n_nodes: usize = 32;
        let metrics = MetricsHandle::new(n_nodes, vec![1.0]);
        let g = StdArc::new(dummy_gossip(11, 200));
        let mut handles = Vec::new();
        for i in 0..n_nodes {
            let m = metrics.clone();
            let g = g.clone();
            handles.push(thread::spawn(move || {
                let t = MonotonicTime::EPOCH + Duration::from_micros(i as u64);
                m.record_first_seen(i as NodeIdx, &g, t);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let stats = metrics.completed_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].id, 11);
        assert_eq!(stats[0].coverage, n_nodes);
        assert_eq!(metrics.total_first_seen(), n_nodes);
    }
}
