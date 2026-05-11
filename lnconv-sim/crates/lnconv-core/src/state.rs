//! Per-node gossip-state owned outside the model so other models /
//! the set-reconciliation protocol can read it concurrently.
//!
//! Each LN node keeps three dedup containers (`chan_updates`,
//! `node_anns`, `chan_anns`). For flooding/cln/lnd these used to live
//! as plain `HashMap`s on the model itself; only the model's own
//! `recv` could touch them. Set-reconciliation needs another node's
//! tick handler to compute a symmetric diff against this node's
//! state, so the storage moves out into [`NodeState`] — one per node,
//! shared via `Arc`.
//!
//! ## Per-kind sharded locking
//!
//! `NodeState` shards its three maps under three independent
//! `RwLock`s. A `chan_updates` write never blocks a `node_anns`
//! reader; a `chan_updates` sketch only takes the `chan_updates`
//! locks on each side. This matches the per-`SketchKind` exchange
//! semantics — there's no reason a sketch over one kind should
//! contend with traffic on another.
//!
//! ## Distribution at sim-init
//!
//! `sim::run` calls [`build_registry`] once to produce a
//! `Vec<SharedNodeState>` indexed by `NodeIdx`. While constructing
//! each model, the simulator hands it (a) its own `Arc<NodeState>`
//! and (b) a `Vec<Arc<NodeState>>` of *only its direct peers'*
//! states (aligned with the model's per-peer Output Vec). The local
//! registry vector is dropped after wiring; the per-node Arcs
//! survive via the model + its peers' references. Nodes never get a
//! handle to the global registry — contention scales with peer
//! degree, not network size.
//!
//! ## Diff semantics
//!
//! [`compute_diff`] returns BOTH:
//!
//! * **Strict-difference counts** (`a_only_count` / `b_only_count` /
//!   `intersection`) — same-key-different-ts pairs count as one
//!   element on each side. Used for sketch capacity-overflow check
//!   and per-direction metrics; matches how a real minisketch
//!   would decode the symmetric difference.
//! * **Newer-only Gossips** (`a_newer` / `b_newer`) — items where
//!   THIS side has the strictly-newer version. Used to build the
//!   wire reply Batch; stale-side entries are never sent (they're
//!   superseded by definition).
//!
//! Lock acquisition is in `NodeIdx`-min-first order to avoid
//! deadlock between two reconciliations on the same kind in
//! opposite directions.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use nexosim::time::MonotonicTime;

use crate::message::{
    Direction, Gossip, GossipKind, NodeId, NodeIdx, Scid, SketchKind,
};

/// Per-node dedup state. The three kinds each live behind their own
/// `RwLock` so a `chan_updates` write doesn't block a `node_anns`
/// reader. Each map's value embeds `size_bytes` so set-recon
/// replies can carry realistic on-the-wire byte counts.
///
/// `Default` exists only to satisfy the `#[derive(Default)]` on the
/// node `Model` structs (each holds a `SharedNodeState`); a default
/// `NodeState` carries `idx = 0` and three empty maps and is never
/// observed at runtime — sim init replaces it with a real Arc from
/// the registry before any model spins up.
#[derive(Default)]
pub struct NodeState {
    pub idx: NodeIdx, // for lock-order tie-breaking
    pub chan_updates: RwLock<HashMap<(Scid, Direction), (u32, u16)>>,
    pub node_anns: RwLock<HashMap<NodeId, (u32, u16)>>,
    pub chan_anns: RwLock<HashMap<Scid, u16>>,
}

pub type SharedNodeState = Arc<NodeState>;

impl NodeState {
    pub fn new(idx: NodeIdx) -> Arc<Self> {
        Arc::new(Self {
            idx,
            chan_updates: RwLock::new(HashMap::new()),
            node_anns: RwLock::new(HashMap::new()),
            chan_anns: RwLock::new(HashMap::new()),
        })
    }
}

/// Build one `Arc<NodeState>` per node. The returned `Vec` is the
/// transient construction-time index used by `sim::run` to hand each
/// model its own state Arc plus an aligned `Vec<Arc<NodeState>>` of
/// its peers' states.
pub fn build_registry(n: usize) -> Vec<SharedNodeState> {
    (0..n).map(|i| NodeState::new(i as NodeIdx)).collect()
}

/// Result of a symmetric-diff computation. See module docstring for
/// the two-quantity model: counts for the capacity/metrics check,
/// newer-only Gossips for the wire reply.
pub struct DiffResult {
    /// Number of items present in `a` but absent (or under a
    /// different ts) in `b`. Includes `a`'s stale entries against
    /// newer `b` entries.
    pub a_only_count: usize,
    /// Number of items present in `b` but absent (or under a
    /// different ts) in `a`. Includes `b`'s stale entries against
    /// newer `a` entries.
    pub b_only_count: usize,
    /// Items where both sides have the same key with the same `ts`
    /// (or, for `chan_anns`, same key — no `ts`).
    pub intersection: usize,
    /// Subset of the diff that is **strictly newer on `a`'s side**
    /// (i.e. `a` has a more recent `ts` than `b`, or `b` is
    /// missing it). Eligible to send to `b` in a reply Batch.
    pub a_newer: Vec<Gossip>,
    /// Subset of the diff that is **strictly newer on `b`'s side**.
    /// What `b` would send back to `a` in response to `a`'s sketch.
    pub b_newer: Vec<Gossip>,
}

/// Compute the symmetric diff between `a` and `b` over `kind`.
/// Acquires each side's kind-specific `RwLock` in NodeIdx-min-first
/// order to avoid deadlock with another in-flight reconciliation
/// in the opposite direction. The other two kinds' locks are never
/// touched — they remain fully concurrent.
pub fn compute_diff(
    a: &SharedNodeState,
    b: &SharedNodeState,
    kind: SketchKind,
) -> DiffResult {
    let (first, second, swapped) = if a.idx <= b.idx {
        (a, b, false)
    } else {
        (b, a, true)
    };

    match kind {
        SketchKind::ChanUpdates => {
            let g_first = first.chan_updates.read().expect("chan_updates poisoned");
            let g_second = second.chan_updates.read().expect("chan_updates poisoned");
            let (la, lb) = if swapped {
                (&*g_second, &*g_first)
            } else {
                (&*g_first, &*g_second)
            };
            diff_chan_updates(la, lb)
        }
        SketchKind::NodeAnns => {
            let g_first = first.node_anns.read().expect("node_anns poisoned");
            let g_second = second.node_anns.read().expect("node_anns poisoned");
            let (la, lb) = if swapped {
                (&*g_second, &*g_first)
            } else {
                (&*g_first, &*g_second)
            };
            diff_node_anns(la, lb)
        }
        SketchKind::ChanAnns => {
            let g_first = first.chan_anns.read().expect("chan_anns poisoned");
            let g_second = second.chan_anns.read().expect("chan_anns poisoned");
            let (la, lb) = if swapped {
                (&*g_second, &*g_first)
            } else {
                (&*g_first, &*g_second)
            };
            diff_chan_anns(la, lb)
        }
    }
}

fn diff_chan_updates(
    la: &HashMap<(Scid, Direction), (u32, u16)>,
    lb: &HashMap<(Scid, Direction), (u32, u16)>,
) -> DiffResult {
    let mut a_only_count = 0usize;
    let mut b_only_count = 0usize;
    let mut intersection = 0usize;
    let mut a_newer = Vec::new();
    let mut b_newer = Vec::new();
    for ((scid, dir), (ts_a, size_a)) in la {
        match lb.get(&(*scid, *dir)) {
            Some((ts_b, _)) if ts_b == ts_a => {
                intersection += 1;
            }
            Some((ts_b, _)) => {
                // Both sides have the key under different ts —
                // counts on both sides; the newer one is eligible
                // for its owner's reply.
                a_only_count += 1;
                b_only_count += 1;
                if ts_a > ts_b {
                    a_newer.push(synth_chan_update(*scid, *dir, *ts_a, *size_a));
                }
            }
            None => {
                // Only a has it ⇒ strictly newer than b's "nothing".
                a_only_count += 1;
                a_newer.push(synth_chan_update(*scid, *dir, *ts_a, *size_a));
            }
        }
    }
    for ((scid, dir), (ts_b, size_b)) in lb {
        match la.get(&(*scid, *dir)) {
            Some((ts_a, _)) if ts_a == ts_b => {
                // Intersection already counted.
            }
            Some((ts_a, _)) => {
                // Same key, different ts — a_only/b_only counts
                // and the b-newer push have already been handled
                // by the first loop's matching branch. Re-check
                // only b_newer here.
                if ts_b > ts_a {
                    b_newer.push(synth_chan_update(*scid, *dir, *ts_b, *size_b));
                }
            }
            None => {
                b_only_count += 1;
                b_newer.push(synth_chan_update(*scid, *dir, *ts_b, *size_b));
            }
        }
    }
    DiffResult {
        a_only_count,
        b_only_count,
        intersection,
        a_newer,
        b_newer,
    }
}

fn diff_node_anns(
    la: &HashMap<NodeId, (u32, u16)>,
    lb: &HashMap<NodeId, (u32, u16)>,
) -> DiffResult {
    let mut a_only_count = 0usize;
    let mut b_only_count = 0usize;
    let mut intersection = 0usize;
    let mut a_newer = Vec::new();
    let mut b_newer = Vec::new();
    for (origin, (ts_a, size_a)) in la {
        match lb.get(origin) {
            Some((ts_b, _)) if ts_b == ts_a => intersection += 1,
            Some((ts_b, _)) => {
                a_only_count += 1;
                b_only_count += 1;
                if ts_a > ts_b {
                    a_newer.push(synth_node_ann(*origin, *ts_a, *size_a));
                }
            }
            None => {
                a_only_count += 1;
                a_newer.push(synth_node_ann(*origin, *ts_a, *size_a));
            }
        }
    }
    for (origin, (ts_b, size_b)) in lb {
        match la.get(origin) {
            Some((ts_a, _)) if ts_a == ts_b => {}
            Some((ts_a, _)) => {
                if ts_b > ts_a {
                    b_newer.push(synth_node_ann(*origin, *ts_b, *size_b));
                }
            }
            None => {
                b_only_count += 1;
                b_newer.push(synth_node_ann(*origin, *ts_b, *size_b));
            }
        }
    }
    DiffResult {
        a_only_count,
        b_only_count,
        intersection,
        a_newer,
        b_newer,
    }
}

fn diff_chan_anns(la: &HashMap<Scid, u16>, lb: &HashMap<Scid, u16>) -> DiffResult {
    let ka: HashSet<&Scid> = la.keys().collect();
    let kb: HashSet<&Scid> = lb.keys().collect();
    let intersection = ka.intersection(&kb).count();
    // No timestamp ⇒ strict counts and newer-only Vecs are
    // identical for chan_anns.
    let a_newer: Vec<Gossip> = ka
        .difference(&kb)
        .map(|&scid| synth_chan_ann(*scid, *la.get(scid).unwrap()))
        .collect();
    let b_newer: Vec<Gossip> = kb
        .difference(&ka)
        .map(|&scid| synth_chan_ann(*scid, *lb.get(scid).unwrap()))
        .collect();
    let a_only_count = a_newer.len();
    let b_only_count = b_newer.len();
    DiffResult {
        a_only_count,
        b_only_count,
        intersection,
        a_newer,
        b_newer,
    }
}

/// Stamp the originator's per-kind state and finalise `msg`'s
/// timestamp + id. Returns `true` if the message should be
/// broadcast, `false` if it was dropped (e.g. duplicate
/// channel_announcement). Shared across every node kind so the
/// origin/timestamp/MsgId conventions stay aligned.
///
/// `self_id` is the originating node's `NodeId`. Used only to set
/// `msg.origin = Some(self_id)` for `NodeAnnouncement`. For
/// `ChannelUpdate` / `ChannelAnnouncement` the wire `origin` is
/// always `None` (BOLT 7 doesn't carry an origin on those kinds).
pub fn originate_stamp(
    state: &SharedNodeState,
    self_id: NodeId,
    msg: &mut Gossip,
    cx_time: MonotonicTime,
) -> bool {
    let now_secs = cx_time.duration_since(MonotonicTime::EPOCH).as_secs() as u32;
    match msg.kind {
        GossipKind::ChannelUpdate => {
            let scid = msg.scid.expect("ChannelUpdate must carry scid");
            msg.origin = None;
            let mut m = state.chan_updates.write().expect("chan_updates poisoned");
            let key = (scid, msg.direction);
            let next_ts = match m.get(&key) {
                Some((stored, _)) => stored.saturating_add(1).max(now_secs),
                None => now_secs,
            };
            msg.timestamp = next_ts;
            m.insert(key, (next_ts, msg.size_bytes));
        }
        GossipKind::NodeAnnouncement => {
            msg.origin = Some(self_id);
            let mut m = state.node_anns.write().expect("node_anns poisoned");
            let next_ts = match m.get(&self_id) {
                Some((stored, _)) => stored.saturating_add(1).max(now_secs),
                None => now_secs,
            };
            msg.timestamp = next_ts;
            m.insert(self_id, (next_ts, msg.size_bytes));
        }
        GossipKind::ChannelAnnouncement => {
            let scid = msg.scid.expect("ChannelAnnouncement must carry scid");
            msg.origin = None;
            let mut m = state.chan_anns.write().expect("chan_anns poisoned");
            if m.insert(scid, msg.size_bytes).is_some() {
                // Already broadcast this scid — drop.
                return false;
            }
        }
    }
    // Re-derive the stable MsgId now that timestamp/origin are final.
    msg.id = Gossip::derive_id(msg.origin, msg.kind, msg.scid, msg.direction, msg.timestamp);
    true
}

fn synth_chan_update(scid: Scid, direction: Direction, timestamp: u32, size_bytes: u16) -> Gossip {
    let origin = None;
    let scid_o = Some(scid);
    Gossip {
        id: Gossip::derive_id(origin, GossipKind::ChannelUpdate, scid_o, direction, timestamp),
        origin,
        kind: GossipKind::ChannelUpdate,
        size_bytes,
        scid: scid_o,
        direction,
        timestamp,
    }
}

fn synth_node_ann(origin_id: NodeId, timestamp: u32, size_bytes: u16) -> Gossip {
    let origin = Some(origin_id);
    let scid_o = None;
    Gossip {
        id: Gossip::derive_id(origin, GossipKind::NodeAnnouncement, scid_o, 0, timestamp),
        origin,
        kind: GossipKind::NodeAnnouncement,
        size_bytes,
        scid: scid_o,
        direction: 0,
        timestamp,
    }
}

fn synth_chan_ann(scid: Scid, size_bytes: u16) -> Gossip {
    let origin = None;
    let scid_o = Some(scid);
    Gossip {
        id: Gossip::derive_id(origin, GossipKind::ChannelAnnouncement, scid_o, 0, 0),
        origin,
        kind: GossipKind::ChannelAnnouncement,
        size_bytes,
        scid: scid_o,
        direction: 0,
        timestamp: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    fn st(idx: NodeIdx) -> SharedNodeState {
        NodeState::new(idx)
    }

    fn write_cu(s: &SharedNodeState, scid: Scid, dir: Direction, ts: u32, size: u16) {
        s.chan_updates
            .write()
            .unwrap()
            .insert((scid, dir), (ts, size));
    }

    fn write_na(s: &SharedNodeState, origin: NodeId, ts: u32, size: u16) {
        s.node_anns.write().unwrap().insert(origin, (ts, size));
    }

    fn write_ca(s: &SharedNodeState, scid: Scid, size: u16) {
        s.chan_anns.write().unwrap().insert(scid, size);
    }

    #[test]
    fn diff_chan_updates_keys_overlap() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&a, 100, 1, 20, 64);
        write_cu(&b, 100, 0, 10, 64);
        write_cu(&b, 200, 0, 30, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates);
        // (100,0,10) shared; (100,1,20) only in a; (200,0,30) only in b.
        assert_eq!(d.intersection, 1);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.a_newer[0].scid, Some(100));
        assert_eq!(d.a_newer[0].direction, 1);
        assert_eq!(d.b_newer[0].scid, Some(200));
    }

    /// Same-key-different-ts pair: counts go up on both sides, but
    /// only the newer-ts side appears in *_newer.
    #[test]
    fn diff_chan_updates_handles_timestamp_diff() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&b, 100, 0, 11, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates);
        assert_eq!(d.intersection, 0);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert!(d.a_newer.is_empty()); // a's ts=10 is stale
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].timestamp, 11);
    }

    /// Capacity check uses the strict counts (so a single
    /// stale/newer pair counts as 2 elements toward capacity).
    #[test]
    fn diff_chan_updates_capacity_counts_stale() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&b, 100, 0, 15, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates);
        let total = d.a_only_count + d.b_only_count;
        assert_eq!(total, 2); // capacity check sees both sides
        assert_eq!(d.b_newer.len(), 1); // but reply has just the newer one
    }

    #[test]
    fn diff_node_anns_basic() {
        let a = st(0);
        let b = st(1);
        write_na(&a, 555, 100, 200);
        write_na(&a, 666, 100, 200);
        write_na(&b, 555, 100, 200);
        write_na(&b, 777, 100, 200);
        let d = compute_diff(&a, &b, SketchKind::NodeAnns);
        assert_eq!(d.intersection, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.a_newer[0].origin, Some(666));
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].origin, Some(777));
    }

    /// Stale node_ann (older ts) should not be in *_newer.
    #[test]
    fn diff_node_anns_handles_timestamp_diff() {
        let a = st(0);
        let b = st(1);
        write_na(&a, 555, 100, 200);
        write_na(&b, 555, 200, 200);
        let d = compute_diff(&a, &b, SketchKind::NodeAnns);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert!(d.a_newer.is_empty());
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].timestamp, 200);
    }

    #[test]
    fn diff_chan_anns_basic() {
        let a = st(0);
        let b = st(1);
        write_ca(&a, 100, 64);
        write_ca(&a, 200, 64);
        write_ca(&b, 100, 64);
        write_ca(&b, 300, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanAnns);
        assert_eq!(d.intersection, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.a_newer[0].scid, Some(200));
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].scid, Some(300));
    }

    /// Synthesised gossip's id equals the canonical derive_id for
    /// the same identity tuple — this is what fixes the MsgInflight
    /// collapse bug on the metrics side.
    #[test]
    fn synth_msg_id_matches_derive() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 50, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates);
        assert_eq!(d.a_newer.len(), 1);
        let expected = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(100), 0, 50);
        assert_eq!(d.a_newer[0].id, expected);
    }

    /// N threads compute pairwise diffs in mixed (a,b) and (b,a)
    /// orders; lock-by-NodeIdx-min must prevent deadlock.
    #[test]
    fn lock_order_no_deadlock() {
        const N: usize = 8;
        let states: Vec<_> = (0..N).map(|i| st(i as NodeIdx)).collect();
        for (i, s) in states.iter().enumerate() {
            for k in 0..16 {
                write_cu(s, k, (i & 1) as u8, k as u32 + i as u32, 64);
            }
        }
        let barrier = Arc::new(Barrier::new(N));
        let mut handles = Vec::new();
        for t in 0..N {
            let states = states.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for round in 0..100 {
                    let a = (t + round) % N;
                    let b = (t + round + 3) % N;
                    let _ = compute_diff(&states[a], &states[b], SketchKind::ChanUpdates);
                    let _ = compute_diff(&states[b], &states[a], SketchKind::ChanUpdates);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
