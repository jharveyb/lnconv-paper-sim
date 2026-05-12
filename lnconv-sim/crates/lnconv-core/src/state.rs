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
//! ## Storage choices
//!
//! Each kind lives behind a [`parking_lot::RwLock`] (faster + smaller
//! than `std::sync::RwLock`; no syscall on uncontended paths). The
//! contained map is a [`nohash_hasher::IntMap`] (i.e. `HashMap` with a
//! `NoHashHasher<u64>` build hasher) — every key is already a
//! `xxhash3_64` output (NodeId, Scid) or a derived packed `u64`
//! ([`pack_cu_key`]), so re-hashing it would just add work.
//!
//! `chan_updates` keys pack `(scid << 1) | direction` into a `u64`.
//! SCIDs from the CSV loader are masked to 63 bits at load time
//! ([`crate::topology::ln_data::hash_scid_string`]) so the shift is
//! lossless; synthetic SCIDs come from a small sequential counter and
//! are already < 2^63. The unpack is `(packed >> 1, packed & 1)`.
//!
//! ## Distribution at sim-init
//!
//! `sim::run` calls [`build_registry`] once to produce a
//! `Vec<SharedNodeState>` indexed by `NodeIdx`. While constructing
//! each model, the simulator hands it (a) its own `Arc<NodeState>`
//! and (b) a `Vec<Arc<NodeState>>` of *only its direct peers'*
//! states (aligned with the model's per-peer Output Vec). The local
//! registry vector is dropped after wiring; the per-node Arcs
//! survive via the model + its peers' references.
//!
//! ## Diff semantics
//!
//! [`compute_diff`] returns both:
//!
//! * **Strict-difference counts** (`a_only_count` / `b_only_count` /
//!   `intersection`) — same-key-different-ts pairs count as one
//!   element on each side. Used for sketch capacity-overflow check
//!   and per-direction metrics; matches how a real minisketch
//!   would decode the symmetric difference.
//! * **Newer-only Gossips** (`a_newer` / `b_newer`) — items where
//!   the named side has the strictly-newer version, eligible to be
//!   sent back. The caller passes [`WhichSide`] to choose which
//!   Vec(s) to materialise; the unselected side comes back empty.
//!
//! Lock acquisition is in `NodeIdx`-min-first order to avoid
//! deadlock between two reconciliations on the same kind in
//! opposite directions when a third party is waiting on a write
//! lock.

use std::sync::Arc;

use nexosim::time::MonotonicTime;
use nohash_hasher::IntMap;
use parking_lot::RwLock;

use crate::message::{Direction, Gossip, GossipKind, NodeId, NodeIdx, Scid, SketchKind};

/// Pack a `(Scid, Direction)` tuple into a single `u64` suitable for
/// `nohash_hasher::IntMap`. Direction is stored in bit 0; SCID
/// occupies bits 1..64. SCIDs are guaranteed `< 2^63` (CSV loader
/// masks the top bit; synthetic SCIDs are sequential), so the shift
/// is lossless.
#[inline]
pub fn pack_cu_key(scid: Scid, direction: Direction) -> u64 {
    (scid << 1) | (direction as u64 & 1)
}

/// Inverse of [`pack_cu_key`].
#[inline]
pub fn unpack_cu_key(packed: u64) -> (Scid, Direction) {
    (packed >> 1, (packed & 1) as Direction)
}

/// Type aliases for the three per-node dedup maps. All use
/// `NoHashHasher<u64>` because the keys are already well-distributed
/// hash outputs.
pub type ChanUpdatesMap = IntMap<u64, (u32, u16)>;
pub type NodeAnnsMap = IntMap<NodeId, (u32, u16)>;
pub type ChanAnnsMap = IntMap<Scid, u16>;

/// Per-node dedup state. Each kind lives behind its own
/// `parking_lot::RwLock` so a `chan_updates` write doesn't block a
/// `node_anns` reader. Each map's value embeds `size_bytes` so
/// set-recon replies can carry realistic on-the-wire byte counts.
///
/// `Default` exists only to satisfy the `#[derive(Default)]` on the
/// node `Model` structs (each holds a `SharedNodeState`); a default
/// `NodeState` carries `idx = 0` and three empty maps and is never
/// observed at runtime — sim init replaces it with a real Arc from
/// the registry before any model spins up.
#[derive(Default)]
pub struct NodeState {
    pub idx: NodeIdx, // for lock-order tie-breaking in `compute_diff`
    pub chan_updates: RwLock<ChanUpdatesMap>,
    pub node_anns: RwLock<NodeAnnsMap>,
    pub chan_anns: RwLock<ChanAnnsMap>,
}

pub type SharedNodeState = Arc<NodeState>;

impl NodeState {
    pub fn new(idx: NodeIdx) -> Arc<Self> {
        Arc::new(Self {
            idx,
            chan_updates: RwLock::new(IntMap::default()),
            node_anns: RwLock::new(IntMap::default()),
            chan_anns: RwLock::new(IntMap::default()),
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

/// Which side of a `compute_diff` should materialise its newer-only
/// Gossip Vec. Production (sketch reply) only consumes `b_newer`, so
/// it passes `WhichSide::B`; tests pass `WhichSide::Both` to assert
/// symmetry. The unselected side comes back as an empty Vec.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WhichSide {
    A,
    B,
    Both,
}

impl WhichSide {
    #[inline]
    fn want_a(self) -> bool {
        matches!(self, WhichSide::A | WhichSide::Both)
    }
    #[inline]
    fn want_b(self) -> bool {
        matches!(self, WhichSide::B | WhichSide::Both)
    }
}

/// Result of a symmetric-diff computation. The `_count` fields are
/// always populated; `a_newer` and `b_newer` are only populated for
/// the side(s) requested via [`WhichSide`].
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
    /// Subset of the diff that is **strictly newer on `a`'s side**.
    /// Empty when caller passed `WhichSide::B`.
    pub a_newer: Vec<Gossip>,
    /// Subset of the diff that is **strictly newer on `b`'s side**.
    /// Empty when caller passed `WhichSide::A`.
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
    which: WhichSide,
) -> DiffResult {
    let (first, second, swapped) = if a.idx <= b.idx {
        (a, b, false)
    } else {
        (b, a, true)
    };
    // After swap, `which` may need to be inverted so the diff helpers
    // still see (la, lb) in the original (a, b) orientation.
    let inner_which = if swapped {
        match which {
            WhichSide::A => WhichSide::B,
            WhichSide::B => WhichSide::A,
            WhichSide::Both => WhichSide::Both,
        }
    } else {
        which
    };

    let res = match kind {
        SketchKind::ChanUpdates => {
            let g_first = first.chan_updates.read();
            let g_second = second.chan_updates.read();
            diff_chan_updates(&g_first, &g_second, inner_which)
        }
        SketchKind::NodeAnns => {
            let g_first = first.node_anns.read();
            let g_second = second.node_anns.read();
            diff_node_anns(&g_first, &g_second, inner_which)
        }
        SketchKind::ChanAnns => {
            let g_first = first.chan_anns.read();
            let g_second = second.chan_anns.read();
            diff_chan_anns(&g_first, &g_second, inner_which)
        }
    };

    // Re-orient the result back to the caller's (a, b) view.
    if swapped {
        DiffResult {
            a_only_count: res.b_only_count,
            b_only_count: res.a_only_count,
            intersection: res.intersection,
            a_newer: res.b_newer,
            b_newer: res.a_newer,
        }
    } else {
        res
    }
}

fn diff_chan_updates(la: &ChanUpdatesMap, lb: &ChanUpdatesMap, which: WhichSide) -> DiffResult {
    let want_a = which.want_a();
    let want_b = which.want_b();
    let mut a_only_count = 0usize;
    let mut b_only_count = 0usize;
    let mut intersection = 0usize;
    let difference_count_estimate = 512;
    let mut a_newer = if want_a { Vec::with_capacity(difference_count_estimate) } else { Vec::new() };
    let mut b_newer = if want_b { Vec::with_capacity(difference_count_estimate) } else { Vec::new() };
    for (packed, (ts_a, size_a)) in la {
        match lb.get(packed) {
            Some((ts_b, _)) if ts_b == ts_a => intersection += 1,
            Some((ts_b, size_b)) => {
                a_only_count += 1;
                b_only_count += 1;
                if ts_a > ts_b {
                    if want_a {
                        let (scid, dir) = unpack_cu_key(*packed);
                        a_newer.push(synth_chan_update(scid, dir, *ts_a, *size_a));
                    }
                } else if want_b {
                    let (scid, dir) = unpack_cu_key(*packed);
                    b_newer.push(synth_chan_update(scid, dir, *ts_b, *size_b));
                }
            }
            None => {
                a_only_count += 1;
                if want_a {
                    let (scid, dir) = unpack_cu_key(*packed);
                    a_newer.push(synth_chan_update(scid, dir, *ts_a, *size_a));
                }
            }
        }
    }
    for (packed, (ts_b, size_b)) in lb {
        if la.contains_key(packed) {
            continue;
        }
        b_only_count += 1;
        if want_b {
            let (scid, dir) = unpack_cu_key(*packed);
            b_newer.push(synth_chan_update(scid, dir, *ts_b, *size_b));
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

fn diff_node_anns(la: &NodeAnnsMap, lb: &NodeAnnsMap, which: WhichSide) -> DiffResult {
    let want_a = which.want_a();
    let want_b = which.want_b();
    let mut a_only_count = 0usize;
    let mut b_only_count = 0usize;
    let mut intersection = 0usize;
    let difference_count_estimate = 128;
    let mut a_newer = if want_a { Vec::with_capacity(difference_count_estimate) } else { Vec::new() };
    let mut b_newer = if want_b { Vec::with_capacity(difference_count_estimate) } else { Vec::new() };
    for (origin, (ts_a, size_a)) in la {
        match lb.get(origin) {
            Some((ts_b, _)) if ts_b == ts_a => intersection += 1,
            Some((ts_b, size_b)) => {
                a_only_count += 1;
                b_only_count += 1;
                if ts_a > ts_b {
                    if want_a {
                        a_newer.push(synth_node_ann(*origin, *ts_a, *size_a));
                    }
                } else if want_b {
                    b_newer.push(synth_node_ann(*origin, *ts_b, *size_b));
                }
            }
            None => {
                a_only_count += 1;
                if want_a {
                    a_newer.push(synth_node_ann(*origin, *ts_a, *size_a));
                }
            }
        }
    }
    for (origin, (ts_b, size_b)) in lb {
        if la.contains_key(origin) {
            continue;
        }
        b_only_count += 1;
        if want_b {
            b_newer.push(synth_node_ann(*origin, *ts_b, *size_b));
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

fn diff_chan_anns(la: &ChanAnnsMap, lb: &ChanAnnsMap, which: WhichSide) -> DiffResult {
    let want_a = which.want_a();
    let want_b = which.want_b();
    let mut a_only_count = 0usize;
    let mut intersection = 0usize;
    let difference_count_estimate = 128;
    let mut a_newer = if want_a { Vec::with_capacity(difference_count_estimate) } else { Vec::new() };
    let mut b_newer = if want_b { Vec::with_capacity(difference_count_estimate) } else { Vec::new() };
    for (scid, size_a) in la {
        if lb.contains_key(scid) {
            intersection += 1;
        } else {
            a_only_count += 1;
            if want_a {
                a_newer.push(synth_chan_ann(*scid, *size_a));
            }
        }
    }
    let mut b_only_count = 0usize;
    for (scid, size_b) in lb {
        if la.contains_key(scid) {
            continue;
        }
        b_only_count += 1;
        if want_b {
            b_newer.push(synth_chan_ann(*scid, *size_b));
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

/// Stamp the originator's per-kind state and finalise `msg`'s
/// timestamp + id. Returns `true` if the message should be
/// broadcast, `false` if it was dropped (e.g. duplicate
/// channel_announcement). Shared across every node kind so the
/// origin/timestamp/MsgId conventions stay aligned.
///
/// Sim time is monotonic per-node (recv is serialised via the
/// Mailbox), so the prior stored ts is not consulted — `now_secs`
/// is always strictly greater (or equal, which collapses to one
/// MsgId; downstream dedup drops duplicates).
pub fn originate_stamp(
    state: &SharedNodeState,
    self_id: NodeId,
    msg: &mut Gossip,
    cx_time: MonotonicTime,
) -> bool {
    let now_secs = cx_time.as_secs() as u32;
    match msg.kind {
        GossipKind::ChannelUpdate => {
            let scid = msg.scid.expect("ChannelUpdate must carry scid");
            msg.origin = None;
            msg.timestamp = now_secs;
            let mut m = state.chan_updates.write();
            m.insert(pack_cu_key(scid, msg.direction), (now_secs, msg.size_bytes));
        }
        GossipKind::NodeAnnouncement => {
            msg.origin = Some(self_id);
            msg.timestamp = now_secs;
            let mut m = state.node_anns.write();
            m.insert(self_id, (now_secs, msg.size_bytes));
        }
        GossipKind::ChannelAnnouncement => {
            let scid = msg.scid.expect("ChannelAnnouncement must carry scid");
            msg.origin = None;
            let mut m = state.chan_anns.write();
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
            .insert(pack_cu_key(scid, dir), (ts, size));
    }

    fn write_na(s: &SharedNodeState, origin: NodeId, ts: u32, size: u16) {
        s.node_anns.write().insert(origin, (ts, size));
    }

    fn write_ca(s: &SharedNodeState, scid: Scid, size: u16) {
        s.chan_anns.write().insert(scid, size);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let cases = [
            (0u64, 0u8),
            (0u64, 1),
            (1, 0),
            (1, 1),
            (0x7FFF_FFFF_FFFF_FFFF, 0),
            (0x7FFF_FFFF_FFFF_FFFF, 1),
            (12345, 0),
            (12345, 1),
        ];
        for (scid, dir) in cases {
            let p = pack_cu_key(scid, dir);
            assert_eq!(unpack_cu_key(p), (scid, dir));
        }
    }

    #[test]
    fn diff_chan_updates_keys_overlap() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&a, 100, 1, 20, 64);
        write_cu(&b, 100, 0, 10, 64);
        write_cu(&b, 200, 0, 30, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::Both);
        assert_eq!(d.intersection, 1);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.a_newer[0].scid, Some(100));
        assert_eq!(d.a_newer[0].direction, 1);
        assert_eq!(d.b_newer[0].scid, Some(200));
    }

    #[test]
    fn diff_chan_updates_handles_timestamp_diff() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&b, 100, 0, 11, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::Both);
        assert_eq!(d.intersection, 0);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert!(d.a_newer.is_empty());
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].timestamp, 11);
    }

    #[test]
    fn diff_chan_updates_capacity_counts_stale() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&b, 100, 0, 15, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::Both);
        let total = d.a_only_count + d.b_only_count;
        assert_eq!(total, 2);
        assert_eq!(d.b_newer.len(), 1);
    }

    #[test]
    fn diff_node_anns_basic() {
        let a = st(0);
        let b = st(1);
        write_na(&a, 555, 100, 200);
        write_na(&a, 666, 100, 200);
        write_na(&b, 555, 100, 200);
        write_na(&b, 777, 100, 200);
        let d = compute_diff(&a, &b, SketchKind::NodeAnns, WhichSide::Both);
        assert_eq!(d.intersection, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.a_newer[0].origin, Some(666));
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].origin, Some(777));
    }

    #[test]
    fn diff_node_anns_handles_timestamp_diff() {
        let a = st(0);
        let b = st(1);
        write_na(&a, 555, 100, 200);
        write_na(&b, 555, 200, 200);
        let d = compute_diff(&a, &b, SketchKind::NodeAnns, WhichSide::Both);
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
        let d = compute_diff(&a, &b, SketchKind::ChanAnns, WhichSide::Both);
        assert_eq!(d.intersection, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.a_newer[0].scid, Some(200));
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.b_newer[0].scid, Some(300));
    }

    #[test]
    fn synth_msg_id_matches_derive() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 50, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::Both);
        assert_eq!(d.a_newer.len(), 1);
        let expected = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(100), 0, 50);
        assert_eq!(d.a_newer[0].id, expected);
    }

    #[test]
    fn which_side_b_yields_empty_a_newer() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&a, 200, 0, 20, 64);
        write_cu(&b, 300, 0, 30, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::B);
        assert!(d.a_newer.is_empty());
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.a_only_count, 2);
        assert_eq!(d.b_only_count, 1);
    }

    #[test]
    fn which_side_a_yields_empty_b_newer() {
        let a = st(0);
        let b = st(1);
        write_cu(&a, 100, 0, 10, 64);
        write_cu(&b, 200, 0, 30, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::A);
        assert!(d.b_newer.is_empty());
        assert_eq!(d.a_newer.len(), 1);
    }

    #[test]
    fn which_side_b_is_correct_under_swap() {
        let a = st(5);
        let b = st(0);
        write_cu(&a, 100, 0, 99, 64);
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::B);
        assert!(d.a_newer.is_empty());
        assert!(d.b_newer.is_empty());
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 0);
    }

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
                    let _ = compute_diff(&states[a], &states[b], SketchKind::ChanUpdates, WhichSide::Both);
                    let _ = compute_diff(&states[b], &states[a], SketchKind::ChanUpdates, WhichSide::B);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
