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
//! ## Storage choices — dense arrays over a shared key registry
//!
//! The *set* of keys for all three kinds is fixed once the event
//! schedule is built (`sim::build_events`): every gossip a node can
//! ever store originates from a scheduled event (or a sketch-synth
//! reply, which only reproduces an already-originated key). So the
//! universe is exactly the keys appearing in the event stream —
//! `chan_updates` the originated `(scid, direction)` pairs,
//! `node_anns` the originating `NodeId`s, `chan_anns` the announced
//! `Scid`s. Channels/nodes that never originate gossip cost nothing.
//! Storing a per-node `HashMap` duplicates the (already-hashed) keys
//! `n_nodes` times.
//!
//! Instead, a single immutable [`KeyRegistry`] is built once at sim
//! init and shared by every node. It assigns each key a dense
//! `0..K` index, and every node stores its per-kind state as a dense
//! array indexed by that index:
//!
//! * [`DenseTsMap`] (`chan_updates`, `node_anns`) — a presence
//!   [`FixedBitSet`] plus a `Vec<u32>` of timestamps. A separate
//!   presence bit is required because `ts == 0` is a legitimate
//!   stored value (sim/parquet t=0), so absence cannot be a sentinel.
//! * [`DenseChanAnns`] (`chan_anns`) — a pure presence bitset;
//!   `channel_announcement`s are dedup'd first-arrival-wins with no
//!   timestamp.
//!
//! `size_bytes` is *not* stored per node. Every message that will
//! ever propagate is known at sim init (`build_events`'s output), so
//! the [`KeyRegistry`] keeps a CSR-packed version table mapping
//! `(key, ts) -> size` and recovers the exact size on demand at
//! sketch-reply synthesis time (see [`KeyRegistry::build`]).
//!
//! Each kind lives behind its own [`parking_lot::RwLock`] so a
//! `chan_updates` write doesn't block a `node_anns` reader.
//!
//! `chan_updates` keys pack `(scid << 1) | direction` into a `u64`.
//! SCIDs from the CSV loader are masked to 63 bits at load time
//! ([`crate::topology::ln_data::hash_scid_string`]) so the shift is
//! lossless; synthetic SCIDs come from a small sequential counter and
//! are already < 2^63. The unpack is `(packed >> 1, packed & 1)`.
//!
//! ## Distribution at sim-init
//!
//! `sim::run` builds the [`KeyRegistry`], then calls [`build_registry`]
//! once to produce a `Vec<SharedNodeState>` indexed by `NodeIdx`.
//! While constructing each model, the simulator hands it (a) its own
//! `Arc<NodeState>` and (b) a `Vec<Arc<NodeState>>` of *only its
//! direct peers'* states (aligned with the model's per-peer Output
//! Vec). The local registry vector is dropped after wiring; the
//! per-node Arcs survive via the model + its peers' references.
//!
//! ## Diff semantics
//!
//! [`compute_diff`] returns both:
//!
//! * **Diff counts** — `a_only_count` / `b_only_count` are keys
//!   present on exactly one side (entirely absent from the other);
//!   `intersection` is same-key-same-ts. `difference` is the full
//!   `(key, ts)`-element symmetric-difference cardinality: one-sided
//!   keys contribute 1 each, same-key-different-ts keys contribute
//!   2 (one `(key, ts_a)` element on A, one `(key, ts_b)` on B —
//!   each is a distinct sketch element). `difference` is what the
//!   sketch capacity-overflow check compares against; `a_only` /
//!   `b_only` are the directional breakdown for metrics. The wire
//!   reply only ships the strictly-newer side of each mismatch
//!   (see `a_newer` / `b_newer` below), but both sides still occupy
//!   sketch capacity at diff time.
//! * **Newer-only Gossips** (`a_newer` / `b_newer`) — items where
//!   the named side has the strictly-newer version, eligible to be
//!   sent back. The caller passes [`WhichSide`] to choose which
//!   Vec(s) to materialise; the unselected side comes back empty.
//!
//! Because both sides index the same shared `0..K` space, the diff
//! is a linear walk over each side's presence bitset (`FixedBitSet`
//! iterators) — no hashing.
//!
//! Lock acquisition is in `NodeIdx`-min-first order to avoid
//! deadlock between two reconciliations on the same kind in
//! opposite directions when a third party is waiting on a write
//! lock.

use std::sync::Arc;
use std::time::Duration;

use fixedbitset::FixedBitSet;
use nexosim::time::MonotonicTime;
use nohash_hasher::IntMap;
use parking_lot::RwLock;

use crate::message::{Direction, Gossip, GossipKind, NodeId, NodeIdx, Scid, SketchKind};

/// Pack a `(Scid, Direction)` tuple into a single `u64`. Direction is
/// stored in bit 0; SCID occupies bits 1..64. SCIDs are guaranteed
/// `< 2^63` (CSV loader masks the top bit; synthetic SCIDs are
/// sequential), so the shift is lossless.
#[inline]
pub fn pack_cu_key(scid: Scid, direction: Direction) -> u64 {
    (scid << 1) | (direction as u64 & 1)
}

/// Inverse of [`pack_cu_key`].
#[inline]
pub fn unpack_cu_key(packed: u64) -> (Scid, Direction) {
    (packed >> 1, (packed & 1) as Direction)
}

// ---------------------------------------------------------------------
// Shared, immutable key registry
// ---------------------------------------------------------------------

/// CSR-packed version table for one timestamped kind. `offsets` has
/// length `K + 1`; the versions of dense index `i` are the slice
/// `values[offsets[i]..offsets[i+1]]`, kept ascending by `ts`. This
/// avoids one heap allocation per key (`Vec<Vec<_>>` would need `K`)
/// and keeps every group a cache-friendly contiguous slice.
#[derive(Default)]
struct VersionTable {
    /// `(ts, size)` pairs, grouped by dense index, each group sorted
    /// by `ts`.
    values: Vec<(u32, u16)>,
    /// Prefix-sum group boundaries; `len == K + 1`.
    offsets: Vec<u32>,
}

impl VersionTable {
    /// Build the CSR layout from per-index version groups. Each group
    /// is sorted by `ts` and deduplicated (two events colliding on
    /// the same second collapse to one entry).
    fn from_groups(mut groups: Vec<Vec<(u32, u16)>>) -> Self {
        let total: usize = groups.iter().map(Vec::len).sum();
        let mut values: Vec<(u32, u16)> = Vec::with_capacity(total);
        let mut offsets: Vec<u32> = Vec::with_capacity(groups.len() + 1);
        offsets.push(0);
        for g in &mut groups {
            g.sort_unstable_by_key(|&(ts, _)| ts);
            g.dedup_by_key(|&mut (ts, _)| ts);
            values.extend_from_slice(g);
            offsets.push(values.len() as u32);
        }
        Self { values, offsets }
    }

    /// Exact `size_bytes` for dense index `idx` at timestamp `ts`.
    /// A node only ever stores a `ts` that originated from a real
    /// message, so the search hits; the fallbacks keep it total.
    ///
    /// Most keys are updated 0–1 times over a run, so those two
    /// cases are branch-only fast paths; only a genuinely
    /// multi-version key pays the (small, ts-sorted) binary search.
    #[inline]
    fn size_of(&self, idx: usize, ts: u32) -> u16 {
        let lo = self.offsets[idx] as usize;
        let hi = self.offsets[idx + 1] as usize;
        match &self.values[lo..hi] {
            [] => 0,
            [(_, size)] => *size,
            group => match group.binary_search_by_key(&ts, |&(t, _)| t) {
                Ok(pos) => group[pos].1,
                Err(_) => group[0].1,
            },
        }
    }
}

/// Shared key data for one timestamped kind (`chan_updates` or
/// `node_anns`): the key→dense-index map, the reverse index→key
/// vector, and the CSR version table for exact size recovery.
#[derive(Default)]
struct KindKeys {
    index: IntMap<u64, u32>,
    key_by_idx: Vec<u64>,
    versions: VersionTable,
}

impl KindKeys {
    #[inline]
    fn idx(&self, key: u64) -> usize {
        *self
            .index
            .get(&key)
            .expect("key not in KeyRegistry — key universe not enumerated at sim init")
            as usize
    }
    #[inline]
    fn key_at(&self, idx: usize) -> u64 {
        self.key_by_idx[idx]
    }
    #[inline]
    fn size_of(&self, idx: usize, ts: u32) -> u16 {
        self.versions.size_of(idx, ts)
    }
    #[inline]
    fn len(&self) -> usize {
        self.key_by_idx.len()
    }
}

/// Shared key data for `chan_anns`. No timestamp dimension — a
/// `channel_announcement` carries one size per scid — so the version
/// table collapses to a plain `Vec<u16>` indexed by dense index.
#[derive(Default)]
struct ChanAnnKeys {
    index: IntMap<u64, u32>,
    key_by_idx: Vec<u64>,
    sizes: Vec<u16>,
}

impl ChanAnnKeys {
    #[inline]
    fn idx(&self, key: u64) -> usize {
        *self
            .index
            .get(&key)
            .expect("scid not in KeyRegistry — key universe not enumerated at sim init")
            as usize
    }
    #[inline]
    fn key_at(&self, idx: usize) -> u64 {
        self.key_by_idx[idx]
    }
    #[inline]
    fn size_at(&self, idx: usize) -> u16 {
        self.sizes[idx]
    }
    #[inline]
    fn len(&self) -> usize {
        self.key_by_idx.len()
    }
}

/// Immutable, process-wide key registry. Built once at sim init from
/// the full event schedule (see [`KeyRegistry::build`]); shared by
/// every [`NodeState`] via the inner `Arc`s. Carries both the
/// dense-index assignment and the per-kind size data so per-node
/// state can drop `size_bytes` entirely.
#[derive(Default)]
pub struct KeyRegistry {
    chan_updates: Arc<KindKeys>,
    node_anns: Arc<KindKeys>,
    chan_anns: Arc<ChanAnnKeys>,
}

/// Build `key -> dense index` from a key vector (index == position).
fn index_of(keys: &[u64]) -> IntMap<u64, u32> {
    let mut m: IntMap<u64, u32> = IntMap::default();
    m.reserve(keys.len());
    for (i, &k) in keys.iter().enumerate() {
        m.insert(k, i as u32);
    }
    m
}

impl KeyRegistry {
    /// Production constructor: derive the key universe **and** the CSR
    /// version table from the full event schedule in a single pass.
    ///
    /// Every key any node will ever store originates from an event
    /// (or a sketch-synth reply, which only ever reproduces an
    /// already-originated key), so the events alone define the
    /// universe — there is no need to enumerate the channel registry
    /// or the topology, and channels/nodes that never originate
    /// gossip cost no dense slots.
    ///
    /// Dense indices are assigned in first-appearance order over the
    /// (delay-sorted) event list, so the assignment is deterministic.
    /// An event's effective wire timestamp is the firing second:
    /// `originate_stamp` stamps `ChannelUpdate`/`NodeAnnouncement`
    /// with `cx.time().as_secs()`, and the firing time is the
    /// scheduled `delay` from t0, so `ts = delay.as_secs()`. A
    /// `NodeAnnouncement`'s key is the *originating* node (the event
    /// tuple's source), since `originate_stamp` forces
    /// `origin = self_id`. `ChannelAnnouncement` carries no timestamp.
    pub fn build(events: &[(Duration, NodeId, Gossip)]) -> Self {
        let mut cu_index: IntMap<u64, u32> = IntMap::default();
        let mut cu_keys: Vec<u64> = Vec::new();
        let mut cu_groups: Vec<Vec<(u32, u16)>> = Vec::new();
        let mut na_index: IntMap<u64, u32> = IntMap::default();
        let mut na_keys: Vec<u64> = Vec::new();
        let mut na_groups: Vec<Vec<(u32, u16)>> = Vec::new();
        let mut ca_index: IntMap<u64, u32> = IntMap::default();
        let mut ca_keys: Vec<u64> = Vec::new();
        let mut ca_sizes: Vec<u16> = Vec::new();

        for (delay, node, g) in events {
            let ts = delay.as_secs() as u32;
            match g.kind {
                GossipKind::ChannelUpdate => {
                    let key = g.state_key();
                    let idx = *cu_index.entry(key).or_insert_with(|| {
                        cu_keys.push(key);
                        cu_groups.push(Vec::new());
                        (cu_keys.len() - 1) as u32
                    }) as usize;
                    cu_groups[idx].push((ts, g.size_bytes));
                }
                GossipKind::NodeAnnouncement => {
                    let key = *node;
                    let idx = *na_index.entry(key).or_insert_with(|| {
                        na_keys.push(key);
                        na_groups.push(Vec::new());
                        (na_keys.len() - 1) as u32
                    }) as usize;
                    na_groups[idx].push((ts, g.size_bytes));
                }
                GossipKind::ChannelAnnouncement => {
                    let key = g.state_key();
                    match ca_index.get(&key) {
                        Some(&i) => ca_sizes[i as usize] = g.size_bytes,
                        None => {
                            ca_index.insert(key, ca_keys.len() as u32);
                            ca_keys.push(key);
                            ca_sizes.push(g.size_bytes);
                        }
                    }
                }
            }
        }

        KeyRegistry {
            chan_updates: Arc::new(KindKeys {
                index: cu_index,
                key_by_idx: cu_keys,
                versions: VersionTable::from_groups(cu_groups),
            }),
            node_anns: Arc::new(KindKeys {
                index: na_index,
                key_by_idx: na_keys,
                versions: VersionTable::from_groups(na_groups),
            }),
            chan_anns: Arc::new(ChanAnnKeys {
                index: ca_index,
                key_by_idx: ca_keys,
                sizes: ca_sizes,
            }),
        }
    }

    /// Test/bench constructor: explicit key universes with empty
    /// version tables. Callers that don't exercise sketch-reply size
    /// recovery (`synth_*` falls back to `0`).
    pub fn from_keys(cu_keys: Vec<u64>, na_keys: Vec<u64>, ca_keys: Vec<u64>) -> Self {
        let cu_index = index_of(&cu_keys);
        let na_index = index_of(&na_keys);
        let ca_index = index_of(&ca_keys);
        let cu_groups = vec![Vec::new(); cu_keys.len()];
        let na_groups = vec![Vec::new(); na_keys.len()];
        let ca_sizes = vec![0u16; ca_keys.len()];
        KeyRegistry {
            chan_updates: Arc::new(KindKeys {
                index: cu_index,
                key_by_idx: cu_keys,
                versions: VersionTable::from_groups(cu_groups),
            }),
            node_anns: Arc::new(KindKeys {
                index: na_index,
                key_by_idx: na_keys,
                versions: VersionTable::from_groups(na_groups),
            }),
            chan_anns: Arc::new(ChanAnnKeys {
                index: ca_index,
                key_by_idx: ca_keys,
                sizes: ca_sizes,
            }),
        }
    }
}

// ---------------------------------------------------------------------
// Per-node dense state
// ---------------------------------------------------------------------

/// Dense per-node state for a timestamped kind. `present[i]` records
/// whether dense index `i` is held; `ts[i]` is its stored timestamp.
/// `size_bytes` is recovered on demand from the shared [`KindKeys`]
/// version table.
#[derive(Default)]
pub struct DenseTsMap {
    keys: Arc<KindKeys>,
    present: FixedBitSet,
    ts: Vec<u32>,
}

impl DenseTsMap {
    fn new(keys: Arc<KindKeys>) -> Self {
        let k = keys.len();
        Self {
            present: FixedBitSet::with_capacity(k),
            ts: vec![0u32; k],
            keys,
        }
    }

    /// Stored timestamp for `key`, or `None` if not held. The hot
    /// dedup path — no size lookup.
    #[inline]
    pub fn get_ts(&self, key: u64) -> Option<u32> {
        let i = self.keys.idx(key);
        self.present.contains(i).then(|| self.ts[i])
    }

    /// Stored `(ts, size_bytes)` for `key`, or `None` if not held.
    /// Used by the inventory-reply path; recovers the exact size
    /// from the shared version table.
    #[inline]
    pub fn get(&self, key: u64) -> Option<(u32, u16)> {
        let i = self.keys.idx(key);
        if self.present.contains(i) {
            let ts = self.ts[i];
            Some((ts, self.keys.size_of(i, ts)))
        } else {
            None
        }
    }

    /// Record `key` as held with timestamp `ts` (overwrites any
    /// prior value — callers gate on supersession first).
    #[inline]
    pub fn insert(&mut self, key: u64, ts: u32) {
        let i = self.keys.idx(key);
        self.present.insert(i);
        self.ts[i] = ts;
    }

    /// Whether `key` is held.
    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        self.present.contains(self.keys.idx(key))
    }
}

/// Dense per-node state for `chan_anns`: a pure presence bitset.
/// `channel_announcement`s are dedup'd first-arrival-wins, so there
/// is nothing to store beyond "have I seen this scid".
#[derive(Default)]
pub struct DenseChanAnns {
    keys: Arc<ChanAnnKeys>,
    present: FixedBitSet,
}

impl DenseChanAnns {
    fn new(keys: Arc<ChanAnnKeys>) -> Self {
        let k = keys.len();
        Self {
            present: FixedBitSet::with_capacity(k),
            keys,
        }
    }

    /// Whether `scid` is held.
    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        self.present.contains(self.keys.idx(key))
    }

    /// Stored `size_bytes` for `scid`, or `None` if not held.
    #[inline]
    pub fn get(&self, key: u64) -> Option<u16> {
        let i = self.keys.idx(key);
        self.present.contains(i).then(|| self.keys.size_at(i))
    }

    /// Mark `scid` as held; returns `true` iff it was newly inserted
    /// (mirrors `HashMap::insert(..).is_none()`).
    #[inline]
    pub fn insert_present(&mut self, key: u64) -> bool {
        let i = self.keys.idx(key);
        !self.present.put(i)
    }
}

/// Per-node dedup state. Each kind lives behind its own
/// `parking_lot::RwLock` so a `chan_updates` write doesn't block a
/// `node_anns` reader.
///
/// `Default` exists only to satisfy the `#[derive(Default)]` on the
/// node `Model` structs (each holds a `SharedNodeState`); a default
/// `NodeState` carries `idx = 0` and three empty maps over an empty
/// `KeyRegistry` and is never observed at runtime — sim init replaces
/// it with a real Arc from the registry before any model spins up.
#[derive(Default)]
pub struct NodeState {
    pub idx: NodeIdx, // for lock-order tie-breaking in `compute_diff`
    pub chan_updates: RwLock<DenseTsMap>,
    pub node_anns: RwLock<DenseTsMap>,
    pub chan_anns: RwLock<DenseChanAnns>,
}

pub type SharedNodeState = Arc<NodeState>;

impl NodeState {
    pub fn new(keys: &KeyRegistry, idx: NodeIdx) -> Arc<Self> {
        Arc::new(Self {
            idx,
            chan_updates: RwLock::new(DenseTsMap::new(keys.chan_updates.clone())),
            node_anns: RwLock::new(DenseTsMap::new(keys.node_anns.clone())),
            chan_anns: RwLock::new(DenseChanAnns::new(keys.chan_anns.clone())),
        })
    }
}

/// Build one `Arc<NodeState>` per node, all sharing `keys`. The
/// returned `Vec` is the transient construction-time index used by
/// `sim::run` to hand each model its own state Arc plus an aligned
/// `Vec<Arc<NodeState>>` of its peers' states.
pub fn build_registry(keys: &KeyRegistry, n: usize) -> Vec<SharedNodeState> {
    (0..n).map(|i| NodeState::new(keys, i as NodeIdx)).collect()
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
    /// Number of keys present **only** in `a` — entirely absent from
    /// `b`. Keys present on both sides under a different ts are *not*
    /// counted here; see `difference`.
    pub a_only_count: usize,
    /// Number of keys present **only** in `b` — entirely absent from
    /// `a`. Keys present on both sides under a different ts are *not*
    /// counted here; see `difference`.
    pub b_only_count: usize,
    /// Items where both sides have the same key with the same `ts`
    /// (or, for `chan_anns`, same key — no `ts`).
    pub intersection: usize,
    /// Total symmetric-difference element count over `(key, ts)`
    /// pairs — the number of sketch elements that would need to be
    /// reconciled. A ts-mismatched key contributes **2** (one
    /// `(key, ts_a)` element on A's side and one `(key, ts_b)` on B's
    /// side — the sketch carries each separately, which is why
    /// `state` keeps the per-side "only send newer" filter so stale
    /// versions aren't actually transmitted). A key present on only
    /// one side contributes 1. This is the quantity checked against
    /// the sketch capacity. Equivalent to the old
    /// `a_only_count + b_only_count` accounting, but tracked
    /// independently now that those two fields only count one-sided
    /// keys. Always `>= a_only_count + b_only_count`.
    pub difference: usize,
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
            difference: res.difference,
            a_newer: res.b_newer,
            b_newer: res.a_newer,
        }
    } else {
        res
    }
}

fn diff_chan_updates(la: &DenseTsMap, lb: &DenseTsMap, which: WhichSide) -> DiffResult {
    let want_a = which.want_a();
    let want_b = which.want_b();
    let keys = &la.keys;
    let mut a_only_count = 0usize;
    let mut b_only_count = 0usize;
    let mut intersection = 0usize;
    let mut difference = 0usize;
    let difference_count_estimate = 512;
    let mut a_newer = if want_a {
        Vec::with_capacity(difference_count_estimate)
    } else {
        Vec::new()
    };
    let mut b_newer = if want_b {
        Vec::with_capacity(difference_count_estimate)
    } else {
        Vec::new()
    };
    for i in la.present.ones() {
        let ts_a = la.ts[i];
        if lb.present.contains(i) {
            let ts_b = lb.ts[i];
            if ts_a == ts_b {
                intersection += 1;
            } else {
                // Same key, different ts: two sketch elements to
                // reconcile (one `(key, ts_a)` on A, one
                // `(key, ts_b)` on B — each is its own `(key, value)`
                // tuple in the sketch). Not an `a_only`/`b_only`
                // key, since both sides do have the key. The wire
                // reply only sends the strictly-newer side
                // (`a_newer` / `b_newer`), but capacity-wise both
                // elements have to fit.
                difference += 2;
                if ts_a > ts_b {
                    if want_a {
                        let (scid, dir) = unpack_cu_key(keys.key_at(i));
                        a_newer.push(synth_chan_update(scid, dir, ts_a, keys.size_of(i, ts_a)));
                    }
                } else if want_b {
                    let (scid, dir) = unpack_cu_key(keys.key_at(i));
                    b_newer.push(synth_chan_update(scid, dir, ts_b, keys.size_of(i, ts_b)));
                }
            }
        } else {
            a_only_count += 1;
            difference += 1;
            if want_a {
                let (scid, dir) = unpack_cu_key(keys.key_at(i));
                a_newer.push(synth_chan_update(scid, dir, ts_a, keys.size_of(i, ts_a)));
            }
        }
    }
    for i in lb.present.difference(&la.present) {
        b_only_count += 1;
        difference += 1;
        if want_b {
            let ts_b = lb.ts[i];
            let (scid, dir) = unpack_cu_key(keys.key_at(i));
            b_newer.push(synth_chan_update(scid, dir, ts_b, keys.size_of(i, ts_b)));
        }
    }
    DiffResult {
        a_only_count,
        b_only_count,
        intersection,
        difference,
        a_newer,
        b_newer,
    }
}

fn diff_node_anns(la: &DenseTsMap, lb: &DenseTsMap, which: WhichSide) -> DiffResult {
    let want_a = which.want_a();
    let want_b = which.want_b();
    let keys = &la.keys;
    let mut a_only_count = 0usize;
    let mut b_only_count = 0usize;
    let mut intersection = 0usize;
    let mut difference = 0usize;
    let difference_count_estimate = 256;
    let mut a_newer = if want_a {
        Vec::with_capacity(difference_count_estimate)
    } else {
        Vec::new()
    };
    let mut b_newer = if want_b {
        Vec::with_capacity(difference_count_estimate)
    } else {
        Vec::new()
    };
    for i in la.present.ones() {
        let ts_a = la.ts[i];
        if lb.present.contains(i) {
            let ts_b = lb.ts[i];
            if ts_a == ts_b {
                intersection += 1;
            } else {
                // Same key, different ts: two sketch elements (one
                // per `(key, ts)` tuple). See `diff_chan_updates`
                // for the full rationale.
                difference += 2;
                if ts_a > ts_b {
                    if want_a {
                        a_newer.push(synth_node_ann(keys.key_at(i), ts_a, keys.size_of(i, ts_a)));
                    }
                } else if want_b {
                    b_newer.push(synth_node_ann(keys.key_at(i), ts_b, keys.size_of(i, ts_b)));
                }
            }
        } else {
            a_only_count += 1;
            difference += 1;
            if want_a {
                a_newer.push(synth_node_ann(keys.key_at(i), ts_a, keys.size_of(i, ts_a)));
            }
        }
    }
    for i in lb.present.difference(&la.present) {
        b_only_count += 1;
        difference += 1;
        if want_b {
            let ts_b = lb.ts[i];
            b_newer.push(synth_node_ann(keys.key_at(i), ts_b, keys.size_of(i, ts_b)));
        }
    }
    DiffResult {
        a_only_count,
        b_only_count,
        intersection,
        difference,
        a_newer,
        b_newer,
    }
}

fn diff_chan_anns(la: &DenseChanAnns, lb: &DenseChanAnns, which: WhichSide) -> DiffResult {
    let want_a = which.want_a();
    let want_b = which.want_b();
    let keys = &la.keys;
    let mut a_only_count = 0usize;
    let mut intersection = 0usize;
    let mut difference = 0usize;
    let difference_count_estimate = 128;
    let mut a_newer = if want_a {
        Vec::with_capacity(difference_count_estimate)
    } else {
        Vec::new()
    };
    let mut b_newer = if want_b {
        Vec::with_capacity(difference_count_estimate)
    } else {
        Vec::new()
    };
    for i in la.present.ones() {
        if lb.present.contains(i) {
            intersection += 1;
        } else {
            a_only_count += 1;
            difference += 1;
            if want_a {
                a_newer.push(synth_chan_ann(keys.key_at(i), keys.size_at(i)));
            }
        }
    }
    let mut b_only_count = 0usize;
    for i in lb.present.difference(&la.present) {
        b_only_count += 1;
        difference += 1;
        if want_b {
            b_newer.push(synth_chan_ann(keys.key_at(i), keys.size_at(i)));
        }
    }
    DiffResult {
        a_only_count,
        b_only_count,
        intersection,
        difference,
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
            state
                .chan_updates
                .write()
                .insert(pack_cu_key(scid, msg.direction), now_secs);
        }
        GossipKind::NodeAnnouncement => {
            msg.origin = Some(self_id);
            msg.timestamp = now_secs;
            state.node_anns.write().insert(self_id, now_secs);
        }
        GossipKind::ChannelAnnouncement => {
            let scid = msg.scid.expect("ChannelAnnouncement must carry scid");
            msg.origin = None;
            if !state.chan_anns.write().insert_present(scid) {
                // Already broadcast this scid — drop.
                return false;
            }
        }
    }
    // Re-derive the stable MsgId now that timestamp/origin are final.
    msg.id = Gossip::derive_id(msg.origin, msg.kind, msg.scid, msg.direction, msg.timestamp);
    true
}

pub fn synth_chan_update(scid: Scid, direction: Direction, timestamp: u32, size_bytes: u16) -> Gossip {
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

pub fn synth_node_ann(origin_id: NodeId, timestamp: u32, size_bytes: u16) -> Gossip {
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

pub fn synth_chan_ann(scid: Scid, size_bytes: u16) -> Gossip {
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
    use std::sync::{Barrier, LazyLock};
    use std::thread;

    /// Shared key registry covering every key the tests below touch:
    /// chan_updates/chan_anns scids `0..=400` and node_anns origins
    /// `0..=400` plus `{555, 666, 777}`. The version table is empty
    /// (no events) — tests never assert on synthesised `size_bytes`.
    static TEST_KEYS: LazyLock<KeyRegistry> = LazyLock::new(|| {
        let mut cu = Vec::new();
        for scid in 0..=400u64 {
            cu.push(pack_cu_key(scid, 0));
            cu.push(pack_cu_key(scid, 1));
        }
        let mut na: Vec<u64> = (0..=400u64).collect();
        na.extend([555u64, 666, 777]);
        let ca: Vec<u64> = (0..=400u64).collect();
        KeyRegistry::from_keys(cu, na, ca)
    });

    fn st(idx: NodeIdx) -> SharedNodeState {
        NodeState::new(&TEST_KEYS, idx)
    }

    fn write_cu(s: &SharedNodeState, scid: Scid, dir: Direction, ts: u32, _size: u16) {
        s.chan_updates.write().insert(pack_cu_key(scid, dir), ts);
    }

    fn write_na(s: &SharedNodeState, origin: NodeId, ts: u32, _size: u16) {
        s.node_anns.write().insert(origin, ts);
    }

    fn write_ca(s: &SharedNodeState, scid: Scid, _size: u16) {
        s.chan_anns.write().insert_present(scid);
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
        // Same key, different ts: two sketch elements (`(key, ts_a)`
        // + `(key, ts_b)`), not an a-only/b-only key.
        assert_eq!(d.a_only_count, 0);
        assert_eq!(d.b_only_count, 0);
        assert_eq!(d.difference, 2);
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
        // A ts-mismatched key contributes 2 to `difference` — both
        // `(key, ts_a)` and `(key, ts_b)` occupy a sketch slot.
        assert_eq!(d.difference, 2);
        assert_eq!(d.a_only_count, 0);
        assert_eq!(d.b_only_count, 0);
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
        // Same key, different ts: two sketch elements, no
        // a-only/b-only.
        assert_eq!(d.a_only_count, 0);
        assert_eq!(d.b_only_count, 0);
        assert_eq!(d.difference, 2);
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

    // ---------------------------------------------------------------
    // Side-coverage tests: each kind exercises `WhichSide::Both` and
    // `WhichSide::A` on a scenario rich enough to populate non-trivial
    // counts AND verify the Vec materialisation matches the requested
    // side. Complements the minimal `which_side_a_yields_empty_b_newer`
    // / `which_side_b_*` tests above, which only cover chan_updates.
    // ---------------------------------------------------------------

    /// 3 shared entries (same key + same ts on both sides) + 1 A-only
    /// (scid 100) + 1 B-only (scid 200). Used by both `_which_both`
    /// and `_which_a` chan_updates tests.
    fn diff_pair_chan_updates() -> (SharedNodeState, SharedNodeState) {
        let a = st(0);
        let b = st(1);
        for k in 0..3u64 {
            let ts = (k + 1) as u32;
            write_cu(&a, k, 0, ts, 64);
            write_cu(&b, k, 0, ts, 64);
        }
        write_cu(&a, 100, 0, 50, 64);
        write_cu(&b, 200, 0, 60, 64);
        (a, b)
    }

    #[test]
    fn diff_chan_updates_which_both_full() {
        let (a, b) = diff_pair_chan_updates();
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::Both);
        assert_eq!(d.intersection, 3);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.a_newer[0].scid, Some(100));
        assert_eq!(d.b_newer[0].scid, Some(200));
    }

    #[test]
    fn diff_chan_updates_which_a_full() {
        let (a, b) = diff_pair_chan_updates();
        let d = compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::A);
        assert_eq!(d.intersection, 3);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1, "a_newer materialised");
        assert!(d.b_newer.is_empty(), "b_newer suppressed");
        assert_eq!(d.a_newer[0].scid, Some(100));
    }

    /// 3 shared origins on both sides at the same ts + 1 A-only
    /// (origin 100) + 1 B-only (origin 200).
    fn diff_pair_node_anns() -> (SharedNodeState, SharedNodeState) {
        let a = st(0);
        let b = st(1);
        for origin in 0..3u64 {
            let ts = (origin + 1) as u32;
            write_na(&a, origin, ts, 64);
            write_na(&b, origin, ts, 64);
        }
        write_na(&a, 100, 50, 64);
        write_na(&b, 200, 60, 64);
        (a, b)
    }

    #[test]
    fn diff_node_anns_which_both_full() {
        let (a, b) = diff_pair_node_anns();
        let d = compute_diff(&a, &b, SketchKind::NodeAnns, WhichSide::Both);
        assert_eq!(d.intersection, 3);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.a_newer[0].origin, Some(100));
        assert_eq!(d.b_newer[0].origin, Some(200));
    }

    #[test]
    fn diff_node_anns_which_a_full() {
        let (a, b) = diff_pair_node_anns();
        let d = compute_diff(&a, &b, SketchKind::NodeAnns, WhichSide::A);
        assert_eq!(d.intersection, 3);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert!(d.b_newer.is_empty());
        assert_eq!(d.a_newer[0].origin, Some(100));
    }

    /// 3 shared scids on both sides + 1 A-only (scid 100) + 1 B-only
    /// (scid 200). chan_anns has no per-entry ts so the diff is pure
    /// presence/absence.
    fn diff_pair_chan_anns() -> (SharedNodeState, SharedNodeState) {
        let a = st(0);
        let b = st(1);
        for scid in 0..3u64 {
            write_ca(&a, scid, 64);
            write_ca(&b, scid, 64);
        }
        write_ca(&a, 100, 64);
        write_ca(&b, 200, 64);
        (a, b)
    }

    #[test]
    fn diff_chan_anns_which_both_full() {
        let (a, b) = diff_pair_chan_anns();
        let d = compute_diff(&a, &b, SketchKind::ChanAnns, WhichSide::Both);
        assert_eq!(d.intersection, 3);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert_eq!(d.b_newer.len(), 1);
        assert_eq!(d.a_newer[0].scid, Some(100));
        assert_eq!(d.b_newer[0].scid, Some(200));
    }

    #[test]
    fn diff_chan_anns_which_a_full() {
        let (a, b) = diff_pair_chan_anns();
        let d = compute_diff(&a, &b, SketchKind::ChanAnns, WhichSide::A);
        assert_eq!(d.intersection, 3);
        assert_eq!(d.a_only_count, 1);
        assert_eq!(d.b_only_count, 1);
        assert_eq!(d.a_newer.len(), 1);
        assert!(d.b_newer.is_empty());
        assert_eq!(d.a_newer[0].scid, Some(100));
    }
}
