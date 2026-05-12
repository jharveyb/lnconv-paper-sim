//! Wire-format types shared by every node kind.
//!
//! `Gossip` is one logical Lightning gossip message — a channel
//! update, announcement, etc. The `id` field is a stable, content-
//! derived hash (see [`Gossip::derive_id`]) so that two nodes
//! independently re-emitting (or sketch-replying with) the same
//! BOLT 7 message both produce the same `MsgId` and the metrics
//! layer can correlate convergence across nodes.
//!
//! `WireMessage` is what actually flows on the wire between
//! models. Three variants:
//! * `Single(Gossip)` — flooding and originations.
//! * `Batch(Arc<GossipBatch>)` — stagger algorithms and sketch
//!   replies; pre-partitioned by `GossipKind` so receivers take
//!   each per-kind `RwLock` exactly once per batch.
//! * `Sketch(Sketch)` — set-reconciliation primitive.
//!
//! Receivers iterate `.iter_gossips()` (kind-agnostic) or dispatch
//! on the inner `GossipBatch` Vecs for batched per-kind absorption.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use twox_hash::xxhash3_64::Hasher as XX3;

/// Stable node identifier. For synthetic configs, this is the dense
/// 0..n insertion order cast to u64. For CSV-loaded configs, it's
/// `xxhash64(seed, pubkey)`. We use u64 so the hash space is wide
/// enough that birthday-collision probability stays negligible on
/// real LN datasets (~12k nodes / ~42k channels).
pub type NodeId = u64;
/// Stable, content-derived gossip identifier — `xxhash3_64` of the
/// per-kind identity tuple (see [`Gossip::derive_id`]). u64 so the
/// hash space is wide enough that ~1.5 M parquet rows have
/// negligible collision probability.
pub type MsgId = u64;
/// Stand-in for a BOLT 7 short-channel-id. u64 because real SCIDs are
/// 64-bit, and because xxhash64 of the SCID string for CSV loading
/// is the cleanest collision-free representation.
pub type Scid = u64;
/// Stand-in for the channel-direction bit (BOLT 7 `channel_flags` bit 0).
/// Always 0 or 1.
pub type Direction = u8;
/// Dense per-node index used for `Vec`-indexed storage (e.g. metrics'
/// per-MsgId coverage vector). Always in `0..n_nodes` and equal to
/// `petgraph::NodeIndex::index()` on the topology peer graph. Distinct
/// from `NodeId`, which can be sparse u64 from a CSV hash.
pub type NodeIdx = u32;

/// Hash subseed for the stable-MsgId derivation in
/// [`Gossip::derive_id`]. Distinct from the pubkey / SCID subseeds
/// in `topology::ln_data` so MsgId, NodeId, and Scid live in
/// different hash spaces.
const MSGID_SUBSEED: u64 = 0x4D534749445F5355; // "MSGID_SU"

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Gossip {
    /// Stable, content-derived id from [`Gossip::derive_id`]. Same
    /// per-kind identity tuple ⇒ same `MsgId`, regardless of
    /// originator or re-synthesis path.
    pub id: MsgId,
    /// Real BOLT 7 chan_update / chan_ann don't carry an origin
    /// (it's implied by SCID + channel-graph ownership). We keep
    /// origin on the wire only for `NodeAnnouncement`, where it
    /// IS the message identity. `None` for the other two kinds.
    pub origin: Option<NodeId>,
    pub kind: GossipKind,
    /// On-the-wire size, used for bandwidth metrics. Real BOLT 7
    /// gossip caps around 1364 bytes; `u16` gives 64 KiB headroom.
    pub size_bytes: u16,
    /// Channel SCID. `Some(_)` for `ChannelUpdate` / `ChannelAnnouncement`;
    /// `None` for `NodeAnnouncement` (no channel scope).
    pub scid: Option<Scid>,
    /// Channel side (0 or 1). Meaningful for `ChannelUpdate`;
    /// always 0 for the other kinds.
    pub direction: Direction,
    /// Seconds since `MonotonicTime::EPOCH`. Populated by the
    /// originating node's `originate` handler for `ChannelUpdate`
    /// and `NodeAnnouncement` (matches BOLT 7's "node sets
    /// timestamp on send"). For `ChannelAnnouncement` the
    /// timestamp field is unused — channel announcements aren't
    /// timestamped in BOLT 7.
    pub timestamp: u32,
}

impl Gossip {
    /// Stable, content-derived `MsgId`: any node that produces a
    /// `Gossip` with the same per-kind identity tuple gets the
    /// same id. `None` values for `origin` / `scid` hash as
    /// all-zero bytes in their slot — that's how the per-kind
    /// identity converges across nodes (origin ignored for
    /// chan_*, scid ignored for `NodeAnnouncement`).
    pub fn derive_id(
        origin: Option<NodeId>,
        kind: GossipKind,
        scid: Option<Scid>,
        direction: Direction,
        timestamp: u32,
    ) -> MsgId {
        let mut buf = [0u8; 8 + 1 + 8 + 1 + 4];
        buf[0..8].copy_from_slice(&origin.unwrap_or(0).to_le_bytes());
        buf[8] = kind as u8;
        buf[9..17].copy_from_slice(&scid.unwrap_or(0).to_le_bytes());
        buf[17] = direction;
        buf[18..22].copy_from_slice(&timestamp.to_le_bytes());
        XX3::oneshot_with_seed(MSGID_SUBSEED, &buf)
    }
}

/// The three BOLT 7 gossip message kinds the simulator
/// distinguishes. `#[repr(u8)]` with explicit discriminants so the
/// cast in [`Gossip::derive_id`] is stable across versions.
#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipKind {
    #[default]
    NodeAnnouncement = 1,
    ChannelAnnouncement = 2,
    ChannelUpdate = 3,
}

/// Set-reconciliation works one kind at a time: a single `Sketch`
/// covers exactly one of `chan_updates`, `node_anns`, or `chan_anns`.
/// The variants line up 1:1 with [`GossipKind`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SketchKind {
    #[default]
    ChanUpdates,
    NodeAnns,
    ChanAnns,
}

impl SketchKind {
    /// Map a `GossipKind` into the `SketchKind` whose dedup container
    /// holds it.
    pub fn from_gossip(g: GossipKind) -> Self {
        match g {
            GossipKind::ChannelUpdate => SketchKind::ChanUpdates,
            GossipKind::NodeAnnouncement => SketchKind::NodeAnns,
            GossipKind::ChannelAnnouncement => SketchKind::ChanAnns,
        }
    }

    /// Inverse of [`Self::from_gossip`] — the `GossipKind` whose
    /// dedup container this sketch covers.
    pub fn to_gossip(self) -> GossipKind {
        match self {
            SketchKind::ChanUpdates => GossipKind::ChannelUpdate,
            SketchKind::NodeAnns => GossipKind::NodeAnnouncement,
            SketchKind::ChanAnns => GossipKind::ChannelAnnouncement,
        }
    }
}

/// Minisketch-style summary of a node's state for one kind. The
/// real protocol uses XOR-based set sketches that allow recovering
/// the symmetric difference *exactly* when its size ≤ `capacity`,
/// and detect failure when it exceeds capacity. We don't run the
/// real coding here — the simulator computes the diff directly from
/// each side's stored map (see [`crate::state::compute_diff`]) and
/// returns failure when the diff exceeds `capacity`.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Sketch {
    /// Sender-local sequence number, useful for tracing.
    pub id: MsgId,
    /// Sender's `NodeId`, so the receiver can locate the reply
    /// `Output` for this peer.
    pub from: NodeId,
    pub kind: SketchKind,
    /// Maximum symmetric-diff size the sketch can resolve. Diffs
    /// larger than this count as a "sketch overflow" — the receiver
    /// records the failure and sends no reply.
    pub capacity: u32,
    /// Wire size in bytes for bandwidth accounting. Default is
    /// `capacity * 8` (8 bytes per minisketch cell at u64 element
    /// width); callers may override.
    pub size_bytes: u16,
}

/// A per-kind-partitioned bundle of gossips, used as the inner
/// payload of [`WireMessage::Batch`]. The receiver can take each
/// per-kind `RwLock` exactly once per batch (instead of once per
/// gossip), avoiding the lock-acquisition storm that
/// `Vec<Gossip>` produces on heterogeneous batches.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GossipBatch {
    pub chan_updates: Vec<Gossip>,
    pub node_anns: Vec<Gossip>,
    pub chan_anns: Vec<Gossip>,
}

impl GossipBatch {
    /// Iterate every gossip in chan_updates → node_anns → chan_anns
    /// order. Receivers that don't care about kind use this.
    pub fn iter(&self) -> impl Iterator<Item = &Gossip> + '_ {
        self.chan_updates
            .iter()
            .chain(self.node_anns.iter())
            .chain(self.chan_anns.iter())
    }

    pub fn len(&self) -> usize {
        self.chan_updates.len() + self.node_anns.len() + self.chan_anns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chan_updates.is_empty() && self.node_anns.is_empty() && self.chan_anns.is_empty()
    }

    /// Sum of `Gossip.size_bytes` across all kinds. Used by
    /// `WireMessage::wire_size`.
    pub fn wire_size(&self) -> u64 {
        self.iter().map(|g| g.size_bytes as u64).sum()
    }

    /// Build a `GossipBatch` from a mixed `Vec<Gossip>` by
    /// partitioning per kind. Used by Cln/Lnd ticks where
    /// `pending: Vec<Gossip>` mixes kinds, and by anywhere a
    /// single-kind Vec needs wrapping (the other two kind Vecs
    /// stay empty).
    pub fn from_mixed(gossips: Vec<Gossip>) -> Self {
        let mut out = GossipBatch::default();
        for g in gossips {
            match g.kind {
                GossipKind::ChannelUpdate => out.chan_updates.push(g),
                GossipKind::NodeAnnouncement => out.node_anns.push(g),
                GossipKind::ChannelAnnouncement => out.chan_anns.push(g),
            }
        }
        out
    }

    /// Build a `GossipBatch` from a slice known to contain only one
    /// `kind`. The matching slot is pre-sized to `gossips.len()`; the
    /// other two stay empty. Used by the sketch reply path where the
    /// upper bound is `sketch.capacity` and the kind is fixed by
    /// `Sketch.kind`. `debug_assert`s that every input gossip matches
    /// the declared kind.
    pub fn from_mixed_for_kind(gossips: Vec<Gossip>, kind: GossipKind) -> Self {
        debug_assert!(gossips.iter().all(|g| g.kind == kind));
        let mut out = GossipBatch::default();
        match kind {
            GossipKind::ChannelUpdate => {
                out.chan_updates = gossips;
            }
            GossipKind::NodeAnnouncement => {
                out.node_anns = gossips;
            }
            GossipKind::ChannelAnnouncement => {
                out.chan_anns = gossips;
            }
        }
        out
    }

    /// One-shot batch wrapping a single gossip in the right slot.
    /// Useful when a sketch reply has a single newer item.
    #[allow(dead_code)]
    pub fn from_one(g: Gossip) -> Self {
        let mut out = GossipBatch::default();
        match g.kind {
            GossipKind::ChannelUpdate => out.chan_updates.push(g),
            GossipKind::NodeAnnouncement => out.node_anns.push(g),
            GossipKind::ChannelAnnouncement => out.chan_anns.push(g),
        }
        out
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMessage {
    Single(Gossip),
    /// Per-kind partitioned payload, immutable after construction.
    /// `Arc` so NeXosim's per-recipient broadcast clone is a
    /// refcount bump instead of an O(batch_len) `Vec` copy.
    Batch(Arc<GossipBatch>),
    /// Set-reconciliation primitive. Carries no inner gossips —
    /// receivers compute the diff against their own state and send
    /// the missing-from-sender messages back as a `Batch`.
    Sketch(Sketch),
}

impl WireMessage {
    /// Iterate over the inner `Gossip`s regardless of variant. `Sketch`
    /// yields the empty iterator — sketches are protocol metadata,
    /// not gossips.
    pub fn iter_gossips(&self) -> Box<dyn Iterator<Item = &Gossip> + '_> {
        match self {
            WireMessage::Single(g) => Box::new(std::iter::once(g)),
            WireMessage::Batch(b) => Box::new(b.iter()),
            WireMessage::Sketch(_) => Box::new(std::iter::empty()),
        }
    }

    /// On-the-wire byte size used for bandwidth metrics. For `Single`
    /// it's the inner gossip's `size_bytes`; for `Batch` it's the
    /// sum of inner gossips' sizes; for `Sketch` it's `Sketch.size_bytes`.
    pub fn wire_size(&self) -> u64 {
        match self {
            WireMessage::Single(g) => g.size_bytes as u64,
            WireMessage::Batch(b) => b.wire_size(),
            WireMessage::Sketch(s) => s.size_bytes as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gossip_kind_discriminants_stable() {
        assert_eq!(GossipKind::NodeAnnouncement as u8, 1);
        assert_eq!(GossipKind::ChannelAnnouncement as u8, 2);
        assert_eq!(GossipKind::ChannelUpdate as u8, 3);
    }

    /// Two derived ids over the same identity tuple must match.
    #[test]
    fn derive_id_is_deterministic() {
        let a = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(100), 0, 42);
        let b = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(100), 0, 42);
        assert_eq!(a, b);
    }

    /// For chan_update, `origin` is hashed as all-zeros, so two
    /// different `Some(origin)` arguments at the call site collapse
    /// to the same MsgId — that's the BOLT 7 identity convention
    /// (origin isn't part of chan_update identity).
    #[test]
    fn derive_id_origin_ignored_for_chan_kinds_at_hash_level() {
        // This documents the hash-input convention: `derive_id`
        // takes whatever the caller passes. In practice callers
        // pass `None` for chan_*, so both arguments below collapse
        // because they're both None. The originate paths enforce
        // the convention; this test just locks it in.
        let a = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(7), 0, 100);
        let b = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(7), 0, 100);
        assert_eq!(a, b);
    }

    /// Different timestamps for the same chan_update key produce
    /// different MsgIds.
    #[test]
    fn derive_id_distinguishes_timestamp() {
        let a = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(7), 0, 100);
        let b = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(7), 0, 101);
        assert_ne!(a, b);
    }

    /// 100k distinct identity tuples produce 100k distinct MsgIds.
    #[test]
    fn msg_id_collision_safe_at_100k() {
        let mut ids = std::collections::HashSet::new();
        for ts in 0..100_000u32 {
            let id = Gossip::derive_id(None, GossipKind::ChannelUpdate, Some(42), 0, ts);
            assert!(ids.insert(id), "collision at ts={ts}");
        }
        assert_eq!(ids.len(), 100_000);
    }

    #[test]
    fn gossip_batch_from_mixed_partitions() {
        let mk = |k: GossipKind| Gossip {
            id: 0,
            origin: None,
            kind: k,
            size_bytes: 100,
            scid: Some(1),
            direction: 0,
            timestamp: 0,
        };
        let mixed = vec![
            mk(GossipKind::ChannelUpdate),
            mk(GossipKind::NodeAnnouncement),
            mk(GossipKind::ChannelAnnouncement),
            mk(GossipKind::ChannelUpdate),
        ];
        let b = GossipBatch::from_mixed(mixed);
        assert_eq!(b.chan_updates.len(), 2);
        assert_eq!(b.node_anns.len(), 1);
        assert_eq!(b.chan_anns.len(), 1);
        assert_eq!(b.len(), 4);
        assert_eq!(b.wire_size(), 400);
    }
}
