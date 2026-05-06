//! Wire-format types shared by every node kind.
//!
//! `Gossip` is one logical Lightning gossip message — a channel update,
//! announcement, etc. The protocol details aren't modelled here; we only
//! track an opaque ID for dedup and a byte-size for future bandwidth
//! accounting.
//!
//! `WireMessage` is what actually flows on the wire between models. It
//! has two variants:
//! * `Single(Gossip)` — used by flooding and by all originations.
//! * `Batch(Vec<Gossip>)` — used by stagger algorithms (CLN/LND) that
//!   coalesce multiple gossips into one transmission per tick.
//!
//! Receivers iterate `.iter_gossips()` and process each inner gossip
//! identically. Having one shared wire enum (rather than per-algorithm
//! types) is what lets a CLN node forward a `Batch` to an LND peer in a
//! mixed population — both nodes' `recv` ports take `WireMessage`.

use serde::{Deserialize, Serialize};

pub type NodeId = u32;
pub type MsgId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Gossip {
    pub id: MsgId,
    pub origin: NodeId,
    pub kind: GossipKind,
    /// On-the-wire size, used for bandwidth metrics (not yet wired up).
    pub size_bytes: u32,
}

/// Future-extension point for inventory entries / reconciliation
/// payloads. Currently every gossip is a `Full` message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipKind {
    Full,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMessage {
    Single(Gossip),
    Batch(Vec<Gossip>),
}

impl WireMessage {
    /// Iterate over the inner `Gossip`s regardless of variant. Receivers
    /// don't need to care whether they got one message or a hundred.
    pub fn iter_gossips(&self) -> Box<dyn Iterator<Item = &Gossip> + '_> {
        match self {
            WireMessage::Single(g) => Box::new(std::iter::once(g)),
            WireMessage::Batch(v) => Box::new(v.iter()),
        }
    }
}
