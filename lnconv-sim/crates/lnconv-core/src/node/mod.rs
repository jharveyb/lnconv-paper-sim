//! Per-algorithm node implementations. One file per algorithm, each
//! exposing a struct that implements NeXosim's `Model` trait.
//!
//! The contract every node kind follows:
//! * `pub out: Output<WireMessage>` — the broadcast port wired up at
//!   sim init.
//! * `pub fn recv(&mut self, WireMessage, &Context<Self>)` — input
//!   port that peers' outputs connect to.
//! * `pub [async] fn originate(&mut self, Gossip, &Context<Self>)` —
//!   input port that the per-node `EventSource` connects to.
//! * Optional `#[nexosim(init)]` setup (used by stagger algos to arm
//!   their periodic ticks).
//! * Optional `#[nexosim(schedulable)]` helpers for delayed sends.
//!
//! Sticking to this shape is what lets the sim runner treat all kinds
//! uniformly when wiring connections and dispatching events.

use std::collections::HashMap;

use nexosim::ports::Output;

use crate::message::{NodeId, WireMessage};

pub mod cln;
pub mod flooding;
pub mod lnd;
pub mod sketch;

/// Push a per-peer Output into the three aligned containers every node
/// holds. Used by each model's `add_peer` to keep that boilerplate to a
/// single line. `outputs[i]`, `peer_ids[i]` and the `peer_id_to_local`
/// reverse-map stay in lock-step.
pub(crate) fn push_peer(
    outputs: &mut Vec<Output<WireMessage>>,
    peer_ids: &mut Vec<NodeId>,
    peer_id_to_local: &mut HashMap<NodeId, usize>,
    peer_id: NodeId,
    out: Output<WireMessage>,
) {
    let local = outputs.len();
    outputs.push(out);
    peer_ids.push(peer_id);
    peer_id_to_local.insert(peer_id, local);
}
