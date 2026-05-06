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

pub mod cln;
pub mod flooding;
pub mod lnd;
